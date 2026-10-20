// Package pipeline implements the data-plane pipeline (design §4.1):
//
//	Ingress → Resolve → Route → Dial → Relay → emit Traffic Signal
//
// Resolve turns the wire identity into (PlatformID, Account): identity
// parsing (spec/identity.md), token authentication, then the platform's
// IdentityPolicy chain. Route consults the bypass rules, otherwise runs
// pick.Select over the platform View and LeaseTable. Dial opens the
// upstream connection (direct dialer for bypass, NodeDialer otherwise,
// with a TLS-handshake timing wrapper for https). Relay writes the
// pre-read head bytes and then pumps both directions with byte counting,
// finally emitting one Traffic Signal (design §6.1).
//
// Hot-path discipline (design P2): the pipeline performs no I/O except
// dialing and relaying itself, never touches the Store, and holds no
// global locks. All state it reads (views, leases, latencies, platform
// metadata) arrives through the narrow read-only Deps callbacks, whose
// single writers live elsewhere.
package pipeline

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/pkg/ext"
	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Ingress protocol identifiers carried in Request.Proto.
const (
	ProtoHTTPForward = "http-forward"
	ProtoSOCKS5      = "socks5"
	ProtoHTTPReverse = "http-reverse"
)

// X-Egress-Error codes (spec/errors.md §1). CodeClientCanceled is a
// pipeline-internal extension, not part of the spec enum: it means the
// downstream client went away mid-flight, so adapters must close the
// connection silently instead of writing an error response.
const (
	CodeAuthRequired          = "AUTH_REQUIRED"
	CodeAuthFailed            = "AUTH_FAILED"
	CodeURLParseError         = "URL_PARSE_ERROR"
	CodeInvalidProtocol       = "INVALID_PROTOCOL"
	CodeInvalidHost           = "INVALID_HOST"
	CodePlatformNotFound      = "PLATFORM_NOT_FOUND"
	CodeAccountRejected       = "ACCOUNT_REJECTED"
	CodeNoAvailableNodes      = "NO_AVAILABLE_NODES"
	CodeUpstreamConnectFailed = "UPSTREAM_CONNECT_FAILED"
	CodeUpstreamTimeout       = "UPSTREAM_TIMEOUT"
	CodeUpstreamRequestFailed = "UPSTREAM_REQUEST_FAILED"
	CodeInternalError         = "INTERNAL_ERROR"
	CodeClientCanceled        = "CLIENT_CANCELED"
	// CodeCapabilityDisabled is returned when the addressed endpoint does
	// not allow the requested ingress kind (spec/errors.md §1.5).
	CodeCapabilityDisabled = "ENDPOINT_CAPABILITY_DISABLED"
)

// ProxyError is a classified proxy failure. A nil *ProxyError means
// success.
type ProxyError struct {
	Code string // one of the Code* constants above
	Msg  string // human-readable detail (text/plain body)
}

func (e *ProxyError) Error() string { return "pipeline: " + e.Code + ": " + e.Msg }

// WriteHTTPError writes the spec/errors.md §1 HTTP error response for
// handler-based ingress (forward/reverse/endpoint mux): X-Egress-Error
// header, text/plain body, Proxy-Authenticate on 407.
// CodeClientCanceled is silent: the client is already gone.
func WriteHTTPError(w http.ResponseWriter, perr *ProxyError) {
	if perr == nil || perr.Code == CodeClientCanceled {
		return
	}
	status := HTTPStatus(perr.Code)
	h := w.Header()
	h.Set("X-Egress-Error", perr.Code)
	h.Set("Content-Type", "text/plain; charset=utf-8")
	if perr.Code == CodeAuthRequired {
		h.Set("Proxy-Authenticate", `Basic realm="egress"`)
	}
	msg := perr.Msg
	if msg == "" {
		msg = perr.Code
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, msg)
}

// HTTPStatus maps an X-Egress-Error code to its HTTP status (spec/errors.md
// §1). Unknown codes map to 500.
func HTTPStatus(code string) int {
	switch code {
	case CodeAuthRequired:
		return 407
	case CodeAuthFailed, CodeAccountRejected:
		return 403
	case CodeURLParseError, CodeInvalidProtocol, CodeInvalidHost:
		return 400
	case CodePlatformNotFound:
		return 404
	case CodeNoAvailableNodes:
		return 503
	case CodeCapabilityDisabled:
		return 403
	case CodeUpstreamConnectFailed, CodeUpstreamRequestFailed:
		return 502
	case CodeUpstreamTimeout:
		return 504
	default:
		return 500
	}
}

// Request is one proxy request as parsed by an ingress adapter. The
// identity is resolved exactly once here; downstream stages only see the
// resulting PlatformID.
type Request struct {
	// Token is the credential's token part, split from the identity by
	// the adapter: Proxy-Authorization password / SOCKS5 RFC1929 password
	// / reverse path first segment.
	Token string
	// RawIdentity is the identity string without the token:
	// "Platform.Account" (the account may itself contain ':').
	RawIdentity string
	// Target is the upstream "host:port".
	Target string
	// Headers carries request headers with lowercase keys (HTTP-class
	// ingress); empty for SOCKS5. Only the first value per key is kept.
	Headers map[string]string
	// Proto is one of ProtoHTTPForward / ProtoSOCKS5 / ProtoHTTPReverse.
	Proto string
	// RemoteAddr is the downstream client's address (observability only).
	RemoteAddr string
	// Head holds bytes already read from the client that must be
	// forwarded to upstream first (e.g. a plain HTTP forward request).
	// Empty for tunnel-style requests (CONNECT, SOCKS5).
	Head []byte
	// Scheme is "http" or "https" when the ingress knows the upstream
	// scheme (reverse proxy, absolute-URI https forward). The pipeline
	// performs the TLS handshake itself for "https" and times it for the
	// Traffic signal. "" means tunnel semantics: no TLS handling here.
	Scheme string
}

// PlatformResolver maps platform names to IDs and provides each
// platform's IdentityPolicy. "Default" always resolves.
type PlatformResolver interface {
	ResolvePlatform(name string) (id string, ok bool)
	PolicyFor(id string) *identity.Policy
}

// Deps wires the pipeline to the rest of the system. Every callback is a
// read-only view over state owned elsewhere (design §2.1: the data plane
// holds no mutable shared state of its own).
type Deps struct {
	Resolver   PlatformResolver
	NodeDialer ports.NodeDialer // dial through a node's outbound
	Direct     ports.Dialer     // direct dial for bypass
	Bus        ports.SignalBus  // non-blocking Traffic signal emission
	Clock      ports.Clock

	Leases *lease.LeaseTable
	Views  func(platformID string) *view.View
	Nodes  func(h pknode.Hash) (pknode.NodeSnapshot, bool)
	// Latency returns a recent per-domain EWMA latency for a node.
	Latency func(h pknode.Hash, domain string) (time.Duration, bool)
	IPLoad  func(platformID string, ip netip.Addr) int64

	// PickPolicyFor returns the selection policy for one platform
	// (per-platform allocation and sticky TTL). Nil ⇒ built-in default
	// (BALANCED, pick.DefaultStickyTTL, no authority comparison).
	PickPolicyFor func(platformID string) pick.Policy
	// Bypass reports whether target ("host:port") goes direct.
	Bypass func(target string) bool
	Rand   *rand.Rand // *math/rand/v2; guarded internally

	// ProxyToken is the expected V1 line-protocol token (EGRESS_PROXY_TOKEN).
	// Empty selects auth-disabled mode: no token comparison is performed.
	ProxyToken string
	// PassiveBreakerOff reports whether a platform disabled its passive
	// breaker; such platforms' traffic is emitted with BreakerExempt.
	// Nil means the breaker is on for every platform.
	PassiveBreakerOff func(platformID string) bool

	// ReqLog consumes the terminal RequestLogRecord of every request
	// (spec/events.md §2), exactly once per request, success or failure.
	// Client-canceled requests produce no record. Must be non-blocking
	// (bounded staging, shed on full); nil disables logging.
	// Adapter-level rejections before the pipeline runs (e.g. reverse-path
	// URL_PARSE_ERROR) are out of scope.
	ReqLog func(rec ports.RequestLogRecord)

	// ActiveSessions, when non-nil, is incremented while a relay runs and
	// decremented when it ends (realtime connections metrics).
	ActiveSessions *atomic.Int64
}

// Pipeline is the Resolve → Route → Dial → Relay chain.
type Pipeline struct {
	d          Deps
	rngMu      sync.Mutex
	tokenValid bool // configured ProxyToken passes identity.ValidateToken
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// New assembles a Pipeline, filling harmless defaults for nil Deps. It
// validates the configured ProxyToken once: a misconfigured token fails
// every request with INTERNAL_ERROR rather than silently disabling auth.
func New(d Deps) *Pipeline {
	if d.Clock == nil {
		d.Clock = systemClock{}
	}
	if d.Rand == nil {
		d.Rand = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0x9E3779B97F4A7C15))
	}
	if d.Views == nil {
		d.Views = func(string) *view.View { return nil }
	}
	if d.Nodes == nil {
		d.Nodes = func(pknode.Hash) (pknode.NodeSnapshot, bool) { return pknode.NodeSnapshot{}, false }
	}
	if d.Latency == nil {
		d.Latency = func(pknode.Hash, string) (time.Duration, bool) { return 0, false }
	}
	if d.IPLoad == nil {
		d.IPLoad = func(string, netip.Addr) int64 { return 0 }
	}
	p := &Pipeline{d: d, tokenValid: true}
	if d.ProxyToken != "" {
		if err := identity.ValidateToken(d.ProxyToken); err != nil {
			p.tokenValid = false
		}
	}
	return p
}

// ProxyToken returns the configured expected token ("", when auth is
// disabled). Adapters use it for wire-level auth decisions (SOCKS5 method
// negotiation, 407-on-malformed-header).
func (p *Pipeline) ProxyToken() string { return p.d.ProxyToken }

// Prepared is the result of Resolve → Route → Dial, ready for the
// ingress to write its success reply (200 / SOCKS5 REP 0x00) before
// Relay starts pumping bytes.
type Prepared struct {
	Upstream net.Conn
	Head     []byte

	// headWritten/headUp record a WriteHead already done by the adapter
	// (plain-request adapters write the head BEFORE hijacking the client
	// so a failure is still mappable to an HTTP error response).
	headWritten bool
	headUp      int64

	NodeHash      pknode.Hash // zero for direct/bypass
	PlatformID    string
	platform      string // wire name, mirrors resolved.platform
	Direct        bool
	HasTLS        bool
	TLSHandshake  time.Duration
	BreakerExempt bool
	startedAt     time.Time
	egressIP      netip.Addr
	account       string
}

// Handle runs the full flow: Resolve → Route → Dial → Relay, then emits
// the Traffic signal. It returns nil on success. The client connection
// is never closed by Handle; the upstream connection always is.
func (p *Pipeline) Handle(ctx context.Context, client net.Conn, req Request) *ProxyError {
	prep, perr := p.Prepare(ctx, req)
	if perr != nil {
		return perr
	}
	return p.Relay(ctx, prep, client, req)
}

// Prepare runs Resolve → Route → Dial and returns the dialed upstream
// connection. Adapters that must send a success reply between dial and
// relay (HTTP CONNECT's 200, SOCKS5's REP 0x00) use Prepare + Relay
// instead of Handle, so dial errors stay mappable to error responses
// (spec/errors.md §2–§3). On dial failure a fail Traffic signal is
// emitted before returning.
func (p *Pipeline) Prepare(ctx context.Context, req Request) (*Prepared, *ProxyError) {
	start := p.d.Clock.Now()
	r, perr := p.resolve(req)
	if perr != nil {
		p.logRequest(req, start, resolved{}, nil, false, "", perr, 0, 0)
		return nil, perr
	}
	rt, perr := p.route(req, r)
	if perr != nil {
		p.logRequest(req, start, r, nil, false, "", perr, 0, 0)
		return nil, perr
	}
	prep := &Prepared{
		Head:          req.Head,
		PlatformID:    r.platformID,
		platform:      r.platform,
		Direct:        rt.direct,
		BreakerExempt: p.d.PassiveBreakerOff != nil && p.d.PassiveBreakerOff(r.platformID),
		startedAt:     start,
		egressIP:      rt.node.EgressIP,
	}
	if !rt.direct {
		prep.NodeHash = rt.node.Hash
	}

	raw, perr := p.dialUpstream(ctx, req, rt)
	if perr != nil {
		// Client-initiated cancel is not a node failure and produces no
		// signal and no log record (spec/errors.md §1.4).
		if perr.Code != CodeClientCanceled {
			p.emit(prep, false, 0, 0, 0, false)
			p.logRequest(req, start, r, prep, false, "dial", perr, 0, 0)
		}
		return nil, perr
	}

	up := raw
	if req.Scheme == "https" {
		tc := &tlsTimingConn{Conn: raw}
		tlsConn := tls.Client(tc, &tls.Config{ServerName: hostOnly(req.Target)})
		if herr := tlsConn.HandshakeContext(ctx); herr != nil {
			_ = raw.Close()
			herr = contextCause(ctx, herr)
			if errors.Is(herr, context.Canceled) {
				return nil, &ProxyError{Code: CodeClientCanceled, Msg: "client canceled during TLS handshake"}
			}
			p.emit(prep, false, 0, 0, 0, false)
			if isTimeoutErr(herr) {
				perr = &ProxyError{Code: CodeUpstreamTimeout, Msg: "TLS handshake timed out: " + herr.Error()}
			} else {
				perr = &ProxyError{Code: CodeUpstreamRequestFailed, Msg: "TLS handshake failed: " + herr.Error()}
			}
			p.logRequest(req, start, r, prep, false, "tls", perr, 0, 0)
			return nil, perr
		}
		prep.HasTLS = true
		prep.TLSHandshake = tc.latency()
		up = tlsConn
	}
	prep.Upstream = up
	prep.account = r.account
	return prep, nil
}

// WriteHead writes the prepared head bytes upstream (plain-request
// adapters call this BEFORE hijacking the client connection so a failure
// is still mappable to a spec/errors.md HTTP response). It is idempotent
// per Prepared: Relay skips the head when WriteHead already ran.
func (p *Pipeline) WriteHead(ctx context.Context, prep *Prepared, req Request) (int64, *ProxyError) {
	if prep.headWritten || len(prep.Head) == 0 {
		return prep.headUp, nil
	}
	n, werr := writeFull(prep.Upstream, prep.Head)
	upN := int64(n)
	if werr == nil {
		prep.headWritten, prep.headUp = true, upN
		return upN, nil
	}
	werr = contextCause(ctx, werr)
	if errors.Is(werr, context.Canceled) {
		return upN, &ProxyError{Code: CodeClientCanceled, Msg: "client canceled during head write"}
	}
	p.emit(prep, false, upN, 0, prep.TLSHandshake, prep.HasTLS)
	var perr *ProxyError
	if isTimeoutErr(werr) {
		perr = &ProxyError{Code: CodeUpstreamTimeout, Msg: "upstream write timed out: " + werr.Error()}
	} else {
		perr = &ProxyError{Code: CodeUpstreamRequestFailed, Msg: "upstream write failed: " + werr.Error()}
	}
	p.logPrepared(req, prep, false, "request", perr, upN, 0)
	return upN, perr
}

// Relay writes the prepared head bytes, pumps both directions with byte
// counting, and emits the Traffic signal. It closes the upstream
// connection; the client connection stays open for the adapter to close.
func (p *Pipeline) Relay(ctx context.Context, prep *Prepared, client net.Conn, req Request) *ProxyError {
	if p.d.ActiveSessions != nil {
		p.d.ActiveSessions.Add(1)
		defer p.d.ActiveSessions.Add(-1)
	}
	up := prep.Upstream
	defer func() { _ = up.Close() }()

	var upN int64
	if prep.headWritten {
		upN = prep.headUp
	} else if len(prep.Head) > 0 {
		n, perr := p.WriteHead(ctx, prep, req)
		upN = n
		if perr != nil {
			return perr
		}
	}

	downN, upExtra, clean := relayConns(client, up)
	upN += upExtra
	p.emit(prep, clean, upN, downN, prep.TLSHandshake, prep.HasTLS)
	if !clean {
		perr := &ProxyError{Code: CodeUpstreamRequestFailed, Msg: "relay failed"}
		p.logPrepared(req, prep, false, "tunnel", perr, upN, downN)
		return perr
	}
	p.logPrepared(req, prep, true, "", nil, upN, downN)
	return nil
}

// --- Resolve ---

type resolved struct {
	platform   string // wire name (identity prefix), for request logs
	platformID string
	account    string
}

func (p *Pipeline) resolve(req Request) (resolved, *ProxyError) {
	if !p.tokenValid {
		return resolved{}, &ProxyError{Code: CodeInternalError, Msg: "proxy token misconfigured"}
	}
	if p.d.ProxyToken != "" {
		// Format check first (a token that fails it can never match).
		if err := identity.ValidateToken(req.Token); err != nil {
			return resolved{}, &ProxyError{Code: CodeAuthFailed, Msg: "invalid token: " + err.Error()}
		}
		switch {
		case req.Token == "":
			// Missing token: 407 on HTTP forward (spec §1.1), 403
			// elsewhere (reverse path token, SOCKS5 password).
			if req.Proto == ProtoHTTPForward {
				return resolved{}, &ProxyError{Code: CodeAuthRequired, Msg: "proxy authentication required"}
			}
			return resolved{}, &ProxyError{Code: CodeAuthFailed, Msg: "token required"}
		case subtle.ConstantTimeCompare([]byte(req.Token), []byte(p.d.ProxyToken)) != 1:
			return resolved{}, &ProxyError{Code: CodeAuthFailed, Msg: "token mismatch"}
		}
	}

	id, err := identity.Parse(req.RawIdentity)
	if err != nil {
		return resolved{}, &ProxyError{Code: CodeInternalError, Msg: "identity parse failed: " + err.Error()}
	}
	platformID, ok := p.d.Resolver.ResolvePlatform(id.PlatformID)
	if !ok {
		return resolved{}, &ProxyError{Code: CodePlatformNotFound, Msg: "platform not found: " + id.PlatformID}
	}
	pol := p.d.Resolver.PolicyFor(platformID)
	policy := identity.DefaultPolicy()
	if pol != nil {
		policy = *pol
	}
	account, ok := policy.Resolve(identity.Meta{
		Headers:     req.Headers,
		Target:      hostOnly(req.Target),
		WireAccount: id.Account,
		IsHTTP:      req.Proto != ProtoSOCKS5,
	})
	if !ok {
		return resolved{}, &ProxyError{Code: CodeAccountRejected, Msg: "account rejected by platform policy"}
	}
	return resolved{platform: id.PlatformID, platformID: platformID, account: account}, nil
}

// --- Route ---

type routed struct {
	node       pknode.NodeSnapshot
	direct     bool
	platformID string
	account    string
}

func (p *Pipeline) route(req Request, r resolved) (routed, *ProxyError) {
	if p.d.Bypass != nil && p.d.Bypass(req.Target) {
		return routed{direct: true, platformID: r.platformID, account: r.account}, nil
	}
	v := p.d.Views(r.platformID)
	if v == nil {
		return routed{}, &ProxyError{Code: CodeNoAvailableNodes, Msg: "no view for platform"}
	}
	var l *pklease.Lease
	if r.account != "" && p.d.Leases != nil {
		if stored, ok := p.d.Leases.Get(r.platformID, r.account); ok {
			cp := stored
			l = &cp
		}
	}
	outcome, err := p.selectNode(v, r, l, req)
	if err == pick.ErrNodeGone {
		// Lost a race with node removal; one retry, mirroring the
		// reference's livePickAttempts.
		outcome, err = p.selectNode(v, r, l, req)
	}
	switch {
	case err == nil:
		// proceed
	case errors.Is(err, pick.ErrNoAvailableNodes):
		return routed{}, &ProxyError{Code: CodeNoAvailableNodes, Msg: "no available nodes"}
	default:
		return routed{}, &ProxyError{Code: CodeInternalError, Msg: "route failed: " + err.Error()}
	}
	if outcome.Put != nil && r.account != "" && p.d.Leases != nil {
		p.d.Leases.Put(r.platformID, r.account, *outcome.Put)
	}
	return routed{node: outcome.Node, platformID: r.platformID, account: r.account}, nil
}

func (p *Pipeline) selectNode(v *view.View, r resolved, l *pklease.Lease, req Request) (pick.Outcome, error) {
	var pol pick.Policy
	if p.d.PickPolicyFor != nil {
		pol = p.d.PickPolicyFor(r.platformID)
	}
	if pol.SelectorName != "" {
		return p.selectViaExt(v, r, req, pol.SelectorName)
	}
	return pick.Select(
		v,
		nodesFunc(p.d.Nodes),
		ipLoadFunc(func(ip netip.Addr) int64 { return p.d.IPLoad(r.platformID, ip) }),
		latencyFunc(p.d.Latency),
		r.account,
		l,
		pol,
		hostOnly(req.Target),
		p.d.Clock.Now(),
		p.deriveRng(),
	)
}

// selectViaExt routes the pick to a registered ext.Selector
// (spec/ext-abi.md §2). Sticky leases are bypassed: the selector owns
// stickiness. An unregistered name is a configuration error surfaced as
// NO_AVAILABLE_NODES (fail-closed, never a silent fallback).
func (p *Pipeline) selectViaExt(v *view.View, r resolved, req Request, name string) (pick.Outcome, error) {
	sel, ok := ext.GetSelector(name)
	if !ok {
		return pick.Outcome{}, pick.ErrNoAvailableNodes
	}
	cands := make([]pknode.NodeSnapshot, 0, v.Size())
	v.Range(func(h pknode.Hash) bool {
		if snap, found := p.d.Nodes(h); found {
			cands = append(cands, snap)
		}
		return true
	})
	if len(cands) == 0 {
		return pick.Outcome{}, pick.ErrNoAvailableNodes
	}
	h := sel.Pick(cands, ext.SelectContext{Target: req.Target, PlatformID: r.platformID})
	snap, found := p.d.Nodes(h)
	if !found {
		return pick.Outcome{}, pick.ErrNodeGone
	}
	if !v.Contains(h) {
		// A selector returning a node outside its candidate list is a bug;
		// refuse it rather than route around the platform view.
		return pick.Outcome{}, fmt.Errorf("select: ext selector %q picked a node outside the view", name)
	}
	return pick.Outcome{Node: snap}, nil
}

// deriveRng mints a per-request RNG from the shared source under a short
// critical section, so concurrent requests never share a *rand.Rand.
func (p *Pipeline) deriveRng() *rand.Rand {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return rand.New(rand.NewPCG(p.d.Rand.Uint64(), p.d.Rand.Uint64()))
}

// --- Dial ---

func (p *Pipeline) dialUpstream(ctx context.Context, req Request, rt routed) (net.Conn, *ProxyError) {
	var raw net.Conn
	var err error
	if rt.direct {
		if p.d.Direct == nil {
			return nil, &ProxyError{Code: CodeInternalError, Msg: "no direct dialer"}
		}
		raw, err = p.d.Direct.DialContext(ctx, "tcp", req.Target)
	} else {
		if p.d.NodeDialer == nil {
			return nil, &ProxyError{Code: CodeInternalError, Msg: "no node dialer"}
		}
		raw, err = p.d.NodeDialer.DialContext(ctx, rt.node.Hash, "tcp", req.Target)
	}
	if err != nil {
		err = contextCause(ctx, err)
		if errors.Is(err, context.Canceled) {
			return nil, &ProxyError{Code: CodeClientCanceled, Msg: "client canceled during dial"}
		}
		if isTimeoutErr(err) {
			return nil, &ProxyError{Code: CodeUpstreamTimeout, Msg: "upstream dial timed out: " + err.Error()}
		}
		return nil, &ProxyError{Code: CodeUpstreamConnectFailed, Msg: "upstream dial failed: " + err.Error()}
	}
	return raw, nil
}

// logRequest emits the terminal RequestLogRecord for one request
// (spec/events.md §2). prep may be nil for pre-route failures; perr nil
// means success.
// logPrepared is logRequest for the prep-backed call sites: the resolved
// identity is read back out of the Prepared state.
func (p *Pipeline) logPrepared(req Request, prep *Prepared, netOK bool, stage string, perr *ProxyError, upN, downN int64) {
	p.logRequest(req, prep.startedAt, resolved{platform: prep.platform, platformID: prep.PlatformID, account: prep.account}, prep, netOK, stage, perr, upN, downN)
}

func (p *Pipeline) logRequest(req Request, start time.Time, r resolved, prep *Prepared, netOK bool, stage string, perr *ProxyError, upN, downN int64) {
	if p.d.ReqLog == nil {
		return
	}
	rec := ports.RequestLogRecord{
		TS:         start,
		ProxyType:  req.Proto,
		Platform:   r.platform,
		PlatformID: r.platformID,
		Account:    r.account,
		Target:     req.Target,
		NetOK:      netOK,
		Stage:      stage,
		DurationMs: p.d.Clock.Now().Sub(start).Milliseconds(),
		BytesUp:    upN,
		BytesDown:  downN,
	}
	if prep != nil {
		rec.NodeHash = prep.NodeHash
		rec.EgressIP = prep.egressIP
	}
	if perr != nil {
		rec.EgressError = perr.Code
	}
	p.d.ReqLog(rec)
}

// --- Signal ---

func (p *Pipeline) emit(prep *Prepared, ok bool, upN, downN int64, tlsMs time.Duration, hasTLS bool) {
	if p.d.Bus == nil {
		return
	}
	p.d.Bus.Emit(pksignal.Signal{
		Kind:          pksignal.KindTraffic,
		NodeHash:      prep.NodeHash, // zero for direct/bypass
		At:            p.d.Clock.Now(),
		PlatformID:    prep.PlatformID,
		OK:            ok,
		BreakerExempt: prep.BreakerExempt,
		TLSHandshake:  tlsMs,
		HasTLS:        hasTLS,
		BytesUp:       uint64(upN),
		BytesDown:     uint64(downN),
	})
}

// --- small helpers ---

// hostOnly strips an optional ":port" suffix for domain-oriented uses
// (latency lookup, header-rule prefix matching, TLS ServerName).
func hostOnly(target string) string {
	if h, _, err := net.SplitHostPort(target); err == nil && h != "" {
		return h
	}
	return target
}

// contextCause prefers the context's own verdict: a dial error that
// merely wraps the client's cancellation must surface as canceled, not
// as a node failure.
func contextCause(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() == context.Canceled && !errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	return err
}
