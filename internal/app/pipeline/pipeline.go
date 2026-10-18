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
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/ports"
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

// X-Resin-Error codes (spec/errors.md §1). CodeClientCanceled is a
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
)

// ProxyError is a classified proxy failure. A nil *ProxyError means
// success.
type ProxyError struct {
	Code string // one of the Code* constants above
	Msg  string // human-readable detail (text/plain body)
}

func (e *ProxyError) Error() string { return "pipeline: " + e.Code + ": " + e.Msg }

// HTTPStatus maps an X-Resin-Error code to its HTTP status (spec/errors.md
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

	PickPolicy pick.Policy
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

	NodeHash      pknode.Hash // zero for direct/bypass
	PlatformID    string
	Direct        bool
	HasTLS        bool
	TLSHandshake  time.Duration
	BreakerExempt bool
	startedAt     time.Time
}

// Handle runs the full flow: Resolve → Route → Dial → Relay, then emits
// the Traffic signal. It returns nil on success. The client connection
// is never closed by Handle; the upstream connection always is.
func (p *Pipeline) Handle(ctx context.Context, client net.Conn, req Request) *ProxyError {
	prep, perr := p.Prepare(ctx, req)
	if perr != nil {
		return perr
	}
	return p.Relay(ctx, prep, client)
}

// Prepare runs Resolve → Route → Dial and returns the dialed upstream
// connection. Adapters that must send a success reply between dial and
// relay (HTTP CONNECT's 200, SOCKS5's REP 0x00) use Prepare + Relay
// instead of Handle, so dial errors stay mappable to error responses
// (spec/errors.md §2–§3). On dial failure a fail Traffic signal is
// emitted before returning.
func (p *Pipeline) Prepare(ctx context.Context, req Request) (*Prepared, *ProxyError) {
	r, perr := p.resolve(req)
	if perr != nil {
		return nil, perr
	}
	rt, perr := p.route(req, r)
	if perr != nil {
		return nil, perr
	}
	prep := &Prepared{
		Head:          req.Head,
		PlatformID:    r.platformID,
		Direct:        rt.direct,
		BreakerExempt: p.d.PassiveBreakerOff != nil && p.d.PassiveBreakerOff(r.platformID),
		startedAt:     p.d.Clock.Now(),
	}
	if !rt.direct {
		prep.NodeHash = rt.node.Hash
	}

	raw, perr := p.dialUpstream(ctx, req, rt)
	if perr != nil {
		// Client-initiated cancel is not a node failure and produces no
		// signal (spec/errors.md §1.4).
		if perr.Code != CodeClientCanceled {
			p.emit(prep, false, 0, 0, 0, false)
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
				return nil, &ProxyError{Code: CodeUpstreamTimeout, Msg: "TLS handshake timed out: " + herr.Error()}
			}
			return nil, &ProxyError{Code: CodeUpstreamRequestFailed, Msg: "TLS handshake failed: " + herr.Error()}
		}
		prep.HasTLS = true
		prep.TLSHandshake = tc.latency()
		up = tlsConn
	}
	prep.Upstream = up
	return prep, nil
}

// Relay writes the prepared head bytes, pumps both directions with byte
// counting, and emits the Traffic signal. It closes the upstream
// connection; the client connection stays open for the adapter to close.
func (p *Pipeline) Relay(ctx context.Context, prep *Prepared, client net.Conn) *ProxyError {
	up := prep.Upstream
	defer func() { _ = up.Close() }()

	var upN int64
	if len(prep.Head) > 0 {
		n, werr := writeFull(up, prep.Head)
		upN = int64(n)
		if werr != nil {
			werr = contextCause(ctx, werr)
			if errors.Is(werr, context.Canceled) {
				return &ProxyError{Code: CodeClientCanceled, Msg: "client canceled during head write"}
			}
			p.emit(prep, false, upN, 0, prep.TLSHandshake, prep.HasTLS)
			if isTimeoutErr(werr) {
				return &ProxyError{Code: CodeUpstreamTimeout, Msg: "upstream write timed out: " + werr.Error()}
			}
			return &ProxyError{Code: CodeUpstreamRequestFailed, Msg: "upstream write failed: " + werr.Error()}
		}
	}

	downN, upExtra, clean := relayConns(client, up)
	upN += upExtra
	p.emit(prep, clean, upN, downN, prep.TLSHandshake, prep.HasTLS)
	if !clean {
		return &ProxyError{Code: CodeUpstreamRequestFailed, Msg: "relay failed"}
	}
	return nil
}

// --- Resolve ---

type resolved struct {
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
	return resolved{platformID: platformID, account: account}, nil
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
	return pick.Select(
		v,
		nodesFunc(p.d.Nodes),
		ipLoadFunc(func(ip netip.Addr) int64 { return p.d.IPLoad(r.platformID, ip) }),
		latencyFunc(p.d.Latency),
		r.account,
		l,
		p.d.PickPolicy,
		hostOnly(req.Target),
		p.d.Clock.Now(),
		p.deriveRng(),
	)
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
