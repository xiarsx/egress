package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Active-probe constants (design §6.3, refmap §2.4).
const (
	// EgressTraceURL is the Cloudflare endpoint whose body carries the
	// egress IP as an "ip=" line.
	EgressTraceURL = "https://www.cloudflare.com/cdn-cgi/trace"
	// egressTraceHost is the TLS server name / HTTP host for the trace URL.
	egressTraceHost = "www.cloudflare.com"
	// egressTraceDomain is the latency domain reported alongside egress probes.
	egressTraceDomain = "cloudflare.com"

	// maxTraceBody caps how much of the trace response is read.
	maxTraceBody = 64 * 1024
)

// DefaultAuthorityDomains mirrors the reference implementation's
// latency_authorities default (refmap §2.2): these domains keep a
// permanent TD-EWMA slot (authority=true) and feed the P2C
// common-authority comparison.
var DefaultAuthorityDomains = map[string]bool{
	"gstatic.com":    true,
	"google.com":     true,
	"cloudflare.com": true,
	"github.com":     true,
}

// TrafficDeferWindow is the default window during which recent passive
// traffic pushes a node's next active probe back (design §6.3: "被动流量会
// 推迟节点的下次探测时间"). Tune via Prober.TrafficDefer.
const TrafficDeferWindow = 5 * time.Minute

// Prober performs active probes (design §6.3): egress probes against
// Cloudflare's /cdn-cgi/trace and latency probes via TLS-handshake timing
// with an HTTP-RTT fallback. Every observation is emitted as a Signal on
// the bus for the HealthProjector (and the other sinks) to consume.
//
// Passive traffic defers probing: NoteTraffic records data-plane traffic
// per node, and ScheduleNext — called by the sync orchestration when
// planning probe timers — pushes the next probe back while traffic is
// fresh. The busier the node, the fewer active probes it needs.
type Prober struct {
	// Dialer dials through the node's own outbound chain (fail-closed:
	// a node without a built outbound is a dial-time error).
	Dialer ports.NodeDialer
	// Direct dials without going through any node (unused by the current
	// probe kinds; kept for future direct reachability probes).
	Direct ports.Dialer
	// Bus receives the resulting signals.
	Bus ports.SignalBus
	// Clock supplies probe timestamps.
	Clock ports.Clock
	// Reg resolves node hashes to snapshots.
	Reg ports.NodeRegistry

	// AuthorityDomains marks latency domains whose samples are authority
	// samples (TD-EWMA permanent slots). Nil selects DefaultAuthorityDomains.
	AuthorityDomains map[string]bool
	// TrafficDefer is how long after the last passive traffic a probe is
	// deferred; zero selects TrafficDeferWindow.
	TrafficDefer time.Duration

	mu          sync.Mutex
	lastTraffic map[pknode.Hash]time.Time
}

// ProbeEgress probes the node's egress IP: GET EgressTraceURL through the
// node's dialer, parse the "ip=" line, and emit an Egress signal (invalid
// IP ⇒ failure) plus the companion Latency(cloudflare.com, authority)
// signal (design §6.3).
func (p *Prober) ProbeEgress(ctx context.Context, h pknode.Hash) {
	ip, rtt, err := p.fetchTrace(ctx, h)

	egress := pksignal.Signal{
		Kind:     pksignal.KindEgress,
		NodeHash: [16]byte(h),
		At:       p.now(),
	}
	if err == nil {
		egress.EgressIP = ip // invalid ⇒ failure, per Signal contract
	}
	p.Bus.Emit(egress)

	lat := pksignal.Signal{
		Kind:      pksignal.KindLatency,
		NodeHash:  [16]byte(h),
		At:        p.now(),
		Domain:    egressTraceDomain,
		Authority: true,
	}
	if err == nil {
		lat.Latency = rtt
		lat.HasLatency = true
	}
	p.Bus.Emit(lat)
}

// fetchTrace performs the trace request through the node's dialer and
// returns the parsed egress IP and the request round-trip time.
func (p *Prober) fetchTrace(ctx context.Context, h pknode.Hash) (netip.Addr, time.Duration, error) {
	start := p.now()
	tr := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			raw, err := p.Dialer.DialContext(ctx, h, network, addr)
			if err != nil {
				return nil, err
			}
			tc := tls.Client(raw, &tls.Config{ServerName: egressTraceHost})
			if err := tc.HandshakeContext(ctx); err != nil {
				raw.Close()
				return nil, err
			}
			return tc, nil
		},
	}
	// No keep-alives: probes are one-shot.
	tr.DisableKeepAlives = true
	client := &http.Client{Transport: tr}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, EgressTraceURL, nil)
	if err != nil {
		return netip.Addr{}, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return netip.Addr{}, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTraceBody))
	if err != nil {
		return netip.Addr{}, 0, err
	}
	rtt := p.now().Sub(start)
	ip, ok := parseTraceIP(string(body))
	if !ok {
		return netip.Addr{}, rtt, fmt.Errorf("prober: no ip= line in trace response")
	}
	return ip, rtt, nil
}

// parseTraceIP extracts the "ip=" line from a /cdn-cgi/trace body.
func parseTraceIP(body string) (netip.Addr, bool) {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "ip="); ok {
			if ip, err := netip.ParseAddr(strings.TrimSpace(v)); err == nil {
				return ip, true
			}
			return netip.Addr{}, false
		}
	}
	return netip.Addr{}, false
}

// ProbeLatency measures the TLS handshake to domain:443 through the node's
// dialer and emits a Latency signal (design §6.3). When the TLS handshake
// is unavailable it falls back to a plain-HTTP request RTT on port 80; when
// both fail the signal carries no latency (failure).
func (p *Prober) ProbeLatency(ctx context.Context, h pknode.Hash, domain string) {
	domain = strings.ToLower(strings.TrimSpace(domain))
	at := p.now()
	sig := pksignal.Signal{
		Kind:      pksignal.KindLatency,
		NodeHash:  [16]byte(h),
		At:        at,
		Domain:    domain,
		Authority: p.isAuthority(domain),
	}

	if ms, ok := p.tlsHandshakeTime(ctx, h, domain); ok {
		sig.Latency = ms
		sig.HasLatency = true
	} else if ms, ok := p.httpRTT(ctx, h, domain); ok {
		sig.Latency = ms
		sig.HasLatency = true
	}
	p.Bus.Emit(sig)
}

// tlsHandshakeTime dials domain:443 through the node and times the TLS handshake.
func (p *Prober) tlsHandshakeTime(ctx context.Context, h pknode.Hash, domain string) (time.Duration, bool) {
	start := p.now()
	raw, err := p.Dialer.DialContext(ctx, h, "tcp", net.JoinHostPort(domain, "443"))
	if err != nil {
		return 0, false
	}
	defer raw.Close()
	tc := tls.Client(raw, &tls.Config{ServerName: domain})
	if err := tc.HandshakeContext(ctx); err != nil {
		return 0, false
	}
	_ = tc.Close()
	return p.now().Sub(start), true
}

// httpRTT is the no-TLS fallback: GET / on port 80 through the node, timed
// to the first response byte.
func (p *Prober) httpRTT(ctx context.Context, h pknode.Hash, domain string) (time.Duration, bool) {
	start := p.now()
	raw, err := p.Dialer.DialContext(ctx, h, "tcp", net.JoinHostPort(domain, "80"))
	if err != nil {
		return 0, false
	}
	defer raw.Close()
	if _, err := fmt.Fprintf(raw, "GET / HTTP/1.0\r\nHost: %s\r\nConnection: close\r\n\r\n", domain); err != nil {
		return 0, false
	}
	// Read one byte: time-to-first-byte is the RTT sample.
	var one [1]byte
	if _, err := io.ReadFull(raw, one[:]); err != nil {
		return 0, false
	}
	return p.now().Sub(start), true
}

// NoteTraffic records passive data-plane traffic for a node. The next
// scheduling decision (ScheduleNext) will defer the node's active probe
// while the traffic is fresh.
func (p *Prober) NoteTraffic(h pknode.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastTraffic == nil {
		p.lastTraffic = make(map[pknode.Hash]time.Time)
	}
	p.lastTraffic[h] = p.now()
}

// ScheduleNext advises the sync orchestration when a node's next probe
// should run: now when no recent traffic was seen, otherwise last-traffic
// + defer window (design §6.3: traffic-heavy nodes are probed less).
func (p *Prober) ScheduleNext(h pknode.Hash) time.Time {
	window := p.TrafficDefer
	if window <= 0 {
		window = TrafficDeferWindow
	}
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if last, ok := p.lastTraffic[h]; ok && now.Sub(last) < window {
		return last.Add(window)
	}
	return now
}

func (p *Prober) isAuthority(domain string) bool {
	if p.AuthorityDomains != nil {
		return p.AuthorityDomains[domain]
	}
	return DefaultAuthorityDomains[domain]
}

func (p *Prober) now() time.Time {
	if p.Clock != nil {
		return p.Clock.Now()
	}
	return time.Now()
}
