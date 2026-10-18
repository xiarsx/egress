package pipeline

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// --- fakes ---

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }

type fakeResolver struct {
	platforms map[string]string // name → id
	policies  map[string]*identity.Policy
}

func (f *fakeResolver) ResolvePlatform(name string) (string, bool) {
	id, ok := f.platforms[name]
	return id, ok
}

func (f *fakeResolver) PolicyFor(id string) *identity.Policy { return f.policies[id] }

type fakeBus struct {
	mu  sync.Mutex
	got []pksignal.Signal
}

func (b *fakeBus) Emit(s pksignal.Signal) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.got = append(b.got, s)
}
func (b *fakeBus) Subscribe() <-chan pksignal.Signal { return nil }
func (b *fakeBus) last() (pksignal.Signal, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.got) == 0 {
		return pksignal.Signal{}, false
	}
	return b.got[len(b.got)-1], true
}
func (b *fakeBus) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.got)
}

// fakeNodeDialer dials a pipe; runEcho serves the far end with a canned
// response after consuming the head.
type fakeNodeDialer struct {
	mu      sync.Mutex
	hashes  []pknode.Hash
	serve   func(up net.Conn)
	dialErr error
}

func (f *fakeNodeDialer) DialContext(ctx context.Context, h pknode.Hash, network, address string) (net.Conn, error) {
	if f.dialErr != nil {
		return nil, f.dialErr
	}
	f.mu.Lock()
	f.hashes = append(f.hashes, h)
	f.mu.Unlock()
	up, far := net.Pipe()
	if f.serve != nil {
		go f.serve(far)
	} else {
		// Discard everything and close: lets relay's upstream→client
		// copy observe EOF instead of blocking forever.
		go func() { _, _ = io.Copy(io.Discard, far); _ = far.Close() }()
	}
	return up, nil
}

func (f *fakeNodeDialer) dialed() []pknode.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pknode.Hash(nil), f.hashes...)
}

type fakeDirect struct {
	mu      sync.Mutex
	targets []string
	serve   func(up net.Conn)
}

func (f *fakeDirect) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	f.mu.Lock()
	f.targets = append(f.targets, address)
	f.mu.Unlock()
	up, far := net.Pipe()
	if f.serve != nil {
		go f.serve(far)
	} else {
		go func() { _, _ = io.Copy(io.Discard, far); _ = far.Close() }()
	}
	return up, nil
}

var (
	testHash   = pknode.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	testIP     = netip.MustParseAddr("203.0.113.7")
	testPlatID = "11111111-1111-1111-1111-111111111111"
	testSnap   = pknode.NodeSnapshot{
		Hash:        testHash,
		EgressIP:    testIP,
		HasOutbound: true,
		Health:      pknode.HealthyHealth(0),
	}
)

type fixture struct {
	p      *Pipeline
	bus    *fakeBus
	dialer *fakeNodeDialer
	direct *fakeDirect
	leases *lease.LeaseTable
	view   *view.View
	clock  *fakeClock
}

func newFixture(t *testing.T, mutate func(*Deps)) *fixture {
	t.Helper()
	v := view.Derive([]pknode.NodeSnapshot{testSnap}, nil)
	bus := &fakeBus{}
	dialer := &fakeNodeDialer{}
	direct := &fakeDirect{}
	leases := lease.NewLeaseTable()
	clock := &fakeClock{t: time.Now()}
	d := Deps{
		Resolver: &fakeResolver{
			platforms: map[string]string{"Default": testPlatID},
			policies:  map[string]*identity.Policy{},
		},
		NodeDialer: dialer,
		Direct:     direct,
		Bus:        bus,
		Clock:      clock,
		Leases:     leases,
		Views:      func(string) *view.View { return v },
		Nodes: func(h pknode.Hash) (pknode.NodeSnapshot, bool) {
			if h == testHash {
				return testSnap, true
			}
			return pknode.NodeSnapshot{}, false
		},
		Latency:    func(pknode.Hash, string) (time.Duration, bool) { return 0, false },
		IPLoad:     func(string, netip.Addr) int64 { return 0 },
		PickPolicy: pick.Policy{Allocation: pick.AllocationBalanced},
		Rand:       rand.New(rand.NewPCG(42, 99)),
		ProxyToken: "tok",
	}
	if mutate != nil {
		mutate(&d)
	}
	return &fixture{
		p: New(d), bus: bus, dialer: dialer, direct: direct,
		leases: leases, view: v, clock: clock,
	}
}

func baseReq() Request {
	return Request{
		Token:       "tok",
		RawIdentity: "Default.alice",
		Target:      "example.com:80",
		Headers:     map[string]string{},
		Proto:       ProtoHTTPForward,
		RemoteAddr:  "10.0.0.1:1234",
	}
}

// echoUpstream reads the head, then echoes everything back.
func echoUpstream(t *testing.T, up net.Conn, headLen int) {
	t.Helper()
	defer up.Close()
	if headLen > 0 {
		if _, err := io.ReadFull(up, make([]byte, headLen)); err != nil {
			return
		}
	}
	_, _ = io.Copy(up, up)
}

// runHandle runs Handle in the background against a piped client; the
// caller drives the client end and must close it to let relay finish.
func runHandle(ctx context.Context, p *Pipeline, client net.Conn, req Request) <-chan *ProxyError {
	done := make(chan *ProxyError, 1)
	go func() { done <- p.Handle(ctx, client, req) }()
	return done
}

func TestHandleFullPath(t *testing.T) {
	fx := newFixture(t, func(d *Deps) {
		d.NodeDialer.(*fakeNodeDialer).serve = func(up net.Conn) { echoUpstream(t, up, len("HELLO")) }
	})
	client, server := net.Pipe()
	defer server.Close()

	req := baseReq()
	req.Head = []byte("HELLO")
	done := runHandle(context.Background(), fx.p, client, req)

	// Drive the downstream client end: ping → echo comes back.
	go func() {
		_, _ = server.Write([]byte("ping"))
	}()
	got := make([]byte, 4)
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("echo = %q, want %q", got, "ping")
	}
	_ = server.Close() // let relay finish
	_ = client.Close()

	perr := <-done
	if perr != nil {
		t.Fatalf("Handle = %v, want nil", perr)
	}

	// Lease created for (platform, alice), anchored at the node egress IP.
	l, ok := fx.leases.Get(testPlatID, "alice")
	if !ok {
		t.Fatal("no lease created")
	}
	if l.EgressIP != testIP || l.NodeHint != testHash {
		t.Fatalf("lease = %+v, want ip=%v hash=%v", l, testIP, testHash)
	}

	// Node dialed by hash.
	if hashes := fx.dialer.dialed(); len(hashes) != 1 || hashes[0] != testHash {
		t.Fatalf("dialed hashes = %v", hashes)
	}

	// Traffic signal: ok, byte counts, platform, node.
	sig, ok := fx.bus.last()
	if !ok {
		t.Fatal("no signal emitted")
	}
	if sig.Kind != pksignal.KindTraffic || !sig.OK {
		t.Fatalf("signal kind/ok = %v/%v", sig.Kind, sig.OK)
	}
	if sig.NodeHash != testHash || sig.PlatformID != testPlatID {
		t.Fatalf("signal node/platform = %v/%v", sig.NodeHash, sig.PlatformID)
	}
	if sig.BytesUp != 9 { // "HELLO" head + "ping"
		t.Fatalf("BytesUp = %d, want 9", sig.BytesUp)
	}
	if sig.BytesDown != 4 {
		t.Fatalf("BytesDown = %d, want 4", sig.BytesDown)
	}
	if sig.BreakerExempt {
		t.Fatal("BreakerExempt should be false by default")
	}
	if sig.HasTLS {
		t.Fatal("HasTLS should be false for plain http")
	}
}

func TestHandleStickyLeaseReuse(t *testing.T) {
	fx := newFixture(t, func(d *Deps) {
		d.NodeDialer.(*fakeNodeDialer).serve = func(up net.Conn) { _, _ = io.Copy(io.Discard, up); up.Close() }
	})
	for i := 0; i < 2; i++ {
		client, server := net.Pipe()
		done := runHandle(context.Background(), fx.p, client, baseReq())
		_ = server.Close()
		_ = client.Close()
		if perr := <-done; perr != nil {
			t.Fatalf("iter %d: %v", i, perr)
		}
	}
	if n := fx.leases.Len(); n != 1 {
		t.Fatalf("leases = %d, want 1", n)
	}
	l, _ := fx.leases.Get(testPlatID, "alice")
	if l.NodeHint != testHash {
		t.Fatalf("sticky hint = %v", l.NodeHint)
	}
}

func TestHandleRandomNoLease(t *testing.T) {
	fx := newFixture(t, nil)
	req := baseReq()
	req.RawIdentity = "Default" // no account → random, no lease
	client, server := net.Pipe()
	done := runHandle(context.Background(), fx.p, client, req)
	_ = server.Close()
	_ = client.Close()
	if perr := <-done; perr != nil {
		t.Fatalf("Handle = %v", perr)
	}
	if n := fx.leases.Len(); n != 0 {
		t.Fatalf("leases = %d, want 0", n)
	}
}

func TestResolveErrors(t *testing.T) {
	tests := []struct {
		name string
		req  Request
		code string
	}{
		{"token mismatch", func() Request { r := baseReq(); r.Token = "wrong"; return r }(), CodeAuthFailed},
		{"token missing http-forward", func() Request { r := baseReq(); r.Token = ""; return r }(), CodeAuthRequired},
		{"token missing reverse", func() Request {
			r := baseReq()
			r.Token = ""
			r.Proto = ProtoHTTPReverse
			return r
		}(), CodeAuthFailed},
		{"unknown platform", func() Request { r := baseReq(); r.RawIdentity = "Nope.alice"; return r }(), CodePlatformNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, nil)
			client, _ := net.Pipe()
			perr := fx.p.Handle(context.Background(), client, tc.req)
			_ = client.Close()
			if perr == nil || perr.Code != tc.code {
				t.Fatalf("code = %v, want %s", perr, tc.code)
			}
			if fx.bus.count() != 0 {
				t.Fatal("resolve failure must not emit a signal")
			}
		})
	}
}

func TestResolveAuthDisabled(t *testing.T) {
	fx := newFixture(t, func(d *Deps) { d.ProxyToken = "" })
	req := baseReq()
	req.Token, req.RawIdentity = "", ""
	client, server := net.Pipe()
	done := runHandle(context.Background(), fx.p, client, req)
	_ = server.Close()
	_ = client.Close()
	if perr := <-done; perr != nil {
		t.Fatalf("Handle = %v, want nil", perr)
	}
}

func TestResolveAccountRejected(t *testing.T) {
	pol := identity.Policy{OnMiss: identity.OnMissReject} // empty chain → reject
	fx := newFixture(t, func(d *Deps) {
		d.Resolver.(*fakeResolver).policies[testPlatID] = &pol
	})
	req := baseReq()
	req.RawIdentity = "Default" // no wire account, no headers → miss → reject
	client, _ := net.Pipe()
	perr := fx.p.Handle(context.Background(), client, req)
	_ = client.Close()
	if perr == nil || perr.Code != CodeAccountRejected {
		t.Fatalf("code = %v, want %s", perr, CodeAccountRejected)
	}
}

func TestResolveHeaderAccountWins(t *testing.T) {
	fx := newFixture(t, nil)
	req := baseReq()
	req.RawIdentity = "Default.wire"
	req.Headers = map[string]string{"x-resin-account": "hdr"}
	client, server := net.Pipe()
	done := runHandle(context.Background(), fx.p, client, req)
	_ = server.Close()
	_ = client.Close()
	if perr := <-done; perr != nil {
		t.Fatalf("Handle = %v", perr)
	}
	if _, ok := fx.leases.Get(testPlatID, "hdr"); !ok {
		t.Fatal("expected lease for header account 'hdr'")
	}
}

func TestRouteNoAvailableNodes(t *testing.T) {
	fx := newFixture(t, func(d *Deps) {
		d.Views = func(string) *view.View { return view.Derive(nil, nil) }
	})
	client, _ := net.Pipe()
	perr := fx.p.Handle(context.Background(), client, baseReq())
	_ = client.Close()
	if perr == nil || perr.Code != CodeNoAvailableNodes {
		t.Fatalf("code = %v, want %s", perr, CodeNoAvailableNodes)
	}
}

func TestRouteNodeGone(t *testing.T) {
	fx := newFixture(t, func(d *Deps) {
		d.Nodes = func(pknode.Hash) (pknode.NodeSnapshot, bool) { return pknode.NodeSnapshot{}, false }
	})
	client, _ := net.Pipe()
	perr := fx.p.Handle(context.Background(), client, baseReq())
	_ = client.Close()
	if perr == nil || perr.Code != CodeInternalError {
		t.Fatalf("code = %v, want %s", perr, CodeInternalError)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "dial timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestDialErrors(t *testing.T) {
	tests := []struct {
		name    string
		dialErr error
		code    string
		signals int // expected signal count
	}{
		{"refused", errors.New("connection refused"), CodeUpstreamConnectFailed, 1},
		{"timeout", timeoutErr{}, CodeUpstreamTimeout, 1},
		{"deadline", context.DeadlineExceeded, CodeUpstreamTimeout, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, func(d *Deps) {
				d.NodeDialer.(*fakeNodeDialer).dialErr = tc.dialErr
			})
			client, _ := net.Pipe()
			perr := fx.p.Handle(context.Background(), client, baseReq())
			_ = client.Close()
			if perr == nil || perr.Code != tc.code {
				t.Fatalf("code = %v, want %s", perr, tc.code)
			}
			if n := fx.bus.count(); n != tc.signals {
				t.Fatalf("signals = %d, want %d", n, tc.signals)
			}
			if sig, ok := fx.bus.last(); ok && sig.OK {
				t.Fatal("dial failure signal must have OK=false")
			}
		})
	}
}

func TestDialClientCanceled(t *testing.T) {
	fx := newFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fx.p.d.NodeDialer = &fakeNodeDialer{dialErr: context.Canceled}
	client, _ := net.Pipe()
	perr := fx.p.Handle(ctx, client, baseReq())
	_ = client.Close()
	if perr == nil || perr.Code != CodeClientCanceled {
		t.Fatalf("code = %v, want %s", perr, CodeClientCanceled)
	}
	if n := fx.bus.count(); n != 0 {
		t.Fatalf("client cancel must not emit a signal, got %d", n)
	}
}

func TestBypassDirect(t *testing.T) {
	fx := newFixture(t, func(d *Deps) {
		d.Bypass = func(target string) bool { return target == "example.com:80" }
		d.Direct.(*fakeDirect).serve = func(up net.Conn) { _, _ = io.Copy(io.Discard, up); up.Close() }
	})
	client, server := net.Pipe()
	done := runHandle(context.Background(), fx.p, client, baseReq())
	_ = server.Close()
	_ = client.Close()
	if perr := <-done; perr != nil {
		t.Fatalf("Handle = %v", perr)
	}
	if len(fx.direct.targets) != 1 || fx.direct.targets[0] != "example.com:80" {
		t.Fatalf("direct targets = %v", fx.direct.targets)
	}
	if len(fx.dialer.dialed()) != 0 {
		t.Fatal("node dialer must not be used for bypass")
	}
	sig, _ := fx.bus.last()
	if sig.NodeHash != (pknode.Hash{}) {
		t.Fatalf("bypass signal node hash = %v, want zero", sig.NodeHash)
	}
	if !sig.OK {
		t.Fatal("bypass signal must be OK")
	}
}

func TestBreakerExempt(t *testing.T) {
	fx := newFixture(t, func(d *Deps) {
		d.PassiveBreakerOff = func(string) bool { return true }
	})
	client, server := net.Pipe()
	done := runHandle(context.Background(), fx.p, client, baseReq())
	_ = server.Close()
	_ = client.Close()
	if perr := <-done; perr != nil {
		t.Fatalf("Handle = %v", perr)
	}
	sig, _ := fx.bus.last()
	if !sig.BreakerExempt {
		t.Fatal("BreakerExempt should be true when passive breaker is off")
	}
}

func TestHTTPStatusMapping(t *testing.T) {
	cases := map[string]int{
		CodeAuthRequired:          407,
		CodeAuthFailed:            403,
		CodeURLParseError:         400,
		CodeInvalidProtocol:       400,
		CodeInvalidHost:           400,
		CodePlatformNotFound:      404,
		CodeAccountRejected:       403,
		CodeNoAvailableNodes:      503,
		CodeUpstreamConnectFailed: 502,
		CodeUpstreamRequestFailed: 502,
		CodeUpstreamTimeout:       504,
		CodeInternalError:         500,
		"something-else":          500,
	}
	for code, want := range cases {
		if got := HTTPStatus(code); got != want {
			t.Errorf("HTTPStatus(%s) = %d, want %d", code, got, want)
		}
	}
}

func TestMisconfiguredTokenFailsClosed(t *testing.T) {
	fx := newFixture(t, func(d *Deps) { d.ProxyToken = "has space" })
	client, _ := net.Pipe()
	req := baseReq()
	req.Token = "has space"
	perr := fx.p.Handle(context.Background(), client, req)
	_ = client.Close()
	if perr == nil || perr.Code != CodeInternalError {
		t.Fatalf("code = %v, want %s", perr, CodeInternalError)
	}
}

func TestProxyErrorString(t *testing.T) {
	e := &ProxyError{Code: CodeAuthFailed, Msg: "nope"}
	if e.Error() == "" {
		t.Fatal("empty Error()")
	}
}
