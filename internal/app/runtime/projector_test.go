package runtime

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/core/node"
	"github.com/xiarsx/egress/internal/ports"
	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// fakeClock is an injectable ports.Clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeRegistry is an in-memory ports.NodeRegistry for projector tests.
type fakeRegistry struct {
	mu    sync.Mutex
	nodes map[pknode.Hash]pknode.NodeSnapshot
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{nodes: make(map[pknode.Hash]pknode.NodeSnapshot)}
}

var _ ports.NodeRegistry = (*fakeRegistry)(nil)

func (f *fakeRegistry) ApplySnapshot(source string, nodes []pknode.NodeSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range nodes {
		f.nodes[n.Hash] = n
	}
}
func (f *fakeRegistry) SnapshotOf(string) []pknode.NodeSnapshot { return nil }
func (f *fakeRegistry) UpdateHealth(h pknode.Hash, health pknode.Health) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n, ok := f.nodes[h]; ok {
		n.Health = health
		f.nodes[h] = n
	}
}

func (f *fakeRegistry) UpdateEgressIP(h pknode.Hash, ip netip.Addr) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n, ok := f.nodes[h]; ok {
		n.EgressIP = ip
		f.nodes[h] = n
	}
}

func (f *fakeRegistry) UpdateLatency(h pknode.Hash, latency time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n, ok := f.nodes[h]; ok {
		n.Latency, n.HasLatency = latency, true
		f.nodes[h] = n
	}
}

func (f *fakeRegistry) UpdateAttrs(pknode.Hash, map[pknode.AttrKey]pknode.AttrValue) {}
func (f *fakeRegistry) SetOnNodeChange(func(h pknode.Hash, removed bool))            {}
func (f *fakeRegistry) Get(h pknode.Hash) (pknode.NodeSnapshot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.nodes[h]
	return n, ok
}
func (f *fakeRegistry) All() []pknode.NodeSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]pknode.NodeSnapshot, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, n)
	}
	return out
}

// latencySample records one UpsertLatency call.
type latencySample struct {
	h         pknode.Hash
	domain    string
	ms        float64
	at        time.Time
	authority bool
}

// fakeCache is an in-memory ports.CacheStore for projector tests.
type fakeCache struct {
	mu        sync.Mutex
	nodes     map[pknode.Hash]pknode.NodeSnapshot
	latencies []latencySample
	flushes   int
}

func newFakeCache() *fakeCache { return &fakeCache{nodes: make(map[pknode.Hash]pknode.NodeSnapshot)} }

var _ ports.CacheStore = (*fakeCache)(nil)

func (f *fakeCache) UpsertNode(n pknode.NodeSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[n.Hash] = n
}
func (f *fakeCache) DeleteNode(h pknode.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, h)
}
func (f *fakeCache) SetSourceNodes(string, []pknode.Hash) {}
func (f *fakeCache) UpsertLatency(h pknode.Hash, domain string, ms float64, at time.Time, authority bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencies = append(f.latencies, latencySample{h, domain, ms, at, authority})
}
func (f *fakeCache) UpsertAttrs(pknode.Hash, map[pknode.AttrKey]pknode.AttrValue) {}
func (f *fakeCache) UpsertLease(string, string, pklease.Lease)                    {}
func (f *fakeCache) DeleteLease(string, string)                                   {}
func (f *fakeCache) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushes++
	return nil
}

var testHash = pknode.Hash{1, 2, 3}

func testNode() pknode.NodeSnapshot {
	return pknode.NodeSnapshot{
		Hash:        testHash,
		HasOutbound: true,
		Health:      node.ProbingHealth(),
	}
}

func newTestProjector(reg *fakeRegistry, cache *fakeCache, clk *fakeClock) *HealthProjector {
	return &HealthProjector{
		Reg:   reg,
		Cache: cache,
		Cfg:   node.BreakerConfig{MaxConsecutiveFailures: 3},
		Clock: clk,
	}
}

func healthOf(t *testing.T, reg *fakeRegistry) pknode.Health {
	t.Helper()
	n, ok := reg.Get(testHash)
	if !ok {
		t.Fatal("node missing from registry")
	}
	return n.Health
}

// TestProjector_OKMovesProbingToHealthy: Step application on the success path.
func TestProjector_OKMovesProbingToHealthy(t *testing.T) {
	reg, cache, clk := newFakeRegistry(), newFakeCache(), &fakeClock{t: time.Now()}
	reg.ApplySnapshot("s", []pknode.NodeSnapshot{testNode()})
	p := newTestProjector(reg, cache, clk)

	p.Project(pksignal.Signal{
		Kind:     pksignal.KindEgress,
		NodeHash: [16]byte(testHash),
		At:       clk.Now(),
		EgressIP: netip.MustParseAddr("1.2.3.4"),
	})

	if got := healthOf(t, reg); got.Kind != pknode.HealthHealthy || got.Failures != 0 {
		t.Fatalf("health = %+v, want Healthy(0)", got)
	}
	// The coalescing cache must have received the updated snapshot.
	cache.mu.Lock()
	cached, ok := cache.nodes[testHash]
	cache.mu.Unlock()
	if !ok {
		t.Fatal("cache did not receive UpsertNode")
	}
	if cached.Health.Kind != pknode.HealthHealthy {
		t.Fatalf("cached health = %+v, want Healthy", cached.Health)
	}
	if cached.EgressIP != netip.MustParseAddr("1.2.3.4") {
		t.Fatalf("cached egress = %v, want 1.2.3.4", cached.EgressIP)
	}
}

// TestProjector_FailuresTripBreaker: three consecutive failures trip the breaker.
func TestProjector_FailuresTripBreaker(t *testing.T) {
	reg, cache, clk := newFakeRegistry(), newFakeCache(), &fakeClock{t: time.Now()}
	n := testNode()
	n.Health = node.HealthyHealth(0)
	reg.ApplySnapshot("s", []pknode.NodeSnapshot{n})
	p := newTestProjector(reg, cache, clk)

	fail := pksignal.Signal{Kind: pksignal.KindTraffic, NodeHash: [16]byte(testHash), At: clk.Now(), OK: false}
	p.Project(fail)
	if got := healthOf(t, reg); got.Kind != pknode.HealthHealthy || got.Failures != 1 {
		t.Fatalf("after 1 fail: health = %+v, want Healthy(1)", got)
	}
	p.Project(fail)
	if got := healthOf(t, reg); got.Kind != pknode.HealthHealthy || got.Failures != 2 {
		t.Fatalf("after 2 fails: health = %+v, want Healthy(2)", got)
	}
	p.Project(fail)
	got := healthOf(t, reg)
	if got.Kind != pknode.HealthTripped {
		t.Fatalf("after 3 fails: health = %+v, want Tripped", got)
	}
	if !got.Since.Equal(clk.Now()) {
		t.Fatalf("Tripped.Since = %v, want projector clock now %v", got.Since, clk.Now())
	}
}

// TestProjector_ExemptFailureLeavesHealth: BreakerExempt failures are ignored.
func TestProjector_ExemptFailureLeavesHealth(t *testing.T) {
	reg, cache, clk := newFakeRegistry(), newFakeCache(), &fakeClock{t: time.Now()}
	n := testNode()
	n.Health = node.HealthyHealth(2)
	reg.ApplySnapshot("s", []pknode.NodeSnapshot{n})
	p := newTestProjector(reg, cache, clk)

	p.Project(pksignal.Signal{
		Kind:          pksignal.KindTraffic,
		NodeHash:      [16]byte(testHash),
		At:            clk.Now(),
		OK:            false,
		BreakerExempt: true,
	})
	if got := healthOf(t, reg); got.Kind != pknode.HealthHealthy || got.Failures != 2 {
		t.Fatalf("exempt fail moved health: %+v, want Healthy(2)", got)
	}
}

// TestProjector_NoOutboundIsIgnored: observations on nodes without an
// outbound instance must not move health (design §2.4 outbound-empty row).
func TestProjector_NoOutboundIsIgnored(t *testing.T) {
	reg, cache, clk := newFakeRegistry(), newFakeCache(), &fakeClock{t: time.Now()}
	n := testNode()
	n.HasOutbound = false
	n.Health = node.HealthyHealth(2)
	reg.ApplySnapshot("s", []pknode.NodeSnapshot{n})
	p := newTestProjector(reg, cache, clk)

	p.Project(pksignal.Signal{Kind: pksignal.KindTraffic, NodeHash: [16]byte(testHash), At: clk.Now(), OK: false})
	if got := healthOf(t, reg); got.Kind != pknode.HealthHealthy || got.Failures != 2 {
		t.Fatalf("fail without outbound moved health: %+v", got)
	}
	p.Project(pksignal.Signal{Kind: pksignal.KindEgress, NodeHash: [16]byte(testHash), At: clk.Now(), EgressIP: netip.MustParseAddr("9.9.9.9")})
	if got := healthOf(t, reg); got.Kind != pknode.HealthHealthy || got.Failures != 2 {
		t.Fatalf("ok without outbound moved health: %+v", got)
	}
}

// TestProjector_LatencySignalFeedsCache: latency samples go to UpsertLatency.
func TestProjector_LatencySignalFeedsCache(t *testing.T) {
	reg, cache, clk := newFakeRegistry(), newFakeCache(), &fakeClock{t: time.Now()}
	reg.ApplySnapshot("s", []pknode.NodeSnapshot{testNode()})
	p := newTestProjector(reg, cache, clk)

	p.Project(pksignal.Signal{
		Kind:       pksignal.KindLatency,
		NodeHash:   [16]byte(testHash),
		At:         clk.Now(),
		Domain:     "cloudflare.com",
		Latency:    120 * time.Millisecond,
		HasLatency: true,
		Authority:  true,
	})
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.latencies) != 1 {
		t.Fatalf("latency samples = %d, want 1", len(cache.latencies))
	}
	s := cache.latencies[0]
	if s.domain != "cloudflare.com" || s.ms != 120 || !s.authority {
		t.Fatalf("sample = %+v, want domain=cloudflare.com ms=120 authority=true", s)
	}
	if !s.at.Equal(clk.Now()) {
		t.Fatalf("sample at = %v, want %v", s.at, clk.Now())
	}
}

// TestProjector_UnknownNodeIsIgnored: signals for hashes not in the
// registry are dropped silently.
func TestProjector_UnknownNodeIsIgnored(t *testing.T) {
	reg, cache, clk := newFakeRegistry(), newFakeCache(), &fakeClock{t: time.Now()}
	p := newTestProjector(reg, cache, clk)

	p.Project(pksignal.Signal{Kind: pksignal.KindEgress, NodeHash: [16]byte(pknode.Hash{9, 9}), At: clk.Now()})

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.nodes) != 0 || len(cache.latencies) != 0 {
		t.Fatal("unknown node produced cache writes")
	}
}

// TestProjector_RunConsumesBus: end-to-end through a real Bus subscription.
func TestProjector_RunConsumesBus(t *testing.T) {
	reg, cache, clk := newFakeRegistry(), newFakeCache(), &fakeClock{t: time.Now()}
	reg.ApplySnapshot("s", []pknode.NodeSnapshot{testNode()})
	p := newTestProjector(reg, cache, clk)

	bus := NewBus(16)
	sub := bus.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx, sub) }()

	bus.Emit(pksignal.Signal{Kind: pksignal.KindEgress, NodeHash: [16]byte(testHash), At: clk.Now(), EgressIP: netip.MustParseAddr("5.6.7.8")})
	waitFor(t, 2*time.Second, func() bool {
		n, _ := reg.Get(testHash)
		return n.Health.Kind == pknode.HealthHealthy
	}, "projector to consume signal")
	if d := bus.Dropped(); d != 0 {
		t.Fatalf("dropped = %d, want 0", d)
	}
	cancel()
	<-done
}
