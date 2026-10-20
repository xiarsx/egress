package boot

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/adapters/geoip"
	"github.com/xiarsx/egress/internal/adapters/source"
	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/pkg/ext"
	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// runOnceScheduler executes exactly the first scheduled task, then stops
// (tick re-schedules itself; running every task would loop forever).
type runOnceScheduler struct {
	done chan struct{}
	once sync.Once
}

func (s *runOnceScheduler) Schedule(_ time.Duration, task ports.Task) { s.run(task) }
func (s *runOnceScheduler) ScheduleAt(_ time.Time, task ports.Task)   { s.run(task) }

func (s *runOnceScheduler) run(task ports.Task) {
	s.once.Do(func() {
		go func() {
			task(context.Background())
			close(s.done)
		}()
	})
}

type fakeBus struct {
	mu      sync.Mutex
	signals []pksignal.Signal
}

func (b *fakeBus) Emit(sig pksignal.Signal) {
	b.mu.Lock()
	b.signals = append(b.signals, sig)
	b.mu.Unlock()
}
func (b *fakeBus) Subscribe() <-chan pksignal.Signal { return nil }
func (b *fakeBus) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.signals)
}

type fakeCacheStore struct {
	mu    sync.Mutex
	attrs map[pknode.Hash]map[pknode.AttrKey]pknode.AttrValue
}

func (s *fakeCacheStore) UpsertNode(pknode.NodeSnapshot)       {}
func (s *fakeCacheStore) DeleteNode(pknode.Hash)               {}
func (s *fakeCacheStore) SetSourceNodes(string, []pknode.Hash) {}
func (s *fakeCacheStore) UpsertLatency(pknode.Hash, string, float64, time.Time, bool) {
}
func (s *fakeCacheStore) UpsertLease(string, string, pklease.Lease) {}
func (s *fakeCacheStore) DeleteLease(string, string)                {}
func (s *fakeCacheStore) Flush() error                              { return nil }

func (s *fakeCacheStore) UpsertAttrs(h pknode.Hash, attrs map[pknode.AttrKey]pknode.AttrValue) {
	s.mu.Lock()
	if s.attrs == nil {
		s.attrs = map[pknode.Hash]map[pknode.AttrKey]pknode.AttrValue{}
	}
	if s.attrs[h] == nil {
		s.attrs[h] = map[pknode.AttrKey]pknode.AttrValue{}
	}
	for k, v := range attrs {
		s.attrs[h][k] = v
	}
	s.mu.Unlock()
}

func testNode() pknode.NodeSnapshot {
	return pknode.NodeSnapshot{
		Hash:        pknode.Hash{9, 9},
		HasOutbound: true,
		EgressIP:    netip.MustParseAddr("192.0.2.9"),
		Health:      pknode.HealthyHealth(0),
	}
}

// --- ext.Prober wiring (spec/ext-abi.md §2, RFC-0002) ---

type stubProber struct {
	probed chan pknode.Hash
}

func (p *stubProber) Name() string { return "test-stub-prober" }
func (p *stubProber) Probe(ctx context.Context, n pknode.NodeSnapshot) []pksignal.Signal {
	p.probed <- n.Hash
	return []pksignal.Signal{{Kind: pksignal.KindEgress, NodeHash: n.Hash, EgressIP: n.EgressIP}}
}

func TestScheduleExtProbers(t *testing.T) {
	prober := &stubProber{probed: make(chan pknode.Hash, 1)}
	ext.RegisterProber(prober)

	reg := source.NewRegistry()
	reg.ApplySnapshot("src", []pknode.NodeSnapshot{testNode()})
	bus := &fakeBus{}
	sched := &runOnceScheduler{done: make(chan struct{})}

	scheduleExtProbers(sched, reg, bus)

	select {
	case h := <-prober.probed:
		if h != (pknode.Hash{9, 9}) {
			t.Fatalf("probed node = %v", h)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prober never ran")
	}
	<-sched.done
	if bus.count() == 0 {
		t.Fatal("probe signals were not emitted onto the bus")
	}
}

// --- ext.AttributeProvider wiring (spec/ext-abi.md §2, RFC-0002) ---

type stubAttrProvider struct{}

func (stubAttrProvider) Name() string { return "test-stub-attr" }
func (stubAttrProvider) Enrich(_ context.Context, ip netip.Addr, h pknode.Hash) map[pknode.AttrKey]pknode.AttrValue {
	return map[pknode.AttrKey]pknode.AttrValue{
		"ext.test-stub-attr.kind": pknode.StringAttr("edge"),
	}
}

func TestScheduleAttrRefresh(t *testing.T) {
	ext.RegisterAttributeProvider(stubAttrProvider{})

	reg := source.NewRegistry()
	reg.ApplySnapshot("src", []pknode.NodeSnapshot{testNode()})
	store := &fakeCacheStore{}
	geo, err := geoip.Open("") // empty provider: no-op
	if err != nil {
		t.Fatal(err)
	}
	sched := &runOnceScheduler{done: make(chan struct{})}

	scheduleAttrRefresh(sched, reg, geo, store)

	select {
	case <-sched.done:
	case <-time.After(5 * time.Second):
		t.Fatal("attr refresh tick never ran")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	got := store.attrs[pknode.Hash{9, 9}]["ext.test-stub-attr.kind"]
	if got.Str != "edge" {
		t.Fatalf("attrs = %v", store.attrs)
	}
}
