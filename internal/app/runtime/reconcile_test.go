package runtime

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/adapters/source"
	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/config"
	"github.com/xiarsx/egress/internal/ports"
	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"

	"github.com/xiarsx/egress/internal/testutil/memstore"
)

// fakeCacheLoader is an empty cache.db candidate source.
type fakeCacheLoader struct{}

func (fakeCacheLoader) LoadSnapshot() ([]pknode.NodeSnapshot, map[string][]pknode.Hash, []ports.LeaseRecord, error) {
	return nil, nil, nil, nil
}

// fakeCacheStore discards all cache writes.
type fakeCacheStore struct{}

func (fakeCacheStore) UpsertNode(pknode.NodeSnapshot)                               {}
func (fakeCacheStore) DeleteNode(pknode.Hash)                                       {}
func (fakeCacheStore) SetSourceNodes(string, []pknode.Hash)                         {}
func (fakeCacheStore) UpsertLatency(pknode.Hash, string, float64, time.Time, bool)  {}
func (fakeCacheStore) UpsertAttrs(pknode.Hash, map[pknode.AttrKey]pknode.AttrValue) {}
func (fakeCacheStore) UpsertLease(string, string, pklease.Lease)                    {}
func (fakeCacheStore) DeleteLease(string, string)                                   {}
func (fakeCacheStore) Flush() error                                                 { return nil }

// TestReconcileDerivesViewByUUID is the regression test for the
// name-vs-UUID key mismatch: the store keys platforms by name, but the
// hot path looks views up by UUID, so reconcile must translate.
func TestReconcileDerivesViewByUUID(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	if err := config.EnsureDefaultPlatform(ctx, store); err != nil {
		t.Fatal(err)
	}
	svc := control.NewService(store, control.NopHooks{})

	rt := New(Deps{
		Store:      store,
		Cache:      fakeCacheLoader{},
		CacheStore: fakeCacheStore{},
		Registry:   source.NewRegistry(),
		Platforms:  svc,
		Sources:    svc,
	})
	if err := rt.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// The Default platform's view must exist under its UUID, the key the
	// pipeline resolves to — not under the store name "Default".
	if v := rt.Deriver().View(control.DefaultPlatformID); v == nil {
		t.Fatal("no view derived under the Default platform UUID")
	}
	if v := rt.Deriver().View(control.DefaultPlatformName); v != nil {
		t.Fatal("view must not be keyed by platform name")
	}
	ids := rt.Deriver().PlatformIDs()
	if len(ids) != 1 || ids[0] != control.DefaultPlatformID {
		t.Fatalf("PlatformIDs = %v", ids)
	}
}

// TestRuntimeNodeLifecycleReachesView is the wiring regression for the
// registry→deriver link: a node published by a source must enter the
// platform View once the projector has folded its egress+latency signals
// (healthy + egress IP + HasLatency = routable, design §5.1). Before the
// hook existed, views froze at their initial full-scan and no node ever
// became routable.
func TestRuntimeNodeLifecycleReachesView(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	if err := config.EnsureDefaultPlatform(ctx, store); err != nil {
		t.Fatal(err)
	}
	svc := control.NewService(store, control.NopHooks{})

	rt := New(Deps{
		Store:      store,
		Cache:      fakeCacheLoader{},
		CacheStore: fakeCacheStore{},
		Registry:   source.NewRegistry(),
		Platforms:  svc,
		Sources:    svc,
	})
	if err := rt.Run(ctx); err != nil {
		t.Fatal(err)
	}

	h := pknode.Hash{0x42}
	rt.Registry().ApplySnapshot("src", []pknode.NodeSnapshot{{
		Hash: h, HasOutbound: true,
	}})

	// Probing + no egress IP + no latency: not routable yet.
	if v := rt.Deriver().View(control.DefaultPlatformID); v.Contains(h) {
		t.Fatal("probing node must not be routable")
	}

	// Probes observe the node: egress IP then latency, via the bus like
	// production (the projector folds them into the registry, the
	// registry's change hook re-derives the view).
	rt.Bus().Emit(pksignal.Signal{Kind: pksignal.KindEgress, NodeHash: h, EgressIP: netip.MustParseAddr("192.0.2.42")})
	rt.Bus().Emit(pksignal.Signal{Kind: pksignal.KindLatency, NodeHash: h, HasLatency: true, Latency: 50 * time.Millisecond})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v := rt.Deriver().View(control.DefaultPlatformID); v.Contains(h) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	v := rt.Deriver().View(control.DefaultPlatformID)
	if !v.Contains(h) {
		snap, _ := rt.Registry().Get(h)
		t.Fatalf("node never became routable; registry image: %+v", snap)
	}

	// The view's by-IP index anchors sticky leases on the learned IP.
	if sibs := v.ByEgressIP(netip.MustParseAddr("192.0.2.42")); len(sibs) != 1 {
		t.Fatalf("ByEgressIP = %v, want the learned egress IP indexed", sibs)
	}

	// Source removal drops the node from the view.
	rt.Registry().ApplySnapshot("src", nil)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !v.Contains(h) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if v.Contains(h) {
		t.Fatal("removed node must leave the view")
	}
}
