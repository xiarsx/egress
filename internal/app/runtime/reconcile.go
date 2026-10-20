package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/xiarsx/egress/internal/core/lease"
	"github.com/xiarsx/egress/internal/core/node"
	"github.com/xiarsx/egress/internal/core/predicate"
	"github.com/xiarsx/egress/internal/ports"
	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// Periodic-task intervals (design §8; defaults follow refmap §2.5).
const (
	// CacheFlushInterval bounds how long coalesced cache writes wait before
	// being flushed to cache.db. (The P1 CacheStore also flushes on its
	// dirty-count threshold; the timer is the backstop.)
	CacheFlushInterval = 5 * time.Minute
	// EphemeralEvictInterval is how often tripped ephemeral nodes are swept.
	EphemeralEvictInterval = 13 * time.Second
	// SourceListLimit caps the resource listing used at startup.
	SourceListLimit = 10000
)

// Syncer is the P2 sync orchestration surface that runtime needs: source
// lifecycle only. The concrete Syncer lives in P2's package; the assembly
// root (cmd/egress) injects it here so runtime stays free of P2 imports.
type Syncer interface {
	// AddSource registers (or re-registers) a source declaration.
	AddSource(ports.SourceSpec)
	// RemoveSource stops and forgets a source.
	RemoveSource(name string)
	// RefreshNow triggers an immediate refresh of a source.
	RefreshNow(name string)
}

// Deps wires the runtime. All fields are interfaces or P4-owned concrete
// types; nothing here imports P1/P2/P5 implementation packages.
type Deps struct {
	Store      ports.Store
	Cache      ports.CacheLoader
	CacheStore ports.CacheStore
	Registry   ports.NodeRegistry
	Sched      *Scheduler
	Bus        *Bus
	Deriver    *Deriver
	Leases     *lease.LeaseTable
	Clock      ports.Clock
	Platforms  ports.PlatformSource
	Sources    ports.SourceSource
	Breaker    node.BreakerConfig
	// Syncer is injected by the assembly root (P2's Syncer).
	Syncer Syncer
	// CacheFlushInterval overrides CacheFlushInterval when > 0 (ops/test
	// knob: crash-consistency window vs. write amplification).
	CacheFlushInterval time.Duration
}

// Runtime is the assembled P4 runtime: reconcile-on-start plus the
// RuntimeHooks the control plane calls on declarative resource changes.
// It implements ports.RuntimeHooks; the assembly root registers it with
// the control plane.
type Runtime struct {
	deps      Deps
	projector *HealthProjector
}

// Compile-time check: Runtime is the RuntimeHooks implementation.
var _ ports.RuntimeHooks = (*Runtime)(nil)

// New assembles a Runtime, filling in P4-owned defaults for any nil piece
// (Scheduler, Bus, Deriver, Leases, Clock) so the assembly root only has to
// provide the cross-group contracts.
func New(d Deps) *Runtime {
	if d.Clock == nil {
		d.Clock = SystemClock{}
	}
	if d.Sched == nil {
		d.Sched = NewScheduler()
	}
	if d.Bus == nil {
		d.Bus = NewBus(DefaultBusBuffer)
	}
	if d.Deriver == nil && d.Registry != nil {
		d.Deriver = NewDeriver(d.Registry)
	}
	if d.Leases == nil {
		d.Leases = lease.NewLeaseTable()
	}
	r := &Runtime{deps: d}
	r.projector = &HealthProjector{
		Reg:   d.Registry,
		Cache: d.CacheStore,
		Cfg:   d.Breaker,
		Clock: d.Clock,
	}
	// The registry → deriver link: every node mutation (source publish,
	// health step, learned egress IP/attrs) re-applies that node to every
	// platform View; removals drop it. Without this hook the views freeze
	// at their initial full-scan and no node ever becomes routable.
	d.Registry.SetOnNodeChange(func(h pknode.Hash, removed bool) {
		if removed {
			d.Deriver.Remove(h)
			return
		}
		if snap, ok := d.Registry.Get(h); ok {
			d.Deriver.Apply(snap)
		}
	})
	return r
}

// Accessors for the assembly root (wiring, observability).
func (r *Runtime) Bus() ports.SignalBus         { return r.deps.Bus }
func (r *Runtime) Sched() ports.Scheduler       { return r.deps.Sched }
func (r *Runtime) Deriver() *Deriver            { return r.deps.Deriver }
func (r *Runtime) Leases() *lease.LeaseTable    { return r.deps.Leases }
func (r *Runtime) Registry() ports.NodeRegistry { return r.deps.Registry }

// Run performs the startup reconcile (design §9.2), steps 1–5:
//
//  1. Load declarative resources (platform list) from state.db.
//  2. Load cache.db candidates (nodes, source_nodes, leases); a missing or
//     corrupt cache is treated as empty.
//  3. Replay each source's snapshot through source_nodes into the registry;
//     orphan nodes (unreferenced) never enter memory and go to the cache
//     Delete set.
//  4. Derive every platform's View; load leases (IPLoad recomputed by Put)
//     and schedule their expiries; drop leases pointing at unknown platforms.
//  5. Start the Scheduler and the SignalBus consumers (health projector;
//     periodic cache flush and ephemeral eviction), and refresh all sources
//     once immediately.
//
// Step 6 (opening listeners) belongs to cmd/egress, not here.
func (r *Runtime) Run(ctx context.Context) error {
	d := r.deps

	// --- 1. declarative resources from state.db ---
	platPage, err := d.Store.List(ctx, ports.KindPlatform, ports.ListOptions{Limit: SourceListLimit})
	if err != nil {
		return fmt.Errorf("runtime: list platforms: %w", err)
	}
	srcPage, err := d.Store.List(ctx, ports.KindSource, ports.ListOptions{Limit: SourceListLimit})
	if err != nil {
		return fmt.Errorf("runtime: list sources: %w", err)
	}
	// Views and leases are keyed by platform UUID (the hot path resolves
	// wire names to UUIDs before lookup), so reconcile must translate the
	// store's name keys up front.
	platformIDs := make([]string, 0, len(platPage.Items))
	platformSet := make(map[string]bool, len(platPage.Items))
	for _, res := range platPage.Items {
		spec, err := d.Platforms.GetPlatformSpec(ctx, res.Name)
		if err != nil {
			return fmt.Errorf("runtime: resolve platform %s: %w", res.Name, err)
		}
		platformIDs = append(platformIDs, spec.ID)
		platformSet[spec.ID] = true
	}

	// --- 2. cache.db candidates ---
	nodes, sourceNodes, leases, err := d.Cache.LoadSnapshot()
	if err != nil {
		// Weak source: a missing/corrupt cache is treated as empty
		// (design §9.2). The next source refresh rebuilds it.
		nodes, sourceNodes, leases = nil, nil, nil
	}

	// --- 3. replay snapshots into the registry ---
	byHash := make(map[pknode.Hash]pknode.NodeSnapshot, len(nodes))
	for _, n := range nodes {
		byHash[n.Hash] = n
	}
	referenced := make(map[pknode.Hash]bool, len(nodes))
	for source, hashes := range sourceNodes {
		snaps := make([]pknode.NodeSnapshot, 0, len(hashes))
		for _, h := range hashes {
			if n, ok := byHash[h]; ok {
				snaps = append(snaps, n)
				referenced[h] = true
			}
		}
		d.Registry.ApplySnapshot(source, snaps)
		d.CacheStore.SetSourceNodes(source, hashes)
	}
	// Orphans (cached nodes no source references) never enter memory;
	// they join the coalescing Delete set.
	for _, n := range nodes {
		if !referenced[n.Hash] {
			d.CacheStore.DeleteNode(n.Hash)
		}
	}

	// --- 4. derive views; load leases ---
	for _, id := range platformIDs {
		if err := r.derivePlatform(ctx, id); err != nil {
			// A bad platform filter must not abort startup; the view
			// falls back to the pass-all predicate. The control plane
			// validates filters on write (TODO: surface to observability).
			d.Deriver.SetPlatform(id, nil)
		}
	}
	for _, lr := range leases {
		if !platformSet[lr.Platform] {
			// Orphan lease: points at a nonexistent platform.
			d.CacheStore.DeleteLease(lr.Platform, lr.Account)
			continue
		}
		d.Leases.Put(lr.Platform, lr.Account, lr.Lease)
		r.scheduleLeaseExpiry(lr.Platform, lr.Account, lr.Lease)
	}

	// --- 5. scheduler + bus consumers ---
	sub := d.Bus.Subscribe()
	go r.projector.Run(ctx, sub)

	flushEvery := d.CacheFlushInterval
	if flushEvery <= 0 {
		flushEvery = CacheFlushInterval
	}
	r.scheduleRecurring(flushEvery, func(context.Context) {
		_ = d.CacheStore.Flush() // best-effort; the final flush happens at shutdown
	})
	r.scheduleRecurring(EphemeralEvictInterval, func(context.Context) {
		r.evictEphemeral()
	})

	// All sources refresh once immediately (design §9.2 step 5).
	if d.Syncer != nil {
		for _, res := range srcPage.Items {
			name := res.Name
			d.Sched.Schedule(0, func(context.Context) {
				d.Syncer.RefreshNow(name)
			})
		}
	}
	return nil
}

// derivePlatform compiles one platform's filter into the full Routable
// derivation and (re)builds its View.
func (r *Runtime) derivePlatform(ctx context.Context, id string) error {
	spec, err := r.deps.Platforms.GetPlatformSpec(ctx, id)
	if err != nil {
		return fmt.Errorf("get platform spec %s: %w", id, err)
	}
	tagAttrs, err := predicate.Compile(spec.Filter)
	if err != nil {
		return fmt.Errorf("compile platform filter %s: %w", id, err)
	}
	full := func(n pknode.NodeSnapshot) bool {
		return node.Routable(node.PlatformFilter{Tags: tagAttrs}, n)
	}
	r.deps.Deriver.SetPlatform(id, full)
	return nil
}

// scheduleRecurring schedules task after every interval, re-arming itself
// after each run. The chain stops when the scheduler stops.
func (r *Runtime) scheduleRecurring(interval time.Duration, task ports.Task) {
	var tick ports.Task
	tick = func(ctx context.Context) {
		task(ctx)
		r.deps.Sched.Schedule(interval, tick)
	}
	r.deps.Sched.Schedule(interval, tick)
}

// scheduleLeaseExpiry arms the scheduler to release a lease at its fixed
// expiry (design §7.1: expiry is never extended, never scanned inline).
func (r *Runtime) scheduleLeaseExpiry(platform, account string, l pklease.Lease) {
	if l.Expiry.IsZero() {
		return
	}
	r.deps.Sched.ScheduleAt(l.Expiry, func(context.Context) {
		if r.deps.Leases.Release(platform, account) {
			r.deps.CacheStore.DeleteLease(platform, account)
		}
	})
}

// TrackLease records a lease created at runtime (e.g. by the data-plane
// selector) and arms its expiry. Callers must call this instead of writing
// the LeaseTable directly so the expiry task is never missed.
func (r *Runtime) TrackLease(platform, account string, l pklease.Lease) {
	r.deps.Leases.Put(platform, account, l)
	r.deps.CacheStore.UpsertLease(platform, account, l)
	r.scheduleLeaseExpiry(platform, account, l)
}

// evictEphemeral drops tripped ephemeral-source nodes that have been down
// longer than the source's EvictDelay (design §8; refmap §2.5).
func (r *Runtime) evictEphemeral() {
	ctx := context.Background()
	now := r.deps.Clock.Now()
	page, err := r.deps.Store.List(ctx, ports.KindSource, ports.ListOptions{Limit: SourceListLimit})
	if err != nil {
		return
	}
	for _, res := range page.Items {
		spec, err := r.deps.Sources.GetSourceSpec(ctx, res.Name)
		if err != nil || spec.EvictDelay <= 0 {
			continue
		}
		snaps := r.deps.Registry.SnapshotOf(res.Name)
		kept := snaps[:0]
		dropped := false
		for _, n := range snaps {
			if n.Health.Kind == pknode.HealthTripped && now.Sub(n.Health.Since) >= spec.EvictDelay {
				dropped = true
				continue
			}
			kept = append(kept, n)
		}
		if dropped {
			r.deps.Registry.ApplySnapshot(res.Name, kept)
		}
	}
}

// --- ports.RuntimeHooks ------------------------------------------------

// OnPlatformChanged rebuilds the platform's View from the latest spec
// (creation and update share the same derivation path as startup).
func (r *Runtime) OnPlatformChanged(id string) {
	_ = r.derivePlatform(context.Background(), id)
}

// OnPlatformDeleted drops the platform's View and releases all of its
// leases (IPLoad counters move inside Release).
func (r *Runtime) OnPlatformDeleted(id string) {
	r.deps.Deriver.RemovePlatform(id)
	var doomed [][2]string
	// Range holds a read lock: collect first, mutate after (the table
	// forbids calling back into it from the iteration function).
	r.deps.Leases.Range(func(platform, account string, _ pklease.Lease) bool {
		if platform == id {
			doomed = append(doomed, [2]string{platform, account})
		}
		return true
	})
	for _, da := range doomed {
		r.deps.Leases.Release(da[0], da[1])
		r.deps.CacheStore.DeleteLease(da[0], da[1])
	}
}

// OnSourceChanged hands the new declaration to the P2 Syncer and triggers
// an immediate refresh.
func (r *Runtime) OnSourceChanged(name string) {
	if r.deps.Syncer == nil {
		return
	}
	spec, err := r.deps.Sources.GetSourceSpec(context.Background(), name)
	if err != nil {
		return
	}
	r.deps.Syncer.AddSource(spec)
	r.deps.Syncer.RefreshNow(name)
}

// OnSourceDeleted stops the source and drops its nodes from the registry.
func (r *Runtime) OnSourceDeleted(name string) {
	if r.deps.Syncer != nil {
		r.deps.Syncer.RemoveSource(name)
	}
	r.deps.Registry.ApplySnapshot(name, nil)
	r.deps.CacheStore.SetSourceNodes(name, nil)
}
