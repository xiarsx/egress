package wiring

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/identity"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/ports"
)

// resolverCacheTTL bounds how stale a platform name→ID or policy answer
// may be. Platform changes are rare and control-plane driven; a short TTL
// keeps the per-request resolve path (pipeline resolve step) off the
// store without needing an invalidation hook. TODO: subscribe to
// control-plane change notifications and drop the TTL.
const resolverCacheTTL = 60 * time.Second

// Compile-time checks: Resolver serves the pipeline's resolve step.
var (
	_ pipeline.PlatformResolver = (*Resolver)(nil)
)

// Resolver implements pipeline.PlatformResolver over the control plane:
// platform names resolve to UUIDs ("Default" always resolves, design P4)
// and each platform's compiled identity.Policy is served from a short-TTL
// cache.
type Resolver struct {
	Store   ports.Store
	Control *control.Service

	// Authorities are the latency authority domains used by P2C's
	// common-authority comparison (injected by the assembly root).
	Authorities []string

	ttl time.Duration

	mu       sync.RWMutex
	idCache  map[string]idEntry
	speCache map[string]specEntry
}

type idEntry struct {
	id  string
	exp time.Time
}

// specEntry caches everything the hot path needs from one platform spec:
// identity policy, selection policy and the passive-breaker flag, all
// loaded by a single store read per TTL window.
type specEntry struct {
	pol        *identity.Policy
	pick       pick.Policy
	breakerOff bool
	exp        time.Time
}

// NewResolver builds a Resolver over the control service.
func NewResolver(store ports.Store, svc *control.Service) *Resolver {
	return &Resolver{
		Store:    store,
		Control:  svc,
		ttl:      resolverCacheTTL,
		idCache:  make(map[string]idEntry),
		speCache: make(map[string]specEntry),
	}
}

// ResolvePlatform maps a wire-protocol platform name to its UUID.
// "Default" always resolves to the built-in platform ID.
func (r *Resolver) ResolvePlatform(name string) (string, bool) {
	if name == control.DefaultPlatformName {
		return control.DefaultPlatformID, true
	}
	now := time.Now()
	r.mu.RLock()
	e, ok := r.idCache[name]
	r.mu.RUnlock()
	if ok && now.Before(e.exp) {
		return e.id, true
	}
	p, err := r.Control.GetPlatform(context.Background(), name)
	if err != nil {
		return "", false
	}
	r.mu.Lock()
	r.idCache[name] = idEntry{id: p.ID, exp: now.Add(r.ttl)}
	r.mu.Unlock()
	return p.ID, true
}

// platformEntry returns the cached hot-path entry for id, loading it from
// the control plane at most once per TTL window.
func (r *Resolver) platformEntry(id string) (specEntry, bool) {
	now := time.Now()
	r.mu.RLock()
	e, ok := r.speCache[id]
	r.mu.RUnlock()
	if ok && now.Before(e.exp) {
		return e, true
	}
	spec, err := r.Control.GetPlatformSpec(context.Background(), id)
	if err != nil {
		return specEntry{}, false
	}
	pol := spec.Policy // copy: the pipeline must not mutate the cached value
	pickPol := pick.Policy{
		Allocation:  allocationOf(spec.Allocation),
		StickyTTL:   spec.StickyTTL,
		Authorities: r.Authorities,
	}
	if name, isExt := strings.CutPrefix(spec.Allocation, "ext."); isExt {
		pickPol.SelectorName = name
	}
	e = specEntry{
		pol:        &pol,
		pick:       pickPol,
		breakerOff: spec.PassiveBreaker == control.PassiveBreakerOff,
		exp:        now.Add(r.ttl),
	}
	r.mu.Lock()
	r.speCache[id] = e
	r.mu.Unlock()
	return e, true
}

// PolicyFor returns the platform's compiled identity policy, or nil when
// the platform is unknown (the pipeline then falls back to
// identity.DefaultPolicy).
func (r *Resolver) PolicyFor(id string) *identity.Policy {
	if e, ok := r.platformEntry(id); ok {
		return e.pol
	}
	return nil
}

// PickPolicyFor returns the platform's selection policy: per-platform
// allocation and sticky TTL, plus the shared authority domains.
func (r *Resolver) PickPolicyFor(id string) pick.Policy {
	if e, ok := r.platformEntry(id); ok {
		return e.pick
	}
	return pick.Policy{Authorities: r.Authorities}
}

// PassiveBreakerOff reports whether the platform disabled its passive
// breaker (design §6.1): such platforms' traffic is emitted with
// BreakerExempt and the health projector skips failure counting.
func (r *Resolver) PassiveBreakerOff(id string) bool {
	e, ok := r.platformEntry(id)
	return ok && e.breakerOff
}

// allocationOf maps the control-plane allocation string to the selector
// enum; unknown values fall back to BALANCED.
func allocationOf(s string) pick.Allocation {
	switch s {
	case control.AllocationPreferLowLatency:
		return pick.AllocationPreferLowLatency
	case control.AllocationPreferIdleIP:
		return pick.AllocationPreferIdleIP
	default:
		return pick.AllocationBalanced
	}
}

// Invalidate drops cached entries for one platform (by UUID, name, or
// both). Wired to control-plane change notifications when available.
func (r *Resolver) Invalidate(id, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.speCache, id)
	if name != "" {
		delete(r.idCache, name)
	}
}
