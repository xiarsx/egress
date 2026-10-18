package wiring

import (
	"context"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/identity"
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

	ttl time.Duration

	mu       sync.RWMutex
	idCache  map[string]idEntry
	polCache map[string]polEntry
}

type idEntry struct {
	id  string
	exp time.Time
}

type polEntry struct {
	pol *identity.Policy
	exp time.Time
}

// NewResolver builds a Resolver over the control service.
func NewResolver(store ports.Store, svc *control.Service) *Resolver {
	return &Resolver{
		Store:    store,
		Control:  svc,
		ttl:      resolverCacheTTL,
		idCache:  make(map[string]idEntry),
		polCache: make(map[string]polEntry),
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

// PolicyFor returns the platform's compiled identity policy, or nil when
// the platform is unknown (the pipeline then falls back to
// identity.DefaultPolicy).
func (r *Resolver) PolicyFor(id string) *identity.Policy {
	now := time.Now()
	r.mu.RLock()
	e, ok := r.polCache[id]
	r.mu.RUnlock()
	if ok && now.Before(e.exp) {
		return e.pol
	}
	spec, err := r.Control.GetPlatformSpec(context.Background(), id)
	if err != nil {
		return nil
	}
	pol := spec.Policy // copy: the pipeline must not mutate the cached value
	r.mu.Lock()
	r.polCache[id] = polEntry{pol: &pol, exp: now.Add(r.ttl)}
	r.mu.Unlock()
	return &pol
}

// PassiveBreakerOff reports whether the platform disabled its passive
// breaker (design §6.1): such platforms' traffic is emitted with
// BreakerExempt and the health projector skips failure counting.
func (r *Resolver) PassiveBreakerOff(id string) bool {
	p, err := r.Control.GetPlatform(context.Background(), id)
	if err != nil {
		return false
	}
	return p.PassiveBreaker == control.PassiveBreakerOff
}

// Invalidate drops cached entries for one platform (by UUID, name, or
// both). Wired to control-plane change notifications when available.
func (r *Resolver) Invalidate(id, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.polCache, id)
	if name != "" {
		delete(r.idCache, name)
	}
}
