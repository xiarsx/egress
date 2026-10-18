package control

import (
	"github.com/xiarsx/egress/internal/core/predicate"
	"github.com/xiarsx/egress/internal/ports"
)

// SetHooks replaces the RuntimeHooks the service notifies on declarative
// resource changes (platform/source create/update/delete).
//
// It exists for the assembly root (cmd/egress, design §16): the control
// service needs the runtime as its hooks, while the runtime needs the
// service as its PlatformSource/SourceSource — a construction cycle that
// is broken by building the service first with NopHooks and swapping in
// the real runtime once both exist.
//
// Call it exactly once during startup, before any API request is served;
// it is not safe for concurrent use with in-flight requests. A nil hooks
// is treated as NopHooks, matching NewService.
func (s *Service) SetHooks(hooks ports.RuntimeHooks) {
	if hooks == nil {
		hooks = NopHooks{}
	}
	s.hooks = hooks
}

// ToPredicateFilter converts a platform filter declaration into the kernel
// predicate.Filter (exported for the wiring backend; same conversion the
// service itself uses when compiling platforms).
func ToPredicateFilter(f PlatformFilter) predicate.Filter { return toPredicateFilter(f) }

// SetRuntimeBackend replaces the RuntimeBackend serving runtime-owned
// reads/actions (nodes, leases, metrics, request logs, geoip, probes).
//
// Same construction-cycle rationale as SetHooks: call it exactly once
// during startup, before any API request is served; not safe for
// concurrent use with in-flight requests. A nil backend leaves the
// previous one (initially none → 501 NOT_IMPLEMENTED) in place.
func (s *Service) SetRuntimeBackend(b RuntimeBackend) {
	if b == nil {
		return
	}
	s.backend = b
}
