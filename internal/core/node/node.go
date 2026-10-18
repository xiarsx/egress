// Package node holds node health and routability logic: the Health sum
// type, the pure step function (design §2.4) and the Routable derivation.
// It performs no I/O and reads no clock; callers inject `now`.
//
// The kernel types (Hash, Health, NodeSnapshot, …) are canonically defined
// in pkg/node — the iron rule forbids importing sibling internal/
// packages, so this package re-exports them as aliases and adds the
// behavior (Step, Routable) on top.
package node

import (
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Re-exported kernel types (canonical definitions in pkg/node).
type (
	Hash         = pknode.Hash
	ScopedTag    = pknode.ScopedTag
	AttrKey      = pknode.AttrKey
	AttrValue    = pknode.AttrValue
	Health       = pknode.Health
	HealthKind   = pknode.HealthKind
	NodeSnapshot = pknode.NodeSnapshot
	Predicate    = pknode.Predicate
)

const (
	HealthProbing = pknode.HealthProbing
	HealthHealthy = pknode.HealthHealthy
	HealthTripped = pknode.HealthTripped
)

// Zero is the zero-value Hash.
var Zero = pknode.Zero

// Health constructors.
func ProbingHealth() Health                { return pknode.ProbingHealth() }
func HealthyHealth(failures int) Health    { return pknode.HealthyHealth(failures) }
func TrippedHealth(since time.Time) Health { return pknode.TrippedHealth(since) }

// BreakerConfig tunes the health step function.
type BreakerConfig struct {
	// MaxConsecutiveFailures trips the breaker: Healthy(n) + fail with
	// n+1 >= MaxConsecutiveFailures becomes Tripped(now). Non-positive
	// means 1 (a single failure trips).
	MaxConsecutiveFailures int
}

// Step is the pure health transition function implementing the design
// §2.4 migration table (see also spec/vectors/health_transition_vectors.yaml):
//
//	Probing/Tripped + ok                          → Healthy(0)
//	Probing + fail                                 → Probing
//	Healthy(n) + ok                                → Healthy(0)
//	Healthy(n) + fail, n+1 < threshold             → Healthy(n+1)
//	Healthy(n) + fail, n+1 >= threshold            → Tripped(now)
//	any + fail with breakerExempt (SignalFailExempt) → unchanged
//	any + observation while outbound is empty
//	    (SignalIgnored)                            → unchanged
//
// It reads no clock and performs no I/O; the caller passes `now` for the
// Tripped timestamp. The "node added → Probing" row is the constructor
// ProbingHealth, not a transition. The outbound-empty rule arrives as
// SignalIgnored: the projector — the only party that knows whether an
// outbound instance exists — maps such observations to SignalIgnored
// instead of calling Step with a counted verdict.
func Step(h Health, sig pksignal.SignalKind, cfg BreakerConfig, now time.Time) Health {
	if sig == pksignal.SignalFailExempt || sig == pksignal.SignalIgnored {
		return h
	}
	threshold := cfg.MaxConsecutiveFailures
	if threshold <= 0 {
		threshold = 1
	}
	switch h.Kind {
	case pknode.HealthProbing:
		if sig == pksignal.SignalOK {
			return pknode.HealthyHealth(0)
		}
		return h
	case pknode.HealthHealthy:
		if sig == pksignal.SignalOK {
			return pknode.HealthyHealth(0)
		}
		if n := h.Failures + 1; n >= threshold {
			return pknode.TrippedHealth(now)
		} else {
			return pknode.HealthyHealth(n)
		}
	case pknode.HealthTripped:
		// Tripped nodes take no traffic but keep receiving active probes;
		// only an ok observation pulls them back to Healthy.
		if sig == pksignal.SignalOK {
			return pknode.HealthyHealth(0)
		}
		return h
	default:
		return h
	}
}

// PlatformFilter is the compiled per-platform node filter used by
// Routable. Predicates are compiled once on the cold path (platform
// config change); nil passes.
type PlatformFilter struct {
	Tags  Predicate // compiled tag filter
	Attrs Predicate // compiled attribute filter
}

// Routable derives whether a node may serve a platform (design §2.3):
//
//	Routable(p, n) = n.Outbound ≠ nil ∧ n.Health = Healthy ∧ n.EgressIP ≠ nil
//	               ∧ n.Latency 非空 ∧ TagFilter(p, n.Tags) ∧ AttrFilter(p, n.Attrs)
//
// It is a pure derivation, never stored.
func Routable(p PlatformFilter, n NodeSnapshot) bool {
	if !n.HasOutbound {
		return false
	}
	if n.Health.Kind != pknode.HealthHealthy {
		return false
	}
	if !n.EgressIP.IsValid() {
		return false
	}
	if !n.HasLatency {
		return false
	}
	if p.Tags != nil && !p.Tags(n) {
		return false
	}
	if p.Attrs != nil && !p.Attrs(n) {
		return false
	}
	return true
}
