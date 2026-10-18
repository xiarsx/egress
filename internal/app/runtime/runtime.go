// Package runtime is the P4 runtime orchestration: the scheduler,
// signal bus, health projector, view deriver, active prober and the
// startup reconcile (design §8, §9.2).
//
// Single-writer map (the iron rule for shared state):
//
//	Health      — only HealthProjector writes, via ports.NodeRegistry.UpdateHealth.
//	View        — only Deriver writes (its own goroutine/hook context, mutex-guarded).
//	Lease/IPLoad — only LeaseTable methods move counters; reconcile and the
//	              data plane schedule expiry through the Scheduler.
//
// The package consumes only ports-layer contracts (internal/ports) plus the
// pure core packages (node, view, lease, predicate) and the pkg kernel
// types (signal, node, lease). It never imports P1/P2/P5 implementation
// packages; assembly lives in cmd/egress.
package runtime
