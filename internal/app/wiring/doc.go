// Package wiring is the assembly glue of egress (design §16): it owns no
// business logic of its own, only the adapters between the groups the
// composition root (cmd/egress) stitches together.
//
// The package is deliberately outside the hot path (design P2): nothing
// here runs per-request except Tracker.Latency, which is a lock-guarded
// map lookup with no allocation on the read path.
package wiring
