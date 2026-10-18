package ext

import (
	pknode "github.com/xiarsx/egress/pkg/node"
)

// SelectContext is the read-only input to a Selector's Pick
// (spec/ext-abi.md §2): the candidate list is a read-only view, ctx
// carries the target, the platform's allocation policy and a lease-count
// snapshot. It may grow additively (backwards-compatible fields only)
// while experimental.
type SelectContext struct {
	// Target is "host:port" of the requested destination.
	Target string
	// PlatformID is the platform UUID the request resolved to.
	PlatformID string
}

// Selector picks one node from the candidates (spec/ext-abi.md §2):
// cost, quota and custom allocation policies.
//
// Stability: experimental (M6 promotes to stable).
//
// HOT PATH — the discipline in doc.go applies strictly: pure function,
// zero allocation, O(candidates) or better. cands must be treated as
// read-only. Admission to the built release is gated by a benchmark
// threshold (TODO: threshold value fixed at M4).
type Selector interface {
	// Name is the selector key used at registration.
	Name() string
	// Pick returns the chosen node's hash.
	Pick(cands []pknode.NodeSnapshot, ctx SelectContext) pknode.Hash
}
