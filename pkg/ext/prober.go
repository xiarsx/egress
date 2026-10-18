package ext

import (
	"context"

	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Prober actively probes a node and reports signals
// (spec/ext-abi.md §2): reachability, reputation, ...
//
// Stability: experimental.
//
// Runs off the hot path in a worker pool; timeouts and concurrency are
// owned by the scheduler, so Probe must honor ctx.
type Prober interface {
	// Name is the prober key used at registration.
	Name() string
	// Probe returns the signals observed for node.
	Probe(ctx context.Context, node pknode.NodeSnapshot) []pksignal.Signal
}
