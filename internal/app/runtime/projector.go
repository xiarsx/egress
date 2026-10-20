package runtime

import (
	"context"
	"time"

	"github.com/xiarsx/egress/internal/core/node"
	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// HealthProjector is the single writer of node Health (design §6.1, §2.4).
// It consumes Signals from the bus and folds each one into the registry:
// verdict → node.Step → NodeRegistry.UpdateHealth. Cache writes are
// coalesced in the P1 CacheStore; the actual flush is driven by a
// Scheduler task (Flush), so the projector never touches the disk itself.
//
// The outbound-empty rule (design §2.4, last row) is applied here: the
// projector is the only party that knows whether an outbound instance
// currently exists for a node, so an observation on a node without one is
// mapped to SignalIgnored instead of being counted by Step.
//
// Signals for unknown nodes (hash not in the registry) are ignored.
type HealthProjector struct {
	// Reg is the node registry; the projector is its only Health writer.
	Reg ports.NodeRegistry
	// Cache is the coalescing cache sink (cache.db write side).
	Cache ports.CacheStore
	// Cfg tunes the breaker step function (threshold, …).
	Cfg node.BreakerConfig
	// Clock supplies "now" for Tripped timestamps (injectable in tests).
	Clock ports.Clock
}

// Run consumes signals until ctx is cancelled or sub is closed.
func (p *HealthProjector) Run(ctx context.Context, sub <-chan pksignal.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-sub:
			if !ok {
				return
			}
			p.Project(sig)
		}
	}
}

// Project folds one signal into the registry and the coalescing cache.
func (p *HealthProjector) Project(sig pksignal.Signal) {
	now := p.now()
	if sig.At.IsZero() {
		sig.At = now
	}
	h := pknode.Hash(sig.NodeHash)
	n, ok := p.Reg.Get(h)
	if !ok {
		return // unknown node: nothing to project onto
	}

	verdict := sig.Verdict()
	if !n.HasOutbound {
		// No outbound instance ⇒ the observation says nothing about the
		// node itself (design §2.4, outbound-empty row).
		verdict = pksignal.SignalIgnored
	}

	if next := node.Step(n.Health, verdict, p.Cfg, now); next != n.Health {
		// Single-writer path: Health moves only here.
		p.Reg.UpdateHealth(h, next)
		n.Health = next
	}

	// The egress IP reported by the probe lands in BOTH stores: the live
	// registry image (views' by-IP index, sticky leases, geo enrichment)
	// and the warm cache. The projector is the single writer.
	if sig.Kind == pksignal.KindEgress && sig.EgressIP.IsValid() {
		n.EgressIP = sig.EgressIP
		p.Reg.UpdateEgressIP(h, sig.EgressIP)
	}
	p.Cache.UpsertNode(n)

	if sig.Kind == pksignal.KindLatency && sig.HasLatency {
		// Live image (routability needs HasLatency) + the raw sample for
		// the TD-EWMA table (cache.db node_latency).
		p.Reg.UpdateLatency(h, sig.Latency)
		ms := float64(sig.Latency) / float64(time.Millisecond)
		p.Cache.UpsertLatency(h, sig.Domain, ms, sig.At, sig.Authority)
	}
}

func (p *HealthProjector) now() time.Time {
	if p.Clock != nil {
		return p.Clock.Now()
	}
	return time.Now()
}
