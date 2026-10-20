package wiring

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Realtime series tuning (GET /api/v1/metrics/realtime/*).
const (
	// RealtimeStep is the sampling interval of every realtime series.
	RealtimeStep = 2 * time.Second
	// realtimeRetention bounds how many samples each series keeps (1h).
	realtimeRetention = time.Hour
	realtimeMaxPoints = int(realtimeRetention / RealtimeStep)
)

type thrPoint struct {
	ts                    time.Time
	ingressBps, egressBps float64
}
type connPoint struct {
	ts      time.Time
	in, out int64
}
type leasePoint struct {
	ts time.Time
	n  int64
}

// RealtimeTracker maintains the in-memory realtime metric series:
// throughput (bytes/s from Traffic signals), active relay sessions and
// per-platform lease counts, all sampled every RealtimeStep. Series are
// ring buffers of one hour; the metrics backend reads them via Series*.
type RealtimeTracker struct {
	up, down atomic.Int64 // bytes accumulated since the last sample

	mu       sync.Mutex
	thr      []thrPoint
	conns    []connPoint
	leases   map[string][]leasePoint // per platform ID
	leasesAt []leasePoint            // global
}

// NewRealtimeTracker creates an empty tracker.
func NewRealtimeTracker() *RealtimeTracker {
	return &RealtimeTracker{leases: map[string][]leasePoint{}}
}

// Run consumes Traffic signals (byte accounting) and samples the gauges
// every RealtimeStep until ctx is done. conns is the pipeline's active
// session gauge (nil ⇒ 0); leaseCount maps a platform ID to its live
// lease count; platformIDs enumerates the platforms to sample.
func (t *RealtimeTracker) Run(ctx context.Context, sub <-chan pksignal.Signal, conns *atomic.Int64, leaseCount func(string) int64, platformIDs func() []string) {
	tick := time.NewTicker(RealtimeStep)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-sub:
			if !ok {
				return
			}
			if sig.Kind == pksignal.KindTraffic {
				t.up.Add(int64(sig.BytesUp))
				t.down.Add(int64(sig.BytesDown))
			}
		case now := <-tick.C:
			t.sample(now, conns, leaseCount, platformIDs)
		}
	}
}

// sample folds the accumulated bytes into one throughput point and
// snapshots the session/lease gauges.
func (t *RealtimeTracker) sample(now time.Time, conns *atomic.Int64, leaseCount func(string) int64, platformIDs func() []string) {
	up := t.up.Swap(0)
	down := t.down.Swap(0)
	var active int64
	if conns != nil {
		active = conns.Load()
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.thr = appendTrim(t.thr, thrPoint{
		ts:         now,
		ingressBps: float64(up) / RealtimeStep.Seconds(),
		egressBps:  float64(down) / RealtimeStep.Seconds(),
	})
	t.conns = appendTrim(t.conns, connPoint{ts: now, in: active, out: active})
	if leaseCount != nil && platformIDs != nil {
		var total int64
		for _, id := range platformIDs() {
			n := leaseCount(id)
			total += n
			t.leases[id] = appendTrim(t.leases[id], leasePoint{ts: now, n: n})
		}
		t.leasesAt = appendTrim(t.leasesAt, leasePoint{ts: now, n: total})
	}
}

func appendTrim[T any](s []T, p T) []T {
	s = append(s, p)
	if len(s) > realtimeMaxPoints {
		s = append([]T(nil), s[len(s)-realtimeMaxPoints:]...)
	}
	return s
}

// inWindow filters points to [from, to]; zero bounds mean "unbounded".
func inWindow[T any](pts []T, from, to time.Time, tsOf func(T) time.Time) []T {
	out := pts[:0:0]
	for _, p := range pts {
		ts := tsOf(p)
		if !from.IsZero() && ts.Before(from) {
			continue
		}
		if !to.IsZero() && ts.After(to) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// Throughput returns the throughput series items for [from, to].
func (t *RealtimeTracker) Throughput(from, to time.Time) []map[string]any {
	t.mu.Lock()
	pts := inWindow(t.thr, from, to, func(p thrPoint) time.Time { return p.ts })
	out := make([]map[string]any, 0, len(pts))
	for _, p := range pts {
		out = append(out, map[string]any{
			"ts":          p.ts.UTC().Format(time.RFC3339),
			"ingress_bps": p.ingressBps,
			"egress_bps":  p.egressBps,
		})
	}
	t.mu.Unlock()
	return out
}

// Connections returns the session series items for [from, to].
func (t *RealtimeTracker) Connections(from, to time.Time) []map[string]any {
	t.mu.Lock()
	pts := inWindow(t.conns, from, to, func(p connPoint) time.Time { return p.ts })
	out := make([]map[string]any, 0, len(pts))
	for _, p := range pts {
		out = append(out, map[string]any{
			"ts":                   p.ts.UTC().Format(time.RFC3339),
			"inbound_connections":  p.in,
			"outbound_connections": p.out,
		})
	}
	t.mu.Unlock()
	return out
}

// Leases returns the lease-count series items for [from, to]: the
// per-platform series when platformID is set, the global one otherwise.
func (t *RealtimeTracker) Leases(from, to time.Time, platformID string) []map[string]any {
	t.mu.Lock()
	src := t.leasesAt
	if platformID != "" {
		src = t.leases[platformID]
	}
	pts := inWindow(src, from, to, func(p leasePoint) time.Time { return p.ts })
	out := make([]map[string]any, 0, len(pts))
	for _, p := range pts {
		out = append(out, map[string]any{
			"ts":            p.ts.UTC().Format(time.RFC3339),
			"active_leases": p.n,
		})
	}
	t.mu.Unlock()
	return out
}
