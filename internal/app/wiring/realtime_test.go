package wiring

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// TestRealtimeTracker covers the realtime ring (spec: metrics realtime
// series — throughput from Traffic signals, connections gauge, per-
// platform + total lease counts, ring retention, from/to windowing).
func TestRealtimeTracker(t *testing.T) {
	tr := NewRealtimeTracker()
	conns := &atomic.Int64{}
	conns.Store(7)
	leaseCount := func(id string) int64 {
		if id == "p1" {
			return 3
		}
		return 0
	}
	platformIDs := func() []string { return []string{"p1", "p2"} }

	// Simulate the Run loop's signal accumulation without the 2s ticker:
	// feed bytes, then sample directly (same package, white box).
	tr.up.Add(2000)
	tr.down.Add(4000)

	now := time.Now()
	tr.sample(now, conns, leaseCount, platformIDs)
	tr.sample(now.Add(RealtimeStep), conns, leaseCount, platformIDs)

	thr := tr.Throughput(time.Time{}, time.Time{})
	if len(thr) != 2 {
		t.Fatalf("throughput points = %d", len(thr))
	}
	first := thr[0]
	// 2000 bytes / 2s step = 1000 Bps ingress; counters reset after sample.
	if first["ingress_bps"].(float64) != 1000 || first["egress_bps"].(float64) != 2000 {
		t.Fatalf("throughput = %+v", first)
	}
	second := thr[1]
	if second["ingress_bps"].(float64) != 0 {
		t.Fatalf("counters must reset after sampling: %+v", second)
	}

	connsSeries := tr.Connections(time.Time{}, time.Time{})
	if len(connsSeries) != 2 || connsSeries[0]["inbound_connections"].(int64) != 7 {
		t.Fatalf("connections = %+v", connsSeries)
	}

	global := tr.Leases(time.Time{}, time.Time{}, "")
	if len(global) != 2 || global[0]["active_leases"].(int64) != 3 {
		t.Fatalf("global leases = %+v", global)
	}
	p1 := tr.Leases(time.Time{}, time.Time{}, "p1")
	if len(p1) != 2 || p1[0]["active_leases"].(int64) != 3 {
		t.Fatalf("p1 leases = %+v", p1)
	}
	if none := tr.Leases(time.Time{}, time.Time{}, "nope"); len(none) != 0 {
		t.Fatalf("unknown platform leases = %+v", none)
	}

	// Window filter: only the second point is inside [now+step, +inf).
	w := tr.Throughput(now.Add(RealtimeStep), time.Time{})
	if len(w) != 1 {
		t.Fatalf("windowed throughput = %d points", len(w))
	}
}

// TestRealtimeTrackerRingRetention proves the ring is bounded
// (realtimeMaxPoints): older points are dropped.
func TestRealtimeTrackerRingRetention(t *testing.T) {
	tr := NewRealtimeTracker()
	now := time.Now()
	for i := 0; i < realtimeMaxPoints+10; i++ {
		tr.sample(now.Add(time.Duration(i)*RealtimeStep), nil, nil, nil)
	}
	if got := len(tr.Throughput(time.Time{}, time.Time{})); got != realtimeMaxPoints {
		t.Fatalf("ring size = %d, want %d", got, realtimeMaxPoints)
	}
}

// TestRealtimeRunFeedsSignals exercises the Run loop's Traffic
// accumulation (bytes land in the counters; cancel stops the loop).
func TestRealtimeRunFeedsSignals(t *testing.T) {
	tr := NewRealtimeTracker()
	ctx, cancel := context.WithCancel(context.Background())
	sub := make(chan pksignal.Signal, 4)
	done := make(chan struct{})
	go func() { tr.Run(ctx, sub, nil, nil, nil); close(done) }()

	sub <- pksignal.Signal{Kind: pksignal.KindTraffic, BytesUp: 10, BytesDown: 20}
	sub <- pksignal.Signal{Kind: pksignal.KindEgress} // ignored kind
	// select can take ctx.Done before the queued signal; wait for the
	// consume deterministically, then cancel.
	deadline := time.Now().Add(2 * time.Second)
	for tr.up.Load() != 10 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if tr.up.Load() != 10 || tr.down.Load() != 20 {
		t.Fatalf("counters = %d/%d", tr.up.Load(), tr.down.Load())
	}
}
