package wiring

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/xiarsx/egress/internal/adapters/sqlite"
	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// MetricsSink fans Traffic/Latency/Egress signals into the metrics.db
// coalescing accumulator (design §6.1, §10).
//
// Series naming follows the sqlite package convention "<domain>.<metric>"
// (see metrics.go): traffic.bytes_up / traffic.bytes_down, requests.total
// / requests.ok / requests.fail, latency.tls_ms, probes.latency_ms /
// probes.count, probes.egress_ok / probes.egress_fail.
//
// Concurrency: Run must be the only goroutine calling it per subscription;
// MetricsDB itself is safe for concurrent use.
type MetricsSink struct {
	DB *sqlite.MetricsDB
}

// Run consumes signals until ctx is done or sub is closed. Record never
// blocks (in-memory coalescing); the DB flush cadence is owned by the
// caller / shutdown path.
func (s *MetricsSink) Run(ctx context.Context, sub <-chan pksignal.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-sub:
			if !ok {
				return
			}
			s.consume(sig)
		}
	}
}

func (s *MetricsSink) consume(sig pksignal.Signal) {
	at := sig.At
	if at.IsZero() {
		at = time.Now()
	}
	db := s.DB
	switch sig.Kind {
	case pksignal.KindTraffic:
		db.Record(at, "traffic.bytes_up", float64(sig.BytesUp))
		db.Record(at, "traffic.bytes_down", float64(sig.BytesDown))
		db.Record(at, "requests.total", 1)
		if sig.OK {
			db.Record(at, "requests.ok", 1)
		} else {
			db.Record(at, "requests.fail", 1)
		}
		if sig.HasTLS {
			db.Record(at, "latency.tls_ms", float64(sig.TLSHandshake)/float64(time.Millisecond))
		}
	case pksignal.KindLatency:
		db.Record(at, "probes.count", 1)
		if sig.HasLatency {
			db.Record(at, "probes.latency_ms", float64(sig.Latency)/float64(time.Millisecond))
		} else {
			db.Record(at, "probes.fail", 1)
		}
	case pksignal.KindEgress:
		if sig.EgressIP.IsValid() {
			db.Record(at, "probes.egress_ok", 1)
		} else {
			db.Record(at, "probes.egress_fail", 1)
		}
	}
}

// ReqLogWriter maps the pipeline's terminal request records
// (spec/events.md §2) onto sqlite entries and appends them. Append is
// non-blocking (bounded staging queue, shed-on-full), so Write is safe
// for the pipeline's hot path.
type ReqLogWriter struct {
	Log *sqlite.ReqLog

	// PlatformName maps a platform UUID to its wire-protocol name for the
	// log's platform column. Nil ⇒ the UUID is logged as-is.
	PlatformName func(id string) string
}

// proxyTypeOf maps pipeline ingress identifiers to the spec's log values.
func proxyTypeOf(proto string) string {
	switch proto {
	case pipeline.ProtoHTTPForward:
		return "http-forward"
	case pipeline.ProtoHTTPReverse:
		return "reverse"
	case pipeline.ProtoSOCKS5:
		return "socks5-forward"
	default:
		return "unknown"
	}
}

// Write converts and stages one request record. Non-blocking.
func (w *ReqLogWriter) Write(rec ports.RequestLogRecord) {
	// The pipeline already resolved the wire name; fall back to the ID
	// only for pre-resolve failures (both empty then).
	platform := rec.Platform
	if platform == "" {
		platform = rec.PlatformID
	}
	e := sqlite.ReqLogEntry{
		TS:          rec.TS,
		ProxyType:   proxyTypeOf(rec.ProxyType),
		Platform:    platform,
		Account:     rec.Account,
		Target:      rec.Target,
		NetOK:       rec.NetOK,
		HTTPStatus:  rec.HTTPStatus,
		Stage:       rec.Stage,
		EgressError: rec.EgressError,
		DurationMs:  rec.DurationMs,
		BytesUp:     rec.BytesUp,
		BytesDown:   rec.BytesDown,
	}
	if rec.NodeHash != (pknode.Hash{}) {
		e.Node = hex.EncodeToString(rec.NodeHash[:])
	}
	if rec.EgressIP.IsValid() {
		e.EgressIP = rec.EgressIP.String()
	}
	w.Log.Append(e)
}

// ScheduleCacheFlush arranges a periodic CacheStore.Flush via the
// scheduler. The task re-arms itself after every run, so a slow or
// failed flush never stalls the queue.
//
// NOTE: runtime.Run already schedules its own flush at
// runtime.CacheFlushInterval (5m, design §9.2 step 5). The assembly root
// must not schedule a second one; this helper exists for tests and for
// embedding scenarios that don't run the full runtime.
func ScheduleCacheFlush(sched ports.Scheduler, store ports.CacheStore, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	var tick ports.Task
	tick = func(ctx context.Context) {
		_ = store.Flush() // best-effort; shutdown always flushes once more
		sched.Schedule(interval, tick)
	}
	sched.Schedule(interval, tick)
}
