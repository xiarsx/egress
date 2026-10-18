package ports

import (
	"context"
	"time"
)

// Task is one unit of scheduled work: egress/latency probes, ephemeral
// eviction checks, lease expiry, source refreshes, GeoIP and attribute
// provider refreshes, cache-sink flushes (design §8). Tasks receive a ctx
// that is cancelled when the scheduler shuts down; long tasks should
// watch it.
type Task func(ctx context.Context)

// Scheduler is a min-heap expiry queue (design §8): each scheduled task
// runs once when its deadline passes. It replaces the previous design's
// family of periodic full-scan goroutines; ~300k heap entries for 100k
// nodes × 3 timers are acceptable in memory and CPU.
//
// Implementations must be safe for concurrent use, must run each task at
// most once per Schedule call, and must keep running remaining tasks when
// one task panics or overruns (isolate failures, never stall the queue).
type Scheduler interface {
	// Schedule runs task once after delay.
	Schedule(delay time.Duration, task Task)

	// ScheduleAt runs task once at time t (past t ⇒ run as soon as possible).
	ScheduleAt(t time.Time, task Task)
}
