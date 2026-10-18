package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestScheduler_RunsOnExpiry(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	var ran atomic.Int32
	s.Schedule(20*time.Millisecond, func(context.Context) { ran.Add(1) })
	waitFor(t, 2*time.Second, func() bool { return ran.Load() == 1 }, "scheduled task")
	if n := ran.Load(); n != 1 {
		t.Fatalf("task ran %d times, want exactly 1", n)
	}
}

func TestScheduler_ScheduleAtPastRunsASAP(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	done := make(chan struct{})
	s.ScheduleAt(time.Now().Add(-time.Hour), func(context.Context) { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("past-deadline task did not run promptly")
	}
}

func TestScheduler_PanicIsolation(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	var ran atomic.Int32
	s.Schedule(10*time.Millisecond, func(context.Context) { panic("boom") })
	s.Schedule(10*time.Millisecond, func(context.Context) { ran.Add(1) })
	waitFor(t, 2*time.Second, func() bool { return ran.Load() == 1 }, "task after panicking task")

	// The scheduler itself must still accept and run work afterwards.
	var ran2 atomic.Int32
	s.Schedule(10*time.Millisecond, func(context.Context) { ran2.Add(1) })
	waitFor(t, 2*time.Second, func() bool { return ran2.Load() == 1 }, "task after panic")
}

func TestScheduler_OverrunDoesNotStallQueue(t *testing.T) {
	s := NewScheduler()
	s.TaskTimeout = 50 * time.Millisecond
	defer s.Stop()

	released := make(chan struct{})
	// A task that blocks until its ctx is cancelled (the overrun path).
	s.Schedule(10*time.Millisecond, func(ctx context.Context) {
		<-ctx.Done()
		close(released)
	})
	var quick atomic.Int32
	s.Schedule(20*time.Millisecond, func(context.Context) { quick.Add(1) })

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("overrunning task was not detached via ctx cancellation")
	}
	waitFor(t, 2*time.Second, func() bool { return quick.Load() == 1 }, "quick task behind overrun")
}

func TestScheduler_StopIsGracefulAndIdempotent(t *testing.T) {
	s := NewScheduler()

	started := make(chan struct{})
	finished := make(chan struct{})
	s.Schedule(10*time.Millisecond, func(ctx context.Context) {
		close(started)
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
		close(finished)
	})
	<-started
	s.Stop()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop returned before in-flight task finished")
	}
	s.Stop() // idempotent

	// Scheduling after Stop is dropped, never run.
	var ran atomic.Int32
	s.Schedule(10*time.Millisecond, func(context.Context) { ran.Add(1) })
	time.Sleep(100 * time.Millisecond)
	if ran.Load() != 0 {
		t.Fatal("task scheduled after Stop ran")
	}
}

func TestScheduler_ManyTasksRunOnce(t *testing.T) {
	s := NewScheduler()
	defer s.Stop()

	const n = 500
	var total atomic.Int32
	for i := 0; i < n; i++ {
		s.Schedule(time.Duration(i%7)*time.Millisecond, func(context.Context) { total.Add(1) })
	}
	waitFor(t, 5*time.Second, func() bool { return total.Load() == n }, "all 500 tasks")
	if p := s.Pending(); p != 0 {
		t.Fatalf("Pending() = %d, want 0", p)
	}
}
