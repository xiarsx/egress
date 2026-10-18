package runtime

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/ports"
)

// DefaultTaskTimeout bounds one scheduled task: a task that overruns is
// detached (its ctx is cancelled) so the expiry queue never stalls.
// Probe tasks are the heaviest legitimate users (~15s); five minutes is a
// generous ceiling. Tune via Scheduler.TaskTimeout before starting.
const DefaultTaskTimeout = 5 * time.Minute

// DefaultBusBuffer is the per-subscriber buffer of the default Bus.
// Slow consumers size their own subscription; the bus never blocks producers.
const DefaultBusBuffer = 1024

// Compile-time checks that the P4 implementations satisfy the ports contracts.
var (
	_ ports.Scheduler = (*Scheduler)(nil)
	_ ports.SignalBus = (*Bus)(nil)
)

// scheduledTask is one heap entry. seq breaks deadline ties so same-deadline
// tasks run in Schedule order.
type scheduledTask struct {
	at   time.Time
	seq  uint64
	task ports.Task
}

// taskHeap is a min-heap ordered by (at, seq).
type taskHeap []scheduledTask

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		return h[i].seq < h[j].seq
	}
	return h[i].at.Before(h[j].at)
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(x any)   { *h = append(*h, x.(scheduledTask)) }
func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[:n-1]
	return t
}

// Scheduler is the design §8 min-heap expiry queue: each scheduled task runs
// once when its deadline passes. It replaces the reference implementation's
// family of periodic full-scan goroutines (~300k heap entries for 100k
// nodes × 3 timers are acceptable in memory and CPU).
//
// Concurrency: safe for concurrent use. Each due task is dispatched to its
// own goroutine, so one slow task never blocks the queue. A panicking task
// is recovered in its own goroutine; an overrunning task (past TaskTimeout)
// has its ctx cancelled and is detached — the queue keeps moving.
//
// Lifecycle: created by NewScheduler; the dispatcher loop starts
// immediately. Stop cancels the scheduler ctx, stops accepting new tasks,
// and waits for in-flight (non-detached) tasks to finish.
type Scheduler struct {
	mu      sync.Mutex
	h       taskHeap
	seq     uint64
	wake    chan struct{} // capacity 1; signals "heap head may have moved earlier"
	done    chan struct{}
	stopped bool

	// TaskTimeout bounds one task; zero means DefaultTaskTimeout.
	TaskTimeout time.Duration

	wg  sync.WaitGroup
	ctx context.Context
	// cancel tears down the scheduler ctx on Stop: well-behaved tasks
	// observe cancellation and return, letting Stop complete.
	cancel context.CancelFunc
}

// NewScheduler creates a running Scheduler.
func NewScheduler() *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

// Schedule runs task once after delay.
func (s *Scheduler) Schedule(delay time.Duration, task ports.Task) {
	s.ScheduleAt(time.Now().Add(delay), task)
}

// ScheduleAt runs task once at time t (past t ⇒ run as soon as possible).
// Scheduling after Stop is a no-op: the task is dropped.
func (s *Scheduler) ScheduleAt(t time.Time, task ports.Task) {
	if task == nil {
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.seq++
	heap.Push(&s.h, scheduledTask{at: t, seq: s.seq, task: task})
	s.mu.Unlock()
	// Non-blocking: at most one pending wake is ever needed.
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Stop shuts the scheduler down gracefully: no new tasks are accepted, the
// dispatcher stops, in-flight tasks see their ctx cancelled and run to
// completion (overrun tasks are already detached), and Stop returns only
// after they finish. Idempotent.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	close(s.done)
	s.cancel()
	s.mu.Unlock()
	s.wg.Wait()
}

// loop is the dispatcher: it sleeps until the heap head is due (or an
// earlier task arrives) and dispatches every due task to its own goroutine.
func (s *Scheduler) loop() {
	defer s.wg.Done()
	var timer *time.Timer
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			return
		}
		if len(s.h) == 0 {
			s.mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			select {
			case <-s.wake:
			case <-s.done:
				return
			}
			continue
		}
		d := time.Until(s.h[0].at)
		if d <= 0 {
			now := time.Now()
			var due []ports.Task
			for len(s.h) > 0 && !s.h[0].at.After(now) {
				due = append(due, heap.Pop(&s.h).(scheduledTask).task)
			}
			s.mu.Unlock()
			for _, t := range due {
				s.dispatch(t)
			}
			continue
		}
		s.mu.Unlock()

		if timer == nil {
			timer = time.NewTimer(d)
		} else {
			timer.Reset(d)
		}
		select {
		case <-timer.C:
			// Loop around: the head is now due.
		case <-s.wake:
			// A new task may expire earlier; recompute the wait.
			// Drain a concurrent firing so the timer can be reused.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-s.done:
			timer.Stop()
			return
		}
	}
}

// dispatch runs one task in its own goroutine with panic isolation and an
// overrun timeout. Panics are recovered; tasks past TaskTimeout are
// detached (ctx cancelled) so the queue never stalls behind them.
func (s *Scheduler) dispatch(task ports.Task) {
	timeout := s.TaskTimeout
	if timeout <= 0 {
		timeout = DefaultTaskTimeout
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			// Failure isolation: a panicking task must not take the
			// dispatcher (or any sibling task) down with it.
			defer func() { _ = recover() }()
			task(ctx)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			// Overrun: detach. The task goroutine lingers until it
			// observes ctx cancellation; the queue has already moved on.
		}
	}()
}

// Pending reports the number of tasks currently in the heap. Test/observability aid.
func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.h)
}
