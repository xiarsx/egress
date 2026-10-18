package runtime

import (
	"sync"
	"sync/atomic"

	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Bus is the design §6.1 fan-out: one network fact (a Signal) goes to every
// consumer — the health projector, metrics sink, request-log sink and cache
// sink. Consumers are independent: removing any one is invisible to the
// data plane.
//
// Delivery contract: Emit never blocks. Each subscriber owns a buffered
// channel; when it is full the signal is dropped for that subscriber and the
// drop is counted (Dropped). Subscribe returns a channel that receives every
// signal emitted after subscribing; slow consumers must size their own
// buffer or shed load — the bus will not slow down producers.
//
// Concurrency: safe for concurrent use. Close is idempotent; Emit after
// Close is ignored.
type Bus struct {
	mu      sync.RWMutex
	subs    []chan pksignal.Signal
	closed  bool
	dropped atomic.Int64
	bufSize int
}

// NewBus creates a Bus whose subscriber channels are buffered with bufSize
// slots each. bufSize <= 0 selects DefaultBusBuffer.
func NewBus(bufSize int) *Bus {
	if bufSize <= 0 {
		bufSize = DefaultBusBuffer
	}
	return &Bus{bufSize: bufSize}
}

// Emit publishes a signal without blocking. A full subscriber buffer means
// the signal is dropped for that subscriber and counted in Dropped.
func (b *Bus) Emit(sig pksignal.Signal) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for _, ch := range b.subs {
		select {
		case ch <- sig:
		default:
			b.dropped.Add(1)
		}
	}
}

// Subscribe returns a channel receiving every signal emitted after
// subscribing. The channel is closed by Close. Each call returns an
// independent channel.
func (b *Bus) Subscribe() <-chan pksignal.Signal {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan pksignal.Signal, b.bufSize)
	if !b.closed {
		b.subs = append(b.subs, ch)
	} else {
		close(ch)
	}
	return ch
}

// Dropped returns the total number of per-subscriber drops since creation.
func (b *Bus) Dropped() int64 { return b.dropped.Load() }

// SubscriberCount reports how many subscriptions are currently registered.
func (b *Bus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// Close closes every subscriber channel and stops the bus. Idempotent.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, ch := range b.subs {
		close(ch)
	}
	b.subs = nil
}
