package wiring

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/pkg/ext"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// recordingSink collects events with a mutex (assertions read after waits).
// Registered once per test binary (the ext registry is process-global and
// panics on duplicates; -count=N re-enters the test).
type recordingSink struct {
	mu     sync.Mutex
	events []ext.Event
}

var (
	sharedSink       = &recordingSink{}
	registerSinkOnce sync.Once
)

func (s *recordingSink) Name() string { return "test-recording-sink" }
func (s *recordingSink) Consume(ev ext.Event) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
}
func (s *recordingSink) kinds() []ext.EventKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ext.EventKind, len(s.events))
	for i, ev := range s.events {
		out[i] = ev.Kind
	}
	return out
}

func waitForSink(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSinkFanout_SignalsAndRequestLogs(t *testing.T) {
	sink := sharedSink
	registerSinkOnce.Do(func() { ext.RegisterSink(sink) })

	f := NewSinkFanout(ext.GetSinks())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := make(chan pksignal.Signal, 16)
	go f.RunSignals(ctx, sub)

	// Signal event.
	sub <- pksignal.Signal{Kind: pksignal.KindTraffic, NodeHash: [16]byte{1}, OK: true}

	// Request-log event via the wrapped logger.
	var primary []ports.RequestLogRecord
	logFn := f.WrapRequestLog(func(rec ports.RequestLogRecord) { primary = append(primary, rec) })
	logFn(ports.RequestLogRecord{ProxyType: "http-forward", Target: "example.com:80", DurationMs: 3})

	waitForSink(t, func() bool { return len(sink.kinds()) >= 2 }, "both event kinds")
	kinds := sink.kinds()
	var sawSignal, sawLog bool
	for _, k := range kinds {
		sawSignal = sawSignal || k == ext.EventSignal
		sawLog = sawLog || k == ext.EventRequestLog
	}
	if !sawSignal || !sawLog {
		t.Fatalf("sink kinds = %v, want signal + request log", kinds)
	}
	if len(primary) != 1 {
		t.Fatalf("primary got %d records, want 1 (fanout must not eat the write)", len(primary))
	}
}

func TestSinkFanout_NoSinksPassThrough(t *testing.T) {
	f := NewSinkFanout(nil)
	called := 0
	logFn := f.WrapRequestLog(func(rec ports.RequestLogRecord) { called++ })
	logFn(ports.RequestLogRecord{})
	if called != 1 {
		t.Fatal("no-sink fanout must pass through to primary")
	}
	// RunSignals drains the subscription so the bus never blocks.
	ctx, cancel := context.WithCancel(context.Background())
	sub := make(chan pksignal.Signal, 1)
	done := make(chan struct{})
	go func() { f.RunSignals(ctx, sub); close(done) }()
	sub <- pksignal.Signal{}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunSignals did not stop on ctx cancel")
	}
}
