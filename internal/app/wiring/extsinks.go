package wiring

import (
	"context"

	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/pkg/ext"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// SinkFanout forwards signals and request logs to every registered
// ext.Sink (spec/ext-abi.md §2). The ABI demands Consume never
// backpressures the data plane, so each sink sits behind its own bounded
// buffer: full buffer → drop for that sink only.
type SinkFanout struct {
	sinks []ext.Sink
	in    chan ports.RequestLogRecord
}

// NewSinkFanout builds the fanout; nil/empty sinks is a valid no-op.
func NewSinkFanout(sinks map[string]ext.Sink) *SinkFanout {
	f := &SinkFanout{in: make(chan ports.RequestLogRecord, 4096)}
	for _, s := range sinks {
		f.sinks = append(f.sinks, s)
	}
	return f
}

// sinkBuffer caps events queued per sink (drop-on-full beyond this).
const sinkBuffer = 4096

// RunSignals fans bus signals out to the sinks until ctx is done.
func (f *SinkFanout) RunSignals(ctx context.Context, sub <-chan pksignal.Signal) {
	if len(f.sinks) == 0 {
		// Drain so the bus never blocks on an unread subscription.
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub:
			}
		}
	}
	type queue struct {
		ch chan ext.Event
	}
	queues := make([]queue, 0, len(f.sinks))
	for _, s := range f.sinks {
		sink := s
		q := queue{ch: make(chan ext.Event, sinkBuffer)}
		queues = append(queues, q)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case ev := <-q.ch:
					sink.Consume(ev)
				}
			}
		}()
	}
	// Request logs arrive on f.in (via WrapRequestLogger) and share the
	// same per-sink queues.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case rec := <-f.in:
				ev := ext.Event{Kind: ext.EventRequestLog, RequestLog: toExtRequestLog(rec)}
				for _, q := range queues {
					select {
					case q.ch <- ev:
					default: // sink is slow; drop (ABI contract)
					}
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-sub:
			if !ok {
				return
			}
			ev := ext.Event{Kind: ext.EventSignal, Signal: sig}
			for _, q := range queues {
				select {
				case q.ch <- ev:
				default: // sink is slow; drop (ABI contract)
				}
			}
		}
	}
}

// WrapRequestLog returns a sink function equivalent to primary that
// additionally queues a best-effort copy for the ext sinks. The ext copy
// never blocks the data plane: a full fanout buffer drops it.
func (f *SinkFanout) WrapRequestLog(primary func(rec ports.RequestLogRecord)) func(rec ports.RequestLogRecord) {
	if len(f.sinks) == 0 {
		return primary
	}
	return func(rec ports.RequestLogRecord) {
		primary(rec)
		select {
		case f.in <- rec:
		default:
		}
	}
}

// toExtRequestLog maps the ports record to the public ABI mirror
// (spec/events.md §2 field-for-field). Platform is the wire name the
// pipeline resolved (falling back to the ID pre-resolve).
func toExtRequestLog(rec ports.RequestLogRecord) ext.RequestLog {
	var node, egressIP string
	if rec.NodeHash != (pknode.Hash{}) {
		node = rec.NodeHash.String()
	}
	if rec.EgressIP.IsValid() {
		egressIP = rec.EgressIP.String()
	}
	platform := rec.Platform
	if platform == "" {
		platform = rec.PlatformID
	}
	return ext.RequestLog{
		TS:          rec.TS.UnixNano(),
		ProxyType:   rec.ProxyType,
		Platform:    platform,
		Account:     rec.Account,
		Target:      rec.Target,
		Node:        node,
		EgressIP:    egressIP,
		NetOK:       rec.NetOK,
		HTTPStatus:  rec.HTTPStatus,
		Stage:       rec.Stage,
		EgressError: rec.EgressError,
		DurationMs:  rec.DurationMs,
		BytesUp:     rec.BytesUp,
		BytesDown:   rec.BytesDown,
	}
}
