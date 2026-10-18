package ports

import (
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// SignalBus fans one network fact out to every consumer — the health
// projector, metrics sink, request-log sink and cache sink (design §6.1).
// Consumers are independent: removing any one of them is invisible to the
// data plane.
//
// Delivery contract: Emit never blocks the data plane. When the bus
// buffer is full the signal is dropped (counted, not queued). Subscribe
// returns a channel that receives every signal emitted after subscribing;
// slow consumers must size their own buffer or shed load — the bus will
// not slow down producers for them.
//
// NOTE: the Signal type is canonical in pkg/signal, not in internal/core.
// This interface package may not import internal/ packages, while the
// core (producers/consumers) and adapters both need the same definition.
type SignalBus interface {
	// Emit publishes a signal without blocking; full buffer ⇒ drop.
	Emit(sig pksignal.Signal)

	// Subscribe returns a channel receiving subsequently emitted signals.
	Subscribe() <-chan pksignal.Signal
}
