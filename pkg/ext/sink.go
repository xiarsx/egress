package ext

import (
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// EventKind classifies what a Sink consumes.
type EventKind int

const (
	// EventSignal carries a network fact (design §6.1).
	EventSignal EventKind = iota + 1
	// EventRequestLog carries one proxied request's record
	// (spec/events.md §2).
	EventRequestLog
)

// RequestLog mirrors the stable request-log schema (spec/events.md §2):
// one record per proxied request.
type RequestLog struct {
	TS         int64  // unix nanos
	ProxyType  string // "http-forward" | "reverse" | "socks5-forward"
	Platform   string // resolved platform name (wire name, not UUID)
	Account    string // "" = random routing
	Target     string
	Node       string // node hash hex, "" when unassigned
	EgressIP   string
	NetOK      bool
	HTTPStatus int
	Stage      string
	ResinError string
	DurationMs int64
	BytesUp    int64
	BytesDown  int64
}

// Event is what a Sink consumes: a Signal or a RequestLog.
type Event struct {
	Kind       EventKind
	Signal     pksignal.Signal // valid when Kind == EventSignal
	RequestLog RequestLog      // valid when Kind == EventRequestLog
}

// Sink consumes events on the side (spec/ext-abi.md §2): Prometheus,
// OTel, ClickHouse, webhooks, ...
//
// Stability: experimental.
//
// Consume MUST be non-blocking — when the sink's internal buffer is full
// it drops, it never backpressures the data plane. Sinks are independent:
// removing any sink is invisible to the data plane.
type Sink interface {
	// Name is the sink key used at registration.
	Name() string
	// Consume handles one event without blocking.
	Consume(ev Event)
}
