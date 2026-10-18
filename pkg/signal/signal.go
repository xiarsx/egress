// Package signal defines the Signal vocabulary shared by producers
// (passive traffic, active probers, subscription downloads) and consumers
// (health projector, metrics/request-log/cache sinks).
//
// It lives in pkg because internal/ports (SignalBus) may not import
// internal/ packages, while both sides need the same definition.
// Values are immutable; the bus carries them by value.
package signal

import (
	"net/netip"
	"time"
)

// Kind classifies what a Signal reports (design §6.1).
type Kind int

const (
	KindEgress Kind = iota + 1
	KindLatency
	KindTraffic
)

// SignalKind is the breaker-relevant classification of a signal, as
// consumed by the health step function (design §2.4).
type SignalKind int

const (
	// SignalOK is a successful observation.
	SignalOK SignalKind = iota
	// SignalFail is a failed observation: it increments the consecutive
	// failure counter and may trip the breaker.
	SignalFail
	// SignalFailExempt is a failure with BreakerExempt set (the platform
	// disabled its passive breaker): health is left unchanged.
	SignalFailExempt
	// SignalIgnored marks an observation that must not affect health at
	// all — e.g. the node currently has no outbound instance, so the
	// failure is not counted (design §2.4, last row). The projector
	// produces this kind; it never comes from the wire.
	SignalIgnored
)

// Signal is a single immutable network fact (design §2.1).
//
//	Signal = Egress  { node, at, ip? }                   // ip invalid ⇒ failure
//	       | Latency { node, at, domain, ms?, authority } // ms missing ⇒ failure
//	       | Traffic { node, at, platform, ok, tlsMs?, bytes, breakerExempt }
//
// Connectivity verdict: the network layer establishing a connection counts
// as ok (even with HTTP 500); handshake failure, timeout and RST are fail.
type Signal struct {
	Kind     Kind
	NodeHash [16]byte
	At       time.Time

	// Egress probe: EgressIP invalid ⇒ failure.
	EgressIP netip.Addr

	// Latency probe: HasLatency false ⇒ failure.
	Domain     string
	Latency    time.Duration
	HasLatency bool
	Authority  bool

	// Passive traffic:
	PlatformID    string
	OK            bool
	BreakerExempt bool
	TLSHandshake  time.Duration // valid iff HasTLS
	HasTLS        bool
	BytesUp       uint64
	BytesDown     uint64
}

// Failed reports whether the signal counts as a failure for its kind.
func (s Signal) Failed() bool {
	switch s.Kind {
	case KindEgress:
		return !s.EgressIP.IsValid()
	case KindLatency:
		return !s.HasLatency
	default:
		return !s.OK
	}
}

// Verdict maps the signal to the breaker-relevant kind.
func (s Signal) Verdict() SignalKind {
	if !s.Failed() {
		return SignalOK
	}
	if s.BreakerExempt {
		return SignalFailExempt
	}
	return SignalFail
}
