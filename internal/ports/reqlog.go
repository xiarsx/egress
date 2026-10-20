package ports

import (
	"net/netip"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// RequestLogRecord is one proxied request's terminal record
// (spec/events.md §2), emitted by the data-plane pipeline exactly once
// per request — success or failure. Client-canceled requests produce no
// record (spec/errors.md §1.4: not a proxy error).
//
// The emitter (ReqLogWriter) must be non-blocking: stage on a bounded
// queue and shed on full, never stall the hot path.
type RequestLogRecord struct {
	TS          time.Time   // request start
	ProxyType   string      // pipeline.Proto*; the writer maps to spec values
	Platform    string      // wire platform name (identity prefix); empty pre-resolve
	PlatformID  string      // resolved platform UUID
	Account     string      // empty = random routing
	Target      string      // "host:port"
	NodeHash    pknode.Hash // zero for direct/bypass or pre-route failures
	EgressIP    netip.Addr  // invalid when unknown
	NetOK       bool        // network-layer verdict
	HTTPStatus  int         // 0 when meaningless (tunnel/SOCKS5/not reached)
	Stage       string      // failure stage: "" | "dial" | "tls" | "request" | "tunnel"
	EgressError string      // X-Egress-Error code; empty on success
	DurationMs  int64
	BytesUp     int64
	BytesDown   int64
}

// RequestLogger consumes terminal request records. Implementations must
// be non-blocking (see RequestLogRecord).
type RequestLogger interface {
	LogRequest(rec RequestLogRecord)
}
