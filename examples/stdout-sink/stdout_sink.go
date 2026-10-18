// Package stdoutsink is an example egress extension module: a Sink that
// prints every event as one JSON line on stdout.
//
// It exists to prove the pkg/ext SDK is usable end to end. To include it
// in a custom binary, import it for its side effects:
//
//	import _ "github.com/xiarsx/egress/examples/stdout-sink"
//
// NOTE: this is example-grade, not production-grade: Consume serializes
// writers with a mutex and a slow stdout can stall the sink's own
// goroutine. Per the Sink contract it never blocks the data plane itself
// (the bus drops on a full subscriber buffer), but a production sink
// should shed load internally instead of serializing on a slow writer.
package stdoutsink

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/xiarsx/egress/pkg/ext"
)

type stdoutSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (s *stdoutSink) Name() string { return "stdout" }

func (s *stdoutSink) Consume(ev ext.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Best effort: a failed encode must not take the sink down.
	_ = s.enc.Encode(eventJSON(ev))
}

// eventJSON renders the event in a stable, greppable shape.
func eventJSON(ev ext.Event) map[string]any {
	switch ev.Kind {
	case ext.EventSignal:
		sig := ev.Signal
		return map[string]any{
			"type":       "signal",
			"kind":       int(sig.Kind),
			"node":       shortHash(sig.NodeHash[:]),
			"at":         sig.At.UnixNano(),
			"ok":         !sig.Failed(),
			"domain":     sig.Domain,
			"platform":   sig.PlatformID,
			"bytes_up":   sig.BytesUp,
			"bytes_down": sig.BytesDown,
		}
	case ext.EventRequestLog:
		rl := ev.RequestLog
		return map[string]any{
			"type":        "request_log",
			"ts":          rl.TS,
			"proxy_type":  rl.ProxyType,
			"platform":    rl.Platform,
			"target":      rl.Target,
			"node":        rl.Node,
			"net_ok":      rl.NetOK,
			"duration_ms": rl.DurationMs,
		}
	default:
		return map[string]any{"type": "unknown"}
	}
}

func shortHash(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, 16)
	for _, c := range b {
		out = append(out, hexd[c>>4], hexd[c&0xf])
		if len(out) == 16 {
			break
		}
	}
	return string(out)
}

func init() {
	ext.RegisterSink(&stdoutSink{enc: json.NewEncoder(os.Stdout)})
}
