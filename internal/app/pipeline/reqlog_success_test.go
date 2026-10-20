package pipeline

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/internal/testutil/pipefake"
)

// Relay success must emit exactly one terminal RequestLog record.
func TestRelaySuccessEmitsRequestLog(t *testing.T) {
	up, upPeer := net.Pipe()
	go func() {
		buf := make([]byte, 64)
		n, _ := upPeer.Read(buf)     // read the head
		_, _ = upPeer.Write(buf[:n]) // echo it back
		_ = upPeer.Close()
	}()

	var recs []ports.RequestLogRecord
	p := New(Deps{
		Clock:  &pipefake.Clock{T: time.Now()},
		ReqLog: func(rec ports.RequestLogRecord) { recs = append(recs, rec) },
	})
	prep := &Prepared{
		Upstream:   up,
		Head:       []byte("ping"),
		startedAt:  time.Now(),
		PlatformID: "p1",
		account:    "alice",
	}
	client, clientPeer := net.Pipe()
	defer clientPeer.Close()
	go io.Copy(io.Discard, clientPeer)

	if perr := p.Relay(context.Background(), prep, client, Request{Proto: ProtoHTTPForward, Target: "x:80"}); perr != nil {
		t.Fatalf("relay: %v", perr)
	}
	if len(recs) != 1 || !recs[0].NetOK {
		t.Fatalf("records = %+v, want one NetOK=true", recs)
	}
}
