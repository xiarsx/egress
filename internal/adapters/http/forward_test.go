package forward

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// --- minimal fakes (mirrors pipeline tests) ---

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }

type fakeResolver struct{ id string }

func (f *fakeResolver) ResolvePlatform(name string) (string, bool) {
	if name == "Default" {
		return f.id, true
	}
	return "", false
}
func (f *fakeResolver) PolicyFor(string) *identity.Policy { return nil }

type fakeBus struct{}

func (b *fakeBus) Emit(pksignal.Signal)              {}
func (b *fakeBus) Subscribe() <-chan pksignal.Signal { return nil }

type fakeDialer struct {
	mu    sync.Mutex
	err   error
	seen  []string
	serve func(net.Conn)
}

func (f *fakeDialer) DialContext(ctx context.Context, h pknode.Hash, network, address string) (net.Conn, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	f.seen = append(f.seen, address)
	f.mu.Unlock()
	up, peer := net.Pipe()
	go func() {
		if f.serve != nil {
			f.serve(peer)
		} else {
			_, _ = io.Copy(io.Discard, peer)
		}
	}()
	return up, nil
}

type fakeDirect struct{}

func (f *fakeDirect) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return nil, errors.New("no direct")
}

var (
	testHash   = pknode.Hash{9, 9, 9}
	testPlatID = "test-platform-id"
)

func newPipeline(t *testing.T, token string, d *fakeDialer) *pipeline.Pipeline {
	t.Helper()
	snap := pknode.NodeSnapshot{Hash: testHash, EgressIP: netip.MustParseAddr("198.51.100.9"), HasOutbound: true, Health: pknode.HealthyHealth(0)}
	v := view.Derive([]pknode.NodeSnapshot{snap}, nil)
	return pipeline.New(pipeline.Deps{
		Resolver:   &fakeResolver{id: testPlatID},
		NodeDialer: d,
		Direct:     &fakeDirect{},
		Bus:        &fakeBus{},
		Clock:      &fakeClock{t: time.Now()},
		Leases:     lease.NewLeaseTable(),
		Views:      func(string) *view.View { return v },
		Nodes: func(h pknode.Hash) (pknode.NodeSnapshot, bool) {
			if h == testHash {
				return snap, true
			}
			return pknode.NodeSnapshot{}, false
		},
		Latency:    func(pknode.Hash, string) (time.Duration, bool) { return 0, false },
		IPLoad:     func(string, netip.Addr) int64 { return 0 },
		PickPolicy: pick.Policy{Allocation: pick.AllocationBalanced},
		Rand:       rand.New(rand.NewPCG(7, 7)),
		ProxyToken: token,
	})
}

func basicAuth(identity, token string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(identity+":"+token))
}

func startServer(t *testing.T, p *pipeline.Pipeline) net.Addr {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go New(p).Serve(l)
	return l.Addr()
}

func readStatusHead(t *testing.T, br *bufio.Reader) (status string, headers map[string]string) {
	t.Helper()
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	headers = map[string]string{}
	for {
		l, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read header: %v", err)
		}
		if l == "\r\n" || l == "\n" {
			break
		}
		if i := strings.Index(l, ":"); i > 0 {
			headers[strings.TrimSpace(l[:i])] = strings.TrimSpace(l[i+1:])
		}
	}
	return strings.TrimSpace(status), headers
}

func TestConnectTunnel(t *testing.T) {
	d := &fakeDialer{serve: func(up net.Conn) {
		defer up.Close()
		_, _ = io.Copy(up, up) // echo
	}}
	addr := startServer(t, newPipeline(t, "tok", d))

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	req := "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n" +
		"Proxy-Authorization: " + basicAuth("Default.alice", "tok") + "\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatal(err)
	}
	status, _ := readStatusHead(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("status = %q", status)
	}
	// Tunnel is live: echo round-trip.
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("tunnel read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("echo = %q", got)
	}
	if len(d.seen) != 1 || d.seen[0] != "example.com:443" {
		t.Fatalf("dialed = %v", d.seen)
	}
}

func TestConnectDialFailure(t *testing.T) {
	d := &fakeDialer{err: errors.New("connection refused")}
	addr := startServer(t, newPipeline(t, "tok", d))

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	req := "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n" +
		"Proxy-Authorization: " + basicAuth("Default.alice", "tok") + "\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatal(err)
	}
	status, headers := readStatusHead(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 502") {
		t.Fatalf("status = %q", status)
	}
	if headers["X-Resin-Error"] != "UPSTREAM_CONNECT_FAILED" {
		t.Fatalf("X-Resin-Error = %q", headers["X-Resin-Error"])
	}
}

func TestConnectAuth(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &fakeDialer{}))

	dial := func(t *testing.T) (net.Conn, *bufio.Reader) {
		t.Helper()
		conn, err := net.Dial("tcp", addr.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn, bufio.NewReader(conn)
	}

	t.Run("missing", func(t *testing.T) {
		conn, br := dial(t)
		_, _ = io.WriteString(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: x\r\n\r\n")
		status, headers := readStatusHead(t, br)
		if !strings.HasPrefix(status, "HTTP/1.1 407") {
			t.Fatalf("status = %q", status)
		}
		if headers["X-Resin-Error"] != "AUTH_REQUIRED" {
			t.Fatalf("X-Resin-Error = %q", headers["X-Resin-Error"])
		}
		if headers["Proxy-Authenticate"] == "" {
			t.Fatal("missing Proxy-Authenticate")
		}
	})
	t.Run("bad token", func(t *testing.T) {
		conn, br := dial(t)
		_, _ = io.WriteString(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: x\r\n"+
			"Proxy-Authorization: "+basicAuth("Default.alice", "wrong")+"\r\n\r\n")
		status, headers := readStatusHead(t, br)
		if !strings.HasPrefix(status, "HTTP/1.1 403") {
			t.Fatalf("status = %q", status)
		}
		if headers["X-Resin-Error"] != "AUTH_FAILED" {
			t.Fatalf("X-Resin-Error = %q", headers["X-Resin-Error"])
		}
	})
	t.Run("malformed header", func(t *testing.T) {
		conn, br := dial(t)
		_, _ = io.WriteString(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: x\r\n"+
			"Proxy-Authorization: Bearer xyz\r\n\r\n")
		status, headers := readStatusHead(t, br)
		if !strings.HasPrefix(status, "HTTP/1.1 407") {
			t.Fatalf("status = %q", status)
		}
		if headers["X-Resin-Error"] != "AUTH_REQUIRED" {
			t.Fatalf("X-Resin-Error = %q", headers["X-Resin-Error"])
		}
	})
}

func TestPlainForward(t *testing.T) {
	headCh := make(chan []byte, 1)
	d := &fakeDialer{serve: func(up net.Conn) {
		defer up.Close()
		br := bufio.NewReader(up)
		var head []byte
		for {
			l, err := br.ReadBytes('\n')
			head = append(head, l...)
			if err != nil || bytes.HasSuffix(head, []byte("\r\n\r\n")) {
				break
			}
		}
		headCh <- head
		_, _ = io.WriteString(up, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
	}}
	addr := startServer(t, newPipeline(t, "tok", d))

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raw := "GET http://example.com/p?q=1 HTTP/1.1\r\nHost: example.com\r\n" +
		"Proxy-Authorization: " + basicAuth("Default.alice", "tok") + "\r\n" +
		"Connection: close\r\n\r\n"
	if _, err := io.WriteString(conn, raw); err != nil {
		t.Fatal(err)
	}
	// Upstream must receive the verbatim head.
	select {
	case head := <-headCh:
		if string(head) != raw {
			t.Fatalf("upstream head = %q, want %q", head, raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never got the head")
	}
	// Client reads the relayed response.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("resp = %d %q", resp.StatusCode, body)
	}
}

func TestPlainPlatformNotFound(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &fakeDialer{}))
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	_, _ = io.WriteString(conn, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n"+
		"Proxy-Authorization: "+basicAuth("Nope.alice", "tok")+"\r\n\r\n")
	status, headers := readStatusHead(t, br)
	if !strings.HasPrefix(status, "HTTP/1.1 404") {
		t.Fatalf("status = %q", status)
	}
	if headers["X-Resin-Error"] != "PLATFORM_NOT_FOUND" {
		t.Fatalf("X-Resin-Error = %q", headers["X-Resin-Error"])
	}
}
