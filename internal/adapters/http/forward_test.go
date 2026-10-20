package forward

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/testutil/pipefake"
	pknode "github.com/xiarsx/egress/pkg/node"
)

var (
	testHash   = pknode.Hash{3, 3, 3}
	testPlatID = "test-platform-id"
)

func newPipeline(t *testing.T, token string, d *pipefake.Dialer) *pipeline.Pipeline {
	t.Helper()
	snap := pknode.NodeSnapshot{Hash: testHash, EgressIP: netip.MustParseAddr("198.51.100.9"), HasOutbound: true, Health: pknode.HealthyHealth(0)}
	v := view.Derive([]pknode.NodeSnapshot{snap}, nil)
	return pipeline.New(pipeline.Deps{
		Resolver:   &pipefake.Resolver{ID: testPlatID},
		NodeDialer: d,
		Direct:     &pipefake.Direct{},
		Bus:        &pipefake.Bus{},
		Clock:      &pipefake.Clock{T: time.Now()},
		Leases:     lease.NewLeaseTable(),
		Views:      func(string) *view.View { return v },
		Nodes: func(h pknode.Hash) (pknode.NodeSnapshot, bool) {
			if h == testHash {
				return snap, true
			}
			return pknode.NodeSnapshot{}, false
		},
		Latency:       func(pknode.Hash, string) (time.Duration, bool) { return 0, false },
		IPLoad:        func(string, netip.Addr) int64 { return 0 },
		PickPolicyFor: func(string) pick.Policy { return pick.Policy{Allocation: pick.AllocationBalanced} },
		Rand:          rand.New(rand.NewPCG(7, 7)),
		ProxyToken:    token,
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
	go (&http.Server{Handler: New(p)}).Serve(l)
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
	d := &pipefake.Dialer{Serve: func(up net.Conn) {
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
	if len(d.Seen) != 1 || d.Seen[0] != "example.com:443" {
		t.Fatalf("dialed = %v", d.Seen)
	}
}

func TestConnectDialFailure(t *testing.T) {
	d := &pipefake.Dialer{Err: errors.New("connection refused")}
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
	if headers["X-Egress-Error"] != "UPSTREAM_CONNECT_FAILED" {
		t.Fatalf("X-Egress-Error = %q", headers["X-Egress-Error"])
	}
}

func TestConnectAuth(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &pipefake.Dialer{}))

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
		if headers["X-Egress-Error"] != "AUTH_REQUIRED" {
			t.Fatalf("X-Egress-Error = %q", headers["X-Egress-Error"])
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
		if headers["X-Egress-Error"] != "AUTH_FAILED" {
			t.Fatalf("X-Egress-Error = %q", headers["X-Egress-Error"])
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
		if headers["X-Egress-Error"] != "AUTH_REQUIRED" {
			t.Fatalf("X-Egress-Error = %q", headers["X-Egress-Error"])
		}
	})
}

func TestPlainForward(t *testing.T) {
	headCh := make(chan []byte, 1)
	d := &pipefake.Dialer{Serve: func(up net.Conn) {
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
	// Upstream must receive the re-serialized origin-form head, with the
	// proxy credentials stripped.
	select {
	case head := <-headCh:
		hs := string(head)
		if !strings.HasPrefix(hs, "GET /p?q=1 HTTP/1.1\r\n") {
			t.Fatalf("upstream head first line = %q", hs)
		}
		if strings.Contains(hs, "Proxy-Authorization") {
			t.Fatalf("credentials must not travel upstream: %q", hs)
		}
		if !strings.Contains(hs, "Host: example.com\r\n") {
			t.Fatalf("missing Host header: %q", hs)
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
	addr := startServer(t, newPipeline(t, "tok", &pipefake.Dialer{}))
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
	if headers["X-Egress-Error"] != "PLATFORM_NOT_FOUND" {
		t.Fatalf("X-Egress-Error = %q", headers["X-Egress-Error"])
	}
}

// TestRequireAuthInfo: with an empty proxy token (auth disabled) and
// RequireProxyAuthInfo set, requests without Proxy-Authorization get 407;
// any well-formed credential passes the local gate (password unchecked).
func TestRequireAuthInfo(t *testing.T) {
	srv := New(newPipeline(t, "", &pipefake.Dialer{}))
	srv.RequireAuthInfo = true
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go (&http.Server{Handler: srv}).Serve(l)
	addr := l.Addr().String()

	t.Run("missing header is 407", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: x\r\n\r\n")
		status, headers := readStatusHead(t, bufio.NewReader(conn))
		if !strings.HasPrefix(status, "HTTP/1.1 407") {
			t.Fatalf("status = %q", status)
		}
		if headers["X-Egress-Error"] != "AUTH_REQUIRED" {
			t.Fatalf("X-Egress-Error = %q", headers["X-Egress-Error"])
		}
	})
	t.Run("any credential passes the gate", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		// Password is NOT validated in no-auth mode; identity is enough.
		_, _ = io.WriteString(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: x\r\n"+
			"Proxy-Authorization: "+basicAuth("Default.alice", "anything")+"\r\n\r\n")
		status, headers := readStatusHead(t, bufio.NewReader(conn))
		// The pipeline then resolves the platform — the fake resolver
		// knows "Default", so the dial fails (no upstream), NOT 407.
		if strings.HasPrefix(status, "HTTP/1.1 407") {
			t.Fatalf("credential must pass the gate, got %q", status)
		}
		if headers["X-Egress-Error"] == "AUTH_REQUIRED" {
			t.Fatalf("X-Egress-Error = %q, want a post-auth failure", headers["X-Egress-Error"])
		}
	})
}
