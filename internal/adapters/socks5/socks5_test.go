package socks5

import (
	"bufio"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/testutil/pipefake"
	"github.com/xiarsx/egress/internal/testutil/sockswire"
	pknode "github.com/xiarsx/egress/pkg/node"
)

var (
	testHash   = pknode.Hash{7, 7, 7}
	testPlatID = "test-platform-id"
)

func newPipeline(t *testing.T, token string, d *pipefake.Dialer) *pipeline.Pipeline {
	t.Helper()
	snap := pknode.NodeSnapshot{Hash: testHash, EgressIP: netip.MustParseAddr("198.51.100.7"), HasOutbound: true, Health: pknode.HealthyHealth(0)}
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
		Rand:          rand.New(rand.NewPCG(3, 3)),
		ProxyToken:    token,
	})
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

// The wire mechanics come from the shared sockswire package; these thin
// wrappers keep the call sites terse.

func dial(t *testing.T, addr net.Addr) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, br := sockswire.Dial(t, addr.String())
	t.Cleanup(func() { conn.Close() })
	return conn, br
}

func readN(t *testing.T, br *bufio.Reader, n int) []byte {
	t.Helper()
	return sockswire.ReadN(t, br, n)
}

// userpassAuth runs greeting (offering only 0x02) + RFC1929 with the given
// credentials, returning the subnegotiation reply.
func userpassAuth(t *testing.T, conn net.Conn, br *bufio.Reader, user, pass string) []byte {
	t.Helper()
	if sel := sockswire.Greet(t, conn, br, 0x02); sel != [2]byte{0x05, 0x02} {
		t.Fatalf("method reply = %v", sel)
	}
	rep := sockswire.UserPass(t, conn, br, user, pass)
	return rep[:]
}

func connectReq(t *testing.T, conn net.Conn, host string, port int) {
	t.Helper()
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, host...)
	req = append(req, byte(port>>8), byte(port))
	sockswire.Write(t, conn, req)
}

func TestMethodNoAcceptable(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &pipefake.Dialer{}))
	conn, br := dial(t, addr)
	// Token configured, client offers only no-auth → 0xFF (AUTH_REQUIRED).
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatal(err)
	}
	if got := readN(t, br, 2); got[1] != 0xFF {
		t.Fatalf("method reply = %v, want 0xFF", got)
	}
}

func TestUserPassFailure(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &pipefake.Dialer{}))
	conn, br := dial(t, addr)
	if got := userpassAuth(t, conn, br, "Default.alice", "wrong"); got[1] != 0x01 {
		t.Fatalf("auth reply = %v, want failure", got)
	}
}

func TestUserPassSuccessConnectFailure(t *testing.T) {
	d := &pipefake.Dialer{Err: errors.New("connection refused")}
	addr := startServer(t, newPipeline(t, "tok", d))
	conn, br := dial(t, addr)
	if got := userpassAuth(t, conn, br, "Default.alice", "tok"); got[1] != 0x00 {
		t.Fatalf("auth reply = %v, want success", got)
	}
	connectReq(t, conn, "example.com", 80)
	// Dial failure → REP 0x01.
	if got := readN(t, br, 4); got[1] != 0x01 {
		t.Fatalf("reply = %v, want REP 0x01", got)
	}
}

func TestConnectSuccessEcho(t *testing.T) {
	d := &pipefake.Dialer{Serve: func(up net.Conn) {
		defer up.Close()
		_, _ = io.Copy(up, up)
	}}
	addr := startServer(t, newPipeline(t, "tok", d))
	conn, br := dial(t, addr)
	if got := userpassAuth(t, conn, br, "Default.alice", "tok"); got[1] != 0x00 {
		t.Fatalf("auth reply = %v", got)
	}
	connectReq(t, conn, "example.com", 443)
	rep := readN(t, br, 10) // VER REP RSV ATYP(4) + PORT(2)
	if rep[0] != 0x05 || rep[1] != 0x00 {
		t.Fatalf("reply = %v, want success", rep)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if got := readN(t, br, 4); string(got) != "ping" {
		t.Fatalf("echo = %q", got)
	}
}

func TestNoAuthWhenDisabled(t *testing.T) {
	d := &pipefake.Dialer{Serve: func(up net.Conn) {
		defer up.Close()
		_, _ = io.Copy(io.Discard, up)
	}}
	addr := startServer(t, newPipeline(t, "", d))
	conn, br := dial(t, addr)
	// No token configured: no-auth accepted.
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatal(err)
	}
	if got := readN(t, br, 2); got[1] != 0x00 {
		t.Fatalf("method reply = %v", got)
	}
	connectReq(t, conn, "example.com", 80)
	if got := readN(t, br, 4); got[1] != 0x00 {
		t.Fatalf("reply = %v, want success", got)
	}
}

func TestCommandNotSupported(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &pipefake.Dialer{}))
	conn, br := dial(t, addr)
	if got := userpassAuth(t, conn, br, "Default.alice", "tok"); got[1] != 0x00 {
		t.Fatalf("auth reply = %v", got)
	}
	// BIND command → REP 0x07.
	if _, err := conn.Write([]byte{0x05, 0x02, 0x00, 0x01, 1, 2, 3, 4, 0, 80}); err != nil {
		t.Fatal(err)
	}
	if got := readN(t, br, 4); got[1] != 0x07 {
		t.Fatalf("reply = %v, want REP 0x07", got)
	}
}

func TestAddressTypeNotSupported(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &pipefake.Dialer{}))
	conn, br := dial(t, addr)
	if got := userpassAuth(t, conn, br, "Default.alice", "tok"); got[1] != 0x00 {
		t.Fatalf("auth reply = %v", got)
	}
	// Unknown ATYP 0x05 → REP 0x08.
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00, 0x05, 0, 80}); err != nil {
		t.Fatal(err)
	}
	if got := readN(t, br, 4); got[1] != 0x08 {
		t.Fatalf("reply = %v, want REP 0x08", got)
	}
}

func TestIPv4Target(t *testing.T) {
	d := &pipefake.Dialer{Serve: func(up net.Conn) {
		defer up.Close()
		_, _ = io.Copy(io.Discard, up)
	}}
	addr := startServer(t, newPipeline(t, "tok", d))
	conn, br := dial(t, addr)
	if got := userpassAuth(t, conn, br, "Default.bob", "tok"); got[1] != 0x00 {
		t.Fatalf("auth reply = %v", got)
	}
	// IPv4 ATYP with 1.2.3.4:5678.
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 1, 2, 3, 4, 0x16, 0x2e}); err != nil {
		t.Fatal(err)
	}
	if got := readN(t, br, 4); got[1] != 0x00 {
		t.Fatalf("reply = %v, want success", got)
	}
}

// TestRequireAuthInfo: with an empty proxy token and RequireAuthInfo set,
// the server negotiates ONLY userpass and demands non-empty credentials
// (password content unchecked — spec/resources/endpoint.md).
func TestRequireAuthInfo(t *testing.T) {
	newSrv := func() net.Addr {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		srv := New(newPipeline(t, "", &pipefake.Dialer{}))
		srv.RequireAuthInfo = true
		go srv.Serve(l)
		return l.Addr()
	}

	t.Run("no-auth offer refused", func(t *testing.T) {
		conn, br := dial(t, newSrv())
		defer conn.Close()
		if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
			t.Fatal(err)
		}
		if got := readN(t, br, 2); got[1] != 0xFF {
			t.Fatalf("method reply = %v, want 0xFF (no acceptable method)", got)
		}
	})
	t.Run("empty userpass refused", func(t *testing.T) {
		conn, br := dial(t, newSrv())
		defer conn.Close()
		if got := userpassAuth(t, conn, br, "Default.alice", ""); got[1] != 0x01 {
			t.Fatalf("auth reply = %v, want failure for empty password", got)
		}
	})
	t.Run("any non-empty userpass passes", func(t *testing.T) {
		conn, br := dial(t, newSrv())
		defer conn.Close()
		if got := userpassAuth(t, conn, br, "Default.alice", "anything"); got[1] != 0x00 {
			t.Fatalf("auth reply = %v, want success", got)
		}
		connectReq(t, conn, "example.com", 80)
		// Auth passed; the fake dialer accepts, so the tunnel opens.
		if got := readN(t, br, 4); got[1] != 0x00 {
			t.Fatalf("reply = %v, want success", got)
		}
	})
}
