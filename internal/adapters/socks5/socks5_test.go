package socks5

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
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
	serve func(net.Conn)
}

func (f *fakeDialer) DialContext(ctx context.Context, h pknode.Hash, network, address string) (net.Conn, error) {
	if f.err != nil {
		return nil, f.err
	}
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
	testHash   = pknode.Hash{7, 7, 7}
	testPlatID = "test-platform-id"
)

func newPipeline(t *testing.T, token string, d *fakeDialer) *pipeline.Pipeline {
	t.Helper()
	snap := pknode.NodeSnapshot{Hash: testHash, EgressIP: netip.MustParseAddr("198.51.100.7"), HasOutbound: true, Health: pknode.HealthyHealth(0)}
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
		Rand:       rand.New(rand.NewPCG(3, 3)),
		ProxyToken: token,
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

func dial(t *testing.T, addr net.Addr) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, bufio.NewReader(conn)
}

func readN(t *testing.T, br *bufio.Reader, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(br, b); err != nil {
		t.Fatalf("read %d: %v", n, err)
	}
	return b
}

// userpassAuth runs greeting (offering only 0x02) + RFC1929 with the given
// credentials, returning the subnegotiation reply.
func userpassAuth(t *testing.T, conn net.Conn, br *bufio.Reader, user, pass string) []byte {
	t.Helper()
	if _, err := conn.Write([]byte{0x05, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	if got := readN(t, br, 2); got[0] != 0x05 || got[1] != 0x02 {
		t.Fatalf("method reply = %v", got)
	}
	req := []byte{0x01, byte(len(user))}
	req = append(req, user...)
	req = append(req, byte(len(pass)))
	req = append(req, pass...)
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
	return readN(t, br, 2)
}

func connectReq(t *testing.T, conn net.Conn, host string, port int) {
	t.Helper()
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, host...)
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
}

func TestMethodNoAcceptable(t *testing.T) {
	addr := startServer(t, newPipeline(t, "tok", &fakeDialer{}))
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
	addr := startServer(t, newPipeline(t, "tok", &fakeDialer{}))
	conn, br := dial(t, addr)
	if got := userpassAuth(t, conn, br, "Default.alice", "wrong"); got[1] != 0x01 {
		t.Fatalf("auth reply = %v, want failure", got)
	}
}

func TestUserPassSuccessConnectFailure(t *testing.T) {
	d := &fakeDialer{err: errors.New("connection refused")}
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
	d := &fakeDialer{serve: func(up net.Conn) {
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
	d := &fakeDialer{serve: func(up net.Conn) {
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
	addr := startServer(t, newPipeline(t, "tok", &fakeDialer{}))
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
	addr := startServer(t, newPipeline(t, "tok", &fakeDialer{}))
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
	d := &fakeDialer{serve: func(up net.Conn) {
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
