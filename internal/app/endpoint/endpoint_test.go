package endpoint

import (
	"bufio"
	"context"
	"fmt"
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

func testPipeline(token string) *pipeline.Pipeline {
	return pipeline.New(pipeline.Deps{
		Resolver:      &pipefake.Resolver{},
		NodeDialer:    failNodeDialer{},
		Direct:        failDialer{},
		Bus:           &pipefake.Bus{},
		Clock:         &pipefake.Clock{T: time.Now()},
		Leases:        lease.NewLeaseTable(),
		Views:         func(string) *view.View { return view.Derive(nil, nil) },
		Nodes:         func(pknode.Hash) (pknode.NodeSnapshot, bool) { return pknode.NodeSnapshot{}, false },
		Latency:       func(pknode.Hash, string) (time.Duration, bool) { return 0, false },
		IPLoad:        func(string, netip.Addr) int64 { return 0 },
		PickPolicyFor: func(string) pick.Policy { return pick.Policy{} },
		Rand:          rand.New(rand.NewPCG(1, 1)),
		ProxyToken:    token,
	})
}

// start applies cfg on a free port and returns the manager + base URL.
func start(t *testing.T, cfg Config) (*Manager, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	cfg.Name = "test"
	cfg.Addr = fmt.Sprintf("127.0.0.1:%d", port)
	mgr := New(testPipeline(""), http.NewServeMux())
	t.Cleanup(mgr.Close)
	if err := mgr.Apply(cfg); err != nil {
		t.Fatalf("apply: %v", err)
	}
	t.Cleanup(func() { mgr.Delete("test") })
	st, ok := mgr.Status("test")
	if !ok || !st.Listening {
		t.Fatalf("status = %+v ok=%v", st, ok)
	}
	return mgr, fmt.Sprintf("http://127.0.0.1:%d", port)
}

func TestHealthzAlwaysAvailable(t *testing.T) {
	_, base := start(t, Config{}) // everything disabled
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
}

func TestManagementDisabled404(t *testing.T) {
	_, base := start(t, Config{AllowManagement: false})
	resp, err := http.Get(base + "/api/v1/system/info")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("/api with allowManagement=false = %d, want 404", resp.StatusCode)
	}
	resp2, err := http.Get(base + "/ui")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("/ui with allowManagement=false = %d, want 404", resp2.StatusCode)
	}
}

func TestForwardCapabilityDisabled403(t *testing.T) {
	_, base := start(t, Config{AllowProxy: true, AllowHTTPForward: false, AllowHTTPReverse: true})
	// Absolute-URI request → forward capability → 403 ENDPOINT_CAPABILITY_DISABLED.
	req, _ := http.NewRequest("GET", "http://example.com/", nil)
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err == nil {
		resp.Body.Close()
	}
	// Go's client hides absolute-form behind the transport; issue the raw
	// request over a dialed connection instead.
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n")
	br := bufio.NewReader(conn)
	r, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	if r.StatusCode != 403 {
		t.Fatalf("status = %d (%s)", r.StatusCode, body)
	}
	if r.Header.Get("X-Egress-Error") != "ENDPOINT_CAPABILITY_DISABLED" {
		t.Fatalf("X-Egress-Error = %q", r.Header.Get("X-Egress-Error"))
	}
}

func TestReverseCapabilityDisabled403(t *testing.T) {
	_, base := start(t, Config{AllowProxy: true, AllowHTTPForward: true, AllowHTTPReverse: false})
	// Origin-form GET (no Host identity configured) → reverse capability → 403.
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /some/path HTTP/1.1\r\nHost: example.com\r\n\r\n")
	r, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 403 {
		t.Fatalf("status = %d", r.StatusCode)
	}
	if r.Header.Get("X-Egress-Error") != "ENDPOINT_CAPABILITY_DISABLED" {
		t.Fatalf("X-Egress-Error = %q", r.Header.Get("X-Egress-Error"))
	}
}

func TestSocks5Demux(t *testing.T) {
	// SOCKS5 enabled but no nodes: greeting must be accepted and the
	// request fails with rep=0x01 (failure), proving the demux reached
	// the SOCKS5 side.
	_, base := start(t, Config{AllowProxy: true, AllowSOCKS5: true, AllowHTTPForward: true})
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// greeting: ver=5, 1 method, no-auth
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("greeting reply = %v, want [5 0] (no-auth accepted)", reply)
	}
	// CONNECT to example.com:80 — pipeline resolves no platform → rep 0x01.
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len("example.com"))}
	req = append(req, "example.com"...)
	req = append(req, 0x00, 0x50)
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
	rep := make([]byte, 10)
	if _, err := io.ReadFull(conn, rep); err != nil {
		t.Fatal(err)
	}
	if rep[1] == 0x00 {
		t.Fatal("expected failure reply with no platforms")
	}
}

func TestSocks5ClosedWhenDisabled(t *testing.T) {
	_, base := start(t, Config{AllowProxy: true, AllowSOCKS5: false})
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte{0x05, 0x01, 0x00})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	one := make([]byte, 1)
	_, err = conn.Read(one)
	if err == nil {
		t.Fatal("SOCKS5-disabled endpoint must close SOCKS5 connections")
	}
}

func TestApplyFailureKeepsOldAndRecordsError(t *testing.T) {
	mgr, base := start(t, Config{AllowManagement: true})
	// Re-apply on an invalid address: the old listener must survive.
	err := mgr.Apply(Config{Name: "test", Addr: "not-an-address"})
	if err == nil {
		t.Fatal("apply to a bad address should fail")
	}
	st, _ := mgr.Status("test")
	if st.LastError == "" {
		t.Fatal("LastError not recorded")
	}
	resp, herr := http.Get(base + "/healthz")
	if herr != nil {
		t.Fatalf("old listener died: %v", herr)
	}
	resp.Body.Close()
}

// failNodeDialer / failDialer: the tested capability-gate paths never
// reach dialing; any dial is a test bug surfacing as a 502, not a hang.
type failNodeDialer struct{}

func (failNodeDialer) DialContext(context.Context, pknode.Hash, string, string) (net.Conn, error) {
	return nil, fmt.Errorf("no nodes in test")
}

type failDialer struct{}

func (failDialer) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	return nil, fmt.Errorf("no direct in test")
}
