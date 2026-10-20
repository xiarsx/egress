//go:build e2e

package e2e

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/xiarsx/egress/internal/testutil/sockswire"
	"time"
)

// TestSocks5ThroughVMessNode drives the SOCKS5 entry on the default
// endpoint through a provisioned vmess node to the local echo server —
// the same full-protocol path as the HTTP forward source test, entered
// via SOCKS5 instead.
func TestSocks5ThroughVMessNode(t *testing.T) {
	echo := startEchoServer(t)
	g := startGateway(t, "EGRESS_EGRESS_TRACE_URL="+echo.URL+"/trace")

	const uuid = "26a1d547-b031-4139-9fc5-6671e1d0408a"
	echoAddr := strings.TrimPrefix(echo.URL, "http://")
	vmessAddr := startVMessServer(t, uuid, echoAddr)
	g.provisionRoutableNode(t, vmessAddr, uuid, echoAddr)

	// SOCKS5 handshake with proxy credentials, CONNECT to the echo server.
	addr := strings.TrimPrefix(g.base, "http://")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)

	host, portStr, _ := net.SplitHostPort(echoAddr)
	port := 80
	if p, err := net.LookupPort("tcp", portStr); err == nil {
		port = p
	}
	head := sockswire.AuthAndConnect(t, conn, br, "Default.alice", g.proxyToken, host, uint16(port))
	if head[1] != 0x00 {
		t.Fatalf("connect REP = 0x%02x, want 0x00", head[1])
	}

	// Plain HTTP through the established tunnel.
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(io.MultiReader(br, conn))
	if err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("tunnel read: %v", err)
	}
	if !strings.Contains(string(out), "e2e-echo") {
		t.Fatalf("tunnel response = %q", out)
	}
}
