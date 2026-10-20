// Package sockswire is the shared SOCKS5 wire driver for tests: greeting,
// RFC1929 user/pass auth and CONNECT, with bound-address draining. It
// replaces the per-suite copies in adapters/socks5, app/conformance and
// e2e.
package sockswire

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
)

// Write sends b or fails the test.
func Write(t *testing.T, conn net.Conn, b []byte) {
	t.Helper()
	if _, err := conn.Write(b); err != nil {
		t.Fatalf("socks write: %v", err)
	}
}

// ReadN reads exactly n bytes or fails the test.
func ReadN(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	out := make([]byte, n)
	if _, err := io.ReadFull(r, out); err != nil {
		t.Fatalf("socks read: %v", err)
	}
	return out
}

// Greet sends the method-selection greeting and returns [ver, method].
func Greet(t *testing.T, conn net.Conn, br *bufio.Reader, methods ...byte) [2]byte {
	t.Helper()
	Write(t, conn, append([]byte{0x05, byte(len(methods))}, methods...))
	sel := ReadN(t, br, 2)
	return [2]byte{sel[0], sel[1]}
}

// UserPass performs RFC1929 auth and returns [ver, status].
func UserPass(t *testing.T, conn net.Conn, br *bufio.Reader, user, pass string) [2]byte {
	t.Helper()
	req := append([]byte{0x01, byte(len(user))}, user...)
	req = append(req, byte(len(pass)))
	Write(t, conn, append(req, pass...))
	rep := ReadN(t, br, 2)
	return [2]byte{rep[0], rep[1]}
}

// Connect sends CONNECT host:port and returns the 4-byte reply head with
// the bound address drained (the reader is positioned at tunnel data).
func Connect(t *testing.T, conn net.Conn, br *bufio.Reader, host string, port uint16) []byte {
	t.Helper()
	req := append([]byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}, host...)
	req = append(req, byte(port>>8), byte(port))
	Write(t, conn, req)
	return ReadReply(t, br)
}

// ReadReply reads a 4-byte reply head and drains the bound address.
func ReadReply(t *testing.T, br *bufio.Reader) []byte {
	t.Helper()
	head := ReadN(t, br, 4)
	switch head[3] {
	case 0x01:
		ReadN(t, br, 6)
	case 0x04:
		ReadN(t, br, 18)
	case 0x03:
		l := ReadN(t, br, 1)
		ReadN(t, br, int(l[0])+2)
	default:
		t.Fatalf("bad reply ATYP 0x%02x", head[3])
	}
	return head
}

// AuthAndConnect is the common chain: greet with user/pass, authenticate,
// CONNECT. It fails the test on any rejection before the CONNECT reply.
func AuthAndConnect(t *testing.T, conn net.Conn, br *bufio.Reader, user, pass, host string, port uint16) []byte {
	t.Helper()
	if sel := Greet(t, conn, br, 0x02); sel != [2]byte{0x05, 0x02} {
		t.Fatalf("method selection = %v, want [05 02]", sel)
	}
	if rep := UserPass(t, conn, br, user, pass); rep[1] != 0x00 {
		t.Fatalf("auth status = 0x%02x, want 0x00", rep[1])
	}
	return Connect(t, conn, br, host, port)
}

// Dial is a convenience net.Dial with test failure on error.
func Dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("socks dial %s: %v", addr, err)
	}
	return conn, bufio.NewReader(conn)
}

// ReplyString formats a reply head for failure messages.
func ReplyString(head []byte) string { return fmt.Sprintf("% x", head) }
