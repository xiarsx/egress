package singbox

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// ---------------------------------------------------------------------------
// test servers
// ---------------------------------------------------------------------------

// startEchoServer runs a TCP echo server; returns its address.
func startEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

// startSocks5Server runs a minimal SOCKS5 proxy (no-auth or user/pass) that
// forwards to the requested target. It reports the requested addresses.
func startSocks5Server(t *testing.T, username, password string) (addr string, requested *[]string) {
	t.Helper()
	var mu sync.Mutex
	var got []string
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if err := serveSocks5(c, username, password, func(target string) {
					mu.Lock()
					got = append(got, target)
					mu.Unlock()
				}); err != nil {
					t.Logf("socks5 server: %v", err)
				}
			}(c)
		}
	}()
	return ln.Addr().String(), &got
}

func serveSocks5(c net.Conn, username, password string, onRequest func(string)) error {
	// Greeting.
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(c, hdr); err != nil {
		return err
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(c, methods); err != nil {
		return err
	}
	method := byte(0x00)
	if username != "" {
		method = 0x02
	}
	if _, err := c.Write([]byte{0x05, method}); err != nil {
		return err
	}
	if method == 0x02 {
		ahdr := make([]byte, 2)
		if _, err := io.ReadFull(c, ahdr); err != nil {
			return err
		}
		ulen := int(ahdr[1])
		ubuf := make([]byte, ulen+1)
		if _, err := io.ReadFull(c, ubuf); err != nil {
			return err
		}
		plen := int(ubuf[ulen])
		pbuf := make([]byte, plen)
		if _, err := io.ReadFull(c, pbuf); err != nil {
			return err
		}
		if string(ubuf[:ulen]) != username || string(pbuf) != password {
			_, _ = c.Write([]byte{0x01, 0x01})
			return errors.New("bad credentials")
		}
		if _, err := c.Write([]byte{0x01, 0x00}); err != nil {
			return err
		}
	}
	// Request.
	req := make([]byte, 4)
	if _, err := io.ReadFull(c, req); err != nil {
		return err
	}
	var host string
	switch req[3] {
	case 0x01:
		ip := make([]byte, 4)
		if _, err := io.ReadFull(c, ip); err != nil {
			return err
		}
		host = net.IP(ip).String()
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return err
		}
		d := make([]byte, l[0])
		if _, err := io.ReadFull(c, d); err != nil {
			return err
		}
		host = string(d)
	case 0x04:
		ip := make([]byte, 16)
		if _, err := io.ReadFull(c, ip); err != nil {
			return err
		}
		host = net.IP(ip).String()
	default:
		return fmt.Errorf("bad atyp %d", req[3])
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(c, port); err != nil {
		return err
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(port[0])<<8|int(port[1])))
	onRequest(target)
	up, err := net.Dial("tcp", target)
	if err != nil {
		_, _ = c.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return err
	}
	defer up.Close()
	if _, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(up, c) }()
	go func() { defer wg.Done(); _, _ = io.Copy(c, up) }()
	wg.Wait()
	return nil
}

// startHTTPProxyServer runs a minimal HTTP CONNECT proxy.
func startHTTPProxyServer(t *testing.T, username, password string) (addr string, requested *[]string) {
	t.Helper()
	var mu sync.Mutex
	var got []string
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				line, err := br.ReadString('\n')
				if err != nil {
					return
				}
				parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
				if len(parts) != 3 || parts[0] != "CONNECT" {
					return
				}
				target := parts[1]
				// Drain headers.
				for {
					h, err := br.ReadString('\n')
					if err != nil || h == "\r\n" || h == "\n" {
						break
					}
				}
				mu.Lock()
				got = append(got, target)
				mu.Unlock()
				up, err := net.Dial("tcp", target)
				if err != nil {
					_, _ = io.WriteString(c, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
					return
				}
				defer up.Close()
				_, _ = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); _, _ = io.Copy(up, br) }()
				go func() { defer wg.Done(); _, _ = io.Copy(c, up) }()
				wg.Wait()
			}(c)
		}
	}()
	return ln.Addr().String(), &got
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func testLookup(nodes map[pknode.Hash]pknode.NodeSnapshot) func(pknode.Hash) (pknode.NodeSnapshot, bool) {
	return func(h pknode.Hash) (pknode.NodeSnapshot, bool) {
		n, ok := nodes[h]
		return n, ok
	}
}

func nodeWithRaw(raw string) (pknode.Hash, pknode.NodeSnapshot) {
	h := pknode.Hash{}
	copy(h[:], []byte("test-hash-123456"))
	return h, pknode.NodeSnapshot{
		Hash:        h,
		RawOptions:  []byte(raw),
		HasOutbound: true,
	}
}

func roundTrip(t *testing.T, conn net.Conn, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	_ = conn.SetDeadline(deadline)
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != msg {
		t.Fatalf("echo = %q, want %q", buf, msg)
	}
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestDialer_NodeNotFound(t *testing.T) {
	d := NewDialer(testLookup(nil))
	_, err := d.DialContext(context.Background(), pknode.Zero, "tcp", "127.0.0.1:80")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v, want node not found", err)
	}
}

func TestDialer_NoOutboundFailClosed(t *testing.T) {
	h := pknode.Zero
	cases := map[string]pknode.NodeSnapshot{
		"no flag":  {Hash: h, RawOptions: []byte(`{"type":"direct"}`)},
		"no raw":   {Hash: h, HasOutbound: true},
		"both off": {},
	}
	for name, snap := range cases {
		d := NewDialer(testLookup(map[pknode.Hash]pknode.NodeSnapshot{h: snap}))
		conn, err := d.DialContext(context.Background(), h, "tcp", "127.0.0.1:1")
		if conn != nil {
			conn.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "no built outbound") {
			t.Fatalf("%s: err = %v, want fail-closed", name, err)
		}
	}
}

func TestDialer_UnsupportedOutbound(t *testing.T) {
	for _, typ := range []string{"vmess", "vless", "trojan", "shadowsocks", "hysteria2", "wireguard"} {
		h, snap := nodeWithRaw(`{"type":"` + typ + `","server":"example.com","server_port":443}`)
		d := NewDialer(testLookup(map[pknode.Hash]pknode.NodeSnapshot{h: snap}))
		_, err := d.DialContext(context.Background(), h, "tcp", "example.com:443")
		if !errors.Is(err, ErrUnsupportedOutbound) {
			t.Fatalf("type %s: err = %v, want ErrUnsupportedOutbound", typ, err)
		}
	}
}

func TestDialer_Direct(t *testing.T) {
	echo := startEchoServer(t)
	h, snap := nodeWithRaw(`{"type":"direct"}`)
	d := NewDialer(testLookup(map[pknode.Hash]pknode.NodeSnapshot{h: snap}))
	conn, err := d.DialContext(context.Background(), h, "tcp", echo)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	roundTrip(t, conn, "hello-direct")
}

func TestDialer_Socks5(t *testing.T) {
	echo := startEchoServer(t)
	proxy, requested := startSocks5Server(t, "alice", "s3cret")
	host, port, _ := net.SplitHostPort(proxy)
	raw := `{"type":"socks","server":"` + host + `","server_port":` + port + `,"username":"alice","password":"s3cret"}`
	h, snap := nodeWithRaw(raw)
	d := NewDialer(testLookup(map[pknode.Hash]pknode.NodeSnapshot{h: snap}))
	conn, err := d.DialContext(context.Background(), h, "tcp", echo)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	roundTrip(t, conn, "hello-socks5")
	if len(*requested) != 1 || (*requested)[0] != echo {
		t.Fatalf("proxy saw requests %v, want [%s]", *requested, echo)
	}
}

func TestDialer_Socks5hRemoteResolve(t *testing.T) {
	echo := startEchoServer(t)
	proxy, requested := startSocks5Server(t, "", "")
	host, port, _ := net.SplitHostPort(proxy)
	raw := `{"type":"socks5h","server":"` + host + `","server_port":` + port + `}`
	h, snap := nodeWithRaw(raw)
	d := NewDialer(testLookup(map[pknode.Hash]pknode.NodeSnapshot{h: snap}))
	// Use a hostname so the test exercises domain (ATYP 0x03) encoding;
	// localhost must resolve to the echo server.
	_, echoPort, _ := net.SplitHostPort(echo)
	conn, err := d.DialContext(context.Background(), h, "tcp", "localhost:"+echoPort)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	roundTrip(t, conn, "hello-socks5h")
	if len(*requested) != 1 || !strings.HasSuffix((*requested)[0], ":"+echoPort) {
		t.Fatalf("proxy saw requests %v", *requested)
	}
	if !strings.HasPrefix((*requested)[0], "localhost:") {
		t.Fatalf("socks5h should send the domain, got %q", (*requested)[0])
	}
}

func TestDialer_HTTPConnect(t *testing.T) {
	echo := startEchoServer(t)
	proxy, requested := startHTTPProxyServer(t, "bob", "pw")
	host, port, _ := net.SplitHostPort(proxy)
	raw := `{"type":"http","server":"` + host + `","server_port":` + port + `,"username":"bob","password":"pw"}`
	h, snap := nodeWithRaw(raw)
	d := NewDialer(testLookup(map[pknode.Hash]pknode.NodeSnapshot{h: snap}))
	conn, err := d.DialContext(context.Background(), h, "tcp", echo)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	roundTrip(t, conn, "hello-http")
	if len(*requested) != 1 || (*requested)[0] != echo {
		t.Fatalf("proxy saw requests %v, want [%s]", *requested, echo)
	}
}

func TestDialer_CacheRemoveAndConcurrent(t *testing.T) {
	echo := startEchoServer(t)
	h, snap := nodeWithRaw(`{"type":"direct"}`)
	nodes := map[pknode.Hash]pknode.NodeSnapshot{h: snap}
	d := NewDialer(testLookup(nodes))

	// Concurrent first-use builds collapse; all dials succeed.
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := d.DialContext(context.Background(), h, "tcp", echo)
			if err == nil {
				roundTrip(t, conn, "x")
				conn.Close()
			} else {
				errs[i] = err
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
	}

	// Remove drops the cache; the next dial rebuilds transparently.
	d.Remove(h)
	conn, err := d.DialContext(context.Background(), h, "tcp", echo)
	if err != nil {
		t.Fatalf("dial after Remove: %v", err)
	}
	conn.Close()

	// Remove of an unknown hash is a no-op.
	d.Remove(pknode.Zero)

	// After the node disappears from lookup, dials fail closed even with a
	// warm cache.
	delete(nodes, h)
	if _, err := d.DialContext(context.Background(), h, "tcp", echo); err == nil {
		t.Fatal("expected error after node removal from lookup")
	}
}

func TestOutbound_DialContext(t *testing.T) {
	// The Outbound interface dials one-shot (no cache).
	echo := startEchoServer(t)
	proxy, _ := startHTTPProxyServer(t, "", "")
	host, port, _ := net.SplitHostPort(proxy)
	obs, err := ParseSubscription([]byte(
		`{"outbounds":[{"type":"http","tag":"h","server":"` + host + `","server_port":` + port + `}]}`))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := obs[0].DialContext(context.Background(), "tcp", echo)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	roundTrip(t, conn, "hello-outbound")

	// Unsupported type through the interface.
	obs, err = ParseSubscription([]byte(
		`{"outbounds":[{"type":"vmess","tag":"v","server":"example.com","server_port":443,"uuid":"26a1d547-b031-4139-9fc5-6671e1d0408a"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := obs[0].DialContext(context.Background(), "tcp", "example.com:443"); !errors.Is(err, ErrUnsupportedOutbound) {
		t.Fatalf("err = %v, want ErrUnsupportedOutbound", err)
	}
}
