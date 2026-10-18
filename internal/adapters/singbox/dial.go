package singbox

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// ErrUnsupportedOutbound is returned when dialing through an outbound type
// that has no native dial implementation in this round.
//
// Supported natively: direct, http, https, socks, socks5, socks5h.
// TODO: vmess, vless, trojan, shadowsocks, hysteria2 (parse keeps them,
// dial fails closed with this sentinel — never a silent direct dial).
var ErrUnsupportedOutbound = errors.New("singbox: unsupported outbound type for native dialing")

var (
	errNodeNotFound = errors.New("singbox: node not found")
	errNoOutbound   = errors.New("singbox: node has no built outbound")
)

// dialOutbound is a built, dialable outbound. Implementations hold no
// persistent connections today; Close exists so Remove can drop future
// stateful outbounds (sessions, listeners).
type dialOutbound interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
	Close() error
}

// rawOutboundOptions is the subset of sing-box outbound fields the native
// dialer reads. Field names follow the sing-box schema, which is what the
// parser emits.
type rawOutboundOptions struct {
	Type       string `json:"type"`
	Server     string `json:"server"`
	ServerPort uint64 `json:"server_port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	TLS        struct {
		Enabled    bool     `json:"enabled"`
		ServerName string   `json:"server_name"`
		Insecure   bool     `json:"insecure"`
		ALPN       []string `json:"alpn"`
	} `json:"tls"`
}

// buildDialOutbound builds a dialable outbound from canonical raw options.
// It is deterministic: the same input always yields the same outcome, so
// callers may cache both successes and errors.
func buildDialOutbound(raw []byte) (dialOutbound, error) {
	var ro rawOutboundOptions
	if err := json.Unmarshal(raw, &ro); err != nil {
		return nil, fmt.Errorf("singbox: decode outbound options: %w", err)
	}
	typ := strings.ToLower(strings.TrimSpace(ro.Type))
	switch typ {
	case "direct":
		return directOutbound{}, nil
	case "http":
		return newHTTPOutbound(ro, ro.TLS.Enabled)
	case "https":
		return newHTTPOutbound(ro, true)
	case "socks", "socks5", "socks5h":
		return newSocks5Outbound(ro, typ)
	default:
		if typ == "" {
			return nil, fmt.Errorf("%w: empty type", ErrUnsupportedOutbound)
		}
		// TODO: native vmess / vless / trojan / shadowsocks / hysteria2
		// (and wireguard / tuic / anytls / ...) dialing.
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedOutbound, typ)
	}
}

func proxyAddr(ro rawOutboundOptions) (string, error) {
	server := strings.TrimSpace(ro.Server)
	if server == "" {
		return "", errors.New("singbox: empty proxy server")
	}
	if ro.ServerPort == 0 || ro.ServerPort > 65535 {
		return "", fmt.Errorf("singbox: bad proxy port %d", ro.ServerPort)
	}
	return net.JoinHostPort(server, strconv.FormatUint(ro.ServerPort, 10)), nil
}

// ---------------------------------------------------------------------------
// direct
// ---------------------------------------------------------------------------

type directOutbound struct{}

func (directOutbound) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func (directOutbound) Close() error { return nil }

// ---------------------------------------------------------------------------
// http / https (CONNECT)
// ---------------------------------------------------------------------------

type httpOutbound struct {
	addr       string
	username   string
	password   string
	useTLS     bool
	serverName string
	insecure   bool
	alpn       []string
}

func newHTTPOutbound(ro rawOutboundOptions, useTLS bool) (*httpOutbound, error) {
	addr, err := proxyAddr(ro)
	if err != nil {
		return nil, err
	}
	serverName := strings.TrimSpace(ro.TLS.ServerName)
	if serverName == "" {
		serverName = strings.TrimSpace(ro.Server)
	}
	return &httpOutbound{
		addr:       addr,
		username:   ro.Username,
		password:   ro.Password,
		useTLS:     useTLS,
		serverName: serverName,
		insecure:   ro.TLS.Insecure,
		alpn:       ro.TLS.ALPN,
	}, nil
}

func (o *httpOutbound) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if !isTCPNetwork(network) {
		return nil, fmt.Errorf("singbox: http outbound does not support network %q", network)
	}
	conn, err := dialWithDeadline(ctx, "tcp", o.addr)
	if err != nil {
		return nil, err
	}
	if o.useTLS {
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName:         o.serverName,
			InsecureSkipVerify: o.insecure,
			NextProtos:         o.alpn,
		})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("singbox: https proxy tls handshake: %w", err)
		}
		conn = tlsConn
	}

	var sb strings.Builder
	sb.WriteString("CONNECT " + address + " HTTP/1.1\r\nHost: " + address + "\r\n")
	if o.username != "" {
		cred := base64.StdEncoding.EncodeToString([]byte(o.username + ":" + o.password))
		sb.WriteString("Proxy-Authorization: Basic " + cred + "\r\n")
	}
	sb.WriteString("\r\n")
	if _, err := io.WriteString(conn, sb.String()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("singbox: http proxy write CONNECT: %w", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("singbox: http proxy read CONNECT response: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		conn.Close()
		return nil, fmt.Errorf("singbox: http proxy CONNECT %s -> %s", address, resp.Status)
	}
	// The buffered reader may hold bytes already read past the headers;
	// keep them visible on Read.
	clearDeadline(conn)
	return &bufConn{Conn: conn, r: br}, nil
}

func (o *httpOutbound) Close() error { return nil }

// bufConn replays bytes buffered during a handshake before reading live.
type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// ---------------------------------------------------------------------------
// socks5 / socks5h
// ---------------------------------------------------------------------------

type socks5Outbound struct {
	addr          string
	username      string
	password      string
	remoteResolve bool // socks5h (and bare "socks"): send the domain to the proxy
}

func newSocks5Outbound(ro rawOutboundOptions, typ string) (*socks5Outbound, error) {
	addr, err := proxyAddr(ro)
	if err != nil {
		return nil, err
	}
	return &socks5Outbound{
		addr:          addr,
		username:      ro.Username,
		password:      ro.Password,
		remoteResolve: typ != "socks5",
	}, nil
}

func (o *socks5Outbound) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if !isTCPNetwork(network) {
		return nil, fmt.Errorf("singbox: socks5 outbound does not support network %q", network)
	}
	conn, err := dialWithDeadline(ctx, "tcp", o.addr)
	if err != nil {
		return nil, err
	}
	if err := o.handshake(conn, address); err != nil {
		conn.Close()
		return nil, err
	}
	clearDeadline(conn)
	return conn, nil
}

func (o *socks5Outbound) handshake(conn net.Conn, address string) error {
	methods := []byte{0x00}
	if o.username != "" {
		methods = append(methods, 0x02)
	}
	req := append([]byte{0x05, byte(len(methods))}, methods...)
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("singbox: socks5 greeting write: %w", err)
	}
	verMethod := make([]byte, 2)
	if _, err := io.ReadFull(conn, verMethod); err != nil {
		return fmt.Errorf("singbox: socks5 greeting read: %w", err)
	}
	if verMethod[0] != 0x05 {
		return fmt.Errorf("singbox: socks5 bad version %d", verMethod[0])
	}
	switch verMethod[1] {
	case 0x00:
	case 0x02:
		if err := o.auth(conn); err != nil {
			return err
		}
	case 0xFF:
		return errors.New("singbox: socks5 no acceptable auth method")
	default:
		return fmt.Errorf("singbox: socks5 unknown auth method %d", verMethod[1])
	}

	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("singbox: socks5 bad target %q: %w", address, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("singbox: socks5 bad target port %q", portStr)
	}

	var atyp byte
	var addrBody []byte
	if !o.remoteResolve {
		// socks5: resolve locally, fail closed on resolution failure.
		ipAddr, err := net.ResolveIPAddr("ip", host)
		if err != nil {
			return fmt.Errorf("singbox: socks5 local resolve %q: %w", host, err)
		}
		if ip4 := ipAddr.IP.To4(); ip4 != nil {
			atyp, addrBody = 0x01, []byte(ip4)
		} else {
			atyp, addrBody = 0x04, []byte(ipAddr.IP.To16())
		}
	} else {
		if len(host) > 255 {
			return fmt.Errorf("singbox: socks5 domain too long")
		}
		atyp, addrBody = 0x03, append([]byte{byte(len(host))}, host...)
	}

	creq := []byte{0x05, 0x01, 0x00, atyp}
	creq = append(creq, addrBody...)
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], uint16(port))
	creq = append(creq, portBytes[:]...)
	if _, err := conn.Write(creq); err != nil {
		return fmt.Errorf("singbox: socks5 request write: %w", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("singbox: socks5 reply read: %w", err)
	}
	if reply[0] != 0x05 {
		return fmt.Errorf("singbox: socks5 bad reply version %d", reply[0])
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("singbox: socks5 connect failed: rep=0x%02x", reply[1])
	}
	// Drain the bound address per RFC 1928.
	var bndLen int
	switch reply[3] {
	case 0x01:
		bndLen = 4
	case 0x04:
		bndLen = 16
	case 0x03:
		var l [1]byte
		if _, err := io.ReadFull(conn, l[:]); err != nil {
			return fmt.Errorf("singbox: socks5 reply addr read: %w", err)
		}
		bndLen = int(l[0])
	default:
		return fmt.Errorf("singbox: socks5 bad reply atyp %d", reply[3])
	}
	if _, err := io.CopyN(io.Discard, conn, int64(bndLen+2)); err != nil {
		return fmt.Errorf("singbox: socks5 reply addr read: %w", err)
	}
	return nil
}

func (o *socks5Outbound) auth(conn net.Conn) error {
	if len(o.username) > 255 || len(o.password) > 255 {
		return errors.New("singbox: socks5 credential too long")
	}
	req := []byte{0x01, byte(len(o.username))}
	req = append(req, o.username...)
	req = append(req, byte(len(o.password)))
	req = append(req, o.password...)
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("singbox: socks5 auth write: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("singbox: socks5 auth read: %w", err)
	}
	if resp[0] != 0x01 || resp[1] != 0x00 {
		return errors.New("singbox: socks5 auth failed")
	}
	return nil
}

func (o *socks5Outbound) Close() error { return nil }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func isTCPNetwork(network string) bool {
	switch network {
	case "tcp", "tcp4", "tcp6":
		return true
	default:
		return false
	}
}

// dialWithDeadline dials and applies the ctx deadline to the handshake so a
// stuck proxy cannot hang the dialer past cancellation.
func dialWithDeadline(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	return conn, nil
}

func clearDeadline(conn net.Conn) {
	_ = conn.SetDeadline(time.Time{})
}

// ---------------------------------------------------------------------------
// Dialer — ports.NodeDialer
// ---------------------------------------------------------------------------

// Dialer builds outbounds lazily from node raw options, caches them, and
// collapses concurrent first-use builds (single-flight). It mirrors the
// reference outbound manager's contract (internal/ports/dialer.go):
//
//   - a node without a built outbound is a dial-time error, never a
//     silent direct dial (fail-closed);
//   - Remove closes and drops the cached outbound.
//
// It is safe for concurrent use.
type Dialer struct {
	lookup func(pknode.Hash) (pknode.NodeSnapshot, bool)

	mu       sync.Mutex
	cached   map[pknode.Hash]*dialEntry
	inflight map[pknode.Hash]*dialCall
}

type dialEntry struct {
	ob  dialOutbound
	err error
}

type dialCall struct {
	done chan struct{}
	ob   dialOutbound
	err  error
}

var _ ports.NodeDialer = (*Dialer)(nil)

// NewDialer creates a Dialer. lookup resolves the current node snapshot by
// hash (typically registry.Get); it is consulted on every dial so removals
// and snapshot updates take effect without cache invalidation.
func NewDialer(lookup func(h pknode.Hash) (pknode.NodeSnapshot, bool)) *Dialer {
	return &Dialer{
		lookup:   lookup,
		cached:   make(map[pknode.Hash]*dialEntry),
		inflight: make(map[pknode.Hash]*dialCall),
	}
}

func (d *Dialer) DialContext(ctx context.Context, node pknode.Hash, network, address string) (net.Conn, error) {
	snap, ok := d.lookup(node)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errNodeNotFound, node.String())
	}
	if !snap.HasOutbound || len(snap.RawOptions) == 0 {
		// Fail closed: never silently dial direct for a node that has no
		// built outbound.
		return nil, fmt.Errorf("%w: %s", errNoOutbound, node.String())
	}
	ob, err := d.getOrBuild(node, snap.RawOptions)
	if err != nil {
		return nil, err
	}
	return ob.DialContext(ctx, network, address)
}

// Remove drops the cached outbound for h and closes it. In-flight builds
// for h complete harmlessly: the next dial re-checks lookup first, so a
// removed node can never be dialed through a stale entry.
func (d *Dialer) Remove(h pknode.Hash) {
	d.mu.Lock()
	e, ok := d.cached[h]
	delete(d.cached, h)
	d.mu.Unlock()
	if ok && e != nil && e.ob != nil {
		_ = e.ob.Close()
	}
}

// getOrBuild returns the cached outbound, building it once when missing.
// Concurrent builders for the same hash collapse onto a single build.
func (d *Dialer) getOrBuild(h pknode.Hash, raw []byte) (dialOutbound, error) {
	d.mu.Lock()
	if e, ok := d.cached[h]; ok {
		d.mu.Unlock()
		return e.ob, e.err
	}
	if c, ok := d.inflight[h]; ok {
		d.mu.Unlock()
		<-c.done
		return c.ob, c.err
	}
	c := &dialCall{done: make(chan struct{})}
	d.inflight[h] = c
	d.mu.Unlock()

	// Build outside the lock. The build is deterministic (pure JSON parse
	// + type switch), so caching the outcome — including errors — is safe:
	// the same hash always implies the same raw options.
	c.ob, c.err = buildDialOutbound(raw)

	d.mu.Lock()
	delete(d.inflight, h)
	d.cached[h] = &dialEntry{ob: c.ob, err: c.err}
	d.mu.Unlock()
	close(c.done)
	return c.ob, c.err
}
