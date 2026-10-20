// Package socks5 implements the SOCKS5 ingress adapter (RFC1928 CONNECT
// only), routed through the pipeline (design §4.1).
//
// Wire behavior mirrors the reference socks5 inbound (spec/identity.md,
// spec/errors.md §2): method negotiation prefers RFC1929 username/password
// (username = "Platform.Account", password = TOKEN) whenever offered so
// the identity can be extracted; a configured token forces the userpass
// method. Password comparison is constant-time. Route/dial failures after
// the CONNECT request map to REP 0x01; the success reply is written after
// a successful dial, before relay starts.
package socks5

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/xiarsx/egress/internal/app/pipeline"
)

const (
	socks5Version = 0x05

	socks5MethodNoAuth       = 0x00
	socks5MethodUserPass     = 0x02
	socks5MethodNoAcceptable = 0xFF

	socks5CommandConnect = 0x01

	socks5AddrIPv4   = 0x01
	socks5AddrDomain = 0x03
	socks5AddrIPv6   = 0x04

	socks5ReplySucceeded          = 0x00
	socks5ReplyGeneralFailure     = 0x01
	socks5ReplyCommandUnsupported = 0x07
	socks5ReplyAddressUnsupported = 0x08

	socks5UserPassVersion = 0x01
	socks5UserPassOK      = 0x00
	socks5UserPassFail    = 0x01
)

// handshakeTimeout bounds the whole SOCKS5 handshake (mirrors the
// reference's 15s handshake phase).
const handshakeTimeout = 15 * time.Second

// Server is the SOCKS5 ingress.
type Server struct {
	p *pipeline.Pipeline

	// RequireAuthInfo applies only in no-auth mode (empty proxy token):
	// when set, the server negotiates ONLY RFC1929 userpass and demands a
	// non-empty username (identity) and password; the password content is
	// not validated. With a configured token it changes nothing.
	RequireAuthInfo bool
}

// New builds a SOCKS5 Server around the pipeline.
func New(p *pipeline.Pipeline) *Server { return &Server{p: p} }

// Serve accepts connections until the listener is closed.
func (s *Server) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		go s.serveConn(c)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	ctx := context.Background()

	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	br := bufio.NewReader(conn)
	rawIdentity, password, ok := s.handshake(conn, br)
	if !ok {
		return
	}
	target, rep, ok := readConnectRequest(br)
	if !ok {
		if rep != 0 {
			_ = writeReply(conn, rep, nil)
		}
		return
	}
	_ = conn.SetDeadline(time.Time{}) // handshake done; relay is conn-driven

	req := pipeline.Request{
		Token:       password,
		RawIdentity: rawIdentity,
		Target:      target,
		Proto:       pipeline.ProtoSOCKS5,
		RemoteAddr:  conn.RemoteAddr().String(),
	}
	prep, perr := s.p.Prepare(ctx, req)
	if perr != nil {
		// Route/dial failures surface as REP 0x01 (spec/errors.md §2);
		// a vanished client gets silence.
		if perr.Code != pipeline.CodeClientCanceled {
			_ = writeReply(conn, socks5ReplyGeneralFailure, nil)
		}
		return
	}
	if err := writeReply(conn, socks5ReplySucceeded, prep.Upstream.LocalAddr()); err != nil {
		_ = prep.Upstream.Close()
		return
	}
	_ = s.p.Relay(ctx, prep, &readWrapConn{Conn: conn, r: io.MultiReader(br, conn)}, req)
}

// handshake performs greeting + method negotiation + optional RFC1929
// auth. It returns the wire identity and password.
func (s *Server) handshake(conn net.Conn, br *bufio.Reader) (rawIdentity, password string, ok bool) {
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil || hdr[0] != socks5Version {
		return "", "", false
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(br, methods); err != nil {
		return "", "", false
	}

	token := s.p.ProxyToken()
	hasUserPass, hasNoAuth := false, false
	for _, m := range methods {
		switch m {
		case socks5MethodUserPass:
			hasUserPass = true
		case socks5MethodNoAuth:
			hasNoAuth = true
		}
	}
	var selected byte
	switch {
	case token != "":
		// Auth required: only userpass is acceptable.
		if hasUserPass {
			selected = socks5MethodUserPass
		} else {
			selected = socks5MethodNoAcceptable // → AUTH_REQUIRED
		}
	case s.RequireAuthInfo:
		// No-auth mode with forced auth info: only userpass is acceptable.
		if hasUserPass {
			selected = socks5MethodUserPass
		} else {
			selected = socks5MethodNoAcceptable // → AUTH_REQUIRED
		}
	default:
		// Auth disabled: prefer userpass when offered so the identity
		// can still be extracted (password unchecked), else no-auth.
		switch {
		case hasUserPass:
			selected = socks5MethodUserPass
		case hasNoAuth:
			selected = socks5MethodNoAuth
		default:
			selected = socks5MethodNoAcceptable
		}
	}
	if _, err := conn.Write([]byte{socks5Version, selected}); err != nil {
		return "", "", false
	}
	if selected == socks5MethodNoAcceptable {
		return "", "", false
	}
	if selected == socks5MethodNoAuth {
		return "", "", true
	}
	return s.authenticateUserPass(conn, br, token)
}

func (s *Server) authenticateUserPass(conn net.Conn, br *bufio.Reader, token string) (string, string, bool) {
	fail := func() (string, string, bool) {
		_, _ = conn.Write([]byte{socks5UserPassVersion, socks5UserPassFail})
		return "", "", false
	}
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil || hdr[0] != socks5UserPassVersion {
		return fail()
	}
	username := make([]byte, hdr[1])
	if _, err := io.ReadFull(br, username); err != nil {
		return "", "", false
	}
	var plen [1]byte
	if _, err := io.ReadFull(br, plen[:]); err != nil {
		return "", "", false
	}
	password := make([]byte, plen[0])
	if _, err := io.ReadFull(br, password); err != nil {
		return "", "", false
	}
	if token != "" && subtle.ConstantTimeCompare(password, []byte(token)) != 1 {
		return fail() // → AUTH_FAILED
	}
	if token == "" && s.RequireAuthInfo && (len(username) == 0 || len(password) == 0) {
		return fail() // forced auth info must be non-empty (content unchecked)
	}
	if _, err := conn.Write([]byte{socks5UserPassVersion, socks5UserPassOK}); err != nil {
		return "", "", false
	}
	return string(username), string(password), true
}

// readConnectRequest parses the SOCKS5 request. ok=false with rep != 0
// means the caller should answer with that reply code.
func readConnectRequest(br *bufio.Reader) (target string, rep byte, ok bool) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return "", 0, false
	}
	if hdr[0] != socks5Version {
		return "", socks5ReplyGeneralFailure, false
	}
	if hdr[1] != socks5CommandConnect {
		return "", socks5ReplyCommandUnsupported, false
	}
	host, ok := readAddress(br, hdr[3])
	if !ok {
		if hdr[3] != socks5AddrIPv4 && hdr[3] != socks5AddrDomain && hdr[3] != socks5AddrIPv6 {
			return "", socks5ReplyAddressUnsupported, false
		}
		return "", socks5ReplyGeneralFailure, false
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(br, portBuf[:]); err != nil {
		return "", 0, false
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBuf[:])))), 0, true
}

func readAddress(br *bufio.Reader, atyp byte) (string, bool) {
	switch atyp {
	case socks5AddrIPv4:
		b := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(br, b); err != nil {
			return "", false
		}
		return net.IP(b).String(), true
	case socks5AddrIPv6:
		b := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(br, b); err != nil {
			return "", false
		}
		return net.IP(b).String(), true
	case socks5AddrDomain:
		var n [1]byte
		if _, err := io.ReadFull(br, n[:]); err != nil {
			return "", false
		}
		b := make([]byte, n[0])
		if _, err := io.ReadFull(br, b); err != nil {
			return "", false
		}
		return string(b), true
	default:
		return "", false
	}
}

// writeReply serializes a SOCKS5 reply, encoding the bound address when
// available (zeros otherwise).
func writeReply(w io.Writer, rep byte, addr net.Addr) error {
	atyp := byte(socks5AddrIPv4)
	hostBytes := []byte{0, 0, 0, 0}
	var port uint16
	if ta, ok := addr.(*net.TCPAddr); ok && ta != nil {
		if ip4 := ta.IP.To4(); ip4 != nil {
			hostBytes = append([]byte(nil), ip4...)
		} else if ip6 := ta.IP.To16(); ip6 != nil {
			atyp = socks5AddrIPv6
			hostBytes = append([]byte(nil), ip6...)
		}
		if ta.Port >= 0 && ta.Port <= 65535 {
			port = uint16(ta.Port)
		}
	}
	buf := []byte{socks5Version, rep, 0x00, atyp}
	buf = append(buf, hostBytes...)
	buf = append(buf, byte(port>>8), byte(port))
	_, err := w.Write(buf)
	return err
}

// readWrapConn replays handshake-buffered bytes before the live conn.
type readWrapConn struct {
	net.Conn
	r io.Reader
}

func (w *readWrapConn) Read(b []byte) (int, error) { return w.r.Read(b) }
