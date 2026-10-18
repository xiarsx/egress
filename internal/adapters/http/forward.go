// Package forward implements the HTTP forward-proxy ingress adapter:
// one raw TCP listener, CONNECT tunneling plus plain absolute-URI
// forwarding, both routed through the pipeline (design §4.1).
//
// Wire behavior mirrors the reference forward proxy (spec/identity.md,
// spec/errors.md): Proxy-Authorization: Basic base64("Platform.Account:TOKEN"),
// 407 + Proxy-Authenticate on missing/malformed credentials, then the
// pipeline's ProxyError → HTTP status mapping.
package forward

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/xiarsx/egress/internal/app/pipeline"
)

// maxHeadBytes caps the request line + headers the adapter buffers.
const maxHeadBytes = 1 << 20

// handshakeReadTimeout bounds the initial request-head read (slowloris guard).
const handshakeReadTimeout = 30 * time.Second

// Server is the HTTP forward-proxy ingress.
type Server struct {
	p *pipeline.Pipeline
}

// New builds a forward-proxy Server around the pipeline.
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
	remote := conn.RemoteAddr().String()

	_ = conn.SetReadDeadline(time.Now().Add(handshakeReadTimeout))
	br := bufio.NewReaderSize(conn, 64*1024)
	head, method, target, hdr, err := readRequestHead(br)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return // malformed head: nothing sane to answer with
	}
	headers := lowerHeaders(hdr)

	rawIdentity, token, authOK := s.credentials(headers)
	if !authOK {
		writeProxyError(conn, &pipeline.ProxyError{Code: pipeline.CodeAuthRequired, Msg: "proxy authentication required"})
		return
	}

	if method == "CONNECT" {
		s.serveConnect(ctx, conn, remote, headers, rawIdentity, token, target)
		return
	}
	s.servePlain(ctx, conn, br, remote, headers, rawIdentity, token, head, target)
}

// credentials extracts (identity, token) from Proxy-Authorization.
// ok=false means the header was present but malformed and auth is
// required → the caller answers 407. A missing header yields empty
// values and lets the pipeline decide (407 when a token is configured).
func (s *Server) credentials(headers map[string]string) (rawIdentity, token string, ok bool) {
	auth := headers["proxy-authorization"]
	if auth == "" {
		return "", "", true
	}
	cred, good := parseBasicCredential(auth)
	if !good {
		if s.p.ProxyToken() != "" {
			return "", "", false
		}
		return "", "", true // auth disabled: ignore the garbage header
	}
	return splitCredential(cred)
}

// serveConnect handles CONNECT host:port: dial first (so dial errors stay
// mappable to 502/504 per spec/errors.md), then 200, then relay.
func (s *Server) serveConnect(ctx context.Context, conn net.Conn, remote string,
	headers map[string]string, rawIdentity, token, target string) {
	if _, _, err := net.SplitHostPort(target); err != nil {
		writeProxyError(conn, &pipeline.ProxyError{Code: pipeline.CodeInvalidHost, Msg: "CONNECT target must be host:port"})
		return
	}
	prep, perr := s.p.Prepare(ctx, pipeline.Request{
		Token:       token,
		RawIdentity: rawIdentity,
		Target:      target,
		Headers:     headers,
		Proto:       pipeline.ProtoHTTPForward,
		RemoteAddr:  remote,
	})
	if perr != nil {
		writeProxyError(conn, perr)
		return
	}
	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = prep.Upstream.Close()
		return
	}
	_ = s.p.Relay(ctx, prep, readWrapped(conn))
}

// servePlain handles a plain (non-CONNECT) request: the already-read head
// bytes go upstream first, then the raw relay streams the response back.
func (s *Server) servePlain(ctx context.Context, conn net.Conn, br *bufio.Reader, remote string,
	headers map[string]string, rawIdentity, token string, head []byte, target string) {
	scheme, hostport, perr := parseForwardTarget(target, headers)
	if perr != nil {
		writeProxyError(conn, perr)
		return
	}
	// Preserve bytes the client pipelined behind the head.
	client := readWrappedConn(conn, br)
	if perr := s.p.Handle(ctx, client, pipeline.Request{
		Token:       token,
		RawIdentity: rawIdentity,
		Target:      hostport,
		Headers:     headers,
		Proto:       pipeline.ProtoHTTPForward,
		RemoteAddr:  remote,
		Head:        head,
		Scheme:      scheme,
	}); perr != nil {
		writeProxyError(conn, perr)
	}
}

// parseForwardTarget resolves the upstream host:port and scheme for a
// plain forward request.
func parseForwardTarget(target string, headers map[string]string) (scheme, hostport string, perr *pipeline.ProxyError) {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		u, err := url.Parse(target)
		if err != nil || u.Hostname() == "" {
			return "", "", &pipeline.ProxyError{Code: pipeline.CodeInvalidHost, Msg: "invalid absolute URI"}
		}
		scheme = strings.ToLower(u.Scheme)
		port := u.Port()
		if port == "" {
			port = "80"
			if scheme == "https" {
				port = "443"
			}
		}
		return scheme, net.JoinHostPort(u.Hostname(), port), nil
	}
	// Origin-form: fall back to the Host header (transparent-proxy style).
	host := headers["host"]
	if host == "" {
		return "", "", &pipeline.ProxyError{Code: pipeline.CodeURLParseError, Msg: "no absolute URI and no Host header"}
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "80")
	}
	return "http", host, nil
}

// readRequestHead reads the request line + headers, returning the raw
// head bytes (forwarded verbatim upstream for plain requests), the parsed
// method/target, and the header map.
func readRequestHead(br *bufio.Reader) (head []byte, method, target string, hdr http.Header, err error) {
	var buf bytes.Buffer
	line, err := readLineCapped(br, &buf)
	if err != nil {
		return nil, "", "", nil, err
	}
	parts := strings.SplitN(strings.TrimRight(string(line), "\r\n"), " ", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return nil, "", "", nil, errors.New("bad request line")
	}
	// Header lines, raw.
	for {
		l, err := readLineCapped(br, &buf)
		if err != nil {
			return nil, "", "", nil, err
		}
		if len(bytes.TrimRight(l, "\r\n")) == 0 {
			break
		}
	}
	raw := buf.Bytes()
	// Parse headers from the raw bytes (positions: first line + \n).
	headEnd := bytes.IndexByte(raw, '\n') + 1
	mime, err := textproto.NewReader(bufio.NewReader(bytes.NewReader(raw[headEnd:]))).ReadMIMEHeader()
	if err != nil {
		return nil, "", "", nil, err
	}
	return raw, parts[0], parts[1], http.Header(mime), nil
}

func readLineCapped(br *bufio.Reader, buf *bytes.Buffer) ([]byte, error) {
	line, err := br.ReadBytes('\n')
	buf.Write(line)
	if buf.Len() > maxHeadBytes {
		return nil, errors.New("request head too large")
	}
	if err != nil {
		return nil, err
	}
	return line, nil
}

func lowerHeaders(hdr http.Header) map[string]string {
	m := make(map[string]string, len(hdr))
	for k, vv := range hdr {
		if len(vv) > 0 {
			m[strings.ToLower(k)] = vv[0]
		}
	}
	return m
}

// parseBasicCredential decodes "Basic base64(...)" (scheme
// case-insensitive, mirroring the reference).
func parseBasicCredential(auth string) (string, bool) {
	parts := strings.Fields(auth)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Basic") {
		return "", false
	}
	b, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	return string(b), true
}

// splitCredential cuts the token at the rightmost ':' (the account itself
// may contain ':').
func splitCredential(cred string) (rawIdentity, token string, ok bool) {
	if i := strings.LastIndexByte(cred, ':'); i >= 0 {
		return cred[:i], cred[i+1:], true
	}
	return cred, "", true
}

// readWrappedConn replays bytes already buffered in br before reading
// from the live connection.
func readWrappedConn(conn net.Conn, br *bufio.Reader) net.Conn {
	return &readWrapConn{Conn: conn, r: io.MultiReader(br, conn)}
}

type readWrapConn struct {
	net.Conn
	r io.Reader
}

func (w *readWrapConn) Read(b []byte) (int, error) { return w.r.Read(b) }

func readWrapped(conn net.Conn) net.Conn { return &readWrapConn{Conn: conn, r: conn} }

// writeProxyError writes the spec/errors.md §1 HTTP error response.
// CodeClientCanceled is silent: the client is already gone.
func writeProxyError(conn net.Conn, perr *pipeline.ProxyError) {
	if perr == nil || perr.Code == pipeline.CodeClientCanceled {
		return
	}
	status := pipeline.HTTPStatus(perr.Code)
	var sb strings.Builder
	fmt.Fprintf(&sb, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	if perr.Code == pipeline.CodeAuthRequired {
		sb.WriteString("Proxy-Authenticate: Basic realm=\"Resin\"\r\n")
	}
	fmt.Fprintf(&sb, "X-Resin-Error: %s\r\n", perr.Code)
	sb.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	body := perr.Msg
	if body == "" {
		body = perr.Code
	}
	fmt.Fprintf(&sb, "Content-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, _ = io.WriteString(conn, sb.String())
}
