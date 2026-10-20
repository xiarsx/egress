// Package forward implements the HTTP forward-proxy ingress adapter as a
// standard http.Handler: CONNECT tunneling plus plain absolute-URI
// forwarding, both routed through the pipeline (design §4.1).
//
// Wire behavior (spec/identity.md, spec/errors.md):
// Proxy-Authorization: Basic base64("Platform.Account:TOKEN"),
// 407 + Proxy-Authenticate on missing/malformed credentials, then the
// pipeline's ProxyError → HTTP status mapping.
//
// Mounting note: http.Server parses CONNECT and absolute-URI requests
// for us; the upstream connection is dialed before hijack so dial errors
// stay mappable to HTTP error responses (same contract as reverse).
package forward

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/xiarsx/egress/internal/app/pipeline"
)

// Server is the HTTP forward-proxy ingress handler.
type Server struct {
	p *pipeline.Pipeline

	// RequireAuthInfo applies only in no-auth mode (empty proxy token):
	// when set, requests without Proxy-Authorization get 407 (the header
	// carries identity; the password is not validated). With a configured
	// token it changes nothing.
	RequireAuthInfo bool
}

// New builds a forward-proxy Server around the pipeline.
func New(p *pipeline.Pipeline) *Server { return &Server{p: p} }

// ServeHTTP implements http.Handler: CONNECT → tunnel; anything else is
// a plain forward request (absolute-URI, or origin-form + Host header).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	headers := lowerHeaders(r.Header)
	rawIdentity, token, perr := s.credentials(headers)
	if perr != nil {
		pipeline.WriteHTTPError(w, perr)
		return
	}
	if r.Method == http.MethodConnect {
		s.serveConnect(w, r, headers, rawIdentity, token)
		return
	}
	s.servePlain(w, r, headers, rawIdentity, token)
}

// credentials extracts (identity, token) from Proxy-Authorization.
// A missing header yields empty values and lets the pipeline decide
// (407 when a token is configured) — except RequireAuthInfo mode, which
// rejects it here. A malformed header is 407 when a token is configured
// and ignored otherwise.
func (s *Server) credentials(headers map[string]string) (rawIdentity, token string, perr *pipeline.ProxyError) {
	auth := headers["proxy-authorization"]
	if auth == "" {
		if s.p.ProxyToken() == "" && s.RequireAuthInfo {
			return "", "", &pipeline.ProxyError{Code: pipeline.CodeAuthRequired, Msg: "proxy authentication required"}
		}
		return "", "", nil
	}
	cred, good := parseBasicCredential(auth)
	if !good {
		if s.p.ProxyToken() != "" {
			return "", "", &pipeline.ProxyError{Code: pipeline.CodeAuthRequired, Msg: "malformed Proxy-Authorization"}
		}
		return "", "", nil // auth disabled: ignore the garbage header
	}
	rawIdentity, token, _ = splitCredential(cred)
	return rawIdentity, token, nil
}

// serveConnect handles CONNECT host:port: dial first (so dial errors stay
// mappable to 502/504 per spec/errors.md), then 200, then relay.
func (s *Server) serveConnect(w http.ResponseWriter, r *http.Request,
	headers map[string]string, rawIdentity, token string) {
	target := r.Host
	if _, _, err := net.SplitHostPort(target); err != nil {
		pipeline.WriteHTTPError(w, &pipeline.ProxyError{Code: pipeline.CodeInvalidHost, Msg: "CONNECT target must be host:port"})
		return
	}
	req := pipeline.Request{
		Token:       token,
		RawIdentity: rawIdentity,
		Target:      target,
		Headers:     headers,
		Proto:       pipeline.ProtoHTTPForward,
		RemoteAddr:  r.RemoteAddr,
	}
	prep, perr := s.p.Prepare(r.Context(), req)
	if perr != nil {
		pipeline.WriteHTTPError(w, perr)
		return
	}
	conn, rw, err := hijack(w)
	if err != nil {
		_ = prep.Upstream.Close()
		pipeline.WriteHTTPError(w, &pipeline.ProxyError{Code: pipeline.CodeInternalError, Msg: "hijack unsupported"})
		return
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = prep.Upstream.Close()
		return
	}
	_ = s.p.Relay(r.Context(), prep, &readWrapConn{Conn: conn, r: io.MultiReader(rw.Reader, conn)}, req)
}

// servePlain handles a plain (non-CONNECT) request: the re-serialized
// head goes upstream first, then the raw relay streams the response back.
func (s *Server) servePlain(w http.ResponseWriter, r *http.Request,
	headers map[string]string, rawIdentity, token string) {
	scheme, hostport, perr := forwardTarget(r, headers)
	if perr != nil {
		pipeline.WriteHTTPError(w, perr)
		return
	}
	req := pipeline.Request{
		Token:       token,
		RawIdentity: rawIdentity,
		Target:      hostport,
		Headers:     headers,
		Proto:       pipeline.ProtoHTTPForward,
		RemoteAddr:  r.RemoteAddr,
		Head:        buildHead(r, scheme),
		Scheme:      scheme,
	}
	prep, perr := s.p.Prepare(r.Context(), req)
	if perr != nil {
		pipeline.WriteHTTPError(w, perr)
		return
	}
	// Write the head BEFORE hijack: a dead upstream is still mappable to
	// 502 UPSTREAM_REQUEST_FAILED here (spec/vectors/error_scenarios.yaml).
	if _, perr := s.p.WriteHead(r.Context(), prep, req); perr != nil {
		_ = prep.Upstream.Close()
		pipeline.WriteHTTPError(w, perr)
		return
	}
	conn, rw, err := hijack(w)
	if err != nil {
		_ = prep.Upstream.Close()
		pipeline.WriteHTTPError(w, &pipeline.ProxyError{Code: pipeline.CodeInternalError, Msg: "hijack unsupported"})
		return
	}
	defer conn.Close()
	// Replay bytes the server may have buffered (request body) before the
	// live connection.
	_ = s.p.Relay(r.Context(), prep, &readWrapConn{Conn: conn, r: io.MultiReader(rw.Reader, conn)}, req)
}

// forwardTarget resolves the upstream scheme and host:port of a plain
// forward request.
func forwardTarget(r *http.Request, headers map[string]string) (scheme, hostport string, perr *pipeline.ProxyError) {
	if r.URL.IsAbs() {
		scheme = strings.ToLower(r.URL.Scheme)
		if scheme != "http" && scheme != "https" {
			return "", "", &pipeline.ProxyError{Code: pipeline.CodeInvalidProtocol, Msg: "scheme must be http or https"}
		}
		if r.URL.Hostname() == "" {
			return "", "", &pipeline.ProxyError{Code: pipeline.CodeInvalidHost, Msg: "invalid absolute URI"}
		}
		port := r.URL.Port()
		if port == "" {
			port = "80"
			if scheme == "https" {
				port = "443"
			}
		}
		return scheme, net.JoinHostPort(r.URL.Hostname(), port), nil
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

// strippedForwardHeaders never travels upstream: proxy-chain credentials,
// hop-by-hop framing and identity-disclosing headers.
var strippedForwardHeaders = []string{
	"Proxy-Authorization",
	"Proxy-Connection",
	"X-Egress-Account",
	"Forwarded",
	"X-Forwarded-For",
	"X-Forwarded-Host",
	"X-Forwarded-Proto",
	"Via",
	"X-Real-IP",
}

func strippedHeader(name string) bool {
	for _, s := range strippedForwardHeaders {
		if strings.EqualFold(s, name) {
			return true
		}
	}
	return false
}

// buildHead re-serializes the request for upstream: origin-form path,
// end-to-end headers, framing headers. The body streams through the
// relay untouched.
func buildHead(r *http.Request, scheme string) []byte {
	path := r.URL.RequestURI() // origin-form path?query (never the absolute URI)
	if path == "" {
		path = "/"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s HTTP/1.1\r\n", r.Method, path)
	sb.WriteString("Host: " + r.Host + "\r\n")
	for name, vv := range r.Header {
		if strippedHeader(name) {
			continue
		}
		for _, v := range vv {
			fmt.Fprintf(&sb, "%s: %s\r\n", name, v)
		}
	}
	// Framing: net/http strips these from r.Header.
	if r.ContentLength > 0 {
		fmt.Fprintf(&sb, "Content-Length: %d\r\n", r.ContentLength)
	}
	for _, enc := range r.TransferEncoding {
		fmt.Fprintf(&sb, "Transfer-Encoding: %s\r\n", enc)
	}
	sb.WriteString("\r\n")
	return []byte(sb.String())
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
// case-insensitive).
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

// hijack takes over the client connection.
func hijack(w http.ResponseWriter) (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("hijack unsupported")
	}
	return hj.Hijack()
}

// readWrapConn replays hijack-buffered bytes before the live conn.
type readWrapConn struct {
	net.Conn
	r io.Reader
}

func (w *readWrapConn) Read(b []byte) (int, error) { return w.r.Read(b) }
