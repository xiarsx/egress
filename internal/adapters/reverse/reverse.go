// Package reverse implements the HTTP reverse-proxy ingress adapter as a
// standard http.Handler, routed through the pipeline (design §4.1).
//
// Path format (spec/identity.md §4): /TOKEN/Platform.Account/proto/host/path.
// Parsing mirrors the reference reverse proxy: r.URL.EscapedPath() is used
// so escaped delimiters (%2F) don't corrupt segment splitting, the token
// is checked before the URL shape, and an empty path is AUTH_FAILED.
//
// Proxying strategy: the request head is re-serialized (path rewritten to
// the upstream origin-form, identity-disclosing headers stripped) and
// passed to the pipeline as Head; the connection is then hijacked and the
// pipeline relays raw bytes, which streams uploads and downloads without
// buffering. Dial happens before hijack so dial errors stay mappable to
// HTTP error responses (spec/errors.md §1).
package reverse

import (
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/xiarsx/egress/internal/app/pipeline"
)

// Handler is the reverse-proxy ingress.
type Handler struct {
	p *pipeline.Pipeline
}

// New builds a reverse-proxy Handler around the pipeline.
func New(p *pipeline.Pipeline) *Handler { return &Handler{p: p} }

type parsedPath struct {
	token    string
	identity string // decoded "Platform.Account" segment
	protocol string // "http" | "https"
	host     string // decoded host segment (may carry :port)
	rest     string // still-escaped path remainder after host
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parsed, perr := h.parsePath(r.URL.EscapedPath())
	if perr != nil {
		writeHTTPError(w, perr)
		return
	}

	headers := lowerHeaders(r.Header)
	head := buildHead(r, parsed)
	prep, perr := h.p.Prepare(r.Context(), pipeline.Request{
		Token:       parsed.token,
		RawIdentity: parsed.identity,
		Target:      parsed.hostPort(),
		Headers:     headers,
		Proto:       pipeline.ProtoHTTPReverse,
		RemoteAddr:  r.RemoteAddr,
		Head:        head,
		Scheme:      parsed.protocol,
	})
	if perr != nil {
		writeHTTPError(w, perr)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = prep.Upstream.Close()
		writeHTTPError(w, &pipeline.ProxyError{Code: pipeline.CodeInternalError, Msg: "hijack unsupported"})
		return
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		_ = prep.Upstream.Close()
		return // connection state unknown; nothing can be written
	}
	defer conn.Close()
	// Replay bytes the server may have buffered (request body) before the
	// live connection.
	client := &readWrapConn{Conn: conn, r: io.MultiReader(rw.Reader, conn)}
	_ = h.p.Relay(r.Context(), prep, client)
}

// parsePath parses the V1 reverse path. rawPath must be r.URL.EscapedPath().
func (h *Handler) parsePath(rawPath string) (*parsedPath, *pipeline.ProxyError) {
	path := strings.TrimPrefix(rawPath, "/")
	if path == "" {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeAuthFailed, Msg: "empty path"}
	}
	segs := strings.SplitN(path, "/", 5) // token, identity, protocol, host, rest
	token, err := url.PathUnescape(segs[0])
	if err != nil {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeURLParseError, Msg: "bad token segment"}
	}
	// Token check precedes URL shape checks (reference order).
	if want := h.p.ProxyToken(); want != "" && subtle.ConstantTimeCompare([]byte(token), []byte(want)) != 1 {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeAuthFailed, Msg: "token mismatch"}
	}
	if len(segs) < 4 {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeURLParseError, Msg: "path needs protocol/host segments"}
	}
	identity, err := url.PathUnescape(segs[1])
	if err != nil {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeURLParseError, Msg: "bad identity segment"}
	}
	protocol, err := url.PathUnescape(segs[2])
	if err != nil {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeURLParseError, Msg: "bad protocol segment"}
	}
	protocol = strings.ToLower(protocol)
	if protocol != "http" && protocol != "https" {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeInvalidProtocol, Msg: "protocol must be http or https"}
	}
	host, err := url.PathUnescape(segs[3])
	if err != nil {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeURLParseError, Msg: "bad host segment"}
	}
	if !isValidHost(host) {
		return nil, &pipeline.ProxyError{Code: pipeline.CodeInvalidHost, Msg: "invalid host"}
	}
	rest := ""
	if len(segs) == 5 {
		rest = segs[4] // kept escaped, like the reference
	}
	return &parsedPath{token: token, identity: identity, protocol: protocol, host: host, rest: rest}, nil
}

// hostPort returns the dial target, defaulting the port by protocol.
func (pp *parsedPath) hostPort() string {
	if _, _, err := net.SplitHostPort(pp.host); err == nil {
		return pp.host
	}
	port := "80"
	if pp.protocol == "https" {
		port = "443"
	}
	return net.JoinHostPort(pp.host, port)
}

// isValidHost mirrors the reference: plain host[:port], no userinfo/path.
func isValidHost(host string) bool {
	if host == "" {
		return false
	}
	if strings.ContainsAny(host, "/ \t\n\r@") {
		return false
	}
	// Unbracketed multi-colon literals are ambiguous; require brackets.
	if strings.Count(host, ":") > 1 && !strings.HasPrefix(host, "[") {
		return false
	}
	u, err := url.Parse("http://" + host)
	if err != nil {
		return false
	}
	if u.User != nil || u.Host == "" || u.Host != host {
		return false
	}
	return u.Hostname() != ""
}

// forwardingIdentityHeaders are stripped from the upstream request
// (reference list): they would disclose proxy-chain identity.
var forwardingIdentityHeaders = []string{
	"X-Resin-Account",
	"Forwarded",
	"X-Forwarded-For",
	"X-Forwarded-Host",
	"X-Forwarded-Proto",
	"X-Forwarded-Port",
	"X-Forwarded-Server",
	"Via",
	"X-Real-IP",
	"X-Client-IP",
	"True-Client-IP",
	"CF-Connecting-IP",
	"X-ProxyUser-Ip",
}

func strippedHeader(name string) bool {
	for _, s := range forwardingIdentityHeaders {
		if strings.EqualFold(s, name) {
			return true
		}
	}
	return false
}

// buildHead re-serializes the inbound request for upstream: request line
// with the rewritten origin-form path, Host, end-to-end headers, and
// framing headers. The body itself streams through the relay untouched.
func buildHead(r *http.Request, pp *parsedPath) []byte {
	path := "/" + pp.rest
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s HTTP/1.1\r\n", r.Method, path)
	sb.WriteString("Host: " + pp.host + "\r\n")
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

// writeHTTPError writes the spec/errors.md §1 HTTP error response.
// CodeClientCanceled is silent: the client is already gone.
func writeHTTPError(w http.ResponseWriter, perr *pipeline.ProxyError) {
	if perr == nil || perr.Code == pipeline.CodeClientCanceled {
		return
	}
	status := pipeline.HTTPStatus(perr.Code)
	h := w.Header()
	h.Set("X-Resin-Error", perr.Code)
	h.Set("Content-Type", "text/plain; charset=utf-8")
	if perr.Code == pipeline.CodeAuthRequired {
		h.Set("Proxy-Authenticate", `Basic realm="Resin"`)
	}
	msg := perr.Msg
	if msg == "" {
		msg = perr.Code
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, msg)
}

// readWrapConn replays hijack-buffered bytes before the live conn.
type readWrapConn struct {
	net.Conn
	r io.Reader
}

func (w *readWrapConn) Read(b []byte) (int, error) { return w.r.Read(b) }
