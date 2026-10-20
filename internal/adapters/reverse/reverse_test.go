package reverse

import (
	"bufio"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/testutil/pipefake"
	pknode "github.com/xiarsx/egress/pkg/node"
)

var (
	testHash   = pknode.Hash{5, 5, 5}
	testPlatID = "test-platform-id"
)

func newPipeline(t *testing.T, token string, d *pipefake.Dialer) *pipeline.Pipeline {
	t.Helper()
	snap := pknode.NodeSnapshot{Hash: testHash, EgressIP: netip.MustParseAddr("198.51.100.5"), HasOutbound: true, Health: pknode.HealthyHealth(0)}
	v := view.Derive([]pknode.NodeSnapshot{snap}, nil)
	return pipeline.New(pipeline.Deps{
		Resolver:   &pipefake.Resolver{ID: testPlatID},
		NodeDialer: d,
		Direct:     &pipefake.Direct{},
		Bus:        &pipefake.Bus{},
		Clock:      &pipefake.Clock{T: time.Now()},
		Leases:     lease.NewLeaseTable(),
		Views:      func(string) *view.View { return v },
		Nodes: func(h pknode.Hash) (pknode.NodeSnapshot, bool) {
			if h == testHash {
				return snap, true
			}
			return pknode.NodeSnapshot{}, false
		},
		Latency:       func(pknode.Hash, string) (time.Duration, bool) { return 0, false },
		IPLoad:        func(string, netip.Addr) int64 { return 0 },
		PickPolicyFor: func(string) pick.Policy { return pick.Policy{Allocation: pick.AllocationBalanced} },
		Rand:          rand.New(rand.NewPCG(11, 11)),
		ProxyToken:    token,
	})
}

func TestParsePath(t *testing.T) {
	h := New(newPipeline(t, "tok", &pipefake.Dialer{}))
	tests := []struct {
		name      string
		path      string // as r.URL.EscapedPath() would return it
		egressErr string // "" = expect success
	}{
		{"empty path", "/", "AUTH_FAILED"},
		{"empty path no slash", "", "AUTH_FAILED"},
		{"token only", "/tok", "URL_PARSE_ERROR"},
		{"token identity only", "/tok/Default.alice", "URL_PARSE_ERROR"},
		{"bad protocol", "/tok/Default.alice/ftp/example.com/", "INVALID_PROTOCOL"},
		{"empty host", "/tok/Default.alice/http/", "INVALID_HOST"},
		{"bad host chars", "/tok/Default.alice/http/a%20b/", "INVALID_HOST"},
		{"wrong token", "/wrong/Default.alice/http/example.com/", "AUTH_FAILED"},
		{"bad token escape", "/%zz/Default.alice/http/example.com/", "URL_PARSE_ERROR"},
		{"bad identity escape", "/tok/%zz/http/example.com/", "URL_PARSE_ERROR"},
		{"uppercase protocol", "/tok/Default.alice/HTTP/example.com/", ""},
		{"host with port", "/tok/Default.alice/http/example.com:8443/a", ""},
		{"ipv6 host", "/tok/Default.alice/http/[::1]:8080/a", ""},
		{"bare ipv6 rejected", "/tok/Default.alice/http/::1/a", "INVALID_HOST"},
		{"ok with rest", "/tok/Default.alice/http/example.com/v1/a%2Fb", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pp, perr := h.parsePath(tc.path)
			if tc.egressErr == "" {
				if perr != nil {
					t.Fatalf("perr = %v, want success", perr)
				}
				if pp == nil {
					t.Fatal("nil parsed path")
				}
				return
			}
			if perr == nil || perr.Code != tc.egressErr {
				t.Fatalf("perr = %v, want %s", perr, tc.egressErr)
			}
		})
	}
}

func TestParsePathIdentitySplit(t *testing.T) {
	h := New(newPipeline(t, "tok", &pipefake.Dialer{}))
	// Escaped identity segment decodes before splitting (spec vector).
	pp, perr := h.parsePath("/tok/MyHub.ops%2Fprod%3Feu%231/http/example.com/")
	if perr != nil {
		t.Fatalf("perr = %v", perr)
	}
	if pp.identity != "MyHub.ops/prod?eu#1" {
		t.Fatalf("identity = %q", pp.identity)
	}
}

func TestServeHTTPErrors(t *testing.T) {
	h := New(newPipeline(t, "tok", &pipefake.Dialer{}))
	tests := []struct {
		name      string
		path      string
		status    int
		egressErr string
	}{
		{"empty path", "/", 403, "AUTH_FAILED"},
		{"short path", "/tok", 400, "URL_PARSE_ERROR"},
		{"bad protocol", "/tok/Default.alice/ftp/example.com/", 400, "INVALID_PROTOCOL"},
		{"bad host", "/tok/Default.alice/http/", 400, "INVALID_HOST"},
		{"wrong token", "/wrong/Default.alice/http/example.com/", 403, "AUTH_FAILED"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if got := rec.Header().Get("X-Egress-Error"); got != tc.egressErr {
				t.Fatalf("X-Egress-Error = %q, want %q", got, tc.egressErr)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
				t.Fatalf("Content-Type = %q", ct)
			}
		})
	}
}

func TestParseUppercaseProtocol(t *testing.T) {
	// Shape-valid path with uppercase protocol: parsing must accept it;
	// the dial stage then fails (failing dialer), proving we got past parse.
	d := &pipefake.Dialer{Err: errors.New("connection refused")}
	h := New(newPipeline(t, "tok", d))
	req := httptest.NewRequest("GET", "/tok/Default.alice/HTTP/example.com/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 502 || rec.Header().Get("X-Egress-Error") != "UPSTREAM_CONNECT_FAILED" {
		t.Fatalf("got %d %q", rec.Code, rec.Header().Get("X-Egress-Error"))
	}
}

// chanListener feeds one conn to http.Serve.
type chanListener struct {
	ch   chan net.Conn
	addr net.Addr
}

func (l *chanListener) Accept() (net.Conn, error) {
	c, ok := <-l.ch
	if !ok {
		return nil, net.ErrClosed
	}
	return c, nil
}
func (l *chanListener) Close() error { return nil }
func (l *chanListener) Addr() net.Addr {
	if l.addr != nil {
		return l.addr
	}
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 80}
}

func TestReverseProxyRoundTrip(t *testing.T) {
	headCh := make(chan []byte, 1)
	d := &pipefake.Dialer{Serve: func(up net.Conn) {
		defer up.Close()
		br := bufio.NewReader(up)
		var head []byte
		for {
			l, err := br.ReadBytes('\n')
			head = append(head, l...)
			if err != nil || strings.HasSuffix(string(head), "\r\n\r\n") {
				break
			}
		}
		headCh <- head
		_, _ = io.WriteString(up, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
	}}
	h := New(newPipeline(t, "tok", d))

	sc, cc := net.Pipe()
	l := &chanListener{ch: make(chan net.Conn, 1)}
	l.ch <- sc
	serveDone := make(chan struct{})
	go func() { _ = http.Serve(l, h); close(serveDone) }()
	defer func() { close(l.ch); <-serveDone }()

	// Raw client speaking to the hijacked connection.
	raw := "GET /tok/Default.alice/http/example.com/p?q=1 HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"X-Egress-Account: bob\r\n" +
		"Connection: close\r\n\r\n"
	if _, err := io.WriteString(cc, raw); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(cc), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	_ = cc.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("resp = %d %q", resp.StatusCode, body)
	}

	select {
	case head := <-headCh:
		hs := string(head)
		if !strings.HasPrefix(hs, "GET /p?q=1 HTTP/1.1\r\n") {
			t.Fatalf("request line = %q", strings.SplitN(hs, "\r\n", 2)[0])
		}
		if !strings.Contains(hs, "Host: example.com\r\n") {
			t.Fatalf("missing Host header in %q", hs)
		}
		if strings.Contains(strings.ToLower(hs), "x-egress-account") {
			t.Fatalf("X-Egress-Account leaked upstream: %q", hs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never got the head")
	}
}

func TestReverseDialFailure(t *testing.T) {
	d := &pipefake.Dialer{Err: errors.New("connection refused")}
	h := New(newPipeline(t, "tok", d))
	req := httptest.NewRequest("GET", "/tok/Default.alice/http/example.com/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 502 || rec.Header().Get("X-Egress-Error") != "UPSTREAM_CONNECT_FAILED" {
		t.Fatalf("got %d %q", rec.Code, rec.Header().Get("X-Egress-Error"))
	}
}

func TestReversePlatformNotFound(t *testing.T) {
	h := New(newPipeline(t, "tok", &pipefake.Dialer{}))
	req := httptest.NewRequest("GET", "/tok/Nope.alice/http/example.com/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 404 || rec.Header().Get("X-Egress-Error") != "PLATFORM_NOT_FOUND" {
		t.Fatalf("got %d %q", rec.Code, rec.Header().Get("X-Egress-Error"))
	}
}

// TestParsePath_ConformanceVectors executes the entry=reverse vectors
// from spec/vectors/identity_vectors.yaml (the spec is the source of
// truth; no hand-mirrored copies).
func TestParsePath_ConformanceVectors(t *testing.T) {
	type expect struct {
		Token    string `yaml:"token"`
		Platform string `yaml:"platform"`
		Account  string `yaml:"account"`
		Proto    string `yaml:"proto"`
		Host     string `yaml:"host"`
	}
	var f struct {
		Vectors []struct {
			Name   string `yaml:"name"`
			Entry  string `yaml:"entry"`
			Path   string `yaml:"path"`
			Expect expect `yaml:"expect"`
		}
	}
	raw, err := os.ReadFile("../../../spec/vectors/identity_vectors.yaml")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}

	h := New(newPipeline(t, "egress-123456", &pipefake.Dialer{}))
	n := 0
	for _, v := range f.Vectors {
		if v.Entry != "reverse" {
			continue
		}
		n++
		t.Run(v.Name, func(t *testing.T) {
			pp, perr := h.parsePath(v.Path)
			if perr != nil {
				t.Fatalf("parsePath(%q): %v", v.Path, perr)
			}
			platform, account := identity.SplitIdentity(pp.identity)
			if pp.token != v.Expect.Token || platform != v.Expect.Platform ||
				account != v.Expect.Account || pp.protocol != v.Expect.Proto || pp.host != v.Expect.Host {
				t.Fatalf("parsePath(%q) = (%q, %q, %q, %q, %q), want %+v",
					v.Path, pp.token, platform, account, pp.protocol, pp.host, v.Expect)
			}
		})
	}
	if n == 0 {
		t.Fatal("no reverse vectors found — spec/vectors/identity_vectors.yaml changed?")
	}
}
