package reverse

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }

type fakeResolver struct{ id string }

func (f *fakeResolver) ResolvePlatform(name string) (string, bool) {
	if name == "Default" {
		return f.id, true
	}
	return "", false
}
func (f *fakeResolver) PolicyFor(string) *identity.Policy { return nil }

type fakeBus struct{}

func (b *fakeBus) Emit(pksignal.Signal)              {}
func (b *fakeBus) Subscribe() <-chan pksignal.Signal { return nil }

type fakeDialer struct {
	mu    sync.Mutex
	err   error
	serve func(net.Conn)
}

func (f *fakeDialer) DialContext(ctx context.Context, h pknode.Hash, network, address string) (net.Conn, error) {
	if f.err != nil {
		return nil, f.err
	}
	up, peer := net.Pipe()
	go func() {
		if f.serve != nil {
			f.serve(peer)
		} else {
			_, _ = io.Copy(io.Discard, peer)
		}
	}()
	return up, nil
}

type fakeDirect struct{}

func (f *fakeDirect) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return nil, errors.New("no direct")
}

var (
	testHash   = pknode.Hash{5, 5, 5}
	testPlatID = "test-platform-id"
)

func newPipeline(t *testing.T, token string, d *fakeDialer) *pipeline.Pipeline {
	t.Helper()
	snap := pknode.NodeSnapshot{Hash: testHash, EgressIP: netip.MustParseAddr("198.51.100.5"), HasOutbound: true, Health: pknode.HealthyHealth(0)}
	v := view.Derive([]pknode.NodeSnapshot{snap}, nil)
	return pipeline.New(pipeline.Deps{
		Resolver:   &fakeResolver{id: testPlatID},
		NodeDialer: d,
		Direct:     &fakeDirect{},
		Bus:        &fakeBus{},
		Clock:      &fakeClock{t: time.Now()},
		Leases:     lease.NewLeaseTable(),
		Views:      func(string) *view.View { return v },
		Nodes: func(h pknode.Hash) (pknode.NodeSnapshot, bool) {
			if h == testHash {
				return snap, true
			}
			return pknode.NodeSnapshot{}, false
		},
		Latency:    func(pknode.Hash, string) (time.Duration, bool) { return 0, false },
		IPLoad:     func(string, netip.Addr) int64 { return 0 },
		PickPolicy: pick.Policy{Allocation: pick.AllocationBalanced},
		Rand:       rand.New(rand.NewPCG(11, 11)),
		ProxyToken: token,
	})
}

func TestParsePath(t *testing.T) {
	h := New(newPipeline(t, "tok", &fakeDialer{}))
	tests := []struct {
		name     string
		path     string // as r.URL.EscapedPath() would return it
		resinErr string // "" = expect success
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
			if tc.resinErr == "" {
				if perr != nil {
					t.Fatalf("perr = %v, want success", perr)
				}
				if pp == nil {
					t.Fatal("nil parsed path")
				}
				return
			}
			if perr == nil || perr.Code != tc.resinErr {
				t.Fatalf("perr = %v, want %s", perr, tc.resinErr)
			}
		})
	}
}

func TestParsePathIdentitySplit(t *testing.T) {
	h := New(newPipeline(t, "tok", &fakeDialer{}))
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
	h := New(newPipeline(t, "tok", &fakeDialer{}))
	tests := []struct {
		name     string
		path     string
		status   int
		resinErr string
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
			if got := rec.Header().Get("X-Resin-Error"); got != tc.resinErr {
				t.Fatalf("X-Resin-Error = %q, want %q", got, tc.resinErr)
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
	d := &fakeDialer{err: errors.New("connection refused")}
	h := New(newPipeline(t, "tok", d))
	req := httptest.NewRequest("GET", "/tok/Default.alice/HTTP/example.com/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 502 || rec.Header().Get("X-Resin-Error") != "UPSTREAM_CONNECT_FAILED" {
		t.Fatalf("got %d %q", rec.Code, rec.Header().Get("X-Resin-Error"))
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
	d := &fakeDialer{serve: func(up net.Conn) {
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
		"X-Resin-Account: bob\r\n" +
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
		if strings.Contains(strings.ToLower(hs), "x-resin-account") {
			t.Fatalf("X-Resin-Account leaked upstream: %q", hs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never got the head")
	}
}

func TestReverseDialFailure(t *testing.T) {
	d := &fakeDialer{err: errors.New("connection refused")}
	h := New(newPipeline(t, "tok", d))
	req := httptest.NewRequest("GET", "/tok/Default.alice/http/example.com/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 502 || rec.Header().Get("X-Resin-Error") != "UPSTREAM_CONNECT_FAILED" {
		t.Fatalf("got %d %q", rec.Code, rec.Header().Get("X-Resin-Error"))
	}
}

func TestReversePlatformNotFound(t *testing.T) {
	h := New(newPipeline(t, "tok", &fakeDialer{}))
	req := httptest.NewRequest("GET", "/tok/Nope.alice/http/example.com/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 404 || rec.Header().Get("X-Resin-Error") != "PLATFORM_NOT_FOUND" {
		t.Fatalf("got %d %q", rec.Code, rec.Header().Get("X-Resin-Error"))
	}
}
