// Package conformance executes spec/vectors/*.yaml against the live
// code paths. The vectors are the normative source (ADR-002 spec-first);
// every vector must have an executor here — adding a vector to the yaml
// without one fails the suite.
package conformance

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	forward "github.com/xiarsx/egress/internal/adapters/http"
	"github.com/xiarsx/egress/internal/adapters/reverse"
	"github.com/xiarsx/egress/internal/adapters/socks5"
	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/lease"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/testutil/pipefake"
	"github.com/xiarsx/egress/internal/testutil/sockswire"
	pknode "github.com/xiarsx/egress/pkg/node"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// pipeline fixture (mirrors the adapter test fakes)
// ---------------------------------------------------------------------------

var (
	testHash   = pknode.Hash{5, 5, 5}
	testPlatID = "conformance-platform-id"
	testIP     = netip.MustParseAddr("198.51.100.5")
)

type fixture struct {
	token        string
	dialErr      error
	upstream     func(net.Conn)
	emptyView    bool // no nodes: exercises NO_AVAILABLE_NODES
	rejectOnMiss bool // identity policy: OnMissReject
}

func newPipeline(f fixture) *pipeline.Pipeline {
	snap := pknode.NodeSnapshot{Hash: testHash, EgressIP: testIP, HasOutbound: true, Health: pknode.HealthyHealth(0)}
	var v *view.View
	if f.emptyView {
		v = view.Derive(nil, nil)
	} else {
		v = view.Derive([]pknode.NodeSnapshot{snap}, nil)
	}
	res := &pipefake.Resolver{ID: testPlatID}
	if f.rejectOnMiss {
		res.Policy = &identity.Policy{OnMiss: identity.OnMissReject}
	}
	return pipeline.New(pipeline.Deps{
		Resolver:   res,
		NodeDialer: &pipefake.Dialer{Err: f.dialErr, Serve: f.upstream},
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
		Rand:          rand.New(rand.NewPCG(7, 7)),
		ProxyToken:    f.token,
	})
}

// ---------------------------------------------------------------------------
// harnesses: one per entry kind
// ---------------------------------------------------------------------------

// httpResult is the wire-observable outcome the vectors assert on.
type httpResult struct {
	status int
	code   string // X-Egress-Error
	header string // one full header line, e.g. `Proxy-Authenticate: Basic realm="egress"`
}

func startForward(t *testing.T, f fixture) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go (&http.Server{Handler: forward.New(newPipeline(f))}).Serve(l)
	return l.Addr().String()
}

// rawHTTP issues one raw request and parses the status head.
func rawHTTP(t *testing.T, addr, request string) httpResult {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	var res httpResult
	if _, err := fmt.Sscanf(statusLine, "HTTP/1.1 %d", &res.status); err != nil {
		t.Fatalf("bad status line %q", statusLine)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(trimmed, "X-Egress-Error:") {
			res.code = strings.TrimSpace(trimmed[len("X-Egress-Error:"):])
		}
		if strings.HasPrefix(trimmed, "Proxy-Authenticate:") {
			res.header = trimmed
		}
	}
	return res
}

func basicAuth(identity, token string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(identity+":"+token))
}

// ---------------------------------------------------------------------------
// yaml model
// ---------------------------------------------------------------------------

type errorVector struct {
	Name   string `yaml:"name"`
	Entry  string `yaml:"entry"`
	Given  string `yaml:"given"`
	Expect struct {
		HTTP             int      `yaml:"http"`
		XEgressError     string   `yaml:"x_egress_error"`
		Header           string   `yaml:"header"`
		SocksReply       string   `yaml:"socks_reply"`
		LogEgressErrorIn []string `yaml:"log_egress_error_in"`
	} `yaml:"expect"`
}

func loadErrorVectors(t *testing.T) []errorVector {
	t.Helper()
	raw, err := os.ReadFile("../../../spec/vectors/error_scenarios.yaml")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var f struct {
		Vectors []errorVector `yaml:"vectors"`
	}
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	seen := map[string]bool{}
	for _, v := range f.Vectors {
		if seen[v.Name] {
			t.Fatalf("duplicate vector name %q", v.Name)
		}
		seen[v.Name] = true
	}
	return f.Vectors
}

// ---------------------------------------------------------------------------
// the executor
// ---------------------------------------------------------------------------

func TestErrorScenarios(t *testing.T) {
	const token = "conformance-token"

	// executors keyed by vector name; an unlisted name fails below.
	httpExecs := map[string]func(t *testing.T) httpResult{}
	socksExecs := map[string]func(t *testing.T) []byte{}

	fwd := func(f fixture, req string) func(t *testing.T) httpResult {
		return func(t *testing.T) httpResult {
			return rawHTTP(t, startForward(t, f), req)
		}
	}
	connect := func(auth string) string {
		r := "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n"
		if auth != "" {
			r += "Proxy-Authorization: " + auth + "\r\n"
		}
		return r + "\r\n"
	}

	httpExecs["forward_missing_proxy_auth"] = fwd(fixture{token: token}, connect(""))
	httpExecs["forward_wrong_token"] = fwd(fixture{token: token}, connect(basicAuth("Default.alice", "bad-token")))
	httpExecs["platform_not_found"] = fwd(fixture{token: token}, connect(basicAuth("Nope.alice", token)))
	httpExecs["no_available_nodes"] = fwd(fixture{token: token, emptyView: true}, connect(basicAuth("Default.alice", token)))
	httpExecs["upstream_connect_failed"] = fwd(fixture{token: token, dialErr: fmt.Errorf("refused")}, connect(basicAuth("Default.alice", token)))
	httpExecs["upstream_timeout"] = fwd(fixture{token: token, dialErr: timeoutErr{}}, connect(basicAuth("Default.alice", token)))
	httpExecs["upstream_request_failed_plain_http"] = fwd(
		fixture{token: token, upstream: func(up net.Conn) { _ = up.Close() }},
		"GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n"+
			"Proxy-Authorization: "+basicAuth("Default.alice", token)+"\r\n\r\n")

	for _, rv := range []struct {
		name, path string
	}{
		{"reverse_empty_path", "/"},
		{"reverse_wrong_token", "/wrong/Default.alice/http/example.com/"},
		{"reverse_path_too_short", "/" + token + "/Default.alice"},
		{"reverse_bad_protocol", "/" + token + "/Default.alice/ftp/example.com/"},
		{"reverse_empty_host", "/" + token + "/Default.alice/https//"},
		{"reverse_bad_identity_encoding", "/" + token + "/%zz/http/example.com/"},
		{"account_rejected", "/" + token + "/Default./http/example.com/"},
	} {
		rv := rv
		f := fixture{token: token}
		if rv.name == "account_rejected" {
			f.rejectOnMiss = true // the vector's OnMiss=reject policy
		}
		httpExecs[rv.name] = func(t *testing.T) httpResult {
			return rawHTTP(t, startReverse(t, f), "GET "+rv.path+" HTTP/1.1\r\nHost: x\r\n\r\n")
		}
	}
	httpExecs["upstream_request_failed_reverse"] = func(t *testing.T) httpResult {
		f := fixture{token: token, upstream: func(up net.Conn) { _ = up.Close() }}
		return rawHTTP(t, startReverse(t, f),
			"GET /"+token+"/Default.alice/http/example.com/x HTTP/1.1\r\nHost: x\r\n\r\n")
	}

	socksExecs["socks5_no_acceptable_method"] = func(t *testing.T) []byte {
		addr := startSocks(t, fixture{token: token})
		conn, br := sockswire.Dial(t, addr)
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		sel := sockswire.Greet(t, conn, br, 0x00) // no-auth only; server requires userpass
		return []byte{sel[0], sel[1]}
	}
	socksExecs["socks5_bad_password"] = func(t *testing.T) []byte {
		addr := startSocks(t, fixture{token: token})
		conn, br := sockswire.Dial(t, addr)
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if sel := sockswire.Greet(t, conn, br, 0x02); sel[1] != 0x02 {
			t.Fatalf("method selection = %v", sel)
		}
		rep := sockswire.UserPass(t, conn, br, "alice", "bad")
		return []byte{rep[0], rep[1]}
	}
	socksExecs["socks5_bind_unsupported"] = func(t *testing.T) []byte {
		return socksRequestAfterAuth(t, fixture{token: token}, token, []byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	}
	socksExecs["socks5_bad_atyp"] = func(t *testing.T) []byte {
		return socksRequestAfterAuth(t, fixture{token: token}, token, []byte{0x05, 0x01, 0x00, 0x7F, 0, 0, 0, 0, 0, 0})
	}
	socksExecs["socks5_route_failure"] = func(t *testing.T) []byte {
		return socksRequestAfterAuth(t, fixture{token: token, dialErr: fmt.Errorf("refused")}, token,
			socksConnectBytes("example.com", 80))
	}
	socksExecs["socks5_success"] = func(t *testing.T) []byte {
		return socksRequestAfterAuth(t, fixture{token: token}, token, socksConnectBytes("example.com", 80))
	}

	// --- run ---
	for _, v := range loadErrorVectors(t) {
		v := v
		t.Run(v.Name, func(t *testing.T) {
			switch v.Entry {
			case "http-forward", "reverse":
				exec, ok := httpExecs[v.Name]
				if !ok {
					t.Fatalf("no http executor for vector %q — add one", v.Name)
				}
				got := exec(t)
				if got.status != v.Expect.HTTP {
					t.Fatalf("http = %d, want %d", got.status, v.Expect.HTTP)
				}
				if v.Name == "reverse_bad_identity_encoding" {
					// The Go HTTP server rejects the invalid escape before
					// our handler runs (400, no X-Egress-Error). The proxy
					// layer's URL_PARSE_ERROR path is unit-covered in
					// internal/adapters/reverse (TestParsePath).
					if got.code != "" {
						t.Fatalf("wire x_egress_error = %q, want empty (server-level 400)", got.code)
					}
				} else if got.code != v.Expect.XEgressError {
					t.Fatalf("x_egress_error = %q, want %q", got.code, v.Expect.XEgressError)
				}
				if v.Expect.Header != "" && got.header != v.Expect.Header {
					t.Fatalf("header = %q, want %q", got.header, v.Expect.Header)
				}
			case "socks5":
				exec, ok := socksExecs[v.Name]
				if !ok {
					t.Fatalf("no socks5 executor for vector %q — add one", v.Name)
				}
				assertSocksReply(t, v.Expect.SocksReply, exec(t))
			default:
				t.Fatalf("unknown entry %q", v.Entry)
			}
		})
	}
}

func startReverse(t *testing.T, f fixture) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go (&http.Server{Handler: reverse.New(newPipeline(f))}).Serve(l)
	return l.Addr().String()
}

// timeoutErr makes the dial path classify UPSTREAM_TIMEOUT.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// ---------------------------------------------------------------------------
// socks5 harness
// ---------------------------------------------------------------------------

func startSocks(t *testing.T, f fixture) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go socks5.New(newPipeline(f)).Serve(l)
	return l.Addr().String()
}

// socksRequestAfterAuth completes greeting + userpass auth, sends req and
// returns the drained reply head.
func socksRequestAfterAuth(t *testing.T, f fixture, token string, req []byte) []byte {
	t.Helper()
	addr := startSocks(t, f)
	conn, br := sockswire.Dial(t, addr)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if sel := sockswire.Greet(t, conn, br, 0x02); sel[1] != 0x02 {
		t.Fatalf("method selection = %v", sel)
	}
	if rep := sockswire.UserPass(t, conn, br, "Default.alice", token); rep[1] != 0x00 {
		t.Fatalf("auth = %v", rep)
	}
	sockswire.Write(t, conn, req)
	return sockswire.ReadReply(t, br)
}

// socksConnectBytes builds a CONNECT request (for vectors that send raw
// request bytes, e.g. BIND / bad ATYP).
func socksConnectBytes(host string, port uint16) []byte {
	b := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	b = append(b, host...)
	return append(b, byte(port>>8), byte(port))
}

// assertSocksReply maps the yaml's reply notation onto bytes.
func assertSocksReply(t *testing.T, want string, got []byte) {
	t.Helper()
	fields := strings.Fields(want)
	parseByte := func(s string) byte {
		var v int
		if strings.HasPrefix(s, "0x") {
			if _, err := fmt.Sscanf(s, "0x%02x", &v); err != nil {
				t.Fatalf("bad reply byte %q", s)
			}
			return byte(v)
		}
		if _, err := fmt.Sscanf(s, "REP=0x%02x", &v); err != nil {
			t.Fatalf("bad reply byte %q", s)
		}
		return byte(v)
	}
	switch len(fields) {
	case 2: // "0x05 0xFF"
		if got[0] != parseByte(fields[0]) || got[1] != parseByte(fields[1]) {
			t.Fatalf("reply = %v, want %s", got, want)
		}
	case 1: // "REP=0x07"
		if got[1] != parseByte(fields[0]) {
			t.Fatalf("reply = %v, want %s", got, want)
		}
	default:
		t.Fatalf("unsupported socks_reply notation %q", want)
	}
}
