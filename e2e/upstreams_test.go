//go:build e2e

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/xiarsx/egress/internal/adapters/singbox"
)

// traceIP is the public-looking egress IP the fake trace endpoint reports
// (TEST-NET ranges are rejected by node validation, so use a real-routable
// address that never actually gets contacted).
const traceIP = "93.184.216.34"

// startEchoServer serves both roles the suite needs behind one port:
//   - /trace → the egress-trace body ("ip=..."), used by ProbeEgress via
//     EGRESS_EGRESS_TRACE_URL
//   - everything else → a plain 200 echo, proving proxy traffic arrived
func startEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/trace" {
			fmt.Fprintf(w, "ip=%s\n", traceIP)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "e2e-echo")
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startVMessServer runs a genuine vmess inbound (sing-box) whose router
// forwards every connection to forwardTo. Returns the listen address.
func startVMessServer(t *testing.T, uuid, forwardTo string) string {
	t.Helper()
	addr, closeFn, err := singbox.NewVMessInbound(uuid, forwardTo)
	if err != nil {
		t.Fatalf("vmess inbound: %v", err)
	}
	t.Cleanup(func() { _ = closeFn() })
	return addr
}

// vmessNodeURL renders a vmess:// share URL the egress parser accepts
// (v2rayN JSON form, plain TCP transport).
func vmessNodeURL(t *testing.T, addr, uuid string) string {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"v": "2", "ps": "e2e-node", "add": host, "port": port,
		"id": uuid, "aid": "0", "scy": "aes-128-gcm", "net": "tcp", "tls": "",
	})
	if err != nil {
		t.Fatal(err)
	}
	return "vmess://" + base64.StdEncoding.EncodeToString(payload)
}

// newSubServer serves a base64-encoded newline-separated node list (the
// auto-detected "sub" subscription format).
func newSubServer(t *testing.T, nodeURIs ...string) *httptest.Server {
	t.Helper()
	body := ""
	for _, u := range nodeURIs {
		body += u + "\n"
	}
	enc := base64.StdEncoding.EncodeToString([]byte(body))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, enc)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// proxyHTTPClient returns a client that forwards through the gateway's
// default endpoint with the given identity credentials.
func (g *gateway) proxyHTTPClient(identity string) *http.Client {
	proxyURL := &url.URL{
		Scheme: "http",
		Host:   g.base[len("http://"):],
		User:   url.UserPassword(identity, g.proxyToken),
	}
	return &http.Client{
		// DisableKeepAlives: a request's terminal log row is emitted when
		// the tunnel closes; keep-alive would hold it open indefinitely.
		Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL), DisableKeepAlives: true},
	}
}
