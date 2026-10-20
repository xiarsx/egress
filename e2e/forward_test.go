//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func (g *gateway) proxyURL() *url.URL {
	u, _ := url.Parse(g.base)
	return u
}

// TestForwardHappyPath is the primary black-box flow: a proxied request
// through the Default platform (via a provisioned vmess node) reaches the
// local echo server, and a persisted request-log row records it.
func TestForwardHappyPath(t *testing.T) {
	echo := startEchoServer(t)
	g := startGateway(t, "EGRESS_EGRESS_TRACE_URL="+echo.URL+"/trace")

	const uuid = "26a1d547-b031-4139-9fc5-6671e1d0408a"
	echoAddr := strings.TrimPrefix(echo.URL, "http://")
	vmessAddr := startVMessServer(t, uuid, echoAddr)
	g.provisionRoutableNode(t, vmessAddr, uuid, echoAddr)

	client := g.proxyHTTPClient("Default.alice")
	resp, err := client.Get(echo.URL + "/")
	if err != nil {
		t.Fatalf("proxied GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "e2e-echo") {
		t.Fatalf("proxied GET = %d %q", resp.StatusCode, body)
	}

	// The request-log row must persist: one row per request, success rows
	// carry net_ok=true and an empty egress_error (spec/events.md §2).
	waitFor(t, "request log row", 10*time.Second, func() bool {
		raw := g.admin(t, http.MethodGet, "/api/v1/request-logs?limit=20", nil, http.StatusOK)
		var page struct {
			Logs []struct {
				Platform string `json:"platform"`
				NetOK    bool   `json:"net_ok"`
				Err      string `json:"egress_error"`
			} `json:"items"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return false
		}
		for _, l := range page.Logs {
			if l.Platform == "Default" && l.NetOK && l.Err == "" {
				return true
			}
		}
		return false
	})
}

// TestForwardAuthRejections covers the wire-visible auth failures.
func TestForwardAuthRejections(t *testing.T) {
	g := startGateway(t)
	const target = "http://example.com/"

	// No Proxy-Authorization → 407 AUTH_REQUIRED.
	bare := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(g.proxyURL())}}
	resp, err := bare.Get(target)
	if err != nil {
		t.Fatalf("bare proxied GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("no-auth = %d, want 407", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Egress-Error"); got != "AUTH_REQUIRED" {
		t.Fatalf("X-Egress-Error = %q, want AUTH_REQUIRED", got)
	}

	// Wrong token → 403 AUTH_FAILED.
	bad := g.proxyURL()
	bad.User = url.UserPassword("Direct.alice", "wrong-token")
	badClient := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(bad)}}
	resp, err = badClient.Get(target)
	if err != nil {
		t.Fatalf("bad-token proxied GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("bad-token = %d, want 403", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Egress-Error"); got != "AUTH_FAILED" {
		t.Fatalf("X-Egress-Error = %q, want AUTH_FAILED", got)
	}
}
