//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestSourceSyncToTraffic is the black-box proof of the full node
// lifecycle — the chain that unit tests cover piecewise:
//
//	subscription fetch → parse → registry → onNodeChange → deriver →
//	view → probes → Healthy+routable → select → vmess dial → traffic
//
// All peers are local (echo server, vmess loopback), no internet needed.
func TestSourceSyncToTraffic(t *testing.T) {
	echo := startEchoServer(t)
	g := startGateway(t, "EGRESS_EGRESS_TRACE_URL="+echo.URL+"/trace")

	const uuid = "26a1d547-b031-4139-9fc5-6671e1d0408a"
	echoAddr := strings.TrimPrefix(echo.URL, "http://")
	vmessAddr := startVMessServer(t, uuid, echoAddr)

	n := g.provisionRoutableNode(t, vmessAddr, uuid, echoAddr)
	if n.EgressIP != traceIP {
		t.Fatalf("egress ip = %q, want %s", n.EgressIP, traceIP)
	}

	// Traffic through the Default platform picks the (only) healthy node:
	// egress → vmess loopback → fixed-forward → echo server.
	client := g.proxyHTTPClient("Default.alice")
	resp, err := client.Get(echo.URL + "/via-node")
	if err != nil {
		t.Fatalf("proxied GET via node: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "e2e-echo") {
		t.Fatalf("proxied GET via node = %d %q", resp.StatusCode, body)
	}

	// Deleting the source must evict the node (EvictDelay default is
	// large, but a deleted source removes its refs immediately).
	g.admin(t, http.MethodDelete, "/api/v1/sources/e2e-src", nil, http.StatusNoContent)
	waitFor(t, "node eviction after source delete", 10*time.Second, func() bool {
		return len(g.listNodes(t)) == 0
	})
}
