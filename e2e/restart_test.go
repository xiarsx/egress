//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestRestartPersistence kills the gateway (crash semantics — Windows
// cannot deliver SIGTERM to a child) and restarts it on the same dirs:
// imported nodes must replay from the cache store and request logs from
// the state DB. Lease flush is graceful-shutdown-only and is covered by
// unit tests, not asserted here.
func TestRestartPersistence(t *testing.T) {
	echo := startEchoServer(t)
	// 1s cache flush: the crash must not lose the freshly synced inventory.
	g := startGateway(t,
		"EGRESS_EGRESS_TRACE_URL="+echo.URL+"/trace",
		"EGRESS_CACHE_FLUSH_INTERVAL=1s",
		"EGRESS_REQLOG_FLUSH_INTERVAL=500ms",
	)

	const uuid = "26a1d547-b031-4139-9fc5-6671e1d0408a"
	echoAddr := strings.TrimPrefix(echo.URL, "http://")
	vmessAddr := startVMessServer(t, uuid, echoAddr)
	n := g.provisionRoutableNode(t, vmessAddr, uuid, echoAddr)

	// One proxied request so there is a log row to survive the crash.
	client := g.proxyHTTPClient("Default.alice")
	resp, err := client.Get(echo.URL + "/")
	if err != nil {
		t.Fatalf("proxied GET: %v", err)
	}
	resp.Body.Close()

	// Give the 1s flush ticker a full beat to commit the staged inventory
	// before the crash (weak persistence: only flushed state replays).
	time.Sleep(1500 * time.Millisecond)
	g.stop(t)

	// Restart the same dirs with a fresh process on a new port.
	port := freePort(t)
	g2 := &gateway{
		adminToken: g.adminToken, proxyToken: g.proxyToken,
		stateDir: g.stateDir, cacheDir: g.cacheDir, logDir: g.logDir,
		base: fmt.Sprintf("http://127.0.0.1:%d", port),
	}
	g2.cmd = exec.Command(binPath, "serve")
	g2.cmd.Env = append(cmdEnv(g, port),
		"EGRESS_EGRESS_TRACE_URL="+echo.URL+"/trace",
		"EGRESS_CACHE_FLUSH_INTERVAL=1s",
		"EGRESS_REQLOG_FLUSH_INTERVAL=500ms",
	)
	g2.cmd.Stdout = &lockedWriter{&g2.mu, &g2.log}
	g2.cmd.Stderr = &lockedWriter{&g2.mu, &g2.log}
	if err := g2.cmd.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	t.Cleanup(func() {
		_ = g2.cmd.Process.Kill()
		_ = g2.cmd.Wait()
		if t.Failed() {
			g2.dumpLog(t)
		}
	})
	g2.waitReady(t)

	// Nodes replay from the cache (no new sync needed).
	waitFor(t, "node replay from cache", 10*time.Second, func() bool {
		for _, got := range g2.listNodes(t) {
			if got.Hash == n.Hash {
				return true
			}
		}
		return false
	})

	// Request logs survive the crash (sqlite commits per insert).
	raw := g2.admin(t, http.MethodGet, "/api/v1/request-logs?limit=50", nil, http.StatusOK)
	var page struct {
		Logs []struct {
			Platform string `json:"platform"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatalf("parse logs: %v", err)
	}
	found := false
	for _, l := range page.Logs {
		if l.Platform == "Default" {
			found = true
		}
	}
	if !found {
		t.Fatal("Default request-log row did not survive restart")
	}
}

// cmdEnv renders the standard gateway env for a restart on a new port,
// inheriting the parent environment (Windows needs SystemRoot et al).
func cmdEnv(g *gateway, port int) []string {
	return append(os.Environ(),
		"EGRESS_ADMIN_TOKEN="+g.adminToken,
		"EGRESS_PROXY_TOKEN="+g.proxyToken,
		"EGRESS_STATE_DIR="+g.stateDir,
		"EGRESS_CACHE_DIR="+g.cacheDir,
		"EGRESS_LOG_DIR="+g.logDir,
		fmt.Sprintf("EGRESS_PORT=%d", port),
		"EGRESS_LOG_LEVEL=debug",
	)
}
