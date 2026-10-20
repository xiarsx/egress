//go:build e2e

// Package e2e is the black-box end-to-end suite: it runs the real egress
// binary as a subprocess with real listeners and drives it over the wire
// (HTTP forward, SOCKS5, admin API). Gated behind -tags e2e so the unit
// suite stays fast:
//
//	go test -tags e2e ./e2e
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var binPath string // built once for the whole suite

func TestMain(m *testing.M) {
	if p := os.Getenv("E2E_BIN"); p != "" {
		binPath = p
	} else {
		tmp, err := os.MkdirTemp("", "egress-e2e-bin")
		if err != nil {
			fmt.Fprintln(os.Stderr, "e2e: mktemp:", err)
			os.Exit(1)
		}
		defer os.RemoveAll(tmp)
		binPath = filepath.Join(tmp, "egress-e2e.exe")
		// Build with the official tags (same shape as the shipped binary).
		build := exec.Command("go", "build", "-tags", "with_quic with_utls", "-o", binPath, "./cmd/egress")
		build.Dir = filepath.Dir(mustWd()) // repo root = parent of e2e/
		build.Stdout, build.Stderr = os.Stdout, os.Stderr
		if err := build.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: build:", err)
			os.Exit(1)
		}
		if _, err := os.Stat(binPath); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: built binary missing:", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "e2e: binary at", binPath)
	}
	os.Exit(m.Run())
}

func mustWd() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return wd
}

// gateway is a running egress subprocess with isolated dirs and port.
type gateway struct {
	base       string // http://127.0.0.1:PORT (default endpoint)
	adminToken string
	proxyToken string
	stateDir   string
	cacheDir   string
	logDir     string

	cmd *exec.Cmd
	mu  sync.Mutex
	log bytes.Buffer // combined stdout+stderr
}

func (g *gateway) dumpLog(t *testing.T) {
	g.mu.Lock()
	defer g.mu.Unlock()
	t.Logf("--- gateway log ---\n%s", g.log.String())
}

// startGateway boots the binary with the given extra env (KEY=VALUE).
func startGateway(t *testing.T, extraEnv ...string) *gateway {
	t.Helper()
	root := t.TempDir()
	g := &gateway{
		adminToken: "e2e-admin",
		proxyToken: "e2e-proxy",
		stateDir:   filepath.Join(root, "state"),
		cacheDir:   filepath.Join(root, "cache"),
		logDir:     filepath.Join(root, "logs"),
	}
	port := freePort(t)
	g.base = fmt.Sprintf("http://127.0.0.1:%d", port)

	g.cmd = exec.Command(binPath, "serve")
	g.cmd.Env = append(os.Environ(),
		"EGRESS_ADMIN_TOKEN="+g.adminToken,
		"EGRESS_PROXY_TOKEN="+g.proxyToken,
		"EGRESS_STATE_DIR="+g.stateDir,
		"EGRESS_CACHE_DIR="+g.cacheDir,
		"EGRESS_LOG_DIR="+g.logDir,
		fmt.Sprintf("EGRESS_PORT=%d", port),
		"EGRESS_LOG_LEVEL=debug",
	)
	g.cmd.Env = append(g.cmd.Env, extraEnv...)
	g.cmd.Stdout = &lockedWriter{&g.mu, &g.log}
	g.cmd.Stderr = &lockedWriter{&g.mu, &g.log}
	if err := g.cmd.Start(); err != nil {
		t.Fatalf("start egress: %v", err)
	}
	t.Cleanup(func() {
		_ = g.cmd.Process.Kill()
		_ = g.cmd.Wait()
		if t.Failed() {
			g.dumpLog(t)
		}
	})
	g.waitReady(t)
	return g
}

// stop kills the process (crash semantics; Windows cannot deliver SIGTERM
// to a child) and waits for exit.
func (g *gateway) stop(t *testing.T) {
	t.Helper()
	_ = g.cmd.Process.Kill()
	_ = g.cmd.Wait()
}

func (g *gateway) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(g.base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	g.dumpLog(t)
	t.Fatal("gateway did not become ready in 15s")
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// admin issues an authenticated admin-API call and fails on unexpected status.
func (g *gateway) admin(t *testing.T, method, path string, body any, wantStatus int) []byte {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, g.base+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+g.adminToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s = %d, want %d: %s", method, path, resp.StatusCode, wantStatus, raw)
	}
	return raw
}

// adminStatus is admin without the status assertion (for negative cases).
func (g *gateway) adminStatus(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, g.base+path, rdr)
	req.Header.Set("Authorization", "Bearer "+g.adminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// lockedWriter serializes subprocess output into the gateway log buffer.
type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// --- shared provisioning -------------------------------------------------

// node is the subset of the admin node DTO the suite asserts on.
type node struct {
	Hash        string `json:"hash"`
	Health      string `json:"health"`
	EgressIP    string `json:"egress_ip"`
	HasOutbound bool   `json:"has_outbound"`
}

func (g *gateway) listNodes(t *testing.T) []node {
	t.Helper()
	raw := g.admin(t, http.MethodGet, "/api/v1/nodes", nil, http.StatusOK)
	var resp struct {
		Nodes []node `json:"items"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("parse nodes: %v (%s)", err, raw)
	}
	return resp.Nodes
}

// provisionRoutableNode wires a full node lifecycle over the wire:
// subscription source -> sync -> manual egress+latency probes -> Healthy.
// The node's vmess inbound forwards to forwardTo (the local echo server).
func (g *gateway) provisionRoutableNode(t *testing.T, vmessAddr, uuid, forwardTo string) node {
	t.Helper()

	// Subscription endpoint serving one vmess node (base64 whole body).
	subBody := vmessNodeURL(t, vmessAddr, uuid)
	sub := newSubServer(t, subBody)

	g.admin(t, http.MethodPost, "/api/v1/sources", map[string]any{
		"name":    "e2e-src",
		"type":    "remote",
		"url":     sub.URL,
		"enabled": true,
	}, http.StatusCreated)
	g.admin(t, http.MethodPost, "/api/v1/sources/e2e-src/actions/refresh", nil, http.StatusAccepted)

	var n node
	waitFor(t, "node to appear after sync", 10*time.Second, func() bool {
		nodes := g.listNodes(t)
		if len(nodes) == 0 {
			return false
		}
		n = nodes[0]
		return n.HasOutbound
	})

	// Manual probes bypass the (slow) production probe schedule.
	for _, action := range []string{"probe-egress", "probe-latency"} {
		// Probe triggers may be 200 (synchronous) or 202 (queued).
		st, raw := g.adminStatus(t, http.MethodPost, "/api/v1/nodes/"+n.Hash+"/actions/"+action, nil)
		if st != http.StatusOK && st != http.StatusAccepted {
			t.Fatalf("%s = %d: %s", action, st, raw)
		}
	}

	waitFor(t, "node to become healthy with an egress IP", 10*time.Second, func() bool {
		nodes := g.listNodes(t)
		if len(nodes) == 0 {
			return false
		}
		n = nodes[0]
		return n.Health == "healthy" && n.EgressIP != ""
	})
	return n
}
