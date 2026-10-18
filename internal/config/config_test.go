package config

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// clearEnv removes every variable Load may read, returning a restore func.
func clearEnv(t *testing.T) {
	t.Helper()
	vars := []string{
		"EGRESS_LISTEN_ADDRESS", "EGRESS_PORT", "EGRESS_ADMIN_TOKEN", "EGRESS_PROXY_TOKEN",
		"EGRESS_STATE_DIR", "EGRESS_CACHE_DIR", "EGRESS_LOG_DIR", "EGRESS_CONFIG",
		"RESIN_LISTEN_ADDRESS", "RESIN_PORT", "RESIN_ADMIN_TOKEN", "RESIN_PROXY_TOKEN",
		"RESIN_STATE_DIR", "RESIN_CACHE_DIR", "RESIN_LOG_DIR", "RESIN_CONFIG",
	}
	saved := map[string]string{}
	unset := map[string]bool{}
	for _, v := range vars {
		if val, ok := os.LookupEnv(v); ok {
			saved[v] = val
		} else {
			unset[v] = true
		}
		os.Unsetenv(v)
	}
	t.Cleanup(func() {
		for v := range unset {
			os.Unsetenv(v)
		}
		for v, val := range saved {
			os.Setenv(v, val)
		}
	})
}

// captureStderr runs f with os.Stderr redirected to a buffer.
func captureStderr(t *testing.T, f func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	defer func() { os.Stderr = old }()
	f()
	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	return buf.String()
}

func mustSetenv(t *testing.T, k, v string) {
	t.Helper()
	t.Setenv(k, v)
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "EGRESS_ADMIN_TOKEN", "admin-secret")
	mustSetenv(t, "EGRESS_PROXY_TOKEN", "proxy-secret")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.ListenAddr != "0.0.0.0:2260" {
		t.Fatalf("ListenAddr = %q", c.ListenAddr)
	}
	if c.AdminToken != "admin-secret" || c.ProxyToken != "proxy-secret" {
		t.Fatalf("tokens = %+v", c)
	}
	if c.StateDir != "/var/lib/resin" || c.CacheDir != "/var/cache/resin" || c.LogDir != "/var/log/resin" {
		t.Fatalf("dirs = %+v", c)
	}
	if c.ConfigFile != "" {
		t.Fatalf("ConfigFile = %q", c.ConfigFile)
	}
}

func TestLoadLegacyFallbackAndWarning(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "RESIN_ADMIN_TOKEN", "legacy-admin")
	mustSetenv(t, "RESIN_PROXY_TOKEN", "legacy-proxy")
	mustSetenv(t, "RESIN_PORT", "8080")
	mustSetenv(t, "RESIN_LISTEN_ADDRESS", "127.0.0.1")

	var c Config
	var err error
	out := captureStderr(t, func() { c, err = Load() })
	if err != nil {
		t.Fatal(err)
	}
	if c.ListenAddr != "127.0.0.1:8080" {
		t.Fatalf("ListenAddr = %q", c.ListenAddr)
	}
	if c.AdminToken != "legacy-admin" || c.ProxyToken != "legacy-proxy" {
		t.Fatalf("tokens = %+v", c)
	}
	for _, want := range []string{
		"[DEPRECATED] 环境变量 RESIN_ADMIN_TOKEN 已更名为 EGRESS_ADMIN_TOKEN，请迁移",
		"[DEPRECATED] 环境变量 RESIN_PROXY_TOKEN 已更名为 EGRESS_PROXY_TOKEN，请迁移",
		"[DEPRECATED] 环境变量 RESIN_PORT 已更名为 EGRESS_PORT，请迁移",
		"[DEPRECATED] 环境变量 RESIN_LISTEN_ADDRESS 已更名为 EGRESS_LISTEN_ADDRESS，请迁移",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stderr missing %q\nstderr=%q", want, out)
		}
	}
}

func TestLoadEgressPriorityNoWarning(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "EGRESS_ADMIN_TOKEN", "new-admin")
	mustSetenv(t, "RESIN_ADMIN_TOKEN", "old-admin")
	mustSetenv(t, "EGRESS_PROXY_TOKEN", "new-proxy")
	mustSetenv(t, "RESIN_PROXY_TOKEN", "old-proxy")

	var c Config
	var err error
	out := captureStderr(t, func() { c, err = Load() })
	if err != nil {
		t.Fatal(err)
	}
	if c.AdminToken != "new-admin" || c.ProxyToken != "new-proxy" {
		t.Fatalf("tokens = %+v", c)
	}
	if strings.Contains(out, "[DEPRECATED]") {
		t.Fatalf("unexpected deprecation warning: %q", out)
	}
}

func TestLoadMissingRequired(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "EGRESS_PROXY_TOKEN", "x")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "EGRESS_ADMIN_TOKEN") {
		t.Fatalf("expected admin token error, got %v", err)
	}

	clearEnv(t)
	mustSetenv(t, "EGRESS_ADMIN_TOKEN", "x")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "EGRESS_PROXY_TOKEN") {
		t.Fatalf("expected proxy token error, got %v", err)
	}
}

func TestLoadEmptyAdminTokenRejected(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "EGRESS_ADMIN_TOKEN", "")
	mustSetenv(t, "EGRESS_PROXY_TOKEN", "x")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for empty admin token")
	}
}

func TestLoadEmptyProxyTokenIsNoAuth(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "EGRESS_ADMIN_TOKEN", "admin")
	mustSetenv(t, "EGRESS_PROXY_TOKEN", "") // explicitly empty = no-auth mode
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.ProxyToken != "" {
		t.Fatalf("ProxyToken = %q", c.ProxyToken)
	}
}

func TestLoadBadProxyToken(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "EGRESS_ADMIN_TOKEN", "admin")
	for _, bad := range []string{"has space", "a:b", "api", "under_score.ok"} {
		mustSetenv(t, "EGRESS_PROXY_TOKEN", bad)
		if _, err := Load(); err == nil {
			t.Fatalf("expected error for token %q", bad)
		}
	}
}

func TestLoadBadPort(t *testing.T) {
	clearEnv(t)
	mustSetenv(t, "EGRESS_ADMIN_TOKEN", "admin")
	mustSetenv(t, "EGRESS_PROXY_TOKEN", "x")
	mustSetenv(t, "EGRESS_PORT", "notaport")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for bad port")
	}
}
