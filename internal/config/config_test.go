package config

import (
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
	if c.StateDir != "/var/lib/egress" || c.CacheDir != "/var/cache/egress" || c.LogDir != "/var/log/egress" {
		t.Fatalf("dirs = %+v", c)
	}
	if c.ConfigFile != "" {
		t.Fatalf("ConfigFile = %q", c.ConfigFile)
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
