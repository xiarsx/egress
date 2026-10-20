// Package config loads the startup-time configuration of egress.
//
// Only launch-essential settings live in the environment (design §13):
// listen address/port, the two tokens, the three data directories and the
// optional declarative config file path. Everything else is a declarative
// resource (see internal/app/control) and can be changed at runtime.
//
// Environment naming: EGRESS_* is the only prefix; no legacy aliases are
// honored.
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/xiarsx/egress/internal/core/identity"
)

// Defaults for the startup environment.
const (
	DefaultListenAddress = "0.0.0.0"
	DefaultPort          = "2260"
	DefaultStateDir      = "/var/lib/egress"
	DefaultCacheDir      = "/var/cache/egress"
	DefaultLogDir        = "/var/log/egress"
)

// Config is the startup configuration, loaded once from the environment.
type Config struct {
	// ListenAddr is "<address>:<port>" the control/ingress listener binds.
	ListenAddr string

	// AdminToken guards the control-plane API
	// (Authorization: Bearer <AdminToken>). Must be non-empty.
	AdminToken string

	// ProxyToken is the V1 line-protocol token. Empty means no-auth mode:
	// HTTP forward accepts requests without credentials and SOCKS5 accepts
	// NO AUTH (spec/identity.md §1).
	ProxyToken string

	StateDir string // state.db lives here
	CacheDir string // cache.db lives here
	LogDir   string // request logs live here

	// ConfigFile is the optional declarative YAML file (EGRESS_CONFIG).
	// Empty means "no file".
	ConfigFile string

	// --- ops/test knobs (optional; zero values select built-in defaults) ---

	// GeoIPDB is the MaxMind .mmdb path; empty = GeoIP enrichment off.
	GeoIPDB string
	// EgressTraceURL overrides the egress-probe trace endpoint.
	EgressTraceURL string
	// CacheFlushInterval narrows the cache.db crash-consistency window
	// (default runtime.CacheFlushInterval).
	CacheFlushInterval time.Duration
	// ReqLogFlushInterval narrows the request-log batch flush window
	// (default 5s).
	ReqLogFlushInterval time.Duration
}

// Load reads the configuration from the environment.
//
// Load returns an error when a required variable is missing or invalid:
//   - EGRESS_ADMIN_TOKEN must be set and non-empty.
//   - EGRESS_PROXY_TOKEN must be set (it may be explicitly empty for
//     no-auth mode); a non-empty value must pass identity.ValidateToken.
func Load() (Config, error) {
	var c Config
	var err error

	addr := envOr("EGRESS_LISTEN_ADDRESS", DefaultListenAddress)
	port := envOr("EGRESS_PORT", DefaultPort)
	if err := validatePort(port); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	c.ListenAddr = net.JoinHostPort(addr, port)

	if c.AdminToken, err = requiredToken("EGRESS_ADMIN_TOKEN", false); err != nil {
		return Config{}, err
	}
	if c.ProxyToken, err = requiredToken("EGRESS_PROXY_TOKEN", true); err != nil {
		return Config{}, err
	}
	if err := identity.ValidateToken(c.ProxyToken); err != nil {
		return Config{}, fmt.Errorf("config: invalid proxy token: %w", err)
	}

	c.StateDir = envOr("EGRESS_STATE_DIR", DefaultStateDir)
	c.CacheDir = envOr("EGRESS_CACHE_DIR", DefaultCacheDir)
	c.LogDir = envOr("EGRESS_LOG_DIR", DefaultLogDir)
	c.ConfigFile = envOr("EGRESS_CONFIG", "")

	// Optional ops/test knobs. Durations fail fast on garbage — a typo'd
	// interval silently falling back to the default is worse than no boot.
	c.GeoIPDB = envOr("EGRESS_GEOIP_DB", "")
	c.EgressTraceURL = envOr("EGRESS_EGRESS_TRACE_URL", "")
	if c.CacheFlushInterval, err = envDuration("EGRESS_CACHE_FLUSH_INTERVAL"); err != nil {
		return Config{}, err
	}
	if c.ReqLogFlushInterval, err = envDuration("EGRESS_REQLOG_FLUSH_INTERVAL"); err != nil {
		return Config{}, err
	}

	return c, nil
}

// envDuration parses an optional duration variable; empty/unset → 0.
func envDuration(name string) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s %q: %w", name, v, err)
	}
	return d, nil
}

// envOr returns the variable's value when set and non-empty, else def.
// An explicitly empty value counts as unset here; callers that need to
// distinguish (the two tokens) use requiredToken instead.
func envOr(name, def string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return def
}

// requiredToken resolves a token variable. When allowEmpty is false the
// value must be non-empty; when true an explicitly empty value is accepted
// (no-auth mode) but the variable itself must still be present.
func requiredToken(name string, allowEmpty bool) (string, error) {
	if v, ok := os.LookupEnv(name); ok {
		if v == "" && !allowEmpty {
			return "", fmt.Errorf("config: 环境变量 %s 不得为空", name)
		}
		return v, nil
	}
	return "", fmt.Errorf("config: 缺少必填环境变量 %s", name)
}

func validatePort(port string) error {
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port %q: must be 1-65535", port)
	}
	return nil
}
