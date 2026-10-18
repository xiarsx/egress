// Package config loads the startup-time configuration of egress.
//
// Only launch-essential settings live in the environment (design §13):
// listen address/port, the two tokens, the three data directories and the
// optional declarative config file path. Everything else is a declarative
// resource (see internal/app/control) and can be changed at runtime.
//
// Environment naming: EGRESS_* is canonical. A missing EGRESS_* falls back
// to the legacy RESIN_* name for one major version and prints a deprecation
// warning to stderr (design §17).
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/xiarsx/egress/internal/core/identity"
)

// Defaults for the startup environment. They mirror the reference
// implementation's RESIN_* defaults (docs/refmap.md §2.9); only the
// variable prefix was renamed.
const (
	DefaultListenAddress = "0.0.0.0"
	DefaultPort          = "2260"
	DefaultStateDir      = "/var/lib/resin"
	DefaultCacheDir      = "/var/cache/resin"
	DefaultLogDir        = "/var/log/resin"
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
}

// Load reads the configuration from the environment.
//
// Resolution order per variable: EGRESS_* first; when unset (or empty,
// except for the two tokens where an explicit empty value is meaningful),
// fall back to RESIN_* and print a deprecation warning to stderr:
//
//	[DEPRECATED] 环境变量 RESIN_X 已更名为 EGRESS_X，请迁移
//
// Load returns an error when a required variable is missing or invalid:
//   - EGRESS_ADMIN_TOKEN must be set and non-empty.
//   - EGRESS_PROXY_TOKEN must be set (it may be explicitly empty for
//     no-auth mode); a non-empty value must pass identity.ValidateToken.
func Load() (Config, error) {
	var c Config
	var err error

	addr := envOrLegacy("EGRESS_LISTEN_ADDRESS", DefaultListenAddress)
	port := envOrLegacy("EGRESS_PORT", DefaultPort)
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

	c.StateDir = envOrLegacy("EGRESS_STATE_DIR", DefaultStateDir)
	c.CacheDir = envOrLegacy("EGRESS_CACHE_DIR", DefaultCacheDir)
	c.LogDir = envOrLegacy("EGRESS_LOG_DIR", DefaultLogDir)
	c.ConfigFile = envOrLegacy("EGRESS_CONFIG", "")

	return c, nil
}

// legacyName maps EGRESS_X to RESIN_X.
func legacyName(egress string) string {
	return "RESIN_" + strings.TrimPrefix(egress, "EGRESS_")
}

// lookup returns the value and whether the variable is set to a non-empty
// value. An explicitly empty value counts as unset here; callers that need
// to distinguish (the two tokens) use lookupRaw instead.
func lookup(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	return v, ok && v != ""
}

// envOrLegacy resolves EGRESS_* with fallback to RESIN_*.
// A used legacy variable prints the deprecation warning.
func envOrLegacy(egress, def string) string {
	if v, ok := lookup(egress); ok {
		return v
	}
	legacy := legacyName(egress)
	if v, ok := lookup(legacy); ok {
		deprecated(legacy, egress)
		return v
	}
	return def
}

// requiredToken resolves a token variable. When allowEmpty is false the
// value must be non-empty; when true an explicitly empty value is accepted
// (no-auth mode) but the variable itself must still be present.
func requiredToken(egress string, allowEmpty bool) (string, error) {
	legacy := legacyName(egress)
	if v, ok := os.LookupEnv(egress); ok {
		if v == "" && !allowEmpty {
			return "", fmt.Errorf("config: 环境变量 %s 不得为空", egress)
		}
		return v, nil
	}
	if v, ok := os.LookupEnv(legacy); ok {
		deprecated(legacy, egress)
		if v == "" && !allowEmpty {
			return "", fmt.Errorf("config: 环境变量 %s（已更名为 %s）不得为空", legacy, egress)
		}
		return v, nil
	}
	return "", fmt.Errorf("config: 缺少必填环境变量 %s（兼容变量 %s）", egress, legacy)
}

// deprecated prints the standard rename warning to stderr.
func deprecated(oldName, newName string) {
	fmt.Fprintf(os.Stderr, "[DEPRECATED] 环境变量 %s 已更名为 %s，请迁移\n", oldName, newName)
}

func validatePort(port string) error {
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port %q: must be 1-65535", port)
	}
	return nil
}
