package control

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// nameForbiddenChars mirrors the platform/token name rules
// (spec/identity.md §3, spec/resources/platform.md).
const nameForbiddenChars = ".:|/\\@?#%~"

// platformNameReserved are names that cannot be created via API/file.
// "Default" already exists (created by EnsureDefaultPlatform); "api"
// collides with control-plane routes.
var platformNameReserved = map[string]bool{"Default": true, "api": true}

// validateResourceName checks the generic name rules shared by resources:
// non-empty after trim, no forbidden characters or whitespace.
func validateResourceName(what, name string) error {
	name = cleanName(name)
	if name == "" {
		return fmt.Errorf("control: %s name must not be empty", what)
	}
	if strings.ContainsAny(name, nameForbiddenChars) {
		return fmt.Errorf("control: %s name %q must not contain any of %q", what, name, nameForbiddenChars)
	}
	for _, r := range name {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			return fmt.Errorf("control: %s name %q must not contain whitespace", what, name)
		}
	}
	return nil
}

// Validate checks a Platform. allowReserved permits the built-in "Default"
// (used by file apply and EnsureDefaultPlatform); the API create path
// passes false.
func (p Platform) Validate(allowReserved bool) error {
	if err := validateResourceName("platform", p.Name); err != nil {
		return err
	}
	if !allowReserved && platformNameReserved[p.Name] {
		return fmt.Errorf("control: platform name %q is reserved", p.Name)
	}
	if time.Duration(p.StickyTTL) <= 0 {
		return fmt.Errorf("control: platform %q: sticky_ttl must be positive", p.Name)
	}
	switch {
	case p.Allocation == AllocationBalanced,
		p.Allocation == AllocationPreferLowLatency,
		p.Allocation == AllocationPreferIdleIP:
	case strings.HasPrefix(p.Allocation, "ext.") && len(p.Allocation) > len("ext."):
		// ext.<selector>: a registered ext.Selector owns the pick
		// (spec/ext-abi.md §2). Resolved at runtime; unregistered names
		// fail closed (NO_AVAILABLE_NODES → route error).
	default:
		return fmt.Errorf("control: platform %q: invalid allocation %q", p.Name, p.Allocation)
	}
	switch p.PassiveBreaker {
	case PassiveBreakerOn, PassiveBreakerOff:
	default:
		return fmt.Errorf("control: platform %q: invalid passive_breaker %q", p.Name, p.PassiveBreaker)
	}
	if p.IdentityPolicyRef == "" {
		return fmt.Errorf("control: platform %q: identity_policy must not be empty", p.Name)
	}
	return validateFilter(p.Filter)
}

func validateFilter(f PlatformFilter) error {
	for _, t := range f.Tags {
		if strings.TrimSpace(t) == "" {
			return fmt.Errorf("control: filter tag must not be empty")
		}
	}
	for k := range f.Attrs {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("control: filter attr key must not be empty")
		}
	}
	return nil
}

// Validate checks a Source.
func (s Source) Validate() error {
	if err := validateResourceName("source", s.Name); err != nil {
		return err
	}
	switch s.Type {
	case SourceTypeRemote:
		if strings.TrimSpace(s.URL) == "" {
			return fmt.Errorf("control: source %q: url is required for type remote", s.Name)
		}
		if iv := time.Duration(s.UpdateInterval); iv != 0 && iv < 30*time.Second {
			return fmt.Errorf("control: source %q: update_interval must be >= 30s for remote", s.Name)
		}
	case SourceTypeLocal:
		if strings.TrimSpace(s.Content) == "" {
			return fmt.Errorf("control: source %q: content is required for type local", s.Name)
		}
	default:
		return fmt.Errorf("control: source %q: invalid type %q (want remote|local)", s.Name, s.Type)
	}
	if time.Duration(s.UpdateInterval) < 0 {
		return fmt.Errorf("control: source %q: update_interval must not be negative", s.Name)
	}
	if s.Ephemeral && time.Duration(s.EvictDelay) <= 0 {
		return fmt.Errorf("control: source %q: evict_delay must be positive when ephemeral", s.Name)
	}
	return nil
}

// Validate checks an Endpoint.
// DefaultEndpointName is the synthesized env-defined endpoint (never
// stored, read-only — spec/resources/endpoint.md).
const DefaultEndpointName = "default"

func (e Endpoint) Validate() error {
	if err := validateResourceName("endpoint", e.Name); err != nil {
		return err
	}
	if e.Name == DefaultEndpointName {
		return fmt.Errorf("control: endpoint name %q is reserved (the env default endpoint)", e.Name)
	}
	if e.Port < 1 || e.Port > 65535 {
		return fmt.Errorf("control: endpoint %q: invalid port %d", e.Name, e.Port)
	}
	return nil
}

// Validate checks an IdentityPolicy.
func (p IdentityPolicy) Validate() error {
	if err := validateResourceName("identity-policy", p.Name); err != nil {
		return err
	}
	return validatePolicyBody(p.Chain, p.OnMiss)
}

func validatePolicyBody(chain []PolicyStep, onMiss string) error {
	if len(chain) == 0 {
		return fmt.Errorf("control: identity policy chain must not be empty")
	}
	for i, s := range chain {
		n := 0
		if s.Header != nil {
			n++
			if strings.TrimSpace(*s.Header) == "" {
				return fmt.Errorf("control: chain[%d]: header must not be empty", i)
			}
		}
		if s.Wire != nil {
			n++
		}
		if s.HeaderRule != nil {
			n++
			if len(s.HeaderRule.Rules) == 0 {
				return fmt.Errorf("control: chain[%d]: headerRule.rules must not be empty", i)
			}
			for j, r := range s.HeaderRule.Rules {
				if strings.TrimSpace(r.Prefix) == "" {
					return fmt.Errorf("control: chain[%d].headerRule.rules[%d]: prefix must not be empty", i, j)
				}
				if len(r.Headers) == 0 {
					return fmt.Errorf("control: chain[%d].headerRule.rules[%d]: headers must not be empty", i, j)
				}
			}
		}
		if s.Resolver != nil {
			n++
			if strings.TrimSpace(*s.Resolver) == "" {
				return fmt.Errorf("control: chain[%d]: resolver must not be empty", i)
			}
		}
		if n != 1 {
			return fmt.Errorf("control: chain[%d]: exactly one of header|wire|headerRule|resolver must be set", i)
		}
	}
	switch onMiss {
	case OnMissRandom, OnMissReject:
	default:
		return fmt.Errorf("control: invalid on_miss %q (want random|reject)", onMiss)
	}
	return nil
}

var semverRe = regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

// Validate checks a Recipe.
func (r Recipe) Validate() error {
	if err := validateResourceName("recipe", r.Name); err != nil {
		return err
	}
	if !semverRe.MatchString(r.Version) {
		return fmt.Errorf("control: recipe %q: version %q is not semver", r.Name, r.Version)
	}
	if r.IdentityPolicy != nil {
		if err := validatePolicyBody(r.IdentityPolicy.Chain, r.IdentityPolicy.OnMiss); err != nil {
			return fmt.Errorf("control: recipe %q identity_policy: %w", r.Name, err)
		}
	}
	if r.PlatformTemplate != nil {
		t := r.PlatformTemplate
		if ttl := time.Duration(t.StickyTTL); t.StickyTTL != 0 && ttl <= 0 {
			return fmt.Errorf("control: recipe %q platform_template: sticky_ttl must be positive", r.Name)
		}
		if t.Allocation != "" {
			switch {
			case t.Allocation == AllocationBalanced,
				t.Allocation == AllocationPreferLowLatency,
				t.Allocation == AllocationPreferIdleIP,
				strings.HasPrefix(t.Allocation, "ext.") && len(t.Allocation) > len("ext."):
			default:
				return fmt.Errorf("control: recipe %q platform_template: invalid allocation %q", r.Name, t.Allocation)
			}
		}
		if t.PassiveBreaker != "" {
			switch t.PassiveBreaker {
			case PassiveBreakerOn, PassiveBreakerOff:
			default:
				return fmt.Errorf("control: recipe %q platform_template: invalid passive_breaker %q", r.Name, t.PassiveBreaker)
			}
		}
		if err := validateFilter(t.Filter); err != nil {
			return fmt.Errorf("control: recipe %q platform_template: %w", r.Name, err)
		}
	}
	return nil
}
