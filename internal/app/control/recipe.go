package control

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrUnprocessable marks a semantically invalid request (e.g. recipe
// requires not satisfied) so the API layer can answer 422.
var ErrUnprocessable = errors.New("control: unprocessable")

// EgressVersion is the current binary version used to evaluate recipe
// `requires: ["egress>=x.y"]` entries. The assembler overrides it with
// WithVersion.
var EgressVersion = "0.1.0"

// ApplyResult is the outcome of applying a recipe.
type ApplyResult struct {
	Platform       Platform
	IdentityPolicy *IdentityPolicy // nil when the recipe has no identityPolicy
}

// ApplyRecipe expands a recipe into a Platform and (optionally) an
// IdentityPolicy resource (spec/resources/recipe.md, design §12).
//
//   - requires entries are checked first: "egress>=x.y.z" against
//     EgressVersion, "ext:<name>>=<v>" against the registered extension set
//     (no extension registry exists yet in M3, so any ext: requirement fails
//     with a clear error).
//   - The platform is created/updated under platformName with the recipe's
//     platformTemplate.
//   - When the recipe carries an identityPolicy, it is stored as
//     "<platformName>-identity" (interim naming until M5 settles the
//     convention; see spec/resources/recipe.md TODO) and the platform's
//     identityPolicyRef points at it.
func (s *Service) ApplyRecipe(ctx context.Context, recipeName, platformName string) (ApplyResult, error) {
	recipe, err := s.GetRecipe(ctx, recipeName)
	if err != nil {
		return ApplyResult{}, err
	}
	if err := checkRequires(recipe.Requires); err != nil {
		return ApplyResult{}, fmt.Errorf("%w: recipe %q: %v", ErrUnprocessable, recipeName, err)
	}

	var result ApplyResult
	policyName := ""
	if recipe.IdentityPolicy != nil {
		policyName = platformName + "-identity"
		pol := IdentityPolicy{
			Name:   policyName,
			Chain:  recipe.IdentityPolicy.Chain,
			OnMiss: recipe.IdentityPolicy.OnMiss,
		}
		if pol.OnMiss == "" {
			pol.OnMiss = OnMissRandom
		}
		if err := s.ApplyIdentityPolicy(ctx, pol); err != nil {
			return ApplyResult{}, fmt.Errorf("control: recipe %q: %w", recipeName, err)
		}
		stored, gerr := s.GetIdentityPolicy(ctx, policyName)
		if gerr != nil {
			return ApplyResult{}, gerr
		}
		result.IdentityPolicy = &stored
	}

	plat := Platform{Name: platformName, IdentityPolicyRef: DefaultIdentityPolicyName}
	if recipe.PlatformTemplate != nil {
		t := recipe.PlatformTemplate
		plat.StickyTTL = t.StickyTTL
		plat.Allocation = t.Allocation
		plat.PassiveBreaker = t.PassiveBreaker
		plat.Filter = t.Filter
		if t.IdentityPolicyRef != "" {
			plat.IdentityPolicyRef = t.IdentityPolicyRef
		}
	}
	if policyName != "" {
		plat.IdentityPolicyRef = policyName
	}
	id, err := s.ApplyPlatform(ctx, plat)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("control: recipe %q: %w", recipeName, err)
	}
	stored, gerr := s.GetPlatform(ctx, id)
	if gerr != nil {
		return ApplyResult{}, gerr
	}
	result.Platform = stored
	return result, nil
}

// ExportRecipe rebuilds a Recipe document from an existing Platform and its
// IdentityPolicy.
//
// TODO(M5): implement recipe export (spec/resources/recipe.md); needs the
// CLI surface and the settled "<name>-identity" naming convention.
func (s *Service) ExportRecipe(ctx context.Context, platformName string) (Recipe, error) {
	return Recipe{}, fmt.Errorf("control: recipe export is not implemented yet (M5)")
}

// checkRequires validates recipe dependency declarations.
func checkRequires(requires []string) error {
	for _, r := range requires {
		r = strings.TrimSpace(r)
		switch {
		case strings.HasPrefix(r, "egress>="):
			want := strings.TrimPrefix(r, "egress>=")
			ok, err := semverGTE(EgressVersion, want)
			if err != nil {
				return fmt.Errorf("invalid requires entry %q: %w", r, err)
			}
			if !ok {
				return fmt.Errorf("requires %q not satisfied (running %s)", r, EgressVersion)
			}
		case strings.HasPrefix(r, "ext:"):
			// M3 has no extension registry; every ext: requirement fails
			// loudly rather than being silently ignored.
			return fmt.Errorf("requires %q not satisfied: extension %q is not installed",
				r, strings.TrimSuffix(strings.TrimPrefix(r, "ext:"), versionSuffix(r)))
		case r == "":
			// ignore
		default:
			return fmt.Errorf("invalid requires entry %q (want egress>=x.y.z or ext:<name>>=<v>)", r)
		}
	}
	return nil
}

// versionSuffix extracts the ">=v" tail of an ext: requirement for the error
// message; empty when the entry has no version constraint.
func versionSuffix(r string) string {
	if i := strings.Index(r, ">="); i >= 0 {
		return r[i:]
	}
	return ""
}

// semverGTE reports whether have >= want (numeric prefix comparison;
// pre-release/build metadata ignored for the M3 check).
func semverGTE(have, want string) (bool, error) {
	h, err := parseSemver(have)
	if err != nil {
		return false, fmt.Errorf("bad version %q: %w", have, err)
	}
	w, err := parseSemver(want)
	if err != nil {
		return false, fmt.Errorf("bad version %q: %w", want, err)
	}
	for i := 0; i < 3; i++ {
		if h[i] != w[i] {
			return h[i] > w[i], nil
		}
	}
	return true, nil
}

func parseSemver(v string) ([3]int, error) {
	var out [3]int
	v = strings.TrimSpace(v)
	if i := strings.IndexByte(v, '-'); i >= 0 {
		v = v[:i]
	}
	if i := strings.IndexByte(v, '+'); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return out, fmt.Errorf("want X.Y.Z")
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return out, fmt.Errorf("bad numeric component %q", p)
		}
		out[i] = n
	}
	return out, nil
}
