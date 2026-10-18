package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/ports"
)

// fileDoc is one YAML document of the declarative config file.
type fileDoc struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name     string   `yaml:"name"`
		Version  string   `yaml:"version"`
		Requires []string `yaml:"requires"`
	} `yaml:"metadata"`
	Spec yaml.Node `yaml:"spec"`
}

// ApplyFile parses a multi-document YAML file (apiVersion: egress/v1) and
// creates/updates each resource in store (design §13: the file is the
// declared source; later API edits are marked drifted).
//
// Supported kinds: Platform, Source, Endpoint, IdentityPolicy, Recipe.
// Every applied document is annotated with "egress.io/source: file"; a
// re-applied file clears a previous "egress.io/drifted" annotation because
// the file is the source of truth again.
func ApplyFile(ctx context.Context, path string, store ports.Store) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	n := 0
	for {
		var doc fileDoc
		if derr := dec.Decode(&doc); derr != nil {
			if errors.Is(derr, io.EOF) {
				break
			}
			return fmt.Errorf("config: %s: decode document %d: %w", path, n+1, derr)
		}
		n++
		if doc.APIVersion == "" && doc.Kind == "" {
			continue // empty document (e.g. trailing "---")
		}
		if err := applyDoc(ctx, store, path, n, doc); err != nil {
			return err
		}
	}
	return nil
}

func applyDoc(ctx context.Context, store ports.Store, path string, n int, doc fileDoc) error {
	where := fmt.Sprintf("%s document %d", path, n)
	if doc.APIVersion != control.APIVersion {
		return fmt.Errorf("config: %s: unsupported apiVersion %q (want %q)", where, doc.APIVersion, control.APIVersion)
	}
	name := strings.TrimSpace(doc.Metadata.Name)
	if name == "" {
		return fmt.Errorf("config: %s: metadata.name is required", where)
	}
	kind := ports.Kind(doc.Kind)
	switch kind {
	case ports.KindPlatform, ports.KindSource, ports.KindEndpoint, ports.KindIdentityPolicy, ports.KindRecipe:
	default:
		return fmt.Errorf("config: %s: unsupported kind %q", where, doc.Kind)
	}

	spec, err := decodeFileSpec(kind, name, doc.Spec)
	if err != nil {
		return fmt.Errorf("config: %s: %w", where, err)
	}
	if r, ok := spec.(control.Recipe); ok {
		// Recipe version/requires live in metadata, not in spec.
		r.Version = doc.Metadata.Version
		r.Requires = doc.Metadata.Requires
		spec = r
	}
	// Validation mirrors the API surface.
	if err := validateFileResource(kind, name, spec); err != nil {
		return fmt.Errorf("config: %s: %w", where, err)
	}

	// Preserve the existing ID (platform UUIDs are stable) and createdAt.
	// Annotations: keep the old ones, force source=file, and clear drifted —
	// a file re-apply makes the file the source of truth again.
	id := name
	annotations := map[string]string{control.AnnotationSource: control.AnnotationSourceFile}
	var createdAtNs int64
	if existing, gerr := store.Get(ctx, kind, name); gerr == nil {
		if edoc, derr := control.DecodeDoc(existing.Data); derr == nil {
			id = edoc.Metadata.ID
			createdAtNs = edoc.Metadata.CreatedAtNs
			for k, v := range edoc.Metadata.Annotations {
				if _, ok := annotations[k]; !ok {
					annotations[k] = v
				}
			}
			delete(annotations, control.AnnotationDrifted)
			if id == "" {
				id = name
			}
		}
	} else if !errors.Is(gerr, ports.ErrNotFound) {
		return fmt.Errorf("config: %s: %w", where, gerr)
	}

	data, err := control.EncodeDoc(string(kind), name, id, spec, annotations, createdAtNs)
	if err != nil {
		return fmt.Errorf("config: %s: %w", where, err)
	}
	if _, gerr := store.Get(ctx, kind, name); errors.Is(gerr, ports.ErrNotFound) {
		if cerr := store.Create(ctx, ports.Resource{Kind: kind, Name: name, Data: data}); cerr != nil {
			return fmt.Errorf("config: %s: %w", where, cerr)
		}
		return nil
	}
	existing, _ := store.Get(ctx, kind, name)
	existing.Data = data
	if uerr := store.Update(ctx, existing); uerr != nil {
		return fmt.Errorf("config: %s: %w", where, uerr)
	}
	return nil
}

// decodeFileSpec decodes the spec node of a file document into the typed
// resource struct for kind. Name-carrying fields come from metadata.
func decodeFileSpec(kind ports.Kind, name string, node yaml.Node) (any, error) {
	switch kind {
	case ports.KindPlatform:
		var spec control.Platform
		if err := node.Decode(&spec); err != nil {
			return nil, fmt.Errorf("bad platform spec: %w", err)
		}
		spec.Name = name
		return spec, nil
	case ports.KindSource:
		var spec control.Source
		if err := node.Decode(&spec); err != nil {
			return nil, fmt.Errorf("bad source spec: %w", err)
		}
		spec.Name = name
		return spec, nil
	case ports.KindEndpoint:
		var spec control.Endpoint
		if err := node.Decode(&spec); err != nil {
			return nil, fmt.Errorf("bad endpoint spec: %w", err)
		}
		spec.Name = name
		return spec, nil
	case ports.KindIdentityPolicy:
		var spec control.IdentityPolicy
		if err := node.Decode(&spec); err != nil {
			return nil, fmt.Errorf("bad identityPolicy spec: %w", err)
		}
		spec.Name = name
		return spec, nil
	case ports.KindRecipe:
		var spec control.Recipe
		if err := node.Decode(&spec); err != nil {
			return nil, fmt.Errorf("bad recipe spec: %w", err)
		}
		spec.Name = name
		// version/requires live in metadata for recipes.
		return spec, nil
	default:
		return nil, fmt.Errorf("unsupported kind %q", kind)
	}
}

func validateFileResource(kind ports.Kind, name string, spec any) error {
	allowReserved := kind == ports.KindPlatform // "Default" may be updated from file
	switch s := spec.(type) {
	case control.Platform:
		return s.Validate(allowReserved)
	case control.Source:
		if s.Type == "" {
			return fmt.Errorf("source %q: type is required", name)
		}
		return s.Validate()
	case control.Endpoint:
		return s.Validate()
	case control.IdentityPolicy:
		return s.Validate()
	case control.Recipe:
		return s.Validate()
	default:
		return fmt.Errorf("unknown spec type for kind %s", kind)
	}
}

// ---------------------------------------------------------------------------
// Default platform bootstrap (design P4)
// ---------------------------------------------------------------------------

// defaultIdentityPolicy is the built-in "default" chain (spec/identity.md
// §5, extended with the catch-all headerRule from
// spec/resources/identitypolicy.md).
func defaultIdentityPolicy() control.IdentityPolicy {
	header := "X-Resin-Account"
	catchAll := control.PolicyStep{HeaderRule: &control.HeaderRuleSpec{
		Rules: []control.HeaderRuleItem{
			{Prefix: "*", Headers: []string{"Authorization", "x-api-key"}},
		},
	}}
	return control.IdentityPolicy{
		Name:   control.DefaultIdentityPolicyName,
		Chain:  []control.PolicyStep{{Header: &header}, {Wire: &struct{}{}}, catchAll},
		OnMiss: control.OnMissRandom,
	}
}

// EnsureDefaultPlatform creates the built-in Default platform (and its
// default IdentityPolicy) when absent. It is idempotent: existing resources
// are left untouched, so user customizations survive restarts.
//
// Default platform: empty filter, StickyTTL 168h (the reference default),
// allocation BALANCED, passiveBreaker on (spec/resources/platform.md TODO:
// default on until decided otherwise), identityPolicyRef "default".
func EnsureDefaultPlatform(ctx context.Context, store ports.Store) error {
	if _, err := store.Get(ctx, ports.KindIdentityPolicy, control.DefaultIdentityPolicyName); errors.Is(err, ports.ErrNotFound) {
		pol := defaultIdentityPolicy()
		spec := pol
		spec.Name = ""
		data, merr := control.EncodeDoc(string(ports.KindIdentityPolicy), pol.Name, pol.Name, spec, nil, 0)
		if merr != nil {
			return merr
		}
		if cerr := store.Create(ctx, ports.Resource{Kind: ports.KindIdentityPolicy, Name: pol.Name, Data: data}); cerr != nil && !errors.Is(cerr, ports.ErrAlreadyExists) {
			return fmt.Errorf("config: create default identity policy: %w", cerr)
		}
	} else if err != nil {
		return fmt.Errorf("config: get default identity policy: %w", err)
	}

	if _, err := store.Get(ctx, ports.KindPlatform, control.DefaultPlatformName); !errors.Is(err, ports.ErrNotFound) {
		return err // exists (nil) or a real error
	}
	plat := control.Platform{
		ID:                control.DefaultPlatformID,
		Name:              control.DefaultPlatformName,
		StickyTTL:         control.Duration(control.DefaultStickyTTL),
		Allocation:        control.AllocationBalanced,
		PassiveBreaker:    control.PassiveBreakerOn,
		IdentityPolicyRef: control.DefaultIdentityPolicyName,
		Filter:            control.PlatformFilter{},
	}
	spec := plat
	spec.ID, spec.Name = "", ""
	data, merr := control.EncodeDoc(string(ports.KindPlatform), plat.Name, plat.ID, spec, nil, 0)
	if merr != nil {
		return merr
	}
	if cerr := store.Create(ctx, ports.Resource{Kind: ports.KindPlatform, Name: plat.Name, Data: data}); cerr != nil && !errors.Is(cerr, ports.ErrAlreadyExists) {
		return fmt.Errorf("config: create default platform: %w", cerr)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Legacy RESIN_DEFAULT_PLATFORM_* compatibility (design §17)
// ---------------------------------------------------------------------------

// legacyDefaultPlatformVars lists the legacy variables mapped onto the
// Default platform / default identity policy.
var legacyDefaultPlatformVars = []string{
	"RESIN_DEFAULT_PLATFORM_STICKY_TTL",
	"RESIN_DEFAULT_PLATFORM_ALLOCATION_POLICY",
	"RESIN_DEFAULT_PLATFORM_PASSIVE_BREAKER",
	"RESIN_DEFAULT_PLATFORM_REGEX_FILTERS",
	"RESIN_DEFAULT_PLATFORM_REGION_FILTERS",
	"RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_MISS_ACTION",
	"RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_EMPTY_ACCOUNT_BEHAVIOR",
	"RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_FIXED_ACCOUNT_HEADER",
}

// ApplyLegacyDefaultEnv reads RESIN_DEFAULT_PLATFORM_* (kept for one major
// version, design §17) and writes the values into the Default platform and
// the default IdentityPolicy. Each used variable prints a deprecation
// warning. Unset variables leave the resource untouched.
func ApplyLegacyDefaultEnv(ctx context.Context, store ports.Store) error {
	set := map[string]string{}
	for _, v := range legacyDefaultPlatformVars {
		if val, ok := os.LookupEnv(v); ok && val != "" {
			set[v] = val
		}
	}
	if len(set) == 0 {
		return nil
	}
	for _, v := range legacyDefaultPlatformVars {
		if _, ok := set[v]; ok {
			deprecatedLegacy(v)
		}
	}

	if err := EnsureDefaultPlatform(ctx, store); err != nil {
		return err
	}

	// Load current Default platform.
	r, err := store.Get(ctx, ports.KindPlatform, control.DefaultPlatformName)
	if err != nil {
		return fmt.Errorf("config: load default platform: %w", err)
	}
	edoc, derr := control.DecodeDoc(r.Data)
	if derr != nil {
		return derr
	}
	var plat control.Platform
	if derr := control.DecodeSpec(edoc, &plat); derr != nil {
		return derr
	}
	plat.Name = control.DefaultPlatformName
	plat.ID = edoc.Metadata.ID

	// Load current default identity policy.
	pr, err := store.Get(ctx, ports.KindIdentityPolicy, control.DefaultIdentityPolicyName)
	if err != nil {
		return fmt.Errorf("config: load default identity policy: %w", err)
	}
	pdoc, derr := control.DecodeDoc(pr.Data)
	if derr != nil {
		return derr
	}
	var pol control.IdentityPolicy
	if derr := control.DecodeSpec(pdoc, &pol); derr != nil {
		return derr
	}
	pol.Name = control.DefaultIdentityPolicyName

	changed, pchanged := false, false

	if v, ok := set["RESIN_DEFAULT_PLATFORM_STICKY_TTL"]; ok {
		d, perr := time.ParseDuration(strings.TrimSpace(v))
		if perr != nil || d <= 0 {
			fmt.Fprintf(os.Stderr, "[DEPRECATED] RESIN_DEFAULT_PLATFORM_STICKY_TTL=%q invalid, ignored\n", v)
		} else {
			plat.StickyTTL = control.Duration(d)
			changed = true
		}
	}
	if v, ok := set["RESIN_DEFAULT_PLATFORM_ALLOCATION_POLICY"]; ok {
		switch strings.ToUpper(strings.TrimSpace(v)) {
		case control.AllocationBalanced, control.AllocationPreferLowLatency, control.AllocationPreferIdleIP:
			plat.Allocation = strings.ToUpper(strings.TrimSpace(v))
			changed = true
		default:
			fmt.Fprintf(os.Stderr, "[DEPRECATED] RESIN_DEFAULT_PLATFORM_ALLOCATION_POLICY=%q invalid, ignored\n", v)
		}
	}
	if v, ok := set["RESIN_DEFAULT_PLATFORM_PASSIVE_BREAKER"]; ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "on":
			plat.PassiveBreaker = control.PassiveBreakerOn
			changed = true
		case "off":
			plat.PassiveBreaker = control.PassiveBreakerOff
			changed = true
		default:
			fmt.Fprintf(os.Stderr, "[DEPRECATED] RESIN_DEFAULT_PLATFORM_PASSIVE_BREAKER=%q invalid, ignored\n", v)
		}
	}
	if v, ok := set["RESIN_DEFAULT_PLATFORM_REGEX_FILTERS"]; ok {
		plat.Filter.Tags = splitList(v)
		changed = true
	}
	if v, ok := set["RESIN_DEFAULT_PLATFORM_REGION_FILTERS"]; ok {
		in, notIn := splitRegions(v)
		attrs := plat.Filter.Attrs
		if attrs == nil {
			attrs = map[string]control.AttrCondition{}
		}
		cond := attrs["geo.country"]
		cond.In = in
		cond.NotIn = notIn
		attrs["geo.country"] = cond
		plat.Filter.Attrs = attrs
		changed = true
	}

	// Reverse-proxy legacy fields → identity policy chain (design §17).
	missAction := strings.ToUpper(strings.TrimSpace(set["RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_MISS_ACTION"]))
	behavior := strings.ToUpper(strings.TrimSpace(set["RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_EMPTY_ACCOUNT_BEHAVIOR"]))
	fixedHeaders := splitList(set["RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_FIXED_ACCOUNT_HEADER"])
	if missAction != "" || behavior != "" || len(fixedHeaders) > 0 {
		chain := []control.PolicyStep{}
		header := "X-Resin-Account"
		chain = append(chain, control.PolicyStep{Header: &header})
		switch behavior {
		case "FIXED_HEADER":
			for _, h := range fixedHeaders {
				h := h
				chain = append(chain, control.PolicyStep{Header: &h})
			}
		case "ACCOUNT_HEADER_RULE":
			// The old global header-rule table has no direct equivalent
			// here; keep the catch-all headerRule step as the fallback.
			chain = append(chain, control.PolicyStep{HeaderRule: &control.HeaderRuleSpec{
				Rules: []control.HeaderRuleItem{{Prefix: "*", Headers: []string{"Authorization", "x-api-key"}}},
			}})
		case "RANDOM", "":
		default:
			fmt.Fprintf(os.Stderr, "[DEPRECATED] RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_EMPTY_ACCOUNT_BEHAVIOR=%q unknown, ignored\n", behavior)
		}
		chain = append(chain, control.PolicyStep{Wire: &struct{}{}})
		pol.Chain = chain
		switch missAction {
		case "REJECT":
			pol.OnMiss = control.OnMissReject
		case "TREAT_AS_EMPTY", "":
			pol.OnMiss = control.OnMissRandom
		default:
			fmt.Fprintf(os.Stderr, "[DEPRECATED] RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_MISS_ACTION=%q unknown, ignored\n", missAction)
			pol.OnMiss = control.OnMissRandom
		}
		pchanged = true
	}

	if changed {
		if verr := plat.Validate(true); verr != nil {
			return fmt.Errorf("config: legacy default platform invalid: %w", verr)
		}
		spec := plat
		spec.ID, spec.Name = "", ""
		data, merr := control.EncodeDoc(string(ports.KindPlatform), plat.Name, plat.ID, spec,
			mergeFileAnnotations(edoc.Metadata.Annotations), edoc.Metadata.CreatedAtNs)
		if merr != nil {
			return merr
		}
		r.Data = data
		if uerr := store.Update(ctx, r); uerr != nil {
			return fmt.Errorf("config: update default platform: %w", uerr)
		}
	}
	if pchanged {
		if verr := pol.Validate(); verr != nil {
			return fmt.Errorf("config: legacy default identity policy invalid: %w", verr)
		}
		spec := pol
		spec.Name = ""
		data, merr := control.EncodeDoc(string(ports.KindIdentityPolicy), pol.Name, pol.Name, spec,
			mergeFileAnnotations(pdoc.Metadata.Annotations), pdoc.Metadata.CreatedAtNs)
		if merr != nil {
			return merr
		}
		pr.Data = data
		if uerr := store.Update(ctx, pr); uerr != nil {
			return fmt.Errorf("config: update default identity policy: %w", uerr)
		}
	}
	return nil
}

// deprecatedLegacy warns about a RESIN_DEFAULT_PLATFORM_* variable: it has
// no EGRESS_* rename target, it is applied onto the Default platform /
// default identity policy instead (design §17, one major version).
func deprecatedLegacy(name string) {
	fmt.Fprintf(os.Stderr, "[DEPRECATED] legacy env %s is deprecated: value applied to Default platform/policy, use declarative resources instead\n", name)
}

// mergeFileAnnotations keeps existing annotations (e.g. egress.io/source)
// across the legacy-env update.
func mergeFileAnnotations(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// splitList splits comma/newline separated env lists, trimming blanks.
func splitList(v string) []string {
	v = strings.ReplaceAll(v, "\n", ",")
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// splitRegions splits region filters; "!xx" entries become notIn
// (reference RegionFilters negation semantics, docs/refmap.md §3.1).
func splitRegions(v string) (in, notIn []string) {
	for _, p := range splitList(v) {
		if strings.HasPrefix(p, "!") {
			if rest := strings.TrimSpace(strings.TrimPrefix(p, "!")); rest != "" {
				notIn = append(notIn, strings.ToLower(rest))
			}
			continue
		}
		in = append(in, strings.ToLower(p))
	}
	return in, notIn
}
