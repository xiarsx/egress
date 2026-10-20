package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

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
	header := "X-Egress-Account"
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
