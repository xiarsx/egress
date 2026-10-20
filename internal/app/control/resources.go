// Package control implements the control plane: declarative resource CRUD,
// the control-plane HTTP API (spec/openapi.yaml) and the ports.PlatformSource
// / ports.SourceSource adapters consumed by the runtime.
//
// Dependency rule (design §16): this package only talks to itself and to
// ports.Store. It never touches the sqlite implementation and never mutates
// runtime state directly — runtime notifications go through ports.RuntimeHooks,
// runtime reads/actions go through RuntimeBackend (implemented by the runtime
// group; routes answer 501 NOT_IMPLEMENTED while it is absent).
package control

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Resource document annotations stored in metadata.annotations.
const (
	// AnnotationSource marks where the resource was declared.
	// Value "file" means it came from the declarative YAML file.
	AnnotationSource = "egress.io/source"
	// AnnotationDrifted is set to "true" when the resource was modified via
	// the API after coming from a file (GitOps drift detection, design §13).
	AnnotationDrifted = "egress.io/drifted"

	AnnotationSourceFile = "file"
)

// APIVersion is the only apiVersion accepted in declarative files.
const APIVersion = "egress/v1"

// DefaultStickyTTL is the lease TTL applied when a platform does not set
// one (spec/resources/platform.md).
const DefaultStickyTTL = 168 * time.Hour

// Allocation policies (spec/resources/platform.md).
const (
	AllocationBalanced         = "BALANCED"
	AllocationPreferLowLatency = "PREFER_LOW_LATENCY"
	AllocationPreferIdleIP     = "PREFER_IDLE_IP"
)

// Passive breaker modes (spec/resources/platform.md).
const (
	PassiveBreakerOn  = "on"
	PassiveBreakerOff = "off"
)

// On-miss behaviours (spec/resources/identitypolicy.md).
const (
	OnMissRandom = "random"
	OnMissReject = "reject"
)

// Source types (spec/resources/source.md).
const (
	SourceTypeRemote = "remote"
	SourceTypeLocal  = "local"
)

// ---------------------------------------------------------------------------
// Scalar helpers
// ---------------------------------------------------------------------------

// Duration is a time.Duration encoded as a Go duration string ("168h") in
// both JSON and YAML. It accepts a JSON number as nanoseconds for leniency.
type Duration time.Duration

func (d Duration) String() string { return time.Duration(d).String() }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		return d.parse(s)
	}
	var ns int64
	if err := json.Unmarshal(b, &ns); err == nil {
		*d = Duration(time.Duration(ns))
		return nil
	}
	return fmt.Errorf("control: invalid duration %s", string(b))
}

func (d Duration) MarshalYAML() (any, error) { return shortDuration(time.Duration(d)), nil }

// shortDuration renders d like time.Duration.String but trims redundant
// zero components ("24h0m0s" -> "24h", "1h30m0s" -> "1h30m").
func shortDuration(d time.Duration) string {
	s := d.String()
	s = strings.TrimSuffix(s, "m0s")
	s = strings.TrimSuffix(s, "h0")
	if s == "" {
		return "0s"
	}
	return s
}

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		// numeric YAML scalar → nanoseconds
		var ns int64
		if err2 := n.Decode(&ns); err2 != nil {
			return fmt.Errorf("control: invalid duration node %q", n.Value)
		}
		*d = Duration(time.Duration(ns))
		return nil
	}
	return d.parse(s)
}

func (d *Duration) parse(s string) error {
	v, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("control: invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

// FlexString is a string that also accepts a JSON/YAML number (stored in
// string form). Used for attribute bounds (gte/lte), where the OpenAPI
// schema says number but the filter semantics are string-oriented.
type FlexString string

func (f *FlexString) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*f = FlexString(s)
		return nil
	}
	var num json.Number
	if err := json.Unmarshal(b, &num); err == nil {
		*f = FlexString(num.String())
		return nil
	}
	return fmt.Errorf("control: invalid string-or-number %s", string(b))
}

func (f FlexString) MarshalJSON() ([]byte, error) { return json.Marshal(string(f)) }

// ---------------------------------------------------------------------------
// Resource specs (dual tags: JSON follows spec/openapi.yaml snake_case,
// YAML follows spec/resources camelCase)
// ---------------------------------------------------------------------------

// PlatformFilter mirrors spec/resources/platform.md §filter and the
// openapi PlatformFilter schema.
type PlatformFilter struct {
	Tags  []string                 `json:"tags,omitempty" yaml:"tags,omitempty"`
	Attrs map[string]AttrCondition `json:"attrs,omitempty" yaml:"attrs,omitempty"`
}

// AttrCondition mirrors predicate.AttrCondition (internal/core/predicate).
type AttrCondition struct {
	In       []string   `json:"in,omitempty" yaml:"in,omitempty"`
	NotIn    []string   `json:"notIn,omitempty" yaml:"notIn,omitempty"`
	Match    string     `json:"match,omitempty" yaml:"match,omitempty"`
	NotMatch string     `json:"notMatch,omitempty" yaml:"notMatch,omitempty"`
	Gte      FlexString `json:"gte,omitempty" yaml:"gte,omitempty"`
	Lte      FlexString `json:"lte,omitempty" yaml:"lte,omitempty"`
}

// Platform is a named routing policy (spec/resources/platform.md).
// ID is the stable UUID used as the runtime PlatformID; Name is the store
// key (kind+name) and the human handle.
type Platform struct {
	ID                string         `json:"id" yaml:"-"`
	Name              string         `json:"name" yaml:"-"`
	StickyTTL         Duration       `json:"sticky_ttl" yaml:"stickyTTL"`
	Allocation        string         `json:"allocation" yaml:"allocation"`
	PassiveBreaker    string         `json:"passive_breaker" yaml:"passiveBreaker"`
	IdentityPolicyRef string         `json:"identity_policy" yaml:"identityPolicyRef"`
	Filter            PlatformFilter `json:"filter" yaml:"filter"`
	CreatedAtNs       int64          `json:"created_at_ns,omitempty" yaml:"-"`
	UpdatedAtNs       int64          `json:"updated_at_ns,omitempty" yaml:"-"`
}

// Source declares where nodes come from (spec/resources/source.md).
// Addressed by Name (store kind+name); the API echoes name as id.
type Source struct {
	ID             string   `json:"id" yaml:"-"`
	Name           string   `json:"name" yaml:"-"`
	Type           string   `json:"type" yaml:"type"`
	URL            string   `json:"url,omitempty" yaml:"url,omitempty"`
	Content        string   `json:"content,omitempty" yaml:"content,omitempty"`
	UpdateInterval Duration `json:"update_interval" yaml:"updateInterval"`
	Enabled        bool     `json:"enabled" yaml:"enabled"`
	Ephemeral      bool     `json:"ephemeral,omitempty" yaml:"ephemeral,omitempty"`
	EvictDelay     Duration `json:"evict_delay,omitempty" yaml:"evictDelay,omitempty"`
	LastError      string   `json:"last_error,omitempty" yaml:"-"`
	CreatedAtNs    int64    `json:"created_at_ns,omitempty" yaml:"-"`
	UpdatedAtNs    int64    `json:"updated_at_ns,omitempty" yaml:"-"`
}

// Endpoint declares a custom inbound listener (spec/resources/endpoint.md).
// Addressed by Name; the API echoes name as id.
type Endpoint struct {
	ID                   string `json:"id" yaml:"-"`
	Name                 string `json:"name" yaml:"-"`
	Port                 int    `json:"port" yaml:"port"`
	Enabled              bool   `json:"enabled" yaml:"enabled"`
	AllowManagement      bool   `json:"allow_management" yaml:"allowManagement"`
	AllowProxy           bool   `json:"allow_proxy" yaml:"allowProxy"`
	RequireProxyAuthInfo bool   `json:"require_proxy_auth_info" yaml:"requireProxyAuthInfo"`
	AllowHTTPForward     bool   `json:"allow_http_forward" yaml:"allowHTTPForward"`
	AllowHTTPReverse     bool   `json:"allow_http_reverse" yaml:"allowHTTPReverse"`
	AllowSOCKS5          bool   `json:"allow_socks5" yaml:"allowSOCKS5"`
	CreatedAtNs          int64  `json:"created_at_ns,omitempty" yaml:"-"`
	UpdatedAtNs          int64  `json:"updated_at_ns,omitempty" yaml:"-"`

	// Runtime-decorated fields (never stored):
	ReadOnly  bool   `json:"read_only" yaml:"-"`            // synthesized env default endpoint
	Listening bool   `json:"listening" yaml:"-"`            // listener currently bound
	LastError string `json:"last_error,omitempty" yaml:"-"` // last bind failure
}

// PolicyStep is one link of an IdentityPolicy chain
// (spec/resources/identitypolicy.md). Exactly one variant must be set.
type PolicyStep struct {
	Header     *string         `json:"header,omitempty" yaml:"header,omitempty"`
	Wire       *struct{}       `json:"wire,omitempty" yaml:"wire,omitempty"`
	HeaderRule *HeaderRuleSpec `json:"headerRule,omitempty" yaml:"headerRule,omitempty"`
	// Resolver names a registered ext.IdentityResolver
	// (spec/ext-abi.md §2); unregistered names resolve to "".
	Resolver *string `json:"resolver,omitempty" yaml:"resolver,omitempty"`
}

// HeaderRuleSpec is the headerRule step body.
type HeaderRuleSpec struct {
	Rules []HeaderRuleItem `json:"rules" yaml:"rules"`
}

// HeaderRuleItem maps a target-URL prefix to candidate account headers.
type HeaderRuleItem struct {
	Prefix  string   `json:"prefix" yaml:"prefix"`
	Headers []string `json:"headers" yaml:"headers"`
}

// IdentityPolicy declares how an Account is resolved from a request
// (spec/resources/identitypolicy.md). Addressed by Name.
type IdentityPolicy struct {
	Name        string       `json:"name" yaml:"-"`
	Chain       []PolicyStep `json:"chain" yaml:"chain"`
	OnMiss      string       `json:"on_miss" yaml:"onMiss"`
	CreatedAtNs int64        `json:"created_at_ns,omitempty" yaml:"-"`
	UpdatedAtNs int64        `json:"updated_at_ns,omitempty" yaml:"-"`
}

// Recipe is a shareable template expanding to a Platform + IdentityPolicy
// (spec/resources/recipe.md). Addressed by Name; version is required.
type Recipe struct {
	Name             string                `json:"name" yaml:"-"`
	Version          string                `json:"version" yaml:"version"`
	Requires         []string              `json:"requires,omitempty" yaml:"requires,omitempty"`
	IdentityPolicy   *IdentityPolicySpec   `json:"identity_policy,omitempty" yaml:"identityPolicy,omitempty"`
	PlatformTemplate *PlatformTemplateSpec `json:"platform_template,omitempty" yaml:"platformTemplate,omitempty"`
	CreatedAtNs      int64                 `json:"created_at_ns,omitempty" yaml:"-"`
	UpdatedAtNs      int64                 `json:"updated_at_ns,omitempty" yaml:"-"`
}

// IdentityPolicySpec is the IdentityPolicy body inside a Recipe (no name).
type IdentityPolicySpec struct {
	Chain  []PolicyStep `json:"chain" yaml:"chain"`
	OnMiss string       `json:"on_miss" yaml:"onMiss"`
}

// PlatformTemplateSpec is the Platform body inside a Recipe (no name/id).
type PlatformTemplateSpec struct {
	StickyTTL         Duration       `json:"sticky_ttl,omitempty" yaml:"stickyTTL,omitempty"`
	Allocation        string         `json:"allocation,omitempty" yaml:"allocation,omitempty"`
	PassiveBreaker    string         `json:"passive_breaker,omitempty" yaml:"passiveBreaker,omitempty"`
	IdentityPolicyRef string         `json:"identity_policy,omitempty" yaml:"identityPolicyRef,omitempty"`
	Filter            PlatformFilter `json:"filter,omitempty" yaml:"filter,omitempty"`
}

// SystemConfig is the runtime configuration resource (openapi SystemConfig
// schema), stored under ports.KindConfig name "runtime".
type SystemConfig struct {
	ListenAddress string `json:"listen_address" yaml:"listenAddress"`
	Port          int    `json:"port" yaml:"port"`
	StateDir      string `json:"state_dir" yaml:"stateDir"`
	CacheDir      string `json:"cache_dir" yaml:"cacheDir"`
	LogDir        string `json:"log_dir" yaml:"logDir"`
}

// SystemConfigResourceName is the KindConfig resource name for SystemConfig.
const SystemConfigResourceName = "runtime"

// ---------------------------------------------------------------------------
// API DTOs for runtime-owned resources (openapi schemas)
// ---------------------------------------------------------------------------

// Node mirrors the openapi Node schema (runtime-owned; served via backend).
// DisplayTag/HasOutbound/Enabled/Region/CircuitOpenSince/FailureCount are
// legacy-compatible fields consumed by the ported React admin console.
type Node struct {
	Hash           string         `json:"hash"`
	Tags           []string       `json:"tags"`
	Attrs          map[string]any `json:"attrs"`
	EgressIP       string         `json:"egress_ip,omitempty"`
	Health         string         `json:"health"`
	Failures       int            `json:"failures,omitempty"`
	TrippedSinceNs int64          `json:"tripped_since_ns,omitempty"`

	DisplayTag       string `json:"display_tag,omitempty"`
	HasOutbound      bool   `json:"has_outbound"`
	Enabled          bool   `json:"enabled"`
	Region           string `json:"region,omitempty"`
	CircuitOpenSince string `json:"circuit_open_since,omitempty"`
	FailureCount     int    `json:"failure_count,omitempty"`
}

// Lease mirrors the openapi Lease schema (runtime-owned; served via backend).
type Lease struct {
	PlatformID     string `json:"platform_id"`
	Account        string `json:"account"`
	EgressIP       string `json:"egress_ip,omitempty"`
	NodeHash       string `json:"node_hash,omitempty"`
	ExpiryNs       int64  `json:"expiry_ns"`
	LastAccessedNs int64  `json:"last_accessed_ns,omitempty"`
}

// RequestLogEntry mirrors the openapi RequestLogEntry schema
// (runtime-owned; served via backend).
type RequestLogEntry struct {
	ID          string `json:"id"`
	TsNs        int64  `json:"ts_ns"`
	ProxyType   string `json:"proxy_type"`
	Platform    string `json:"platform"`
	Account     string `json:"account"`
	Target      string `json:"target"`
	Node        string `json:"node"`
	EgressIP    string `json:"egress_ip"`
	NetOk       bool   `json:"net_ok"`
	HTTPStatus  int    `json:"http_status"`
	Stage       string `json:"stage"`
	EgressError string `json:"egress_error"`
	DurationMs  int64  `json:"duration_ms"`
	BytesUp     int64  `json:"bytes_up"`
	BytesDown   int64  `json:"bytes_down"`
}

// ---------------------------------------------------------------------------
// Resource document envelope (what is stored in ports.Resource.Data)
// ---------------------------------------------------------------------------

// DocMetadata carries the store key plus bookkeeping.
type DocMetadata struct {
	Name        string            `json:"name"`
	ID          string            `json:"id,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	CreatedAtNs int64             `json:"createdAtNs,omitempty"`
	UpdatedAtNs int64             `json:"updatedAtNs,omitempty"`
}

// ResourceDoc is the JSON envelope persisted as ports.Resource.Data:
// the full declarative document (apiVersion/kind/metadata/spec).
type ResourceDoc struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Metadata   DocMetadata     `json:"metadata"`
	Spec       json.RawMessage `json:"spec"`
}

// encodeDoc builds the envelope JSON for a resource.
func encodeDoc(kind, name, id string, spec any, annotations map[string]string, createdAtNs int64, nowNs int64) ([]byte, error) {
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("control: encode %s/%s spec: %w", kind, name, err)
	}
	if createdAtNs == 0 {
		createdAtNs = nowNs
	}
	doc := ResourceDoc{
		APIVersion: APIVersion,
		Kind:       kind,
		Metadata: DocMetadata{
			Name:        name,
			ID:          id,
			Annotations: annotations,
			CreatedAtNs: createdAtNs,
			UpdatedAtNs: nowNs,
		},
		Spec: specJSON,
	}
	return json.Marshal(doc)
}

// decodeDoc parses envelope JSON and decodes the spec into out.
func decodeDoc(data []byte, out any) (ResourceDoc, error) {
	var doc ResourceDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return ResourceDoc{}, fmt.Errorf("control: decode resource doc: %w", err)
	}
	if out != nil {
		if err := json.Unmarshal(doc.Spec, out); err != nil {
			return ResourceDoc{}, fmt.Errorf("control: decode %s/%s spec: %w", doc.Kind, doc.Metadata.Name, err)
		}
	}
	return doc, nil
}

// cleanName trims a resource name.
func cleanName(s string) string { return strings.TrimSpace(s) }

// EncodeDoc builds the stored JSON envelope for a resource document.
// It is exported for the config package (file apply path); the Service
// itself uses it internally too.
func EncodeDoc(kind, name, id string, spec any, annotations map[string]string, createdAtNs int64) ([]byte, error) {
	return encodeDoc(kind, name, id, spec, annotations, createdAtNs, nowNs())
}

// DecodeDoc parses envelope JSON without decoding the spec.
func DecodeDoc(data []byte) (ResourceDoc, error) { return decodeDoc(data, nil) }

// DecodeSpec decodes the spec section of an already-parsed document.
func DecodeSpec(doc ResourceDoc, out any) error {
	if err := json.Unmarshal(doc.Spec, out); err != nil {
		return fmt.Errorf("control: decode %s/%s spec: %w", doc.Kind, doc.Metadata.Name, err)
	}
	return nil
}
