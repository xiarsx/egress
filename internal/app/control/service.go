package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/predicate"
	"github.com/xiarsx/egress/internal/ports"
)

// DefaultIdentityPolicyName is the IdentityPolicy every platform references
// unless it says otherwise (spec/resources/platform.md).
const DefaultIdentityPolicyName = "default"

// DefaultPlatformName is the built-in platform (design P4).
const DefaultPlatformName = "Default"

// DefaultPlatformID is the stable UUID of the built-in Default platform,
// kept compatible with the reference implementation.
const DefaultPlatformID = "00000000-0000-0000-0000-000000000000"

// Compile-time assertions: Service implements the cross-group contracts
// defined in ports/runtime.go.
var (
	_ ports.PlatformSource = (*Service)(nil)
	_ ports.SourceSource   = (*Service)(nil)
)

// NopHooks is a ports.RuntimeHooks that does nothing. Useful in tests and
// before the runtime group wires the real hooks.
type NopHooks struct{}

func (NopHooks) OnPlatformChanged(string) {}
func (NopHooks) OnPlatformDeleted(string) {}
func (NopHooks) OnSourceChanged(string)   {}
func (NopHooks) OnSourceDeleted(string)   {}

// EndpointHooks notifies the ingress layer of endpoint changes
// (spec/resources/endpoint.md). Wired by the assembly root to the
// endpoint.Manager; separate from ports.RuntimeHooks because endpoints
// are ingress, not runtime.
type EndpointHooks interface {
	OnEndpointChanged(name string)
	OnEndpointDeleted(name string)
}

// EndpointStatus reports one endpoint's runtime state.
type EndpointStatus struct {
	Listening bool
	LastError string
}

// Service is the control-plane use-case layer: resource CRUD over
// ports.Store plus the PlatformSource/SourceSource adapters the runtime
// consumes. It never touches runtime state directly; change notifications
// go through hooks, runtime reads/actions through RuntimeBackend.
type Service struct {
	store ports.Store
	hooks ports.RuntimeHooks

	endpointHooks  EndpointHooks
	endpointStatus func(name string) (EndpointStatus, bool)

	adminToken      string
	nodeBackend     NodeBackend
	leaseBackend    LeaseBackend
	viewBackend     ViewBackend
	sourceBackend   SourceBackend
	metricsBackend  MetricsBackend
	reqLogBackend   ReqLogBackend
	geoBackend      GeoBackend
	version         string
	startTime       time.Time
	bootConfig      SystemConfig
	apiMaxBodyBytes int64
}

// Option configures a Service.
type Option func(*Service)

// WithAdminToken sets the Bearer token guarding the control-plane API.
// How it gets there is the assembler's choice (config.Load +
// SetAdminToken, or NewService(..., WithAdminToken(...))).
// An empty token disables authentication — development only; config.Load
// requires a non-empty EGRESS_ADMIN_TOKEN in production.
func WithAdminToken(t string) Option {
	return func(s *Service) { s.adminToken = t }
}

// WithRuntimeBackend wires runtime-owned reads/actions (nodes, leases,
// metrics, request logs, geoip, probes). While absent those routes answer
// 501 NOT_IMPLEMENTED.
func WithRuntimeBackend(b RuntimeBackend) Option {
	return func(s *Service) {
		s.nodeBackend, s.leaseBackend, s.viewBackend = b, b, b
		s.sourceBackend, s.metricsBackend, s.reqLogBackend, s.geoBackend = b, b, b, b
	}
}

// WithVersion sets the version reported by GET /api/v1/system/info.
func WithVersion(v string) Option {
	return func(s *Service) { s.version = v }
}

// WithSystemConfig sets the boot-time config returned by
// GET /api/v1/system/config/default and used as the seed for the
// "runtime" config resource.
func WithSystemConfig(c SystemConfig) Option {
	return func(s *Service) { s.bootConfig = c }
}

// WithAPIMaxBodyBytes caps request body sizes on the API (default 8MiB).
func WithAPIMaxBodyBytes(n int64) Option {
	return func(s *Service) { s.apiMaxBodyBytes = n }
}

// WithEndpointHooks wires the ingress-layer endpoint change notifications
// and the status probe used to decorate endpoint reads.
func WithEndpointHooks(h EndpointHooks, status func(name string) (EndpointStatus, bool)) Option {
	return func(s *Service) { s.endpointHooks, s.endpointStatus = h, status }
}

// NewService builds a control-plane Service. hooks may be nil (treated as
// NopHooks) so the control plane can run before the runtime group wires up.
func NewService(store ports.Store, hooks ports.RuntimeHooks, opts ...Option) *Service {
	if hooks == nil {
		hooks = NopHooks{}
	}
	s := &Service{
		store:           store,
		hooks:           hooks,
		version:         "0.1.0",
		startTime:       time.Now(),
		apiMaxBodyBytes: 8 << 20,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// SetAdminToken sets the Bearer token after construction (alternative to
// WithAdminToken). Empty disables authentication (development only).
func (s *Service) SetAdminToken(t string) { s.adminToken = t }

// AdminToken returns the configured admin token (for wiring checks).
func (s *Service) AdminToken() string { return s.adminToken }

// ---------------------------------------------------------------------------
// Internal store helpers
// ---------------------------------------------------------------------------

func nowNs() int64 { return time.Now().UnixNano() }

// asInvalid marks a client-side validation failure so the API layer can
// answer 400 INVALID_ARGUMENT (ports.ErrInvalid).
func asInvalid(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ports.ErrInvalid, err)
}

// upsertResource creates or updates a resource document. On update the
// existing ID and CreatedAtNs are preserved; annotations from the caller win,
// except keys it does not mention are kept.
func (s *Service) upsertResource(ctx context.Context, kind ports.Kind, name, id string, spec any, annotations map[string]string) (ResourceDoc, error) {
	name = cleanName(name)
	existing, err := s.store.Get(ctx, kind, name)
	switch {
	case err == nil:
		doc, derr := decodeDoc(existing.Data, nil)
		if derr != nil {
			return ResourceDoc{}, derr
		}
		merged := mergeAnnotations(doc.Metadata.Annotations, annotations)
		if id == "" {
			id = doc.Metadata.ID
		}
		data, merr := encodeDoc(string(kind), name, id, spec, merged, doc.Metadata.CreatedAtNs, nowNs())
		if merr != nil {
			return ResourceDoc{}, merr
		}
		existing.Data = data
		if uerr := s.store.Update(ctx, existing); uerr != nil {
			return ResourceDoc{}, uerr
		}
		return decodeDoc(data, nil)
	case errors.Is(err, ports.ErrNotFound):
		data, merr := encodeDoc(string(kind), name, id, spec, annotations, 0, nowNs())
		if merr != nil {
			return ResourceDoc{}, merr
		}
		if cerr := s.store.Create(ctx, ports.Resource{Kind: kind, Name: name, Data: data}); cerr != nil {
			return ResourceDoc{}, cerr
		}
		return decodeDoc(data, nil)
	default:
		return ResourceDoc{}, err
	}
}

// wrapSpecPatch nests a resource-level JSON merge-patch under the envelope's
// "spec" key: the API contract patches the resource, the store holds the
// full apiVersion/kind/metadata/spec document.
func wrapSpecPatch(patch []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"spec":`)
	buf.Write(patch)
	buf.WriteByte('}')
	return buf.Bytes()
}

// setDrifted marks a resource as API-drifted (design §13: the file was the
// declared source, an API edit diverged from it).
func (s *Service) setDrifted(ctx context.Context, kind ports.Kind, name string) error {
	r, err := s.store.Get(ctx, kind, name)
	if err != nil {
		return err
	}
	doc, err := decodeDoc(r.Data, nil)
	if err != nil {
		return err
	}
	if doc.Metadata.Annotations == nil {
		doc.Metadata.Annotations = map[string]string{}
	}
	doc.Metadata.Annotations[AnnotationDrifted] = "true"
	doc.Metadata.UpdatedAtNs = nowNs()
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	r.Data = data
	return s.store.Update(ctx, r)
}

func mergeAnnotations(old, new map[string]string) map[string]string {
	if len(old) == 0 && len(new) == 0 {
		return nil
	}
	out := make(map[string]string, len(old)+len(new))
	for k, v := range old {
		out[k] = v
	}
	for k, v := range new {
		out[k] = v
	}
	return out
}

// getSpec loads a resource and decodes its spec.
func (s *Service) getSpec(ctx context.Context, kind ports.Kind, name string, out any) (ResourceDoc, error) {
	r, err := s.store.Get(ctx, kind, cleanName(name))
	if err != nil {
		return ResourceDoc{}, err
	}
	return decodeDoc(r.Data, out)
}

// newUUID returns 32 lowercase hex chars from crypto/rand.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("control: uuid: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// ---------------------------------------------------------------------------
// Platforms
// ---------------------------------------------------------------------------

// applyPlatformDefaults fills zero-value fields with spec defaults.
func applyPlatformDefaults(p *Platform) {
	if time.Duration(p.StickyTTL) <= 0 {
		p.StickyTTL = Duration(DefaultStickyTTL)
	}
	if p.Allocation == "" {
		p.Allocation = AllocationBalanced
	}
	if p.PassiveBreaker == "" {
		p.PassiveBreaker = PassiveBreakerOn
	}
	if p.IdentityPolicyRef == "" {
		p.IdentityPolicyRef = DefaultIdentityPolicyName
	}
}

// ApplyPlatform creates or updates a platform by name. A new platform gets a
// fresh UUID; an existing one keeps its ID. Notifies hooks.OnPlatformChanged.
// Resources written through the API are marked drifted (design §13).
func (s *Service) ApplyPlatform(ctx context.Context, p Platform) (string, error) {
	p.Name = cleanName(p.Name)
	applyPlatformDefaults(&p)
	// Reserved names (Default/api) cannot be *created*, but the existing
	// built-in Default platform may be updated.
	id := p.ID
	exists := false
	if r, err := s.store.Get(ctx, ports.KindPlatform, p.Name); err == nil {
		exists = true
		// Updates never change the UUID, even if the caller passes one.
		if doc, derr := decodeDoc(r.Data, nil); derr == nil {
			id = doc.Metadata.ID
		}
	} else if !errors.Is(err, ports.ErrNotFound) {
		return "", err
	}
	if err := p.Validate(exists); err != nil {
		return "", asInvalid(err)
	}
	if id == "" {
		var err error
		if id, err = newUUID(); err != nil {
			return "", err
		}
	}
	p.ID = id
	doc, err := s.upsertResource(ctx, ports.KindPlatform, p.Name, id, platformSpecOf(p),
		map[string]string{AnnotationDrifted: "true"})
	if err != nil {
		return "", err
	}
	syncPlatformMeta(&p, doc.Metadata)
	s.hooks.OnPlatformChanged(id)
	return id, nil
}

// platformSpecOf strips identity fields (they live in metadata).
func platformSpecOf(p Platform) Platform {
	p.ID, p.Name = "", ""
	p.CreatedAtNs, p.UpdatedAtNs = 0, 0
	return p
}

func syncPlatformMeta(p *Platform, m DocMetadata) {
	p.ID = m.ID
	p.Name = m.Name
	p.CreatedAtNs = m.CreatedAtNs
	p.UpdatedAtNs = m.UpdatedAtNs
}

// GetPlatform returns a platform addressed by ID or name.
func (s *Service) GetPlatform(ctx context.Context, idOrName string) (Platform, error) {
	p, _, err := s.resolvePlatform(ctx, idOrName)
	return p, err
}

func (s *Service) resolvePlatform(ctx context.Context, idOrName string) (Platform, ResourceDoc, error) {
	idOrName = cleanName(idOrName)
	// Fast path: store key is the name.
	if r, err := s.store.Get(ctx, ports.KindPlatform, idOrName); err == nil {
		var p Platform
		doc, derr := decodeDoc(r.Data, &p)
		if derr != nil {
			return Platform{}, ResourceDoc{}, derr
		}
		syncPlatformMeta(&p, doc.Metadata)
		return p, doc, nil
	} else if !errors.Is(err, ports.ErrNotFound) {
		return Platform{}, ResourceDoc{}, err
	}
	// Slow path: match by UUID.
	page, err := s.store.List(ctx, ports.KindPlatform, ports.ListOptions{Limit: 100000})
	if err != nil {
		return Platform{}, ResourceDoc{}, err
	}
	for _, r := range page.Items {
		var p Platform
		doc, derr := decodeDoc(r.Data, &p)
		if derr != nil {
			return Platform{}, ResourceDoc{}, derr
		}
		if doc.Metadata.ID == idOrName {
			syncPlatformMeta(&p, doc.Metadata)
			return p, doc, nil
		}
	}
	return Platform{}, ResourceDoc{}, fmt.Errorf("%w: platform %q", ports.ErrNotFound, idOrName)
}

// ListPlatforms returns a page of platforms ordered by name.
func (s *Service) ListPlatforms(ctx context.Context, limit, offset int) ([]Platform, int, error) {
	page, err := s.store.List(ctx, ports.KindPlatform, ports.ListOptions{Limit: limit, Offset: offset})
	if err != nil {
		return nil, 0, err
	}
	out := make([]Platform, 0, len(page.Items))
	for _, r := range page.Items {
		var p Platform
		doc, derr := decodeDoc(r.Data, &p)
		if derr != nil {
			return nil, 0, derr
		}
		syncPlatformMeta(&p, doc.Metadata)
		out = append(out, p)
	}
	return out, page.Total, nil
}

// PatchPlatform applies a JSON merge-patch to a platform.
func (s *Service) PatchPlatform(ctx context.Context, idOrName string, patch []byte) (Platform, error) {
	p, doc, err := s.resolvePlatform(ctx, idOrName)
	if err != nil {
		return Platform{}, err
	}
	r, err := s.store.Patch(ctx, ports.KindPlatform, doc.Metadata.Name, wrapSpecPatch(patch))
	if err != nil {
		return Platform{}, err
	}
	var updated Platform
	ndoc, derr := decodeDoc(r.Data, &updated)
	if derr != nil {
		return Platform{}, derr
	}
	// Never allow the patch to rename the platform or change its ID.
	updated.Name = p.Name
	syncPlatformMeta(&updated, ndoc.Metadata)
	applyPlatformDefaults(&updated)
	if verr := updated.Validate(platformNameReserved[updated.Name]); verr != nil {
		return Platform{}, asInvalid(verr)
	}
	if err := s.setDrifted(ctx, ports.KindPlatform, updated.Name); err != nil {
		return Platform{}, err
	}
	s.hooks.OnPlatformChanged(updated.ID)
	return updated, nil
}

// DeletePlatform deletes a platform addressed by ID or name.
func (s *Service) DeletePlatform(ctx context.Context, idOrName string) error {
	p, doc, err := s.resolvePlatform(ctx, idOrName)
	if err != nil {
		return err
	}
	_ = p
	if err := s.store.Delete(ctx, ports.KindPlatform, doc.Metadata.Name); err != nil {
		return err
	}
	s.hooks.OnPlatformDeleted(doc.Metadata.ID)
	return nil
}

// ResetPlatform resets a platform to the built-in defaults, keeping its
// name and ID.
func (s *Service) ResetPlatform(ctx context.Context, idOrName string) (Platform, error) {
	p, _, err := s.resolvePlatform(ctx, idOrName)
	if err != nil {
		return Platform{}, err
	}
	p.StickyTTL = Duration(DefaultStickyTTL)
	p.Allocation = AllocationBalanced
	p.PassiveBreaker = PassiveBreakerOn
	p.IdentityPolicyRef = DefaultIdentityPolicyName
	p.Filter = PlatformFilter{}
	return s.applyPlatformKeepID(ctx, p)
}

func (s *Service) applyPlatformKeepID(ctx context.Context, p Platform) (Platform, error) {
	applyPlatformDefaults(&p)
	if err := p.Validate(platformNameReserved[p.Name]); err != nil {
		return Platform{}, asInvalid(err)
	}
	doc, err := s.upsertResource(ctx, ports.KindPlatform, p.Name, p.ID, platformSpecOf(p),
		map[string]string{AnnotationDrifted: "true"})
	if err != nil {
		return Platform{}, err
	}
	syncPlatformMeta(&p, doc.Metadata)
	s.hooks.OnPlatformChanged(p.ID)
	return p, nil
}

// ---------------------------------------------------------------------------
// IdentityPolicies
// ---------------------------------------------------------------------------

// ApplyIdentityPolicy creates or updates an identity policy. Platforms that
// reference it are notified via hooks.OnPlatformChanged so the runtime can
// recompile their resolution chains.
func (s *Service) ApplyIdentityPolicy(ctx context.Context, p IdentityPolicy) error {
	p.Name = cleanName(p.Name)
	if err := p.Validate(); err != nil {
		return asInvalid(err)
	}
	spec := p
	spec.Name = ""
	if _, err := s.upsertResource(ctx, ports.KindIdentityPolicy, p.Name, p.Name, spec,
		map[string]string{AnnotationDrifted: "true"}); err != nil {
		return err
	}
	s.notifyPolicyReferencers(ctx, p.Name)
	return nil
}

// GetIdentityPolicy returns a policy by name.
func (s *Service) GetIdentityPolicy(ctx context.Context, name string) (IdentityPolicy, error) {
	var p IdentityPolicy
	doc, err := s.getSpec(ctx, ports.KindIdentityPolicy, name, &p)
	if err != nil {
		return IdentityPolicy{}, err
	}
	p.Name = doc.Metadata.Name
	p.CreatedAtNs = doc.Metadata.CreatedAtNs
	p.UpdatedAtNs = doc.Metadata.UpdatedAtNs
	return p, nil
}

// ListIdentityPolicies returns a page of policies ordered by name.
func (s *Service) ListIdentityPolicies(ctx context.Context, limit, offset int) ([]IdentityPolicy, int, error) {
	page, err := s.store.List(ctx, ports.KindIdentityPolicy, ports.ListOptions{Limit: limit, Offset: offset})
	if err != nil {
		return nil, 0, err
	}
	out := make([]IdentityPolicy, 0, len(page.Items))
	for _, r := range page.Items {
		var p IdentityPolicy
		doc, derr := decodeDoc(r.Data, &p)
		if derr != nil {
			return nil, 0, derr
		}
		p.Name = doc.Metadata.Name
		p.CreatedAtNs = doc.Metadata.CreatedAtNs
		p.UpdatedAtNs = doc.Metadata.UpdatedAtNs
		out = append(out, p)
	}
	return out, page.Total, nil
}

// PatchIdentityPolicy applies a JSON merge-patch to a policy.
func (s *Service) PatchIdentityPolicy(ctx context.Context, name string, patch []byte) (IdentityPolicy, error) {
	r, err := s.store.Patch(ctx, ports.KindIdentityPolicy, cleanName(name), wrapSpecPatch(patch))
	if err != nil {
		return IdentityPolicy{}, err
	}
	var p IdentityPolicy
	doc, derr := decodeDoc(r.Data, &p)
	if derr != nil {
		return IdentityPolicy{}, derr
	}
	p.Name = doc.Metadata.Name
	if verr := p.Validate(); verr != nil {
		return IdentityPolicy{}, asInvalid(verr)
	}
	if err := s.setDrifted(ctx, ports.KindIdentityPolicy, p.Name); err != nil {
		return IdentityPolicy{}, err
	}
	s.notifyPolicyReferencers(ctx, p.Name)
	return p, nil
}

// DeleteIdentityPolicy deletes a policy. A policy referenced by any platform
// is rejected with a conflict error (openapi 409).
func (s *Service) DeleteIdentityPolicy(ctx context.Context, name string) error {
	name = cleanName(name)
	platforms, _, err := s.ListPlatforms(ctx, 100000, 0)
	if err != nil {
		return err
	}
	for _, p := range platforms {
		if p.IdentityPolicyRef == name {
			return fmt.Errorf("%w: identity policy %q is referenced by platform %q",
				ports.ErrConflict, name, p.Name)
		}
	}
	return s.store.Delete(ctx, ports.KindIdentityPolicy, name)
}

// notifyPolicyReferencers tells the runtime to recompile every platform that
// references the named policy.
func (s *Service) notifyPolicyReferencers(ctx context.Context, policyName string) {
	platforms, _, err := s.ListPlatforms(ctx, 100000, 0)
	if err != nil {
		return
	}
	for _, p := range platforms {
		if p.IdentityPolicyRef == policyName {
			s.hooks.OnPlatformChanged(p.ID)
		}
	}
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

// ApplySource creates or updates a source by name.
func (s *Service) ApplySource(ctx context.Context, src Source) error {
	src.Name = cleanName(src.Name)
	if src.Type == "" {
		return asInvalid(fmt.Errorf("control: source %q: type is required", src.Name))
	}
	if time.Duration(src.UpdateInterval) == 0 {
		src.UpdateInterval = Duration(5 * time.Minute)
	}
	if err := src.Validate(); err != nil {
		return asInvalid(err)
	}
	spec := src
	spec.ID, spec.Name = "", ""
	spec.CreatedAtNs, spec.UpdatedAtNs = 0, 0
	if _, err := s.upsertResource(ctx, ports.KindSource, src.Name, src.Name, spec,
		map[string]string{AnnotationDrifted: "true"}); err != nil {
		return err
	}
	s.hooks.OnSourceChanged(src.Name)
	return nil
}

// GetSource returns a source by name.
func (s *Service) GetSource(ctx context.Context, name string) (Source, error) {
	var src Source
	doc, err := s.getSpec(ctx, ports.KindSource, name, &src)
	if err != nil {
		return Source{}, err
	}
	src.ID = doc.Metadata.ID
	src.Name = doc.Metadata.Name
	src.CreatedAtNs = doc.Metadata.CreatedAtNs
	src.UpdatedAtNs = doc.Metadata.UpdatedAtNs
	return src, nil
}

// ListSources returns a page of sources ordered by name.
func (s *Service) ListSources(ctx context.Context, limit, offset int) ([]Source, int, error) {
	page, err := s.store.List(ctx, ports.KindSource, ports.ListOptions{Limit: limit, Offset: offset})
	if err != nil {
		return nil, 0, err
	}
	out := make([]Source, 0, len(page.Items))
	for _, r := range page.Items {
		var src Source
		doc, derr := decodeDoc(r.Data, &src)
		if derr != nil {
			return nil, 0, derr
		}
		src.ID = doc.Metadata.ID
		src.Name = doc.Metadata.Name
		src.CreatedAtNs = doc.Metadata.CreatedAtNs
		src.UpdatedAtNs = doc.Metadata.UpdatedAtNs
		out = append(out, src)
	}
	return out, page.Total, nil
}

// PatchSource applies a JSON merge-patch to a source.
func (s *Service) PatchSource(ctx context.Context, name string, patch []byte) (Source, error) {
	r, err := s.store.Patch(ctx, ports.KindSource, cleanName(name), wrapSpecPatch(patch))
	if err != nil {
		return Source{}, err
	}
	var src Source
	doc, derr := decodeDoc(r.Data, &src)
	if derr != nil {
		return Source{}, derr
	}
	src.ID = doc.Metadata.ID
	src.Name = doc.Metadata.Name
	if verr := src.Validate(); verr != nil {
		return Source{}, asInvalid(verr)
	}
	if err := s.setDrifted(ctx, ports.KindSource, src.Name); err != nil {
		return Source{}, err
	}
	s.hooks.OnSourceChanged(src.Name)
	return src, nil
}

// DeleteSource deletes a source by name.
func (s *Service) DeleteSource(ctx context.Context, name string) error {
	name = cleanName(name)
	if err := s.store.Delete(ctx, ports.KindSource, name); err != nil {
		return err
	}
	s.hooks.OnSourceDeleted(name)
	return nil
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

// ApplyEndpoint creates or updates an endpoint by name.
func (s *Service) ApplyEndpoint(ctx context.Context, e Endpoint) error {
	e.Name = cleanName(e.Name)
	// Creation default per spec/resources/endpoint.md; an update keeps the
	// stored enabled state unless the caller sets it explicitly (PATCH).
	if _, err := s.store.Get(ctx, ports.KindEndpoint, e.Name); errors.Is(err, ports.ErrNotFound) {
		e.Enabled = true
	} else if err != nil {
		return err
	} else {
		cur, gerr := s.GetEndpoint(ctx, e.Name)
		if gerr != nil {
			return gerr
		}
		e.Enabled = cur.Enabled
	}
	if err := e.Validate(); err != nil {
		return asInvalid(err)
	}
	spec := e
	spec.ID, spec.Name = "", ""
	spec.CreatedAtNs, spec.UpdatedAtNs = 0, 0
	_, err := s.upsertResource(ctx, ports.KindEndpoint, e.Name, e.Name, spec,
		map[string]string{AnnotationDrifted: "true"})
	if err != nil {
		return err
	}
	s.notifyEndpointChanged(e.Name)
	return nil
}

// GetEndpoint returns an endpoint by name. The env-defined default
// endpoint is synthesized on the fly (never stored, read-only).
func (s *Service) GetEndpoint(ctx context.Context, name string) (Endpoint, error) {
	if cleanName(name) == DefaultEndpointName {
		return s.defaultEndpoint(), nil
	}
	var e Endpoint
	doc, err := s.getSpec(ctx, ports.KindEndpoint, name, &e)
	if err != nil {
		return Endpoint{}, err
	}
	e.ID = doc.Metadata.ID
	e.Name = doc.Metadata.Name
	e.CreatedAtNs = doc.Metadata.CreatedAtNs
	e.UpdatedAtNs = doc.Metadata.UpdatedAtNs
	s.decorateEndpoint(&e)
	return e, nil
}

// ListEndpoints returns a page of endpoints ordered by name. The
// synthesized default endpoint sorts first on the first page.
func (s *Service) ListEndpoints(ctx context.Context, limit, offset int) ([]Endpoint, int, error) {
	page, err := s.store.List(ctx, ports.KindEndpoint, ports.ListOptions{Limit: limit, Offset: offset})
	if err != nil {
		return nil, 0, err
	}
	out := make([]Endpoint, 0, len(page.Items)+1)
	if offset == 0 {
		out = append(out, s.defaultEndpoint())
	}
	for _, r := range page.Items {
		var e Endpoint
		doc, derr := decodeDoc(r.Data, &e)
		if derr != nil {
			return nil, 0, derr
		}
		e.ID = doc.Metadata.ID
		e.Name = doc.Metadata.Name
		e.CreatedAtNs = doc.Metadata.CreatedAtNs
		e.UpdatedAtNs = doc.Metadata.UpdatedAtNs
		s.decorateEndpoint(&e)
		out = append(out, e)
	}
	return out, page.Total + 1, nil
}

// defaultEndpoint synthesizes the env-defined default endpoint
// (spec/resources/endpoint.md: 环境变量定义的默认入口在运行时合成，永不存入资源).
func (s *Service) defaultEndpoint() Endpoint {
	e := Endpoint{
		ID:                   DefaultEndpointName,
		Name:                 DefaultEndpointName,
		Port:                 s.bootConfig.Port,
		Enabled:              true,
		AllowManagement:      true,
		AllowProxy:           true,
		RequireProxyAuthInfo: false,
		AllowHTTPForward:     true,
		AllowHTTPReverse:     true,
		AllowSOCKS5:          true,
		ReadOnly:             true,
	}
	s.decorateEndpoint(&e)
	return e
}

// decorateEndpoint fills the runtime fields (listening, last_error) via
// the wired status probe.
func (s *Service) decorateEndpoint(e *Endpoint) {
	if s.endpointStatus == nil {
		return
	}
	if st, ok := s.endpointStatus(e.Name); ok {
		e.Listening = st.Listening
		e.LastError = st.LastError
	}
}

// notifyEndpointChanged fires the ingress-layer hook, if wired.
func (s *Service) notifyEndpointChanged(name string) {
	if s.endpointHooks != nil {
		s.endpointHooks.OnEndpointChanged(name)
	}
}

// notifyEndpointDeleted fires the ingress-layer hook, if wired.
func (s *Service) notifyEndpointDeleted(name string) {
	if s.endpointHooks != nil {
		s.endpointHooks.OnEndpointDeleted(name)
	}
}

// PatchEndpoint applies a JSON merge-patch to an endpoint.
func (s *Service) PatchEndpoint(ctx context.Context, name string, patch []byte) (Endpoint, error) {
	r, err := s.store.Patch(ctx, ports.KindEndpoint, cleanName(name), wrapSpecPatch(patch))
	if err != nil {
		return Endpoint{}, err
	}
	var e Endpoint
	doc, derr := decodeDoc(r.Data, &e)
	if derr != nil {
		return Endpoint{}, derr
	}
	e.ID = doc.Metadata.ID
	e.Name = doc.Metadata.Name
	if verr := e.Validate(); verr != nil {
		return Endpoint{}, asInvalid(verr)
	}
	if err := s.setDrifted(ctx, ports.KindEndpoint, e.Name); err != nil {
		return Endpoint{}, err
	}
	s.notifyEndpointChanged(e.Name)
	return e, nil
}

// DeleteEndpoint deletes an endpoint by name.
func (s *Service) DeleteEndpoint(ctx context.Context, name string) error {
	name = cleanName(name)
	if name == DefaultEndpointName {
		return asInvalid(fmt.Errorf("control: the default endpoint is read-only"))
	}
	if err := s.store.Delete(ctx, ports.KindEndpoint, name); err != nil {
		return err
	}
	s.notifyEndpointDeleted(name)
	return nil
}

// ---------------------------------------------------------------------------
// Recipes (import CRUD; apply lives in recipe.go)
// ---------------------------------------------------------------------------

// ImportRecipe creates or updates a recipe (POST /api/v1/recipes).
func (s *Service) ImportRecipe(ctx context.Context, r Recipe) error {
	r.Name = cleanName(r.Name)
	if err := r.Validate(); err != nil {
		return asInvalid(err)
	}
	spec := r
	spec.Name = ""
	spec.CreatedAtNs, spec.UpdatedAtNs = 0, 0
	_, err := s.upsertResource(ctx, ports.KindRecipe, r.Name, r.Name, spec,
		map[string]string{AnnotationDrifted: "true"})
	return err
}

// GetRecipe returns a recipe by name.
func (s *Service) GetRecipe(ctx context.Context, name string) (Recipe, error) {
	var r Recipe
	doc, err := s.getSpec(ctx, ports.KindRecipe, name, &r)
	if err != nil {
		return Recipe{}, err
	}
	r.Name = doc.Metadata.Name
	r.CreatedAtNs = doc.Metadata.CreatedAtNs
	r.UpdatedAtNs = doc.Metadata.UpdatedAtNs
	return r, nil
}

// ListRecipes returns a page of recipes ordered by name.
func (s *Service) ListRecipes(ctx context.Context, limit, offset int) ([]Recipe, int, error) {
	page, err := s.store.List(ctx, ports.KindRecipe, ports.ListOptions{Limit: limit, Offset: offset})
	if err != nil {
		return nil, 0, err
	}
	out := make([]Recipe, 0, len(page.Items))
	for _, r := range page.Items {
		var rec Recipe
		doc, derr := decodeDoc(r.Data, &rec)
		if derr != nil {
			return nil, 0, derr
		}
		rec.Name = doc.Metadata.Name
		rec.CreatedAtNs = doc.Metadata.CreatedAtNs
		rec.UpdatedAtNs = doc.Metadata.UpdatedAtNs
		out = append(out, rec)
	}
	return out, page.Total, nil
}

// DeleteRecipe deletes a recipe by name.
func (s *Service) DeleteRecipe(ctx context.Context, name string) error {
	return s.store.Delete(ctx, ports.KindRecipe, cleanName(name))
}

// ---------------------------------------------------------------------------
// Config resource (ports.KindConfig)
// ---------------------------------------------------------------------------

// GetSystemConfig returns the "runtime" config resource, falling back to the
// boot config when no resource was ever stored.
func (s *Service) GetSystemConfig(ctx context.Context) (SystemConfig, error) {
	var c SystemConfig
	_, err := s.getSpec(ctx, ports.KindConfig, SystemConfigResourceName, &c)
	if errors.Is(err, ports.ErrNotFound) {
		return s.bootConfig, nil
	}
	return c, err
}

// PatchSystemConfig applies a JSON merge-patch to the "runtime" config
// resource, creating it from the boot config when absent.
func (s *Service) PatchSystemConfig(ctx context.Context, patch []byte) (SystemConfig, error) {
	if _, err := s.store.Get(ctx, ports.KindConfig, SystemConfigResourceName); errors.Is(err, ports.ErrNotFound) {
		data, merr := encodeDoc(string(ports.KindConfig), SystemConfigResourceName, SystemConfigResourceName, s.bootConfig, nil, 0, nowNs())
		if merr != nil {
			return SystemConfig{}, merr
		}
		if cerr := s.store.Create(ctx, ports.Resource{Kind: ports.KindConfig, Name: SystemConfigResourceName, Data: data}); cerr != nil {
			return SystemConfig{}, cerr
		}
	} else if err != nil {
		return SystemConfig{}, err
	}
	r, err := s.store.Patch(ctx, ports.KindConfig, SystemConfigResourceName, wrapSpecPatch(patch))
	if err != nil {
		return SystemConfig{}, err
	}
	var c SystemConfig
	if _, derr := decodeDoc(r.Data, &c); derr != nil {
		return SystemConfig{}, derr
	}
	if err := s.setDrifted(ctx, ports.KindConfig, SystemConfigResourceName); err != nil {
		return SystemConfig{}, err
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// ports.PlatformSource / ports.SourceSource
// ---------------------------------------------------------------------------

// GetPlatformSpec assembles the runtime compile input for a platform:
// filter → predicate.Filter, identityPolicyRef → identity.Policy.
func (s *Service) GetPlatformSpec(ctx context.Context, id string) (ports.PlatformSpec, error) {
	p, _, err := s.resolvePlatform(ctx, id)
	if err != nil {
		return ports.PlatformSpec{}, err
	}
	policy, err := s.compilePolicy(ctx, p.IdentityPolicyRef)
	if err != nil {
		return ports.PlatformSpec{}, err
	}
	ttl := time.Duration(p.StickyTTL)
	if ttl <= 0 {
		ttl = DefaultStickyTTL
	}
	return ports.PlatformSpec{
		ID:             p.ID,
		Name:           p.Name,
		Filter:         toPredicateFilter(p.Filter),
		Policy:         policy,
		StickyTTL:      ttl,
		Allocation:     p.Allocation,
		PassiveBreaker: p.PassiveBreaker,
	}, nil
}

// GetSourceSpec assembles the source declaration for a source name.
func (s *Service) GetSourceSpec(ctx context.Context, name string) (ports.SourceSpec, error) {
	src, err := s.GetSource(ctx, name)
	if err != nil {
		return ports.SourceSpec{}, err
	}
	return ports.SourceSpec{
		Name:           src.Name,
		Kind:           src.Type,
		URL:            src.URL,
		Content:        src.Content,
		UpdateInterval: time.Duration(src.UpdateInterval),
		EvictDelay:     time.Duration(src.EvictDelay),
	}, nil
}

// toPredicateFilter converts a platform filter declaration into the kernel
// predicate.Filter (compilation of regexes happens in predicate.Compile on
// the runtime side).
func toPredicateFilter(f PlatformFilter) predicate.Filter {
	attrs := make(map[string]predicate.AttrCondition, len(f.Attrs))
	for k, c := range f.Attrs {
		attrs[k] = predicate.AttrCondition{
			In:       c.In,
			NotIn:    c.NotIn,
			Match:    c.Match,
			NotMatch: c.NotMatch,
			Gte:      string(c.Gte),
			Lte:      string(c.Lte),
			HasGte:   c.Gte != "",
			HasLte:   c.Lte != "",
		}
	}
	return predicate.Filter{Tags: f.Tags, Attrs: attrs}
}

// compilePolicy resolves an IdentityPolicy resource into identity.Policy.
func (s *Service) compilePolicy(ctx context.Context, name string) (identity.Policy, error) {
	if name == "" {
		name = DefaultIdentityPolicyName
	}
	p, err := s.GetIdentityPolicy(ctx, name)
	if err != nil {
		return identity.Policy{}, fmt.Errorf("control: identity policy %q: %w", name, err)
	}
	return toIdentityPolicy(p)
}

// toIdentityPolicy maps resource chain steps to kernel identity steps.
func toIdentityPolicy(p IdentityPolicy) (identity.Policy, error) {
	pol := identity.Policy{}
	for i, st := range p.Chain {
		var step identity.Step
		switch {
		case st.Header != nil:
			step = identity.HeaderStep(*st.Header)
		case st.Wire != nil:
			step = identity.WireStep()
		case st.HeaderRule != nil:
			rules := make([]identity.HeaderRule, 0, len(st.HeaderRule.Rules))
			for _, r := range st.HeaderRule.Rules {
				rules = append(rules, identity.HeaderRule{Prefix: r.Prefix, Headers: r.Headers})
			}
			step = identity.HeaderRuleStep(rules)
		case st.Resolver != nil:
			step = identity.ResolverStep(*st.Resolver)
		default:
			return identity.Policy{}, fmt.Errorf("control: chain[%d]: no step variant set", i)
		}
		pol.Chain = append(pol.Chain, step)
	}
	switch p.OnMiss {
	case OnMissRandom:
		pol.OnMiss = identity.OnMissRandom
	case OnMissReject:
		pol.OnMiss = identity.OnMissReject
	default:
		return identity.Policy{}, fmt.Errorf("control: invalid on_miss %q", p.OnMiss)
	}
	return pol, nil
}
