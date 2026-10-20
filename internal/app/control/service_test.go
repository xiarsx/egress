package control

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/predicate"
	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/internal/testutil/memstore"
	pknode "github.com/xiarsx/egress/pkg/node"
)

func yamlUnmarshal(b []byte, v any) error { return yaml.Unmarshal(b, v) }

// recordHooks records hook calls for assertions.
type recordHooks struct {
	mu              sync.Mutex
	platformChanged []string
	platformDeleted []string
	sourceChanged   []string
	sourceDeleted   []string
}

func (h *recordHooks) OnPlatformChanged(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.platformChanged = append(h.platformChanged, id)
}
func (h *recordHooks) OnPlatformDeleted(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.platformDeleted = append(h.platformDeleted, id)
}
func (h *recordHooks) OnSourceChanged(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sourceChanged = append(h.sourceChanged, name)
}
func (h *recordHooks) OnSourceDeleted(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sourceDeleted = append(h.sourceDeleted, name)
}

func compileForTest(spec ports.PlatformSpec) (pknode.Predicate, error) {
	return predicate.Compile(spec.Filter)
}

func testService() (*Service, *memstore.Store, *recordHooks) {
	st := memstore.New()
	h := &recordHooks{}
	s := NewService(st, h)
	return s, st, h
}

func strp(s string) *string { return &s }

func TestPlatformCRUD(t *testing.T) {
	s, _, h := testService()
	ctx := context.Background()

	id, err := s.ApplyPlatform(ctx, Platform{Name: "video", Allocation: "BALANCED"})
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 32 {
		t.Fatalf("expected 32-hex UUID, got %q", id)
	}
	if len(h.platformChanged) != 1 || h.platformChanged[0] != id {
		t.Fatalf("expected OnPlatformChanged(%s), got %+v", id, h.platformChanged)
	}

	// Get by name and by ID.
	byName, err := s.GetPlatform(ctx, "video")
	if err != nil {
		t.Fatal(err)
	}
	byID, err := s.GetPlatform(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if byName.ID != id || byID.Name != "video" {
		t.Fatalf("mismatch: %+v %+v", byName, byID)
	}
	// Defaults applied.
	if time.Duration(byName.StickyTTL) != DefaultStickyTTL {
		t.Fatalf("stickyTTL default = %v", byName.StickyTTL)
	}
	if byName.PassiveBreaker != PassiveBreakerOn || byName.IdentityPolicyRef != "default" {
		t.Fatalf("defaults not applied: %+v", byName)
	}

	// Update keeps the ID.
	id2, err := s.ApplyPlatform(ctx, Platform{Name: "video", Allocation: "PREFER_LOW_LATENCY"})
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id {
		t.Fatalf("ID changed on update: %s -> %s", id, id2)
	}

	// Reserved names cannot be created.
	if _, err := s.ApplyPlatform(ctx, Platform{Name: "api"}); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("expected ErrInvalid for reserved name, got %v", err)
	}

	// Delete by name; hook fires with the UUID.
	if err := s.DeletePlatform(ctx, "video"); err != nil {
		t.Fatal(err)
	}
	if len(h.platformDeleted) != 1 || h.platformDeleted[0] != id {
		t.Fatalf("expected OnPlatformDeleted(%s), got %+v", id, h.platformDeleted)
	}
	if _, err := s.GetPlatform(ctx, id); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestPlatformPatchDrift(t *testing.T) {
	s, st, _ := testService()
	ctx := context.Background()
	id, err := s.ApplyPlatform(ctx, Platform{Name: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	_ = id
	p, err := s.PatchPlatform(ctx, "p1", []byte(`{"sticky_ttl":"24h"}`))
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(p.StickyTTL) != 24*time.Hour {
		t.Fatalf("patch did not apply: %v", p.StickyTTL)
	}
	r, _ := st.Get(ctx, ports.KindPlatform, "p1")
	doc, _ := DecodeDoc(r.Data)
	if doc.Metadata.Annotations[AnnotationDrifted] != "true" {
		t.Fatalf("expected drifted annotation, got %v", doc.Metadata.Annotations)
	}
}

func TestIdentityPolicyDeleteConflict(t *testing.T) {
	s, _, _ := testService()
	ctx := context.Background()
	header := "X-Account"
	if err := s.ApplyIdentityPolicy(ctx, IdentityPolicy{
		Name:   "strict",
		Chain:  []PolicyStep{{Header: &header}},
		OnMiss: OnMissReject,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyPlatform(ctx, Platform{Name: "p", IdentityPolicyRef: "strict"}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteIdentityPolicy(ctx, "strict"); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
	// Unreferenced policy deletes fine.
	if err := s.ApplyIdentityPolicy(ctx, IdentityPolicy{
		Name:   "unused",
		Chain:  []PolicyStep{{Wire: &struct{}{}}},
		OnMiss: OnMissRandom,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteIdentityPolicy(ctx, "unused"); err != nil {
		t.Fatal(err)
	}
}

// TestPlatformSpecAssembly checks the Platform → predicate.Filter and
// IdentityPolicy → identity.Policy assembly used by GetPlatformSpec.
func TestPlatformSpecAssembly(t *testing.T) {
	s, _, _ := testService()
	ctx := context.Background()

	header := "X-Egress-Account"
	if err := s.ApplyIdentityPolicy(ctx, IdentityPolicy{
		Name: "default",
		Chain: []PolicyStep{
			{Header: &header},
			{Wire: &struct{}{}},
			{HeaderRule: &HeaderRuleSpec{Rules: []HeaderRuleItem{
				{Prefix: "*", Headers: []string{"Authorization", "x-api-key"}},
			}}},
		},
		OnMiss: OnMissRandom,
	}); err != nil {
		t.Fatal(err)
	}
	id, err := s.ApplyPlatform(ctx, Platform{
		Name:      "video",
		StickyTTL: Duration(72 * time.Hour),
		Filter: PlatformFilter{
			Tags: []string{"^sub-a/", "*hk|jp", "!expired"},
			Attrs: map[string]AttrCondition{
				"geo.country": {In: []string{"hk", "jp"}},
				"asn.org":     {NotMatch: "(?i)cloud|hosting"},
				"asn.number":  {Gte: "100", Lte: "200"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	spec, err := s.GetPlatformSpec(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if spec.ID != id || spec.Name != "video" {
		t.Fatalf("spec identity: %+v", spec)
	}
	if spec.StickyTTL != 72*time.Hour {
		t.Fatalf("stickyTTL = %v", spec.StickyTTL)
	}
	// Filter assembly.
	if len(spec.Filter.Tags) != 3 || spec.Filter.Tags[0] != "^sub-a/" {
		t.Fatalf("tags = %v", spec.Filter.Tags)
	}
	cc := spec.Filter.Attrs["geo.country"]
	if len(cc.In) != 2 || cc.In[0] != "hk" {
		t.Fatalf("geo.country = %+v", cc)
	}
	org := spec.Filter.Attrs["asn.org"]
	if org.NotMatch != "(?i)cloud|hosting" {
		t.Fatalf("asn.org = %+v", org)
	}
	num := spec.Filter.Attrs["asn.number"]
	if !num.HasGte || !num.HasLte || num.Gte != "100" || num.Lte != "200" {
		t.Fatalf("asn.number = %+v", num)
	}
	// Policy assembly.
	if len(spec.Policy.Chain) != 3 {
		t.Fatalf("chain len = %d", len(spec.Policy.Chain))
	}
	st0 := spec.Policy.Chain[0]
	if st0.Kind != identity.StepHeader || st0.Header != "X-Egress-Account" {
		t.Fatalf("step0 = %+v", st0)
	}
	if spec.Policy.Chain[1].Kind != identity.StepWire {
		t.Fatalf("step1 = %+v", spec.Policy.Chain[1])
	}
	st2 := spec.Policy.Chain[2]
	if st2.Kind != identity.StepHeaderRule || len(st2.Rules) != 1 || st2.Rules[0].Prefix != "*" {
		t.Fatalf("step2 = %+v", st2)
	}
	if spec.Policy.OnMiss != identity.OnMissRandom {
		t.Fatalf("onMiss = %v", spec.Policy.OnMiss)
	}

	// The compiled filter must actually match nodes.
	pred, err := compileForTest(spec)
	if err != nil {
		t.Fatal(err)
	}
	match := pknode.NodeSnapshot{
		// One tag satisfies both the ANY (^sub-a/) and MUST (hk|jp) rules.
		Tags: []pknode.ScopedTag{"sub-a/hk-node"},
		Attrs: map[pknode.AttrKey]pknode.AttrValue{
			"geo.country": {Str: "hk"},
			"asn.org":     {Str: "Example ISP"},
			"asn.number":  {Str: "150", IsNum: true, Num: 150},
		},
	}
	if !pred(match) {
		t.Fatal("expected node to match")
	}
	noMatch := match
	noMatch.Tags = []pknode.ScopedTag{"sub-a/node1", "expired"}
	if pred(noMatch) {
		t.Fatal("MUST_NOT tag should reject")
	}
	cloud := match
	cloud.Attrs["asn.org"] = pknode.AttrValue{Str: "Cloud Hosting Ltd"}
	if pred(cloud) {
		t.Fatal("notMatch attr should reject")
	}
}

func TestSourceSpecAssembly(t *testing.T) {
	s, _, h := testService()
	ctx := context.Background()
	if err := s.ApplySource(ctx, Source{
		Name: "main", Type: "remote", URL: "https://example.com/sub.txt",
	}); err != nil {
		t.Fatal(err)
	}
	if len(h.sourceChanged) != 1 {
		t.Fatalf("hooks: %+v", h.sourceChanged)
	}
	spec, err := s.GetSourceSpec(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Name != "main" || spec.Kind != "remote" || spec.URL != "https://example.com/sub.txt" {
		t.Fatalf("spec = %+v", spec)
	}
	if spec.UpdateInterval != 5*time.Minute {
		t.Fatalf("default updateInterval = %v", spec.UpdateInterval)
	}
	// remote faster than 30s is rejected.
	if err := s.ApplySource(ctx, Source{Name: "fast", Type: "remote", URL: "https://x", UpdateInterval: Duration(10 * time.Second)}); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
	// local requires content.
	if err := s.ApplySource(ctx, Source{Name: "l", Type: "local"}); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
	if err := s.DeleteSource(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	if len(h.sourceDeleted) != 1 {
		t.Fatalf("hooks: %+v", h.sourceDeleted)
	}
}

func TestApplyRecipe(t *testing.T) {
	s, _, _ := testService()
	ctx := context.Background()

	header := "X-Egress-Account"
	if err := s.ImportRecipe(ctx, Recipe{
		Name:    "example-api",
		Version: "1.2.0",
		IdentityPolicy: &IdentityPolicySpec{
			Chain:  []PolicyStep{{Header: &header}, {Wire: &struct{}{}}},
			OnMiss: OnMissRandom,
		},
		PlatformTemplate: &PlatformTemplateSpec{
			StickyTTL:  Duration(168 * time.Hour),
			Allocation: AllocationBalanced,
			Filter: PlatformFilter{
				Attrs: map[string]AttrCondition{"asn.org": {NotMatch: "(?i)cloud"}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := s.ApplyRecipe(ctx, "example-api", "shop-api")
	if err != nil {
		t.Fatal(err)
	}
	if res.Platform.Name != "shop-api" {
		t.Fatalf("platform = %+v", res.Platform)
	}
	if res.IdentityPolicy == nil || res.IdentityPolicy.Name != "shop-api-identity" {
		t.Fatalf("policy = %+v", res.IdentityPolicy)
	}
	if res.Platform.IdentityPolicyRef != "shop-api-identity" {
		t.Fatalf("ref = %q", res.Platform.IdentityPolicyRef)
	}
	if len(res.IdentityPolicy.Chain) != 2 {
		t.Fatalf("chain = %+v", res.IdentityPolicy.Chain)
	}

	// requires: unsatisfied egress version.
	if err := s.ImportRecipe(ctx, Recipe{Name: "future", Version: "1.0.0", Requires: []string{"egress>=99.0"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyRecipe(ctx, "future", "p2"); !errors.Is(err, ErrUnprocessable) {
		t.Fatalf("expected ErrUnprocessable, got %v", err)
	}
	// requires: missing extension.
	if err := s.ImportRecipe(ctx, Recipe{Name: "needext", Version: "1.0.0", Requires: []string{"ext:asn>=1"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyRecipe(ctx, "needext", "p3"); !errors.Is(err, ErrUnprocessable) {
		t.Fatalf("expected ErrUnprocessable, got %v", err)
	}
	// satisfied egress requirement passes.
	if err := s.ImportRecipe(ctx, Recipe{Name: "ok", Version: "1.0.0", Requires: []string{"egress>=0.1.0"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyRecipe(ctx, "ok", "p4"); err != nil {
		t.Fatal(err)
	}
	// missing recipe.
	if _, err := s.ApplyRecipe(ctx, "nope", "p5"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestEndpointCRUD(t *testing.T) {
	s, _, _ := testService()
	ctx := context.Background()
	if err := s.ApplyEndpoint(ctx, Endpoint{Name: "socks-only", Port: 1080, AllowSOCKS5: true}); err != nil {
		t.Fatal(err)
	}
	e, err := s.GetEndpoint(ctx, "socks-only")
	if err != nil {
		t.Fatal(err)
	}
	if !e.Enabled || e.Port != 1080 {
		t.Fatalf("endpoint = %+v", e)
	}
	if err := s.ApplyEndpoint(ctx, Endpoint{Name: "bad", Port: 99999}); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
	if err := s.DeleteEndpoint(ctx, "socks-only"); err != nil {
		t.Fatal(err)
	}
}

func TestSystemConfigResource(t *testing.T) {
	svc, _, _ := testService()
	ctx := context.Background()
	svc.bootConfig = SystemConfig{ListenAddress: "0.0.0.0", Port: 2260, StateDir: "/s", CacheDir: "/c", LogDir: "/l"}

	c, err := svc.GetSystemConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != 2260 {
		t.Fatalf("default config = %+v", c)
	}
	c2, err := svc.PatchSystemConfig(ctx, []byte(`{"port":9090}`))
	if err != nil {
		t.Fatal(err)
	}
	if c2.Port != 9090 || c2.ListenAddress != "0.0.0.0" {
		t.Fatalf("patched = %+v", c2)
	}
	c3, err := svc.GetSystemConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c3.Port != 9090 {
		t.Fatalf("reloaded = %+v", c3)
	}
}

func TestResetPlatform(t *testing.T) {
	s, _, _ := testService()
	ctx := context.Background()
	id, err := s.ApplyPlatform(ctx, Platform{Name: "p", Allocation: "PREFER_IDLE_IP", StickyTTL: Duration(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.ResetPlatform(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != id || p.Name != "p" {
		t.Fatalf("identity changed: %+v", p)
	}
	if p.Allocation != AllocationBalanced || time.Duration(p.StickyTTL) != DefaultStickyTTL || p.PassiveBreaker != PassiveBreakerOn {
		t.Fatalf("not reset: %+v", p)
	}
}

func TestWireStepYAMLRoundTrip(t *testing.T) {
	// `wire: {}` in YAML must decode into a non-nil Wire marker.
	var p IdentityPolicy
	y := "chain:\n  - header: X-Egress-Account\n  - wire: {}\n  - headerRule:\n      rules:\n        - {prefix: \"*\", headers: [Authorization]}\nonMiss: random\n"
	if err := yamlUnmarshal([]byte(y), &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Chain) != 3 || p.Chain[0].Header == nil || p.Chain[1].Wire == nil || p.Chain[2].HeaderRule == nil {
		t.Fatalf("chain = %+v", p.Chain)
	}
	pol, err := toIdentityPolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	if pol.Chain[1].Kind != identity.StepWire {
		t.Fatalf("step1 kind = %v", pol.Chain[1].Kind)
	}
	if !strings.Contains(pol.Chain[0].Header, "X-Egress-Account") {
		t.Fatalf("step0 = %+v", pol.Chain[0])
	}
}

// TestEndpointDefaultSynthesis covers spec/resources/endpoint.md: the
// env-defined default endpoint is synthesized (read-only, all caps),
// "default" is reserved, and runtime state decorates reads.
func TestEndpointDefaultSynthesis(t *testing.T) {
	s := NewService(memstore.New(), NopHooks{},
		WithSystemConfig(SystemConfig{ListenAddress: "0.0.0.0", Port: 2260}),
	)
	s.SetEndpointHooks(nil, func(name string) (EndpointStatus, bool) {
		if name == DefaultEndpointName {
			return EndpointStatus{Listening: true}, true
		}
		return EndpointStatus{}, false
	})
	ctx := context.Background()

	items, total, err := s.ListEndpoints(ctx, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("list = %d/%d", len(items), total)
	}
	def := items[0]
	if def.Name != DefaultEndpointName || !def.ReadOnly || !def.Listening {
		t.Fatalf("default = %+v", def)
	}
	if def.Port != 2260 || !def.AllowManagement || !def.AllowProxy || !def.AllowSOCKS5 ||
		!def.AllowHTTPForward || !def.AllowHTTPReverse || !def.Enabled {
		t.Fatalf("default caps = %+v", def)
	}

	got, err := s.GetEndpoint(ctx, "default")
	if err != nil || !got.ReadOnly {
		t.Fatalf("get default = %+v, %v", got, err)
	}

	// "default" is reserved across all write paths.
	if err := s.ApplyEndpoint(ctx, Endpoint{Name: "default", Port: 9999}); err == nil {
		t.Fatal("apply must reject the reserved name")
	}
	if _, err := s.PatchEndpoint(ctx, "default", []byte(`{"port":9999}`)); err == nil {
		t.Fatal("patch must reject the reserved name")
	}
	if err := s.DeleteEndpoint(ctx, "default"); err == nil {
		t.Fatal("delete must reject the read-only default")
	}
}

// TestEndpointHooksFire verifies the ingress layer is notified on
// create/patch/delete (spec/resources/endpoint.md: 增删改立即生效).
func TestEndpointHooksFire(t *testing.T) {
	s := NewService(memstore.New(), NopHooks{})
	var changed, deleted []string
	s.SetEndpointHooks(hooksFunc{
		changed: func(n string) { changed = append(changed, n) },
		deleted: func(n string) { deleted = append(deleted, n) },
	}, nil)
	ctx := context.Background()

	if err := s.ApplyEndpoint(ctx, Endpoint{Name: "e1", Port: 2301, AllowProxy: true, AllowSOCKS5: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PatchEndpoint(ctx, "e1", []byte(`{"enabled":false}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteEndpoint(ctx, "e1"); err != nil {
		t.Fatal(err)
	}
	if len(changed) != 2 || changed[0] != "e1" || changed[1] != "e1" {
		t.Fatalf("changed = %v, want 2 notifications", changed)
	}
	if len(deleted) != 1 || deleted[0] != "e1" {
		t.Fatalf("deleted = %v", deleted)
	}
}

type hooksFunc struct {
	changed func(string)
	deleted func(string)
}

func (h hooksFunc) OnEndpointChanged(name string) { h.changed(name) }
func (h hooksFunc) OnEndpointDeleted(name string) { h.deleted(name) }
