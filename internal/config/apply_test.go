package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/app/control"
)

const testFileYAML = `apiVersion: egress/v1
kind: IdentityPolicy
metadata:
  name: default
spec:
  chain:
    - header: X-Resin-Account
    - wire: {}
    - headerRule:
        rules:
          - {prefix: "*", headers: [Authorization, x-api-key]}
  onMiss: random
---
apiVersion: egress/v1
kind: Platform
metadata:
  name: video
spec:
  stickyTTL: 72h
  allocation: PREFER_LOW_LATENCY
  passiveBreaker: on
  identityPolicyRef: default
  filter:
    tags: ["^sub-a/"]
    attrs:
      geo.country: {in: [hk, jp]}
---
apiVersion: egress/v1
kind: Source
metadata:
  name: main
spec:
  type: remote
  url: https://example.com/sub.txt
  updateInterval: 5m
  enabled: true
---
apiVersion: egress/v1
kind: Endpoint
metadata:
  name: socks
spec:
  port: 1080
  enabled: true
  allowSOCKS5: true
---
apiVersion: egress/v1
kind: Recipe
metadata:
  name: example-api
  version: 1.2.0
  requires: ["egress>=0.1.0"]
spec:
  identityPolicy:
    chain:
      - header: X-Resin-Account
      - wire: {}
    onMiss: random
  platformTemplate:
    stickyTTL: 168h
    allocation: BALANCED
`

func writeTestFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "egress.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestApplyFile(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	if err := ApplyFile(ctx, writeTestFile(t, testFileYAML), st); err != nil {
		t.Fatal(err)
	}

	// Platform stored with file annotation and parsed durations.
	r, err := st.Get(ctx, "Platform", "video")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := control.DecodeDoc(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Metadata.Annotations[control.AnnotationSource] != "file" {
		t.Fatalf("annotations = %v", doc.Metadata.Annotations)
	}
	if doc.Metadata.ID == "" {
		t.Fatal("platform has no ID")
	}
	var p control.Platform
	if err := control.DecodeSpec(doc, &p); err != nil {
		t.Fatal(err)
	}
	if time.Duration(p.StickyTTL) != 72*time.Hour {
		t.Fatalf("stickyTTL = %v", p.StickyTTL)
	}
	if p.Allocation != "PREFER_LOW_LATENCY" || len(p.Filter.Tags) != 1 {
		t.Fatalf("platform = %+v", p)
	}
	if p.Filter.Attrs["geo.country"].In[0] != "hk" {
		t.Fatalf("attrs = %+v", p.Filter.Attrs)
	}

	// Wire step decoded from `wire: {}`.
	pr, err := st.Get(ctx, "IdentityPolicy", "default")
	if err != nil {
		t.Fatal(err)
	}
	pdoc, _ := control.DecodeDoc(pr.Data)
	var pol control.IdentityPolicy
	if err := control.DecodeSpec(pdoc, &pol); err != nil {
		t.Fatal(err)
	}
	if len(pol.Chain) != 3 || pol.Chain[1].Wire == nil {
		t.Fatalf("chain = %+v", pol.Chain)
	}

	// Source defaults: enabled preserved, ID == name.
	sr, err := st.Get(ctx, "Source", "main")
	if err != nil {
		t.Fatal(err)
	}
	sdoc, _ := control.DecodeDoc(sr.Data)
	if sdoc.Metadata.ID != "main" {
		t.Fatalf("source id = %q", sdoc.Metadata.ID)
	}

	// Recipe metadata version/require preserved.
	rr, err := st.Get(ctx, "Recipe", "example-api")
	if err != nil {
		t.Fatal(err)
	}
	rdoc, _ := control.DecodeDoc(rr.Data)
	var rec control.Recipe
	if err := control.DecodeSpec(rdoc, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Version != "1.2.0" || len(rec.Requires) != 1 {
		t.Fatalf("recipe = %+v", rec)
	}

	// Re-apply is idempotent and keeps the platform ID.
	idBefore := doc.Metadata.ID
	if err := ApplyFile(ctx, writeTestFile(t, testFileYAML), st); err != nil {
		t.Fatal(err)
	}
	r2, _ := st.Get(ctx, "Platform", "video")
	doc2, _ := control.DecodeDoc(r2.Data)
	if doc2.Metadata.ID != idBefore {
		t.Fatalf("platform ID changed: %s -> %s", idBefore, doc2.Metadata.ID)
	}
}

func TestApplyFileRejects(t *testing.T) {
	ctx := context.Background()
	for name, content := range map[string]string{
		"bad apiVersion": "apiVersion: v2\nkind: Platform\nmetadata:\n  name: x\nspec: {}\n",
		"bad kind":       "apiVersion: egress/v1\nkind: Nope\nmetadata:\n  name: x\nspec: {}\n",
		"no name":        "apiVersion: egress/v1\nkind: Platform\nmetadata:\n  name: ''\nspec: {}\n",
		"bad duration":   "apiVersion: egress/v1\nkind: Platform\nmetadata:\n  name: x\nspec:\n  stickyTTL: soon\n",
		"bad enum":       "apiVersion: egress/v1\nkind: Platform\nmetadata:\n  name: x\nspec:\n  allocation: NOPE\n",
	} {
		st := newMemStore()
		if err := ApplyFile(ctx, writeTestFile(t, content), st); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestApplyFileClearsDrift(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	path := writeTestFile(t, testFileYAML)
	if err := ApplyFile(ctx, path, st); err != nil {
		t.Fatal(err)
	}
	// Simulate an API edit via the control service: marks drifted.
	svc := control.NewService(st, nil, control.WithAdminToken("t"))
	id, err := svc.ApplyPlatform(ctx, control.Platform{Name: "video", Allocation: "BALANCED"})
	if err != nil {
		t.Fatal(err)
	}
	_ = id
	r, _ := st.Get(ctx, "Platform", "video")
	doc, _ := control.DecodeDoc(r.Data)
	if doc.Metadata.Annotations[control.AnnotationDrifted] != "true" {
		t.Fatalf("expected drifted after API edit, got %v", doc.Metadata.Annotations)
	}
	// Re-applying the file clears the drift flag and restores file values.
	if err := ApplyFile(ctx, path, st); err != nil {
		t.Fatal(err)
	}
	r2, _ := st.Get(ctx, "Platform", "video")
	doc2, _ := control.DecodeDoc(r2.Data)
	if _, ok := doc2.Metadata.Annotations[control.AnnotationDrifted]; ok {
		t.Fatalf("drifted not cleared: %v", doc2.Metadata.Annotations)
	}
	var p control.Platform
	if err := control.DecodeSpec(doc2, &p); err != nil {
		t.Fatal(err)
	}
	if p.Allocation != "PREFER_LOW_LATENCY" {
		t.Fatalf("file values not restored: %+v", p)
	}
}

func TestEnsureDefaultPlatformIdempotent(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	for i := 0; i < 2; i++ {
		if err := EnsureDefaultPlatform(ctx, st); err != nil {
			t.Fatal(err)
		}
	}
	r, err := st.Get(ctx, "Platform", "Default")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := control.DecodeDoc(r.Data)
	if doc.Metadata.ID != control.DefaultPlatformID {
		t.Fatalf("default platform ID = %q", doc.Metadata.ID)
	}
	var p control.Platform
	if err := control.DecodeSpec(doc, &p); err != nil {
		t.Fatal(err)
	}
	if time.Duration(p.StickyTTL) != control.DefaultStickyTTL {
		t.Fatalf("stickyTTL = %v", p.StickyTTL)
	}
	if p.Allocation != control.AllocationBalanced || p.PassiveBreaker != control.PassiveBreakerOn {
		t.Fatalf("platform = %+v", p)
	}
	if p.IdentityPolicyRef != "default" {
		t.Fatalf("identityPolicyRef = %q", p.IdentityPolicyRef)
	}
	// Default identity policy: header -> wire -> headerRule, onMiss random.
	pr, err := st.Get(ctx, "IdentityPolicy", "default")
	if err != nil {
		t.Fatal(err)
	}
	pdoc, _ := control.DecodeDoc(pr.Data)
	var pol control.IdentityPolicy
	if err := control.DecodeSpec(pdoc, &pol); err != nil {
		t.Fatal(err)
	}
	if len(pol.Chain) != 3 || pol.Chain[0].Header == nil || pol.Chain[1].Wire == nil || pol.Chain[2].HeaderRule == nil {
		t.Fatalf("default chain = %+v", pol.Chain)
	}
	if pol.OnMiss != control.OnMissRandom {
		t.Fatalf("onMiss = %q", pol.OnMiss)
	}
}

func TestApplyLegacyDefaultEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("RESIN_DEFAULT_PLATFORM_STICKY_TTL", "24h")
	t.Setenv("RESIN_DEFAULT_PLATFORM_ALLOCATION_POLICY", "PREFER_LOW_LATENCY")
	t.Setenv("RESIN_DEFAULT_PLATFORM_PASSIVE_BREAKER", "off")
	t.Setenv("RESIN_DEFAULT_PLATFORM_REGEX_FILTERS", "^sub-a/, !expired")
	t.Setenv("RESIN_DEFAULT_PLATFORM_REGION_FILTERS", "hk, jp, !cn")
	t.Setenv("RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_MISS_ACTION", "REJECT")
	t.Setenv("RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_EMPTY_ACCOUNT_BEHAVIOR", "FIXED_HEADER")
	t.Setenv("RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_FIXED_ACCOUNT_HEADER", "X-Custom-Account")

	ctx := context.Background()
	st := newMemStore()
	out := captureStderr(t, func() {
		if err := ApplyLegacyDefaultEnv(ctx, st); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, "[DEPRECATED]") {
		t.Fatalf("expected deprecation warnings, got %q", out)
	}

	r, _ := st.Get(ctx, "Platform", "Default")
	doc, _ := control.DecodeDoc(r.Data)
	var p control.Platform
	if err := control.DecodeSpec(doc, &p); err != nil {
		t.Fatal(err)
	}
	if time.Duration(p.StickyTTL) != 24*time.Hour {
		t.Fatalf("stickyTTL = %v", p.StickyTTL)
	}
	if p.Allocation != "PREFER_LOW_LATENCY" || p.PassiveBreaker != "off" {
		t.Fatalf("platform = %+v", p)
	}
	if len(p.Filter.Tags) != 2 || p.Filter.Tags[0] != "^sub-a/" {
		t.Fatalf("tags = %v", p.Filter.Tags)
	}
	cond := p.Filter.Attrs["geo.country"]
	if len(cond.In) != 2 || cond.In[0] != "hk" || len(cond.NotIn) != 1 || cond.NotIn[0] != "cn" {
		t.Fatalf("geo.country = %+v", cond)
	}

	pr, _ := st.Get(ctx, "IdentityPolicy", "default")
	pdoc, _ := control.DecodeDoc(pr.Data)
	var pol control.IdentityPolicy
	if err := control.DecodeSpec(pdoc, &pol); err != nil {
		t.Fatal(err)
	}
	if pol.OnMiss != control.OnMissReject {
		t.Fatalf("onMiss = %q", pol.OnMiss)
	}
	// Chain: X-Resin-Account, X-Custom-Account (fixed header), wire.
	if len(pol.Chain) != 3 {
		t.Fatalf("chain = %+v", pol.Chain)
	}
	if pol.Chain[0].Header == nil || *pol.Chain[0].Header != "X-Resin-Account" {
		t.Fatalf("chain[0] = %+v", pol.Chain[0])
	}
	if pol.Chain[1].Header == nil || *pol.Chain[1].Header != "X-Custom-Account" {
		t.Fatalf("chain[1] = %+v", pol.Chain[1])
	}
	if pol.Chain[2].Wire == nil {
		t.Fatalf("chain[2] = %+v", pol.Chain[2])
	}
}

func TestApplyLegacyDefaultEnvNoop(t *testing.T) {
	clearEnv(t)
	ctx := context.Background()
	st := newMemStore()
	if err := ApplyLegacyDefaultEnv(ctx, st); err != nil {
		t.Fatal(err)
	}
	// Nothing set → Default platform untouched (not even created).
	if _, err := st.Get(ctx, "Platform", "Default"); err == nil {
		t.Fatal("expected no Default platform when no legacy vars are set")
	}
}
