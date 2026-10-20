package wiring

import (
	"context"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/config"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/testutil/memstore"
)

// newResolverFixture builds a Service + Resolver over a memstore with the
// Default platform ensured.
func newResolverFixture(t *testing.T) (*control.Service, *Resolver) {
	t.Helper()
	store := memstore.New()
	if err := config.EnsureDefaultPlatform(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	svc := control.NewService(store, control.NopHooks{})
	return svc, NewResolver(store, svc)
}

func TestResolver_PickPolicyMapping(t *testing.T) {
	svc, r := newResolverFixture(t)
	r.Authorities = []string{"google.com"}

	// Built-in allocation + per-platform sticky TTL land in the pick policy.
	if _, err := svc.ApplyPlatform(context.Background(), control.Platform{
		Name:       "custom",
		Allocation: control.AllocationPreferIdleIP,
		StickyTTL:  control.Duration(6 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	id, ok := r.ResolvePlatform("custom")
	if !ok {
		t.Fatal("custom platform did not resolve")
	}
	pol := r.PickPolicyFor(id)
	if pol.Allocation != pick.AllocationPreferIdleIP {
		t.Fatalf("Allocation = %v", pol.Allocation)
	}
	if pol.StickyTTL != 6*time.Hour {
		t.Fatalf("StickyTTL = %v", pol.StickyTTL)
	}
	if len(pol.Authorities) != 1 || pol.Authorities[0] != "google.com" {
		t.Fatalf("Authorities = %v", pol.Authorities)
	}
	if pol.SelectorName != "" {
		t.Fatalf("SelectorName = %q, want empty for built-in allocation", pol.SelectorName)
	}
}

func TestResolver_ExtSelectorAllocation(t *testing.T) {
	svc, r := newResolverFixture(t)

	// allocation "ext.<name>" passes control validation and lands as
	// Policy.SelectorName (RFC-0002): the pipeline's ext path picks it up.
	if _, err := svc.ApplyPlatform(context.Background(), control.Platform{
		Name:       "extsel",
		Allocation: "ext.my-selector",
	}); err != nil {
		t.Fatal(err)
	}
	id, ok := r.ResolvePlatform("extsel")
	if !ok {
		t.Fatal("extsel platform did not resolve")
	}
	pol := r.PickPolicyFor(id)
	if pol.SelectorName != "my-selector" {
		t.Fatalf("SelectorName = %q", pol.SelectorName)
	}
}

func TestResolver_UnknownPlatformFailsClosed(t *testing.T) {
	_, r := newResolverFixture(t)
	if _, ok := r.ResolvePlatform("nope"); ok {
		t.Fatal("unknown platform must not resolve")
	}
	if pol := r.PickPolicyFor("no-such-id"); pol.Allocation != pick.AllocationBalanced || pol.SelectorName != "" {
		t.Fatalf("unknown platform policy = %+v, want zero/balanced default", pol)
	}
}
