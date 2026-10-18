package source

import (
	"testing"

	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
)

var _ ports.NodeRegistry = (*Registry)(nil) // compile-time contract check in test too

func TestHashOutbound(t *testing.T) {
	// xxh3-128 vectors (independent of HashOutbound itself).
	cases := map[string]string{
		`{"server":"example.com","server_port":443,"type":"vmess"}`: "e38b5e784ba91895b9a6bbc4ba870a5d",
		`{"type":"socks","server":"127.0.0.1","server_port":1080}`:  "4250df1f1312b8fc02113055c68c0169",
		"": "99aa06d3014798d86001c324468d497f",
	}
	for in, wantHex := range cases {
		if got := HashOutbound([]byte(in)).String(); got != wantHex {
			t.Fatalf("HashOutbound(%q) = %s, want %s", in, got, wantHex)
		}
	}
	// Deterministic and sensitive to input.
	a := HashOutbound([]byte(`{"type":"http"}`))
	if a != HashOutbound([]byte(`{"type":"http"}`)) {
		t.Fatal("not deterministic")
	}
	if a == HashOutbound([]byte(`{"type":"https"}`)) {
		t.Fatal("collision on distinct input")
	}
}

func snapFor(raw, tag string) pknode.NodeSnapshot {
	return pknode.NodeSnapshot{
		Hash:        HashOutbound([]byte(raw)),
		RawOptions:  []byte(raw),
		HasOutbound: true,
		Tags:        []pknode.ScopedTag{pknode.ScopedTag(tag)},
	}
}

func tagsOfSnapshot(n pknode.NodeSnapshot) []string {
	out := make([]string, 0, len(n.Tags))
	for _, t := range n.Tags {
		out = append(out, string(t))
	}
	return out
}

func TestRegistry_ApplySnapshotBasic(t *testing.T) {
	r := NewRegistry()
	r.ApplySnapshot("srcA", []pknode.NodeSnapshot{
		snapFor(`{"type":"http","server":"10.0.0.1"}`, "web"),
		snapFor(`{"type":"socks","server":"10.0.0.2"}`, "socks"),
	})
	all := r.All()
	if len(all) != 2 {
		t.Fatalf("All() = %d nodes, want 2", len(all))
	}
	for _, n := range all {
		if n.Health.Kind != pknode.HealthProbing {
			t.Fatalf("new node health = %v, want Probing", n.Health.Kind)
		}
		if n.Attrs == nil {
			t.Fatal("new node Attrs nil, want empty map")
		}
		if len(n.Tags) != 1 || string(n.Tags[0])[:5] != "srcA/" {
			t.Fatalf("tags = %v, want srcA/-scoped", n.Tags)
		}
	}
	snap := r.SnapshotOf("srcA")
	if len(snap) != 2 {
		t.Fatalf("SnapshotOf = %d, want 2", len(snap))
	}
	if got := r.SnapshotOf("nope"); len(got) != 0 {
		t.Fatalf("SnapshotOf(unknown) = %d, want 0", len(got))
	}
}

func TestRegistry_MergeAcrossSources(t *testing.T) {
	r := NewRegistry()
	h1 := HashOutbound([]byte(`{"type":"http","server":"10.0.0.1"}`))
	h2 := HashOutbound([]byte(`{"type":"socks","server":"10.0.0.2"}`))

	r.ApplySnapshot("srcA", []pknode.NodeSnapshot{snapFor(`{"type":"http","server":"10.0.0.1"}`, "t1"), snapFor(`{"type":"socks","server":"10.0.0.2"}`, "t2")})
	r.ApplySnapshot("srcB", []pknode.NodeSnapshot{snapFor(`{"type":"http","server":"10.0.0.1"}`, "t3")})

	n1, ok := r.Get(h1)
	if !ok {
		t.Fatal("h1 missing")
	}
	if got := tagsOfSnapshot(n1); !equalStrings(got, []string{"srcA/t1", "srcB/t3"}) {
		t.Fatalf("h1 tags = %v", got)
	}
	n2, _ := r.Get(h2)
	if got := tagsOfSnapshot(n2); !equalStrings(got, []string{"srcA/t2"}) {
		t.Fatalf("h2 tags = %v", got)
	}

	// srcB republishes with a changed tag: srcA's contribution is untouched.
	r.ApplySnapshot("srcB", []pknode.NodeSnapshot{snapFor(`{"type":"http","server":"10.0.0.1"}`, "t3b")})
	n1, _ = r.Get(h1)
	if got := tagsOfSnapshot(n1); !equalStrings(got, []string{"srcA/t1", "srcB/t3b"}) {
		t.Fatalf("h1 tags after republish = %v", got)
	}

	// srcA withdraws: h2 (refcount 0) is deleted, h1 survives via srcB.
	r.ApplySnapshot("srcA", nil)
	if _, ok := r.Get(h2); ok {
		t.Fatal("h2 should be deleted after refcount hits zero")
	}
	n1, ok = r.Get(h1)
	if !ok {
		t.Fatal("h1 should survive via srcB")
	}
	if got := tagsOfSnapshot(n1); !equalStrings(got, []string{"srcB/t3b"}) {
		t.Fatalf("h1 tags after srcA withdraw = %v", got)
	}
	if len(r.All()) != 1 {
		t.Fatalf("All() = %d, want 1", len(r.All()))
	}
}

func TestRegistry_RepublishSameSourceReplacesTags(t *testing.T) {
	r := NewRegistry()
	raw := `{"type":"http","server":"10.0.0.9"}`
	h := HashOutbound([]byte(raw))
	r.ApplySnapshot("s", []pknode.NodeSnapshot{snapFor(raw, "old")})
	// Mark healthy, then republish: health must survive the republish.
	r.UpdateHealth(h, pknode.HealthyHealth(0))
	r.ApplySnapshot("s", []pknode.NodeSnapshot{snapFor(raw, "new")})
	n, _ := r.Get(h)
	if got := tagsOfSnapshot(n); !equalStrings(got, []string{"s/new"}) {
		t.Fatalf("tags = %v, want [s/new]", got)
	}
	if n.Health.Kind != pknode.HealthHealthy {
		t.Fatalf("health = %v after republish, want Healthy (single writer preserved)", n.Health.Kind)
	}
}

func TestRegistry_TagScopingIdempotent(t *testing.T) {
	r := NewRegistry()
	raw := `{"type":"http","server":"10.0.0.10"}`
	h := HashOutbound([]byte(raw))
	// Pre-scoped tags (as produced by a Syncer that already prefixed) must
	// not be double-prefixed.
	n := snapFor(raw, "s/pre")
	r.ApplySnapshot("s", []pknode.NodeSnapshot{n})
	got, _ := r.Get(h)
	if tags := tagsOfSnapshot(got); !equalStrings(tags, []string{"s/pre"}) {
		t.Fatalf("tags = %v, want [s/pre]", tags)
	}
}

func TestRegistry_DedupeWithinSnapshot(t *testing.T) {
	r := NewRegistry()
	raw := `{"type":"http","server":"10.0.0.11"}`
	r.ApplySnapshot("s", []pknode.NodeSnapshot{snapFor(raw, "a"), snapFor(raw, "b")})
	if len(r.All()) != 1 {
		t.Fatalf("All() = %d, want 1 (deduped)", len(r.All()))
	}
}

func TestRegistry_GetMissing(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Get(pknode.Zero); ok {
		t.Fatal("Get(zero) = true, want false")
	}
	// UpdateHealth on unknown hash is a no-op, not a panic.
	r.UpdateHealth(pknode.Zero, pknode.HealthyHealth(0))
}

func TestRegistry_ReadsAreCopies(t *testing.T) {
	r := NewRegistry()
	raw := `{"type":"http","server":"10.0.0.12"}`
	h := HashOutbound([]byte(raw))
	r.ApplySnapshot("s", []pknode.NodeSnapshot{snapFor(raw, "t")})
	n, _ := r.Get(h)
	n.Tags[0] = "mutated"
	n.Attrs["x"] = pknode.StringAttr("y")
	n.RawOptions[0] = 'X'
	again, _ := r.Get(h)
	if string(again.Tags[0]) != "s/t" || len(again.Attrs) != 0 || again.RawOptions[0] == 'X' {
		t.Fatal("Get returned aliased memory")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
