package predicate

import (
	"testing"

	pknode "github.com/xiarsx/egress/pkg/node"
)

func snap(tags ...pknode.ScopedTag) pknode.NodeSnapshot {
	return pknode.NodeSnapshot{Tags: tags}
}

func mustCompile(t *testing.T, f Filter) pknode.Predicate {
	t.Helper()
	p, err := Compile(f)
	if err != nil {
		t.Fatalf("Compile(%+v) error: %v", f, err)
	}
	return p
}

func TestTags_AnyOrSemantics(t *testing.T) {
	p := mustCompile(t, Filter{Tags: []string{"hk|jp"}}) // no prefix = ANY
	if !p(snap("sub-a/hk-01", "sub-a/us-01")) {
		t.Fatal("expected match: one tag satisfies the ANY rule")
	}
	if p(snap("sub-a/us-01", "sub-b/sg-01")) {
		t.Fatal("expected no match: no tag satisfies the ANY rule")
	}
}

func TestTags_MustAndSemantics(t *testing.T) {
	p := mustCompile(t, Filter{Tags: []string{"*hk", "*premium"}}) // MUST = AND
	if !p(snap("sub-a/hk-premium-01")) {
		t.Fatal("expected match: one tag satisfies all MUST rules")
	}
	// Each MUST is satisfied, but by *different* tags → reject.
	if p(snap("sub-a/hk-01", "sub-a/premium-02")) {
		t.Fatal("expected no match: positive conditions must hold on the SAME tag")
	}
}

func TestTags_MixedAnyMustSameTag(t *testing.T) {
	// Design §5.4 example shape: ["^sub-a/", "*hk|jp", "!expired"]
	p := mustCompile(t, Filter{Tags: []string{"^sub-a/", "*hk|jp", "!expired"}})
	if !p(snap("sub-a/hk-01")) {
		t.Fatal("expected match")
	}
	if p(snap("sub-b/hk-01")) {
		t.Fatal("expected no match: ANY ^sub-a/ unsatisfied")
	}
	if p(snap("sub-a/us-01")) {
		t.Fatal("expected no match: MUST hk|jp unsatisfied")
	}
	if p(snap("sub-a/hk-expired")) {
		t.Fatal("expected no match: MUST_NOT expired hit")
	}
	// Positive rules split across tags → reject even though MUST_NOT is clean.
	if p(snap("sub-a/us-01", "sub-b/hk-01")) {
		t.Fatal("expected no match: no single tag satisfies ANY+MUST together")
	}
}

func TestTags_MustNotInspectsEveryTag(t *testing.T) {
	p := mustCompile(t, Filter{Tags: []string{"!expired"}})
	if !p(snap("sub-a/hk-01")) {
		t.Fatal("expected match: no MUST_NOT hit")
	}
	if p(snap("sub-a/hk-01", "sub-b/expired")) {
		t.Fatal("expected no match: MUST_NOT matched the second tag")
	}
	// No positive rules at all: only MUST_NOT can reject.
	p2 := mustCompile(t, Filter{Tags: nil})
	if !p2(snap("sub-a/hk-01")) {
		t.Fatal("empty tag filter should pass")
	}
}

func TestTags_BadPatternIsCompileError(t *testing.T) {
	if _, err := Compile(Filter{Tags: []string{"*([bad"}}); err == nil {
		t.Fatal("expected Compile error for bad regex")
	}
}

func TestAttrs_InNotIn(t *testing.T) {
	p := mustCompile(t, Filter{Attrs: map[string]AttrCondition{
		"geo.country": {In: []string{"hk", "jp", "us"}},
	}})
	n := pknode.NodeSnapshot{Attrs: map[pknode.AttrKey]pknode.AttrValue{
		"geo.country": pknode.StringAttr("hk"),
	}}
	if !p(n) {
		t.Fatal("expected in-match")
	}
	n.Attrs["geo.country"] = pknode.StringAttr("sg")
	if p(n) {
		t.Fatal("expected no match: sg not in set")
	}

	p2 := mustCompile(t, Filter{Attrs: map[string]AttrCondition{
		"geo.country": {NotIn: []string{"cn"}},
	}})
	n.Attrs["geo.country"] = pknode.StringAttr("hk")
	if !p2(n) {
		t.Fatal("expected notIn pass")
	}
	n.Attrs["geo.country"] = pknode.StringAttr("cn")
	if p2(n) {
		t.Fatal("expected notIn reject")
	}
}

func TestAttrs_MatchNotMatch(t *testing.T) {
	// Design §5.4 example: asn.org notMatch "(?i)cloud|hosting"
	p := mustCompile(t, Filter{Attrs: map[string]AttrCondition{
		"asn.org": {NotMatch: "(?i)cloud|hosting"},
	}})
	n := pknode.NodeSnapshot{Attrs: map[pknode.AttrKey]pknode.AttrValue{
		"asn.org": pknode.StringAttr("Example ISP"),
	}}
	if !p(n) {
		t.Fatal("expected notMatch pass")
	}
	n.Attrs["asn.org"] = pknode.StringAttr("CloudHost Inc")
	if p(n) {
		t.Fatal("expected notMatch reject (case-insensitive)")
	}

	p2 := mustCompile(t, Filter{Attrs: map[string]AttrCondition{
		"ip.type": {Match: "^datacenter$"},
	}})
	n.Attrs["ip.type"] = pknode.StringAttr("datacenter")
	if !p2(n) {
		t.Fatal("expected match pass")
	}
	n.Attrs["ip.type"] = pknode.StringAttr("residential")
	if p2(n) {
		t.Fatal("expected match reject")
	}
}

func TestAttrs_GteLte(t *testing.T) {
	p := mustCompile(t, Filter{Attrs: map[string]AttrCondition{
		"score": {Gte: "80", HasGte: true, Lte: "95", HasLte: true},
	}})
	mk := func(v float64) pknode.NodeSnapshot {
		return pknode.NodeSnapshot{Attrs: map[pknode.AttrKey]pknode.AttrValue{
			"score": pknode.NumberAttr(v),
		}}
	}
	if !p(mk(80)) || !p(mk(95)) || !p(mk(90)) {
		t.Fatal("expected bounds to be inclusive")
	}
	if p(mk(79.9)) || p(mk(95.1)) {
		t.Fatal("expected out-of-range reject")
	}

	// GteIs/LteIs helpers.
	p2 := mustCompile(t, Filter{Attrs: map[string]AttrCondition{"n": GteIs("10")}})
	if !p2(mk2("n", 10)) || p2(mk2("n", 9)) {
		t.Fatal("GteIs helper broken")
	}
}

func mk2(k string, v float64) pknode.NodeSnapshot {
	return pknode.NodeSnapshot{Attrs: map[pknode.AttrKey]pknode.AttrValue{
		pknode.AttrKey(k): pknode.NumberAttr(v),
	}}
}

func TestAttrs_MissingKey(t *testing.T) {
	n := pknode.NodeSnapshot{} // no attrs at all

	// Positive conditions fail on a missing key.
	for name, cond := range map[string]AttrCondition{
		"in":     {In: []string{"hk"}},
		"match":  {Match: "hk"},
		"gte":    GteIs("1"),
		"lte":    LteIs("1"),
		"in+neg": {In: []string{"hk"}, NotIn: []string{"cn"}}, // any positive poisons
	} {
		p := mustCompile(t, Filter{Attrs: map[string]AttrCondition{"geo.country": cond}})
		if p(n) {
			t.Fatalf("%s: expected missing key to not match", name)
		}
	}

	// Pure-negative conditions pass on a missing key.
	for name, cond := range map[string]AttrCondition{
		"notIn":    {NotIn: []string{"cn"}},
		"notMatch": {NotMatch: "cloud"},
		"both-neg": {NotIn: []string{"cn"}, NotMatch: "cloud"},
	} {
		p := mustCompile(t, Filter{Attrs: map[string]AttrCondition{"geo.country": cond}})
		if !p(n) {
			t.Fatalf("%s: expected missing key to pass pure-negative condition", name)
		}
	}
}

func TestAttrs_BadPatternIsCompileError(t *testing.T) {
	if _, err := Compile(Filter{Attrs: map[string]AttrCondition{"a": {Match: "([bad"}}}); err == nil {
		t.Fatal("expected Compile error for bad match regex")
	}
	if _, err := Compile(Filter{Attrs: map[string]AttrCondition{"a": {NotMatch: "([bad"}}}); err == nil {
		t.Fatal("expected Compile error for bad notMatch regex")
	}
}

func TestCombined_TagsAndAttrs(t *testing.T) {
	p := mustCompile(t, Filter{
		Tags: []string{"*hk"},
		Attrs: map[string]AttrCondition{
			"geo.country": {In: []string{"hk"}},
		},
	})
	n := pknode.NodeSnapshot{
		Tags: []pknode.ScopedTag{"sub-a/hk-01"},
		Attrs: map[pknode.AttrKey]pknode.AttrValue{
			"geo.country": pknode.StringAttr("hk"),
		},
	}
	if !p(n) {
		t.Fatal("expected combined match")
	}
	n.Tags = []pknode.ScopedTag{"sub-a/us-01"}
	if p(n) {
		t.Fatal("expected tag side to reject")
	}
	n.Tags = []pknode.ScopedTag{"sub-a/hk-01"}
	n.Attrs["geo.country"] = pknode.StringAttr("us")
	if p(n) {
		t.Fatal("expected attr side to reject")
	}
}
