package identity

import (
	"testing"

	"github.com/xiarsx/egress/pkg/ext"
)

func TestPolicy_FirstNonEmptyWins(t *testing.T) {
	p := Policy{
		Chain:  []Step{HeaderStep("X-Egress-Account"), WireStep()},
		OnMiss: OnMissRandom,
	}
	m := Meta{
		IsHTTP:      true,
		Headers:     map[string]string{"X-Egress-Account": "hdr-acct"},
		WireAccount: "wire-acct",
	}
	if acct, ok := p.Resolve(m); !ok || acct != "hdr-acct" {
		t.Fatalf("Resolve = (%q, %v), want (%q, true)", acct, ok, "hdr-acct")
	}

	// Header empty → wire wins.
	m.Headers = map[string]string{}
	if acct, ok := p.Resolve(m); !ok || acct != "wire-acct" {
		t.Fatalf("Resolve = (%q, %v), want (%q, true)", acct, ok, "wire-acct")
	}
}

func TestPolicy_HeaderCaseInsensitive(t *testing.T) {
	p := Policy{Chain: []Step{HeaderStep("X-Egress-Account")}, OnMiss: OnMissRandom}
	m := Meta{IsHTTP: true, Headers: map[string]string{"x-egress-account": "acct"}}
	if acct, ok := p.Resolve(m); !ok || acct != "acct" {
		t.Fatalf("Resolve = (%q, %v), want (acct, true)", acct, ok)
	}
}

func TestPolicy_HeaderSkippedOnSocks5(t *testing.T) {
	// Header-reading steps only apply to HTTP-class ingress.
	p := Policy{Chain: []Step{HeaderStep("X-Egress-Account"), WireStep()}, OnMiss: OnMissRandom}
	m := Meta{
		IsHTTP:      false, // SOCKS5
		Headers:     map[string]string{"X-Egress-Account": "hdr-acct"},
		WireAccount: "wire-acct",
	}
	if acct, ok := p.Resolve(m); !ok || acct != "wire-acct" {
		t.Fatalf("Resolve = (%q, %v), want (wire-acct, true)", acct, ok)
	}
}

func TestPolicy_HeaderRuleLongestPrefix(t *testing.T) {
	p := Policy{
		Chain: []Step{HeaderRuleStep([]HeaderRule{
			{Prefix: "*", Headers: []string{"Authorization", "x-api-key"}},
			{Prefix: "api.example.com/v1", Headers: []string{"X-V1-Account"}},
			{Prefix: "api.example.com", Headers: []string{"X-Acct"}},
		})},
		OnMiss: OnMissRandom,
	}

	// Longest prefix wins over shorter and over "*".
	m := Meta{
		IsHTTP: true,
		Target: "api.example.com/v1/users",
		Headers: map[string]string{
			"X-V1-Account":  "v1-acct",
			"X-Acct":        "short-acct",
			"Authorization": "auth-acct",
		},
	}
	if acct, ok := p.Resolve(m); !ok || acct != "v1-acct" {
		t.Fatalf("Resolve = (%q, %v), want (v1-acct, true)", acct, ok)
	}

	// Shorter prefix when the longer does not match.
	m.Target = "api.example.com/other"
	m.Headers = map[string]string{"X-Acct": "short-acct", "Authorization": "auth-acct"}
	if acct, ok := p.Resolve(m); !ok || acct != "short-acct" {
		t.Fatalf("Resolve = (%q, %v), want (short-acct, true)", acct, ok)
	}

	// Catch-all "*" with header fallback order.
	m.Target = "other.example.com/"
	m.Headers = map[string]string{"x-api-key": "key-acct"}
	if acct, ok := p.Resolve(m); !ok || acct != "key-acct" {
		t.Fatalf("Resolve = (%q, %v), want (key-acct, true)", acct, ok)
	}

	// No rule matches at all (no "*" rule) → miss.
	p2 := Policy{
		Chain:  []Step{HeaderRuleStep([]HeaderRule{{Prefix: "a.example", Headers: []string{"X-A"}}})},
		OnMiss: OnMissRandom,
	}
	m2 := Meta{IsHTTP: true, Target: "b.example", Headers: map[string]string{"X-A": "x"}}
	if acct, ok := p2.Resolve(m2); !ok || acct != "" {
		t.Fatalf("Resolve = (%q, %v), want (\"\", true)", acct, ok)
	}
}

func TestPolicy_OnMiss(t *testing.T) {
	m := Meta{IsHTTP: true}

	random := Policy{Chain: []Step{WireStep()}, OnMiss: OnMissRandom}
	if acct, ok := random.Resolve(m); !ok || acct != "" {
		t.Fatalf("OnMissRandom Resolve = (%q, %v), want (\"\", true)", acct, ok)
	}

	reject := Policy{Chain: []Step{WireStep()}, OnMiss: OnMissReject}
	if acct, ok := reject.Resolve(m); ok || acct != "" {
		t.Fatalf("OnMissReject Resolve = (%q, %v), want (\"\", false)", acct, ok)
	}
}

func TestDefaultPolicy(t *testing.T) {
	p := DefaultPolicy()
	// Spec §5: X-Egress-Account header outranks the wire account.
	m := Meta{IsHTTP: true, Headers: map[string]string{"X-Egress-Account": "h"}, WireAccount: "w"}
	if acct, ok := p.Resolve(m); !ok || acct != "h" {
		t.Fatalf("DefaultPolicy Resolve = (%q, %v), want (h, true)", acct, ok)
	}
	m.Headers = nil
	if acct, ok := p.Resolve(m); !ok || acct != "w" {
		t.Fatalf("DefaultPolicy Resolve = (%q, %v), want (w, true)", acct, ok)
	}
	// Miss → random (empty account, ok).
	m.WireAccount = ""
	if acct, ok := p.Resolve(m); !ok || acct != "" {
		t.Fatalf("DefaultPolicy Resolve = (%q, %v), want (\"\", true)", acct, ok)
	}
}

// --- ext.IdentityResolver step (spec/ext-abi.md §2, RFC-0002) ---

type stubResolver struct {
	account string
	handled bool
	seen    *ext.RequestMeta
}

func (s stubResolver) Name() string { return "test-stub-resolver" }
func (s stubResolver) Resolve(m ext.RequestMeta) (string, bool) {
	if s.seen != nil {
		*s.seen = m
	}
	return s.account, s.handled
}

func TestPolicy_ResolverStep(t *testing.T) {
	var seen ext.RequestMeta
	ext.RegisterIdentityResolver(stubResolver{account: "ext-account", handled: true, seen: &seen})

	p := Policy{Chain: []Step{ResolverStep("test-stub-resolver")}, OnMiss: OnMissReject}
	account, ok := p.Resolve(Meta{
		Headers:     map[string]string{"x": "y"},
		Target:      "example.com:443",
		WireAccount: "wire-acct",
		IsHTTP:      true,
	})
	if !ok || account != "ext-account" {
		t.Fatalf("Resolve = (%q, %v), want (ext-account, true)", account, ok)
	}
	// The resolver sees the request metadata (no body, no I/O).
	if seen.Target != "example.com:443" || seen.WireAccount != "wire-acct" || !seen.IsHTTP {
		t.Fatalf("resolver meta = %+v", seen)
	}
}

func TestPolicy_ResolverStepUnregisteredFallsThrough(t *testing.T) {
	// Unregistered name resolves to "" and the chain continues (fail-closed
	// via OnMiss), never a panic.
	p := Policy{
		Chain:  []Step{ResolverStep("never-registered"), WireStep()},
		OnMiss: OnMissRandom,
	}
	account, ok := p.Resolve(Meta{WireAccount: "wire-acct"})
	if !ok || account != "wire-acct" {
		t.Fatalf("Resolve = (%q, %v), want (wire-acct, true)", account, ok)
	}
}

type passthroughResolver struct{}

func (passthroughResolver) Name() string { return "test-passthrough-resolver" }
func (passthroughResolver) Resolve(ext.RequestMeta) (string, bool) {
	return "", false
}

func TestPolicy_ResolverStepNotHandledContinuesChain(t *testing.T) {
	ext.RegisterIdentityResolver(passthroughResolver{})
	p := Policy{
		Chain:  []Step{ResolverStep("test-passthrough-resolver"), WireStep()},
		OnMiss: OnMissRandom,
	}
	account, ok := p.Resolve(Meta{WireAccount: "wire-acct"})
	if !ok || account != "wire-acct" {
		t.Fatalf("Resolve = (%q, %v), want (wire-acct, true)", account, ok)
	}
}
