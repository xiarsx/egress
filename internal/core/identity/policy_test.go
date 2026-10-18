package identity

import "testing"

func TestPolicy_FirstNonEmptyWins(t *testing.T) {
	p := Policy{
		Chain:  []Step{HeaderStep("X-Resin-Account"), WireStep()},
		OnMiss: OnMissRandom,
	}
	m := Meta{
		IsHTTP:      true,
		Headers:     map[string]string{"X-Resin-Account": "hdr-acct"},
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
	p := Policy{Chain: []Step{HeaderStep("X-Resin-Account")}, OnMiss: OnMissRandom}
	m := Meta{IsHTTP: true, Headers: map[string]string{"x-resin-account": "acct"}}
	if acct, ok := p.Resolve(m); !ok || acct != "acct" {
		t.Fatalf("Resolve = (%q, %v), want (acct, true)", acct, ok)
	}
}

func TestPolicy_HeaderSkippedOnSocks5(t *testing.T) {
	// Header-reading steps only apply to HTTP-class ingress.
	p := Policy{Chain: []Step{HeaderStep("X-Resin-Account"), WireStep()}, OnMiss: OnMissRandom}
	m := Meta{
		IsHTTP:      false, // SOCKS5
		Headers:     map[string]string{"X-Resin-Account": "hdr-acct"},
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
	// Spec §5: X-Resin-Account header outranks the wire account.
	m := Meta{IsHTTP: true, Headers: map[string]string{"X-Resin-Account": "h"}, WireAccount: "w"}
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
