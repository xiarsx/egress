package identity

import "strings"

// OnMiss decides what happens when no chain step yields an account.
type OnMiss int

const (
	// OnMissRandom routes with an empty account: random pick, no lease.
	OnMissRandom OnMiss = iota
	// OnMissReject rejects the request (403 ACCOUNT_REJECTED).
	OnMissReject
)

// StepKind discriminates the links of an IdentityPolicy chain.
type StepKind int

const (
	// StepHeader resolves the account from one request header.
	StepHeader StepKind = iota
	// StepWire resolves the account from the line-protocol identity.
	StepWire
	// StepHeaderRule resolves via longest-prefix header rules.
	StepHeaderRule
)

// HeaderRule maps a target-URL prefix to candidate account headers.
// Prefix "*" is the catch-all fallback (lowest priority).
type HeaderRule struct {
	Prefix  string
	Headers []string
}

// Step is one link of the identity resolution chain (design §4.2).
// The zero Step resolves to "" (skipped).
type Step struct {
	Kind StepKind
	// Header name for StepHeader.
	Header string
	// Rules for StepHeaderRule; longest matching prefix wins.
	Rules []HeaderRule
}

// HeaderStep resolves the account from a single request header
// (HTTP-class ingress only).
func HeaderStep(name string) Step { return Step{Kind: StepHeader, Header: name} }

// WireStep resolves the account from the line-protocol identity
// (SOCKS5 username / Proxy-Authorization identity / reverse path).
func WireStep() Step { return Step{Kind: StepWire} }

// HeaderRuleStep resolves via longest-prefix header rules
// (HTTP-class ingress only).
func HeaderRuleStep(rules []HeaderRule) Step { return Step{Kind: StepHeaderRule, Rules: rules} }

// Policy is the ordered resolution chain: the first non-empty result wins
// (design §4.2). OnMiss applies when every step yields "".
type Policy struct {
	Chain  []Step
	OnMiss OnMiss
}

// DefaultPolicy is the built-in chain (spec/identity.md §5):
// X-Resin-Account header, then the wire account, then random on miss.
// NOTE: header steps only apply to HTTP-class ingress; on SOCKS5 they
// are skipped automatically.
func DefaultPolicy() Policy {
	return Policy{
		Chain: []Step{
			HeaderStep("X-Resin-Account"),
			WireStep(),
		},
		OnMiss: OnMissRandom,
	}
}

// Meta carries only request metadata — resolution performs no I/O.
type Meta struct {
	// Headers for HTTP-class ingress; lookup is case-insensitive.
	Headers map[string]string
	// Target is the request target URL/host, used for headerRule
	// longest-prefix matching.
	Target string
	// WireAccount is the account parsed from the line protocol.
	WireAccount string
	// IsHTTP reports whether header-reading steps apply; on SOCKS5 they
	// are skipped.
	IsHTTP bool
}

// Resolve runs the chain and returns the winning account.
// ok=false means the request is rejected (OnMissReject with no account).
// A ("", true) result means "proceed with an empty account": random
// routing, no lease.
func (p Policy) Resolve(m Meta) (account string, ok bool) {
	for _, s := range p.Chain {
		if a := s.resolve(m); a != "" {
			return a, true
		}
	}
	if p.OnMiss == OnMissReject {
		return "", false
	}
	return "", true
}

func (s Step) resolve(m Meta) string {
	switch s.Kind {
	case StepHeader:
		if !m.IsHTTP {
			return ""
		}
		return lookupHeader(m.Headers, s.Header)
	case StepWire:
		return m.WireAccount
	case StepHeaderRule:
		if !m.IsHTTP {
			return ""
		}
		for _, h := range matchHeaderRule(s.Rules, m.Target) {
			if v := lookupHeader(m.Headers, h); v != "" {
				return v
			}
		}
		return ""
	default:
		return ""
	}
}

// matchHeaderRule returns the headers of the longest-prefix matching
// rule for target. Prefix "*" matches everything at the lowest priority.
func matchHeaderRule(rules []HeaderRule, target string) []string {
	var best []string
	bestLen := -2 // below the "*" catch-all priority of -1
	for _, r := range rules {
		if l := prefixMatchLen(r.Prefix, target); l > bestLen {
			bestLen = l
			best = r.Headers
		}
	}
	return best
}

func prefixMatchLen(prefix, target string) int {
	if prefix == "*" {
		return -1
	}
	if prefix != "" && strings.HasPrefix(target, prefix) {
		return len(prefix)
	}
	return -2
}

// lookupHeader finds a header value case-insensitively (HTTP header names
// are case-insensitive); empty values never win.
func lookupHeader(headers map[string]string, name string) string {
	if v, ok := headers[name]; ok && v != "" {
		return v
	}
	for k, v := range headers {
		if v != "" && strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}
