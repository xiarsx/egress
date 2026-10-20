package node

import (
	"os"

	"gopkg.in/yaml.v3"
	"net/netip"
	"testing"
	"time"

	pksignal "github.com/xiarsx/egress/pkg/signal"
)

func ipAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	ip, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return ip
}

func ipAddrZero() netip.Addr { return netip.Addr{} }

// Mirrors spec/vectors/health_transition_vectors.yaml with
// maxConsecutiveFailures = 3. Every row of the design §2.4 table is
// covered, including the breaker-exempt and outbound-empty rows.
// TestStep_ConformanceVectors executes the transition table directly
// from spec/vectors/health_transition_vectors.yaml (the spec is the
// source of truth; no hand-mirrored copies).
func TestStep_ConformanceVectors(t *testing.T) {
	cfg := BreakerConfig{MaxConsecutiveFailures: 3}
	now := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)

	for _, v := range loadHealthVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			// node_added is the constructor row, not a transition.
			if v.Signal.Kind == yaml.ScalarNode && v.Signal.Value == "node_added" {
				if v.Expect.Health != "probing" {
					t.Fatalf("node_added expect = %+v", v.Expect)
				}
				if got := ProbingHealth(); got.Kind != HealthProbing {
					t.Fatalf("ProbingHealth() = %+v", got)
				}
				return
			}

			sig := v.signalKind(t)
			got := Step(v.currentHealth(t), sig, cfg, now)
			want := v.expectHealth(t)
			if got.Kind != want.Kind || got.Failures != want.Failures {
				t.Fatalf("Step = %+v, want %+v (yaml)", got, want)
			}
			// Staying tripped preserves the original since.
			if v.Current.Health == "tripped" && got.Kind == HealthTripped {
				if !got.Since.Equal(v.currentHealth(t).Since) {
					t.Fatalf("tripped since = %v, want preserved %v", got.Since, v.Current.Health)
				}
			}
		})
	}
}

// Supplementary transitions the yaml does not cover.
func TestStep_SupplementaryTransitions(t *testing.T) {
	cfg := BreakerConfig{MaxConsecutiveFailures: 3}
	now := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)
	trippedSince := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name string
		cur  Health
		sig  pksignal.SignalKind
		want Health
	}{
		{"tripped_fail_stays_tripped", TrippedHealth(trippedSince), pksignal.SignalFail, TrippedHealth(trippedSince)},
		{"ignored_ok_probing_unchanged", ProbingHealth(), pksignal.SignalIgnored, ProbingHealth()},
		{"ignored_ok_tripped_unchanged", TrippedHealth(trippedSince), pksignal.SignalIgnored, TrippedHealth(trippedSince)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Step(tc.cur, tc.sig, cfg, now); got != tc.want {
				t.Fatalf("Step = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// --- yaml loader (spec/vectors is the normative conformance source) ---

type healthVectorFile struct {
	Config struct {
		MaxConsecutiveFailures int `yaml:"maxConsecutiveFailures"`
	} `yaml:"config"`
	Vectors []healthVector `yaml:"vectors"`
}

type healthVector struct {
	Name    string      `yaml:"name"`
	Current healthState `yaml:"current"`
	Signal  yaml.Node   `yaml:"signal"`
	Expect  healthState `yaml:"expect"`
}

type healthState struct {
	Health   string `yaml:"health"`
	Failures int    `yaml:"failures"`
	Since    string `yaml:"since"`
}

func loadHealthVectors(t *testing.T) []healthVector {
	t.Helper()
	raw, err := os.ReadFile("../../../spec/vectors/health_transition_vectors.yaml")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var f healthVectorFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if f.Config.MaxConsecutiveFailures != 3 {
		t.Fatalf("yaml threshold = %d; the executor hardcodes 3 — keep them in sync", f.Config.MaxConsecutiveFailures)
	}
	if len(f.Vectors) == 0 {
		t.Fatal("no vectors loaded")
	}
	return f.Vectors
}

func (hs healthState) toHealth(t *testing.T) Health {
	t.Helper()
	since, err := time.Parse(time.RFC3339, orEpoch(hs.Since))
	if err != nil {
		t.Fatalf("bad since %q: %v", hs.Since, err)
	}
	switch hs.Health {
	case "", "probing":
		return ProbingHealth()
	case "healthy":
		return HealthyHealth(hs.Failures)
	case "tripped":
		return TrippedHealth(since)
	default:
		t.Fatalf("unknown health %q", hs.Health)
		return Health{}
	}
}

func orEpoch(s string) string {
	if s == "" {
		return "1970-01-01T00:00:00Z"
	}
	return s
}

func (v healthVector) currentHealth(t *testing.T) Health { return v.Current.toHealth(t) }
func (v healthVector) expectHealth(t *testing.T) Health  { return v.Expect.toHealth(t) }

// signalKind maps the yaml signal (scalar or map with flags) to the
// kernel's signal kind.
func (v healthVector) signalKind(t *testing.T) pksignal.SignalKind {
	t.Helper()
	if v.Signal.Kind == yaml.ScalarNode {
		switch v.Signal.Value {
		case "ok":
			return pksignal.SignalOK
		case "fail":
			return pksignal.SignalFail
		}
		t.Fatalf("unknown scalar signal %q", v.Signal.Value)
	}
	var s struct {
		Kind          string `yaml:"kind"`
		BreakerExempt bool   `yaml:"breakerExempt"`
		OutboundEmpty bool   `yaml:"outboundEmpty"`
	}
	if err := v.Signal.Decode(&s); err != nil {
		t.Fatalf("decode signal: %v", err)
	}
	if s.Kind != "fail" {
		t.Fatalf("unknown signal kind %q", s.Kind)
	}
	switch {
	case s.OutboundEmpty:
		return pksignal.SignalIgnored
	case s.BreakerExempt:
		return pksignal.SignalFailExempt
	default:
		return pksignal.SignalFail
	}
}

func TestStep_NodeAddedBecomesProbing(t *testing.T) {
	// The "node added" row of the table is the constructor, not a transition.
	if got := ProbingHealth(); got.Kind != HealthProbing {
		t.Fatalf("ProbingHealth() = %+v, want Probing", got)
	}
}

func TestStep_ThresholdGuard(t *testing.T) {
	// Non-positive threshold means 1: a single failure trips.
	now := time.Now()
	got := Step(HealthyHealth(0), pksignal.SignalFail, BreakerConfig{}, now)
	if got.Kind != HealthTripped || !got.Since.Equal(now) {
		t.Fatalf("zero threshold: got %+v, want Tripped(%v)", got, now)
	}
}

func TestStep_TrippedSinceIsNow(t *testing.T) {
	now := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)
	got := Step(HealthyHealth(9), pksignal.SignalFail, BreakerConfig{MaxConsecutiveFailures: 3}, now)
	if got.Kind != HealthTripped {
		t.Fatalf("got %+v, want Tripped", got)
	}
	if !got.Since.Equal(now) {
		t.Fatalf("Tripped.Since = %v, want %v (caller-supplied now, no clock read)", got.Since, now)
	}
}

func TestRoutable(t *testing.T) {
	ip := ipAddr(t, "1.2.3.4")
	base := NodeSnapshot{
		Hash:        Hash{1},
		HasOutbound: true,
		Health:      HealthyHealth(0),
		EgressIP:    ip,
		HasLatency:  true,
	}
	p := PlatformFilter{} // no tag/attr filters

	if !Routable(p, base) {
		t.Fatal("healthy node with outbound, egress IP and latency should be routable")
	}

	cases := []struct {
		name   string
		mutate func(*NodeSnapshot)
		filter PlatformFilter
	}{
		{"no_outbound", func(n *NodeSnapshot) { n.HasOutbound = false }, p},
		{"probing", func(n *NodeSnapshot) { n.Health = ProbingHealth() }, p},
		{"tripped", func(n *NodeSnapshot) { n.Health = TrippedHealth(time.Now()) }, p},
		{"no_egress_ip", func(n *NodeSnapshot) { n.EgressIP = ipAddrZero() }, p},
		{"no_latency", func(n *NodeSnapshot) { n.HasLatency = false }, p},
		{"tag_filter_reject", func(n *NodeSnapshot) {}, PlatformFilter{Tags: func(NodeSnapshot) bool { return false }}},
		{"attr_filter_reject", func(n *NodeSnapshot) {}, PlatformFilter{Attrs: func(NodeSnapshot) bool { return false }}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := base
			tc.mutate(&n)
			if Routable(tc.filter, n) {
				t.Fatalf("expected not routable: %+v", n)
			}
		})
	}
}
