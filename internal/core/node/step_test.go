package node

import (
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
func TestStep_TransitionTable(t *testing.T) {
	cfg := BreakerConfig{MaxConsecutiveFailures: 3}
	now := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)
	trippedSince := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		cur  Health
		sig  pksignal.SignalKind
		want Health
	}{
		// Probing / Tripped + ok → Healthy(0)
		{"probing_ok_becomes_healthy", ProbingHealth(), pksignal.SignalOK, HealthyHealth(0)},
		{"tripped_ok_becomes_healthy", TrippedHealth(trippedSince), pksignal.SignalOK, HealthyHealth(0)},

		// Probing + fail → Probing
		{"probing_fail_stays_probing", ProbingHealth(), pksignal.SignalFail, ProbingHealth()},

		// Healthy(n) + ok → Healthy(0)
		{"healthy_ok_resets_counter", HealthyHealth(2), pksignal.SignalOK, HealthyHealth(0)},

		// Healthy(n) + fail, n+1 < threshold → Healthy(n+1)
		{"healthy_fail_increments", HealthyHealth(0), pksignal.SignalFail, HealthyHealth(1)},
		{"healthy_fail_increments_2", HealthyHealth(1), pksignal.SignalFail, HealthyHealth(2)},

		// Healthy(n) + fail, n+1 >= threshold → Tripped(now)
		{"healthy_fail_trips_at_threshold", HealthyHealth(2), pksignal.SignalFail, TrippedHealth(now)},

		// Tripped + fail → Tripped (only ok recovers; probes keep flowing)
		{"tripped_fail_stays_tripped", TrippedHealth(trippedSince), pksignal.SignalFail, TrippedHealth(trippedSince)},

		// fail + breakerExempt → unchanged, on every state
		{"exempt_fail_healthy_unchanged", HealthyHealth(2), pksignal.SignalFailExempt, HealthyHealth(2)},
		{"exempt_fail_probing_unchanged", ProbingHealth(), pksignal.SignalFailExempt, ProbingHealth()},
		{"exempt_fail_tripped_unchanged", TrippedHealth(trippedSince), pksignal.SignalFailExempt, TrippedHealth(trippedSince)},

		// Outbound empty → unchanged (projector maps these to SignalIgnored)
		{"ignored_fail_healthy_unchanged", HealthyHealth(1), pksignal.SignalIgnored, HealthyHealth(1)},
		{"ignored_ok_probing_unchanged", ProbingHealth(), pksignal.SignalIgnored, ProbingHealth()},
		{"ignored_ok_tripped_unchanged", TrippedHealth(trippedSince), pksignal.SignalIgnored, TrippedHealth(trippedSince)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Step(tc.cur, tc.sig, cfg, now)
			if got.Kind != tc.want.Kind || got.Failures != tc.want.Failures || !got.Since.Equal(tc.want.Since) {
				t.Fatalf("Step(%+v, %v) = %+v, want %+v", tc.cur, tc.sig, got, tc.want)
			}
		})
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
