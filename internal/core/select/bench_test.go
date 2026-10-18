package pick

import (
	"math/rand/v2"
	"testing"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// BenchmarkSelectorPick is the merge gate referenced by the Selector
// contract: a new Selector ships only if Pick stays within budget
// (p99 < 1µs/op, 0 allocs/op on the reference dev machine).
func BenchmarkSelectorPick(b *testing.B) {
	s := P2CSelector{Allocation: AllocationBalanced}
	cands := []Candidate{
		{Latency: 50 * time.Millisecond, LeaseCount: 2},
		{Latency: 20 * time.Millisecond, LeaseCount: 5},
	}
	rng := rand.New(rand.NewPCG(1, 2))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := s.Pick(cands, rng); got != 1 {
			b.Fatalf("got %d, want 1", got)
		}
	}
}

// BenchmarkPickP2C measures the full two-draw pick path (view reads +
// latency comparison + scoring) with scripted fakes.
func BenchmarkPickP2C(b *testing.B) {
	a, h2 := hashOf(1), hashOf(2)
	v, n, l, lat := p2cFixture(b, []pknode.Hash{a, h2},
		map[pknode.Hash]map[string]time.Duration{
			a:  {"example.com": 50 * time.Millisecond},
			h2: {"example.com": 20 * time.Millisecond},
		},
		map[pknode.Hash]int64{a: 2, h2: 5})
	pol := Policy{Allocation: AllocationBalanced}
	rng := rand.New(rand.NewPCG(7, 7))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.picks = []pknode.Hash{a, h2} // re-arm the scripted draws
		if _, err := pickP2C(v, n, l, lat, pol, "example.com", rng); err != nil {
			b.Fatal(err)
		}
	}
}
