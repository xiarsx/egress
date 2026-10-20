package pick

import (
	"os"

	"errors"
	"gopkg.in/yaml.v3"
	"math/rand/v2"
	"net/netip"
	"testing"
	"time"

	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// --- fakes ---

type fakeView struct {
	picks []pknode.Hash
	size  int
	byIP  map[netip.Addr][]pknode.Hash
	set   map[pknode.Hash]bool
}

func (f *fakeView) Size() int { return f.size }
func (f *fakeView) RandomPick(_ *rand.Rand) (pknode.Hash, bool) {
	if len(f.picks) == 0 {
		return pknode.Hash{}, false
	}
	h := f.picks[0]
	f.picks = f.picks[1:]
	return h, true
}
func (f *fakeView) ByEgressIP(ip netip.Addr) []pknode.Hash { return f.byIP[ip] }
func (f *fakeView) Contains(h pknode.Hash) bool            { return f.set[h] }

type fakeNodes struct {
	snaps map[pknode.Hash]pknode.NodeSnapshot
}

func (f *fakeNodes) Snapshot(h pknode.Hash) (pknode.NodeSnapshot, bool) {
	s, ok := f.snaps[h]
	return s, ok
}

type fakeLoads struct{ m map[netip.Addr]int64 }

func (f *fakeLoads) Get(ip netip.Addr) int64 { return f.m[ip] }

type fakeLatency struct {
	m map[pknode.Hash]map[string]time.Duration
}

func (f *fakeLatency) Latency(h pknode.Hash, domain string) (time.Duration, bool) {
	d, ok := f.m[h][domain]
	return d, ok
}

func hashOf(b byte) pknode.Hash {
	var h pknode.Hash
	h[0] = b
	return h
}

func mustIP(t testing.TB, s string) netip.Addr {
	t.Helper()
	ip, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return ip
}

// p2cFixture builds a two-candidate world. picks controls the P2C draw
// order; lat maps candidate id → domain → latency ms; loads maps candidate
// id → lease count.
func p2cFixture(t testing.TB, picks []pknode.Hash, lat map[pknode.Hash]map[string]time.Duration, loads map[pknode.Hash]int64) (*fakeView, *fakeNodes, *fakeLoads, *fakeLatency) {
	t.Helper()
	ipA, ipB := mustIP(t, "10.0.0.1"), mustIP(t, "10.0.0.2")
	ips := map[pknode.Hash]netip.Addr{picks[0]: ipA}
	if len(picks) > 1 {
		ips[picks[1]] = ipB
	}
	snaps := map[pknode.Hash]pknode.NodeSnapshot{}
	set := map[pknode.Hash]bool{}
	loadMap := map[netip.Addr]int64{}
	for h, ip := range ips {
		snaps[h] = pknode.NodeSnapshot{Hash: h, EgressIP: ip}
		set[h] = true
		loadMap[ip] = loads[h]
	}
	return &fakeView{picks: append([]pknode.Hash(nil), picks...), size: len(picks), set: set},
		&fakeNodes{snaps: snaps},
		&fakeLoads{m: loadMap},
		&fakeLatency{m: lat}
}

func runP2C(t *testing.T, v *fakeView, n *fakeNodes, l *fakeLoads, lat *fakeLatency, pol Policy, target string) pknode.Hash {
	t.Helper()
	h, err := pickP2C(v, n, l, lat, pol, target, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// --- scoring vectors: executed directly from
// spec/vectors/p2c_scoring_vectors.yaml (the spec is the source of truth;
// no hand-mirrored copies) ---

func TestP2C_ConformanceVectors(t *testing.T) {
	// Candidate ids map to deterministic hashes (A → 1, B → 2, C → 3).
	idHash := func(id string) pknode.Hash {
		if len(id) != 1 || id[0] < 'A' || id[0] > 'Z' {
			t.Fatalf("vector candidate id %q: executor supports single letters", id)
		}
		return hashOf(byte(id[0]-'A') + 1)
	}
	const authority = "cloudflare.com" // executor's authority-domain fixture

	for _, v := range loadP2CVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			var picks []pknode.Hash
			lat := map[pknode.Hash]map[string]time.Duration{}
			loads := map[pknode.Hash]int64{}
			for _, c := range v.Candidates {
				h := idHash(c.ID)
				picks = append(picks, h)
				loads[h] = c.LeaseCount
				if c.LatencyMs != nil {
					domain := v.Target
					if c.LatencyKind == "common_authority_mean" {
						domain = authority
					} else if c.LatencyKind != "target_domain" {
						t.Fatalf("unknown latencyKind %q", c.LatencyKind)
					}
					lat[h] = map[string]time.Duration{
						domain: time.Duration(*c.LatencyMs) * time.Millisecond,
					}
				}
			}
			pol := Policy{Allocation: v.allocation(t), Authorities: []string{authority}}
			fv, n, l, la := p2cFixture(t, picks, lat, loads)
			if got := runP2C(t, fv, n, l, la, pol, v.Target); got != idHash(v.Expect) {
				t.Fatalf("winner = %v, want %s (yaml scores: %+v)", got, v.Expect, v.Scores)
			}
		})
	}
}

// --- yaml loader (spec/vectors is the normative conformance source) ---

type p2cVectorFile struct {
	Vectors []p2cVector `yaml:"vectors"`
}

type p2cVector struct {
	Name       string `yaml:"name"`
	Allocation string `yaml:"allocation"`
	Target     string `yaml:"target"`
	Candidates []struct {
		ID          string `yaml:"id"`
		LeaseCount  int64  `yaml:"leaseCount"`
		LatencyMs   *int   `yaml:"latencyMs"`
		LatencyKind string `yaml:"latencyKind"`
	} `yaml:"candidates"`
	Scores map[string]int `yaml:"scores"`
	Expect string         `yaml:"expect"`
}

func loadP2CVectors(t *testing.T) []p2cVector {
	t.Helper()
	raw, err := os.ReadFile("../../../spec/vectors/p2c_scoring_vectors.yaml")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var f p2cVectorFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if len(f.Vectors) == 0 {
		t.Fatal("no vectors loaded")
	}
	return f.Vectors
}

func (v p2cVector) allocation(t *testing.T) Allocation {
	t.Helper()
	switch v.Allocation {
	case "BALANCED":
		return AllocationBalanced
	case "PREFER_LOW_LATENCY":
		return AllocationPreferLowLatency
	case "PREFER_IDLE_IP":
		return AllocationPreferIdleIP
	default:
		t.Fatalf("unknown allocation %q", v.Allocation)
		return 0
	}
}

func TestP2C_TieFavorsSecondCandidate(t *testing.T) {
	a, b := hashOf(1), hashOf(2)
	v, n, l, lat := p2cFixture(t, []pknode.Hash{a, b},
		map[pknode.Hash]map[string]time.Duration{
			a: {"example.com": 50 * time.Millisecond},
			b: {"example.com": 50 * time.Millisecond},
		},
		map[pknode.Hash]int64{a: 2, b: 2})
	if got := runP2C(t, v, n, l, lat, Policy{Allocation: AllocationBalanced}, "example.com"); got != b {
		t.Fatalf("tie: got %v, want second candidate B (reference behavior)", got)
	}
}

func TestP2C_EmptyView(t *testing.T) {
	v := &fakeView{size: 0}
	_, err := pickP2C(v, &fakeNodes{}, &fakeLoads{}, &fakeLatency{}, Policy{}, "x", rand.New(rand.NewPCG(1, 2)))
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("err = %v, want ErrNoAvailableNodes", err)
	}
}

func TestP2CSelector_Contract(t *testing.T) {
	s := P2CSelector{Allocation: AllocationBalanced}
	if got := s.Pick(nil, nil); got != -1 {
		t.Fatalf("Pick(nil) = %d, want -1", got)
	}
	cands := []Candidate{{Latency: 50 * time.Millisecond, LeaseCount: 0}}
	if got := s.Pick(cands, nil); got != 0 {
		t.Fatalf("Pick(1) = %d, want 0", got)
	}
	// (0+1)*100=100 vs (9+1)*20=200 → first wins
	cands = []Candidate{
		{Latency: 100 * time.Millisecond, LeaseCount: 0},
		{Latency: 20 * time.Millisecond, LeaseCount: 9},
	}
	if got := s.Pick(cands, nil); got != 0 {
		t.Fatalf("Pick = %d, want 0", got)
	}
}

// --- Select (§7.2) ---

func selectFixture(t testing.TB) (*fakeView, *fakeNodes, *fakeLoads, *fakeLatency, pknode.Hash, pknode.Hash, netip.Addr) {
	t.Helper()
	h1, h2 := hashOf(1), hashOf(2)
	ip := mustIP(t, "10.0.0.9")
	snaps := map[pknode.Hash]pknode.NodeSnapshot{
		h1: {Hash: h1, EgressIP: ip},
		h2: {Hash: h2, EgressIP: ip},
	}
	v := &fakeView{
		picks: []pknode.Hash{h1, h2},
		size:  2,
		set:   map[pknode.Hash]bool{h1: true, h2: true},
		byIP:  map[netip.Addr][]pknode.Hash{ip: {h1, h2}},
	}
	// Deterministic P2C: h1 has the lower target-domain latency.
	lat := &fakeLatency{m: map[pknode.Hash]map[string]time.Duration{
		h1: {"example.com": 10 * time.Millisecond},
		h2: {"example.com": 100 * time.Millisecond},
	}}
	return v, &fakeNodes{snaps: snaps}, &fakeLoads{m: map[netip.Addr]int64{}},
		lat, h1, h2, ip
}

func TestSelect_RandomAccountNoLease(t *testing.T) {
	v, n, l, lat, h1, _, _ := selectFixture(t)
	now := time.Now()
	out, err := Select(v, n, l, lat, "", nil, Policy{}, "example.com", now, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if out.Node.Hash != h1 {
		t.Fatalf("node = %v, want first pick %v", out.Node.Hash, h1)
	}
	if out.Put != nil {
		t.Fatalf("random routing must not create a lease, got %+v", out.Put)
	}
}

func TestSelect_LeaseHitTouches(t *testing.T) {
	v, n, l, lat, h1, _, ip := selectFixture(t)
	now := time.Now()
	expiry := now.Add(time.Hour)
	lease := &pklease.Lease{EgressIP: ip, NodeHint: h1, Expiry: expiry, LastAccessed: now.Add(-time.Minute)}
	out, err := Select(v, n, l, lat, "acct", lease, Policy{}, "example.com", now, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if out.Node.Hash != h1 {
		t.Fatalf("node = %v, want hint %v", out.Node.Hash, h1)
	}
	if out.Put == nil {
		t.Fatal("expected a touch Put")
	}
	if !out.Put.Expiry.Equal(expiry) {
		t.Fatalf("expiry changed on hit: %v vs %v (must not extend)", out.Put.Expiry, expiry)
	}
	if !out.Put.LastAccessed.Equal(now) {
		t.Fatalf("LastAccessed = %v, want %v", out.Put.LastAccessed, now)
	}
	if out.Put.NodeHint != h1 {
		t.Fatalf("hint changed on hit: %v", out.Put.NodeHint)
	}
}

func TestSelect_HintGoneRotatesSameIP(t *testing.T) {
	v, n, l, lat, _, h2, ip := selectFixture(t)
	gone := hashOf(9)
	// The hinted node left the view; a sibling shares the egress IP.
	v.set[hashOf(1)] = false
	v.byIP[ip] = []pknode.Hash{h2}
	lat.m[h2] = map[string]time.Duration{"example.com": 10 * time.Millisecond}
	now := time.Now()
	expiry := now.Add(time.Hour)
	lease := &pklease.Lease{EgressIP: ip, NodeHint: gone, Expiry: expiry}
	out, err := Select(v, n, l, lat, "acct", lease, Policy{}, "example.com", now, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if out.Node.Hash != h2 {
		t.Fatalf("node = %v, want rotated sibling %v", out.Node.Hash, h2)
	}
	if out.Put == nil || out.Put.NodeHint != h2 {
		t.Fatalf("expected rotation Put with new hint, got %+v", out.Put)
	}
	if !out.Put.Expiry.Equal(expiry) {
		t.Fatalf("expiry changed on rotation: must not extend")
	}
}

func TestSelect_NoSiblingRecreatesLease(t *testing.T) {
	v, n, l, lat, h1, _, _ := selectFixture(t)
	otherIP := mustIP(t, "10.9.9.9")
	now := time.Now()
	lease := &pklease.Lease{EgressIP: otherIP, NodeHint: hashOf(9), Expiry: now.Add(time.Hour)}
	out, err := Select(v, n, l, lat, "acct", lease, Policy{StickyTTL: 30 * time.Minute}, "example.com", now, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if out.Put == nil {
		t.Fatal("expected a create Put")
	}
	if out.Put.NodeHint != h1 || out.Node.Hash != h1 {
		t.Fatalf("expected fresh P2C pick, got node=%v hint=%v", out.Node.Hash, out.Put.NodeHint)
	}
	if !out.Put.Expiry.Equal(now.Add(30 * time.Minute)) {
		t.Fatalf("expiry = %v, want now+TTL", out.Put.Expiry)
	}
}

func TestSelect_ExpiredLeaseRecreates(t *testing.T) {
	v, n, l, lat, h1, _, ip := selectFixture(t)
	now := time.Now()
	lease := &pklease.Lease{EgressIP: ip, NodeHint: h1, Expiry: now.Add(-time.Second)}
	out, err := Select(v, n, l, lat, "acct", lease, Policy{}, "example.com", now, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if out.Put == nil || out.Put.Expiry.Before(now) {
		t.Fatalf("expired lease must be recreated, got %+v", out.Put)
	}
}

func TestPickP2C_SingleNode(t *testing.T) {
	a := hashOf(1)
	v := &fakeView{picks: []pknode.Hash{a}, size: 1, set: map[pknode.Hash]bool{a: true}}
	h, err := pickP2C(v, &fakeNodes{}, &fakeLoads{}, &fakeLatency{}, Policy{}, "x", rand.New(rand.NewPCG(1, 2)))
	if err != nil || h != a {
		t.Fatalf("got (%v, %v), want (%v, nil)", h, err, a)
	}
}

func TestPickP2C_DuplicateDrawRetries(t *testing.T) {
	a, b := hashOf(1), hashOf(2)
	newWorld := func(picks []pknode.Hash) (*fakeView, *fakeNodes, *fakeLoads, *fakeLatency) {
		return p2cFixture(t, picks, nil, nil)
	}
	// [a, a, a, a, b]: second draw duplicates, retry finds b.
	v, n, l, lat := newWorld([]pknode.Hash{a, a, a, a, b})
	h, err := pickP2C(v, n, l, lat, Policy{}, "x", rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if h != a && h != b {
		t.Fatalf("unexpected winner %v", h)
	}
	// All draws duplicate → fall back to the first candidate.
	v2, n2, l2, lat2 := newWorld([]pknode.Hash{a, a, a, a, a})
	h, err = pickP2C(v2, n2, l2, lat2, Policy{}, "x", rand.New(rand.NewPCG(1, 2)))
	if err != nil || h != a {
		t.Fatalf("got (%v, %v), want (%v, nil)", h, err, a)
	}
}

func TestSelect_NodeGoneRace(t *testing.T) {
	// View yields a hash the registry no longer knows: caller must retry.
	a := hashOf(1)
	v := &fakeView{picks: []pknode.Hash{a}, size: 1, set: map[pknode.Hash]bool{a: true}}
	_, err := Select(v, &fakeNodes{snaps: map[pknode.Hash]pknode.NodeSnapshot{}},
		&fakeLoads{}, &fakeLatency{}, "acct", nil, Policy{}, "x", time.Now(), rand.New(rand.NewPCG(1, 2)))
	if !errors.Is(err, ErrNodeGone) {
		t.Fatalf("err = %v, want ErrNodeGone", err)
	}
}

func TestRotateSameIP_NoLatencyFallsBackToFirst(t *testing.T) {
	v, _, _, _, h1, h2, ip := selectFixture(t)
	// No latency data anywhere → first candidate wins (reference behavior).
	lat := &fakeLatency{m: map[pknode.Hash]map[string]time.Duration{}}
	if best, ok := rotateSameIP(v, lat, ip, "example.com", nil); !ok || best != h1 {
		t.Fatalf("got (%v, %v), want (%v, true)", best, ok, h1)
	}
	_ = h2
}

func TestRotateSameIP_AuthorityMean(t *testing.T) {
	v, _, _, _, h1, h2, ip := selectFixture(t)
	lat := &fakeLatency{m: map[pknode.Hash]map[string]time.Duration{
		h1: {"cloudflare.com": 90 * time.Millisecond},
		h2: {"cloudflare.com": 10 * time.Millisecond},
	}}
	// No target-domain data → authority means decide.
	if best, ok := rotateSameIP(v, lat, ip, "example.com", []string{"cloudflare.com"}); !ok || best != h2 {
		t.Fatalf("got (%v, %v), want (%v, true)", best, ok, h2)
	}
}

func TestRotateSameIP_TargetBeatsAuthority(t *testing.T) {
	v, _, _, _, h1, h2, ip := selectFixture(t)
	lat := &fakeLatency{m: map[pknode.Hash]map[string]time.Duration{
		h1: {"example.com": 5 * time.Millisecond, "cloudflare.com": 500 * time.Millisecond},
		h2: {"cloudflare.com": 10 * time.Millisecond},
	}}
	if best, ok := rotateSameIP(v, lat, ip, "example.com", []string{"cloudflare.com"}); !ok || best != h1 {
		t.Fatalf("got (%v, %v), want (%v, true)", best, ok, h1)
	}
}

func TestSelect_HintIPChangedRotates(t *testing.T) {
	v, n, l, lat, _, h2, ip := selectFixture(t)
	h1 := hashOf(1)
	// Hint is still in the view, but its egress IP changed → no hit;
	// rotation over the lease's old IP finds the sibling.
	n.snaps[h1] = pknode.NodeSnapshot{Hash: h1, EgressIP: mustIP(t, "10.0.0.77")}
	v.byIP[ip] = []pknode.Hash{h2} // h1 moved away from the lease's IP
	now := time.Now()
	expiry := now.Add(time.Hour)
	lease := &pklease.Lease{EgressIP: ip, NodeHint: h1, Expiry: expiry}
	out, err := Select(v, n, l, lat, "acct", lease, Policy{}, "example.com", now, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if out.Node.Hash != h2 || out.Put == nil || out.Put.NodeHint != h2 {
		t.Fatalf("expected rotation to sibling, got node=%v put=%+v", out.Node.Hash, out.Put)
	}
	if !out.Put.Expiry.Equal(expiry) {
		t.Fatal("rotation must not extend expiry")
	}
}

func TestSelect_EmptyViewErrors(t *testing.T) {
	v := &fakeView{size: 0}
	_, err := Select(v, &fakeNodes{}, &fakeLoads{}, &fakeLatency{}, "acct", nil, Policy{}, "x", time.Now(), rand.New(rand.NewPCG(1, 2)))
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("err = %v, want ErrNoAvailableNodes", err)
	}
}
