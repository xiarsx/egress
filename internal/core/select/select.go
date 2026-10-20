// Package pick implements the routing selection algorithm as pure
// functions (design §7.2): sticky-lease fast path, same-IP rotation, and
// P2C fallback with policy scoring.
//
// (The directory is named select/ after the design's "select" step, but
// select is a Go keyword, so the package is called pick.)
//
// Hot-path discipline (design P2, §11.1): no I/O, no locks, no map writes
// on the pick path. Latency evidence, lease counts, node snapshots and
// randomness are injected through narrow read-only interfaces so the logic
// stays deterministic under test. The default StickyTTL guard is 24h.
//
// The function works on leases passed by value; the single lease-table
// write it decides is returned in Outcome.Put for the caller (the
// LeaseTable owner) to apply — the selector never mutates shared state.
package pick

import (
	"errors"
	"math/rand/v2"
	"net/netip"
	"time"

	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
)

var (
	// ErrNoAvailableNodes is returned when the view is empty.
	ErrNoAvailableNodes = errors.New("select: no available nodes")
	// ErrNodeGone is returned when a picked hash no longer resolves to a
	// snapshot (lost the race with node removal); the caller retries.
	ErrNodeGone = errors.New("select: picked node disappeared from registry")
)

// DefaultStickyTTL guards a non-positive Policy.StickyTTL.
const DefaultStickyTTL = 24 * time.Hour

// Allocation is the platform's allocation strategy (design §7.2).
type Allocation int

const (
	// AllocationBalanced scores (leaseCount+1)*latency.
	AllocationBalanced Allocation = iota
	// AllocationPreferLowLatency scores latency only.
	AllocationPreferLowLatency
	// AllocationPreferIdleIP scores leaseCount only.
	AllocationPreferIdleIP
)

// Policy bundles the platform's selection inputs.
type Policy struct {
	Allocation  Allocation
	Authorities []string // latency authority domains for P2C comparison
	StickyTTL   time.Duration
	// SelectorName, when set, names a registered ext.Selector that fully
	// owns the pick (spec/ext-abi.md §2): the built-in allocation and the
	// sticky-lease machinery are bypassed (the selector owns stickiness).
	SelectorName string
}

// View is the read side of a platform's routable set (hot path, lock-free).
type View interface {
	Size() int
	RandomPick(rng *rand.Rand) (pknode.Hash, bool)
	ByEgressIP(ip netip.Addr) []pknode.Hash
	Contains(h pknode.Hash) bool
}

// Nodes resolves node hashes to snapshots (the node registry, read-only).
type Nodes interface {
	Snapshot(h pknode.Hash) (pknode.NodeSnapshot, bool)
}

// IPLoad exposes per-IP lease counts (read-only).
type IPLoad interface {
	Get(ip netip.Addr) int64
}

// LatencySource exposes recent per-domain EWMA latencies (read-only).
// It reports only recent samples; staleness filtering is the provider's job.
type LatencySource interface {
	Latency(h pknode.Hash, domain string) (time.Duration, bool)
}

// Outcome is the result of Select: the chosen node plus the single
// lease-table write the caller must apply, if any.
type Outcome struct {
	Node pknode.NodeSnapshot
	// Put, when non-nil, must be applied as LeaseTable.Put(account, *Put).
	// Set for lease creation, same-IP hint rotation and LastAccessed
	// touches. Never set for random (account == "") routing.
	Put *pklease.Lease
}

// Select implements design §7.2:
//
//	if account 为空:                    return P2C(view)                       // no lease
//	if lease 有效:
//	   if nodeHint ∈ view ∧ hint.egressIP = lease.egressIP: return hint      // touch
//	   c := view.byEgress[lease.egressIP]
//	   if c 非空: lease.nodeHint = argmin_latency(c, target); return 它       // rotate, expiry unchanged
//	   释放 lease                                                            // falls through
//	n := P2C(view); 新建 lease{n.egressIP, n, now+TTL}; return n
//
// A "released" lease needs no explicit delete here: creating the
// replacement via Put overwrites it and the table rebalances IPLoad.
func Select(
	v View,
	nodes Nodes,
	loads IPLoad,
	lat LatencySource,
	account string,
	l *pklease.Lease,
	pol Policy,
	target string,
	now time.Time,
	rng *rand.Rand,
) (Outcome, error) {
	if account == "" {
		return selectRandom(v, nodes, loads, lat, pol, target, rng)
	}
	if l != nil && !l.Expired(now) {
		// Sticky fast path: the hinted node is still routable behind the
		// same egress IP.
		if hint, ok := nodes.Snapshot(l.NodeHint); ok &&
			v.Contains(l.NodeHint) && hint.EgressIP == l.EgressIP {
			touched := *l
			touched.LastAccessed = now
			return Outcome{Node: hint, Put: &touched}, nil
		}
		// Same-IP rotation: keep the lease's IP anchor, pick the
		// lowest-latency sibling. Expiry is not extended.
		if best, ok := rotateSameIP(v, lat, l.EgressIP, target, pol.Authorities); ok {
			snap, ok := nodes.Snapshot(best)
			if !ok {
				return Outcome{}, ErrNodeGone
			}
			rotated := *l
			rotated.NodeHint = best
			rotated.LastAccessed = now
			return Outcome{Node: snap, Put: &rotated}, nil
		}
		// No sibling shares the IP: fall through and recreate.
	}
	h, err := pickP2C(v, nodes, loads, lat, pol, target, rng)
	if err != nil {
		return Outcome{}, err
	}
	snap, ok := nodes.Snapshot(h)
	if !ok {
		return Outcome{}, ErrNodeGone
	}
	created := &pklease.Lease{
		EgressIP:     snap.EgressIP,
		NodeHint:     h,
		Expiry:       now.Add(stickyTTL(pol)),
		LastAccessed: now,
	}
	return Outcome{Node: snap, Put: created}, nil
}

func selectRandom(
	v View,
	nodes Nodes,
	loads IPLoad,
	lat LatencySource,
	pol Policy,
	target string,
	rng *rand.Rand,
) (Outcome, error) {
	h, err := pickP2C(v, nodes, loads, lat, pol, target, rng)
	if err != nil {
		return Outcome{}, err
	}
	snap, ok := nodes.Snapshot(h)
	if !ok {
		return Outcome{}, ErrNodeGone
	}
	return Outcome{Node: snap}, nil
}

func stickyTTL(pol Policy) time.Duration {
	if pol.StickyTTL > 0 {
		return pol.StickyTTL
	}
	return DefaultStickyTTL
}

// pickP2C draws two candidates from the view and returns the winner.
// Single-node views short-circuit; a duplicate second draw is retried a
// few times before giving up and returning the first candidate.
func pickP2C(
	v View,
	nodes Nodes,
	loads IPLoad,
	lat LatencySource,
	pol Policy,
	target string,
	rng *rand.Rand,
) (pknode.Hash, error) {
	size := v.Size()
	if size == 0 {
		return pknode.Hash{}, ErrNoAvailableNodes
	}
	h1, ok := v.RandomPick(rng)
	if !ok {
		return pknode.Hash{}, ErrNoAvailableNodes
	}
	if size == 1 {
		return h1, nil
	}
	h2, ok := v.RandomPick(rng)
	if !ok {
		return h1, nil
	}
	if h2 == h1 {
		for i := 0; i < 3; i++ {
			c, ok := v.RandomPick(rng)
			if !ok {
				break
			}
			if c != h1 {
				h2 = c
				break
			}
		}
		if h2 == h1 {
			return h1, nil
		}
	}

	lat1, lat2 := compareLatencies(h1, h2, lat, target, pol.Authorities)
	s1 := scoreCandidate(lat1, leaseCountOf(h1, nodes, loads), pol.Allocation)
	s2 := scoreCandidate(lat2, leaseCountOf(h2, nodes, loads), pol.Allocation)
	// Lower wins; ties favor the second candidate (reference behavior).
	if s1 < s2 {
		return h1, nil
	}
	return h2, nil
}

// compareLatencies implements the 3-level comparison (design §7.2,
// spec/vectors/p2c_scoring_vectors.yaml):
//  1. both candidates have recent latency for the target domain → use it;
//  2. else both share recent data on common authorities → use the means;
//  3. else (0, 0): latency is not comparable, score falls back to
//     leaseCount for both.
//
// "Same caliber" is strict: target-domain latency and authority means are
// never mixed.
func compareLatencies(
	h1, h2 pknode.Hash,
	lat LatencySource,
	target string,
	authorities []string,
) (time.Duration, time.Duration) {
	if target != "" {
		if l1, ok1 := lat.Latency(h1, target); ok1 {
			if l2, ok2 := lat.Latency(h2, target); ok2 {
				return l1, l2
			}
		}
	}
	var sum1, sum2 time.Duration
	var n int64
	for _, a := range authorities {
		l1, ok1 := lat.Latency(h1, a)
		l2, ok2 := lat.Latency(h2, a)
		if ok1 && ok2 {
			sum1 += l1
			sum2 += l2
			n++
		}
	}
	if n > 0 {
		return sum1 / time.Duration(n), sum2 / time.Duration(n)
	}
	return 0, 0
}

// scoreCandidate scores one candidate (lower is better). A non-positive
// latency means "not comparable": the score is strictly the lease count,
// regardless of allocation policy.
func scoreCandidate(latency time.Duration, leaseCount int64, alloc Allocation) float64 {
	if latency <= 0 {
		return float64(leaseCount)
	}
	switch alloc {
	case AllocationPreferLowLatency:
		return float64(latency)
	case AllocationPreferIdleIP:
		return float64(leaseCount)
	default: // AllocationBalanced
		return float64(leaseCount+1) * float64(latency)
	}
}

func leaseCountOf(h pknode.Hash, nodes Nodes, loads IPLoad) int64 {
	snap, ok := nodes.Snapshot(h)
	if !ok || !snap.EgressIP.IsValid() {
		return 0
	}
	return loads.Get(snap.EgressIP)
}

// rotateSameIP picks the lowest-latency sibling behind ip: target-domain
// latency first, then the node's own authority mean, else the first
// candidate (reference behavior).
func rotateSameIP(
	v View,
	lat LatencySource,
	ip netip.Addr,
	target string,
	authorities []string,
) (pknode.Hash, bool) {
	cands := v.ByEgressIP(ip)
	if len(cands) == 0 {
		return pknode.Hash{}, false
	}
	best := cands[0]
	var bestLat time.Duration
	bestKnown := false
	for _, h := range cands {
		l, ok := effectiveLatency(h, lat, target, authorities)
		if ok && (!bestKnown || l < bestLat) {
			best, bestLat, bestKnown = h, l, true
		}
	}
	return best, true
}

func effectiveLatency(
	h pknode.Hash,
	lat LatencySource,
	target string,
	authorities []string,
) (time.Duration, bool) {
	if target != "" {
		if l, ok := lat.Latency(h, target); ok {
			return l, true
		}
	}
	var sum time.Duration
	var n int64
	for _, a := range authorities {
		if l, ok := lat.Latency(h, a); ok {
			sum += l
			n++
		}
	}
	if n > 0 {
		return sum / time.Duration(n), true
	}
	return 0, false
}

// Candidate is one contender presented to a Selector, with pre-resolved
// scoring inputs.
type Candidate struct {
	Hash       pknode.Hash
	Latency    time.Duration // comparable latency for the target; <=0 ⇒ unknown
	LeaseCount int64
}

// Selector is the hot-path node-picking extension point (design §11.1).
//
// Contract — every implementation MUST:
//  1. be a pure function: no I/O, no syscalls, no locks, no channels;
//  2. not retain, mutate or reallocate cands;
//  3. run in time independent of platform size (it only ever sees the
//     pre-filtered contenders).
//
// Merge gate: a new Selector ships only after BenchmarkSelectorPick shows
// p99 < 1µs/op with 0 allocs/op on the reference dev machine. The
// built-in P2CSelector below is the baseline every contender is measured
// against.
type Selector interface {
	// Pick returns the index of the winning candidate in cands;
	// len(cands)==0 ⇒ -1.
	Pick(cands []Candidate, rng *rand.Rand) int
}

// P2CSelector is the built-in power-of-two-choices selector. It scores
// the first two candidates with the policy formula (see scoreCandidate)
// and picks the lower score; ties favor the later candidate.
type P2CSelector struct{ Allocation Allocation }

// Pick implements Selector.
func (s P2CSelector) Pick(cands []Candidate, _ *rand.Rand) int {
	switch len(cands) {
	case 0:
		return -1
	case 1:
		return 0
	}
	best, bestScore := 1, scoreCandidate(cands[1].Latency, cands[1].LeaseCount, s.Allocation)
	if sc := scoreCandidate(cands[0].Latency, cands[0].LeaseCount, s.Allocation); sc < bestScore {
		best = 0
	}
	return best
}
