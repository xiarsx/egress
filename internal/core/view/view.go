// Package view holds one platform's routable node set: a 64-shard
// collection with O(1) random pick/add/remove plus a secondary index
// EgressIP → []NodeHash for same-IP rotation (design §5.3).
//
// Concurrency: the hot path is read-only and lock-free — each shard is an
// immutable value swapped through an atomic pointer (COW), and the IP
// index stores immutable slices in a sync.Map. Write methods (Apply,
// Remove) are cold-path only and must be called by a single writer per
// View (the ViewDeriver); they are still safe if that contract is
// tightened later, but concurrent writers are not supported.
//
// Membership predicate: the View stores a Predicate consulted by Apply.
// Compose the full Routable derivation (health, egress IP, latency,
// tag/attr filters) into that predicate at construction; the View itself
// only asks "is this snapshot currently routable?".
package view

import (
	"math/rand/v2"
	"net/netip"
	"sync"
	"sync/atomic"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// NumShards is the shard count: O(1) pick/add/remove per shard.
const NumShards = 64

// shard is one immutable shard value: a dense slice for random picks plus
// a set for O(1) membership tests. Published via atomic.Pointer; never
// mutated after publication.
type shard struct {
	hashes []pknode.Hash
	index  map[pknode.Hash]struct{}
}

// View is one platform's routable set.
type View struct {
	pred    pknode.Predicate
	shards  [NumShards]atomic.Pointer[shard]
	size    atomic.Int64
	members sync.Map // pknode.Hash → netip.Addr (egress IP of the member)
	byIP    sync.Map // netip.Addr → []pknode.Hash (immutable snapshot)
}

func shardFor(h pknode.Hash) int { return int(h[0]) % NumShards }

// Derive builds a View from scratch (cold path: platform filter changed →
// full registry scan). A nil pred passes every node.
func Derive(nodes []pknode.NodeSnapshot, pred pknode.Predicate) *View {
	v := &View{pred: pred}
	for _, n := range nodes {
		v.Apply(n)
	}
	return v
}

// routable reports the current membership verdict for a snapshot.
func (v *View) routable(n pknode.NodeSnapshot) bool {
	return v.pred == nil || v.pred(n)
}

// Apply incrementally re-evaluates one node snapshot: routable snapshots
// are added (or kept, refreshing a changed egress IP), others are
// removed. It returns (added, removed).
//
// Single-writer contract: call only from the platform's ViewDeriver.
func (v *View) Apply(n pknode.NodeSnapshot) (added, removed bool) {
	if !v.routable(n) {
		return false, v.Remove(n.Hash)
	}
	if old, ok := v.members.Load(n.Hash); ok {
		if oldIP := old.(netip.Addr); oldIP != n.EgressIP {
			v.members.Store(n.Hash, n.EgressIP)
			v.removeFromIPIndex(oldIP, n.Hash)
			v.addToIPIndex(n.EgressIP, n.Hash)
		}
		return false, false
	}
	v.members.Store(n.Hash, n.EgressIP)
	v.addToShard(n.Hash)
	v.addToIPIndex(n.EgressIP, n.Hash)
	v.size.Add(1)
	return true, false
}

// Remove deletes a hash from the view; no-op if absent.
// Single-writer contract: call only from the platform's ViewDeriver.
func (v *View) Remove(h pknode.Hash) bool {
	old, ok := v.members.LoadAndDelete(h)
	if !ok {
		return false
	}
	v.removeFromShard(h)
	v.removeFromIPIndex(old.(netip.Addr), h)
	v.size.Add(-1)
	return true
}

// Size returns the total member count. Lock-free.
func (v *View) Size() int { return int(v.size.Load()) }

// Contains reports membership. Lock-free.
func (v *View) Contains(h pknode.Hash) bool {
	s := v.shards[shardFor(h)].Load()
	if s == nil {
		return false
	}
	_, ok := s.index[h]
	return ok
}

// RandomPick returns a uniformly random member hash (hot path, lock-free).
// ok=false when the view is empty.
func (v *View) RandomPick(rng *rand.Rand) (pknode.Hash, bool) {
	total := v.size.Load()
	if total == 0 {
		return pknode.Zero, false
	}
	target := rng.IntN(int(total))
	for i := range v.shards {
		s := v.shards[i].Load()
		var n int
		if s != nil {
			n = len(s.hashes)
		}
		if target < n {
			return s.hashes[target], true
		}
		target -= n
	}
	return pknode.Zero, false
}

// Range calls fn for each member hash; fn=false stops iteration. Lock-free.
func (v *View) Range(fn func(pknode.Hash) bool) {
	for i := range v.shards {
		s := v.shards[i].Load()
		if s == nil {
			continue
		}
		for _, h := range s.hashes {
			if !fn(h) {
				return
			}
		}
	}
}

// ByEgressIP returns the member hashes currently behind ip (hot path,
// lock-free). The returned slice is immutable and must not be mutated.
func (v *View) ByEgressIP(ip netip.Addr) []pknode.Hash {
	if !ip.IsValid() {
		return nil
	}
	if v, ok := v.byIP.Load(ip); ok {
		return v.([]pknode.Hash)
	}
	return nil
}

// --- cold-path writers (COW) ---

func (v *View) addToShard(h pknode.Hash) {
	s := &v.shards[shardFor(h)]
	for {
		old := s.Load()
		nw := &shard{}
		if old != nil {
			if _, dup := old.index[h]; dup {
				return
			}
			nw.hashes = make([]pknode.Hash, len(old.hashes), len(old.hashes)+1)
			copy(nw.hashes, old.hashes)
			nw.index = make(map[pknode.Hash]struct{}, len(old.index)+1)
			for k := range old.index {
				nw.index[k] = struct{}{}
			}
		} else {
			nw.hashes = make([]pknode.Hash, 0, 1)
			nw.index = make(map[pknode.Hash]struct{}, 1)
		}
		nw.index[h] = struct{}{}
		nw.hashes = append(nw.hashes, h)
		if s.CompareAndSwap(old, nw) {
			return
		}
	}
}

func (v *View) removeFromShard(h pknode.Hash) {
	s := &v.shards[shardFor(h)]
	for {
		old := s.Load()
		if old == nil {
			return
		}
		if _, ok := old.index[h]; !ok {
			return
		}
		nw := &shard{
			hashes: make([]pknode.Hash, 0, len(old.hashes)-1),
			index:  make(map[pknode.Hash]struct{}, len(old.index)-1),
		}
		for _, x := range old.hashes {
			if x == h {
				continue
			}
			nw.index[x] = struct{}{}
			nw.hashes = append(nw.hashes, x)
		}
		if s.CompareAndSwap(old, nw) {
			return
		}
	}
}

// addToIPIndex / removeFromIPIndex replace the immutable per-IP slice
// wholesale; readers always see a complete snapshot. Single-writer only.
func (v *View) addToIPIndex(ip netip.Addr, h pknode.Hash) {
	if !ip.IsValid() {
		return
	}
	var cur []pknode.Hash
	if old, ok := v.byIP.Load(ip); ok {
		cur = old.([]pknode.Hash)
		for _, x := range cur {
			if x == h {
				return
			}
		}
	}
	nw := make([]pknode.Hash, len(cur)+1)
	copy(nw, cur)
	nw[len(cur)] = h
	v.byIP.Store(ip, nw)
}

func (v *View) removeFromIPIndex(ip netip.Addr, h pknode.Hash) {
	if !ip.IsValid() {
		return
	}
	old, ok := v.byIP.Load(ip)
	if !ok {
		return
	}
	cur := old.([]pknode.Hash)
	nw := make([]pknode.Hash, 0, len(cur))
	for _, x := range cur {
		if x != h {
			nw = append(nw, x)
		}
	}
	if len(nw) == len(cur) {
		return // not present
	}
	if len(nw) == 0 {
		v.byIP.Delete(ip)
		return
	}
	v.byIP.Store(ip, nw)
}
