// Package lease owns the lease table: (Platform, Account) → Lease and the
// per-IP lease counters. It is the sole writer of IPLoad (design §7.1):
// counters move only when a lease is created or deleted. Expiry is never
// scanned inline — the scheduler's expiry queue deletes expired leases
// (design §7.2).
//
// The Lease value type is canonical in pkg/lease (shared with the
// selector); this package adds the table behavior.
package lease

import (
	"net/netip"
	"sync"

	pklease "github.com/xiarsx/egress/pkg/lease"
)

// Lease is the (Platform, Account) → {EgressIP, NodeHint, Expiry} anchor.
type Lease = pklease.Lease

// LeaseTable maps (platform, account) to leases and tracks per-IP lease
// counts per platform. All methods are safe for concurrent use.
type LeaseTable struct {
	mu     sync.RWMutex
	leases map[string]pklease.Lease
	ipLoad map[string]int64
}

// NewLeaseTable creates an empty table.
func NewLeaseTable() *LeaseTable {
	return &LeaseTable{
		leases: make(map[string]pklease.Lease),
		ipLoad: make(map[string]int64),
	}
}

func leaseKey(platform, account string) string { return platform + "\x00" + account }
func ipLoadKey(platform string, ip netip.Addr) string {
	return platform + "\x00" + ip.String()
}

// Get returns the lease for (platform, account). Expired leases are
// returned as-is — expiry is enforced by the scheduler's expiry queue,
// never by an inline scan.
func (t *LeaseTable) Get(platform, account string) (pklease.Lease, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	l, ok := t.leases[leaseKey(platform, account)]
	return l, ok
}

// Put creates or overwrites the lease for (platform, account), adjusting
// the per-IP counters only when the anchored egress IP changes:
//   - new account → counter for the new IP +1;
//   - overwrite with a different IP → old IP -1, new IP +1;
//   - overwrite with the same IP (touch / hint rotation) → no change.
func (t *LeaseTable) Put(platform, account string, l pklease.Lease) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := leaseKey(platform, account)
	if old, ok := t.leases[k]; ok {
		if old.EgressIP != l.EgressIP {
			t.decLocked(platform, old.EgressIP)
			t.incLocked(platform, l.EgressIP)
		}
	} else {
		t.incLocked(platform, l.EgressIP)
	}
	t.leases[k] = l
}

// Release deletes the lease for (platform, account) and decrements its
// IP counter. It reports whether a lease was deleted.
func (t *LeaseTable) Release(platform, account string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := leaseKey(platform, account)
	old, ok := t.leases[k]
	if !ok {
		return false
	}
	delete(t.leases, k)
	t.decLocked(platform, old.EgressIP)
	return true
}

// IPLoad returns the number of live leases anchored at ip for platform —
// the value P2C scoring reads (design §7.2).
func (t *LeaseTable) IPLoad(platform string, ip netip.Addr) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ipLoad[ipLoadKey(platform, ip)]
}

// Len returns the number of leases in the table.
func (t *LeaseTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.leases)
}

// Range calls fn for every lease; fn=false stops iteration. It is used by
// reconcile (full IPLoad recomputation) and the control plane. The table
// is read-locked during iteration, so fn must not call back into the table.
func (t *LeaseTable) Range(fn func(platform, account string, l pklease.Lease) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for k, l := range t.leases {
		platform, account := splitKey(k)
		if !fn(platform, account, l) {
			return
		}
	}
}

func splitKey(k string) (platform, account string) {
	for i := 0; i < len(k); i++ {
		if k[i] == 0 {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

func (t *LeaseTable) incLocked(platform string, ip netip.Addr) {
	if !ip.IsValid() {
		return
	}
	t.ipLoad[ipLoadKey(platform, ip)]++
}

func (t *LeaseTable) decLocked(platform string, ip netip.Addr) {
	if !ip.IsValid() {
		return
	}
	k := ipLoadKey(platform, ip)
	if t.ipLoad[k] <= 1 {
		delete(t.ipLoad, k)
		return
	}
	t.ipLoad[k]--
}
