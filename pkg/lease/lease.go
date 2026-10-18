// Package lease defines the Lease value type shared by the lease table
// (internal/core/lease, the sole writer of IP load counters) and the
// selector (internal/core/select, which decides lease effects purely).
package lease

import (
	"net/netip"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// Lease anchors (Platform, Account) to an egress IP (design §7.1,
// promise P1: the egress IP does not change while the lease lives).
//
// The anchor is EgressIP; NodeHint is only a cache — any node behind the
// same IP may serve. Expiry is fixed at creation and never extended.
type Lease struct {
	EgressIP     netip.Addr
	NodeHint     pknode.Hash
	Expiry       time.Time
	LastAccessed time.Time
}

// Expired reports whether the lease is past its fixed expiry.
// Expiry is enforced by the scheduler's expiry queue, never by a table
// scan (design §7.2).
func (l Lease) Expired(now time.Time) bool { return !now.Before(l.Expiry) }
