package lease

import (
	"net/netip"
	"testing"
	"time"

	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
)

func mkLease(ip string, hint byte) pklease.Lease {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		panic(err)
	}
	var h pknode.Hash
	h[0] = hint
	return pklease.Lease{
		EgressIP:     addr,
		NodeHint:     h,
		Expiry:       time.Now().Add(time.Hour),
		LastAccessed: time.Now(),
	}
}

func TestPutGetRelease_IPLoadAccounting(t *testing.T) {
	tb := NewLeaseTable()
	ip1, _ := netip.ParseAddr("10.0.0.1")
	ip2, _ := netip.ParseAddr("10.0.0.2")

	// Create: counter +1.
	tb.Put("p", "a", mkLease("10.0.0.1", 1))
	if got, ok := tb.Get("p", "a"); !ok || got.EgressIP != ip1 {
		t.Fatalf("Get = %+v, %v", got, ok)
	}
	if c := tb.IPLoad("p", ip1); c != 1 {
		t.Fatalf("IPLoad = %d, want 1", c)
	}

	// Second account, same IP: counter +1.
	tb.Put("p", "b", mkLease("10.0.0.1", 2))
	if c := tb.IPLoad("p", ip1); c != 2 {
		t.Fatalf("IPLoad = %d, want 2", c)
	}

	// Overwrite with same IP (touch/rotation): no counter change.
	l, _ := tb.Get("p", "a")
	l.LastAccessed = time.Now()
	tb.Put("p", "a", l)
	if c := tb.IPLoad("p", ip1); c != 2 {
		t.Fatalf("IPLoad after touch = %d, want 2 (unchanged)", c)
	}

	// Overwrite with different IP: old -1, new +1.
	l.EgressIP = ip2
	tb.Put("p", "a", l)
	if c := tb.IPLoad("p", ip1); c != 1 {
		t.Fatalf("IPLoad(ip1) = %d, want 1", c)
	}
	if c := tb.IPLoad("p", ip2); c != 1 {
		t.Fatalf("IPLoad(ip2) = %d, want 1", c)
	}

	// Namespaces are per-platform.
	tb.Put("q", "a", mkLease("10.0.0.1", 3))
	if c := tb.IPLoad("q", ip1); c != 1 {
		t.Fatalf("IPLoad(q, ip1) = %d, want 1", c)
	}
	if c := tb.IPLoad("p", ip1); c != 1 {
		t.Fatalf("IPLoad(p, ip1) = %d, want 1 (platform isolation)", c)
	}

	// Release: counter -1, entry gone.
	if !tb.Release("p", "b") {
		t.Fatal("Release should report true")
	}
	if c := tb.IPLoad("p", ip1); c != 0 {
		t.Fatalf("IPLoad after release = %d, want 0", c)
	}
	if _, ok := tb.Get("p", "b"); ok {
		t.Fatal("released lease should be gone")
	}
	if tb.Release("p", "b") {
		t.Fatal("second Release should report false")
	}
}

func TestGet_MissingLease(t *testing.T) {
	tb := NewLeaseTable()
	if _, ok := tb.Get("p", "nope"); ok {
		t.Fatal("expected miss")
	}
}

func TestGet_ReturnsExpiredAsIs(t *testing.T) {
	// Expiry is enforced by the scheduler's expiry queue, never by scan:
	// Get returns expired leases untouched.
	tb := NewLeaseTable()
	l := mkLease("10.0.0.1", 1)
	l.Expiry = time.Now().Add(-time.Second)
	tb.Put("p", "a", l)
	got, ok := tb.Get("p", "a")
	if !ok {
		t.Fatal("expected hit")
	}
	if !got.Expired(time.Now()) {
		t.Fatal("expected the lease to read as expired")
	}
}

func TestRange(t *testing.T) {
	tb := NewLeaseTable()
	tb.Put("p", "a", mkLease("10.0.0.1", 1))
	tb.Put("p", "b", mkLease("10.0.0.2", 2))
	seen := map[string]bool{}
	tb.Range(func(platform, account string, l pklease.Lease) bool {
		seen[platform+"/"+account] = true
		return true
	})
	if len(seen) != 2 || !seen["p/a"] || !seen["p/b"] {
		t.Fatalf("Range saw %v", seen)
	}
}
