package view

import (
	"math/rand/v2"
	"net/netip"
	"testing"

	pknode "github.com/xiarsx/egress/pkg/node"
)

func mkHash(b byte) pknode.Hash {
	var h pknode.Hash
	h[0] = b
	h[1] = b ^ 0xff
	return h
}

func mkSnap(b byte, ip string) pknode.NodeSnapshot {
	var addr netip.Addr
	if ip != "" {
		var err error
		addr, err = netip.ParseAddr(ip)
		if err != nil {
			panic(err)
		}
	}
	return pknode.NodeSnapshot{Hash: mkHash(b), EgressIP: addr}
}

func TestDerive_Apply_Remove(t *testing.T) {
	v := Derive([]pknode.NodeSnapshot{mkSnap(1, "10.0.0.1"), mkSnap(2, "10.0.0.2")}, nil)
	if v.Size() != 2 {
		t.Fatalf("Size = %d, want 2", v.Size())
	}
	if !v.Contains(mkHash(1)) || !v.Contains(mkHash(2)) {
		t.Fatal("Contains missing member")
	}
	if v.Contains(mkHash(3)) {
		t.Fatal("Contains false positive")
	}

	// Non-routable snapshot is not added.
	v2 := Derive(nil, func(n pknode.NodeSnapshot) bool { return false })
	if added, removed := v2.Apply(mkSnap(1, "10.0.0.1")); added || removed || v2.Size() != 0 {
		t.Fatalf("non-routable Apply = (%v,%v) size=%d", added, removed, v2.Size())
	}

	// Apply removes a member that stopped being routable.
	flip := true
	v3 := Derive([]pknode.NodeSnapshot{mkSnap(1, "10.0.0.1")}, func(n pknode.NodeSnapshot) bool { return flip })
	if added, removed := v3.Apply(mkSnap(1, "10.0.0.1")); added || removed {
		t.Fatal("re-apply of member should be a no-op")
	}
	flip = false
	if added, removed := v3.Apply(mkSnap(1, "10.0.0.1")); added || !removed || v3.Size() != 0 {
		t.Fatalf("Apply = (%v,%v) size=%d, want (false,true) size=0", added, removed, v3.Size())
	}

	// Remove of absent hash is a no-op.
	if v3.Remove(mkHash(9)) {
		t.Fatal("Remove of absent hash should return false")
	}
}

func TestByEgressIP_Index(t *testing.T) {
	v := Derive([]pknode.NodeSnapshot{
		mkSnap(1, "10.0.0.1"),
		mkSnap(2, "10.0.0.1"),
		mkSnap(3, "10.0.0.2"),
		mkSnap(4, ""), // no egress IP: member, but not indexed
	}, nil)
	ip1, _ := netip.ParseAddr("10.0.0.1")
	ip2, _ := netip.ParseAddr("10.0.0.2")

	got := v.ByEgressIP(ip1)
	if len(got) != 2 {
		t.Fatalf("ByEgressIP(10.0.0.1) = %d hashes, want 2", len(got))
	}
	if len(v.ByEgressIP(ip2)) != 1 {
		t.Fatal("ByEgressIP(10.0.0.2) should have 1 hash")
	}

	// Removal updates the index.
	v.Remove(mkHash(1))
	if got := v.ByEgressIP(ip1); len(got) != 1 || got[0] != mkHash(2) {
		t.Fatalf("after Remove, ByEgressIP = %v", got)
	}

	// Egress IP change on re-Apply moves the index entry.
	v.Apply(mkSnap(2, "10.0.0.2"))
	if len(v.ByEgressIP(ip1)) != 0 {
		t.Fatal("old IP index should be empty after move")
	}
	if len(v.ByEgressIP(ip2)) != 2 {
		t.Fatalf("new IP index should have 2 hashes, got %d", len(v.ByEgressIP(ip2)))
	}
	if v.Size() != 3 { // h1 removed, h2/h3/h4 remain
		t.Fatalf("Size = %d, want 3", v.Size())
	}
}

func TestRandomPick_UniformCoverage(t *testing.T) {
	const n = 200
	snaps := make([]pknode.NodeSnapshot, n)
	for i := range snaps {
		snaps[i] = mkSnap(byte(i), "")
	}
	v := Derive(snaps, nil)
	rng := rand.New(rand.NewPCG(42, 1))
	seen := map[pknode.Hash]int{}
	for i := 0; i < 20*n; i++ {
		h, ok := v.RandomPick(rng)
		if !ok {
			t.Fatal("RandomPick failed on non-empty view")
		}
		seen[h]++
	}
	if len(seen) != n {
		t.Fatalf("only %d/%d distinct hashes picked", len(seen), n)
	}
	// Rough uniformity: every shard-visible member picked at least a few times.
	for h, c := range seen {
		if c < 5 {
			t.Fatalf("hash %v picked only %d times in %d draws", h, c, 20*n)
		}
	}
}

func TestRandomPick_Empty(t *testing.T) {
	v := Derive(nil, nil)
	if _, ok := v.RandomPick(rand.New(rand.NewPCG(1, 1))); ok {
		t.Fatal("RandomPick on empty view should return ok=false")
	}
}

func TestRange(t *testing.T) {
	v := Derive([]pknode.NodeSnapshot{mkSnap(1, ""), mkSnap(2, ""), mkSnap(3, "")}, nil)
	count := 0
	v.Range(func(h pknode.Hash) bool { count++; return true })
	if count != 3 {
		t.Fatalf("Range visited %d, want 3", count)
	}
	count = 0
	v.Range(func(h pknode.Hash) bool { count++; return false })
	if count != 1 {
		t.Fatalf("Range early stop visited %d, want 1", count)
	}
}
