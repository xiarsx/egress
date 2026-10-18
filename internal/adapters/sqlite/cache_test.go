package sqlite

import (
	"net/netip"
	"testing"
	"time"

	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
)

func testHash(b byte) pknode.Hash {
	var h pknode.Hash
	for i := range h {
		h[i] = b
	}
	return h
}

func openTestCache(t *testing.T) *Cache {
	t.Helper()
	c, err := OpenCache(t.TempDir() + "/cache.db")
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestCacheNodeRoundTrip(t *testing.T) {
	c := openTestCache(t)
	s := c.Store()

	since := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	nodes := []pknode.NodeSnapshot{
		{
			Hash:       testHash(0x01),
			RawOptions: []byte(`{"type":"vmess"}`),
			Tags:       []pknode.ScopedTag{"sub1/tag-a", "sub1/tag-b"},
			Attrs: map[pknode.AttrKey]pknode.AttrValue{
				"geo.country": pknode.StringAttr("us"),
				"asn.number":  pknode.NumberAttr(15169),
			},
			EgressIP: netip.MustParseAddr("1.2.3.4"),
			Health:   pknode.ProbingHealth(),
		},
		{
			Hash:       testHash(0x02),
			RawOptions: []byte(`{"type":"trojan"}`),
			Health:     pknode.HealthyHealth(2),
		},
		{
			Hash:       testHash(0x03),
			RawOptions: []byte(`{"type":"ss"}`),
			Health:     pknode.TrippedHealth(since),
		},
	}
	for _, n := range nodes {
		s.UpsertNode(n)
	}
	s.SetSourceNodes("sub1", []pknode.Hash{testHash(0x01), testHash(0x02)})
	s.SetSourceNodes("sub2", []pknode.Hash{testHash(0x03)})
	now := time.Now()
	s.UpsertLatency(testHash(0x01), "example.com", 123.5, now, false)
	s.UpsertLatency(testHash(0x01), "gstatic.com", 42.0, now.Add(time.Second), true)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, sourceNodes, leases, err := c.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("nodes = %d, want 3", len(got))
	}
	byHash := map[pknode.Hash]pknode.NodeSnapshot{}
	for _, n := range got {
		byHash[n.Hash] = n
	}

	n1 := byHash[testHash(0x01)]
	if string(n1.RawOptions) != `{"type":"vmess"}` {
		t.Fatalf("n1 raw options = %s", n1.RawOptions)
	}
	if len(n1.Tags) != 2 || n1.Tags[0] != "sub1/tag-a" || n1.Tags[1] != "sub1/tag-b" {
		t.Fatalf("n1 tags = %v", n1.Tags)
	}
	if n1.Attrs["geo.country"] != pknode.StringAttr("us") {
		t.Fatalf("n1 geo.country = %+v", n1.Attrs["geo.country"])
	}
	if av := n1.Attrs["asn.number"]; !av.IsNum || av.Num != 15169 || av.Str != "15169" {
		t.Fatalf("n1 asn.number = %+v", av)
	}
	if n1.EgressIP != netip.MustParseAddr("1.2.3.4") {
		t.Fatalf("n1 egress ip = %v", n1.EgressIP)
	}
	if n1.Health.Kind != pknode.HealthProbing {
		t.Fatalf("n1 health = %+v, want Probing", n1.Health)
	}
	// The most recent latency sample wins.
	if !n1.HasLatency || n1.Latency != 42*time.Millisecond {
		t.Fatalf("n1 latency = %v has=%v, want 42ms", n1.Latency, n1.HasLatency)
	}

	n2 := byHash[testHash(0x02)]
	if n2.Health.Kind != pknode.HealthHealthy || n2.Health.Failures != 2 {
		t.Fatalf("n2 health = %+v, want Healthy{2}", n2.Health)
	}
	if n2.HasLatency {
		t.Fatalf("n2 should have no latency")
	}

	n3 := byHash[testHash(0x03)]
	if n3.Health.Kind != pknode.HealthTripped || !n3.Health.Since.Equal(since) {
		t.Fatalf("n3 health = %+v, want Tripped{%v}", n3.Health, since)
	}

	if len(sourceNodes["sub1"]) != 2 || len(sourceNodes["sub2"]) != 1 {
		t.Fatalf("sourceNodes = %v", sourceNodes)
	}
	if len(leases) != 0 {
		t.Fatalf("leases = %v, want empty", leases)
	}
}

func TestCacheCoalescing(t *testing.T) {
	c := openTestCache(t)
	s := c.Store()

	h := testHash(0x0A)
	// Upsert then delete: only the delete survives the flush.
	s.UpsertNode(pknode.NodeSnapshot{Hash: h, RawOptions: []byte(`{}`)})
	s.DeleteNode(h)
	// Double delete is fine.
	s.DeleteNode(h)
	// SetSourceNodes replaces, last call wins.
	s.SetSourceNodes("sub", []pknode.Hash{h, testHash(0x0B)})
	s.SetSourceNodes("sub", []pknode.Hash{testHash(0x0B)})

	l := pklease.Lease{
		EgressIP:     netip.MustParseAddr("9.9.9.9"),
		NodeHint:     h,
		Expiry:       time.Now().Add(time.Hour).Round(0),
		LastAccessed: time.Now().Round(0),
	}
	s.UpsertLease("plat", "alice", l)
	// Upsert then delete: lease must not survive.
	s.DeleteLease("plat", "alice")
	// Re-upsert after delete: survives again.
	s.UpsertLease("plat", "bob", l)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	nodes, sourceNodes, leases, err := c.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes = %d, want 0 after coalesced delete", len(nodes))
	}
	if got := sourceNodes["sub"]; len(got) != 1 || got[0] != testHash(0x0B) {
		t.Fatalf("sourceNodes[sub] = %v, want [0x0B]", got)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %d, want 1", len(leases))
	}
	rec := leases[0]
	if rec.Platform != "plat" || rec.Account != "bob" {
		t.Fatalf("lease id = %v/%v", rec.Platform, rec.Account)
	}
	if rec.Lease.EgressIP != netip.MustParseAddr("9.9.9.9") || rec.Lease.NodeHint != h {
		t.Fatalf("lease = %+v", rec.Lease)
	}
	if !rec.Lease.Expiry.Equal(l.Expiry) || !rec.Lease.LastAccessed.Equal(l.LastAccessed) {
		t.Fatalf("lease times = %+v, want %+v", rec.Lease, l)
	}
}

func TestCacheAttrsMergeIntoNodeUpsert(t *testing.T) {
	c := openTestCache(t)
	s := c.Store()

	h := testHash(0x0C)
	// Node upserted without attrs, then attrs staged: flush must persist both.
	s.UpsertNode(pknode.NodeSnapshot{Hash: h, Health: pknode.ProbingHealth()})
	s.UpsertAttrs(h, map[pknode.AttrKey]pknode.AttrValue{"geo.country": pknode.StringAttr("de")})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	nodes, _, _, err := c.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Attrs["geo.country"] != pknode.StringAttr("de") {
		t.Fatalf("nodes = %+v", nodes)
	}

	// Standalone attr update on the persisted node.
	s.UpsertAttrs(h, map[pknode.AttrKey]pknode.AttrValue{"geo.country": pknode.StringAttr("fr")})
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	nodes, _, _, err = c.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Attrs["geo.country"] != pknode.StringAttr("fr") {
		t.Fatalf("nodes after attr update = %+v", nodes)
	}

	// Deleting the node also drops its latency rows on flush.
	s.UpsertLatency(h, "example.com", 10, time.Now(), false)
	s.DeleteNode(h)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	nodes, _, _, err = c.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes = %d, want 0", len(nodes))
	}
}

func TestCacheFlushEmptyAndClose(t *testing.T) {
	c := openTestCache(t)
	if err := c.Store().Flush(); err != nil {
		t.Fatalf("empty Flush: %v", err)
	}
	// Close flushes implicitly.
	c2, err := OpenCache(t.TempDir() + "/cache2.db")
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	c2.Store().UpsertNode(pknode.NodeSnapshot{Hash: testHash(0xFF)})
	if err := c2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
