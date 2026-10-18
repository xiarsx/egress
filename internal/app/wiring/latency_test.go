package wiring

import (
	"math"
	"testing"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

func TestTrackerUpdateAndLatency(t *testing.T) {
	tr := NewTracker()
	var h pknode.Hash
	h[0] = 1
	at := time.Now()
	tr.Update(h, "www.example.com", 100*time.Millisecond, at, false)

	got, ok := tr.Latency(h, "www.example.com")
	if !ok {
		t.Fatal("expected latency sample")
	}
	if got != 100*time.Millisecond {
		t.Fatalf("got %v, want 100ms", got)
	}
	// Same eTLD+1 bucket: different subdomain hits the same bucket.
	if _, ok := tr.Latency(h, "api.example.com"); !ok {
		t.Fatal("expected same-bucket hit for api.example.com")
	}
	// Different eTLD+1: unknown.
	if _, ok := tr.Latency(h, "other.net"); ok {
		t.Fatal("unexpected hit for other.net")
	}
}

func TestTrackerEWMA(t *testing.T) {
	tr := NewTracker(WithDecayWindow(time.Minute))
	var h pknode.Hash
	h[0] = 2
	at := time.Now()
	tr.Update(h, "example.com", 100*time.Millisecond, at, false)
	// Second sample one decay window later: w = e^-1 ≈ 0.368.
	at2 := at.Add(time.Minute)
	tr.Update(h, "example.com", 200*time.Millisecond, at2, false)
	got, ok := tr.Latency(h, "example.com")
	if !ok {
		t.Fatal("expected latency sample")
	}
	// Freeze time for the recency check.
	w := math.Exp(-1) // one decay window elapsed
	f := float64(100*time.Millisecond)*w + float64(200*time.Millisecond)*(1-w)
	want := time.Duration(f)
	if d := got - want; d < -time.Millisecond || d > time.Millisecond {
		t.Fatalf("got %v, want ~%v", got, want)
	}
}

func TestTrackerStale(t *testing.T) {
	tr := NewTracker(WithDecayWindow(time.Minute))
	var h pknode.Hash
	h[0] = 3
	old := time.Now().Add(-time.Hour)
	tr.Update(h, "example.com", 100*time.Millisecond, old, false)
	if _, ok := tr.Latency(h, "example.com"); ok {
		t.Fatal("stale sample must not be reported")
	}
}

func TestTrackerAuthorityPinned(t *testing.T) {
	tr := NewTracker(WithNormalBuckets(1))
	var h pknode.Hash
	h[0] = 4
	at := time.Now()
	tr.Update(h, "probe.example.com", 50*time.Millisecond, at, true)
	// Fill the 1-slot normal LRU with two other domains.
	tr.Update(h, "a.com", 10*time.Millisecond, at, false)
	tr.Update(h, "b.com", 10*time.Millisecond, at, false)
	if _, ok := tr.Latency(h, "probe.example.com"); !ok {
		t.Fatal("authority bucket must survive normal-zone eviction")
	}
	if _, ok := tr.Latency(h, "a.com"); ok {
		t.Fatal("a.com should have been evicted from the 1-slot LRU")
	}
}

func TestEtld1(t *testing.T) {
	cases := map[string]string{
		"www.example.com": "example.com",
		"EXAMPLE.COM.":    "example.com",
		"localhost":       "localhost",
		"a.b.c.d":         "c.d",
	}
	for in, want := range cases {
		if got := etld1(in); got != want {
			t.Fatalf("etld1(%q) = %q, want %q", in, got, want)
		}
	}
}
