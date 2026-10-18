package geoip

import (
	"net/netip"
	"testing"
)

func TestOpen_EmptyPathYieldsEmptyProvider(t *testing.T) {
	p, err := Open("")
	if err != nil {
		t.Fatalf("Open(\"\") = %v, want nil error", err)
	}
	if got := p.Enrich(netip.MustParseAddr("8.8.8.8")); len(got) != 0 {
		t.Fatalf("Enrich on empty provider = %v, want empty map", got)
	}
	if got := p.Enrich(netip.Addr{}); len(got) != 0 {
		t.Fatalf("Enrich on invalid IP = %v, want empty map", got)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close on empty provider = %v", err)
	}
}

func TestOpen_MissingFileErrors(t *testing.T) {
	if _, err := Open("/nonexistent/GeoLite2-City.mmdb"); err == nil {
		t.Fatal("Open(missing) = nil, want error")
	}
}

func TestEnrich_NilProviderIsSafe(t *testing.T) {
	var p *Provider
	if got := p.Enrich(netip.MustParseAddr("1.1.1.1")); len(got) != 0 {
		t.Fatalf("Enrich on nil provider = %v, want empty map", got)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close on nil provider = %v", err)
	}
}
