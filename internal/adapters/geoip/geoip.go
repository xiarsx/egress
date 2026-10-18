// Package geoip is the cold-path AttributeProvider that enriches node
// snapshots with GeoIP attributes (design §5.4: Attrs are the extension
// lever; the old RegionFilters are just a predicate on geo.country).
//
// It wraps a MaxMind DB reader. The provider is a cold-path component:
// enrichment happens at sync/probe time, never on the hot path.
package geoip

import (
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/oschwald/maxminddb-golang"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// Attribute keys emitted by Enrich. The country code is lowercase ISO
// (e.g. "us"), matching the predicate syntax filter.attrs.geo.country.in.
const (
	AttrCountry   pknode.AttrKey = "geo.country"
	AttrContinent pknode.AttrKey = "geo.continent"
	AttrCity      pknode.AttrKey = "geo.city"
	AttrLatitude  pknode.AttrKey = "geo.latitude"
	AttrLongitude pknode.AttrKey = "geo.longitude"
)

// Provider enriches IP addresses with GeoIP attributes. The zero value (or
// a Provider from Open("") with an empty path) is a valid empty provider:
// Enrich returns an empty map and never errors.
//
// TODO: ship the DB. Download GeoLite2-City from MaxMind (free account +
// license key required):
//
//	https://www.maxmind.com/en/geolite2/signup
//	https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=<KEY>&suffix=tar.gz
//
// Point the geoip db path at the extracted GeoLite2-City.mmdb. A scheduled
// refresh (design §8 "GeoIP 与 AttributeProvider 刷新") can swap the
// provider by calling Open again — maxminddb.Reader is safe for concurrent
// use, so the old Provider can simply be replaced.
type Provider struct {
	db *maxminddb.Reader
}

// Open opens the MaxMind DB at path. An empty path returns an empty
// Provider (no error): enrichment yields no attributes until a DB is
// configured.
func Open(path string) (*Provider, error) {
	if path == "" {
		return &Provider{}, nil
	}
	db, err := maxminddb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("geoip: open %q: %w", path, err)
	}
	return &Provider{db: db}, nil
}

// Close releases the DB reader. Safe to call on an empty Provider.
func (p *Provider) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	return p.db.Close()
}

// geoRecord mirrors the GeoLite2-City fields we expose as attributes.
type geoRecord struct {
	Country struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	Continent struct {
		Code string `maxminddb:"code"`
	} `maxminddb:"continent"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Location struct {
		Latitude  float64 `maxminddb:"latitude"`
		Longitude float64 `maxminddb:"longitude"`
	} `maxminddb:"location"`
}

// Enrich returns the GeoIP attributes for ip. It returns an empty map when
// the provider has no DB, the IP is invalid/unset, the lookup fails, or the
// address is not in the database. maxminddb.Reader is safe for concurrent
// use, so Enrich is safe for concurrent use.
func (p *Provider) Enrich(ip netip.Addr) map[pknode.AttrKey]pknode.AttrValue {
	out := make(map[pknode.AttrKey]pknode.AttrValue)
	if p == nil || p.db == nil || !ip.IsValid() || ip.IsUnspecified() {
		return out
	}
	var rec geoRecord
	if err := p.db.Lookup(net.IP(ip.AsSlice()), &rec); err != nil {
		return out
	}
	if code := strings.ToLower(strings.TrimSpace(rec.Country.IsoCode)); code != "" {
		out[AttrCountry] = pknode.StringAttr(code)
	}
	if code := strings.ToLower(strings.TrimSpace(rec.Continent.Code)); code != "" {
		out[AttrContinent] = pknode.StringAttr(code)
	}
	if name := cityName(rec.City.Names); name != "" {
		out[AttrCity] = pknode.StringAttr(name)
	}
	if rec.Location.Latitude != 0 || rec.Location.Longitude != 0 {
		out[AttrLatitude] = pknode.NumberAttr(rec.Location.Latitude)
		out[AttrLongitude] = pknode.NumberAttr(rec.Location.Longitude)
	}
	return out
}

// cityName prefers the English name, falling back to any available name.
func cityName(names map[string]string) string {
	if names == nil {
		return ""
	}
	if n := names["en"]; n != "" {
		return n
	}
	for _, n := range names {
		if n != "" {
			return n
		}
	}
	return ""
}
