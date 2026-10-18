// Package node defines the pure node-domain types shared by the kernel.
//
// It performs no I/O and holds no mutable global state. These types live in
// pkg (rather than internal/core) because the iron rule forbids
// internal/core/* and internal/ports from importing other internal/
// packages, while node, view, lease, select, predicate and ports all need
// the same definitions.
package node

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"strconv"
	"time"
)

// Hash is the 128-bit identity of a node: xxh3-128 over the canonicalized
// raw options with the "tag" field removed. Two nodes with identical
// configuration (ignoring tag) share a Hash.
type Hash [16]byte

// Zero is the zero-value Hash.
var Zero Hash

// String returns the lowercase hex encoding of the hash.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// ParseHash parses a 32-char lowercase hex string into a Hash.
func ParseHash(s string) (Hash, error) {
	var h Hash
	if len(s) != 32 {
		return Zero, fmt.Errorf("node: bad hash length %d", len(s))
	}
	if _, err := hex.Decode(h[:], []byte(s)); err != nil {
		return Zero, fmt.Errorf("node: bad hash %q: %w", s, err)
	}
	return h, nil
}

// ScopedTag is a "<sourceName>/<rawTag>" string. Tags from every source are
// merged into the node snapshot at sync time, so the filter path never
// consults sources.
type ScopedTag string

// AttrKey is a namespaced attribute key, e.g. "geo.country", "asn.number".
type AttrKey string

// AttrValue is one attribute value, either textual or numeric.
type AttrValue struct {
	Str   string
	Num   float64
	IsNum bool
}

// StringAttr builds a textual attribute value.
func StringAttr(s string) AttrValue { return AttrValue{Str: s} }

// NumberAttr builds a numeric attribute value. Str is set to the
// shortest round-trip form so textual predicates (in/notIn) keep working.
func NumberAttr(n float64) AttrValue {
	return AttrValue{Str: strconv.FormatFloat(n, 'f', -1, 64), Num: n, IsNum: true}
}

// HealthKind is the discriminant of the Health sum type.
type HealthKind int

const (
	HealthProbing HealthKind = iota
	HealthHealthy
	HealthTripped
)

// Health is the sum type from design §2.3:
//
//	Probing                // newly added or just recovered; not yet observed
//	Healthy { Failures n } // n < MaxConsecutiveFailures
//	Tripped { Since t }    // breaker open; takes no traffic, still probed
//
// Routability is derived, never stored (see core/node Routable).
type Health struct {
	Kind     HealthKind
	Failures int       // valid when Kind == HealthHealthy
	Since    time.Time // valid when Kind == HealthTripped
}

// ProbingHealth is the initial health of an added node.
func ProbingHealth() Health { return Health{Kind: HealthProbing} }

// HealthyHealth builds a Healthy state with n consecutive failures.
func HealthyHealth(n int) Health { return Health{Kind: HealthHealthy, Failures: n} }

// TrippedHealth builds a Tripped state opened at since.
func TrippedHealth(since time.Time) Health { return Health{Kind: HealthTripped, Since: since} }

// NodeSnapshot is the immutable runtime image of one node (design §2.2).
// Static fields never change after creation; observed fields are replaced
// wholesale (COW) by their single writer.
type NodeSnapshot struct {
	Hash        Hash
	RawOptions  []byte
	HasOutbound bool // an outbound instance is built and ready
	Tags        []ScopedTag
	Attrs       map[AttrKey]AttrValue
	EgressIP    netip.Addr // invalid = unknown
	Latency     time.Duration
	HasLatency  bool
	Health      Health
}

// Predicate is a compiled node predicate (tag / attribute filter).
// Predicates are compiled once on the cold path (platform config change);
// the hot path only observes View membership, never evaluates predicates.
type Predicate func(NodeSnapshot) bool
