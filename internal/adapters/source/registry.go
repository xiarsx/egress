// Package source aggregates subscription snapshots into the node
// registry and drives periodic source refreshes.
//
// The Registry implements ports.NodeRegistry: it merges every enabled
// source's snapshot into one node set, keeps per-source tag contributions
// (scoped "<sourceName>/<rawTag>", interned with the standard unique
// package), and reference-counts nodes — a node whose reference set
// becomes empty is deleted. Health is the single-writer field of the
// HealthProjector (P4); the registry never changes it except assigning
// ProbingHealth to brand-new nodes.
package source

import (
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"
	"unique"

	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
	"github.com/zeebo/xxh3"
)

// HashOutbound returns the 128-bit node identity: xxh3-128 over the
// canonicalized raw options with the "tag" field removed (see
// pkg/node.Hash). Two nodes with identical configuration share a Hash.
func HashOutbound(rawOptions []byte) pknode.Hash {
	return pknode.Hash(xxh3.Hash128(rawOptions).Bytes())
}

// Registry is the node registry (ports.NodeRegistry).
//
// Snapshots are stored copy-on-write: readers (Get/All/SnapshotOf) receive
// deep copies and never observe a torn update. It is safe for concurrent
// use.
type Registry struct {
	mu      sync.RWMutex
	nodes   map[pknode.Hash]*nodeRecord
	sources map[string]*sourceRecord
	// OnApplySnapshot, when set, fires after each ApplySnapshot completes
	// (outside the registry lock). The composition root uses it to mirror
	// snapshots into the weak-persistence cache; the registry itself
	// stays a pure in-memory authority.
	OnApplySnapshot func(source string, nodes []pknode.NodeSnapshot)

	// onNodeChange is the single fine-grained observer (the view
	// deriver), fired per hash after adds/updates/removals, outside the
	// lock. Registered via SetOnNodeChange (ports.NodeRegistry).
	onNodeChange func(h pknode.Hash, removed bool)
}

// nodeRecord is one aggregated node: the merged snapshot plus the set of
// sources currently referencing it.
type nodeRecord struct {
	snap pknode.NodeSnapshot
	refs map[string]struct{}
}

// sourceRecord is one source's published snapshot: ordered hashes plus
// that source's own tag contribution per hash.
type sourceRecord struct {
	hashes []pknode.Hash
	tags   map[pknode.Hash][]pknode.ScopedTag
}

var _ ports.NodeRegistry = (*Registry)(nil)

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		nodes:   make(map[pknode.Hash]*nodeRecord),
		sources: make(map[string]*sourceRecord),
	}
}

// ApplySnapshot publishes source's new snapshot, diffing against its
// previous one:
//
//   - hashes new to this source gain a reference; a hash seen for the
//     first time becomes a node with Health=ProbingHealth(), empty Attrs
//     and this source's scoped tags;
//   - hashes dropped by this source lose a reference; nodes whose
//     reference set becomes empty are deleted;
//   - hashes kept by this source keep their node (health, attrs, egress
//     observations untouched) with only this source's tag contribution
//     replaced.
//
// Tags in nodes may be raw or already scoped; scoping is idempotent:
// "source/tag" is never double-prefixed.
func (r *Registry) ApplySnapshot(source string, nodes []pknode.NodeSnapshot) {
	byHash := make(map[pknode.Hash]pknode.NodeSnapshot, len(nodes))
	newHashes := make([]pknode.Hash, 0, len(nodes))
	newTags := make(map[pknode.Hash][]pknode.ScopedTag, len(nodes))
	for _, n := range nodes {
		if _, dup := byHash[n.Hash]; dup {
			continue // dedupe repeated hashes inside one snapshot
		}
		byHash[n.Hash] = n
		newHashes = append(newHashes, n.Hash)
		newTags[n.Hash] = scopeTags(source, n.Tags)
	}

	r.mu.Lock()
	var (
		// merged holds the post-publish snapshots for the callback;
		// cb is invoked after the lock is released.
		merged []pknode.NodeSnapshot
		cb     func(string, []pknode.NodeSnapshot)
	)
	// Change events for the fine-grained observer (deriver): adds and
	// removals, fired after unlock. Refreshes of kept nodes are covered
	// by the payload refresh below.
	var added, removed []pknode.Hash
	defer func() {
		r.mu.Unlock()
		if cb != nil {
			cb(source, merged)
		}
		if fn := r.onNodeChange; fn != nil {
			for _, h := range added {
				fn(h, false)
			}
			for _, h := range removed {
				fn(h, true)
			}
		}
	}()

	rec, ok := r.sources[source]
	if !ok {
		rec = &sourceRecord{tags: make(map[pknode.Hash][]pknode.ScopedTag)}
		r.sources[source] = rec
	}
	oldSet := make(map[pknode.Hash]struct{}, len(rec.hashes))
	for _, h := range rec.hashes {
		oldSet[h] = struct{}{}
	}
	newSet := make(map[pknode.Hash]struct{}, len(newHashes))
	for _, h := range newHashes {
		newSet[h] = struct{}{}
	}

	// Publish the new tag contribution before merging, so mergedTagsLocked
	// observes this source's current contribution for every hash.
	rec.tags = newTags
	for _, h := range newHashes {
		if _, kept := oldSet[h]; kept {
			// Re-published: refresh the snapshot payload (a new snapshot
			// may carry updated RawOptions/HasOutbound), then merge tags.
			if nr := r.nodes[h]; nr != nil {
				incoming := byHash[h]
				nr.snap.RawOptions = append([]byte(nil), incoming.RawOptions...)
				nr.snap.HasOutbound = incoming.HasOutbound
				nr.snap.Tags = r.mergedTagsLocked(h)
			}
			continue
		}
		if _, existed := r.nodes[h]; !existed {
			added = append(added, h)
		}
		r.addRefLocked(source, h, byHash[h], newTags[h])
	}
	for h := range oldSet {
		if _, kept := newSet[h]; !kept {
			before := len(r.nodes)
			r.releaseRefLocked(source, h)
			if len(r.nodes) < before {
				removed = append(removed, h)
			}
		}
	}
	rec.hashes = newHashes

	// Notify after the mutation is fully published. The merged snapshots
	// are read here under the write lock; the callback itself runs after
	// unlock (see the deferred function above).
	cb = r.OnApplySnapshot
	if cb != nil {
		merged = make([]pknode.NodeSnapshot, 0, len(newHashes))
		for _, h := range newHashes {
			if nr := r.nodes[h]; nr != nil {
				merged = append(merged, cloneSnapshot(nr.snap))
			}
		}
	}
}

// SnapshotOf returns source's current snapshot in publish order.
func (r *Registry) SnapshotOf(source string) []pknode.NodeSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.sources[source]
	if !ok {
		return nil
	}
	out := make([]pknode.NodeSnapshot, 0, len(rec.hashes))
	for _, h := range rec.hashes {
		if nr, ok := r.nodes[h]; ok {
			out = append(out, cloneSnapshot(nr.snap))
		}
	}
	return out
}

// UpdateHealth replaces a node's health. The single writer is the
// HealthProjector (P4); unknown hashes are ignored.
func (r *Registry) UpdateHealth(h pknode.Hash, health pknode.Health) {
	r.mu.Lock()
	nr, ok := r.nodes[h]
	if ok {
		nr.snap.Health = health
	}
	r.mu.Unlock()
	if ok {
		r.fireNodeChange(h, false)
	}
}

// UpdateEgressIP records the egress IP learned by an egress probe
// (ports.NodeRegistry). Unknown hashes are ignored.
func (r *Registry) UpdateEgressIP(h pknode.Hash, ip netip.Addr) {
	r.mu.Lock()
	nr, ok := r.nodes[h]
	if ok && nr.snap.EgressIP != ip {
		nr.snap.EgressIP = ip
	}
	r.mu.Unlock()
	if ok {
		r.fireNodeChange(h, false)
	}
}

// UpdateLatency records a learned latency sample (ports.NodeRegistry).
// Unknown hashes are ignored.
func (r *Registry) UpdateLatency(h pknode.Hash, latency time.Duration) {
	r.mu.Lock()
	nr, ok := r.nodes[h]
	if ok {
		nr.snap.Latency = latency
		nr.snap.HasLatency = true
	}
	r.mu.Unlock()
	if ok {
		r.fireNodeChange(h, false)
	}
}

// UpdateAttrs merges learned attributes (ports.NodeRegistry). Unknown
// hashes are ignored.
func (r *Registry) UpdateAttrs(h pknode.Hash, attrs map[pknode.AttrKey]pknode.AttrValue) {
	r.mu.Lock()
	nr, ok := r.nodes[h]
	if ok {
		if nr.snap.Attrs == nil {
			nr.snap.Attrs = make(map[pknode.AttrKey]pknode.AttrValue, len(attrs))
		}
		for k, v := range attrs {
			nr.snap.Attrs[k] = v
		}
	}
	r.mu.Unlock()
	if ok {
		r.fireNodeChange(h, false)
	}
}

// SetOnNodeChange registers the single change observer
// (ports.NodeRegistry).
func (r *Registry) SetOnNodeChange(fn func(h pknode.Hash, removed bool)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onNodeChange = fn
}

// fireNodeChange invokes the observer outside the lock.
func (r *Registry) fireNodeChange(h pknode.Hash, removed bool) {
	r.mu.RLock()
	fn := r.onNodeChange
	r.mu.RUnlock()
	if fn != nil {
		fn(h, removed)
	}
}

// Get returns a copy of the node snapshot for h.
func (r *Registry) Get(h pknode.Hash) (pknode.NodeSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nr, ok := r.nodes[h]
	if !ok {
		return pknode.NodeSnapshot{}, false
	}
	return cloneSnapshot(nr.snap), true
}

// All returns copies of every node snapshot.
func (r *Registry) All() []pknode.NodeSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]pknode.NodeSnapshot, 0, len(r.nodes))
	for _, nr := range r.nodes {
		out = append(out, cloneSnapshot(nr.snap))
	}
	return out
}

// ---------------------------------------------------------------------------
// internals (r.mu held)
// ---------------------------------------------------------------------------

func (r *Registry) addRefLocked(source string, h pknode.Hash, incoming pknode.NodeSnapshot, tags []pknode.ScopedTag) {
	nr, ok := r.nodes[h]
	if !ok {
		snap := pknode.NodeSnapshot{
			Hash:        h,
			RawOptions:  append([]byte(nil), incoming.RawOptions...),
			HasOutbound: incoming.HasOutbound,
			Tags:        tags,
			Attrs:       make(map[pknode.AttrKey]pknode.AttrValue),
			Health:      pknode.ProbingHealth(),
		}
		// Cache-replayed snapshots arrive warm (egress IP, attrs, latency);
		// keep them. Source publishes carry zeros and change nothing.
		snap.EgressIP = incoming.EgressIP
		snap.HasLatency = incoming.HasLatency
		snap.Latency = incoming.Latency
		for k, v := range incoming.Attrs {
			snap.Attrs[k] = v
		}
		r.nodes[h] = &nodeRecord{snap: snap, refs: map[string]struct{}{source: {}}}
		return
	}
	nr.refs[source] = struct{}{}
	nr.snap.Tags = r.mergedTagsLocked(h)
}

func (r *Registry) releaseRefLocked(source string, h pknode.Hash) {
	nr, ok := r.nodes[h]
	if !ok {
		return
	}
	delete(nr.refs, source)
	if len(nr.refs) == 0 {
		delete(r.nodes, h)
		return
	}
	nr.snap.Tags = r.mergedTagsLocked(h)
}

// mergedTagsLocked unions every referencing source's tag contribution for
// h, deduplicated and sorted for determinism.
func (r *Registry) mergedTagsLocked(h pknode.Hash) []pknode.ScopedTag {
	nr := r.nodes[h]
	set := make(map[pknode.ScopedTag]struct{})
	for s := range nr.refs {
		if rec, ok := r.sources[s]; ok {
			for _, t := range rec.tags[h] {
				set[t] = struct{}{}
			}
		}
	}
	out := make([]pknode.ScopedTag, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// scopeTags prefixes raw tags with "source/", interning every string with
// the standard unique package (design §2.2). Already-scoped tags pass
// through unchanged so double application is harmless.
func scopeTags(source string, tags []pknode.ScopedTag) []pknode.ScopedTag {
	prefix := source + "/"
	out := make([]pknode.ScopedTag, 0, len(tags))
	for _, t := range tags {
		s := string(t)
		if !strings.HasPrefix(s, prefix) {
			s = prefix + s
		}
		out = append(out, pknode.ScopedTag(unique.Make(s).Value()))
	}
	return out
}

func cloneSnapshot(n pknode.NodeSnapshot) pknode.NodeSnapshot {
	c := n
	c.RawOptions = append([]byte(nil), n.RawOptions...)
	c.Tags = append([]pknode.ScopedTag(nil), n.Tags...)
	if n.Attrs != nil {
		c.Attrs = make(map[pknode.AttrKey]pknode.AttrValue, len(n.Attrs))
		for k, v := range n.Attrs {
			c.Attrs[k] = v
		}
	}
	return c
}
