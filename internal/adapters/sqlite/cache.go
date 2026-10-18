package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// cacheMigrations is the version chain for cache.db (design §9.1: weak
// consistency — losing the last flush window is acceptable).
var cacheMigrations = []migration{
	{
		version: 1,
		name:    "create-cache-tables",
		up: `
CREATE TABLE IF NOT EXISTS nodes(
	hash            BLOB PRIMARY KEY,
	raw_options     BLOB NOT NULL,
	tags            TEXT NOT NULL,  -- JSON array of "<source>/<tag>"
	attrs           TEXT NOT NULL,  -- JSON object
	health_kind     INTEGER NOT NULL,
	health_failures INTEGER NOT NULL DEFAULT 0,
	health_since    INTEGER NOT NULL DEFAULT 0,  -- unixnano, valid when tripped
	egress_ip       TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS node_latency(
	hash      BLOB NOT NULL,
	domain    TEXT NOT NULL,
	ms        REAL NOT NULL,
	at        INTEGER NOT NULL,  -- unixnano of the observation
	authority INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY(hash, domain)
);
CREATE INDEX IF NOT EXISTS idx_node_latency_hash ON node_latency(hash);
CREATE TABLE IF NOT EXISTS source_nodes(
	source TEXT NOT NULL,
	hash   BLOB NOT NULL,
	PRIMARY KEY(source, hash)
);
CREATE INDEX IF NOT EXISTS idx_source_nodes_hash ON source_nodes(hash);
CREATE TABLE IF NOT EXISTS leases(
	platform      TEXT NOT NULL,
	account       TEXT NOT NULL,
	egress_ip     TEXT NOT NULL,
	node_hint     BLOB NOT NULL,
	expiry        INTEGER NOT NULL,  -- unixnano, fixed at creation
	last_accessed INTEGER NOT NULL,  -- unixnano
	PRIMARY KEY(platform, account)
);`,
	},
	{
		version: 2,
		name:    "nodes-has-outbound",
		up:      `ALTER TABLE nodes ADD COLUMN has_outbound INTEGER NOT NULL DEFAULT 0;`,
	},
}

// latencyKey addresses one (node, domain) latency sample in the dirty set.
type latencyKey struct {
	hash   pknode.Hash
	domain string
}

// latencyVal is one buffered latency observation.
type latencyVal struct {
	ms        float64
	at        time.Time
	authority bool
}

// Cache is the cache.db handle. It implements ports.CacheLoader (reading
// the last flushed state) and vends a ports.CacheStore for the write side.
//
// Write path (design §9.1, BatchSink/Coalescing): the CacheStore methods
// only stage changes in memory. For every key only the last Upsert or
// Delete is kept; Flush writes the whole dirty set in one transaction.
// LoadSnapshot intentionally reads the last *flushed* state, not the dirty
// set — at startup nothing is dirty yet, which is exactly the reconcile
// input (design §9.2 step 2).
type Cache struct {
	db *sql.DB

	mu        sync.Mutex
	nodes     map[string]pknode.NodeSnapshot // hash bytes -> snapshot (upserts)
	delNodes  map[string]struct{}            // hash bytes -> deleted
	sources   map[string][]pknode.Hash       // source -> full replacement set
	latency   map[latencyKey]latencyVal
	attrs     map[string]map[pknode.AttrKey]pknode.AttrValue // hash bytes -> full replacement
	leases    map[string]pklease.Lease                       // platform\x00account -> lease
	delLeases map[string]struct{}

	store *CacheStore
}

var _ ports.CacheLoader = (*Cache)(nil)
var _ ports.CacheStore = (*CacheStore)(nil)

// OpenCache opens (creating when missing) the cache.db at path and runs
// pending migrations.
//
// Corruption contract (design §9.2 step 2): when the file exists but is
// unreadable, OpenCache returns an error instead of silently discarding
// it. The reconcile owner treats that as "cache empty" by removing the
// file and reopening — the decision stays with the caller, not the adapter.
func OpenCache(path string) (*Cache, error) {
	db, err := openDB(path, "")
	if err != nil {
		return nil, fmt.Errorf("sqlite: open cache db: %w", err)
	}
	if err := runMigrations(context.Background(), db, cacheMigrations); err != nil {
		db.Close()
		return nil, err
	}
	c := &Cache{
		db:        db,
		nodes:     map[string]pknode.NodeSnapshot{},
		delNodes:  map[string]struct{}{},
		sources:   map[string][]pknode.Hash{},
		latency:   map[latencyKey]latencyVal{},
		attrs:     map[string]map[pknode.AttrKey]pknode.AttrValue{},
		leases:    map[string]pklease.Lease{},
		delLeases: map[string]struct{}{},
	}
	c.store = &CacheStore{c: c}
	return c, nil
}

// Store returns the coalescing write side of this cache.
func (c *Cache) Store() *CacheStore { return c.store }

// Close flushes pending writes and then closes the database.
func (c *Cache) Close() error {
	if err := c.Flush(); err != nil {
		c.db.Close()
		return err
	}
	return c.db.Close()
}

// DB exposes the underlying handle for tests and tooling. It is not part
// of any ports interface.
func (c *Cache) DB() *sql.DB { return c.db }

// ---------------------------------------------------------------------------
// Load (ports.CacheLoader)
// ---------------------------------------------------------------------------

// attrJSON is the on-disk form of one pknode.AttrValue.
type attrJSON struct {
	Str   string  `json:"s"`
	Num   float64 `json:"n,omitempty"`
	IsNum bool    `json:"num"`
}

func encodeTags(tags []pknode.ScopedTag) (string, error) {
	if tags == nil {
		tags = []pknode.ScopedTag{}
	}
	s := make([]string, len(tags))
	for i, t := range tags {
		s[i] = string(t)
	}
	b, err := json.Marshal(s)
	return string(b), err
}

func decodeTags(raw string) ([]pknode.ScopedTag, error) {
	var s []string
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, err
	}
	tags := make([]pknode.ScopedTag, len(s))
	for i, t := range s {
		tags[i] = pknode.ScopedTag(t)
	}
	return tags, nil
}

func encodeAttrs(attrs map[pknode.AttrKey]pknode.AttrValue) (string, error) {
	if attrs == nil {
		attrs = map[pknode.AttrKey]pknode.AttrValue{}
	}
	m := make(map[string]attrJSON, len(attrs))
	for k, v := range attrs {
		m[string(k)] = attrJSON{Str: v.Str, Num: v.Num, IsNum: v.IsNum}
	}
	b, err := json.Marshal(m)
	return string(b), err
}

func decodeAttrs(raw string) (map[pknode.AttrKey]pknode.AttrValue, error) {
	var m map[string]attrJSON
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	attrs := make(map[pknode.AttrKey]pknode.AttrValue, len(m))
	for k, v := range m {
		attrs[pknode.AttrKey(k)] = pknode.AttrValue{Str: v.Str, Num: v.Num, IsNum: v.IsNum}
	}
	return attrs, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func encodeHealth(h pknode.Health) (kind, failures int, since int64) {	kind = int(h.Kind)
	failures = h.Failures
	if h.Kind == pknode.HealthTripped {
		since = h.Since.UTC().UnixNano()
	}
	return kind, failures, since
}

func decodeHealth(kind, failures int, since int64) pknode.Health {
	switch pknode.HealthKind(kind) {
	case pknode.HealthHealthy:
		return pknode.HealthyHealth(failures)
	case pknode.HealthTripped:
		return pknode.TrippedHealth(time.Unix(0, since).UTC())
	default:
		// Unknown kinds (e.g. from a newer writer) fall back to Probing:
		// the node is simply treated as not yet observed.
		return pknode.ProbingHealth()
	}
}

// LoadSnapshot implements ports.CacheLoader. It returns the last flushed
// state: node snapshots (with the most recent latency sample folded in),
// the source -> hashes membership map, and the persisted leases.
func (c *Cache) LoadSnapshot() (nodes []pknode.NodeSnapshot, sourceNodes map[string][]pknode.Hash, leases []ports.LeaseRecord, err error) {
	ctx := context.Background()

	rows, err := c.db.QueryContext(ctx, `
		SELECT hash, raw_options, tags, attrs, health_kind, health_failures, health_since, egress_ip, has_outbound
		FROM nodes`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("sqlite: load nodes: %w", err)
	}
	byHash := map[string]int{}
	nodes = []pknode.NodeSnapshot{}
	for rows.Next() {
		var (
			hashBytes                   []byte
			rawOptions                  []byte
			tagsRaw, attrsRaw, egressIP string
			healthKind, healthFailures  int
			healthSince                 int64
			hasOutboundInt              int
		)
		if err := rows.Scan(&hashBytes, &rawOptions, &tagsRaw, &attrsRaw,
			&healthKind, &healthFailures, &healthSince, &egressIP, &hasOutboundInt); err != nil {
			rows.Close()
			return nil, nil, nil, fmt.Errorf("sqlite: scan node: %w", err)
		}
		var h pknode.Hash
		copy(h[:], hashBytes)
		tags, err := decodeTags(tagsRaw)
		if err != nil {
			rows.Close()
			return nil, nil, nil, fmt.Errorf("sqlite: decode tags: %w", err)
		}
		attrs, err := decodeAttrs(attrsRaw)
		if err != nil {
			rows.Close()
			return nil, nil, nil, fmt.Errorf("sqlite: decode attrs: %w", err)
		}
		n := pknode.NodeSnapshot{
			Hash:        h,
			RawOptions:  rawOptions,
			HasOutbound: hasOutboundInt != 0,
			Tags:        tags,
			Attrs:       attrs,
			Health:      decodeHealth(healthKind, healthFailures, healthSince),
		}
		if addr, perr := netip.ParseAddr(egressIP); perr == nil {
			n.EgressIP = addr
		}
		byHash[string(h[:])] = len(nodes)
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, nil, fmt.Errorf("sqlite: iterate nodes: %w", err)
	}
	rows.Close()

	// Fold in the most recent latency sample per node (used only as a
	// seed; the in-memory TD-EWMA table is rebuilt from observations).
	lrows, err := c.db.QueryContext(ctx, `
		SELECT hash, ms FROM node_latency ORDER BY hash, at DESC`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("sqlite: load latency: %w", err)
	}
	seenLatency := map[string]bool{}
	for lrows.Next() {
		var hashBytes []byte
		var ms float64
		if err := lrows.Scan(&hashBytes, &ms); err != nil {
			lrows.Close()
			return nil, nil, nil, fmt.Errorf("sqlite: scan latency: %w", err)
		}
		k := string(hashBytes)
		if seenLatency[k] {
			continue // rows are ordered by at DESC; first wins
		}
		seenLatency[k] = true
		if idx, ok := byHash[k]; ok {
			nodes[idx].Latency = time.Duration(ms * float64(time.Millisecond))
			nodes[idx].HasLatency = true
		}
	}
	if err := lrows.Err(); err != nil {
		lrows.Close()
		return nil, nil, nil, fmt.Errorf("sqlite: iterate latency: %w", err)
	}
	lrows.Close()

	sourceNodes = map[string][]pknode.Hash{}
	srows, err := c.db.QueryContext(ctx, `SELECT source, hash FROM source_nodes ORDER BY source`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("sqlite: load source_nodes: %w", err)
	}
	for srows.Next() {
		var source string
		var hashBytes []byte
		if err := srows.Scan(&source, &hashBytes); err != nil {
			srows.Close()
			return nil, nil, nil, fmt.Errorf("sqlite: scan source_nodes: %w", err)
		}
		var h pknode.Hash
		copy(h[:], hashBytes)
		sourceNodes[source] = append(sourceNodes[source], h)
	}
	if err := srows.Err(); err != nil {
		srows.Close()
		return nil, nil, nil, fmt.Errorf("sqlite: iterate source_nodes: %w", err)
	}
	srows.Close()

	leases = []ports.LeaseRecord{}
	lrrows, err := c.db.QueryContext(ctx, `
		SELECT platform, account, egress_ip, node_hint, expiry, last_accessed FROM leases`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("sqlite: load leases: %w", err)
	}
	for lrrows.Next() {
		var (
			platform, account, egressIP string
			hintBytes                   []byte
			expiry, lastAccessed        int64
		)
		if err := lrrows.Scan(&platform, &account, &egressIP, &hintBytes, &expiry, &lastAccessed); err != nil {
			lrrows.Close()
			return nil, nil, nil, fmt.Errorf("sqlite: scan lease: %w", err)
		}
		rec := ports.LeaseRecord{Platform: platform, Account: account}
		if addr, perr := netip.ParseAddr(egressIP); perr == nil {
			rec.Lease.EgressIP = addr
		}
		copy(rec.Lease.NodeHint[:], hintBytes)
		rec.Lease.Expiry = time.Unix(0, expiry).UTC()
		rec.Lease.LastAccessed = time.Unix(0, lastAccessed).UTC()
		leases = append(leases, rec)
	}
	if err := lrrows.Err(); err != nil {
		lrrows.Close()
		return nil, nil, nil, fmt.Errorf("sqlite: iterate leases: %w", err)
	}
	lrrows.Close()

	return nodes, sourceNodes, leases, nil
}

// ---------------------------------------------------------------------------
// Store (ports.CacheStore, coalescing)
// ---------------------------------------------------------------------------

// CacheStore is the write side of cache.db. Every method only stages the
// change in memory (coalescing: for one key only the last Upsert or Delete
// survives); Flush commits the dirty set in a single transaction.
type CacheStore struct {
	c *Cache
}

// Store returns the receiver so a CacheStore can be passed where a
// *CacheStore is expected without reaching back into the Cache.
func (s *CacheStore) Store() *CacheStore { return s }

func leaseKey(platform, account string) string { return platform + "\x00" + account }

func (s *CacheStore) UpsertNode(n pknode.NodeSnapshot) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	k := string(n.Hash[:])
	delete(c.delNodes, k)
	// Copy the snapshot's slices/maps so later caller mutations cannot
	// corrupt the staged value.
	cp := n
	cp.Tags = append([]pknode.ScopedTag(nil), n.Tags...)
	cp.RawOptions = append([]byte(nil), n.RawOptions...)
	if n.Attrs != nil {
		cp.Attrs = make(map[pknode.AttrKey]pknode.AttrValue, len(n.Attrs))
		for ak, av := range n.Attrs {
			cp.Attrs[ak] = av
		}
	}
	c.nodes[k] = cp
}

func (s *CacheStore) DeleteNode(h pknode.Hash) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	k := string(h[:])
	delete(c.nodes, k)
	delete(c.attrs, k)
	c.delNodes[k] = struct{}{}
}

func (s *CacheStore) SetSourceNodes(source string, hashes []pknode.Hash) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sources[source] = append([]pknode.Hash(nil), hashes...)
}

func (s *CacheStore) UpsertLatency(h pknode.Hash, domain string, ms float64, at time.Time, authority bool) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latency[latencyKey{hash: h, domain: domain}] = latencyVal{ms: ms, at: at, authority: authority}
}

func (s *CacheStore) UpsertAttrs(h pknode.Hash, attrs map[pknode.AttrKey]pknode.AttrValue) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	k := string(h[:])
	cp := make(map[pknode.AttrKey]pknode.AttrValue, len(attrs))
	for ak, av := range attrs {
		cp[ak] = av
	}
	c.attrs[k] = cp
}

func (s *CacheStore) UpsertLease(platform, account string, l pklease.Lease) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	k := leaseKey(platform, account)
	delete(c.delLeases, k)
	c.leases[k] = l
}

func (s *CacheStore) DeleteLease(platform, account string) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	k := leaseKey(platform, account)
	delete(c.leases, k)
	c.delLeases[k] = struct{}{}
}

// Flush implements ports.CacheStore.
func (s *CacheStore) Flush() error { return s.c.Flush() }

// Flush commits the staged dirty set in a single transaction. It is a
// no-op when nothing is dirty. On success the dirty set is cleared.
func (c *Cache) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.nodes) == 0 && len(c.delNodes) == 0 && len(c.sources) == 0 &&
		len(c.latency) == 0 && len(c.attrs) == 0 &&
		len(c.leases) == 0 && len(c.delLeases) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: cache flush begin: %w", err)
	}
	// Roll back on any failure below; a successful commit clears the flag.
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	for k := range c.delNodes {
		hb := []byte(k)
		if _, err := tx.ExecContext(ctx, `DELETE FROM nodes WHERE hash = ?`, hb); err != nil {
			return fmt.Errorf("sqlite: cache flush delete node: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM node_latency WHERE hash = ?`, hb); err != nil {
			return fmt.Errorf("sqlite: cache flush delete latency: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM source_nodes WHERE hash = ?`, hb); err != nil {
			return fmt.Errorf("sqlite: cache flush delete source_nodes: %w", err)
		}
	}

	for k, n := range c.nodes {
		attrs := n.Attrs
		if staged, ok := c.attrs[k]; ok {
			attrs = staged // a staged UpsertAttrs wins over the snapshot
		}
		tagsEnc, err := encodeTags(n.Tags)
		if err != nil {
			return fmt.Errorf("sqlite: cache flush encode tags: %w", err)
		}
		attrsEnc, err := encodeAttrs(attrs)
		if err != nil {
			return fmt.Errorf("sqlite: cache flush encode attrs: %w", err)
		}
		rawOptions := n.RawOptions
		if rawOptions == nil {
			rawOptions = []byte{}
		}
		hk, hf, hs := encodeHealth(n.Health)
		egressIP := ""
		if n.EgressIP.IsValid() {
			egressIP = n.EgressIP.String()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO nodes(hash, raw_options, tags, attrs, health_kind, health_failures, health_since, egress_ip, has_outbound)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(hash) DO UPDATE SET
				raw_options=excluded.raw_options,
				tags=excluded.tags,
				attrs=excluded.attrs,
				health_kind=excluded.health_kind,
				health_failures=excluded.health_failures,
				health_since=excluded.health_since,
				egress_ip=excluded.egress_ip,
				has_outbound=excluded.has_outbound`,
			[]byte(k), rawOptions, tagsEnc, attrsEnc, hk, hf, hs, egressIP, boolToInt(n.HasOutbound)); err != nil {
			return fmt.Errorf("sqlite: cache flush upsert node: %w", err)
		}
	}

	for source, hashes := range c.sources {
		if _, err := tx.ExecContext(ctx, `DELETE FROM source_nodes WHERE source = ?`, source); err != nil {
			return fmt.Errorf("sqlite: cache flush clear source_nodes: %w", err)
		}
		for _, h := range hashes {
			hb := h[:]
			if _, err := tx.ExecContext(ctx,
				`INSERT OR IGNORE INTO source_nodes(source, hash) VALUES(?, ?)`, source, hb); err != nil {
				return fmt.Errorf("sqlite: cache flush insert source_nodes: %w", err)
			}
		}
	}

	for lk, lv := range c.latency {
		auth := 0
		if lv.authority {
			auth = 1
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO node_latency(hash, domain, ms, at, authority)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(hash, domain) DO UPDATE SET
				ms=excluded.ms, at=excluded.at, authority=excluded.authority`,
			lk.hash[:], lk.domain, lv.ms, lv.at.UTC().UnixNano(), auth); err != nil {
			return fmt.Errorf("sqlite: cache flush upsert latency: %w", err)
		}
	}

	for k, attrs := range c.attrs {
		if _, ok := c.nodes[k]; ok {
			continue // already folded into the node upsert above
		}
		// Standalone attr writes only make sense for a persisted node;
		// silently drop orphans (reconcile drops them anyway, §9.2).
		enc, err := encodeAttrs(attrs)
		if err != nil {
			return fmt.Errorf("sqlite: cache flush encode attrs: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE nodes SET attrs = ? WHERE hash = ?`, enc, []byte(k)); err != nil {
			return fmt.Errorf("sqlite: cache flush update attrs: %w", err)
		}
	}

	for k := range c.delLeases {
		platform, account := splitLeaseKey(k)
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM leases WHERE platform = ? AND account = ?`, platform, account); err != nil {
			return fmt.Errorf("sqlite: cache flush delete lease: %w", err)
		}
	}

	for k, l := range c.leases {
		platform, account := splitLeaseKey(k)
		egressIP := ""
		if l.EgressIP.IsValid() {
			egressIP = l.EgressIP.String()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO leases(platform, account, egress_ip, node_hint, expiry, last_accessed)
			VALUES(?, ?, ?, ?, ?, ?)
			ON CONFLICT(platform, account) DO UPDATE SET
				egress_ip=excluded.egress_ip,
				node_hint=excluded.node_hint,
				expiry=excluded.expiry,
				last_accessed=excluded.last_accessed`,
			platform, account, egressIP, l.NodeHint[:],
			l.Expiry.UTC().UnixNano(), l.LastAccessed.UTC().UnixNano()); err != nil {
			return fmt.Errorf("sqlite: cache flush upsert lease: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: cache flush commit: %w", err)
	}
	committed = true

	// Clear the dirty set only after a successful commit.
	for k := range c.nodes {
		delete(c.nodes, k)
	}
	for k := range c.delNodes {
		delete(c.delNodes, k)
	}
	for k := range c.sources {
		delete(c.sources, k)
	}
	for k := range c.latency {
		delete(c.latency, k)
	}
	for k := range c.attrs {
		delete(c.attrs, k)
	}
	for k := range c.leases {
		delete(c.leases, k)
	}
	for k := range c.delLeases {
		delete(c.delLeases, k)
	}
	return nil
}

func splitLeaseKey(k string) (platform, account string) {
	for i := 0; i < len(k); i++ {
		if k[i] == 0 {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}
