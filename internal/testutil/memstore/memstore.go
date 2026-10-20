// Package memstore is the shared in-memory ports.Store for unit tests
// (test-only; the production store is the sqlite adapter). It enforces
// the same contract the sqlite store does: optimistic versioning,
// ErrNotFound / ErrAlreadyExists / ErrConflict / ErrInvalid semantics
// and RFC 7386 merge-patch.
package memstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/xiarsx/egress/internal/ports"
)

// Store is an in-memory ports.Store. It is safe for concurrent use.
type Store struct {
	mu      sync.Mutex
	items   map[ports.Kind]map[string]ports.Resource
	version int64
}

// New creates an empty Store.
func New() *Store {
	return &Store{items: map[ports.Kind]map[string]ports.Resource{}}
}

var _ ports.Store = (*Store)(nil)

func (m *Store) Get(_ context.Context, kind ports.Kind, name string) (ports.Resource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.items[kind][name]
	if !ok {
		return ports.Resource{}, fmt.Errorf("%w: %s/%s", ports.ErrNotFound, kind, name)
	}
	return r, nil
}

func (m *Store) List(_ context.Context, kind ports.Kind, opts ports.ListOptions) (ports.Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []ports.Resource
	for _, r := range m.items[kind] {
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	total := len(all)
	if opts.Offset > total {
		opts.Offset = total
	}
	end := total
	if opts.Limit >= 0 && opts.Offset+opts.Limit < end {
		end = opts.Offset + opts.Limit
	}
	return ports.Page{Items: all[opts.Offset:end], Total: total, Limit: opts.Limit, Offset: opts.Offset}, nil
}

func (m *Store) Create(_ context.Context, r ports.Resource) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.Kind == "" || r.Name == "" || len(r.Data) == 0 {
		return fmt.Errorf("%w: empty kind/name/data", ports.ErrInvalid)
	}
	if m.items[r.Kind] == nil {
		m.items[r.Kind] = map[string]ports.Resource{}
	}
	if _, ok := m.items[r.Kind][r.Name]; ok {
		return fmt.Errorf("%w: %s/%s", ports.ErrAlreadyExists, r.Kind, r.Name)
	}
	m.version++
	r.Version = m.version
	m.items[r.Kind][r.Name] = r
	return nil
}

func (m *Store) Update(_ context.Context, r ports.Resource) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.items[r.Kind][r.Name]
	if !ok {
		return fmt.Errorf("%w: %s/%s", ports.ErrNotFound, r.Kind, r.Name)
	}
	if r.Version != cur.Version {
		return fmt.Errorf("%w: %s/%s", ports.ErrConflict, r.Kind, r.Name)
	}
	m.version++
	r.Version = m.version
	m.items[r.Kind][r.Name] = r
	return nil
}

func (m *Store) Delete(_ context.Context, kind ports.Kind, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[kind][name]; !ok {
		return fmt.Errorf("%w: %s/%s", ports.ErrNotFound, kind, name)
	}
	delete(m.items[kind], name)
	return nil
}

// Patch applies an RFC 7386 JSON merge-patch to the resource Data.
func (m *Store) Patch(_ context.Context, kind ports.Kind, name string, patch []byte) (ports.Resource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.items[kind][name]
	if !ok {
		return ports.Resource{}, fmt.Errorf("%w: %s/%s", ports.ErrNotFound, kind, name)
	}
	var doc map[string]any
	if err := json.Unmarshal(cur.Data, &doc); err != nil {
		return ports.Resource{}, err
	}
	var p map[string]any
	if err := json.Unmarshal(patch, &p); err != nil {
		return ports.Resource{}, fmt.Errorf("%w: bad merge-patch: %v", ports.ErrInvalid, err)
	}
	merged := mergePatch(doc, p)
	data, err := json.Marshal(merged)
	if err != nil {
		return ports.Resource{}, err
	}
	m.version++
	cur.Version = m.version
	cur.Data = data
	m.items[kind][name] = cur
	return cur, nil
}

func mergePatch(doc, patch map[string]any) map[string]any {
	out := make(map[string]any, len(doc))
	for k, v := range doc {
		out[k] = v
	}
	for k, v := range patch {
		if v == nil {
			delete(out, k)
			continue
		}
		if pm, ok := v.(map[string]any); ok {
			if dm, ok := out[k].(map[string]any); ok {
				out[k] = mergePatch(dm, pm)
				continue
			}
		}
		out[k] = v
	}
	return out
}
