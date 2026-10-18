package control

// In-memory ports.Store for unit tests (test-only; the real store is the
// sqlite adapter owned by the P1 group).

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/xiarsx/egress/internal/ports"
)

type memStore struct {
	mu      sync.Mutex
	items   map[ports.Kind]map[string]ports.Resource
	version int64
}

func newMemStore() *memStore {
	return &memStore{items: map[ports.Kind]map[string]ports.Resource{}}
}

func (m *memStore) Get(_ context.Context, kind ports.Kind, name string) (ports.Resource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.items[kind][name]
	if !ok {
		return ports.Resource{}, fmt.Errorf("%w: %s/%s", ports.ErrNotFound, kind, name)
	}
	return r, nil
}

func (m *memStore) List(_ context.Context, kind ports.Kind, opts ports.ListOptions) (ports.Page, error) {
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

func (m *memStore) Create(_ context.Context, r ports.Resource) error {
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

func (m *memStore) Update(_ context.Context, r ports.Resource) error {
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

func (m *memStore) Delete(_ context.Context, kind ports.Kind, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[kind][name]; !ok {
		return fmt.Errorf("%w: %s/%s", ports.ErrNotFound, kind, name)
	}
	delete(m.items[kind], name)
	return nil
}

// Patch applies an RFC 7386 JSON merge-patch to the resource Data.
func (m *memStore) Patch(_ context.Context, kind ports.Kind, name string, patch []byte) (ports.Resource, error) {
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

// recordHooks records hook calls for assertions.
type recordHooks struct {
	mu              sync.Mutex
	platformChanged []string
	platformDeleted []string
	sourceChanged   []string
	sourceDeleted   []string
}

func (h *recordHooks) OnPlatformChanged(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.platformChanged = append(h.platformChanged, id)
}
func (h *recordHooks) OnPlatformDeleted(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.platformDeleted = append(h.platformDeleted, id)
}
func (h *recordHooks) OnSourceChanged(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sourceChanged = append(h.sourceChanged, name)
}
func (h *recordHooks) OnSourceDeleted(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sourceDeleted = append(h.sourceDeleted, name)
}
