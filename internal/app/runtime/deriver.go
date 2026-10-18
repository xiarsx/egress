package runtime

import (
	"sync"

	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// Deriver owns every platform's routable View (design §5). It is the single
// writer of all Views: SetPlatform / RemovePlatform / Apply / Remove must be
// called from one logical writer — the runtime goroutine and the
// RuntimeHooks callbacks that funnel through it. A mutex guards the map so a
// future tightening of the contract stays safe; concurrent writers are still
// not supported.
//
// The membership predicate is composed at SetPlatform time: callers pass the
// full Routable derivation (health, egress IP, latency, tag/attr filters —
// typically node.Routable over a compiled predicate.Filter). view.View.Apply
// then consults that predicate internally: Apply is incremental
// add/remove driven by the snapshot, and Derive does the full scan.
type Deriver struct {
	mu    sync.Mutex
	reg   ports.NodeRegistry
	views map[string]*view.View
}

// NewDeriver creates a Deriver that full-scans reg on SetPlatform.
func NewDeriver(reg ports.NodeRegistry) *Deriver {
	return &Deriver{reg: reg, views: make(map[string]*view.View)}
}

// SetPlatform (re)builds the platform's View from a full registry scan with
// the given predicate.
func (d *Deriver) SetPlatform(id string, pred pknode.Predicate) {
	v := view.Derive(d.reg.All(), pred)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.views[id] = v
}

// RemovePlatform drops the platform's View.
func (d *Deriver) RemovePlatform(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.views, id)
}

// Apply incrementally re-evaluates one node snapshot against every View
// (added/kept/removed per each View's own predicate).
func (d *Deriver) Apply(n pknode.NodeSnapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, v := range d.views {
		v.Apply(n)
	}
}

// Remove deletes a node hash from every View.
func (d *Deriver) Remove(h pknode.Hash) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, v := range d.views {
		v.Remove(h)
	}
}

// View returns the platform's current View, or nil if the platform has no
// derived view. The returned View is safe for concurrent hot-path reads.
func (d *Deriver) View(id string) *view.View {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.views[id]
}

// PlatformIDs lists the platforms that currently have a derived View.
func (d *Deriver) PlatformIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := make([]string, 0, len(d.views))
	for id := range d.views {
		ids = append(ids, id)
	}
	return ids
}
