package singbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// ErrUnsupportedOutbound is returned when a node's outbound type has no
// constructor in the embedded sing-box engine (unknown type, or a QUIC
// protocol in a binary built without `-tags with_quic`). It always fails
// closed — never a silent direct dial (design §5.1).
var ErrUnsupportedOutbound = errors.New("singbox: unsupported outbound type")

var (
	errNodeNotFound = errors.New("singbox: node not found")
	errNoOutbound   = errors.New("singbox: node has no built outbound")
)

// dialOutbound is a built, dialable outbound. Close releases stateful
// resources (multiplex sessions, transports) so Remove can drop them.
type dialOutbound interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
	Close() error
}

// buildDialOutbound builds a dialable outbound from canonical
// sing-box-schema raw options via the embedded engine (engine.go).
// Deterministic: the same input always yields the same outcome, so
// callers may cache both successes and errors.
func buildDialOutbound(raw []byte) (dialOutbound, error) {
	e, err := sharedEngine()
	if err != nil {
		return nil, err
	}
	return e.build(raw)
}

// ---------------------------------------------------------------------------
// Dialer — ports.NodeDialer
// ---------------------------------------------------------------------------

// Dialer builds outbounds lazily from node raw options, caches them, and
// collapses concurrent first-use builds (single-flight). It mirrors the
// reference outbound manager's contract (internal/ports/dialer.go):
//
//   - a node without a built outbound is a dial-time error, never a
//     silent direct dial (fail-closed);
//   - Remove closes and drops the cached outbound.
//
// It is safe for concurrent use.
type Dialer struct {
	lookup func(pknode.Hash) (pknode.NodeSnapshot, bool)

	mu       sync.Mutex
	cached   map[pknode.Hash]*dialEntry
	inflight map[pknode.Hash]*dialCall
}

type dialEntry struct {
	ob  dialOutbound
	err error
}

type dialCall struct {
	done chan struct{}
	ob   dialOutbound
	err  error
}

var _ ports.NodeDialer = (*Dialer)(nil)

// NewDialer creates a Dialer. lookup resolves the current node snapshot by
// hash (typically registry.Get); it is consulted on every dial so removals
// and snapshot updates take effect without cache invalidation.
func NewDialer(lookup func(h pknode.Hash) (pknode.NodeSnapshot, bool)) *Dialer {
	return &Dialer{
		lookup:   lookup,
		cached:   make(map[pknode.Hash]*dialEntry),
		inflight: make(map[pknode.Hash]*dialCall),
	}
}

func (d *Dialer) DialContext(ctx context.Context, node pknode.Hash, network, address string) (net.Conn, error) {
	snap, ok := d.lookup(node)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errNodeNotFound, node.String())
	}
	if !snap.HasOutbound || len(snap.RawOptions) == 0 {
		// Fail closed: never silently dial direct for a node that has no
		// built outbound.
		return nil, fmt.Errorf("%w: %s", errNoOutbound, node.String())
	}
	ob, err := d.getOrBuild(node, snap.RawOptions)
	if err != nil {
		return nil, err
	}
	return ob.DialContext(ctx, network, address)
}

// Remove drops the cached outbound for h and closes it. In-flight builds
// for h complete harmlessly: the next dial re-checks lookup first, so a
// removed node can never be dialed through a stale entry.
func (d *Dialer) Remove(h pknode.Hash) {
	d.mu.Lock()
	e, ok := d.cached[h]
	delete(d.cached, h)
	d.mu.Unlock()
	if ok && e != nil && e.ob != nil {
		_ = e.ob.Close()
	}
}

// getOrBuild returns the cached outbound, building it once when missing.
// Concurrent builders for the same hash collapse onto a single build.
func (d *Dialer) getOrBuild(h pknode.Hash, raw []byte) (dialOutbound, error) {
	d.mu.Lock()
	if e, ok := d.cached[h]; ok {
		d.mu.Unlock()
		return e.ob, e.err
	}
	if c, ok := d.inflight[h]; ok {
		d.mu.Unlock()
		<-c.done
		return c.ob, c.err
	}
	c := &dialCall{done: make(chan struct{})}
	d.inflight[h] = c
	d.mu.Unlock()

	// Build outside the lock. The build is deterministic (pure JSON parse
	// + type switch), so caching the outcome — including errors — is safe:
	// the same hash always implies the same raw options.
	c.ob, c.err = buildDialOutbound(raw)

	d.mu.Lock()
	delete(d.inflight, h)
	d.cached[h] = &dialEntry{ob: c.ob, err: c.err}
	d.mu.Unlock()
	close(c.done)
	return c.ob, c.err
}
