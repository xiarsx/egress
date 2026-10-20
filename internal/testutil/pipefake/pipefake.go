// Package pipefake provides the shared test doubles for pipeline.Deps:
// clock, resolver, bus, node dialer and direct dialer. Every knob is an
// exported field; the zero value is the strictest useful default
// (nothing resolves, every dial fails). Adapters, pipeline, endpoint and
// conformance tests share these instead of re-declaring per-package fakes.
package pipefake

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/core/identity"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Clock is a fixed-time ports.Clock.
type Clock struct{ T time.Time }

func (c *Clock) Now() time.Time { return c.T }

// Resolver maps platform wire names to IDs and serves identity policies.
//
// Resolution order: Platforms (exact) → ID when name is "Default".
// PolicyFor returns Policies[id] when the map is set, else Policy (which
// may be nil → callers apply the default policy).
type Resolver struct {
	Platforms map[string]string
	ID        string
	Policy    *identity.Policy
	Policies  map[string]*identity.Policy
}

func (r *Resolver) ResolvePlatform(name string) (string, bool) {
	if r.Platforms != nil {
		id, ok := r.Platforms[name]
		return id, ok
	}
	if r.ID != "" && name == "Default" {
		return r.ID, true
	}
	return "", false
}

func (r *Resolver) PolicyFor(id string) *identity.Policy {
	if r.Policies != nil {
		return r.Policies[id]
	}
	return r.Policy
}

// Bus collects emitted signals; Subscribe returns nil (tests pull Got).
type Bus struct {
	Mu  sync.Mutex
	Got []pksignal.Signal
}

func (b *Bus) Emit(s pksignal.Signal) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.Got = append(b.Got, s)
}

func (b *Bus) Subscribe() <-chan pksignal.Signal { return nil }

// Signals returns a locked copy of everything emitted so far.
func (b *Bus) Signals() []pksignal.Signal {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return append([]pksignal.Signal(nil), b.Got...)
}

// Count reports how many signals were emitted.
func (b *Bus) Count() int {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return len(b.Got)
}

// Last returns the most recent signal.
func (b *Bus) Last() (pksignal.Signal, bool) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	if len(b.Got) == 0 {
		return pksignal.Signal{}, false
	}
	return b.Got[len(b.Got)-1], true
}

// Dialer is a ports.NodeDialer: Err fails the dial; otherwise it returns
// one end of a net.Pipe whose peer runs Serve (discard when nil). Dialed
// addresses accumulate in Seen.
type Dialer struct {
	Mu     sync.Mutex
	Err    error
	Seen   []string
	Hashes []pknode.Hash
	Serve  func(up net.Conn)
}

func (d *Dialer) DialContext(_ context.Context, h pknode.Hash, _, address string) (net.Conn, error) {
	d.Mu.Lock()
	d.Seen = append(d.Seen, address)
	d.Hashes = append(d.Hashes, h)
	err := d.Err
	serve := d.Serve
	d.Mu.Unlock()
	if err != nil {
		return nil, err
	}
	up, peer := net.Pipe()
	go func() {
		if serve != nil {
			serve(peer)
			return
		}
		// Discard and close so the relay's upstream→client copy sees EOF.
		_, _ = io.Copy(io.Discard, peer)
		_ = peer.Close()
	}()
	return up, nil
}

// DialedHashes returns the node hashes dialed so far, in order.
func (d *Dialer) DialedHashes() []pknode.Hash {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	return append([]pknode.Hash(nil), d.Hashes...)
}

// Direct is a ports.Dialer that fails by default (most tests route
// through nodes). Allow makes it succeed with a discard-and-close pipe;
// Serve takes over the peer entirely.
type Direct struct {
	Mu      sync.Mutex
	Err     error
	Allow   bool
	Targets []string
	Serve   func(up net.Conn)
}

func (d *Direct) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	d.Mu.Lock()
	d.Targets = append(d.Targets, address)
	err := d.Err
	serve := d.Serve
	allow := d.Allow
	d.Mu.Unlock()
	if err != nil {
		return nil, err
	}
	if serve == nil && !allow {
		return nil, errors.New("pipefake: direct dial disabled")
	}
	up, peer := net.Pipe()
	go func() {
		if serve != nil {
			serve(peer)
			return
		}
		_, _ = io.Copy(io.Discard, peer)
		_ = peer.Close()
	}()
	return up, nil
}
