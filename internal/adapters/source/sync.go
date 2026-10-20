package source

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/pkg/ext"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// minRemoteInterval is the minimum refresh interval for remote sources
// (design §5.1: remote sources refresh at most every 30s).
const minRemoteInterval = 30 * time.Second

// fetchTimeout bounds one source refresh (download + parse) when the
// caller did not supply its own deadline.
const fetchTimeout = 30 * time.Second

// ErrFetchTooFrequent is returned by the remote provider when a source is
// refreshed before minRemoteInterval has elapsed since its last success.
var ErrFetchTooFrequent = errors.New("source: remote refresh below 30s minimum interval")

// Provider fetches one source's raw subscription bytes. Built-ins:
// "remote" (URL download via ports.Fetcher) and "local" (inline content).
type Provider interface {
	Fetch(ctx context.Context, spec ports.SourceSpec) ([]byte, error)
}

// remoteProvider downloads the subscription URL with ports.Fetcher and
// clamps successful refreshes to minRemoteInterval apart.
type remoteProvider struct {
	fetcher ports.Fetcher

	mu   sync.Mutex
	last map[string]time.Time // last successful fetch per source name
}

func (p *remoteProvider) Fetch(ctx context.Context, spec ports.SourceSpec) ([]byte, error) {
	p.mu.Lock()
	if last, ok := p.last[spec.Name]; ok && time.Since(last) < minRemoteInterval {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: source %q", ErrFetchTooFrequent, spec.Name)
	}
	p.mu.Unlock()

	body, err := p.fetcher.Fetch(ctx, spec.URL)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.last[spec.Name] = time.Now()
	p.mu.Unlock()
	return body, nil
}

// localProvider returns the inline subscription content.
type localProvider struct{}

func (localProvider) Fetch(_ context.Context, spec ports.SourceSpec) ([]byte, error) {
	return []byte(spec.Content), nil
}

// Syncer keeps sources refreshed: it fetches, parses and publishes
// snapshots to the Registry. It owns scheduling only — parsing is the
// injected parse callback supplied by the assembler, and tag scoping is
// applied (idempotently) before publishing.
//
// A failed download or parse keeps the previous snapshot and records the
// error, retrievable via LastError (design §5.1).
//
// It is safe for concurrent use.
type Syncer struct {
	reg     *Registry
	sched   ports.Scheduler
	parse   func([]byte) ([]pknode.NodeSnapshot, error)
	remotes *remoteProvider

	mu          sync.Mutex
	specs       map[string]ports.SourceSpec
	generation  map[string]uint64 // invalidates chained schedules on change/remove
	lastErr     map[string]string
	lastErrTime map[string]time.Time
}

// NewSyncer creates a Syncer. fetcher backs the built-in remote provider;
// sched drives periodic refreshes; parse converts raw subscription bytes
// to node snapshots (raw tags; the Syncer scopes them per source).
func NewSyncer(reg *Registry, fetcher ports.Fetcher, sched ports.Scheduler, parse func([]byte) ([]pknode.NodeSnapshot, error)) *Syncer {
	return &Syncer{
		reg:         reg,
		sched:       sched,
		parse:       parse,
		remotes:     &remoteProvider{fetcher: fetcher, last: make(map[string]time.Time)},
		specs:       make(map[string]ports.SourceSpec),
		generation:  make(map[string]uint64),
		lastErr:     make(map[string]string),
		lastErrTime: make(map[string]time.Time),
	}
}

// nodeProvider yields parsed node snapshots (built-ins fetch bytes and
// run them through the Syncer's parser; ext providers return snapshots
// directly — spec/ext-abi.md §2).
type nodeProvider interface {
	FetchNodes(ctx context.Context, spec ports.SourceSpec) ([]pknode.NodeSnapshot, error)
}

// providerFor resolves the provider for a source kind: built-ins first,
// then registered ext.SourceProviders. An unknown kind fails closed with
// an error surfaced in the source's LastError.
func (s *Syncer) providerFor(kind string) (nodeProvider, error) {
	switch kind {
	case "remote":
		return &byteProvider{prov: s.remotes, parse: s.parse}, nil
	case "local":
		return &byteProvider{prov: localProvider{}, parse: s.parse}, nil
	default:
		if p, ok := ext.GetSourceProvider(kind); ok {
			return &extProvider{p: p, kind: kind}, nil
		}
		return nil, fmt.Errorf("source: unknown source kind %q", kind)
	}
}

// byteProvider adapts a byte-fetching Provider + parser to nodeProvider.
type byteProvider struct {
	prov  Provider
	parse func([]byte) ([]pknode.NodeSnapshot, error)
}

func (b *byteProvider) FetchNodes(ctx context.Context, spec ports.SourceSpec) ([]pknode.NodeSnapshot, error) {
	body, err := b.prov.Fetch(ctx, spec)
	if err != nil {
		return nil, err
	}
	return b.parse(body)
}

// extProvider adapts a registered ext.SourceProvider.
type extProvider struct {
	p    ext.SourceProvider
	kind string
}

func (e *extProvider) FetchNodes(ctx context.Context, spec ports.SourceSpec) ([]pknode.NodeSnapshot, error) {
	return e.p.Fetch(ctx, ext.SourceSpec{
		Name:           spec.Name,
		Kind:           e.kind,
		URL:            spec.URL,
		Content:        spec.Content,
		UpdateInterval: spec.UpdateInterval,
	})
}

// AddSource registers a source, refreshes it once immediately (async), and
// schedules periodic refreshes at UpdateInterval. Remote intervals below
// 30s are clamped to minRemoteInterval. Re-adding a source replaces it and
// restarts its schedule.
func (s *Syncer) AddSource(spec ports.SourceSpec) {
	interval := spec.UpdateInterval
	if spec.Kind == "remote" && interval < minRemoteInterval {
		interval = minRemoteInterval
	}

	s.mu.Lock()
	s.specs[spec.Name] = spec
	s.generation[spec.Name]++
	gen := s.generation[spec.Name]
	delete(s.lastErr, spec.Name)
	delete(s.lastErrTime, spec.Name)
	s.mu.Unlock()

	go s.refreshOnce(spec.Name)
	s.scheduleNext(spec.Name, gen, interval)
}

// RemoveSource unregisters a source, stops its schedule, and withdraws its
// snapshot (reference counts drop; unreferenced nodes are deleted).
func (s *Syncer) RemoveSource(name string) {
	s.mu.Lock()
	_, ok := s.specs[name]
	delete(s.specs, name)
	s.generation[name]++
	delete(s.lastErr, name)
	delete(s.lastErrTime, name)
	s.mu.Unlock()
	if ok {
		s.reg.ApplySnapshot(name, nil)
	}
}

// RefreshNow refreshes a source immediately, outside its schedule.
// A failure keeps the old snapshot and is recorded for LastError.
func (s *Syncer) RefreshNow(name string) error {
	return s.refreshOnce(name)
}

// LastError returns the last refresh error message for name, or "" when
// the last refresh succeeded (or the source is unknown).
func (s *Syncer) LastError(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr[name]
}

// scheduleNext chains one periodic refresh: the task re-arms itself only
// while the source is still registered under the same generation, so
// RemoveSource/AddSource cleanly retire stale chains without explicit
// cancellation handles.
func (s *Syncer) scheduleNext(name string, gen uint64, delay time.Duration) {
	if delay <= 0 {
		return
	}
	s.sched.Schedule(delay, func(ctx context.Context) {
		if ctx.Err() != nil {
			return
		}
		if !s.current(name, gen) {
			return
		}
		_ = s.refreshOnceWithCtx(ctx, name)
		if s.current(name, gen) {
			s.scheduleNext(name, gen, s.intervalOf(name))
		}
	})
}

func (s *Syncer) current(name string, gen uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.specs[name]
	return ok && s.generation[name] == gen
}

func (s *Syncer) intervalOf(name string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.specs[name]
	if !ok {
		return 0
	}
	if spec.Kind == "remote" && spec.UpdateInterval < minRemoteInterval {
		return minRemoteInterval
	}
	return spec.UpdateInterval
}

func (s *Syncer) refreshOnce(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()
	return s.refreshOnceWithCtx(ctx, name)
}

// refreshOnceWithCtx performs one fetch → parse → publish cycle.
func (s *Syncer) refreshOnceWithCtx(ctx context.Context, name string) error {
	s.mu.Lock()
	spec, ok := s.specs[name]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("source: unknown source %q", name)
	}
	prov, err := s.providerFor(spec.Kind)
	if err != nil {
		s.recordError(name, err)
		return err
	}
	nodes, err := prov.FetchNodes(ctx, spec)
	if err != nil {
		// Keep the old snapshot; record the failure.
		s.recordError(name, err)
		return err
	}
	for i := range nodes {
		if nodes[i].Hash == pknode.Zero && len(nodes[i].RawOptions) > 0 {
			nodes[i].Hash = HashOutbound(nodes[i].RawOptions)
		}
		nodes[i].Tags = scopeTags(spec.Name, nodes[i].Tags)
	}
	s.reg.ApplySnapshot(spec.Name, nodes)
	s.clearError(name)
	return nil
}

func (s *Syncer) recordError(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErr[name] = err.Error()
	s.lastErrTime[name] = time.Now()
}

func (s *Syncer) clearError(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.lastErr, name)
	delete(s.lastErrTime, name)
}
