package wiring

import (
	"container/list"
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/xxh3"

	pick "github.com/xiarsx/egress/internal/core/select"
	pknode "github.com/xiarsx/egress/pkg/node"
	pksignal "github.com/xiarsx/egress/pkg/signal"
)

// Latency tuning (design §6.2).
const (
	// DefaultDecayWindow is the TD-EWMA decay window: a sample's weight is
	// e^(-Δt/DecayWindow).
	DefaultDecayWindow = 10 * time.Minute
	// DefaultNormalBuckets is the default capacity of the per-node normal
	// (non-authority) LRU zone; the hard cap is MaxNormalBuckets.
	DefaultNormalBuckets = 12
	MaxNormalBuckets     = 32
)

// Compile-time check: Tracker is the select package's LatencySource.
var _ pick.LatencySource = (*Tracker)(nil)

// bucketKey is the xxh3-128 of the bucket domain (design §6.2, ADR-003:
// a vanishing collision probability is accepted).
type bucketKey [16]byte

// latencyBucket holds one TD-EWMA cell.
type latencyBucket struct {
	ewma time.Duration
	at   time.Time // last sample time
}

// nodeLatency is one node's two-zone latency table (design §6.2):
// authority domains are pinned, everything else lives in an
// approximately-LRU zone.
type nodeLatency struct {
	authority map[bucketKey]*latencyBucket
	normal    *bucketLRU
}

// bucketLRU is a tiny capacity-bounded LRU over latency buckets.
type bucketLRU struct {
	cap  int
	ll   *list.List // front = most recently used
	elem map[bucketKey]*list.Element
}

type lruEntry struct {
	key    bucketKey
	bucket *latencyBucket
}

func newBucketLRU(cap int) *bucketLRU {
	if cap <= 0 {
		cap = DefaultNormalBuckets
	}
	if cap > MaxNormalBuckets {
		cap = MaxNormalBuckets
	}
	return &bucketLRU{
		cap:  cap,
		ll:   list.New(),
		elem: make(map[bucketKey]*list.Element),
	}
}

func (l *bucketLRU) get(k bucketKey) *latencyBucket {
	if e, ok := l.elem[k]; ok {
		l.ll.MoveToFront(e)
		return e.Value.(*lruEntry).bucket
	}
	return nil
}

func (l *bucketLRU) put(k bucketKey, b *latencyBucket) {
	if e, ok := l.elem[k]; ok {
		l.ll.MoveToFront(e)
		e.Value.(*lruEntry).bucket = b
		return
	}
	e := l.ll.PushFront(&lruEntry{key: k, bucket: b})
	l.elem[k] = e
	for l.ll.Len() > l.cap {
		back := l.ll.Back()
		if back == nil {
			break
		}
		l.ll.Remove(back)
		delete(l.elem, back.Value.(*lruEntry).key)
	}
}

// Tracker is the live TD-EWMA latency table (design §6.2). Domains are
// bucketed by eTLD+1; authority domains (e.g. the egress probe target)
// are pinned per node, normal domains share an LRU zone.
//
// Concurrency: safe for concurrent use.
type Tracker struct {
	mu sync.RWMutex
	// nodes maps node hash → two-zone table. Entries are created lazily on
	// first Update and never deleted (bounded by the node population; a
	// restart rebuilds them from cache.db's node_latency via CacheStore).
	nodes map[pknode.Hash]*nodeLatency

	decay     time.Duration
	normalCap int
	now       func() time.Time
}

// TrackerOption tunes a Tracker.
type TrackerOption func(*Tracker)

// WithDecayWindow overrides the TD-EWMA decay window.
func WithDecayWindow(d time.Duration) TrackerOption {
	return func(t *Tracker) {
		if d > 0 {
			t.decay = d
		}
	}
}

// WithNormalBuckets overrides the per-node normal-zone LRU capacity
// (clamped to [1, MaxNormalBuckets]).
func WithNormalBuckets(n int) TrackerOption {
	return func(t *Tracker) {
		if n > 0 {
			t.normalCap = n
		}
	}
}

// NewTracker builds a latency Tracker.
func NewTracker(opts ...TrackerOption) *Tracker {
	t := &Tracker{
		nodes:     make(map[pknode.Hash]*nodeLatency),
		decay:     DefaultDecayWindow,
		normalCap: DefaultNormalBuckets,
		now:       time.Now,
	}
	for _, o := range opts {
		o(t)
	}
	if t.normalCap > MaxNormalBuckets {
		t.normalCap = MaxNormalBuckets
	}
	return t
}

// etld1 returns the bucket domain for d: the last two DNS labels,
// lower-cased, without a trailing dot.
//
// TODO: replace with golang.org/x/net/publicsuffix.EffectiveTLDPlusOne
// once x/net is a (direct) dependency; the two-label fallback mis-buckets
// multi-level public suffixes (e.g. example.co.uk → co.uk).
func etld1(d string) string {
	d = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(d), "."))
	if d == "" {
		return ""
	}
	labels := strings.Split(d, ".")
	if len(labels) >= 2 {
		return strings.Join(labels[len(labels)-2:], ".")
	}
	return d
}

// bucketKeyOf hashes the eTLD+1 bucket of domain.
func bucketKeyOf(domain string) bucketKey {
	return bucketKey(xxh3.Hash128([]byte(etld1(domain))).Bytes())
}

// Update records one latency sample for (node, domain). ms<=0 is ignored
// (a failed probe carries no latency information); at is the sample time
// (normally the signal's timestamp).
func (t *Tracker) Update(h pknode.Hash, domain string, sample time.Duration, at time.Time, authority bool) {
	if sample <= 0 || domain == "" {
		return
	}
	if at.IsZero() {
		at = t.now()
	}
	k := bucketKeyOf(domain)

	t.mu.Lock()
	defer t.mu.Unlock()
	nl, ok := t.nodes[h]
	if !ok {
		nl = &nodeLatency{
			authority: make(map[bucketKey]*latencyBucket),
			normal:    newBucketLRU(t.normalCap),
		}
		t.nodes[h] = nl
	}
	var b *latencyBucket
	if authority {
		b = nl.authority[k]
		if b == nil {
			b = &latencyBucket{}
			nl.authority[k] = b
		}
	} else {
		b = nl.normal.get(k)
		if b == nil {
			b = &latencyBucket{}
			nl.normal.put(k, b)
		}
	}
	// TD-EWMA: weight = e^(-Δt/DecayWindow); ewma' = ewma*w + sample*(1-w).
	// A bucket with no prior sample (at zero) takes the sample as-is.
	if b.at.IsZero() || !at.After(b.at) {
		if b.at.IsZero() {
			b.ewma = sample
		} else {
			// Out-of-order sample: fold it in with full weight rather
			// than corrupting the decay math.
			b.ewma = (b.ewma + sample) / 2
		}
		b.at = at
		return
	}
	dt := at.Sub(b.at)
	w := math.Exp(-float64(dt) / float64(t.decay))
	b.ewma = time.Duration(float64(b.ewma)*w + float64(sample)*(1-w))
	b.at = at
}

// Latency implements pick.LatencySource: it reports the EWMA for
// (node, domain)'s eTLD+1 bucket, but only when the last sample is
// recent (within one decay window of now). Stale buckets are treated as
// unknown rather than reported with a decayed value, so the selector
// never routes on ancient measurements.
func (t *Tracker) Latency(h pknode.Hash, domain string) (time.Duration, bool) {
	k := bucketKeyOf(domain)
	now := t.now()

	t.mu.RLock()
	defer t.mu.RUnlock()
	nl, ok := t.nodes[h]
	if !ok {
		return 0, false
	}
	var b *latencyBucket
	if ab, ok := nl.authority[k]; ok {
		b = ab
	} else {
		b = nl.normal.get(k)
	}
	if b == nil || b.at.IsZero() {
		return 0, false
	}
	if now.Sub(b.at) > t.decay {
		return 0, false
	}
	return b.ewma, true
}

// Run consumes Latency signals from the bus until ctx is done or sub is
// closed. Failed probes (no sample) are skipped; successful ones update
// the table. It never blocks the bus: sub is a dedicated subscription.
func (t *Tracker) Run(ctx context.Context, sub <-chan pksignal.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig, ok := <-sub:
			if !ok {
				return
			}
			if sig.Kind != pksignal.KindLatency || !sig.HasLatency {
				continue
			}
			t.Update(sig.NodeHash, sig.Domain, sig.Latency, sig.At, sig.Authority)
		}
	}
}
