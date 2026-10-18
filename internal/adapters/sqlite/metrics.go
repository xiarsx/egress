package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// metricsMigrations is the version chain for metrics.db (design §9.1:
// weak consistency; hourly-bucketed aggregates for the dashboard history
// views — traffic, requests, access latency, probes, node pool, lease
// lifetime — see design §10).
//
// Series naming convention: "<domain>.<metric>", e.g.
// "traffic.bytes_up", "requests.ok", "probes.count". Readers compute
// averages as sum/cnt per bucket. Finer rollups (per-platform, per-node)
// are a TODO for the metrics writer group.
var metricsMigrations = []migration{
	{
		version: 1,
		name:    "create-metric-buckets",
		up: `
CREATE TABLE IF NOT EXISTS metric_buckets(
	series       TEXT NOT NULL,
	bucket_start INTEGER NOT NULL,  -- unix seconds of the UTC hour start
	sum          REAL NOT NULL,
	cnt          INTEGER NOT NULL,
	PRIMARY KEY(series, bucket_start)
);`,
	},
}

// metricKey addresses one (series, hour bucket) cell in the dirty set.
type metricKey struct {
	series string
	bucket int64
}

// metricAgg is the coalesced aggregate for one cell.
type metricAgg struct {
	sum   float64
	count int64
}

// MetricsDB is the metrics.db handle: an in-memory coalescing accumulator
// (design §9.1, BatchSink/Coalescing) over hourly buckets, merged into the
// database additively on Flush.
//
// It is safe for concurrent use.
type MetricsDB struct {
	mu      sync.Mutex
	db      *sql.DB
	pending map[metricKey]metricAgg
	closed  bool
}

// OpenMetrics opens (creating when missing) the metrics.db at path and
// runs pending migrations.
func OpenMetrics(path string) (*MetricsDB, error) {
	db, err := openDB(path, "")
	if err != nil {
		return nil, fmt.Errorf("sqlite: open metrics db: %w", err)
	}
	if err := runMigrations(context.Background(), db, metricsMigrations); err != nil {
		db.Close()
		return nil, err
	}
	return &MetricsDB{db: db, pending: map[metricKey]metricAgg{}}, nil
}

// Record adds one sample to the (series, UTC-hour) bucket. It never blocks
// on I/O: the sample is only staged in memory until Flush.
//
// An empty series is ignored. NaN values are ignored (they would poison
// the bucket sum).
func (m *MetricsDB) Record(t time.Time, series string, value float64) {
	if series == "" || isNaN(value) {
		return
	}
	k := metricKey{series: series, bucket: t.UTC().Truncate(time.Hour).Unix()}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	agg := m.pending[k]
	agg.sum += value
	agg.count++
	m.pending[k] = agg
}

// Pending returns the number of dirty (series, bucket) cells. Tests and
// the flush scheduler use it to decide whether a Flush is worthwhile.
func (m *MetricsDB) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending)
}

// Flush merges every staged cell into metric_buckets additively
// (sum += staged.sum, cnt += staged.count) in a single transaction and
// clears the dirty set on success. It is a no-op when nothing is pending.
func (m *MetricsDB) Flush() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fmt.Errorf("sqlite: metrics db closed")
	}
	if len(m.pending) == 0 {
		m.mu.Unlock()
		return nil
	}
	staged := m.pending
	m.pending = map[metricKey]metricAgg{}
	m.mu.Unlock()

	ctx := context.Background()
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		m.requeue(staged)
		return fmt.Errorf("sqlite: metrics flush begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
			m.requeue(staged)
		}
	}()
	for k, agg := range staged {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO metric_buckets(series, bucket_start, sum, cnt)
			VALUES(?, ?, ?, ?)
			ON CONFLICT(series, bucket_start) DO UPDATE SET
				sum = metric_buckets.sum + excluded.sum,
				cnt = metric_buckets.cnt + excluded.cnt`,
			k.series, k.bucket, agg.sum, agg.count); err != nil {
			return fmt.Errorf("sqlite: metrics flush upsert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: metrics flush commit: %w", err)
	}
	committed = true
	return nil
}

// requeue puts staged cells back into the dirty set after a failed flush
// so the samples are not lost.
func (m *MetricsDB) requeue(staged map[metricKey]metricAgg) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	for k, agg := range staged {
		cur := m.pending[k]
		cur.sum += agg.sum
		cur.count += agg.count
		m.pending[k] = cur
	}
}

// Close flushes pending samples and then closes the database. Record
// calls after Close are ignored.
func (m *MetricsDB) Close() error {
	if err := m.Flush(); err != nil {
		m.db.Close()
		return err
	}
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return m.db.Close()
}

// DB exposes the underlying handle for tests and tooling. It is not part
// of any ports interface.
func (m *MetricsDB) DB() *sql.DB { return m.db }

func isNaN(f float64) bool { return f != f }
