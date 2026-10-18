package sqlite

import (
	"strconv"
	"testing"
	"time"
)

func openTestMetrics(t *testing.T) *MetricsDB {
	t.Helper()
	m, err := OpenMetrics(t.TempDir() + "/metrics.db")
	if err != nil {
		t.Fatalf("OpenMetrics: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func TestMetricsCoalescing(t *testing.T) {
	m := openTestMetrics(t)
	base := time.Date(2026, 10, 17, 10, 15, 0, 0, time.UTC)

	m.Record(base, "requests.ok", 1)
	m.Record(base.Add(30*time.Minute), "requests.ok", 1) // same hour bucket
	m.Record(base.Add(2*time.Hour), "requests.ok", 1)    // next bucket
	m.Record(base, "traffic.bytes_up", 100)
	m.Record(base, "", 5) // ignored
	if got := m.Pending(); got != 3 {
		t.Fatalf("Pending = %d, want 3", got)
	}
	if err := m.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := m.Pending(); got != 0 {
		t.Fatalf("Pending after flush = %d, want 0", got)
	}

	rows, err := m.DB().Query(`SELECT series, bucket_start, sum, cnt FROM metric_buckets ORDER BY series, bucket_start`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	type cell struct {
		series string
		bucket int64
		sum    float64
		cnt    int64
	}
	var cells []cell
	for rows.Next() {
		var c cell
		if err := rows.Scan(&c.series, &c.bucket, &c.sum, &c.cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		cells = append(cells, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(cells) != 3 {
		t.Fatalf("cells = %+v, want 3", cells)
	}
	b10 := base.UTC().Truncate(time.Hour).Unix()
	b12 := base.Add(2 * time.Hour).UTC().Truncate(time.Hour).Unix()
	want := map[string]cell{
		"requests.ok\x00" + strconv.FormatInt(b10, 10):      {series: "requests.ok", bucket: b10, sum: 2, cnt: 2},
		"requests.ok\x00" + strconv.FormatInt(b12, 10):      {series: "requests.ok", bucket: b12, sum: 1, cnt: 1},
		"traffic.bytes_up\x00" + strconv.FormatInt(b10, 10): {series: "traffic.bytes_up", bucket: b10, sum: 100, cnt: 1},
	}
	for _, c := range cells {
		w, ok := want[c.series+"\x00"+strconv.FormatInt(c.bucket, 10)]
		if !ok || w != c {
			t.Fatalf("cell = %+v, want %+v", c, w)
		}
	}

	// A second flush merges additively into existing buckets.
	m.Record(base, "requests.ok", 1)
	if err := m.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	var sum float64
	var cnt int64
	if err := m.DB().QueryRow(`SELECT sum, cnt FROM metric_buckets WHERE series='requests.ok' AND bucket_start=?`, b10).Scan(&sum, &cnt); err != nil {
		t.Fatalf("query: %v", err)
	}
	if sum != 3 || cnt != 3 {
		t.Fatalf("merged = (%v,%v), want (3,3)", sum, cnt)
	}
}
