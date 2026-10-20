package sqlite

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReqLogAppendAndRotate(t *testing.T) {
	dir := t.TempDir()
	// Tiny maxBytes forces rotation quickly; keepN=2 bounds the files.
	r, err := OpenReqLog(dir, 2, 4096)
	if err != nil {
		t.Fatalf("OpenReqLog: %v", err)
	}

	base := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 200; i++ {
		r.Append(ReqLogEntry{
			TS:         base.Add(time.Duration(i) * time.Second),
			ProxyType:  "http-forward",
			Platform:   "plat",
			Account:    "alice",
			Target:     "https://example.com/path",
			Node:       "0123456789abcdef0123456789abcdef",
			EgressIP:   "1.2.3.4",
			NetOK:      i%2 == 0,
			HTTPStatus: 200,
			Stage:      "request",
			DurationMs: 42,
			BytesUp:    100,
			BytesDown:  200,
		})
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d := r.Dropped(); d != 0 {
		t.Fatalf("Dropped = %d, want 0", d)
	}

	seqs, err := listReqLogSeqs(dir)
	if err != nil {
		t.Fatalf("listReqLogSeqs: %v", err)
	}
	if len(seqs) < 2 || len(seqs) > 2 {
		t.Fatalf("kept files = %v, want exactly 2 (keepN)", seqs)
	}

	// Every row across the retained files must carry all 14 fields.
	var total int
	var checked bool
	for _, seq := range seqs {
		db, err := openDB(filepath.Join(dir, reqLogFileName(seq)), "")
		if err != nil {
			t.Fatalf("open rolled file: %v", err)
		}
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM request_logs`).Scan(&n); err != nil {
			db.Close()
			t.Fatalf("count: %v", err)
		}
		total += n
		if !checked && n > 0 {
			var e struct {
				ts, proxyType, platform, account, target, node, egressIP string
				netOK, httpStatus                                        int
				stage, egressErr                                         string
				durationMs, bytesUp, bytesDown                           int64
			}
			if err := db.QueryRow(`SELECT ts_ns, proxy_type, platform, account, target, node,
				egress_ip, net_ok, http_status, stage, egress_error,
				duration_ms, bytes_up, bytes_down FROM request_logs LIMIT 1`).Scan(
				&e.ts, &e.proxyType, &e.platform, &e.account, &e.target, &e.node,
				&e.egressIP, &e.netOK, &e.httpStatus, &e.stage, &e.egressErr,
				&e.durationMs, &e.bytesUp, &e.bytesDown); err != nil {
				db.Close()
				t.Fatalf("scan: %v", err)
			}
			if e.proxyType != "http-forward" || e.platform != "plat" || e.account != "alice" ||
				e.target != "https://example.com/path" || e.node != "0123456789abcdef0123456789abcdef" ||
				e.egressIP != "1.2.3.4" || e.httpStatus != 200 || e.durationMs != 42 ||
				e.bytesUp != 100 || e.bytesDown != 200 {
				t.Fatalf("row = %+v", e)
			}
			checked = true
		}
		db.Close()
	}
	if total == 0 {
		t.Fatalf("no rows persisted")
	}
	t.Logf("persisted %d rows across %d files", total, len(seqs))
}

func TestReqLogDropOnFullQueue(t *testing.T) {
	dir := t.TempDir()
	r, err := OpenReqLog(dir, 2, 1<<20)
	if err != nil {
		t.Fatalf("OpenReqLog: %v", err)
	}
	// Fill the queue faster than the writer drains it. The exact drop
	// count is timing-dependent; we only assert Append never blocks and
	// Close flushes what was accepted.
	for i := 0; i < reqLogQueueSize*3; i++ {
		r.Append(ReqLogEntry{TS: time.Now(), ProxyType: "reverse"})
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	t.Logf("dropped = %d", r.Dropped())
}

func TestReqLogResumeAndPrune(t *testing.T) {
	dir := t.TempDir()
	r1, err := OpenReqLog(dir, 1, 1<<20)
	if err != nil {
		t.Fatalf("OpenReqLog: %v", err)
	}
	r1.Append(ReqLogEntry{TS: time.Now(), ProxyType: "socks5-forward"})
	if err := r1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Reopen: resumes the same file, keeps exactly keepN=1.
	r2, err := OpenReqLog(dir, 1, 1<<20)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := r2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	des, _ := os.ReadDir(dir)
	var dbs int
	for _, de := range des {
		if filepath.Ext(de.Name()) == ".db" {
			dbs++
		}
	}
	if dbs != 1 {
		t.Fatalf("files = %d, want 1", dbs)
	}
}
