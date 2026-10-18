package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Request-log rolling files live in one directory as
// request_logs-<seq>.db (seq zero-padded, monotonically increasing).
const reqLogFilePrefix = "request_logs-"

// Tuning for the append path (reference: RESIN_REQUEST_LOG_QUEUE_SIZE
// 8192, batch 4096, 5m interval; interval shortened here — see below).
const (
	reqLogQueueSize  = 8192
	reqLogBatchSize  = 4096
	reqLogFlushEvery = 5 * time.Second
	reqLogDirPerm    = 0o755
)

// ReqLogEntry is one proxied request. The 14 fields mirror spec/events.md
// §2 (RequestLog); ts is the request start time.
//
// Payload capture (req/resp headers/body) is an opt-in switch defaulting
// to off (design §10) and is intentionally not part of this record —
// see TODO in OpenReqLog.
type ReqLogEntry struct {
	TS         time.Time
	ProxyType  string // "http-forward" | "reverse" | "socks5-forward"
	Platform   string // resolved platform name (wire-protocol name, not UUID)
	Account    string // empty = random routing
	Target     string // host or full URL, decided by the ingress
	Node       string // node hash hex; empty when unassigned
	EgressIP   string // empty when unknown
	NetOK      bool
	HTTPStatus int // 0 when meaningless (tunnel/SOCKS5)
	Stage      string
	ResinError string // e.g. UPSTREAM_TIMEOUT; empty on success
	DurationMs int64
	BytesUp    int64
	BytesDown  int64
}

// reqLogDDL is applied to every rolling file (schema is append-only and
// versioned implicitly by the file sequence; no migrations needed).
const reqLogDDL = `
CREATE TABLE IF NOT EXISTS request_logs(
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	ts_ns       INTEGER NOT NULL,
	proxy_type  TEXT NOT NULL,
	platform    TEXT NOT NULL,
	account     TEXT NOT NULL,
	target      TEXT NOT NULL,
	node        TEXT NOT NULL,
	egress_ip   TEXT NOT NULL,
	net_ok      INTEGER NOT NULL,
	http_status INTEGER NOT NULL,
	stage       TEXT NOT NULL,
	resin_error TEXT NOT NULL,
	duration_ms INTEGER NOT NULL,
	bytes_up    INTEGER NOT NULL,
	bytes_down  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_request_logs_ts_id ON request_logs(ts_ns, id);`

// ReqLog is an append-only, size-rolled request log over
// request_logs-*.db files (design §9.1: weak, append).
//
// Append never blocks the caller: entries are staged on a bounded channel
// and a background goroutine batches them to disk. When the channel is
// full the entry is dropped and counted (see Dropped) — the hot path must
// never stall on logging.
//
// When the current file reaches maxBytes it is closed and a new sequence
// file is opened; only the newest keepN files are retained.
type ReqLog struct {
	dir      string
	keepN    int
	maxBytes int64

	mu      sync.Mutex // guards db, curSeq, curSize
	db      *sql.DB
	curSeq  int
	curSize int64

	ch      chan ReqLogEntry
	done    chan struct{}
	wg      sync.WaitGroup
	closeMu sync.Mutex // guards against double Close
	closed  bool
	dropped atomic.Int64
}

// OpenReqLog opens the rolling request log in dir, keeping the newest
// keepN files and rolling the current file once it reaches maxBytes.
//
//   - keepN < 1 is clamped to 1; maxBytes <= 0 defaults to 512 MiB.
//   - When dir already holds rolled files, appending resumes in the newest
//     file while it is still under maxBytes.
//
// TODO(payload): optional request/response payload capture (spec/events.md
// "可选 payload 字段", default off) needs a request_log_payloads table and
// a capture switch; not implemented — entries carry no payload today.
func OpenReqLog(dir string, keepN int, maxBytes int64) (*ReqLog, error) {
	if keepN < 1 {
		keepN = 1
	}
	if maxBytes <= 0 {
		maxBytes = 512 << 20
	}
	if err := os.MkdirAll(dir, reqLogDirPerm); err != nil {
		return nil, fmt.Errorf("sqlite: create request log dir: %w", err)
	}
	r := &ReqLog{
		dir:      dir,
		keepN:    keepN,
		maxBytes: maxBytes,
		ch:       make(chan ReqLogEntry, reqLogQueueSize),
		done:     make(chan struct{}),
	}
	seqs, err := listReqLogSeqs(dir)
	if err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		if err := r.openSeqLocked(1); err != nil {
			return nil, err
		}
	} else {
		last := seqs[len(seqs)-1]
		if err := r.openSeqLocked(last); err != nil {
			return nil, err
		}
		if r.curSize >= r.maxBytes {
			if err := r.rotateLocked(); err != nil {
				r.db.Close()
				return nil, err
			}
		}
		r.pruneLocked()
	}
	r.wg.Add(1)
	go r.run()
	return r, nil
}

// Append stages one entry for batched writing. It never blocks: when the
// internal queue is full the entry is dropped and Dropped is incremented.
func (r *ReqLog) Append(e ReqLogEntry) {
	select {
	case r.ch <- e:
	default:
		r.dropped.Add(1)
	}
}

// Dropped reports how many entries were discarded because the queue was
// full.
func (r *ReqLog) Dropped() int64 { return r.dropped.Load() }

// Close stops the background writer, flushes everything staged, and
// closes the current file. Appends after Close are dropped. It is safe to
// call Close more than once.
func (r *ReqLog) Close() error {
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return nil
	}
	r.closed = true
	r.closeMu.Unlock()

	close(r.done)
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db != nil {
		err := r.db.Close()
		r.db = nil
		return err
	}
	return nil
}

// run batches staged entries to disk: on batch-size or on ticker, and a
// final drain on shutdown.
func (r *ReqLog) run() {
	defer r.wg.Done()
	batch := make([]ReqLogEntry, 0, reqLogBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		r.writeBatch(batch)
		batch = batch[:0]
	}
	ticker := time.NewTicker(reqLogFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case e := <-r.ch:
			batch = append(batch, e)
			if len(batch) >= reqLogBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-r.done:
			// Drain everything that was staged before Close returned.
			for {
				select {
				case e := <-r.ch:
					batch = append(batch, e)
					if len(batch) >= reqLogBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// writeBatch inserts one batch in a single transaction, then rolls the
// file when it reached maxBytes. r.db == nil (after Close) can only
// happen for entries staged before Close drained — they are discarded.
func (r *ReqLog) writeBatch(batch []ReqLogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return
	}
	ctx := context.Background()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO request_logs(ts_ns, proxy_type, platform, account, target, node,
			egress_ip, net_ok, http_status, stage, resin_error,
			duration_ms, bytes_up, bytes_down)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return
	}
	for _, e := range batch {
		netOK := 0
		if e.NetOK {
			netOK = 1
		}
		if _, err := stmt.ExecContext(ctx,
			e.TS.UTC().UnixNano(),
			e.ProxyType, e.Platform, e.Account, e.Target, e.Node,
			e.EgressIP, netOK, e.HTTPStatus, e.Stage, e.ResinError,
			e.DurationMs, e.BytesUp, e.BytesDown); err != nil {
			stmt.Close()
			tx.Rollback()
			return
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		return
	}
	r.refreshSizeLocked()
	if r.curSize >= r.maxBytes {
		// Best effort: a failed rotate keeps appending to the old file;
		// the next batch retries.
		_ = r.rotateLocked()
	}
}

// openSeqLocked opens (creating and migrating) the file for seq and makes
// it current. Callers hold r.mu.
func (r *ReqLog) openSeqLocked(seq int) error {
	path := filepath.Join(r.dir, reqLogFileName(seq))
	db, err := openDB(path, "")
	if err != nil {
		return fmt.Errorf("sqlite: open request log file: %w", err)
	}
	if _, err := db.ExecContext(context.Background(), reqLogDDL); err != nil {
		db.Close()
		return fmt.Errorf("sqlite: init request log schema: %w", err)
	}
	r.db = db
	r.curSeq = seq
	r.refreshSizeLocked()
	return nil
}

// rotateLocked closes the current file and opens the next sequence,
// pruning old files beyond keepN. Callers hold r.mu.
func (r *ReqLog) rotateLocked() error {
	if r.db != nil {
		if err := r.db.Close(); err != nil {
			return fmt.Errorf("sqlite: close rolled request log: %w", err)
		}
		r.db = nil
	}
	if err := r.openSeqLocked(r.curSeq + 1); err != nil {
		return err
	}
	r.pruneLocked()
	return nil
}

// pruneLocked deletes rolled files beyond the newest keepN. Callers hold
// r.mu (it only touches the filesystem, but keep the discipline).
func (r *ReqLog) pruneLocked() {
	seqs, err := listReqLogSeqs(r.dir)
	if err != nil || len(seqs) <= r.keepN {
		return
	}
	for _, seq := range seqs[:len(seqs)-r.keepN] {
		_ = os.Remove(filepath.Join(r.dir, reqLogFileName(seq)))
	}
}

func (r *ReqLog) refreshSizeLocked() {
	if r.db == nil {
		return
	}
	path := filepath.Join(r.dir, reqLogFileName(r.curSeq))
	if fi, err := os.Stat(path); err == nil {
		r.curSize = fi.Size()
	}
}

func reqLogFileName(seq int) string {
	return fmt.Sprintf("%s%06d.db", reqLogFilePrefix, seq)
}

// listReqLogSeqs returns the sorted sequence numbers of
// request_logs-*.db files in dir.
func listReqLogSeqs(dir string) ([]int, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list request log dir: %w", err)
	}
	var seqs []int
	for _, de := range des {
		name := de.Name()
		if de.IsDir() || !strings.HasPrefix(name, reqLogFilePrefix) || !strings.HasSuffix(name, ".db") {
			continue
		}
		num := strings.TrimSuffix(strings.TrimPrefix(name, reqLogFilePrefix), ".db")
		seq, err := strconv.Atoi(num)
		if err != nil {
			continue // ignore foreign files
		}
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	return seqs, nil
}
