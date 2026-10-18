package sqlite

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// reqlog_query.go: read API over the rolling request-log files.
//
// The writer (ReqLog) is append-only; readers open the sequence files
// read-only and never interfere with the background writer. Entry IDs are
// per-file AUTOINCREMENT values; the API exposes them as "seq:id".

// ReqLogID identifies one entry across rolling files.
type ReqLogID struct {
	Seq int
	ID  int64
}

func (i ReqLogID) String() string { return fmt.Sprintf("%d:%d", i.Seq, i.ID) }

// ParseReqLogID parses "seq:id" or a bare per-file id (seq resolved as -1,
// meaning "search newest file first").
func ParseReqLogID(s string) (ReqLogID, error) {
	if strings.Contains(s, ":") {
		var id ReqLogID
		if _, err := fmt.Sscanf(s, "%d:%d", &id.Seq, &id.ID); err != nil {
			return ReqLogID{}, fmt.Errorf("sqlite: bad request log id %q", s)
		}
		return id, nil
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return ReqLogID{}, fmt.Errorf("sqlite: bad request log id %q", s)
	}
	return ReqLogID{Seq: -1, ID: n}, nil
}

// QueryReqLogs reads entries newest-first across all rolling files.
// Filters are exact matches; empty means no filter. Returns the page and
// the total count. It opens files read-only; safe to call while the
// writer is appending.
func (r *ReqLog) QueryReqLogs(limit, offset int, platform, account, resinErr string) ([]ReqLogEntry, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100000 {
		limit = 100000
	}
	seqs, err := listReqLogSeqs(r.dir)
	if err != nil {
		return nil, 0, err
	}
	var conds []string
	var args []any
	if platform != "" {
		conds = append(conds, "platform = ?")
		args = append(args, platform)
	}
	if account != "" {
		conds = append(conds, "account = ?")
		args = append(args, account)
	}
	if resinErr != "" {
		conds = append(conds, "resin_error = ?")
		args = append(args, resinErr)
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	var all []reqLogRow
	total := 0
	// Newest file first so pagination is recency-ordered.
	for i := len(seqs) - 1; i >= 0; i-- {
		db, err := sql.Open("sqlite", "file:"+r.dir+"/"+reqLogFileName(seqs[i])+"?mode=ro")
		if err != nil {
			return nil, 0, err
		}
		rows, qerr := queryReqLogFile(db, seqs[i], where, args)
		db.Close()
		if qerr != nil {
			return nil, 0, qerr
		}
		total += len(rows)
		all = append(all, rows...)
		// Safety cap: never hold more than ~2M rows in memory for a query.
		if len(all) > 2000000 {
			break
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].Entry.TS.Equal(all[j].Entry.TS) {
			return all[i].Entry.TS.After(all[j].Entry.TS)
		}
		if all[i].Seq != all[j].Seq {
			return all[i].Seq > all[j].Seq
		}
		return all[i].ID > all[j].ID
	})
	if offset > len(all) {
		offset = len(all)
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	out := make([]ReqLogEntry, 0, end-offset)
	for _, row := range all[offset:end] {
		out = append(out, row.Entry)
	}
	_ = total
	return out, len(all), nil
}

type reqLogRow struct {
	Seq   int
	ID    int64
	Entry ReqLogEntry
}

func queryReqLogFile(db *sql.DB, seq int, where string, args []any) ([]reqLogRow, error) {
	q := `SELECT id, ts_ns, proxy_type, platform, account, target, node,
		egress_ip, net_ok, http_status, stage, resin_error,
		duration_ms, bytes_up, bytes_down
		FROM request_logs ` + where + ` ORDER BY ts_ns DESC, id DESC`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: query request logs: %w", err)
	}
	defer rows.Close()
	var out []reqLogRow
	for rows.Next() {
		var e ReqLogEntry
		var id, tsNs int64
		var netOk int
		if err := rows.Scan(&id, &tsNs, &e.ProxyType, &e.Platform, &e.Account,
			&e.Target, &e.Node, &e.EgressIP, &netOk, &e.HTTPStatus, &e.Stage,
			&e.ResinError, &e.DurationMs, &e.BytesUp, &e.BytesDown); err != nil {
			return nil, err
		}
		e.TS = time.Unix(0, tsNs)
		e.NetOK = netOk != 0
		out = append(out, reqLogRow{Seq: seq, ID: id, Entry: e})
	}
	return out, rows.Err()
}

// GetReqLog fetches one entry by id ("seq:id", or bare id searched
// newest-file-first).
func (r *ReqLog) GetReqLog(id ReqLogID) (ReqLogEntry, int64, error) {
	seqs, err := listReqLogSeqs(r.dir)
	if err != nil {
		return ReqLogEntry{}, 0, err
	}
	if id.Seq >= 0 {
		db, err := sql.Open("sqlite", "file:"+r.dir+"/"+reqLogFileName(id.Seq)+"?mode=ro")
		if err != nil {
			return ReqLogEntry{}, 0, err
		}
		defer db.Close()
		return scanOneReqLog(db, id.ID)
	}
	for i := len(seqs) - 1; i >= 0; i-- {
		db, err := sql.Open("sqlite", "file:"+r.dir+"/"+reqLogFileName(seqs[i])+"?mode=ro")
		if err != nil {
			continue
		}
		e, rid, qerr := scanOneReqLog(db, id.ID)
		db.Close()
		if qerr == nil {
			return e, rid, nil
		}
	}
	return ReqLogEntry{}, 0, sql.ErrNoRows
}

func scanOneReqLog(db *sql.DB, id int64) (ReqLogEntry, int64, error) {
	var e ReqLogEntry
	var tsNs int64
	var netOk int
	err := db.QueryRow(`SELECT id, ts_ns, proxy_type, platform, account, target, node,
		egress_ip, net_ok, http_status, stage, resin_error,
		duration_ms, bytes_up, bytes_down
		FROM request_logs WHERE id = ?`, id).Scan(
		&id, &tsNs, &e.ProxyType, &e.Platform, &e.Account,
		&e.Target, &e.Node, &e.EgressIP, &netOk, &e.HTTPStatus, &e.Stage,
		&e.ResinError, &e.DurationMs, &e.BytesUp, &e.BytesDown)
	if err != nil {
		return ReqLogEntry{}, 0, err
	}
	e.TS = time.Unix(0, tsNs)
	e.NetOK = netOk != 0
	return e, id, nil
}
