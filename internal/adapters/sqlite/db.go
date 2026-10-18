// Package sqlite implements the persistence adapters: state.db
// (ports.Store), cache.db (ports.CacheLoader/CacheStore), metrics.db and
// the rolling request_logs-*.db files (design §9).
//
// Every database file is independent: no ATTACH, no cross-database SQL
// (design §9.2). Each file carries its own schema_version table and its
// own numbered migration chain (see migrate.go).
package sqlite

import (
	"database/sql"
	"errors"
	"net/url"
	"strings"
	"time"

	msqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// busyTimeout is applied to every database handle so concurrent readers
// and the single writer block briefly instead of failing with SQLITE_BUSY.
const busyTimeout = 5 * time.Second

// openDB opens path with the given journal mode and applies the standard
// pragmas. path may be ":memory:" (used by tests); in that case the pool
// is pinned to a single connection because each :memory: connection would
// otherwise get its own private database.
func openDB(path, journalMode string) (*sql.DB, error) {
	var dsn string
	if path == ":memory:" {
		dsn = ":memory:"
	} else {
		q := url.Values{}
		q.Add("_pragma", "busy_timeout(5000)")
		if journalMode != "" {
			q.Add("_pragma", "journal_mode("+journalMode+")")
		}
		dsn = "file:" + path + "?" + q.Encode()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	} else {
		// A small pool is plenty: one writer plus a few readers.
		db.SetMaxOpenConns(8)
	}
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// isUniqueViolation reports whether err is a SQLite uniqueness conflict
// (UNIQUE or PRIMARY KEY constraint failure). It is used to map races on
// INSERT to ports.ErrAlreadyExists.
func isUniqueViolation(err error) bool {
	var serr *msqlite.Error
	if errors.As(err, &serr) {
		// Extended result codes pack the base code in the low byte:
		// SQLITE_CONSTRAINT_UNIQUE (2067) and SQLITE_CONSTRAINT_PRIMARYKEY
		// (1555) both mask down to SQLITE_CONSTRAINT (19).
		return serr.Code()&0xFF == sqlite3.SQLITE_CONSTRAINT
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed") ||
		strings.Contains(err.Error(), "PRIMARY KEY constraint failed")
}
