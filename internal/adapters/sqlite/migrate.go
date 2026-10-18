package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// migration is one numbered, irreversible schema step. There is no down:
// downgrades are handled by restoring a backup (design §9.1).
type migration struct {
	version int
	name    string
	up      string // one or more statements; executed in a single transaction
}

const schemaVersionDDL = `
CREATE TABLE IF NOT EXISTS schema_version(
	version    INTEGER PRIMARY KEY,
	name       TEXT NOT NULL,
	applied_at INTEGER NOT NULL
);`

// runMigrations creates schema_version when missing and applies every
// pending migration in version order. Each migration runs in its own
// transaction; applied migrations are never re-run.
func runMigrations(ctx context.Context, db *sql.DB, migrations []migration) error {
	if _, err := db.ExecContext(ctx, schemaVersionDDL); err != nil {
		return fmt.Errorf("sqlite: create schema_version: %w", err)
	}
	applied := map[int]bool{}
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_version`)
	if err != nil {
		return fmt.Errorf("sqlite: read schema_version: %w", err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("sqlite: scan schema_version: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("sqlite: iterate schema_version: %w", err)
	}
	rows.Close()

	ms := append([]migration(nil), migrations...)
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	for _, m := range ms {
		if applied[m.version] {
			continue
		}
		if err := applyMigration(ctx, db, m); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, m migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin migration %d (%s): %w", m.version, m.name, err)
	}
	if _, err := tx.ExecContext(ctx, m.up); err != nil {
		tx.Rollback()
		return fmt.Errorf("sqlite: apply migration %d (%s): %w", m.version, m.name, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_version(version, name, applied_at) VALUES(?,?,?)`,
		m.version, m.name, time.Now().UnixNano()); err != nil {
		tx.Rollback()
		return fmt.Errorf("sqlite: record migration %d (%s): %w", m.version, m.name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit migration %d (%s): %w", m.version, m.name, err)
	}
	return nil
}

// schemaVersion returns the highest applied migration version, or 0 when
// the database is fresh.
func schemaVersion(ctx context.Context, db *sql.DB) (int, error) {
	var v sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT MAX(version) FROM schema_version`).Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return int(v.Int64), nil
}
