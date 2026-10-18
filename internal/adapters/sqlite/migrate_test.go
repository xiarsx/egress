package sqlite

import (
	"context"
	"testing"
)

func TestMigrationsApplyOnce(t *testing.T) {
	ctx := context.Background()
	db, err := openDB(":memory:", "")
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	ms := []migration{
		{version: 2, name: "second", up: `CREATE TABLE t2(x INTEGER);`},
		{version: 1, name: "first", up: `CREATE TABLE t1(x INTEGER);`},
	}
	// Out-of-order input must still apply in version order.
	if err := runMigrations(ctx, db, ms); err != nil {
		t.Fatalf("runMigrations: %v", err)
	}
	if v, err := schemaVersion(ctx, db); err != nil || v != 2 {
		t.Fatalf("schemaVersion = %d, %v; want 2", v, err)
	}
	// Re-running is a no-op and must not fail.
	if err := runMigrations(ctx, db, ms); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if v, err := schemaVersion(ctx, db); err != nil || v != 2 {
		t.Fatalf("schemaVersion after re-run = %d, %v; want 2", v, err)
	}

	// A failing migration aborts without recording the version.
	bad := []migration{{version: 3, name: "bad", up: `CREATE TABLE oops(`}}
	if err := runMigrations(ctx, db, bad); err == nil {
		t.Fatalf("bad migration: want error")
	}
	if v, err := schemaVersion(ctx, db); err != nil || v != 2 {
		t.Fatalf("schemaVersion after failure = %d, %v; want 2", v, err)
	}
}
