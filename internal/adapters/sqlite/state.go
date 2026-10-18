package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xiarsx/egress/internal/ports"
)

// stateMigrations is the version chain for state.db (design §9.1: the
// strongly-consistent store of declarative resources).
var stateMigrations = []migration{
	{
		version: 1,
		name:    "create-resources",
		up: `
CREATE TABLE IF NOT EXISTS resources(
	kind    TEXT NOT NULL,
	name    TEXT NOT NULL,
	data    BLOB NOT NULL,
	version INTEGER NOT NULL,
	PRIMARY KEY(kind, name)
);`,
	},
}

// stateDB is the ports.Store implementation backed by state.db.
//
// Consistency: every mutating call commits before it returns
// (design §9.1: "API 返回成功 = 已提交").
type stateDB struct {
	db *sql.DB
}

var _ ports.Store = (*stateDB)(nil)

// OpenStateDB opens (creating when missing) the state.db at path and runs
// pending migrations, then returns it as a ports.Store.
func OpenStateDB(path string) (ports.Store, error) {
	db, err := openDB(path, "WAL")
	if err != nil {
		return nil, fmt.Errorf("sqlite: open state db: %w", err)
	}
	if err := runMigrations(context.Background(), db, stateMigrations); err != nil {
		db.Close()
		return nil, err
	}
	return &stateDB{db: db}, nil
}

// Close releases the underlying database handle. It is not part of
// ports.Store; owners that need deterministic shutdown type-assert to
// interface{ Close() error }.
func (s *stateDB) Close() error { return s.db.Close() }

func (s *stateDB) Get(ctx context.Context, kind ports.Kind, name string) (ports.Resource, error) {
	if kind == "" || name == "" {
		return ports.Resource{}, ports.ErrInvalid
	}
	var r ports.Resource
	var k string
	err := s.db.QueryRowContext(ctx,
		`SELECT kind, name, data, version FROM resources WHERE kind = ? AND name = ?`,
		string(kind), name).Scan(&k, &r.Name, &r.Data, &r.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return ports.Resource{}, ports.ErrNotFound
	}
	if err != nil {
		return ports.Resource{}, fmt.Errorf("sqlite: get resource: %w", err)
	}
	r.Kind = ports.Kind(k)
	return r, nil
}

func (s *stateDB) List(ctx context.Context, kind ports.Kind, opts ports.ListOptions) (ports.Page, error) {
	if kind == "" {
		return ports.Page{}, ports.ErrInvalid
	}
	limit, offset := opts.Limit, opts.Offset
	if offset < 0 {
		offset = 0
	}
	var total int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM resources WHERE kind = ?`, string(kind)).Scan(&total); err != nil {
		return ports.Page{}, fmt.Errorf("sqlite: count resources: %w", err)
	}
	query := `SELECT kind, name, data, version FROM resources WHERE kind = ? ORDER BY name`
	switch {
	case limit > 0:
		query += fmt.Sprintf(` LIMIT %d OFFSET %d`, limit, offset)
	case offset > 0:
		query += fmt.Sprintf(` LIMIT -1 OFFSET %d`, offset)
	}
	rows, err := s.db.QueryContext(ctx, query, string(kind))
	if err != nil {
		return ports.Page{}, fmt.Errorf("sqlite: list resources: %w", err)
	}
	defer rows.Close()
	items := []ports.Resource{}
	for rows.Next() {
		var r ports.Resource
		var k string
		if err := rows.Scan(&k, &r.Name, &r.Data, &r.Version); err != nil {
			return ports.Page{}, fmt.Errorf("sqlite: scan resource: %w", err)
		}
		r.Kind = ports.Kind(k)
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return ports.Page{}, fmt.Errorf("sqlite: iterate resources: %w", err)
	}
	return ports.Page{Items: items, Total: int(total), Limit: limit, Offset: offset}, nil
}

func (s *stateDB) Create(ctx context.Context, r ports.Resource) error {
	if r.Kind == "" || r.Name == "" {
		return ports.ErrInvalid
	}
	data := r.Data
	if data == nil {
		data = []byte{}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO resources(kind, name, data, version) VALUES(?, ?, ?, 1)`,
		string(r.Kind), r.Name, data)
	if err != nil {
		if isUniqueViolation(err) {
			return ports.ErrAlreadyExists
		}
		return fmt.Errorf("sqlite: create resource: %w", err)
	}
	return nil
}

func (s *stateDB) Update(ctx context.Context, r ports.Resource) error {
	if r.Kind == "" || r.Name == "" {
		return ports.ErrInvalid
	}
	if r.Version <= 0 {
		// A caller that never read the resource cannot hold a valid
		// version; treat it as stale rather than silently succeeding.
		return ports.ErrConflict
	}
	data := r.Data
	if data == nil {
		data = []byte{}
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE resources SET data = ?, version = version + 1
		  WHERE kind = ? AND name = ? AND version = ?`,
		data, string(r.Kind), r.Name, r.Version)
	if err != nil {
		return fmt.Errorf("sqlite: update resource: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: update resource rows: %w", err)
	}
	if n == 1 {
		return nil
	}
	// No row matched: distinguish "deleted since read" from "changed since read".
	_, gerr := s.Get(ctx, r.Kind, r.Name)
	if errors.Is(gerr, ports.ErrNotFound) {
		return ports.ErrNotFound
	}
	if gerr != nil {
		return fmt.Errorf("sqlite: update resource re-read: %w", gerr)
	}
	return ports.ErrConflict
}

func (s *stateDB) Delete(ctx context.Context, kind ports.Kind, name string) error {
	if kind == "" || name == "" {
		return ports.ErrInvalid
	}
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM resources WHERE kind = ? AND name = ?`,
		string(kind), name)
	if err != nil {
		return fmt.Errorf("sqlite: delete resource: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete resource rows: %w", err)
	}
	if n == 0 {
		return ports.ErrNotFound
	}
	return nil
}

func (s *stateDB) Patch(ctx context.Context, kind ports.Kind, name string, patch []byte) (ports.Resource, error) {
	if kind == "" || name == "" {
		return ports.Resource{}, ports.ErrInvalid
	}
	if !json.Valid(patch) {
		return ports.Resource{}, ports.ErrInvalid
	}
	cur, err := s.Get(ctx, kind, name)
	if err != nil {
		return ports.Resource{}, err
	}
	merged, err := jsonMergePatch(cur.Data, patch)
	if err != nil {
		return ports.Resource{}, fmt.Errorf("%w: %v", ports.ErrInvalid, err)
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE resources SET data = ?, version = version + 1
		  WHERE kind = ? AND name = ? AND version = ?`,
		merged, string(kind), name, cur.Version)
	if err != nil {
		return ports.Resource{}, fmt.Errorf("sqlite: patch resource: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return ports.Resource{}, fmt.Errorf("sqlite: patch resource rows: %w", err)
	}
	if n == 0 {
		// Lost a race with a concurrent writer or deleter.
		_, gerr := s.Get(ctx, kind, name)
		if errors.Is(gerr, ports.ErrNotFound) {
			return ports.Resource{}, ports.ErrNotFound
		}
		if gerr != nil {
			return ports.Resource{}, fmt.Errorf("sqlite: patch resource re-read: %w", gerr)
		}
		return ports.Resource{}, ports.ErrConflict
	}
	return ports.Resource{Kind: kind, Name: name, Data: merged, Version: cur.Version + 1}, nil
}

// jsonMergePatch applies an RFC 7386 JSON merge-patch to doc. A null or
// empty document is treated as {}. A non-object patch replaces the
// document wholesale.
func jsonMergePatch(doc, patch []byte) ([]byte, error) {
	var docVal any
	if trimmed := bytes.TrimSpace(doc); len(trimmed) == 0 || string(trimmed) == "null" {
		docVal = map[string]any{}
	} else if err := json.Unmarshal(doc, &docVal); err != nil {
		return nil, fmt.Errorf("document is not JSON: %w", err)
	}
	var patchVal any
	if err := json.Unmarshal(patch, &patchVal); err != nil {
		return nil, fmt.Errorf("patch is not JSON: %w", err)
	}
	return json.Marshal(mergePatchValue(docVal, patchVal))
}

func mergePatchValue(doc, patch any) any {
	patchObj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	docObj, _ := doc.(map[string]any)
	if docObj == nil {
		docObj = map[string]any{}
	}
	for k, v := range patchObj {
		if v == nil {
			delete(docObj, k)
			continue
		}
		docObj[k] = mergePatchValue(docObj[k], v)
	}
	return docObj
}
