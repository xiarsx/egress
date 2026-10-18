package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xiarsx/egress/internal/ports"
)

func openTestState(t *testing.T) ports.Store {
	t.Helper()
	s, err := OpenStateDB(":memory:")
	if err != nil {
		t.Fatalf("OpenStateDB: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := s.(interface{ Close() error }); ok {
			c.Close()
		}
	})
	return s
}

func TestStateCRUD(t *testing.T) {
	ctx := context.Background()
	s := openTestState(t)

	// Create assigns nothing to the caller; version starts at 1 on read.
	if err := s.Create(ctx, ports.Resource{Kind: ports.KindPlatform, Name: "p1", Data: []byte(`{"a":1}`)}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get(ctx, ports.KindPlatform, "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != 1 || string(got.Data) != `{"a":1}` || got.Name != "p1" || got.Kind != ports.KindPlatform {
		t.Fatalf("Get = %+v, want version 1 and original data", got)
	}

	// Duplicate create.
	if err := s.Create(ctx, ports.Resource{Kind: ports.KindPlatform, Name: "p1"}); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("duplicate Create = %v, want ErrAlreadyExists", err)
	}
	// Same name under another kind is a different resource.
	if err := s.Create(ctx, ports.Resource{Kind: ports.KindSource, Name: "p1"}); err != nil {
		t.Fatalf("Create other kind: %v", err)
	}

	// Update with the fresh version succeeds and bumps the version.
	got.Data = []byte(`{"a":2}`)
	if err := s.Update(ctx, got); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got2, err := s.Get(ctx, ports.KindPlatform, "p1")
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if got2.Version != 2 || string(got2.Data) != `{"a":2}` {
		t.Fatalf("after update = %+v, want version 2", got2)
	}

	// Stale version -> conflict.
	got.Data = []byte(`{"a":3}`) // got.Version is still 1
	if err := s.Update(ctx, got); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("stale Update = %v, want ErrConflict", err)
	}
	// Zero version -> conflict as well.
	if err := s.Update(ctx, ports.Resource{Kind: ports.KindPlatform, Name: "p1", Data: []byte(`{}`)}); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("zero-version Update = %v, want ErrConflict", err)
	}
	// Missing resource -> not found.
	if err := s.Update(ctx, ports.Resource{Kind: ports.KindPlatform, Name: "nope", Version: 1}); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("missing Update = %v, want ErrNotFound", err)
	}

	// Delete.
	if err := s.Delete(ctx, ports.KindPlatform, "p1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, ports.KindPlatform, "p1"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, ports.KindPlatform, "p1"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("double Delete = %v, want ErrNotFound", err)
	}
}

func TestStateInvalid(t *testing.T) {
	ctx := context.Background()
	s := openTestState(t)
	if _, err := s.Get(ctx, "", "x"); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("Get empty kind = %v, want ErrInvalid", err)
	}
	if err := s.Create(ctx, ports.Resource{Kind: ports.KindPlatform}); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("Create empty name = %v, want ErrInvalid", err)
	}
	if _, err := s.Patch(ctx, ports.KindPlatform, "x", []byte(`{`)); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("Patch bad JSON = %v, want ErrInvalid", err)
	}
	if _, err := s.Patch(ctx, ports.KindPlatform, "missing", []byte(`{}`)); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("Patch missing = %v, want ErrNotFound", err)
	}
}

func TestStatePatch(t *testing.T) {
	ctx := context.Background()
	s := openTestState(t)
	if err := s.Create(ctx, ports.Resource{
		Kind: ports.KindPlatform, Name: "p",
		Data: []byte(`{"a":1,"nested":{"x":1,"y":2},"gone":"yes"}`),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Patch(ctx, ports.KindPlatform, "p",
		[]byte(`{"a":2,"nested":{"y":null,"z":3},"gone":null,"added":true}`))
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if got.Version != 2 {
		t.Fatalf("Patch version = %d, want 2", got.Version)
	}
	// RFC 7386: null deletes keys, objects merge recursively.
	want := `{"a":2,"added":true,"nested":{"x":1,"z":3}}`
	if compact(got.Data) != want {
		t.Fatalf("Patch data = %s, want %s", got.Data, want)
	}
	// Non-object patch replaces wholesale.
	got, err = s.Patch(ctx, ports.KindPlatform, "p", []byte(`[1,2]`))
	if err != nil {
		t.Fatalf("Patch replace: %v", err)
	}
	if compact(got.Data) != `[1,2]` || got.Version != 3 {
		t.Fatalf("Patch replace = %s v%d, want [1,2] v3", got.Data, got.Version)
	}
}

func TestStateListPagination(t *testing.T) {
	ctx := context.Background()
	s := openTestState(t)
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		if err := s.Create(ctx, ports.Resource{Kind: ports.KindSource, Name: n, Data: []byte(`{}`)}); err != nil {
			t.Fatalf("Create %s: %v", n, err)
		}
	}
	// A different kind must not leak in.
	if err := s.Create(ctx, ports.Resource{Kind: ports.KindPlatform, Name: "a", Data: []byte(`{}`)}); err != nil {
		t.Fatalf("Create platform: %v", err)
	}

	page, err := s.List(ctx, ports.KindSource, ports.ListOptions{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if page.Total != 5 || page.Limit != 2 || page.Offset != 1 {
		t.Fatalf("page meta = %+v, want total 5 limit 2 offset 1", page)
	}
	if len(page.Items) != 2 || page.Items[0].Name != "b" || page.Items[1].Name != "c" {
		t.Fatalf("page items = %+v, want [b c]", page.Items)
	}
	for _, it := range page.Items {
		if it.Kind != ports.KindSource || it.Version != 1 {
			t.Fatalf("item = %+v, want kind Source version 1", it)
		}
	}

	// No limit: everything, ordered by name.
	page, err = s.List(ctx, ports.KindSource, ports.ListOptions{})
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if page.Total != 5 || len(page.Items) != 5 || page.Items[4].Name != "e" {
		t.Fatalf("List all = %+v", page)
	}

	// Offset past the end: empty items, total still reported.
	page, err = s.List(ctx, ports.KindSource, ports.ListOptions{Offset: 99})
	if err != nil {
		t.Fatalf("List offset: %v", err)
	}
	if page.Total != 5 || len(page.Items) != 0 {
		t.Fatalf("List offset = %+v, want total 5 empty items", page)
	}
}

func TestStateMigrationsIdempotent(t *testing.T) { // Opening twice (same file) must not re-apply migrations or fail.
	dir := t.TempDir()
	path := dir + "/state.db"
	s1, err := OpenStateDB(path)
	if err != nil {
		t.Fatalf("first OpenStateDB: %v", err)
	}
	if c, ok := s1.(interface{ Close() error }); ok {
		c.Close()
	}
	s2, err := OpenStateDB(path)
	if err != nil {
		t.Fatalf("second OpenStateDB: %v", err)
	}
	ctx := context.Background()
	if err := s2.Create(ctx, ports.Resource{Kind: ports.KindConfig, Name: "main", Data: []byte(`{}`)}); err != nil {
		t.Fatalf("Create after reopen: %v", err)
	}
	if c, ok := s2.(interface{ Close() error }); ok {
		c.Close()
	}
}

// compact renders JSON in a canonical key-sorted form for comparisons.
func compact(b []byte) string {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		panic(err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(out)
}
