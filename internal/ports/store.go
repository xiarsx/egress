// Package ports declares the interfaces between the kernel and the
// outside world. It contains zero implementations: adapters provide them,
// the app layer consumes them. It never imports internal/ packages.
package ports

import (
	"context"
	"errors"
)

// Kind identifies a declarative resource family persisted in state.db
// (design §2.1, §9.1): the user-declared layer that never participates in
// hot-path computation.
type Kind string

const (
	KindPlatform       Kind = "Platform"
	KindSource         Kind = "Source"
	KindEndpoint       Kind = "Endpoint"
	KindIdentityPolicy Kind = "IdentityPolicy"
	KindRecipe         Kind = "Recipe"
	KindConfig         Kind = "config"
)

// Sentinel errors returned by Store. Callers use errors.Is.
var (
	// ErrNotFound is returned when kind+name does not exist.
	ErrNotFound = errors.New("ports: resource not found")
	// ErrAlreadyExists is returned by Create when kind+name already exists.
	ErrAlreadyExists = errors.New("ports: resource already exists")
	// ErrInvalid is returned when the resource fails validation
	// (empty kind/name, malformed payload).
	ErrInvalid = errors.New("ports: invalid resource")
	// ErrConflict is returned by Update when the resource changed since
	// the caller read it (stale Version).
	ErrConflict = errors.New("ports: resource version conflict")
)

// Resource is a single declarative resource. Data carries the opaque,
// versioned payload (JSON per spec/resources/); the store treats it as
// bytes and never interprets it.
type Resource struct {
	Kind    Kind
	Name    string
	Data    []byte
	Version int64 // monotonically increasing; assigned by the store
}

// ListOptions carries pagination for List (design §14:
// {items,total,limit,offset}).
type ListOptions struct {
	Limit  int
	Offset int
}

// Page is one List result page.
type Page struct {
	Items  []Resource
	Total  int
	Limit  int
	Offset int
}

// Store is the strongly-consistent resource store (state.db semantics):
// a returned success means the write is committed (design §9.1).
// It is the only writer of declarative resources; the data plane never
// touches it on the hot path.
type Store interface {
	// Get returns the resource addressed by kind+name.
	Get(ctx context.Context, kind Kind, name string) (Resource, error)

	// List returns one page of resources of a kind, ordered by name.
	List(ctx context.Context, kind Kind, opts ListOptions) (Page, error)

	// Create inserts a new resource. The store assigns Version.
	Create(ctx context.Context, r Resource) error

	// Update replaces the whole resource. Implementations must reject a
	// stale Version with ErrConflict.
	Update(ctx context.Context, r Resource) error

	// Delete removes the resource addressed by kind+name.
	Delete(ctx context.Context, kind Kind, name string) error

	// Patch applies a JSON merge-patch to the resource's Data and
	// returns the updated resource.
	Patch(ctx context.Context, kind Kind, name string, patch []byte) (Resource, error)
}
