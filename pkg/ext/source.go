package ext

import (
	"context"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// SourceSpec describes one source declaration handed to a SourceProvider.
type SourceSpec struct {
	Name           string
	Kind           string // "remote" | "local" | a custom provider key
	URL            string
	Content        string // inline content for "local"
	UpdateInterval time.Duration
}

// SourceProvider fetches node snapshots from a new source type
// (spec/ext-abi.md §2).
//
// Stability: experimental.
//
// Called on the cold path: on the source refresh interval and on manual
// refresh. Unparseable entries should be skipped and counted, not fail
// the whole fetch.
type SourceProvider interface {
	// Name is the provider key used at registration.
	Name() string
	// Fetch returns the node's snapshots for spec.
	Fetch(ctx context.Context, spec SourceSpec) ([]pknode.NodeSnapshot, error)
}
