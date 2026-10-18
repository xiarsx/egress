package wiring

import (
	"github.com/xiarsx/egress/internal/adapters/source"
	"github.com/xiarsx/egress/internal/app/runtime"
	"github.com/xiarsx/egress/internal/ports"
)

// Compile-time check: SyncerAdapter is the surface runtime needs.
var _ runtime.Syncer = (*SyncerAdapter)(nil)

// SyncerAdapter adapts P2's *source.Syncer to the runtime.Syncer
// interface. The only mismatch is RefreshNow: the source syncer reports
// errors (and records them on the source via LastError / recordError),
// while the runtime treats refresh as fire-and-forget — a failed refresh
// keeps the old snapshot by design (design §5.1), so the error is
// intentionally dropped here after being recorded there.
type SyncerAdapter struct {
	Inner *source.Syncer
}

// AddSource registers (or re-registers) a source declaration.
func (a *SyncerAdapter) AddSource(spec ports.SourceSpec) { a.Inner.AddSource(spec) }

// RemoveSource stops and forgets a source.
func (a *SyncerAdapter) RemoveSource(name string) { a.Inner.RemoveSource(name) }

// RefreshNow triggers an immediate refresh of a source.
func (a *SyncerAdapter) RefreshNow(name string) { _ = a.Inner.RefreshNow(name) }
