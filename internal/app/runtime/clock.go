package runtime

import (
	"time"

	"github.com/xiarsx/egress/internal/ports"
)

// SystemClock is the production ports.Clock: the wall clock. Tests inject a
// fake clock instead.
type SystemClock struct{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// Compile-time check that SystemClock satisfies the contract.
var _ ports.Clock = SystemClock{}
