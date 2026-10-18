package ports

import "time"

// Clock abstracts wall-clock reads so pure logic and tests can inject a
// fake. Production code uses the system clock; the kernel itself never
// reads time.Now directly on paths covered by conformance vectors.
type Clock interface {
	Now() time.Time
}
