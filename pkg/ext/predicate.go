package ext

import (
	pknode "github.com/xiarsx/egress/pkg/node"
)

// Attrs is the attribute map a compiled predicate evaluates.
type Attrs map[pknode.AttrKey]pknode.AttrValue

// PredicateFactory compiles custom filter dimensions
// (spec/ext-abi.md §2).
//
// Stability: experimental.
//
// Compile runs ONCE on the cold path (platform config change); the hot
// path only executes the returned closure, so Compile must do all the
// expensive work (regex compilation, parsing) up front. Undefined
// attribute keys: in/match/gte/lte count as no-match, notIn/notMatch
// count as pass.
type PredicateFactory interface {
	// Name is the factory key used at registration.
	Name() string
	// Compile builds the predicate closure from its arguments.
	Compile(args map[string]string) (func(Attrs) bool, error)
}
