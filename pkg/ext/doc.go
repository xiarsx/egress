// Package ext is the egress extension SDK v0 (design §11,
// spec/ext-abi.md). Extensions are community-owned; the kernel stays
// closed. Extensions ship as compile-time modules (Caddy/xcaddy model):
// a module calls the Register* functions from init() and is linked into
// a custom binary via `egress build --with <module>`.
//
// # Stability levels
//
// Every interface carries a stability level (spec/ext-abi.md §5):
//
//   - experimental: the signature may change in any minor release; the
//     change is noted in the CHANGELOG.
//   - stable: breaking changes require an RFC and cross a major version;
//     only backwards-compatible additions are allowed within a major.
//
// In v0 ALL extension points are experimental. The roadmap freezes
// Selector and IdentityResolver at M6.
//
// # Hot-path discipline (design P2)
//
// Selector and IdentityResolver run on the hot path and MUST all satisfy:
//
//  1. Pure functions: no I/O, no network, no disk reads, no global locks,
//     no goroutine spawning.
//  2. Zero allocation (Selector): admission to the built release is gated
//     by a benchmark threshold.
//  3. Bounded time: O(candidates) or better; no regex compilation, map
//     growth or other hidden costs inside the call — precompile on the
//     cold path and close over the result.
//  4. IdentityResolver reads request metadata only.
//
// Violations must not be merged into the built release.
package ext
