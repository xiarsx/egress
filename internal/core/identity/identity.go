// Package identity resolves request identities: it splits the V1 wire
// credential into platform/account/token (spec/identity.md §2) and runs
// the IdentityPolicy chain to determine the final account.
// Pure logic: no I/O, no clock reads.
package identity

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var (
	// ErrInvalidToken is returned by ValidateToken when the token contains
	// forbidden characters or whitespace.
	ErrInvalidToken = errors.New("identity: token contains forbidden characters")
	// ErrReservedToken is returned by ValidateToken when the token takes a
	// reserved path-segment value.
	ErrReservedToken = errors.New("identity: token is a reserved value")
)

// DefaultPlatformID is used when the identity carries no platform.
const DefaultPlatformID = "Default"

// Identity is the resolved request identity (design §4.2).
type Identity struct {
	PlatformID string
	Account    string
	Token      string
	// Random is true when Account is empty: route randomly inside the
	// platform and do not create a lease.
	Random bool
}

// Parse splits a V1 credential "<identity>:<token>" (spec/identity.md §2):
//
//  1. cut the Token at the rightmost ':' (the account itself may contain
//     ':'); without a ':' the whole input is the identity and Token is "".
//  2. split the identity at the first '.' or ':' into Platform / Account.
//
// An empty Platform becomes DefaultPlatformID; an empty Account sets
// Random (random routing, no lease). This function only splits — token
// policy is enforced separately by ValidateToken, never here, so values
// like token "." parse successfully per the conformance vectors.
//
// Parse is infallible on string input; the error return keeps the door
// open for future wire-format rejections without changing call sites.
func Parse(raw string) (Identity, error) {
	identity, token := splitCredential(raw)
	platform, account := SplitIdentity(identity)
	if platform == "" {
		platform = DefaultPlatformID
	}
	return Identity{
		PlatformID: platform,
		Account:    account,
		Token:      token,
		Random:     account == "",
	}, nil
}

// splitCredential cuts the token at the rightmost ':'.
func splitCredential(credential string) (identity, token string) {
	if idx := strings.LastIndexByte(credential, ':'); idx >= 0 {
		return credential[:idx], credential[idx+1:]
	}
	return credential, ""
}

// SplitIdentity splits a bare identity segment at the first '.' or ':'
// (used by the reverse-proxy path after path-segment decoding, and by
// SOCKS5 where username and password arrive separately).
func SplitIdentity(identity string) (platform, account string) {
	dot := strings.IndexByte(identity, '.')
	colon := strings.IndexByte(identity, ':')
	switch {
	case dot < 0 && colon < 0:
		return identity, ""
	case dot < 0:
		return identity[:colon], identity[colon+1:]
	case colon < 0:
		return identity[:dot], identity[dot+1:]
	case dot < colon:
		return identity[:dot], identity[dot+1:]
	default:
		return identity[:colon], identity[colon+1:]
	}
}

// reservedTokens are URL path-segment values that must never be tokens
// (they would collide with control-plane routes).
var reservedTokens = map[string]struct{}{
	"api":     {},
	"healthz": {},
	"ui":      {},
}

// tokenForbiddenChars mirrors the reference validator: none of these may
// appear in a token. Note ':' is included — by construction the split
// above never yields a token containing ':', but tokens arriving from
// other paths (config, API) are still rejected.
const tokenForbiddenChars = ".:|/\\@?#%~"

// ValidateToken enforces the token rules (design §13, spec/identity.md §1):
// the token must not contain ".:|/\@?#%~" or whitespace, and must not be
// one of the reserved values api/healthz/ui.
func ValidateToken(token string) error {
	if _, reserved := reservedTokens[token]; reserved {
		return fmt.Errorf("%w: %q", ErrReservedToken, token)
	}
	if strings.ContainsAny(token, tokenForbiddenChars) {
		return fmt.Errorf("%w: must not contain any of %q", ErrInvalidToken, tokenForbiddenChars)
	}
	for _, r := range token {
		if unicode.IsSpace(r) {
			return fmt.Errorf("%w: must not contain whitespace", ErrInvalidToken)
		}
	}
	return nil
}
