package ext

// RequestMeta is the read-only request metadata an IdentityResolver may
// inspect (spec/ext-abi.md §2). It deliberately carries no body, no
// connection handle and no I/O affordance.
type RequestMeta struct {
	// Target is "host:port" of the requested destination.
	Target string
	// Headers are the request headers (HTTP) or equivalent metadata.
	Headers map[string]string
	// WireAccount is the account parsed from the line protocol
	// ("Platform.Account"), "" when the request carries none.
	WireAccount string
	// IsHTTP distinguishes HTTP-ish ingress from SOCKS5.
	IsHTTP bool
}

// IdentityResolver maps a request to an account (spec/ext-abi.md §2):
// cookie, JWT sub, header rules, ...
//
// Stability: experimental (M6 promotes to stable).
//
// HOT PATH — the discipline in doc.go applies: pure function, request
// metadata only, no I/O, no blocking, no body reads. Returning ("", false)
// means "not handled", and the chain continues to the next resolver.
type IdentityResolver interface {
	// Name is the resolver key used at registration.
	Name() string
	// Resolve returns the account and true, or ("", false) to pass.
	Resolve(meta RequestMeta) (string, bool)
}
