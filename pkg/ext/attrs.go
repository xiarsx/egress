package ext

import (
	"context"
	"net/netip"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// AttributeProvider enriches nodes with extra attributes
// (spec/ext-abi.md §2): ASN, IP type, reputation scores, ...
//
// Stability: experimental.
//
// Called on the cold path by the scheduler; failures never block routing,
// they only shrink the attrs filter dimension. Attribute key namespace:
// v0 convention is "ext.<provider>.<key>" (spec/ext-abi.md §2); a formal
// registry mechanism is still TODO (design §20).
type AttributeProvider interface {
	// Name is the provider key used at registration.
	Name() string
	// Enrich returns attribute key/value pairs for the node's egress IP.
	Enrich(ctx context.Context, egressIP netip.Addr, node pknode.Hash) map[pknode.AttrKey]pknode.AttrValue
}
