package ports

import (
	"context"
	"net"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// Dialer opens direct outbound connections: bypass traffic, subscription
// fetchers and active probes that must not recurse through the proxy
// pipeline. Implementations must honor ctx cancellation and deadlines;
// callers wrap the returned conn for TLS-handshake timing where needed.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// NodeDialer dials through the outbound built for a specific node — i.e.
// the node's proxy chain (vmess/vless/trojan/ss/hysteria2/...), mirroring
// the reference outbound manager's behavior:
//
//   - the outbound instance is built lazily from the node's raw options on
//     first use and cached afterwards;
//   - concurrent first-use builds are collapsed so only one instance wins;
//   - a node without a built outbound is a dial-time error, never a
//     silent direct dial (fail-closed);
//   - removing a node closes and drops its outbound.
//
// Like Dialer, implementations honor ctx; the pipeline wraps the conn for
// TLS timing and emits Traffic signals on completion.
type NodeDialer interface {
	DialContext(ctx context.Context, node pknode.Hash, network, address string) (net.Conn, error)
}
