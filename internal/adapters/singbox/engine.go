package singbox

// engine.go — the embedded sing-box outbound engine.
//
// The parser layer emits canonical sing-box-schema JSON for every node
// (refparse.go); this engine builds real, dialable outbounds from that
// JSON via the upstream registry (include.OutboundRegistry) — one
// implementation, every protocol sing-box supports: vmess, vless,
// shadowsocks, trojan, socks, http, direct, … QUIC protocols
// (hysteria2, tuic, hysteria) are compiled in with `-tags with_quic`.
//
// Construction context (what protocol outbounds actually read):
//
//	ctx     — include.Context() + NetworkManager (v1.14 dialers
//	          dereference it unconditionally) + DNSTransportManager with
//	          one "local" DNS transport (domain server addresses resolve
//	          via the system resolver)
//	router  — unused by protocol outbounds (only groups need it); nil
//	logger  — NOP factory; dial errors surface via the returned error
//
// Unknown types fail closed: ErrUnsupportedOutbound, never a silent
// direct dial (design §5.1 fail-closed rule).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	sboutbound "github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/route"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

// engine is the process-wide construction context for embedded outbounds.
type engine struct {
	baseCtx  context.Context
	registry *sboutbound.Registry
	logger   log.ContextLogger
}

func newEngine() (*engine, error) {
	logger := log.NewNOPFactory().NewLogger("outbound")
	baseCtx := include.Context(context.Background())

	networkMgr, err := route.NewNetworkManager(baseCtx, logger, option.RouteOptions{}, option.DNSOptions{})
	if err != nil {
		return nil, fmt.Errorf("singbox: network manager: %w", err)
	}
	ctx := service.ContextWith[adapter.NetworkManager](baseCtx, networkMgr)

	endpointMgr := endpoint.NewManager(include.EndpointRegistry())
	outboundMgr := sboutbound.NewManager(include.OutboundRegistry(), endpointMgr, "")
	dnsTransportMgr := dns.NewTransportManager(include.DNSTransportRegistry(), outboundMgr, "")
	if err := dnsTransportMgr.Create(ctx, logger, "local", "local", &option.LocalDNSServerOptions{}); err != nil {
		return nil, fmt.Errorf("singbox: local dns transport: %w", err)
	}
	ctx = service.ContextWith[adapter.DNSTransportManager](ctx, dnsTransportMgr)
	ctx = service.ContextWith[adapter.OutboundManager](ctx, outboundMgr)
	ctx = service.ContextWith[adapter.EndpointManager](ctx, endpointMgr)

	return &engine{
		baseCtx:  ctx,
		registry: include.OutboundRegistry(),
		logger:   logger,
	}, nil
}

var (
	engineOnce     sync.Once
	engineInstance *engine
	engineErr      error
)

// sharedEngine returns the process-wide engine (registries are immutable
// after construction; the ctx is read-only).
func sharedEngine() (*engine, error) {
	engineOnce.Do(func() { engineInstance, engineErr = newEngine() })
	return engineInstance, engineErr
}

// build constructs a dialable outbound from canonical sing-box JSON.
func (e *engine) build(raw []byte) (dialOutbound, error) {
	var ob option.Outbound
	if err := ob.UnmarshalJSONContext(e.baseCtx, raw); err != nil {
		if isUnknownTypeErr(err) {
			return nil, fmt.Errorf("%w: %q (%v)", ErrUnsupportedOutbound, typeOf(raw), err)
		}
		return nil, fmt.Errorf("singbox: decode outbound options: %w", err)
	}
	created, err := e.registry.CreateOutbound(e.baseCtx, nil, e.logger, "out", ob.Type, ob.Options)
	if err != nil {
		if isUnknownTypeErr(err) {
			return nil, fmt.Errorf("%w: %q (%v)", ErrUnsupportedOutbound, ob.Type, err)
		}
		return nil, fmt.Errorf("singbox: build %q outbound: %w", ob.Type, err)
	}
	return &engineOutbound{ob: created}, nil
}

// isUnknownTypeErr detects the registry's "unknown outbound type"
// rejection and the build-tag stubs ("… is not included in this build,
// rebuild with -tags …") — upstream wraps both in plain error chains,
// no sentinels.
func isUnknownTypeErr(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "unknown outbound type") ||
		strings.Contains(msg, "is not included in this build")
}

// typeOf extracts the outbound type for the error message.
func typeOf(raw []byte) string {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &head) != nil || head.Type == "" {
		return "<undecodable>"
	}
	return head.Type
}

// engineOutbound adapts adapter.Outbound to the dialOutbound interface.
type engineOutbound struct {
	ob outboundAdapter
}

// outboundAdapter is the slice of adapter.Outbound we use (N.Dialer plus
// optional Close); declared locally so tests can stub it.
type outboundAdapter interface {
	DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
}

func (o *engineOutbound) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return o.ob.DialContext(ctx, network, M.ParseSocksaddr(address))
}

func (o *engineOutbound) Close() error {
	if c, ok := o.ob.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
