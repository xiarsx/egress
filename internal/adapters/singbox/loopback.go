package singbox

// loopback.go provides test support for the black-box e2e suite: a genuine
// vmess inbound whose router forwards every accepted connection to one
// fixed local address, so tests exercise the real protocol path without
// internet access. Not used by production wiring.

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/vmess"
	N "github.com/sagernet/sing/common/network"
)

// NewVMessInbound starts a vmess inbound on a fresh 127.0.0.1 port; every
// accepted connection is dialed through to forwardTo regardless of the
// target the client requested. Returns the listen address.
func NewVMessInbound(uuid, forwardTo string) (addr string, closeFn func() error, err error) {
	e, err := sharedEngine()
	if err != nil {
		return "", nil, err
	}
	serverIB, err := vmess.NewInbound(e.baseCtx, fixedRouter{forwardTo: forwardTo}, e.logger, "e2e-in",
		option.VMessInboundOptions{Users: []option.VMessUser{{UUID: uuid}}})
	if err != nil {
		return "", nil, err
	}
	server := serverIB.(interface {
		NewConnection(context.Context, net.Conn, adapter.InboundContext, N.CloseHandlerFunc)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go server.NewConnection(context.Background(), c, adapter.InboundContext{}, nil)
		}
	}()
	return ln.Addr().String(), ln.Close, nil
}

// fixedRouter implements adapter.Router by forwarding every connection to
// one fixed address (the local echo/trace server in e2e tests).
type fixedRouter struct{ forwardTo string }

func (fixedRouter) Start(adapter.StartStage, *adapter.Scope) error { return nil }
func (fixedRouter) PreMatch(adapter.InboundContext, []byte) adapter.PreMatchResult {
	return adapter.PreMatchResult{}
}
func (fixedRouter) HijackDNSPacket(context.Context, []byte, N.PacketWriter, adapter.InboundContext) {
}
func (fixedRouter) NeedFindProcess() bool                      { return false }
func (fixedRouter) NeedFindNeighbor() bool                     { return false }
func (fixedRouter) NeighborResolver() adapter.NeighborResolver { return nil }
func (fixedRouter) Rules() []adapter.Rule                      { return nil }
func (fixedRouter) RuleSet(string) (adapter.RuleSet, bool)     { return nil, false }
func (fixedRouter) AppendTracker(adapter.ConnectionTracker)    {}
func (fixedRouter) ResetNetwork()                              {}

func (r fixedRouter) RouteConnection(ctx context.Context, conn net.Conn, _ adapter.InboundContext) error {
	defer conn.Close()
	return r.pump(ctx, conn)
}

func (r fixedRouter) RouteConnectionEx(ctx context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go func() {
		defer conn.Close()
		_ = r.pump(ctx, conn)
		N.CloseOnHandshakeFailure(conn, onClose, nil)
	}()
}

func (fixedRouter) RoutePacketConnection(context.Context, N.PacketConn, adapter.InboundContext) error {
	return errors.New("no udp in loopback")
}

func (fixedRouter) RoutePacketConnectionEx(context.Context, N.PacketConn, adapter.InboundContext, N.CloseHandlerFunc) {
}

// pump dials the fixed target and copies both directions.
func (r fixedRouter) pump(ctx context.Context, client net.Conn) error {
	up, err := (&net.Dialer{}).DialContext(ctx, "tcp", r.forwardTo)
	if err != nil {
		return err
	}
	defer up.Close()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, up); done <- struct{}{} }()
	<-done
	return nil
}
