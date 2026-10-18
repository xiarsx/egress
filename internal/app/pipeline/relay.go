package pipeline

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// --- thin adapters from Deps callbacks to pick's read-only interfaces ---

type nodesFunc func(pknode.Hash) (pknode.NodeSnapshot, bool)

func (f nodesFunc) Snapshot(h pknode.Hash) (pknode.NodeSnapshot, bool) { return f(h) }

type ipLoadFunc func(netip.Addr) int64

func (f ipLoadFunc) Get(ip netip.Addr) int64 { return f(ip) }

type latencyFunc func(pknode.Hash, string) (time.Duration, bool)

func (f latencyFunc) Latency(h pknode.Hash, domain string) (time.Duration, bool) {
	return f(h, domain)
}

// --- relay ---

// relayConns pumps both directions until both sides are done. It returns
// downstream/upstream byte counts and whether the relay was clean.
// Connectivity verdict (design §6.1, task P3): dial success already
// counts as ok; a mid-relay RST or other non-benign copy error flips the
// verdict to fail, while EOF / closed connections / a client reset after
// the response started are benign shutdown artifacts.
func relayConns(client, upstream net.Conn) (downN, upN int64, clean bool) {
	var wg sync.WaitGroup
	var upErr, downErr error
	var upCount, downCount int64
	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			_ = client.Close()
			_ = upstream.Close()
		})
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, err := io.Copy(upstream, client)
		upCount, upErr = n, err
		// Benign end → half-close the write side so the peer sees EOF;
		// when half-close is unsupported (e.g. in-memory pipes) or the
		// copy failed, close both sides to unblock the other direction.
		if !isBenignCopyError(err) || closeWrite(upstream) != nil {
			closeBoth()
		}
	}()
	go func() {
		defer wg.Done()
		n, err := io.Copy(client, upstream)
		downCount, downErr = n, err
		if !isBenignCopyError(err) || closeWrite(client) != nil {
			closeBoth()
		}
	}()
	wg.Wait()

	upBenign := isBenignCopyError(upErr)
	downBenign := isBenignCopyError(downErr)
	// A client-side TCP reset after the upstream response has already
	// started is a shutdown artifact, not an upstream failure.
	if !upBenign && downCount > 0 && isResetError(upErr) {
		upBenign = true
	}
	return downCount, upCount, upBenign && downBenign
}

// isBenignCopyError mirrors the reference tunnel semantics: EOF, closed
// connections and context cancellation are normal relay endings.
func isBenignCopyError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "closed network connection")
}

// isResetError reports TCP resets / aborts.
func isResetError(err error) bool {
	if err == nil {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		if errno == syscall.ECONNRESET || errno == syscall.ECONNABORTED {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "software caused connection abort")
}

// closeWrite half-closes the write side when supported.
func closeWrite(c net.Conn) error {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errHalfCloseUnsupported
}

var errHalfCloseUnsupported = errors.New("pipeline: half-close unsupported")

// writeFull writes the whole buffer, returning bytes written on error.
func writeFull(c net.Conn, b []byte) (int, error) {
	total := 0
	for total < len(b) {
		n, err := c.Write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// isTimeoutErr reports dial/handshake/write timeouts (OS or context
// deadline), mirroring the reference classify*Error helpers.
func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	return os.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded)
}

// tlsTimingConn measures the TLS handshake as ClientHello-write → first
// read (design §6.2), without parsing TLS records.
type tlsTimingConn struct {
	net.Conn
	writeT atomic.Int64 // unixnano of first successful write
	firstR atomic.Int64 // handshake nanos once measured, -1 armed
	done   atomic.Bool
}

func (c *tlsTimingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 && err == nil {
		c.writeT.CompareAndSwap(0, time.Now().UnixNano())
	}
	return n, err
}

func (c *tlsTimingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && err == nil && !c.done.Swap(true) {
		if t0 := c.writeT.Load(); t0 > 0 {
			c.firstR.Store(time.Now().UnixNano() - t0)
		}
	}
	return n, err
}

// latency returns the measured ClientHello → first-read duration, or 0 if
// no measurement completed.
func (c *tlsTimingConn) latency() time.Duration {
	if v := c.firstR.Load(); v > 0 {
		return time.Duration(v)
	}
	return 0
}
