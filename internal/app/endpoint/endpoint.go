// Package endpoint owns ingress listeners: each Endpoint (resource or the
// synthesized default) is one TCP listener that demultiplexes by first
// byte (0x05 → SOCKS5, else HTTP) and then by capability flags
// (spec/resources/endpoint.md).
//
//	accept ──peek 1B──┬─ 0x05 ──► socks5 chanListener → socks5.Server
//	                  └─ HTTP ───► http.Server(per-endpoint mux)
//	                                ├─ /healthz          (always)
//	                                ├─ /api, /ui         (allowManagement, else 404)
//	                                ├─ CONNECT|abs-URI   → forward (allowProxy+allowHTTPForward)
//	                                └─ other             → reverse (allowProxy+allowHTTPReverse)
//
// Proxy traffic on a disabled capability gets 403
// ENDPOINT_CAPABILITY_DISABLED (spec/errors.md §1.5); management paths on
// a management-disabled endpoint get 404 (they must not exist there).
//
// The Manager applies/deletes endpoints at runtime (control-plane hooks);
// each Apply replaces the listener atomically from the callers' view: a
// listen failure keeps the old listener and is surfaced via Status.
package endpoint

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	forward "github.com/xiarsx/egress/internal/adapters/http"
	"github.com/xiarsx/egress/internal/adapters/reverse"
	"github.com/xiarsx/egress/internal/adapters/socks5"
	"github.com/xiarsx/egress/internal/app/pipeline"
)

// Config is one endpoint's static configuration (from the Endpoint
// resource spec, or the synthesized env default).
type Config struct {
	Name string
	Addr string // host:port to bind

	AllowManagement      bool
	AllowProxy           bool
	RequireProxyAuthInfo bool
	AllowHTTPForward     bool
	AllowHTTPReverse     bool
	AllowSOCKS5          bool
}

// Status is the runtime state of one endpoint (control-plane reads).
type Status struct {
	Listening bool
	LastError string
}

// Manager owns every listener. It is safe for concurrent use.
type Manager struct {
	pipe   *pipeline.Pipeline
	apiMux http.Handler // control plane + WebUI, mounted when AllowManagement

	mu        sync.Mutex
	listeners map[string]*listener
}

// New builds a Manager. apiMux serves the control-plane API and WebUI for
// management-enabled endpoints (it must already contain /api/… and /ui).
func New(pipe *pipeline.Pipeline, apiMux http.Handler) *Manager {
	return &Manager{
		pipe:      pipe,
		apiMux:    apiMux,
		listeners: make(map[string]*listener),
	}
}

// listener is one bound endpoint.
type listener struct {
	cfg     Config
	ln      net.Listener
	httpLn  *chanListener
	socksLn *chanListener
	httpSrv *http.Server
	cancel  context.CancelFunc
	lastErr string
}

// Apply opens or replaces the listener for cfg. A bind failure keeps any
// previous listener for the same name, records LastError (visible via
// Status) and returns the error — the declarative write already succeeded,
// so failing closed is not an option; the operator sees the error.
func (m *Manager) Apply(cfg Config) error {
	l := &listener{cfg: cfg}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		cancel()
		// Keep the previous listener serving; only record the failure.
		m.mu.Lock()
		if old, ok := m.listeners[cfg.Name]; ok {
			old.lastErr = err.Error()
		} else {
			m.listeners[cfg.Name] = &listener{cfg: cfg, cancel: cancel, lastErr: err.Error()}
		}
		m.mu.Unlock()
		return fmt.Errorf("endpoint %s: listen %s: %w", cfg.Name, cfg.Addr, err)
	}
	l.ln = ln

	// Demux plumbing: HTTP conns and SOCKS5 conns are fanned out to two
	// in-memory listeners feeding the two servers.
	l.httpLn = newChanListener()
	l.socksLn = newChanListener()
	l.httpSrv = &http.Server{Handler: m.buildMux(cfg), ReadHeaderTimeout: 10 * time.Second}

	socksSrv := socks5.New(m.pipe)
	socksSrv.RequireAuthInfo = cfg.RequireProxyAuthInfo

	go l.httpSrv.Serve(l.httpLn)
	go socksSrv.Serve(l.socksLn)
	go l.acceptLoop(ctx)

	m.mu.Lock()
	old := m.listeners[cfg.Name]
	m.listeners[cfg.Name] = l
	m.mu.Unlock()
	if old != nil {
		old.close()
	}
	return nil
}

// Delete closes and forgets the endpoint's listener.
func (m *Manager) Delete(name string) {
	m.mu.Lock()
	l, ok := m.listeners[name]
	delete(m.listeners, name)
	m.mu.Unlock()
	if ok {
		l.close()
	}
}

// Status reports the runtime state of one endpoint.
func (m *Manager) Status(name string) (Status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.listeners[name]
	if !ok {
		return Status{}, false
	}
	return Status{Listening: l.ln != nil, LastError: l.lastErr}, true
}

// Close shuts every listener down (assembly-root shutdown order: before
// the runtime stops).
func (m *Manager) Close() {
	m.mu.Lock()
	ls := make([]*listener, 0, len(m.listeners))
	for _, l := range m.listeners {
		ls = append(ls, l)
	}
	m.listeners = map[string]*listener{}
	m.mu.Unlock()
	for _, l := range ls {
		l.close()
	}
}

// close stops the accept loop and both servers. A listener whose bind
// failed has no demux plumbing — close only what exists.
func (l *listener) close() {
	l.cancel()
	if l.ln != nil {
		_ = l.ln.Close()
	}
	if l.httpLn != nil {
		l.httpLn.close()
	}
	if l.socksLn != nil {
		l.socksLn.close()
	}
	if l.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = l.httpSrv.Shutdown(ctx)
		cancel()
	}
}

// acceptLoop peeks the first byte of every connection and routes it to
// the SOCKS5 or HTTP side according to the capability flags.
func (l *listener) acceptLoop(ctx context.Context) {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go l.dispatch(ctx, c)
	}
}

// sniffTimeout bounds the first-byte peek (slowloris guard).
const sniffTimeout = 15 * time.Second

func (l *listener) dispatch(ctx context.Context, conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(sniffTimeout))
	br := bufio.NewReader(conn)
	b, err := br.Peek(1)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		_ = conn.Close()
		return
	}
	wrapped := &peekedConn{Conn: conn, r: br}

	if b[0] == 0x05 {
		if l.cfg.AllowProxy && l.cfg.AllowSOCKS5 {
			l.socksLn.deliver(wrapped)
			return
		}
		// SOCKS5 disabled: the protocol has no error channel; close.
		_ = conn.Close()
		return
	}
	l.httpLn.deliver(wrapped)
}

// buildMux composes the per-endpoint HTTP handler from the capability
// flags. Each Apply builds fresh adapters so RequireProxyAuthInfo is
// per-endpoint.
func (m *Manager) buildMux(cfg Config) http.Handler {
	fwd := forward.New(m.pipe)
	fwd.RequireAuthInfo = cfg.RequireProxyAuthInfo
	rev := reverse.New(m.pipe)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/healthz":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		case path == "/api" || len(path) > 4 && path[:4] == "/api" ||
			path == "/ui" || len(path) > 3 && path[:3] == "/ui":
			if !cfg.AllowManagement {
				http.NotFound(w, r)
				return
			}
			m.apiMux.ServeHTTP(w, r)
			return
		}

		isForward := r.Method == http.MethodConnect || r.URL.IsAbs()
		if isForward {
			if !cfg.AllowProxy || !cfg.AllowHTTPForward {
				pipeline.WriteHTTPError(w, &pipeline.ProxyError{
					Code: pipeline.CodeCapabilityDisabled,
					Msg:  "endpoint does not allow HTTP forward",
				})
				return
			}
			fwd.ServeHTTP(w, r)
			return
		}
		if !cfg.AllowProxy || !cfg.AllowHTTPReverse {
			pipeline.WriteHTTPError(w, &pipeline.ProxyError{
				Code: pipeline.CodeCapabilityDisabled,
				Msg:  "endpoint does not allow HTTP reverse",
			})
			return
		}
		rev.ServeHTTP(w, r)
	})
}

// peekedConn replays the bytes Peek buffered before reading live.
type peekedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// chanListener is an in-memory net.Listener fed by the demux loop.
type chanListener struct {
	ch     chan net.Conn
	done   chan struct{}
	once   sync.Once
	closed chan struct{}
}

func newChanListener() *chanListener {
	return &chanListener{
		ch:     make(chan net.Conn, 128),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
}

func (l *chanListener) deliver(c net.Conn) {
	select {
	case l.ch <- c:
	case <-l.done:
		_ = c.Close()
	default:
		_ = c.Close() // server stalled; shed
	}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, errors.New("listener closed")
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *chanListener) close() { _ = l.Close() }

func (l *chanListener) Addr() net.Addr { return dummyAddr("chan-listener") }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }
