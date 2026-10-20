// Package boot is the composition root of the egress gateway
// (design §16, §9.2), exported so that custom builds
// (`egress build --with <module>`) can link extension registrations into
// the binary. It is assembly, not API: no stability promise.
//
// It does no business logic: it builds the config, opens the databases,
// wires the groups together, and owns the single sequencing constraint
// the design keeps — listeners open LAST, after everything else is
// running (design §9.2 step 6).
//
// Startup order:
//
//  1. config.Load (EGRESS_*, failure is fatal)
//  2. state.db → control.Service → EnsureDefaultPlatform + ApplyFile
//  3. cache.db / metrics.db / request-log DBs
//  4. source registry + sing-box dialer + fetcher → source syncer
//  5. runtime.New → runtime.Run (design §9.2 steps 1–5)
//  6. wiring: latency tracker, bus sinks (metrics/request-log/ext),
//     pipeline, attribute/prober refresh
//  7. listeners LAST: the endpoint Manager (per-endpoint first-byte
//     demux, spec/resources/endpoint.md) + the env-defined default
//
// Shutdown (SIGINT/SIGTERM): stop listeners → stop runtime → flush
// cache/metrics/request-log → close databases.
package boot

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xiarsx/egress/internal/adapters/geoip"
	"github.com/xiarsx/egress/internal/adapters/singbox"
	"github.com/xiarsx/egress/internal/adapters/source"
	"github.com/xiarsx/egress/internal/adapters/sqlite"
	"github.com/xiarsx/egress/internal/adapters/webui"
	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/app/endpoint"
	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/app/runtime"
	"github.com/xiarsx/egress/internal/app/wiring"
	"github.com/xiarsx/egress/internal/config"
	corenode "github.com/xiarsx/egress/internal/core/node"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/ports"
	"github.com/xiarsx/egress/pkg/ext"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// Main runs the gateway: no args → serve; subcommands dispatch. It is
// the single entry point for both cmd/egress and custom builds.
func Main() int {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "build":
			return runBuild(os.Args[2:])
		case "serve":
			return serve(os.Args[2:])
		}
	}
	return serve(nil)
}

func serve(_ []string) int {
	showVersion := flag.Bool("version", false, "print version and exit")
	configTest := flag.Bool("config-test", false, "validate configuration and exit without starting")
	flag.Parse()

	if *showVersion {
		fmt.Println("egress " + appVersion())
		return 0
	}

	// --- 1. config -------------------------------------------------
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "egress: config: %v\n", err)
		return 1
	}
	if *configTest {
		fmt.Printf("config OK: listen=%s state_dir=%s cache_dir=%s log_dir=%s admin_token=set proxy_token=%s config_file=%s\n",
			cfg.ListenAddr, cfg.StateDir, cfg.CacheDir, cfg.LogDir,
			tokenState(cfg.ProxyToken), orNone(cfg.ConfigFile))
		return 0
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 数据目录不存在则创建（P4 两步可用：启动 → 填订阅，不应因缺目录失败）。
	for _, dir := range []string{cfg.StateDir, cfg.CacheDir, cfg.LogDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fatalf("create dir %s: %v", dir, err)
		}
	}

	// --- 2. state.db + control plane --------------------------------
	store, err := sqlite.OpenStateDB(filepath.Join(cfg.StateDir, "state.db"))
	if err != nil {
		return fatalf("open state.db: %v", err)
	}
	svc := control.NewService(store, control.NopHooks{},
		control.WithAdminToken(cfg.AdminToken),
		control.WithVersion(appVersion()),
		control.WithSystemConfig(bootSystemConfig(cfg)),
	)
	if err := config.EnsureDefaultPlatform(ctx, store); err != nil {
		return fatalf("ensure default platform: %v", err)
	}
	if cfg.ConfigFile != "" {
		if err := config.ApplyFile(ctx, cfg.ConfigFile, store); err != nil {
			return fatalf("apply config file: %v", err)
		}
	}

	// --- 3. cache / metrics / request logs ---------------------------
	cacheDB, err := sqlite.OpenCache(filepath.Join(cfg.CacheDir, "cache.db"))
	if err != nil {
		return fatalf("open cache.db: %v", err)
	}
	metricsDB, err := sqlite.OpenMetrics(filepath.Join(cfg.CacheDir, "metrics.db"))
	if err != nil {
		return fatalf("open metrics.db: %v", err)
	}
	reqLog, err := sqlite.OpenReqLog(cfg.LogDir, 10, 512<<20)
	if err != nil {
		return fatalf("open request logs: %v", err)
	}
	// EGRESS_REQLOG_FLUSH_INTERVAL narrows the crash-consistency window
	// of the batched writer (default 5s).
	reqLog.FlushEvery = cfg.ReqLogFlushInterval

	// --- 4. sources ---------------------------------------------------
	registry := source.NewRegistry()
	// Weak persistence (§8): mirror every published snapshot into the
	// coalescing cache so a restart replays node inventory. The callback
	// runs outside the registry lock; CacheStore is concurrency-safe.
	registry.OnApplySnapshot = func(src string, nodes []pknode.NodeSnapshot) {
		hashes := make([]pknode.Hash, 0, len(nodes))
		for _, n := range nodes {
			hashes = append(hashes, n.Hash)
			cacheDB.Store().UpsertNode(n)
		}
		cacheDB.Store().SetSourceNodes(src, hashes)
	}
	dialer := singbox.NewDialer(registry.Get)
	fetcher := wiring.NewFetcher()
	sched := runtime.NewScheduler()
	// The syncer applies source scoping itself; the parse callback must
	// not pre-scope (see wiring.ParseSubscriptionToSnapshots).
	parse := func(content []byte) ([]pknode.NodeSnapshot, error) {
		return wiring.ParseSubscriptionToSnapshots("", content)
	}
	syncer := &wiring.SyncerAdapter{Inner: source.NewSyncer(registry, fetcher, sched, parse)}

	// --- 5. runtime (reconcile steps 1–5) ------------------------------
	rt := runtime.New(runtime.Deps{
		Store:      store,
		Cache:      cacheDB,
		CacheStore: cacheDB.Store(),
		Registry:   registry,
		Sched:      sched,
		Clock:      runtime.SystemClock{},
		Platforms:  svc,
		Sources:    svc,
		Breaker:    corenode.BreakerConfig{MaxConsecutiveFailures: 3},
		Syncer:     syncer,
		// EGRESS_CACHE_FLUSH_INTERVAL narrows the crash-consistency window
		// (default runtime.CacheFlushInterval = 5m).
		CacheFlushInterval: cfg.CacheFlushInterval,
	})
	svc.SetHooks(rt) // break the control→runtime→control construction cycle
	rtErr := make(chan error, 1)
	go func() { rtErr <- rt.Run(ctx) }()
	// Wait for reconcile steps 1–5 to complete; a failure here is fatal.
	// Listeners (step 6) open only after this returns.
	if err := <-rtErr; err != nil {
		return fatalf("runtime: %v", err)
	}

	// --- 6. wiring -----------------------------------------------------
	bus := rt.Bus()

	tracker := wiring.NewTracker()
	go tracker.Run(ctx, bus.Subscribe())

	go (&wiring.MetricsSink{DB: metricsDB}).Run(ctx, bus.Subscribe())

	activeSessions := &atomic.Int64{}
	realtime := wiring.NewRealtimeTracker()
	go realtime.Run(ctx, bus.Subscribe(), activeSessions, rt.Leases().Count, rt.Deriver().PlatformIDs)

	reqWriter := &wiring.ReqLogWriter{Log: reqLog}

	// ext.Sink fanout (spec/ext-abi.md §2): every signal and every request
	// log also flows to registered sinks. Sinks must never backpressure the
	// data plane, so each Consume call runs behind a per-sink drop-on-full
	// buffer.
	extSinks := wiring.NewSinkFanout(ext.GetSinks())
	go extSinks.RunSignals(ctx, bus.Subscribe())

	geo, err := geoip.Open(cfg.GeoIPDB)
	if err != nil {
		return fatalf("open geoip db: %v", err)
	}
	scheduleAttrRefresh(sched, registry, geo, cacheDB.Store())
	scheduleExtProbers(sched, registry, bus)

	// Fetcher fallback: subscriptions/GeoIP downloads retry through the
	// Default platform when direct egress fails (design §5.1). Wired here,
	// after the data plane exists.
	fetcher.Fallback = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: wiring.DialViaPlatform(
				func() *view.View { return rt.Deriver().View(control.DefaultPlatformID) },
				dialer,
			),
		},
	}

	resolver := wiring.NewResolver(store, svc)
	resolver.Authorities = runtime.DefaultAuthorityDomainList()
	pipe := pipeline.New(pipeline.Deps{
		Resolver:          resolver,
		NodeDialer:        dialer,
		Direct:            &net.Dialer{Timeout: 30 * time.Second},
		Bus:               bus,
		Clock:             runtime.SystemClock{},
		Leases:            rt.Leases(),
		Views:             rt.Deriver().View,
		Nodes:             registry.Get,
		Latency:           tracker.Latency,
		IPLoad:            rt.Leases().IPLoad,
		PickPolicyFor:     resolver.PickPolicyFor,
		ProxyToken:        cfg.ProxyToken,
		PassiveBreakerOff: resolver.PassiveBreakerOff,
		ReqLog:            extSinks.WrapRequestLog(reqWriter.Write),
		ActiveSessions:    activeSessions,
	})

	// --- 7. runtime backend for the control API --------------------------
	// Wire runtime-owned reads/actions (nodes, leases, metrics, logs,
	// geoip, probes) into the control plane; this clears the 501s on
	// those routes.
	prober := &runtime.Prober{
		Dialer:   dialer,
		Bus:      bus,
		Clock:    runtime.SystemClock{},
		Reg:      registry,
		TraceURL: cfg.EgressTraceURL, // empty → built-in Cloudflare trace
	}
	svc.SetRuntimeBackend(&wiring.Backend{
		Reg:       registry,
		Deriver:   rt.Deriver(),
		Leases:    rt.Leases(),
		Syncer:    syncer,
		Platforms: svc,
		Metrics:   metricsDB,
		ReqLog:    reqLog,
		Cache:     cacheDB.Store(),
		Geo:       geo,
		GeoDBPath: cfg.GeoIPDB,
		Prober:    prober,
		Clock:     runtime.SystemClock{},
		Realtime:  realtime,
	})

	// --- probe orchestration (design §6.3, §8) -------------------------
	// Without this, nodes would stay Probing forever and never become
	// routable: the Prober knows how to probe, the ProbeScheduler decides
	// when, per node health state.
	probeSched := runtime.NewProbeScheduler(registry, prober, sched, runtime.SystemClock{})
	probeSched.Start(ctx)
	go func() {
		sub := bus.Subscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-sub:
				probeSched.FeedTraffic(sig)
			}
		}
	}()

	// --- 8. listeners LAST (the only ordering constraint) ---------------
	// Management handler for management-enabled endpoints: control-plane
	// API + WebUI. /healthz and the proxy ingresses are per-endpoint
	// concerns of the endpoint.Manager.
	mgmtMux := http.NewServeMux()
	svc.RegisterRoutes(mgmtMux)
	mgmtMux.Handle("/ui", webui.Handler())
	mgmtMux.Handle("/ui/", webui.Handler())

	epMgr := endpoint.New(pipe, mgmtMux)
	svc.SetEndpointHooks(&endpointHooks{mgr: epMgr, svc: svc}, epMgrStatus(epMgr))

	// The env-defined default endpoint (spec/resources/endpoint.md:
	// synthesized at runtime, never stored, all capabilities).
	if err := epMgr.Apply(endpoint.Config{
		Name:             control.DefaultEndpointName,
		Addr:             cfg.ListenAddr,
		AllowManagement:  true,
		AllowProxy:       true,
		AllowHTTPForward: true,
		AllowHTTPReverse: true,
		AllowSOCKS5:      true,
	}); err != nil {
		return fatalf("default endpoint: %v", err)
	}
	logf("default endpoint listening on %s (http-forward / socks5 / reverse+api / ui)", cfg.ListenAddr)

	// Stored endpoints open after the default one.
	if items, _, err := svc.ListEndpoints(context.Background(), 100000, 0); err == nil {
		for _, e := range items {
			if e.ReadOnly || !e.Enabled {
				continue
			}
			if err := epMgr.Apply(endpointConfigOf(e, cfg.ListenAddr)); err != nil {
				logf("endpoint %s: %v", e.Name, err)
			}
		}
	}

	// --- shutdown -------------------------------------------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logf("received %s, shutting down", sig)

	// 1. stop listeners
	epMgr.Close()
	// 2. stop the runtime (scheduler, bus consumers, sinks)
	cancel()
	// 3. flush everything (design §9: flush before exit is mandatory)
	if err := cacheDB.Store().Flush(); err != nil {
		logf("cache flush: %v", err)
	}
	if err := metricsDB.Flush(); err != nil {
		logf("metrics flush: %v", err)
	}
	// 4. close databases (request-log Close drains its writer loop first)
	if err := reqLog.Close(); err != nil {
		logf("close request logs: %v", err)
	}
	if err := geo.Close(); err != nil {
		logf("close geoip: %v", err)
	}
	if err := metricsDB.Close(); err != nil {
		logf("close metrics.db: %v", err)
	}
	if err := cacheDB.Close(); err != nil {
		logf("close cache.db: %v", err)
	}
	if c, ok := store.(io.Closer); ok {
		if err := c.Close(); err != nil {
			logf("close state.db: %v", err)
		}
	}
	logf("egress stopped")
	return 0
}

// endpointHooks adapts control-plane endpoint change notifications to
// the endpoint.Manager: fetch the declared spec, then apply/close.
type endpointHooks struct {
	mgr *endpoint.Manager
	svc *control.Service
}

func (h *endpointHooks) OnEndpointChanged(name string) {
	e, err := h.svc.GetEndpoint(context.Background(), name)
	if err != nil || e.ReadOnly {
		return
	}
	if !e.Enabled {
		h.mgr.Delete(name)
		return
	}
	if err := h.mgr.Apply(endpointConfigOf(e, "")); err != nil {
		logf("endpoint %s: %v", name, err)
	}
}

func (h *endpointHooks) OnEndpointDeleted(name string) {
	h.mgr.Delete(name)
}

// epMgrStatus adapts endpoint.Manager.Status to the control-plane probe.
func epMgrStatus(mgr *endpoint.Manager) func(string) (control.EndpointStatus, bool) {
	return func(name string) (control.EndpointStatus, bool) {
		st, ok := mgr.Status(name)
		return control.EndpointStatus{Listening: st.Listening, LastError: st.LastError}, ok
	}
}

// endpointConfigOf maps the control-plane Endpoint DTO to the manager's
// listener Config. listenHost supplies the bind host when the endpoint
// only names a port (endpoints bind the same host as the default).
func endpointConfigOf(e control.Endpoint, listenHost string) endpoint.Config {
	host := listenHost
	if host == "" {
		host = "0.0.0.0"
	}
	return endpoint.Config{
		Name:                 e.Name,
		Addr:                 net.JoinHostPort(host, strconv.Itoa(e.Port)),
		AllowManagement:      e.AllowManagement,
		AllowProxy:           e.AllowProxy,
		RequireProxyAuthInfo: e.RequireProxyAuthInfo,
		AllowHTTPForward:     e.AllowHTTPForward,
		AllowHTTPReverse:     e.AllowHTTPReverse,
		AllowSOCKS5:          e.AllowSOCKS5,
	}
}

// scheduleAttrRefresh periodically enriches node attributes from the
// GeoIP database and every registered ext.AttributeProvider
// (design §8 "GeoIP 与 AttributeProvider 刷新", spec/ext-abi.md §2).
// Only nodes with a known egress IP are looked up; failures yield no
// attrs and never block routing. An empty provider set is a no-op.
// scheduleRecurring runs fn every interval via the runtime Scheduler —
// the single self-rescheduling pattern (no hand-rolled tickers in boot).
func scheduleRecurring(sched ports.Scheduler, interval time.Duration, fn func(ctx context.Context)) {
	var tick func(ctx context.Context)
	tick = func(ctx context.Context) {
		fn(ctx)
		sched.Schedule(interval, tick)
	}
	sched.Schedule(interval, tick)
}

func scheduleAttrRefresh(sched ports.Scheduler, registry *source.Registry, geo *geoip.Provider, store ports.CacheStore) {
	const interval = 6 * time.Hour
	scheduleRecurring(sched, interval, func(ctx context.Context) {
		providers := ext.GetAttributeProviders()
		for _, n := range registry.All() {
			if !n.EgressIP.IsValid() {
				continue
			}
			if attrs := geo.Enrich(n.EgressIP); len(attrs) > 0 {
				store.UpsertAttrs(n.Hash, attrs)
				registry.UpdateAttrs(n.Hash, attrs)
			}
			for _, p := range providers {
				if attrs := p.Enrich(ctx, n.EgressIP, n.Hash); len(attrs) > 0 {
					store.UpsertAttrs(n.Hash, attrs)
					registry.UpdateAttrs(n.Hash, attrs)
				}
			}
		}
	})
}

// scheduleExtProbers runs every registered ext.Prober against every node
// on a fixed cadence (spec/ext-abi.md §2) and emits the observed signals
// onto the bus. Probers run off the hot path; each Probe call is bounded
// by proberTimeout.
func scheduleExtProbers(sched ports.Scheduler, registry *source.Registry, bus ports.SignalBus) {
	const (
		interval      = 5 * time.Minute
		proberTimeout = 30 * time.Second
	)
	scheduleRecurring(sched, interval, func(ctx context.Context) {
		probers := ext.GetProbers()
		if len(probers) == 0 {
			return
		}
		nodes := registry.All()
		for _, p := range probers {
			for _, n := range nodes {
				pctx, cancel := context.WithTimeout(ctx, proberTimeout)
				for _, sig := range p.Probe(pctx, n) {
					bus.Emit(sig)
				}
				cancel()
			}
		}
	})
}

// bootSystemConfig builds the boot-time system config shown in the admin
// console (GET /api/v1/system/config) from the actual startup environment.
func bootSystemConfig(cfg config.Config) control.SystemConfig {
	host, portStr, err := net.SplitHostPort(cfg.ListenAddr)
	port := 0
	if err == nil {
		if p, perr := strconv.Atoi(portStr); perr == nil {
			port = p
		} else {
			host = cfg.ListenAddr
		}
	} else {
		host = cfg.ListenAddr
	}
	return control.SystemConfig{
		ListenAddress: host,
		Port:          port,
		StateDir:      cfg.StateDir,
		CacheDir:      cfg.CacheDir,
		LogDir:        cfg.LogDir,
	}
}

func appVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	v := bi.Main.Version
	if v == "" || v == "(devel)" {
		v = "dev"
	}
	var rev string
	for _, s := range bi.Settings {
		if s.Key == "vcs.revision" {
			rev = s.Value
		}
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if rev != "" {
		v += "+" + rev
	}
	return v
}

func fatalf(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "egress: "+format+"\n", args...)
	return 1
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "egress: "+format+"\n", args...)
}

func tokenState(t string) string {
	if t == "" {
		return "empty (no-auth mode)"
	}
	return "set"
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
