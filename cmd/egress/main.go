// Command egress is the composition root of the egress gateway
// (design §16, §9.2).
//
// It does no business logic: it builds the config, opens the databases,
// wires the groups together, and owns the single sequencing constraint
// the design keeps — listeners open LAST, after everything else is
// running (design §9.2 step 6).
//
// Startup order:
//
//  1. config.Load (EGRESS_*, failure is fatal)
//  2. state.db → control.Service → EnsureDefaultPlatform + ApplyFile +
//     ApplyLegacyDefaultEnv
//  3. cache.db / metrics.db / request-log DBs
//  4. source registry + sing-box dialer + fetcher → source syncer
//  5. runtime.New → runtime.Run (design §9.2 steps 1–5)
//  6. wiring: latency tracker, bus sinks (metrics/request-log),
//     pipeline, GeoIP
//  7. listeners LAST: HTTP forward, SOCKS5, reverse+control-API mux
//
// Shutdown (SIGINT/SIGTERM): stop listeners → stop runtime → flush
// cache/metrics/request-log → close databases.
package main

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
	"syscall"
	"time"

	"github.com/xiarsx/egress/internal/adapters/geoip"
	forward "github.com/xiarsx/egress/internal/adapters/http"
	"github.com/xiarsx/egress/internal/adapters/reverse"
	"github.com/xiarsx/egress/internal/adapters/singbox"
	"github.com/xiarsx/egress/internal/adapters/socks5"
	"github.com/xiarsx/egress/internal/adapters/source"
	"github.com/xiarsx/egress/internal/adapters/sqlite"
	"github.com/xiarsx/egress/internal/adapters/webui"
	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/app/pipeline"
	"github.com/xiarsx/egress/internal/app/runtime"
	"github.com/xiarsx/egress/internal/app/wiring"
	"github.com/xiarsx/egress/internal/config"
	corenode "github.com/xiarsx/egress/internal/core/node"
	pick "github.com/xiarsx/egress/internal/core/select"
	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// Listener defaults. The main EGRESS_PORT serves the HTTP forward proxy;
// the other ingress/API listeners default to adjacent ports on the same
// host and can be moved or disabled (empty value) via the environment.
const (
	defaultSOCKSPort = "2261"
	defaultAPIPort   = "2262"
)

func main() {
	os.Exit(run())
}

func run() int {
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
	if err := config.ApplyLegacyDefaultEnv(ctx, store); err != nil {
		return fatalf("apply legacy env: %v", err)
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

	reqSink := &wiring.ReqLogSink{
		Log: reqLog,
		PlatformName: func(id string) string {
			if p, err := svc.GetPlatform(ctx, id); err == nil && p.Name != "" {
				return p.Name
			}
			return id
		},
	}
	go reqSink.Run(ctx, bus.Subscribe())

	geo, err := geoip.Open(os.Getenv("EGRESS_GEOIP_DB"))
	if err != nil {
		return fatalf("open geoip db: %v", err)
	}
	scheduleGeoIPRefresh(sched, registry, geo, cacheDB.Store())

	resolver := wiring.NewResolver(store, svc)
	pipe := pipeline.New(pipeline.Deps{
		Resolver:   resolver,
		NodeDialer: dialer,
		Direct:     &net.Dialer{Timeout: 30 * time.Second},
		Bus:        bus,
		Clock:      runtime.SystemClock{},
		Leases:     rt.Leases(),
		Views:      rt.Deriver().View,
		Nodes:      registry.Get,
		Latency:    tracker.Latency,
		IPLoad:     rt.Leases().IPLoad,
		// TODO: per-platform allocation from the platform spec; the
		// pipeline currently takes a single policy for all platforms.
		PickPolicy:        pick.Policy{Allocation: pick.AllocationBalanced},
		ProxyToken:        cfg.ProxyToken,
		PassiveBreakerOff: resolver.PassiveBreakerOff,
	})

	// --- 7. runtime backend for the control API --------------------------
	// Wire runtime-owned reads/actions (nodes, leases, metrics, logs,
	// geoip, probes) into the control plane; this clears the 501s on
	// those routes.
	prober := &runtime.Prober{
		Dialer: dialer,
		Bus:    bus,
		Clock:  runtime.SystemClock{},
		Reg:    registry,
	}
	svc.SetRuntimeBackend(&wiring.Backend{
		Reg:         registry,
		Deriver:     rt.Deriver(),
		Leases:      rt.Leases(),
		Syncer:      syncer,
		Platforms:   svc,
		Metrics:     metricsDB,
		ReqLog:      reqLog,
		Cache:       cacheDB.Store(),
		Geo:         geo,
		GeoDBPath:   os.Getenv("EGRESS_GEOIP_DB"),
		Prober:      prober,
		Clock:       runtime.SystemClock{},
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
	var closers []io.Closer

	fwdLn, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fatalf("listen %s (http forward): %v", cfg.ListenAddr, err)
	}
	closers = append(closers, fwdLn)
	go func() {
		_ = forward.New(pipe).Serve(fwdLn) // nil when the listener closes
	}()
	logf("http-forward listening on %s", fwdLn.Addr())

	if addr := listenerAddr("EGRESS_SOCKS_LISTEN", cfg.ListenAddr, 1, defaultSOCKSPort); addr != "" {
		socksLn, err := net.Listen("tcp", addr)
		if err != nil {
			return fatalf("listen %s (socks5): %v", addr, err)
		}
		closers = append(closers, socksLn)
		go func() {
			_ = socks5.New(pipe).Serve(socksLn)
		}()
		logf("socks5 listening on %s", socksLn.Addr())
	} else {
		logf("socks5 disabled (EGRESS_SOCKS_LISTEN empty)")
	}

	apiMux := http.NewServeMux()
	svc.RegisterRoutes(apiMux)
	// WebUI mounted at /ui/ (longest-prefix routing keeps /api/v1/… and
	// /ui/ ahead of the reverse handler on "/").
	apiMux.Handle("/ui", webui.Handler())
	apiMux.Handle("/ui/", webui.Handler())
	apiMux.Handle("/", reverse.New(pipe))
	apiSrv := &http.Server{Handler: apiMux, ReadHeaderTimeout: 10 * time.Second}
	if addr := listenerAddr("EGRESS_API_LISTEN", cfg.ListenAddr, 2, defaultAPIPort); addr != "" {
		apiLn, err := net.Listen("tcp", addr)
		if err != nil {
			return fatalf("listen %s (api): %v", addr, err)
		}
		closers = append(closers, apiLn)
		go func() {
			_ = apiSrv.Serve(apiLn) // http.ErrServerClosed on shutdown
		}()
		logf("api+reverse listening on %s", apiLn.Addr())
	} else {
		logf("api disabled (EGRESS_API_LISTEN empty)")
	}

	// --- shutdown -------------------------------------------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logf("received %s, shutting down", sig)

	// 1. stop listeners
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = apiSrv.Shutdown(shutCtx)
	shutCancel()
	for _, c := range closers {
		_ = c.Close()
	}
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

// listenerAddr resolves an optional listener address: an explicitly set
// (even empty) env wins; otherwise host is taken from the main listen
// address and defaultPort is used. Empty means "disabled".
func listenerAddr(env, mainAddr string, portOffset int, fallbackPort string) string {
	if v, ok := os.LookupEnv(env); ok {
		return v // 显式设置（含空字符串=禁用）优先
	}
	host, portStr, err := net.SplitHostPort(mainAddr)
	if err != nil || host == "" {
		host = "0.0.0.0"
	}
	if p, err := strconv.Atoi(portStr); err == nil && p > 0 && p+portOffset <= 65535 {
		return net.JoinHostPort(host, strconv.Itoa(p+portOffset))
	}
	return net.JoinHostPort(host, fallbackPort)
}

// scheduleGeoIPRefresh periodically enriches node attributes from the
// GeoIP database (design §8 "GeoIP 与 AttributeProvider 刷新"). Only
// nodes with a known egress IP are looked up; failures yield no attrs and
// never block routing. An empty provider (no EGRESS_GEOIP_DB) is a no-op.
func scheduleGeoIPRefresh(sched ports.Scheduler, registry *source.Registry, geo *geoip.Provider, store ports.CacheStore) {
	const interval = 6 * time.Hour
	var tick func(ctx context.Context)
	tick = func(ctx context.Context) {
		for _, n := range registry.All() {
			if !n.EgressIP.IsValid() {
				continue
			}
			if attrs := geo.Enrich(n.EgressIP); len(attrs) > 0 {
				store.UpsertAttrs(n.Hash, attrs)
			}
		}
		sched.Schedule(interval, tick)
	}
	sched.Schedule(interval, tick)
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
