package runtime

// probe_sched.go: 探测编排（设计 §6.3、§8）。
//
// Prober 知道"怎么探"，但不知道"何时探"。ProbeScheduler 是缺失的那块
// 编排：它按节点健康状态决定探测节奏，并把被动流量（Traffic 信号）
// 反馈给 Prober 以推迟探测（"流量越大，主动探测越少"）。
//
// 节奏（可调常量）：
//   - Probing（新节点/恢复）：每 30s 一次出口探测，直到 Healthy
//   - Healthy：每 5min 一次出口探测（附带 cloudflare.com 延迟信号）
//   - Tripped（熔断）：每 60s 一次，熔断期间继续接受主动探测（设计 §2.3）
//   - 被动流量新鲜（TrafficDeferWindow 内）时整轮跳过
//
// 实现上是一个轻量调度扫描：每 sweepInterval 遍历一次全量节点，只做
// 时间戳比较（10 万节点约数毫秒），到期则把一次探测任务扔进
// ports.Scheduler 的最小堆。这不是设计 §8 要消灭的那种"全量 I/O
// 扫描协程"——实际的网络 I/O 只发生在到期的单个探测任务里。

import (
	"context"
	"sync"
	"time"

	pksignal "github.com/xiarsx/egress/pkg/signal"
	pknode "github.com/xiarsx/egress/pkg/node"
	"github.com/xiarsx/egress/internal/ports"
)

const (
	// probeSweepInterval 是调度扫描的周期。
	probeSweepInterval = 30 * time.Second
	// probingInterval 是 Probing 节点的探测间隔。
	probingInterval = 30 * time.Second
	// healthyProbeInterval 是 Healthy 节点的出口探测间隔。
	healthyProbeInterval = 5 * time.Minute
	// trippedProbeInterval 是 Tripped 节点的探测间隔（熔断期间仍探测）。
	trippedProbeInterval = 60 * time.Second
)

// ProbeScheduler 驱动 Prober，是 Scheduler 之上的编排层。
type ProbeScheduler struct {
	reg    ports.NodeRegistry
	prober *Prober
	sched  ports.Scheduler
	clock  ports.Clock

	mu        sync.Mutex
	lastProbe map[pknode.Hash]time.Time
}

// NewProbeScheduler 创建探测编排器。prober/sched 由组装根传入。
func NewProbeScheduler(reg ports.NodeRegistry, prober *Prober, sched ports.Scheduler, clock ports.Clock) *ProbeScheduler {
	if clock == nil {
		clock = SystemClock{}
	}
	return &ProbeScheduler{
		reg:       reg,
		prober:    prober,
		sched:     sched,
		clock:     clock,
		lastProbe: make(map[pknode.Hash]time.Time),
	}
}

// Start 启动编排：立即做一次全量扫描，之后每 probeSweepInterval 扫描一次。
// 只调用一次；用 ctx 取消。被动流量的反馈由组装根完成：订阅 Bus 的
// Traffic 信号并调 FeedTraffic。
func (ps *ProbeScheduler) Start(ctx context.Context) {
	var sweep func(ctx context.Context)
	sweep = func(ctx context.Context) {
		ps.sweepOnce(ctx)
		ps.sched.Schedule(probeSweepInterval, sweep)
	}
	ps.sched.Schedule(0, sweep)
}

// FeedTraffic 供组装根把 Traffic 信号喂进来（bus 订阅在组装层做，保持
// 本包不直接依赖 Bus 的具体订阅方式）。
func (ps *ProbeScheduler) FeedTraffic(sig pksignal.Signal) {
	if sig.Kind != pksignal.KindTraffic || !sig.OK {
		return
	}
	ps.prober.NoteTraffic(sig.NodeHash)
}

// sweepOnce 做一轮调度扫描：对每个节点按健康状态决定是否到期。
func (ps *ProbeScheduler) sweepOnce(ctx context.Context) {
	now := ps.clock.Now()
	for _, n := range ps.reg.All() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		h := n.Hash
		var interval time.Duration
		switch n.Health.Kind {
		case pknode.HealthProbing:
			interval = probingInterval
		case pknode.HealthHealthy:
			interval = healthyProbeInterval
		case pknode.HealthTripped:
			interval = trippedProbeInterval
		default:
			interval = trippedProbeInterval
		}
		// 被动流量新鲜时整轮跳过（设计 §6.3）。注意 ScheduleNext 内部会
		// 再取一次 now()，与此处的 now 有纳秒级漂移，故用 1s 容差比较，
		// 否则无流量时也会被误判为"需推迟"而永远跳过。
		if next := ps.prober.ScheduleNext(h); next.After(now.Add(time.Second)) {
			continue
		}
		ps.mu.Lock()
		last, ok := ps.lastProbe[h]
		ps.mu.Unlock()
		if ok && now.Sub(last) < interval {
			continue
		}
		ps.fireProbe(h, now)
	}
	ps.gc(now)
}

// fireProbe 记录时间并把一次出口探测扔进调度器（立即执行）。
func (ps *ProbeScheduler) fireProbe(h pknode.Hash, now time.Time) {
	ps.mu.Lock()
	ps.lastProbe[h] = now
	ps.mu.Unlock()
	ps.sched.Schedule(0, func(ctx context.Context) {
		ps.prober.ProbeEgress(ctx, h)
	})
}

// gc 清理已删除节点的 lastProbe 记录，防止 map 无限增长。
func (ps *ProbeScheduler) gc(now time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if len(ps.lastProbe) < 200000 {
		return
	}
	alive := make(map[pknode.Hash]struct{}, len(ps.lastProbe))
	for _, n := range ps.reg.All() {
		alive[n.Hash] = struct{}{}
	}
	for h := range ps.lastProbe {
		if _, ok := alive[h]; !ok {
			delete(ps.lastProbe, h)
		}
	}
}
