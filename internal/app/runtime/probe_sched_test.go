package runtime

// probe_sched_test.go: 验证探测编排真的会触发探测（活性回归测试）。
//
// 背景：Prober 曾长期无人调度，新节点会永远卡在 Probing。此测试用
// 计数 fake dialer 证明 sweep → fireProbe → ProbeEgress 的链路是通的。

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pknode "github.com/xiarsx/egress/pkg/node"
)

type countingDialer struct{ calls *int32 }

func (d countingDialer) DialContext(ctx context.Context, h pknode.Hash, network, address string) (net.Conn, error) {
	atomic.AddInt32(d.calls, 1)
	return nil, errors.New("fake dialer: no network in test")
}

type memRegistry struct {
	nodes []pknode.NodeSnapshot
}

func (m *memRegistry) ApplySnapshot(source string, nodes []pknode.NodeSnapshot) {}
func (m *memRegistry) SnapshotOf(source string) []pknode.NodeSnapshot            { return nil }
func (m *memRegistry) UpdateHealth(h pknode.Hash, health pknode.Health)         {}
func (m *memRegistry) Get(h pknode.Hash) (pknode.NodeSnapshot, bool)            { return pknode.NodeSnapshot{}, false }
func (m *memRegistry) All() []pknode.NodeSnapshot                               { return m.nodes }

func TestProbeSchedulerFiresProbes(t *testing.T) {
	var h pknode.Hash
	h[0] = 0xAB
	reg := &memRegistry{nodes: []pknode.NodeSnapshot{
		{Hash: h, Health: pknode.ProbingHealth(), HasOutbound: true},
	}}

	var calls int32
	bus := NewBus(1024)
	sched := NewScheduler()
	defer sched.Stop()
	prober := &Prober{
		Dialer: countingDialer{calls: &calls},
		Bus:    bus,
		Clock:  SystemClock{},
		Reg:    reg,
	}
	// 消费掉探测发出的信号，避免 bus 缓冲区满影响后续（本测试只关心触发）。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sub := bus.Subscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub:
			}
		}
	}()

	ps := NewProbeScheduler(reg, prober, sched, SystemClock{})
	ps.Start(ctx)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got == 0 {
		t.Fatalf("probe scheduler never fired ProbeEgress within 10s")
	} else {
		t.Logf("ProbeEgress fired %d time(s)", got)
	}
}

func TestProbeSchedulerSkipsFreshTraffic(t *testing.T) {
	var h pknode.Hash
	h[0] = 0xCD
	reg := &memRegistry{nodes: []pknode.NodeSnapshot{
		{Hash: h, Health: pknode.HealthyHealth(0), HasOutbound: true},
	}}
	var calls int32
	prober := &Prober{
		Dialer: countingDialer{calls: &calls},
		Clock:  SystemClock{},
		Reg:    reg,
	}
	// 刚有被动流量 → ScheduleNext 返回未来时间，sweep 应整轮跳过。
	prober.NoteTraffic(h)

	sched := NewScheduler()
	defer sched.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ps := NewProbeScheduler(reg, prober, sched, SystemClock{})
	ps.sweepOnce(ctx) // 直接调一轮，不经过 Start 的异步调度

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("expected no probe while traffic is fresh, got %d", got)
	}
}
