package ports

import (
	"context"
	"net"
	"time"

	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
	"github.com/xiarsx/egress/internal/core/identity"
	"github.com/xiarsx/egress/internal/core/predicate"
)

// ---------------------------------------------------------------------------
// 跨组共享契约（第二阶段）
//
// 这些接口/类型定义在 ports 层，供多个组共同遵守，避免组间循环导入：
//   - P1（sqlite）实现 CacheLoader/CacheStore/NodeRegistry 侧的持久化；
//   - P2（source）实现 NodeRegistry；
//   - P4（runtime）消费 NodeRegistry/CacheLoader/CacheStore/PlatformSource/SourceSource；
//   - P5（control）实现 PlatformSource/SourceSource，并调用 RuntimeHooks。
// 各实现方用 `var _ ports.Xxx = ...` 做编译期断言。
// ---------------------------------------------------------------------------

// RuntimeHooks 是控制面（internal/app/control）在声明式资源变更时调用的
// 钩子，由运行时（internal/app/runtime）实现。组装根（cmd/egress）负责接线。
type RuntimeHooks interface {
	// OnPlatformChanged 在 Platform 资源被创建或更新后调用（id 为平台 UUID）。
	OnPlatformChanged(id string)
	// OnPlatformDeleted 在 Platform 资源被删除后调用。
	OnPlatformDeleted(id string)
	// OnSourceChanged 在 Source 资源被创建、更新或启用/禁用后调用。
	OnSourceChanged(name string)
	// OnSourceDeleted 在 Source 资源被删除后调用。
	OnSourceDeleted(name string)
}

// PlatformSpec 是运行时需要的平台编译输入（控制面从 Store 读取并组装）。
type PlatformSpec struct {
	ID        string
	Name      string
	Filter    predicate.Filter
	Policy    identity.Policy
	StickyTTL time.Duration
}

// PlatformSource 供运行时按 UUID 获取平台编译输入（P5 的 control.Service 实现）。
type PlatformSource interface {
	GetPlatformSpec(ctx context.Context, id string) (PlatformSpec, error)
}

// SourceSpec 是订阅源的声明（P2 的 Syncer 与 P5 的 control 共用同一形状）。
type SourceSpec struct {
	Name           string
	Kind           string // "remote" | "local"
	URL            string
	Content        string
	UpdateInterval time.Duration
	EvictDelay     time.Duration
}

// SourceSource 供运行时按名获取订阅源声明（P5 的 control.Service 实现）。
type SourceSource interface {
	GetSourceSpec(ctx context.Context, name string) (SourceSpec, error)
}

// NodeRegistry 是节点注册表契约（P2 的 internal/adapters/source 实现，
// P4 的 HealthProjector/ViewDeriver/Sync 消费；Health 的唯一写入者是投影器）。
type NodeRegistry interface {
	// ApplySnapshot 发布某来源的新快照（增量合并 Tags、维护引用计数）。
	ApplySnapshot(source string, nodes []pknode.NodeSnapshot)
	// SnapshotOf 返回某来源当前快照（供 Ephemeral 驱逐过滤后重发布）。
	SnapshotOf(source string) []pknode.NodeSnapshot
	// UpdateHealth 更新节点健康（唯一写入者：HealthProjector）。
	UpdateHealth(h pknode.Hash, health pknode.Health)
	// Get/All 读快照。
	Get(h pknode.Hash) (pknode.NodeSnapshot, bool)
	All() []pknode.NodeSnapshot
}

// LeaseRecord 是持久化的租约记录。
type LeaseRecord struct {
	Platform string
	Account  string
	Lease    pklease.Lease
}

// CacheLoader 从 cache.db 读启动候选（P1 的 sqlite.Cache 实现）。
type CacheLoader interface {
	LoadSnapshot() (nodes []pknode.NodeSnapshot, sourceNodes map[string][]pknode.Hash, leases []LeaseRecord, err error)
}

// CacheStore 是 cache.db 的写侧（Coalescing 语义，Flush 落盘；P1 实现）。
type CacheStore interface {
	UpsertNode(n pknode.NodeSnapshot)
	DeleteNode(h pknode.Hash)
	SetSourceNodes(source string, hashes []pknode.Hash)
	UpsertLatency(h pknode.Hash, domain string, ms float64, at time.Time, authority bool)
	UpsertAttrs(h pknode.Hash, attrs map[pknode.AttrKey]pknode.AttrValue)
	UpsertLease(platform, account string, l pklease.Lease)
	DeleteLease(platform, account string)
	Flush() error
}

// NodeDialerFactory 供 P2 的 singbox.Dialer 被组装根发现（可选约定）。
type NodeDialerFactory interface {
	DialContext(ctx context.Context, node pknode.Hash, network, address string) (net.Conn, error)
}
