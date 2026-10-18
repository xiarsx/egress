# egress 架构（一屏）

> 设计总纲 §3 四层结构的展开。契约见 `spec/`，决策理由见 `docs/adr/`。

```
┌─────────────────────────────────────────────────────────────────┐
│ 第 3 层  Recipe（社区共享的知识）                                 │
│  "某个站点怎么用"：Platform 模板 + IdentityPolicy + 过滤器 + TTL  │
│  apply 时展开为资源；export 从现网导出；社区仓库版本化分发         │
├─────────────────────────────────────────────────────────────────┤
│ 第 2 层  扩展点（社区拥有）                                        │
│  冷路径/旁路： SourceProvider · AttributeProvider · Predicate      │
│                IdentityResolver · Prober · Sink                   │
│  热路径：      Selector（仅纯函数，benchmark 门槛）                │
│  分发：编译期模块（egress build --with）/ 旁路进程外协议           │
├─────────────────────────────────────────────────────────────────┤
│ 第 1 层  内核（维护者拥有，封闭）                                   │
│                                                                  │
│  数据面流水线（热路径 O(1)，无 I/O，无全局锁）：                    │
│                                                                  │
│   TCP accept ─首字节0x05?─┬─ SOCKS5 Ingress ─┐                    │
│                           └─ HTTP Ingress ───┼─ 正向/反向/控制面    │
│                                              ▼                    │
│                        Request{token, rawIdentity, target}        │
│                                              ▼                    │
│                        Resolve：Token 校验 → IdentityPolicy 链     │
│                                → Identity{PlatformID, Account}    │
│                                              ▼                    │
│                        Route：Bypass? → DirectNode                │
│                               否则 select(View[p], Lease[p][a])   │
│                                              ▼                    │
│                        Dial → Relay → emit Signal（非阻塞满则丢）   │
│                                                                  │
│  运行时原语（每状态唯一写入者）：                                    │
│   Node（HealthProjector 写）· View（ViewDeriver 写）                │
│   Lease（LeaseTable 写）· Signal（不可变值）· Scheduler · Store   │
│                                                                  │
│  冷路径：SourceSyncer → 快照 → Registry 汇聚（COW）→ View 增量派生  │
│          HealthProjector ← SignalBus ← 被动流量/主动探测/下载       │
│          BatchSink → cache.db / metrics.db / request_logs-*.db   │
├─────────────────────────────────────────────────────────────────┤
│ 第 0 层  契约 spec/（社区共有，版本化）                             │
│  identity.md · errors.md · resources/ · events.md · openapi.yaml │
│  ext-abi.md · vectors/（conformance 测试向量）                     │
└─────────────────────────────────────────────────────────────────┘
```

## 关键结构决策

- **资源 vs 原语**（§2.1）：Platform / Source / IdentityPolicy / Recipe 是
  用户声明的**资源**（存 `state.db`，走 API / `egress.yaml`），本身不参与
  热路径计算；Node / View / Lease / Signal 是内存中的**运行时原语**，
  每个只有一个写入者。数据面只持有 `ReadOnlyView` 与 `SignalEmitter`。
- **内核封闭**：P2 只能靠"热路径上没有第三方代码"来保证，不能靠评审保证
  （→ [ADR-001](./adr/001-kernel-closed.md)）。
- **spec-first**：OpenAPI 手写并评审，代码由它生成；conformance 向量
  是第三方 SDK 自证兼容的方式（→ [ADR-002](./adr/002-spec-first.md)）。
- **持久化**：`state.db`（强一致：API 成功 = 已提交）存配置资源；
  `cache.db`（弱一致）存节点/延迟/Health/租约——没有它，重启即违反 P1；
  `metrics.db` 存小时桶聚合；`request_logs-*.db` 按大小滚动。
  写入统一走 `BatchSink`（Coalescing / AppendOnly），退出前必 flush。
- **启动 = 一次 reconcile**：加载资源 → 载入 cache 为候选 → 重放 Source 快照
  → 派生 View → 载入租约重算 IPLoad → 启动 Scheduler 与 SignalBus →
  **最后**打开 listener（唯一的顺序约束）。

## 目录结构（规划）

```
cmd/egress/          组合根：装配、启停顺序（唯一的顺序约定）
internal/core/       纯逻辑，零 I/O：identity  node(Health, step)  view  lease  select  predicate
internal/app/        用例编排：pipeline  sync  probe  reconcile  control
internal/ports/      Store  Dialer  Clock  Fetcher  SignalBus  Scheduler
internal/adapters/   sqlite  singbox  http  socks5  reverse  geoip  webui
pkg/ext/             扩展 SDK（唯一的对外 Go API，按稳定级别分包）
spec/  docs/  rfcs/
```

依赖方向：`core` 不 import 任何其他层；控制面只调用 `app/control`，
不直接访问 `Store`。
