# 扩展点接口与稳定级别（Extension ABI）

> 扩展点是社区拥有的；内核（维护者拥有）保持封闭。
> 每个接口标注稳定级别：`experimental`（可随时改）或 `stable`
>（破坏性变更必须走 RFC，并跨一个大版本）。

## 1. 扩展点一览

| 扩展点 | 概念签名 | 位置 | 典型社区扩展 | 稳定级别 |
|---|---|---|---|---|
| SourceProvider | `Fetch(ctx, spec) -> Snapshot` | 冷路径 | 新订阅格式、服务商 API | `experimental` |
| AttributeProvider | `Enrich(egressIP, node) -> Attrs` | 冷路径 | ASN、IP 类型、信誉分 | `experimental` |
| Predicate | `Compile(args) -> func(Attrs) bool` | 冷路径（编译进 View） | 自定义过滤维度 | `experimental` |
| IdentityResolver | `Resolve(Request) -> Account?` | 入口（受限） | Cookie、JWT sub | `experimental`（M6 升 stable） |
| Prober | `Probe(ctx, node) -> []Signal` | 旁路 | 可达性、信誉探测 | `experimental` |
| Sink | `Consume(Event)` | 旁路 | Prometheus、OTel、ClickHouse、Webhook | `experimental` |
| Selector | `Pick(cands, ctx) -> Node` | **热路径** | 成本、配额策略 | `experimental`（M6 升 stable） |

## 2. 接口契约

### SourceProvider（冷）
- 输入：Source 资源的 `spec`（type/url/content 等）与 `context.Context`
  （携带超时与取消）。
- 输出：节点快照（节点配置列表）。解析失败的条目跳过并计数，不必整体失败。
- 调用时机：Source 刷新周期（`updateInterval`，remote 最短 30s）与手动 `:refresh`。

### AttributeProvider（冷）
- 输入：节点的 `egressIP` 与节点 hash；输出：属性键值对（命名空间 key，
  如 `asn.org`、`ip.type`、`reputation.score`）。
- 由 Scheduler 周期性刷新；失败不阻塞路由，只影响 `attrs` 过滤维度。
- **属性 key 命名空间注册机制**：TODO（设计 §20 未决第 4 条），v0 约定
  `ext.<provider>.<key>` 前缀，社区防冲突。

### Predicate（冷，编译进 View）
- `Compile(args) -> func(Attrs) bool`：在 Platform 配置变更时编译一次；
  热路径执行的是编译后的闭包，**永不**在热路径执行 `Compile`。
- 未定义的属性 key：`in/match/gte/lte` 视为不匹配，`notIn/notMatch` 视为通过。

### IdentityResolver（入口，受限）
- 只读**请求元数据**（目标、Header、线协议身份串）；不得做 I/O，
  不得读 body，不得阻塞。
- 返回 `Account?`：`null` 表示"本 resolver 不处理"，继续链的下一步。

### Prober（旁路）
- 输入：节点；输出：`[]Signal`（见 [events.md](./events.md)）。
- 运行在 WorkerPool 旁路，不在热路径；超时与并发由 Scheduler 统一控制。

### Sink（旁路）
- 输入：`Event`（Signal 或 RequestLog）；`Consume` 必须非阻塞——
  内部满则丢，**不允许**反压数据面。
- 消费者相互独立：删掉任何一个 Sink，数据面零感知。

### Selector（热路径）
- `Pick(cands, ctx) -> Node`：`cands` 为候选节点只读视图，
  `ctx` 携带目标、平台分配策略与租约计数快照。
- 约束见 §3 热路径纪律。

## 3. 热路径纪律（P2）

Selector 与 IdentityResolver 位于热路径，必须同时满足：

1. **纯函数**：无 I/O、无网络、无读盘、无全局锁、无 goroutine 派生。
2. **零分配**（Selector）：合并前必须通过 benchmark 门槛。
3. **有界时间**：O(候选数) 或更好；不允许正则编译、map 扩容等隐性开销
   出现在调用内（预编译好再进热路径）。
4. IdentityResolver 只读请求元数据。

违反者不得合入内置发布版；编译期模块的违规由代码评审拦截
（`egress build` 不做运行时检查；benchmark 门槛数值留待后续里程碑）。

## 4. 分发：保持单二进制（P3）

- **编译期模块**（Caddy/xcaddy 模式）：扩展在 `init()` 中向 `pkg/ext` 注册，
  通过 `egress build --with <module> [-o out]` 产出定制二进制：在调用方
  Go 模块根目录执行，生成 blank-import 各扩展模块并调用 `boot.Main` 的
  临时 main 包，`go build` 后清理。
  官方发布版只含内置模块。
- **进程外协议**（仅限旁路扩展：SourceProvider、Sink、Prober）：HTTP 或
  stdio JSON 协议，任何语言都能写，egress 只作为客户端。
  （协议细节 TODO：M4 确定 JSON 信封。）
- 不用 Go `plugin` 包（ABI 脆弱）；热路径不跑 WASM。

## 5. 稳定级别规则

- `experimental`：签名可随小版本变更，但必须在 CHANGELOG 中注明。
- `stable`：破坏性变更必须走 RFC，并跨一个大版本；
  同大版本内只允许向后兼容的加法。
- 路线图：M4 先开放冷路径的 SourceProvider、AttributeProvider、Sink；
  M6 将 Selector 与 IdentityResolver 升级为 stable，并冻结 spec v1。
