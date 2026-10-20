# RFC-0002：扩展点运行时接线与资源 schema 扩展

- 状态：Accepted（随实现落地）
- 日期：2026-02-19

## 背景

`pkg/ext` 注册表自始存在但零调用方（设计自检 §3.3.2 已标记），7 个扩展点全部"未接线"；
`egress build` 不存在。扩展点是架构不变量（spec/ext-abi.md），必须落地。

## 决定

### 接线（无 spec 变更）

| 扩展点 | 接法 |
|---|---|
| Sink | 信号总线 + RequestLog 扇出（每 sink 独立有界队列，满即丢，符合 ABI 非阻塞契约） |
| SourceProvider | source.Syncer 未知 kind 回退到注册表；ext 直接返回 NodeSnapshot，跳过字节解析 |
| AttributeProvider | 并入 GeoIP 刷新循环（6h），key 命名空间 `ext.<provider>.<key>` |
| Prober | 定时器（5min）对全节点跑 Probe，信号注入总线；单次 30s 超时 |
| PredicateFactory | 平台过滤属性 key `ext.<factory>` 委托编译期工厂；未注册 = 编译期错误 |

### spec 变更（本 RFC 记录）

1. **IdentityPolicy 链新增 `resolver` 步**：委托给注册的 `IdentityResolver`；
   未注册名字按未命中处理（链继续，OnMiss 兜底 = fail-closed）。
2. **Platform `allocation` 新增 `ext.<selector>`**：委托给注册的 `Selector`；
   粘性租约机制被绕过（选择器自行负责粘性）；未注册名字 fail-closed 为
   `NO_AVAILABLE_NODES`。
3. **ext-abi.md §4**：`egress build --with` 从"计划"变为已实现（描述以实现为准）；
   benchmark 门槛 TODO 移除（评审拦截，不做运行时检查）。

## 替代方案

- 进程外 gRPC 扩展：保留在 ext-abi.md §4 路线图，v0 不做。
- Selector 保留粘性租约：会让内置租约机与扩展决策互相打架，拒绝。

## 影响

- 资源 schema 纯增量：`chain[].resolver`、`allocation: ext.*` 旧配置不受影响。
- 无扩展注册时行为与接线前完全一致（所有委托点 fail-closed 或 no-op）。
