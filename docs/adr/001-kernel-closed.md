# ADR-001：内核封闭（第 1 层不开放 SPI）

- 状态：已接受
- 日期：2026-10-17
- 相关：设计 §3、§4.1、§11；P2

## 背景

egress 的核心性能承诺是 P2：**热路径 O(1)、无 I/O、无全局锁**。
数据面流水线（Ingress → Resolve → Route → Dial → Relay）每请求都要经过，
任何在热路径上执行的第三方代码都可能引入 I/O、锁或不可预测的延迟，
从而悄悄违反 P2。

同时社区有强烈的扩展诉求：新订阅格式、新过滤维度、新分配策略、
新探测目标、指标导出等。

## 决策

1. **内核（第 1 层）封闭**，由维护者拥有，不开放 SPI。
   特别是 **Ingress 封闭为三种**（HTTP 正向、SOCKS5、HTTP 反向）：
   新入口协议走 RFC 进内核，不开放为 SPI。
2. 扩展只发生在**第 2 层扩展点**（社区拥有），并按位置分级约束：
   - 冷路径/旁路扩展（SourceProvider、AttributeProvider、Predicate、
     IdentityResolver、Prober、Sink）：约束宽松；
   - 热路径扩展（Selector）：必须是纯函数、零分配、无 I/O，
     合并前必须通过 benchmark 门槛。
3. 不用 Go `plugin` 包（ABI 脆弱），热路径不跑 WASM；
   分发走编译期模块（`egress build --with`）与旁路进程外协议。

## 放弃的替代方案

| 方案 | 否决理由 |
|---|---|
| 开放 Ingress SPI | 热路径进入第三方代码；且新协议未必能承载 `Platform.Account` 身份（如 WireGuard、TProxy），身份是路由的前提 |
| 热路径上执行谓词过滤（开放 Filter SPI） | 直接违反 P2；谓词一律在冷路径编译进 View |
| Go plugin 动态加载 | ABI 脆弱，升级即断；且无法做编译期 benchmark 门槛 |
| 热路径 WASM | 有性能风险，且 WASM 沙箱解决不了"逻辑正确性"问题 |

## 后果

- 正面：P2 由**结构**保证而非评审保证；内核可独立演进、独立做性能回归。
- 负面：新入口协议必须走 RFC、由维护者合入，社区不能自助；
  热路径扩展（Selector）的开发门槛高于普通扩展。
- 扩展 ABI 的稳定级别（`experimental` / `stable`）见 `spec/ext-abi.md`；
  `stable` 接口的破坏性变更必须走 RFC 并跨大版本。
