# RFC-0001：去 Resin 命名与契约独立演进

- 状态：已接受
- 作者：维护者
- 日期：2026-10-19
- 影响范围：spec/identity.md | spec/errors.md | spec/events.md | spec/openapi.yaml | spec/resources/* | P4

## 背景

项目以 Resin 线协议为起点并承诺兼容。实践中兼容承诺变成纯成本：
命名分叉（`X-Resin-*` 头与 egress 二进制并存）、默认目录沿用
`/var/*/resin`、`RESIN_*` 环境变量回退与 `RESIN_DEFAULT_PLATFORM_*`
映射长期背负，而上游并不存在需要兼容的真实客户端生态。

## 提案

1. 线协议改名（破坏性）：
   - `X-Resin-Error` → `X-Egress-Error`；407 的 `Proxy-Authenticate`
     realm `"Resin"` → `"egress"`；
   - 默认身份头 `X-Resin-Account` → `X-Egress-Account`；
   - 请求日志字段 `resin_error` → `egress_error`（API、SQLite 列、事件）。
2. 环境变量：删除 `RESIN_*` 回退与 `RESIN_DEFAULT_PLATFORM_*` 映射；
   默认数据目录改为 `/var/{lib,cache,log}/egress`。
3. spec 漂移修正（与实现对齐）：
   - `events.md` Latency.`authority` 由 `string` 更正为 `bool`；
   - Traffic 字节字段定稿为 `bytesUp` / `bytesDown`；
   - `errors.md` 新增 `ENDPOINT_CAPABILITY_DISABLED`（403，Endpoint 能力关闭）。
4. spec 文档删除一切"与 Resin 兼容"叙述；README 明确不保持兼容。
5. 派生关系与 MIT 署名保留在 LICENSE 与 README 致谢中，不受影响。

## 兼容性影响

- 客户端：使用 `X-Resin-*` 头或默认身份头的调用方需改名；无迁移期
  （项目处于 v0，spec 按 ADR-002 尚未冻结 v1）。
- 部署：默认数据目录变更；旧 `/var/*/resin` 数据不自动迁移
  （state.db 需手动移动，cache/metrics/logs 可丢弃重建）。
- `RESIN_*` 环境变量不再读取，启动报缺失必填项。

## 放弃的替代方案

| 方案 | 否决理由 |
|---|---|
| 双头共存（同时发 `X-Resin-*` 与 `X-Egress-*`） | 永久性冗余，违背"去除历史包袱"原则 |
| 保留 `RESIN_*` 回退一个大版本 | 没有真实存量用户需要迁移，纯成本 |
| authority 按 spec 原文改为 string | bool 才符合 TD-EWMA 语义（domain 本身是 key），是 spec 写错 |

## 未决问题

无。
