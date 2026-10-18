# IdentityPolicy（资源）

> `apiVersion: egress/v1`，`kind: IdentityPolicy`。
> 声明"如何从请求得到 Account"：一条有序解析链，首个非空结果生效。
> 原 Platform 的三个反代字段（`ReverseProxyEmptyAccountBehavior` /
> `FixedAccountHeader` / `MissAction`）与全局 Header 规则表全部收敛为这一种资源。

```yaml
apiVersion: egress/v1
kind: IdentityPolicy
metadata:
  name: default
spec:
  chain:
    - header: X-Resin-Account        # 默认第一位：从指定请求头取 Account
    - wire: {}                        # 线协议中的 Account（见 ../identity.md）
    - headerRule:                     # 按目标 URL 前缀选 Header（最长前缀匹配）
        rules:
          - { prefix: "api.example.com/v1", headers: [Authorization] }
          - { prefix: "*", headers: [Authorization, x-api-key] }
  onMiss: random                      # random | reject
```

## 字段表

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `metadata.name` | string | 是 | — | 策略名；`default` 为内置默认策略 |
| `spec.chain` | step[] | 是 | — | 有序解析链，首个非空结果生效 |
| `spec.chain[].header` | string | 二选一 | — | 从该 HTTP 请求头取值；SOCKS5 入口自动跳过 |
| `spec.chain[].wire` | `{}` | 二选一 | — | 取线协议身份串中的 Account 部分 |
| `spec.chain[].headerRule` | object | 二选一 | — | 按目标 URL 前缀（最长前缀，`*` 兜底）选一组 Header 依次尝试，首个非空生效 |
| `spec.onMiss` | enum | 否 | `random` | 链全部未命中时：`random`（随机路由，不建租约）\| `reject`（403 `ACCOUNT_REJECTED`） |

## 步骤语义

- `headerRule` 等需要读 Header 的步骤只对 HTTP 类入口生效；在 SOCKS5 上自动跳过。
- 前缀匹配：先匹配 domain（不区分大小写，归一化到小写），再按 `/` 分段匹配
  路径段；`*` 为最后的兜底。
- 链在内部用 `⊕` 组合，对用户只暴露声明式列表，不暴露代数。
- Account 来源优先级（固定）：`X-Resin-Account` 请求头（默认链第一位）
  > URL/线协议身份段的 Account > 请求头提取规则。

## 未决

- 设计 §20 未决事项第 2 条：`X-Resin-Account` 与线协议 Account **冲突**时，
  是静默优先还是可配置为拒绝。**v0 保持静默优先**（链顺序即优先级）。
