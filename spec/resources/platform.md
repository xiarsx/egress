# Platform（资源）

> `apiVersion: egress/v1`，`kind: Platform`。
> Platform 是用户唯一需要理解的路由单元：一个命名的路由策略。
> 运行时没有"Platform 对象"参与热路径计算，只有它编译出的 **View**
>（可路由集合）与 **Lease 命名空间**；热路径只拿 `PlatformID` 去索引这两者。

```yaml
apiVersion: egress/v1
kind: Platform
metadata:
  name: video               # 必填。先 trim；非空、全局唯一；保留字 Default/api 不可用；
                            # 不得包含 .:|/\@?#%~ 与空白字符
spec:
  stickyTTL: 168h           # 租约 TTL。租约 expiry 固定，不续期
  allocation: BALANCED       # BALANCED | PREFER_LOW_LATENCY | PREFER_IDLE_IP | ext.<selector>
  passiveBreaker: on        # on | off。off 时数据面发射 Signal 带 breakerExempt=true
  identityPolicyRef: default # 引用的 IdentityPolicy 名称（默认 "default"）
  filter:
    tags: ["^sub-a/", "*hk|jp", "!expired"]   # 见下
    attrs:
      geo.country: { in: [hk, jp, us] }
      asn.org:     { notMatch: "(?i)cloud|hosting" }
```

## 字段表

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `metadata.name` | string | 是 | — | 平台名，校验规则见上 |
| `spec.stickyTTL` | duration | 否 | TODO（见下） | 租约有效期；到期后由调度器到期队列删除，不全表扫描 |
| `spec.allocation` | enum | 否 | `BALANCED` | 分配策略：`BALANCED`（`(leaseCount+1)×latency`）、`PREFER_LOW_LATENCY`（`latency`）、`PREFER_IDLE_IP`（`leaseCount`）、`ext.<selector>`（委托给已注册的扩展 `Selector`，此时粘性租约由扩展自行负责；未注册名字 fail-closed 为 `NO_AVAILABLE_NODES`） |
| `spec.passiveBreaker` | enum | 否 | TODO（见下） | `on` \| `off`；`off` 时被动失败不计入熔断（`breakerExempt=true`） |
| `spec.identityPolicyRef` | string | 否 | `default` | IdentityPolicy 名称 |
| `spec.filter.tags` | string[] | 否 | `[]`（不过滤） | 标签过滤，沿用原语法：无前缀 = ANY（至少一个匹配），`*` 前缀 = MUST（正向条件须由**同一个** Tag 同时满足），`!` 前缀 = MUST_NOT（检查全部 Tag） |
| `spec.filter.attrs` | map | 否 | `{}` | 属性过滤，key 为属性名（如 `geo.country`、`asn.number`、`asn.org`、`ip.type`）；每个属性支持 `in / notIn / match / notMatch / gte / lte`；key 未定义视为不匹配，`notIn` 与 `notMatch` 视为通过 |

## 可路由判定（Routable，派生函数，不存储）

```
Routable(p, n) = n.Outbound ≠ nil ∧ n.Health = Healthy ∧ n.EgressIP ≠ nil
               ∧ n.Latency 非空 ∧ TagFilter(p, n.Tags) ∧ AttrFilter(p, n.Attrs)
```

谓词在 Platform 配置变更时**编译**（正则预编译、属性谓词转为比较函数）；
热路径永远不执行过滤（P2）。

## 默认值 TODO

- `stickyTTL` 默认值：参考实现为纳秒整数字段，未找到文档化默认值。**TODO**：
  M3 资源化时确定（建议与参考实现行为对齐后写入）。
- `passiveBreaker` 默认值：设计 §20 未决事项第 1 条，需用现网数据定。
  **TODO**：确定前默认 `on`。
