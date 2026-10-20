# Recipe（资源）

> `apiVersion: egress/v1`，`kind: Recipe`。
> Recipe 是可分享、可版本化的知识包：关于"某个目标站点怎么用"的知识属于社区，
> 不属于某台网关。`apply` 时展开为 Platform 与 IdentityPolicy 资源；
> Recipe 本身不参与热路径。

```yaml
apiVersion: egress/v1
kind: Recipe
metadata:
  name: example-api
  version: 1.2.0
  requires: [egress>=2.0, ext:asn>=1]   # 依赖声明：缺失时拒绝 apply 并给出清晰报错
spec:
  identityPolicy:                       # 展开为 IdentityPolicy 资源
    chain:
      - header: X-Egress-Account
      - wire: {}
      - headerRule: { rules: [ { prefix: "api.example.com", headers: [Authorization] } ] }
    onMiss: random
  platformTemplate:                     # 展开为 Platform 资源
    stickyTTL: 168h
    allocation: BALANCED
    passiveBreaker: on
    filter:
      attrs: { asn.org: { notMatch: "(?i)cloud" } }
```

## 字段表

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `metadata.name` | string | 是 | — | Recipe 名 |
| `metadata.version` | semver | 是 | — | 版本号，遵循 semver |
| `metadata.requires` | string[] | 否 | `[]` | 依赖：`egress>=x.y` 或 `ext:<name>>=v`；缺失的扩展导致拒绝 apply |
| `spec.identityPolicy` | object | 否 | — | IdentityPolicy spec（同 [identitypolicy.md](./identitypolicy.md)，无 metadata） |
| `spec.platformTemplate` | object | 否 | — | Platform spec（同 [platform.md](./platform.md)，无 metadata） |

## 操作语义

- `egress recipe apply <file|url> --platform <name>`：创建或更新名为 `<name>`
  的 Platform 与配套 IdentityPolicy（命名规则 TODO：默认 `<name>-identity`？
  还是复用 `default`？**TODO**：M5 确定）。
- `egress recipe export`：从现有 Platform / IdentityPolicy 导出 Recipe。
- 社区 recipes 仓库带版本号与 schema 校验。
- `requires` 中声明的扩展缺失时拒绝应用，并给出清晰报错。
