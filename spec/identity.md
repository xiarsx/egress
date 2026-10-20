# 身份线协议 V1（Identity Wire Protocol V1）

> 状态：**stable**。V1 线协议是稳定契约；任何破坏性变更必须走 RFC。

身份（Identity）是 `Platform.Account` 二元组。入口只解析一次，之后全链路
只传 `PlatformID`（UUID），不再传名字。

## 1. 三个入口的身份位置与 Token 规则

| 入口 | 身份位置 | Token 位置 |
|---|---|---|
| HTTP 正向 | `Proxy-Authorization: Basic <base64(user:pass)>`，其中 `user = Platform.Account`，`pass = TOKEN` | 密码段（`:` 右侧） |
| SOCKS5 | RFC1929 用户名密码认证：`username = Platform.Account`，`password = TOKEN` | 密码段 |
| HTTP 反向 | 路径首段：`/TOKEN/Platform.Account/proto/host/path?query` | 路径第一段 |

Token 规则：

- `EGRESS_PROXY_TOKEN` 非空时始终执行令牌认证；Token 校验失败 → `AUTH_FAILED`。
- `EGRESS_PROXY_TOKEN` 为空时：HTTP 正向允许无认证（视为 `Default` 平台请求，
  见 §3）；SOCKS5 允许 `NO AUTH (0x00)`，但若客户端同时提供 RFC1929
  用户名密码认证，服务端优先选择该方法以提取身份，此时密码不做校验。
- Token 不得包含 `.:|/\@?#%~` 与空白字符，不得取保留值 `api` / `healthz` / `ui`。

## 2. 切分规则（V1，精确语义）

1. **先切 Token**：按**最右** `:` 切分。左侧为身份串，右侧为 Token。
   （Account 本身可包含 `:`，例如 `bEA:234`；而 Token 不得包含 `:`——见上。）
2. **再切身份串**：按身份串中**第一个出现的 `.` 或 `:`** 切分，
   左侧为 Platform，右侧为 Account。若两者都不存在，整个身份串为 Platform，
   Account 为空。
3. 反向代理的身份段在切分前先按 **URL Path Segment** 规则做百分号解码
   （`url.PathUnescape` 语义），解码失败 → `URL_PARSE_ERROR`。

形式化（伪代码，对应 `spec/vectors/identity_vectors.yaml`）：

```
function splitCredential(credential):          # 正向代理 user:pass 整体
    idx = lastIndexOf(credential, ":")
    identity = credential[:idx] if idx >= 0 else credential
    token    = credential[idx+1:] if idx >= 0 else ""
    platform, account = splitIdentity(identity)
    return (token, platform, account)

function splitIdentity(identity):
    dot   = indexOf(identity, ".")   # -1 表示不存在
    colon = indexOf(identity, ":")
    if dot < 0 and colon < 0:   return (identity, "")
    if dot < 0:                 return (identity[:colon], identity[colon+1:])
    if colon < 0:               return (identity[:dot],   identity[dot+1:])
    if dot < colon:             return (identity[:dot],   identity[dot+1:])
    else:                       return (identity[:colon], identity[colon+1:])
```

## 3. 空值语义

- **Platform 为空** → 使用 `Default` 平台。
- **Account 为空** → 在平台内随机路由，**不建租约**。
- Platform 名称在创建/更新时先 trim，再校验：非空、全局唯一、不可命中保留字
  （`Default` / `api`），且不得包含 `.:|/\@?#%~` 与空格、tab、换行、回车。

## 4. 反向代理路径格式

```
/TOKEN/<identity>/<protocol>/<host>/<path...>?query
```

- `<protocol>` 必须为 `http` 或 `https`（不区分大小写，统一小写比较），
  否则 → `INVALID_PROTOCOL`。
- `<host>` 不能为空且必须合法，否则 → `INVALID_HOST`。
- 路径至少需要 `protocol/host` 两段（含 Token 段共 4 段），不足 → `URL_PARSE_ERROR`。
- 空路径（去掉 `/` 后为空）→ `AUTH_FAILED`（认证阶段失败优先）。
- 若 Account 含 `/` `?` `#` 等路径保留字符，调用方**必须**仅对身份段
  按 Path Segment 规则编码后再拼接；
  若输入导致路径分段错位，实现可在不同校验阶段失败，返回任一解析类错误
  （`URL_PARSE_ERROR` / `INVALID_PROTOCOL` / `INVALID_HOST`）都视为符合要求。

## 5. IdentityPolicy（解析链）

线协议只解决"身份串从哪来"；**Account 最终如何确定**由 `IdentityPolicy`
资源决定（见 [resources/identitypolicy.md](./resources/identitypolicy.md)）。
默认链（`default`）：

1. `header: X-Egress-Account`（HTTP 类入口；SOCKS5 上自动跳过）
2. `wire: {}`（线协议中的 Account）
3. `onMiss: random`（未命中则随机路由；`reject` → 403 `ACCOUNT_REJECTED`）

优先级：`X-Egress-Account` 请求头 > URL 身份段的 Account > 请求头提取规则。

## 6. 例子

完整例子表见 [vectors/identity_vectors.yaml](./vectors/identity_vectors.yaml)。
摘要：

| 输入 | Platform | Account |
|---|---|---|
| `Nimbus.Tom:egress-123456`（正向 `user:pass`） | `Nimbus` | `Tom` |
| `.Tom:egress-123456` | 空 → `Default` | `Tom` |
| `Nimbus:egress-123456` | `Nimbus` | 空 → 随机路由 |
| `MyHub.bEA:234:egress-123456` | `MyHub` | `bEA:234`（Account 可含 `:`） |
| `/egress-123456/MyHub.ops%2Fprod%3Feu%231/...`（反向） | `MyHub` | `ops/prod?eu#1`（解码后） |
