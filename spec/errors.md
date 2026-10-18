# 错误契约（Error Contract）

> 状态：**stable**。错误码与映射关系保持与 Resin 兼容；新增错误码必须走 RFC。
> 完整错误场景向量见 [vectors/error_scenarios.yaml](./vectors/error_scenarios.yaml)。

代理错误同时使用标准 HTTP 状态码与 `X-Resin-Error` 响应头（程序化识别），
Body 为纯文本（`text/plain; charset=utf-8`）的人类可读描述。

- `X-Resin-Error` 头名在 V1 中**保持不变**（兼容 Resin 客户端）。
- 407 响应附加 `Proxy-Authenticate: Basic realm="Resin"` 头。
- SOCKS5 入口不返回 HTTP 状态码或 `X-Resin-Error` 头，使用 RFC1928/RFC1929
  协议回复；更细粒度的 `resin_error` / `upstream_stage` 只记录在请求日志中。

## 1. HTTP 入口：X-Resin-Error 枚举与状态码

### 1.1 认证阶段

| X-Resin-Error | HTTP | 场景 | 说明 |
|---|---|---|---|
| `AUTH_REQUIRED` | 407 | PROXY_TOKEN 缺失或格式错误（HTTP 正向） | 未提供 `Proxy-Authorization` 头或格式不正确；附加 `Proxy-Authenticate` 头 |
| `AUTH_FAILED` | 403 | Token 校验失败（HTTP 正向） | 格式正确但 Token 不匹配 |
| `AUTH_FAILED` | 403 | Token 缺失或不匹配（反向代理） | 路径首段 Token 为空或不等于配置的 Token；空路径也归此类 |

### 1.2 反向代理 URL 解析

| X-Resin-Error | HTTP | 场景 | 说明 |
|---|---|---|---|
| `URL_PARSE_ERROR` | 400 | 路径段不足 | 至少需要 `protocol/host` 两段（连 Token 共 4 段） |
| `URL_PARSE_ERROR` | 400 | 身份段解码失败 | Path Segment 百分号解码失败 |
| `INVALID_PROTOCOL` | 400 | protocol 无效 | 必须为 `http` 或 `https` |
| `INVALID_HOST` | 400 | host 无效 | host 为空或含非法字符 |

### 1.3 路由阶段

| X-Resin-Error | HTTP | 场景 | 说明 |
|---|---|---|---|
| `PLATFORM_NOT_FOUND` | 404 | Platform 不存在 | 解析出的 Platform 名找不到对应 Platform |
| `ACCOUNT_REJECTED` | 403 | Account 提取失败且策略为拒绝 | IdentityPolicy `onMiss: reject` |
| `NO_AVAILABLE_NODES` | 503 | 无可用节点 | Platform 的可路由集合为空，无法分配节点 |

### 1.4 上游连接 / 请求

| X-Resin-Error | HTTP | 场景 | 说明 |
|---|---|---|---|
| `UPSTREAM_CONNECT_FAILED` | 502 | Dial 上游失败 | CONNECT 拨号阶段失败（连接被拒、网络不可达、RST 等） |
| `UPSTREAM_TIMEOUT` | 504 | 上游超时 | 连接上游或等待上游响应超时（含 context deadline） |
| `UPSTREAM_REQUEST_FAILED` | 502 | 上游请求失败 | 拨号成功后的读写失败（HTTP 正向非 CONNECT 路径、反向代理统一归此类） |

连通性判定：**网络层建连成功即为 ok**（即便 HTTP 返回 500）；
握手失败、超时、RST 为 fail。客户端主动取消（`context.Canceled`）不计为
节点失败，不产生代理错误。

### 1.5 内部错误

| X-Resin-Error | HTTP | 场景 |
|---|---|---|
| `INTERNAL_ERROR` | 500 | 未分类的内部错误（路由层未知错误等） |

## 2. SOCKS5 协议回复映射

SOCKS5 正向代理只支持 `CONNECT`。所有路由/上游类错误在协议面统一暴露为
`REP=0x01`（GENERAL FAILURE）。

| 场景 | 协议回复 | 对应 X-Resin-Error（仅记日志） |
|---|---|---|
| 客户端未提供可接受的认证方法 | 方法协商回复 `0x05 0xFF` | `AUTH_REQUIRED` |
| RFC1929 用户名密码认证失败 | 子协商回复 `0x01 0x01` | `AUTH_FAILED` |
| 命令不是 `CONNECT` | reply `REP=0x07` | — |
| 地址类型不支持 | reply `REP=0x08` | — |
| 路由失败 / 无可用节点 / 拨号失败 / 超时 / 向客户端写成功回复失败 | reply `REP=0x01` | `PLATFORM_NOT_FOUND` / `NO_AVAILABLE_NODES` / `UPSTREAM_CONNECT_FAILED` / `UPSTREAM_TIMEOUT` |
| 成功建立隧道 | reply `REP=0x00`，进入原始双向 TCP 隧道 | — |

## 3. 隧道特殊行为

- HTTP 正向 `CONNECT` 成功 → `HTTP/1.1 200 Connection Established`。
- SOCKS5 成功 → `REP=0x00`。
- 隧道建立后不再介入更高层协议语义；隧道建立后的网络错误不再生成
  HTTP 响应或新的 SOCKS5 reply，连接直接中断。

## 4. 错误响应示例

正向代理认证失败：

```
HTTP/1.1 407 Proxy Authentication Required
Proxy-Authenticate: Basic realm="Resin"
X-Resin-Error: AUTH_REQUIRED
Content-Type: text/plain; charset=utf-8

Proxy authentication required
```

反向代理无可用节点：

```
HTTP/1.1 503 Service Unavailable
X-Resin-Error: NO_AVAILABLE_NODES
Content-Type: text/plain; charset=utf-8

No available nodes for routing
```
