# Endpoint（资源）

> `apiVersion: egress/v1`，`kind: Endpoint`。
> Endpoint 声明一个自定义入站监听器：端口、启用哪些协议入口、是否允许
> 管理/API。环境变量定义的**默认入口**在运行时合成，永不存入资源。

```yaml
apiVersion: egress/v1
kind: Endpoint
metadata:
  name: socks-only
spec:
  port: 1080
  enabled: true
  allowManagement: false     # 是否允许控制面 API / WebUI
  allowProxy: true           # 是否允许代理流量
  requireProxyAuthInfo: false # EGRESS_PROXY_TOKEN 为空时，是否仍强制客户端发送代理认证信息
  allowHTTPForward: false
  allowHTTPReverse: false
  allowSOCKS5: true
```

## 字段表

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `metadata.name` | string | 是 | — | 入口名，全局唯一 |
| `spec.port` | int | 是 | — | 监听端口 |
| `spec.enabled` | bool | 否 | `true` | 是否启用 |
| `spec.allowManagement` | bool | 否 | `false` | 允许控制面 API 与 WebUI（关闭时这些路径返回 404；`/healthz` 始终可用） |
| `spec.allowProxy` | bool | 否 | `true` | 允许代理流量 |
| `spec.requireProxyAuthInfo` | bool | 否 | `false` | Token 为空时仍强制客户端发送代理认证信息；HTTP 正向缺认证信息 → 407，SOCKS5 仅接受 `0x02` 且用户名密码非空（不校验密码内容）。空 Token 下只用于强制携带身份，不构成安全认证；开启后空认证不再视为 Default 平台请求 |
| `spec.allowHTTPForward` | bool | 否 | `true` | 允许 HTTP 正向代理 |
| `spec.allowHTTPReverse` | bool | 否 | `true` | 允许 HTTP 反向代理 |
| `spec.allowSOCKS5` | bool | 否 | `true` | 允许 SOCKS5 |

## 分流规则

- 每个接入点在 TCP 层按**首字节**分流：`0x05` → SOCKS5；其余 → HTTP 入口，
  再区分控制面 API、WebUI、HTTP 正向代理与反向代理。
- `/healthz` 在所有接入点始终可用，无需认证。

## 说明

- **Ingress 封闭为三种**（HTTP 正向、SOCKS5、HTTP 反向），不开放为 SPI：
  新入口协议走 RFC 进内核。理由：入口在热路径上；入口必须能承载
  `Platform.Account` 身份，而 WireGuard、TProxy 这类协议做不到。
