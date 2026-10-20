# 事件 Schema（Signal / RequestLog）

> 状态：**stable**。Event schema 对外稳定，外部 Sink 与分析工具依赖它；
> 破坏性变更必须走 RFC。

## 1. Signal

Signal 是一次**网络事实**：被动流量、主动探测、资源下载统一成这一种不可变
值类型，经 `SignalBus` 扇出给 HealthProjector、MetricsSink、RequestLogSink、
CacheSink。各消费者相互独立——删掉任何一个，数据面零感知。

```
Signal = Egress  { node, at, ip? }                        // ip=None 即失败
       | Latency { node, at, domain, ms?, authority }      // ms=None 即失败
       | Traffic { node, at, platform, ok, tlsMs?, bytesUp, bytesDown, breakerExempt }
```

| 变体 | 字段 | 类型 | 说明 |
|---|---|---|---|
| `Egress` | `node` | NodeHash | 产生事实的节点 |
| | `at` | timestamp | 事实发生时间 |
| | `ip` | string? | 观测到的出口 IP；`null` 即出口探测失败 |
| `Latency` | `node` | NodeHash | 节点 |
| | `at` | timestamp | 时间 |
| | `domain` | string | 探测目标域名 |
| | `ms` | float? | 延迟毫秒；`null` 即失败 |
| | `authority` | bool | `true` 表示该样本计入权威桶（TD-EWMA 常驻区），桶 key 由 `domain` 的 eTLD+1 决定 |
| `Traffic` | `node` | NodeHash | 节点 |
| | `at` | timestamp | 时间 |
| | `platform` | PlatformID | 所属平台（用于 `passiveBreaker` 豁免判定） |
| | `ok` | bool | 网络层建连成功即 `true`（即便 HTTP 500）；握手失败/超时/RST 为 `false` |
| | `tlsMs` | float? | TLS 握手耗时（隧道流量：ClientHello 到首次读；反向代理：`httptrace` TLS 钩子；非 TLS 流量不计） |
| | `bytesUp` | int | 客户端 → 上游字节数 |
| | `bytesDown` | int | 上游 → 客户端字节数 |
| | `breakerExempt` | bool | 被动熔断豁免：Platform `passiveBreaker: off` 时数据面在发射时置 `true`，HealthProjector 跳过该失败计数 |

**序列化约定**：JSON，`type` 字段取 `egress` / `latency` / `traffic`；
时间统一为 Unix 纳秒（`at_ns`）。

## 2. RequestLog

每条代理请求一条记录。payload 抓取为可选开关，默认关闭。

| 字段 | 类型 | 说明 |
|---|---|---|
| `ts` | int（Unix 纳秒） | 请求开始时间 |
| `proxyType` | enum | `http-forward` \| `reverse` \| `socks5-forward` |
| `platform` | string | 平台名（解析后的；线协议名，非 UUID） |
| `account` | string | 账号（空表示随机路由） |
| `target` | string | 目标（host 或完整 URL，由入口决定） |
| `node` | string | 节点 hash（未分配时为空） |
| `egressIP` | string | 出口 IP（未知时为空） |
| `netOk` | bool | 网络层是否成功 |
| `httpStatus` | int | HTTP 状态码（隧道/SOCKS5 无意义时为 0） |
| `stage` | string | 上游/网络失败发生的阶段（如 `dial` / `request` / `tunnel`） |
| `egressError` | string | 逻辑代理错误码，见 [errors.md](./errors.md)（如 `UPSTREAM_TIMEOUT`；成功时为空） |
| `durationMs` | int | 请求总耗时毫秒 |
| `bytesUp` | int | 客户端 → 上游字节数（含头含体） |
| `bytesDown` | int | 上游 → 客户端字节数（含头含体） |

可选 payload 字段（开关默认关闭；开启后按配置截断并标记）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `reqHeaders` / `reqHeadersTruncated` | bytes / bool | 请求头 |
| `reqBody` / `reqBodyTruncated` | bytes / bool | 请求体 |
| `respHeaders` / `respHeadersTruncated` | bytes / bool | 响应头 |
| `respBody` / `respBodyTruncated` | bytes / bool | 响应体 |

**隐私说明**：Account 默认进入日志（用于路由匹配一致性）；进入日志、审计
或外部存储时，应由部署方配置脱敏、最小化留存与访问控制策略，并遵循适用
法律法规。

## 3. 指标（Metrics）事件

Dashboard 指标保留以下项（实时曲线用内存 ring buffer，历史按小时桶落库）：

吞吐、连接数、流量、请求数与成功率、访问延迟分布、探测次数、节点数、
平台节点数、租约数、租约存活分布。

Prometheus、OTel、Webhook 作为 **Sink** 扩展提供，不进内核
（见 [ext-abi.md](./ext-abi.md)）。
