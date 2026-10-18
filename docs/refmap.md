# 参考实现 → 新架构映射（refmap）

> 参考实现：`~/workspace/resin-reference`（Resin，module `github.com/Resinat/Resin`，Go 1.25，约 221 个 Go 文件）
> 设计文档：`~/workspace/user/files/DESIGN-final.md`（下文简称"设计"）
> 本文档只读参考实现、未做任何修改。所有文件名、函数名、参数值均从代码原文摘录。

---

## 1. 包映射表

| 参考实现路径 | 内容摘要 | 新架构目标包 | 备注 |
|---|---|---|---|
| `cmd/resin` | `main.go`（入口）+ `app_runtime.go`（装配与启停顺序）+ `endpoint_runtime.go`（多 Endpoint 监听管理）+ `inbound_mux.go`（单端口多路复用：forward/反代/控制面/tokenAPI 分发）+ `inbound_demux.go`（HTTP/SOCKS5 嗅探分流） | `cmd/egress` 组合根 | 设计 §16 明确 cmd 只做"装配、启停顺序（唯一的顺序约定）"；`inbound_mux.go` 的分发规则（`shouldRouteForwardProxy` / `shouldRouteControlPlane` / `shouldRouteTokenAPI`）是行为契约，建议原样搬入 `internal/adapters/http` 或 `socks5` |
| `internal/api` | REST 控制面：`server.go`（全部路由注册）、各 `handler_*.go`、鉴权/限流中间件、错误映射、WebUI 嵌入（`webui.go:registerEmbeddedWebUI`） | `internal/adapters/http`（控制面 API） | 注意：handler 直接依赖 `service.ControlPlaneService` **具体类型**；新架构要求"控制面只调用 `app/control`"，需先抽接口。另见 §2.8：新设计 §14 的资源划分与现状路由有出入 |
| `internal/buildinfo` | ldflags 版本号（`Version/GitCommit/BuildTime`，VCS 回退） | `cmd/egress` 或 `internal/adapters/version` | 新架构无对应包，新建小包即可 |
| `internal/config` | `env.go`（全部 `RESIN_*` 读取+校验）+ `runtime.go`（`RuntimeConfig` 热更新配置+默认值）+ `auth_version.go` / `token_strength.go` | `internal/ports`（配置端口/接口）+ `internal/adapters`（env 读取实现） | `RuntimeConfig` 的字段名与 JSON key 是 API 契约（`GET /system/config`）；设计 §17：`RESIN_DEFAULT_PLATFORM_*` 兼容一个大版本 |
| `internal/geoip` | `geoip.go:Service`（MMDB/sing-box 双 reader，RWMutex 热替换，robfig/cron 定时更新） | `internal/adapters/geoip` | `ports` 层只需 `Lookup(netip.Addr) string` 接口（`GeoReader`） |
| `internal/metrics` | `collector.go`（事件收集）+ `bucket.go`（小时桶聚合）+ `realtime.go`（`RealtimeRing` 环形缓冲）+ `repo.go`（metrics.db 建表）+ `manager.go` | `internal/adapters/metrics` | 设计未决 #3：`metrics.db` 是否并入请求日志库，待定 |
| `internal/model` | DTO：`Endpoint`、`AccountHeaderRule` 等 | 按 spec-first 重建（`spec/openapi.yaml` 为准） | 不直接搬运，字段以 spec 为准 |
| `internal/netutil` | `domain.go:ExtractDomain`（eTLD+1）、`downloader.go`（订阅抓取）、`outbound_http.go` | `internal/core`（`ExtractDomain` 纯函数）+ `internal/ports.Fetcher` | `ExtractDomain` 是 P2C/TD-EWMA/反代 account 规则的公共分桶函数，必须原样保留语义 |
| `internal/node` | `entry.go:NodeEntry`（原子字段+`LatencyTable`）、`latency.go:LatencyTable`（TD-EWMA）、`hash.go:Hash` | `internal/core/node`（Health、step） | `LatencyTable` 是纯逻辑，可几乎原样迁入 `core`（零 I/O，已满足） |
| `internal/outbound` | `builder.go`（sing-box outbound 构建器）、`manager.go`、`secure_dns.go` | `internal/adapters/singbox` | 依赖 `sagernet/sing-box v1.12.21` + `anytls` 等，版本钉死 |
| `internal/platform` | `platform.go:Platform`（过滤器+反代 3 字段+分配策略）、`routableview.go:RoutableView`（可路由视图快照） | `internal/core/view` + `IdentityPolicy`（`internal/core` 或 `app`） | 设计 §17：3 个反代字段 + `account_header_rules` → 每个 Platform 一个 `IdentityPolicy`；`RegionFilters`→`filter.attrs.geo.country.in`；`RegexFilters`→`filter.tags`（语法不变） |
| `internal/probe` | `manager.go:ProbeManager`（双优先级队列+worker 池+去重状态机）、`fetcher.go` | `internal/app/probe`（编排）+ `internal/ports.Fetcher` | `Fetcher` 签名 `func(hash node.Hash, url string) (body []byte, latency time.Duration, err error)` 可直接作为 port 定义 |
| `internal/requestlog` | `service.go`（BatchSink：队列+批量刷盘）、`repo.go`（滚动 sqlite）、`schema.go` | sink 实现 → `internal/adapters/*`；`BatchSink` 语义 → `internal/ports` | 队列满**丢弃**不阻塞热路径，是关键语义 |
| `internal/scanloop` | `scanloop.go:Run`（抖动定时器：首次立即执行，之后 `minInterval + [0, jitter)`） | `internal/ports.Scheduler` 的实现参考 | 新 Scheduler 的任务清单见 §2.5；注意**首次立即触发**语义 |
| `internal/service` | `ControlPlaneService`（平台/订阅/节点/租约/系统配置等全部用例） | `internal/app/control` | 设计约束：控制面只调用 `app/control`，不直接访问 `Store` |
| `internal/state` | `engine.go:StateEngine`（state.db+cache.db 双库）、`flush.go:CacheFlushWorker`、`migrations/` | `internal/adapters/sqlite` + `internal/ports.Store` | schema 用 golang-migrate 版本化；不可逆迁移不提供 down（设计 §13/迁移约束） |
| `internal/subscription` | `parser.go`（4814 行多格式解析器）+ `subscription.go`（订阅模型） | 解析 → `internal/adapters`（source provider）；调度 → `internal/app/sync` | 解析器是纯函数，可整体搬运 |
| `internal/testutil` | 测试脚手架 | 无对应 | 各包自带测试；conformance 向量放 `spec/vectors/`（设计 M1） |
| `internal/topology` | `pool.go:GlobalNodePool`（节点池+健康）、`subscription_scheduler.go`、`ephemeral_cleaner.go` | `internal/app/sync`（订阅同步）+ `internal/app/reconcile`；池状态机拆分到 `core/node` + `app` | `GlobalNodePool` 同时承担存储+健康+事件通知，新架构需拆开 |
| `webui/` | React 19 + Vite 7 控制台（`src/features/*` 10 个功能域） | `internal/adapters/webui`（静态资源嵌入） | 构建产物由 Go 二进制内嵌（`registerEmbeddedWebUI`），挂载 `/ui` |
| `docker/` | `entrypoint.sh`（目录初始化、权限、chown） | 部署资产 | 无代码对应；`RESIN_SKIP_CHOWN` 等仅部署用 |
| `scripts/` | `diagnose-resin-probe.sh`（探针诊断采集） | 运维脚本 | 无对应；用了 `RESIN_BASE_URL` 等脚本专用变量 |
| `.github/` | `workflows/release.yml`（Node 22 构建 WebUI → Go 1.25 编译 → 多架构 release+Docker 镜像）、`close-inactive-issues.yml` | CI 流程 | 复用思路：WebUI 先行构建再嵌入 Go 二进制 |

**依赖方向校验**（设计 §16）：参考实现中 `internal/api` → `internal/service`（具体类型）、`internal/proxy` → `internal/routing` → `internal/topology`/`internal/platform`，与新架构"core 不 import 任何其他层、控制面只调 app/control"**不一致**，重构时需逐层解耦（见 §3 风险）。

---

## 2. 行为细节清单

### 2.1 身份解析（线协议 V1）——设计 §17 要求保持兼容

**三个入口**（`RESIN_PROXY_TOKEN` 精确字符串匹配；为空则进入免鉴权模式）：

1. **HTTP 正向代理** — `internal/proxy/forward.go:ForwardProxy.authenticateV1`
   - 头：`Proxy-Authorization: Basic base64("<Platform><delim><Account>:<TOKEN>")`
   - `parseProxyAuthorizationCredentialV1`：scheme 大小写不敏感（`strings.EqualFold(authFields[0], "Basic")`），`base64.StdEncoding` 解码；格式不对 → `AUTH_REQUIRED`(407)
   - token 取**最右** `:` 右侧（`identity_v1.go:parseForwardCredentialV1` 用 `strings.LastIndexByte`）；token 不匹配 → `AUTH_FAILED`(403)
2. **SOCKS5** — `internal/proxy/socks5.go`（用户名/密码认证段）
   - username = `<Platform><delim><Account>`，password = TOKEN；`s.token != "" && string(password) != s.token` → 回写 `0x01 0x01` 失败
   - `RequireProxyAuthInfo`（endpoint 级开关）且 token 为空时，用户名/密码不得为空
3. **反向代理** — `internal/proxy/reverse.go:parsePathV1`
   - 路径：`/{token}/{identity}/{http|https}/{host}/{rest...}`；`strings.SplitN(path, "/", 5)`；每段 `url.PathUnescape`
   - **必须用 `r.URL.EscapedPath`**（注释原话：保留转义分隔符），否则 `%2F` 会被提前解码切错段
   - 空路径 → `AUTH_FAILED`；段数 <4 → `URL_PARSE_ERROR`；protocol 非 http/https → `INVALID_PROTOCOL`；host 空或非法 → `INVALID_HOST`

**切分规则** — `internal/proxy/identity_v1.go:parseV1PlatformAccountIdentity`：
- 分隔符为 `.` 或 `:`，**取先出现者**切分（`dot < colon` 用 dot，否则用 colon；只其一存在则用其一）
- 两者都不存在 → `platform=identity, account=""`
- 因此 Platform 名中不能含 `.`/`:`（否则会被切错）——隐含约束

**Token 校验规则** — `internal/config/env.go:ValidateProxyTokenForV1`：
- 不得包含 `.:|/\@?#%~`（`v1ProxyTokenForbiddenChars`）及空白 ` \t\r\n`
- 不得为保留值 `api` / `healthz` / `ui`（与控制面路径冲突，`env.go:182`）
- `zxcvbn` 弱口令检测（score<3 告警，不阻断）：`token_strength.go:IsWeakToken`
- 免鉴权模式（`RESIN_PROXY_TOKEN` 为空）：`parseForwardCredentialV1WhenAuthDisabled` — 无 credential 也可过；但 endpoint 开启 `RequireProxyAuthInfo` 时仍要求 identity 非空

**控制面鉴权** — `internal/api/middleware.go:AuthMiddleware`：`Authorization: Bearer <RESIN_ADMIN_TOKEN>` 精确匹配；`RESIN_ADMIN_TOKEN` 为空 = 免鉴权；失败 → 401 `UNAUTHORIZED`。

**单端口多路复用** — `cmd/resin/inbound_mux.go`（endpoint 级能力开关 `model.Endpoint` 控制）：
- CONNECT 方法或绝对 URI → 正向代理（需 `AllowProxy && AllowHTTPForward`）
- `/`、`/healthz`、`/api/*`、`/ui/*` → 控制面（`AllowManagement`，`healthz` 除外）
- `/{token}/api/...` → tokenAction（需 `AllowProxy`）
- 其余 → 反向代理（需 `AllowProxy && AllowHTTPReverse`；token 不匹配直接 403 `AUTH_FAILED`）
- 能力未开 → 403 `ENDPOINT_CAPABILITY_DISABLED`

### 2.2 P2C 打分 —— `internal/routing/random.go`

- 从 `plat.View()`（可路由视图）随机取 2 个候选：`view.RandomPick(rng)`，rng 来自 `sync.Pool` 的 `rand.NewPCG`；`size==1` 直接返回；第二候选**最多重试 3 次**取与第一不同者，仍相同则直接用第一
- `compareLatencies` 三级"同口径"比较（`latency_eval.go`）：
  1. **目标域名桶**：双方 `LatencyTable` 都有目标 eTLD+1 域名，且 `now - LastUpdated <= p2c_latency_window`（默认 **10min**）→ 比较各自 EWMA
  2. **共同 Authority 均值**：对 `latency_authorities`（默认 `gstatic.com, google.com, cloudflare.com, github.com`）中**双方都有近期数据**的域名集合，分别取均值再比较（`averageComparableAuthorityLatencies`：交集域名各自求和/计数）
  3. 都无 → 双方 latency=0
- `calculateScore`（越小越好，平局选 h2）：
  - `leaseCount` 口径 = `IPLoadStats.Get(entry.GetEgressIP())` —— 该**出口 IP 当前活跃租约数**（注意不是节点租约数；IP 未知时为 0）
  - `latency <= 0` → score = `leaseCount`（纯负载）
  - `PreferLowLatency` → score = latency；`PreferIdleIP` → score = leaseCount；`Balanced`（默认）→ `(leaseCount+1) * latency`
- 热路径注释：`randomRoute` **信任 View 快照**，不做二次可用性校验；选中后节点被删的竞态由调用方 `RouteRequest` 处理

### 2.3 TD-EWMA —— `internal/node/latency.go`

- 公式（`tdEWMAUpdate`）：`weight = exp(-Δt / decayWindow)`；`newEwma = old*weight + sample*(1-weight)`；**首次观测直接取样**（`Ewma=latency`）
- `DecayWindow` = `latency_decay_window`，默认 **10min**；`decay<=0` 时防除零取 1 秒
- **Authority 常驻区**：`authorities []latencySlot` 无界 append，只增不驱逐（更新时先 pop 再重插）
- **普通区 LRU**：固定数组 `regular[maxEntries]`，默认 **12**（`RESIN_MAX_LATENCY_TABLE_ENTRIES`），env 校验上限 **32**；满时驱逐 `lastAccessNs` 最小者；`UpdateClassified` 返回 `(wasEmpty, evictedDomain, evicted)`
- **eTLD+1 分桶**：`netutil.ExtractDomain` — `publicsuffix.EffectiveTLDPlusOne`，失败（IP/localhost/裸 TLD）则**回退原文**；`pool.RecordLatency` 注释明确 rawTarget 经此归一化
- 键：`xxh3.HashString(domain)` 64-bit，**无冲突处理**（注释承认极低概率冲突，内存/性能权衡）
- 读 touch 节流：`latencyReadTouchMinInterval = 100ms`，`GetDomainStats` 内限频更新 `lastAccessNs`
- `AverageEWMAForDomainsMs`：多域名 EWMA 毫秒均值（metrics 快照用）

### 2.4 健康/熔断

- `MaxConsecutiveFailures` 默认 **3**（`config/runtime.go:NewDefaultRuntimeConfig`），经函数闭包热更新（`topology/pool.go:currentMaxConsecutiveFailures`）；`<=0` 视为关闭熔断
- `topology/pool.go:RecordResult`：成功 → `FailureCount` 清零 + `CircuitOpenSince` 清零；失败 → `FailureCount+1`，`>=阈值` 则 `CircuitOpenSince.CompareAndSwap(0, now)` 开熔断；**仅熔断状态变化时** `notifyAllPlatformsDirty(hash)`（重建相关平台视图）
- **被动熔断豁免**（`pool.go:RecordPassiveResult`）：平台 `PassiveCircuitBreakerDisabled=true` 时，**失败的被动流量反馈被忽略，成功的仍计入**正向健康；被动上报经 `recordPassiveResultAsync` 起 goroutine 异步（`proxy/health.go`）
- **新节点初始即熔断**：`pool.go:AddNodeFromSub` — `entry.CircuitOpenSince.Store(createdAt.UnixNano())`，注释"must be proven healthy by probes"；需经探测成功（或被动成功）才解除。对应设计 §17 `CircuitOpenSince/FailureCount → Health 枚举 + since`：Health 需有"未验证"初态
- 探测间隔（`RuntimeConfig` 默认值）：`max_egress_test_interval` **24h**、`max_latency_test_interval` **1h**、`max_authority_latency_test_interval` **3h**；扫描 lookahead **15s**（`probe/manager.go:scanEgress/scanLatency`）；探测 URL `https://www.gstatic.com/generate_204`，egress 探测 `https://cloudflare.com/cdn-cgi/trace`
- 探测并发：`RESIN_PROBE_CONCURRENCY` 默认 **1000**，上限 10000；任务去重状态机（`taskFlagQueued/Running/Dirty/DirtyHigh/QueuedHigh`），高/普通双优先级队列，**两者非空时 10% 概率取普通队列**（`ChooseNormalWhenBoth` 默认）

### 2.5 调度 —— 5 个扫描协程 → 新 Scheduler 任务清单

统一节拍 `internal/scanloop`：`DefaultMinInterval=13s`、`DefaultJitterRange=4s` → 每次 **13–17s** 抖动；`Run` **首次立即执行**（`time.NewTimer(0)` + drain），之后按抖动间隔循环。

| # | 协程 | 位置 | 职责 |
|---|---|---|---|
| 1 | probe.scanEgress | `internal/probe/manager.go:286` | 遍历全池，`LastEgressUpdateAttempt + 24h − 15s <= now` 且有 outbound、未被订阅禁用 → 入队 egress 探测 |
| 2 | probe.scanLatency | `internal/probe/manager.go:292` | 普通域名 `1h − 15s` / Authority 域名 `3h − 15s` 到期 → 入队 latency 探测 |
| 3 | leaseCleaner | `internal/routing/lease_cleaner.go:26`（`newLeaseCleanerWithIntervals(router, 13s, 4s)`） | 清理过期租约 |
| 4 | ephemeralCleaner | `internal/topology/ephemeral_cleaner.go:47` | 驱逐超 `ephemeral_node_evict_delay_ns` 的 ephemeral 订阅节点（TOCTOU 双检） |
| 5 | subscriptionScheduler | `internal/topology/subscription_scheduler.go:77` | `lastChecked + update_interval − 15s(schedulerLookahead) <= now` 的订阅刷新；worker 数 = `GOMAXPROCS`；启动时 `ForceRefreshAllAsync` 全量刷新一次 |

另：GeoIP 更新为 cron `0 7 * * *`（`RESIN_GEOIP_UPDATE_SCHEDULE`，robfig/cron/v3）；`CacheFlushWorker`：`dirty >= 1000(CacheFlushDirtyThreshold)` **或** 距上次 flush `>= 5min(CacheFlushInterval)` 则刷盘（`internal/state/flush.go`）。

### 2.6 订阅解析 —— `internal/subscription/parser.go`（4814 行）

- 入口 `ParseGeneralSubscription`：`normalizeInput` → `parseSubscriptionContent`；未识别 → `tryDecodeBase64ToText`（StdEncoding/URLEncoding 均试）解码后再走一遍；仍未识别 → 报错 `unsupported format or no supported nodes found`
- **识别顺序**：JSON（sing-box `{"outbounds":[...]}` / Clash JSON `{"proxies":[...]}`）→ Clash YAML（`proxies:`）→ Surge（`[Proxy]` 段，含 WireGuard peer 段）→ URI 行 → 纯文本 `IP:PORT` / `IP:PORT:USER:PASS`（`parsePlainHTTPProxyLine`）
- URI 行 scheme（大小写不敏感，`strings.ToLower` 后前缀匹配）：`vmess://`（base64 JSON）、`vmess1://`、`vless://`、`trojan://`、`ss://`、`hysteria2://`、`hy2://`、`ssd://`（可展开多节点）、`socks://`、`tg://socks`、`https://t.me/socks`、`tg://http`、`https://t.me/http(s)`、`netch://`、`http(s)://`、`socks5(h)://`
- `supportedOutboundTypes`（16 种）：`socks, http, shadowsocks, vmess, trojan, wireguard, hysteria, vless, shadowtls, tuic, hysteria2, anytls, tor, ssh, naive`
- 行规则：空行与 `#` 开头行跳过；**注意**：`hysteria://` URI 行分支不存在（仅 hysteria2/hy2），但 Clash/sing-box 路径支持 `hysteria` 类型
- Clash→节点转换在 `convertClashProxyToNode`（约 450 行），含 TLS/传输层归一化

### 2.7 错误码

**代理面**（`internal/proxy/errors.go`，`X-Resin-Error` 响应头 + 纯文本 body；407 自动加 `Proxy-Authenticate: Basic realm="Resin"`）：

| X-Resin-Error | HTTP | 含义 |
|---|---|---|
| AUTH_REQUIRED | 407 | 缺认证 |
| AUTH_FAILED | 403 | token 错误 |
| URL_PARSE_ERROR / INVALID_PROTOCOL / INVALID_HOST | 400 | 反代路径非法 |
| PLATFORM_NOT_FOUND | 404 | 平台不存在 |
| ACCOUNT_REJECTED | 403 | account 提取失败且平台拒绝 |
| NO_AVAILABLE_NODES | 503 | 无可用节点 |
| UPSTREAM_CONNECT_FAILED / UPSTREAM_REQUEST_FAILED | 502 | 上游失败 |
| UPSTREAM_TIMEOUT | 504 | 上游超时 |
| INTERNAL_ERROR | 500 | 内部错误 |
| ENDPOINT_CAPABILITY_DISABLED | 403 | endpoint 未开对应能力（`inbound_mux.go`，非 errors.go） |

与设计 §15 表**完全一致**；SOCKS5 映射按设计 §15：`AUTH_REQUIRED→0x05 0xFF`、`AUTH_FAILED→0x01 0x01`、其余→`REP 0x01`。

**控制面**（`internal/api/errors.go` ← `service.ServiceError{Code, Message}`）：`INVALID_ARGUMENT→400`、`NOT_FOUND→404`、`CONFLICT→409`、其他→`500 INTERNAL`；另有 `401 UNAUTHORIZED`（鉴权中间件）、`413 PAYLOAD_TOO_LARGE`（`RESIN_API_MAX_BODY_BYTES`，默认 1MB）。响应体 `{error:{code,message}}` —— 与设计 §14 约定一致。

### 2.8 API 路径 —— `internal/api/server.go` 全部路由

`GET /healthz` 公开；其余 `/api/*` 需 `Bearer` 鉴权 + body 限流。★ 为控制面必须保留（对应新设计 §14 资源）：

- ★ `GET /api/v1/system/info`、`GET /api/v1/system/config`、`GET /api/v1/system/config/default`、`GET /api/v1/system/config/env`、`PATCH /api/v1/system/config`
- ★ platforms：`GET/POST /api/v1/platforms`、`GET/PATCH/DELETE /api/v1/platforms/{id}`、`POST /api/v1/platforms/preview-filter`、`POST /api/v1/platforms/{id}/actions/reset-to-default`、`POST /api/v1/platforms/{id}/actions/rebuild-routable-view`
- ★ endpoints：`GET/POST /api/v1/endpoints`、`GET/PATCH/DELETE /api/v1/endpoints/{id}`
- ★ leases：`GET/DELETE /api/v1/platforms/{id}/leases`、`GET/DELETE /api/v1/platforms/{id}/leases/{account}`、`GET /api/v1/platforms/{id}/ip-load`
- ★ subscriptions：`GET/POST /api/v1/subscriptions`、`GET/PATCH/DELETE /api/v1/subscriptions/{id}`、`POST /api/v1/subscriptions/{id}/actions/refresh`、`POST /api/v1/subscriptions/{id}/actions/cleanup-circuit-open-nodes`
- account-header-rules：`GET /api/v1/account-header-rules`、`PUT /api/v1/account-header-rules/{prefix...}`、`POST /api/v1/account-header-rules:resolve`、`DELETE /api/v1/account-header-rules/{prefix...}`（设计 §17：转为 IdentityPolicy 的 headerRule 步骤）
- ★ nodes：`GET /api/v1/nodes`、`GET /api/v1/nodes/{hash}`、`POST /api/v1/nodes/{hash}/actions/probe-egress`、`POST /api/v1/nodes/{hash}/actions/probe-latency`
- geoip：`GET /api/v1/geoip/status`、`GET/POST /api/v1/geoip/lookup`、`POST /api/v1/geoip/actions/update-now`
- ★ request-logs（→新 `/logs`）：`GET /api/v1/request-logs`、`GET /api/v1/request-logs/{log_id}`、`GET /api/v1/request-logs/{log_id}/payloads`
- ★ metrics（→新 `/metrics`）：realtime：`GET /api/v1/metrics/realtime/{throughput,connections,leases}`；history：`GET /api/v1/metrics/history/{traffic,requests,access-latency,probes,node-pool,lease-lifetime}`；snapshots：`GET /api/v1/metrics/snapshots/{node-pool,platform-node-pool,node-latency-distribution}`
- token 面（挂在 endpoint 入口，非 `/api`）：`POST /{token}/api/v1/{platform}/actions/inherit-lease`（`handler_token_action.go`，经 `inbound_mux.go:shouldRouteTokenAPI` 分发；token 错回 404 伪装）

### 2.9 环境变量 —— 全部 `RESIN_*`

| 变量 | 默认值 | 含义 |
|---|---|---|
| `RESIN_ADMIN_TOKEN` | （必填，可显式设空） | 控制面 `Bearer` token |
| `RESIN_PROXY_TOKEN` | （必填，可显式设空=免鉴权） | 线协议 token；禁字符见 §2.1；禁保留值 `api/healthz/ui` |
| `RESIN_AUTH_VERSION` | `V1` | 仅支持 V1 |
| `RESIN_PORT` | `2260` | 主端口 |
| `RESIN_LISTEN_ADDRESS` | `0.0.0.0` | 监听地址 |
| `RESIN_API_MAX_BODY_BYTES` | `1048576` | API body 上限 |
| `RESIN_CACHE_DIR` / `RESIN_STATE_DIR` / `RESIN_LOG_DIR` | `/var/cache/resin` / `/var/lib/resin` / `/var/log/resin` | cache.db / state.db / 请求日志目录 |
| `RESIN_MAX_LATENCY_TABLE_ENTRIES` | `12`（≤32） | TD-EWMA 普通区大小 |
| `RESIN_PROBE_CONCURRENCY` | `1000`（≤10000） | 探测 worker 数 |
| `RESIN_PROBE_TIMEOUT` | `15s` | 单次探测超时 |
| `RESIN_RESOURCE_FETCH_TIMEOUT` | `30s` | 订阅/GeoIP 抓取超时 |
| `RESIN_GEOIP_UPDATE_SCHEDULE` | `0 7 * * *` | GeoIP cron |
| `RESIN_NODE_DNS_UPSTREAMS` | `https://doh.pub/dns-query,https://dns.alidns.com/dns-query,tls://223.5.5.5?sni=dns.alidns.com,local` | 节点出站 DNS |
| `RESIN_DEFAULT_PLATFORM_STICKY_TTL` | `168h`（7 天） | 租约粘性 TTL |
| `RESIN_DEFAULT_PLATFORM_REGEX_FILTERS` | `[]` | 默认平台正则过滤 |
| `RESIN_DEFAULT_PLATFORM_REGION_FILTERS` | `[]` | 默认平台地域过滤 |
| `RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_MISS_ACTION` | `TREAT_AS_EMPTY` | `TREAT_AS_EMPTY`/`REJECT` |
| `RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_EMPTY_ACCOUNT_BEHAVIOR` | `RANDOM` | `RANDOM`/`FIXED_HEADER`/`ACCOUNT_HEADER_RULE` |
| `RESIN_DEFAULT_PLATFORM_REVERSE_PROXY_FIXED_ACCOUNT_HEADER` | `` | `FIXED_HEADER` 时必填（换行分隔多 header 名） |
| `RESIN_DEFAULT_PLATFORM_ALLOCATION_POLICY` | `BALANCED` | `BALANCED`/`PREFER_LOW_LATENCY`/`PREFER_IDLE_IP` |
| `RESIN_PROXY_TRANSPORT_MAX_IDLE_CONNS` | `1024` | 出站传输池 |
| `RESIN_PROXY_TRANSPORT_MAX_IDLE_CONNS_PER_HOST` | `64` | （≤上者） |
| `RESIN_PROXY_TRANSPORT_IDLE_CONN_TIMEOUT` | `90s` | 空闲连接超时 |
| `RESIN_PROXY_BYPASS` | `[]` | 直连 bypass 规则（分隔符分隔） |
| `RESIN_REQUEST_LOG_QUEUE_SIZE` | `8192` | BatchSink 队列 |
| `RESIN_REQUEST_LOG_QUEUE_FLUSH_BATCH_SIZE` | `4096` | BatchSink 批量 |
| `RESIN_REQUEST_LOG_QUEUE_FLUSH_INTERVAL` | `5m` | BatchSink 定时刷盘 |
| `RESIN_REQUEST_LOG_DB_MAX_MB` | `512` | 单个滚动 DB 上限（触发 rotate） |
| `RESIN_REQUEST_LOG_DB_RETAIN_COUNT` | `2` | 保留滚动 DB 数 |
| `RESIN_METRIC_THROUGHPUT_INTERVAL_SECONDS` | `2` | realtime 采样间隔 |
| `RESIN_METRIC_THROUGHPUT_RETENTION_SECONDS` | `3600` | realtime 保留 |
| `RESIN_METRIC_CONNECTIONS_INTERVAL_SECONDS` | `15` | |
| `RESIN_METRIC_CONNECTIONS_RETENTION_SECONDS` | `18000` | |
| `RESIN_METRIC_LEASES_INTERVAL_SECONDS` | `5` | |
| `RESIN_METRIC_LEASES_RETENTION_SECONDS` | `18000` | |
| `RESIN_METRIC_BUCKET_SECONDS` | `3600` | 小时桶聚合 |
| `RESIN_METRIC_LATENCY_BIN_WIDTH_MS` | `100` | 延迟直方图 bin |
| `RESIN_METRIC_LATENCY_BIN_OVERFLOW_MS` | `3000` | 溢出 bin |

脚本专用（`scripts/diagnose-resin-probe.sh`，非主程序）：`RESIN_BASE_URL`、`RESIN_DIAGNOSTIC_DIR`、`RESIN_DOCKER_CONTAINER`、`RESIN_DOCKER_LOG_SINCE`、`RESIN_KEEP_DIAGNOSTIC`、`RESIN_LOG_FILE`。

`EGRESS_` 兼容建议：机械替换前缀即可，但 `RESIN_DEFAULT_PLATFORM_*` 按设计 §17 需"兼容一个大版本，启动时写入 Default 平台"（`platform.DefaultPlatformID = "00000000-0000-0000-0000-000000000000"`）。

### 2.10 持久化

**state.db**（golang-migrate 版本化，`internal/state/migrations/state/` 共 9 个迁移）：
- `system_config`（`id=1` 单行：`config_json, version, updated_at_ns`）
- `platforms`（`id, name UNIQUE, sticky_ttl_ns, regex_filters_json, region_filters_json, reverse_proxy_miss_action, allocation_policy, updated_at_ns`；迁移 000002/3/4/6 逐步加字段）
- `subscriptions`（`id, name, source_type, url, content, update_interval_ns, enabled, ephemeral, ephemeral_node_evict_delay_ns, created_at_ns, updated_at_ns`）
- `account_header_rules`（`url_prefix PK, headers_json, updated_at_ns`）
- `endpoints`（迁移 000007/000008：`id, port, enabled, allow_management, allow_proxy, require_proxy_auth_info, allow_http_forward, allow_http_reverse, allow_socks5`）

**cache.db**（`internal/state/migrations/cache/` 2 个迁移；可丢弃重建）：
- `nodes_static`（`hash PK, raw_options_json, created_at_ns`）
- `nodes_dynamic`（`hash PK, failure_count, circuit_open_since, egress_ip, egress_region, egress_updated_at_ns, last_latency_probe_attempt_ns, last_authority_latency_probe_attempt_ns, last_egress_update_attempt_ns`）
- `node_latency`（`(node_hash, domain) PK, ewma_ns, last_updated_ns`）
- `leases`（`(platform_id, account) PK, node_hash, egress_ip, created_at_ns, expiry_ns, last_accessed_ns`）
- `subscription_nodes`（`(subscription_id, node_hash) PK, tags_json`）

**metrics.db**（`internal/metrics/repo.go` 建表）：`metric_traffic_bucket`、`metric_request_bucket`、`metric_access_latency_bucket`、`metric_probe_bucket`、`metric_node_pool_bucket`、`metric_lease_lifetime_bucket`。

**请求日志**（滚动 sqlite，`requestlog.NewRepo(logDir, maxBytes=512MB, retainCount=2)`）：
- `request_logs`（30 列：`id PK, ts_ns, proxy_type, client_ip, platform_id/name, account, target_host, target_url, node_hash, node_tag, egress_ip, duration_ns, first_byte_duration_ns, net_ok, http_method, http_status, resin_error, upstream_stage/err_kind/err_no/err_msg, ingress/egress_bytes, payload_present, req/resp headers/body 长度与截断标记各 4 列）
- `request_log_payloads`（`log_id PK REFERENCES request_logs(id) ON DELETE CASCADE, req_headers, req_body, resp_headers, resp_body BLOB`——大字段分离）
- 7 个索引（`idx_request_logs_ts_id` 等）；查询跨**所有保留 DB**（`List queries all retained DBs`）

**BatchSink 语义**（`internal/requestlog/service.go` + `internal/state/flush.go`）：
- 请求日志：`chan` 队列（默认 8192），`EmitRequestLog` **非阻塞，满则丢弃**；`flushLoop` 按 batch（4096）或定时（5min）刷盘；`FlushNow`/`drainAndFlush` 支持优雅关闭
- cache 刷盘：`CacheFlushWorker` — `dirty >= 1000` **或** `5min` 未刷则执行；关闭前 final flush（`app_runtime.go:599`）；state.db 写走 `StateEngine` 脏集（`dirtyset.go`）

### 2.11 WebUI

- 技术栈：**Vite 7** + React 19 + TypeScript 5.9 + react-router-dom 6 + @tanstack/react-query 5 + react-hook-form/zod + zustand + recharts + i18next（中英双语）+ lucide 图标
- 构建嵌入：Dockerfile 先 `npm run build`，Go 经 `internal/api/webui.go:registerEmbeddedWebUI` 内嵌 dist，挂载 `/ui`（`/` 与 `/ui/*` 走控制面 mux，见 `inbound_mux.go:shouldRouteControlPlane`）
- 页面（`webui/src/app/routes.tsx`）：`/login`、`/dashboard`、`/platforms`、`/platforms/:platformId`、`/subscriptions`、`/nodes`、`/endpoints`、`/rules`、`/request-logs`、`/resources`（GeoIP）、`/system-config`
- API 调用点：`webui/src/lib/api-client.ts` — 基址 `VITE_API_BASE_URL`（默认同源）；`Authorization: Bearer <token>`（`features/auth/auth-store` 持久化）；错误体按 `{error:{code,message}}` 解析为 `ApiError`

---

## 3. 风险提示

### 3.1 设计 §17 迁移表逐项确认

| §17 迁移项 | 参考实现现状 | 风险/注意 |
|---|---|---|
| 3 反代字段 + `account_header_rules` → 每个 Platform 一个 `IdentityPolicy`；`ACCOUNT_HEADER_RULE` → headerRule 步骤 | `platform.go:37-40`（MissAction/EmptyAccountBehavior/FixedAccountHeader(s)）；`reverse.go` account 解析有**三层 fallback**：path identity → header rule（`rule_prefix.go` 前缀匹配）→ fixed header → miss action（`TREAT_AS_EMPTY`/`REJECT`） | fallback 顺序是行为契约，IdentityPolicy 步骤编排必须保留；另设计未决 #2：`X-Resin-Account` 与线协议 Account 冲突时 v0 保持**静默优先** |
| `RegionFilters` → `filter.attrs.geo.country.in` | 小写 ISO，支持 `!xx` **否定**前缀（`platform.go:33`） | 否定语义必须保留到 attrs 过滤 |
| `RegexFilters` → `filter.tags`（语法不变） | `node.TagFilter{Any, Must, MustNot}`：Must=AND、Any=OR、MustNot 一票否决；候选格式 `<订阅名>/<tag>`（`entry.go:MatchTagFilter`）；空规则+subLookup 非空 → 要求至少一个启用订阅 | 正则按行匹配（`regexp` 非锚定），大小写敏感；照抄 |
| `CircuitOpenSince/FailureCount` → Health 枚举 + since | **新节点创建即熔断态**（`pool.go:AddNodeFromSub`，"must be proven healthy by probes"） | Health 枚举必须有"未验证/初始熔断"初态，否则迁移丢失语义 |
| `RESIN_DEFAULT_PLATFORM_*` 兼容一个大版本 | `env.go` 7 个变量 → 启动写入 Default 平台 | `EGRESS_` 前缀做机械兼容映射 |
| cache.db 旧表 migration 转换；不可转视为缓存丢弃 | `node_latency.ewma_ns` 可直接转 TD-EWMA 初值 | 仅影响重探，可接受 |

**§14 vs §17 的张力**：§17 要求"API 路径保持兼容"，但 §14 把资源重划为 `/platforms /identity-policies /sources /recipes /nodes /leases /endpoints /logs /metrics /config /system`（现状是 `/api/v1/...` 且无 identity-policies/sources/recipes）。后续组需裁决：是双路径兼容，还是仅"线协议/错误码兼容、API 允许重定"。

### 3.2 发现的坑（并发与全局状态）

1. **热路径信任快照**：`randomRoute` 明确不做二次可用性校验；`RouteRequest` 的 `livePickAttempts=2`（一次+一次重试）是唯一的兜底。新 `core/select` 保持此假设。
2. **`scanloop.Run` 首次立即执行**：新 Scheduler 若语义改为"先等一周期"，则启动行为变化（订阅全量刷新、探测首轮会延迟 13s）。
3. **反代必须用 `EscapedPath`**：`parsePathV1` 依赖未解码路径切分；新 `adapters/reverse` 照抄。
4. **被动健康上报起 goroutine**（`recordPassiveResultAsync`）：高 QPS 下 goroutine 爆发；新实现建议走有界队列。
5. **SOCKS5 token 对比非恒定时间**（`string(password) != s.token`）：行为兼容前提下建议换 `subtle.ConstantTimeCompare`。
6. **租约决策原子性**：`routeSticky` 用 `xsync.Map.Compute` 做"检查-决策-写入"原子操作；新 `core/lease` 需同等原子语义，不能拆成"读+写"两步。
7. **探测任务去重位**（`taskFlagQueued/Running/Dirty...`）：`TriggerImmediate*` 与扫描器并发入队靠 CAS 去重；新 `app/probe` 保留"同一 (hash, kind) 不重复排队"语义。
8. **`LatencyTable` 读也加写锁**（`GetDomainStats` 更新 `lastAccessNs` 取 `t.mu.Lock()`）：高并发读存在锁竞争；100ms touch 节流是刻意为之的缓解。
9. **`xxh3` 域名键无冲突处理**：跨实现需用同一哈希（或接受极低概率串扰）；conformance 向量应覆盖。
10. **关闭顺序**（`app_runtime.go:550-599`）：endpoint → leaseCleaner → ephemeralCleaner → scheduler → probeMgr → geoSvc → requestlog → metrics → flushWorker(final flush) → DB 关。设计"cmd 是唯一的顺序约定"——此顺序即应写入新 `cmd/egress` 的文档注释。
11. **`internal/api` 直接依赖 `service` 具体类型**、多处 `atomic.Pointer` 跨包共享可变配置（`runtimeCfg *atomic.Pointer[config.RuntimeConfig]`）：新架构切到接口 + 不可变快照时，注意热更新语义（当前是"读时取最新指针"，替换即生效）。
12. **请求日志跨多滚动 DB 查询**：`List` 要合并 `retainCount` 个 DB 并做全局分页（`Fetch one extra row across retained DBs`）；新 `/logs` 实现注意。
13. **`inbound_demux.go` 的嗅探**：同一端口同时服务 HTTP 与 SOCKS5（首字节嗅探，`inboundDemuxSniffTimeout=15s`）；新 `adapters/socks5` + `adapters/http` 若拆端口，此兼容行为需显式决策。
