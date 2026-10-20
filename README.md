# egress

**egress** 是一个智能代理池网关：Go 单二进制，把零散的代理订阅汇聚成一个
支持**会话保持（粘性路由）**的多协议代理网关。

- **数据面**：HTTP 正向代理、SOCKS5 正向代理、HTTP 反向代理三种入口。
- **核心能力**：同一 `(Platform, Account)` 在租约期内出口 IP 不变（重启也不变）；
  按平台配置的过滤器与分配策略在节点池中路由；被动流量 + 主动探测驱动节点健康。
- **控制面**：声明式资源（Platform / Source / IdentityPolicy / Recipe / Endpoint）
  通过 REST API 或 `egress.yaml` 管理；自带 WebUI。
- 本项目参考并衍生自 [Resin](https://github.com/Resinat/Resin)（MIT），
  身份线协议 V1、错误契约与控制面 API 路径与其保持兼容。详见 [LICENSE](./LICENSE)。

## 四个承诺

| # | 承诺 | 说明 |
|---|---|---|
| P1 | **同账号同出口 IP** | `(Platform, Account)` 在租约期内出口 IP 不变，重启也不变（Lease 锚定 EgressIP，弱持久化） |
| P2 | **热路径 O(1)、无 I/O、无全局锁** | 过滤只在冷路径编译进 View；热路径只允许纯函数扩展 |
| P3 | **单二进制、零外部依赖** | SQLite 内置；扩展按编译期模块装入 |
| P4 | **两步可用** | 启动 → 填订阅；`Default` 平台、默认身份链、默认配置永远存在 |

任何破坏上述承诺的改动都必须走 RFC（见 [rfcs/README.md](./rfcs/README.md)）。

## 快速开始

**第 1 步：启动**

```bash
export EGRESS_ADMIN_TOKEN=<redacted>      # 控制面管理令牌
export EGRESS_PROXY_TOKEN=<redacted>    # 代理认证令牌
./egress
```

**第 2 步：填订阅**

打开 WebUI（默认 `http://127.0.0.1:2260/ui/`），或调用 API 添加一个节点来源：

```bash
curl -X POST http://127.0.0.1:2260/api/v1/sources \
  -H "Authorization: Bearer <redacted>" \
  -H "Content-Type: application/json" \
  -d '{"name":"main","type":"remote","url":"https://example.com/sub.txt"}'
```

然后把客户端的代理指向 `127.0.0.1:2260`，用身份
`Default.你的账号:proxy-secret`（HTTP Basic 用户名 / SOCKS5 用户名）
即可开始使用。

## 文档索引

| 文档 | 内容 |
|---|---|
| [spec/identity.md](./spec/identity.md) | 身份线协议 V1：三入口的身份位置、Token 规则与切分算法 |
| [spec/errors.md](./spec/errors.md) | `X-Egress-Error` 枚举与 HTTP / SOCKS5 映射 |
| [spec/resources/](./spec/resources/) | Platform / Source / Endpoint / IdentityPolicy / Recipe 的 YAML schema |
| [spec/events.md](./spec/events.md) | Signal / RequestLog 事件 schema |
| [spec/openapi.yaml](./spec/openapi.yaml) | 控制面 API（spec-first：先改 spec，再生成代码） |
| [spec/ext-abi.md](./spec/ext-abi.md) | 扩展点接口与稳定级别（`experimental` / `stable`） |
| [spec/vectors/](./spec/vectors/) | conformance 测试向量（身份解析 / 健康迁移 / P2C 打分 / 错误场景） |
| [docs/arch.md](./docs/arch.md) | 一屏架构图 |
| [docs/adr/](./docs/adr/) | 架构决策记录 |
| [rfcs/README.md](./rfcs/README.md) | RFC 流程：什么改动必须走 RFC |

## 兼容性说明

egress 早期参考 Resin 的线协议设计，**现已独立演进，不保持兼容**：
响应头为 `X-Egress-*`，默认身份头为 `X-Egress-Account`，环境变量仅有
`EGRESS_*` 前缀，默认数据目录为 `/var/{lib,cache,log}/egress`。

## 构建与运行

### 本地构建

```bash
export PATH=$HOME/workspace/.tools/go/bin:$PATH   # 如 go 不在 PATH 中
go build -o egress ./cmd/egress
```

需要 Go ≥ 1.26（`go.mod` 要求）。SQLite 驱动是纯 Go（modernc），`CGO_ENABLED=0`
也可构建。

### 运行（两步可用）

```bash
export EGRESS_ADMIN_TOKEN=<你的管理 token>   # 控制面 API 的 Bearer token，必填
export EGRESS_PROXY_TOKEN=<你的代理 token>   # 可为空字符串（空 = 无认证模式）
./egress
```

第 1 步启动后，第 2 步填订阅（二选一）：

- 声明式文件：`EGRESS_CONFIG=/path/to/egress.yaml ./egress`（示例见
  [egress.yaml.example](./egress.yaml.example)）
- 或调 API：`curl -X POST http://127.0.0.1:2260/api/v1/sources \
  -H "Authorization: Bearer <EGRESS_ADMIN_TOKEN>" -d '{...}'`

`Default` 平台、默认身份链永远存在，开箱即用。

### 测试

```bash
go test ./...                                # 白盒（单元 + 规范向量执行器）
go test -tags "with_quic with_utls" ./...    # 官方 tag 配置同上
go test -tags e2e ./e2e                      # 黑盒端到端（拉起真二进制，约 20s）
```

黑盒套件（e2e/）以子进程运行真实二进制，经线网驱动 HTTP forward / SOCKS5 /
管理 API，断言订阅同步→探测→可路由→转发→请求日志落盘→重启重放全链路；对端
（echo、vmess 入站）均为本地回环，无需外网。

### 端口

| 端口 | 服务 | 环境变量 |
|---|---|---|
| 2260 | 默认入口：HTTP 正向代理 + SOCKS5（首字节分流）+ 反向代理 + 控制面 API + WebUI `/ui/` | `EGRESS_PORT` |

默认入口由环境变量定义，运行时合成、只读、不出现在存储中；额外入口（独立端口、能力受限）用
Endpoint 资源声明（`POST /api/v1/endpoints`，能力标志见 spec/resources/endpoint.md）。

全部环境变量见 [.env.example](./.env.example)。

### Docker

```bash
docker build -t egress .
docker run -d --name egress \
  -p 2260:2260 \
  -e EGRESS_ADMIN_TOKEN=<admin> -e EGRESS_PROXY_TOKEN=<proxy> \
  -v egress-state:/var/lib/egress -v egress-cache:/var/cache/egress \
  -v egress-logs:/var/log/egress \
  egress
```

### 常用 flag

```bash
./egress --version       # 打印版本
./egress --config-test   # 只校验环境变量配置，不启动
```

优雅关闭：`SIGINT` / `SIGTERM` → 停监听器 → 停运行时 → flush
cache/metrics/请求日志 → 关库。
