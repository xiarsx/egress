# Source（资源）

> `apiVersion: egress/v1`，`kind: Source`。
> Source 声明"从哪里拿节点"，驱动 SourceSyncer 产出 NodeSnapshot。
> Source 本身是资源，不参与热路径计算；其运行时产物是节点快照。

```yaml
apiVersion: egress/v1
kind: Source
metadata:
  name: main
spec:
  type: remote            # remote | local | <扩展 SourceProvider 名>
  url: https://example.com/sub.txt   # type=remote 时必填
  # content: |            # type=local 时必填：内联订阅内容
  #   vmess://...
  updateInterval: 5m      # 刷新间隔；remote 最短 30s
  enabled: true
  ephemeral: false        # true 时节点按 evictDelay 驱逐
  evictDelay: 24h         # ephemeral 源的节点驱逐延迟
```

## 字段表

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `metadata.name` | string | 是 | — | 来源名，全局唯一；节点 Tag 形如 `<sourceName>/<rawTag>` |
| `spec.type` | enum | 是 | — | `remote`（URL 下载）\| `local`（内联内容）\| 其他已注册的扩展 `SourceProvider` 名（spec/ext-abi.md §2；未注册 = 刷新报错并保留旧快照） |
| `spec.url` | string | `remote` 时必填 | — | 订阅 URL |
| `spec.content` | string | `local` 时必填 | — | 内联订阅内容 |
| `spec.updateInterval` | duration | 否 | TODO | 同步间隔；`remote` 不得短于 30s |
| `spec.enabled` | bool | 否 | `true` | 禁用后其快照不再参与汇聚（效果等同于重发空快照） |
| `spec.ephemeral` | bool | 否 | `false` | 临时源：`Tripped.since + evictDelay` 到期后从快照移除节点 |
| `spec.evictDelay` | duration | 否 | TODO | 驱逐延迟，仅 `ephemeral=true` 时有效 |

## 快照语义

- 每次同步只做一件事：**发布新快照**。改名、启用、禁用、更新，都只是
  重发一次快照。
- 下载失败时保留旧快照并记录 `lastError`（运行时状态，不进声明）。
- 解析器支持：sing-box JSON、Clash JSON/YAML、URI 行
  （vmess / vless / trojan / ss / hysteria2 / http(s) / socks5(h)）、
  `IP:PORT[:USER:PASS]` 文本，以及它们的 base64 包裹。
- **拨号能力**（能力矩阵）：拨号由内嵌 sing-box 出站引擎提供，覆盖其全部
  协议族 —— `vmess`（含 ws/grpc 传输）、`vless`（含 REALITY）、`trojan`、
  `shadowsocks`（含 2022 系列）、`socks`、`http`、`direct` 等。
  QUIC 协议（`hysteria2` / `tuic` / `hysteria`）与 VLESS REALITY 的 uTLS
  依赖 build tag；官方构建使用 `-tags "with_quic with_utls"`（Dockerfile 已带）。
  `wireguard` 不编译进二进制；未支持类型一律 fail-closed
  （`ErrUnsupportedOutbound` → `UPSTREAM_CONNECT_FAILED`），不会静默直连。
  拨号类型扩展随 sing-box 版本升级获得，不动解析器。
- 新格式与新来源通过 **SourceProvider** 扩展点接入（见 [../ext-abi.md](../ext-abi.md)）。
- 资源下载统一走 `Fetcher = Direct ⊕ ViaDefaultPlatform ⊕ ViaDefaultPlatform`，
  订阅与 GeoIP 共用。

## 汇聚（Registry）

`NodeRegistry` 汇聚所有**启用** Source 的快照：对每个节点 hash 合并 Tags，
计算引用集合；引用集合为空则删除节点。节点快照用原子指针整体替换（COW），
读侧无锁。
