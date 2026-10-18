package wiring

// backend.go: control.RuntimeBackend 的组装实现。
//
// 控制面（internal/app/control）拥有声明式资源，但节点、租约、指标、
// 请求日志等运行时状态归 runtime 所有。Service 通过 RuntimeBackend
// 接口读取这些状态；本文件在组装根把各运行时部件接到该接口上，
// 消灭 501 NOT_IMPLEMENTED。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"

	pklease "github.com/xiarsx/egress/pkg/lease"
	pknode "github.com/xiarsx/egress/pkg/node"
	"github.com/xiarsx/egress/internal/adapters/geoip"
	"github.com/xiarsx/egress/internal/adapters/sqlite"
	"github.com/xiarsx/egress/internal/app/control"
	"github.com/xiarsx/egress/internal/app/runtime"
	"github.com/xiarsx/egress/internal/core/lease"
	corenode "github.com/xiarsx/egress/internal/core/node"
	"github.com/xiarsx/egress/internal/core/predicate"
	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/ports"
)

// Backend 实现 control.RuntimeBackend。
type Backend struct {
	Reg       ports.NodeRegistry
	Deriver   *runtime.Deriver
	Leases    *lease.LeaseTable
	Syncer    runtime.Syncer
	Platforms ports.PlatformSource
	Metrics   *sqlite.MetricsDB
	ReqLog    *sqlite.ReqLog
	Cache     ports.CacheStore
	Geo       *geoip.Provider
	GeoDBPath string // 为空表示未配置 GeoIP
	Prober    *runtime.Prober
	Clock     ports.Clock
}

var _ control.RuntimeBackend = (*Backend)(nil)

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func toControlNode(n pknode.NodeSnapshot) control.Node {
	attrs := make(map[string]any, len(n.Attrs))
	for k, v := range n.Attrs {
		if v.IsNum {
			attrs[string(k)] = v.Num
		} else {
			attrs[string(k)] = v.Str
		}
	}
	tags := make([]string, len(n.Tags))
	for i, t := range n.Tags {
		tags[i] = string(t)
	}
	var health string
	var failures int
	var sinceNs int64
	switch n.Health.Kind {
	case pknode.HealthProbing:
		health = "probing"
	case pknode.HealthHealthy:
		health = "healthy"
		failures = n.Health.Failures
	case pknode.HealthTripped:
		health = "tripped"
		sinceNs = n.Health.Since.UnixNano()
	default:
		health = "unknown"
	}
	return control.Node{
		Hash:           n.Hash.String(),
		Tags:           tags,
		Attrs:          attrs,
		EgressIP:       addrStr(n.EgressIP),
		Health:         health,
		Failures:       failures,
		TrippedSinceNs: sinceNs,

		// Legacy-compatible fields for the ported admin console.
		DisplayTag:       displayTag(tags),
		HasOutbound:      n.HasOutbound,
		Enabled:          true,
		Region:           attrStr(attrs, "geo.country"),
		CircuitOpenSince: trippedSinceStr(sinceNs),
		FailureCount:     failures,
	}
}

// displayTag returns the first tag with the "<source>/" prefix stripped,
// e.g. "verify/US-Los Angeles-1" -> "US-Los Angeles-1".
func displayTag(tags []string) string {
	if len(tags) == 0 {
		return ""
	}
	t := tags[0]
	if i := strings.Index(t, "/"); i >= 0 {
		t = t[i+1:]
	}
	return t
}

func attrStr(attrs map[string]any, key string) string {
	v, ok := attrs[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func trippedSinceStr(ns int64) string {
	if ns <= 0 {
		return ""
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339)
}

func addrStr(a netip.Addr) string {
	if !a.IsValid() {
		return ""
	}
	return a.String()
}

func (b *Backend) ListNodes(ctx context.Context, limit, offset int, health, platform string) ([]control.Node, int, error) {
	if limit <= 0 {
		limit = 50
	}
	all := b.Reg.All()
	var view *view.View
	if platform != "" {
		view = b.Deriver.View(platform)
		if view == nil {
			return nil, 0, fmt.Errorf("platform %s: %w", platform, ports.ErrNotFound)
		}
	}
	health = strings.ToLower(health)
	var out []control.Node
	for _, n := range all {
		if view != nil && !view.Contains(n.Hash) {
			continue
		}
		cn := toControlNode(n)
		if health != "" && cn.Health != health {
			continue
		}
		out = append(out, cn)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hash < out[j].Hash })
	total := len(out)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return out[offset:end], total, nil
}

func (b *Backend) GetNode(ctx context.Context, hash string) (control.Node, error) {
	h, err := pknode.ParseHash(hash)
	if err != nil {
		return control.Node{}, fmt.Errorf("bad node hash %q: %w", hash, ports.ErrInvalid)
	}
	n, ok := b.Reg.Get(h)
	if !ok {
		return control.Node{}, fmt.Errorf("node %s: %w", hash, ports.ErrNotFound)
	}
	return toControlNode(n), nil
}

func (b *Backend) ProbeNode(ctx context.Context, hash string, kind control.ProbeKind) error {
	h, err := pknode.ParseHash(hash)
	if err != nil {
		return fmt.Errorf("bad node hash %q: %w", hash, ports.ErrInvalid)
	}
	if _, ok := b.Reg.Get(h); !ok {
		return fmt.Errorf("node %s: %w", hash, ports.ErrNotFound)
	}
	if b.Prober == nil {
		return errors.New("prober not wired")
	}
	switch kind {
	case control.ProbeKindEgress:
		b.Prober.ProbeEgress(ctx, h)
	case control.ProbeKindLatency:
		b.Prober.ProbeLatency(ctx, h, "cloudflare.com")
	default:
		return fmt.Errorf("unknown probe kind %q: %w", kind, ports.ErrInvalid)
	}
	return nil
}

func (b *Backend) PreviewFilter(ctx context.Context, f control.PlatformFilter) (matched, total int, err error) {
	pred, err := predicate.Compile(control.ToPredicateFilter(f))
	if err != nil {
		return 0, 0, err
	}
	all := b.Reg.All()
	for _, n := range all {
		if pred(n) {
			matched++
		}
	}
	return matched, len(all), nil
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------

func toControlLease(platformID, account string, l pklease.Lease) control.Lease {
	return control.Lease{
		PlatformID:     platformID,
		Account:        account,
		EgressIP:       addrStr(l.EgressIP),
		NodeHash:       l.NodeHint.String(),
		ExpiryNs:       l.Expiry.UnixNano(),
		LastAccessedNs: l.LastAccessed.UnixNano(),
	}
}

func (b *Backend) ListLeases(ctx context.Context, platformID string, limit, offset int) ([]control.Lease, int, error) {
	if limit <= 0 {
		limit = 50
	}
	var out []control.Lease
	b.Leases.Range(func(platform, account string, l pklease.Lease) bool {
		if platformID != "" && platform != platformID {
			return true
		}
		out = append(out, toControlLease(platform, account, l))
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].PlatformID != out[j].PlatformID {
			return out[i].PlatformID < out[j].PlatformID
		}
		return out[i].Account < out[j].Account
	})
	total := len(out)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return out[offset:end], total, nil
}

func (b *Backend) GetLease(ctx context.Context, platformID, account string) (control.Lease, error) {
	l, ok := b.Leases.Get(platformID, account)
	if !ok {
		return control.Lease{}, fmt.Errorf("lease %s/%s: %w", platformID, account, ports.ErrNotFound)
	}
	return toControlLease(platformID, account, l), nil
}

func (b *Backend) ReleaseLease(ctx context.Context, platformID, account string) error {
	if !b.Leases.Release(platformID, account) {
		return fmt.Errorf("lease %s/%s: %w", platformID, account, ports.ErrNotFound)
	}
	if b.Cache != nil {
		b.Cache.DeleteLease(platformID, account)
	}
	return nil
}

func (b *Backend) ReleaseAllLeases(ctx context.Context, platformID string) error {
	var keys [][2]string
	b.Leases.Range(func(platform, account string, l pklease.Lease) bool {
		if platformID == "" || platform == platformID {
			keys = append(keys, [2]string{platform, account})
		}
		return true
	})
	for _, k := range keys {
		b.Leases.Release(k[0], k[1])
		if b.Cache != nil {
			b.Cache.DeleteLease(k[0], k[1])
		}
	}
	return nil
}

func (b *Backend) IPLoad(ctx context.Context, platformID string) (map[string]int64, error) {
	ips := map[netip.Addr]struct{}{}
	b.Leases.Range(func(platform, account string, l pklease.Lease) bool {
		if platformID != "" && platform != platformID {
			return true
		}
		if l.EgressIP.IsValid() {
			ips[l.EgressIP] = struct{}{}
		}
		return true
	})
	out := make(map[string]int64, len(ips))
	for ip := range ips {
		p := platformID
		if p == "" {
			// 跨平台汇总时按租约实际平台分别取值后累加
			var sum int64
			b.Leases.Range(func(platform, account string, l pklease.Lease) bool {
				if l.EgressIP == ip {
					sum += b.Leases.IPLoad(platform, ip)
				}
				return true
			})
			out[ip.String()] = sum
			continue
		}
		out[ip.String()] = b.Leases.IPLoad(p, ip)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Views and sources
// ---------------------------------------------------------------------------

func (b *Backend) RebuildRoutableView(ctx context.Context, platformID string) error {
	spec, err := b.Platforms.GetPlatformSpec(ctx, platformID)
	if err != nil {
		return err
	}
	compiled, err := predicate.Compile(control.ToPredicateFilter(platformFilterOf(spec)))
	if err != nil {
		return err
	}
	full := func(n pknode.NodeSnapshot) bool {
		return corenode.Routable(corenode.PlatformFilter{Tags: compiled}, n)
	}
	b.Deriver.SetPlatform(platformID, full)
	return nil
}

// platformFilterOf 从 ports.PlatformSpec 还原 control.PlatformFilter。
// ports.PlatformSpec.Filter 已经是 predicate.Filter；这里把它转回声明形态
// 以便复用 control 的编译入口。predicate.Filter 与声明是一一对应的。
func platformFilterOf(spec ports.PlatformSpec) control.PlatformFilter {
	// ports.PlatformSpec.Filter 即 predicate.Filter；control.ToPredicateFilter
	// 接受 control.PlatformFilter。我们直接复用 predicate.Filter 的字段：
	// 为避免重复实现转换，这里走"声明→编译"时传入等价声明。
	// 由于 predicate.Filter{Tags, Attrs} 与 control.PlatformFilter{Tags, Attrs}
	// 字段一一对应，直接构造：
	return control.PlatformFilter{
		Tags:  spec.Filter.Tags,
		Attrs: toControlAttrConds(spec.Filter.Attrs),
	}
}

func toControlAttrConds(in map[string]predicate.AttrCondition) map[string]control.AttrCondition {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]control.AttrCondition, len(in))
	for k, c := range in {
		out[k] = control.AttrCondition{
			In:       c.In,
			NotIn:    c.NotIn,
			Match:    c.Match,
			NotMatch: c.NotMatch,
			Gte:      control.FlexString(c.Gte),
			Lte:      control.FlexString(c.Lte),
		}
	}
	return out
}

func (b *Backend) RefreshSource(ctx context.Context, name string) error {
	if b.Syncer == nil {
		return errors.New("syncer not wired")
	}
	b.Syncer.RefreshNow(name)
	return nil
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

type metricPoint struct {
	Series     string  `json:"series"`
	BucketUnix int64   `json:"bucket_unix"`
	Sum        float64 `json:"sum"`
	Count      int64   `json:"count"`
}

func (b *Backend) queryMetrics(name string, limit int) ([]metricPoint, error) {
	if b.Metrics == nil {
		return nil, errors.New("metrics not wired")
	}
	rows, err := b.Metrics.DB().Query(
		`SELECT series, bucket_start, sum, cnt FROM metric_buckets
		 WHERE series LIKE ? ORDER BY bucket_start DESC LIMIT ?`, name+"%", limit)
	if err != nil {
		// metrics 表可能尚未创建（无数据时）——返回空而非报错
		if strings.Contains(err.Error(), "no such table") {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var out []metricPoint
	for rows.Next() {
		var p metricPoint
		var bucketStart int64
		var sum float64
		var cnt int64
		if err := rows.Scan(&p.Series, &bucketStart, &sum, &cnt); err != nil {
			return nil, err
		}
		p.BucketUnix = bucketStart
		p.Sum = sum
		p.Count = cnt
		out = append(out, p)
	}
	return out, rows.Err()
}

func (b *Backend) MetricRealtime(ctx context.Context, name string) (any, error) {
	// 实时 ring buffer 尚未实现（TODO）；返回最近 flush 的桶近似。
	pts, err := b.queryMetrics(name, 50)
	if err != nil {
		return nil, err
	}
	return map[string]any{"points": pts, "realtime": false}, nil
}

func (b *Backend) MetricHistory(ctx context.Context, name string) (any, error) {
	pts, err := b.queryMetrics(name, 500)
	if err != nil {
		return nil, err
	}
	return map[string]any{"points": pts}, nil
}

func (b *Backend) MetricSnapshot(ctx context.Context, name string) (any, error) {
	pts, err := b.queryMetrics(name, 200)
	if err != nil {
		return nil, err
	}
	latest := map[string]metricPoint{}
	for _, p := range pts {
		if _, ok := latest[p.Series]; !ok {
			latest[p.Series] = p
		}
	}
	return map[string]any{"series": latest}, nil
}

// ---------------------------------------------------------------------------
// Request logs
// ---------------------------------------------------------------------------

func toControlReqLog(e sqlite.ReqLogEntry) control.RequestLogEntry {
	return control.RequestLogEntry{
		TsNs:       e.TS.UnixNano(),
		ProxyType:  e.ProxyType,
		Platform:   e.Platform,
		Account:    e.Account,
		Target:     e.Target,
		Node:       e.Node,
		EgressIP:   e.EgressIP,
		NetOk:      e.NetOK,
		HTTPStatus: e.HTTPStatus,
		Stage:      e.Stage,
		ResinError: e.ResinError,
		DurationMs: e.DurationMs,
		BytesUp:    e.BytesUp,
		BytesDown:  e.BytesDown,
	}
}

func (b *Backend) ListRequestLogs(ctx context.Context, limit, offset int, platform, account, resinErr string) ([]control.RequestLogEntry, int, error) {
	if b.ReqLog == nil {
		return nil, 0, errors.New("request log not wired")
	}
	entries, total, err := b.ReqLog.QueryReqLogs(limit, offset, platform, account, resinErr)
	if err != nil {
		return nil, 0, err
	}
	out := make([]control.RequestLogEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, toControlReqLog(e))
	}
	return out, total, nil
}

func (b *Backend) GetRequestLog(ctx context.Context, id string) (control.RequestLogEntry, error) {
	if b.ReqLog == nil {
		return control.RequestLogEntry{}, errors.New("request log not wired")
	}
	rid, err := sqlite.ParseReqLogID(id)
	if err != nil {
		return control.RequestLogEntry{}, fmt.Errorf("%w: %v", ports.ErrInvalid, err)
	}
	e, _, err := b.ReqLog.GetReqLog(rid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return control.RequestLogEntry{}, fmt.Errorf("request log %s: %w", id, ports.ErrNotFound)
		}
		return control.RequestLogEntry{}, err
	}
	return toControlReqLog(e), nil
}

func (b *Backend) GetRequestLogPayload(ctx context.Context, id string) (any, error) {
	// payload 抓取尚未实现（spec/events.md 默认关闭，sqlite 侧 TODO）。
	return nil, fmt.Errorf("payload capture not implemented: %w", ports.ErrInvalid)
}

// ---------------------------------------------------------------------------
// GeoIP
// ---------------------------------------------------------------------------

func (b *Backend) GeoIPStatus(ctx context.Context) (any, error) {
	return map[string]any{
		"configured": b.GeoDBPath != "",
		"path":       b.GeoDBPath,
	}, nil
}

func (b *Backend) GeoIPLookup(ctx context.Context, ips []string) (any, error) {
	if b.Geo == nil {
		return nil, errors.New("geoip not wired")
	}
	out := make(map[string]map[string]any, len(ips))
	for _, s := range ips {
		addr, err := netip.ParseAddr(s)
		if err != nil || !addr.IsValid() {
			out[s] = nil
			continue
		}
		attrs := b.Geo.Enrich(addr)
		m := make(map[string]any, len(attrs))
		for k, v := range attrs {
			if v.IsNum {
				m[string(k)] = v.Num
			} else {
				m[string(k)] = v.Str
			}
		}
		out[s] = m
	}
	return map[string]any{"results": out}, nil
}

func (b *Backend) GeoIPUpdateNow(ctx context.Context) error {
	if b.Geo == nil || b.GeoDBPath == "" {
		return errors.New("geoip not configured")
	}
	if b.Reg == nil || b.Cache == nil {
		return errors.New("geoip refresh missing registry/cache")
	}
	for _, n := range b.Reg.All() {
		if !n.EgressIP.IsValid() {
			continue
		}
		if attrs := b.Geo.Enrich(n.EgressIP); len(attrs) > 0 {
			b.Cache.UpsertAttrs(n.Hash, attrs)
		}
	}
	return b.Cache.Flush()
}
