"use strict";
/* egress 管理后台：纯手写 JS，无依赖。
 * 所有 API 路径严格对照 spec/openapi.yaml；spec 未定义的端点不调用。
 * TODO 集中见文件末尾注释。
 */
const API = "/api/v1";
const PAGE = 20;

class APIError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function shortHash(h) { return h && h.length > 12 ? h.slice(0, 12) + "…" : (h || "—"); }
function fmtTime(ns) {
  if (!ns) return "—";
  const d = new Date(Number(ns) / 1e6);
  return isNaN(d) ? "—" : d.toLocaleString("zh-CN");
}
function fmtDur(ns) {
  if (!ns) return "—";
  const s = Math.floor(Number(ns) / 1e9);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}小时${m}分` : m > 0 ? `${m}分${s % 60}秒` : `${s}秒`;
}
function badge(text, cls) { return `<span class="badge ${cls}">${esc(text)}</span>`; }
function healthBadge(h) {
  return h === "healthy" ? badge("健康", "healthy")
    : h === "tripped" ? badge("熔断", "tripped")
    : h === "probing" ? badge("探测中", "probing") : badge(h || "—", "off");
}

let toastTimer = null;
function toast(msg) {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.remove("hidden");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.add("hidden"), 2600);
}

function token() { return localStorage.getItem("egress.adminToken") || ""; }

async function api(path, opts = {}) {
  const headers = Object.assign({}, opts.headers || {});
  const t = token();
  if (t) headers["Authorization"] = "Bearer " + t;
  let r;
  try {
    r = await fetch(API + path, Object.assign({}, opts, { headers }));
  } catch (e) {
    throw new APIError(0, "网络错误：无法连接 " + location.host + "（" + e.message + "）");
  }
  if (r.status === 401) throw new APIError(401, "未授权（401）：请在顶部填写 AdminToken 后重试");
  if (r.status === 204) return null;
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (_) { /* 非 JSON */ }
  if (!r.ok) {
    const em = data && data.error;
    throw new APIError(r.status,
      em && em.message ? `错误 ${r.status} [${em.code || "?"}] ${em.message}` : `HTTP ${r.status}`);
  }
  return data;
}

function renderPager(el, total, limit, offset, go) {
  const pages = Math.max(1, Math.ceil(total / limit));
  const cur = Math.floor(offset / limit) + 1;
  el.innerHTML =
    `<button ${cur <= 1 ? "disabled" : ""} data-p="prev">上一页</button>` +
    `<span>第 ${cur} / ${pages} 页，共 ${total} 条</span>` +
    `<button ${cur >= pages ? "disabled" : ""} data-p="next">下一页</button>`;
  el.querySelector('[data-p="prev"]').onclick = () => go(offset - limit);
  el.querySelector('[data-p="next"]').onclick = () => go(offset + limit);
}

function failBox(el, e) {
  el.innerHTML = `<div class="err">${esc(e.message || String(e))}</div>`;
}

// ---------- tab 切换 ----------
document.querySelectorAll("#tabs .tab").forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll("#tabs .tab").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("page-" + btn.dataset.tab).classList.add("active");
    loaders[btn.dataset.tab]();
  };
});

// ---------- token ----------
const tokenInput = document.getElementById("token");
tokenInput.value = token();
document.getElementById("tokenSave").onclick = () => {
  localStorage.setItem("egress.adminToken", tokenInput.value.trim());
  toast("AdminToken 已保存");
};
tokenInput.addEventListener("keydown", e => { if (e.key === "Enter") document.getElementById("tokenSave").click(); });

let platformCache = null;
async function getPlatforms() {
  if (!platformCache) {
    const d = await api("/platforms?limit=100");
    platformCache = (d && d.items) || [];
  }
  return platformCache;
}

// ---------- 概览 ----------
async function loadOverview() {
  const cards = document.getElementById("ovCards");
  const plats = document.getElementById("ovPlatforms");
  const metrics = document.getElementById("ovMetrics");
  try {
    const [info, platforms, nodes, tripped] = await Promise.all([
      api("/system/info"),
      getPlatforms(),
      api("/nodes?limit=1"),
      api("/nodes?limit=1&health=tripped"),
    ]);
    const per = await Promise.all(platformCache.map(async p => {
      // ?platform= 取该 Platform 可路由集合；传 id（PlatformId 为 UUID）。
      // TODO: 若后端实际按 name 过滤，此处需改为 p.name。
      try {
        const [nn, ll] = await Promise.all([
          api(`/nodes?limit=1&platform=${encodeURIComponent(p.id)}`),
          api(`/platforms/${encodeURIComponent(p.id)}/leases?limit=1`),
        ]);
        return { p, nodes: (nn && nn.total) || 0, leases: (ll && ll.total) || 0 };
      } catch (e) {
        return { p, nodes: "?", leases: "?", err: e.message };
      }
    }));
    const totalNodes = (nodes && nodes.total) || 0;
    const trippedNodes = (tripped && tripped.total) || 0;
    cards.innerHTML =
      card("版本", esc((info && info.version) || "—")) +
      card("运行时间", fmtDur(info && info.uptime_ns)) +
      card("平台数", platformCache.length) +
      card("节点总数", totalNodes) +
      card("熔断节点", trippedNodes) +
      card("租约总数", per.reduce((a, x) => a + (typeof x.leases === "number" ? x.leases : 0), 0));
    plats.innerHTML = per.map(x =>
      `<div class="card"><div class="k">${esc(x.p.name || x.p.id)}</div>` +
      `<div class="v">${x.nodes}</div><div class="sub">节点 · 租约 ${x.leases}</div>` +
      (x.err ? `<div class="sub">统计失败：${esc(x.err)}</div>` : "") +
      `</div>`).join("") || `<div class="empty">暂无平台</div>`;
  } catch (e) { failBox(cards, e); }
  try {
    // 快照结构在 spec 中为 type:object，未细化——原样展示，待细化渲染。
    const [np, lat] = await Promise.all([
      api("/metrics/snapshots/node-pool").catch(e => ({ _error: e.message })),
      api("/metrics/snapshots/node-latency-distribution").catch(e => ({ _error: e.message })),
    ]);
    metrics.innerHTML =
      `<details class="raw"><summary>节点池快照 /metrics/snapshots/node-pool</summary><pre class="raw">${esc(JSON.stringify(np, null, 2))}</pre></details>` +
      `<details class="raw"><summary>节点延迟分布快照 /metrics/snapshots/node-latency-distribution</summary><pre class="raw">${esc(JSON.stringify(lat, null, 2))}</pre></details>`;
  } catch (e) { metrics.innerHTML = `<div class="err">${esc(e.message)}</div>`; }
}
function card(k, v) { return `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`; }

// ---------- 节点 ----------
const nodeState = { offset: 0, limit: PAGE };
async function fillPlatformSelect(sel, withAll) {
  try {
    const ps = await getPlatforms();
    sel.innerHTML = (withAll ? `<option value="">全部</option>` : "") +
      ps.map(p => `<option value="${esc(p.id)}">${esc(p.name || p.id)}</option>`).join("");
  } catch (e) { /* 保留空选项 */ }
}
async function loadNodes() {
  const plat = document.getElementById("nodePlatform").value;
  const health = document.getElementById("nodeHealth").value;
  const box = document.getElementById("nodeTable");
  const pager = document.getElementById("nodePager");
  try {
    let q = `/nodes?limit=${nodeState.limit}&offset=${nodeState.offset}`;
    if (health) q += `&health=${encodeURIComponent(health)}`;
    if (plat) q += `&platform=${encodeURIComponent(plat)}`;
    const d = await api(q);
    const items = (d && d.items) || [];
    const total = (d && d.total) || 0;
    if (!items.length) { box.innerHTML = `<div class="empty">暂无节点</div>`; pager.innerHTML = ""; return; }
    box.innerHTML = `<table><thead><tr>
      <th>hash</th><th>出口 IP</th><th>健康</th><th>连续失败</th><th>延迟</th><th>tags</th><th>操作</th>
      </tr></thead><tbody>` + items.map(n => {
      // spec 的 Node schema 没有延迟字段；参考实现曾用 reference_latency_ms。
      // 若后端将其放入 attrs 命名空间属性，则从 attrs 读取，否则显示 —。
      const lat = n.attrs && n.attrs.reference_latency_ms;
      const tags = (n.tags || []).slice(0, 3).map(t => `<span class="tag">${esc(t)}</span>`).join("") +
        ((n.tags || []).length > 3 ? `<span class="tag">+${n.tags.length - 3}</span>` : "");
      return `<tr>
        <td class="mono" title="${esc(n.hash)}">${esc(shortHash(n.hash))}</td>
        <td class="mono">${esc(n.egress_ip || "—")}</td>
        <td>${healthBadge(n.health)}</td>
        <td>${n.failures != null ? esc(n.failures) : "—"}</td>
        <td>${typeof lat === "number" ? esc(lat) + " ms" : "—"}</td>
        <td>${tags || "—"}</td>
        <td>
          <button data-act="probe-egress" data-h="${esc(n.hash)}">探测出口</button>
          <button data-act="probe-latency" data-h="${esc(n.hash)}">探测延迟</button>
        </td></tr>`;
    }).join("") + `</tbody></table>`;
    box.querySelectorAll("button[data-act]").forEach(b => {
      b.onclick = async () => {
        try {
          await api(`/nodes/${encodeURIComponent(b.dataset.h)}/actions/${b.dataset.act}`, { method: "POST" });
          toast("已接受探测请求（202）");
        } catch (e) { toast("探测失败：" + e.message); }
      };
    });
    renderPager(pager, total, nodeState.limit, nodeState.offset, off => { nodeState.offset = Math.max(0, off); loadNodes(); });
  } catch (e) { failBox(box, e); pager.innerHTML = ""; }
}

// ---------- 来源 ----------
async function loadSources() {
  const box = document.getElementById("sourceTable");
  try {
    const d = await api("/sources?limit=100");
    const items = (d && d.items) || [];
    if (!items.length) { box.innerHTML = `<div class="empty">暂无来源</div>`; return; }
    box.innerHTML = `<table><thead><tr>
      <th>名称</th><th>类型</th><th>URL</th><th>启用</th><th>更新间隔</th><th>上次错误</th><th>操作</th>
      </tr></thead><tbody>` + items.map(s =>
      `<tr>
        <td>${esc(s.name || s.id)}</td>
        <td>${esc(s.type || "—")}</td>
        <td class="mono">${esc(s.url || "—")}</td>
        <td>${s.enabled ? badge("启用", "healthy") : badge("禁用", "off")}</td>
        <td>${esc(s.update_interval || "—")}</td>
        <td>${s.last_error ? `<span style="color:var(--bad)">${esc(s.last_error)}</span>` : "—"}</td>
        <td><button data-id="${esc(s.id)}">立即刷新</button></td>
      </tr>`).join("") + `</tbody></table>`;
    box.querySelectorAll("button[data-id]").forEach(b => {
      b.onclick = async () => {
        b.disabled = true;
        try {
          await api(`/sources/${encodeURIComponent(b.dataset.id)}/actions/refresh`, { method: "POST" });
          toast("刷新请求已接受（202）");
        } catch (e) { toast("刷新失败：" + e.message); }
        b.disabled = false;
      };
    });
  } catch (e) { failBox(box, e); }
}

// ---------- 租约 ----------
// spec 没有全局 /api/v1/leases；租约挂在平台下：/api/v1/platforms/{id}/leases。
const leaseState = { offset: 0, limit: PAGE };
async function loadLeases() {
  const pid = document.getElementById("leasePlatform").value;
  const box = document.getElementById("leaseTable");
  const pager = document.getElementById("leasePager");
  if (!pid) { box.innerHTML = `<div class="empty">请选择平台</div>`; pager.innerHTML = ""; return; }
  try {
    const d = await api(`/platforms/${encodeURIComponent(pid)}/leases?limit=${leaseState.limit}&offset=${leaseState.offset}`);
    const items = (d && d.items) || [];
    const total = (d && d.total) || 0;
    if (!items.length) { box.innerHTML = `<div class="empty">暂无租约</div>`; pager.innerHTML = ""; return; }
    box.innerHTML = `<table><thead><tr>
      <th>账号</th><th>出口 IP</th><th>节点 hash</th><th>过期时间</th><th>最后访问</th><th>操作</th>
      </tr></thead><tbody>` + items.map(l =>
      `<tr>
        <td class="mono">${esc(l.account)}</td>
        <td class="mono">${esc(l.egress_ip || "—")}</td>
        <td class="mono" title="${esc(l.node_hash)}">${esc(shortHash(l.node_hash))}</td>
        <td>${fmtTime(l.expiry_ns)}</td>
        <td>${fmtTime(l.last_accessed_ns)}</td>
        <td><button class="danger" data-acc="${esc(l.account)}">释放</button></td>
      </tr>`).join("") + `</tbody></table>`;
    box.querySelectorAll("button[data-acc]").forEach(b => {
      b.onclick = async () => {
        if (!confirm(`确定释放账号 ${b.dataset.acc} 的租约？`)) return;
        try {
          await api(`/platforms/${encodeURIComponent(pid)}/leases/${encodeURIComponent(b.dataset.acc)}`, { method: "DELETE" });
          toast("租约已释放");
          loadLeases();
        } catch (e) { toast("释放失败：" + e.message); }
      };
    });
    renderPager(pager, total, leaseState.limit, leaseState.offset, off => { leaseState.offset = Math.max(0, off); loadLeases(); });
  } catch (e) { failBox(box, e); pager.innerHTML = ""; }
}

// ---------- 日志 ----------
// spec: GET /api/v1/request-logs?limit&offset&platform&account&resin_error（无 target 参数）。
const logState = { offset: 0, limit: PAGE };
async function loadLogs() {
  const box = document.getElementById("logTable");
  const pager = document.getElementById("logPager");
  document.getElementById("logDetail").innerHTML = "";
  try {
    let q = `/request-logs?limit=${logState.limit}&offset=${logState.offset}`;
    const pf = document.getElementById("logPlatform").value.trim();
    const ac = document.getElementById("logAccount").value.trim();
    const er = document.getElementById("logError").value.trim();
    if (pf) q += `&platform=${encodeURIComponent(pf)}`;
    if (ac) q += `&account=${encodeURIComponent(ac)}`;
    if (er) q += `&resin_error=${encodeURIComponent(er)}`;
    const d = await api(q);
    const items = (d && d.items) || [];
    const total = (d && d.total) || 0;
    if (!items.length) { box.innerHTML = `<div class="empty">暂无日志</div>`; pager.innerHTML = ""; return; }
    box.innerHTML = `<table><thead><tr>
      <th>时间</th><th>类型</th><th>平台</th><th>账号</th><th>目标</th><th>节点</th><th>出口 IP</th>
      <th>网络</th><th>状态码</th><th>耗时</th><th>错误码</th>
      </tr></thead><tbody>` + items.map(l =>
      `<tr class="clickable" data-id="${esc(l.log_id || l.id || "")}">
        <td class="mono">${fmtTime(l.ts_ns)}</td>
        <td>${esc(l.proxy_type || "—")}</td>
        <td>${esc(l.platform || "—")}</td>
        <td class="mono">${esc(l.account || "—")}</td>
        <td class="mono">${esc(l.target || "—")}</td>
        <td class="mono" title="${esc(l.node)}">${esc(shortHash(l.node))}</td>
        <td class="mono">${esc(l.egress_ip || "—")}</td>
        <td>${l.net_ok ? badge("OK", "healthy") : badge("失败", "tripped")}</td>
        <td>${l.http_status != null ? esc(l.http_status) : "—"}</td>
        <td>${l.duration_ms != null ? esc(l.duration_ms) + " ms" : "—"}</td>
        <td class="mono">${esc(l.resin_error || "—")}</td>
      </tr>`).join("") + `</tbody></table>`;
    box.querySelectorAll("tr[data-id]").forEach(tr => {
      tr.onclick = async () => {
        const id = tr.dataset.id;
        if (!id) { toast("该日志条目没有 log_id"); return; }
        const det = document.getElementById("logDetail");
        try {
          const one = await api(`/request-logs/${encodeURIComponent(id)}`);
          det.innerHTML = `<details class="raw" open><summary>日志详情 ${esc(id)}</summary>` +
            `<pre class="raw">${esc(JSON.stringify(one, null, 2))}</pre></details>`;
        } catch (e) { det.innerHTML = `<div class="err">${esc(e.message)}</div>`; }
      };
    });
    renderPager(pager, total, logState.limit, logState.offset, off => { logState.offset = Math.max(0, off); loadLogs(); });
  } catch (e) { failBox(box, e); pager.innerHTML = ""; }
}

// ---------- 事件绑定 ----------
document.getElementById("nodeReload").onclick = () => { nodeState.offset = 0; loadNodes(); };
document.getElementById("nodePlatform").onchange = () => { nodeState.offset = 0; loadNodes(); };
document.getElementById("nodeHealth").onchange = () => { nodeState.offset = 0; loadNodes(); };
document.getElementById("sourceReload").onclick = loadSources;
document.getElementById("leaseReload").onclick = () => { leaseState.offset = 0; loadLeases(); };
document.getElementById("leasePlatform").onchange = () => { leaseState.offset = 0; loadLeases(); };
document.getElementById("logSearch").onclick = () => { logState.offset = 0; loadLogs(); };

const loaders = {
  overview: loadOverview,
  nodes: loadNodes,
  sources: loadSources,
  leases: loadLeases,
  logs: loadLogs,
};

// 初始化：填充平台下拉框后加载概览
(async function init() {
  const sel1 = document.getElementById("nodePlatform");
  const sel2 = document.getElementById("leasePlatform");
  await fillPlatformSelect(sel1, true);
  await fillPlatformSelect(sel2, false);
  loadOverview();
})();

/* ================= TODO（对照 spec/openapi.yaml 发现） =================
 * 1. /api/v1/nodes?platform=：spec 语义为"某 Platform 可路由集合中的节点"，
 *    参数值是 id 还是 name 未明确；当前传 platform.id，若后端按 name 过滤需改为 p.name。
 * 2. Node schema 无延迟字段：当前从 attrs.reference_latency_ms 降级读取
 *    （参考实现 Resin 的旧字段），若 egress 后端有正式延迟字段需替换。
 * 3. 没有全局 /api/v1/leases：租约页按平台查看；跨平台汇总暂不实现。
 * 4. /api/v1/request-logs 无 target 查询参数：日志页只按 platform/account/resin_error 过滤。
 * 5. /api/v1/metrics/* 快照与曲线结构在 spec 中为 type:object 未细化：
 *    概览页仅原样展示 JSON，实时曲线（throughput/connections/leases）与历史曲线
 *    （traffic/requests/access-latency/probes/node-pool/lease-lifetime）尚未做图表。
 * 6. RequestLogEntry 的标识字段在 spec 中未命名：详情查询用 l.log_id（回退 l.id），
 *    若后端字段名不同需修正。
 */
