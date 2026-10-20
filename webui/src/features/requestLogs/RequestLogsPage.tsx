import { useQuery } from "@tanstack/react-query";
import { createColumnHelper } from "@tanstack/react-table";
import { AlertTriangle, RefreshCw, X } from "lucide-react";
import { useMemo, useState } from "react";
import { Badge } from "../../components/ui/Badge";
import { Button } from "../../components/ui/Button";
import { Card } from "../../components/ui/Card";
import { DataTable } from "../../components/ui/DataTable";
import { Input } from "../../components/ui/Input";
import { OffsetPagination } from "../../components/ui/OffsetPagination";
import { useI18n } from "../../i18n";
import { listRequestLogs } from "./api";
import { EMPTY_REQUEST_LOG_FILTERS, type RequestLogEntry, type RequestLogFilters } from "./types";

const PAGE_SIZE_OPTIONS = [20, 50, 100] as const;

function proxyTypeBadge(proxyType: string, t: (key: string) => string) {
  switch (proxyType) {
    case "http-forward":
      return <Badge className="request-logs-proxy-badge-forward">{t("正向")}</Badge>;
    case "reverse":
      return <Badge className="request-logs-proxy-badge-reverse">{t("反向")}</Badge>;
    case "socks5-forward":
      return <Badge className="request-logs-proxy-badge-socks">SOCKS5</Badge>;
    default:
      return proxyType || "-";
  }
}

function formatTs(tsNs: number): { time: string; date: string } {
  const d = new Date(tsNs / 1_000_000);
  if (Number.isNaN(d.getTime())) {
    return { time: "--", date: "" };
  }
  const pad = (n: number) => String(n).padStart(2, "0");
  return {
    time: `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`,
    date: `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`,
  };
}

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`;
}

const col = createColumnHelper<RequestLogEntry>();

export function RequestLogsPage() {
  const { t } = useI18n();
  const [draft, setDraft] = useState<RequestLogFilters>(EMPTY_REQUEST_LOG_FILTERS);
  const [applied, setApplied] = useState<RequestLogFilters>(EMPTY_REQUEST_LOG_FILTERS);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState<number>(PAGE_SIZE_OPTIONS[0]);
  const [selected, setSelected] = useState<RequestLogEntry | null>(null);

  const logsQuery = useQuery({
    queryKey: ["request-logs", applied, page, pageSize],
    queryFn: () => listRequestLogs(applied, pageSize, page * pageSize),
    refetchInterval: 15_000,
    placeholderData: (previous) => previous,
  });

  const items = logsQuery.data?.items ?? [];
  const total = logsQuery.data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  const applyFilters = () => {
    setPage(0);
    setApplied(draft);
  };
  const resetFilters = () => {
    setDraft(EMPTY_REQUEST_LOG_FILTERS);
    setApplied(EMPTY_REQUEST_LOG_FILTERS);
    setPage(0);
  };

  const columns = useMemo(
    () => [
      col.accessor("ts_ns", {
        header: t("时间"),
        cell: (info) => {
          const ts = formatTs(info.getValue());
          return (
            <div className="logs-cell-stack">
              <span>{ts.time}</span>
              <small>{ts.date}</small>
            </div>
          );
        },
      }),
      col.accessor("proxy_type", {
        header: t("代理"),
        cell: (info) => proxyTypeBadge(info.getValue(), t),
      }),
      col.display({
        id: "platform_account",
        header: t("平台 / 账号"),
        cell: (info) => {
          const log = info.row.original;
          return (
            <div className="logs-cell-stack">
              <span>{log.platform || "-"}</span>
              <small>{log.account || "-"}</small>
            </div>
          );
        },
      }),
      col.accessor("target", {
        header: t("目标"),
        cell: (info) => info.getValue() || "-",
      }),
      col.accessor("egress_ip", {
        header: t("出口 IP"),
        cell: (info) => info.getValue() || "-",
      }),
      col.accessor("net_ok", {
        header: t("结果"),
        cell: (info) =>
          info.getValue() ? (
            <Badge variant="success">OK</Badge>
          ) : (
            <Badge variant="danger">{t("失败")}</Badge>
          ),
      }),
      col.accessor("http_status", {
        header: t("状态"),
        cell: (info) => info.getValue() || "-",
      }),
      col.accessor("egress_error", {
        header: t("错误"),
        cell: (info) => info.getValue() || "-",
      }),
      col.accessor("duration_ms", {
        header: t("耗时"),
        cell: (info) => `${info.getValue()} ms`,
      }),
      col.display({
        id: "bytes",
        header: t("上行 / 下行"),
        cell: (info) => {
          const log = info.row.original;
          return `${formatBytes(log.bytes_up)} / ${formatBytes(log.bytes_down)}`;
        },
      }),
    ],
    [t],
  );

  return (
    <section className="logs-page">
      <header className="module-header">
        <h2>{t("请求日志")}</h2>
        <p className="module-description">{t("每条代理请求一条记录，按时间倒序。")}</p>
      </header>

      <Card className="logs-table-card">
        <div className="logs-toolbar">
          <div className="logs-filter-grid">
            <Input
              placeholder={t("平台")}
              value={draft.platform}
              onChange={(e) => setDraft({ ...draft, platform: e.target.value })}
            />
            <Input
              placeholder={t("账号")}
              value={draft.account}
              onChange={(e) => setDraft({ ...draft, account: e.target.value })}
            />
            <Input
              placeholder={t("错误码（如 UPSTREAM_TIMEOUT）")}
              value={draft.egressError}
              onChange={(e) => setDraft({ ...draft, egressError: e.target.value })}
            />
          </div>
          <div className="endpoint-toolbar-actions">
            <Button variant="primary" onClick={applyFilters}>
              {t("查询")}
            </Button>
            <Button onClick={resetFilters}>{t("重置")}</Button>
            <Button onClick={() => void logsQuery.refetch()} aria-label={t("刷新")}>
              <RefreshCw size={14} />
            </Button>
          </div>
        </div>

        {logsQuery.isError ? (
          <div className="callout callout-error" role="alert">
            <AlertTriangle size={14} />
            <span>{t("请求日志加载失败")}</span>
          </div>
        ) : null}

        <DataTable
          data={items}
          columns={columns}
          getRowId={(log) => log.id}
          onRowClick={setSelected}
          selectedRowId={selected?.id}
        />

        <OffsetPagination
          page={page}
          totalPages={totalPages}
          totalItems={total}
          pageSize={pageSize}
          pageSizeOptions={PAGE_SIZE_OPTIONS}
          disabled={logsQuery.isFetching && logsQuery.isPlaceholderData}
          onPageChange={setPage}
          onPageSizeChange={(size) => {
            setPageSize(size);
            setPage(0);
          }}
        />
      </Card>

      {selected ? (
        <div className="drawer-overlay" onClick={() => setSelected(null)}>
          <Card className="drawer-panel" onClick={(event) => event.stopPropagation()}>
            <div className="drawer-header">
              <div>
                <h3>{t("请求详情")}</h3>
                <p>{selected.id}</p>
              </div>
              <div className="drawer-header-actions">
                <Button onClick={() => setSelected(null)} aria-label={t("关闭")}>
                  <X size={14} />
                </Button>
              </div>
            </div>
            <table className="data-table">
              <tbody>
                {(
                  [
                    [t("时间"), `${formatTs(selected.ts_ns).date} ${formatTs(selected.ts_ns).time}`],
                    [t("代理类型"), selected.proxy_type || "-"],
                    [t("平台"), selected.platform || "-"],
                    [t("账号"), selected.account || "-"],
                    [t("目标"), selected.target || "-"],
                    [t("节点"), selected.node || "-"],
                    [t("出口 IP"), selected.egress_ip || "-"],
                    [t("网络结果"), selected.net_ok ? "OK" : t("失败")],
                    [t("HTTP 状态"), selected.http_status || "-"],
                    [t("失败阶段"), selected.stage || "-"],
                    [t("错误码"), selected.egress_error || "-"],
                    [t("耗时"), `${selected.duration_ms} ms`],
                    [t("上行字节"), formatBytes(selected.bytes_up)],
                    [t("下行字节"), formatBytes(selected.bytes_down)],
                  ] as const
                ).map(([label, value]) => (
                  <tr key={label}>
                    <th style={{ textAlign: "left", width: "30%" }}>{label}</th>
                    <td style={{ wordBreak: "break-all" }}>{value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        </div>
      ) : null}
    </section>
  );
}
