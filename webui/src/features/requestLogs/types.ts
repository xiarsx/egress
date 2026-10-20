// 与后端契约一致（internal/app/control RequestLogEntry）。
// 负载（payloads）抓取后端未实现，前端不提供对应 UI。

export interface RequestLogEntry {
  /** 跨滚动文件的唯一标识（"seq:id"）。 */
  id: string;
  ts_ns: number;
  /** "http-forward" | "reverse" | "socks5-forward" | "unknown" */
  proxy_type: string;
  platform: string;
  account: string;
  target: string;
  node: string;
  egress_ip: string;
  net_ok: boolean;
  http_status: number;
  stage: string;
  egress_error: string;
  duration_ms: number;
  bytes_up: number;
  bytes_down: number;
}

export interface RequestLogListResponse {
  items: RequestLogEntry[];
  total: number;
  limit: number;
  offset: number;
}

export interface RequestLogFilters {
  platform: string;
  account: string;
  egressError: string;
}

export const EMPTY_REQUEST_LOG_FILTERS: RequestLogFilters = {
  platform: "",
  account: "",
  egressError: "",
};
