import { apiRequest } from "../../lib/api-client";
import type { RequestLogEntry, RequestLogFilters, RequestLogListResponse } from "./types";

const basePath = "/api/v1/request-logs";

export async function listRequestLogs(
  filters: RequestLogFilters,
  limit: number,
  offset: number,
): Promise<RequestLogListResponse> {
  const query = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  if (filters.platform.trim()) {
    query.set("platform", filters.platform.trim());
  }
  if (filters.account.trim()) {
    query.set("account", filters.account.trim());
  }
  if (filters.egressError.trim()) {
    query.set("egress_error", filters.egressError.trim());
  }
  return apiRequest<RequestLogListResponse>(`${basePath}?${query.toString()}`);
}

export async function getRequestLog(id: string): Promise<RequestLogEntry> {
  return apiRequest<RequestLogEntry>(`${basePath}/${encodeURIComponent(id)}`);
}
