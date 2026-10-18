import { apiRequest } from "../../lib/api-client";
import { DEFAULT_IDENTITY_POLICY } from "./constants";
import type {
  ListPlatformLeasesInput,
  PageResponse,
  Platform,
  PlatformCreateInput,
  PlatformFilter,
  PlatformFilterPreview,
  PlatformIpLoad,
  PlatformLease,
  PlatformUpdateInput,
} from "./types";

const basePath = "/api/v1/platforms";

type ApiPlatformFilterCondition = {
  in?: string[] | null;
  notIn?: string[] | null;
  match?: string | null;
  notMatch?: string | null;
  gte?: number | null;
  lte?: number | null;
};

type ApiPlatformFilter = {
  tags?: string[] | null;
  attrs?: Record<string, ApiPlatformFilterCondition> | null;
};

type ApiPlatform = Partial<Omit<Platform, "filter">> & {
  filter?: ApiPlatformFilter | null;
};

type ApiPlatformLease = Partial<PlatformLease>;

function toStringOrEmpty(raw: unknown): string {
  return typeof raw === "string" ? raw : "";
}

function toNumberOrZero(raw: unknown): number {
  const value = Number(raw);
  return Number.isFinite(value) ? value : 0;
}

function normalizeFilter(raw: ApiPlatform["filter"]): Platform["filter"] {
  const tags = Array.isArray(raw?.tags)
    ? raw.tags.filter((tag): tag is string => typeof tag === "string")
    : [];
  const attrs =
    raw?.attrs && typeof raw.attrs === "object" ? (raw.attrs as Platform["filter"]["attrs"]) : {};
  return { tags, attrs: attrs ?? {} };
}

function normalizePlatform(raw: ApiPlatform): Platform {
  return {
    id: toStringOrEmpty(raw.id),
    name: toStringOrEmpty(raw.name),
    sticky_ttl: toStringOrEmpty(raw.sticky_ttl),
    allocation:
      raw.allocation === "BALANCED" ||
      raw.allocation === "PREFER_LOW_LATENCY" ||
      raw.allocation === "PREFER_IDLE_IP"
        ? raw.allocation
        : "BALANCED",
    passive_breaker: raw.passive_breaker === "on" || raw.passive_breaker === "off" ? raw.passive_breaker : "on",
    identity_policy: toStringOrEmpty(raw.identity_policy) || DEFAULT_IDENTITY_POLICY,
    filter: normalizeFilter(raw.filter),
    updated_at_ns: toNumberOrZero(raw.updated_at_ns),
  };
}

function normalizePlatformPage(raw: PageResponse<ApiPlatform>): PageResponse<Platform> {
  return {
    ...raw,
    items: Array.isArray(raw.items) ? raw.items.map(normalizePlatform) : [],
  };
}

function normalizeLease(raw: ApiPlatformLease): PlatformLease {
  return {
    platform_id: toStringOrEmpty(raw.platform_id),
    account: toStringOrEmpty(raw.account),
    egress_ip: toStringOrEmpty(raw.egress_ip),
    node_hash: toStringOrEmpty(raw.node_hash),
    expiry_ns: toNumberOrZero(raw.expiry_ns),
    last_accessed_ns: toNumberOrZero(raw.last_accessed_ns),
  };
}

function normalizeLeasePage(raw: PageResponse<ApiPlatformLease>): PageResponse<PlatformLease> {
  return {
    ...raw,
    items: Array.isArray(raw.items) ? raw.items.map(normalizeLease) : [],
  };
}

export type ListPlatformsPageInput = {
  limit?: number;
  offset?: number;
};

export async function listPlatforms(input: ListPlatformsPageInput = {}): Promise<PageResponse<Platform>> {
  const query = new URLSearchParams({
    limit: String(input.limit ?? 50),
    offset: String(input.offset ?? 0),
  });

  const data = await apiRequest<PageResponse<ApiPlatform>>(`${basePath}?${query.toString()}`);
  return normalizePlatformPage(data);
}

export async function getPlatform(id: string): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(`${basePath}/${id}`);
  return normalizePlatform(data);
}

export async function createPlatform(input: PlatformCreateInput): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(basePath, {
    method: "POST",
    body: input,
  });
  return normalizePlatform(data);
}

export async function updatePlatform(id: string, input: PlatformUpdateInput): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(`${basePath}/${id}`, {
    method: "PATCH",
    body: input,
  });
  return normalizePlatform(data);
}

export async function deletePlatform(id: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}`, {
    method: "DELETE",
  });
}

export async function resetPlatform(id: string): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(`${basePath}/${id}/actions/reset-to-default`, {
    method: "POST",
  });
  return normalizePlatform(data);
}

export async function rebuildPlatform(id: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}/actions/rebuild-routable-view`, {
    method: "POST",
  });
}

export async function previewPlatformFilter(filter: PlatformFilter): Promise<PlatformFilterPreview> {
  const data = await apiRequest<{ matched?: number; total?: number }>(
    `${basePath}/preview-filter`,
    {
      method: "POST",
      body: { filter },
    },
  );
  return {
    matched: toNumberOrZero(data?.matched),
    total: toNumberOrZero(data?.total),
  };
}

export async function listPlatformLeases(id: string, input: ListPlatformLeasesInput = {}): Promise<PageResponse<PlatformLease>> {
  const query = new URLSearchParams({
    limit: String(input.limit ?? 50),
    offset: String(input.offset ?? 0),
  });

  const data = await apiRequest<PageResponse<ApiPlatformLease>>(`${basePath}/${id}/leases?${query.toString()}`);
  return normalizeLeasePage(data);
}

export async function getPlatformLease(id: string, account: string): Promise<PlatformLease> {
  const data = await apiRequest<ApiPlatformLease>(`${basePath}/${id}/leases/${encodeURIComponent(account)}`);
  return normalizeLease(data);
}

export async function deletePlatformLease(id: string, account: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}/leases/${encodeURIComponent(account)}`, {
    method: "DELETE",
  });
}

export async function clearAllPlatformLeases(id: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}/leases`, {
    method: "DELETE",
  });
}

export async function getPlatformIpLoad(id: string): Promise<PlatformIpLoad> {
  const data = await apiRequest<Record<string, number> | null>(`${basePath}/${id}/ip-load`);
  const result: PlatformIpLoad = {};
  for (const [ip, load] of Object.entries(data ?? {})) {
    result[ip] = toNumberOrZero(load);
  }
  return result;
}
