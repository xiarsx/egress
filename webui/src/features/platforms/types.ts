export type PlatformAllocation = "BALANCED" | "PREFER_LOW_LATENCY" | "PREFER_IDLE_IP";
export type PassiveBreaker = "on" | "off";

export type PlatformAttrCondition = {
  in?: string[] | null;
  notIn?: string[] | null;
  match?: string | null;
  notMatch?: string | null;
  gte?: number | null;
  lte?: number | null;
};

export type PlatformFilter = {
  tags?: string[] | null;
  attrs?: Record<string, PlatformAttrCondition> | null;
};

export type Platform = {
  id: string;
  name: string;
  sticky_ttl: string;
  allocation: PlatformAllocation;
  passive_breaker: PassiveBreaker;
  identity_policy: string;
  filter: PlatformFilter;
  updated_at_ns: number;
};

export type PageResponse<T> = {
  items: T[];
  total: number;
  limit: number;
  offset: number;
};

export type PlatformCreateInput = {
  name: string;
  sticky_ttl?: string;
  allocation?: PlatformAllocation;
  passive_breaker?: PassiveBreaker;
  identity_policy?: string;
  filter?: PlatformFilter;
};

export type PlatformUpdateInput = {
  name?: string;
  sticky_ttl?: string;
  allocation?: PlatformAllocation;
  passive_breaker?: PassiveBreaker;
  identity_policy?: string;
  filter?: PlatformFilter;
};

export type PlatformLease = {
  platform_id: string;
  account: string;
  egress_ip: string;
  node_hash: string;
  expiry_ns: number;
  last_accessed_ns: number;
};

export type PlatformIpLoad = Record<string, number>;

export type PlatformFilterPreview = {
  matched: number;
  total: number;
};

export type ListPlatformLeasesInput = {
  limit?: number;
  offset?: number;
};

/** 纳秒时间戳转 ISO 字符串；缺失/非法时返回空字符串。 */
export function formatNsToIso(ns: number | null | undefined): string {
  if (typeof ns !== "number" || !Number.isFinite(ns) || ns <= 0) {
    return "";
  }
  return new Date(Math.floor(ns / 1_000_000)).toISOString();
}
