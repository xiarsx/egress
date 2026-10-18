import type { PassiveBreaker, PlatformAllocation } from "./types";

export const allocationPolicies: PlatformAllocation[] = [
  "BALANCED",
  "PREFER_LOW_LATENCY",
  "PREFER_IDLE_IP",
];

export const passiveBreakerOptions: PassiveBreaker[] = ["on", "off"];

export const allocationPolicyLabel: Record<PlatformAllocation, string> = {
  BALANCED: "均衡",
  PREFER_LOW_LATENCY: "优先低延迟",
  PREFER_IDLE_IP: "优先空闲出口 IP",
};

export const passiveBreakerLabel: Record<PassiveBreaker, string> = {
  on: "开启",
  off: "关闭",
};

/** filter.attrs 中承载国家过滤的属性键。 */
export const GEO_COUNTRY_ATTR_KEY = "geo.country";

/** identity_policy 为空时的回退值。 */
export const DEFAULT_IDENTITY_POLICY = "default";
