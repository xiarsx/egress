import { apiRequest } from "../../lib/api-client";
import type {
  PageResponse,
  SourceType,
  Subscription,
  SubscriptionCreateInput,
  SubscriptionUpdateInput,
} from "./types";

const basePath = "/api/v1/sources";

type ApiSubscription = {
  id: string;
  name: string;
  type?: SourceType;
  url?: string;
  content?: string;
  update_interval?: string;
  enabled?: boolean;
  ephemeral?: boolean;
  evict_delay?: string;
  last_error?: string | null;
  created_at_ns?: number | null;
  updated_at_ns?: number | null;
};

function normalizeSubscription(raw: ApiSubscription): Subscription {
  return {
    id: raw.id,
    name: raw.name,
    type: raw.type ?? "remote",
    url: raw.url ?? "",
    content: raw.content ?? "",
    update_interval: raw.update_interval ?? "",
    enabled: raw.enabled ?? true,
    ephemeral: raw.ephemeral ?? false,
    evict_delay: raw.evict_delay ?? "",
    last_error: raw.last_error || "",
    created_at_ns: raw.created_at_ns ?? 0,
    updated_at_ns: raw.updated_at_ns ?? 0,
  };
}

function normalizeSubscriptionPage(raw: PageResponse<ApiSubscription>): PageResponse<Subscription> {
  return {
    ...raw,
    items: raw.items.map(normalizeSubscription),
  };
}

export type ListSubscriptionsInput = {
  limit?: number;
  offset?: number;
};

export async function listSubscriptions(input: ListSubscriptionsInput = {}): Promise<PageResponse<Subscription>> {
  const query = new URLSearchParams({
    limit: String(input.limit ?? 50),
    offset: String(input.offset ?? 0),
  });

  const data = await apiRequest<PageResponse<ApiSubscription>>(`${basePath}?${query.toString()}`);
  return normalizeSubscriptionPage(data);
}

export async function createSubscription(input: SubscriptionCreateInput): Promise<Subscription> {
  const data = await apiRequest<ApiSubscription>(basePath, {
    method: "POST",
    body: input,
  });
  return normalizeSubscription(data);
}

export async function updateSubscription(id: string, input: SubscriptionUpdateInput): Promise<Subscription> {
  const data = await apiRequest<ApiSubscription>(`${basePath}/${id}`, {
    method: "PATCH",
    body: input,
  });
  return normalizeSubscription(data);
}

export async function deleteSubscription(id: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}`, {
    method: "DELETE",
  });
}

export async function refreshSubscription(id: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}/actions/refresh`, {
    method: "POST",
  });
}
