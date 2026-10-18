export type SourceType = "remote" | "local";

export type Subscription = {
  id: string;
  name: string;
  type: SourceType;
  url: string;
  content?: string;
  update_interval: string;
  enabled: boolean;
  ephemeral: boolean;
  evict_delay: string;
  last_error?: string;
  created_at_ns: number;
  updated_at_ns: number;
};

export type PageResponse<T> = {
  items: T[];
  total: number;
  limit: number;
  offset: number;
};

export type SubscriptionCreateInput = {
  name: string;
  type?: SourceType;
  url?: string;
  content?: string;
  update_interval?: string;
  enabled?: boolean;
  ephemeral?: boolean;
  evict_delay?: string;
};

export type SubscriptionUpdateInput = {
  name?: string;
  type?: SourceType;
  url?: string;
  content?: string;
  update_interval?: string;
  enabled?: boolean;
  ephemeral?: boolean;
  evict_delay?: string;
};
