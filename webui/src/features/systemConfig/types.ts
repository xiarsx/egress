export type RuntimeConfig = {
  listen_address: string;
  port: number;
  state_dir: string;
  cache_dir: string;
  log_dir: string;
};

// GET /api/v1/system/config/env returns a flat EGRESS_* dictionary.
// Secret values are masked as "***" by the backend.
export type EnvConfig = Record<string, string>;

export type RuntimeConfigPatch = Partial<RuntimeConfig>;
