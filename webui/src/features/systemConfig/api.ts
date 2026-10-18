import { apiRequest } from "../../lib/api-client";
import type { EnvConfig, RuntimeConfig, RuntimeConfigPatch } from "./types";

const path = "/api/v1/system/config";

const DEFAULT_CONFIG: RuntimeConfig = {
  listen_address: "",
  port: 2260,
  state_dir: "",
  cache_dir: "",
  log_dir: "",
};

function asNumber(raw: unknown, fallback: number): number {
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return value;
}

function asString(raw: unknown, fallback: string): string {
  if (typeof raw !== "string") {
    return fallback;
  }
  return raw;
}

function normalizeRuntimeConfig(raw: Partial<RuntimeConfig> | null | undefined): RuntimeConfig {
  if (!raw) {
    return DEFAULT_CONFIG;
  }

  return {
    listen_address: asString(raw.listen_address, DEFAULT_CONFIG.listen_address),
    port: asNumber(raw.port, DEFAULT_CONFIG.port),
    state_dir: asString(raw.state_dir, DEFAULT_CONFIG.state_dir),
    cache_dir: asString(raw.cache_dir, DEFAULT_CONFIG.cache_dir),
    log_dir: asString(raw.log_dir, DEFAULT_CONFIG.log_dir),
  };
}

export async function getSystemConfig(): Promise<RuntimeConfig> {
  const data = await apiRequest<RuntimeConfig>(path);
  return normalizeRuntimeConfig(data);
}

export async function getDefaultSystemConfig(): Promise<RuntimeConfig> {
  const data = await apiRequest<RuntimeConfig>(path + "/default");
  return normalizeRuntimeConfig(data);
}

export async function patchSystemConfig(patch: RuntimeConfigPatch): Promise<RuntimeConfig> {
  const data = await apiRequest<RuntimeConfig>(path, {
    method: "PATCH",
    body: patch,
  });
  return normalizeRuntimeConfig(data);
}

export async function getEnvConfig(): Promise<EnvConfig> {
  return await apiRequest<EnvConfig>(path + "/env");
}
