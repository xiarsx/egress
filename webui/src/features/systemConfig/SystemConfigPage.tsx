import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, RefreshCw, Save } from "lucide-react";
import { useMemo, useState } from "react";
import { Button } from "../../components/ui/Button";
import { Card } from "../../components/ui/Card";
import { DataTable } from "../../components/ui/DataTable";
import { Input } from "../../components/ui/Input";
import { ToastContainer } from "../../components/ui/Toast";
import { useToast } from "../../hooks/useToast";
import { useI18n } from "../../i18n";
import { formatApiErrorMessage } from "../../lib/error-message";
import { getEnvConfig, getSystemConfig, patchSystemConfig } from "./api";
import type { RuntimeConfig, RuntimeConfigPatch } from "./types";

type RuntimeConfigForm = {
  listen_address: string;
  port: string;
  state_dir: string;
  cache_dir: string;
  log_dir: string;
};

const FIELD_LABELS: Record<keyof RuntimeConfig, string> = {
  listen_address: "监听地址",
  port: "服务端口",
  state_dir: "状态目录",
  cache_dir: "缓存目录",
  log_dir: "日志目录",
};

function configToForm(config: RuntimeConfig): RuntimeConfigForm {
  return {
    listen_address: config.listen_address,
    port: String(config.port),
    state_dir: config.state_dir,
    cache_dir: config.cache_dir,
    log_dir: config.log_dir,
  };
}

function parsePort(field: string, raw: string): number {
  const value = raw.trim();
  if (!value) {
    throw new Error(`${field} 不能为空`);
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new Error(`${field} 必须是 1-65535 的整数`);
  }
  return parsed;
}

function parseForm(form: RuntimeConfigForm): RuntimeConfig {
  return {
    listen_address: form.listen_address.trim(),
    port: parsePort("服务端口", form.port),
    state_dir: form.state_dir.trim(),
    cache_dir: form.cache_dir.trim(),
    log_dir: form.log_dir.trim(),
  };
}

function buildPatch(current: RuntimeConfig, next: RuntimeConfig): RuntimeConfigPatch {
  const patch: RuntimeConfigPatch = {};
  const patchMutable = patch as Record<string, unknown>;
  for (const field of Object.keys(FIELD_LABELS) as Array<keyof RuntimeConfig>) {
    if (current[field] !== next[field]) {
      patchMutable[field] = next[field];
    }
  }
  return patch;
}

export function SystemConfigPage() {
  const { t } = useI18n();
  const [draftForm, setDraftForm] = useState<RuntimeConfigForm | null>(null);
  const { toasts, showToast, dismissToast } = useToast();
  const queryClient = useQueryClient();

  const configQuery = useQuery({
    queryKey: ["system-config"],
    queryFn: getSystemConfig,
    staleTime: 30_000,
  });

  const envConfigQuery = useQuery({
    queryKey: ["system-config-env"],
    queryFn: getEnvConfig,
    staleTime: Infinity, // Env config does not change at runtime
  });

  const baseline = configQuery.data ?? null;
  const envBaseline = envConfigQuery.data ?? null;

  const form = useMemo(() => {
    if (!baseline) {
      return null;
    }
    return draftForm ?? configToForm(baseline);
  }, [baseline, draftForm]);

  const parsedResult = useMemo(() => {
    if (!form) {
      return { config: null as RuntimeConfig | null, error: "" };
    }

    try {
      return { config: parseForm(form), error: "" };
    } catch (error) {
      return { config: null, error: formatApiErrorMessage(error, t) };
    }
  }, [form, t]);

  const envRows = useMemo(
    () =>
      envBaseline
        ? Object.entries(envBaseline)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([key, value]) => ({ key, value }))
        : [],
    [envBaseline],
  );

  const saveMutation = useMutation({
    mutationFn: async () => {
      if (!baseline || !form) {
        throw new Error("配置尚未加载完成");
      }
      const parsed = parseForm(form);
      const patch = buildPatch(baseline, parsed);
      const changedCount = Object.keys(patch).length;
      if (!changedCount) {
        throw new Error("没有可提交的变更");
      }
      const updated = await patchSystemConfig(patch);
      return { updated, changedCount };
    },
    onSuccess: ({ updated, changedCount }) => {
      queryClient.setQueryData(["system-config"], updated);
      setDraftForm(null);
      showToast("success", t("配置已更新（{{count}} 项变更）", { count: changedCount }));
    },
    onError: (error) => {
      showToast("error", formatApiErrorMessage(error, t));
    },
  });

  const setFormField = <K extends keyof RuntimeConfigForm>(key: K, value: RuntimeConfigForm[K]) => {
    setDraftForm((prev) => {
      if (!baseline) {
        return prev;
      }
      const source = prev ?? configToForm(baseline);
      return { ...source, [key]: value };
    });
  };

  const reloadFromServer = async () => {
    if (parsedResult.config && baseline && Object.keys(buildPatch(baseline, parsedResult.config)).length > 0) {
      const confirmed = window.confirm(t("当前有未保存变更，确认丢弃并重新加载运行时配置？"));
      if (!confirmed) {
        return;
      }
    }

    setDraftForm(null);
    const result = await configQuery.refetch();
    if (result.data) {
      showToast("success", t("已加载最新运行时配置"));
    }
  };

  const isSaveDisabled =
    saveMutation.isPending || Boolean(parsedResult.error) || !baseline || !parsedResult.config ||
    Object.keys(buildPatch(baseline, parsedResult.config)).length === 0;

  return (
    <section className="syscfg-page">
      <header className="module-header">
        <div>
          <h2>{t("系统配置")}</h2>
          <p className="module-description">{t("查看和调整系统运行参数，保存后立即生效。")}</p>
        </div>
      </header>

      <ToastContainer toasts={toasts} onDismiss={dismissToast} />

      {!form ? (
        <Card className="syscfg-form-card platform-directory-card">
          {configQuery.isLoading || envConfigQuery.isLoading ? <p className="muted">{t("正在加载配置...")}</p> : null}
          {configQuery.isError ? (
            <div className="callout callout-error">
              <AlertTriangle size={14} />
              <span>{formatApiErrorMessage(configQuery.error, t)}</span>
            </div>
          ) : null}
          {envConfigQuery.isError ? (
            <div className="callout callout-error">
              <AlertTriangle size={14} />
              <span>
                {t("静态配置加载失败")}: {formatApiErrorMessage(envConfigQuery.error, t)}
              </span>
            </div>
          ) : null}
        </Card>
      ) : (
        <div className="syscfg-layout">
          <div className="syscfg-main" style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
            <Card className="syscfg-form-card platform-directory-card">
              <div className="detail-header">
                <div>
                  <h3>{t("运行时配置")}</h3>
                  <p>{t("修改后点击保存，变更立即生效。")}</p>
                </div>
                <Button variant="secondary" size="sm" onClick={() => void reloadFromServer()} disabled={configQuery.isFetching}>
                  <RefreshCw size={16} className={configQuery.isFetching ? "spin" : undefined} />
                  {t("刷新")}
                </Button>
              </div>

              {parsedResult.error ? (
                <div className="callout callout-error">
                  <AlertTriangle size={14} />
                  <span>{parsedResult.error}</span>
                </div>
              ) : null}

              <div className="form-grid">
                <div className="field-group">
                  <label className="field-label" htmlFor="sys-listen-address">
                    {t("监听地址")}
                  </label>
                  <Input
                    id="sys-listen-address"
                    value={form.listen_address}
                    onChange={(event) => setFormField("listen_address", event.target.value)}
                  />
                </div>

                <div className="field-group">
                  <label className="field-label" htmlFor="sys-port">
                    {t("服务端口")}
                  </label>
                  <Input
                    id="sys-port"
                    type="number"
                    min={1}
                    max={65535}
                    value={form.port}
                    onChange={(event) => setFormField("port", event.target.value)}
                  />
                </div>

                <div className="field-group">
                  <label className="field-label" htmlFor="sys-state-dir">
                    {t("状态目录")}
                  </label>
                  <Input
                    id="sys-state-dir"
                    value={form.state_dir}
                    onChange={(event) => setFormField("state_dir", event.target.value)}
                  />
                </div>

                <div className="field-group">
                  <label className="field-label" htmlFor="sys-cache-dir">
                    {t("缓存目录")}
                  </label>
                  <Input
                    id="sys-cache-dir"
                    value={form.cache_dir}
                    onChange={(event) => setFormField("cache_dir", event.target.value)}
                  />
                </div>

                <div className="field-group">
                  <label className="field-label" htmlFor="sys-log-dir">
                    {t("日志目录")}
                  </label>
                  <Input
                    id="sys-log-dir"
                    value={form.log_dir}
                    onChange={(event) => setFormField("log_dir", event.target.value)}
                  />
                </div>
              </div>

              <div className="detail-actions" style={{ justifyContent: "flex-end", marginTop: "16px" }}>
                <Button onClick={() => void saveMutation.mutateAsync()} disabled={isSaveDisabled}>
                  <Save size={14} />
                  {saveMutation.isPending ? t("保存中...") : t("保存配置")}
                </Button>
              </div>
            </Card>

            <Card className="syscfg-form-card platform-directory-card syscfg-static-card">
              <div className="detail-header">
                <div>
                  <h3>{t("环境变量")}</h3>
                  <p>{t("来自 EGRESS_* 环境变量的只读配置，令牌类值已脱敏。")}</p>
                </div>
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={async () => {
                    const result = await envConfigQuery.refetch();
                    if (result.data) showToast("success", t("已加载最新环境变量"));
                  }}
                  disabled={envConfigQuery.isFetching}
                >
                  <RefreshCw size={16} className={envConfigQuery.isFetching ? "spin" : undefined} />
                  {t("刷新")}
                </Button>
              </div>

              <DataTable
                data={envRows}
                getRowId={(row) => row.key}
                columns={[
                  {
                    id: "key",
                    header: () => t("变量"),
                    cell: (ctx) => <code>{ctx.row.original.key}</code>,
                  },
                  {
                    id: "value",
                    header: () => t("值"),
                    cell: (ctx) => <code>{ctx.row.original.value || "—"}</code>,
                  },
                ]}
              />
            </Card>
          </div>
        </div>
      )}
    </section>
  );
}
