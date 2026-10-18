import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Plus, RefreshCw, Search, Sparkles, X } from "lucide-react";
import { useMemo, useState } from "react";
import { Controller, useForm } from "react-hook-form";
import { useNavigate } from "react-router-dom";
import { Badge } from "../../components/ui/Badge";
import { Button } from "../../components/ui/Button";
import { Card } from "../../components/ui/Card";
import { Input } from "../../components/ui/Input";
import { OffsetPagination } from "../../components/ui/OffsetPagination";
import { Select } from "../../components/ui/Select";
import { Textarea } from "../../components/ui/Textarea";
import { ToastContainer } from "../../components/ui/Toast";
import { useToast } from "../../hooks/useToast";
import { useI18n } from "../../i18n";
import { formatApiErrorMessage } from "../../lib/error-message";
import { formatGoDuration, formatRelativeTime } from "../../lib/time";
import { createPlatform, listPlatforms } from "./api";
import {
  allocationPolicies,
  allocationPolicyLabel,
  GEO_COUNTRY_ATTR_KEY,
  passiveBreakerLabel,
  passiveBreakerOptions,
} from "./constants";
import { CountryMultiSelect } from "./CountryMultiSelect";
import {
  defaultPlatformFormValues,
  platformFormSchema,
  platformNameRuleHint,
  toPlatformCreateInput,
  type PlatformFormValues,
} from "./formModel";
import { formatNsToIso, type Platform } from "./types";

const ZERO_UUID = "00000000-0000-0000-0000-000000000000";
const EMPTY_PLATFORMS: Platform[] = [];
const PAGE_SIZE_OPTIONS = [12, 24, 48, 96] as const;

function countryCount(platform: Platform): number {
  return platform.filter?.attrs?.[GEO_COUNTRY_ATTR_KEY]?.in?.length ?? 0;
}

function tagCount(platform: Platform): number {
  return platform.filter?.tags?.length ?? 0;
}

export function PlatformPage() {
  const { t } = useI18n();
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState<number>(24);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const { toasts, showToast, dismissToast } = useToast();

  const queryClient = useQueryClient();
  const formatPlatformMutationError = (error: unknown) => {
    const base = formatApiErrorMessage(error, t);
    if (base.includes("name:")) {
      return `${base}；${t(platformNameRuleHint)}`;
    }
    return base;
  };

  const platformsQuery = useQuery({
    queryKey: ["platforms", "page", page, pageSize],
    queryFn: () =>
      listPlatforms({
        limit: pageSize,
        offset: page * pageSize,
      }),
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });

  // 后端列表接口暂不支持 keyword/sort_by，在当前页内做过滤与按名称排序。
  const platforms = useMemo(() => {
    const items = platformsQuery.data?.items ?? EMPTY_PLATFORMS;
    const keyword = search.trim().toLowerCase();
    const filtered = keyword
      ? items.filter((platform) => platform.name.toLowerCase().includes(keyword))
      : [...items];
    filtered.sort((a, b) => a.name.localeCompare(b.name));
    return filtered;
  }, [platformsQuery.data, search]);

  const totalPlatforms = platformsQuery.data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalPlatforms / pageSize));
  const currentPage = Math.min(page, totalPages - 1);

  const createForm = useForm<PlatformFormValues>({
    resolver: zodResolver(platformFormSchema),
    defaultValues: defaultPlatformFormValues,
  });

  const createMutation = useMutation({
    mutationFn: createPlatform,
    onSuccess: async (created) => {
      await queryClient.invalidateQueries({ queryKey: ["platforms"] });
      setCreateModalOpen(false);
      createForm.reset();
      showToast("success", t("平台 {{name}} 创建成功", { name: created.name }));
      navigate(`/platforms/${created.id}`);
    },
    onError: (error) => {
      showToast("error", formatPlatformMutationError(error));
    },
  });

  const onCreateSubmit = createForm.handleSubmit(async (values) => {
    await createMutation.mutateAsync(toPlatformCreateInput(values));
  });

  const changePageSize = (next: number) => {
    setPageSize(next);
    setPage(0);
  };

  return (
    <section className="platform-page">
      <header className="module-header">
        <div>
          <h2>{t("平台管理")}</h2>
          <p className="module-description">{t("集中维护平台策略与节点分配规则。")}</p>
        </div>
      </header>

      <ToastContainer toasts={toasts} onDismiss={dismissToast} />

      <Card className="platform-list-card platform-directory-card">
        <div className="list-card-header">
          <div>
            <h3>{t("平台列表")}</h3>
            <p>{t("共 {{count}} 个平台", { count: totalPlatforms })}</p>
          </div>
          <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
            <label className="search-box" htmlFor="platform-search" style={{ maxWidth: 200, margin: 0, gap: 6 }}>
              <Search size={16} />
              <Input
                id="platform-search"
                placeholder={t("搜索平台")}
                value={search}
                onChange={(event) => {
                  setSearch(event.target.value);
                  setPage(0);
                }}
                style={{ padding: "6px 10px", borderRadius: 8 }}
              />
            </label>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setCreateModalOpen(true)}
            >
              <Plus size={16} />
              {t("新建")}
            </Button>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => platformsQuery.refetch()}
              disabled={platformsQuery.isFetching}
            >
              <RefreshCw size={16} className={platformsQuery.isFetching ? "spin" : undefined} />
              {t("刷新")}
            </Button>
          </div>
        </div>
      </Card>

      <Card className="platform-cards-container">
        {platformsQuery.isLoading ? <p className="muted">{t("正在加载平台数据...")}</p> : null}

        {platformsQuery.isError ? (
          <div className="callout callout-error">
            <AlertTriangle size={14} />
            <span>{formatApiErrorMessage(platformsQuery.error, t)}</span>
          </div>
        ) : null}

        {!platformsQuery.isLoading && !platforms.length ? (
          <div className="empty-box">
            <Sparkles size={16} />
            <p>{t("没有匹配的平台")}</p>
          </div>
        ) : null}

        <div className="platform-card-grid">
          {platforms.map((platform) => {
            const stickyTTL = formatGoDuration(platform.sticky_ttl, t("默认"));

            return (
              <button
                key={platform.id}
                type="button"
                className="platform-tile"
                onClick={() => navigate(`/platforms/${platform.id}`)}
              >
                <div className="platform-tile-head">
                  <p>{platform.name}</p>
                  <Badge variant={platform.id === ZERO_UUID ? "warning" : "success"}>
                    {platform.id === ZERO_UUID ? t("内置平台") : t("自定义平台")}
                  </Badge>
                </div>
                <div className="platform-tile-facts">
                  <span className="platform-fact">
                    <span>{t("区域")}</span>
                    <strong>{countryCount(platform)}</strong>
                  </span>
                  <span className="platform-fact">
                    <span>{t("标签")}</span>
                    <strong>{tagCount(platform)}</strong>
                  </span>
                  <span className="platform-fact">
                    <span>{t("租约时长")}</span>
                    <strong>{stickyTTL}</strong>
                  </span>
                </div>
                <div className="platform-tile-foot">
                  <span className="platform-tile-meta platform-tile-updated">
                    {t("更新于 {{time}}", { time: formatRelativeTime(formatNsToIso(platform.updated_at_ns)) })}
                  </span>
                </div>
              </button>
            );
          })}
        </div>

        <OffsetPagination
          page={currentPage}
          totalPages={totalPages}
          totalItems={totalPlatforms}
          pageSize={pageSize}
          pageSizeOptions={PAGE_SIZE_OPTIONS}
          onPageChange={setPage}
          onPageSizeChange={changePageSize}
        />
      </Card>

      {createModalOpen ? (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <Card className="modal-card">
            <div className="modal-header">
              <h3>{t("新建平台")}</h3>
              <Button variant="ghost" size="sm" aria-label={t("关闭")} onClick={() => setCreateModalOpen(false)}>
                <X size={16} />
              </Button>
            </div>

            <form className="form-grid" onSubmit={onCreateSubmit}>
              <div className="field-group">
                <label className="field-label" htmlFor="create-name">
                  {t("名称")}
                </label>
                <Input id="create-name" invalid={Boolean(createForm.formState.errors.name)} {...createForm.register("name")} />
                {createForm.formState.errors.name?.message ? (
                  <p className="field-error">{t(createForm.formState.errors.name.message)}</p>
                ) : null}
                <p className="muted" style={{ marginTop: 4, fontSize: 12 }}>
                  {t(platformNameRuleHint)}
                </p>
              </div>

              <div className="field-group">
                <label className="field-label" htmlFor="create-sticky">
                  {t("租约保持时长（可选）")}
                </label>
                <Input id="create-sticky" placeholder={t("例如 168h")} {...createForm.register("sticky_ttl")} />
              </div>

              <div className="field-group">
                <label className="field-label" htmlFor="create-identity-policy">
                  {t("身份策略")}
                </label>
                <Input
                  id="create-identity-policy"
                  placeholder="default"
                  {...createForm.register("identity_policy")}
                />
                <p className="muted" style={{ marginTop: 4, fontSize: 12 }}>
                  {t("引用的 IdentityPolicy 名称，留空则使用 default。")}
                </p>
              </div>

              <div className="field-group">
                <label className="field-label" htmlFor="create-policy">
                  {t("节点分配策略")}
                </label>
                <Select id="create-policy" {...createForm.register("allocation")}>
                  {allocationPolicies.map((item) => (
                    <option key={item} value={item}>
                      {t(allocationPolicyLabel[item])}
                    </option>
                  ))}
                </Select>
              </div>

              <div className="field-group">
                <label className="field-label" htmlFor="create-passive-breaker">
                  {t("被动熔断")}
                </label>
                <Select id="create-passive-breaker" {...createForm.register("passive_breaker")}>
                  {passiveBreakerOptions.map((item) => (
                    <option key={item} value={item}>
                      {t(passiveBreakerLabel[item])}
                    </option>
                  ))}
                </Select>
                <p className="muted" style={{ marginTop: 4, fontSize: 12 }}>
                  {t("开启后，此平台的代理请求失败会计入节点熔断计数；主动探测不受影响。")}
                </p>
              </div>

              <div className="field-group">
                <label className="field-label" htmlFor="create-tags">
                  {t("节点标签过滤规则（可选）")}
                </label>
                <Textarea
                  id="create-tags"
                  rows={4}
                  placeholder={t("每行一条标签，例如：\n\n香港\n日本\n*专线\n!过期\n!失效")}
                  {...createForm.register("tags_filters_text")}
                />
                <div className="muted" style={{ marginTop: 4, fontSize: 12 }}>
                  <div>{t("无前缀表示满足其一（ANY），* 开头表示必须包含（MUST），! 开头表示排除（MUST_NOT）。写入 filter.tags。")}</div>
                </div>
              </div>

              <div className="field-group">
                <label className="field-label" htmlFor="create-countries">
                  {t("国家过滤（可选）")}
                </label>
                <Controller
                  control={createForm.control}
                  name="countries"
                  render={({ field }) => (
                    <CountryMultiSelect
                      id="create-countries"
                      value={field.value}
                      onChange={field.onChange}
                      invalid={Boolean(createForm.formState.errors.countries)}
                    />
                  )}
                />
              </div>

              <div className="detail-actions">
                <Button type="submit" disabled={createMutation.isPending}>
                  {createMutation.isPending ? t("创建中...") : t("确认创建")}
                </Button>
                <Button variant="secondary" onClick={() => setCreateModalOpen(false)}>
                  {t("取消")}
                </Button>
              </div>
            </form>
          </Card>
        </div>
      ) : null}
    </section>
  );
}
