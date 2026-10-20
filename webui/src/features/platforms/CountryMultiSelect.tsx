import { useMemo, useState } from "react";
import { Input } from "../../components/ui/Input";
import { Select } from "../../components/ui/Select";
import { useI18n } from "../../i18n";
import { getAllRegions } from "../nodes/regions";
import { GEO_COUNTRY_ATTR_KEY } from "./constants";

type CountryMultiSelectProps = {
  id: string;
  value: string[];
  onChange: (codes: string[]) => void;
  invalid?: boolean;
  disabled?: boolean;
};

/** 国家多选：选项写入 filter.attrs["geo.country"].in。 */
export function CountryMultiSelect({ id, value, onChange, invalid, disabled }: CountryMultiSelectProps) {
  const { locale, t } = useI18n();
  const [keyword, setKeyword] = useState("");

  const regions = useMemo(() => getAllRegions(locale), [locale]);
  const selected = useMemo(() => new Set(value.map((code) => code.toUpperCase())), [value]);

  const visibleRegions = useMemo(() => {
    const query = keyword.trim().toLowerCase();
    const filtered = query
      ? regions.filter(
          (region) =>
            region.code.toLowerCase().includes(query) ||
            region.name.toLowerCase().includes(query),
        )
      : regions;
    // 已选但被搜索过滤掉的选项保留在顶部，避免切换选择时丢失。
    const filteredCodes = new Set(filtered.map((region) => region.code));
    const missing = regions.filter(
      (region) => selected.has(region.code) && !filteredCodes.has(region.code),
    );
    return [...missing, ...filtered];
  }, [regions, keyword, selected]);

  return (
    <div className="country-multi-select">
      <Input
        placeholder={t("搜索国家代码或名称…")}
        aria-label={t("搜索国家")}
        value={keyword}
        disabled={disabled}
        onChange={(event) => setKeyword(event.target.value)}
        style={{ marginBottom: 6 }}
      />
      <Select
        id={id}
        multiple
        size={8}
        value={Array.from(selected)}
        invalid={invalid}
        disabled={disabled}
        onChange={(event) => {
          onChange(
            Array.from(event.target.selectedOptions, (option) => option.value.toUpperCase()),
          );
        }}
      >
        {visibleRegions.map((region) => (
          <option key={region.code} value={region.code}>
            {region.name}
          </option>
        ))}
      </Select>
      <p className="muted" style={{ marginTop: 4, fontSize: 12 }}>
        {t("已选择 {{count}} 个国家；按住 Ctrl/⌘ 可多选。写入 filter.attrs[\"{{key}}\"].in。", {
          count: selected.size,
          key: GEO_COUNTRY_ATTR_KEY,
        })}
      </p>
    </div>
  );
}
