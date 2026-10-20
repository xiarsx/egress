import * as countries from "i18n-iso-countries";
import enLocale from "i18n-iso-countries/langs/en.json";
import zhLocale from "i18n-iso-countries/langs/zh.json";
import { getCurrentLocale, isEnglishLocale } from "../../i18n/locale";
import type { AppLocale } from "../../i18n/locale";

countries.registerLocale(enLocale);
countries.registerLocale(zhLocale);

export interface RegionOption {
    code: string;
    name: string;
}

function getCountryLocale(locale?: AppLocale): "zh" | "en" {
    return isEnglishLocale(locale ?? getCurrentLocale()) ? "en" : "zh";
}

// locale 显式传参：调用点若在 useMemo 中缓存结果，依赖对 eslint 可见。
export const getAllRegions = (locale: AppLocale): RegionOption[] => {
    const names = countries.getNames(getCountryLocale(locale), { select: "official" });
    return Object.entries(names).map(([code, name]) => ({
        code,
        name: `${code} (${name})`,
    })).sort((a, b) => a.code.localeCompare(b.code));
};

export const getRegionName = (code: string): string | undefined => {
    return countries.getName(code, getCountryLocale(), { select: "official" });
};
