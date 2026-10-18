import { z } from "zod";
import {
  allocationPolicies,
  DEFAULT_IDENTITY_POLICY,
  GEO_COUNTRY_ATTR_KEY,
  passiveBreakerOptions,
} from "./constants";
import { parseLinesToList } from "./formParsers";
import type {
  Platform,
  PlatformCreateInput,
  PlatformFilter,
  PlatformUpdateInput,
} from "./types";

const platformNameForbiddenChars = ".:|/\\@?#%~";
const platformNameForbiddenSpacing = " \t\r\n";
const platformNameReserved = "api";

function containsAny(source: string, chars: string): boolean {
  for (const ch of chars) {
    if (source.includes(ch)) {
      return true;
    }
  }
  return false;
}

export const platformNameRuleHint = "平台名不能包含 .:|/\\@?#%~、空格、Tab、换行、回车，也不能为保留字。";

export const platformFormSchema = z.object({
  name: z.string().trim()
    .min(1, "平台名称不能为空")
    .refine((value) => !containsAny(value, platformNameForbiddenChars), {
      message: "平台名称不能包含字符 .:|/\\@?#%~",
    })
    .refine((value) => !containsAny(value, platformNameForbiddenSpacing), {
      message: "平台名称不能包含空格、Tab、换行、回车",
    })
    .refine((value) => value.toLowerCase() !== platformNameReserved, {
      message: "平台名称不能为保留字",
    }),
  sticky_ttl: z.string().optional(),
  tags_filters_text: z.string().optional(),
  countries: z.array(z.string()),
  identity_policy: z.string().optional(),
  allocation: z.enum(allocationPolicies),
  passive_breaker: z.enum(passiveBreakerOptions),
});

export type PlatformFormValues = z.infer<typeof platformFormSchema>;

export const defaultPlatformFormValues: PlatformFormValues = {
  name: "",
  sticky_ttl: "",
  tags_filters_text: "",
  countries: [],
  identity_policy: DEFAULT_IDENTITY_POLICY,
  allocation: "BALANCED",
  passive_breaker: "on",
};

export function platformToFormValues(platform: Platform): PlatformFormValues {
  const filter = platform.filter ?? {};
  const tags = Array.isArray(filter.tags) ? filter.tags : [];
  const countryIn = filter.attrs?.[GEO_COUNTRY_ATTR_KEY]?.in;

  return {
    name: platform.name,
    sticky_ttl: platform.sticky_ttl ?? "",
    tags_filters_text: tags.join("\n"),
    countries: Array.isArray(countryIn) ? countryIn.filter((code) => typeof code === "string") : [],
    identity_policy: platform.identity_policy || DEFAULT_IDENTITY_POLICY,
    allocation: platform.allocation ?? "BALANCED",
    passive_breaker: platform.passive_breaker ?? "on",
  };
}

function buildFilter(values: PlatformFormValues): PlatformFilter | undefined {
  const tags = parseLinesToList(values.tags_filters_text);
  const countries = values.countries
    .map((code) => code.trim().toUpperCase())
    .filter(Boolean);

  const filter: PlatformFilter = {};
  if (tags.length > 0) {
    filter.tags = tags;
  }
  if (countries.length > 0) {
    filter.attrs = { [GEO_COUNTRY_ATTR_KEY]: { in: countries } };
  }
  return Object.keys(filter).length > 0 ? filter : undefined;
}

function toPlatformPayloadBase(values: PlatformFormValues) {
  return {
    name: values.name.trim(),
    allocation: values.allocation,
    passive_breaker: values.passive_breaker,
    identity_policy: values.identity_policy?.trim() || DEFAULT_IDENTITY_POLICY,
    filter: buildFilter(values),
  };
}

export function toPlatformCreateInput(values: PlatformFormValues): PlatformCreateInput {
  return {
    ...toPlatformPayloadBase(values),
    sticky_ttl: values.sticky_ttl?.trim() || undefined,
  };
}

export function toPlatformUpdateInput(values: PlatformFormValues): PlatformUpdateInput {
  return {
    ...toPlatformPayloadBase(values),
    sticky_ttl: values.sticky_ttl?.trim() || "",
  };
}
