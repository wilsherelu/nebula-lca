import { getApiBase } from "../apiBase";
import { useEffect, useMemo, useState } from "react";

const API_BASE = getApiBase();

export type TidasLocationOption = {
  code: string;
  name_en?: string;
  name_zh?: string;
  aliases?: string[];
};

export const normalizeTidasLocationValue = (value: string, options: TidasLocationOption[] = []): string => {
  const raw = value.trim();
  if (!raw) return "";
  const key = normalizeLocationAlias(raw);
  for (const option of options) {
    const candidates = [
      option.code,
      option.name_en ?? "",
      option.name_zh ?? "",
      ...(option.aliases ?? []),
    ].filter((candidate) => candidate.trim().length > 0);
    if (candidates.some((candidate) => normalizeLocationAlias(candidate) === key)) {
      return option.code;
    }
  }
  return raw;
};

type Props = {
  value: string;
  uiLanguage: "zh" | "en";
  allowBlank?: boolean;
  blankLabel?: string;
  onChange: (value: string) => void;
  onCatalogStatus?: (ok: boolean, errorText: string) => void;
};

export function TidasLocationCascade({
  value,
  uiLanguage,
  allowBlank = true,
  blankLabel,
  onChange,
  onCatalogStatus,
}: Props) {
  const [options, setOptions] = useState<TidasLocationOption[]>([]);
  const [loadError, setLoadError] = useState("");
  const zh = uiLanguage === "zh";

  useEffect(() => {
    let canceled = false;
    const loadLocations = async () => {
      try {
        const resp = await fetch(`${API_BASE}/export/tidas/reference/locations`, { cache: "no-store" });
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }
        const payload = (await resp.json()) as { items?: TidasLocationOption[] };
        const items = Array.isArray(payload.items) ? payload.items.filter((item) => item.code) : [];
        if (items.length < 50) {
          throw new Error("TIDAS location catalog is incomplete");
        }
        if (!canceled) {
          setOptions(items);
          setLoadError("");
          onCatalogStatus?.(true, "");
        }
      } catch {
        if (!canceled) {
          const message = zh
            ? "TIDAS 地理位置目录加载失败，请检查后端 reference catalog。"
            : "Failed to load TIDAS location catalog. Check backend reference catalog.";
          setOptions([]);
          setLoadError(message);
          onCatalogStatus?.(false, message);
        }
      }
    };
    void loadLocations();
    return () => {
      canceled = true;
    };
  }, [zh]);

  const normalizedLocation = normalizeTidasLocationValue(value, options);
  const selectedParentLocation = getLocationParentCode(normalizedLocation, options);
  const parentLocationOptions = useMemo(() => getParentLocationOptions(options), [options]);
  const level2LocationOptions = useMemo(
    () => getLocationLevelOptions(selectedParentLocation, options),
    [options, selectedParentLocation],
  );
  const selectedLevel2Location = getSelectedLocationAtLevel(normalizedLocation, 2, options);
  const level3LocationOptions = useMemo(
    () => getLocationLevelOptions(selectedLevel2Location, options),
    [options, selectedLevel2Location],
  );
  const selectedLevel3Location = getSelectedLocationAtLevel(normalizedLocation, 3, options);

  return (
    <>
      <div className="pm-location-cascade">
        <select
          value={selectedParentLocation}
          disabled={options.length === 0}
          onChange={(event) => onChange(event.target.value)}
        >
          {allowBlank && (
            <option value="">
              {loadError
                ? (zh ? "地理目录加载失败" : "Location catalog failed")
                : (blankLabel ?? (zh ? "选择全球、国家或区域" : "Select global, country, or region"))}
            </option>
          )}
          {parentLocationOptions.map((item) => (
            <option key={item.code} value={item.code}>
              {formatLocationOptionLabel(item, uiLanguage)}
            </option>
          ))}
        </select>
        {level2LocationOptions.length > 0 && (
          <select
            value={selectedLevel2Location}
            onChange={(event) => onChange(event.target.value || selectedParentLocation)}
          >
            <option value="">{zh ? "全部 / 不细分" : "All / no subdivision"}</option>
            {level2LocationOptions.map((item) => (
              <option key={item.code} value={item.code}>
                {formatLocationOptionLabel(item, uiLanguage)}
              </option>
            ))}
          </select>
        )}
        {level3LocationOptions.length > 0 && (
          <select
            value={selectedLevel3Location}
            onChange={(event) => onChange(event.target.value || selectedLevel2Location)}
          >
            <option value="">{zh ? "全部 / 不细分到城市" : "All / no city subdivision"}</option>
            {level3LocationOptions.map((item) => (
              <option key={item.code} value={item.code}>
                {formatLocationOptionLabel(item, uiLanguage)}
              </option>
            ))}
          </select>
        )}
      </div>
      {loadError && <span className="pm-field-error">{loadError}</span>}
    </>
  );
}

const normalizeLocationAlias = (value: string): string => value.trim().toLowerCase().replace(/\s+/g, "");

const formatLocationOptionLabel = (item: TidasLocationOption, uiLanguage: "zh" | "en"): string => {
  const primaryName = uiLanguage === "zh" ? item.name_zh : item.name_en;
  const secondaryName = uiLanguage === "zh" ? item.name_en : item.name_zh;
  const names = [primaryName, secondaryName].filter((name) => String(name ?? "").trim()).join(" / ");
  return names ? `${item.code} - ${names}` : item.code;
};

const getParentLocationOptions = (options: TidasLocationOption[]): TidasLocationOption[] =>
  options.filter((item) => item.code !== "NULL" && !item.code.includes("-"));

const getLocationParentCode = (code: string, options: TidasLocationOption[]): string => {
  if (!code) return "";
  const codes = new Set(options.map((item) => item.code));
  if (codes.has(code) && !code.includes("-")) return code;
  const parent = code.split("-")[0];
  return codes.has(parent) ? parent : code;
};

const getLocationSegmentCount = (code: string): number => code.split("-").filter(Boolean).length;

const getLocationLevelOptions = (parentCode: string, options: TidasLocationOption[]): TidasLocationOption[] => {
  if (!parentCode) return [];
  const prefix = `${parentCode}-`;
  const expectedSegments = getLocationSegmentCount(parentCode) + 1;
  return options.filter((item) => item.code.startsWith(prefix) && getLocationSegmentCount(item.code) === expectedSegments);
};

const getSelectedLocationAtLevel = (code: string, level: number, options: TidasLocationOption[]): string => {
  if (!code) return "";
  const parts = code.split("-");
  if (parts.length < level) return "";
  const candidate = parts.slice(0, level).join("-");
  return options.some((item) => item.code === candidate) ? candidate : "";
};
