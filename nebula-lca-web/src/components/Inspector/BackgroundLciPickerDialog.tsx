import { useEffect, useMemo, useState } from "react";
import type { ProviderCandidate } from "../../services/intermediateFlowLinks";

type Props = {
  open: boolean;
  busy: boolean;
  providers: ProviderCandidate[];
  sourceFlowName?: string;
  targetFlowUuid?: string;
  language: "zh" | "en";
  onClose: () => void;
  onSelect: (provider: ProviderCandidate) => void;
};

type ProcessKind = "all" | "market_group" | "market" | "production" | "other";

const PAGE_SIZE = 20;

function providerKind(provider: ProviderCandidate): Exclude<ProcessKind, "all"> {
  const name = `${provider.process_name} ${provider.process_name_en ?? ""}`.trim().toLocaleLowerCase();
  if (name.startsWith("market group for ")) return "market_group";
  if (name.startsWith("market for ")) return "market";
  if (name.includes("production") || name.includes("operation")) return "production";
  return "other";
}

export function BackgroundLciPickerDialog({
  open,
  busy,
  providers,
  sourceFlowName = "",
  targetFlowUuid = "",
  language,
  onClose,
  onSelect,
}: Props) {
  const [query, setQuery] = useState("");
  const [location, setLocation] = useState("");
  const [kind, setKind] = useState<ProcessKind>("all");
  const [page, setPage] = useState(1);
  const t = (zh: string, en: string) => (language === "zh" ? zh : en);

  useEffect(() => {
    if (!open) return;
    setQuery("");
    setLocation("");
    setKind("all");
    setPage(1);
  }, [open]);

  const locations = useMemo(
    () => Array.from(new Set(providers.map((provider) => provider.location || "-").filter(Boolean)))
      .sort((left, right) => left.localeCompare(right)),
    [providers],
  );

  const filteredProviders = useMemo(() => {
    const keyword = query.trim().toLocaleLowerCase();
    return providers.filter((provider) => {
      if (!provider.has_lci_vector) return false;
      if (location && (provider.location || "-") !== location) return false;
      if (kind !== "all" && providerKind(provider) !== kind) return false;
      if (!keyword) return true;
      return [
        provider.process_name,
        provider.process_name_en,
        provider.reference_product_name,
        provider.reference_product_unit,
        provider.location,
      ].some((value) => String(value ?? "").toLocaleLowerCase().includes(keyword));
    });
  }, [kind, location, providers, query]);

  const totalPages = Math.max(1, Math.ceil(filteredProviders.length / PAGE_SIZE));
  const visibleProviders = filteredProviders.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  useEffect(() => {
    setPage(1);
  }, [kind, location, query]);

  useEffect(() => {
    if (page > totalPages) setPage(totalPages);
  }, [page, totalPages]);

  if (!open) return null;
  return (
    <div className="overlay-modal background-lci-picker-overlay" onMouseDown={() => !busy && onClose()}>
      <section
        className="overlay-panel background-lci-picker-dialog"
        role="dialog"
        aria-modal="true"
        aria-label={t("选择背景数据库", "Choose Background Database")}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="overlay-head background-lci-picker-head">
          <div className="intermediate-flow-link-title">
            <strong>{t("选择背景数据库", "Choose Background Database")}</strong>
            <span>{filteredProviders.length}</span>
          </div>
          <button type="button" className="drawer-close-btn" disabled={busy} onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </header>

        <div className="background-lci-picker-toolbar">
          <div className="background-lci-picker-context">
            <strong>{t("已转换流", "Converted flow")}</strong>
            <span title={sourceFlowName}>{sourceFlowName || targetFlowUuid || "-"}</span>
            {targetFlowUuid && <small title={targetFlowUuid}>{targetFlowUuid}</small>}
          </div>
          <label>
            <span>{t("过程名称", "Process")}</span>
            <input
              value={query}
              disabled={busy}
              placeholder={t("搜索过程、参考产品", "Search process or reference product")}
              onChange={(event) => setQuery(event.target.value)}
            />
          </label>
          <label>
            <span>{t("过程类型", "Process type")}</span>
            <select value={kind} disabled={busy} onChange={(event) => setKind(event.target.value as ProcessKind)}>
              <option value="all">{t("全部类型", "All types")}</option>
              <option value="market_group">{t("市场组", "Market group")}</option>
              <option value="market">{t("市场过程", "Market")}</option>
              <option value="production">{t("生产/运行过程", "Production / operation")}</option>
              <option value="other">{t("其他过程", "Other")}</option>
            </select>
          </label>
          <label>
            <span>{t("地理位置", "Location")}</span>
            <select value={location} disabled={busy} onChange={(event) => setLocation(event.target.value)}>
              <option value="">{t("全部地区", "All locations")}</option>
              {locations.map((item) => <option key={item} value={item}>{item}</option>)}
            </select>
          </label>
          <button
            type="button"
            className="flow-link-button secondary background-lci-picker-reset"
            disabled={busy || (!query && !location && kind === "all")}
            onClick={() => {
              setQuery("");
              setLocation("");
              setKind("all");
            }}
          >
            {t("重置", "Reset")}
          </button>
        </div>

        <div className="background-lci-picker-table-wrap">
          <table className="background-lci-picker-table">
            <thead>
              <tr>
                <th>{t("过程名称", "Process")}</th>
                <th>{t("来源", "Source")}</th>
                <th>{t("参考产品", "Reference product")}</th>
                <th>{t("单位", "Unit")}</th>
                <th>{t("过程类型", "Type")}</th>
                <th>{t("地区", "Location")}</th>
                <th>{t("清单", "Inventory")}</th>
                <th>{t("操作", "Action")}</th>
              </tr>
            </thead>
            <tbody>
              {visibleProviders.map((provider) => {
                const type = providerKind(provider);
                const kindLabel = type === "market_group"
                  ? t("市场组", "Market group")
                  : type === "market"
                    ? t("市场过程", "Market")
                    : type === "production"
                      ? t("生产/运行", "Production / operation")
                      : t("其他", "Other");
                return (
                  <tr key={provider.process_uuid}>
                    <td title={provider.process_name}>{provider.process_name}</td>
                    <td>{provider.source || "-"}</td>
                    <td title={provider.reference_product_name || "-"}>{provider.reference_product_name || "-"}</td>
                    <td>{provider.reference_product_unit || "-"}</td>
                    <td>{kindLabel}</td>
                    <td>{provider.location || "-"}</td>
                    <td><span className="background-lci-vector-badge">{provider.vector_nnz.toLocaleString()}</span></td>
                    <td>
                      <button type="button" className="flow-link-button ghost compact" disabled={busy} onClick={() => onSelect(provider)}>
                        {busy ? t("关联中…", "Linking…") : t("关联", "Link")}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {visibleProviders.length === 0 && (
            <div className="table-empty">{t("没有符合筛选条件的背景 LCI", "No background LCI matches the filters")}</div>
          )}
        </div>

        <footer className="background-lci-picker-footer">
          <span>{t(
            `共 ${filteredProviders.length} 个可计算 provider，第 ${page}/${totalPages} 页`,
            `${filteredProviders.length} calculable providers, page ${page}/${totalPages}`,
          )}</span>
          {totalPages > 1 && (
            <div>
              <button type="button" className="flow-link-button secondary" disabled={busy || page <= 1} onClick={() => setPage((current) => current - 1)}>
                {t("上一页", "Previous")}
              </button>
              <button type="button" className="flow-link-button secondary" disabled={busy || page >= totalPages} onClick={() => setPage((current) => current + 1)}>
                {t("下一页", "Next")}
              </button>
            </div>
          )}
        </footer>
      </section>
    </div>
  );
}
