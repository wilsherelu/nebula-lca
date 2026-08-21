export type FlowPickerCatalogFlow = {
  flow_uuid: string;
  flow_name: string;
  flow_name_en?: string | null;
  flow_type: string;
  default_unit: string;
  unit_group: string;
  compartment?: string | null;
  subcompartment?: string | null;
  source?: string | null;
  source_namespace?: string | null;
  source_version?: string | null;
  version_label?: string | null;
  is_custom?: boolean;
  conversion_compatible?: boolean | null;
  conversion_mode?: "bidirectional" | "canonical" | "one_way_canonicalization" | null;
  conversion_target_flow_uuid?: string | null;
  conversion_package_version?: string | null;
};

type Props = {
  language: "zh" | "en";
  elementary: boolean;
  flows: FlowPickerCatalogFlow[];
  loading: boolean;
  error: string;
  searchInput: string;
  category: string;
  categories: Array<{ category: string; count: number }>;
  source: string;
  compatibleOnly: boolean;
  page: number;
  totalPages: number;
  total: number;
  refreshingFlowUuid: string;
  onClose: () => void;
  onSearchInputChange: (value: string) => void;
  onSearch: () => void;
  onCategoryChange: (value: string) => void;
  onSourceChange: (value: string) => void;
  onCompatibleOnlyChange: (value: boolean) => void;
  onCreate: () => void;
  onUse: (flow: FlowPickerCatalogFlow) => void;
  onRefresh: (flowUuid: string) => void;
  onPageChange: (page: number) => void;
  displayFlowType: (value: string) => string;
};

const distinctName = (primary: string, secondary: string | null | undefined): boolean =>
  Boolean(secondary && secondary.trim().toLocaleLowerCase() !== primary.trim().toLocaleLowerCase());

export function FlowPickerDialog({
  language,
  elementary,
  flows,
  loading,
  error,
  searchInput,
  category,
  categories,
  source,
  compatibleOnly,
  page,
  totalPages,
  total,
  refreshingFlowUuid,
  onClose,
  onSearchInputChange,
  onSearch,
  onCategoryChange,
  onSourceChange,
  onCompatibleOnlyChange,
  onCreate,
  onUse,
  onRefresh,
  onPageChange,
  displayFlowType,
}: Props) {
  const zh = language === "zh";
  const t = (cn: string, en: string) => (zh ? cn : en);
  return (
    <div className="overlay-modal">
      <section className="overlay-panel flow-picker-panel" role="dialog" aria-modal="true" aria-label={elementary ? t("引用基本流", "Use Elementary Flow") : t("引用中间流", "Use Intermediate Flow")}>
        <header className="overlay-head flow-picker-head">
          <div className="flow-picker-heading">
            <strong>{elementary ? t("引用基本流", "Use Elementary Flow") : t("引用中间流", "Use Intermediate Flow")}</strong>
            <span>{t(`共 ${total.toLocaleString()} 条`, `${total.toLocaleString()} records`)}</span>
          </div>
          <button type="button" className="drawer-close-btn" onClick={onClose}>{t("关闭", "Close")}</button>
        </header>
        <div className="overlay-filters flow-picker-toolbar">
          <div className="flow-picker-toolbar-filters">
            <input
              value={searchInput}
              aria-label={t("按流名称检索", "Search by flow name")}
              onChange={(event) => onSearchInputChange(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  onSearch();
                }
              }}
              placeholder={t("输入中文名、英文名或 UUID", "Chinese name, English name, or UUID")}
            />
            <select value={category} aria-label={t("分类", "Category")} onChange={(event) => onCategoryChange(event.target.value)}>
              <option value="">{t("全部分类", "All Categories")}</option>
              {categories.map((item) => <option key={item.category} value={item.category}>{`${item.category} (${item.count})`}</option>)}
            </select>
            <select value={source} aria-label={t("来源", "Source")} onChange={(event) => onSourceChange(event.target.value)}>
              <option value="">{t("全部来源", "All Sources")}</option>
              <option value="ecoinvent">ecoinvent</option>
              <option value="tiangong">TIDAS / EF</option>
              <option value="custom">custom</option>
            </select>
          </div>
          <div className="flow-picker-toolbar-actions">
            {elementary && (
              <label className="flow-picker-compatible-toggle">
                <input type="checkbox" checked={compatibleOnly} onChange={(event) => onCompatibleOnlyChange(event.target.checked)} />
                <span>{t("仅可转换", "Convertible only")}</span>
              </label>
            )}
            <button type="button" className="search-btn" onClick={onSearch}>{t("检索", "Search")}</button>
            {!elementary && <button type="button" className="search-btn flow-picker-create-btn" onClick={onCreate}>{t("新建自定义流", "Create Custom Flow")}</button>}
          </div>
        </div>
        <div className="overlay-table flow-picker-results">
          {loading && <div className="table-empty">{t("正在加载…", "Loading…")}</div>}
          {!loading && error && <div className="table-empty error-text">{error}</div>}
          {!loading && !error && flows.length === 0 && <div className="table-empty">{t("没有符合条件的流", "No matching flows")}</div>}
          {!loading && !error && flows.length > 0 && (
            <table>
              <thead>
                <tr>
                  {!elementary && <th className="flow-picker-type-col">{t("类型", "Type")}</th>}
                  <th className="flow-picker-name-col">{t("名称", "Name")}</th>
                  {elementary && <th className="flow-picker-english-name-col">{t("英文名称", "English name")}</th>}
                  <th className="flow-picker-unit-col">{t("单位", "Unit")}</th>
                  <th className="flow-picker-category-col">{t("分类", "Compartment")}</th>
                  {elementary && <th className="flow-picker-subcategory-col">{t("子分类", "Subcompartment")}</th>}
                  <th className="flow-picker-source-col">{t("来源 / 版本", "Source / Version")}</th>
                  <th className="flow-picker-action-col">{t("操作", "Action")}</th>
                </tr>
              </thead>
              <tbody>
                {flows.map((flow) => {
                  const englishName = String(flow.flow_name_en ?? "").trim();
                  const hasSeparateEnglishName = distinctName(flow.flow_name, englishName);
                  const primaryName = zh && hasSeparateEnglishName ? flow.flow_name : (englishName || flow.flow_name);
                  return (
                    <tr key={flow.flow_uuid}>
                      {!elementary && <td className="flow-picker-type-cell">{displayFlowType(flow.flow_type)}</td>}
                      <td className="flow-picker-name-cell">
                        <span className="flow-picker-ellipsis">{primaryName}</span>
                        {flow.conversion_compatible && <span className={`flow-conversion-badge flow-conversion-badge-${flow.conversion_mode ?? "canonical"}`}>{flow.conversion_mode === "bidirectional" ? t("双向", "Bidirectional") : flow.conversion_mode === "one_way_canonicalization" ? t("单向", "One-way") : t("EF 规范流", "Canonical EF")}</span>}
                      </td>
                      {elementary && <td className="flow-picker-english-name-col"><span className="flow-picker-ellipsis">{hasSeparateEnglishName ? englishName : "—"}</span></td>}
                      <td>{flow.default_unit || "—"}</td>
                      <td><span className="flow-picker-ellipsis">{flow.compartment || "—"}</span></td>
                      {elementary && <td className="flow-picker-subcategory-col"><span className="flow-picker-ellipsis">{flow.subcompartment || "—"}</span></td>}
                      <td className="flow-picker-source-cell">
                        <span>{flow.source || "unknown"}</span>
                        {(flow.version_label || flow.source_version) && <small>{flow.version_label || flow.source_version}</small>}
                      </td>
                      <td className="flow-picker-action-cell">
                        <div className="flow-picker-action-group">
                          {flow.source === "tiangong" && <button type="button" className="pm-link-btn" onClick={() => onRefresh(flow.flow_uuid)} disabled={Boolean(refreshingFlowUuid)}>{refreshingFlowUuid === flow.flow_uuid ? t("刷新中", "Refreshing") : t("刷新", "Refresh")}</button>}
                          <button type="button" className="pm-link-btn" onClick={() => onUse(flow)}>{t("引用", "Use")}</button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
        {!loading && !error && (
          <footer className="overlay-pagination">
            <span>{t(`第 ${page} / ${totalPages} 页`, `Page ${page} / ${totalPages}`)}</span>
            <div className="overlay-pagination-actions">
              <button type="button" className="ghost-btn" disabled={page <= 1} onClick={() => onPageChange(1)}>{t("首页", "First")}</button>
              <button type="button" className="ghost-btn" disabled={page <= 1} onClick={() => onPageChange(Math.max(1, page - 1))}>{t("上一页", "Prev")}</button>
              <button type="button" className="ghost-btn" disabled={page >= totalPages} onClick={() => onPageChange(Math.min(totalPages, page + 1))}>{t("下一页", "Next")}</button>
            </div>
          </footer>
        )}
      </section>
    </div>
  );
}
