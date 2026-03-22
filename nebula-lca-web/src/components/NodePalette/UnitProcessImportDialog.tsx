import { useEffect, useMemo, useState } from "react";
import {
  useLcaGraphStore,
  type FilteredExchangeEvidence,
  type ImportedUnitProcessPayload,
  type NodeCreateKind,
  type ProcessImportMode,
} from "../../store/lcaGraphStore";
import type { ExchangeType, FlowPort } from "../../model/node";

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;
const CATALOG_CACHE_PREFIX = "nebula:import-catalog:";
const CATALOG_CACHE_TTL_MS = 30_000;

type CatalogProcessItem = {
  process_uuid?: string;
  process_name?: string;
  process_name_en?: string;
  location?: string;
  process_kind?: string;
  source_kind?: string;
  suggested_kind?: string;
  reference_flow_uuid?: string;
  reference_flow_name?: string;
  reference_flow_name_en?: string;
  exchange_count?: number;
  filtered_exchange_count?: number;
};

type ProcessExchange = {
  flow_uuid?: string;
  flow_name?: string;
  amount?: number;
  unit?: string;
  unit_group?: string;
  unitGroup?: string;
  flow_type?: string;
  type?: string;
  direction?: string;
  is_product?: boolean;
  isProduct?: boolean;
  is_reference_flow?: boolean;
};

type ProcessDetailResponse = {
  process_name?: string;
  process_name_en?: string;
  process_uuid?: string;
  process_json?: Record<string, unknown>;
} & Record<string, unknown>;

type FlowRefInfo = {
  uuid: string;
  flow_name?: string;
  flow_name_en?: string;
  flow_type?: string;
  default_unit?: string;
  unit?: string;
  __status?: "ok" | "missing_uuid";
};

type ApiError = {
  code?: string;
  message?: string;
};

type CacheEnvelope = {
  ts: number;
  data: {
    items: CatalogProcessItem[];
    total: number;
  };
};

const targetKindLabel = (kind: NodeCreateKind, zh: boolean): string => {
  if (kind === "market_process") return zh ? "市场过程" : "Market Process";
  if (kind === "lci_dataset") return zh ? "LCI 数据集" : "LCI Dataset";
  if (kind === "pts_module") return zh ? "PTS 模块" : "PTS Module";
  return zh ? "单元过程" : "Unit Process";
};

const displayProcessKind = (raw: string | undefined, fallback: NodeCreateKind, zh: boolean): string => {
  const value = String(raw ?? "").trim();
  if (value === "unit_process") return zh ? "单元过程" : "Unit Process";
  if (value === "market_process") return zh ? "市场过程" : "Market Process";
  if (value === "lci_dataset") return zh ? "LCI 数据集" : "LCI Dataset";
  if (value === "pts_module") return zh ? "PTS 模块" : "PTS Module";
  return targetKindLabel(fallback, zh);
};

const mapExchangeType = (raw?: string): ExchangeType => {
  const text = String(raw ?? "").toLowerCase();
  if (text.includes("biosphere") || text.includes("elementary") || text.includes("basic")) return "biosphere";
  if (text.includes("energy")) return "energy";
  return "technosphere";
};

const toPort = (ex: ProcessExchange, direction: "input" | "output", idx: number): FlowPort => ({
  id: `${direction}_${Math.random().toString(36).slice(2, 8)}_${idx}`,
  flowUuid: String(ex.flow_uuid ?? "").trim(),
  name: String(ex.flow_name ?? ex.flow_uuid ?? "Unnamed Flow"),
  unit: String(ex.unit ?? "kg"),
  unitGroup: String(ex.unit_group ?? ex.unitGroup ?? "").trim() || undefined,
  amount: Number.isFinite(Number(ex.amount)) ? Number(ex.amount) : 0,
  isProduct: Boolean(ex.is_product ?? ex.isProduct),
  type: mapExchangeType(ex.flow_type ?? ex.type),
  direction,
  showOnNode: true,
});

const parsePortRows = (raw: unknown, direction: "input" | "output"): FlowPort[] => {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((item, idx) => toPort((item as ProcessExchange) ?? {}, direction, idx))
    .filter((port) => Boolean(port.flowUuid));
};

const parseImportedRows = (
  payload: Record<string, unknown>,
  importMode: ProcessImportMode,
  uiLanguage: "zh" | "en",
): ImportedUnitProcessPayload[] => {
  const rawRows = Array.isArray(payload.imported_processes)
    ? payload.imported_processes
    : Array.isArray(payload.processes)
      ? payload.processes
      : [];

  return rawRows
    .map((item) => (typeof item === "object" && item ? (item as Record<string, unknown>) : null))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item, idx) => {
      const inputs = parsePortRows(item.inputs, "input");
      const outputs = parsePortRows(item.outputs, "output");
      const exchanges = Array.isArray(item.exchanges) ? (item.exchanges as ProcessExchange[]) : [];
      const fallbackInputs = exchanges
        .filter((ex) => String(ex.direction ?? "").toLowerCase().includes("in"))
        .map((ex, i) => toPort(ex, "input", i));
      const fallbackOutputs = exchanges
        .filter((ex) => String(ex.direction ?? "").toLowerCase().includes("out"))
        .map((ex, i) => toPort(ex, "output", i));

      const referenceFlow = String(item.reference_flow_uuid ?? item.reference_flow_internal_id ?? "").trim();
      const nextOutputs = (outputs.length > 0 ? outputs : fallbackOutputs).map((port) => ({
        ...port,
        isProduct: referenceFlow ? port.flowUuid === referenceFlow : Boolean(port.isProduct),
      }));
      const hasProduct = nextOutputs.some((port) => port.isProduct);
      const sourceProcessUuid = String(item.source_process_uuid ?? item.process_uuid ?? "").trim();
      const processUuidRaw = String(item.process_uuid ?? "").trim();
      const processUuid =
        processUuidRaw ||
        (importMode === "editable_clone"
          ? `proc_clone_${sourceProcessUuid || idx}_${Math.random().toString(36).slice(2, 6)}`
          : sourceProcessUuid || `proc_import_${idx}_${Math.random().toString(36).slice(2, 6)}`);
      const processNameZh = String(item.process_name ?? item.name ?? "").trim();
      const processNameEn = String(item.process_name_en ?? "").trim();
      const referenceProductZh = String(item.reference_flow_name ?? item.reference_product ?? "").trim();
      const referenceProductEn = String(item.reference_flow_name_en ?? "").trim();

      const warningList: string[] = [];
      if (!hasProduct) {
        warningList.push("未根据参考流命中产品，请在节点中手动定义产品。");
      }

      return {
        processUuid,
        sourceProcessUuid: sourceProcessUuid || undefined,
        importMode,
        name:
          (uiLanguage === "en" ? processNameEn || processNameZh : processNameZh || processNameEn) ||
          processUuid ||
          (uiLanguage === "zh" ? "导入单元过程" : "Imported Unit Process"),
        location: String(item.location ?? ""),
        referenceProduct:
          (uiLanguage === "en" ? referenceProductEn || referenceProductZh : referenceProductZh || referenceProductEn) || "",
        referenceProductFlowUuid: referenceFlow || undefined,
        referenceProductDirection: "output" as const,
        inputs: inputs.length > 0 ? inputs : fallbackInputs,
        outputs: nextOutputs,
        warnings: warningList,
      };
    });
};

const cacheKeyForCatalog = (params: {
  targetKind: NodeCreateKind;
  page: number;
  pageSize: number;
  query: string;
  uiLanguage: "zh" | "en";
}): string => `${CATALOG_CACHE_PREFIX}${params.targetKind}:${params.page}:${params.pageSize}:${params.query}:lang=${params.uiLanguage}`;

const readCatalogCache = (key: string): CacheEnvelope | null => {
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as CacheEnvelope;
    if (!parsed || typeof parsed.ts !== "number" || !parsed.data) return null;
    return parsed;
  } catch {
    return null;
  }
};

const writeCatalogCache = (key: string, data: { items: CatalogProcessItem[]; total: number }) => {
  try {
    const envelope: CacheEnvelope = { ts: Date.now(), data };
    window.sessionStorage.setItem(key, JSON.stringify(envelope));
  } catch {
    // ignore cache write failures
  }
};

function ProcessInventoryModal(props: {
  open: boolean;
  uiLanguage: "zh" | "en";
  busy: boolean;
  process: CatalogProcessItem | null;
  detail: ProcessDetailResponse | null;
  flowRefs: Map<string, FlowRefInfo>;
  errorText: string;
  onClose: () => void;
}) {
  const { open, uiLanguage, busy, process, detail, flowRefs, errorText, onClose } = props;
  const zh = uiLanguage === "zh";
  const [activeDirection, setActiveDirection] = useState<"input" | "output">("input");

  useEffect(() => {
    if (open) setActiveDirection("input");
  }, [open, process?.process_uuid]);

  if (!open || !process) return null;

  const source = (detail?.process_json && typeof detail.process_json === "object" ? detail.process_json : detail) as
    | Record<string, unknown>
    | null;
  const rawExchanges = Array.isArray(source?.exchanges) ? (source?.exchanges as Array<Record<string, unknown>>) : [];

  const normalizedExchanges = rawExchanges.map((ex) => {
    const flowUuid = String(ex.flow_uuid ?? ex.flowUuid ?? "");
    const directionRaw = String(ex.direction ?? ex.input_output ?? "").toLowerCase();
    const direction: "input" | "output" = directionRaw.includes("input") ? "input" : "output";
    const ref = flowUuid ? flowRefs.get(flowUuid) : undefined;
    const flowType = String(ref?.flow_type ?? ex.flow_type ?? "").toLowerCase();
    const isElementary = flowType.includes("elementary") || flowType.includes("basic") || flowType.includes("biosphere");
    const hasResolvedName = Boolean(ref?.flow_name || ref?.flow_name_en);
    const refStatus: "ok" | "missing_uuid" | "missing_name" =
      ref?.__status === "missing_uuid" ? "missing_uuid" : hasResolvedName ? "ok" : "missing_name";

    return {
      name: String(ex.name ?? ex.flow_name ?? ex.flowName ?? ref?.flow_name ?? ref?.flow_name_en ?? flowUuid ?? "-"),
      flowUuid,
      amount: String(ex.amount ?? "-"),
      unit: String(ex.unit ?? ref?.default_unit ?? ref?.unit ?? "-"),
      isReference: Boolean(ex.is_reference_flow),
      refStatus,
      direction,
      isElementary,
    };
  });

  const inputIntermediate = normalizedExchanges.filter((e) => e.direction === "input" && !e.isElementary);
  const inputElementary = normalizedExchanges.filter((e) => e.direction === "input" && e.isElementary);
  const outputIntermediate = normalizedExchanges.filter((e) => e.direction === "output" && !e.isElementary);
  const outputElementary = normalizedExchanges.filter((e) => e.direction === "output" && e.isElementary);
  const currentIntermediateRows = activeDirection === "input" ? inputIntermediate : outputIntermediate;
  const currentElementaryRows = activeDirection === "input" ? inputElementary : outputElementary;

  const renderTable = (
    rows: Array<{
      name: string;
      flowUuid: string;
      amount: string;
      unit: string;
      isReference: boolean;
      refStatus: "ok" | "missing_uuid" | "missing_name";
    }>,
    title: string,
  ) => (
    <div className="pm-process-section">
      <h4>{title}</h4>
      <div className="pm-table-wrap">
        <table className="pm-table pm-process-detail-table">
          <thead>
            <tr>
              <th>{zh ? "名称" : "Name"}</th>
              <th>UUID</th>
              <th>{zh ? "数值" : "Amount"}</th>
              <th>{zh ? "单位" : "Unit"}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => {
              const missingFlow = row.refStatus === "missing_uuid" || row.refStatus === "missing_name";
              const missingText = zh
                ? "中间流不存在，导入该过程会无视这条流记录"
                : "Flow missing; import will ignore this flow record.";
              const displayName = row.isReference ? `${row.name}@${zh ? "参考流" : "Reference Flow"}` : row.name;
              return (
                <tr key={`${title}-${row.flowUuid}-${idx}`}>
                  <td className={missingFlow ? "pm-inline-hint warn" : row.isReference ? "pm-ref-quantity-name" : ""}>
                    {missingFlow ? missingText : displayName}
                  </td>
                  <td>{row.flowUuid || "-"}</td>
                  <td>{row.amount}</td>
                  <td>{row.unit}</td>
                </tr>
              );
            })}
            {rows.length === 0 && (
              <tr>
                <td colSpan={4}>{zh ? "无数据" : "No data"}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  return (
    <div className="pm-modal-mask" onClick={onClose}>
      <div className="pm-modal pm-process-detail-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{zh ? "过程详情" : "Process Details"}</strong>
          <button type="button" className="pm-link-btn" onClick={onClose}>
            {zh ? "关闭" : "Close"}
          </button>
        </div>
        <div className="pm-process-summary">
          <div><b>{zh ? "名称" : "Name"}:</b> {process.process_name || "-"}</div>
          <div><b>{zh ? "参考流" : "Reference Flow"}:</b> {process.reference_flow_name || process.reference_flow_uuid || "-"}</div>
        </div>
        {busy && <div className="pm-empty-note">{zh ? "详情加载中..." : "Loading details..."}</div>}
        {!busy && errorText && <div className="pm-error">{errorText}</div>}
        {!busy && !errorText && (
          <div className="pm-process-detail-grid">
            <div className="pm-detail-tab-row">
              <button type="button" className={activeDirection === "input" ? "active" : ""} onClick={() => setActiveDirection("input")}>
                {zh ? "输入" : "Inputs"}
              </button>
              <button type="button" className={activeDirection === "output" ? "active" : ""} onClick={() => setActiveDirection("output")}>
                {zh ? "输出" : "Outputs"}
              </button>
            </div>
            {renderTable(currentIntermediateRows, zh ? "中间流" : "Intermediate Flows")}
            {renderTable(currentElementaryRows, zh ? "基本流" : "Elementary Flows")}
          </div>
        )}
      </div>
    </div>
  );
}

export function UnitProcessImportDialog() {
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const dialog = useLcaGraphStore((state) => state.unitProcessImportDialog);
  const closeDialog = useLcaGraphStore((state) => state.closeUnitProcessImportDialog);
  const addImportedUnitProcesses = useLcaGraphStore((state) => state.addImportedUnitProcesses);
  const setConnectionHint = useLcaGraphStore((state) => state.setConnectionHint);

  const zh = uiLanguage === "zh";
  const targetKind = dialog.targetKind ?? "unit_process";

  const [queryInput, setQueryInput] = useState("");
  const [query, setQuery] = useState("");
  const [searchNonce, setSearchNonce] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(12);
  const [total, setTotal] = useState(0);
  const [catalog, setCatalog] = useState<CatalogProcessItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [importMode, setImportMode] = useState<ProcessImportMode>("locked");
  const [importing, setImporting] = useState(false);

  const [detailOpen, setDetailOpen] = useState(false);
  const [detailBusy, setDetailBusy] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [detailProcess, setDetailProcess] = useState<CatalogProcessItem | null>(null);
  const [detailPayload, setDetailPayload] = useState<ProcessDetailResponse | null>(null);
  const [detailFlowRefs, setDetailFlowRefs] = useState<Map<string, FlowRefInfo>>(new Map());

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const cacheKey = useMemo(
    () => cacheKeyForCatalog({ targetKind, page, pageSize, query, uiLanguage }),
    [targetKind, page, pageSize, query, uiLanguage],
  );

  const applySearch = () => {
    setQuery(queryInput.trim());
    setPage(1);
    setSearchNonce((prev) => prev + 1);
  };

  useEffect(() => {
    if (!dialog.open) return;
    let canceled = false;

    const cached = readCatalogCache(cacheKey);
    if (cached?.data?.items) {
      setCatalog(cached.data.items);
      setTotal(cached.data.total);
    }

    const run = async () => {
      const shouldBackgroundRefresh = Boolean(cached && Date.now() - cached.ts <= CATALOG_CACHE_TTL_MS);
      if (!shouldBackgroundRefresh) {
        setLoading(true);
      }
      setErrorText("");
      try {
        const url = new URL(`${API_BASE}/reference/processes/catalog`, window.location.origin);
        url.searchParams.set("page", String(page));
        url.searchParams.set("page_size", String(pageSize));
        url.searchParams.set("target_kind", targetKind);
        if (query) {
          url.searchParams.set("search", query);
          url.searchParams.set("q", query);
          url.searchParams.set("keyword", query);
        }
        const resp = await fetch(url.toString(), { method: "GET" });
        if (!resp.ok) throw new Error(`Catalog request failed (${resp.status})`);
        const payload = (await resp.json()) as Record<string, unknown>;
        const items = Array.isArray(payload.items) ? payload.items : Array.isArray(payload) ? payload : [];
        if (canceled) return;
        const list = items
          .map((item) => (typeof item === "object" && item ? (item as CatalogProcessItem) : null))
          .filter((item): item is CatalogProcessItem => Boolean(item));
        const nextTotal = Number.isFinite(Number(payload.total)) ? Number(payload.total) : items.length;
        setCatalog(list);
        setTotal(nextTotal);
        writeCatalogCache(cacheKey, { items: list, total: nextTotal });
      } catch (error) {
        if (!canceled) {
          setErrorText(error instanceof Error ? error.message : zh ? "加载失败" : "Load failed");
          if (!cached) {
            setCatalog([]);
            setTotal(0);
          }
        }
      } finally {
        if (!canceled) {
          setLoading(false);
        }
      }
    };

    void run();
    return () => {
      canceled = true;
    };
  }, [cacheKey, dialog.open, page, pageSize, query, searchNonce, targetKind, zh]);

  useEffect(() => {
    if (!dialog.open) {
      setImportMode("locked");
      setPage(1);
      setQueryInput("");
      setQuery("");
      setErrorText("");
      setDetailOpen(false);
      setDetailBusy(false);
      setDetailError("");
      setDetailProcess(null);
      setDetailPayload(null);
      setDetailFlowRefs(new Map());
    }
  }, [dialog.open]);

  const openProcessDetail = async (row: CatalogProcessItem) => {
    const processUuid = String(row.process_uuid ?? "").trim();
    if (!processUuid) {
      setDetailError(zh ? "当前过程缺少 process_uuid，无法查看详情。" : "Missing process_uuid for detail.");
      setDetailProcess(row);
      setDetailPayload(null);
      setDetailFlowRefs(new Map());
      setDetailOpen(true);
      return;
    }
    setDetailProcess(row);
    setDetailOpen(true);
    setDetailBusy(true);
    setDetailError("");
    setDetailPayload(null);
    setDetailFlowRefs(new Map());
    try {
      const resp = await fetch(`${API_BASE}/processes/${encodeURIComponent(processUuid)}`);
      if (!resp.ok) throw new Error(`detail api ${resp.status}`);
      const detail = (await resp.json()) as ProcessDetailResponse;
      setDetailPayload(detail);

      const src = (detail.process_json && typeof detail.process_json === "object" ? detail.process_json : detail) as Record<string, unknown>;
      const ports = Array.isArray(src.exchanges)
        ? (src.exchanges as Array<Record<string, unknown>>)
        : [
            ...(Array.isArray(src.inputs) ? (src.inputs as Array<Record<string, unknown>>) : []),
            ...(Array.isArray(src.outputs) ? (src.outputs as Array<Record<string, unknown>>) : []),
          ];
      const uuids = Array.from(new Set(ports.map((p) => String(p.flow_uuid ?? p.flowUuid ?? "")).filter((v) => v.length > 0)));
      if (uuids.length > 0) {
        const entries: Array<[string, FlowRefInfo]> = await Promise.all(
          uuids.map(async (uuid): Promise<[string, FlowRefInfo]> => {
            try {
              const flowResp = await fetch(`${API_BASE}/reference/flows/${encodeURIComponent(uuid)}`);
              if (flowResp.ok) {
                const payload = (await flowResp.json()) as FlowRefInfo;
                return [uuid, { ...payload, uuid, __status: "ok" }];
              }
              return [uuid, { uuid, __status: "missing_uuid" }];
            } catch {
              return [uuid, { uuid, __status: "missing_uuid" }];
            }
          }),
        );
        setDetailFlowRefs(new Map(entries));
      }
    } catch {
      setDetailError(zh ? "过程详情接口不可用，当前无法展示完整清单。" : "Process detail API unavailable.");
    } finally {
      setDetailBusy(false);
    }
  };

  const onImport = async (row: CatalogProcessItem) => {
    setImporting(true);
    setErrorText("");
    try {
      const body = {
        import_mode: importMode,
        target_kind: targetKind,
        process_uuids: row.process_uuid ? [row.process_uuid] : [],
      };
      const resp = await fetch(`${API_BASE}/reference/processes/import`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as ApiError;
        if (err.code === "TARGET_KIND_NOT_IMPLEMENTED") {
          setErrorText(zh ? `后端暂未实现 ${targetKindLabel(targetKind, true)} 导入。` : `${targetKindLabel(targetKind, false)} import not implemented yet.`);
          return;
        }
        throw new Error(err.message || `Import failed (${resp.status})`);
      }

      const payload = (await resp.json()) as Record<string, unknown>;
      const filteredExchanges = Array.isArray(payload.filtered_exchanges)
        ? (payload.filtered_exchanges as FilteredExchangeEvidence[])
        : [];
      if (targetKind === "unit_process") {
        const parsedRows = parseImportedRows(payload, importMode, uiLanguage);
        const groupedFiltered = new Map<string, FilteredExchangeEvidence[]>();
        for (const evidence of filteredExchanges) {
          const key = String(evidence.process_uuid ?? "");
          const list = groupedFiltered.get(key) ?? [];
          list.push(evidence);
          groupedFiltered.set(key, list);
        }
        const rowsWithEvidence = parsedRows.map((parsed) => ({
          ...parsed,
          filteredExchanges: groupedFiltered.get(parsed.processUuid) ?? groupedFiltered.get(parsed.sourceProcessUuid ?? "") ?? [],
        }));
        addImportedUnitProcesses(rowsWithEvidence, dialog.position);
      } else {
        setErrorText(zh ? "当前前端仅支持单元过程直接落图，其他类型暂未接入画布落图。" : "Only unit process canvas import is supported currently.");
        return;
      }

      const topMissingFlowUuids = Array.isArray(payload.top_missing_flow_uuids)
        ? payload.top_missing_flow_uuids.map((item) => String(item))
        : [];
      const filteredCount = Number(payload.filtered_exchange_count ?? filteredExchanges.length);
      if (filteredCount > 0) {
        const flowHint = topMissingFlowUuids.length > 0 ? ` Top missing flow UUID: ${topMissingFlowUuids.join(", ")}` : "";
        setConnectionHint(
          zh
            ? `导入完成：有 ${filteredCount} 条 exchange 因无匹配 flow 被过滤，未参与建模计算。${flowHint}`
            : `Import completed: ${filteredCount} exchanges were filtered due to unmatched flows.${flowHint}`,
        );
      }
      closeDialog();
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : zh ? "导入失败" : "Import failed");
    } finally {
      setImporting(false);
    }
  };

  if (!dialog.open) return null;

  return (
    <div className="overlay-modal">
      <div className="overlay-panel unit-import-panel" onClick={(event) => event.stopPropagation()}>
        <div className="overlay-head">
          <strong>{zh ? `导入已有${targetKindLabel(targetKind, true)}` : `Import Existing ${targetKindLabel(targetKind, false)}`}</strong>
          <button type="button" className="drawer-close-btn" onClick={closeDialog}>
            {zh ? "关闭" : "Close"}
          </button>
        </div>
        <div className="overlay-filters">
          <div className="search-row">
            <input
              placeholder={zh ? "按过程名称/UUID检索（回车确认）" : "Search by process name/UUID (press Enter)"}
              value={queryInput}
              onChange={(event) => setQueryInput(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  applySearch();
                }
              }}
            />
            <button type="button" className="search-btn" onClick={applySearch}>
              {zh ? "检索" : "Search"}
            </button>
          </div>
          <div className="import-mode-row">
            <span className="unit-import-search-hint-inline">
              {zh ? "从已有过程库中选择并导入到当前画布" : "Select from process library and import into current canvas"}
            </span>
            <div className="import-mode-group">
              <label>
                <input type="radio" name="import_mode" checked={importMode === "locked"} onChange={() => setImportMode("locked")} />
                {zh ? "锁定编辑" : "Locked"}
              </label>
              <label>
                <input
                  type="radio"
                  name="import_mode"
                  checked={importMode === "editable_clone"}
                  onChange={() => setImportMode("editable_clone")}
                />
                {zh ? "可编辑克隆" : "Editable Clone"}
              </label>
            </div>
          </div>
        </div>
        <div className="overlay-table">
          {loading && <div className="table-empty">{zh ? "加载中..." : "Loading..."}</div>}
          {!loading && errorText && <div className="table-empty">{errorText}</div>}
          {!loading && !errorText && (
            <table className="unit-import-table">
              <colgroup>
                <col className="col-process-name" />
                <col className="col-process-kind" />
                <col className="col-process-ref" />
                <col className="col-process-actions" />
              </colgroup>
              <thead>
                <tr>
                  <th>{zh ? "过程名称" : "Process"}</th>
                  <th>{zh ? "类型" : "Kind"}</th>
                  <th>{zh ? "参考流" : "Reference Flow"}</th>
                  <th>{zh ? "操作" : "Actions"}</th>
                </tr>
              </thead>
              <tbody>
                {catalog.map((item, index) => {
                  const id = item.process_uuid || `row_${index}`;
                  const processName =
                    (zh
                      ? item.process_name || item.process_name_en
                      : item.process_name_en || item.process_name) || "-";
                  const processKind = displayProcessKind(item.process_kind || item.suggested_kind || item.source_kind, targetKind, zh);
                  const referenceQuantity =
                    (zh
                      ? item.reference_flow_name || item.reference_flow_name_en
                      : item.reference_flow_name_en || item.reference_flow_name) ||
                    item.reference_flow_uuid ||
                    "-";
                  return (
                    <tr key={id}>
                      <td className="unit-import-cell-ellipsis" title={processName}>{processName}</td>
                      <td className="unit-import-cell-ellipsis" title={processKind}>{processKind}</td>
                      <td className="unit-import-cell-ellipsis" title={referenceQuantity}>{referenceQuantity}</td>
                      <td className="unit-import-actions-cell">
                        <div className="pm-row-actions">
                          <button type="button" className="link-btn" onClick={() => void openProcessDetail(item)}>
                            {zh ? "查看" : "View"}
                          </button>
                          <button type="button" className="link-btn primary" disabled={importing} onClick={() => void onImport(item)}>
                            {importing ? (zh ? "导入中..." : "Importing...") : (zh ? "导入" : "Import")}
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
                {catalog.length === 0 && (
                  <tr>
                    <td colSpan={4}>{zh ? "暂无数据" : "No data"}</td>
                  </tr>
                )}
              </tbody>
            </table>
          )}
        </div>
        <div className="overlay-pagination">
          <span>{zh ? `第 ${page} / ${totalPages} 页，共 ${total} 条` : `Page ${page} / ${totalPages}, ${total} items`}</span>
          <div className="overlay-pagination-actions">
            <button type="button" className="ghost-btn" disabled={page <= 1} onClick={() => setPage(1)}>
              {zh ? "首页" : "First"}
            </button>
            <button type="button" className="ghost-btn" disabled={page <= 1} onClick={() => setPage((prev) => prev - 1)}>
              {zh ? "上一页" : "Prev"}
            </button>
            <button type="button" className="ghost-btn" disabled={page >= totalPages} onClick={() => setPage((prev) => prev + 1)}>
              {zh ? "下一页" : "Next"}
            </button>
          </div>
        </div>
      </div>
      <ProcessInventoryModal
        open={detailOpen}
        uiLanguage={uiLanguage}
        busy={detailBusy}
        process={detailProcess}
        detail={detailPayload}
        flowRefs={detailFlowRefs}
        errorText={detailError}
        onClose={() => setDetailOpen(false)}
      />
    </div>
  );
}

