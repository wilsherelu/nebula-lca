import { useEffect, useRef, useState } from "react";
import { CreateFlowDialog } from "../CreateFlowDialog";
import Ef31ImportJobPanel from "../Ef31ImportJobPanel";
import { FlowAllocationPropertiesModal, type FlowAllocationProperty } from "../FlowAllocationPropertiesModal";
import { TidasLocationCascade, normalizeTidasLocationValue } from "../TidasLocationCascade";

export type ProjectListItem = {
  project_id: string;
  name: string;
  created_at: string;
  latest_version: number | null;
  latest_version_created_at: string | null;
  source_policy?: SourcePolicy | null;
  reference_product?: string | null;
  functional_unit?: string | null;
  system_boundary?: string | null;
  time_representativeness?: string | null;
  geography?: string | null;
  description?: string | null;
  process_count?: number | null;
  flow_count?: number | null;
  status?: string | null;
};
export type SourcePolicy = "open_mixed" | "tidas_compliant" | "ecoinvent_strict" | "explicit_mapped_mixed";
type ProjectApiItem = {
  project_id: string;
  name: string;
  reference_product?: string | null;
  functional_unit?: string | null;
  system_boundary?: string | null;
  time_representativeness?: string | null;
  geography?: string | null;
  description?: string | null;
  process_count?: number | null;
  flow_count?: number | null;
  latest_version?: number | null;
  latest_version_created_at?: string | null;
  created_at?: string | null;
  source_policy?: SourcePolicy | null;
  status?: string | null;
};
type PagedResponse<T> = {
  items: T[];
  total: number;
  page: number;
  page_size: number;
};
type StatsResponse = {
  projects: number;
  processes: number;
  flows: number;
  flows_latest_graph?: number;
  flows_library_total?: number;
};

export type CreateProjectForm = {
  projectName: string;
  sourcePolicy: SourcePolicy;
  referenceProduct: string;
  functionalUnit: string;
  systemBoundary: string;
  timeRepresentativeness: string;
  geography: string;
  description: string;
};

type ProjectRow = {
  projectId: string;
  projectName: string;
  sourcePolicy: SourcePolicy;
  referenceProduct: string;
  functionalUnit: string;
  systemBoundary: string;
  timeRepresentativeness: string;
  geography: string;
  description: string;
  processCount: number;
  flowCount: number;
  lastModified: string;
  status: string;
  latestVersion: number | null;
};
type ProcessRow = {
  processId: string;
  processUuid: string;
  processName: string;
  processNameEn?: string;
  type: "unit_process" | "market_process";
  referenceFlowUuid: string;
  referenceFlowName: string;
  referenceFlowInternalId?: string;
  inputs: number;
  outputs: number;
  usedInProjects: number;
  balanceStatus: "balanced" | "unchecked" | "error";
  lastModified: string;
};
type ProcessApiItem = {
  process_uuid?: string;
  process_name?: string;
  process_name_en?: string | null;
  type?: "unit_process" | "market_process";
  reference_flow_uuid?: string | null;
  reference_flow_name?: string | null;
  reference_flow_internal_id?: string | null;
  input_count?: number;
  output_count?: number;
  used_in_projects?: number;
  balance_status?: "balanced" | "unchecked" | "error";
  last_modified?: string;
};
type ProcessDetailResponse = {
  process_name?: string;
  process_name_en?: string | null;
  process_uuid?: string;
  type?: "unit_process" | "market_process";
  reference_flow_uuid?: string | null;
  reference_flow_name?: string | null;
  reference_flow_name_en?: string | null;
  reference_flow_internal_id?: string | null;
  process_json?: Record<string, unknown>;
} & Record<string, unknown>;
type FlowRow = {
  id: string;
  flowName: string;
  flowNameEn?: string;
  type: "intermediate_flow" | "elementary_flow" | "product_flow" | "waste_flow";
  unit: string;
  category: string;
  source?: string;
  isCustom?: boolean;
  tidasCompatible?: boolean;
  tidasUnitGroup?: string | null;
  tidasFlowPropertyUuid?: string | null;
  tidasReferenceSource?: string | null;
  allocationProperties?: FlowAllocationProperty[];
  usedInProcesses: number;
  lastModified: string;
};
type FlowRefInfo = {
  uuid: string;
  flow_name?: string;
  flow_name_en?: string;
  flow_type?: string;
  default_unit?: string;
  unit?: string;
  unit_group?: string;
  __status?: "ok" | "missing_uuid";
};

const getDisplayFlowName = (
  row: Pick<FlowRow, "flowName" | "flowNameEn">,
  uiLanguage: "zh" | "en",
): string => {
  if (uiLanguage === "en") {
    return String(row.flowNameEn ?? "").trim() || row.flowName;
  }
  return row.flowName;
};

const getDisplayProcessName = (
  row: Pick<ProcessRow, "processName" | "processNameEn">,
  uiLanguage: "zh" | "en",
): string => {
  if (uiLanguage === "en") {
    return String(row.processNameEn ?? "").trim() || row.processName;
  }
  return row.processName;
};

type NavModule = "project" | "process" | "flow";
type NavItem = "recent_projects" | "all_projects" | "all_processes" | "all_flows";
export type TidasRepairTarget = Record<string, unknown> & {
  desired_inspector_tab?: "external_in" | "external_out";
  desired_process_modal?: "metadata";
};

type Props = {
  projects: ProjectListItem[];
  busy?: boolean;
  uiLanguage?: "zh" | "en";
  onChangeLanguage?: (lang: "zh" | "en") => void;
  onStatus?: (text: string) => void;
  onOpenProject: (projectId: string, projectName?: string, repairTarget?: TidasRepairTarget) => void;
  onCreateProject: (form: CreateProjectForm) => Promise<void>;
  onDeleteProject: (projectId: string) => void;
  onCreateProcess?: () => void;
};

const defaultForm: CreateProjectForm = {
  projectName: "",
  sourcePolicy: "open_mixed",
  referenceProduct: "",
  functionalUnit: "",
  systemBoundary: "",
  timeRepresentativeness: "",
  geography: "",
  description: "",
};
const sourcePolicyOptions: Array<{ value: SourcePolicy; zh: string; en: string }> = [
  { value: "open_mixed", zh: "混合模式", en: "Open Mixed" },
  { value: "tidas_compliant", zh: "天工/TIDAS 合规模式", en: "TIDAS Compliant" },
  { value: "ecoinvent_strict", zh: "ecoinvent 严格模式", en: "ecoinvent Strict" },
];
const normalizeSourcePolicy = (value: unknown): SourcePolicy => {
  if (value === "tidas_compliant" || value === "ecoinvent_strict" || value === "explicit_mapped_mixed") {
    return value;
  }
  return "open_mixed";
};
const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;
const PM_CACHE_TTL_MS = 30_000;

const readPmCacheEntry = <T,>(key: string): { ts: number; data: T; etag?: string } | null => {
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { ts: number; data: T; etag?: string };
    if (!parsed || typeof parsed.ts !== "number") return null;
    if (Date.now() - parsed.ts > PM_CACHE_TTL_MS) return null;
    return parsed;
  } catch {
    return null;
  }
};

const readPmCache = <T,>(key: string): T | null => readPmCacheEntry<T>(key)?.data ?? null;

const readPmCacheStale = <T,>(key: string): T | null => {
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { data?: T };
    return (parsed?.data as T) ?? null;
  } catch {
    return null;
  }
};

const writePmCache = (key: string, data: unknown, etag?: string): void => {
  try {
    window.sessionStorage.setItem(key, JSON.stringify({ ts: Date.now(), data, etag }));
  } catch {
    // ignore cache write errors
  }
};

const clearPmCacheByPrefix = (prefix: string): void => {
  try {
    const keysToRemove: string[] = [];
    for (let i = 0; i < window.sessionStorage.length; i += 1) {
      const key = window.sessionStorage.key(i);
      if (key && key.startsWith(prefix)) {
        keysToRemove.push(key);
      }
    }
    keysToRemove.forEach((key) => window.sessionStorage.removeItem(key));
  } catch {
    // ignore cache clear errors
  }
};

const formatTime = (value?: string | null): string => {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
};
const useDebouncedValue = (value: string, delayMs = 300): string => {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(timer);
  }, [value, delayMs]);
  return debounced;
};

const mapProjectStatus = (raw: string | null | undefined, zh: boolean): string => {
  const value = String(raw ?? "").trim().toLowerCase();
  if (zh) {
    if (value === "active") return "进行中";
    if (value === "draft") return "草稿";
    if (value === "archived") return "归档";
  } else {
    if (value === "active") return "Active";
    if (value === "draft") return "Draft";
    if (value === "archived") return "Archived";
  }
  return raw && String(raw).trim() ? String(raw) : zh ? "进行中" : "Active";
};
const mapSystemBoundary = (raw: string | null | undefined, zh: boolean): string => {
  const text = String(raw ?? "").trim();
  if (!text) {
    return zh ? "Cradle-to-gate（从摇篮到工厂）" : "Cradle-to-gate";
  }
  if (text === "从摇篮到工厂") {
    return zh ? "Cradle-to-gate（从摇篮到工厂）" : "Cradle-to-gate";
  }
  return text;
};
type FlowBusinessType = "basic" | "product" | "waste";
type FlowCategoryItem = {
  category: string;
  count: number;
};

// TIDAS Export types
type TidasExportWarning = {
  message?: string;
  code?: string;
  severity?: "warning" | "error";
  category?: string;
  details?: Record<string, unknown>;
  context?: Record<string, unknown>;
};
type TidasExportPreviewResponse = {
  can_export: boolean;
  flow_count: number;
  process_count: number;
  exported_model_count: number;
  multi_product_process_count: number;
  allocation_warnings: Array<string | TidasExportWarning>;
  manual_allocation_required_processes: string[];
  reference_flow_by_process: Record<string, string>;
  warnings: Array<string | TidasExportWarning>;
  errors: string[];
  blocking?: TidasExportWarning[];
  info?: TidasExportWarning[];
  missing_flows: string[];
  missing_processes: string[];
};

type TidasUnitDefinition = {
  unit_group: string;
  unit_name: string;
  is_reference?: boolean;
};

const PTS_TIDAS_EXPORT_ERROR_CODE = "PTS_MODULE_NOT_SUPPORTED_FOR_TIDAS_EXPORT";
const SOURCE_SPACE_TIDAS_BLOCKED_CODE = "TIANGONG_TIDAS_BLOCKED_BY_SOURCE_SPACE";

const formatTidasWarning = (warning: string | TidasExportWarning): string => {
  if (typeof warning === "string") return warning;
  return warning.message ?? warning.code ?? warning.category ?? JSON.stringify(warning);
};

const formatTidasExportError = (message: string, zh: boolean): string => {
  if (message.includes(PTS_TIDAS_EXPORT_ERROR_CODE)) {
    return zh
      ? "当前 TIDAS 导出不支持 PTS 模块。请先在建模界面解封 PTS，再导出单元过程、市场过程或 LCI 数据。"
      : "Current TIDAS export does not support PTS modules. Please unpack PTS in the modeling canvas before exporting unit process, market process, or LCI data.";
  }
  if (message.includes(SOURCE_SPACE_TIDAS_BLOCKED_CODE)) {
    return zh
      ? "当前模型包含非 EF/Tiangong 基本流。开源版不支持 EF 与 ecoinvent 基本流自动转换，不能导出天工 TIDAS。请替换为 EF 基本流后再导出。"
      : "This model contains non-EF/Tiangong elementary flows. The open-source edition does not convert EF and ecoinvent elementary flows automatically, so Tiangong TIDAS export is blocked. Please replace them with EF elementary flows before exporting.";
  }
  return message;
};

type TidasIssueSummary = {
  key: string;
  issue: string | TidasExportWarning;
  message: string;
  count: number;
};

const formatTidasIssue = (issue: string | TidasExportWarning, zh: boolean): string => {
  if (typeof issue === "string") return formatTidasExportError(issue, zh);
  if (issue.code === "FLOW_DEFAULT_UNIT_CONVERSION_REQUIRED") {
    return zh
      ? "Flow 当前建模单位无法换回默认单位，不能导出 TIDAS。"
      : "Flow current modelling unit cannot be converted back to the flow default unit for TIDAS export.";
  }
  return formatTidasWarning(issue);
};

const summarizeTidasIssues = (
  issues: Array<string | TidasExportWarning>,
  zh: boolean,
): TidasIssueSummary[] => {
  const summaries = new Map<string, TidasIssueSummary>();
  issues.forEach((issue) => {
    const rawMessage =
      typeof issue === "string"
        ? issue
        : issue.code ?? issue.message ?? issue.category ?? JSON.stringify(issue);
    const repairTarget =
      typeof issue === "string" ? "" : String(issue.details?.repair_target ?? issue.category ?? "");
    const key = `${rawMessage}::${repairTarget}`;
    const current = summaries.get(key);
    if (current) {
      current.count += 1;
      return;
    }
    summaries.set(key, {
      key,
      issue,
      message: formatTidasIssue(issue, zh),
      count: 1,
    });
  });
  return Array.from(summaries.values());
};

type TidasImportKind = "flows" | "processes" | "models";
type ImportedProjectSummary = {
  projectId: string;
  name: string;
};
type TidasImportResult = {
  importedCount: number;
  insertedCount: number;
  updatedCount: number;
  skippedCount: number;
  filteredCount: number;
  warningCount: number;
  failedCount: number;
  unresolvedCount: number;
  importedProcessCount: number;
  importedExchangeCount: number;
  topMissingFlowUuids: string[];
  errors: string[];
  jobId?: string;
  createdProjects: ImportedProjectSummary[];
};
const inferFlowBusinessType = (
  type: "intermediate_flow" | "elementary_flow" | "product_flow" | "waste_flow" | string,
  category?: string,
  flowName?: string,
): FlowBusinessType => {
  const rawType = String(type ?? "").trim().toLowerCase();
  if (rawType === "elementary_flow") {
    return "basic";
  }
  if (rawType === "waste_flow") {
    return "waste";
  }
  if (rawType === "product_flow") {
    return "product";
  }
  const categoryText = String(category ?? "").toLowerCase();
  const nameText = String(flowName ?? "").toLowerCase();
  const isWaste = categoryText.includes("waste") || categoryText.includes("废") || nameText.includes("废");
  return isWaste ? "waste" : "product";
};
const normalizeFlowType = (value: string | null | undefined): FlowRow["type"] => {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw.includes("elementary") || raw.includes("basic")) return "elementary_flow";
  if (raw.includes("waste")) return "waste_flow";
  if (raw.includes("product")) return "product_flow";
  return "intermediate_flow";
};
const mapFlowTypeLabel = (businessType: FlowBusinessType, zh: boolean): string => {
  if (zh) {
    if (businessType === "basic") return "基本流";
    if (businessType === "waste") return "废物流";
    return "产品流";
  }
  if (businessType === "basic") return "Elementary Flow";
  if (businessType === "waste") return "Waste Flow";
  return "Product Flow";
};
const toProjectRows = (projects: ProjectListItem[]): ProjectRow[] =>
  projects.map((item) => ({
    projectId: item.project_id,
    projectName: item.name,
    sourcePolicy: normalizeSourcePolicy(item.source_policy),
    referenceProduct: String(item.reference_product ?? ""),
    functionalUnit: String(item.functional_unit ?? ""),
    systemBoundary: String(item.system_boundary ?? ""),
    timeRepresentativeness: String(item.time_representativeness ?? ""),
    geography: String(item.geography ?? ""),
    description: String(item.description ?? ""),
    processCount: Number(item.process_count ?? 0),
    flowCount: Number(item.flow_count ?? 0),
    lastModified: formatTime(item.latest_version_created_at ?? item.created_at),
    status: String(item.status ?? (item.latest_version ? "启用" : "草稿")),
    latestVersion: item.latest_version,
  }));

function CreateProjectModal(props: {
  open: boolean;
  busy?: boolean;
  uiLanguage: "zh" | "en";
  mode?: "create" | "edit";
  initialForm?: CreateProjectForm;
  onClose: () => void;
  onSubmit: (form: CreateProjectForm) => Promise<void>;
}) {
  const { open, busy, uiLanguage, mode = "create", initialForm, onClose, onSubmit } = props;
  const [form, setForm] = useState<CreateProjectForm>(defaultForm);
  const [errorText, setErrorText] = useState("");
  const [locationLoadError, setLocationLoadError] = useState("");
  const zh = uiLanguage === "zh";

  useEffect(() => {
    if (open) {
      setForm(initialForm ?? defaultForm);
      setErrorText("");
      setLocationLoadError("");
    }
  }, [open, initialForm]);

  if (!open) {
    return null;
  }

  const setField = (key: keyof CreateProjectForm, value: string) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  const submit = async () => {
    if (!form.projectName.trim()) {
      setErrorText(zh ? "项目名称不能为空。" : "Project Name is required.");
      return;
    }
    if (locationLoadError && form.sourcePolicy === "tidas_compliant") {
      setErrorText(locationLoadError);
      return;
    }
    setErrorText("");
    await onSubmit({
      ...form,
      geography: normalizeTidasLocationValue(form.geography),
    });
    setForm(defaultForm);
    onClose();
  };

  return (
    <div className="pm-modal-mask">
      <div className="pm-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{mode === "edit" ? (zh ? "修改项目信息" : "Edit Project Info") : (zh ? "新建项目" : "Create Project")}</strong>
          <button type="button" className="pm-link-btn" onClick={onClose}>
            {zh ? "关闭" : "Close"}
          </button>
        </div>
        <div className="pm-modal-grid">
          <label>
            <span>{zh ? "项目名称" : "Project Name"}</span>
            <input value={form.projectName} onChange={(e) => setField("projectName", e.target.value)} />
          </label>
          <label>
            <span>{zh ? "数据源合规模式" : "Source Policy"}</span>
            <select value={form.sourcePolicy} onChange={(e) => setField("sourcePolicy", e.target.value)}>
              {sourcePolicyOptions.map((item) => (
                <option key={item.value} value={item.value}>
                  {zh ? item.zh : item.en}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>{zh ? "参考产品" : "Reference Product"}</span>
            <input
              value={form.referenceProduct}
              placeholder={zh ? "可留空，导出时优先从目标产品推导" : "Optional; inferred from target product first"}
              onChange={(e) => setField("referenceProduct", e.target.value)}
            />
          </label>
          <label>
            <span>{zh ? "功能单位" : "Functional Unit"}</span>
            <input
              value={form.functionalUnit}
              placeholder={zh ? "可留空，优先从 reference flow 推导" : "Optional; inferred from reference flow first"}
              onChange={(e) => setField("functionalUnit", e.target.value)}
            />
          </label>
          <label>
            <span>{zh ? "系统边界" : "System Boundary"}</span>
            <input
              value={form.systemBoundary}
              placeholder={zh ? "文档字段，例如 cradle-to-gate" : "Documentation field, e.g. cradle-to-gate"}
              onChange={(e) => setField("systemBoundary", e.target.value)}
            />
          </label>
          <label>
            <span>{zh ? "时间代表性" : "Time Representativeness"}</span>
            <input
              value={form.timeRepresentativeness}
              placeholder={zh ? "文档字段，例如 2024 或 2022-2024" : "Documentation field, e.g. 2024 or 2022-2024"}
              onChange={(e) => setField("timeRepresentativeness", e.target.value)}
            />
          </label>
          <label>
            <span>{zh ? "地理代表性" : "Geography"}</span>
            <TidasLocationCascade
              value={form.geography}
              uiLanguage={uiLanguage}
              onChange={(value) => setField("geography", value)}
              onCatalogStatus={(ok, message) => setLocationLoadError(ok ? "" : message)}
            />
          </label>
          <label className="span-2">
            <span>{zh ? "说明" : "Description"}</span>
            <textarea value={form.description} onChange={(e) => setField("description", e.target.value)} rows={4} />
          </label>
        </div>
        {errorText && <div className="pm-error">{errorText}</div>}
        <div className="pm-modal-actions">
          <button type="button" className="pm-ghost-btn" onClick={onClose}>
            {zh ? "取消" : "Cancel"}
          </button>
          <button type="button" onClick={() => void submit()} disabled={busy}>
            {mode === "edit" ? (zh ? "保存" : "Save") : (zh ? "创建" : "Create")}
          </button>
        </div>
      </div>
    </div>
  );
}

function normalizeUnitGroupKey(value: string | null | undefined): string {
  return String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");
}

function TidasCompatibilityModal(props: {
  open: boolean;
  uiLanguage: "zh" | "en";
  flow: FlowRow | null;
  onClose: () => void;
  onSaved: (flow: FlowRow) => void;
  onStatus?: (text: string) => void;
}) {
  const { open, uiLanguage, flow, onClose, onSaved, onStatus } = props;
  const zh = uiLanguage === "zh";
  const [unitDefinitions, setUnitDefinitions] = useState<TidasUnitDefinition[]>([]);
  const [allowedUnitGroups, setAllowedUnitGroups] = useState<Set<string>>(new Set());
  const [tidasCompatible, setTidasCompatible] = useState(false);
  const [tidasUnitGroup, setTidasUnitGroup] = useState("");
  const [tidasFlowPropertyUuid, setTidasFlowPropertyUuid] = useState("");
  const [tidasReferenceSource, setTidasReferenceSource] = useState("user_declared");
  const [busy, setBusy] = useState(false);
  const [errorText, setErrorText] = useState("");

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    Promise.all([
      fetch(`${API_BASE}/reference/units`).then((resp) => (resp.ok ? resp.json() : [])),
      fetch(`${API_BASE}/reference/tidas-policy`).then((resp) => (resp.ok ? resp.json() : null)),
    ])
      .then(([unitRows, policy]) => {
        if (cancelled) return;
        setUnitDefinitions(Array.isArray(unitRows) ? unitRows : []);
        const allowed = Array.isArray(policy?.allowed_unit_groups) ? policy.allowed_unit_groups : [];
        setAllowedUnitGroups(new Set(allowed.map((item: unknown) => normalizeUnitGroupKey(String(item)))));
      })
      .catch(() => {
        if (!cancelled) {
          setUnitDefinitions([]);
          setAllowedUnitGroups(new Set());
        }
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  useEffect(() => {
    if (!open || !flow) return;
    setTidasCompatible(Boolean(flow.tidasCompatible));
    setTidasUnitGroup(String(flow.tidasUnitGroup ?? ""));
    setTidasFlowPropertyUuid(String(flow.tidasFlowPropertyUuid ?? ""));
    setTidasReferenceSource(String(flow.tidasReferenceSource ?? "user_declared"));
    setErrorText("");
  }, [flow, open]);

  if (!open || !flow) {
    return null;
  }

  const unitGroups = Array.from(new Set(unitDefinitions.map((item) => item.unit_group).filter(Boolean)))
    .filter((group) => allowedUnitGroups.size === 0 || allowedUnitGroups.has(normalizeUnitGroupKey(group)));

  const save = async () => {
    if (tidasCompatible && allowedUnitGroups.size > 0 && !allowedUnitGroups.has(normalizeUnitGroupKey(tidasUnitGroup))) {
      setErrorText(zh ? "该单位组不在 TIDAS 允许范围内。" : "This unit group is not allowed by TIDAS.");
      return;
    }
    setBusy(true);
    setErrorText("");
    try {
      const resp = await fetch(`${API_BASE}/flows/${encodeURIComponent(flow.id)}/tidas-compatibility`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tidasCompatible,
          tidasUnitGroup: tidasCompatible ? tidasUnitGroup : undefined,
          tidasFlowPropertyUuid: tidasCompatible ? tidasFlowPropertyUuid : undefined,
          tidasReferenceSource: tidasCompatible ? tidasReferenceSource : undefined,
        }),
      });
      if (!resp.ok) {
        const payload = (await resp.json().catch(() => ({}))) as { detail?: { message?: string }; message?: string };
        throw new Error(payload.detail?.message ?? payload.message ?? `HTTP ${resp.status}`);
      }
      const payload = await resp.json() as {
        flow_uuid: string;
        flow_name: string;
        flow_name_en?: string | null;
        flow_type: FlowRow["type"];
        default_unit?: string;
        compartment?: string | null;
        source?: string | null;
        is_custom?: boolean;
        tidas_compatible?: boolean;
        tidas_unit_group?: string | null;
        tidas_flow_property_uuid?: string | null;
        tidas_reference_source?: string | null;
        source_updated_at?: string | null;
      };
      onSaved({
        id: payload.flow_uuid,
        flowName: payload.flow_name,
        flowNameEn: String(payload.flow_name_en ?? "").trim() || undefined,
        type: payload.flow_type,
        unit: String(payload.default_unit ?? flow.unit),
        category: String(payload.compartment ?? flow.category ?? "-"),
        source: String(payload.source ?? flow.source ?? ""),
        isCustom: Boolean(payload.is_custom),
        tidasCompatible: Boolean(payload.tidas_compatible),
        tidasUnitGroup: payload.tidas_unit_group ?? null,
        tidasFlowPropertyUuid: payload.tidas_flow_property_uuid ?? null,
        tidasReferenceSource: payload.tidas_reference_source ?? null,
        usedInProcesses: flow.usedInProcesses,
        lastModified: formatTime(String(payload.source_updated_at ?? "")),
      });
      onStatus?.(zh ? "TIDAS 兼容信息已保存，请重新运行 readiness。" : "TIDAS compatibility saved. Re-run readiness.");
      onClose();
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : zh ? "保存失败" : "Save failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="overlay-modal" onClick={onClose}>
      <section className="pm-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{zh ? "TIDAS 兼容补录" : "TIDAS Compatibility"}</strong>
          <button type="button" className="drawer-close-btn" onClick={onClose}>{zh ? "关闭" : "Close"}</button>
        </div>
        <div className="target-product-form">
          <label className="span-2">
            {zh ? "Flow" : "Flow"}
            <input value={getDisplayFlowName(flow, uiLanguage)} disabled />
          </label>
          <label>
            {zh ? "TIDAS 兼容" : "TIDAS compatible"}
            <select value={tidasCompatible ? "yes" : "no"} onChange={(event) => setTidasCompatible(event.target.value === "yes")}>
              <option value="yes">{zh ? "是" : "Yes"}</option>
              <option value="no">{zh ? "否" : "No"}</option>
            </select>
          </label>
          <label>
            {zh ? "TIDAS 单位组" : "TIDAS unit group"}
            <select disabled={!tidasCompatible} value={tidasUnitGroup} onChange={(event) => setTidasUnitGroup(event.target.value)}>
              <option value="">{zh ? "请选择" : "Select"}</option>
              {unitGroups.map((group) => <option key={group} value={group}>{group}</option>)}
            </select>
          </label>
          <label>
            {zh ? "Flow property UUID" : "Flow property UUID"}
            <input disabled={!tidasCompatible} value={tidasFlowPropertyUuid} onChange={(event) => setTidasFlowPropertyUuid(event.target.value)} />
          </label>
          <label>
            {zh ? "来源" : "Source"}
            <input disabled={!tidasCompatible} value={tidasReferenceSource} onChange={(event) => setTidasReferenceSource(event.target.value)} />
          </label>
        </div>
        {errorText && <div className="pm-form-error">{errorText}</div>}
        <div className="pm-modal-actions">
          <button type="button" className="pm-ghost-btn" onClick={onClose}>{zh ? "取消" : "Cancel"}</button>
          <button type="button" disabled={busy} onClick={() => void save()}>{zh ? "保存" : "Save"}</button>
        </div>
      </section>
    </div>
  );
}

function ProcessDetailModal(props: {
  open: boolean;
  uiLanguage: "zh" | "en";
  busy: boolean;
  process: ProcessRow | null;
  detail: ProcessDetailResponse | null;
  flowRefs: Map<string, FlowRefInfo>;
  errorText: string;
  onClose: () => void;
}) {
  const { open, uiLanguage, busy, process, detail, flowRefs, errorText, onClose } = props;
  const zh = uiLanguage === "zh";
  const [activeDirection, setActiveDirection] = useState<"input" | "output">("input");
  useEffect(() => {
    if (open) {
      setActiveDirection("input");
    }
  }, [open, process?.processId]);
  if (!open || !process) {
    return null;
  }
  const source = (detail?.process_json && typeof detail.process_json === "object"
    ? detail.process_json
    : detail) as Record<string, unknown> | null;
  const rawExchanges = Array.isArray(source?.exchanges) ? (source?.exchanges as Array<Record<string, unknown>>) : [];
  const normalizedExchanges = rawExchanges.map((ex) => {
    const flowUuid = String(ex.flow_uuid ?? ex.flowUuid ?? "");
    const directionRaw = String(ex.direction ?? ex.input_output ?? "").toLowerCase();
    const direction: "input" | "output" = directionRaw.includes("input") ? "input" : "output";
    const ref = flowUuid ? flowRefs.get(flowUuid) : undefined;
    const flowType = String(ref?.flow_type ?? ex.flow_type ?? "").toLowerCase();
    const isElementary = flowType.includes("elementary") || flowType.includes("basic") || flowType.includes("基本");
    const fallbackName = flowUuid || "-";
    const hasResolvedName = Boolean(ref?.flow_name || ref?.flow_name_en);
    const refStatus: "ok" | "missing_uuid" | "missing_name" =
      ref?.__status === "missing_uuid" ? "missing_uuid" : hasResolvedName ? "ok" : "missing_name";
    const localizedExchangeName = zh
      ? ex.name ?? ex.flow_name ?? ex.flowName ?? ref?.flow_name ?? ref?.flow_name_en ?? fallbackName
      : ex.flow_name_en ?? ex.flowNameEn ?? ref?.flow_name_en ?? ex.name ?? ex.flow_name ?? ex.flowName ?? ref?.flow_name ?? fallbackName;
    const localizedResolvedName = zh
      ? ref?.flow_name ?? ref?.flow_name_en ?? fallbackName
      : ref?.flow_name_en ?? ref?.flow_name ?? fallbackName;
    return {
      name: String(localizedExchangeName),
      flowUuid,
      amount: String(ex.amount ?? "-"),
      unit: String(ex.unit ?? ref?.default_unit ?? ref?.unit ?? "-"),
      isProduct: Boolean(ex.is_reference_flow ?? ex.is_product ?? ex.isProduct),
      isReference: Boolean(ex.is_reference_flow),
      resolvedName: String(localizedResolvedName),
      refStatus,
      direction,
      isElementary,
    };
  });
  const inputIntermediate = normalizedExchanges.filter((e) => e.direction === "input" && !e.isElementary);
  const inputElementary = normalizedExchanges.filter((e) => e.direction === "input" && e.isElementary);
  const outputIntermediate = normalizedExchanges.filter((e) => e.direction === "output" && !e.isElementary);
  const outputElementary = normalizedExchanges.filter((e) => e.direction === "output" && e.isElementary);

  const renderExchangeTable = (
    rows: Array<{
      name: string;
      flowUuid: string;
      amount: string;
      unit: string;
      isProduct: boolean;
      isReference: boolean;
      resolvedName: string;
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
              const displayName = row.isReference
                ? `${row.name}@${zh ? "参考流" : "Reference Flow"}`
                : row.name;
              const cellClassName = [
                "pm-cell-ellipsis",
                missingFlow ? "pm-inline-hint warn" : "",
                row.isReference ? "pm-ref-quantity-name" : "",
              ]
                .filter(Boolean)
                .join(" ");
              return (
                <tr key={`${title}-${row.flowUuid}-${idx}`}>
                  <td className={cellClassName} title={missingFlow ? missingText : displayName}>
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

  const currentIntermediateRows = activeDirection === "input" ? inputIntermediate : outputIntermediate;
  const currentElementaryRows = activeDirection === "input" ? inputElementary : outputElementary;
  const displayProcessName = zh
    ? process.processName
    : String(detail?.process_name_en ?? process.processNameEn ?? "").trim() || process.processName;
  const displayReferenceFlowName = zh
    ? process.referenceFlowName || detail?.reference_flow_name || ""
    : String(detail?.reference_flow_name_en ?? "").trim() ||
    process.referenceFlowName ||
    detail?.reference_flow_name ||
    "";
  const displayReferenceFlowIdentity =
    displayReferenceFlowName ||
    detail?.reference_flow_uuid ||
    process.referenceFlowUuid ||
    detail?.reference_flow_internal_id ||
    process.referenceFlowInternalId ||
    "-";

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
          <div>
            <b>{zh ? "名称" : "Name"}:</b> {displayProcessName}
          </div>
          <div>
            <b>{zh ? "参考流" : "Reference Flow"}:</b>{" "}
            {displayReferenceFlowIdentity}
            {displayReferenceFlowName && (detail?.reference_flow_uuid || process.referenceFlowUuid)
              ? ` (${detail?.reference_flow_uuid || process.referenceFlowUuid})`
              : ""}
          </div>
          <div><b>ID:</b> {process.processId}</div>
          <div><b>{zh ? "最近修改" : "Last Modified"}:</b> {process.lastModified}</div>
          {process.processUuid && process.processUuid !== process.processId && (
            <div><b>UUID:</b> {process.processUuid}</div>
          )}
        </div>
        {busy && <div className="pm-empty-note">{zh ? "详情加载中..." : "Loading details..."}</div>}
        {!busy && errorText && <div className="pm-error">{errorText}</div>}
        {!busy && !errorText && (
          <div className="pm-process-detail-grid">
            <div className="pm-detail-tabs">
              <div className="pm-detail-tab-row">
                <button
                  type="button"
                  className={activeDirection === "input" ? "active" : ""}
                  onClick={() => setActiveDirection("input")}
                >
                  {zh ? "输入" : "Inputs"}
                </button>
                <button
                  type="button"
                  className={activeDirection === "output" ? "active" : ""}
                  onClick={() => setActiveDirection("output")}
                >
                  {zh ? "输出" : "Outputs"}
                </button>
              </div>
            </div>
            {renderExchangeTable(currentIntermediateRows, zh ? "中间流" : "Intermediate Flows")}
            {renderExchangeTable(currentElementaryRows, zh ? "基本流" : "Elementary Flows")}
          </div>
        )}
      </div>
    </div>
  );
}

function TidasImportModal(props: {
  open: boolean;
  uiLanguage: "zh" | "en";
  importKind: TidasImportKind;
  fixedKind?: boolean;
  onClose: () => void;
  onImported: (kind: TidasImportKind, result: TidasImportResult) => void;
  onStatus?: (text: string) => void;
}) {
  const { open, uiLanguage, importKind, fixedKind = false, onClose, onImported, onStatus } = props;
  const zh = uiLanguage === "zh";
  const [tab, setTab] = useState<TidasImportKind>(importKind);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [result, setResult] = useState<TidasImportResult | null>(null);
  const [reportBusy, setReportBusy] = useState(false);
  const [reportPayload, setReportPayload] = useState<Record<string, unknown> | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!open) {
      setTab(importKind);
      setBusy(false);
      setErrorText("");
      setResult(null);
      setReportBusy(false);
      setReportPayload(null);
      setSelectedFile(null);
    }
  }, [importKind, open]);

  useEffect(() => {
    if (open) {
      setTab(importKind);
      setSelectedFile(null);
    }
  }, [importKind, open]);

  const compactImportLabel = (kind: TidasImportKind): string => {
    if (zh) {
      if (kind === "models") return "导入模型";
      if (kind === "processes") return "导入过程";
      return "导入流";
    }
    if (kind === "models") return "Import Model";
    if (kind === "processes") return "Import Process";
    return "Import Flow";
  };

  const modalTitle = (() => {
    if (tab === "models") {
      return zh ? "从天工 ZIP / 模型 JSON 新建项目" : "Create Project from TIDAS ZIP / Model JSON";
    }
    if (tab === "processes") {
      return zh ? "导入天工过程（入库）" : "Import TIDAS Processes";
    }
    return zh ? "导入天工流（入库）" : "Import TIDAS Flows";
  })();

  const parseResult = (payload: Record<string, unknown>): TidasImportResult => {
    const importedRaw =
      payload.imported_count ??
      payload.imported_flow_count ??
      payload.imported_process_count ??
      payload.imported_model_count ??
      payload.inserted ??
      0;
    const insertedRaw = payload.inserted ?? 0;
    const updatedRaw = payload.updated ?? 0;
    const skippedRaw = payload.skipped ?? 0;
    const filteredRaw = payload.filtered_count ?? payload.filtered_exchange_count ?? 0;
    const warningRaw = payload.warning_count ?? (Array.isArray(payload.warnings) ? payload.warnings.length : 0);
    const failedRaw = payload.failed_count ?? payload.failed ?? 0;
    const unresolvedRaw = payload.unresolved_count ?? 0;
    const importedProcessRaw = payload.imported_process_count ?? 0;
    const importedExchangeRaw = payload.imported_exchange_count ?? 0;
    const topMissing = Array.isArray(payload.top_missing_flow_uuids)
      ? payload.top_missing_flow_uuids.map((item) => String(item))
      : [];
    const errors = Array.isArray(payload.errors) ? payload.errors.map((item) => String(item)) : [];
    const createdProjects = Array.isArray(payload.created_projects)
      ? payload.created_projects
        .map((item) => {
          if (!item || typeof item !== "object") {
            return null;
          }
          const row = item as Record<string, unknown>;
          const projectId = String(row.project_id ?? "").trim();
          if (!projectId) {
            return null;
          }
          return {
            projectId,
            name: String(row.name ?? projectId).trim() || projectId,
          } satisfies ImportedProjectSummary;
        })
        .filter((item): item is ImportedProjectSummary => Boolean(item))
      : [];
    return {
      importedCount: Number.isFinite(Number(importedRaw)) ? Number(importedRaw) : 0,
      insertedCount: Number.isFinite(Number(insertedRaw)) ? Number(insertedRaw) : 0,
      updatedCount: Number.isFinite(Number(updatedRaw)) ? Number(updatedRaw) : 0,
      skippedCount: Number.isFinite(Number(skippedRaw)) ? Number(skippedRaw) : 0,
      filteredCount: Number.isFinite(Number(filteredRaw)) ? Number(filteredRaw) : 0,
      warningCount: Number.isFinite(Number(warningRaw)) ? Number(warningRaw) : 0,
      failedCount: Number.isFinite(Number(failedRaw)) ? Number(failedRaw) : 0,
      unresolvedCount: Number.isFinite(Number(unresolvedRaw)) ? Number(unresolvedRaw) : 0,
      importedProcessCount: Number.isFinite(Number(importedProcessRaw)) ? Number(importedProcessRaw) : 0,
      importedExchangeCount: Number.isFinite(Number(importedExchangeRaw)) ? Number(importedExchangeRaw) : 0,
      topMissingFlowUuids: topMissing,
      errors,
      jobId: typeof payload.job_id === "string" ? payload.job_id : undefined,
      createdProjects,
    };
  };

  const isModelImportPackageMismatch = (kind: TidasImportKind, next: TidasImportResult): boolean =>
    kind === "models" &&
    next.createdProjects.length === 0 &&
    next.failedCount > 0 &&
    next.errors.some((message) => /model_file|no model json found/i.test(message));

  const buildModelImportPackageMismatchText = (): string =>
    zh
      ? "导入部分失败：ZIP 包中未找到模型文件，未创建项目。这个包更像过程包，请使用“导入过程”。"
      : 'Import partially failed: no model found in bundle. No project was created. This package looks like a process package. Please use "Import Process".';

  const buildImportStatusText = (kind: TidasImportKind, next: TidasImportResult): string => {
    if (isModelImportPackageMismatch(kind, next)) {
      return buildModelImportPackageMismatchText();
    }
    const label = zh
      ? kind === "models"
        ? "模型导入"
        : kind === "processes"
          ? "过程导入"
          : "流导入"
      : kind === "models"
        ? "Model import"
        : kind === "processes"
          ? "Process import"
          : "Flow import";
    const summary = zh
      ? `新增 ${next.insertedCount}，更新 ${next.updatedCount}，跳过 ${next.skippedCount}，失败 ${next.failedCount}`
      : `Inserted ${next.insertedCount}, updated ${next.updatedCount}, skipped ${next.skippedCount}, failed ${next.failedCount}`;
    const extra =
      next.importedProcessCount > 0 || next.importedExchangeCount > 0 || next.filteredCount > 0
        ? zh
          ? `；过程 ${next.importedProcessCount}，交换 ${next.importedExchangeCount}，过滤 ${next.filteredCount}`
          : `; processes ${next.importedProcessCount}, exchanges ${next.importedExchangeCount}, filtered ${next.filteredCount}`
        : "";
    if (next.failedCount > 0) {
      return zh ? `警告：${label}部分完成。${summary}${extra}` : `Warning: ${label} partially completed. ${summary}${extra}`;
    }
    if (next.warningCount > 0) {
      return zh ? `警告：${label}完成，但有提醒。${summary}${extra}` : `Warning: ${label} completed with warnings. ${summary}${extra}`;
    }
    return zh ? `${label}完成。${summary}${extra}` : `${label} completed. ${summary}${extra}`;
  };

  const runImport = async () => {
    if (!selectedFile) {
      setErrorText(
        tab === "models"
          ? (zh ? "请选择一个 ZIP 或 model JSON 文件。" : "Please choose a ZIP or model JSON file.")
          : tab === "processes"
            ? (zh ? "请选择一个 process JSON 或 ZIP 文件。" : "Please choose a process JSON or ZIP file.")
            : (zh ? "请选择一个 flow JSON 文件。" : "Please choose a flow JSON file."),
      );
      return;
    }
    setBusy(true);
    setErrorText("");
    setResult(null);
    setReportPayload(null);
    try {
      const body = new FormData();
      body.append("file", selectedFile);
      body.append("dry_run", "false");
      body.append("strict_mode", "false");
      if (tab === "models") {
        body.append("display_lang", uiLanguage);
      }
      const endpoint =
        tab === "models" && selectedFile.name.toLowerCase().endsWith(".zip")
          ? `${API_BASE}/import/tidas/bundle`
          : `${API_BASE}/import/tidas/${tab}`;
      const resp = await fetch(endpoint, {
        method: "POST",
        body,
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { message?: string };
        throw new Error(err.message ?? `HTTP ${resp.status}`);
      }
      const payload = (await resp.json()) as Record<string, unknown>;
      const nextResult = parseResult(payload);
      setResult(nextResult);
      const statusText = buildImportStatusText(tab, nextResult);
      onStatus?.(statusText);
      if (isModelImportPackageMismatch(tab, nextResult)) {
        setErrorText(statusText);
        return;
      }
      onImported(tab, nextResult);
    } catch (error) {
      const message = error instanceof Error ? error.message : zh ? "导入失败" : "Import failed";
      setErrorText(message);
      onStatus?.(zh ? `导入失败: ${message}` : `Import failed: ${message}`);
    } finally {
      setBusy(false);
    }
  };

  const viewReport = async () => {
    if (!result?.jobId) return;
    setReportBusy(true);
    setErrorText("");
    try {
      const resp = await fetch(`${API_BASE}/import/reports/${encodeURIComponent(result.jobId)}`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      const payload = (await resp.json()) as Record<string, unknown>;
      setReportPayload(payload);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : zh ? "报告加载失败" : "Report load failed");
    } finally {
      setReportBusy(false);
    }
  };

  if (!open) return null;

  return (
    <div className="pm-modal-mask" onClick={onClose}>
      <div className="pm-modal pm-tidas-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{modalTitle}</strong>
          <button type="button" className="pm-link-btn" onClick={onClose}>
            {zh ? "关闭" : "Close"}
          </button>
        </div>
        {!fixedKind && (
          <div className="pm-tidas-tabs">
            <button type="button" className={tab === "flows" ? "active" : ""} onClick={() => setTab("flows")}>
              {compactImportLabel("flows")}
            </button>
            <button type="button" className={tab === "processes" ? "active" : ""} onClick={() => setTab("processes")}>
              {compactImportLabel("processes")}
            </button>
            <button type="button" className={tab === "models" ? "active" : ""} onClick={() => setTab("models")}>
              {compactImportLabel("models")}
            </button>
          </div>
        )}
        <div className="pm-modal-grid">
          <label className="span-2">
            <span>
              {tab === "models"
                ? (zh ? "ZIP 包 / 模型 JSON 文件" : "ZIP Package / Model JSON File")
                : tab === "processes"
                  ? (zh ? "过程 JSON / ZIP 文件" : "Process JSON / ZIP File")
                  : (zh ? "流 JSON 文件" : "Flow JSON File")}
            </span>
            <div className="pm-file-picker-row">
              <input
                className="pm-file-picker-display"
                value={selectedFile?.name ?? ""}
                readOnly
                placeholder={
                  tab === "models"
                    ? (zh ? "请选择 .zip 或 model/*.json 文件" : "Choose .zip or model/*.json")
                    : tab === "processes"
                      ? (zh ? "请选择 process/*.json 或 .zip 文件" : "Choose process/*.json or .zip")
                      : (zh ? "请选择 flow/*.json 文件" : "Choose flow/*.json")
                }
              />
              <button type="button" className="pm-ghost-btn" onClick={() => fileInputRef.current?.click()}>
                {zh ? "选择文件" : "Browse"}
              </button>
              <input
                ref={fileInputRef}
                type="file"
                accept={
                  tab === "models" || tab === "processes"
                    ? ".zip,.json,application/json,application/zip"
                    : ".json,application/json"
                }
                style={{ display: "none" }}
                onChange={(event) => {
                  const file = event.target.files?.[0] ?? null;
                  setSelectedFile(file);
                  setErrorText("");
                }}
              />
            </div>
          </label>
        </div>
        {tab === "models" && (
          <div className="pm-help-text">
            {zh
              ? "优先选择天工 ZIP 包；也兼容单个 model JSON 文件。ZIP 会按 manifest 顺序导入 flow / process / model。"
              : "Prefer a Tiangong ZIP bundle; a single model JSON file is also supported. ZIP imports flow / process / model in manifest order."}
          </div>
        )}
        {tab === "processes" && (
          <div className="pm-help-text">
            {zh
              ? "支持单个 process JSON，也支持包含 manifest.json + flow/ + process/ 的 ZIP。ZIP 会先导入 flow，再导入 process。"
              : "Supports a single process JSON and ZIP bundles with manifest.json + flow/ + process/. ZIP imports flows first, then processes."}
          </div>
        )}
        {errorText && <div className="pm-error">{errorText}</div>}
        {result && (
          <div className="pm-tidas-result">
            <div>{zh ? `成功数量: ${result.importedCount}` : `Imported: ${result.importedCount}`}</div>
            <div>{zh ? `过滤数量: ${result.filteredCount}` : `Filtered: ${result.filteredCount}`}</div>
            <div>{zh ? `告警数量: ${result.warningCount}` : `Warnings: ${result.warningCount}`}</div>
            <div>{zh ? `失败数量: ${result.failedCount}` : `Failed: ${result.failedCount}`}</div>
            {result.unresolvedCount > 0 && <div>{zh ? `未解析数量: ${result.unresolvedCount}` : `Unresolved: ${result.unresolvedCount}`}</div>}
            {result.topMissingFlowUuids.length > 0 && (
              <div>{zh ? "Top 缺失 flow UUID: " : "Top Missing Flow UUIDs: "}{result.topMissingFlowUuids.join(", ")}</div>
            )}
            {result.jobId && (
              <div className="pm-tidas-report-row">
                <span>Job ID: {result.jobId}</span>
                <button type="button" className="pm-link-btn primary" onClick={() => void viewReport()} disabled={reportBusy}>
                  {reportBusy ? (zh ? "加载中..." : "Loading...") : (zh ? "查看报告" : "View Report")}
                </button>
              </div>
            )}
          </div>
        )}
        {reportPayload && (
          <div className="pm-tidas-report">
            <pre>{JSON.stringify(reportPayload, null, 2)}</pre>
          </div>
        )}
        <div className="pm-modal-actions">
          <button type="button" className="pm-ghost-btn" onClick={onClose}>
            {zh ? "关闭" : "Close"}
          </button>
          <button type="button" onClick={() => void runImport()} disabled={busy}>
            {busy
              ? (zh ? "导入中..." : "Importing...")
              : tab === "models"
                ? (zh ? "从 ZIP / 模型 JSON 新建项目" : "Create Project from ZIP / Model JSON")
                : (zh ? "开始导入" : "Start Import")}
          </button>
        </div>
      </div>
    </div>
  );
}

function TidasExportModal(props: {
  open: boolean;
  uiLanguage: "zh" | "en";
  busy: boolean;
  previewBusy: boolean;
  preview: TidasExportPreviewResponse | null;
  targetProject: { projectId: string; projectName: string; latestVersion: number | null } | null;
  onClose: () => void;
  onExport: () => void;
  onRepair: (issue: string | TidasExportWarning) => void;
}) {
  const { open, uiLanguage, busy, previewBusy, preview, targetProject, onClose, onExport, onRepair } = props;
  const zh = uiLanguage === "zh";

  if (!open || !targetProject) return null;

  const hasErrors = preview ? preview.errors.length > 0 || !preview.can_export : false;
  const hasManualAllocation = preview ? preview.manual_allocation_required_processes.length > 0 : false;
  const hasWarnings = preview ? preview.warnings.length > 0 || preview.allocation_warnings.length > 0 : false;
  const blockingIssues = preview ? (preview.blocking && preview.blocking.length > 0 ? preview.blocking : preview.errors) : [];
  const blockingSummaries = summarizeTidasIssues(blockingIssues, zh);

  return (
    <div className="pm-modal-mask" onClick={onClose}>
      <div className="pm-modal pm-tidas-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{zh ? "导出 TIDAS/ILCD 风格 Bundle" : "Export TIDAS/ILCD-style Bundle"}</strong>
          <button type="button" className="pm-link-btn" onClick={onClose}>
            {zh ? "关闭" : "Close"}
          </button>
        </div>
        <div className="pm-tidas-export-info">
          <div><b>{zh ? "项目" : "Project"}:</b> {targetProject.projectName}</div>
          <div><b>ID:</b> {targetProject.projectId}</div>
          {targetProject.latestVersion && <div><b>{zh ? "版本" : "Version"}:</b> {targetProject.latestVersion}</div>}
        </div>
        {previewBusy && <div className="pm-empty-note">{zh ? "预览加载中..." : "Loading preview..."}</div>}
        {!previewBusy && preview && (
          <div className="pm-tidas-result">
            <div className="pm-tidas-export-counts">
              <div>{zh ? "流数量" : "Flows"}: {preview.flow_count}</div>
              <div>{zh ? "过程数量" : "Processes"}: {preview.process_count}</div>
              <div>{zh ? "导出模型数量" : "Exported Models"}: {preview.exported_model_count}</div>
              {preview.multi_product_process_count > 0 && (
                <div>{zh ? "多产品过程" : "Multi-product Processes"}: {preview.multi_product_process_count}</div>
              )}
            </div>
            {hasErrors && (
              <div className="pm-tidas-errors">
                <h4>
                  {zh ? "错误" : "Errors"} ({blockingIssues.length})
                </h4>
                <ul className="pm-tidas-scroll-list pm-tidas-issue-list">
                  {blockingSummaries.map((item) => (
                    <li key={item.key}>
                      <span className="pm-tidas-issue-text">
                        {item.message}
                        {item.count > 1 && <span className="pm-tidas-issue-count">×{item.count}</span>}
                      </span>
                      <button type="button" className="pm-link-btn" onClick={() => onRepair(item.issue)}>
                        {zh ? "修复" : "Fix"}
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {preview.missing_flows.length > 0 && (
              <div className="pm-tidas-warnings">
                <h4>{zh ? "缺失的流" : "Missing Flows"}</h4>
                <ul>
                  {preview.missing_flows.slice(0, 10).map((f, i) => <li key={i}>{f}</li>)}
                </ul>
                {preview.missing_flows.length > 10 && <div>({zh ? "显示前 10 条" : "Showing first 10"})</div>}
              </div>
            )}
            {preview.missing_processes.length > 0 && (
              <div className="pm-tidas-warnings">
                <h4>{zh ? "缺失的过程" : "Missing Processes"}</h4>
                <ul>
                  {preview.missing_processes.slice(0, 10).map((p, i) => <li key={i}>{p}</li>)}
                </ul>
                {preview.missing_processes.length > 10 && <div>({zh ? "显示前 10 条" : "Showing first 10"})</div>}
              </div>
            )}
            {hasWarnings && (
              <div className="pm-tidas-warnings">
                <h4>
                  {zh ? "警告" : "Warnings"} ({preview.warnings.length + preview.allocation_warnings.length})
                </h4>
                <ul className="pm-tidas-scroll-list">
                  {preview.warnings.map((w, i) => <li key={`w-${i}`}>{formatTidasWarning(w)}</li>)}
                  {preview.allocation_warnings.map((w, i) => (
                    <li key={`alloc-${i}`}>
                      <span>{formatTidasWarning(w)}</span>
                      <button type="button" className="pm-link-btn" onClick={() => onRepair(w)}>
                        {zh ? "修复" : "Fix"}
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {hasManualAllocation && (
              <div className="pm-tidas-warnings pm-tidas-allocation-warning">
                <h4>{zh ? "需要手动分配的多产品过程" : "Multi-product Processes Requiring Manual Allocation"}</h4>
                <ul>
                  {preview.manual_allocation_required_processes.map((p, i) => (
                    <li key={i}>
                      <span>{p}</span>
                      <button type="button" className="pm-link-btn" onClick={() => onRepair({ code: "manual_allocation_required", message: p, details: { process_uuid: p, repair_target: "allocation" } })}>
                        {zh ? "修复" : "Fix"}
                      </button>
                    </li>
                  ))}
                </ul>
                <p className="pm-tidas-allocation-note">
                  {zh ? "⚠ 部分多产品过程需要手动分配，导出不会写入伪分配比例。" : "⚠ Some multi-product processes require manual allocation. Export will not write pseudo-allocation ratios."}
                </p>
              </div>
            )}
            <p className="pm-tidas-disclaimer">
              {zh ? "⚠ 天工平台导入需真实样包/API 验证，导出结果不保证完全兼容。" : "⚠ Tiangong platform import requires real sample bundle/API verification. Export result is not guaranteed to be fully compatible."}
            </p>
          </div>
        )}
        <div className="pm-modal-actions">
          <button type="button" className="pm-ghost-btn" onClick={onClose}>
            {zh ? "关闭" : "Close"}
          </button>
          <button type="button" className="pm-primary-btn" onClick={() => void onExport()} disabled={busy || hasErrors || previewBusy}>
            {busy
              ? (zh ? "导出中..." : "Exporting...")
              : (zh ? "下载 ZIP" : "Download ZIP")}
          </button>
        </div>
      </div>
    </div>
  );
}

export function ProjectManagement(props: Props) {
  const { projects, busy, uiLanguage = "zh", onChangeLanguage, onStatus, onOpenProject, onCreateProject, onDeleteProject, onCreateProcess } = props;
  const zh = uiLanguage === "zh";
  const [activeModule, setActiveModule] = useState<NavModule>("project");
  const [activeItem, setActiveItem] = useState<NavItem>("recent_projects");
  const [projectSearch, setProjectSearch] = useState("");
  const [processSearch, setProcessSearch] = useState("");
  const [flowSearch, setFlowSearch] = useState("");
  const [flowTypeFilter, setFlowTypeFilter] = useState<"all" | FlowBusinessType>("all");
  const [flowCategoryFilter, setFlowCategoryFilter] = useState("");
  const [modalOpen, setModalOpen] = useState(false);
  const [editingProject, setEditingProject] = useState<ProjectRow | null>(null);
  const [projectEditBusy, setProjectEditBusy] = useState(false);
  const [processDetailOpen, setProcessDetailOpen] = useState(false);
  const [processDetailBusy, setProcessDetailBusy] = useState(false);
  const [processDetailError, setProcessDetailError] = useState("");
  const [selectedProcess, setSelectedProcess] = useState<ProcessRow | null>(null);
  const [processDetail, setProcessDetail] = useState<ProcessDetailResponse | null>(null);
  const [processFlowRefs, setProcessFlowRefs] = useState<Map<string, FlowRefInfo>>(new Map());
  const [serverProjectRows, setServerProjectRows] = useState<ProjectRow[]>(toProjectRows(projects));
  const [serverProcessRows, setServerProcessRows] = useState<ProcessRow[]>([]);
  const [serverFlowRows, setServerFlowRows] = useState<FlowRow[]>([]);
  const [flowCategories, setFlowCategories] = useState<FlowCategoryItem[]>([]);
  const [projectPage, setProjectPage] = useState(1);
  const [processPage, setProcessPage] = useState(1);
  const [flowPage, setFlowPage] = useState(1);
  const [projectTotal, setProjectTotal] = useState(0);
  const [processTotal, setProcessTotal] = useState(0);
  const [flowTotal, setFlowTotal] = useState(0);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [statsLoading, setStatsLoading] = useState(false);
  const [tidasImportOpen, setTidasImportOpen] = useState(false);
  const [tidasImportKind, setTidasImportKind] = useState<TidasImportKind>("models");
  const [tidasExportOpen, setTidasExportOpen] = useState(false);
  const [exportTargetProject, setExportTargetProject] = useState<{ projectId: string; projectName: string; latestVersion: number | null } | null>(null);
  const [exportPreviewBusy, setExportPreviewBusy] = useState(false);
  const [exportPreview, setExportPreview] = useState<TidasExportPreviewResponse | null>(null);
  const [exportBusy, setExportBusy] = useState(false);
  const [importRefreshTick, setImportRefreshTick] = useState(0);
  const [forceProjectRefresh, setForceProjectRefresh] = useState(false);
  const [forceProcessRefresh, setForceProcessRefresh] = useState(false);
  const [forceFlowRefresh, setForceFlowRefresh] = useState(false);
  const [forceStatsRefresh, setForceStatsRefresh] = useState(false);
  const [createFlowDialogOpen, setCreateFlowDialogOpen] = useState(false);
  const [tidasCompatibilityFlow, setTidasCompatibilityFlow] = useState<FlowRow | null>(null);
  const [allocationPropertiesFlow, setAllocationPropertiesFlow] = useState<FlowRow | null>(null);
  const [ef31ImportOpen, setEf31ImportOpen] = useState(false);
  const projectPageSize = 20;
  const processPageSize = 20;
  const flowPageSize = 20;
  const projectSearchDebounced = useDebouncedValue(projectSearch, 300);
  const processSearchDebounced = useDebouncedValue(processSearch, 300);
  const flowSearchDebounced = useDebouncedValue(flowSearch, 300);

  const rows = serverProjectRows;
  const processRows = serverProcessRows;
  const flowRows = serverFlowRows;
  const openTidasImport = (kind: TidasImportKind) => {
    setTidasImportKind(kind);
    setTidasImportOpen(true);
  };
  const handleTidasImported = async (kind: TidasImportKind, result: TidasImportResult) => {
    if (kind === "processes") {
      clearPmCacheByPrefix("pm:processes:");
      clearPmCacheByPrefix("pm:flows:");
      clearPmCacheByPrefix("pm:stats");
      setForceProcessRefresh(true);
      setForceFlowRefresh(true);
      setForceStatsRefresh(true);
    } else if (kind === "flows") {
      clearPmCacheByPrefix("pm:flows:");
      clearPmCacheByPrefix("pm:stats");
      setForceFlowRefresh(true);
      setForceStatsRefresh(true);
    } else if (kind === "models") {
      clearPmCacheByPrefix("pm:projects:");
      clearPmCacheByPrefix("pm:stats");
      setForceProjectRefresh(true);
      setForceStatsRefresh(true);
    }
    setImportRefreshTick((prev) => prev + 1);
    if (kind !== "models") {
      return;
    }
    const createdProject = result.createdProjects[0];
    if (createdProject?.projectId) {
      onOpenProject(createdProject.projectId, createdProject.name);
    }
  };

  const openProjectEdit = (row: ProjectRow) => {
    setEditingProject(row);
    setModalOpen(true);
  };

  const closeProjectModal = () => {
    setModalOpen(false);
    setEditingProject(null);
  };

  const updateProjectMetadata = async (projectId: string, form: CreateProjectForm) => {
    setProjectEditBusy(true);
    try {
      const resp = await fetch(`${API_BASE}/projects/${encodeURIComponent(projectId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: form.projectName.trim(),
          source_policy: form.sourcePolicy,
          reference_product: form.referenceProduct.trim() || null,
          functional_unit: form.functionalUnit.trim() || null,
          system_boundary: form.systemBoundary.trim() || null,
          time_representativeness: form.timeRepresentativeness.trim() || null,
          geography: normalizeTidasLocationValue(form.geography) || null,
          description: form.description.trim() || null,
        }),
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { detail?: { message?: string } | string; message?: string };
        const detail = typeof err.detail === "string" ? err.detail : err.detail?.message;
        throw new Error(detail ?? err.message ?? `HTTP ${resp.status}`);
      }
      clearPmCacheByPrefix("pm:projects:");
      clearPmCacheByPrefix("pm:stats");
      setForceProjectRefresh(true);
      setForceStatsRefresh(true);
      setImportRefreshTick((prev) => prev + 1);
      onStatus?.(zh ? "项目信息已保存" : "Project metadata saved");
    } finally {
      setProjectEditBusy(false);
    }
  };

  const openTidasExport = async (projectId: string, projectName: string, latestVersion: number | null) => {
    setExportTargetProject({ projectId, projectName, latestVersion });
    setExportPreview(null);
    setExportPreviewBusy(true);
    setTidasExportOpen(true);
    try {
      const resp = await fetch(`${API_BASE}/export/tidas/bundle/readiness`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          project_id: projectId,
          version: latestVersion ?? undefined,
        }),
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { detail?: { message?: string }; message?: string };
        throw new Error(err.detail?.message ?? err.message ?? `HTTP ${resp.status}`);
      }
      const payload = (await resp.json()) as TidasExportPreviewResponse;
      setExportPreview({
        ...payload,
        errors: (payload.blocking ?? []).map((item) => item.message ?? item.code ?? JSON.stringify(item)),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : zh ? "预览失败" : "Preview failed";
      onStatus?.(zh ? `TIDAS 导出预览失败: ${message}` : `TIDAS export preview failed: ${message}`);
      setExportPreview({
        can_export: false,
        flow_count: 0,
        process_count: 0,
        exported_model_count: 0,
        multi_product_process_count: 0,
        allocation_warnings: [],
        manual_allocation_required_processes: [],
        reference_flow_by_process: {},
        warnings: [],
        errors: [message],
        missing_flows: [],
        missing_processes: [],
        blocking: [{ code: "preview_failed", message }],
      });
    } finally {
      setExportPreviewBusy(false);
    }
  };

  const runTidasExport = async () => {
    if (!exportTargetProject) return;
    setExportBusy(true);
    try {
      const resp = await fetch(`${API_BASE}/export/tidas/bundle`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          project_id: exportTargetProject.projectId,
          version: exportTargetProject.latestVersion ?? undefined,
          display_lang: uiLanguage,
        }),
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { detail?: { message?: string }; message?: string };
        throw new Error(err.detail?.message ?? err.message ?? `HTTP ${resp.status}`);
      }
      // Get filename from Content-Disposition header
      const contentDisposition = resp.headers.get("Content-Disposition");
      let filename = `nebula-tidas-${exportTargetProject.projectId}.zip`;
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename\*?=["']?(?:UTF-8'')?([^"';\n]+)/i);
        if (filenameMatch && filenameMatch[1]) {
          filename = decodeURIComponent(filenameMatch[1]);
        }
      }
      const blob = await resp.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      onStatus?.(zh ? "已导出 TIDAS/ILCD 风格 Bundle" : "Exported TIDAS/ILCD-style Bundle");
      setTidasExportOpen(false);
      setExportTargetProject(null);
      setExportPreview(null);
    } catch (error) {
      const message = error instanceof Error ? error.message : zh ? "导出失败" : "Export failed";
      onStatus?.(zh ? `TIDAS 导出失败: ${message}` : `TIDAS export failed: ${message}`);
    } finally {
      setExportBusy(false);
    }
  };

  const refreshFlowAfterTidasCompatibilitySave = (flow: FlowRow) => {
    clearPmCacheByPrefix("pm:flows:");
    setForceFlowRefresh(true);
    setServerFlowRows((prev) => prev.map((item) => (item.id === flow.id ? { ...item, ...flow } : item)));
  };

  const openTidasCompatibilityForFlowId = async (flowUuid: string) => {
    const existing = serverFlowRows.find((item) => item.id === flowUuid);
    if (existing) {
      setTidasCompatibilityFlow(existing);
      return;
    }
    try {
      const resp = await fetch(`${API_BASE}/reference/flows/${encodeURIComponent(flowUuid)}`, { cache: "no-store" });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      const payload = await resp.json() as {
        flow_uuid: string;
        flow_name: string;
        flow_name_en?: string | null;
        flow_type?: string;
        default_unit?: string;
        unit_group?: string;
        compartment?: string | null;
        source?: string | null;
        is_custom?: boolean;
        tidas_compatible?: boolean;
        tidas_unit_group?: string | null;
        tidas_flow_property_uuid?: string | null;
        tidas_reference_source?: string | null;
      };
      setTidasCompatibilityFlow({
        id: payload.flow_uuid,
        flowName: payload.flow_name,
        flowNameEn: String(payload.flow_name_en ?? "").trim() || undefined,
        type: normalizeFlowType(payload.flow_type),
        unit: String(payload.default_unit ?? "-"),
        category: String(payload.compartment ?? "-"),
        source: String(payload.source ?? ""),
        isCustom: Boolean(payload.is_custom),
        tidasCompatible: Boolean(payload.tidas_compatible),
        tidasUnitGroup: payload.tidas_unit_group ?? null,
        tidasFlowPropertyUuid: payload.tidas_flow_property_uuid ?? null,
        tidasReferenceSource: payload.tidas_reference_source ?? null,
        usedInProcesses: 0,
        lastModified: "-",
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "flow load failed";
      onStatus?.(zh ? `Flow 补录入口打开失败：${message}` : `Failed to open flow repair: ${message}`);
    }
  };

  const repairTidasIssue = (issue: string | TidasExportWarning) => {
    if (!exportTargetProject) {
      return;
    }
    const details = typeof issue === "string" ? {} : issue.details ?? issue.context ?? {};
    const code = typeof issue === "string" ? "" : String(issue.code ?? "");
    const target = String(details.repair_target ?? "");
    const processUuid = String(details.process_uuid ?? details.node_id ?? "");
    const flowUuid = String(details.flow_uuid ?? "");
    setTidasExportOpen(false);
    if (target === "project_settings") {
      const row = rows.find((item) => item.projectId === exportTargetProject.projectId);
      if (row) {
        openProjectEdit(row);
      }
      onStatus?.(zh ? "请补全项目设置后重新运行 readiness。" : "Complete project settings, then re-run readiness.");
      return;
    }
    if (flowUuid && code.startsWith("custom_flow_")) {
      void openTidasCompatibilityForFlowId(flowUuid);
      onStatus?.(zh ? `请补录 Flow ${flowUuid} 的 TIDAS 兼容信息。` : `Complete TIDAS compatibility for flow ${flowUuid}.`);
      return;
    }
    const repairTarget: TidasRepairTarget = {
      ...details,
      desired_inspector_tab: target === "allocation" ? "external_out" : undefined,
      desired_process_modal: target === "process_metadata" ? "metadata" : undefined,
    };
    onOpenProject(exportTargetProject.projectId, exportTargetProject.projectName, repairTarget);
    if (target === "process_metadata") {
      onStatus?.(zh ? `已打开项目，请补录过程 ${processUuid || "-"} 的过程信息。` : `Project opened. Complete process metadata for ${processUuid || "-"}.`);
    } else if (target === "allocation" || processUuid) {
      onStatus?.(zh ? `已打开项目，请在过程 ${processUuid || "-"} 的输出面板补充分配。` : `Project opened. Edit allocation in process ${processUuid || "-"}.`);
    } else if (flowUuid) {
      onStatus?.(zh ? `已打开项目，请替换或补录 Flow ${flowUuid}。` : `Project opened. Replace or complete flow ${flowUuid}.`);
    } else if (target === "unit_group") {
      onStatus?.(zh ? "已打开项目，请检查不受支持的单位组。" : "Project opened. Check the unsupported unit group.");
    } else {
      onStatus?.(zh ? "已打开项目，请根据 readiness 问题修复模型。" : "Project opened. Fix the model according to readiness issues.");
    }
  };

  useEffect(() => {
    if (activeModule !== "project" || activeItem !== "recent_projects") {
      return;
    }
    let cancelled = false;
    const timer = window.setTimeout(() => {
      const loadStats = async () => {
        const cacheKey = "pm:stats";
        const cached = forceStatsRefresh ? null : readPmCache<StatsResponse>(cacheKey) ?? readPmCacheStale<StatsResponse>(cacheKey);
        if (cached && !cancelled) {
          setStats(cached);
        }
        setStatsLoading(!cached);
        try {
          const resp = await fetch(`${API_BASE}/stats`);
          if (!resp.ok || cancelled) {
            if (!cancelled) setStatsLoading(false);
            return;
          }
          const payload = (await resp.json()) as StatsResponse;
          const nextStats = {
            projects: Number(payload.projects ?? 0),
            processes: Number(payload.processes ?? 0),
            flows: Number(payload.flows_library_total ?? payload.flows ?? 0),
            flows_latest_graph: Number(payload.flows_latest_graph ?? 0),
            flows_library_total: Number(payload.flows_library_total ?? payload.flows ?? 0),
          };
          if (!cancelled) {
            setStats(nextStats);
            writePmCache(cacheKey, nextStats);
            setStatsLoading(false);
            setForceStatsRefresh(false);
          }
        } catch {
          if (!cancelled) {
            setStatsLoading(false);
          }
          // keep fallback
        }
      };
      void loadStats();
    }, 350);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [activeModule, activeItem, projects.length, importRefreshTick, forceStatsRefresh]);

  const renderStatValue = (value: number | undefined) => {
    if (statsLoading && stats == null) {
      return (
        <span className="pm-stat-loading">
          <span className="pm-spin" />
          {zh ? "加载中" : "Loading"}
        </span>
      );
    }
    if (typeof value !== "number" || Number.isNaN(value)) {
      return "-";
    }
    return String(value);
  };

  useEffect(() => {
    const loadProjects = async () => {
      try {
        const recentMode = activeItem === "recent_projects";
        const params = new URLSearchParams(
          recentMode
            ? { recent: "true", limit: "9", page: "1", page_size: "9" }
            : { search: projectSearchDebounced.trim(), page: String(projectPage), page_size: String(projectPageSize) },
        );
        const cacheKey = `pm:projects:${params.toString()}:zh=${zh ? "1" : "0"}`;
        const cachedEntry = forceProjectRefresh ? null : readPmCacheEntry<{ rows: ProjectRow[]; total: number }>(cacheKey);
        if (cachedEntry?.data) {
          setServerProjectRows(cachedEntry.data.rows);
          setProjectTotal(cachedEntry.data.total);
        }
        const resp = await fetch(`${API_BASE}/projects?${params.toString()}`, {
          headers: cachedEntry?.etag ? { "If-None-Match": cachedEntry.etag } : undefined,
        });
        if (resp.status === 304 && cachedEntry?.data) {
          return;
        }
        if (!resp.ok) {
          throw new Error("projects load failed");
        }
        const payload = (await resp.json()) as PagedResponse<ProjectApiItem> | ProjectApiItem[];
        const items = Array.isArray(payload) ? payload : payload.items ?? [];
        const total = Array.isArray(payload) ? payload.length : Number(payload.total ?? items.length);
        const mapped: ProjectRow[] = items.map((item) => ({
          projectId: item.project_id,
          projectName: item.name,
          sourcePolicy: normalizeSourcePolicy(item.source_policy),
          referenceProduct: String(item.reference_product ?? ""),
          functionalUnit: String(item.functional_unit ?? ""),
          systemBoundary: mapSystemBoundary(item.system_boundary, zh),
          timeRepresentativeness: String(item.time_representativeness ?? ""),
          geography: String(item.geography ?? ""),
          description: String(item.description ?? ""),
          processCount: Number(item.process_count ?? 0),
          flowCount: Number(item.flow_count ?? 0),
          lastModified: formatTime(item.latest_version_created_at ?? item.created_at),
          status: mapProjectStatus(item.status, zh),
          latestVersion: item.latest_version ?? null,
        }));
        setServerProjectRows(mapped);
        setProjectTotal(total);
        writePmCache(cacheKey, { rows: mapped, total }, resp.headers.get("ETag") ?? undefined);
        setForceProjectRefresh(false);
      } catch {
        const fallback = toProjectRows(projects);
        const keyword = projectSearchDebounced.trim().toLowerCase();
        const filtered = !keyword
          ? fallback
          : fallback.filter(
            (row) =>
              row.projectName.toLowerCase().includes(keyword) || row.projectId.toLowerCase().includes(keyword),
          );
        const start = (projectPage - 1) * projectPageSize;
        setServerProjectRows(filtered.slice(start, start + projectPageSize));
        setProjectTotal(filtered.length);
      }
    };
    void loadProjects();
  }, [activeItem, projectSearchDebounced, projectPage, projects, zh, importRefreshTick, forceProjectRefresh]);

  useEffect(() => {
    const loadProcesses = async () => {
      try {
        const params = new URLSearchParams({
          search: processSearchDebounced.trim(),
          page: String(processPage),
          page_size: String(processPageSize),
          include_legacy: "false",
        });
        const cacheKey = `pm:processes:${params.toString()}`;
        const cachedEntry = forceProcessRefresh ? null : readPmCacheEntry<{ rows: ProcessRow[]; total: number }>(cacheKey);
        if (cachedEntry?.data) {
          setServerProcessRows(cachedEntry.data.rows);
          setProcessTotal(cachedEntry.data.total);
        }
        const resp = await fetch(`${API_BASE}/processes?${params.toString()}`, {
          headers: cachedEntry?.etag ? { "If-None-Match": cachedEntry.etag } : undefined,
        });
        if (resp.status === 304 && cachedEntry?.data) {
          return;
        }
        if (!resp.ok) {
          throw new Error("processes load failed");
        }
        const payload = (await resp.json()) as PagedResponse<ProcessApiItem> | ProcessApiItem[];
        const items = Array.isArray(payload) ? payload : payload.items ?? [];
        const total = Array.isArray(payload) ? items.length : Number(payload.total ?? items.length);
        const mappedRows = items.map((item) => ({
          processId: String(item.process_uuid ?? ""),
          processUuid: String(item.process_uuid ?? ""),
          processName: String(item.process_name ?? "未命名过程"),
          processNameEn: String(item.process_name_en ?? "").trim() || undefined,
          type: (item.type as "unit_process" | "market_process") ?? "unit_process",
          referenceFlowUuid: String(item.reference_flow_uuid ?? ""),
          referenceFlowName: String(item.reference_flow_name ?? ""),
          referenceFlowInternalId: String(item.reference_flow_internal_id ?? "").trim() || undefined,
          inputs: Number(item.input_count ?? 0),
          outputs: Number(item.output_count ?? 0),
          usedInProjects: Number(item.used_in_projects ?? 0),
          balanceStatus: (item.balance_status as "balanced" | "unchecked" | "error") ?? "unchecked",
          lastModified: formatTime(String(item.last_modified ?? "")),
        }));
        setServerProcessRows(mappedRows);
        setProcessTotal(total);
        writePmCache(cacheKey, { rows: mappedRows, total }, resp.headers.get("ETag") ?? undefined);
        setForceProcessRefresh(false);
      } catch {
        setServerProcessRows([]);
        setProcessTotal(0);
      }
    };
    if (activeModule === "process") {
      void loadProcesses();
    }
  }, [activeModule, processSearchDebounced, processPage, importRefreshTick, forceProcessRefresh]);

  useEffect(() => {
    const loadFlows = async () => {
      try {
        type FlowApiRow = {
          flow_id?: string;
          flow_name?: string;
          flow_name_en?: string | null;
          type?: "intermediate_flow" | "elementary_flow" | "product_flow" | "waste_flow";
          unit?: string;
          category?: string;
          source?: string | null;
          is_custom?: boolean;
          tidas_compatible?: boolean;
          tidas_unit_group?: string | null;
          tidas_flow_property_uuid?: string | null;
          tidas_reference_source?: string | null;
          allocation_properties?: FlowAllocationProperty[];
          used_in_processes?: number;
          last_modified?: string;
        };
        const params = new URLSearchParams({
          search: flowSearchDebounced.trim(),
          page: String(flowPage),
          page_size: String(flowPageSize),
        });
        // Backend-side filtering contract (required for large datasets).
        if (flowTypeFilter === "basic") {
          params.set("type", "elementary_flow");
        } else if (flowTypeFilter === "product") {
          params.set("type", "product_flow");
        } else if (flowTypeFilter === "waste") {
          params.set("type", "waste_flow");
        }
        if (flowCategoryFilter.trim()) {
          params.set("category_level_1", flowCategoryFilter.trim());
        }
        const cacheKey = `pm:flows:${params.toString()}:zh=${zh ? "1" : "0"}`;
        const cachedEntry = forceFlowRefresh ? null : readPmCacheEntry<{ rows: FlowRow[]; total: number }>(cacheKey);
        if (cachedEntry?.data) {
          setServerFlowRows(cachedEntry.data.rows);
          setFlowTotal(cachedEntry.data.total);
        }
        const resp = await fetch(`${API_BASE}/flows?${params.toString()}`, {
          headers: cachedEntry?.etag ? { "If-None-Match": cachedEntry.etag } : undefined,
        });
        if (resp.status === 304 && cachedEntry?.data) {
          return;
        }
        if (!resp.ok) {
          throw new Error("flows load failed");
        }
        const payload = (await resp.json()) as PagedResponse<FlowApiRow> | FlowApiRow[];
        const items = Array.isArray(payload) ? payload : payload.items ?? [];
        const total = Array.isArray(payload) ? items.length : Number(payload.total ?? items.length);
        const mappedRows = items.map((item) => ({
          id: String(item.flow_id ?? ""),
          flowName: String(item.flow_name ?? "未命名流"),
          flowNameEn: String(item.flow_name_en ?? "").trim() || undefined,
          type: (item.type as "intermediate_flow" | "elementary_flow" | "product_flow" | "waste_flow") ?? "intermediate_flow",
          unit: String(item.unit ?? "-"),
          category: String(item.category ?? "-"),
          source: String(item.source ?? ""),
          isCustom: Boolean(item.is_custom),
          tidasCompatible: Boolean(item.tidas_compatible),
          tidasUnitGroup: item.tidas_unit_group ?? null,
          tidasFlowPropertyUuid: item.tidas_flow_property_uuid ?? null,
          tidasReferenceSource: item.tidas_reference_source ?? null,
          allocationProperties: Array.isArray(item.allocation_properties) ? item.allocation_properties : [],
          usedInProcesses: Number(item.used_in_processes ?? 0),
          lastModified: formatTime(String(item.last_modified ?? "")),
        }));
        setServerFlowRows(mappedRows);
        setFlowTotal(total);
        writePmCache(cacheKey, { rows: mappedRows, total }, resp.headers.get("ETag") ?? undefined);
        setForceFlowRefresh(false);
      } catch {
        setServerFlowRows([]);
        setFlowTotal(0);
      }
    };
    if (activeModule === "flow") {
      void loadFlows();
    }
  }, [activeModule, flowSearchDebounced, flowCategoryFilter, flowPage, flowTypeFilter, importRefreshTick, forceFlowRefresh]);

  useEffect(() => {
    const loadFlowCategories = async () => {
      if (activeModule !== "flow") {
        return;
      }
      try {
        const params = new URLSearchParams();
        params.set("level", "1");
        if (flowTypeFilter === "basic") {
          params.set("type", "elementary_flow");
        } else if (flowTypeFilter === "product") {
          params.set("type", "product_flow");
        } else if (flowTypeFilter === "waste") {
          params.set("type", "waste_flow");
        }
        const resp = await fetch(`${API_BASE}/flows/categories?${params.toString()}`);
        if (!resp.ok) {
          throw new Error("flow categories load failed");
        }
        const payload = (await resp.json()) as { items?: FlowCategoryItem[] } | FlowCategoryItem[];
        const items = Array.isArray(payload) ? payload : payload.items ?? [];
        setFlowCategories(items.filter((x) => String(x.category ?? "").trim().length > 0));
      } catch {
        setFlowCategories([]);
      }
    };
    void loadFlowCategories();
  }, [activeModule, flowTypeFilter]);

  const handleViewProcess = async (row: ProcessRow) => {
    setSelectedProcess(row);
    setProcessDetailOpen(true);
    setProcessDetailBusy(true);
    setProcessDetailError("");
    setProcessDetail(null);
    setProcessFlowRefs(new Map());
    try {
      const resp = await fetch(`${API_BASE}/processes/${encodeURIComponent(row.processUuid || row.processId)}`);
      if (!resp.ok) {
        throw new Error(`detail api ${resp.status}`);
      }
      const detail = (await resp.json()) as ProcessDetailResponse;
      setProcessDetail(detail);

      const src = (detail.process_json && typeof detail.process_json === "object"
        ? detail.process_json
        : detail) as Record<string, unknown>;
      const ports = Array.isArray(src.exchanges)
        ? (src.exchanges as Array<Record<string, unknown>>)
        : [
          ...(Array.isArray(src.inputs) ? (src.inputs as Array<Record<string, unknown>>) : []),
          ...(Array.isArray(src.outputs) ? (src.outputs as Array<Record<string, unknown>>) : []),
        ];
      const uuids = Array.from(
        new Set(
          ports
            .map((p) => String(p.flow_uuid ?? p.flowUuid ?? ""))
            .filter((v) => v.length > 0),
        ),
      );
      if (uuids.length > 0) {
        const entries: Array<[string, FlowRefInfo]> = await Promise.all(
          uuids.map(async (uuid): Promise<[string, FlowRefInfo]> => {
            try {
              const flowResp = await fetch(`${API_BASE}/reference/flows/${encodeURIComponent(uuid)}`);
              if (flowResp.ok) {
                const payload = (await flowResp.json()) as FlowRefInfo;
                return [uuid, { ...payload, uuid, __status: "ok" }];
              }
              // Fallback: resolve from /api/flows list API by exact uuid match.
              const listResp = await fetch(
                `${API_BASE}/flows?search=${encodeURIComponent(uuid)}&page=1&page_size=5`,
              );
              if (listResp.ok) {
                const listPayload = (await listResp.json()) as
                  | PagedResponse<{
                    flow_id?: string;
                    flow_name?: string;
                    type?: string;
                    unit?: string;
                  }>
                  | Array<Record<string, unknown>>;
                const listItems = Array.isArray(listPayload) ? listPayload : listPayload.items ?? [];
                const matched = listItems.find((x) => String((x as { flow_id?: string }).flow_id ?? "") === uuid);
                if (matched) {
                  return [
                    uuid,
                    {
                      uuid,
                      flow_name: String((matched as { flow_name?: string }).flow_name ?? ""),
                      flow_type: String((matched as { type?: string }).type ?? ""),
                      unit: String((matched as { unit?: string }).unit ?? ""),
                      __status: "ok",
                    },
                  ];
                }
              }
              return [uuid, { uuid, __status: "missing_uuid" }];
            } catch {
              return [uuid, { uuid, __status: "missing_uuid" }];
            }
          }),
        );
        setProcessFlowRefs(new Map(entries));
      }
    } catch {
      setProcessDetailError(
        zh
          ? "过程详情接口不可用，当前仅展示列表基础字段。若需完整字段，请后端补充 GET /api/processes/{process_uuid}。"
          : "Process detail API unavailable. Showing list fields only.",
      );
    } finally {
      setProcessDetailBusy(false);
    }
  };

  const renderPagination = (page: number, total: number, pageSize: number, setPage: (next: number) => void) => {
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    return (
      <div className="pm-pagination">
        <span className="pm-pagination-meta">
          {zh ? `第 ${page} / ${totalPages} 页，共 ${total} 条` : `Page ${page} / ${totalPages}, ${total} items`}
        </span>
        <div className="pm-pagination-actions">
          <button type="button" className="pm-ghost-btn" disabled={page <= 1} onClick={() => setPage(1)}>
            {zh ? "首页" : "First"}
          </button>
          <button type="button" className="pm-ghost-btn" disabled={page <= 1} onClick={() => setPage(page - 1)}>
            {zh ? "上一页" : "Prev"}
          </button>
          <button
            type="button"
            className="pm-ghost-btn"
            disabled={page >= totalPages}
            onClick={() => setPage(page + 1)}
          >
            {zh ? "下一页" : "Next"}
          </button>
          <button
            type="button"
            className="pm-ghost-btn"
            disabled={page >= totalPages}
            onClick={() => setPage(totalPages)}
          >
            {zh ? "末页" : "Last"}
          </button>
        </div>
      </div>
    );
  };

  return (
    <div className="pm-layout">
      <header className="pm-topbar">
        <div className="pm-logo">{zh ? "星云 LCA" : "Nebula LCA"}</div>
        {onChangeLanguage && (
          <div className="lang-switch">
            <button
              type="button"
              className={uiLanguage === "zh" ? "active" : ""}
              onClick={() => onChangeLanguage("zh")}
              title="中文"
            >
              中
            </button>
            <button
              type="button"
              className={uiLanguage === "en" ? "active" : ""}
              onClick={() => onChangeLanguage("en")}
              title="English"
            >
              EN
            </button>
          </div>
        )}
      </header>

      <div className="pm-body">
        <aside className="pm-sidebar">
          <div className="pm-sidebar-section">
            <div className="pm-sidebar-title">{zh ? "项目" : "Project"}</div>
            <button
              type="button"
              className={`pm-nav-item ${activeItem === "recent_projects" ? "active" : ""}`}
              onClick={() => {
                setActiveModule("project");
                setActiveItem("recent_projects");
                setProjectPage(1);
              }}
            >
              {zh ? "最近" : "Recent"}
            </button>
            <button
              type="button"
              className={`pm-nav-item ${activeItem === "all_projects" ? "active" : ""}`}
              onClick={() => {
                setActiveModule("project");
                setActiveItem("all_projects");
                setProjectPage(1);
              }}
            >
              {zh ? "全部项目" : "All Projects"}
            </button>
          </div>

          <div className="pm-sidebar-section">
            <div className="pm-sidebar-title">{zh ? "过程" : "Process"}</div>
            <button
              type="button"
              className={`pm-nav-item ${activeItem === "all_processes" ? "active" : ""}`}
              onClick={() => {
                setActiveModule("process");
                setActiveItem("all_processes");
                setProcessPage(1);
              }}
            >
              {zh ? "全部过程" : "All Processes"}
            </button>
          </div>

          <div className="pm-sidebar-section">
            <div className="pm-sidebar-title">{zh ? "流" : "Flow"}</div>
            <button
              type="button"
              className={`pm-nav-item ${activeItem === "all_flows" ? "active" : ""}`}
              onClick={() => {
                setActiveModule("flow");
                setActiveItem("all_flows");
                setFlowPage(1);
              }}
            >
              {zh ? "全部流" : "All Flows"}
            </button>
          </div>

          <div className="pm-sidebar-section">
            <div className="pm-sidebar-title">{zh ? "数据库导入" : "Database Import"}</div>
            <button
              type="button"
              className="pm-nav-item"
              onClick={() => setEf31ImportOpen(true)}
            >
              {zh ? "LCI 数据库" : "LCI Database"}
            </button>
          </div>
        </aside>

        <main className="pm-main">
          {activeModule === "project" && (
            <section className="pm-page">
              <div className="pm-page-head">
                <div>
                  <h2>{zh ? "LCA 项目" : "LCA Projects"}</h2>
                  <p>{zh ? "管理和维护你的生命周期评价模型项目" : "Manage and maintain your LCA modeling projects."}</p>
                </div>
                <div className="pm-head-actions">
                  <button
                    type="button"
                    className="pm-ghost-btn"
                    title={zh ? "导入天工模型格式" : "Import TIDAS model format"}
                    onClick={() => openTidasImport("models")}
                  >
                    {zh ? "导入" : "Import"}
                  </button>
                  <button
                    type="button"
                    className="pm-primary-btn"
                    title={zh ? "新建项目" : "Create project"}
                    onClick={() => setModalOpen(true)}
                  >
                    {zh ? "新建" : "Create"}
                  </button>
                </div>
              </div>
              {activeItem === "recent_projects" ? (
                <>
                  <div className="pm-stats compact">
                    <div className="pm-stat-card">
                      <div className="pm-stat-label">{zh ? "项目数" : "Projects"}</div>
                      <div className="pm-stat-value">{renderStatValue(stats?.projects)}</div>
                    </div>
                    <div className="pm-stat-card">
                      <div className="pm-stat-label">{zh ? "过程数" : "Processes"}</div>
                      <div className="pm-stat-value">{renderStatValue(stats?.processes)}</div>
                    </div>
                    <div className="pm-stat-card">
                      <div className="pm-stat-label">{zh ? "流数量" : "Flows"}</div>
                      <div className="pm-stat-value">{renderStatValue(stats?.flows)}</div>
                    </div>
                  </div>
                  <div className="pm-recent-head">
                    <h3>{zh ? "最近项目" : "Recent Projects"}</h3>
                    <button
                      type="button"
                      className="pm-link-btn primary"
                      onClick={() => {
                        setActiveItem("all_projects");
                        setProjectPage(1);
                      }}
                    >
                      {zh ? "查看全部" : "View All"}
                    </button>
                  </div>
                  <div className="pm-recent-grid">
                    {rows.map((row) => (
                      <article key={row.projectId} className="pm-project-card">
                        <div className="pm-project-card-top">
                          <div className="pm-project-card-title">{row.projectName}</div>
                          <button type="button" className="pm-project-info-btn" onClick={() => openProjectEdit(row)}>
                            {zh ? "修改项目信息" : "Edit Info"}
                          </button>
                        </div>
                        <div className="pm-project-card-meta">{zh ? "参考产品：" : "Reference: "}{row.referenceProduct}</div>
                        <div className="pm-project-card-meta">{zh ? "系统边界：" : "Boundary: "}{row.systemBoundary}</div>
                        <div className="pm-project-card-meta">{zh ? "最近修改：" : "Updated: "}{row.lastModified}</div>
                        <div className="pm-project-card-actions">
                          <button type="button" className="pm-link-btn primary" onClick={() => onOpenProject(row.projectId)}>
                            {zh ? "打开" : "Open"}
                          </button>
                          <button type="button" className="pm-link-btn" onClick={() => void openTidasExport(row.projectId, row.projectName, row.latestVersion)} disabled={!row.latestVersion}>
                            {zh ? "导出" : "Export"}
                          </button>
                          <button type="button" className="pm-link-btn danger" onClick={() => onDeleteProject(row.projectId)}>
                            {zh ? "删除" : "Delete"}
                          </button>
                        </div>
                      </article>
                    ))}
                    {rows.length === 0 && <div className="pm-empty-note">{zh ? "暂无最近项目。" : "No recent projects."}</div>}
                  </div>
                </>
              ) : (
                <>
                  <div className="pm-head-actions pm-head-tools">
                    <input
                      className="pm-page-search"
                      placeholder={zh ? "搜索项目名称、ID或元字段" : "Search project name, id or metadata"}
                      value={projectSearch}
                      onChange={(event) => {
                        setProjectSearch(event.target.value);
                        setProjectPage(1);
                      }}
                    />
                  </div>
                  <div className="pm-table-wrap">
                    <table className="pm-table">
                      <thead>
                        <tr>
                          <th>{zh ? "项目名称" : "Project Name"}</th>
                          <th>{zh ? "参考产品" : "Reference Product"}</th>
                          <th>{zh ? "系统边界" : "System Boundary"}</th>
                          <th>{zh ? "时间代表性" : "Time Representativeness"}</th>
                          <th>{zh ? "地理代表性" : "Geography"}</th>
                          <th>{zh ? "过程数" : "Process Count"}</th>
                          <th>{zh ? "流数量" : "Flow Count"}</th>
                          <th>{zh ? "最近修改" : "Last Modified"}</th>
                          <th>{zh ? "状态" : "Status"}</th>
                          <th>{zh ? "操作" : "Actions"}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((row) => (
                          <tr key={row.projectId}>
                            <td>{row.projectName}</td>
                            <td>{row.referenceProduct}</td>
                            <td>{row.systemBoundary}</td>
                            <td>{row.timeRepresentativeness}</td>
                            <td>{row.geography}</td>
                            <td>{row.processCount}</td>
                            <td>{row.flowCount}</td>
                            <td>{row.lastModified}</td>
                            <td>{row.status}</td>
                            <td>
                              <div className="pm-row-actions">
                                <button type="button" className="pm-link-btn primary" onClick={() => onOpenProject(row.projectId)}>
                                  {zh ? "打开" : "Open"}
                                </button>
                                <button type="button" className="pm-link-btn" onClick={() => void openTidasExport(row.projectId, row.projectName, row.latestVersion)} disabled={!row.latestVersion}>
                                  {zh ? "导出" : "Export"}
                                </button>
                                <button type="button" className="pm-link-btn" onClick={() => openProjectEdit(row)}>{zh ? "修改信息" : "Edit Info"}</button>
                                <button type="button" className="pm-link-btn">{zh ? "复制" : "Duplicate"}</button>
                                <button type="button" className="pm-link-btn danger" onClick={() => onDeleteProject(row.projectId)}>
                                  {zh ? "删除" : "Delete"}
                                </button>
                              </div>
                            </td>
                          </tr>
                        ))}
                        {rows.length === 0 && (
                          <tr>
                            <td colSpan={10}>{zh ? "暂无项目。" : "No projects found."}</td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                  {renderPagination(projectPage, projectTotal, projectPageSize, setProjectPage)}
                </>
              )}
            </section>
          )}

          {activeModule === "process" && (
            <section className="pm-page">
              <div className="pm-page-head">
                <div>
                  <h2>{zh ? "过程" : "Processes"}</h2>
                </div>
                <div className="pm-head-actions pm-head-buttons">
                  <button
                    type="button"
                    className="pm-ghost-btn"
                    title={zh ? "导入天工格式过程" : "Import TIDAS process format"}
                    onClick={() => openTidasImport("processes")}
                  >
                    {zh ? "导入" : "Import"}
                  </button>
                  <button
                    type="button"
                    className="pm-primary-btn"
                    title={zh ? "新建过程" : "Create process"}
                    onClick={() => (onCreateProcess ? onCreateProcess() : undefined)}
                  >
                    {zh ? "新建" : "Create"}
                  </button>
                </div>
              </div>
              <div className="pm-head-tools pm-head-tools--process">
                <input
                  className="pm-page-search"
                  placeholder={zh ? "搜索过程名称" : "Search process name"}
                  value={processSearch}
                  onChange={(event) => {
                    setProcessSearch(event.target.value);
                    setProcessPage(1);
                  }}
                />
              </div>
              <div className="pm-table-wrap">
                <table className="pm-table">
                  <thead>
                    <tr>
                      <th className="pm-process-name-col">{zh ? "过程名称" : "Process Name"}</th>
                      <th>{zh ? "输入流数量" : "Input Flows"}</th>
                      <th>{zh ? "输出流数量" : "Output Flows"}</th>
                      <th>{zh ? "最近修改" : "Last Modified"}</th>
                      <th>{zh ? "操作" : "Actions"}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {processRows.map((row) => (
                      <tr key={row.processId || `${row.processName}-${row.lastModified}`}>
                        <td className="pm-cell-ellipsis pm-process-name-col" title={getDisplayProcessName(row, uiLanguage)}>
                          {getDisplayProcessName(row, uiLanguage)}
                        </td>
                        <td>{row.inputs}</td>
                        <td>{row.outputs}</td>
                        <td>{row.lastModified}</td>
                        <td>
                          <div className="pm-row-actions">
                            <button type="button" className="pm-link-btn primary" onClick={() => void handleViewProcess(row)}>
                              {zh ? "查看" : "View"}
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {processRows.length === 0 && (
                      <tr>
                        <td colSpan={5}>{zh ? "暂无过程。" : "No processes found."}</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
              {renderPagination(processPage, processTotal, processPageSize, setProcessPage)}
            </section>
          )}

          {activeModule === "flow" && (
            <section className="pm-page">
              <div className="pm-page-head">
                <div>
                  <h2>{zh ? "流" : "Flows"}</h2>
                </div>
                <div className="pm-head-actions pm-head-buttons">
                  <button
                    type="button"
                    className="pm-ghost-btn"
                    title={zh ? "导入天工格式流" : "Import TIDAS flow format"}
                    onClick={() => openTidasImport("flows")}
                  >
                    {zh ? "导入" : "Import"}
                  </button>
                  <button
                    type="button"
                    className="pm-primary-btn"
                    title={zh ? "新建流" : "Create flow"}
                    onClick={() => setCreateFlowDialogOpen(true)}
                  >
                    {zh ? "新建" : "Create"}
                  </button>
                </div>
              </div>
              <div className="pm-head-tools pm-head-tools--flow">
                <input
                  className="pm-page-search"
                  placeholder={zh ? "搜索流名称" : "Search flow name"}
                  value={flowSearch}
                  onChange={(event) => {
                    setFlowSearch(event.target.value);
                    setFlowPage(1);
                  }}
                />
                <select
                  className="pm-page-select"
                  value={flowTypeFilter}
                  onChange={(event) => {
                    setFlowTypeFilter(event.target.value as "all" | FlowBusinessType);
                    setFlowPage(1);
                  }}
                >
                  <option value="all">{zh ? "全部类型" : "All Types"}</option>
                  <option value="basic">{mapFlowTypeLabel("basic", zh)}</option>
                  <option value="product">{mapFlowTypeLabel("product", zh)}</option>
                  <option value="waste">{mapFlowTypeLabel("waste", zh)}</option>
                </select>
                <select
                  className="pm-page-select"
                  value={flowCategoryFilter}
                  onChange={(event) => {
                    setFlowCategoryFilter(event.target.value);
                    setFlowPage(1);
                  }}
                >
                  <option value="">{zh ? "全部一级分类" : "All Level-1 Categories"}</option>
                  {flowCategories.map((item) => (
                    <option key={item.category} value={item.category}>
                      {`${item.category} (${item.count})`}
                    </option>
                  ))}
                </select>
              </div>
              <div className="pm-table-wrap">
                <table className="pm-table">
                  <thead>
                    <tr>
                      <th>{zh ? "流名称" : "Flow Name"}</th>
                      <th>{zh ? "类型" : "Type"}</th>
                      <th>{zh ? "单位" : "Unit"}</th>
                      <th>TIDAS</th>
                      <th>{zh ? "分类" : "Category"}</th>
                      <th>{zh ? "应用过程数" : "Used In Processes"}</th>
                      <th>{zh ? "最近修改" : "Last Modified"}</th>
                      <th>{zh ? "操作" : "Actions"}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {flowRows.map((row) => (
                      <tr key={row.id || `${row.flowName}-${row.lastModified}`}>
                        <td>{getDisplayFlowName(row, uiLanguage)}</td>
                        <td>{mapFlowTypeLabel(inferFlowBusinessType(row.type, row.category, getDisplayFlowName(row, uiLanguage)), zh)}</td>
                        <td>{row.unit}</td>
                        <td>{row.tidasCompatible ? (row.tidasUnitGroup || "OK") : "-"}</td>
                        <td>{row.category}</td>
                        <td>{row.usedInProcesses}</td>
                        <td>{row.lastModified}</td>
                        <td>
                          <div className="pm-row-actions">
                            {row.type !== "elementary_flow" && (
                              <button type="button" className="pm-link-btn" onClick={() => setAllocationPropertiesFlow(row)}>
                                {zh ? "换算" : "Conversion"}
                              </button>
                            )}
                            {row.isCustom && row.type !== "elementary_flow" && (
                              <button type="button" className="pm-link-btn" onClick={() => setTidasCompatibilityFlow(row)}>
                                {zh ? "补录" : "Edit"}
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                    {flowRows.length === 0 && (
                      <tr>
                        <td colSpan={8}>{zh ? "暂无流。" : "No flows found."}</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
              {renderPagination(flowPage, flowTotal, flowPageSize, setFlowPage)}
            </section>
          )}
        </main>
      </div>

      <ProcessDetailModal
        open={processDetailOpen}
        uiLanguage={uiLanguage}
        busy={processDetailBusy}
        process={selectedProcess}
        detail={processDetail}
        flowRefs={processFlowRefs}
        errorText={processDetailError}
        onClose={() => setProcessDetailOpen(false)}
      />
      <TidasImportModal
        open={tidasImportOpen}
        uiLanguage={uiLanguage}
        importKind={tidasImportKind}
        fixedKind
        onClose={() => setTidasImportOpen(false)}
        onStatus={onStatus}
        onImported={(kind, result) => {
          setTidasImportOpen(false);
          void handleTidasImported(kind, result);
        }}
      />
      <TidasExportModal
        open={tidasExportOpen}
        uiLanguage={uiLanguage}
        busy={exportBusy}
        previewBusy={exportPreviewBusy}
        preview={exportPreview}
        targetProject={exportTargetProject}
        onClose={() => {
          setTidasExportOpen(false);
          setExportTargetProject(null);
          setExportPreview(null);
        }}
        onRepair={repairTidasIssue}
        onExport={() => void runTidasExport()}
      />
      <CreateProjectModal
        open={modalOpen}
        busy={busy || projectEditBusy}
        uiLanguage={uiLanguage}
        mode={editingProject ? "edit" : "create"}
        initialForm={editingProject ? {
          projectName: editingProject.projectName,
          sourcePolicy: editingProject.sourcePolicy,
          referenceProduct: editingProject.referenceProduct,
          functionalUnit: editingProject.functionalUnit,
          systemBoundary: editingProject.systemBoundary,
          timeRepresentativeness: editingProject.timeRepresentativeness,
          geography: editingProject.geography,
          description: editingProject.description,
        } : undefined}
        onClose={closeProjectModal}
        onSubmit={async (form) => {
          if (editingProject) {
            await updateProjectMetadata(editingProject.projectId, form);
          } else {
            await onCreateProject(form);
          }
        }}
      />
      <CreateFlowDialog
        open={createFlowDialogOpen}
        uiLanguage={uiLanguage}
        defaultFlowType="product_flow"
        onSuccess={(flow) => {
          // Clear cache and refresh flow list
          clearPmCacheByPrefix("pm:flows:");
          setForceFlowRefresh(true);
          onStatus?.(zh ? `Flow 创建成功：${flow.flow_name}` : `Flow created successfully: ${flow.flow_name}`);
        }}
        onReuse={(flow) => {
          // Reuse existing flow - just refresh the list
          clearPmCacheByPrefix("pm:flows:");
          setForceFlowRefresh(true);
          onStatus?.(zh ? `已复用已有 Flow：${flow.flow_name}` : `Reused existing flow: ${flow.flow_name}`);
        }}
        onClose={() => setCreateFlowDialogOpen(false)}
        onStatus={onStatus}
      />
      <TidasCompatibilityModal
        open={Boolean(tidasCompatibilityFlow)}
        uiLanguage={uiLanguage}
        flow={tidasCompatibilityFlow}
        onClose={() => setTidasCompatibilityFlow(null)}
        onSaved={refreshFlowAfterTidasCompatibilitySave}
        onStatus={onStatus}
      />
      <FlowAllocationPropertiesModal
        open={Boolean(allocationPropertiesFlow)}
        uiLanguage={uiLanguage}
        flowUuid={allocationPropertiesFlow?.id ?? null}
        flowName={allocationPropertiesFlow ? getDisplayFlowName(allocationPropertiesFlow, uiLanguage) : ""}
        onClose={() => setAllocationPropertiesFlow(null)}
        onSaved={(properties) => {
          setServerFlowRows((prev) => prev.map((item) => (
            item.id === allocationPropertiesFlow?.id ? { ...item, allocationProperties: properties } : item
          )));
          clearPmCacheByPrefix("pm:flows:");
          setForceFlowRefresh(true);
        }}
        onStatus={onStatus}
      />
      <Ef31ImportJobPanel
        open={ef31ImportOpen}
        uiLanguage={uiLanguage}
        onClose={() => setEf31ImportOpen(false)}
      />
    </div>
  );
}
