import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { GraphCanvas } from "./components/GraphCanvas/GraphCanvas";
import { FlowBalanceDialog } from "./components/Inspector/FlowBalanceDialog";
import { InspectorPanel } from "./components/Inspector/InspectorPanel";
import { PtsPortEditorDialog } from "./components/Inspector/PtsPortEditorDialog";
import { PtsVersionHistoryDialog } from "./components/Inspector/PtsVersionHistoryDialog";
import { NodeCreatorDrawer } from "./components/NodePalette/NodeCreatorDrawer";
import { NodePalette } from "./components/NodePalette/NodePalette";
import { UnitProcessImportDialog } from "./components/NodePalette/UnitProcessImportDialog";
import { ProjectManagement, type CreateProjectForm } from "./components/ProjectManagement/ProjectManagement";
import type { LcaGraphPayload } from "./model/exchange";
import type { FlowPort, LcaNodeKind, ProcessMode } from "./model/node";
import { useLcaGraphStore } from "./store/lcaGraphStore";

type ModelCreateResponse = {
  project_id: string;
  version: number;
  created_at: string;
  created_new_version?: boolean;
  graph_hash?: string | null;
  message?: string | null;
  pts_compile_count?: number;
  pts_compiled_uuids?: string[];
  pts_failed_count?: number;
  pts_failed_items?: Array<Record<string, unknown>>;
};

type ModelVersionResponse = {
  project_id: string;
  version: number;
  graph: LcaGraphPayload;
  created_at: string;
};

type PtsResourceResponse = {
  project_id: string;
  pts_uuid: string;
  name?: string | null;
  pts_node_id?: string | null;
  latest_graph_hash?: string | null;
  compiled_graph_hash?: string | null;
  latest_compile_version?: number | null;
  latest_published_version?: number | null;
  active_published_version?: number | null;
  published_at?: string | null;
  ports_policy?: Record<string, unknown>;
  shell_node?: Record<string, unknown>;
  pts_graph?: LcaGraphPayload | Record<string, unknown>;
};

type PtsCompileResponse = {
  compile_id: string;
  project_id: string;
  pts_node_id: string;
  pts_uuid: string;
  graph_hash: string;
  compile_version?: number | null;
  cached?: boolean;
  ok: boolean;
  errors: string[];
  warnings: string[];
  matrix_size: number;
  invertible: boolean;
  artifact: Record<string, unknown>;
  external_preview?: Record<string, unknown>;
};

type PtsPackFinalizeResponse = {
  compile_id?: string | null;
  compile_version?: number | null;
  published_artifact_id: string;
  published_version: number;
  active_published_version?: number | null;
  graph_hash?: string | null;
  port_id_map?: Record<string, string> | null;
  shell_node?: Record<string, unknown> | null;
  shell_inputs?: Array<Record<string, unknown>> | null;
  shell_outputs?: Array<Record<string, unknown>> | null;
  resource?: PtsResourceResponse | null;
  warnings?: unknown[] | null;
  defaultVisiblePortIds?: string[] | null;
  default_visible_port_ids?: string[] | null;
};

type PtsUnpackResponse = {
  project_id?: string | null;
  pts_uuid?: string | null;
  pts_node_id?: string | null;
  pts_graph?: LcaGraphPayload | Record<string, unknown> | null;
  shell_node?: Record<string, unknown> | null;
  resource?: PtsResourceResponse | null;
};

type PtsVersionItem = {
  id: string;
  graph_hash: string;
  version?: number | null;
  created_at: string;
  updated_at: string;
  ok?: boolean | null;
  matrix_size?: number | null;
  invertible?: boolean | null;
  source_compile_id?: string | null;
  source_compile_version?: number | null;
};

type PtsCompileHistoryResponse = {
  project_id: string;
  pts_uuid: string;
  items: PtsVersionItem[];
};

type PtsPublishedHistoryResponse = {
  project_id: string;
  pts_uuid: string;
  active_published_version?: number | null;
  items: PtsVersionItem[];
};

type PtsPortsResponse = {
  project_id: string;
  pts_uuid: string;
  pts_node_id?: string | null;
  graph_hash?: string | null;
  published_version?: number | null;
  source_compile_version?: number | null;
  ports?: {
    inputs?: Array<Record<string, unknown>>;
    outputs?: Array<Record<string, unknown>>;
    elementary?: Array<Record<string, unknown>>;
  };
};

type ProjectResponse = {
  project_id: string;
  name: string;
  created_at: string;
  latest_version: number | null;
  latest_version_created_at: string | null;
};
type ProjectListPagedResponse = {
  items: ProjectResponse[];
  total: number;
  page: number;
  page_size: number;
};
type ProjectDetailResponse = ProjectResponse & {
  flow_name_sync_needed?: boolean;
  outdated_flow_refs_count?: number;
  outdated_flow_ref_examples?: Array<Record<string, unknown>>;
};

type RunResponse = {
  run_id: string;
  status: string;
  summary: Record<string, unknown>;
  tiangong_like_input?: Record<string, unknown>;
  lci_result: Record<string, unknown>;
};

type ProductResultRow = {
  productKey: string;
  viewKey: string;
  matchKey: string;
  processUuid: string;
  rawProcessUuid: string;
  processName: string;
  productPortId: string;
  productFlowUuid: string;
  productName: string;
  isReferenceProduct: boolean;
  unit: string;
  unitGroup?: string;
  ptsProcessName?: string;
};

type IndicatorDisplayRow = {
  idx: number;
  method: string;
  name: string;
  unit: string;
  selectedValue: number;
};

type UnitDefinitionRow = {
  unit_group: string;
  unit_name: string;
  factor_to_reference: number;
  is_reference: boolean;
};

type PtsModelingWarning = {
  code: string;
  severity: string;
  message: string;
  ptsUuid?: string;
  ptsNodeId?: string;
  nodeName?: string;
  expectedTotal?: number;
  actualTotal?: number;
  evidence: unknown[];
};
type PtsDefaultVisiblePortHint = {
  direction: "input" | "output";
  flowUuid: string;
  name?: string;
  sourceProcessUuid?: string;
  sourceProcessName?: string;
  sourceNodeId?: string;
};
type ResultUnitMode = "defined" | "reference";
type ResultProductViewMode = "target_total" | "unit_product";

type SaveMode = "manual" | "auto" | "interval";
type MessageLevel = "success" | "warning" | "error" | "info";
type AppMode = "management" | "editor";
type MissingProductNode = { nodeId: string; processUuid: string; name: string };
type FlowNameSyncState = {
  needed: boolean;
  outdatedCount: number;
  examples: Array<Record<string, unknown>>;
};

type ProjectTargetQuantityMode = "functional_unit" | "custom";

type ProjectTargetProductConfig = {
  processUuid: string;
  flowUuid: string;
  quantityMode: ProjectTargetQuantityMode;
  quantity: number;
};

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;
const APP_DEBUG = Boolean(import.meta.env.VITE_DEBUG);
const debugPts = (scope: string, payload?: unknown) => {
  if (!APP_DEBUG) {
    return;
  }
  if (payload === undefined) {
    console.info(`[PTS_DEBUG] ${scope}`);
    return;
  }
  console.info(`[PTS_DEBUG] ${scope}`, payload);
};
const API_BASE_NO_PREFIX = API_BASE.replace(/\/api$/, "");
const DEFAULT_PROJECT_NAME = "测试项目";
const CURRENT_PROJECT_KEY = "nebula:current_project_id";
const FLOW_ANIM_KEY = "nebula:flow_animation_enabled";
const AUTO_POPUP_KEY = "nebula:auto_popup_enabled";
const UNIT_AUTOSCALE_KEY = "nebula:unit_autoscale_enabled";
const UI_LANG_KEY = "nebula:ui_language";
const AUTO_SAVE_DEBOUNCE_MS = 8000;
const INTERVAL_SAVE_MS = 60000;
const draftKey = (projectId: string) => `nebula:${projectId}:draft`;
const snapshotKey = (projectId: string) => `nebula:${projectId}:snapshot`;
const PROJECT_ROUTE_PREFIX = "/projects/";

const getProjectIdFromPathname = (pathname: string): string => {
  const normalized = String(pathname ?? "").trim();
  if (!normalized.startsWith(PROJECT_ROUTE_PREFIX)) {
    return "";
  }
  const suffix = normalized.slice(PROJECT_ROUTE_PREFIX.length).split("/")[0] ?? "";
  try {
    return decodeURIComponent(suffix).trim();
  } catch {
    return suffix.trim();
  }
};

const buildProjectPathname = (projectId: string): string =>
  projectId ? `${PROJECT_ROUTE_PREFIX}${encodeURIComponent(projectId)}` : "/";

const buildProjectTargetMatchKey = (processUuid: string, flowUuid: string): string =>
  `${String(processUuid ?? "").trim()}::${String(flowUuid ?? "").trim()}`;

const formatTargetProductDisplayLabel = (
  productName: string,
  processName: string,
  ptsProcessName?: string,
): string => {
  const cleanProductName = String(productName ?? "").trim();
  const cleanProcessName = String(processName ?? "").trim();
  const cleanPtsProcessName = String(ptsProcessName ?? "").trim();
  if (cleanPtsProcessName) {
    const ptsMarker = `@${cleanPtsProcessName}`;
    if (cleanProductName.includes(ptsMarker)) {
      return cleanProductName;
    }
    const atIndex = cleanProductName.lastIndexOf("@");
    if (atIndex >= 0) {
      const base = cleanProductName.slice(0, atIndex).trim();
      const suffix = cleanProductName.slice(atIndex).trim();
      return `${base}${ptsMarker} ${suffix}`;
    }
    if (cleanProcessName && cleanProcessName !== cleanPtsProcessName) {
      return `${cleanProductName} ${ptsMarker} @ ${cleanProcessName}`;
    }
    return `${cleanProductName} @ ${cleanPtsProcessName}`;
  }
  if (cleanProductName.includes("@")) {
    return cleanProductName;
  }
  return cleanProcessName ? `${cleanProductName} @ ${cleanProcessName}` : cleanProductName;
};

const parseTargetProductQuantity = (value: string): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
};

const parseFunctionalUnitQuantity = (functionalUnit: string): number => {
  const match = String(functionalUnit ?? "").trim().match(/^([0-9]+(?:\.[0-9]+)?)/);
  if (!match) {
    return 1;
  }
  return parseTargetProductQuantity(match[1] ?? "1");
};

const readProjectTargetProductConfig = (graph?: LcaGraphPayload | null): ProjectTargetProductConfig | null => {
  const metadata = (graph?.metadata as { project_preferences?: { target_product?: Record<string, unknown> } } | undefined)
    ?.project_preferences;
  const raw = metadata?.target_product;
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const processUuid = String(raw.process_uuid ?? raw.processUuid ?? "").trim();
  const flowUuid = String(raw.flow_uuid ?? raw.flowUuid ?? "").trim();
  const legacyProductKey = String(raw.product_key ?? raw.productKey ?? "").trim();
  const quantityRaw = Number(raw.quantity ?? 1);
  const quantityModeRaw = String(raw.quantity_mode ?? raw.quantityMode ?? (raw.lock_to_functional_unit ? "functional_unit" : "custom")).trim();
  const quantityMode: ProjectTargetQuantityMode = quantityModeRaw === "functional_unit" ? "functional_unit" : "custom";
  if (!processUuid || !flowUuid) {
    if (!legacyProductKey.includes("::")) {
      return null;
    }
    const [legacyProcessUuid, legacyFlowUuid] = legacyProductKey.split("::");
    if (!legacyProcessUuid || !legacyFlowUuid) {
      return null;
    }
    return {
      processUuid: legacyProcessUuid,
      flowUuid: legacyFlowUuid,
      quantityMode,
      quantity: Number.isFinite(quantityRaw) && quantityRaw > 0 ? quantityRaw : 1,
    };
  }
  if (!processUuid || !flowUuid) {
    return null;
  }
  return {
    processUuid,
    flowUuid,
    quantityMode,
    quantity: Number.isFinite(quantityRaw) && quantityRaw > 0 ? quantityRaw : 1,
  };
};

const applyProjectTargetProductConfig = (
  graph: LcaGraphPayload,
  config: ProjectTargetProductConfig | null,
): LcaGraphPayload => {
  const nextMetadata = { ...(graph.metadata ?? {}) } as Record<string, unknown>;
  const projectPreferences =
    nextMetadata.project_preferences && typeof nextMetadata.project_preferences === "object"
      ? { ...(nextMetadata.project_preferences as Record<string, unknown>) }
      : {};
  if (config?.processUuid && config?.flowUuid) {
    projectPreferences.target_product = {
      process_uuid: config.processUuid,
      flow_uuid: config.flowUuid,
      quantity_mode: config.quantityMode,
      lock_to_functional_unit: config.quantityMode === "functional_unit",
      quantity: config.quantity,
    };
  } else {
    delete projectPreferences.target_product;
  }
  if (Object.keys(projectPreferences).length > 0) {
    nextMetadata.project_preferences = projectPreferences;
  } else {
    delete nextMetadata.project_preferences;
  }
  return {
    ...graph,
    metadata: nextMetadata,
  };
};

const getPtsSourceSnapshotIdentityKey = (
  direction: "input" | "output",
  port: Pick<
    FlowPort,
    "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId" | "portKey" | "productKey"
  >,
): string => {
  if (direction === "output") {
    const portKey = String(port.portKey ?? "").trim();
    if (portKey) {
      return `port:${portKey}`;
    }
    const productKey = String(port.productKey ?? "").trim();
    if (productKey) {
      return `product:${productKey}`;
    }
    const sourceProcessUuid = String(port.sourceProcessUuid ?? "").trim();
    if (sourceProcessUuid) {
      return `${port.flowUuid}@@${sourceProcessUuid}`;
    }
    const sourceNodeId = String(port.sourceNodeId ?? "").trim();
    if (sourceNodeId) {
      return `${port.flowUuid}@@node:${sourceNodeId}`;
    }
    const sourceProcessName = String(port.sourceProcessName ?? "").trim();
    if (sourceProcessName) {
      return `${port.flowUuid}@@name:${sourceProcessName}`;
    }
  }
  return String(port.flowUuid ?? "").trim();
};

const mergePtsPortsWithRootOverrides = (
  direction: "input" | "output",
  basePorts: FlowPort[],
  currentPorts: FlowPort[],
  options?: { preserveVisibilityFromBase?: boolean },
): FlowPort[] => {
  const currentByKey = new Map(currentPorts.map((port) => [getPtsSourceSnapshotIdentityKey(direction, port), port]));
  return basePorts.map((port) => {
    const override = currentByKey.get(getPtsSourceSnapshotIdentityKey(direction, port));
    if (!override) {
      return { ...port };
    }
    return {
      ...port,
      showOnNode:
        options?.preserveVisibilityFromBase === true ? Boolean(port.showOnNode) : Boolean(override.showOnNode),
    };
  });
};

const detectMessageLevel = (message: string): MessageLevel => {
  const text = String(message ?? "").toLowerCase();
  const infoMarkers = [
    "已恢复项目",
    "已恢复草稿",
    "项目读取中",
    "正在切换到历史版本",
    "已到最早版本",
    "已到最新版本",
  ];
  if (infoMarkers.some((key) => text.includes(key))) {
    return "info";
  }
  const warningMarkers = ["警告", "告警", "warning", "warn"];
  if (warningMarkers.some((key) => text.includes(key))) {
    return "warning";
  }
  const successMarkers = ["完成", "成功", "已保存", "已创建", "saved", "complete", "completed", "success", "created"];
  if (successMarkers.some((key) => text.includes(key))) {
    return "success";
  }
  const errorMarkers = [
    "失败",
    "错误",
    "无效",
    "无法",
    "阻止",
    "必须",
    "missing",
    "invalid",
    "failed",
    "error",
    "forbidden",
    "blocked",
    "cannot",
  ];
  if (errorMarkers.some((key) => text.includes(key))) {
    return "error";
  }
  return "warning";
};

const translateStatusText = (message: string, uiLanguage: "zh" | "en"): string => {
  const raw = String(message ?? "").trim();
  if (!raw || uiLanguage !== "en") {
    return raw;
  }
  const exactMap = new Map<string, string>([
    ["项目读取中...", "Loading project..."],
    ["项目读取失败，已保留当前草稿。", "Failed to load project. The current draft was kept."],
    ["已到最早版本。", "Already at the earliest version."],
    ["已到最新版本。", "Already at the latest version."],
    ["已同步当前项目的 flow 名称。", "Flow names for the current project have been synced."],
    ["项目列表读取失败。", "Failed to load project list."],
    ["正在保存模型...", "Saving model..."],
    ["仅支持在主图中解封 PTS 节点。", "Only PTS nodes on the main graph can be unpacked."],
    ["当前 PTS 缺少 pts_uuid，无法解封。", "The current PTS is missing pts_uuid and cannot be unpacked."],
    ["正在解封 PTS...", "Unpacking PTS..."],
    [
      "PTS 解封完成，已尝试自动连接并保存主图。未连上的边可后续手动补连。",
      "PTS unpacked. The main graph was auto-connected and saved where possible. Unlinked edges can be connected manually later.",
    ],
    [
      "主图暂时不能运行：存在一个空白草稿 PTS。请双击进入该 PTS，补充内部节点和连线并发布后，再回到主图运行。",
      "The main graph cannot run yet because there is an empty draft PTS. Open that PTS, add internal nodes and edges, publish it, and then run the main graph again.",
    ],
    [
      "警告：当前过程未定义产品，已关闭清单分析，可稍后继续补充。",
      "Warning: the current process has no defined product. Inventory analysis was closed and can be completed later.",
    ],
    ["正在运行求解...", "Running calculation..."],
    ["仅在 PTS 编辑模式可执行该操作。", "This action is only available in PTS editing mode."],
    ["操作失败：未定位到当前 PTS。", "Action failed: current PTS could not be located."],
    ["操作失败：缺少项目 ID。", "Action failed: missing project ID."],
    ["仅在 PTS 编辑模式可执行保存。", "Save is only available in PTS editing mode."],
    ["仅在 PTS 编辑模式可查看版本历史。", "Version history is only available in PTS editing mode."],
    ["新建项目失败: 项目名称不能为空。", "Failed to create project: project name cannot be empty."],
    ["项目已删除", "Project deleted."],
    ["已初始化空白 PTS 草稿。", "Empty PTS draft initialized."],
    ["New Process 将在下一步接入。", "New Process will be connected in a later step."],
    ["New Flow 将在下一步接入。", "New Flow will be connected in a later step."],
    ["运行完成", "Run completed"],
  ]);
  const exact = exactMap.get(raw);
  if (exact) {
    return exact;
  }
  return raw
    .replace(/^无法连线：来源过程或目标过程不存在。$/, "Cannot connect: source or target process does not exist.")
    .replace(/^无法连线：不允许同一过程自连接。$/, "Cannot connect: self-loop connections are not allowed.")
    .replace(/^无法连线：LCI 不能直接连接到 LCI。$/, "Cannot connect: LCI cannot connect directly to LCI.")
    .replace(/^无法连线：LCI provider 仅可输出到 Unit\/PTS。$/, "Cannot connect: LCI provider can only output to Unit/PTS.")
    .replace(/^无法连线：LCI waste_sink 仅可接收 Unit\/PTS 输入。$/, "Cannot connect: LCI waste_sink only accepts Unit/PTS inputs.")
    .replace(/^无法连线：来源过程没有可显示的输出端口。$/, "Cannot connect: source process has no visible output port.")
    .replace(/^无法连线：产品流不能直接连接到产品流。$/, "Cannot connect: product flows cannot connect directly to product flows.")
    .replace(/^无法连线：非产品中间流不能直接连接到非产品中间流。$/, "Cannot connect: non-product intermediate flows cannot connect directly to non-product intermediate flows.")
    .replace(/^无法连线：非产品中间流只能关联唯一产品中间流。$/, "Cannot connect: a non-product intermediate flow can only associate with a single product intermediate flow.")
    .replace(/^无法连线：多源汇聚到单一输入时请使用市场过程。$/, "Cannot connect: use a market process when multiple sources converge to one input.")
    .replace(/^无法连线：多源与多汇并存时请插入市场过程。$/, "Cannot connect: insert a market process when multiple sources and multiple sinks coexist.")
    .replace(/^无法连线：市场过程只能处理单一中间流 UUID。$/, "Cannot connect: a market process can only handle a single intermediate flow UUID.")
    .replace(/^无法连线：市场过程输入只能连接已定义为产品的来源端口。$/, "Cannot connect: market-process inputs can only connect to source ports defined as products.")
    .replace(/^无法连线：流 UUID 缺失或非法，请先从数据库引用中间流或基本流。$/, "Cannot connect: flow UUID is missing or invalid. Please reference an intermediate or elementary flow from the database first.")
    .replace(/^已恢复项目:/, "Project restored:")
    .replace(/^已恢复草稿:/, "Draft restored:")
    .replace(/^项目为空:/, "Project is empty:")
    .replace(/^正在切换到历史版本 v(.+)\.\.\.$/, "Switching to historical version v$1...")
    .replace(/^版本 (.+) 不存在（可能已到最早\/最新边界）。$/, "Version $1 does not exist (possibly already at the earliest/latest boundary).")
    .replace(/^历史版本读取失败:/, "Failed to load historical version:")
    .replace(/^同步 flow 信息失败:/, "Failed to sync flow info:")
    .replace(/^PTS 独立保存失败:/, "Failed to save standalone PTS:")
    .replace(/^未保存：PTS 输出来源绑定缺失/, "Not saved: missing PTS output source bindings")
    .replace(/^未保存：PTS 内部连线数量校验失败/, "Not saved: PTS internal edge amount validation failed for")
    .replace(/^未保存：当前模型为空图/, "Not saved: current model graph is empty")
    .replace(/^已保存项目 /, "Saved project ")
    .replace(/^保存失败:/, "Save failed:")
    .replace(/^PTS 解封失败:/, "PTS unpack failed:")
    .replace(/^运行已阻止：PTS 输出来源绑定缺失/, "Run blocked: missing PTS output source bindings")
    .replace(/^运行已阻止：PTS 内部连线数量校验失败/, "Run blocked: PTS internal edge amount validation failed for")
    .replace(/^运行已阻止：process_uuid 重复/, "Run blocked: duplicate process_uuid found in")
    .replace(/^运行已阻止：产品单位组校验失败/, "Run blocked: product unit-group validation failed for")
    .replace(/^运行已阻止：LCI 校验失败/, "Run blocked: LCI validation failed for")
    .replace(/^运行已阻止：市场过程校验失败/, "Run blocked: market-process validation failed for")
    .replace(/^运行完成:/, "Run completed:")
    .replace(/^运行失败:/, "Run failed:")
    .replace(/^正在发布 PTS:/, "Publishing PTS:")
    .replace(/^正在保存编译 PTS:/, "Saving compiled PTS:")
    .replace(/^PTS 发布完成:/, "PTS published:")
    .replace(/^PTS 保存编译完成:/, "PTS compile saved:")
    .replace(/^PTS 发布失败:/, "PTS publish failed:")
    .replace(/^PTS 保存编译失败:/, "PTS compile save failed:")
    .replace(/^PTS 版本历史已加载:/, "PTS version history loaded:")
    .replace(/^PTS 版本历史读取失败:/, "Failed to load PTS version history:")
    .replace(/^返回主图前刷新 PTS 投影失败:/, "Failed to refresh PTS projection before returning to the main graph:")
    .replace(/^PTS封装完成，已自动编译、发布并保存主图/, "PTS packaging completed. It was auto-compiled, published, and saved to the main graph")
    .replace(/^PTS 自动编译\/发布失败:/, "PTS auto compile/publish failed:")
    .replace(/^已创建项目:/, "Project created:")
    .replace(/^新建项目失败:/, "Project creation failed:")
    .replace(/^删除项目失败:/, "Project deletion failed:")
    .replace(/^PTS 读取失败:/, "Failed to load PTS:")
    .replace(/^PTS 草稿初始化失败:/, "PTS draft initialization failed:")
    .replace(/^已自动修复 (\d+) 条归一化连线，线型已同步为虚线。$/, "Automatically repaired $1 normalized edges; line style was synced to dashed.")
    .replace(/警告：/g, "Warning: ")
    .replace(/示例：/g, "Example: ")
    .replace(/ 项。/g, " items. ")
    .replace(/：/g, ": ")
    .replace(/。/g, ". ")
    .replace(/；/g, "; ")
    .replace(/（/g, "(")
    .replace(/）/g, ")");
};

const isEmptyGraph = (graph: LcaGraphPayload): boolean =>
  (graph.nodes?.length ?? 0) === 0 && (graph.exchanges?.length ?? 0) === 0;

const coerceOptionalBoolean = (value: unknown): boolean | undefined => {
  if (value == null) {
    return undefined;
  }
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  const text = String(value).trim().toLowerCase();
  if (!text) {
    return undefined;
  }
  if (["false", "0", "no", "off", "null", "undefined"].includes(text)) {
    return false;
  }
  if (["true", "1", "yes", "on"].includes(text)) {
    return true;
  }
  return Boolean(value);
};

const parsePayloadHandlePortId = (
  handleId: string | null | undefined,
  direction: "input" | "output",
): string | undefined => {
  if (!handleId) {
    return undefined;
  }
  const raw = String(handleId).trim();
  const prefixes = direction === "input" ? ["in:", "inl:", "inr:", "input_"] : ["out:", "outl:", "outr:", "output_"];
  const matched = prefixes.find((prefix) => raw.startsWith(prefix));
  if (!matched) {
    return undefined;
  }
  return raw.slice(matched.length);
};

const buildPayloadHandleId = (
  handleId: string | null | undefined,
  direction: "input" | "output",
  portId: string,
): string => {
  const raw = String(handleId ?? "").trim();
  if (direction === "input") {
    if (raw.startsWith("inl:")) {
      return `inl:${portId}`;
    }
    if (raw.startsWith("inr:")) {
      return `inr:${portId}`;
    }
    return `in:${portId}`;
  }
  if (raw.startsWith("outl:")) {
    return `outl:${portId}`;
  }
  if (raw.startsWith("outr:")) {
    return `outr:${portId}`;
  }
  return `out:${portId}`;
};

const parseLegacyPayloadHandleBinding = (
  handleId: string | null | undefined,
  direction: "input" | "output",
): { flowUuid?: string; occurrence?: number } => {
  if (!handleId) {
    return {};
  }
  const raw = String(handleId).trim();
  if (direction === "input") {
    const match = /^in:in::([^:]+)::(\d+)$/.exec(raw);
    if (!match) {
      return {};
    }
    const occurrence = Number.parseInt(match[2], 10);
    return {
      flowUuid: match[1],
      occurrence: Number.isFinite(occurrence) && occurrence > 0 ? occurrence - 1 : undefined,
    };
  }
  const rest =
    raw.startsWith("outl:") ? raw.slice("outl:".length) :
    raw.startsWith("outr:") ? raw.slice("outr:".length) :
    raw.startsWith("out:") ? raw.slice("out:".length) :
    "";
  if (!rest.includes("::")) {
    return {};
  }
  const parts = rest.split("::");
  const flowUuid = parts.at(-1)?.trim();
  return flowUuid ? { flowUuid } : {};
};

const resolvePayloadEdgePort = (
  ports: FlowPort[],
  direction: "input" | "output",
  values: {
    handle?: unknown;
    portId?: unknown;
    flowUuid?: string;
  },
): FlowPort | undefined => {
  const rawPortId = String(values.portId ?? "").trim();
  const normalizedPortId = rawPortId.startsWith(":") ? rawPortId.slice(1) : rawPortId;
  const handlePortId = parsePayloadHandlePortId(String(values.handle ?? "").trim(), direction);
  const explicitPortId = normalizedPortId || handlePortId || "";
  if (explicitPortId) {
    const exact = ports.find(
      (port) =>
        (port.id === explicitPortId || String(port.legacyPortId ?? "").trim() === explicitPortId) &&
        (!values.flowUuid || port.flowUuid === values.flowUuid),
    );
    if (exact) {
      return exact;
    }
  }

  const legacy = parseLegacyPayloadHandleBinding(String(values.handle ?? "").trim(), direction);
  const flowUuid = values.flowUuid || legacy.flowUuid || "";
  if (!flowUuid) {
    return undefined;
  }
  const byFlow = ports.filter((port) => port.flowUuid === flowUuid);
  if (byFlow.length === 0) {
    return undefined;
  }
  if (typeof legacy.occurrence === "number" && legacy.occurrence >= 0 && legacy.occurrence < byFlow.length) {
    return byFlow[legacy.occurrence];
  }
  return byFlow.length === 1 ? byFlow[0] : undefined;
};

const normalizeGraphPayload = (graph: LcaGraphPayload): LcaGraphPayload => {
  const normalizeNode = (node: LcaGraphPayload["nodes"][number]) => {
    const normalizePorts = (ports: NonNullable<typeof node.inputs>) =>
      (ports ?? []).map((port) => {
        const raw = port as Record<string, unknown>;
          const displayName = String(raw.display_name ?? "").trim();
          const displayNameEn = String(raw.display_name_en ?? raw.displayNameEn ?? "").trim();
          const processName = String(raw.process_name ?? "").trim();
        const sourceProcessName = String(raw.source_process_name ?? raw.sourceProcessName ?? "").trim();
        const nestedSourceProcessName = String(
          raw.nested_source_process_name ?? raw.nestedSourceProcessName ?? "",
        ).trim();
        const sourceProcessUuid = String(raw.source_process_uuid ?? raw.sourceProcessUuid ?? "").trim();
        const sourceNodeId = String(raw.source_node_id ?? raw.sourceNodeId ?? "").trim();
        const portKey = String(raw.port_key ?? raw.portKey ?? "").trim();
        const productKey = String(raw.product_key ?? raw.productKey ?? "").trim();
        const flowUuid = String(raw.flow_uuid ?? raw.flowUuid ?? port.flowUuid ?? "").trim();
        const baseName = String(port.name ?? "").trim();
        const flowNameEn = String(raw.flow_name_en ?? raw.flowNameEn ?? port.flowNameEn ?? "").trim();
        const directionRaw = String(raw.direction ?? port.direction ?? "input").trim().toLowerCase();
        const direction: "input" | "output" = directionRaw === "output" ? "output" : "input";
        const typeRaw = String(raw.type ?? port.type ?? "technosphere").trim().toLowerCase();
        const type: "technosphere" | "biosphere" =
          typeRaw === "biosphere" ? "biosphere" : "technosphere";
        const identitySeed =
          portKey || productKey || sourceNodeId || sourceProcessUuid || sourceProcessName || flowUuid || "port";
        const fallbackId = `${direction}_${identitySeed}`;
        const resolvedName =
          displayName ||
          (processName && baseName && processName.includes("@") ? processName : "") ||
          baseName;
        const rawId = String(raw.id ?? port.id ?? "").trim();
        const legacyPortId = String(raw.legacyPortId ?? raw.legacy_port_id ?? port.legacyPortId ?? "").trim();
        const showOnNode = coerceOptionalBoolean(raw.show_on_node ?? raw.showOnNode ?? port.showOnNode) ?? false;
        const internalExposed = coerceOptionalBoolean(
          raw.internal_exposed ?? raw.internalExposed ?? port.internalExposed,
        );
        const isProduct = coerceOptionalBoolean(raw.is_product ?? raw.isProduct ?? port.isProduct) ?? false;
        return {
          ...port,
          id: rawId || portKey || productKey || fallbackId,
          legacyPortId: legacyPortId || port.legacyPortId,
          flowUuid,
          flowNameEn: flowNameEn || port.flowNameEn,
          portKey: portKey || port.portKey,
          productKey: productKey || port.productKey,
          sourceProcessUuid: sourceProcessUuid || port.sourceProcessUuid,
          nestedSourceProcessName: nestedSourceProcessName || port.nestedSourceProcessName,
            sourceNodeId: sourceNodeId || port.sourceNodeId,
            name: resolvedName || baseName,
            displayNameEn: displayNameEn || port.displayNameEn,
            sourceProcessName: sourceProcessName || port.sourceProcessName,
            amount: Number(raw.amount ?? port.amount ?? 0),
          externalSaleAmount: Number(raw.external_sale_amount ?? raw.externalSaleAmount ?? port.externalSaleAmount ?? 0),
          type,
          direction,
          unit: String(raw.unit ?? port.unit ?? "kg"),
          unitGroup: String(raw.unit_group ?? raw.unitGroup ?? port.unitGroup ?? ""),
          showOnNode,
          internalExposed,
          isProduct,
        };
      });
    return {
      ...node,
      inputs: normalizePorts(node.inputs ?? []),
      outputs: normalizePorts(node.outputs ?? []),
    };
  };

  const sanitizeEdges = <T extends Record<string, unknown>>(nodes: LcaGraphPayload["nodes"], edges: T[]): T[] => {
    const nodeById = new Map(nodes.map((node) => [String(node.id), node]));
    return edges.map((edge) => {
      const sourceNodeId = String(edge.fromNode ?? edge.source ?? "").trim();
      const targetNodeId = String(edge.toNode ?? edge.target ?? "").trim();
      const sourceNode = sourceNodeId ? nodeById.get(sourceNodeId) : undefined;
      const targetNode = targetNodeId ? nodeById.get(targetNodeId) : undefined;
      const flowUuid = String(edge.flowUuid ?? edge.flow_uuid ?? "").trim();
      if (!sourceNode && !targetNode) {
        return edge;
      }

      let nextEdge = edge;
      const sourcePort = sourceNode
        ? resolvePayloadEdgePort(sourceNode.outputs ?? [], "output", {
            handle: edge.sourceHandle ?? edge.source_handle,
            portId: edge.source_port_id,
            flowUuid,
          })
        : undefined;
      if (sourcePort) {
        nextEdge = {
          ...nextEdge,
          source_port_id: sourcePort.id,
          sourceHandle: buildPayloadHandleId(
            String(edge.sourceHandle ?? edge.source_handle ?? edge.source_port_id ?? ""),
            "output",
            sourcePort.id,
          ),
        };
      }

      const targetPort = targetNode
        ? resolvePayloadEdgePort(targetNode.inputs ?? [], "input", {
            handle: edge.targetHandle ?? edge.target_handle,
            portId: edge.target_port_id,
            flowUuid,
          })
        : undefined;
      if (targetPort) {
        nextEdge = {
          ...nextEdge,
          target_port_id: targetPort.id,
          targetHandle: buildPayloadHandleId(
            String(edge.targetHandle ?? edge.target_handle ?? edge.target_port_id ?? ""),
            "input",
            targetPort.id,
          ),
        };
      }
      return nextEdge;
    });
  };

  const metadata = graph.metadata as { canvases?: Array<Record<string, unknown>> } | undefined;
  const normalizedRootNodes = (graph.nodes ?? []).map((node) => normalizeNode(node));
  const nextCanvases = (metadata?.canvases ?? []).map((canvas) => {
    const rawNodes = Array.isArray(canvas.nodes) ? (canvas.nodes as LcaGraphPayload["nodes"]) : [];
    const normalizedCanvasNodes = rawNodes.map((node) => normalizeNode(node));
    const rawEdges = Array.isArray(canvas.edges) ? (canvas.edges as Array<Record<string, unknown>>) : [];
    return {
      ...canvas,
      nodes: normalizedCanvasNodes,
      ...(rawEdges.length > 0 ? { edges: sanitizeEdges(normalizedCanvasNodes, rawEdges) } : {}),
    };
  });

  return {
    ...graph,
    nodes: normalizedRootNodes,
    exchanges: sanitizeEdges(normalizedRootNodes, graph.exchanges ?? []) as LcaGraphPayload["exchanges"],
    metadata: {
      ...(graph.metadata ?? {}),
      ...(nextCanvases.length > 0 ? { canvases: nextCanvases } : {}),
    },
  };
};

const normalizePortIdMap = (raw: unknown): Record<string, string> => {
  if (!raw || typeof raw !== "object") {
    return {};
  }
  return Object.fromEntries(
    Object.entries(raw as Record<string, unknown>)
      .map(([key, value]) => [String(key).trim(), String(value ?? "").trim()] as const)
      .filter(([key, value]) => key.length > 0 && value.length > 0),
  );
};

const parsePtsSyntheticProductProcessUuid = (
  rawProcessUuid: string,
): { ptsUuid: string; flowUuid: string } | null => {
  const match = /^([^:]+)::product::([^:]+?)(?:::(.+))?$/.exec(String(rawProcessUuid ?? "").trim());
  if (!match) {
    return null;
  }
  const ptsUuid = String(match[1] ?? "").trim();
  const flowUuid = String(match[2] ?? "").trim();
  if (!ptsUuid || !flowUuid) {
    return null;
  }
  return { ptsUuid, flowUuid };
};

const normalizePtsProjectedPort = (
  rawPort: Record<string, unknown>,
  bucket: "inputs" | "outputs" = "inputs",
  overrides?: Record<string, unknown>,
) => {
  const normalizedOverrides = {
    ...(bucket === "outputs" ? { direction: "output" } : { direction: "input" }),
    ...overrides,
  };
  const payload = normalizeGraphPayload({
    functionalUnit: "",
    nodes: [
      {
        id: "tmp",
        node_kind: "pts_module",
        mode: "normalized",
        process_uuid: "tmp",
        name: "tmp",
        location: "",
        reference_product: "",
        inputs: bucket === "inputs" ? ([{ ...rawPort, ...normalizedOverrides }] as never) : [],
        outputs: bucket === "outputs" ? ([{ ...rawPort, ...normalizedOverrides }] as never) : [],
      },
    ],
    exchanges: [],
  });
  return bucket === "outputs" ? payload.nodes[0]?.outputs?.[0] : payload.nodes[0]?.inputs?.[0];
};

const getPtsPortIdentityKey = (port: {
  flowUuid?: string;
  isProduct?: boolean;
  type?: string;
  portKey?: string;
  productKey?: string;
  sourceNodeId?: string;
  sourceProcessUuid?: string;
  sourceProcessName?: string;
}) => {
  const flowUuid = String(port.flowUuid ?? "").trim();
  if (String(port.type ?? "").trim() === "biosphere") {
    const portKey = String(port.portKey ?? "").trim();
    if (portKey) {
      return `elem:${portKey}`;
    }
    const sourceNodeId = String(port.sourceNodeId ?? "").trim();
    if (sourceNodeId) {
      return `${flowUuid}@@elem-node:${sourceNodeId}`;
    }
    const sourceProcessUuid = String(port.sourceProcessUuid ?? "").trim();
    if (sourceProcessUuid) {
      return `${flowUuid}@@elem-proc:${sourceProcessUuid}`;
    }
    const sourceProcessName = String(port.sourceProcessName ?? "").trim();
    if (sourceProcessName) {
      return `${flowUuid}@@elem-name:${sourceProcessName}`;
    }
    return `elem:${flowUuid}`;
  }
  if (!port.isProduct) {
    return flowUuid;
  }
  const portKey = String(port.portKey ?? "").trim();
  if (portKey) {
    return `port:${portKey}`;
  }
  const productKey = String(port.productKey ?? "").trim();
  if (productKey) {
    return `product:${productKey}`;
  }
  const sourceNodeId = String(port.sourceNodeId ?? "").trim();
  if (sourceNodeId) {
    return `${flowUuid}@@node:${sourceNodeId}`;
  }
  const sourceProcessUuid = String(port.sourceProcessUuid ?? "").trim();
  if (sourceProcessUuid) {
    return `${flowUuid}@@proc:${sourceProcessUuid}`;
  }
  const sourceProcessName = String(port.sourceProcessName ?? "").trim();
  if (sourceProcessName) {
    return `${flowUuid}@@name:${sourceProcessName}`;
  }
  return flowUuid;
};

const summarizePtsPortsForDebug = (ports: FlowPort[] | undefined) =>
  (ports ?? []).map((port) => ({
    id: port.id,
    key: getPtsPortIdentityKey(port),
    flowUuid: port.flowUuid,
    showOnNode: port.showOnNode,
    internalExposed: port.internalExposed,
    isProduct: port.isProduct,
    sourceNodeId: port.sourceNodeId,
    sourceProcessUuid: port.sourceProcessUuid,
  }));

const summarizeRawPtsPortsForDebug = (ports: unknown) =>
  (Array.isArray(ports) ? ports : []).map((item) => {
    const port = typeof item === "object" && item ? (item as Record<string, unknown>) : {};
    return {
      id: port.id,
      flowUuid: port.flowUuid ?? port.flow_uuid,
      showOnNode: port.showOnNode,
      show_on_node: port.show_on_node,
      showOnNodeType:
        port.showOnNode == null ? String(port.showOnNode) : typeof port.showOnNode,
      show_on_nodeType:
        port.show_on_node == null ? String(port.show_on_node) : typeof port.show_on_node,
      internalExposed: port.internalExposed,
      internal_exposed: port.internal_exposed,
      internalExposedType:
        port.internalExposed == null ? String(port.internalExposed) : typeof port.internalExposed,
      internal_exposedType:
        port.internal_exposed == null ? String(port.internal_exposed) : typeof port.internal_exposed,
    };
  });

const mergeProjectedPortsWithRootVisibility = (
  existingPorts: FlowPort[] | undefined,
  projectedPorts: FlowPort[],
  options?: {
    resetVisibility?: boolean;
    preferredVisiblePortIds?: Iterable<string>;
    debugLabel?: string;
  },
): FlowPort[] => {
  if (options?.resetVisibility) {
    const result = projectedPorts.map((port) => ({
      ...port,
      showOnNode: true,
    }));
    if (options.debugLabel) {
      debugPts(options.debugLabel, {
        mode: "resetVisibility",
        existing: summarizePtsPortsForDebug(existingPorts),
        projected: summarizePtsPortsForDebug(projectedPorts),
        result: summarizePtsPortsForDebug(result),
      });
    }
    return result;
  }
  const preferredVisiblePortIdSet = new Set(
    Array.from(options?.preferredVisiblePortIds ?? [])
      .map((item) => String(item ?? "").trim())
      .filter((item) => item.length > 0),
  );
  const projectedByKey = new Map<string, FlowPort>();
  for (const port of projectedPorts) {
    const key = getPtsPortIdentityKey(port);
    if (!projectedByKey.has(key)) {
      projectedByKey.set(key, port);
    }
  }
  const existingByKey = new Map(
    (existingPorts ?? []).map((port) => [getPtsPortIdentityKey(port), port]),
  );
  const result = Array.from(projectedByKey.values()).map((port) => {
    if (preferredVisiblePortIdSet.size > 0) {
      return {
        ...port,
        showOnNode: preferredVisiblePortIdSet.has(String(port.id ?? "").trim()),
      };
    }
    const portKey = getPtsPortIdentityKey(port);
    const existing = existingByKey.get(portKey);
    if (!existing) {
      return {
        ...port,
        showOnNode: true,
      };
    }
    return {
      ...port,
      showOnNode: Boolean(existing.showOnNode),
    };
  });
  if (options?.debugLabel) {
    debugPts(options.debugLabel, {
      mergeMode: "existing_visible_plus_new_allowed",
      existing: summarizePtsPortsForDebug(existingPorts),
      projected: summarizePtsPortsForDebug(projectedPorts),
      result: summarizePtsPortsForDebug(result),
    });
  }
  return result;
};

const mergeProjectedPortsWithExistingEnglish = (
  existingPorts: FlowPort[] | undefined,
  projectedPorts: FlowPort[],
): FlowPort[] => {
  const existingByKey = new Map(
    (existingPorts ?? []).map((port) => [getPtsPortIdentityKey(port), port]),
  );
  return projectedPorts.map((port) => {
    const existing = existingByKey.get(getPtsPortIdentityKey(port));
    const existingFlowNameEn = String(existing?.flowNameEn ?? "").trim();
    const nextFlowNameEn = String(port.flowNameEn ?? "").trim();
    if (!existing || nextFlowNameEn || !existingFlowNameEn) {
      return port;
    }
    return {
      ...port,
      flowNameEn: existingFlowNameEn,
    };
  });
};

const buildRootPtsShellPatch = (
  currentNode: {
    data: {
      nodeKind: LcaNodeKind;
      mode: ProcessMode;
      ptsUuid?: string;
      processUuid: string;
      name: string;
      location: string;
      referenceProduct: string;
      referenceProductFlowUuid?: string;
      referenceProductDirection?: "input" | "output";
      ptsPublishedVersion?: number;
      ptsPublishedArtifactId?: string;
    };
  },
  shellNodeRaw: Record<string, unknown> | null | undefined,
  shellInputs: FlowPort[],
  shellOutputs: FlowPort[],
  publishedVersion?: number,
  publishedArtifactId?: string,
) => ({
  nodeKind:
    (typeof shellNodeRaw?.node_kind === "string" ? (shellNodeRaw.node_kind as LcaNodeKind) : undefined) ??
    currentNode.data.nodeKind,
  mode:
    (shellNodeRaw?.mode === "balanced" || shellNodeRaw?.mode === "normalized"
      ? shellNodeRaw.mode
      : undefined) ?? currentNode.data.mode,
  ptsUuid:
    (typeof shellNodeRaw?.pts_uuid === "string" && shellNodeRaw.pts_uuid.trim().length > 0
      ? shellNodeRaw.pts_uuid
      : undefined) ?? currentNode.data.ptsUuid,
  processUuid:
    (typeof shellNodeRaw?.process_uuid === "string" && shellNodeRaw.process_uuid.trim().length > 0
      ? shellNodeRaw.process_uuid
      : undefined) ?? currentNode.data.processUuid,
  name:
    (typeof shellNodeRaw?.name === "string" && shellNodeRaw.name.trim().length > 0 ? shellNodeRaw.name : undefined) ??
    currentNode.data.name,
  location:
    (typeof shellNodeRaw?.location === "string" ? shellNodeRaw.location : undefined) ?? currentNode.data.location,
  referenceProduct:
    (typeof shellNodeRaw?.reference_product === "string" ? shellNodeRaw.reference_product : undefined) ??
    currentNode.data.referenceProduct,
  referenceProductFlowUuid:
    (typeof shellNodeRaw?.reference_product_flow_uuid === "string"
      ? shellNodeRaw.reference_product_flow_uuid
      : undefined) ?? currentNode.data.referenceProductFlowUuid,
  referenceProductDirection:
    (shellNodeRaw?.reference_product_direction === "input" || shellNodeRaw?.reference_product_direction === "output"
      ? shellNodeRaw.reference_product_direction
      : undefined) ?? currentNode.data.referenceProductDirection,
  inputs: shellInputs,
  outputs: shellOutputs,
  publishedVersion: publishedVersion ?? currentNode.data.ptsPublishedVersion,
  publishedArtifactId: publishedArtifactId ?? currentNode.data.ptsPublishedArtifactId,
});

const extractMarketSourceNameFromPortName = (name?: string): string | undefined => {
  const rawName = String(name ?? "").trim();
  const atIndex = rawName.lastIndexOf("@");
  if (atIndex < 0) {
    return undefined;
  }
  const suffix = rawName.slice(atIndex + 1).trim();
  return suffix || undefined;
};

const buildPtsDefaultVisiblePortHints = (ptsNode: {
  data: {
    inputs: FlowPort[];
    outputs: FlowPort[];
  };
}): Array<PtsDefaultVisiblePortHint & Record<string, unknown>> => {
  const hints: Array<PtsDefaultVisiblePortHint & Record<string, unknown>> = [];
  const append = (direction: "input" | "output", ports: FlowPort[]) => {
    ports.forEach((port) => {
      if (!port.flowUuid || !port.showOnNode) {
        return;
      }
      const sourceProcessName = port.sourceProcessName || extractMarketSourceNameFromPortName(port.name);
      hints.push({
        direction,
        flow_direction: direction,
        flowUuid: port.flowUuid,
        flow_uuid: port.flowUuid,
        name: port.name,
        flow_name: port.name,
        sourceProcessUuid: port.sourceProcessUuid,
        source_process_uuid: port.sourceProcessUuid,
        sourceProcessName,
        source_process_name: sourceProcessName,
        sourceNodeId: port.sourceNodeId,
        source_node_id: port.sourceNodeId,
      });
    });
  };
  append("input", ptsNode.data.inputs ?? []);
  append("output", ptsNode.data.outputs ?? []);
  return hints;
};

const getLciNodeValidationIssues = (graph: LcaGraphPayload): string[] => {
  const nodes = graph.nodes ?? [];
  const issues: string[] = [];
  for (const node of nodes) {
    if (node.node_kind !== "lci_dataset") {
      continue;
    }
    const intermediatesIn = (node.inputs ?? []).filter((p) => p.type !== "biosphere");
    const intermediatesOut = (node.outputs ?? []).filter((p) => p.type !== "biosphere");
    const totalIntermediate = intermediatesIn.length + intermediatesOut.length;
    if (node.mode !== "normalized") {
      issues.push(`${node.name}: LCI 必须为 normalized。`);
    }
    if (totalIntermediate !== 1) {
      issues.push(`${node.name}: LCI 必须且只能有 1 条中间流。`);
      continue;
    }
    const chosen = [...intermediatesOut, ...intermediatesIn][0];
    if (!chosen?.isProduct) {
      issues.push(`${node.name}: LCI 中间流必须定义为产品。`);
    }
  }
  return issues;
};

const getUnitProcessProductUnitGroupIssues = (graph: LcaGraphPayload): string[] => {
  const nodes = graph.nodes ?? [];
  const issues: string[] = [];
  const flowUnitGroupMap = new Map<string, string>();
  nodes.forEach((node) => {
    [...(node.inputs ?? []), ...(node.outputs ?? [])].forEach((port) => {
      const flowUuid = String(port.flowUuid ?? "").trim();
      const unitGroup = String(port.unitGroup ?? "").trim();
      if (flowUuid && unitGroup && !flowUnitGroupMap.has(flowUuid)) {
        flowUnitGroupMap.set(flowUuid, unitGroup);
      }
    });
  });

  const resolvePortUnitGroup = (
    port: NonNullable<LcaGraphPayload["nodes"]>[number]["outputs"][number] | undefined,
  ): string => {
    if (!port) {
      return "";
    }
    const flowUuid = String(port.flowUuid ?? "").trim();
    return flowUuid ? flowUnitGroupMap.get(flowUuid) ?? "" : "";
  };

  for (const node of nodes) {
    if (node.node_kind !== "unit_process" && node.node_kind !== "market_process") {
      continue;
    }
    const groups = Array.from(
      new Set(
        (node.outputs ?? [])
          .filter((port) => port.type !== "biosphere" && Boolean(port.isProduct))
          .map((port) => resolvePortUnitGroup(port))
          .filter((group) => group.length > 0),
      ),
    );
    if (groups.length > 1) {
      issues.push(`${node.name}: 多产品分配仅支持同一单位组，当前包含 ${groups.join(" / ")}。`);
    }
  }
  return issues;
};

const getDuplicateProcessUuidIssues = (graph: LcaGraphPayload): string[] => {
  const rows = graph.nodes ?? [];
  const byProcessUuid = new Map<string, Array<{ id: string; name: string }>>();
  for (const node of rows) {
    const key = String(node.process_uuid ?? "").trim();
    if (!key) {
      continue;
    }
    const list = byProcessUuid.get(key) ?? [];
    list.push({ id: node.id, name: node.name });
    byProcessUuid.set(key, list);
  }
  const issues: string[] = [];
  for (const [processUuid, nodes] of byProcessUuid.entries()) {
    if (nodes.length <= 1) {
      continue;
    }
    const details = nodes.map((item) => `${item.name}(${item.id})`).join("、");
    issues.push(`process_uuid=${processUuid} 在图中重复：${details}`);
  }
  return issues;
};

const getPtsInternalEdgeAmountIssues = (graph: LcaGraphPayload): string[] => {
  const metadata = graph.metadata as { canvases?: Array<Record<string, unknown>> } | undefined;
  const canvases = Array.isArray(metadata?.canvases) ? metadata?.canvases : [];
  const issues: string[] = [];
  const parseInputPortId = (handleId: unknown): string | undefined => {
    if (typeof handleId !== "string" || !handleId) {
      return undefined;
    }
    const prefixes = ["in:", "inl:", "inr:"];
    for (const prefix of prefixes) {
      if (handleId.startsWith(prefix)) {
        return handleId.slice(prefix.length);
      }
    }
    return undefined;
  };
  for (const canvas of canvases) {
    if (canvas.kind !== "pts_internal") {
      continue;
    }
    const canvasName = String(canvas.name ?? canvas.id ?? "PTS");
    const edges = Array.isArray(canvas.edges) ? canvas.edges : [];
    const nodes = Array.isArray(canvas.nodes) ? canvas.nodes : [];
    const nodeById = new Map<string, Record<string, unknown>>();
    for (const rawNode of nodes) {
      if (!rawNode || typeof rawNode !== "object") {
        continue;
      }
      const node = rawNode as Record<string, unknown>;
      const nodeId = String(node.id ?? "");
      if (!nodeId) {
        continue;
      }
      nodeById.set(nodeId, node);
    }
    for (const raw of edges) {
      if (!raw || typeof raw !== "object") {
        continue;
      }
      const edge = raw as Record<string, unknown>;
      const quantityMode = String(edge.quantityMode ?? edge.quantity_mode ?? "single");
      const flowName = String(edge.flowName ?? edge.flow_name ?? edge.flowUuid ?? edge.flow_uuid ?? "unknown_flow");
      const edgeId = String(edge.id ?? "");
      const flowUuid = String(edge.flowUuid ?? edge.flow_uuid ?? "").trim();
      const targetNodeId = String(edge.toNode ?? edge.target ?? "").trim();
      const targetNode = targetNodeId ? nodeById.get(targetNodeId) : undefined;
      const targetHandle = edge.targetHandle ?? edge.target_handle;
      const targetPortId = parseInputPortId(targetHandle);
      const targetInputs = Array.isArray(targetNode?.inputs) ? (targetNode?.inputs as Array<Record<string, unknown>>) : [];
      const targetPort =
        (targetPortId
          ? targetInputs.find((port) => String(port.id ?? "") === targetPortId && String(port.flowUuid ?? "") === flowUuid)
          : undefined) ?? targetInputs.find((port) => String(port.flowUuid ?? "") === flowUuid);

      const targetAmount = Number(targetPort?.amount);
      const fallbackAmount =
        quantityMode === "dual"
          ? Number(edge.consumerAmount ?? edge.consumer_amount ?? edge.amount ?? 0)
          : Number(edge.amount ?? edge.consumerAmount ?? edge.consumer_amount ?? 0);
      const derivedAmount = Number.isFinite(targetAmount) ? targetAmount : fallbackAmount;
      if (!Number.isFinite(derivedAmount) || derivedAmount <= 0) {
        issues.push(`${canvasName}: 边 ${edgeId || "(no-id)"} 的数量无效（${flowName}=${derivedAmount}），必须 > 0。`);
      }
    }
  }
  return issues;
};

const getPtsOutputSourceBindingIssues = (graph: LcaGraphPayload): string[] => {
  const issues: string[] = [];
  for (const node of graph.nodes ?? []) {
    if (node.node_kind !== "pts_module") {
      continue;
    }
    const outputs = (node.outputs ?? []).filter(
      (port) =>
        port.type !== "biosphere" &&
        (Boolean((port as { internalExposed?: boolean }).internalExposed) || Boolean(port.showOnNode)),
    );
    const flowCount = new Map<string, number>();
    for (const port of outputs) {
      const flowUuid = String(port.flowUuid ?? "").trim();
      if (!flowUuid) {
        continue;
      }
      flowCount.set(flowUuid, (flowCount.get(flowUuid) ?? 0) + 1);
    }
    for (const port of outputs) {
      const flowUuid = String(port.flowUuid ?? "").trim();
      if (!flowUuid || (flowCount.get(flowUuid) ?? 0) <= 1) {
        continue;
      }
      const sourceProcessUuid = String((port as { sourceProcessUuid?: string }).sourceProcessUuid ?? "").trim();
      const sourceNodeId = String((port as { sourceNodeId?: string }).sourceNodeId ?? "").trim();
      if (!sourceProcessUuid && !sourceNodeId) {
        issues.push(`${node.name}: 已暴露输出端口 ${port.name}（${flowUuid}）存在多来源，需绑定来源过程。`);
      }
    }
  }
  return issues;
};

const toFlowNameSyncState = (
  payload:
    | Partial<ProjectDetailResponse>
    | {
        flow_name_sync_needed?: boolean;
        outdated_flow_refs_count?: number;
        evidence?: Array<Record<string, unknown>>;
      }
    | null
    | undefined,
): FlowNameSyncState => ({
  needed: Boolean(payload?.flow_name_sync_needed),
  outdatedCount: Number(payload?.outdated_flow_refs_count ?? 0),
  examples: Array.isArray((payload as { outdated_flow_ref_examples?: Array<Record<string, unknown>> } | null)?.outdated_flow_ref_examples)
    ? ((payload as { outdated_flow_ref_examples?: Array<Record<string, unknown>> }).outdated_flow_ref_examples ?? [])
    : Array.isArray((payload as { evidence?: Array<Record<string, unknown>> } | null)?.evidence)
      ? ((payload as { evidence?: Array<Record<string, unknown>> }).evidence ?? [])
      : [],
});


const getLciaGroupLabel = (method: string): string => {
  const text = String(method ?? "").trim();
  if (!text) {
    return "未分类";
  }
  const firstSplit = text.split(/[，,]/)[0]?.trim() ?? text;
  const climatePrefix = firstSplit.split(/[-—]/)[0]?.trim() ?? firstSplit;
  return climatePrefix || text;
};

const formatApiError = (raw: unknown): string => {
  const text = String(raw ?? "");
  const jsonText = text.startsWith("Error: ") ? text.slice("Error: ".length) : text;
  try {
    const parsed = JSON.parse(jsonText) as {
      code?: string;
      message?: string;
      detail?: { code?: string; message?: string };
    };
    const payload = parsed.detail && typeof parsed.detail === "object" ? parsed.detail : parsed;
    if (payload.code === "AMBIGUOUS_PTS_OUTPUT_PRODUCER") {
      return "请为该外部输出端口指定来源过程（存在同 flowUuid 多生产者冲突）。";
    }
    if (payload.code === "INVALID_INTERNAL_EDGE_AMOUNT") {
      return "PTS 内部连线数量无效（需 > 0），请检查边数量设置。";
    }
    if (payload.code === "INVALID_EDGE_PORT_BINDING") {
      return "连线端口绑定无效：请检查 source/target 端口与 flowUuid 是否一致。";
    }
    if (payload.code === "DUPLICATE_EDGE_BINDING") {
      return "检测到重复连线绑定：同一来源端口与目标端口的重复边不允许。";
    }
    if (payload.code === "DUPLICATE_MARKET_PORT_SOURCE_PROCESS") {
      return "市场过程输入重复：同一 flow 下，来源过程不能重复连接到同一市场端口。";
    }
    if (payload.code === "NON_PRODUCT_INPUT_MULTI_PRODUCT_SOURCE") {
      return "非产品输入端口不允许多个产品来源：请改用市场过程进行汇聚。";
    }
    if (payload.code === "PTS_PUBLISHED_ARTIFACT_REQUIRED_FOR_MAIN_GRAPH_SAVE") {
      return "主图保存失败：存在未发布的 PTS，请先在 PTS 编辑界面完成发布。";
    }
    if (payload.code === "PTS_DRAFT_EMPTY_NOT_RUNNABLE") {
      return "主图暂时不能运行：存在一个空白草稿 PTS。请双击进入该 PTS，补充内部节点和连线并发布后，再回到主图运行。";
    }
    if (payload.code === "PTS_MAIN_GRAPH_STALE") {
      return "主图保存失败：当前 PTS 壳端口已过期，请先刷新主图中的 PTS 最新发布端口后再保存。";
    }
    return payload.message ? `${payload.message}` : text;
  } catch {
    return text;
  }
};

const normalizePtsModelingWarnings = (raw: unknown): PtsModelingWarning[] => {
  if (!Array.isArray(raw)) {
    return [];
  }
  const normalized: PtsModelingWarning[] = [];
  raw.forEach((item, index) => {
    if (typeof item === "string") {
      const message = item.trim();
      if (!message) {
        return;
      }
      normalized.push({
        code: `PTS_WARNING_${index + 1}`,
        severity: "warning",
        message,
        evidence: [],
      });
      return;
    }
    if (!item || typeof item !== "object") {
      return;
    }
    const obj = item as Record<string, unknown>;
    const message = String(obj.message ?? obj.detail ?? obj.code ?? "").trim();
    if (!message) {
      return;
    }
    const expectedTotalRaw = Number(obj.expected_total);
    const actualTotalRaw = Number(obj.actual_total);
    normalized.push({
      code: String(obj.code ?? `PTS_WARNING_${index + 1}`),
      severity: String(obj.severity ?? "warning"),
      message,
      ptsUuid: String(obj.pts_uuid ?? "").trim() || undefined,
      ptsNodeId: String(obj.pts_node_id ?? "").trim() || undefined,
      nodeName: String(obj.node_name ?? "").trim() || undefined,
      expectedTotal: Number.isFinite(expectedTotalRaw) ? expectedTotalRaw : undefined,
      actualTotal: Number.isFinite(actualTotalRaw) ? actualTotalRaw : undefined,
      evidence: Array.isArray(obj.evidence) ? obj.evidence : [],
    });
  });
  return normalized;
};

const formatPtsModelingWarningMessage = (warning: PtsModelingWarning, uiLanguage: "zh" | "en"): string => {
  const actualText = warning.actualTotal != null ? warning.actualTotal.toFixed(6) : "-";
  const expectedText = warning.expectedTotal != null ? `${warning.expectedTotal}` : "1";
  if (warning.code === "PTS_INPUT_SHARE_ZERO" || warning.code === "PTS_INPUT_SHARE_BELOW_TARGET") {
    return uiLanguage === "zh"
      ? `输入份额合计 ${actualText}（目标 ${expectedText}）`
      : `Input share total is ${actualText} (target ${expectedText})`;
  }
  return warning.message || warning.code;
};

const buildPtsPublishWarningStatusText = (
  warnings: PtsModelingWarning[],
  uiLanguage: "zh" | "en",
  mode: "publish" | "pack_finalize",
): string => {
  const first = warnings[0];
  const nodeLabel = first?.nodeName || first?.ptsUuid || first?.ptsNodeId || first?.code || "PTS";
  const detail = first ? formatPtsModelingWarningMessage(first, uiLanguage) : "";
  if (uiLanguage === "zh") {
    return mode === "publish"
      ? `警告：PTS保存成功，但市场份额总和不为1。示例：${nodeLabel}（${detail}）`
      : `警告：PTS封装并发布成功，但市场份额总和不为1。示例：${nodeLabel}（${detail}）`;
  }
  return mode === "publish"
    ? `Warning: PTS saved successfully, but market share total is not 1. Example: ${nodeLabel} (${detail})`
    : `Warning: PTS packed and published successfully, but market share total is not 1. Example: ${nodeLabel} (${detail})`;
};

export default function App() {
  const functionalUnit = useLcaGraphStore((state) => state.functionalUnit);
  const nodes = useLcaGraphStore((state) => state.nodes);
  const edges = useLcaGraphStore((state) => state.edges);
  const viewport = useLcaGraphStore((state) => state.viewport);
  const exportGraph = useLcaGraphStore((state) => state.exportGraph);
  const importGraph = useLcaGraphStore((state) => state.importGraph);
  const replacePtsInternalCanvasGraph = useLcaGraphStore((state) => state.replacePtsInternalCanvasGraph);
  const replaceRootPtsShell = useLcaGraphStore((state) => state.replaceRootPtsShell);
  const applyHandleValidationIssues = useLcaGraphStore((state) => state.applyHandleValidationIssues);
  const rebindRootEdgeHandlesForPtsShell = useLcaGraphStore((state) => state.rebindRootEdgeHandlesForPtsShell);
  const repairRootEdgeHandles = useLcaGraphStore((state) => state.rebindRootEdgeHandles);
  const autoConnectByUuid = useLcaGraphStore((state) => state.autoConnectByUuid);
  const connectionHint = useLcaGraphStore((state) => state.connectionHint);
  const connectionFix = useLcaGraphStore((state) => state.connectionFix);
  const clearConnectionHint = useLcaGraphStore((state) => state.clearConnectionHint);
  const applyConnectionFix = useLcaGraphStore((state) => state.applyConnectionFix);
  const goToParentCanvas = useLcaGraphStore((state) => state.goToParentCanvas);
  const activeCanvasKind = useLcaGraphStore((state) => state.activeCanvasKind);
  const activeCanvasId = useLcaGraphStore((state) => state.activeCanvasId);
  const canvases = useLcaGraphStore((state) => state.canvases);
  const getBalancedWarnings = useLcaGraphStore((state) => state.getBalancedWarnings);
  const getBalancedWarningsForCanvas = useLcaGraphStore((state) => state.getBalancedWarningsForCanvas);
  const getMarketWarnings = useLcaGraphStore((state) => state.getMarketWarnings);
  const flowAnimationEnabled = useLcaGraphStore((state) => state.flowAnimationEnabled);
  const setFlowAnimationEnabled = useLcaGraphStore((state) => state.setFlowAnimationEnabled);
  const autoPopupEnabled = useLcaGraphStore((state) => state.autoPopupEnabled);
  const setAutoPopupEnabled = useLcaGraphStore((state) => state.setAutoPopupEnabled);
  const unitAutoScaleEnabled = useLcaGraphStore((state) => state.unitAutoScaleEnabled);
  const setUnitAutoScaleEnabled = useLcaGraphStore((state) => state.setUnitAutoScaleEnabled);
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const setUiLanguage = useLcaGraphStore((state) => state.setUiLanguage);
  const updateEdgeData = useLcaGraphStore((state) => state.updateEdgeData);
  const pendingPtsCompileNodeId = useLcaGraphStore((state) => state.pendingPtsCompileNodeId);
  const consumePendingPtsCompile = useLcaGraphStore((state) => state.consumePendingPtsCompile);
  const openNodeInspector = useLcaGraphStore((state) => state.openNodeInspector);

  const [projectId, setProjectId] = useState("");
  const [version, setVersion] = useState("1");
  const [projectName, setProjectName] = useState(DEFAULT_PROJECT_NAME);
  const [projects, setProjects] = useState<ProjectResponse[]>([]);
  const [busy, setBusy] = useState(false);
  const [statusText, setStatusText] = useState("");
  const [ptsHistoryOpen, setPtsHistoryOpen] = useState(false);
  const [ptsHistoryLoading, setPtsHistoryLoading] = useState(false);
  const [ptsCompileHistory, setPtsCompileHistory] = useState<PtsVersionItem[]>([]);
  const [ptsPublishedHistory, setPtsPublishedHistory] = useState<PtsVersionItem[]>([]);
  const [ptsHistoryActivePublishedVersion, setPtsHistoryActivePublishedVersion] = useState<number | null>(null);
  const [lastRun, setLastRun] = useState<RunResponse | null>(null);
  const [showRunAnalysis, setShowRunAnalysis] = useState(false);
  const [showRunWarnings, setShowRunWarnings] = useState(false);
  const [ptsPublishWarnings, setPtsPublishWarnings] = useState<PtsModelingWarning[]>([]);
  const [showPtsPublishWarnings, setShowPtsPublishWarnings] = useState(false);
  const [showTargetProductDialog, setShowTargetProductDialog] = useState(false);
  const [canvasLoadKey, setCanvasLoadKey] = useState(0);
  const [selectedProductKey, setSelectedProductKey] = useState("");
  const [targetProductQuantityMode, setTargetProductQuantityMode] = useState<ProjectTargetQuantityMode>("custom");
  const [targetProductQuantity, setTargetProductQuantity] = useState("1");
  const [targetProductDraftKey, setTargetProductDraftKey] = useState("");
  const [targetProductDraftQuantityMode, setTargetProductDraftQuantityMode] = useState<ProjectTargetQuantityMode>("custom");
  const [targetProductDraftQuantity, setTargetProductDraftQuantity] = useState("1");
  const [resultProductViewKey, setResultProductViewKey] = useState("");
  const [resultProductViewMode, setResultProductViewMode] = useState<ResultProductViewMode>("target_total");
  const [expandedResultProcesses, setExpandedResultProcesses] = useState<string[]>([]);
  const [hydrated, setHydrated] = useState(false);
  const [ptsCanvasLoading, setPtsCanvasLoading] = useState(false);
  const [unitDefinitions, setUnitDefinitions] = useState<UnitDefinitionRow[]>([]);
  const [resultUnitMode, setResultUnitMode] = useState<ResultUnitMode>("defined");
  const [versionTraveling, setVersionTraveling] = useState(false);
  const [appMode, setAppMode] = useState<AppMode>("management");
  const [routeProjectId, setRouteProjectId] = useState(() => getProjectIdFromPathname(window.location.pathname));
  const [missingProductNodes, setMissingProductNodes] = useState<MissingProductNode[]>([]);
  const [flowNameSyncState, setFlowNameSyncState] = useState<FlowNameSyncState>({
    needed: false,
    outdatedCount: 0,
    examples: [],
  });
  const [dismissedNormalizedEdgeFixFingerprint, setDismissedNormalizedEdgeFixFingerprint] = useState("");
  const hydratedRef = useRef(false);
  const routeSyncRef = useRef("");
  const routeHydrationAttemptRef = useRef("");

  const syncProjectRoute = useCallback((nextProjectId: string, nextAppMode: AppMode, replace = false) => {
    const nextPath = nextAppMode === "editor" && nextProjectId ? buildProjectPathname(nextProjectId) : "/";
    const currentPath = window.location.pathname || "/";
    if (currentPath === nextPath) {
      routeSyncRef.current = nextPath;
      return;
    }
    const method = replace ? "replaceState" : "pushState";
    window.history[method](window.history.state, "", nextPath);
    routeSyncRef.current = nextPath;
    setRouteProjectId(getProjectIdFromPathname(nextPath));
  }, []);
  const navigateToManagement = useCallback(() => {
    setStatusText("");
    setAppMode("management");
    syncProjectRoute("", "management");
  }, [syncProjectRoute]);
  const lastSavedFingerprintRef = useRef<string>("");
  const backgroundSaveInFlightRef = useRef(false);
  const suppressAutoSaveRef = useRef(false);
  const suppressAutoSaveUntilRef = useRef(0);
  const ptsResourceHydrationRef = useRef("");
  const ptsAutoPublishInFlightRef = useRef("");
  const rootPtsProjectionHydrationRef = useRef<Record<string, string>>({});
  const ptsDraftInitInFlightRef = useRef<Record<string, true>>({});
  const ptsDraftInitializedRef = useRef<Record<string, true>>({});

  const bumpCanvasLoadKey = useCallback(() => {
    setCanvasLoadKey((prev) => prev + 1);
  }, []);

  const importGraphWithLoadKey = useCallback(
    (graph: LcaGraphPayload) => {
      bumpCanvasLoadKey();
      importGraph(graph);
    },
    [bumpCanvasLoadKey, importGraph],
  );

  const replacePtsInternalCanvasGraphWithLoadKey = useCallback(
    (ptsNodeId: string, graph: LcaGraphPayload, options?: { name?: string }) => {
      bumpCanvasLoadKey();
      replacePtsInternalCanvasGraph(ptsNodeId, graph, options);
    },
    [bumpCanvasLoadKey, replacePtsInternalCanvasGraph],
  );

  const graphFingerprint = useMemo(
    () => JSON.stringify({ nodes, edges, fu: functionalUnit, viewport }),
    [nodes, edges, functionalUnit, viewport],
  );
  const activeCanvas = useMemo(() => canvases[activeCanvasId], [activeCanvasId, canvases]);
  const activePtsNode = useMemo(() => {
    if (activeCanvasKind !== "pts_internal") {
      return undefined;
    }
    const ptsNodeId = activeCanvas?.parentPtsNodeId;
    if (!ptsNodeId) {
      return undefined;
    }
    return canvases.root?.nodes.find((node) => node.id === ptsNodeId && node.data.nodeKind === "pts_module");
  }, [activeCanvas, activeCanvasKind, canvases.root]);
  const activePtsUuid = activePtsNode?.data.ptsUuid ?? activePtsNode?.data.processUuid ?? "";
  const activePtsHydrationKey =
    activeCanvasKind === "pts_internal" && projectId && activePtsUuid && activeCanvasId
      ? `${projectId}:${activePtsUuid}:${activeCanvasId}`
      : "";
  const showPtsCanvasLoading =
    activeCanvasKind === "pts_internal" &&
    Boolean(activePtsHydrationKey) &&
    (ptsCanvasLoading || ptsResourceHydrationRef.current !== activePtsHydrationKey);

  const buildPtsInternalGraphPayload = useCallback((canvasId: string): LcaGraphPayload | null => {
    const canvas = canvases[canvasId];
    if (!canvas || canvas.kind !== "pts_internal") {
      return null;
    }
    return {
      functionalUnit: canvas.name || "PTS",
      metadata: {
        source: "pts_resource",
        canvas_id: canvas.id,
        parent_pts_node_id: canvas.parentPtsNodeId,
        node_positions: Object.fromEntries(
          canvas.nodes.map((node) => [
            node.id,
            {
              x: node.position.x,
              y: node.position.y,
            },
          ]),
        ),
      },
      nodes: canvas.nodes.map((node) => ({
          id: node.id,
          node_kind: node.data.nodeKind,
          mode: node.data.mode,
          market_allow_mixed_flows: node.data.marketAllowMixedFlows,
          lci_role: node.data.lciRole,
          pts_uuid: node.data.ptsUuid,
          process_uuid: node.data.processUuid,
          name: node.data.name,
          location: node.data.location,
          reference_product: node.data.referenceProduct,
          reference_product_flow_uuid: node.data.referenceProductFlowUuid,
          reference_product_direction: node.data.referenceProductDirection,
          inputs: node.data.inputs,
          outputs: node.data.outputs,
          position: {
            x: node.position.x,
            y: node.position.y,
          },
        })) as LcaGraphPayload["nodes"],
      exchanges: canvas.edges.map((edge) => ({
        id: edge.id,
        fromNode: edge.source,
        toNode: edge.target,
        sourceHandle: edge.sourceHandle ?? undefined,
        targetHandle: edge.targetHandle ?? undefined,
        flowUuid: edge.data?.flowUuid ?? "",
        flowName: edge.data?.flowName ?? "",
        quantityMode: edge.data?.quantityMode ?? "single",
        amount: edge.data?.amount ?? 0,
        providerAmount: edge.data?.providerAmount,
        consumerAmount: edge.data?.consumerAmount,
        unit: edge.data?.unit ?? "kg",
        type: edge.data?.type ?? "technosphere",
        allocation: edge.data?.allocation ?? "none",
        dbMapping: edge.data?.dbMapping,
      })),
    };
  }, [canvases]);

  const buildPtsCompileFallbackGraph = useCallback((params: {
    ptsCanvasId: string;
    ptsNodeId: string;
    ptsUuid: string;
  }): LcaGraphPayload | null => {
    const ptsGraph = buildPtsInternalGraphPayload(params.ptsCanvasId);
    const ptsNode = canvases.root?.nodes.find(
      (node) => node.id === params.ptsNodeId && node.data.nodeKind === "pts_module",
    );
    const ptsCanvas = canvases[params.ptsCanvasId];
    if (!ptsGraph || !ptsNode) {
      return null;
    }
    const shellNode = {
      id: ptsNode.id,
      node_kind: "pts_module" as const,
      mode: ptsNode.data.mode ?? "normalized",
      pts_uuid: params.ptsUuid,
      process_uuid: ptsNode.data.processUuid,
      name: ptsNode.data.name,
      location: ptsNode.data.location,
      reference_product: ptsNode.data.referenceProduct,
      reference_product_flow_uuid: ptsNode.data.referenceProductFlowUuid,
      reference_product_direction: ptsNode.data.referenceProductDirection,
      inputs: [],
      outputs: [],
    };
    return normalizeGraphPayload({
      ...ptsGraph,
      nodes: [...ptsGraph.nodes, shellNode],
      metadata: {
        ...(ptsGraph.metadata ?? {}),
        canvases: [
          {
            id: "root",
            name: "主图",
            kind: "root",
            nodes: [shellNode],
            edges: [],
          },
          {
            id: params.ptsCanvasId,
            name: ptsCanvas?.name ?? ptsGraph.functionalUnit ?? "PTS",
            kind: "pts_internal",
            parentPtsNodeId: ptsNode.id,
            nodes: ptsGraph.nodes,
            edges: ptsGraph.exchanges,
          },
        ],
      },
    });
  }, [buildPtsInternalGraphPayload, canvases.root, canvases]);

  const buildPtsResourcePayloadForNode = useCallback(async (
    params: {
      ptsNodeId: string;
      ptsCanvasId: string;
      ptsUuid: string;
    },
    options?: { includePortsPolicy?: boolean },
  ) => {
    const ptsNode = canvases.root?.nodes.find(
      (node) => node.id === params.ptsNodeId && node.data.nodeKind === "pts_module",
    );
    const ptsGraph = buildPtsInternalGraphPayload(params.ptsCanvasId);
    if (!ptsNode || !ptsGraph) {
      return null;
    }
    const currentRootInputs = (ptsNode.data.inputs ?? [])
      .map((port) => normalizePtsProjectedPort(port as unknown as Record<string, unknown>, "inputs"))
      .filter(Boolean);
    const currentRootOutputs = (ptsNode.data.outputs ?? [])
      .map((port) => normalizePtsProjectedPort(port as unknown as Record<string, unknown>, "outputs"))
      .filter(Boolean);
    let baseShellInputs = currentRootInputs;
    let baseShellOutputs = currentRootOutputs;
    const shouldFetchExistingResource = Boolean(
      options?.includePortsPolicy ||
      ptsNode.data.ptsPublishedVersion ||
      ptsNode.data.ptsPublishedArtifactId,
    );
    if (shouldFetchExistingResource) {
      try {
        const resourceResp = await fetch(`${API_BASE}/pts/${encodeURIComponent(params.ptsUuid)}`);
        if (resourceResp.ok) {
          const payload = (await resourceResp.json()) as PtsResourceResponse;
          const shellNode = payload.shell_node ?? {};
          const shellInputs = (Array.isArray(shellNode.inputs) ? shellNode.inputs : [])
            .map((port) => normalizePtsProjectedPort(port as Record<string, unknown>, "inputs"))
            .filter(Boolean);
          const shellOutputs = (Array.isArray(shellNode.outputs) ? shellNode.outputs : [])
            .map((port) => normalizePtsProjectedPort(port as Record<string, unknown>, "outputs"))
            .filter(Boolean);
          if (shellInputs.length > 0) {
            baseShellInputs = shellInputs;
          }
          if (shellOutputs.length > 0) {
            baseShellOutputs = shellOutputs;
          }
        }
      } catch {
        // fall back to current root shell only when backend resource is not available yet
      }
    }
    const includePortsPolicy = options?.includePortsPolicy === true;
    const portsPolicyInputs = includePortsPolicy
      ? mergePtsPortsWithRootOverrides("input", currentRootInputs, currentRootInputs)
      : undefined;
    const portsPolicyOutputs = includePortsPolicy
      ? mergePtsPortsWithRootOverrides("output", currentRootOutputs, currentRootOutputs)
      : undefined;
    const preserveBackendShellVisibility = shouldFetchExistingResource;
    const shellInputs = mergePtsPortsWithRootOverrides("input", baseShellInputs, currentRootInputs, {
      preserveVisibilityFromBase: preserveBackendShellVisibility,
    });
    const shellOutputs = mergePtsPortsWithRootOverrides("output", baseShellOutputs, currentRootOutputs, {
      preserveVisibilityFromBase: preserveBackendShellVisibility,
    });
    const requestBody = {
      project_id: projectId,
      name: ptsNode.data.name,
      pts_node_id: ptsNode.id,
      pts_graph: ptsGraph,
      ...(includePortsPolicy
        ? {
            ports_policy: {
              inputs: portsPolicyInputs ?? [],
              outputs: portsPolicyOutputs ?? [],
            },
          }
        : {}),
      shell_node: {
        id: ptsNode.id,
        node_kind: ptsNode.data.nodeKind,
        mode: ptsNode.data.mode,
        pts_uuid: ptsNode.data.ptsUuid,
        process_uuid: ptsNode.data.processUuid,
        name: ptsNode.data.name,
        location: ptsNode.data.location,
        reference_product: ptsNode.data.referenceProduct,
        reference_product_flow_uuid: ptsNode.data.referenceProductFlowUuid,
        reference_product_direction: ptsNode.data.referenceProductDirection,
        inputs: shellInputs,
        outputs: shellOutputs,
        emissions: [],
      },
    };
    debugPts("buildPtsResourcePayloadForNode", {
      ptsNodeId: ptsNode.id,
      ptsUuid: params.ptsUuid,
      includePortsPolicy,
      portsPolicyInputs,
      portsPolicyOutputs,
      shellInputs,
      shellOutputs,
    });
    return requestBody;
  }, [buildPtsInternalGraphPayload, canvases.root, projectId]);

  const persistPtsResourceForNode = useCallback(async (params: {
    ptsNodeId: string;
    ptsCanvasId: string;
    ptsUuid: string;
  }) => {
    if (!projectId) {
      return;
    }
    const requestBody = await buildPtsResourcePayloadForNode(params, { includePortsPolicy: true });
    if (!requestBody) {
      return;
    }
    const response = await fetch(`${API_BASE}/pts/${encodeURIComponent(params.ptsUuid)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody),
    });
    if (!response.ok) {
      throw new Error((await response.text()) || `persist pts failed: ${response.status}`);
    }
  }, [buildPtsResourcePayloadForNode, projectId]);

  const persistPtsResource = useCallback(async () => {
    if (activeCanvasKind !== "pts_internal" || !activeCanvas || !activePtsNode || !activePtsUuid) {
      return;
    }
    await persistPtsResourceForNode({
      ptsNodeId: activePtsNode.id,
      ptsCanvasId: activeCanvas.id,
      ptsUuid: activePtsUuid,
    });
  }, [activeCanvas, activeCanvasKind, activePtsNode, activePtsUuid, persistPtsResourceForNode]);

  const refreshSingleRootPtsProjectionFromBackend = useCallback(
    async (
      ptsNodeId: string,
      ptsUuid: string,
      targetProjectId?: string,
      options?: { rebindEdges?: boolean; resetVisibility?: boolean; preferredVisiblePortIds?: string[] },
    ) => {
      const effectiveProjectId = targetProjectId ?? projectId;
      if (!effectiveProjectId || !ptsNodeId || !ptsUuid) {
        return;
      }
      const currentNode = useLcaGraphStore
        .getState()
        .canvases.root?.nodes.find((node) => node.id === ptsNodeId && node.data.nodeKind === "pts_module");
      if (!currentNode) {
        return;
      }
      debugPts("refreshSingleRootPtsProjectionFromBackend:start", {
        projectId: effectiveProjectId,
        ptsNodeId,
        ptsUuid,
        rebindEdges: options?.rebindEdges !== false,
      });
      const resourceResp = await fetch(`${API_BASE}/pts/${encodeURIComponent(ptsUuid)}`);
      if (!resourceResp.ok) {
        return;
      }
      const resourcePayload = (await resourceResp.json()) as PtsResourceResponse;
      const params = new URLSearchParams({ project_id: effectiveProjectId });
      const portsResp = await fetch(`${API_BASE}/pts/${encodeURIComponent(ptsUuid)}/ports?${params.toString()}`);
      if (!portsResp.ok) {
        return;
      }
      const portsPayload = (await portsResp.json()) as PtsPortsResponse;
        const nextInputsRaw = (portsPayload.ports?.inputs ?? []).map((port) => normalizePtsProjectedPort(port, "inputs")).filter(Boolean);
        const nextOutputsRaw = (portsPayload.ports?.outputs ?? []).map((port) => normalizePtsProjectedPort(port, "outputs")).filter(Boolean);
      const shellNode = resourcePayload.shell_node ?? {};
      const shellPublishedArtifactId =
        typeof shellNode.pts_published_artifact_id === "string" ? shellNode.pts_published_artifact_id : undefined;
        const shellPatch = buildRootPtsShellPatch(
          currentNode,
          shellNode,
          mergeProjectedPortsWithRootVisibility(currentNode.data.inputs, nextInputsRaw, {
            resetVisibility: options?.resetVisibility,
            preferredVisiblePortIds: options?.preferredVisiblePortIds,
            debugLabel: "refreshSingleRootPtsProjectionFromBackend:mergeInputs",
          }),
          mergeProjectedPortsWithRootVisibility(currentNode.data.outputs, nextOutputsRaw, {
            resetVisibility: options?.resetVisibility,
            preferredVisiblePortIds: options?.preferredVisiblePortIds,
            debugLabel: "refreshSingleRootPtsProjectionFromBackend:mergeOutputs",
          }),
          resourcePayload.active_published_version ?? portsPayload.published_version ?? currentNode.data.ptsPublishedVersion,
          shellPublishedArtifactId ?? currentNode.data.ptsPublishedArtifactId,
        );
      replaceRootPtsShell(ptsNodeId, {
        nodeKind: shellPatch.nodeKind,
        mode: shellPatch.mode,
        ptsUuid: shellPatch.ptsUuid,
        processUuid: shellPatch.processUuid,
        name: shellPatch.name,
        location: shellPatch.location,
        referenceProduct: shellPatch.referenceProduct,
        referenceProductFlowUuid: shellPatch.referenceProductFlowUuid,
        referenceProductDirection: shellPatch.referenceProductDirection,
        inputs: shellPatch.inputs,
        outputs: shellPatch.outputs,
        publishedVersion: shellPatch.publishedVersion,
        publishedArtifactId: shellPatch.publishedArtifactId,
      });
      if (options?.rebindEdges !== false) {
        rebindRootEdgeHandlesForPtsShell(ptsNodeId, {
          inputs: shellPatch.inputs,
          outputs: shellPatch.outputs,
        });
      }
      debugPts("refreshSingleRootPtsProjectionFromBackend:end", {
        ptsNodeId,
        ptsUuid,
        inputs: nextInputsRaw.map((port) => ({ id: port.id, flowUuid: port.flowUuid, showOnNode: port.showOnNode })),
        outputs: nextOutputsRaw.map((port) => ({ id: port.id, flowUuid: port.flowUuid, showOnNode: port.showOnNode })),
      });
    },
    [projectId, rebindRootEdgeHandlesForPtsShell, replaceRootPtsShell],
  );

  const applySingleRootPtsProjection = useCallback(
    (
      ptsNodeId: string,
      shellNodeRaw: Record<string, unknown> | null | undefined,
      shellInputs: FlowPort[],
      shellOutputs: FlowPort[],
      options?: {
        publishedVersion?: number | null;
        publishedArtifactId?: string;
        rebindEdges?: boolean;
      },
    ) => {
      const currentNode = useLcaGraphStore
        .getState()
        .canvases.root?.nodes.find((node) => node.id === ptsNodeId && node.data.nodeKind === "pts_module");
      if (!currentNode) {
        return;
      }
      const shellPatch = buildRootPtsShellPatch(
        currentNode,
        shellNodeRaw,
        shellInputs,
        shellOutputs,
        options?.publishedVersion ?? currentNode.data.ptsPublishedVersion,
        options?.publishedArtifactId ?? currentNode.data.ptsPublishedArtifactId,
      );
      replaceRootPtsShell(ptsNodeId, {
        nodeKind: shellPatch.nodeKind,
        mode: shellPatch.mode,
        ptsUuid: shellPatch.ptsUuid,
        processUuid: shellPatch.processUuid,
        name: shellPatch.name,
        location: shellPatch.location,
        referenceProduct: shellPatch.referenceProduct,
        referenceProductFlowUuid: shellPatch.referenceProductFlowUuid,
        referenceProductDirection: shellPatch.referenceProductDirection,
        inputs: shellPatch.inputs,
        outputs: shellPatch.outputs,
        publishedVersion: shellPatch.publishedVersion,
        publishedArtifactId: shellPatch.publishedArtifactId,
      });
      if (options?.rebindEdges !== false) {
        rebindRootEdgeHandlesForPtsShell(ptsNodeId, {
          inputs: shellPatch.inputs,
          outputs: shellPatch.outputs,
        });
      }
    },
    [rebindRootEdgeHandlesForPtsShell, replaceRootPtsShell],
  );


  const i18n = useMemo(
    () =>
      uiLanguage === "zh"
        ? {
            appTitle: "星云LCA",
            projectNameLabel: "项目名称",
            backHome: "返回主页",
            createProject: "新建项目",
            deleteProject: "删除项目",
            atLeastOneProject: "至少保留一个项目",
            deleteCurrentProject: "删除当前项目",
            ptsMode: "PTS模块编辑模式",
            save: "保存",
            publish: "发布",
            history: "版本历史",
            saveBack: "返回主图",
            run: "计算",
            result: "结果分析",
            close: "关闭",
            summary: "Summary",
            issues: "内容",
          }
        : {
            appTitle: "Nebula LCA",
            projectNameLabel: "Project",
            backHome: "Back Home",
            createProject: "New Project",
            deleteProject: "Delete Project",
            atLeastOneProject: "Keep at least one project",
            deleteCurrentProject: "Delete current project",
            ptsMode: "PTS Module Edit Mode",
            save: "Save",
            publish: "Publish",
            history: "History",
            saveBack: "Back To Model",
            run: "Compute",
            result: "Run Analysis",
            close: "Close",
            summary: "Summary",
            issues: "Message",
          },
    [uiLanguage],
  );

  const fetchProjectFlowSyncState = useCallback(async (targetProjectId: string): Promise<FlowNameSyncState> => {
    const response = await fetch(`${API_BASE}/projects/${encodeURIComponent(targetProjectId)}`);
    if (!response.ok) {
      throw new Error(await response.text());
    }
    const payload = (await response.json()) as ProjectDetailResponse;
    return toFlowNameSyncState(payload);
  }, []);

  const loadProjectGraph = useCallback(
    async (targetProjectId: string, targetProjectName?: string) => {
      setStatusText("项目读取中...");
      try {
        const latestResp = await fetch(`${API_BASE}/projects/${encodeURIComponent(targetProjectId)}/latest`);
        if (latestResp.ok) {
        const latest = (await latestResp.json()) as ModelVersionResponse;
        debugPts("loadProjectGraph:latest:raw", {
          projectId: latest.project_id,
          version: latest.version,
          ptsNodeIds: ((latest.graph?.nodes ?? []) as Array<Record<string, unknown>>)
            .filter((node) => String(node.node_kind ?? "") === "pts_module")
            .map((node) => ({
              id: node.id,
              ptsUuid: node.pts_uuid,
              inputs: summarizeRawPtsPortsForDebug(node.inputs),
              outputs: summarizeRawPtsPortsForDebug(node.outputs),
            })),
        });
        const normalizedLatestGraph = normalizeGraphPayload(latest.graph);
          debugPts("loadProjectGraph:latest", {
            projectId: latest.project_id,
            version: latest.version,
            nodeCount: normalizedLatestGraph.nodes?.length ?? 0,
            edgeCount: normalizedLatestGraph.exchanges?.length ?? 0,
            ptsNodeIds: (normalizedLatestGraph.nodes ?? [])
              .filter((node) => node.node_kind === "pts_module")
              .map((node) => ({
                id: node.id,
                ptsUuid: node.pts_uuid,
                inputs: summarizePtsPortsForDebug(node.inputs as FlowPort[] | undefined),
                outputs: summarizePtsPortsForDebug(node.outputs as FlowPort[] | undefined),
              })),
            edges: (normalizedLatestGraph.exchanges ?? []).map((edge) => ({
              id: edge.id,
              sourceHandle: edge.sourceHandle,
              targetHandle: edge.targetHandle,
              source_port_id: edge.source_port_id,
              target_port_id: edge.target_port_id,
              flowUuid: edge.flowUuid,
            })),
          });
          ptsResourceHydrationRef.current = "";
          rootPtsProjectionHydrationRef.current = {};
          importGraphWithLoadKey(normalizedLatestGraph);
          const latestHandleValidation = (latest as { handle_validation?: { issues?: Array<Record<string, unknown>> } }).handle_validation;
          if (Array.isArray(latestHandleValidation?.issues) && latestHandleValidation.issues.length > 0) {
            applyHandleValidationIssues(latestHandleValidation.issues as Array<{ edge_id?: string; suggested_source_port_id?: string; suggested_target_port_id?: string }>);
          }
          try {
            setFlowNameSyncState(await fetchProjectFlowSyncState(targetProjectId));
          } catch {
            setFlowNameSyncState({ needed: false, outdatedCount: 0, examples: [] });
          }
          lastSavedFingerprintRef.current = JSON.stringify(normalizedLatestGraph);
          suppressAutoSaveUntilRef.current = Date.now() + 30000;
          {
            const targetProductConfig = readProjectTargetProductConfig(normalizedLatestGraph);
            setSelectedProductKey(
              targetProductConfig ? buildProjectTargetMatchKey(targetProductConfig.processUuid, targetProductConfig.flowUuid) : "",
            );
            setTargetProductQuantityMode(targetProductConfig?.quantityMode ?? "custom");
            setTargetProductQuantity(String(targetProductConfig?.quantity ?? 1));
          }
          setProjectId(latest.project_id);
          setProjectName(targetProjectName ?? latest.project_id);
          setVersion(String(latest.version));
          localStorage.setItem(CURRENT_PROJECT_KEY, latest.project_id);
          localStorage.setItem(
            snapshotKey(latest.project_id),
            JSON.stringify({ project_id: latest.project_id, version: latest.version }),
          );
          setVersionTraveling(false);
          setStatusText(`已恢复项目: ${targetProjectName ?? latest.project_id} (version=${latest.version})`);
          return;
        }

        const currentDraftRaw = localStorage.getItem(draftKey(targetProjectId));
        if (currentDraftRaw) {
          const draft = JSON.parse(currentDraftRaw) as LcaGraphPayload;
          ptsResourceHydrationRef.current = "";
          rootPtsProjectionHydrationRef.current = {};
          importGraphWithLoadKey(normalizeGraphPayload(draft));
          try {
            setFlowNameSyncState(await fetchProjectFlowSyncState(targetProjectId));
          } catch {
            setFlowNameSyncState({ needed: false, outdatedCount: 0, examples: [] });
          }
          lastSavedFingerprintRef.current = "";
          {
            const targetProductConfig = readProjectTargetProductConfig(draft);
            setSelectedProductKey(
              targetProductConfig ? buildProjectTargetMatchKey(targetProductConfig.processUuid, targetProductConfig.flowUuid) : "",
            );
            setTargetProductQuantityMode(targetProductConfig?.quantityMode ?? "custom");
            setTargetProductQuantity(String(targetProductConfig?.quantity ?? 1));
          }
          setProjectId(targetProjectId);
          setProjectName(targetProjectName ?? targetProjectId);
          setVersion("0");
          setVersionTraveling(false);
          setStatusText(`已恢复草稿: ${targetProjectName ?? targetProjectId}`);
          return;
        }

        ptsResourceHydrationRef.current = "";
        rootPtsProjectionHydrationRef.current = {};
        importGraphWithLoadKey({ functionalUnit: "1 kg 对二甲苯", nodes: [], exchanges: [], metadata: {} });
        setFlowNameSyncState({ needed: false, outdatedCount: 0, examples: [] });
        lastSavedFingerprintRef.current = "";
        setSelectedProductKey("");
        setTargetProductQuantityMode("custom");
        setTargetProductQuantity("1");
        setProjectId(targetProjectId);
        setProjectName(targetProjectName ?? targetProjectId);
        setVersion("0");
        setVersionTraveling(false);
        setStatusText(`项目为空: ${targetProjectName ?? targetProjectId}`);
      } catch {
        setStatusText("项目读取失败，已保留当前草稿。");
      } finally {
        setTimeout(() => {
          setStatusText((prev) => (prev === "项目读取中..." ? "" : prev));
        }, 600);
      }
    },
    [fetchProjectFlowSyncState, importGraphWithLoadKey],
  );

  const loadProjectVersion = useCallback(
    async (targetProjectId: string, targetVersion: number, targetProjectName?: string) => {
      if (!targetProjectId || !Number.isFinite(targetVersion) || targetVersion < 1) {
        return;
      }
      setStatusText(`正在切换到历史版本 v${targetVersion}...`);
      setBusy(true);
      try {
        const resp = await fetch(
          `${API_BASE}/projects/${encodeURIComponent(targetProjectId)}/versions/${encodeURIComponent(String(targetVersion))}`,
        );
        if (resp.ok) {
          const payload = (await resp.json()) as ModelVersionResponse;
          const normalizedPayloadGraph = normalizeGraphPayload(payload.graph);
          debugPts("loadProjectVersion:payload", {
            projectId: payload.project_id,
            version: payload.version,
            nodeCount: normalizedPayloadGraph.nodes?.length ?? 0,
            edgeCount: normalizedPayloadGraph.exchanges?.length ?? 0,
            ptsNodeIds: (normalizedPayloadGraph.nodes ?? [])
              .filter((node) => node.node_kind === "pts_module")
              .map((node) => ({
                id: node.id,
                ptsUuid: node.pts_uuid,
                inputs: summarizePtsPortsForDebug(node.inputs as FlowPort[] | undefined),
                outputs: summarizePtsPortsForDebug(node.outputs as FlowPort[] | undefined),
              })),
          });
          ptsResourceHydrationRef.current = "";
          rootPtsProjectionHydrationRef.current = {};
          importGraphWithLoadKey(normalizedPayloadGraph);
          const versionHandleValidation = (payload as { handle_validation?: { issues?: Array<Record<string, unknown>> } }).handle_validation;
          if (Array.isArray(versionHandleValidation?.issues) && versionHandleValidation.issues.length > 0) {
            applyHandleValidationIssues(versionHandleValidation.issues as Array<{ edge_id?: string; suggested_source_port_id?: string; suggested_target_port_id?: string }>);
          }
          try {
            setFlowNameSyncState(await fetchProjectFlowSyncState(targetProjectId));
          } catch {
            setFlowNameSyncState({ needed: false, outdatedCount: 0, examples: [] });
          }
          lastSavedFingerprintRef.current = JSON.stringify(normalizedPayloadGraph);
          suppressAutoSaveUntilRef.current = Date.now() + 30000;
          {
            const targetProductConfig = readProjectTargetProductConfig(normalizedPayloadGraph);
            setSelectedProductKey(
              targetProductConfig ? buildProjectTargetMatchKey(targetProductConfig.processUuid, targetProductConfig.flowUuid) : "",
            );
            setTargetProductQuantityMode(targetProductConfig?.quantityMode ?? "custom");
            setTargetProductQuantity(String(targetProductConfig?.quantity ?? 1));
          }
          setProjectId(payload.project_id);
          setVersion(String(payload.version));
          localStorage.setItem(CURRENT_PROJECT_KEY, payload.project_id);
          localStorage.setItem(
            snapshotKey(payload.project_id),
            JSON.stringify({ project_id: payload.project_id, version: payload.version }),
          );
          setVersionTraveling(true);
          setStatusText(`已恢复项目: ${targetProjectName ?? payload.project_id} (version=${payload.version})`);
          return;
        }
        if (resp.status === 404) {
          setStatusText(`版本 ${targetVersion} 不存在（可能已到最早/最新边界）。`);
          return;
        }
        throw new Error(await resp.text());
      } catch (error) {
        setStatusText(`历史版本读取失败: ${formatApiError(error)}`);
      } finally {
        setBusy(false);
      }
    },
    [fetchProjectFlowSyncState, importGraphWithLoadKey],
  );
  const navigateVersion = useCallback(
    (step: -1 | 1) => {
      if (busy || !projectId) {
        return;
      }
      const currentVersion = Number(version);
      if (!Number.isFinite(currentVersion) || currentVersion < 1) {
        return;
      }
      const targetVersion = currentVersion + step;
      if (targetVersion < 1) {
        setStatusText("已到最早版本。");
        return;
      }
      const latestKnown = projects.find((item) => item.project_id === projectId)?.latest_version;
      if (step > 0 && Number.isFinite(Number(latestKnown)) && targetVersion > Number(latestKnown)) {
        setStatusText("已到最新版本。");
        return;
      }
      const targetProjectName = projects.find((item) => item.project_id === projectId)?.name ?? projectName;
      void loadProjectVersion(projectId, targetVersion, targetProjectName);
    },
    [busy, loadProjectVersion, projectId, projectName, projects, version],
  );
  const refreshProjects = useCallback(async (): Promise<ProjectResponse[]> => {
    const response = await fetch(`${API_BASE}/projects`);
    if (!response.ok) {
      throw new Error(await response.text());
    }
    const payload = (await response.json()) as ProjectResponse[] | ProjectListPagedResponse;
    const rows = Array.isArray(payload) ? payload : Array.isArray(payload.items) ? payload.items : [];
    setProjects(rows);
    return rows;
  }, []);

  const syncProjectFlowNames = useCallback(async () => {
    if (!projectId) {
      return;
    }
    setBusy(true);
    try {
      const response = await fetch(`${API_BASE}/projects/${encodeURIComponent(projectId)}/sync-flow-names`, {
        method: "POST",
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const target = projects.find((item) => item.project_id === projectId);
      await loadProjectGraph(projectId, target?.name ?? projectName);
      setStatusText("已同步当前项目的 flow 名称。");
    } catch (error) {
      setStatusText(`同步 flow 信息失败: ${formatApiError(error)}`);
    } finally {
      setBusy(false);
    }
  }, [loadProjectGraph, projectId, projectName, projects]);

  const hydrate = useCallback(async () => {
    try {
      const rows = await refreshProjects();
      if (!rows.length) {
        setAppMode("management");
        return;
      }
      const routeTargetProjectId = getProjectIdFromPathname(window.location.pathname);
      if (routeTargetProjectId) {
        const target = rows.find((item) => item.project_id === routeTargetProjectId);
        routeHydrationAttemptRef.current = routeTargetProjectId;
        setAppMode("editor");
        await loadProjectGraph(routeTargetProjectId, target?.name ?? routeTargetProjectId);
      }
    } catch {
      setStatusText("项目列表读取失败。");
    } finally {
      setHydrated(true);
    }
  }, [loadProjectGraph, refreshProjects]);

  const persistModel = useCallback(
    async (mode: SaveMode) => {
      const strictValidation = mode === "manual";
      const backgroundSave = mode !== "manual";
      const nonBlockingWarnings: string[] = [];
      if (backgroundSave && suppressAutoSaveRef.current) {
        return;
      }
      if (backgroundSave && Date.now() < suppressAutoSaveUntilRef.current) {
        return;
      }
      if (busy && mode !== "manual") {
        return;
      }
      if (backgroundSave && backgroundSaveInFlightRef.current) {
        return;
      }
      if (mode !== "manual" && versionTraveling) {
        return;
      }
      if (mode === "manual" && versionTraveling) {
        setVersionTraveling(false);
      }
        if (flowNameSyncState.needed) {
          const warningText =
            mode === "manual"
              ? uiLanguage === "zh"
                ? `未保存：当前项目有 ${flowNameSyncState.outdatedCount || "部分"} 个流名称与流库不一致，请先同步 flow 信息。`
                : `Not saved: the current project has ${flowNameSyncState.outdatedCount || "some"} flow name mismatches. Please sync flow info first.`
              : uiLanguage === "zh"
                ? "自动保存已跳过：当前项目存在未同步的 flow 名称，请先同步 flow 信息。"
                : "Autosave skipped: the current project has unsynced flow names. Please sync flow info first.";
          setStatusText(warningText);
          return;
        }
      if (strictValidation && activeCanvasKind === "pts_internal") {
        const ptsWarnings = getBalancedWarningsForCanvas(activeCanvasId);
        if (ptsWarnings.length > 0) {
          const first = ptsWarnings[0];
          setStatusText(
            `未保存：PTS 内存在 ${ptsWarnings.length} 条未配平守恒关系。示例：${first.flowName}（输出=${first.outputTotal.toFixed(
              6,
            )}，外售=${first.externalSaleTotal.toFixed(6)}，输入=${first.inputTotal.toFixed(6)}）。`,
          );
          return;
        }
      }
      if (strictValidation) {
        const marketWarnings = getMarketWarnings();
        if (marketWarnings.length > 0) {
          const first = marketWarnings[0];
          nonBlockingWarnings.push(
            `市场过程校验失败 ${marketWarnings.length} 项。示例：${first.nodeName}（${first.issues[0]}）。`,
          );
        }
        const balancedWarnings = getBalancedWarnings();
        if (balancedWarnings.length > 0) {
          const first = balancedWarnings[0];
          nonBlockingWarnings.push(
            `存在 ${balancedWarnings.length} 条未匹配流。示例：${first.flowName}（输出=${first.outputTotal.toFixed(6)}，输入=${first.inputTotal.toFixed(6)}，外售=${first.externalSaleTotal.toFixed(6)}，差值=${(first.outputTotal - first.externalSaleTotal - first.inputTotal).toFixed(6)}）。`,
          );
        }
      }

      if (activeCanvasKind === "pts_internal") {
        try {
          await persistPtsResource();
          if (mode !== "manual") {
            return;
          }
        } catch (error) {
          setStatusText(`PTS 独立保存失败: ${formatApiError(error)}`);
          return;
        }
      }

      repairRootEdgeHandles();
      let graph = applyProjectTargetProductConfig(
        normalizeGraphPayload(exportGraph()),
        selectedProductKey
          ? {
              processUuid: selectedProductKey.split("::")[0] ?? "",
              flowUuid: selectedProductKey.split("::")[1] ?? "",
              quantityMode: targetProductQuantityMode,
              quantity: parseTargetProductQuantity(targetProductQuantity),
            }
          : null,
      );
      if (strictValidation) {
        const ptsOutputBindingIssues = getPtsOutputSourceBindingIssues(graph);
        if (ptsOutputBindingIssues.length > 0) {
          setStatusText(`未保存：PTS 输出来源绑定缺失 ${ptsOutputBindingIssues.length} 项。示例：${ptsOutputBindingIssues[0]}`);
          return;
        }
        const ptsAmountIssues = getPtsInternalEdgeAmountIssues(graph);
        if (ptsAmountIssues.length > 0) {
          setStatusText(`未保存：PTS 内部连线数量校验失败 ${ptsAmountIssues.length} 项。示例：${ptsAmountIssues[0]}`);
          return;
        }
      }
      const currentFingerprint = JSON.stringify(graph);
      if (isEmptyGraph(graph)) {
        if (strictValidation) {
          setStatusText("未保存：当前模型为空图（请先添加节点或连线）。");
        }
        return;
      }
      if (mode !== "manual" && lastSavedFingerprintRef.current && lastSavedFingerprintRef.current === currentFingerprint) {
        return;
      }
      try {
        if (projectId) {
          localStorage.setItem(draftKey(projectId), JSON.stringify(graph));
        }
      } catch {
        // ignore
      }

      if (mode === "manual") {
        setBusy(true);
        setStatusText("正在保存模型...");
      }
      if (backgroundSave) {
        backgroundSaveInFlightRef.current = true;
      }

      try {
        if (!projectId) {
          throw new Error("project_id missing");
        }
        const endpoint = `${API_BASE}/projects/${encodeURIComponent(projectId)}/versions`;
        const saveGraph = async (nextGraph: LcaGraphPayload) =>
          fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ graph: nextGraph }),
          });
        if (backgroundSave) {
          debugPts("persistModel:background:graph", {
            mode,
            projectId,
            ptsNodeIds: (graph.nodes ?? [])
              .filter((node) => node.node_kind === "pts_module")
              .map((node) => ({
                id: node.id,
                ptsUuid: node.pts_uuid,
                inputs: summarizePtsPortsForDebug(node.inputs as FlowPort[] | undefined),
                outputs: summarizePtsPortsForDebug(node.outputs as FlowPort[] | undefined),
              })),
          });
        }
        const response = await saveGraph(graph);
        if (!response.ok) {
          throw new Error((await response.text()) || `save failed: ${response.status}`);
        }
        const payload = (await response.json()) as ModelCreateResponse;
        setProjectId(payload.project_id);
        setVersion(String(payload.version));
        const projectNameForMessage =
          projects.find((item) => item.project_id === payload.project_id)?.name ?? projectName;
        localStorage.setItem(
          snapshotKey(payload.project_id),
          JSON.stringify({
            project_id: payload.project_id,
            version: payload.version,
          }),
        );
        lastSavedFingerprintRef.current = currentFingerprint;
        void refreshProjects();

        if (mode === "manual") {
          const ptsCompileCount = Number(payload.pts_compile_count ?? 0);
          const ptsFailedCount = Number(payload.pts_failed_count ?? 0);
          const ptsCompiledUuids = Array.isArray(payload.pts_compiled_uuids) ? payload.pts_compiled_uuids : [];
          const ptsFailedItems = Array.isArray(payload.pts_failed_items) ? payload.pts_failed_items : [];
          const ptsStatusParts: string[] = [];
          if (ptsCompileCount > 0) {
            ptsStatusParts.push(
              `PTS编译 ${ptsCompileCount} 个${ptsCompiledUuids.length > 0 ? `（${ptsCompiledUuids.join("，")}）` : ""}`,
            );
          }
          if (ptsFailedCount > 0) {
            const firstFailed = ptsFailedItems[0];
            const failedUuid =
              firstFailed && typeof firstFailed === "object"
                ? String((firstFailed as Record<string, unknown>).pts_uuid ?? (firstFailed as Record<string, unknown>).ptsUuid ?? "")
                : "";
            ptsStatusParts.push(`PTS编译失败 ${ptsFailedCount} 个${failedUuid ? `（示例：${failedUuid}）` : ""}`);
          }
          const ptsStatusText = ptsStatusParts.length > 0 ? `。${ptsStatusParts.join("；")}` : "";
          setStatusText(
            nonBlockingWarnings.length > 0
              ? `已保存项目 ${projectNameForMessage}: version=${payload.version}${ptsStatusText}。警告：${nonBlockingWarnings.join("；")}`
              : `已保存项目 ${projectNameForMessage}: version=${payload.version}${ptsStatusText}`,
          );
        }
      } catch (error) {
        if (mode === "manual") {
          setStatusText(`保存失败: ${formatApiError(error)}`);
        }
      } finally {
        if (backgroundSave) {
          backgroundSaveInFlightRef.current = false;
        }
        if (mode === "manual") {
          setBusy(false);
        }
      }
    },
    [
      activeCanvasId,
      activeCanvasKind,
      busy,
      exportGraph,
      getBalancedWarningsForCanvas,
      getBalancedWarnings,
      getMarketWarnings,
      persistPtsResource,
      projectId,
      projectName,
      projects,
      repairRootEdgeHandles,
      refreshProjects,
      selectedProductKey,
      targetProductQuantity,
      targetProductQuantityMode,
      versionTraveling,
    ],
  );

  const persistRootModelSnapshot = useCallback(
    async (targetProjectId: string) => {
      repairRootEdgeHandles();
      let graph = normalizeGraphPayload(exportGraph());
      debugPts("persistRootModelSnapshot:graph", {
        projectId: targetProjectId,
        ptsNodeIds: (graph.nodes ?? [])
          .filter((node) => node.node_kind === "pts_module")
          .map((node) => ({
            id: node.id,
            ptsUuid: node.pts_uuid,
            inputs: summarizePtsPortsForDebug(node.inputs as FlowPort[] | undefined),
            outputs: summarizePtsPortsForDebug(node.outputs as FlowPort[] | undefined),
          })),
      });
      if (isEmptyGraph(graph)) {
        return null;
      }
      const response = await fetch(`${API_BASE}/projects/${encodeURIComponent(targetProjectId)}/versions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ graph }),
      });
      if (!response.ok) {
        throw new Error((await response.text()) || `save failed: ${response.status}`);
      }
      const payload = (await response.json()) as ModelCreateResponse;
      debugPts("persistRootModelSnapshot:saved", {
        projectId: payload.project_id,
        version: payload.version,
      });
      setProjectId(payload.project_id);
      setVersion(String(payload.version));
      try {
        localStorage.setItem(
          snapshotKey(payload.project_id),
          JSON.stringify({
            project_id: payload.project_id,
            version: payload.version,
          }),
        );
      } catch {
        // ignore
      }
      lastSavedFingerprintRef.current = JSON.stringify(graph);
      suppressAutoSaveUntilRef.current = Date.now() + 15000;
      void refreshProjects();
      return payload;
    },
    [exportGraph, refreshProjects, repairRootEdgeHandles],
  );

  const handleUnpackPts = useCallback(
    async (ptsNodeId: string) => {
      const currentGraph = normalizeGraphPayload(exportGraph());
      const ptsNode = (currentGraph.nodes ?? []).find((node) => node.id === ptsNodeId && node.node_kind === "pts_module");
      if (!ptsNode) {
        setStatusText("仅支持在主图中解封 PTS 节点。");
        return;
      }
      const ptsUuid = String(ptsNode.pts_uuid ?? "").trim();
      if (!ptsUuid) {
        setStatusText("当前 PTS 缺少 pts_uuid，无法解封。");
        return;
      }

      setBusy(true);
      setStatusText("正在解封 PTS...");
      try {
        const response = await fetch(`${API_BASE}/pts/${encodeURIComponent(ptsUuid)}/unpack`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        });
        if (!response.ok) {
          throw new Error((await response.text()) || `unpack failed: ${response.status}`);
        }
        const payload = (await response.json()) as PtsUnpackResponse;
        if (!payload.pts_graph || typeof payload.pts_graph !== "object") {
          throw new Error("unpack response missing pts_graph");
        }
        const targetProjectId = String(payload.project_id ?? projectId ?? "").trim();
        if (!targetProjectId) {
          throw new Error("unpack response missing project_id");
        }

        const unpackGraph = normalizeGraphPayload(payload.pts_graph as LcaGraphPayload);
        const unpackNodes = (unpackGraph.nodes ?? []).filter((node) => node.node_kind !== "pts_module");
        const unpackEdges = unpackGraph.exchanges ?? [];
        if (unpackNodes.length === 0) {
          throw new Error("PTS 内部没有可恢复节点。");
        }

        const currentNodes = currentGraph.nodes ?? [];
        const currentEdges = currentGraph.exchanges ?? [];
        const avgPosition = unpackNodes.reduce(
          (acc, node) => {
            const rawNode = node as typeof node & { position?: { x?: number; y?: number } };
            acc.x += Number(rawNode.position?.x ?? 0);
            acc.y += Number(rawNode.position?.y ?? 0);
            return acc;
          },
          { x: 0, y: 0 },
        );
        const avgX = avgPosition.x / unpackNodes.length;
        const avgY = avgPosition.y / unpackNodes.length;
        const ptsRawNode = ptsNode as typeof ptsNode & { position?: { x?: number; y?: number } };
        const dx = Number(ptsRawNode.position?.x ?? 0) - avgX;
        const dy = Number(ptsRawNode.position?.y ?? 0) - avgY;

        const restoredNodes = unpackNodes.map((node) => {
          const rawNode = node as typeof node & { position?: { x?: number; y?: number } };
          return {
            ...node,
            position: {
              x: Number(rawNode.position?.x ?? 0) + dx,
              y: Number(rawNode.position?.y ?? 0) + dy,
            },
          };
        });

        const unaffectedEdges = currentEdges.filter((edge) => edge.fromNode !== ptsNodeId && edge.toNode !== ptsNodeId);

        const nextMetadata = {
          ...(currentGraph.metadata ?? {}),
          canvases: Array.isArray((currentGraph.metadata as { canvases?: Array<Record<string, unknown>> } | undefined)?.canvases)
            ? (((currentGraph.metadata as { canvases?: Array<Record<string, unknown>> } | undefined)?.canvases ?? []).filter(
                (canvas) =>
                  !(canvas.kind === "pts_internal" && String(canvas.parentPtsNodeId ?? "") === ptsNodeId),
              ))
            : undefined,
        };
        if (!nextMetadata.canvases || nextMetadata.canvases.length === 0) {
          delete (nextMetadata as Record<string, unknown>).canvases;
        }

        const nextGraph = normalizeGraphPayload({
          ...currentGraph,
          nodes: [...currentNodes.filter((node) => node.id !== ptsNodeId), ...restoredNodes],
          exchanges: [...unaffectedEdges, ...unpackEdges],
          metadata: nextMetadata,
        });

        importGraphWithLoadKey(nextGraph);
        autoConnectByUuid({ silentNoCandidate: true, silentSuccess: true });
        await persistRootModelSnapshot(targetProjectId);
        setStatusText("PTS 解封完成，已尝试自动连接并保存主图。未连上的边可后续手动补连。");
      } catch (error) {
        setStatusText(`PTS 解封失败: ${formatApiError(error)}`);
      } finally {
        setBusy(false);
      }
    },
    [autoConnectByUuid, exportGraph, importGraphWithLoadKey, persistRootModelSnapshot, projectId],
  );

  const runModel = useCallback(async () => {
    repairRootEdgeHandles();
    const graph = applyProjectTargetProductConfig(
      normalizeGraphPayload(exportGraph()),
      selectedProductKey
        ? {
            processUuid: selectedProductKey.split("::")[0] ?? "",
            flowUuid: selectedProductKey.split("::")[1] ?? "",
            quantityMode: targetProductQuantityMode,
            quantity: parseTargetProductQuantity(targetProductQuantity),
          }
        : null,
    );
    const nodesMissingProduct = (graph.nodes ?? [])
      .filter((node) => node.node_kind === "unit_process" && !String(node.process_uuid ?? "").startsWith("market_"))
      .filter((node) => {
        const ports = [...(node.inputs ?? []), ...(node.outputs ?? [])].filter((port) => port.type !== "biosphere");
        return !ports.some((port) => Boolean(port.isProduct));
      })
      .map((node) => ({
        nodeId: String(node.id),
        processUuid: String(node.process_uuid ?? ""),
        name: String(node.name ?? node.process_uuid ?? node.id),
      }));
    setMissingProductNodes(nodesMissingProduct);
    if (nodesMissingProduct.length > 0) {
      setStatusText(
        `运行警告：存在 ${nodesMissingProduct.length} 个过程未定义产品，计算继续执行。可点击下方提示快速定位。`,
      );
    }
    const ptsOutputBindingIssues = getPtsOutputSourceBindingIssues(graph);
    if (ptsOutputBindingIssues.length > 0) {
      setStatusText(`运行已阻止：PTS 输出来源绑定缺失 ${ptsOutputBindingIssues.length} 项。示例：${ptsOutputBindingIssues[0]}`);
      return;
    }
    const ptsAmountIssues = getPtsInternalEdgeAmountIssues(graph);
    if (ptsAmountIssues.length > 0) {
      setStatusText(`运行已阻止：PTS 内部连线数量校验失败 ${ptsAmountIssues.length} 项。示例：${ptsAmountIssues[0]}`);
      return;
    }
    const duplicateProcessUuidIssues = getDuplicateProcessUuidIssues(graph);
    if (duplicateProcessUuidIssues.length > 0) {
      setStatusText(`运行已阻止：process_uuid 重复 ${duplicateProcessUuidIssues.length} 项。示例：${duplicateProcessUuidIssues[0]}`);
      return;
    }
    const unitGroupIssues = getUnitProcessProductUnitGroupIssues(graph);
    if (unitGroupIssues.length > 0) {
      setStatusText(`运行已阻止：产品单位组校验失败 ${unitGroupIssues.length} 项。示例：${unitGroupIssues[0]}`);
      return;
    }
    const lciIssues = getLciNodeValidationIssues(graph);
    if (lciIssues.length > 0) {
      setStatusText(`运行已阻止：LCI 校验失败 ${lciIssues.length} 项。示例：${lciIssues[0]}`);
      return;
    }
    const balancedWarnings = getBalancedWarnings();
    if (balancedWarnings.length > 0) {
      const first = balancedWarnings[0];
      setStatusText(
        `运行已阻止：存在 ${balancedWarnings.length} 条未配平守恒关系。示例：${first.flowName}（输出=${first.outputTotal.toFixed(
          6,
        )}，外售=${first.externalSaleTotal.toFixed(6)}，输入=${first.inputTotal.toFixed(6)}）。`,
      );
      return;
    }
    const marketWarnings = getMarketWarnings();
    if (marketWarnings.length > 0) {
      const first = marketWarnings[0];
      setStatusText(`运行已阻止：市场过程校验失败 ${marketWarnings.length} 项。示例：${first.nodeName}（${first.issues[0]}）。`);
      return;
    }
    setBusy(true);
    setStatusText("正在运行求解...");
    setLastRun(null);
    try {
      const body = JSON.stringify({
        graph,
        model_version_id: projectId && version ? `${projectId}:${version}` : undefined,
        project_id: projectId || undefined,
      });
      const runCandidates = [`${API_BASE}/model/run`, `${API_BASE_NO_PREFIX}/model/run`];
      let payload: RunResponse | null = null;
      let lastErrorText = "";
      for (const endpoint of runCandidates) {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
        });
        if (response.ok) {
          payload = (await response.json()) as RunResponse;
          break;
        }
        const text = await response.text();
        lastErrorText = text || `${response.status}`;
        const lower = lastErrorText.toLowerCase();
        if (lower.includes("pts_published_artifact_not_found") || lower.includes("published external artifact not found")) {
          throw new Error("PTS 尚未发布可运行版本。请先在 PTS 编辑界面执行“发布”，再回主图运行。");
        }
        const shouldContinue = response.status === 404 || lower.includes("not found");
        if (!shouldContinue) {
          throw new Error(lastErrorText);
        }
      }
      if (!payload) {
        throw new Error(lastErrorText || "run failed");
      }
      setLastRun(payload);
      setShowRunAnalysis(true);
      setStatusText(APP_DEBUG ? `运行完成: run_id=${payload.run_id}` : "运行完成");
    } catch (error) {
      setStatusText(`运行失败: ${formatApiError(error)}`);
    } finally {
      setBusy(false);
    }
    }, [exportGraph, getBalancedWarnings, getMarketWarnings, projectId, repairRootEdgeHandles, selectedProductKey, targetProductQuantity, targetProductQuantityMode, version]);

  const compileCurrentPts = useCallback(async (mode: "save_compile" | "publish") => {
    if (activeCanvasKind !== "pts_internal") {
      setStatusText("仅在 PTS 编辑模式可执行该操作。");
      return false;
    }
    const activeCanvas = canvases[activeCanvasId];
    const ptsNodeId = activeCanvas?.parentPtsNodeId;
    const ptsNode =
      ptsNodeId && canvases.root
        ? canvases.root.nodes.find((node) => node.id === ptsNodeId && node.data.nodeKind === "pts_module")
        : undefined;
    const ptsUuid = ptsNode?.data.ptsUuid ?? ptsNode?.data.processUuid;
    if (!ptsNode || !ptsUuid) {
      setStatusText("操作失败：未定位到当前 PTS。");
      return false;
    }
    if (!projectId) {
      setStatusText("操作失败：缺少项目 ID。");
      return false;
    }
    setBusy(true);
    setStatusText(mode === "publish" ? `正在发布 PTS: ${ptsUuid}` : `正在保存编译 PTS: ${ptsUuid}`);
    try {
      if (mode === "publish") {
        const requestBody = await buildPtsResourcePayloadForNode({
          ptsNodeId: ptsNode.id,
          ptsCanvasId: activeCanvas.id,
          ptsUuid,
        }, { includePortsPolicy: false });
        if (!requestBody) {
          throw new Error("missing pts resource payload");
        }
        const finalizeResp = await fetch(`${API_BASE}/pts/${encodeURIComponent(ptsUuid)}/pack-finalize`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            ...requestBody,
            defaultVisiblePortHints: buildPtsDefaultVisiblePortHints(ptsNode),
            default_visible_port_hints: buildPtsDefaultVisiblePortHints(ptsNode),
            force_recompile: false,
            set_active: true,
          }),
        });
        if (!finalizeResp.ok) {
          throw new Error(await finalizeResp.text());
        }
        const finalized = (await finalizeResp.json()) as PtsPackFinalizeResponse;
        const modelingWarnings = normalizePtsModelingWarnings(finalized.warnings);
        setPtsPublishWarnings(modelingWarnings);
        setShowPtsPublishWarnings(false);
        const shellNode = finalized.shell_node ?? {};
        const shellInputsRaw = Array.isArray(finalized.shell_inputs)
          ? finalized.shell_inputs
          : Array.isArray((shellNode as Record<string, unknown>).inputs)
            ? ((shellNode as Record<string, unknown>).inputs as Array<Record<string, unknown>>)
            : [];
        const shellOutputsRaw = Array.isArray(finalized.shell_outputs)
          ? finalized.shell_outputs
          : Array.isArray((shellNode as Record<string, unknown>).outputs)
            ? ((shellNode as Record<string, unknown>).outputs as Array<Record<string, unknown>>)
            : [];
        const shellPublishedArtifactId =
          typeof (shellNode as Record<string, unknown>).pts_published_artifact_id === "string"
            ? ((shellNode as Record<string, unknown>).pts_published_artifact_id as string)
            : undefined;
        const shellInputs = shellInputsRaw
          .map((port) => normalizePtsProjectedPort(port, "inputs"))
          .filter(Boolean);
        const shellOutputs = shellOutputsRaw
          .map((port) => normalizePtsProjectedPort(port, "outputs"))
          .filter(Boolean);
        const projectedShellInputs = mergeProjectedPortsWithExistingEnglish(ptsNode.data.inputs, shellInputs);
        const projectedShellOutputs = mergeProjectedPortsWithExistingEnglish(ptsNode.data.outputs, shellOutputs);
        const defaultVisiblePortIdsRaw = Array.isArray(finalized.defaultVisiblePortIds)
          ? finalized.defaultVisiblePortIds
          : Array.isArray(finalized.default_visible_port_ids)
            ? finalized.default_visible_port_ids
            : [];
        const defaultVisiblePortIds = defaultVisiblePortIdsRaw
          .map((item) => String(item ?? "").trim())
          .filter((item) => item.length > 0);
        const portIdMap = normalizePortIdMap(finalized.port_id_map);
        debugPts("packFinalize:manual:response", {
          ptsUuid,
          publishedVersion: finalized.published_version ?? null,
          activePublishedVersion: finalized.active_published_version ?? null,
          portIdMap,
          shellInputs: shellInputs.map((port) => ({
            id: port.id,
            legacyPortId: port.legacyPortId,
            flowUuid: port.flowUuid,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
          })),
          shellOutputs: shellOutputs.map((port) => ({
            id: port.id,
            legacyPortId: port.legacyPortId,
            flowUuid: port.flowUuid,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
            isProduct: port.isProduct,
          })),
          defaultVisiblePortIds,
        });
        suppressAutoSaveRef.current = true;
        applySingleRootPtsProjection(ptsNode.id, shellNode as Record<string, unknown>, projectedShellInputs, projectedShellOutputs, {
          rebindEdges: true,
          publishedVersion: finalized.active_published_version ?? finalized.published_version,
          publishedArtifactId: shellPublishedArtifactId,
        });
        const versionText = finalized.published_version != null ? `，published_version=${finalized.published_version}` : "";
        const compileText =
          finalized.compile_version != null ? `，compile_version=${finalized.compile_version}` : "";
        const activeText =
          finalized.active_published_version != null ? `，active=${finalized.active_published_version}` : "";
        setStatusText(
          modelingWarnings.length > 0
            ? buildPtsPublishWarningStatusText(modelingWarnings, uiLanguage, "publish")
            : `PTS 发布完成: ${ptsUuid}${versionText}${compileText}${activeText}`,
        );
      } else {
        await persistPtsResource();
        const graph: LcaGraphPayload = {
          functionalUnit: "PTS",
          nodes: [],
          exchanges: [],
          metadata: {},
        };
        const ptsOutputBindingIssues = getPtsOutputSourceBindingIssues(graph);
        if (ptsOutputBindingIssues.length > 0) {
          throw new Error(ptsOutputBindingIssues[0]);
        }
        const ptsAmountIssues = getPtsInternalEdgeAmountIssues(graph);
        if (ptsAmountIssues.length > 0) {
          throw new Error(ptsAmountIssues[0]);
        }
        const compileBody = (nextGraph: LcaGraphPayload) =>
          JSON.stringify({
            project_id: projectId,
            pts_uuid: ptsUuid,
            graph: nextGraph,
            force_recompile: true,
          });
        let compileResp = await fetch(`${API_BASE}/pts/compile`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: compileBody(graph),
        });
        if (!compileResp.ok) {
          const errorText = await compileResp.text();
          if (errorText.includes("PTS_NODE_NOT_FOUND_BY_UUID")) {
            const retryGraph =
              buildPtsCompileFallbackGraph({
                ptsCanvasId: activeCanvas.id,
                ptsNodeId: ptsNode.id,
                ptsUuid,
              }) ?? graph;
            compileResp = await fetch(`${API_BASE}/pts/compile`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: compileBody(retryGraph),
            });
            if (!compileResp.ok) {
              throw new Error((await compileResp.text()) || errorText);
            }
          } else {
            throw new Error(errorText);
          }
        }
        const compiled = (await compileResp.json()) as PtsCompileResponse;
        const compileText = compiled.compile_version != null ? `，compile_version=${compiled.compile_version}` : "";
        const cacheText = compiled.cached ? "，使用缓存" : "";
        setStatusText(`PTS 保存编译完成: ${ptsUuid}${compileText}${cacheText}`);
      }
      return true;
    } catch (error) {
      if (mode === "publish") {
        setPtsPublishWarnings([]);
        setShowPtsPublishWarnings(false);
      }
      setStatusText(`${mode === "publish" ? "PTS 发布失败" : "PTS 保存编译失败"}: ${formatApiError(error)}`);
      return false;
    } finally {
      suppressAutoSaveRef.current = false;
      setBusy(false);
    }
  }, [
    activeCanvasId,
    activeCanvasKind,
    canvases,
    buildPtsCompileFallbackGraph,
    buildPtsResourcePayloadForNode,
    exportGraph,
    persistPtsResource,
    projectId,
    refreshSingleRootPtsProjectionFromBackend,
    uiLanguage,
  ]);

  const saveCurrentPts = useCallback(async () => {
    if (activeCanvasKind !== "pts_internal") {
      setStatusText("仅在 PTS 编辑模式可执行保存。");
      return false;
    }
    return compileCurrentPts("publish");
  }, [activeCanvasKind, compileCurrentPts, setStatusText]);

  // 开源版先隐藏版本历史入口，后续可直接恢复顶部按钮。
  const openPtsHistory = useCallback(async () => {
    if (activeCanvasKind !== "pts_internal" || !activePtsUuid || !projectId) {
      setStatusText("仅在 PTS 编辑模式可查看版本历史。");
      return;
    }
    setPtsHistoryOpen(true);
    setPtsHistoryLoading(true);
    try {
      const [compileResp, publishedResp] = await Promise.all([
        fetch(`${API_BASE}/pts/${encodeURIComponent(activePtsUuid)}/compile-history?project_id=${encodeURIComponent(projectId)}`),
        fetch(`${API_BASE}/pts/${encodeURIComponent(activePtsUuid)}/published-history?project_id=${encodeURIComponent(projectId)}`),
      ]);
      if (!compileResp.ok) {
        throw new Error(await compileResp.text());
      }
      if (!publishedResp.ok) {
        throw new Error(await publishedResp.text());
      }
      const compilePayload = (await compileResp.json()) as PtsCompileHistoryResponse;
      const publishedPayload = (await publishedResp.json()) as PtsPublishedHistoryResponse;
      setPtsCompileHistory(Array.isArray(compilePayload.items) ? compilePayload.items : []);
      setPtsPublishedHistory(Array.isArray(publishedPayload.items) ? publishedPayload.items : []);
      setPtsHistoryActivePublishedVersion(publishedPayload.active_published_version ?? null);
      setStatusText(`PTS 版本历史已加载: ${activePtsUuid}`);
    } catch (error) {
      setPtsHistoryOpen(false);
      setStatusText(`PTS 版本历史读取失败: ${formatApiError(error)}`);
    } finally {
      setPtsHistoryLoading(false);
    }
  }, [activeCanvasKind, activePtsUuid, projectId]);
  void openPtsHistory;

  const saveAndReturnToModeling = useCallback(async () => {
      const ok = await saveCurrentPts();
      if (!ok) {
        return;
      }
      try {
        if (projectId && activePtsNode?.id && activePtsUuid) {
          await refreshSingleRootPtsProjectionFromBackend(activePtsNode.id, activePtsUuid, projectId, {
            rebindEdges: true,
          });
        }
      } catch (error) {
        setStatusText(`返回主图前刷新 PTS 投影失败: ${formatApiError(error)}`);
        return;
      }
      goToParentCanvas();
    }, [activePtsNode?.id, activePtsUuid, goToParentCanvas, projectId, refreshSingleRootPtsProjectionFromBackend, saveCurrentPts]);

  useEffect(() => {
    if (!pendingPtsCompileNodeId) {
      return;
    }
    if (!projectId) {
      consumePendingPtsCompile();
      return;
    }
    const autoPublishKey = `${projectId}:${pendingPtsCompileNodeId}`;
    if (ptsAutoPublishInFlightRef.current === autoPublishKey) {
      return;
    }
    ptsAutoPublishInFlightRef.current = autoPublishKey;
    setStatusText(uiLanguage === "zh" ? "正在封装并自动发布 PTS..." : "Packing and auto-publishing PTS...");
    consumePendingPtsCompile();
    const runCompile = async () => {
      try {
        const ptsNode = canvases.root?.nodes.find(
          (node) => node.id === pendingPtsCompileNodeId && node.data.nodeKind === "pts_module",
        );
        const ptsUuid = ptsNode?.data.ptsUuid ?? ptsNode?.data.processUuid;
        if (!ptsNode || !ptsUuid) {
          throw new Error("missing pts_uuid for compile");
        }
        const ptsCanvasId = ptsNode.data.ptsCanvasId;
        if (!ptsCanvasId) {
          throw new Error("missing pts internal canvas for auto publish");
        }
        const requestBody = await buildPtsResourcePayloadForNode({
          ptsNodeId: ptsNode.id,
          ptsCanvasId,
          ptsUuid,
        }, { includePortsPolicy: false });
        if (!requestBody) {
          throw new Error("missing pts resource payload");
        }
        const response = await fetch(`${API_BASE}/pts/${encodeURIComponent(ptsUuid)}/pack-finalize`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            ...requestBody,
            defaultVisiblePortHints: buildPtsDefaultVisiblePortHints(ptsNode),
            default_visible_port_hints: buildPtsDefaultVisiblePortHints(ptsNode),
            force_recompile: false,
            set_active: true,
          }),
        });
        if (!response.ok) {
          const lastErrorText = await response.text();
          throw new Error(lastErrorText || `pack finalize failed: ${response.status}`);
        }
        const published = (await response.json()) as PtsPackFinalizeResponse;
        const modelingWarnings = normalizePtsModelingWarnings(published.warnings);
        setPtsPublishWarnings(modelingWarnings);
        setShowPtsPublishWarnings(false);
        const shellNode = published.shell_node ?? {};
        const shellInputsRaw = Array.isArray(published.shell_inputs)
          ? published.shell_inputs
          : Array.isArray((shellNode as Record<string, unknown>).inputs)
            ? ((shellNode as Record<string, unknown>).inputs as Array<Record<string, unknown>>)
            : [];
        const shellOutputsRaw = Array.isArray(published.shell_outputs)
          ? published.shell_outputs
          : Array.isArray((shellNode as Record<string, unknown>).outputs)
            ? ((shellNode as Record<string, unknown>).outputs as Array<Record<string, unknown>>)
            : [];
        const shellPublishedArtifactId =
          typeof (shellNode as Record<string, unknown>).pts_published_artifact_id === "string"
            ? ((shellNode as Record<string, unknown>).pts_published_artifact_id as string)
            : undefined;
        const shellInputs = shellInputsRaw
          .map((port) => normalizePtsProjectedPort(port, "inputs"))
          .filter(Boolean);
        const shellOutputs = shellOutputsRaw
          .map((port) => normalizePtsProjectedPort(port, "outputs"))
          .filter(Boolean);
        const projectedShellInputs = mergeProjectedPortsWithExistingEnglish(ptsNode.data.inputs, shellInputs);
        const projectedShellOutputs = mergeProjectedPortsWithExistingEnglish(ptsNode.data.outputs, shellOutputs);
        const defaultVisiblePortIdsRaw = Array.isArray(published.defaultVisiblePortIds)
          ? published.defaultVisiblePortIds
          : Array.isArray(published.default_visible_port_ids)
            ? published.default_visible_port_ids
            : [];
        const defaultVisiblePortIds = defaultVisiblePortIdsRaw
          .map((item) => String(item ?? "").trim())
          .filter((item) => item.length > 0);
        const portIdMap = normalizePortIdMap(published.port_id_map);
        debugPts("packFinalize:auto:response", {
          ptsUuid,
          publishedVersion: published.published_version ?? null,
          activePublishedVersion: published.active_published_version ?? null,
          portIdMap,
          shellInputs: shellInputs.map((port) => ({
            id: port.id,
            legacyPortId: port.legacyPortId,
            flowUuid: port.flowUuid,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
          })),
          shellOutputs: shellOutputs.map((port) => ({
            id: port.id,
            legacyPortId: port.legacyPortId,
            flowUuid: port.flowUuid,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
            isProduct: port.isProduct,
          })),
          defaultVisiblePortIds,
        });
        suppressAutoSaveRef.current = true;
        applySingleRootPtsProjection(ptsNode.id, shellNode as Record<string, unknown>, projectedShellInputs, projectedShellOutputs, {
          rebindEdges: true,
          publishedVersion: published.active_published_version ?? published.published_version,
          publishedArtifactId: shellPublishedArtifactId,
        });
        autoConnectByUuid({ silentNoCandidate: true, silentSuccess: true });
        await persistRootModelSnapshot(projectId);
        const publishText =
          published.published_version != null ? `，published_version=${published.published_version}` : "";
        setStatusText(
          modelingWarnings.length > 0
            ? buildPtsPublishWarningStatusText(modelingWarnings, uiLanguage, "pack_finalize")
            : `PTS封装完成，已自动编译、发布并保存主图${publishText}。`,
        );
      } catch (error) {
        setPtsPublishWarnings([]);
        setShowPtsPublishWarnings(false);
        setStatusText(`PTS 自动编译/发布失败: ${formatApiError(error)}`);
      } finally {
        suppressAutoSaveRef.current = false;
        if (ptsAutoPublishInFlightRef.current === autoPublishKey) {
          ptsAutoPublishInFlightRef.current = "";
        }
      }
    };
    void runCompile();
  }, [
    buildPtsResourcePayloadForNode,
    canvases,
    autoConnectByUuid,
    consumePendingPtsCompile,
    exportGraph,
    pendingPtsCompileNodeId,
    persistRootModelSnapshot,
    applySingleRootPtsProjection,
    refreshSingleRootPtsProjectionFromBackend,
    projectId,
    uiLanguage,
  ]);

  const handleSwitchProject = useCallback(
    async (nextProjectId: string, nextProjectName?: string) => {
      if (!nextProjectId) {
        return;
      }
      if (nextProjectId === projectId && appMode === "editor") {
        return;
      }
      const target = projects.find((p) => p.project_id === nextProjectId);
      await loadProjectGraph(nextProjectId, nextProjectName ?? target?.name);
    },
    [appMode, loadProjectGraph, projectId, projects],
  );

  const handleCreateProject = useCallback(async (form: CreateProjectForm) => {
    const name = form.projectName.trim();
    if (!name) {
      setStatusText("新建项目失败: 项目名称不能为空。");
      return;
    }
    setBusy(true);
    try {
      const response = await fetch(`${API_BASE}/projects`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const created = (await response.json()) as ProjectResponse;
      try {
        localStorage.removeItem(draftKey(created.project_id));
        localStorage.removeItem(snapshotKey(created.project_id));
      } catch {
        // ignore
      }
        await refreshProjects();
        await loadProjectGraph(created.project_id, created.name);
        setAppMode("editor");
        setStatusText(`已创建项目: ${created.name}`);
      } catch (error) {
        setStatusText(`新建项目失败: ${String(error)}`);
      } finally {
        setBusy(false);
      }
    }, [loadProjectGraph, refreshProjects]);

  const handleDeleteProjectById = useCallback(async (targetProjectId: string) => {
    if (!targetProjectId) {
      return;
    }
    const target = projects.find((p) => p.project_id === targetProjectId);
    const ok = window.confirm(`删除项目 "${target?.name ?? targetProjectId}"？此操作不可恢复。`);
    if (!ok) {
      return;
    }
    setBusy(true);
    try {
      const response = await fetch(`${API_BASE}/projects/${encodeURIComponent(targetProjectId)}`, { method: "DELETE" });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const rows = await refreshProjects();
      if (!rows.length) {
        setProjectId("");
        setProjectName("");
        setVersion("0");
        setAppMode("management");
        syncProjectRoute("", "management", true);
      } else {
        const next = rows[0];
        setProjectName(next.name);
        await loadProjectGraph(next.project_id, next.name);
      }
      setStatusText("项目已删除");
    } catch (error) {
      setStatusText(`删除项目失败: ${String(error)}`);
    } finally {
      setBusy(false);
    }
  }, [loadProjectGraph, projects, refreshProjects, syncProjectRoute]);

  const loadUnits = useCallback(async (): Promise<boolean> => {
    try {
      const resp = await fetch(`${API_BASE}/reference/units`);
      if (!resp.ok) {
        return false;
      }
      const rows = (await resp.json()) as UnitDefinitionRow[];
      setUnitDefinitions(Array.isArray(rows) ? rows : []);
      return Array.isArray(rows) && rows.length > 0;
    } catch {
      return false;
    }
  }, []);

  useEffect(() => {
    void loadUnits();
  }, [loadUnits]);

  const switchToReferenceUnits = useCallback(() => {
    void (async () => {
      if (!unitDefinitions.length) {
        await loadUnits();
      }
      setResultUnitMode("reference");
    })();
  }, [loadUnits, unitDefinitions.length]);

  const unitFactorMap = useMemo(() => {
    const map = new Map<string, number>();
    unitDefinitions.forEach((row) => {
      map.set(`${row.unit_group}::${row.unit_name}`, Number(row.factor_to_reference || 0));
    });
    return map;
  }, [unitDefinitions]);

  const referenceUnitByGroup = useMemo(() => {
    const map = new Map<string, string>();
    unitDefinitions.forEach((row) => {
      if (row.is_reference && row.unit_group && row.unit_name) {
        map.set(row.unit_group, row.unit_name);
      }
    });
    return map;
  }, [unitDefinitions]);

  const toDisplayResultValueByUnit = useCallback(
    (rawValue: number, unit: string, unitGroup?: string): { value: number; unitLabel: string } => {
      if (!unit) {
        return { value: rawValue, unitLabel: "kg CO2-eq / reference unit" };
      }
      if (resultUnitMode === "defined") {
        return { value: rawValue, unitLabel: `kg CO2-eq / ${unit}` };
      }
      const referenceUnit = unitGroup ? referenceUnitByGroup.get(unitGroup) : undefined;
      if (!unitGroup || !referenceUnit) {
        return { value: rawValue, unitLabel: `kg CO2-eq / ${unit}` };
      }
      const factor = unitFactorMap.get(`${unitGroup}::${unit}`);
      if (!factor || !Number.isFinite(factor) || factor <= 0) {
        return { value: rawValue, unitLabel: `kg CO2-eq / ${referenceUnit}` };
      }
      return {
        value: rawValue / factor,
        unitLabel: `kg CO2-eq / ${referenceUnit}`,
      };
    },
    [referenceUnitByGroup, resultUnitMode, unitFactorMap],
  );

  useEffect(() => {
    try {
      const uiLangCached = localStorage.getItem(UI_LANG_KEY);
      if (uiLangCached === "zh" || uiLangCached === "en") {
        setUiLanguage(uiLangCached);
      }
      const flowAnimCached = localStorage.getItem(FLOW_ANIM_KEY);
      if (flowAnimCached === "0") {
        setFlowAnimationEnabled(false);
      } else if (flowAnimCached === "1") {
        setFlowAnimationEnabled(true);
      }
      const autoPopupCached = localStorage.getItem(AUTO_POPUP_KEY);
      if (autoPopupCached === "0") {
        setAutoPopupEnabled(false);
      } else if (autoPopupCached === "1") {
        setAutoPopupEnabled(true);
      }
      const unitScaleCached = localStorage.getItem(UNIT_AUTOSCALE_KEY);
      if (unitScaleCached === "0") {
        setUnitAutoScaleEnabled(false);
      } else if (unitScaleCached === "1") {
        setUnitAutoScaleEnabled(true);
      }
    } catch {
      // ignore
    }
  }, [setAutoPopupEnabled, setFlowAnimationEnabled, setUiLanguage, setUnitAutoScaleEnabled]);

  useEffect(() => {
    try {
      localStorage.setItem(UI_LANG_KEY, uiLanguage);
    } catch {
      // ignore
    }
  }, [uiLanguage]);

  useEffect(() => {
    if (activeCanvasKind !== "pts_internal") {
      ptsResourceHydrationRef.current = "";
      setPtsCanvasLoading(false);
    }
  }, [activeCanvasKind, activeCanvasId]);

  useEffect(() => {
    if (activeCanvasKind !== "pts_internal" || !projectId || !activePtsUuid || !activePtsNode) {
      return;
    }
    const hydrationKey = `${projectId}:${activePtsUuid}:${activeCanvasId}`;
    if (ptsResourceHydrationRef.current === hydrationKey) {
      setPtsCanvasLoading(false);
      return;
    }
    let cancelled = false;
    setPtsCanvasLoading(true);
    const hydratePtsResource = async () => {
      try {
        const response = await fetch(`${API_BASE}/pts/${encodeURIComponent(activePtsUuid)}`);
        if (response.status === 404) {
          await persistPtsResourceForNode({
            ptsNodeId: activePtsNode.id,
            ptsCanvasId: activeCanvasId,
            ptsUuid: activePtsUuid,
          });
          if (!cancelled) {
            ptsResourceHydrationRef.current = hydrationKey;
            setStatusText("已初始化空白 PTS 草稿。");
            setPtsCanvasLoading(false);
          }
          return;
        }
        if (!response.ok) {
          throw new Error((await response.text()) || `load pts failed: ${response.status}`);
        }
        const payload = (await response.json()) as PtsResourceResponse;
        const ptsGraph = payload.pts_graph;
        if (!cancelled && ptsGraph && typeof ptsGraph === "object") {
          const rawPtsGraph = normalizeGraphPayload(ptsGraph as LcaGraphPayload);
          const sanitizedPtsGraph: LcaGraphPayload = {
            ...rawPtsGraph,
            nodes: (rawPtsGraph.nodes ?? []).filter(
              (node) =>
                !(
                  node.node_kind === "pts_module" &&
                  (String(node.pts_uuid ?? node.process_uuid ?? "") === activePtsUuid || String(node.id ?? "") === activePtsNode.id)
                ),
            ),
            exchanges: (rawPtsGraph.exchanges ?? []).filter(
              (edge) => String(edge.fromNode ?? "") !== activePtsNode.id && String(edge.toNode ?? "") !== activePtsNode.id,
            ),
          };
          replacePtsInternalCanvasGraphWithLoadKey(activePtsNode.id, sanitizedPtsGraph, {
            name: payload.name ?? activePtsNode.data.name,
          });
          ptsResourceHydrationRef.current = hydrationKey;
          setPtsCanvasLoading(false);
        }
      } catch (error) {
        if (!cancelled) {
          setStatusText(`PTS 读取失败: ${formatApiError(error)}`);
          setPtsCanvasLoading(false);
        }
      }
    };
    void hydratePtsResource();
    return () => {
      cancelled = true;
    };
  }, [
    activeCanvasId,
    activeCanvasKind,
    activePtsNode,
    activePtsUuid,
    persistPtsResourceForNode,
    projectId,
    replacePtsInternalCanvasGraphWithLoadKey,
  ]);

  useEffect(() => {
    if (!projectId || !canvases.root) {
      return;
    }
    const draftPtsNodes = canvases.root.nodes.filter(
      (node) => {
        if (
          node.data.nodeKind !== "pts_module" ||
          !node.data.ptsUuid ||
          !node.data.ptsCanvasId ||
          node.data.ptsPublishedArtifactId
        ) {
          return false;
        }
        if (pendingPtsCompileNodeId && node.id === pendingPtsCompileNodeId) {
          return false;
        }
        const internalCanvas = canvases[node.data.ptsCanvasId];
        if (!internalCanvas) {
          return false;
        }
        return (internalCanvas.nodes?.length ?? 0) === 0 && (internalCanvas.edges?.length ?? 0) === 0;
      },
    );
    if (draftPtsNodes.length === 0) {
      return;
    }
    let cancelled = false;
    const initDrafts = async () => {
      for (const ptsNode of draftPtsNodes) {
        const ptsUuid = ptsNode.data.ptsUuid ?? ptsNode.data.processUuid;
        const ptsCanvasId = ptsNode.data.ptsCanvasId;
        if (!ptsUuid || !ptsCanvasId) {
          continue;
        }
        const initKey = `${projectId}:${ptsNode.id}:${ptsUuid}`;
        if (ptsDraftInitializedRef.current[initKey]) {
          continue;
        }
        if (ptsDraftInitInFlightRef.current[initKey]) {
          continue;
        }
        ptsDraftInitInFlightRef.current[initKey] = true;
        try {
          await persistPtsResourceForNode({
            ptsNodeId: ptsNode.id,
            ptsCanvasId,
            ptsUuid,
          });
          ptsDraftInitializedRef.current[initKey] = true;
        } catch {
          if (!cancelled) {
            setStatusText(`PTS 草稿初始化失败: ${ptsNode.data.name}`);
          }
        } finally {
          delete ptsDraftInitInFlightRef.current[initKey];
        }
      }
    };
    void initDrafts();
    return () => {
      cancelled = true;
    };
  }, [canvases, canvases.root, pendingPtsCompileNodeId, persistPtsResourceForNode, projectId]);

  useEffect(() => {
    try {
      localStorage.setItem(FLOW_ANIM_KEY, flowAnimationEnabled ? "1" : "0");
    } catch {
      // ignore
    }
  }, [flowAnimationEnabled]);

  useEffect(() => {
    try {
      localStorage.setItem(AUTO_POPUP_KEY, autoPopupEnabled ? "1" : "0");
    } catch {
      // ignore
    }
  }, [autoPopupEnabled]);

  useEffect(() => {
    try {
      localStorage.setItem(UNIT_AUTOSCALE_KEY, unitAutoScaleEnabled ? "1" : "0");
    } catch {
      // ignore
    }
  }, [unitAutoScaleEnabled]);

  useEffect(() => {
    const text = String(statusText ?? "").trim();
    if (!text || text.startsWith("正在")) {
      return;
    }
    const snapshot = text;
    const timer = window.setTimeout(() => {
      setStatusText((prev) => (String(prev ?? "").trim() === snapshot ? "" : prev));
    }, 5000);
    return () => window.clearTimeout(timer);
  }, [statusText]);

  useEffect(() => {
    if (appMode !== "management") {
      return;
    }
    setStatusText("");
  }, [appMode]);

  useEffect(() => {
    const text = String(connectionHint ?? "").trim();
    if (!text) {
      return;
    }
    const shouldUseStatusToast =
      text === "PTS封装完成。" ||
      text === "PTS解封完成。" ||
      text.startsWith("自动连线完成") ||
      text.startsWith("自动连线未找到") ||
      text.startsWith("Auto-connect complete") ||
      text.startsWith("Auto-connect: no candidate") ||
      text === "No unique auto-connect candidates found." ||
      text.startsWith("无法连线") ||
      text.startsWith("Cannot connect") ||
      text === "警告：当前过程未定义产品，已关闭清单分析，可稍后继续补充。";
    if (shouldUseStatusToast) {
      setStatusText(text);
      clearConnectionHint();
    }
  }, [clearConnectionHint, connectionHint]);

  useEffect(() => {
    if (hydratedRef.current) {
      return;
    }
    hydratedRef.current = true;
    void hydrate();
  }, [hydrate]);

  useEffect(() => {
    const onPopState = () => {
      const nextRouteProjectId = getProjectIdFromPathname(window.location.pathname);
      routeSyncRef.current = window.location.pathname || "/";
      setRouteProjectId(nextRouteProjectId);
      if (!nextRouteProjectId) {
        setAppMode("management");
      }
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    if (!hydrated || !routeProjectId) {
      return;
    }
    if (routeHydrationAttemptRef.current === routeProjectId) {
      routeHydrationAttemptRef.current = "";
      return;
    }
    if (routeProjectId === projectId && appMode === "editor") {
      return;
    }
    const target = projects.find((item) => item.project_id === routeProjectId);
    setAppMode("editor");
    void loadProjectGraph(routeProjectId, target?.name ?? routeProjectId);
  }, [appMode, hydrated, loadProjectGraph, projectId, projects, routeProjectId]);

  useEffect(() => {
    if (!hydrated) {
      return;
    }
    const nextPath = appMode === "editor" && projectId ? buildProjectPathname(projectId) : "/";
    if (routeSyncRef.current === nextPath) {
      return;
    }
    syncProjectRoute(projectId, appMode, true);
  }, [appMode, hydrated, projectId, syncProjectRoute]);

  useEffect(() => {
    if (!hydrated) {
      return;
    }
    try {
      if (projectId) {
        localStorage.setItem(
          draftKey(projectId),
          JSON.stringify(
            applyProjectTargetProductConfig(
              exportGraph(),
              selectedProductKey
                ? {
                    processUuid: selectedProductKey.split("::")[0] ?? "",
                    flowUuid: selectedProductKey.split("::")[1] ?? "",
                    quantityMode: targetProductQuantityMode,
                    quantity: parseTargetProductQuantity(targetProductQuantity),
                  }
                : null,
            ),
          ),
        );
      }
    } catch {
      // ignore
    }
  }, [graphFingerprint, hydrated, exportGraph, projectId, selectedProductKey, targetProductQuantity, targetProductQuantityMode]);

  useEffect(() => {
    if (!hydrated || appMode !== "editor" || !projectId || activeCanvasKind === "pts_internal") {
      return;
    }
    const timer = window.setTimeout(() => {
      void persistModel("auto");
    }, AUTO_SAVE_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [activeCanvasKind, appMode, graphFingerprint, persistModel, hydrated, projectId]);

  useEffect(() => {
    if (!hydrated || appMode !== "editor" || !projectId || activeCanvasKind === "pts_internal") {
      return;
    }
    const interval = window.setInterval(() => {
      void persistModel("interval");
    }, INTERVAL_SAVE_MS);
    return () => window.clearInterval(interval);
  }, [activeCanvasKind, appMode, persistModel, hydrated, projectId]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const isMac = navigator.platform.toUpperCase().includes("MAC");
      const hasPrimary = isMac ? event.metaKey : event.ctrlKey;
      if (!hasPrimary || event.altKey) {
        return;
      }
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName?.toLowerCase();
      const isTyping = target?.isContentEditable || tag === "input" || tag === "textarea" || tag === "select";
      if (isTyping) {
        return;
      }

      const key = String(event.key ?? "").toLowerCase();
      const code = String(event.code ?? "");
      const wantsUndo = (key === "z" || code === "KeyZ") && !event.shiftKey;
      const wantsRedo =
        key === "y" ||
        code === "KeyY" ||
        key === "redo" ||
        code === "F4" ||
        ((key === "z" || code === "KeyZ") && event.shiftKey);
      if (!wantsUndo && !wantsRedo) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();
      navigateVersion(wantsUndo ? -1 : 1);
    };

    window.addEventListener("keydown", onKeyDown, true);
    document.addEventListener("keydown", onKeyDown, true);
    return () => {
      window.removeEventListener("keydown", onKeyDown, true);
      document.removeEventListener("keydown", onKeyDown, true);
    };
  }, [navigateVersion]);

  useEffect(() => {
    const onUndo = () => navigateVersion(-1);
    const onRedo = () => navigateVersion(1);
    window.addEventListener("nebula:history-undo", onUndo as EventListener);
    window.addEventListener("nebula:history-redo", onRedo as EventListener);
    return () => {
      window.removeEventListener("nebula:history-undo", onUndo as EventListener);
      window.removeEventListener("nebula:history-redo", onRedo as EventListener);
    };
  }, [navigateVersion]);

  const runIssues = useMemo(() => {
    if (!lastRun) {
      return [];
    }
    const issues = lastRun.lci_result?.issues;
    return Array.isArray(issues) ? issues : [];
  }, [lastRun]);

  const warningBannerText = useMemo(() => {
    if (runIssues.length === 0) {
      return "";
    }
    const first = String(runIssues[0] ?? "");
    if (runIssues.length === 1) {
      return `发现 1 条警告：${first}`;
    }
    return `发现 ${runIssues.length} 条警告：${first}`;
  }, [runIssues]);

  const ptsResultContextByUuid = useMemo(() => {
    const map = new Map<
      string,
      {
        processName: string;
        outputsByFlow: Map<string, FlowPort[]>;
        outputsByPortId: Map<string, FlowPort>;
      }
    >();
    const rootNodes = canvases.root?.nodes ?? [];
    rootNodes.forEach((node) => {
      if (node.data.nodeKind !== "pts_module") {
        return;
      }
      const ptsUuid = String(node.data.ptsUuid ?? node.data.processUuid ?? "").trim();
      if (!ptsUuid) {
        return;
      }
      const outputsByFlow = new Map<string, FlowPort[]>();
      const outputsByPortId = new Map<string, FlowPort>();
      (node.data.outputs ?? []).forEach((port) => {
        const flowUuid = String(port.flowUuid ?? "").trim();
        if (!flowUuid) {
          return;
        }
        const portId = String(port.id ?? "").trim();
        if (portId && !outputsByPortId.has(portId)) {
          outputsByPortId.set(portId, port);
        }
        const legacyPortId = String(port.legacyPortId ?? "").trim();
        if (legacyPortId && !outputsByPortId.has(legacyPortId)) {
          outputsByPortId.set(legacyPortId, port);
        }
        const list = outputsByFlow.get(flowUuid) ?? [];
        list.push(port);
        outputsByFlow.set(flowUuid, list);
      });
      map.set(ptsUuid, {
        processName: node.data.name,
        outputsByFlow,
        outputsByPortId,
      });
    });
    return map;
  }, [canvases.root?.nodes]);

  const rootProductPortByPortKey = useMemo(() => {
    const map = new Map<string, FlowPort>();
    const rootNodes = canvases.root?.nodes ?? [];
    rootNodes.forEach((node) => {
      const processUuid = String(node.data.processUuid ?? "").trim();
      if (!processUuid) {
        return;
      }
      (node.data.outputs ?? []).forEach((port) => {
        const portId = String(port.id ?? "").trim();
        if (!portId) {
          const legacyPortId = String(port.legacyPortId ?? "").trim();
          if (!legacyPortId) {
            return;
          }
          const legacyKey = `${processUuid}::${legacyPortId}`;
          if (!map.has(legacyKey)) {
            map.set(legacyKey, port);
          }
          return;
        }
        const key = `${processUuid}::${portId}`;
        if (!map.has(key)) {
          map.set(key, port);
        }
        const legacyPortId = String(port.legacyPortId ?? "").trim();
        if (legacyPortId) {
          const legacyKey = `${processUuid}::${legacyPortId}`;
          if (!map.has(legacyKey)) {
            map.set(legacyKey, port);
          }
        }
      });
    });
    return map;
  }, [canvases.root?.nodes]);

  const rootProductPortsByMatchKey = useMemo(() => {
    const map = new Map<string, FlowPort[]>();
    const rootNodes = canvases.root?.nodes ?? [];
    rootNodes.forEach((node) => {
      const processUuid = String(node.data.processUuid ?? "").trim();
      if (!processUuid) {
        return;
      }
      (node.data.outputs ?? []).forEach((port) => {
        const flowUuid = String(port.flowUuid ?? "").trim();
        if (!flowUuid) {
          return;
        }
        const key = buildProjectTargetMatchKey(processUuid, flowUuid);
        const list = map.get(key) ?? [];
        list.push(port);
        map.set(key, list);
      });
    });
    return map;
  }, [canvases.root?.nodes]);

  const productColumns = useMemo(() => {
    if (!lastRun) {
      return [] as ProductResultRow[];
    }
    const rowsRaw = lastRun.lci_result?.product_result_index;
    const unitMapRaw = lastRun.lci_result?.product_unit_map;
    const unitMap =
      unitMapRaw && typeof unitMapRaw === "object" ? (unitMapRaw as Record<string, Record<string, unknown>>) : {};
    if (!Array.isArray(rowsRaw)) {
      return [] as ProductResultRow[];
    }
    const ptsFlowOccurrence = new Map<string, number>();
    return rowsRaw.map((item, idx) => {
      const obj = typeof item === "object" && item !== null ? (item as Record<string, unknown>) : {};
      const productKey = String(obj.product_key ?? `product_${idx}`);
      const rawProcessUuid = String(obj.process_uuid ?? "");
      const parsedPtsProduct = parsePtsSyntheticProductProcessUuid(rawProcessUuid);
      const productFlowUuid = String(obj.product_flow_uuid ?? parsedPtsProduct?.flowUuid ?? "");
      const productPortId = String(obj.product_port_id ?? "").trim();
      const unitRow = unitMap[productKey] && typeof unitMap[productKey] === "object" ? unitMap[productKey] : {};
      const productNameZh = String(obj.product_name ?? obj.product_flow_uuid ?? productKey).trim();
      const productNameEn = String(obj.product_name_en ?? "").trim();
      let processUuid = rawProcessUuid;
      let processName = String(obj.process_name ?? obj.process_uuid ?? "");
      let ptsProcessName = "";
      let productName =
        (uiLanguage === "en" ? productNameEn || productNameZh : productNameZh) || productKey;
      if (parsedPtsProduct) {
        processUuid = parsedPtsProduct.ptsUuid;
        const ptsContext = ptsResultContextByUuid.get(parsedPtsProduct.ptsUuid);
        if (ptsContext) {
          ptsProcessName = ptsContext.processName || "";
          processName = ptsContext.processName || processName || parsedPtsProduct.ptsUuid;
          const matchedPortById = productPortId ? ptsContext.outputsByPortId.get(productPortId) : undefined;
          const occurrenceKey = `${parsedPtsProduct.ptsUuid}::${productFlowUuid}`;
          const occurrenceIndex = ptsFlowOccurrence.get(occurrenceKey) ?? 0;
          ptsFlowOccurrence.set(occurrenceKey, occurrenceIndex + 1);
          const candidatePorts = ptsContext.outputsByFlow.get(productFlowUuid) ?? [];
          const matchedPort = matchedPortById ?? candidatePorts[occurrenceIndex] ?? candidatePorts[0];
          if (matchedPort) {
            const sourceProcessName = String(matchedPort.sourceProcessName ?? "").trim();
            const matchedPortBaseName =
              uiLanguage === "en"
                ? String(matchedPort.flowNameEn ?? "").trim() ||
                  String(obj.product_name_en ?? "").trim() ||
                  String(matchedPort.name ?? "").trim() ||
                  String(obj.product_name ?? "").trim()
                : String(matchedPort.name ?? "").trim() ||
                  String(obj.product_name ?? "").trim() ||
                  String(matchedPort.flowNameEn ?? "").trim() ||
                  String(obj.product_name_en ?? "").trim();
            const resolvedBaseName =
              matchedPortBaseName ||
              String(matchedPort.productKey ?? "").trim() ||
              productName;
            if (sourceProcessName) {
              const cleanName = resolvedBaseName.replace(/\s*@\s*pts_[^@]+$/i, "").trim();
              productName = cleanName.includes(`@ ${sourceProcessName}`) ? cleanName : `${cleanName} @ ${sourceProcessName}`;
            } else {
              productName = resolvedBaseName.replace(/\s*@\s*pts_[^@]+$/i, "").trim() || productName;
            }
          }
        }
      }
      const matchedRootPortById = rootProductPortByPortKey.get(`${processUuid}::${productPortId}`);
      const rootPortsByFlow =
        rootProductPortsByMatchKey.get(buildProjectTargetMatchKey(processUuid, String(productFlowUuid ?? unitRow.flow_uuid ?? ""))) ?? [];
      const matchedRootPort =
        matchedRootPortById ??
        (parsedPtsProduct
          ? rootPortsByFlow.length === 1
            ? rootPortsByFlow[0]
            : undefined
          : rootPortsByFlow[0]);
      if (matchedRootPort) {
        const rootProductName =
          uiLanguage === "en"
            ? String(matchedRootPort.flowNameEn ?? "").trim() || String(matchedRootPort.name ?? "").trim()
            : String(matchedRootPort.name ?? "").trim() || String(matchedRootPort.flowNameEn ?? "").trim();
        if (rootProductName) {
          productName = rootProductName;
        }
      }
      return {
        productKey,
        viewKey: productPortId || productKey,
        matchKey: buildProjectTargetMatchKey(processUuid, String(productFlowUuid ?? unitRow.flow_uuid ?? "")),
        processUuid,
        rawProcessUuid,
        processName,
        productPortId,
        productFlowUuid: String(productFlowUuid ?? unitRow.flow_uuid ?? ""),
        productName,
        isReferenceProduct: Boolean(obj.is_reference_product),
        unit: String(unitRow.unit ?? obj.unit ?? "").trim(),
        unitGroup: String(unitRow.unit_group ?? obj.unit_group ?? "").trim() || undefined,
        ptsProcessName,
      };
    });
  }, [lastRun, ptsResultContextByUuid, rootProductPortsByMatchKey, rootProductPortByPortKey, uiLanguage]);

  const effectiveTargetProductKey = useMemo(
    () => selectedProductKey || productColumns[0]?.matchKey || "",
    [productColumns, selectedProductKey],
  );

  const selectedProductIndex = useMemo(
    () => {
      const matchedIndex = productColumns.findIndex((item) => item.matchKey === effectiveTargetProductKey);
      if (matchedIndex >= 0) {
        return matchedIndex;
      }
      return productColumns.length > 0 ? 0 : -1;
    },
    [effectiveTargetProductKey, productColumns],
  );
  const selectedProduct = useMemo(
    () => (selectedProductIndex >= 0 ? productColumns[selectedProductIndex] ?? undefined : undefined),
    [productColumns, selectedProductIndex],
  );
  const targetProductQuantityValue = useMemo(() => {
    if (targetProductQuantityMode === "functional_unit") {
      return parseFunctionalUnitQuantity(functionalUnit);
    }
    return parseTargetProductQuantity(targetProductQuantity);
  }, [functionalUnit, targetProductQuantity, targetProductQuantityMode]);
  const viewedProductKey = useMemo(
    () => resultProductViewKey || selectedProduct?.viewKey || productColumns[0]?.viewKey || "",
    [productColumns, resultProductViewKey, selectedProduct?.viewKey],
  );
  const viewedProductIndex = useMemo(() => {
    const matchedIndex = productColumns.findIndex((item) => item.viewKey === viewedProductKey);
    if (matchedIndex >= 0) {
      return matchedIndex;
    }
    return selectedProductIndex;
  }, [productColumns, selectedProductIndex, viewedProductKey]);
  const viewedProduct = useMemo(
    () => (viewedProductIndex >= 0 ? productColumns[viewedProductIndex] ?? undefined : undefined),
    [productColumns, viewedProductIndex],
  );
  const viewedProductQuantityValue = useMemo(
    () => (resultProductViewMode === "target_total" ? targetProductQuantityValue : 1),
    [resultProductViewMode, targetProductQuantityValue],
  );

  const indicatorRows = useMemo(() => {
    if (!lastRun) {
      return [] as IndicatorDisplayRow[];
    }
    const index = lastRun.lci_result?.indicator_index;
    const valuesRaw = lastRun.lci_result?.product_values;
    const values = Array.isArray(valuesRaw) ? valuesRaw : [];
    const indicators = Array.isArray(index) ? index : [];
    const denominatorUnit = viewedProduct
      ? `${viewedProductQuantityValue} ${viewedProduct.unit || "unit"}`
      : "reference unit";
    const rows: IndicatorDisplayRow[] = indicators.map((indicator, i) => {
      const obj = typeof indicator === "object" && indicator !== null ? (indicator as Record<string, unknown>) : {};
      const methodZh = String(obj.method_zh ?? "").trim();
      const methodEn = String(obj.method_en ?? "").trim();
      const methodGeneric = String(obj.method ?? "").trim();
      const method =
        uiLanguage === "zh"
          ? methodZh || methodEn || methodGeneric || `method_${i}`
          : methodEn || methodZh || methodGeneric || `method_${i}`;
      const nameZh = String(obj.indicator_zh ?? "").trim();
      const nameEn = String(obj.indicator_en ?? "").trim();
      const nameGeneric = String(obj.indicator ?? "").trim();
      const name =
        uiLanguage === "zh"
          ? nameZh || nameEn || nameGeneric || `indicator_${i}`
          : nameEn || nameZh || nameGeneric || `indicator_${i}`;
      const numeratorUnit = String(obj.indicator_unit ?? obj.unit ?? "").trim();
      const valueRaw = values[i];
      const vector = Array.isArray(valueRaw)
        ? valueRaw.map((v) => (typeof v === "number" ? v : Number(v ?? 0)))
        : null;
      const scalar = typeof valueRaw === "number" ? valueRaw : Number(valueRaw ?? 0);
      const selectedValue = vector && viewedProductIndex >= 0 && viewedProductIndex < vector.length ? vector[viewedProductIndex] : scalar;
      const normalizedValue = Number.isFinite(selectedValue) ? selectedValue : 0;
      const displayValue = viewedProduct
        ? toDisplayResultValueByUnit(
            normalizedValue * viewedProductQuantityValue,
            viewedProduct.unit,
            viewedProduct.unitGroup,
          ).value
        : normalizedValue;
      return {
        idx: i + 1,
        method,
        name,
        unit: numeratorUnit ? `${numeratorUnit} / ${denominatorUnit}` : "-",
        selectedValue: displayValue,
      };
    });
    return rows;
  }, [lastRun, toDisplayResultValueByUnit, uiLanguage, viewedProduct, viewedProductIndex, viewedProductQuantityValue]);

  const groupedIndicatorRows = useMemo(() => {
    const rows: Array<{ type: "group"; method: string } | ({ type: "row" } & IndicatorDisplayRow)> = [];
    let currentMethod = "";
    indicatorRows.forEach((row) => {
      const groupLabel = getLciaGroupLabel(row.method);
      if (groupLabel !== currentMethod) {
        currentMethod = groupLabel;
        rows.push({ type: "group", method: currentMethod });
      }
      rows.push({ type: "row", ...row });
    });
    return rows;
  }, [indicatorRows]);

  const productCfpRows = useMemo(() => {
    if (!lastRun) {
      return [] as Array<{
        productKey: string;
        viewKey: string;
        processUuid: string;
        processName: string;
        productFlowUuid: string;
        productName: string;
        isReferenceProduct: boolean;
        value: number;
        unitLabel: string;
      }>;
    }
    const indicatorIndexRaw = lastRun.lci_result?.indicator_index;
    const indicatorIndex = Array.isArray(indicatorIndexRaw) ? indicatorIndexRaw : [];
    const valuesRaw = lastRun.lci_result?.product_values;
    const values = Array.isArray(valuesRaw) ? valuesRaw : [];

    const climateIdx = indicatorIndex.findIndex((item) => {
      if (typeof item === "object" && item !== null) {
        const obj = item as Record<string, unknown>;
        const idx = Number(obj.indicator_index ?? -1);
        const methodEn = String(obj.method_en ?? "");
        const category = String(obj.ecoinvent_category ?? "");
        if (idx === 1) {
          return true;
        }
        return methodEn === "Climate change" && category === "climate change";
      }
      return Number(item) === 1;
    });

    if (climateIdx < 0 || climateIdx >= values.length) {
      return [];
    }
    const row = values[climateIdx];
    const vector = Array.isArray(row) ? row : [row];
    return productColumns.map((product, idx) => {
      const raw = vector[idx];
      const value = typeof raw === "number" ? raw : Number(raw ?? 0);
      const normalized = Number.isFinite(value) ? value : 0;
      const display = toDisplayResultValueByUnit(normalized, product.unit, product.unitGroup);
      return {
        productKey: product.productKey,
        viewKey: product.viewKey,
        processUuid: product.processUuid,
        processName: product.processName,
        productFlowUuid: product.productFlowUuid,
        productName: product.productName,
        isReferenceProduct: product.isReferenceProduct,
        value: display.value,
        unitLabel: display.unitLabel,
      };
    });
  }, [lastRun, productColumns, toDisplayResultValueByUnit]);

  const groupedProductCfpRows = useMemo(() => {
    const groups = new Map<
      string,
      {
        processUuid: string;
        processName: string;
        items: typeof productCfpRows;
      }
    >();
    productCfpRows.forEach((row) => {
      const existed = groups.get(row.processUuid);
      if (existed) {
        existed.items.push(row);
        return;
      }
      groups.set(row.processUuid, {
        processUuid: row.processUuid,
        processName: row.processName,
        items: [row],
      });
    });
    return Array.from(groups.values());
  }, [productCfpRows]);

  const displayedStatusText = useMemo(() => translateStatusText(statusText, uiLanguage), [statusText, uiLanguage]);
  const statusLevel = useMemo(() => detectMessageLevel(statusText), [statusText]);
  const connectionHintLevel = useMemo(() => detectMessageLevel(connectionHint ?? ""), [connectionHint]);
  const normalizedEdgeFixCandidates = useMemo(() => {
    const resolveNodeMode = (nodeId: string): "balanced" | "normalized" => {
      const node = nodes.find((item) => item.id === nodeId);
      if (!node) {
        return "normalized";
      }
      if (node.data.nodeKind !== "unit_process") {
        return "normalized";
      }
      if ((node.data.processUuid ?? "").startsWith("market_")) {
        return "normalized";
      }
      return node.data.mode ?? "balanced";
    };

    return edges.filter((edge) => {
      if ((edge.data?.quantityMode ?? "single") !== "single") {
        return false;
      }
      return resolveNodeMode(edge.source) !== "balanced" || resolveNodeMode(edge.target) !== "balanced";
    });
  }, [edges, nodes]);
  const showNormalizedEdgeFixBanner =
    appMode === "editor" &&
    activeCanvasKind === "root" &&
    normalizedEdgeFixCandidates.length > 0 &&
    dismissedNormalizedEdgeFixFingerprint !== graphFingerprint;
  const firstFlowNameSyncEvidence = useMemo(() => {
    if (!flowNameSyncState.examples.length) {
      return "";
    }
    const first = flowNameSyncState.examples[0] ?? {};
    const expected = String(first.expected_flow_name ?? "");
    const actual = String(first.actual_port_name ?? "");
    if (expected && actual) {
      return uiLanguage === "zh"
        ? `示例：当前为“${actual}”，标准流名应为“${expected}”`
        : `Example: current is "${actual}", standard flow name should be "${expected}"`;
    }
    return "";
  }, [flowNameSyncState.examples, uiLanguage]);

  const flowNameSyncBannerText = useMemo(() => {
    const countLabel = flowNameSyncState.outdatedCount || (uiLanguage === "zh" ? "部分" : "some");
    const base =
      uiLanguage === "zh"
        ? `检测到当前项目有 ${countLabel} 个流名称与流库不一致，请先同步 flow 信息后再保存。`
        : `Detected ${countLabel} flow name mismatches between the current project and the flow catalog. Please sync flow info before saving.`;
    return firstFlowNameSyncEvidence ? `${base} ${firstFlowNameSyncEvidence}` : base;
  }, [firstFlowNameSyncEvidence, flowNameSyncState.outdatedCount, uiLanguage]);

  const autoFixNormalizedEdges = useCallback(() => {
    if (!normalizedEdgeFixCandidates.length) {
      return;
    }
    normalizedEdgeFixCandidates.forEach((edge) => {
      updateEdgeData(edge.id, { quantityMode: "dual" });
    });
    setDismissedNormalizedEdgeFixFingerprint("");
    setStatusText(`已自动修复 ${normalizedEdgeFixCandidates.length} 条归一化连线，线型已同步为虚线。`);
  }, [normalizedEdgeFixCandidates, updateEdgeData]);

  useEffect(() => {
    if (normalizedEdgeFixCandidates.length > 0) {
      return;
    }
    setDismissedNormalizedEdgeFixFingerprint("");
  }, [normalizedEdgeFixCandidates.length]);

  const targetProductCandidates = useMemo(() => {
    const rootNodes = canvases.root?.nodes ?? [];
    return rootNodes.flatMap((node) => {
      const outputs = node.data.outputs ?? [];
      return outputs
        .filter((port) => {
          if (!port.flowUuid) {
            return false;
          }
          if (port.isProduct) {
            return true;
          }
          return (
            node.data.referenceProductFlowUuid &&
            port.flowUuid === node.data.referenceProductFlowUuid &&
            (node.data.referenceProductDirection ?? "output") === "output"
          );
        })
        .map((port) => ({
          productKey: buildProjectTargetMatchKey(node.data.processUuid, port.flowUuid),
          nodeId: node.id,
          processUuid: node.data.processUuid,
          processName: node.data.name,
          productFlowUuid: port.flowUuid,
          productName: port.name,
          unit: port.unit || "",
          unitGroup: port.unitGroup,
          ptsProcessName: node.data.nodeKind === "pts_module" ? node.data.name : undefined,
        }));
    });
  }, [canvases.root?.nodes]);

  const draftTargetProduct = useMemo(
    () => targetProductCandidates.find((item) => item.productKey === targetProductDraftKey) ?? null,
    [targetProductCandidates, targetProductDraftKey],
  );
  const configuredTargetProductKey = useMemo(
    () => selectedProductKey || targetProductCandidates[0]?.productKey || "",
    [selectedProductKey, targetProductCandidates],
  );

  useEffect(() => {
    setResultProductViewKey(selectedProduct?.viewKey || productColumns[0]?.viewKey || "");
    setResultProductViewMode("target_total");
  }, [lastRun?.run_id, productColumns, selectedProduct?.viewKey]);
  const openTargetProductDialog = useCallback(() => {
    setTargetProductDraftKey(configuredTargetProductKey);
    setTargetProductDraftQuantityMode(targetProductQuantityMode);
    setTargetProductDraftQuantity(targetProductQuantity);
    setResultProductViewKey(selectedProduct?.viewKey || productColumns[0]?.viewKey || "");
    setResultProductViewMode("target_total");
    setShowTargetProductDialog(true);
  }, [configuredTargetProductKey, productColumns, selectedProduct?.viewKey, targetProductQuantity, targetProductQuantityMode]);
  const focusTargetProductTotalView = useCallback(() => {
    setResultProductViewKey(selectedProduct?.viewKey || productColumns[0]?.viewKey || "");
    setResultProductViewMode("target_total");
  }, [productColumns, selectedProduct?.viewKey]);
  const saveTargetProductConfig = useCallback(() => {
    setSelectedProductKey(targetProductDraftKey || targetProductCandidates[0]?.productKey || "");
    setTargetProductQuantityMode(targetProductDraftQuantityMode);
    setTargetProductQuantity(targetProductDraftQuantity || "1");
    const nextSelectedMatchKey = targetProductDraftKey || targetProductCandidates[0]?.productKey || "";
    const nextSelectedViewKey =
      productColumns.find((row) => row.matchKey === nextSelectedMatchKey)?.viewKey || productColumns[0]?.viewKey || "";
    setResultProductViewKey(nextSelectedViewKey);
    setResultProductViewMode("target_total");
    setShowTargetProductDialog(false);
  }, [productColumns, targetProductCandidates, targetProductDraftKey, targetProductDraftQuantity, targetProductDraftQuantityMode]);
  const hasProductResultView = productColumns.length > 0 && Array.isArray(lastRun?.lci_result?.product_values);
  const selectedProductClimateRow = useMemo(
    () =>
      productCfpRows.find(
        (row) => buildProjectTargetMatchKey(row.processUuid, row.productFlowUuid) === configuredTargetProductKey,
      ) ?? productCfpRows[0] ?? null,
    [configuredTargetProductKey, productCfpRows],
  );
  const selectedProductClimateTotal = useMemo(
    () => (selectedProductClimateRow ? selectedProductClimateRow.value * targetProductQuantityValue : null),
    [selectedProductClimateRow, targetProductQuantityValue],
  );
  const targetProductClimateUnitLabel = useMemo(() => {
    if (!selectedProduct) {
      return "";
    }
    return `kg CO2-eq / ${targetProductQuantityValue} ${selectedProduct.unit || "unit"}`;
  }, [selectedProduct, targetProductQuantityValue]);
  const targetProductSummaryLabel = useMemo(() => {
    if (!selectedProduct) {
      return uiLanguage === "zh" ? "未选择产品" : "No product selected";
    }
    return formatTargetProductDisplayLabel(
      selectedProduct.productName,
      selectedProduct.processName,
      selectedProduct.ptsProcessName,
    );
  }, [selectedProduct, uiLanguage]);
  const draftTargetProductQuantityValue = useMemo(() => {
    if (targetProductDraftQuantityMode === "functional_unit") {
      return parseFunctionalUnitQuantity(functionalUnit);
    }
    return parseTargetProductQuantity(targetProductDraftQuantity);
  }, [functionalUnit, targetProductDraftQuantity, targetProductDraftQuantityMode]);
  const draftTargetProductDisplayUnit = useMemo(() => {
    if (!draftTargetProduct) {
      return "";
    }
    return toDisplayResultValueByUnit(0, draftTargetProduct.unit, draftTargetProduct.unitGroup).unitLabel.replace(
      /^kg CO2-eq \/ /,
      "",
    );
  }, [draftTargetProduct, toDisplayResultValueByUnit]);

  useEffect(() => {
    setExpandedResultProcesses((prev) => {
      const available = new Set(groupedProductCfpRows.map((group) => group.processUuid));
      if (!available.size) {
        return [];
      }
      if (!prev.length) {
        return Array.from(available);
      }
      const next = prev.filter((key) => available.has(key));
      if (next.length === available.size) {
        return next;
      }
      for (const key of available) {
        if (!next.includes(key)) {
          next.push(key);
        }
      }
      return next;
    });
  }, [groupedProductCfpRows]);

  useEffect(() => {
    setPtsPublishWarnings([]);
    setShowPtsPublishWarnings(false);
  }, [projectId]);

  if (appMode === "management") {
    return (
      <>
        {displayedStatusText && <div className={`status-toast status-bar--${statusLevel}`}>{displayedStatusText}</div>}
        <ProjectManagement
          projects={projects}
          busy={busy}
          uiLanguage={uiLanguage}
          onChangeLanguage={setUiLanguage}
          onStatus={setStatusText}
          onOpenProject={(targetProjectId, targetProjectName) => {
            void (async () => {
              await handleSwitchProject(targetProjectId, targetProjectName);
              setAppMode("editor");
            })();
          }}
          onCreateProject={handleCreateProject}
          onDeleteProject={(targetProjectId) => {
            void handleDeleteProjectById(targetProjectId);
          }}
          onCreateProcess={() => setStatusText("New Process 将在下一步接入。")}
          onCreateFlow={() => setStatusText("New Flow 将在下一步接入。")}
        />
      </>
    );
  }

  return (
    <div className="layout">
      <header className="topbar">
        <div className="topbar-brand">
          <img className="topbar-logo" src="/favicon.ico" alt="Nebula logo" />
          <div className="title">{i18n.appTitle}</div>
          <div className="project-name-strong" title={projectName || "-"}>
            <span>{i18n.projectNameLabel}：</span>
            <strong>{projectName || "-"}</strong>
          </div>
        </div>
        {activeCanvasKind === "pts_internal" && <span className="topbar-mode-badge">{i18n.ptsMode}</span>}
        <div className="topbar-actions">
          <button type="button" onClick={navigateToManagement}>
            {i18n.backHome}
          </button>
          {activeCanvasKind === "pts_internal" ? (
            <>
              <button onClick={() => void saveCurrentPts()} disabled={busy}>
                {i18n.save}
              </button>
              {/* 开源版暂隐藏“发布 / 版本历史”入口，后续可直接恢复这两个按钮。 */}
              <button onClick={() => void saveAndReturnToModeling()} disabled={busy}>
                {i18n.saveBack}
              </button>
            </>
          ) : (
            <>
              <button onClick={() => void persistModel("manual")} disabled={busy}>{i18n.save}</button>
              <button onClick={() => void runModel()} disabled={busy}>
                {i18n.run}
              </button>
            </>
          )}
          <div className="lang-switch">
            <button type="button" className={uiLanguage === "zh" ? "active" : ""} onClick={() => setUiLanguage("zh")}>
              中文
            </button>
            <button type="button" className={uiLanguage === "en" ? "active" : ""} onClick={() => setUiLanguage("en")}>
              EN
            </button>
          </div>
        </div>
      </header>
      {displayedStatusText && <div className={`status-toast status-bar--${statusLevel}`}>{displayedStatusText}</div>}
        {flowNameSyncState.needed && (
          <div className="inline-banner inline-banner--warning inline-banner--action">
            <span>{flowNameSyncBannerText}</span>
            <div className="inline-banner-actions">
              <button type="button" className="link-btn" onClick={() => void syncProjectFlowNames()} disabled={busy}>
                {uiLanguage === "zh" ? "同步 flow 信息" : "Sync flow info"}
              </button>
            </div>
          </div>
        )}
      {showNormalizedEdgeFixBanner && (
        <div className="inline-banner inline-banner--warning inline-banner--action">
          <span>{`检测到 ${normalizedEdgeFixCandidates.length} 条归一化连线仍为实线，是否自动修复为虚线？`}</span>
          <div className="inline-banner-actions">
            <button type="button" className="link-btn" onClick={autoFixNormalizedEdges}>
              自动修复
            </button>
            <button
              type="button"
              className="link-btn"
              onClick={() => setDismissedNormalizedEdgeFixFingerprint(graphFingerprint)}
            >
              暂不处理
            </button>
          </div>
        </div>
      )}
      {missingProductNodes.length > 0 && (
        <div className="inline-banner inline-banner--warning missing-product-banner">
          <span>{`未定义产品节点（${missingProductNodes.length}）：`}</span>
          {missingProductNodes.slice(0, 5).map((item) => (
            <button key={item.nodeId} type="button" className="link-btn" onClick={() => openNodeInspector(item.nodeId)}>
              {item.name}
            </button>
          ))}
        </div>
      )}
      {versionTraveling && (
        <div className="inline-banner inline-banner--warning">
          {uiLanguage === "zh"
            ? `当前处于历史版本浏览模式（v${version}），自动保存已暂停。确认回退结果后，请尽快点击“保存”生成最新版本。`
            : `History browse mode (v${version}) is active. Auto-save is paused. Please click Save to create a new latest version after confirming.`}
        </div>
      )}
      {lastRun && showRunAnalysis && (
        <div
          className="run-analysis-overlay"
          onClick={() => {
            setShowRunWarnings(false);
            setShowRunAnalysis(false);
          }}
        >
        <section className="run-analysis run-analysis-modal" onClick={(event) => event.stopPropagation()}>
          <div className="run-analysis-head">
            <strong>{i18n.result}</strong>
            <div className="run-analysis-head-actions">
              <span className="run-analysis-head-status">
                {lastRun.status === "completed"
                  ? uiLanguage === "zh"
                    ? "计算已完成"
                    : "Run completed"
                  : lastRun.status}
              </span>
              <button
                type="button"
                className="drawer-close-btn"
                onClick={() => {
                  setShowRunWarnings(false);
                  setShowRunAnalysis(false);
                }}
              >
                {i18n.close}
              </button>
            </div>
          </div>
          {warningBannerText && <div className="run-analysis-warning-banner">{warningBannerText}</div>}
          <div className="run-analysis-summary">
            <div className="run-analysis-summary-card">
              <span className="run-analysis-summary-label">{uiLanguage === "zh" ? "过程数" : "Processes"}</span>
              <strong className="run-analysis-summary-value">{groupedProductCfpRows.length}</strong>
            </div>
            <div className="run-analysis-summary-card">
              <span className="run-analysis-summary-label">{uiLanguage === "zh" ? "产品数" : "Products"}</span>
              <strong className="run-analysis-summary-value">{productColumns.length}</strong>
            </div>
            <div className="run-analysis-summary-card">
              <span className="run-analysis-summary-label">{uiLanguage === "zh" ? "警告数" : "Warnings"}</span>
              <strong className="run-analysis-summary-value">{runIssues.length}</strong>
              <button
                type="button"
                className="run-analysis-summary-link"
                onClick={() => setShowRunWarnings(true)}
                disabled={runIssues.length === 0}
              >
                {uiLanguage === "zh" ? "查看详情" : "View details"}
              </button>
            </div>
            <button
              type="button"
              className="run-analysis-summary-card run-analysis-summary-card--result run-analysis-summary-card--clickable"
              onClick={focusTargetProductTotalView}
            >
                <span className="run-analysis-summary-label">
                  {uiLanguage === "zh" ? "目标产品 / 目标产量" : "Target Product / Target Quantity"}
                </span>
              <strong className="run-analysis-summary-value">
                {selectedProductClimateTotal !== null
                  ? `${selectedProductClimateTotal.toExponential(3)} ${targetProductClimateUnitLabel || ""}`.trim()
                  : "-"}
              </strong>
              <span
                className="run-analysis-summary-meta"
                title={targetProductSummaryLabel}
              >
                {targetProductSummaryLabel}
              </span>
            </button>
          </div>
          <div className="run-analysis-grid">
            <div className="run-analysis-card run-analysis-card--wide">
              <div className="run-analysis-title run-analysis-title-flex">
                <span>
                  {uiLanguage === "zh"
                    ? "单位产品碳足迹结果（Climate change）"
                    : "Unit Product CFP Results (Climate change)"}
                </span>
                <span className="run-analysis-segment">
                  <button
                    type="button"
                    onClick={() => setResultUnitMode("defined")}
                    className={resultUnitMode === "defined" ? "active" : ""}
                  >
                    {uiLanguage === "zh" ? "清单定义单位" : "Inventory unit"}
                  </button>
                  <button
                    type="button"
                    onClick={switchToReferenceUnits}
                    className={resultUnitMode === "reference" ? "active" : ""}
                  >
                    {uiLanguage === "zh" ? "单位组默认单位" : "Default unit group unit"}
                  </button>
                </span>
              </div>
              <div className="run-analysis-table-wrap run-analysis-table-wrap-tall">
                <table className="run-analysis-table run-analysis-table--product-cfp">
                  <colgroup>
                    <col className="run-analysis-col-process" />
                    <col className="run-analysis-col-product" />
                    <col className="run-analysis-col-value" />
                    <col className="run-analysis-col-unit" />
                  </colgroup>
                  <thead>
                    <tr>
                      <th>{uiLanguage === "zh" ? "过程" : "Process"}</th>
                      <th>{uiLanguage === "zh" ? "产品（点击查看全指标）" : "Product (click to view full indicators)"}</th>
                      <th className="numeric">{uiLanguage === "zh" ? "单位产品结果" : "Unit product result"}</th>
                      <th>{uiLanguage === "zh" ? "单位" : "Unit"}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {groupedProductCfpRows.map((group) => {
                      const expanded = expandedResultProcesses.includes(group.processUuid);
                      return [
                        <tr key={`group-${group.processUuid}`} className="run-analysis-process-group-row">
                          <td colSpan={4}>
                            <button
                              type="button"
                              className="run-analysis-process-toggle"
                              onClick={() =>
                                setExpandedResultProcesses((prev) =>
                                  prev.includes(group.processUuid)
                                    ? prev.filter((key) => key !== group.processUuid)
                                    : [...prev, group.processUuid],
                                )
                              }
                            >
                              <span className="run-analysis-process-toggle-icon">{expanded ? "▾" : "▸"}</span>
                              <span className="run-analysis-process-toggle-label" title={group.processName}>
                                {group.processName}
                              </span>
                              <span className="run-analysis-process-toggle-count">
                                {uiLanguage === "zh" ? `${group.items.length} 个产品` : `${group.items.length} product(s)`}
                              </span>
                            </button>
                          </td>
                        </tr>,
                        ...(expanded
                          ? group.items.map((row) => (
                              <tr key={row.productKey}>
                                <td title={row.processName}>
                                  <span className="run-analysis-process-cell">{row.processName}</span>
                                </td>
                                <td title={row.productName}>
                                  <div className="run-analysis-product-cell">
                                    <span className="run-analysis-product-name">{row.productName}</span>
                                    <button
                                      type="button"
                                      className={`run-analysis-view-switch${
                                        resultProductViewMode === "unit_product" &&
                                        viewedProduct &&
                                        row.viewKey === viewedProduct.viewKey
                                          ? " active"
                                          : ""
                                      }`}
                                      onClick={() => {
                                        setResultProductViewKey(row.viewKey);
                                        setResultProductViewMode("unit_product");
                                      }}
                                      title={
                                        uiLanguage === "zh"
                                          ? `${row.productName}（点击查看全指标）`
                                          : `${row.productName} (click to view full indicators)`
                                      }
                                    >
                                      {resultProductViewMode === "unit_product" &&
                                      viewedProduct &&
                                      row.viewKey === viewedProduct.viewKey
                                        ? uiLanguage === "zh"
                                          ? "展示全指标中"
                                          : "Showing full indicators"
                                        : uiLanguage === "zh"
                                          ? "查看全指标"
                                          : "View full indicators"}
                                    </button>
                                  </div>
                                </td>
                                <td className={`numeric ${row.value === 0 ? "run-analysis-value--zero" : ""}`}>{row.value.toExponential(3)}</td>
                                <td className="run-analysis-unit-cell">{row.unitLabel}</td>
                              </tr>
                            ))
                          : []),
                      ];
                    })}
                    {groupedProductCfpRows.length === 0 && (
                      <tr>
                        <td colSpan={4}>
                          {!hasProductResultView
                            ? "后端尚未返回产品结果视图（product_result_index / product_values）。"
                            : "未找到 Climate change 结果"}
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
            <div className="run-analysis-card run-analysis-indicators">
              <div className="run-analysis-title">
                {`${uiLanguage === "zh" ? "指标结果" : "Indicator Results"} (${indicatorRows.length})${
                  viewedProduct
                    ? resultProductViewMode === "target_total"
                      ? ` · ${uiLanguage === "zh" ? "目标产品总量全指标" : "Target-total full indicators"}：${viewedProduct.productName}`
                      : ` · ${uiLanguage === "zh" ? "单位产品全指标" : "Unit-product full indicators"}：${viewedProduct.productName}`
                    : ""
                }`}
              </div>
              <div className="run-analysis-table-wrap">
                <table className="run-analysis-table">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>{uiLanguage === "zh" ? "方法" : "Method"}</th>
                      <th>{uiLanguage === "zh" ? "指标" : "Indicator"}</th>
                      <th>{uiLanguage === "zh" ? "单位" : "Unit"}</th>
                      <th className="numeric">
                        {resultProductViewMode === "target_total"
                          ? uiLanguage === "zh"
                            ? "目标产品总量结果"
                            : "Target-total result"
                          : uiLanguage === "zh"
                            ? "该单位产品结果"
                            : "This unit-product result"}
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {groupedIndicatorRows.map((row, rowIndex) =>
                      row.type === "group" ? (
                        <tr key={`group-${row.method}-${rowIndex}`} className="run-analysis-group-row">
                          <td colSpan={5}>{row.method}</td>
                        </tr>
                      ) : (
                        <tr key={`${row.idx}-${row.method}-${row.name}`}>
                          <td>{row.idx}</td>
                          <td title={row.method}>
                            <span className="run-analysis-method-cell">{row.method}</span>
                          </td>
                          <td title={row.name}>
                            <span className="run-analysis-indicator-cell">{row.name}</span>
                          </td>
                          <td className="run-analysis-unit-cell">{row.unit || "-"}</td>
                          <td className={`numeric ${row.selectedValue === 0 ? "run-analysis-value--zero" : ""}`}>
                            {row.selectedValue.toExponential(3)}
                          </td>
                        </tr>
                      ),
                    )}
                    {indicatorRows.length === 0 && (
                      <tr>
                        <td colSpan={5}>
                          {!hasProductResultView
                            ? "后端尚未返回产品结果视图，暂不展示产品指标。"
                            : "无指标数值"}
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
          {showRunWarnings && (
            <div className="run-analysis-subdialog-mask" onClick={() => setShowRunWarnings(false)}>
              <section className="run-analysis-subdialog" onClick={(event) => event.stopPropagation()}>
                <div className="run-analysis-subdialog-head">
                  <strong>{`警告列表 (${runIssues.length})`}</strong>
                  <button type="button" className="drawer-close-btn" onClick={() => setShowRunWarnings(false)}>
                    {i18n.close}
                  </button>
                </div>
                <div className="run-analysis-table-wrap run-analysis-table-wrap-warning-dialog">
                  <table className="run-analysis-table">
                    <thead>
                      <tr>
                        <th style={{ width: 64 }}>#</th>
                        <th>内容</th>
                      </tr>
                    </thead>
                    <tbody>
                      {runIssues.map((issue, idx) => {
                        const text = typeof issue === "object" ? JSON.stringify(issue) : String(issue);
                        return (
                          <tr key={`warn_dialog_${idx}`}>
                            <td className="run-analysis-warning-index">{idx + 1}</td>
                            <td title={text}>{text}</td>
                          </tr>
                        );
                      })}
                      {runIssues.length === 0 && (
                        <tr>
                          <td colSpan={2}>无警告</td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </section>
            </div>
          )}
        </section>
        </div>
      )}
      {showTargetProductDialog && (
        <div className="overlay-modal" onClick={() => setShowTargetProductDialog(false)}>
          <section className="pm-modal target-product-modal" onClick={(event) => event.stopPropagation()}>
            <div className="pm-modal-head">
              <strong>{uiLanguage === "zh" ? "目标产品设置" : "Target Product"}</strong>
              <button type="button" className="drawer-close-btn" onClick={() => setShowTargetProductDialog(false)}>
                {i18n.close}
              </button>
            </div>
            <div className="target-product-form">
              <label className="span-2">
                {uiLanguage === "zh" ? "目标产品" : "Target product"}
                <select
                  value={targetProductDraftKey}
                  onChange={(event) => setTargetProductDraftKey(event.target.value)}
                  disabled={targetProductCandidates.length === 0}
                >
                  {targetProductCandidates.map((candidate) => (
                    <option key={candidate.productKey} value={candidate.productKey}>
                      {`${candidate.processName} / ${formatTargetProductDisplayLabel(
                        candidate.productName,
                        candidate.processName,
                        candidate.ptsProcessName,
                      )}`}
                    </option>
                  ))}
                </select>
              </label>
              <label className="span-2">
                {uiLanguage === "zh" ? "总量来源" : "Quantity source"}
                <select
                  value={targetProductDraftQuantityMode}
                  onChange={(event) => setTargetProductDraftQuantityMode(event.target.value as ProjectTargetQuantityMode)}
                >
                  <option value="functional_unit">{uiLanguage === "zh" ? "跟随清单界面数量" : "Follow inventory quantity"}</option>
                  <option value="custom">{uiLanguage === "zh" ? "自定义输入" : "Custom input"}</option>
                </select>
              </label>
              <label>
                {uiLanguage === "zh" ? "目标产量" : "Target quantity"}
                <input
                  type="number"
                  min="0"
                  step="any"
                  value={targetProductDraftQuantity}
                  onChange={(event) => setTargetProductDraftQuantity(event.target.value)}
                  disabled={targetProductDraftQuantityMode === "functional_unit"}
                />
              </label>
              <div className="target-product-preview">
                <span className="target-product-preview-label">{uiLanguage === "zh" ? "当前说明" : "Preview"}</span>
                <strong>
                  {draftTargetProduct
                    ? `${draftTargetProduct.processName} / ${formatTargetProductDisplayLabel(
                        draftTargetProduct.productName,
                        draftTargetProduct.processName,
                        draftTargetProduct.ptsProcessName,
                      )}`
                    : uiLanguage === "zh"
                      ? "默认首个产品"
                      : "Default first product"}
                </strong>
                <span>
                  {draftTargetProduct
                    ? uiLanguage === "zh"
                      ? `结果展示将按 ${draftTargetProductQuantityValue} ${draftTargetProductDisplayUnit || draftTargetProduct.unit || "单位"} 计算。`
                      : `Results will be displayed for ${draftTargetProductQuantityValue} ${draftTargetProductDisplayUnit || draftTargetProduct.unit || "unit"}.`
                    : uiLanguage === "zh"
                      ? "请选择目标产品。"
                      : "Please choose a target product."}
                </span>
                <span>
                  {targetProductDraftQuantityMode === "functional_unit"
                    ? uiLanguage === "zh"
                      ? "数量来源：跟随清单界面数量"
                      : "Quantity source: follow inventory quantity"
                    : uiLanguage === "zh"
                      ? "数量来源：自定义输入"
                      : "Quantity source: custom input"}
                </span>
              </div>
            </div>
            <div className="pm-modal-actions">
              <button type="button" className="pm-ghost-btn" onClick={() => setShowTargetProductDialog(false)}>
                {uiLanguage === "zh" ? "取消" : "Cancel"}
              </button>
              <button type="button" onClick={saveTargetProductConfig}>
                {uiLanguage === "zh" ? "保存" : "Save"}
              </button>
            </div>
          </section>
        </div>
      )}
      <main className="main">
        <NodePalette />
        <section className="workspace">
          {connectionHint && (
            <div
              className={`inline-banner inline-banner--${connectionHintLevel === "error" ? "warning" : connectionHintLevel} inline-banner--action`}
            >
              <span onClick={clearConnectionHint}>{connectionHint}</span>
              {connectionFix?.type === "insert_market" && (
                <div className="inline-banner-actions">
                  <button
                    type="button"
                    className="link-btn"
                    onClick={(event) => {
                      event.stopPropagation();
                      applyConnectionFix();
                    }}
                  >
                    自动插入 Market 并重连
                  </button>
                </div>
              )}
            </div>
          )}
          {hydrated ? (
            <>
              {showPtsCanvasLoading ? (
                <div className="workspace-loading">{uiLanguage === "zh" ? "正在加载 PTS..." : "Loading PTS..."}</div>
              ) : (
                <GraphCanvas
                  canvasLoadKey={canvasLoadKey}
                  onOpenProjectTarget={openTargetProductDialog}
                  onRequestUnpackPts={handleUnpackPts}
                />
              )}
              <NodeCreatorDrawer />
              <UnitProcessImportDialog />
              {showPtsPublishWarnings && (
                <div className="run-analysis-subdialog-mask" onClick={() => setShowPtsPublishWarnings(false)}>
                  <section className="run-analysis-subdialog pts-warning-subdialog" onClick={(event) => event.stopPropagation()}>
                    <div className="run-analysis-subdialog-head">
                      <strong>
                        {uiLanguage === "zh"
                          ? `PTS 建模提醒 (${ptsPublishWarnings.length})`
                          : `PTS Modeling Warnings (${ptsPublishWarnings.length})`}
                      </strong>
                      <button type="button" className="drawer-close-btn" onClick={() => setShowPtsPublishWarnings(false)}>
                        {i18n.close}
                      </button>
                    </div>
                    <div className="run-analysis-table-wrap run-analysis-table-wrap-warning-dialog">
                      <table className="run-analysis-table">
                        <thead>
                          <tr>
                            <th style={{ width: 64 }}>#</th>
                            <th>{uiLanguage === "zh" ? "节点" : "Node"}</th>
                            <th>{uiLanguage === "zh" ? "提醒" : "Warning"}</th>
                            <th>{uiLanguage === "zh" ? "定位" : "Locate"}</th>
                          </tr>
                        </thead>
                        <tbody>
                          {ptsPublishWarnings.map((warning, idx) => {
                            const warningText = formatPtsModelingWarningMessage(warning, uiLanguage);
                            const metrics =
                              warning.actualTotal != null || warning.expectedTotal != null
                                ? `actual=${warning.actualTotal ?? "-"}, expected=${warning.expectedTotal ?? "-"}`
                                : "";
                            const ptsNodeId = warning.ptsNodeId;
                            return (
                              <tr key={`pts_publish_warning_${warning.code}_${idx}`}>
                                <td className="run-analysis-warning-index">{idx + 1}</td>
                                <td title={warning.nodeName || warning.ptsUuid || warning.ptsNodeId || warning.code}>
                                  {warning.nodeName || warning.ptsUuid || warning.ptsNodeId || warning.code}
                                </td>
                                <td title={warningText}>
                                  <div>{warningText}</div>
                                  <div className="pts-warning-meta">
                                    {[warning.code, metrics].filter(Boolean).join(" | ")}
                                  </div>
                                </td>
                                <td>
                                  {ptsNodeId ? (
                                    <button
                                      type="button"
                                      className="link-btn"
                                      onClick={() => {
                                        openNodeInspector(ptsNodeId);
                                        setShowPtsPublishWarnings(false);
                                      }}
                                    >
                                      {uiLanguage === "zh" ? "定位 PTS" : "Locate PTS"}
                                    </button>
                                  ) : (
                                    "-"
                                  )}
                                </td>
                              </tr>
                            );
                          })}
                          {ptsPublishWarnings.length === 0 && (
                            <tr>
                              <td colSpan={4}>{uiLanguage === "zh" ? "无提醒" : "No warnings"}</td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </section>
                </div>
              )}
              <InspectorPanel />
              <FlowBalanceDialog />
              <PtsPortEditorDialog />
              <PtsVersionHistoryDialog
                open={ptsHistoryOpen}
                loading={ptsHistoryLoading}
                ptsName={activePtsNode?.data.name ?? ""}
                ptsUuid={activePtsUuid ?? ""}
                compileItems={ptsCompileHistory}
                publishedItems={ptsPublishedHistory}
                activePublishedVersion={ptsHistoryActivePublishedVersion}
                onClose={() => setPtsHistoryOpen(false)}
              />
            </>
          ) : (
            <div className="workspace-loading">正在恢复项目视图...</div>
          )}
        </section>
      </main>
    </div>
  );
}


