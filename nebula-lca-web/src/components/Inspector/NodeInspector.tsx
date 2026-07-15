import { getApiBase } from "../../apiBase";
import type { Node } from "@xyflow/react";
import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { AllocationBasisMethod, FlowPort, LcaNodeData, ProcessMode, UnitGroupSwitchSnapshot } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { CreateFlowDialog } from "../CreateFlowDialog";
import { FlowAllocationPropertiesModal, type FlowAllocationProperty } from "../FlowAllocationPropertiesModal";
import { TidasLocationCascade, normalizeTidasLocationValue } from "../TidasLocationCascade";
import { Checkbox } from "../ui/Checkbox";
import { MultiProductAllocationModal } from "./MultiProductAllocationModal";
import { IntermediateFlowLinkPanel } from "./IntermediateFlowLinkPanel";
import type { SourcePolicy } from "../ProjectManagement/ProjectManagement";
import { useTianGongFlowRefresh } from "../../services/tiangongFlowRefresh";

const DEV_NODE_DEBUG = Boolean(import.meta.env.DEV);
const debugNode = (scope: string, payload?: unknown) => {
  if (!DEV_NODE_DEBUG) {
    return;
  }
  if (payload === undefined) {
    console.info(`[NODE_DEBUG] ${scope}`);
    return;
  }
  console.info(`[NODE_DEBUG] ${scope}`, payload);
};

type Props = {
  node: Node<LcaNodeData>;
  onStatus?: (text: string) => void;
  sourcePolicy?: SourcePolicy;
  initialTab?: TabKey;
  openProcessInfoOnMount?: boolean;
};

type TabKey = "external_in" | "external_out";
type FlowTarget = "in_intermediate" | "in_elementary" | "out_intermediate" | "out_elementary";
type AssocDirection = "input" | "output";

type CatalogFlow = {
  flow_uuid: string;
  flow_name: string;
  flow_name_en?: string | null;
  flow_type: string;
  default_unit: string;
  unit_group: string;
  compartment?: string | null;
  source?: string | null;
  is_custom?: boolean;
  conversion_compatible?: boolean | null;
  conversion_mode?: "bidirectional" | "canonical" | "one_way_canonicalization" | null;
  conversion_target_flow_uuid?: string | null;
  conversion_package_version?: string | null;
};

type UnitDefinition = {
  unit_group: string;
  unit_name: string;
  factor_to_reference: number;
  is_reference: boolean;
};

type AllocationPreview = {
  factors: Record<string, number> | null;
  weights: Record<string, number>;
  method: string;
  message: string;
  ok: boolean;
};

type ProcessInfoDraft = {
  location: string;
  referenceYear: string;
  timeRepresentativeness: string;
  technologyDescription: string;
  referenceProductFlowUuid: string;
  referenceProductText: string;
};

type RemoteInventoryGroupKey = "in_intermediate" | "out_intermediate" | "in_elementary" | "out_elementary";

type RemoteInventoryGroupState = {
  items: FlowPort[];
  total: number;
  page: number;
  loading: boolean;
  error: string;
};

const API_BASE = getApiBase();

function updatePortValue(ports: FlowPort[], portId: string, key: keyof FlowPort, value: unknown): FlowPort[] {
  return ports.map((port) => {
    if (port.id !== portId) {
      return port;
    }
    if (key === "amount") {
      return { ...port, amount: Number(value) || 0 };
    }
    if (key === "unit") {
      return { ...port, unit: String(value) };
    }
    if (key === "externalSaleAmount") {
      return { ...port, externalSaleAmount: Number(value) || 0 };
    }
    if (key === "showOnNode") {
      return { ...port, showOnNode: Boolean(value) };
    }
    if (key === "isProduct") {
      return { ...port, isProduct: Boolean(value) };
    }
    return { ...port, [key]: value as never };
  });
}

function processModeForNode(node: Node<LcaNodeData>): ProcessMode {
  if (node.data.nodeKind !== "unit_process") {
    return "normalized";
  }
  return node.data.mode;
}

function isMarketProcess(node: Node<LcaNodeData>): boolean {
  return node.data.nodeKind === "market_process" || node.data.processUuid.startsWith("market_");
}

function isLciDatasetNode(node: Node<LcaNodeData>): boolean {
  return node.data.nodeKind === "lci_dataset";
}

function parseHandlePortId(handle: string | undefined | null, prefix: string): string | undefined {
  if (!handle) {
    return undefined;
  }
  if (!handle.startsWith(prefix)) {
    return undefined;
  }
  return handle.slice(prefix.length);
}

function findOutputPortForFlow(node: Node<LcaNodeData>, flowUuid: string): FlowPort | undefined {
  return node.data.outputs.find((port) => port.flowUuid === flowUuid) ?? node.data.outputs[0];
}

function isElementaryFlow(flow: CatalogFlow): boolean {
  const t = (flow.flow_type || "").toLowerCase();
  return t.includes("elementary") || t.includes("basic") || t.includes("biosphere");
}

function getCatalogFlowDisplayName(flow: CatalogFlow, uiLanguage: "zh" | "en"): string {
  if (uiLanguage === "en") {
    return String(flow.flow_name_en ?? "").trim() || flow.flow_name;
  }
  return flow.flow_name;
}

function flowSourceGroup(flow: CatalogFlow): "ecoinvent" | "custom" | "tiangong" | "unknown" {
  const source = String(flow.source ?? "").trim().toLowerCase();
  if (source.startsWith("ecoinvent")) {
    return "ecoinvent";
  }
  if (flow.is_custom || source.includes("custom")) {
    return "custom";
  }
  if (source.includes("tidas") || source.includes("tiangong") || source.includes("ef3.1") || source === "ef") {
    return "tiangong";
  }
  return source ? "tiangong" : "unknown";
}

function normalizeUnitGroup(value: string | null | undefined): string {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\*/g, "_")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

const PREFERRED_UNIT_GROUP_BY_UNIT: Record<string, string> = {
  pg: "Units of mass",
  ng: "Units of mass",
  ug: "Units of mass",
  mg: "Units of mass",
  g: "Units of mass",
  kg: "Units of mass",
  t: "Units of mass",
  m: "Units of length",
  km: "Units of length",
  mm: "Units of length",
  cm: "Units of length",
  m2: "Units of area",
  "m²": "Units of area",
  l: "Units of volume",
  ml: "Units of volume",
  m3: "Units of volume",
  "m³": "Units of volume",
  j: "Units of energy",
  kj: "Units of energy",
  mj: "Units of energy",
  gj: "Units of energy",
  wh: "Units of energy",
  kwh: "Units of energy",
};

function isFlowAllowedBySourcePolicy(flow: CatalogFlow, target: FlowTarget | null, sourcePolicy: SourcePolicy, tidasAllowedUnitGroups: Set<string>): boolean {
  if (!target) {
    return true;
  }
  const sourceGroup = flowSourceGroup(flow);
  const isElementaryTarget = target === "in_elementary" || target === "out_elementary";
  if (sourcePolicy === "tidas_compliant") {
    const unitGroup = normalizeUnitGroup(flow.unit_group);
    if (unitGroup && tidasAllowedUnitGroups.size > 0 && !tidasAllowedUnitGroups.has(unitGroup)) {
      return false;
    }
    return isElementaryTarget ? sourceGroup === "tiangong" : sourceGroup === "tiangong" || sourceGroup === "custom";
  }
  if (sourcePolicy === "ecoinvent_strict" && isElementaryTarget) {
    return sourceGroup === "ecoinvent";
  }
  return true;
}

function flowSourceSpaceForRequest(
  sourceFilter: string,
  sourcePolicy: SourcePolicy,
  isElementaryTarget: boolean,
): string {
  if (sourceFilter) {
    return sourceFilter;
  }
  if (isElementaryTarget && sourcePolicy === "tidas_compliant") {
    return "tiangong";
  }
  if (isElementaryTarget && sourcePolicy === "ecoinvent_strict") {
    return "ecoinvent";
  }
  return "";
}

function toPortFromReference(flow: CatalogFlow, direction: "input" | "output", type: "technosphere" | "biosphere"): FlowPort {
  const suffix = Math.random().toString(36).slice(2, 8);
  return {
    id: `${direction}_${suffix}`,
    flowUuid: flow.flow_uuid,
    name: flow.flow_name,
    flowNameEn: String(flow.flow_name_en ?? "").trim() || undefined,
    unit: flow.default_unit || "kg",
    unitGroup: flow.unit_group || undefined,
    amount: 0,
    isProduct: false,
    type,
    direction,
    showOnNode: type !== "biosphere",
  };
}

type FlowSectionProps = {
  title: string;
  uiLanguage: "zh" | "en";
  readOnly?: boolean;
  lockFields?: boolean;
  plainReadOnly?: boolean;
  allowShowOnNodeToggle?: boolean;
  showNodeColumn?: boolean;
  showActionColumn?: boolean;
  ports: FlowPort[];
  getDisplayName?: (port: FlowPort) => string;
  onChange: (next: FlowPort[]) => void;
  onAdd?: () => void;
  headerAction?: ReactNode;
  remoteTotal?: number;
  remotePage?: number;
  remoteLoading?: boolean;
  remoteError?: string;
  onRemotePageChange?: (page: number) => void;
  onDelete?: (id: string) => void;
  extraHeader?: string;
  renderExtraCell?: (port: FlowPort, idx: number) => ReactNode;
  extraHeader2?: string;
  renderExtraCell2?: (port: FlowPort, idx: number) => ReactNode;
  unitOptionsByPort?: Record<string, string[]>;
  onUnitChange?: (port: FlowPort, nextUnit: string) => void;
  onLink?: (port: FlowPort) => void;
};

function FlowSection({
  title,
  uiLanguage,
  readOnly = false,
  lockFields = false,
  plainReadOnly = false,
  allowShowOnNodeToggle = false,
  showNodeColumn = true,
  showActionColumn = true,
  ports,
  getDisplayName,
  onChange,
  onAdd,
  headerAction,
  remoteTotal,
  remotePage,
  remoteLoading = false,
  remoteError = "",
  onRemotePageChange,
  onDelete,
  extraHeader,
  renderExtraCell,
  extraHeader2,
  renderExtraCell2,
  unitOptionsByPort,
  onUnitChange,
  onLink,
}: FlowSectionProps) {
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);
  const pageSize = 10;
  const [page, setPage] = useState(1);
  const remoteMode = Boolean(onRemotePageChange);
  const showOnNodeLocked = readOnly || (lockFields && !allowShowOnNodeToggle);
  const hasExtra = Boolean(extraHeader && renderExtraCell);
  const hasExtra2 = Boolean(extraHeader2 && renderExtraCell2);
  const locked = readOnly || lockFields || plainReadOnly;
  const totalItems = remoteMode ? Number(remoteTotal ?? ports.length) || 0 : ports.length;
  const pageCount = Math.max(1, Math.ceil(totalItems / pageSize));
  const currentPage = remoteMode ? Math.min(remotePage ?? 1, pageCount) : Math.min(page, pageCount);
  const visiblePorts = remoteMode ? ports : ports.slice((currentPage - 1) * pageSize, currentPage * pageSize);
  return (
    <section className="inventory-section">
      <div className="inventory-section-head">
        <h4>{title} <span className="inventory-section-count">({totalItems})</span></h4>
        {headerAction}
        {onAdd && !plainReadOnly && (
          <button type="button" className="text-btn" disabled={locked} onClick={onAdd}>
            {t("+ 新增流", "+ Add Flow")}
          </button>
        )}
      </div>
      {totalItems > pageSize && (
        <div className="inventory-section-controls">
          <div className="inventory-section-pagination">
            <button
              type="button"
              className="ghost-btn"
              disabled={currentPage <= 1 || remoteLoading}
              onClick={() => {
                if (remoteMode) {
                  onRemotePageChange?.(Math.max(1, currentPage - 1));
                } else {
                  setPage((value) => Math.max(1, value - 1));
                }
              }}
            >
              {t("上一页", "Previous")}
            </button>
            <span>{currentPage} / {pageCount}</span>
            <button
              type="button"
              className="ghost-btn"
              disabled={currentPage >= pageCount || remoteLoading}
              onClick={() => {
                if (remoteMode) {
                  onRemotePageChange?.(Math.min(pageCount, currentPage + 1));
                } else {
                  setPage((value) => Math.min(pageCount, value + 1));
                }
              }}
            >
              {t("下一页", "Next")}
            </button>
          </div>
        </div>
      )}
      {remoteLoading && <div className="table-empty">{t("加载中", "Loading")}</div>}
      {!remoteLoading && remoteError && <div className="table-empty">{remoteError}</div>}
      <div className="inventory-grid-header">
        <div>{t("序号", "No.")}</div>
        <div>{t("流名称", "Flow Name")}</div>
        <div>{t("数值", "Amount")}</div>
        <div>{t("单位", "Unit")}</div>
        {hasExtra ? <div>{extraHeader}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
        {hasExtra2 ? <div>{extraHeader2}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
        {showNodeColumn ? <div>{t("显示", "Show")}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
        {showActionColumn ? <div>{t("操作", "Action")}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
      </div>
      {!remoteLoading && !remoteError && visiblePorts.map((port, idx) => (
        <div key={port.id} className="inventory-grid-row">
          <div>{(currentPage - 1) * pageSize + idx + 1}</div>
          <div className="flow-name-readonly" title={getDisplayName ? getDisplayName(port) : port.name}>
            {getDisplayName ? getDisplayName(port) : port.name}
          </div>
          {plainReadOnly ? (
            <div className="flow-value-readonly" title={String(Number.isFinite(port.amount) ? port.amount : 0)}>
              {Number.isFinite(port.amount) ? port.amount : 0}
            </div>
          ) : (
            <input
              type="number"
              disabled={locked}
              value={Number.isFinite(port.amount) ? port.amount : 0}
              onChange={(event) => {
                const next = Number(event.target.value);
                onChange(updatePortValue(ports, port.id, "amount", Number.isFinite(next) ? next : 0));
              }}
            />
          )}
          {plainReadOnly ? (
            <div className="flow-value-readonly" title={port.unit}>{port.unit}</div>
          ) : (
            <select
              disabled={locked}
              value={port.unit}
              onChange={(event) => {
                if (onUnitChange) {
                  onUnitChange(port, event.target.value);
                  return;
                }
                onChange(updatePortValue(ports, port.id, "unit", event.target.value));
              }}
            >
              {(unitOptionsByPort?.[port.id] ?? [port.unit]).map((unit) => (
                <option key={`${port.id}_${unit}`} value={unit}>
                  {unit}
                </option>
              ))}
            </select>
          )}
          {hasExtra ? <div className="extra-column-cell">{renderExtraCell?.(port, idx)}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
          {hasExtra2 ? <div className="extra-column-cell">{renderExtraCell2?.(port, idx)}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
          {showNodeColumn ? (
            <label className="inline-checkbox">
              <input
                type="checkbox"
                checked={port.showOnNode}
                disabled={showOnNodeLocked || plainReadOnly}
                onChange={(event) => onChange(updatePortValue(ports, port.id, "showOnNode", event.target.checked))}
              />
            </label>
          ) : <div className="inventory-grid-spacer" aria-hidden="true" />}
          {showActionColumn ? (
            <div className="inventory-action-cell">
              {onLink && port.type !== "biosphere" && !plainReadOnly && (
                <button
                  type="button"
                  className="link-btn"
                  disabled={locked}
                  onClick={() => onLink(port)}
                >
                  {t("关联", "Link")}
                </button>
              )}
              {!plainReadOnly && (
                <button
                  type="button"
                  className="link-btn danger"
                  disabled={locked || !onDelete}
                  onClick={() => onDelete?.(port.id)}
                >
                  {t("删除", "Delete")}
                </button>
              )}
            </div>
          ) : <div className="inventory-grid-spacer" aria-hidden="true" />}
        </div>
      ))}
      {!remoteLoading && !remoteError && ports.length === 0 && <div className="table-empty">{t("暂无数据", "No data")}</div>}
    </section>
  );
}

export function NodeInspector({ node, onStatus, sourcePolicy = "open_mixed", initialTab, openProcessInfoOnMount = false }: Props) {
  const [tab, setTab] = useState<TabKey>("external_in");
  const [flowPicker, setFlowPicker] = useState<{ open: boolean; target: FlowTarget | null }>({ open: false, target: null });
  const [createFlowDialog, setCreateFlowDialog] = useState<{ open: boolean; target: FlowTarget | null }>({ open: false, target: null });
  const [flowSearchInput, setFlowSearchInput] = useState("");
  const [flowSearchQuery, setFlowSearchQuery] = useState("");
  const [flowCategoryLevel1, setFlowCategoryLevel1] = useState("");
  const [flowSourceFilter, setFlowSourceFilter] = useState("");
  const [flowConversionCompatibleOnly, setFlowConversionCompatibleOnly] = useState(false);
  const [flowCategoryOptions, setFlowCategoryOptions] = useState<Array<{ category: string; count: number }>>([]);
  const [catalogFlows, setCatalogFlows] = useState<CatalogFlow[]>([]);
  const [allocationPropertyPort, setAllocationPropertyPort] = useState<FlowPort | null>(null);
  const [allocationModalOpen, setAllocationModalOpen] = useState(false);
  const [processInfoOpen, setProcessInfoOpen] = useState(false);
  const [processInfoDraft, setProcessInfoDraft] = useState<ProcessInfoDraft>({
    location: "",
    referenceYear: "2026",
    timeRepresentativeness: "",
    technologyDescription: "",
    referenceProductFlowUuid: "",
    referenceProductText: "",
  });
  const [processLocationLoadError, setProcessLocationLoadError] = useState("");
  const [tidasAllowedUnitGroups, setTidasAllowedUnitGroups] = useState<Set<string>>(new Set());
  const [unitDefinitions, setUnitDefinitions] = useState<UnitDefinition[]>([]);
  const [flowUnitGroupByUuid, setFlowUnitGroupByUuid] = useState<Record<string, string>>({});
  const [flowDefaultUnitByUuid, setFlowDefaultUnitByUuid] = useState<Record<string, string>>({});
  const [flowTypeByUuid, setFlowTypeByUuid] = useState<Record<string, string>>({});
  const [flowNameEnByUuid, setFlowNameEnByUuid] = useState<Record<string, string>>({});
  const [loadingFlows, setLoadingFlows] = useState(false);
  const [flowLoadError, setFlowLoadError] = useState("");
  const [flowPage, setFlowPage] = useState(1);
  const [flowTotal, setFlowTotal] = useState(0);
  const flowPageSize = 20;
  const [assocDialog, setAssocDialog] = useState<{ open: boolean; direction: AssocDirection; port: FlowPort | null }>({
    open: false,
    direction: "output",
    port: null,
  });
  const [saleDialog, setSaleDialog] = useState<{ open: boolean; portId: string | null; value: number }>({
    open: false,
    portId: null,
    value: 0,
  });
  const [pendingMarketOutputSelection, setPendingMarketOutputSelection] = useState(false);
  const [productRuleHint, setProductRuleHint] = useState("");
  const [selectedNodeId, setSelectedNodeId] = useState("");
  const createEmptyRemoteGroup = (): RemoteInventoryGroupState => ({
    items: [],
    total: 0,
    page: 1,
    loading: false,
    error: "",
  });
  const [lciInventoryGroups, setLciInventoryGroups] = useState<Record<RemoteInventoryGroupKey, RemoteInventoryGroupState>>({
    in_intermediate: createEmptyRemoteGroup(),
    out_intermediate: createEmptyRemoteGroup(),
    in_elementary: createEmptyRemoteGroup(),
    out_elementary: createEmptyRemoteGroup(),
  });

  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const zh = uiLanguage === "zh";
  const tiangongFlowRefresh = useTianGongFlowRefresh({
    language: uiLanguage,
    onSuccess: (result) => {
      setCatalogFlows((current) => current.map((flow) => (
        flow.flow_uuid === result.flow_uuid
          ? { ...flow, ...result.flow }
          : flow
      )));
    },
    onStatus,
  });
  const updateNode = useLcaGraphStore((state) => state.updateNode);
  const setNodeMode = useLcaGraphStore((state) => state.setNodeMode);
  const setMarketAllowMixedFlows = useLcaGraphStore((state) => state.setMarketAllowMixedFlows);
  const replaceMarketOutputFlow = useLcaGraphStore((state) => state.replaceMarketOutputFlow);
  const setConnectionHint = useLcaGraphStore((state) => state.setConnectionHint);
  const nodes = useLcaGraphStore((state) => state.nodes);
  const getPortDisplayName = (port: FlowPort): string => {
    if (uiLanguage !== "en") {
      return port.name;
    }
    return String(port.flowNameEn ?? "").trim() || String(flowNameEnByUuid[port.flowUuid] ?? "").trim() || port.name;
  };
  const edges = useLcaGraphStore((state) => state.edges);
  const upsertOutputLink = useLcaGraphStore((state) => state.upsertOutputLink);
  const updateEdgeData = useLcaGraphStore((state) => state.updateEdgeData);
  const unitAutoScaleEnabled = useLcaGraphStore((state) => state.unitAutoScaleEnabled);
  const setUnitAutoScaleEnabled = useLcaGraphStore((state) => state.setUnitAutoScaleEnabled);
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);

  useEffect(() => {
    if (initialTab) {
      setTab(initialTab);
    }
  }, [initialTab, node.id]);

  const unitOptionsByGroup = useMemo(() => {
    const map = new Map<string, string[]>();
    for (const row of unitDefinitions) {
      if (!row.unit_group) {
        continue;
      }
      const list = map.get(row.unit_group) ?? [];
      if (!list.includes(row.unit_name)) {
        list.push(row.unit_name);
      }
      map.set(row.unit_group, list);
    }
    return map;
  }, [unitDefinitions]);

  const normalizedUnitGroupLookup = useMemo(() => {
    const normalize = (value: string) => value.trim().toLowerCase();
    const map = new Map<string, string>();
    for (const key of unitOptionsByGroup.keys()) {
      const norm = normalize(key);
      if (!map.has(norm)) {
        map.set(norm, key);
      }
    }
    return map;
  }, [unitOptionsByGroup]);

  const resolveUnitGroupKey = useMemo(() => {
    const normalize = (value: string) => value.trim().toLowerCase();
    return (rawGroup?: string): string | undefined => {
      if (!rawGroup) {
        return undefined;
      }
      if (unitOptionsByGroup.has(rawGroup)) {
        return rawGroup;
      }
      return normalizedUnitGroupLookup.get(normalize(rawGroup));
    };
  }, [normalizedUnitGroupLookup, unitOptionsByGroup]);

  const unitGroupsByUnitName = useMemo(() => {
    const map = new Map<string, Set<string>>();
    for (const row of unitDefinitions) {
      const unitName = String(row.unit_name ?? "").trim().toLowerCase();
      if (!unitName || !row.unit_group) {
        continue;
      }
      const groups = map.get(unitName) ?? new Set<string>();
      groups.add(row.unit_group);
      map.set(unitName, groups);
    }
    return map;
  }, [unitDefinitions]);

  const resolveUnitGroupByUnit = useMemo(() => {
    return (unit?: string): string | undefined => {
      const unitName = String(unit ?? "").trim().toLowerCase();
      if (!unitName) {
        return undefined;
      }
      const groups = unitGroupsByUnitName.get(unitName);
      if (!groups || groups.size === 0) {
        return undefined;
      }
      const preferred = PREFERRED_UNIT_GROUP_BY_UNIT[unitName];
      if (preferred && groups.has(preferred)) {
        return preferred;
      }
      if (groups.size === 1) {
        return Array.from(groups)[0];
      }
      return undefined;
    };
  }, [unitGroupsByUnitName]);

  const resolvePortUnitGroup = useMemo(() => {
    return (port?: FlowPort): string | undefined => {
      if (!port) {
        return undefined;
      }
      if (port.unitGroup) {
        return port.unitGroup;
      }
      return flowUnitGroupByUuid[port.flowUuid];
    };
  }, [flowUnitGroupByUuid]);

  const resolvePortUnitGroupKey = useMemo(() => {
    return (port?: FlowPort): string | undefined => {
      if (!port) {
        return undefined;
      }
      return (
        resolveUnitGroupKey(port.unitGroupSwitch?.targetUnitGroup)
        ?? resolveUnitGroupByUnit(port.unit)
        ?? resolveUnitGroupKey(resolvePortUnitGroup(port))
      );
    };
  }, [resolvePortUnitGroup, resolveUnitGroupByUnit, resolveUnitGroupKey]);

  const unitOptionsByPort = useMemo(() => {
    const allPorts = [...node.data.inputs, ...node.data.outputs];
    const result: Record<string, string[]> = {};
    for (const port of allPorts) {
      const group = resolvePortUnitGroupKey(port);
      const groupOptions = group ? unitOptionsByGroup.get(group) : undefined;
      const options = groupOptions && groupOptions.length > 0 ? groupOptions : [port.unit];
      result[port.id] = Array.from(new Set([port.unit, ...options]));
    }
    return result;
  }, [node.data.inputs, node.data.outputs, resolvePortUnitGroupKey, unitOptionsByGroup]);

  const unitFactorByGroupAndName = useMemo(() => {
    const map = new Map<string, number>();
    for (const row of unitDefinitions) {
      map.set(`${row.unit_group}||${row.unit_name}`, Number(row.factor_to_reference) || 1);
    }
    return map;
  }, [unitDefinitions]);

  const referenceUnitByGroup = useMemo(() => {
    const map = new Map<string, string>();
    for (const row of unitDefinitions) {
      if (!row.unit_group) {
        continue;
      }
      if (row.is_reference || !map.has(row.unit_group)) {
        map.set(row.unit_group, row.unit_name);
      }
    }
    return map;
  }, [unitDefinitions]);

  const normalizeAllocationWeights = (weights: Record<string, number>): Record<string, number> | null => {
    const total = Object.values(weights).reduce((sum, value) => sum + (value > 0 ? value : 0), 0);
    if (total <= 0) {
      return null;
    }
    return Object.fromEntries(
      Object.entries(weights)
        .filter(([, value]) => value > 0)
        .map(([id, value]) => [id, value / total]),
    );
  };

  const calculateAllocationPreview = (ports: FlowPort[]): AllocationPreview => {
    if (ports.length <= 1) {
      return {
        factors: ports[0] ? { [ports[0].id]: 1 } : {},
        weights: ports[0] ? { [ports[0].id]: 1 } : {},
        method: "single_product",
        message: t("单产品过程无需分配", "Single product process does not need allocation"),
        ok: true,
      };
    }

    const manualPorts = ports.filter((port) =>
      port.allocationBasis?.method === "manual_factor"
      || port.allocationFactor !== null && port.allocationFactor !== undefined
    );
    if (manualPorts.length > 0) {
      const factors: Record<string, number> = {};
      for (const port of ports) {
        const factorValue = Number(port.allocationFactor);
        factors[port.id] = Number.isFinite(factorValue) ? factorValue : 0;
      }
      const complete = manualPorts.length === ports.length && Object.values(factors).every((value) => Number.isFinite(value) && value >= 0);
      const total = Object.values(factors).reduce((sum, value) => sum + (Number.isFinite(value) ? value : 0), 0);
      if (complete && Math.abs(total - 1) < 0.01) {
        return {
          factors,
          weights: factors,
          method: "manual_factor",
          message: t("手填分配系数有效", "Manual allocation factors are valid"),
          ok: true,
        };
      }
      return {
        factors: null,
        weights: factors,
        method: "manual_factor",
        message: t("手填分配系数必须完整填写且合计为 1", "Manual allocation factors must be complete and sum to 1"),
        ok: false,
      };
    }

    const groups = Array.from(new Set(ports.map((port) => resolvePortUnitGroupKey(port) ?? port.unitGroup ?? "").filter(Boolean)));
    const weights = Object.fromEntries(
      ports.map((port) => {
        const unitGroup = resolvePortUnitGroupKey(port) ?? port.unitGroup ?? "";
        const unitFactor = unitFactorByGroupAndName.get(`${unitGroup}||${port.unit}`) ?? 1;
        return [port.id, Math.abs(Number(port.amount) || 0) * unitFactor];
      }),
    );
    const factors = normalizeAllocationWeights(weights);
    return {
      factors,
      weights,
      method: "quantity",
      message: factors
        ? groups.length > 1
          ? t("单位组不一致，仅按单位换算后的数值预览分配；计算和导出前仍需切换到同一单位组或手填系数。", "Unit groups differ; preview is based on unit-converted amounts. Calculation and export still require one unit group or manual factors.")
          : t("已按单位组默认单位预览分配", "Previewing allocation by unit-group reference amounts.")
        : t("产品总量必须大于 0", "Total product amount must be greater than 0"),
      ok: Boolean(factors),
    };
  };

  const convertValue = async (
    value: number,
    fromUnit: string,
    toUnit: string,
    unitGroup?: string,
  ): Promise<number> => {
    if (!Number.isFinite(value) || fromUnit === toUnit) {
      return value;
    }
    try {
      const resp = await fetch(`${API_BASE}/units/convert`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          value,
          from_unit: fromUnit,
          to_unit: toUnit,
          unit_group: unitGroup,
        }),
      });
      if (!resp.ok) {
        return value;
      }
      const payload = (await resp.json()) as { converted_value?: number };
      return Number.isFinite(payload.converted_value) ? Number(payload.converted_value) : value;
    } catch {
      return value;
    }
  };

  const updatePortUnitWithConversion = async (
    section: "inputs" | "outputs",
    port: FlowPort,
    nextUnit: string,
  ) => {
    if (port.unit === nextUnit) {
      return;
    }
    const unitGroup = resolvePortUnitGroupKey(port);
    const nextUnitGroup = resolveUnitGroupByUnit(nextUnit) ?? unitGroup;
    if (marketProcess && (section === "inputs" || section === "outputs")) {
      const currentOutput = externalOutIntermediate[0];
      const canonicalGroup = nextUnitGroup ?? resolvePortUnitGroupKey(currentOutput ?? port);
      const marketInputs = externalInIntermediate;

      if (!unitAutoScaleEnabled) {
        updateNode(node.id, (current) => ({
          ...current,
          data: {
            ...current.data,
            inputs: current.data.inputs.map((item) =>
              item.type === "biosphere"
                ? item
                : {
                  ...item,
                  unit: nextUnit,
                  unitGroup: canonicalGroup ?? item.unitGroup,
                },
            ),
            outputs: current.data.outputs.map((item) => ({
              ...item,
              unit: nextUnit,
              unitGroup: canonicalGroup ?? item.unitGroup,
            })),
          },
        }));
        return;
      }

      const convertedInputs = await Promise.all(
        marketInputs.map(async (item) => ({
          id: item.id,
          amount: await convertValue(item.amount, item.unit, nextUnit, canonicalGroup),
        })),
      );
      const inputAmountById = new Map(convertedInputs.map((item) => [item.id, item.amount]));
      const convertedOutputAmount = currentOutput
        ? await convertValue(currentOutput.amount, currentOutput.unit, nextUnit, canonicalGroup)
        : undefined;
      const convertedOutputSale = currentOutput
        ? await convertValue(currentOutput.externalSaleAmount ?? 0, currentOutput.unit, nextUnit, canonicalGroup)
        : undefined;

      updateNode(node.id, (current) => ({
        ...current,
        data: {
          ...current.data,
          inputs: current.data.inputs.map((item) =>
            item.type === "biosphere"
              ? item
              : {
                ...item,
                unit: nextUnit,
                unitGroup: canonicalGroup ?? item.unitGroup,
                amount: inputAmountById.get(item.id) ?? item.amount,
              },
          ),
          outputs: current.data.outputs.map((item) => ({
            ...item,
            unit: nextUnit,
            unitGroup: canonicalGroup ?? item.unitGroup,
            amount: convertedOutputAmount ?? item.amount,
            externalSaleAmount: convertedOutputSale ?? item.externalSaleAmount,
          })),
        },
      }));
      return;
    }

    if (!unitAutoScaleEnabled) {
      updateNode(node.id, (current) => {
        const list = current.data[section] as FlowPort[];
        const nextList = list.map((item) =>
          item.id === port.id
            ? {
              ...item,
              unit: nextUnit,
              unitGroup: nextUnitGroup ?? item.unitGroup,
            }
            : item,
        );
        return {
          ...current,
          data: {
            ...current.data,
            [section]: nextList,
          },
        };
      });
      return;
    }
    const nextAmount = await convertValue(port.amount, port.unit, nextUnit, unitGroup);
    const nextExternalSale =
      section === "outputs"
        ? await convertValue(port.externalSaleAmount ?? 0, port.unit, nextUnit, unitGroup)
        : undefined;

    updateNode(node.id, (current) => {
      const list = current.data[section] as FlowPort[];
      const isMarket = isMarketProcess(current);
      const nextList = list.map((item) =>
        item.id === port.id
          ? {
            ...item,
            unit: nextUnit,
            unitGroup: nextUnitGroup ?? item.unitGroup,
            amount: nextAmount,
            externalSaleAmount: section === "outputs" ? nextExternalSale : item.externalSaleAmount,
          }
          : item,
      );
      const dataPatch: Partial<LcaNodeData> = {
        [section]: nextList,
      };
      if (isMarket && section === "outputs") {
        dataPatch.inputs = current.data.inputs.map((input) => ({
          ...input,
          unit: nextUnit,
          unitGroup: nextUnitGroup ?? input.unitGroup,
        }));
      }
      return {
        ...current,
        data: {
          ...current.data,
          ...dataPatch,
        },
      };
    });
  };

  const applyFlowUnitGroupSwitch = (sourcePort: FlowPort, properties: FlowAllocationProperty[]) => {
    const property = properties[0];
    const factor = Number(property?.value);
    if (!property || !property.targetUnitGroup || !Number.isFinite(factor) || factor <= 0) {
      return;
    }
    const buildSwitchedPort = (port: FlowPort): FlowPort => {
      const existingSwitch = port.unitGroupSwitch;
      const flowDefaultUnitGroup = flowUnitGroupByUuid[port.flowUuid] ?? flowUnitGroupByUuid[sourcePort.flowUuid] ?? "";
      const flowDefaultUnit = flowDefaultUnitByUuid[port.flowUuid] ?? flowDefaultUnitByUuid[sourcePort.flowUuid] ?? "";
      const sourceUnitGroup = flowDefaultUnitGroup || existingSwitch?.sourceUnitGroup || port.unitGroup || sourcePort.unitGroup || "";
      const sourceUnit = flowDefaultUnit || existingSwitch?.sourceUnit || port.unit;
      const sourceReferenceUnit = referenceUnitByGroup.get(sourceUnitGroup) || flowDefaultUnit || existingSwitch?.sourceReferenceUnit || property.basisUnit || sourceUnit;
      const currentUnitGroup = port.unitGroup || sourceUnitGroup;
      const currentUnitFactor = unitFactorByGroupAndName.get(`${currentUnitGroup}||${port.unit}`) ?? 1;
      const sourceReferenceFactor = unitFactorByGroupAndName.get(`${sourceUnitGroup}||${sourceReferenceUnit}`) ?? 1;
      const targetUnit = property.targetUnit || referenceUnitByGroup.get(property.targetUnitGroup) || "";
      const targetUnitFactor = unitFactorByGroupAndName.get(`${property.targetUnitGroup}||${targetUnit}`) ?? 1;
      const currentUnitGroupKey = normalizeUnitGroup(currentUnitGroup);
      const sourceUnitGroupKey = normalizeUnitGroup(sourceUnitGroup);
      const targetUnitGroupKey = normalizeUnitGroup(property.targetUnitGroup);
      const deriveSourceAmount = (rawAmount: number | undefined, existingSourceAmount?: number): number | undefined => {
        if (rawAmount === undefined) {
          return undefined;
        }
        const amount = Number(rawAmount) || 0;
        if (currentUnitGroupKey && currentUnitGroupKey === targetUnitGroupKey) {
          return sourceReferenceFactor > 0 && factor > 0
            ? (amount * currentUnitFactor) / (sourceReferenceFactor * factor)
            : amount / factor;
        }
        if (currentUnitGroupKey && currentUnitGroupKey === sourceUnitGroupKey) {
          return sourceReferenceFactor > 0
            ? (amount * currentUnitFactor) / sourceReferenceFactor
            : amount;
        }
        return Number.isFinite(existingSourceAmount ?? NaN) ? Number(existingSourceAmount) : amount;
      };
      const sourceAmount = deriveSourceAmount(Number(port.amount), existingSwitch?.sourceAmount) ?? 0;
      const sourceExternalSaleAmount = deriveSourceAmount(port.externalSaleAmount, existingSwitch?.sourceExternalSaleAmount);
      const nextAmount = targetUnitFactor > 0
        ? (sourceAmount * sourceReferenceFactor * factor) / targetUnitFactor
        : sourceAmount * factor;
      const nextExternalSaleAmount =
        sourceExternalSaleAmount === undefined
          ? undefined
          : targetUnitFactor > 0
            ? ((Number(sourceExternalSaleAmount) || 0) * sourceReferenceFactor * factor) / targetUnitFactor
            : (Number(sourceExternalSaleAmount) || 0) * factor;
      const unitGroupSwitch: UnitGroupSwitchSnapshot = {
        sourceFlowUuid: port.flowUuid,
        sourceUnitGroup,
        sourceUnit,
        sourceReferenceUnit,
        sourceAmount,
        sourceExternalSaleAmount,
        targetUnitGroup: property.targetUnitGroup,
        targetUnit,
        targetReferenceUnit: targetUnit,
        factor,
        source: property.source ?? undefined,
        note: property.note ?? undefined,
      };
      return {
        ...port,
        unit: targetUnit || port.unit,
        unitGroup: property.targetUnitGroup,
        amount: nextAmount,
        externalSaleAmount: nextExternalSaleAmount,
        unitGroupSwitch,
      };
    };
    const switchedSourcePort = buildSwitchedPort(sourcePort);

    updateNode(node.id, (current) => {
      const applyToPort = (port: FlowPort): FlowPort => {
        if (port.id !== sourcePort.id) {
          return port;
        }
        return buildSwitchedPort(port);
      };
      return {
        ...current,
        data: {
          ...current.data,
          inputs: current.data.inputs.map(applyToPort),
          outputs: current.data.outputs.map(applyToPort),
        },
      };
    });

    for (const edge of edges) {
      const sourcePortId = parseHandlePortId(edge.sourceHandle, "out:");
      const targetPortId = parseHandlePortId(edge.targetHandle, "in:");
      if (edge.source === node.id && sourcePortId === sourcePort.id) {
        if (edge.data?.quantityMode === "dual") {
          updateEdgeData(edge.id, { unit: switchedSourcePort.unit, providerAmount: switchedSourcePort.amount });
        }
      } else if (edge.target === node.id && targetPortId === sourcePort.id) {
        if (edge.data?.quantityMode === "dual") {
          updateEdgeData(edge.id, { unit: switchedSourcePort.unit, consumerAmount: switchedSourcePort.amount });
        }
      }
    }
  };

  const resetFlowUnitGroupSwitch = (sourcePort: FlowPort) => {
    const existingSwitch = sourcePort.unitGroupSwitch;
    const sourceUnitGroup = flowUnitGroupByUuid[sourcePort.flowUuid] ?? existingSwitch?.sourceUnitGroup ?? sourcePort.unitGroup ?? "";
    const sourceUnit = flowDefaultUnitByUuid[sourcePort.flowUuid] ?? existingSwitch?.sourceUnit ?? existingSwitch?.sourceReferenceUnit ?? sourcePort.unit;
    const sourceReferenceUnit = referenceUnitByGroup.get(sourceUnitGroup) ?? existingSwitch?.sourceReferenceUnit ?? sourceUnit;
    const sourceReferenceFactor = unitFactorByGroupAndName.get(`${sourceUnitGroup}||${sourceReferenceUnit}`) ?? 1;
    const sourceUnitFactor = unitFactorByGroupAndName.get(`${sourceUnitGroup}||${sourceUnit}`) ?? sourceReferenceFactor;
    const targetUnitGroup = sourcePort.unitGroup || existingSwitch?.targetUnitGroup || "";
    const targetUnit = sourcePort.unit || existingSwitch?.targetUnit || "";
    const targetUnitFactor = unitFactorByGroupAndName.get(`${targetUnitGroup}||${targetUnit}`) ?? 1;
    const switchFactor = Number(existingSwitch?.factor);

    const toSourceUnitAmount = (rawAmount: number | undefined, storedSourceAmount?: number): number | undefined => {
      if (rawAmount === undefined) {
        return undefined;
      }
      let sourceReferenceAmount = Number(storedSourceAmount);
      if (!Number.isFinite(sourceReferenceAmount)) {
        const amount = Number(rawAmount) || 0;
        sourceReferenceAmount =
          Number.isFinite(switchFactor) && switchFactor > 0 && sourceReferenceFactor > 0
            ? (amount * targetUnitFactor) / (sourceReferenceFactor * switchFactor)
            : amount;
      }
      return sourceUnitFactor > 0 ? (sourceReferenceAmount * sourceReferenceFactor) / sourceUnitFactor : sourceReferenceAmount;
    };

    const nextAmount = toSourceUnitAmount(Number(sourcePort.amount), existingSwitch?.sourceAmount) ?? Number(sourcePort.amount || 0);
    const nextExternalSaleAmount = toSourceUnitAmount(sourcePort.externalSaleAmount, existingSwitch?.sourceExternalSaleAmount);

    const restoredPort: FlowPort = {
      ...sourcePort,
      unit: sourceUnit,
      unitGroup: sourceUnitGroup,
      amount: nextAmount,
      externalSaleAmount: nextExternalSaleAmount,
      unitGroupSwitch: undefined,
    };

    updateNode(node.id, (current) => {
      const applyToPort = (port: FlowPort): FlowPort => {
        if (port.id !== sourcePort.id) {
          return port;
        }
        return {
          ...port,
          unit: restoredPort.unit,
          unitGroup: restoredPort.unitGroup,
          amount: restoredPort.amount,
          externalSaleAmount: restoredPort.externalSaleAmount,
          unitGroupSwitch: undefined,
        };
      };
      return {
        ...current,
        data: {
          ...current.data,
          inputs: current.data.inputs.map(applyToPort),
          outputs: current.data.outputs.map(applyToPort),
        },
      };
    });

    for (const edge of edges) {
      const sourcePortId = parseHandlePortId(edge.sourceHandle, "out:");
      const targetPortId = parseHandlePortId(edge.targetHandle, "in:");
      if (edge.source === node.id && sourcePortId === sourcePort.id) {
        if (edge.data?.quantityMode === "dual") {
          updateEdgeData(edge.id, { unit: restoredPort.unit, providerAmount: restoredPort.amount });
        }
      } else if (edge.target === node.id && targetPortId === sourcePort.id) {
        if (edge.data?.quantityMode === "dual") {
          updateEdgeData(edge.id, { unit: restoredPort.unit, consumerAmount: restoredPort.amount });
        }
      }
    }
    setAllocationPropertyPort(null);
    onStatus?.(t("已恢复为 flow 默认单位组。", "Restored the flow default unit group."));
  };


  const externalInIntermediate = node.data.inputs.filter((p) => p.type !== "biosphere");
  const externalInElementary = node.data.inputs.filter((p) => p.type === "biosphere");
  const externalOutIntermediate = node.data.outputs.filter((p) => p.type !== "biosphere");
  const externalOutElementary = node.data.outputs.filter((p) => p.type === "biosphere");
  const productOutputs = externalOutIntermediate.filter((port) => Boolean(port.isProduct));
  const updateLciInventoryGroup = (groupKey: RemoteInventoryGroupKey, patch: Partial<RemoteInventoryGroupState>) => {
    setLciInventoryGroups((prev) => ({
      ...prev,
      [groupKey]: { ...prev[groupKey], ...patch },
    }));
  };
  const openProcessInfoModal = () => {
    const selectedProduct =
      productOutputs.find((port) => port.flowUuid === node.data.referenceProductFlowUuid) ??
      productOutputs.find((port) => port.name === node.data.referenceProduct) ??
      productOutputs[0];
    setProcessInfoDraft({
      location: node.data.location ?? "",
      referenceYear: String(node.data.referenceYear ?? 2026),
      timeRepresentativeness: node.data.timeRepresentativeness ?? "",
      technologyDescription: node.data.technologyDescription ?? "Nebula generated",
      referenceProductFlowUuid: selectedProduct?.flowUuid ?? "",
      referenceProductText: node.data.referenceProduct || selectedProduct?.name || "",
    });
    setProcessLocationLoadError("");
    setProcessInfoOpen(true);
  };
  const saveProcessInfo = () => {
    if (processLocationLoadError && processInfoDraft.location.trim()) {
      onStatus?.(processLocationLoadError);
      return;
    }
    const selectedProduct = productOutputs.find((port) => port.flowUuid === processInfoDraft.referenceProductFlowUuid);
    const rawYear = Number(processInfoDraft.referenceYear);
    const referenceYear = Number.isInteger(rawYear) && rawYear >= 1000 && rawYear <= 9999 ? rawYear : 2026;
    updateNode(node.id, (current) => ({
      ...current,
      data: {
        ...current.data,
        location: normalizeTidasLocationValue(processInfoDraft.location),
        referenceYear,
        timeRepresentativeness: processInfoDraft.timeRepresentativeness.trim(),
        technologyDescription: processInfoDraft.technologyDescription.trim() || "Nebula generated",
        referenceProduct: selectedProduct?.name ?? processInfoDraft.referenceProductText.trim(),
        referenceProductFlowUuid: selectedProduct?.flowUuid ?? current.data.referenceProductFlowUuid,
        referenceProductDirection: selectedProduct ? "output" : current.data.referenceProductDirection,
      },
    }));
    setProcessInfoOpen(false);
    onStatus?.(t("过程信息已保存。", "Process metadata saved."));
  };
  useEffect(() => {
    if (openProcessInfoOnMount) {
      openProcessInfoModal();
    }
  }, [openProcessInfoOnMount, node.id]);
  const allocationPreview = useMemo(
    () => calculateAllocationPreview(productOutputs),
    [productOutputs, unitFactorByGroupAndName, resolvePortUnitGroupKey, uiLanguage],
  );

  const updateOutputPortAllocation = (portId: string, patch: Partial<FlowPort>) => {
    updateNode(node.id, (current) => ({
      ...current,
      data: {
        ...current.data,
        outputs: current.data.outputs.map((port) => (port.id === portId ? { ...port, ...patch } : port)),
      },
    }));
  };

  const applyAllocationFactors = (factors: Record<string, number> | null, method: AllocationBasisMethod) => {
    if (!factors) {
      setProductRuleHint(t("分配依据还不完整，无法写入分配系数。", "Allocation basis is incomplete; factors were not applied."));
      return;
    }
    updateNode(node.id, (current) => ({
      ...current,
      data: {
        ...current.data,
        outputs: current.data.outputs.map((port) => {
          if (factors[port.id] === undefined) {
            return port;
          }
          return {
            ...port,
            allocationFactor: Number(factors[port.id].toFixed(8)),
            allocationBasis: { ...(port.allocationBasis ?? {}), method },
          };
        }),
      },
    }));
  };

  const setProductQuantityAllocationMode = () => {
    updateNode(node.id, (current) => ({
      ...current,
      data: {
        ...current.data,
        outputs: current.data.outputs.map((port) =>
          port.isProduct
            ? {
              ...port,
              allocationFactor: null,
              allocationBasis: { method: "quantity" },
            }
            : port,
        ),
      },
    }));
  };

  const setProductManualAllocationMode = () => {
    updateNode(node.id, (current) => ({
      ...current,
      data: {
        ...current.data,
        outputs: current.data.outputs.map((port) => {
          if (!port.isProduct) {
            return port;
          }
          const previewFactor = allocationPreview.factors?.[port.id];
          return {
            ...port,
            allocationFactor: Number.isFinite(previewFactor ?? NaN) ? Number(previewFactor) : 0,
            allocationBasis: {
              ...(port.allocationBasis ?? {}),
              method: "manual_factor",
            },
          };
        }),
      },
    }));
  };

  useEffect(() => {
    debugNode("inventory:nodePorts", {
      nodeId: node.id,
      nodeName: node.data.name,
      outputs: node.data.outputs.map((port) => ({
        id: port.id,
        name: port.name,
        flowUuid: port.flowUuid,
        type: port.type,
        isProduct: Boolean(port.isProduct),
      })),
      grouped: {
        externalOutIntermediate: externalOutIntermediate.map((port) => ({
          id: port.id,
          name: port.name,
          type: port.type,
          isProduct: Boolean(port.isProduct),
        })),
        externalOutElementary: externalOutElementary.map((port) => ({
          id: port.id,
          name: port.name,
          type: port.type,
          isProduct: Boolean(port.isProduct),
        })),
      },
    });
  }, [externalOutElementary, externalOutIntermediate, node.id, node.data.name, node.data.outputs]);

  const mode = processModeForNode(node);
  const marketProcess = isMarketProcess(node);
  const lciNode = isLciDatasetNode(node);
  const lciVectorProcessUuid =
    lciNode && node.data.processUuid && !node.data.processUuid.startsWith("lci_")
      ? node.data.processUuid
      : "";
  const usesRemoteLciInventory = Boolean(lciNode && lciVectorProcessUuid);
  const ptsNode = node.data.nodeKind === "pts_module";
  const importedLocked = node.data.importMode === "locked";
  const marketAllowMixedFlows = Boolean(node.data.marketAllowMixedFlows);
  const enforceMarketUuidConsistency = !marketAllowMixedFlows;
  const marketInputShareTotal = marketProcess
    ? externalInIntermediate.reduce((sum, port) => sum + (Number.isFinite(port.amount) ? port.amount : 0), 0)
    : 0;
  const marketInputShareDiff = Math.abs(marketInputShareTotal - 1);
  const marketInputShareOk = marketInputShareDiff <= 1e-9;
  const marketOutput = marketProcess ? externalOutIntermediate[0] : undefined;
  const marketInputUnitOptionsByPort = useMemo(() => {
    if (!marketProcess) {
      return unitOptionsByPort;
    }
    const group = resolvePortUnitGroupKey(marketOutput ?? externalInIntermediate[0]);
    if (!group) {
      return unitOptionsByPort;
    }
    const candidates = unitOptionsByGroup.get(group);
    if (!candidates || candidates.length === 0) {
      return unitOptionsByPort;
    }
    const next: Record<string, string[]> = { ...unitOptionsByPort };
    for (const port of externalInIntermediate) {
      next[port.id] = candidates;
    }
    return next;
  }, [externalInIntermediate, marketOutput, marketProcess, resolvePortUnitGroupKey, unitOptionsByGroup, unitOptionsByPort]);
  const marketOutputOk = Boolean(
    marketOutput &&
    externalOutIntermediate.length === 1 &&
    Math.abs((marketOutput.amount ?? 0) - 1) <= 1e-9 &&
    marketOutput.isProduct,
  );
  const settingCandidates = nodes.filter((candidate) => candidate.id !== node.id);
  const canAutoNormalizeMarketInputs = marketProcess && externalInIntermediate.length > 0 && marketInputShareTotal > 0;

  const toRemotePort = (item: Record<string, unknown>, groupKey: RemoteInventoryGroupKey): FlowPort => {
    const direction: "input" | "output" = groupKey.startsWith("out_") ? "output" : "input";
    const elementary = groupKey.endsWith("_elementary");
    const flowUuid = String(item.flow_uuid ?? "");
    const flowKeyId = String(item.flow_key_id ?? "").trim();
    return {
      id: `exchange-summary::${groupKey}::${flowKeyId || flowUuid || Math.random().toString(36).slice(2, 8)}`,
      flowUuid,
      name: String(item.flow_name ?? flowUuid),
      flowNameEn: String(item.flow_name_en ?? "").trim() || undefined,
      unit: String(item.unit ?? ""),
      unitGroup: String(item.unit_group ?? "").trim() || undefined,
      amount: Number.isFinite(Number(item.amount)) ? Number(item.amount) : 0,
      type: elementary ? "biosphere" : "technosphere",
      direction,
      showOnNode: false,
      isProduct: Boolean(item.is_product),
      sourceSystem: String(item.source ?? "").trim() || undefined,
    };
  };

  useEffect(() => {
    if (!usesRemoteLciInventory) {
      setLciInventoryGroups({
        in_intermediate: createEmptyRemoteGroup(),
        out_intermediate: createEmptyRemoteGroup(),
        in_elementary: createEmptyRemoteGroup(),
        out_elementary: createEmptyRemoteGroup(),
      });
      return;
    }
    const groupKeys: RemoteInventoryGroupKey[] = ["in_intermediate", "out_intermediate", "in_elementary", "out_elementary"];
    const controllers = new Map<RemoteInventoryGroupKey, AbortController>();
    groupKeys.forEach((groupKey) => {
      const current = lciInventoryGroups[groupKey];
      const controller = new AbortController();
      controllers.set(groupKey, controller);
      setLciInventoryGroups((prev) => ({
        ...prev,
        [groupKey]: { ...prev[groupKey], loading: true, error: "" },
      }));
      const params = new URLSearchParams({
        group: groupKey,
        page: String(current.page),
        page_size: "10",
      });
      fetch(`${API_BASE}/reference/processes/${encodeURIComponent(lciVectorProcessUuid)}/exchange-summary?${params.toString()}`, {
        signal: controller.signal,
      })
        .then((resp) => {
          if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
          }
          return resp.json();
        })
        .then((payload) => {
          const group = payload?.groups?.[groupKey] ?? {};
          const items = Array.isArray(group.items) ? group.items : [];
          setLciInventoryGroups((prev) => ({
            ...prev,
            [groupKey]: {
              ...prev[groupKey],
              items: items.map((item: Record<string, unknown>) => toRemotePort(item, groupKey)),
              total: Number(group.total ?? 0) || 0,
              loading: false,
              error: "",
            },
          }));
        })
        .catch((error: unknown) => {
          if (controller.signal.aborted) {
            return;
          }
          setLciInventoryGroups((prev) => ({
            ...prev,
            [groupKey]: {
              ...prev[groupKey],
              items: [],
              total: 0,
              loading: false,
              error: error instanceof Error ? error.message : String(error),
            },
          }));
        });
    });
    return () => {
      controllers.forEach((controller) => controller.abort());
    };
  }, [
    usesRemoteLciInventory,
    lciVectorProcessUuid,
    lciInventoryGroups.in_intermediate.page,
    lciInventoryGroups.out_intermediate.page,
    lciInventoryGroups.in_elementary.page,
    lciInventoryGroups.out_elementary.page,
  ]);

  useEffect(() => {
    if (!marketProcess) {
      if (pendingMarketOutputSelection) {
        setPendingMarketOutputSelection(false);
      }
      return;
    }
    if (externalOutIntermediate.length === 1 && pendingMarketOutputSelection) {
      setPendingMarketOutputSelection(false);
    }
  }, [externalOutIntermediate.length, marketProcess, pendingMarketOutputSelection]);

  useEffect(() => {
    if (!flowPicker.open) {
      return;
    }
    setFlowCategoryLevel1("");
    setFlowSearchInput("");
    setFlowSearchQuery("");
    setFlowSourceFilter("");
    setFlowConversionCompatibleOnly(false);
    setFlowLoadError("");
    setFlowPage(1);
    setFlowTotal(0);
  }, [flowPicker.open, flowPicker.target]);

  useEffect(() => {
    if (!flowPicker.open) {
      return;
    }

    let canceled = false;
    const target = flowPicker.target;
    const isElementaryTarget = target === "in_elementary" || target === "out_elementary";

    setLoadingFlows(true);
    setFlowLoadError("");
    const params = new URLSearchParams();
    params.set("page", String(flowPage));
    params.set("page_size", String(flowPageSize));
    if (flowSearchQuery.trim()) {
      params.set("search", flowSearchQuery.trim());
      params.set("keyword", flowSearchQuery.trim());
      params.set("q", flowSearchQuery.trim());
    }
    if (flowCategoryLevel1) {
      params.set("category_level_1", flowCategoryLevel1);
    }
    if (isElementaryTarget) {
      params.set("type", "elementary_flow");
    } else {
      params.set("type", "intermediate_flow");
    }
    const sourceSpace = flowSourceSpaceForRequest(flowSourceFilter, sourcePolicy, isElementaryTarget);
    if (sourceSpace) {
      params.set("source_space", sourceSpace);
    }
    if (isElementaryTarget && flowConversionCompatibleOnly) {
      params.set("conversion_target", "ef_tidas");
    }
    params.set("_ts", String(Date.now()));
    const endpoint = `${API_BASE}/flows?${params.toString()}`;
    fetch(endpoint, { cache: "no-store" })
      .then((resp) => {
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }
        return resp.json() as Promise<
          | CatalogFlow[]
          | {
            items?: Array<{
              flow_id?: string;
              flow_uuid?: string;
              flow_name?: string;
              flow_name_en?: string | null;
              type?: string;
              flow_type?: string;
              unit?: string;
              default_unit?: string;
              unit_group?: string;
              category?: string | null;
              compartment?: string | null;
            }>;
            total?: number;
            page?: number;
            page_size?: number;
          }
        >;
      })
      .then((payload) => {
        if (!canceled) {
          const rowsRaw = Array.isArray(payload) ? payload : payload.items ?? [];
          const total = Array.isArray(payload) ? rowsRaw.length : Number(payload.total ?? rowsRaw.length);
          const rows: CatalogFlow[] = rowsRaw.map((item) => {
            const row = item as Record<string, unknown>;
            return {
              flow_uuid: String(row.flow_uuid ?? row.flow_id ?? ""),
              flow_name: String(row.flow_name ?? ""),
              flow_name_en: (row.flow_name_en as string | null | undefined) ?? null,
              flow_type: String(row.flow_type ?? row.type ?? ""),
              default_unit: String(row.default_unit ?? row.unit ?? "kg"),
              unit_group: String(row.unit_group ?? ""),
              compartment:
                (row.compartment as string | null | undefined) ??
                (row.category as string | null | undefined) ??
                null,
              source: (row.source as string | null | undefined) ?? null,
              is_custom: Boolean(row.is_custom),
              conversion_compatible: Boolean(row.conversion_compatible),
              conversion_mode: (row.conversion_mode as CatalogFlow["conversion_mode"]) ?? null,
              conversion_target_flow_uuid: (row.conversion_target_flow_uuid as string | null | undefined) ?? null,
              conversion_package_version: (row.conversion_package_version as string | null | undefined) ?? null,
            };
          });
          setCatalogFlows(rows);
          setFlowTotal(Number.isFinite(total) ? total : rows.length);
        }
      })
      .catch(() => {
        if (!canceled) {
          setCatalogFlows([]);
          setFlowLoadError(t("流检索失败，请检查筛选条件或后端接口。", "Failed to load flows. Please check the filters or backend API."));
        }
      })
      .finally(() => {
        if (!canceled) {
          setLoadingFlows(false);
        }
      });

    return () => {
      canceled = true;
    };
  }, [flowCategoryLevel1, flowConversionCompatibleOnly, flowPage, flowPicker.open, flowPicker.target, flowSearchQuery, flowPageSize, flowSourceFilter, sourcePolicy]);

  useEffect(() => {
    if (!flowPicker.open) {
      return;
    }
    let canceled = false;
    const target = flowPicker.target;
    const isElementaryTarget = target === "in_elementary" || target === "out_elementary";
    const params = new URLSearchParams();
    params.set("level", "1");
    if (isElementaryTarget) {
      params.set("type", "elementary_flow");
    }
    const sourceSpace = flowSourceSpaceForRequest(flowSourceFilter, sourcePolicy, isElementaryTarget);
    if (sourceSpace) {
      params.set("source_space", sourceSpace);
    }
    if (isElementaryTarget && flowConversionCompatibleOnly) {
      params.set("conversion_target", "ef_tidas");
    }
    params.set("_ts", String(Date.now()));
    fetch(`${API_BASE}/flows/categories?${params.toString()}`, { cache: "no-store" })
      .then((resp) => {
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }
        return resp.json() as Promise<{ items?: Array<{ category?: string; count?: number }> }>;
      })
      .then((payload) => {
        if (canceled) {
          return;
        }
        const rows = Array.isArray(payload?.items) ? payload.items : [];
        const mapped = rows
          .map((row) => ({
            category: String(row.category ?? "").trim(),
            count: Number(row.count ?? 0),
          }))
          .filter((row) => row.category.length > 0);
        setFlowCategoryOptions(mapped);
      })
      .catch(() => {
        if (!canceled) {
          setFlowCategoryOptions([]);
        }
      });
    return () => {
      canceled = true;
    };
  }, [flowConversionCompatibleOnly, flowPicker.open, flowPicker.target, flowSourceFilter, sourcePolicy]);

  useEffect(() => {
    let canceled = false;
    fetch(`${API_BASE}/reference/units`)
      .then((resp) => {
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }
        return resp.json() as Promise<UnitDefinition[]>;
      })
      .then((rows) => {
        if (!canceled) {
          setUnitDefinitions(rows);
        }
      })
      .catch(() => {
        if (!canceled) {
          setUnitDefinitions([]);
        }
      });

    return () => {
      canceled = true;
    };
  }, []);

  useEffect(() => {
    let canceled = false;
    fetch(`${API_BASE}/reference/tidas-policy`, { cache: "no-store" })
      .then((resp) => {
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }
        return resp.json() as Promise<{ allowed_unit_groups?: string[] }>;
      })
      .then((payload) => {
        if (!canceled) {
          const allowed = Array.isArray(payload.allowed_unit_groups) ? payload.allowed_unit_groups : [];
          setTidasAllowedUnitGroups(new Set(allowed.map(normalizeUnitGroup).filter(Boolean)));
        }
      })
      .catch(() => {
        if (!canceled) {
          setTidasAllowedUnitGroups(new Set());
        }
      });
    return () => {
      canceled = true;
    };
  }, []);

  useEffect(() => {
    const visiblePorts = [...node.data.inputs, ...node.data.outputs].filter((port) => port.flowUuid);
    const needFetch = Array.from(new Set(visiblePorts.map((port) => port.flowUuid))).filter((uuid) => {
      if (!flowUnitGroupByUuid[uuid]) {
        return true;
      }
      if (!flowDefaultUnitByUuid[uuid]) {
        return true;
      }
      if (!flowTypeByUuid[uuid]) {
        return true;
      }
      const hasInlineEnglish = visiblePorts.some(
        (port) => port.flowUuid === uuid && String(port.flowNameEn ?? "").trim().length > 0,
      );
      if (uiLanguage === "en" && !hasInlineEnglish && !flowNameEnByUuid[uuid]) {
        return true;
      }
      return false;
    });
    if (needFetch.length === 0) {
      return;
    }
    let canceled = false;
    Promise.all(
      needFetch.map(async (flowUuid) => {
        const resp = await fetch(`${API_BASE}/reference/flows/${encodeURIComponent(flowUuid)}`);
        if (!resp.ok) {
          return null;
        }
        const row = (await resp.json()) as CatalogFlow;
        return row.flow_uuid ? row : null;
      }),
    )
      .then((rows) => {
        if (canceled) {
          return;
        }
        const unitPatch: Record<string, string> = {};
        const defaultUnitPatch: Record<string, string> = {};
        const typePatch: Record<string, string> = {};
        const namePatch: Record<string, string> = {};
        rows.forEach((row) => {
          if (!row) {
            return;
          }
          if (row.unit_group) {
            unitPatch[row.flow_uuid] = row.unit_group;
          }
          if (row.default_unit) {
            defaultUnitPatch[row.flow_uuid] = row.default_unit;
          }
          if (row.flow_type) {
            typePatch[row.flow_uuid] = row.flow_type;
          }
          const englishName = String(row.flow_name_en ?? "").trim();
          if (englishName) {
            namePatch[row.flow_uuid] = englishName;
          }
        });
        if (Object.keys(unitPatch).length > 0) {
          setFlowUnitGroupByUuid((prev) => ({ ...prev, ...unitPatch }));
        }
        if (Object.keys(defaultUnitPatch).length > 0) {
          setFlowDefaultUnitByUuid((prev) => ({ ...prev, ...defaultUnitPatch }));
        }
        if (Object.keys(typePatch).length > 0) {
          setFlowTypeByUuid((prev) => ({ ...prev, ...typePatch }));
        }
        if (Object.keys(namePatch).length > 0) {
          setFlowNameEnByUuid((prev) => ({ ...prev, ...namePatch }));
        }
      })
      .catch(() => {
        // ignore
      });
    return () => {
      canceled = true;
    };
  }, [flowDefaultUnitByUuid, flowNameEnByUuid, flowTypeByUuid, flowUnitGroupByUuid, node.data.inputs, node.data.outputs, uiLanguage]);

  const filteredFlows = useMemo(() => {
    const target = flowPicker.target;
    return catalogFlows
      .filter((flow) => {
        if (!target) {
          return true;
        }
        const elementary = isElementaryFlow(flow);
        if (target === "in_elementary" || target === "out_elementary") {
          return elementary;
        }
        if (!elementary) {
          return true;
        }
        return false;
      })
      .filter((flow) => {
        if (!isFlowAllowedBySourcePolicy(flow, target, sourcePolicy, tidasAllowedUnitGroups)) {
          return false;
        }
        return true;
      })
  }, [flowPicker.target, catalogFlows, sourcePolicy, tidasAllowedUnitGroups]);

  const applyFlowSearch = () => {
    setFlowSearchQuery(flowSearchInput);
    setFlowPage(1);
  };

  const totalPages = Math.max(1, Math.ceil(flowTotal / flowPageSize));

  const displayFlowType = (rawType: string): string => {
    const t = rawType.toLowerCase();
    if (t.includes("elementary") || t.includes("basic") || t.includes("biosphere")) {
      return uiLanguage === "zh" ? "基本流" : "Elementary Flow";
    }
    if (t.includes("waste")) {
      return uiLanguage === "zh" ? "废物流" : "Waste Flow";
    }
    if (t.includes("product")) {
      return uiLanguage === "zh" ? "产品流" : "Product Flow";
    }
    if (t.includes("intermediate")) {
      return uiLanguage === "zh" ? "中间流" : "Intermediate Flow";
    }
    return rawType || "-";
  };

  const addCatalogFlowForTarget = (flow: CatalogFlow, target: FlowTarget) => {
    if (flow.flow_uuid) {
      if (flow.unit_group) {
        setFlowUnitGroupByUuid((prev) => ({ ...prev, [flow.flow_uuid]: flow.unit_group }));
      }
      if (flow.flow_type) {
        setFlowTypeByUuid((prev) => ({ ...prev, [flow.flow_uuid]: flow.flow_type }));
      }
    }

    const isInput = target.startsWith("in_");
    const isElementary = target.endsWith("elementary");
    const direction = isInput ? "input" : "output";
    const type = isElementary ? "biosphere" : "technosphere";
    const newPort = toPortFromReference(flow, direction, type);
    if (marketProcess && !isInput && !isElementary) {
      replaceMarketOutputFlow(node.id, {
        flowUuid: newPort.flowUuid,
        name: newPort.name,
        unit: newPort.unit,
        unitGroup: newPort.unitGroup,
        type: newPort.type,
      });
      setPendingMarketOutputSelection(false);
      return;
    }

    updateNode(node.id, (current) => {
      if (isInput && !isElementary) {
        if (marketProcess && !marketAllowMixedFlows) {
          const strictUuid = current.data.outputs[0]?.flowUuid ?? current.data.inputs[0]?.flowUuid;
          if (strictUuid && strictUuid !== newPort.flowUuid) {
            return current;
          }
        }
        if (isLciDatasetNode(current)) {
          const existingIntermediateCount = current.data.inputs.filter((p) => p.type !== "biosphere").length;
          if (existingIntermediateCount > 0) {
            const confirmed = window.confirm(
              t(
                "LCI模块只允许一条中间流（且默认定义为产品），是否将新增的中间流替换原有的？",
                "LCI datasets allow only one intermediate flow, which is treated as the default product. Replace the existing intermediate flow?",
              ),
            );
            if (!confirmed) {
              return current;
            }
          }
          return {
            ...current,
            data: {
              ...current.data,
              mode: "normalized",
              lciRole: "waste_sink",
              inputs: [
                ...current.data.inputs.filter((p) => p.type === "biosphere"),
                { ...newPort, direction: "input", isProduct: true },
              ],
              outputs: [],
              referenceProduct: newPort.name,
              referenceProductFlowUuid: newPort.flowUuid,
              referenceProductDirection: "input",
            },
          };
        }
        if (
          !marketProcess &&
          current.data.inputs.some((p) => p.flowUuid === newPort.flowUuid && p.type !== "biosphere")
        ) {
          return current;
        }
        const marketOutputUnit = marketProcess ? current.data.outputs[0]?.unit : undefined;
        return {
          ...current,
          data: {
            ...current.data,
            inputs: [
              ...current.data.inputs,
              marketProcess && marketOutputUnit
                ? {
                  ...newPort,
                  unit: marketOutputUnit,
                }
                : newPort,
            ],
          },
        };
      }
      if (isInput && isElementary) {
        if (current.data.inputs.some((p) => p.flowUuid === newPort.flowUuid && p.type === "biosphere")) {
          return current;
        }
        return {
          ...current,
          data: {
            ...current.data,
            inputs: [...current.data.inputs, newPort],
          },
        };
      }
      if (!isInput && !isElementary) {
        if (isLciDatasetNode(current)) {
          const existingIntermediateCount = current.data.outputs.filter((p) => p.type !== "biosphere").length;
          if (existingIntermediateCount > 0) {
            const confirmed = window.confirm(
              t(
                "LCI模块只允许一条中间流（且默认定义为产品），是否将新增的中间流替换原有的？",
                "LCI datasets allow only one intermediate flow, which is treated as the default product. Replace the existing intermediate flow?",
              ),
            );
            if (!confirmed) {
              return current;
            }
          }
          return {
            ...current,
            data: {
              ...current.data,
              mode: "normalized",
              lciRole: "provider",
              inputs: current.data.inputs.filter((p) => p.type === "biosphere"),
              outputs: [{ ...newPort, direction: "output", isProduct: true }],
              referenceProduct: newPort.name,
              referenceProductFlowUuid: newPort.flowUuid,
              referenceProductDirection: "output",
            },
          };
        }
        if (current.data.outputs.some((p) => p.flowUuid === newPort.flowUuid)) {
          return current;
        }
        return {
          ...current,
          data: {
            ...current.data,
            outputs: [...current.data.outputs, newPort],
          },
        };
      }
      if (current.data.outputs.some((p) => p.flowUuid === newPort.flowUuid && p.type === "biosphere")) {
        return current;
      }
      return {
        ...current,
        data: {
          ...current.data,
          outputs: [...current.data.outputs, newPort],
        },
      };
    });
  };

  const addCatalogFlow = (flow: CatalogFlow) => {
    if (!flowPicker.target) {
      return;
    }
    addCatalogFlowForTarget(flow, flowPicker.target);
    setFlowPicker({ open: false, target: null });
  };

  const openAssociationDialog = (direction: AssocDirection, port: FlowPort) => {
    setAssocDialog({ open: true, direction, port });
    setSelectedNodeId("");
  };

  const applyAssociation = () => {
    if (!assocDialog.port || !selectedNodeId) {
      return;
    }
    if (assocDialog.direction === "output") {
      upsertOutputLink(node.id, assocDialog.port.id, selectedNodeId);
    } else {
      const sourceNode = nodes.find((n) => n.id === selectedNodeId);
      if (!sourceNode) {
        return;
      }
      const sourcePort = findOutputPortForFlow(sourceNode, assocDialog.port.flowUuid);
      if (!sourcePort) {
        return;
      }
      upsertOutputLink(sourceNode.id, sourcePort.id, node.id, assocDialog.port.id);
    }
    setAssocDialog({ open: false, direction: "output", port: null });
  };

  const linkedInputNames = (port: FlowPort): string[] => {
    const incoming = edges.filter((edge) => {
      if (edge.target !== node.id) {
        return false;
      }
      const targetPortId = parseHandlePortId(edge.targetHandle, "in:");
      return targetPortId === port.id;
    });
    return incoming.map((edge) => nodes.find((n) => n.id === edge.source)?.data.name).filter(Boolean) as string[];
  };

  const linkedOutputNames = (port: FlowPort): string[] => {
    const outgoing = edges.filter((edge) => edge.source === node.id && edge.data?.flowUuid === port.flowUuid);
    return outgoing.map((edge) => nodes.find((n) => n.id === edge.target)?.data.name).filter(Boolean) as string[];
  };

  const isReferenceProductPort = (port: FlowPort, _direction: AssocDirection): boolean => Boolean(port.isProduct);

  const getProductUnitGroupMismatches = (outputs: FlowPort[]): string[] => {
    const groups = Array.from(
      new Set(
        outputs
          .filter((port) => port.type !== "biosphere" && port.isProduct)
          .map((port) => resolvePortUnitGroupKey(port) ?? port.unitGroup ?? "")
          .map((group) => group.trim())
          .filter((group) => group.length > 0),
      ),
    );
    return groups;
  };

  const applyProductToggle = (direction: AssocDirection, portId: string, checked: boolean) => {
    let inputProductWarning = "";
    let productAllocationWarning = "";
    updateNode(node.id, (current) => {
      const patchPorts = (ports: FlowPort[], portDirection: AssocDirection) =>
        ports.map((port) => {
          if (portDirection === direction && port.id === portId) {
            return { ...port, isProduct: checked };
          }
          return port;
        });
      const nextInputs = patchPorts(current.data.inputs, "input");
      const nextOutputs = patchPorts(current.data.outputs, "output");

      if (current.data.nodeKind === "unit_process" && !isMarketProcess(current)) {
        const groups = getProductUnitGroupMismatches(nextOutputs);
        if (groups.length > 1) {
          productAllocationWarning = t("多产品存在不同单位组，请填写手动分配系数或密度/热值换算依据。", "Products use different unit groups; enter manual allocation factors or density/heating-value basis.");
        }
      }
      const toggledPort =
        direction === "input"
          ? nextInputs.find((port) => port.id === portId)
          : nextOutputs.find((port) => port.id === portId);
      const toggledFlowType = (toggledPort?.flowUuid ? flowTypeByUuid[toggledPort.flowUuid] : "") || "";
      const isCatalogProductFlow = toggledFlowType.toLowerCase().includes("product");
      if (current.data.nodeKind === "unit_process" && !isMarketProcess(current) && checked && direction === "input") {
        inputProductWarning = isCatalogProductFlow
          ? t("警告：输入的 Product flow 被定义为过程产品，请检查产品定义。", "Warning: the input Product flow was marked as a process product. Please check the product definition.")
          : t("警告：输入端被定义为产品流，请检查产品定义是否符合预期。", "Warning: the input was marked as a product flow. Please check whether the product definition is intended.");
      }

      const primaryProduct =
        nextOutputs.find((port) => port.isProduct) ??
        nextInputs.find((port) => port.isProduct) ??
        nextOutputs.find((port) => (current.data.referenceProductFlowUuid ?? "") === port.flowUuid) ??
        nextInputs.find((port) => (current.data.referenceProductFlowUuid ?? "") === port.flowUuid);

      return {
        ...current,
        data: {
          ...current.data,
          inputs: nextInputs,
          outputs: nextOutputs,
          referenceProduct: primaryProduct?.name ?? "",
          referenceProductFlowUuid: primaryProduct?.flowUuid,
          referenceProductDirection: primaryProduct?.direction,
        },
      };
    });
    setProductRuleHint(productAllocationWarning);
    if (inputProductWarning) {
      setConnectionHint(inputProductWarning);
    }
  };

  const assocLinkedItems = assocDialog.port
    ? assocDialog.direction === "input"
      ? linkedInputNames(assocDialog.port)
      : linkedOutputNames(assocDialog.port)
    : [];

  return (
    <div className="inspector-block">
      <div className="inspector-control-row">
        <div className="tabs compact-tabs page-tabs">
          <button type="button" className={tab === "external_in" ? "active" : ""} onClick={() => setTab("external_in")}>
            {t("输入", "Inputs")}
          </button>
          <button type="button" className={tab === "external_out" ? "active" : ""} onClick={() => setTab("external_out")}>
            {t("输出", "Outputs")}
          </button>
        </div>
        <div className="mode-switch-row">
          <span>{t("过程模式", "Process Mode")}</span>
          <div className="mode-toggle-group">
            <button
              type="button"
              className={mode === "balanced" ? "active" : ""}
              disabled={node.data.nodeKind !== "unit_process" || marketProcess}
              onClick={() => setNodeMode(node.id, "balanced")}
            >
              {t("守恒（balanced）", "Balanced")}
            </button>
            <button
              type="button"
              className={mode === "normalized" ? "active" : ""}
              disabled={false}
              onClick={() => setNodeMode(node.id, "normalized")}
            >
              {t("归一化（normalized）", "Normalized")}
            </button>
          </div>
          {!lciNode && (
            <label className="inline-checkbox">
              <input
                type="checkbox"
                checked={unitAutoScaleEnabled}
                disabled={importedLocked}
                onChange={(event) => setUnitAutoScaleEnabled(event.target.checked)}
              />
              {t("单位自动换算", "Unit auto conversion")}
            </label>
          )}
          {!ptsNode && (
            <button
              type="button"
              className="text-btn inspector-toolbar-btn"
              onClick={openProcessInfoModal}
            >
              {t("过程信息", "Process Info")}
            </button>
          )}
          {!marketProcess && !ptsNode && !lciNode && (
            <button
              type="button"
              className="text-btn inspector-toolbar-btn"
              onClick={() => setAllocationModalOpen(true)}
            >
              {t("多产品分配", "Allocation")}
            </button>
          )}
          {marketProcess && (
            <label className="inline-checkbox">
              <input
                type="checkbox"
                checked={enforceMarketUuidConsistency}
                disabled={importedLocked}
                onChange={(event) => setMarketAllowMixedFlows(node.id, !event.target.checked)}
              />
              {t("UUID一致", "UUID aligned")}
            </label>
          )}
        </div>
      </div>

      {(node.data.nodeKind !== "unit_process" && !marketProcess) && (
        <div className="mode-lock-hint">
          {lciNode
            ? t(
              "LCI 节点固定为归一化，只能定义一条输入或输出中间流（默认定义为产品）。",
              "LCI nodes are fixed to normalized mode and allow only one input or output intermediate flow, which is treated as the default product.",
            )
            : t("PTS 节点固定为归一化。", "PTS nodes are fixed to normalized mode.")}
        </div>
      )}
      {marketProcess && (
        <div className="market-option-block">
          <div className="market-option-row">
            <span className="market-inline-hint">{t("市场过程固定为归一化。", "Market processes are fixed to normalized mode.")}</span>
            <div
              className={`market-share-badge ${marketInputShareOk && marketOutputOk ? "ok" : "warn"}`}
              title={t("市场输入份额总和应为 1", "The sum of market input shares should be 1")}
            >
              {t("输入份额合计", "Input share total")} {marketInputShareTotal.toFixed(6)} {t("（目标 1）", "(target 1)")}
            </div>
            <button
              type="button"
              className="market-normalize-btn"
              disabled={importedLocked || !canAutoNormalizeMarketInputs}
              onClick={() =>
                updateNode(node.id, (current) => {
                  const technoInputs = current.data.inputs.filter((p) => p.type !== "biosphere");
                  const total = technoInputs.reduce((sum, p) => sum + (Number.isFinite(p.amount) ? p.amount : 0), 0);
                  if (!Number.isFinite(total) || total <= 0) {
                    return current;
                  }
                  return {
                    ...current,
                    data: {
                      ...current.data,
                      inputs: current.data.inputs.map((port) =>
                        port.type === "biosphere"
                          ? port
                          : {
                            ...port,
                            amount: (Number.isFinite(port.amount) ? port.amount : 0) / total,
                          },
                      ),
                    },
                  };
                })
              }
              title={t("按现有输入比例一键归一到总和 1", "Normalize current input shares to sum to 1")}
            >
              {t("自动归一化", "Auto Normalize")}
            </button>
            <button
              type="button"
              className="market-normalize-btn"
              disabled={importedLocked}
              onClick={() => {
                setPendingMarketOutputSelection(true);
                setFlowPicker({ open: true, target: "out_intermediate" });
              }}
              title={t("更换市场过程产品输出流", "Replace the product output flow for this market process")}
            >
              {t("更换产品流", "Change Product Flow")}
            </button>
          </div>
          {pendingMarketOutputSelection && (
            <div className="mode-lock-hint">{t("市场过程必须有且仅有一个产品输出，请先选择替代流。", "A market process must have exactly one product output. Please choose a replacement flow first.")}</div>
          )}
        </div>
      )}
      {productRuleHint && <div className="mode-lock-hint">{productRuleHint}</div>}
      {importedLocked && (
        <div className="mode-lock-hint">{t("该节点来自锁定导入模式，清单内容只读；允许切换“显示节点”和过程模式。", "This node comes from locked import mode. Inventory content is read-only; only show-node visibility and process mode can be changed.")}</div>
      )}
      {Array.isArray(node.data.importWarnings) && node.data.importWarnings.length > 0 && (
        <div className="mode-lock-hint">{t("导入提示：", "Import note: ")}{node.data.importWarnings[0]}</div>
      )}
      {tab === "external_in" && !lciNode && !ptsNode && !marketProcess && !importedLocked && (
        <IntermediateFlowLinkPanel node={node} onStatus={onStatus} />
      )}
      {tab === "external_in" && (
        <>
          {ptsNode ? (
            <FlowSection
              title={t("中间流", "Intermediate Flows")}
              uiLanguage={uiLanguage}
              readOnly
              lockFields
              ports={externalInIntermediate.filter((port) => (port.internalExposed ?? true) === true)}
              getDisplayName={getPortDisplayName}
              unitOptionsByPort={unitOptionsByPort}
              onChange={() => undefined}
            />
          ) : (
            <>
              <FlowSection
                title={t("中间流", "Intermediate Flows")}
                uiLanguage={uiLanguage}
                lockFields={importedLocked || lciNode}
                plainReadOnly={lciNode}
                allowShowOnNodeToggle={importedLocked && !lciNode}
                showNodeColumn={!lciNode}
                showActionColumn={!lciNode}
                ports={usesRemoteLciInventory ? lciInventoryGroups.in_intermediate.items : externalInIntermediate}
                getDisplayName={getPortDisplayName}
                unitOptionsByPort={marketInputUnitOptionsByPort}
                onUnitChange={(port, nextUnit) => {
                  void updatePortUnitWithConversion("inputs", port, nextUnit);
                }}
                extraHeader={t("单位组", "Unit Group")}
                extraHeader2={lciNode ? undefined : t("定义产品", "Product Def.")}
                renderExtraCell={(port) => (
                  lciNode
                    ? <div className="flow-value-readonly" title={resolvePortUnitGroupKey(port) || ""}>{resolvePortUnitGroupKey(port) || "-"}</div>
                    : (
                      <div className="row-setting-cell">
                        <button type="button" className="link-btn" disabled={!port.flowUuid} onClick={() => setAllocationPropertyPort(port)}>
                          {t("切换", "Switch")}
                        </button>
                      </div>
                    )
                )}
                renderExtraCell2={
                  lciNode
                    ? undefined
                    : (port) => (
                      <Checkbox
                        checked={isReferenceProductPort(port, "input")}
                        disabled={importedLocked || marketProcess}
                        ariaLabel={t("定义为产品", "Define as product")}
                        onCheckedChange={(checked) => applyProductToggle("input", port.id, checked)}
                      />
                    )
                }
                remoteTotal={usesRemoteLciInventory ? lciInventoryGroups.in_intermediate.total : undefined}
                remotePage={usesRemoteLciInventory ? lciInventoryGroups.in_intermediate.page : undefined}
                remoteLoading={usesRemoteLciInventory ? lciInventoryGroups.in_intermediate.loading : undefined}
                remoteError={usesRemoteLciInventory ? lciInventoryGroups.in_intermediate.error : undefined}
                onRemotePageChange={usesRemoteLciInventory ? (page) => updateLciInventoryGroup("in_intermediate", { page }) : undefined}
                onChange={(next) =>
                  updateNode(node.id, (current) => {
                    const marketUnit = marketProcess ? current.data.outputs[0]?.unit : undefined;
                    const patchedNext =
                      marketProcess && marketUnit
                        ? next.map((port) => ({
                          ...port,
                          unit: marketUnit,
                        }))
                        : next;
                    return {
                      ...current,
                      data: {
                        ...current.data,
                        inputs: [
                          ...patchedNext,
                          ...current.data.inputs.filter((port) => port.type === "biosphere"),
                        ],
                      },
                    };
                  })
                }
                onAdd={lciNode ? undefined : () => setFlowPicker({ open: true, target: "in_intermediate" })}
                onLink={!marketProcess && !lciNode ? (port) => openAssociationDialog("input", port) : undefined}
                onDelete={lciNode ? undefined : (id) =>
                  updateNode(node.id, (current) => ({
                    ...current,
                    data: {
                      ...current.data,
                      inputs: current.data.inputs.filter((p) => p.id !== id),
                    },
                  }))
                }
              />
              {!marketProcess && !ptsNode && (
                <FlowSection
                  title={
                    t("基本流", "Elementary Flows")
                  }
                  uiLanguage={uiLanguage}
                  readOnly={lciNode}
                  lockFields={importedLocked || lciNode}
                  plainReadOnly={lciNode}
                  showNodeColumn={false}
                  showActionColumn={!lciNode}
                  ports={usesRemoteLciInventory ? lciInventoryGroups.in_elementary.items : externalInElementary}
                  getDisplayName={getPortDisplayName}
                  unitOptionsByPort={unitOptionsByPort}
                  remoteTotal={usesRemoteLciInventory ? lciInventoryGroups.in_elementary.total : undefined}
                  remotePage={usesRemoteLciInventory ? lciInventoryGroups.in_elementary.page : undefined}
                  remoteLoading={usesRemoteLciInventory ? lciInventoryGroups.in_elementary.loading : undefined}
                  remoteError={usesRemoteLciInventory ? lciInventoryGroups.in_elementary.error : undefined}
                  onRemotePageChange={usesRemoteLciInventory ? (page) => updateLciInventoryGroup("in_elementary", { page }) : undefined}
                  onUnitChange={(port, nextUnit) => {
                    void updatePortUnitWithConversion("inputs", port, nextUnit);
                  }}
                  onChange={(next) =>
                    updateNode(node.id, (current) => ({
                      ...current,
                      data: {
                        ...current.data,
                        inputs: [
                          ...current.data.inputs.filter((port) => port.type !== "biosphere"),
                          ...next,
                        ],
                      },
                    }))
                  }
                  onAdd={lciNode ? undefined : () => setFlowPicker({ open: true, target: "in_elementary" })}
                  onDelete={lciNode ? undefined : (id) =>
                    updateNode(node.id, (current) => ({
                      ...current,
                      data: {
                        ...current.data,
                        inputs: current.data.inputs.filter((p) => p.id !== id),
                      },
                    }))
                  }
                />
              )}
            </>
          )}
        </>
      )}

      {tab === "external_out" && (
        <>
          {ptsNode ? (
            <FlowSection
              title={t("中间流", "Intermediate Flows")}
              uiLanguage={uiLanguage}
              readOnly
              lockFields
              ports={externalOutIntermediate.filter((port) => (port.internalExposed ?? true) === true)}
              getDisplayName={getPortDisplayName}
              unitOptionsByPort={unitOptionsByPort}
              onChange={() => undefined}
            />
          ) : (
            <>
              <FlowSection
                title={t("中间流", "Intermediate Flows")}
                uiLanguage={uiLanguage}
                readOnly={lciNode}
                lockFields={importedLocked || lciNode}
                plainReadOnly={lciNode}
                allowShowOnNodeToggle={importedLocked && !lciNode}
                showNodeColumn={!lciNode}
                showActionColumn={!lciNode}
                ports={usesRemoteLciInventory ? lciInventoryGroups.out_intermediate.items : externalOutIntermediate}
                getDisplayName={getPortDisplayName}
                unitOptionsByPort={unitOptionsByPort}
                onUnitChange={(port, nextUnit) => {
                  void updatePortUnitWithConversion("outputs", port, nextUnit);
                }}
                extraHeader={t("单位组", "Unit Group")}
                extraHeader2={lciNode ? undefined : t("定义产品", "Product Def.")}
                renderExtraCell={(port) => (
                  lciNode
                    ? <div className="flow-value-readonly" title={resolvePortUnitGroupKey(port) || ""}>{resolvePortUnitGroupKey(port) || "-"}</div>
                    : (
                      <div className="row-setting-cell">
                        <button type="button" className="link-btn" disabled={!port.flowUuid} onClick={() => setAllocationPropertyPort(port)}>
                          {t("切换", "Switch")}
                        </button>
                      </div>
                    )
                )}
                renderExtraCell2={lciNode ? undefined : (port) => {
                  if (marketProcess) {
                    return <span className="market-fixed-product">{t("固定产品", "Fixed Product")}</span>;
                  }
                  const checked = isReferenceProductPort(port, "output");
                  return (
                    <div className="product-sale-cell">
                      <Checkbox
                        checked={checked}
                        disabled={importedLocked || lciNode}
                        ariaLabel={t("定义为产品", "Define as product")}
                        onCheckedChange={(nextChecked) => applyProductToggle("output", port.id, nextChecked)}
                      />
                      <button
                        type="button"
                        className="sale-link-btn"
                        disabled={importedLocked || !checked}
                        onClick={() =>
                          setSaleDialog({
                            open: true,
                            portId: port.id,
                            value: Number.isFinite(port.externalSaleAmount ?? 0) ? (port.externalSaleAmount ?? 0) : 0,
                          })
                        }
                      >
                        {checked ? `${t("外售", "External Sale")}: ${port.externalSaleAmount ?? 0}` : `${t("外售", "External Sale")}: -`}
                      </button>
                    </div>
                  );
                }}
                remoteTotal={usesRemoteLciInventory ? lciInventoryGroups.out_intermediate.total : undefined}
                remotePage={usesRemoteLciInventory ? lciInventoryGroups.out_intermediate.page : undefined}
                remoteLoading={usesRemoteLciInventory ? lciInventoryGroups.out_intermediate.loading : undefined}
                remoteError={usesRemoteLciInventory ? lciInventoryGroups.out_intermediate.error : undefined}
                onRemotePageChange={usesRemoteLciInventory ? (page) => updateLciInventoryGroup("out_intermediate", { page }) : undefined}
                onChange={(next) =>
                  updateNode(node.id, (current) => ({
                    ...current,
                    data: {
                      ...current.data,
                      outputs: [
                        ...next.map((port, idx) =>
                          marketProcess
                            ? {
                              ...port,
                              amount: idx === 0 ? 1 : port.amount,
                              isProduct: idx === 0 ? true : Boolean(port.isProduct),
                            }
                            : port,
                        ),
                        ...current.data.outputs.filter((port) => port.type === "biosphere"),
                      ],
                    },
                  }))
                }
                onAdd={
                  lciNode
                    ? undefined
                    : marketProcess
                    ? () => {
                      setPendingMarketOutputSelection(true);
                      setFlowPicker({ open: true, target: "out_intermediate" });
                    }
                    : () => setFlowPicker({ open: true, target: "out_intermediate" })
                }
                onLink={lciNode ? undefined : (port) => openAssociationDialog("output", port)}
                onDelete={lciNode ? undefined : (id) => {
                  if (marketProcess) {
                    setPendingMarketOutputSelection(true);
                    updateNode(node.id, (current) => ({
                      ...current,
                      data: {
                        ...current.data,
                        outputs: current.data.outputs.filter((p) => p.id !== id),
                        referenceProduct: "",
                        referenceProductFlowUuid: undefined,
                        referenceProductDirection: undefined,
                      },
                    }));
                    setFlowPicker({ open: true, target: "out_intermediate" });
                    return;
                  }
                  updateNode(node.id, (current) => ({
                    ...current,
                    data: {
                      ...current.data,
                      outputs: current.data.outputs.filter((p) => p.id !== id),
                    },
                  }));
                }}
              />
              {!marketProcess && !ptsNode && (
                <FlowSection
                  title={
                    t("基本流", "Elementary Flows")
                  }
                  uiLanguage={uiLanguage}
                  readOnly={lciNode}
                  lockFields={importedLocked || lciNode}
                  plainReadOnly={lciNode}
                  showNodeColumn={false}
                  showActionColumn={!lciNode}
                  ports={usesRemoteLciInventory ? lciInventoryGroups.out_elementary.items : externalOutElementary}
                  getDisplayName={getPortDisplayName}
                  unitOptionsByPort={unitOptionsByPort}
                  remoteTotal={usesRemoteLciInventory ? lciInventoryGroups.out_elementary.total : undefined}
                  remotePage={usesRemoteLciInventory ? lciInventoryGroups.out_elementary.page : undefined}
                  remoteLoading={usesRemoteLciInventory ? lciInventoryGroups.out_elementary.loading : undefined}
                  remoteError={usesRemoteLciInventory ? lciInventoryGroups.out_elementary.error : undefined}
                  onRemotePageChange={usesRemoteLciInventory ? (page) => updateLciInventoryGroup("out_elementary", { page }) : undefined}
                  onUnitChange={(port, nextUnit) => {
                    void updatePortUnitWithConversion("outputs", port, nextUnit);
                  }}
                  onChange={(next) =>
                    updateNode(node.id, (current) => ({
                      ...current,
                      data: {
                        ...current.data,
                        outputs: [
                          ...current.data.outputs.filter((port) => port.type !== "biosphere"),
                          ...next,
                        ],
                      },
                    }))
                  }
                  onAdd={lciNode ? undefined : () => setFlowPicker({ open: true, target: "out_elementary" })}
                  onDelete={lciNode ? undefined : (id) =>
                    updateNode(node.id, (current) => ({
                      ...current,
                      data: {
                        ...current.data,
                        outputs: current.data.outputs.filter((p) => p.id !== id),
                      },
                    }))
                  }
                />
              )}
            </>
          )}
        </>
      )}

      {flowPicker.open && (
        <div className="overlay-modal">
          <div className="overlay-panel flow-picker-panel">
            <div className="overlay-head">
              <strong>{flowPicker.target?.includes("elementary") ? t("引用基本流", "Use Elementary Flow") : t("引用中间流", "Use Intermediate Flow")}</strong>
              <button type="button" className="drawer-close-btn" onClick={() => setFlowPicker({ open: false, target: null })}>
                {t("关闭", "Close")}
              </button>
            </div>
            <div className="overlay-filters">
              <div className="search-row flow-picker-search-row">
                <input
                  value={flowSearchInput}
                  onChange={(e) => setFlowSearchInput(e.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      applyFlowSearch();
                    }
                  }}
                  placeholder={t("输入流名称检索", "Search flow name")}
                />
                <select
                  value={flowCategoryLevel1}
                  onChange={(event) => {
                    setFlowCategoryLevel1(event.target.value);
                    setFlowPage(1);
                  }}
                >
                  <option value="">{t("全部分类", "All Categories")}</option>
                  {flowCategoryOptions.map((item) => (
                    <option key={item.category} value={item.category}>
                      {`${item.category} (${item.count})`}
                    </option>
                  ))}
                </select>
                <select
                  value={flowSourceFilter}
                  onChange={(event) => {
                    setFlowSourceFilter(event.target.value);
                    setFlowPage(1);
                  }}
                >
                  <option value="">{t("全部来源", "All Sources")}</option>
                  <option value="ecoinvent">ecoinvent</option>
                  <option value="tiangong">TIDAS/EF</option>
                  <option value="custom">custom</option>
                </select>
                {flowPicker.target?.includes("elementary") && (
                  <label className="flow-picker-compatible-toggle">
                    <input
                      type="checkbox"
                      checked={flowConversionCompatibleOnly}
                      onChange={(event) => {
                        setFlowConversionCompatibleOnly(event.target.checked);
                        setFlowCategoryLevel1("");
                        setFlowPage(1);
                      }}
                    />
                    <span>{t("仅显示 EF/ecoinvent 可转换流", "Convertible EF/ecoinvent only")}</span>
                  </label>
                )}
                <button type="button" className="search-btn" onClick={applyFlowSearch}>
                  {t("检索", "Search")}
                </button>
                {!flowPicker.target?.includes("elementary") && (
                  <button
                    type="button"
                    className="search-btn flow-picker-create-btn"
                    onClick={() => {
                      setFlowPicker({ open: false, target: null });
                      setCreateFlowDialog({ open: true, target: flowPicker.target });
                    }}
                  >
                    {t("+ 新建自定义流", "+ Create Custom Flow")}
                  </button>
                )}
              </div>
            </div>
            <div className="overlay-table">
              {loadingFlows && <div className="table-empty">{t("加载中...", "Loading...")}</div>}
              {!loadingFlows && flowLoadError && <div className="table-empty">{flowLoadError}</div>}
              {!loadingFlows && !flowLoadError && (
                <table>
                  <thead>
                    <tr>
                      <th className="flow-picker-type-col">{t("类型", "Type")}</th>
                      <th className="flow-picker-name-col">{t("流名称", "Flow Name")}</th>
                      <th className="flow-picker-unit-col">{t("单位", "Unit")}</th>
                      <th className="flow-picker-category-col">{t("分类", "Category")}</th>
                      <th className="flow-picker-source-col">{t("来源", "Source")}</th>
                      <th className="flow-picker-action-col">{t("操作", "Action")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredFlows.map((flow) => (
                      <tr key={flow.flow_uuid}>
                        <td className="flow-picker-type-cell">{displayFlowType(flow.flow_type)}</td>
                        <td className="flow-picker-name-cell" title={getCatalogFlowDisplayName(flow, uiLanguage)}>
                          <span>{getCatalogFlowDisplayName(flow, uiLanguage)}</span>
                          {flow.conversion_compatible && (
                            <span
                              className={`flow-conversion-badge flow-conversion-badge-${flow.conversion_mode ?? "canonical"}`}
                              title={`${t("EF 目标", "EF target")}: ${flow.conversion_target_flow_uuid ?? flow.flow_uuid}`}
                            >
                              {flow.conversion_mode === "bidirectional"
                                ? t("双向", "Bidirectional")
                                : flow.conversion_mode === "one_way_canonicalization"
                                  ? t("单向规范化", "One-way")
                                  : t("EF 规范流", "Canonical EF")}
                            </span>
                          )}
                        </td>
                        <td>{flow.default_unit}</td>
                        <td className="flow-picker-category-cell" title={flow.compartment || "-"}>
                          {flow.compartment || "-"}
                        </td>
                        <td className="flow-picker-source-cell" title={flow.source || "unknown"}>
                          {flow.source || "unknown"}
                        </td>
                        <td className="flow-picker-action-cell">
                          {flow.source === "tiangong" && (
                            <button
                              type="button"
                              className="pm-link-btn"
                              title={zh ? "从天工平台刷新该 Flow" : "Refresh this flow from TianGong platform"}
                              onClick={(e) => {
                                e.stopPropagation();
                                void tiangongFlowRefresh.refresh(flow.flow_uuid);
                              }}
                              disabled={tiangongFlowRefresh.busyFlowUuid !== ""}
                              aria-label={zh ? "刷新 Flow" : "Refresh flow"}
                            >
                              {tiangongFlowRefresh.busyFlowUuid === flow.flow_uuid
                                ? (zh ? "刷新中" : "Refreshing")
                                : (zh ? "刷新" : "Refresh")}
                            </button>
                          )}
                          <button type="button" className="flow-picker-use-btn" onClick={() => addCatalogFlow(flow)}>
                            {t("引用", "Use")}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            {!loadingFlows && !flowLoadError && (
              <div className="overlay-pagination">
                <span>{uiLanguage === "zh" ? `第 ${flowPage} / ${totalPages} 页，共 ${flowTotal} 条` : `Page ${flowPage} / ${totalPages}, total ${flowTotal}`}</span>
                <div className="overlay-pagination-actions">
                  <button type="button" className="ghost-btn" disabled={flowPage <= 1} onClick={() => setFlowPage(1)}>
                    {t("首页", "First")}
                  </button>
                  <button type="button" className="ghost-btn" disabled={flowPage <= 1} onClick={() => setFlowPage((prev) => Math.max(1, prev - 1))}>
                    {t("上一页", "Prev")}
                  </button>
                  <button
                    type="button"
                    className="ghost-btn"
                    disabled={flowPage >= totalPages}
                    onClick={() => setFlowPage((prev) => Math.min(totalPages, prev + 1))}
                  >
                    {t("下一页", "Next")}
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {assocDialog.open && assocDialog.port && (
        <div className="overlay-modal">
          <div className="overlay-panel small">
            <div className="overlay-head">
              <strong>{t("关联过程", "Link Process")}</strong>
              <button
                type="button"
                className="drawer-close-btn"
                onClick={() => setAssocDialog({ open: false, direction: "output", port: null })}
              >
                {t("关闭", "Close")}
              </button>
            </div>
            <div className="overlay-filters">
              <div className="flow-name-readonly" title={getPortDisplayName(assocDialog.port)}>{getPortDisplayName(assocDialog.port)}</div>
              {assocLinkedItems.length > 0 ? (
                <div className="linked-process-list">
                  {assocLinkedItems.map((item, idx) => (
                    <div key={`${item}_${idx}`} className="linked-process-item" title={item}>
                      {item}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="table-empty">{t("当前未关联过程", "No linked processes")}</div>
              )}
              <select value={selectedNodeId} onChange={(e) => setSelectedNodeId(e.target.value)}>
                <option value="">{t("选择已有过程", "Select Existing Process")}</option>
                {settingCandidates.map((candidate) => (
                  <option key={candidate.id} value={candidate.id}>
                    {candidate.data.name}
                  </option>
                ))}
              </select>
              <div className="assoc-actions">
                <button type="button" className="ghost-btn" onClick={() => setAssocDialog({ open: false, direction: "output", port: null })}>
                  {t("取消", "Cancel")}
                </button>
                <button type="button" onClick={applyAssociation} disabled={!selectedNodeId}>
                  {t("关联", "Link")}
                </button>
              </div>
              <div className="mode-lock-hint">{t("也可关闭弹窗后，在建模界面手动画线完成关联。", "You can also close this dialog and draw the connection manually on the modeling canvas.")}</div>
            </div>
          </div>
        </div>
      )}

      {saleDialog.open && saleDialog.portId && (
        <div className="overlay-modal">
          <div className="overlay-panel small">
            <div className="overlay-head">
              <strong>{t("编辑外售量", "Edit External Sale")}</strong>
              <button type="button" className="drawer-close-btn" onClick={() => setSaleDialog({ open: false, portId: null, value: 0 })}>
                {t("关闭", "Close")}
              </button>
            </div>
            <div className="overlay-filters">
              <label>
                {t("外售量", "External Sale Amount")}
                <input
                  type="number"
                  min={0}
                  value={Number.isFinite(saleDialog.value) ? saleDialog.value : 0}
                  onChange={(event) => {
                    const next = Number(event.target.value);
                    setSaleDialog((prev) => ({
                      ...prev,
                      value: Number.isFinite(next) ? Math.max(0, next) : 0,
                    }));
                  }}
                />
              </label>
              <div className="assoc-actions">
                <button type="button" className="ghost-btn" onClick={() => setSaleDialog({ open: false, portId: null, value: 0 })}>
                  {t("取消", "Cancel")}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    const portId = saleDialog.portId;
                    if (!portId) {
                      return;
                    }
                    updateNode(node.id, (current) => ({
                      ...current,
                      data: {
                        ...current.data,
                        outputs: updatePortValue(current.data.outputs, portId, "externalSaleAmount", saleDialog.value),
                      },
                    }));
                    setSaleDialog({ open: false, portId: null, value: 0 });
                  }}
                >
                  {t("确认", "Confirm")}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {processInfoOpen && (
        <div className="overlay-modal">
          <div className="overlay-panel process-info-modal">
            <div className="overlay-head">
              <strong>{t("过程信息", "Process Info")}</strong>
              <button type="button" className="drawer-close-btn" onClick={() => setProcessInfoOpen(false)}>
                {t("关闭", "Close")}
              </button>
            </div>
            <div className="process-info-body">
              <label>
                {t("地理位置", "Geography")}
                <TidasLocationCascade
                  value={processInfoDraft.location}
                  uiLanguage={uiLanguage}
                  blankLabel={t("留空继承项目地理位置", "Blank inherits project geography")}
                  onChange={(value) => setProcessInfoDraft((prev) => ({ ...prev, location: value }))}
                  onCatalogStatus={(ok, message) => setProcessLocationLoadError(ok ? "" : message)}
                />
              </label>
              <label>
                {t("数据年份", "Data Year")}
                <input
                  type="number"
                  min={1000}
                  max={9999}
                  value={processInfoDraft.referenceYear}
                  onChange={(event) => setProcessInfoDraft((prev) => ({ ...prev, referenceYear: event.target.value }))}
                />
              </label>
              <label className="process-info-field span-2">
                {t("时间代表性说明", "Time Representativeness Description")}
                <input
                  value={processInfoDraft.timeRepresentativeness}
                  placeholder={t("可选，例如 2024 年平均数据", "Optional, e.g. 2024 annual average data")}
                  onChange={(event) => setProcessInfoDraft((prev) => ({ ...prev, timeRepresentativeness: event.target.value }))}
                />
              </label>
              <label className="process-info-field span-2">
                {t("技术描述", "Technology Description")}
                <textarea
                  rows={3}
                  value={processInfoDraft.technologyDescription}
                  onChange={(event) => setProcessInfoDraft((prev) => ({ ...prev, technologyDescription: event.target.value }))}
                />
              </label>
              {productOutputs.length > 0 ? (
                <label className="process-info-field span-2">
                  {t("参考产品", "Reference Product")}
                  <select
                    value={processInfoDraft.referenceProductFlowUuid}
                    disabled={productOutputs.length <= 1}
                    onChange={(event) => setProcessInfoDraft((prev) => ({ ...prev, referenceProductFlowUuid: event.target.value }))}
                  >
                    {productOutputs.map((port) => (
                      <option key={port.id} value={port.flowUuid}>
                        {getPortDisplayName(port)}
                      </option>
                    ))}
                  </select>
                </label>
              ) : (
                <label className="process-info-field span-2">
                  {t("参考产品", "Reference Product")}
                  <input
                    value={processInfoDraft.referenceProductText}
                    onChange={(event) => setProcessInfoDraft((prev) => ({ ...prev, referenceProductText: event.target.value }))}
                  />
                </label>
              )}
            </div>
            <div className="assoc-actions process-info-actions">
              <button type="button" className="ghost-btn" onClick={() => setProcessInfoOpen(false)}>
                {t("取消", "Cancel")}
              </button>
              <button type="button" onClick={saveProcessInfo}>
                {t("保存", "Save")}
              </button>
            </div>
          </div>
        </div>
      )}

      <MultiProductAllocationModal
        open={allocationModalOpen}
        uiLanguage={uiLanguage}
        products={productOutputs}
        preview={allocationPreview}
        importedLocked={importedLocked}
        lciNode={lciNode}
        getDisplayName={getPortDisplayName}
        getUnitGroupLabel={(port) => port.unitGroupSwitch?.targetUnitGroup ?? resolveUnitGroupByUnit(port.unit) ?? resolvePortUnitGroupKey(port) ?? port.unitGroup ?? "-"}
        onClose={() => setAllocationModalOpen(false)}
        onApplyFactors={applyAllocationFactors}
        onSetQuantityMode={setProductQuantityAllocationMode}
        onSetManualMode={setProductManualAllocationMode}
        onUpdateProduct={updateOutputPortAllocation}
        onOpenFlowProperty={(port) => setAllocationPropertyPort(port)}
      />

      <FlowAllocationPropertiesModal
        open={Boolean(allocationPropertyPort)}
        uiLanguage={uiLanguage}
        flowUuid={allocationPropertyPort?.flowUuid ?? null}
        flowName={allocationPropertyPort ? getPortDisplayName(allocationPropertyPort) : ""}
        sourceUnit={
          allocationPropertyPort
            ? flowDefaultUnitByUuid[allocationPropertyPort.flowUuid]
              ?? allocationPropertyPort.unitGroupSwitch?.sourceUnit
              ?? allocationPropertyPort.unitGroupSwitch?.sourceReferenceUnit
              ?? allocationPropertyPort.unit
            : undefined
        }
        sourceUnitGroup={
          allocationPropertyPort
            ? flowUnitGroupByUuid[allocationPropertyPort.flowUuid]
              ?? allocationPropertyPort.unitGroupSwitch?.sourceUnitGroup
              ?? allocationPropertyPort.unitGroup
            : undefined
        }
        sourcePolicy={sourcePolicy}
        tidasAllowedUnitGroups={tidasAllowedUnitGroups}
        onClose={() => setAllocationPropertyPort(null)}
        onSaved={(properties) => {
          if (allocationPropertyPort) {
            applyFlowUnitGroupSwitch(allocationPropertyPort, properties);
          }
        }}
        onResetToDefault={
          allocationPropertyPort?.unitGroupSwitch
            ? () => resetFlowUnitGroupSwitch(allocationPropertyPort)
            : undefined
        }
        onStatus={onStatus}
      />

      {createFlowDialog.open && (
        <CreateFlowDialog
          open={createFlowDialog.open}
          uiLanguage={uiLanguage}
          defaultFlowType={createFlowDialog.target?.includes("out") ? "product_flow" : "product_flow"}
          defaultCategory={flowCategoryLevel1}
          sourcePolicy={sourcePolicy}
          onSuccess={(flow) => {
            // 将后端返回的 flow 映射成 CatalogFlow，使用保存的 target 加入节点
            const catalogFlow: CatalogFlow = {
              flow_uuid: flow.flow_uuid,
              flow_name: flow.flow_name,
              flow_name_en: flow.flow_name_en,
              flow_type: flow.flow_type,
              default_unit: flow.default_unit,
              unit_group: flow.unit_group,
              compartment: flow.category ?? flow.compartment,
            };
            if (createFlowDialog.target) {
              addCatalogFlowForTarget(catalogFlow, createFlowDialog.target);
              setCreateFlowDialog({ open: false, target: null });
              setFlowPicker({ open: false, target: null });
            }
          }}
          onClose={() => setCreateFlowDialog({ open: false, target: null })}
          onStatus={(text) => {
            onStatus?.(text);
          }}
        />
      )}
    </div>
  );
}
