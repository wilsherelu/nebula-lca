import type { Node } from "@xyflow/react";
import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { FlowPort, LcaNodeData, ProcessMode } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

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
};

type UnitDefinition = {
  unit_group: string;
  unit_name: string;
  factor_to_reference: number;
  is_reference: boolean;
};

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;

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
  allowShowOnNodeToggle?: boolean;
  showNodeColumn?: boolean;
  ports: FlowPort[];
  getDisplayName?: (port: FlowPort) => string;
  onChange: (next: FlowPort[]) => void;
  onAdd?: () => void;
  onDelete?: (id: string) => void;
  extraHeader?: string;
  renderExtraCell?: (port: FlowPort, idx: number) => ReactNode;
  extraHeader2?: string;
  renderExtraCell2?: (port: FlowPort, idx: number) => ReactNode;
  unitOptionsByPort?: Record<string, string[]>;
  onUnitChange?: (port: FlowPort, nextUnit: string) => void;
};

function FlowSection({
  title,
  uiLanguage,
  readOnly = false,
  lockFields = false,
  allowShowOnNodeToggle = false,
  showNodeColumn = true,
  ports,
  getDisplayName,
  onChange,
  onAdd,
  onDelete,
  extraHeader,
  renderExtraCell,
  extraHeader2,
  renderExtraCell2,
  unitOptionsByPort,
  onUnitChange,
}: FlowSectionProps) {
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);
  const showOnNodeLocked = readOnly || (lockFields && !allowShowOnNodeToggle);
  const hasExtra = Boolean(extraHeader && renderExtraCell);
  const hasExtra2 = Boolean(extraHeader2 && renderExtraCell2);
  return (
    <section className="inventory-section">
      <div className="inventory-section-head">
        <h4>{title}</h4>
        {onAdd && (
          <button type="button" className="text-btn" disabled={readOnly || lockFields} onClick={onAdd}>
            {t("+ 新增流", "+ Add Flow")}
          </button>
        )}
      </div>
      <div className="inventory-grid-header">
        <div>{t("序号", "No.")}</div>
        <div>{t("流名称", "Flow Name")}</div>
        <div>{t("数值", "Amount")}</div>
        <div>{t("单位", "Unit")}</div>
        {hasExtra ? <div>{extraHeader}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
        {hasExtra2 ? <div>{extraHeader2}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
        {showNodeColumn ? <div>{t("显示节点", "Show Node")}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
        <div>{t("操作", "Action")}</div>
      </div>
      {ports.map((port, idx) => (
        <div key={port.id} className="inventory-grid-row">
          <div>{idx + 1}</div>
          <div className="flow-name-readonly" title={getDisplayName ? getDisplayName(port) : port.name}>
            {getDisplayName ? getDisplayName(port) : port.name}
          </div>
          <input
            type="number"
            disabled={readOnly || lockFields}
            value={Number.isFinite(port.amount) ? port.amount : 0}
            onChange={(event) => {
              const next = Number(event.target.value);
              onChange(updatePortValue(ports, port.id, "amount", Number.isFinite(next) ? next : 0));
            }}
          />
          <select
            disabled={readOnly || lockFields}
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
          {hasExtra ? <div className="extra-column-cell">{renderExtraCell?.(port, idx)}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
          {hasExtra2 ? <div className="extra-column-cell">{renderExtraCell2?.(port, idx)}</div> : <div className="inventory-grid-spacer" aria-hidden="true" />}
          {showNodeColumn ? (
              <label className="inline-checkbox">
                <input
                  type="checkbox"
                  checked={port.showOnNode}
                  disabled={showOnNodeLocked}
                  onChange={(event) => onChange(updatePortValue(ports, port.id, "showOnNode", event.target.checked))}
                />
              </label>
          ) : <div className="inventory-grid-spacer" aria-hidden="true" />}
          <button
            type="button"
            className="link-btn danger"
            disabled={readOnly || lockFields || !onDelete}
            onClick={() => onDelete?.(port.id)}
          >
            {t("删除", "Delete")}
          </button>
        </div>
      ))}
      {ports.length === 0 && <div className="table-empty">{t("暂无数据", "No data")}</div>}
    </section>
  );
}

export function NodeInspector({ node }: Props) {
  const [tab, setTab] = useState<TabKey>("external_in");
  const [flowPicker, setFlowPicker] = useState<{ open: boolean; target: FlowTarget | null }>({ open: false, target: null });
  const [flowSearchInput, setFlowSearchInput] = useState("");
  const [flowSearchQuery, setFlowSearchQuery] = useState("");
  const [flowCategoryLevel1, setFlowCategoryLevel1] = useState("");
  const [flowCategoryOptions, setFlowCategoryOptions] = useState<Array<{ category: string; count: number }>>([]);
  const [catalogFlows, setCatalogFlows] = useState<CatalogFlow[]>([]);
  const [unitDefinitions, setUnitDefinitions] = useState<UnitDefinition[]>([]);
  const [flowUnitGroupByUuid, setFlowUnitGroupByUuid] = useState<Record<string, string>>({});
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
  const [linkedDialog, setLinkedDialog] = useState<{
    open: boolean;
    flowUuid?: string;
    title: string;
    items: string[];
  }>({
    open: false,
    flowUuid: undefined,
    title: "",
    items: [],
  });
  const [pendingMarketOutputSelection, setPendingMarketOutputSelection] = useState(false);
  const [productRuleHint, setProductRuleHint] = useState("");
  const [selectedNodeId, setSelectedNodeId] = useState("");

  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
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
  const unitAutoScaleEnabled = useLcaGraphStore((state) => state.unitAutoScaleEnabled);
  const setUnitAutoScaleEnabled = useLcaGraphStore((state) => state.setUnitAutoScaleEnabled);
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);

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

  const unitOptionsByPort = useMemo(() => {
    const allPorts = [...node.data.inputs, ...node.data.outputs];
    const result: Record<string, string[]> = {};
    for (const port of allPorts) {
      const group = resolveUnitGroupKey(resolvePortUnitGroup(port));
      const groupOptions = group ? unitOptionsByGroup.get(group) : undefined;
      const options = groupOptions && groupOptions.length > 0 ? groupOptions : [port.unit];
      result[port.id] = Array.from(new Set([port.unit, ...options]));
    }
    return result;
  }, [node.data.inputs, node.data.outputs, resolvePortUnitGroup, resolveUnitGroupKey, unitOptionsByGroup]);

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
    const unitGroup = resolveUnitGroupKey(resolvePortUnitGroup(port));
    if (marketProcess && (section === "inputs" || section === "outputs")) {
      const currentOutput = externalOutIntermediate[0];
      const canonicalGroup = resolveUnitGroupKey(
        resolvePortUnitGroup(currentOutput ?? port),
      );
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
                    unitGroup: item.unitGroup || canonicalGroup,
                  },
            ),
            outputs: current.data.outputs.map((item) => ({
              ...item,
              unit: nextUnit,
              unitGroup: item.unitGroup || canonicalGroup,
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
                  unitGroup: item.unitGroup || canonicalGroup,
                  amount: inputAmountById.get(item.id) ?? item.amount,
                },
          ),
          outputs: current.data.outputs.map((item) => ({
            ...item,
            unit: nextUnit,
            unitGroup: item.unitGroup || canonicalGroup,
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
                unitGroup: item.unitGroup || unitGroup,
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
              unitGroup: item.unitGroup || unitGroup,
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
          unitGroup: input.unitGroup || unitGroup,
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


  const externalInIntermediate = node.data.inputs.filter((p) => p.type !== "biosphere");
  const externalInElementary = node.data.inputs.filter((p) => p.type === "biosphere");
  const externalOutIntermediate = node.data.outputs.filter((p) => p.type !== "biosphere");
  const externalOutElementary = node.data.outputs.filter((p) => p.type === "biosphere");

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
    const group = resolveUnitGroupKey(resolvePortUnitGroup(marketOutput ?? externalInIntermediate[0]));
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
  }, [externalInIntermediate, marketOutput, marketProcess, resolvePortUnitGroup, resolveUnitGroupKey, unitOptionsByGroup, unitOptionsByPort]);
  const marketOutputOk = Boolean(
    marketOutput &&
      externalOutIntermediate.length === 1 &&
      Math.abs((marketOutput.amount ?? 0) - 1) <= 1e-9 &&
      marketOutput.isProduct,
  );
  const settingCandidates = nodes.filter((candidate) => candidate.id !== node.id);
  const canAutoNormalizeMarketInputs = marketProcess && externalInIntermediate.length > 0 && marketInputShareTotal > 0;

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
  }, [flowCategoryLevel1, flowPage, flowPicker.open, flowPicker.target, flowSearchQuery, flowPageSize]);

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
  }, [flowPicker.open, flowPicker.target]);

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
    const visiblePorts = [...node.data.inputs, ...node.data.outputs].filter((port) => port.flowUuid);
    const needFetch = Array.from(new Set(visiblePorts.map((port) => port.flowUuid))).filter((uuid) => {
      if (!flowUnitGroupByUuid[uuid]) {
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
        const typePatch: Record<string, string> = {};
        const namePatch: Record<string, string> = {};
        rows.forEach((row) => {
          if (!row) {
            return;
          }
          if (row.unit_group) {
            unitPatch[row.flow_uuid] = row.unit_group;
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
  }, [flowNameEnByUuid, flowTypeByUuid, flowUnitGroupByUuid, node.data.inputs, node.data.outputs, uiLanguage]);

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
        return !elementary;
      });
  }, [flowPicker.target, catalogFlows]);

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

  const addCatalogFlow = (flow: CatalogFlow) => {
    if (!flowPicker.target) {
      return;
    }

    if (flow.flow_uuid) {
      if (flow.unit_group) {
        setFlowUnitGroupByUuid((prev) => ({ ...prev, [flow.flow_uuid]: flow.unit_group }));
      }
      if (flow.flow_type) {
        setFlowTypeByUuid((prev) => ({ ...prev, [flow.flow_uuid]: flow.flow_type }));
      }
    }

    const isInput = flowPicker.target.startsWith("in_");
    const isElementary = flowPicker.target.endsWith("elementary");
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
      setFlowPicker({ open: false, target: null });
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

  const ensureEnglishFlowName = async (port: FlowPort) => {
    if (uiLanguage !== "en" || !port.flowUuid || port.flowNameEn || flowNameEnByUuid[port.flowUuid]) {
      return;
    }
    try {
      const resp = await fetch(`${API_BASE}/reference/flows/${encodeURIComponent(port.flowUuid)}`);
      if (!resp.ok) {
        return;
      }
      const row = (await resp.json()) as CatalogFlow;
      const englishName = String(row.flow_name_en ?? "").trim();
      if (englishName) {
        setFlowNameEnByUuid((prev) => ({ ...prev, [port.flowUuid]: englishName }));
      }
    } catch {
      // ignore
    }
  };

  const openLinkedDialog = (port: FlowPort, items: string[]) => {
    setLinkedDialog({
      open: true,
      flowUuid: port.flowUuid,
      title: getPortDisplayName(port),
      items,
    });
  };

  useEffect(() => {
    if (!linkedDialog.open || !linkedDialog.flowUuid || uiLanguage !== "en") {
      return;
    }
    const englishName = String(flowNameEnByUuid[linkedDialog.flowUuid] ?? "").trim();
    if (!englishName || linkedDialog.title === englishName) {
      return;
    }
    setLinkedDialog((prev) =>
      prev.open && prev.flowUuid === linkedDialog.flowUuid
        ? {
            ...prev,
            title: englishName,
          }
        : prev,
    );
  }, [flowNameEnByUuid, linkedDialog.flowUuid, linkedDialog.open, linkedDialog.title, uiLanguage]);

  const isReferenceProductPort = (port: FlowPort, _direction: AssocDirection): boolean => Boolean(port.isProduct);

  const getProductUnitGroupMismatches = (outputs: FlowPort[]): string[] => {
    const groups = Array.from(
      new Set(
        outputs
          .filter((port) => port.type !== "biosphere" && port.isProduct)
          .map((port) => resolveUnitGroupKey(resolvePortUnitGroup(port)) ?? port.unitGroup ?? "")
          .map((group) => group.trim())
          .filter((group) => group.length > 0),
      ),
    );
    return groups;
  };

  const applyProductToggle = (direction: AssocDirection, portId: string, checked: boolean) => {
    let blocked = false;
    let inputProductWarning = "";
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
          blocked = true;
          return current;
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
    if (blocked) {
      setProductRuleHint(
        t(
          "当前版本仅支持同单位组多产品分配。请先统一该单元过程产品流的单位组。",
          "This version only supports multi-product allocation within the same unit group. Please align the unit groups of this unit process product flow first.",
        ),
      );
      return;
    }
    setProductRuleHint("");
    if (inputProductWarning) {
      setConnectionHint(inputProductWarning);
    }
  };

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
          <label className="inline-checkbox">
            <input
              type="checkbox"
              checked={unitAutoScaleEnabled}
              disabled={importedLocked}
              onChange={(event) => setUnitAutoScaleEnabled(event.target.checked)}
            />
            {t("单位自动换算", "Unit auto conversion")}
          </label>
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
            lockFields={importedLocked}
            allowShowOnNodeToggle={importedLocked}
            ports={externalInIntermediate}
            getDisplayName={getPortDisplayName}
            unitOptionsByPort={marketInputUnitOptionsByPort}
            onUnitChange={(port, nextUnit) => {
              void updatePortUnitWithConversion("inputs", port, nextUnit);
            }}
            extraHeader={t("关联", "Link")}
            extraHeader2={t("定义产品", "Product Def.")}
            renderExtraCell={(port) => (
              <div className="row-setting-cell">
                {(() => {
                  const linkedItems = linkedInputNames(port);
                  const linkedLabel =
                    linkedItems.length > 0
                      ? `${t("已连接", "Linked")} (${linkedItems.length})`
                      : t("未关联", "Not linked");
                  return (
                    <button
                      type="button"
                      className="linked-summary-btn"
                      onClick={() => {
                        void ensureEnglishFlowName(port);
                        openLinkedDialog(port, linkedItems);
                      }}
                    >
                      {linkedLabel}
                    </button>
                  );
                })()}
                {!marketProcess && (
                  <button type="button" className="link-btn" disabled={importedLocked} onClick={() => openAssociationDialog("input", port)}>
                    {t("关联", "Link")}
                  </button>
                )}
              </div>
            )}
            renderExtraCell2={(port) => (
              <label className="inline-checkbox">
                <input
                  type="checkbox"
                  checked={isReferenceProductPort(port, "input")}
                  disabled={importedLocked || lciNode || marketProcess}
                  onChange={(event) => applyProductToggle("input", port.id, event.target.checked)}
                />
              </label>
            )}
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
            onAdd={() => setFlowPicker({ open: true, target: "in_intermediate" })}
            onDelete={(id) =>
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
              title={t("基本流", "Elementary Flows")}
              uiLanguage={uiLanguage}
              lockFields={importedLocked}
              showNodeColumn={false}
              ports={externalInElementary}
              getDisplayName={getPortDisplayName}
              unitOptionsByPort={unitOptionsByPort}
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
              onAdd={() => setFlowPicker({ open: true, target: "in_elementary" })}
              onDelete={(id) =>
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
            readOnly={false}
            lockFields={importedLocked}
            allowShowOnNodeToggle={importedLocked}
            ports={externalOutIntermediate}
            getDisplayName={getPortDisplayName}
            unitOptionsByPort={unitOptionsByPort}
            onUnitChange={(port, nextUnit) => {
              void updatePortUnitWithConversion("outputs", port, nextUnit);
            }}
            extraHeader={t("关联", "Link")}
            extraHeader2={t("定义产品", "Product Def.")}
            renderExtraCell={(port) => (
              <div className="row-setting-cell">
                {(() => {
                  const linkedItems = linkedOutputNames(port);
                  const linkedLabel =
                    linkedItems.length > 0
                      ? `${t("已连接", "Linked")} (${linkedItems.length})`
                      : t("未关联", "Not linked");
                  return (
                    <button
                      type="button"
                      className="linked-summary-btn"
                      onClick={() => {
                        void ensureEnglishFlowName(port);
                        openLinkedDialog(port, linkedItems);
                      }}
                    >
                      {linkedLabel}
                    </button>
                  );
                })()}
                <button type="button" className="link-btn" disabled={importedLocked} onClick={() => openAssociationDialog("output", port)}>
                  {t("关联", "Link")}
                </button>
              </div>
            )}
            renderExtraCell2={(port) => {
              if (marketProcess) {
                return <span className="market-fixed-product">{t("固定产品", "Fixed Product")}</span>;
              }
              const checked = isReferenceProductPort(port, "output");
              return (
                <div className="product-sale-cell">
                  <label className="inline-checkbox">
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={importedLocked || lciNode}
                      onChange={(event) => applyProductToggle("output", port.id, event.target.checked)}
                    />
                  </label>
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
              marketProcess
                ? () => {
                    setPendingMarketOutputSelection(true);
                    setFlowPicker({ open: true, target: "out_intermediate" });
                  }
                : () => setFlowPicker({ open: true, target: "out_intermediate" })
            }
            onDelete={(id) => {
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
              title={t("基本流", "Elementary Flows")}
              uiLanguage={uiLanguage}
              lockFields={importedLocked}
              showNodeColumn={false}
              ports={externalOutElementary}
              getDisplayName={getPortDisplayName}
              unitOptionsByPort={unitOptionsByPort}
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
              onAdd={() => setFlowPicker({ open: true, target: "out_elementary" })}
              onDelete={(id) =>
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
          <div className="overlay-panel">
            <div className="overlay-head">
              <strong>{flowPicker.target?.includes("elementary") ? t("引用基本流", "Use Elementary Flow") : t("引用中间流", "Use Intermediate Flow")}</strong>
              <button type="button" className="drawer-close-btn" onClick={() => setFlowPicker({ open: false, target: null })}>
                {t("关闭", "Close")}
              </button>
            </div>
            <div className="overlay-filters">
              <div className="search-row">
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
                <button type="button" className="search-btn" onClick={applyFlowSearch}>
                  {t("检索", "Search")}
                </button>
              </div>
            </div>
            <div className="overlay-table">
              {loadingFlows && <div className="table-empty">{t("加载中...", "Loading...")}</div>}
              {!loadingFlows && flowLoadError && <div className="table-empty">{flowLoadError}</div>}
              {!loadingFlows && !flowLoadError && (
                <table>
                  <thead>
                    <tr>
                      <th>{t("类型", "Type")}</th>
                      <th>{t("流名称", "Flow Name")}</th>
                      <th>{t("单位", "Unit")}</th>
                      <th>{t("分类", "Category")}</th>
                      <th>{t("操作", "Action")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredFlows.map((flow) => (
                      <tr key={flow.flow_uuid}>
                        <td>{displayFlowType(flow.flow_type)}</td>
                        <td>{getCatalogFlowDisplayName(flow, uiLanguage)}</td>
                        <td>{flow.default_unit}</td>
                        <td>{flow.compartment || "-"}</td>
                        <td>
                          <button type="button" className="link-btn" onClick={() => addCatalogFlow(flow)}>
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

      {linkedDialog.open && (
        <div className="overlay-modal">
          <div className="overlay-panel small">
            <div className="overlay-head">
              <strong>{t("已关联过程", "Linked Processes")}</strong>
              <button type="button" className="drawer-close-btn" onClick={() => setLinkedDialog({ open: false, flowUuid: undefined, title: "", items: [] })}>
                {t("关闭", "Close")}
              </button>
            </div>
            <div className="overlay-filters">
              <label className="linked-dialog-label">
                <span>{t("流", "Flow")}</span>
                <div className="flow-name-readonly" title={linkedDialog.title}>{linkedDialog.title}</div>
              </label>
              {linkedDialog.items.length > 0 ? (
                <div className="linked-process-list">
                  {linkedDialog.items.map((item, idx) => (
                    <div key={`${item}_${idx}`} className="linked-process-item" title={item}>
                      {item}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="table-empty">{t("当前未关联过程", "No linked processes")}</div>
              )}
              <div className="assoc-actions">
                <button type="button" className="ghost-btn" onClick={() => setLinkedDialog({ open: false, flowUuid: undefined, title: "", items: [] })}>
                  {t("关闭", "Close")}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}


