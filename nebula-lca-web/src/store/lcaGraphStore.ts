import {
  applyEdgeChanges,
  applyNodeChanges,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
  type XYPosition,
} from "@xyflow/react";
import { create } from "zustand";
import { processLibrary } from "../data/processLibrary";
import type { LcaEdgeData, LcaExchange, LcaGraphPayload } from "../model/exchange";
import type { FlowPort, LcaNodeData, LcaProcessTemplate, LciRole, ProcessMode } from "../model/node";
import {
  buildGraphRelations,
  createEmptyGraphRelations,
  type GraphRelations,
} from "./graphRelations";

const UI_LANGUAGE_STORAGE_KEY = "nebula_ui_language";
const resolveInitialUiLanguage = (): "zh" | "en" => {
  try {
    const saved = String(window.localStorage.getItem(UI_LANGUAGE_STORAGE_KEY) ?? "").trim().toLowerCase();
    if (saved === "zh" || saved === "en") {
      return saved;
    }
  } catch {
    // ignore storage access failures
  }
  try {
    const preferred = String(window.navigator.language ?? "").trim().toLowerCase();
    if (preferred.startsWith("zh")) {
      return "zh";
    }
  } catch {
    // ignore navigator access failures
  }
  return "en";
};

const DEV_PTS_DEBUG = Boolean(import.meta.env.DEV);
const debugPts = (scope: string, payload?: unknown) => {
  if (!DEV_PTS_DEBUG) {
    return;
  }
  if (payload === undefined) {
    console.info(`[PTS_DEBUG] ${scope}`);
    return;
  }
  console.info(`[PTS_DEBUG] ${scope}`, payload);
};
const DEBUG_FLOW_TYPE_NODE_ID = "node_4d85c6b9-0ca0-4136-be15-fa501edd296f";
const debugFlowTypesForNode = (scope: string, node: Node<LcaNodeData> | undefined) => {
  if (!DEV_PTS_DEBUG || !node || node.id !== DEBUG_FLOW_TYPE_NODE_ID) {
    return;
  }
  debugPts(scope, {
    nodeId: node.id,
    nodeName: node.data.name,
    nodeKind: node.data.nodeKind,
    processUuid: node.data.processUuid,
    isMarketProcessNode: isMarketProcessNode(node),
    outputs: node.data.outputs.map((port) => ({
      id: port.id,
      name: port.name,
      flowUuid: port.flowUuid,
      type: port.type,
      isProduct: port.isProduct,
    })),
  });
};

type Selection = {
  nodeIds: string[];
  edgeIds: string[];
  nodeId?: string;
  edgeId?: string;
};

type CanvasKind = "root" | "pts_internal";
type FlowBalanceDialogState = {
  open: boolean;
  edgeId?: string;
  sourceNodeId?: string;
  flowUuid?: string;
};

type CanvasGraph = {
  id: string;
  name: string;
  kind: CanvasKind;
  parentCanvasId?: string;
  parentPtsNodeId?: string;
  nodes: Array<Node<LcaNodeData>>;
  edges: Array<Edge<LcaEdgeData>>;
};

type MarketWarning = {
  nodeId: string;
  nodeName: string;
  issues: string[];
};

type ConnectionFix =
  | {
      type: "insert_market";
      sourceNodeId: string;
      sourcePortId: string;
      targetNodeId: string;
      flowUuid: string;
      flowName: string;
    }
  | undefined;

export type NodeCreateKind = "unit_process" | "market_process" | "pts_module" | "lci_dataset";
export type ProcessImportMode = "locked" | "editable_clone";
export type FilteredExchangeEvidence = {
  process_uuid?: string;
  exchange_internal_id?: string;
  flow_uuid?: string;
  reason?: string;
};
export type ImportedUnitProcessPayload = {
  processUuid: string;
  sourceProcessUuid?: string;
  importMode: ProcessImportMode;
  name: string;
  location?: string;
  referenceProduct?: string;
  referenceProductFlowUuid?: string;
  referenceProductDirection?: "input" | "output";
  inputs: FlowPort[];
  outputs: FlowPort[];
  warnings?: string[];
  filteredExchanges?: FilteredExchangeEvidence[];
};
type CanvasViewport = { x: number; y: number; zoom: number };
type PtsPortEditorState = {
  open: boolean;
  ptsNodeId?: string;
};
type UnitProcessImportDialogState = {
  open: boolean;
  position?: XYPosition;
  targetKind: NodeCreateKind;
};
type PendingEdgeItem = {
  canvasId: string;
  edge: Edge<LcaEdgeData>;
  retries: number;
  openBalanceEditor: boolean;
};
type HandleValidationIssue = {
  edge_id?: string;
  suggested_source_port_id?: string;
  suggested_target_port_id?: string;
};

type LcaGraphState = {
  functionalUnit: string;
  uiLanguage: "zh" | "en";
  flowAnimationEnabled: boolean;
  edgeRoutingStyle: "classic_curve" | "orthogonal_avoid";
  flowAnimationEpoch: number;
  autoPopupEnabled: boolean;
  unitAutoScaleEnabled: boolean;
  canvases: Record<string, CanvasGraph>;
  activeCanvasId: string;
  activeCanvasKind: CanvasKind;
  canvasPath: string[];
  viewport: CanvasViewport;
  nodes: Array<Node<LcaNodeData>>;
  edges: Array<Edge<LcaEdgeData>>;
  graphRelations: GraphRelations;
  selection: Selection;
  inspectorOpen: boolean;
  flowBalanceDialog: FlowBalanceDialogState;
  ptsPortEditor: PtsPortEditorState;
  connectionHint?: string;
  connectionFix?: ConnectionFix;
  pendingEdges: PendingEdgeItem[];
  deferredBalanceEdgeId?: string;
  pendingAutoConnect: boolean;
  pendingPtsCompileNodeId?: string;
  unitProcessImportDialog: UnitProcessImportDialogState;
  setFlowAnimationEnabled: (enabled: boolean) => void;
  setEdgeRoutingStyle: (style: "classic_curve" | "orthogonal_avoid") => void;
  setUiLanguage: (lang: "zh" | "en") => void;
  setAutoPopupEnabled: (enabled: boolean) => void;
  setUnitAutoScaleEnabled: (enabled: boolean) => void;
  onNodesChange: (changes: NodeChange<Node<LcaNodeData>>[]) => void;
  onEdgesChange: (changes: EdgeChange<Edge<LcaEdgeData>>[]) => void;
  onConnect: (connection: Connection) => void;
  flushPendingEdges: () => void;
  applyHandleValidationIssues: (issues: HandleValidationIssue[]) => void;
  rebindRootEdgeHandles: () => void;
  rebindRootEdgeHandlesForNode: (nodeId: string) => void;
  rebindRootEdgeHandlesForPtsShell: (
    nodeId: string,
    ports: { inputs?: FlowPort[]; outputs?: FlowPort[] },
    portIdMap?: Record<string, string>,
  ) => void;
  consumeDeferredBalanceEdge: () => void;
  consumePendingAutoConnect: () => void;
  consumePendingPtsCompile: () => void;
  setSelection: (selection: Selection) => void;
  openNodeInspector: (nodeId: string) => void;
  openEdgeInspector: (edgeId: string) => void;
  openFlowBalanceDialogForEdge: (edgeId: string) => void;
  closeFlowBalanceDialog: () => void;
  openPtsPortEditor: (nodeId?: string) => void;
  closePtsPortEditor: () => void;
  syncPtsPortsFromInternal: (ptsNodeId: string) => void;
  setPtsPortExposureByFlow: (
    ptsNodeId: string,
    direction: "input" | "output",
    flow: Pick<
      FlowPort,
      | "flowUuid"
      | "name"
      | "portKey"
      | "productKey"
      | "unit"
      | "unitGroup"
      | "type"
      | "sourceProcessUuid"
      | "sourceProcessName"
      | "sourceNodeId"
      | "exposureMode"
      | "isProduct"
    >,
    enabled: boolean,
  ) => void;
  setPtsPortExposureMode: (ptsNodeId: string, portId: string, mode: "boundary_only" | "force_product_expose") => void;
  setPtsPortVisibility: (ptsNodeId: string, direction: "input" | "output", portId: string, visible: boolean) => void;
  setFlowBalanceTotal: (
    sourceNodeId: string,
    flowUuid: string,
    nextTotal: number,
    externalSaleAmount?: number,
    preserveInputs?: boolean,
  ) => void;
  setFlowBalanceEdgeAmount: (edgeId: string, nextAmount: number) => void;
  setFlowBalanceUnit: (sourceNodeId: string, flowUuid: string, nextUnit: string) => void;
  closeInspector: (options?: { requireProductConfirm?: boolean }) => void;
  setConnectionHint: (hint?: string) => void;
  clearConnectionHint: () => void;
  applyConnectionFix: () => void;
  setViewport: (viewport: CanvasViewport) => void;
  addNodeFromTemplate: (templateId: string, position: XYPosition) => void;
  updateNode: (nodeId: string, updater: (node: Node<LcaNodeData>) => Node<LcaNodeData>) => void;
  setMarketAllowMixedFlows: (nodeId: string, allow: boolean) => void;
  replaceMarketOutputFlow: (
    nodeId: string,
    flow: Pick<FlowPort, "flowUuid" | "name" | "unit" | "unitGroup" | "type">,
  ) => void;
  setNodeMode: (nodeId: string, mode: ProcessMode) => void;
  updateEdgeData: (edgeId: string, data: Partial<LcaEdgeData>) => void;
  removeEdge: (edgeId: string) => void;
  removeNode: (nodeId: string) => void;
  cloneNodeAt: (nodeId: string, position: XYPosition) => void;
  upsertOutputLink: (
    sourceNodeId: string,
    outputPortId: string,
    targetNodeId: string,
    targetInputPortId?: string,
  ) => void;
  createAndLinkOutputTarget: (
    sourceNodeId: string,
    outputPortId: string,
    targetKind: "unit_process" | "market_process" | "pts_module" | "lci_dataset",
  ) => void;
  addBlankUnitProcess: () => void;
  addBlankMarketProcess: () => void;
  addBlankPts: () => void;
  addBlankLciDataset: () => void;
  addBlankNodeAt: (kind: NodeCreateKind, position: XYPosition) => void;
  openUnitProcessImportDialog: (position?: XYPosition, targetKind?: NodeCreateKind) => void;
  closeUnitProcessImportDialog: () => void;
  addImportedUnitProcesses: (rows: ImportedUnitProcessPayload[], position?: XYPosition) => string[];
  autoConnectByUuid: (options?: { silentNoCandidate?: boolean; silentSuccess?: boolean }) => void;
  packageSelectionAsPts: () => void;
  unpackPtsNode: (nodeId?: string) => void;
  enterPtsNode: (nodeId: string) => void;
  goToParentCanvas: () => void;
  updatePtsPublishedBinding: (
    ptsNodeId: string,
    binding: { publishedVersion?: number; publishedArtifactId?: string },
  ) => void;
  replaceRootPtsShell: (
    ptsNodeId: string,
    shell: {
      nodeKind?: LcaNodeData["nodeKind"];
      mode?: ProcessMode;
      ptsUuid?: string;
      processUuid?: string;
      name?: string;
      location?: string;
      referenceProduct?: string;
      referenceProductFlowUuid?: string;
      referenceProductDirection?: "input" | "output";
      inputs: FlowPort[];
      outputs: FlowPort[];
      publishedVersion?: number;
      publishedArtifactId?: string;
    },
  ) => void;
  replacePtsInternalCanvasGraph: (ptsNodeId: string, graph: LcaGraphPayload, options?: { name?: string }) => void;
  exportGraph: () => LcaGraphPayload;
  importGraph: (graph: LcaGraphPayload) => void;
  getBalancedWarningsForCanvas: (
    canvasId: string,
  ) => Array<{
    flowUuid: string;
    flowName: string;
    outputTotal: number;
    inputTotal: number;
    externalSaleTotal: number;
    processNames: string[];
  }>;
  getBalancedWarnings: () => Array<{
    flowUuid: string;
    flowName: string;
    outputTotal: number;
    inputTotal: number;
    externalSaleTotal: number;
    processNames: string[];
  }>;
  getMarketWarnings: () => MarketWarning[];
  getMarketWarningsForCanvas: (canvasId: string) => MarketWarning[];
};

type TemplatePort = LcaProcessTemplate["inputs"][number];

const ROOT_CANVAS_ID = "root";
const RULE_HINTS = {
  missingNode: `无法连线：来源过程或目标过程不存在。`,
  selfLoop: `无法连线：不允许同一过程自连接。`,
  lciToLci: `无法连线：LCI 不能直接连接到 LCI。`,
  lciProviderOnlyOut: `无法连线：LCI provider 仅可输出到 Unit/PTS。`,
  lciWasteOnlyIn: `无法连线：LCI waste_sink 仅可接收 Unit/PTS 输入。`,
  noVisibleOutput: `无法连线：来源过程没有可显示的输出端口。`,
  productToProductForbidden: `无法连线：产品流不能直接连接到产品流。`,
  nonProductToNonProductForbidden: `无法连线：非产品中间流不能直接连接到非产品中间流。`,
  nonProductOneToOneOnly: `无法连线：非产品中间流只能关联唯一产品中间流。`,
  needMarketForMultiSource: `无法连线：多源汇聚到单一输入时请使用市场过程。`,
  needMarketForManyToMany: `无法连线：多源与多汇并存时请插入市场过程。`,
  marketSingleFlowOnly: `无法连线：市场过程只能处理单一中间流 UUID。`,
  marketInputRequiresProductSource: `无法连线：市场过程输入只能连接已定义为产品的来源端口。`,
  invalidFlowUuid: `无法连线：流 UUID 缺失或非法，请先从数据库引用中间流或基本流。`,
  targetNoMatchingInput: `目标过程不存在同 UUID 输入端口，是否自动创建？`,
  targetNoMatchingInputCanceled: `已取消连线：目标过程未创建同 UUID 输入端口。`,
};

const toPort = (port: TemplatePort, direction: "input" | "output"): FlowPort => ({
  ...port,
  amount: port.amount ?? 0,
  externalSaleAmount: port.externalSaleAmount ?? 0,
  direction,
  showOnNode: true,
});

const uid = () => {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `id_${Math.random().toString(36).slice(2, 10)}`;
};

const isUuidLike = (value: string | undefined): boolean => Boolean(value && value.trim().length > 0);

const toStablePosition = (seed: string): XYPosition => {
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0;
  }
  const col = hash % 7;
  const row = Math.floor(hash / 7) % 6;
  return {
    x: 120 + col * 180,
    y: 100 + row * 130,
  };
};

const isValidPosition = (value: unknown): value is XYPosition => {
  if (!value || typeof value !== "object") {
    return false;
  }
  const maybe = value as { x?: unknown; y?: unknown };
  return typeof maybe.x === "number" && Number.isFinite(maybe.x) && typeof maybe.y === "number" && Number.isFinite(maybe.y);
};

const isValidViewport = (value: unknown): value is CanvasViewport => {
  if (!value || typeof value !== "object") {
    return false;
  }
  const maybe = value as { x?: unknown; y?: unknown; zoom?: unknown };
  return (
    typeof maybe.x === "number" &&
    Number.isFinite(maybe.x) &&
    typeof maybe.y === "number" &&
    Number.isFinite(maybe.y) &&
    typeof maybe.zoom === "number" &&
    Number.isFinite(maybe.zoom)
  );
};

const getCanvasPath = (canvases: Record<string, CanvasGraph>, activeCanvasId: string): string[] => {
  const path: string[] = [];
  let current: CanvasGraph | undefined = canvases[activeCanvasId];
  while (current) {
    path.unshift(current.name);
    current = current.parentCanvasId ? canvases[current.parentCanvasId] : undefined;
  }
  return path;
};

const resolvePtsInternalCanvas = (
  canvases: Record<string, CanvasGraph>,
  ptsNodeId: string,
  preferredCanvasId?: string,
): CanvasGraph | undefined => {
  const candidates = Object.values(canvases).filter(
    (canvas) => canvas.kind === "pts_internal" && canvas.parentPtsNodeId === ptsNodeId,
  );
  if (candidates.length === 0) {
    return undefined;
  }
  const preferred = preferredCanvasId ? canvases[preferredCanvasId] : undefined;
  const preferredIsCandidate =
    preferred &&
    preferred.kind === "pts_internal" &&
    preferred.parentPtsNodeId === ptsNodeId;
  const preferredScore = preferredIsCandidate ? preferred.nodes.length + preferred.edges.length : -1;
  const richest = [...candidates].sort(
    (a, b) => b.nodes.length + b.edges.length - (a.nodes.length + a.edges.length),
  )[0];
  const richestScore = richest.nodes.length + richest.edges.length;
  if (!preferredIsCandidate) {
    return richest;
  }
  if (preferredScore === 0 && richestScore > 0) {
    return richest;
  }
  return preferred;
};

const setActiveCanvas = (
  state: LcaGraphState,
  nextCanvases: Record<string, CanvasGraph>,
  nextCanvasId: string,
  keepSelection = false,
) => {
  const active = nextCanvases[nextCanvasId];
  const previousRelations = state.activeCanvasId === nextCanvasId ? state.graphRelations : undefined;
  const graphRelations = buildGraphRelations(active.nodes, active.edges, previousRelations);
  return {
    canvases: nextCanvases,
    activeCanvasId: nextCanvasId,
    activeCanvasKind: active.kind,
    canvasPath: getCanvasPath(nextCanvases, nextCanvasId),
    nodes: active.nodes,
    edges: active.edges,
    graphRelations,
    selection: keepSelection ? state.selection : { nodeIds: [], edgeIds: [] },
  };
};

const updateActiveCanvas = (
  state: LcaGraphState,
  updater: (canvas: CanvasGraph) => CanvasGraph,
): Partial<LcaGraphState> => {
  const active = state.canvases[state.activeCanvasId];
  const updatedActive = updater(active);
  const nextCanvases = {
    ...state.canvases,
    [updatedActive.id]: updatedActive,
  };
  return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
};

const normalizeProductFlags = (params: {
  nodeKind: LcaNodeData["nodeKind"];
  referenceProductFlowUuid?: string;
  referenceProductDirection?: "input" | "output";
  inputs: FlowPort[];
  outputs: FlowPort[];
}): { inputs: FlowPort[]; outputs: FlowPort[] } => {
  const normalizeList = (ports: FlowPort[]): FlowPort[] =>
    ports.map((port) => {
      return {
        ...port,
        isProduct: Boolean(port.isProduct),
      };
    });
  return {
    inputs: normalizeList(params.inputs),
    outputs: normalizeList(params.outputs),
  };
};

const parseOutputPort = (
  sourceNode: Node<LcaNodeData> | undefined,
  handleId: string | null,
): FlowPort | undefined => {
  if (!sourceNode) {
    return undefined;
  }
  if (handleId?.startsWith("out:") || handleId?.startsWith("outl:") || handleId?.startsWith("outr:")) {
    const id = handleId.slice(handleId.indexOf(":") + 1);
    return sourceNode.data.outputs.find((p) => p.id === id);
  }
  return undefined;
};

const parseInputPort = (
  sourceNode: Node<LcaNodeData> | undefined,
  handleId: string | null,
): FlowPort | undefined => {
  if (!sourceNode) {
    return undefined;
  }
  if (handleId?.startsWith("in:") || handleId?.startsWith("inl:") || handleId?.startsWith("inr:")) {
    const id = handleId.slice(handleId.indexOf(":") + 1);
    return sourceNode.data.inputs.find((p) => p.id === id);
  }
  return undefined;
};

const isInputHandleId = (handleId: string | null | undefined): boolean =>
  Boolean(handleId && (handleId.startsWith("in:") || handleId.startsWith("inl:") || handleId.startsWith("inr:")));

const getConnectionHint = (
  sourceNode: Node<LcaNodeData> | undefined,
  targetNode: Node<LcaNodeData> | undefined,
): string | undefined => {
  if (!sourceNode || !targetNode) {
    return RULE_HINTS.missingNode;
  }
  if (sourceNode.id === targetNode.id) {
    return RULE_HINTS.selfLoop;
  }

  const sourceKind = sourceNode.data.nodeKind;
  const targetKind = targetNode.data.nodeKind;
  const sourceIsLci = sourceKind === "lci_dataset";
  const targetIsLci = targetKind === "lci_dataset";
  const sourceLciRole = sourceIsLci ? inferLciRoleForConnection(sourceNode, false) : undefined;
  const targetLciRole = targetIsLci ? inferLciRoleForConnection(targetNode, true) : undefined;

  if (sourceIsLci && targetIsLci) {
    return RULE_HINTS.lciToLci;
  }
  if (sourceIsLci && sourceLciRole !== "provider") {
    return RULE_HINTS.lciProviderOnlyOut;
  }
  if (targetIsLci && targetLciRole !== "waste_sink") {
    return RULE_HINTS.lciWasteOnlyIn;
  }

  return undefined;
};

const getNodeMode = (node: Node<LcaNodeData> | undefined): ProcessMode => {
  if (!node) {
    return "normalized";
  }
  if (node.data.nodeKind !== "unit_process") {
    return "normalized";
  }
  // Legacy market nodes may still be stored as unit_process + market_*.
  if ((node.data.processUuid ?? "").startsWith("market_")) {
    return "normalized";
  }
  return node.data.mode ?? "balanced";
};

const resolveQuantityMode = (
  sourceNode: Node<LcaNodeData> | undefined,
  targetNode: Node<LcaNodeData> | undefined,
): "single" | "dual" => {
  const sourceMode = getNodeMode(sourceNode);
  const targetMode = getNodeMode(targetNode);
  return sourceMode === "balanced" && targetMode === "balanced" ? "single" : "dual";
};

const isMarketProcessNode = (node: Node<LcaNodeData> | undefined): boolean =>
  Boolean(node && (node.data.nodeKind === "market_process" || node.data.processUuid.startsWith("market_")));

const isLciNode = (node: Node<LcaNodeData> | undefined): boolean =>
  Boolean(node && node.data.nodeKind === "lci_dataset");

const inferLciRoleByIntermediates = (node: Node<LcaNodeData>): LcaNodeData["lciRole"] => {
  const hasInputIntermediate = node.data.inputs.some((p) => p.type !== "biosphere");
  const hasOutputIntermediate = node.data.outputs.some((p) => p.type !== "biosphere");
  if (hasInputIntermediate && !hasOutputIntermediate) {
    return "waste_sink";
  }
  if (hasOutputIntermediate && !hasInputIntermediate) {
    return "provider";
  }
  return node.data.lciRole ?? "provider";
};

const inferLciRoleForConnection = (node: Node<LcaNodeData>, asTarget: boolean): LciRole => {
  const hasInputIntermediate = node.data.inputs.some((p) => p.type !== "biosphere");
  const hasOutputIntermediate = node.data.outputs.some((p) => p.type !== "biosphere");
  if (hasInputIntermediate && !hasOutputIntermediate) {
    return "waste_sink";
  }
  if (hasOutputIntermediate && !hasInputIntermediate) {
    return "provider";
  }
  if (!hasInputIntermediate && !hasOutputIntermediate) {
    return asTarget ? "waste_sink" : "provider";
  }
  return (node.data.lciRole ?? (asTarget ? "waste_sink" : "provider")) as LciRole;
};

const shouldEnforceMarketSingleFlow = (node: Node<LcaNodeData> | undefined): boolean =>
  Boolean(isMarketProcessNode(node) && !node?.data.marketAllowMixedFlows);

const toPtsPort = (edge: Edge<LcaEdgeData>, direction: "input" | "output", index: number): FlowPort => ({
  id: `${direction}_${index}_${uid().slice(0, 6)}`,
  flowUuid: edge.data?.flowUuid ?? `flow_${index}`,
  name: edge.data?.flowName ?? `flow_${index}`,
  flowNameEn: edge.data?.flowNameEn,
  unit: edge.data?.unit ?? "kg",
  amount:
    edge.data?.quantityMode === "dual"
      ? edge.data?.consumerAmount ?? edge.data?.amount ?? 0
      : edge.data?.amount ?? edge.data?.consumerAmount ?? 0,
  externalSaleAmount: 0,
  type: edge.data?.type ?? "technosphere",
  direction,
  showOnNode: true,
  internalExposed: true,
  dbMapping: edge.data?.dbMapping,
});

const resolveEdgeBoundaryPort = (
  nodeById: Map<string, Node<LcaNodeData>>,
  edge: Edge<LcaEdgeData>,
  side: "source" | "target",
): FlowPort | undefined => {
  const node = nodeById.get(side === "source" ? edge.source : edge.target);
  if (!node) {
    return undefined;
  }
  const flowUuid = String(edge.data?.flowUuid ?? "").trim();
  const handleId = side === "source" ? edge.sourceHandle : edge.targetHandle;
  const portId =
    side === "source"
      ? parseHandlePortId(handleId ?? undefined, "out:")
      : parseHandlePortId(handleId ?? undefined, "in:");
  const ports = side === "source" ? node.data.outputs : node.data.inputs;
  if (portId) {
    const exact = ports.find((port) => port.id === portId && (!flowUuid || port.flowUuid === flowUuid));
    if (exact) {
      return exact;
    }
  }
  return flowUuid ? ports.find((port) => port.flowUuid === flowUuid) : undefined;
};

const buildNodeFromTemplate = (template: LcaProcessTemplate, position: XYPosition): Node<LcaNodeData> =>
  sanitizeMarketNode({
    id: `node_${uid()}`,
    type: "lcaProcess",
    position,
    data: {
      nodeKind: template.nodeKind,
      mode: template.mode ?? (template.nodeKind === "unit_process" ? "balanced" : "normalized"),
      marketAllowMixedFlows: template.processUuid.startsWith("market_") ? false : undefined,
      lciRole: template.lciRole,
      ptsUuid: template.nodeKind === "pts_module" ? template.processUuid : undefined,
      ptsCanvasId: undefined,
      processUuid: template.processUuid,
      name: template.name,
      location: template.location,
      referenceProduct: template.referenceProduct,
      inputs: template.inputs.map((port) => toPort(port, "input")),
      outputs: template.outputs.map((port) => toPort(port, "output")),
    },
  });

const getTargetHandle = (
  targetNode: Node<LcaNodeData>,
  flowUuid: string,
  edges?: Array<Edge<LcaEdgeData>>,
): string | undefined => {
  const byFlow = targetNode.data.inputs.filter((item) => item.flowUuid === flowUuid);
  if (byFlow.length === 0) {
    return undefined;
  }
  if (!edges) {
    return `in:${byFlow[0].id}`;
  }
  const occupied = new Set(
    edges
      .filter((edge) => edge.target === targetNode.id)
      .map((edge) => parseHandlePortId(edge.targetHandle ?? undefined, "in:"))
      .filter((id): id is string => Boolean(id)),
  );
  const free = byFlow.find((port) => !occupied.has(port.id));
  if (free) {
    return `in:${free.id}`;
  }
  // For market process, each upstream source should occupy its own input row.
  // Returning undefined here triggers auto-creation of a new input row.
  if (isMarketProcessNode(targetNode)) {
    return undefined;
  }
  return `in:${byFlow[0].id}`;
};

const buildAutoInputPort = (sourcePort: FlowPort): FlowPort => ({
  id: `in_${uid().slice(0, 8)}`,
  flowUuid: sourcePort.flowUuid,
  name: sourcePort.name,
  flowNameEn: sourcePort.flowNameEn,
  unit: sourcePort.unit,
  unitGroup: sourcePort.unitGroup,
  amount: 0,
  externalSaleAmount: 0,
  type: sourcePort.type,
  direction: "input",
  showOnNode: true,
});

const extractMarketSourceSuffix = (flowName: string): string => {
  const trimmed = (flowName ?? "").trim();
  if (!trimmed) {
    return "";
  }
  const at = trimmed.indexOf("@");
  return at >= 0 ? trimmed.slice(at + 1).trim() : "";
};

const formatMarketInputName = (
  flowName: string,
  sourceProcessName?: string,
  nestedProcessName?: string,
): string => {
  const trimmedFlow = (flowName ?? "").trim();
  const trimmedSource = (sourceProcessName ?? "").trim();
  const trimmedNested = (nestedProcessName ?? "").trim();
  const suffixParts = [trimmedSource, trimmedNested].filter(
    (part, index, arr) => part && arr.indexOf(part) === index,
  );
  if (!trimmedFlow) {
    return suffixParts.length > 0 ? `flow@${suffixParts.join("@")}` : "flow";
  }
  return suffixParts.length > 0 ? `${trimmedFlow}@${suffixParts.join("@")}` : trimmedFlow;
};

const withMarketInputSourceIdentity = (
  port: FlowPort,
  source: { nodeId?: string; processUuid?: string; processName?: string; portName?: string; nestedProcessName?: string },
): FlowPort => ({
  ...port,
  name: formatMarketInputName(
    stripMarketSourceSuffix(source.portName || port.name),
    source.processName,
    source.nestedProcessName || extractMarketSourceSuffix(source.portName || port.name),
  ),
  sourceNodeId: source.nodeId || port.sourceNodeId,
  sourceProcessUuid: source.processUuid || port.sourceProcessUuid,
  sourceProcessName: source.processName || port.sourceProcessName,
  nestedSourceProcessName: source.nestedProcessName || port.nestedSourceProcessName,
});

const stripMarketSourceSuffix = (flowName: string): string => {
  const trimmed = (flowName ?? "").trim();
  if (!trimmed) {
    return trimmed;
  }
  const at = trimmed.indexOf("@");
  return at > 0 ? trimmed.slice(0, at).trim() : trimmed;
};

const buildAutoOutputPortFromInput = (inputPort: FlowPort): FlowPort => ({
  id: `out_${uid().slice(0, 8)}`,
  flowUuid: inputPort.flowUuid,
  name: stripMarketSourceSuffix(inputPort.name),
  flowNameEn: inputPort.flowNameEn,
  unit: inputPort.unit,
  unitGroup: inputPort.unitGroup,
  amount: 0,
  externalSaleAmount: 0,
  type: inputPort.type,
  direction: "output",
  showOnNode: true,
});

const getAutoNodePosition = (canvas: CanvasGraph): XYPosition => {
  const count = canvas.nodes.length;
  return {
    x: 120 + (count % 6) * 52,
    y: 120 + Math.floor(count / 6) * 42,
  };
};

const collectBalancedWarningsForCanvas = (
  canvas: CanvasGraph,
): Array<{
  flowUuid: string;
  flowName: string;
  outputTotal: number;
  inputTotal: number;
  externalSaleTotal: number;
  processNames: string[];
}> => {
  const nodeById = new Map(canvas.nodes.map((node) => [node.id, node]));
  const agg = new Map<
    string,
    {
      flowName: string;
      outputs: number;
      inputs: number;
      externalSales: number;
      outputKeys: Set<string>;
      externalSaleKeys: Set<string>;
      processNames: Set<string>;
    }
  >();

  for (const edge of canvas.edges) {
    const sourceNode = nodeById.get(edge.source);
    const targetNode = nodeById.get(edge.target);
    if (!sourceNode || !targetNode) {
      continue;
    }
    if (sourceNode.data.mode !== "balanced" || targetNode.data.mode !== "balanced") {
      continue;
    }
    const flowUuid = edge.data?.flowUuid ?? "";
    if (!flowUuid) {
      continue;
    }
    const row = agg.get(flowUuid) ?? {
      flowName: edge.data?.flowName ?? flowUuid,
      outputs: 0,
      inputs: 0,
      externalSales: 0,
      outputKeys: new Set<string>(),
      externalSaleKeys: new Set<string>(),
      processNames: new Set<string>(),
    };
    const inAmount = edge.data?.consumerAmount ?? edge.data?.amount ?? 0;
    row.inputs += inAmount;
    const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
    const sourcePort =
      sourceNode.data.outputs.find((p) => p.id === sourcePortId) ??
      sourceNode.data.outputs.find((p) => p.flowUuid === flowUuid);
    if (sourcePort) {
      const sourceKey = `${sourceNode.id}::${sourcePort.id}`;
      if (!row.outputKeys.has(sourceKey)) {
        row.outputKeys.add(sourceKey);
        row.outputs += sourcePort.amount ?? 0;
      }
      if (!row.externalSaleKeys.has(sourceKey)) {
        row.externalSaleKeys.add(sourceKey);
        row.externalSales += sourcePort.externalSaleAmount ?? 0;
      }
    }
    row.processNames.add(sourceNode.data.name);
    row.processNames.add(targetNode.data.name);
    agg.set(flowUuid, row);
  }

  const warnings: Array<{
    flowUuid: string;
    flowName: string;
    outputTotal: number;
    inputTotal: number;
    externalSaleTotal: number;
    processNames: string[];
  }> = [];
  for (const [flowUuid, row] of agg.entries()) {
    const balancedOutput = row.outputs - row.externalSales;
    if (Math.abs(balancedOutput - row.inputs) > 1e-9) {
      warnings.push({
        flowUuid,
        flowName: row.flowName,
        outputTotal: row.outputs,
        inputTotal: row.inputs,
        externalSaleTotal: row.externalSales,
        processNames: Array.from(row.processNames),
      });
    }
  }
  return warnings;
};

const collectMarketWarningsForCanvas = (canvas: CanvasGraph): MarketWarning[] => {
  const warnings: MarketWarning[] = [];
  for (const node of canvas.nodes) {
    if (!isMarketProcessNode(node)) {
      continue;
    }
    const issues: string[] = [];
    if (node.data.mode !== "normalized") {
      issues.push("市场过程必须为归一化（normalized）");
    }

    const technoInputs = node.data.inputs.filter((p) => p.type !== "biosphere");
    const technoOutputs = node.data.outputs.filter((p) => p.type !== "biosphere");

    if (technoOutputs.length !== 1) {
      if (technoOutputs.length === 0) {
        issues.push("市场过程必须有且仅有一个产品输出，请先选择替代流");
      } else {
        issues.push("市场过程仅允许 1 条中间流输出");
      }
    }

    const output = technoOutputs[0];
    if (output) {
      if (Math.abs((output.amount ?? 0) - 1) > 1e-9) {
        issues.push("市场过程输出数值必须为 1");
      }
      if (!output.isProduct) {
        issues.push("市场过程输出必须定义为产品");
      }
    }

    const sumInputs = technoInputs.reduce((sum, port) => sum + (Number.isFinite(port.amount) ? port.amount : 0), 0);
    if (technoInputs.length > 0 && Math.abs(sumInputs - 1) > 1e-9) {
      issues.push("市场过程输入份额总和必须为 1");
    }

    if (!node.data.marketAllowMixedFlows && output) {
      const mismatch = technoInputs.some((input) => input.flowUuid !== output.flowUuid);
      if (mismatch) {
        issues.push("未开启混合流时，输入与输出必须使用同一 UUID");
      }
    }

    if (issues.length > 0) {
      warnings.push({
        nodeId: node.id,
        nodeName: node.data.name,
        issues,
      });
    }
  }
  return warnings;
};

const createBlankNodeData = (kind: NodeCreateKind, uiLanguage: "zh" | "en"): LcaNodeData => {
  if (kind === "unit_process") {
    const suffix = uid().slice(0, 8);
    return {
      nodeKind: "unit_process",
      mode: "balanced",
      processUuid: `proc_custom_${suffix}`,
      name: uiLanguage === "zh" ? "新建单元过程" : "New Unit Process",
      location: "",
      referenceProduct: "",
      inputs: [],
      outputs: [],
    };
  }

  if (kind === "market_process") {
    return {
      nodeKind: "market_process",
      mode: "normalized",
      processUuid: `market_${uid().slice(0, 8)}`,
      name: uiLanguage === "zh" ? "新建市场过程" : "New Market Process",
      location: "",
      referenceProduct: "",
      marketAllowMixedFlows: false,
      inputs: [],
      outputs: [],
    };
  }

  if (kind === "pts_module") {
    const ptsUuid = `pts_${uid().slice(0, 8)}`;
    return {
      nodeKind: "pts_module",
      mode: "normalized",
      ptsUuid,
      processUuid: ptsUuid,
      name: uiLanguage === "zh" ? "新建PTS模块" : "New PTS Module",
      location: "",
      referenceProduct: "",
      inputs: [],
      outputs: [],
    };
  }

  return {
    nodeKind: "lci_dataset",
    mode: "normalized",
    lciRole: "provider",
    processUuid: `lci_${uid().slice(0, 8)}`,
    name: uiLanguage === "zh" ? "新建LCI数据集" : "New LCI Dataset",
    location: "",
    referenceProduct: "",
    inputs: [],
    outputs: [],
  };
};

const normalizeNodeName = (value: string) => String(value ?? "").trim().toLowerCase();

const buildUniqueNodeName = (
  existingNodes: Array<Node<LcaNodeData>>,
  baseName: string,
  uiLanguage: "zh" | "en",
): string => {
  const trimmedBaseName = String(baseName ?? "").trim() || (uiLanguage === "zh" ? "新建过程" : "New Process");
  const existingNames = new Set(existingNodes.map((node) => normalizeNodeName(node.data.name)));
  if (!existingNames.has(normalizeNodeName(trimmedBaseName))) {
    return trimmedBaseName;
  }
  for (let index = 1; index < 1000; index += 1) {
    const candidate = uiLanguage === "zh" ? `${trimmedBaseName}（${index}）` : `${trimmedBaseName} (${index})`;
    if (!existingNames.has(normalizeNodeName(candidate))) {
      return candidate;
    }
  }
  return `${trimmedBaseName}_${uid().slice(0, 4)}`;
};

const buildClonedProcessIdentity = (
  source: Node<LcaNodeData>,
): Pick<LcaNodeData, "processUuid" | "ptsUuid" | "ptsCanvasId"> => {
  if (source.data.nodeKind === "pts_module") {
    const ptsUuid = `pts_${uid().slice(0, 8)}`;
    return {
      processUuid: ptsUuid,
      ptsUuid,
      ptsCanvasId: undefined,
    };
  }
  if (isLciNode(source)) {
    return {
      processUuid: `lci_${uid().slice(0, 8)}`,
      ptsUuid: undefined,
      ptsCanvasId: undefined,
    };
  }
  if (isMarketProcessNode(source)) {
    return {
      processUuid: `market_${uid().slice(0, 8)}`,
      ptsUuid: undefined,
      ptsCanvasId: undefined,
    };
  }
  return {
    processUuid: `proc_custom_${uid().slice(0, 8)}`,
    ptsUuid: undefined,
    ptsCanvasId: undefined,
  };
};

const buildBlankNode = (kind: NodeCreateKind, position: XYPosition, uiLanguage: "zh" | "en"): Node<LcaNodeData> =>
  sanitizeMarketNode({
    id: `node_${uid()}`,
    type: "lcaProcess",
    position,
    data: createBlankNodeData(kind, uiLanguage),
  });

const buildImportedUnitProcessNode = (
  payload: ImportedUnitProcessPayload,
  position: XYPosition,
): Node<LcaNodeData> =>
  sanitizeMarketNode({
    id: `node_${uid()}`,
    type: "lcaProcess",
    position,
    data: {
      nodeKind: "unit_process",
      mode: "balanced",
      importMode: payload.importMode,
      sourceProcessUuid: payload.sourceProcessUuid,
      importWarnings: payload.warnings,
      filteredExchanges: payload.filteredExchanges,
      processUuid: payload.processUuid,
      name: payload.name,
      location: payload.location ?? "",
      referenceProduct: payload.referenceProduct ?? "",
      referenceProductFlowUuid: payload.referenceProductFlowUuid,
      referenceProductDirection: payload.referenceProductDirection,
      inputs: payload.inputs,
      outputs: payload.outputs,
    },
  });

const sanitizeMarketNode = (node: Node<LcaNodeData>): Node<LcaNodeData> => {
  if (node.data.nodeKind === "pts_module") {
    const ptsUuid = node.data.ptsUuid ?? node.data.processUuid;
    return {
      ...node,
      data: {
        ...node.data,
        mode: "normalized",
        ptsUuid,
        processUuid: node.data.processUuid || ptsUuid,
      },
    };
  }

  if (isLciNode(node)) {
    const inputIntermediates = node.data.inputs.filter((p) => p.type !== "biosphere");
    const outputIntermediates = node.data.outputs.filter((p) => p.type !== "biosphere");
    const inputBiosphere = node.data.inputs.filter((p) => p.type === "biosphere");
    const outputBiosphere = node.data.outputs.filter((p) => p.type === "biosphere");
    const allIntermediates = [...outputIntermediates, ...inputIntermediates];
    const chosen =
      allIntermediates.find((p) => p.flowUuid === node.data.referenceProductFlowUuid) ??
      allIntermediates[0];

    const nextInputs = inputBiosphere.concat(
      chosen && chosen.direction === "input" ? [{ ...chosen, direction: "input" as const }] : [],
    );
    const nextOutputs = outputBiosphere.concat(
      chosen && chosen.direction === "output" ? [{ ...chosen, direction: "output" as const }] : [],
    );
    const inferredLciRole = inferLciRoleByIntermediates({
      ...node,
      data: {
        ...node.data,
        inputs: nextInputs,
        outputs: nextOutputs,
      },
    });

    return {
      ...node,
      data: {
        ...node.data,
        nodeKind: "lci_dataset",
        mode: "normalized",
        lciRole: inferredLciRole,
        referenceProduct: chosen?.name ?? node.data.referenceProduct,
        referenceProductFlowUuid: chosen?.flowUuid,
        referenceProductDirection: chosen?.direction,
        inputs: nextInputs,
        outputs: nextOutputs,
      },
    };
  }

  if (!isMarketProcessNode(node)) {
    return node;
  }
  const resolveCanonicalUnitGroup = (): string | undefined => {
    const candidates = [...(node.data.outputs ?? []), ...(node.data.inputs ?? [])];
    for (const port of candidates) {
      if (port.unitGroup) {
        return port.unitGroup;
      }
    }
    return undefined;
  };
  const enforceSingleFlow = shouldEnforceMarketSingleFlow(node);
  const isMarketPlaceholder = (port: FlowPort | undefined): boolean => {
    if (!port) {
      return true;
    }
    const name = (port.name ?? "").trim().toLowerCase();
    const uuid = (port.flowUuid ?? "").trim().toLowerCase();
    return name === "market product" || uuid === "flow_market";
  };

  if (!enforceSingleFlow) {
    const firstOutput = node.data.outputs[0];
    const fallback = node.data.inputs[0];
    const preferredOutput = isMarketPlaceholder(firstOutput) && fallback ? undefined : firstOutput;
    const canonicalUnitGroup = resolveCanonicalUnitGroup();
    const output: FlowPort | undefined =
      preferredOutput || fallback
        ? {
            ...(preferredOutput ?? buildAutoOutputPortFromInput(fallback as FlowPort)),
            flowUuid: preferredOutput?.flowUuid || fallback?.flowUuid || node.data.referenceProductFlowUuid || "",
            name: preferredOutput?.name || fallback?.name || node.data.referenceProduct || "Market Product",
            unit: preferredOutput?.unit || fallback?.unit || "kg",
            unitGroup: canonicalUnitGroup,
            amount: 1,
            isProduct: true,
            type: "technosphere",
            direction: "output",
            showOnNode: preferredOutput?.showOnNode ?? true,
            externalSaleAmount: 0,
          }
        : undefined;

    return {
      ...node,
      data: {
        ...node.data,
        nodeKind: "market_process",
        mode: "normalized",
        marketAllowMixedFlows: true,
        inputs: node.data.inputs.map((port) => ({
          ...port,
          unit: output?.unit ?? port.unit,
          unitGroup: output?.unitGroup ?? port.unitGroup,
          type: "technosphere",
          direction: "input",
        })),
        outputs: output ? [output] : [],
        referenceProduct: output?.name ?? node.data.referenceProduct,
        referenceProductFlowUuid: output?.flowUuid,
        referenceProductDirection: output ? "output" : node.data.referenceProductDirection,
      },
    };
  }

  if (node.data.inputs.length === 0 && node.data.outputs.length === 0) {
    return {
      ...node,
      data: {
        ...node.data,
        nodeKind: "market_process",
        mode: "normalized",
        marketAllowMixedFlows: false,
      },
    };
  }

  const firstOutput = node.data.outputs[0];
  const firstInput = node.data.inputs[0];
  const preferredOutput = isMarketPlaceholder(firstOutput) && firstInput ? undefined : firstOutput;
  const seedFlowUuid =
    preferredOutput?.flowUuid ||
    node.data.referenceProductFlowUuid ||
    firstInput?.flowUuid ||
    `flow_market_${uid().slice(0, 6)}`;
  const seedName = stripMarketSourceSuffix(
    preferredOutput?.name || node.data.referenceProduct || firstInput?.name || "Market Product",
  );
  const seedFlowNameEn =
    String(preferredOutput?.flowNameEn ?? "").trim() ||
    String(firstInput?.flowNameEn ?? "").trim() ||
    undefined;
  const seedUnit = preferredOutput?.unit || firstInput?.unit || "kg";
  const seedUnitGroup = resolveCanonicalUnitGroup();

  const canonicalOutput: FlowPort = {
    id: preferredOutput?.id || firstOutput?.id || `out_${uid().slice(0, 8)}`,
    flowUuid: seedFlowUuid,
    name: seedName,
    flowNameEn: seedFlowNameEn,
    unit: seedUnit,
    unitGroup: seedUnitGroup,
    amount: 1,
    isProduct: true,
    externalSaleAmount: 0,
    type: "technosphere",
    direction: "output",
    showOnNode: preferredOutput?.showOnNode ?? firstOutput?.showOnNode ?? true,
  };

  const inputs = node.data.inputs.map((port) => ({
    ...port,
    flowUuid: seedFlowUuid,
    name: (port.name ?? "").trim() || seedName,
    unit: seedUnit,
    unitGroup: seedUnitGroup ?? port.unitGroup,
    type: "technosphere" as const,
    direction: "input" as const,
  }));

  return {
    ...node,
    data: {
      ...node.data,
      nodeKind: "market_process",
      mode: "normalized",
      marketAllowMixedFlows: false,
      referenceProduct: canonicalOutput.name,
      referenceProductFlowUuid: canonicalOutput.flowUuid,
      referenceProductDirection: "output",
      inputs,
      outputs: [canonicalOutput],
    },
  };
};

const isBalancedUnitNode = (node: Node<LcaNodeData> | undefined): boolean =>
  Boolean(
    node &&
      node.data.nodeKind === "unit_process" &&
      node.data.mode === "balanced" &&
      !((node.data.processUuid ?? "").startsWith("market_")),
  );

const isProductPort = (port: FlowPort | undefined): boolean => Boolean(port?.isProduct);
const isIntermediatePort = (port: FlowPort | undefined): boolean => Boolean(port && port.type !== "biosphere");
const isProductIntermediate = (port: FlowPort | undefined): boolean => isIntermediatePort(port) && isProductPort(port);
const isNonProductIntermediate = (port: FlowPort | undefined): boolean => isIntermediatePort(port) && !isProductPort(port);
const hasIntermediateProductPort = (node: Node<LcaNodeData> | undefined): boolean =>
  Boolean(
    node &&
      [...node.data.inputs, ...node.data.outputs].some(
        (port) => port.type !== "biosphere" && Boolean(port.isProduct),
      ),
  );

const getRelationViolationHint = (params: {
  sourcePort: FlowPort;
  targetPort: FlowPort;
  targetNode: Node<LcaNodeData>;
  existingIncomingFromOtherSources: number;
  existingOutgoingToOtherTargets: number;
  targetInputOccupiedByOtherSource?: boolean;
}): string | undefined => {
  const {
    sourcePort,
    targetPort,
    targetNode,
    existingIncomingFromOtherSources,
    existingOutgoingToOtherTargets,
    targetInputOccupiedByOtherSource,
  } = params;
  if (isProductIntermediate(sourcePort) && isProductIntermediate(targetPort)) {
    return RULE_HINTS.productToProductForbidden;
  }
  if (isNonProductIntermediate(sourcePort) && isNonProductIntermediate(targetPort)) {
    return RULE_HINTS.nonProductToNonProductForbidden;
  }
  if (isNonProductIntermediate(targetPort) && targetInputOccupiedByOtherSource) {
    return RULE_HINTS.nonProductOneToOneOnly;
  }
  if (isNonProductIntermediate(sourcePort) && isProductIntermediate(targetPort) && existingOutgoingToOtherTargets > 0) {
    return RULE_HINTS.nonProductOneToOneOnly;
  }
  if (
    isProductIntermediate(sourcePort) &&
    isNonProductIntermediate(targetPort) &&
    existingIncomingFromOtherSources > 0 &&
    !isMarketProcessNode(targetNode)
  ) {
    return RULE_HINTS.nonProductOneToOneOnly;
  }
  return undefined;
};

const findOutputPortByHandleOrFlow = (
  node: Node<LcaNodeData> | undefined,
  handleId: string | null | undefined,
  flowUuid: string,
): FlowPort | undefined => {
  if (!node) {
    return undefined;
  }
  const handlePortId = parseHandlePortId(handleId, "out:");
  if (!handlePortId) {
    return undefined;
  }
  return node.data.outputs.find(
    (port) =>
      (port.id === handlePortId || String(port.legacyPortId ?? "").trim() === handlePortId) &&
      port.flowUuid === flowUuid,
  );
};

const findInputPortByHandleOrFlow = (
  node: Node<LcaNodeData> | undefined,
  handleId: string | null | undefined,
  flowUuid: string,
): FlowPort | undefined => {
  if (!node) {
    return undefined;
  }
  const handlePortId = parseHandlePortId(handleId, "in:");
  if (!handlePortId) {
    return undefined;
  }
  return node.data.inputs.find(
    (port) =>
      (port.id === handlePortId || String(port.legacyPortId ?? "").trim() === handlePortId) &&
      port.flowUuid === flowUuid,
  );
};

const getConnectionRuleHint = (params: {
  sourceNode: Node<LcaNodeData>;
  sourcePort: FlowPort;
  targetNode: Node<LcaNodeData>;
  targetPort: FlowPort;
  edges: Array<Edge<LcaEdgeData>>;
}): string | undefined => {
  const { sourceNode, sourcePort, targetNode, targetPort, edges } = params;
  const baseHint = getConnectionHint(sourceNode, targetNode);
  if (baseHint) {
    return baseHint;
  }
  if (isMarketProcessNode(targetNode) && !isProductIntermediate(sourcePort)) {
    return RULE_HINTS.marketInputRequiresProductSource;
  }
  const existingIncomingFromOtherSources = edges.filter(
    (edge) => edge.target === targetNode.id && edge.data?.flowUuid === sourcePort.flowUuid && edge.source !== sourceNode.id,
  ).length;
  const existingOutgoingToOtherTargets = edges.filter(
    (edge) => edge.source === sourceNode.id && edge.data?.flowUuid === sourcePort.flowUuid && edge.target !== targetNode.id,
  ).length;
  const targetInputOccupiedByOtherSource = edges.some(
    (edge) =>
      edge.target === targetNode.id &&
      parseHandlePortId(edge.targetHandle ?? undefined, "in:") === targetPort.id &&
      edge.source !== sourceNode.id,
  );
  return getRelationViolationHint({
    sourcePort,
    targetPort,
    targetNode,
    existingIncomingFromOtherSources,
    existingOutgoingToOtherTargets,
    targetInputOccupiedByOtherSource,
  });
};

const parseHandlePortId = (handleId: string | null | undefined, prefix: "in:" | "out:"): string | undefined => {
  if (!handleId) {
    return undefined;
  }
  const normalizedPrefixes =
    prefix === "in:" ? ["in:", "inl:", "inr:", "input_"] : ["out:", "outl:", "outr:", "output_"];
  const matched = normalizedPrefixes.find((p) => handleId.startsWith(p));
  if (!matched) {
    return undefined;
  }
  return handleId.slice(matched.length);
};

const toBackendPortId = (value: string | null | undefined, direction: "input" | "output"): string | undefined => {
  if (!value) {
    return undefined;
  }
  const raw = String(value);
  if (direction === "input") {
    if (raw.startsWith("input_")) {
      return raw.slice("input_".length);
    }
    if (raw.startsWith("inl:")) {
      return raw.slice("inl:".length);
    }
    if (raw.startsWith("inr:")) {
      return raw.slice("inr:".length);
    }
    if (raw.startsWith("in:in::")) {
      return raw.slice("in:".length);
    }
    if (raw.startsWith("in::")) {
      return raw;
    }
    if (raw.startsWith("in:")) {
      return raw.slice("in:".length);
    }
    return raw;
  }
  if (raw.startsWith("output_")) {
    return raw.slice("output_".length);
  }
  if (raw.startsWith("outl:")) {
    return raw.slice("outl:".length);
  }
  if (raw.startsWith("outr:")) {
    return raw.slice("outr:".length);
  }
  if (raw.startsWith("out:")) {
    return raw.slice("out:".length);
  }
  if (raw.startsWith(":")) {
    return raw.slice(1);
  }
  return raw;
};

const toCanvasHandleId = (value: string | null | undefined, direction: "input" | "output"): string | undefined => {
  if (!value) {
    return undefined;
  }
  const raw = String(value);
  if (direction === "input") {
    if (raw.startsWith("input_")) {
      return `in:${raw.slice("input_".length)}`;
    }
    if (raw.startsWith("inl:") || raw.startsWith("inr:") || raw.startsWith("in:")) {
      return raw;
    }
    return `in:${raw}`;
  }
  if (raw.startsWith("output_")) {
    return `out:${raw.slice("output_".length)}`;
  }
  if (raw.startsWith("outl:") || raw.startsWith("outr:") || raw.startsWith("out:")) {
    return raw;
  }
  return `out:${raw}`;
};

const buildHandleWithSameSide = (
  handleId: string | null | undefined,
  prefix: "in:" | "out:",
  portId: string,
): string => {
  const sidePrefixes = prefix === "in:" ? ["inl:", "inr:", "in:"] : ["outl:", "outr:", "out:"];
  const matched = sidePrefixes.find((p) => Boolean(handleId && handleId.startsWith(p)));
  return `${matched ?? prefix}${portId}`;
};

const hasInputHandleId = (node: Node<LcaNodeData>, handleId: string | undefined): boolean => {
  const portId = parseHandlePortId(handleId, "in:");
  if (!portId) {
    return false;
  }
  return node.data.inputs.some((port) => port.id === portId);
};

const resolveTargetInputAmount = (
  targetNode: Node<LcaNodeData>,
  targetHandle: string | undefined,
  flowUuid: string,
  fallback: number,
): number => {
  const targetPortId = parseHandlePortId(targetHandle, "in:");
  if (!targetPortId) {
    return fallback;
  }
  const targetPort = targetNode.data.inputs.find((p) => p.id === targetPortId && p.flowUuid === flowUuid);
  return targetPort && Number.isFinite(targetPort.amount) ? targetPort.amount : fallback;
};

const resolveEdgeDataByNodes = (
  edge: Edge<LcaEdgeData>,
  nodeById: Map<string, Node<LcaNodeData>>,
): { data: LcaEdgeData; sourcePort: FlowPort; targetPort: FlowPort } | undefined => {
  const sourceNode = nodeById.get(edge.source);
  const targetNode = nodeById.get(edge.target);
  if (!sourceNode || !targetNode) {
    return undefined;
  }

  const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
  const targetPortId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
  const edgeFlowUuid = edge.data?.flowUuid ?? "";
  const sourcePortById = sourcePortId
    ? sourceNode.data.outputs.find((p) => p.id === sourcePortId || String(p.legacyPortId ?? "").trim() === sourcePortId)
    : undefined;
  const targetPortById = targetPortId
    ? targetNode.data.inputs.find((p) => p.id === targetPortId || String(p.legacyPortId ?? "").trim() === targetPortId)
    : undefined;
  const sourceFallbackCandidates = edgeFlowUuid
    ? sourceNode.data.outputs.filter((p) => p.flowUuid === edgeFlowUuid)
    : [];
  const targetFallbackCandidates = edgeFlowUuid
    ? targetNode.data.inputs.filter((p) => p.flowUuid === edgeFlowUuid)
    : [];
  const sourcePortResolved =
    sourcePortById ??
    (sourceFallbackCandidates.length === 1 ? sourceFallbackCandidates[0] : undefined);
  const targetPortResolved =
    targetPortById ??
    (targetFallbackCandidates.length === 1 ? targetFallbackCandidates[0] : undefined);
  if (!sourcePortResolved || !targetPortResolved) {
    return undefined;
  }
  const flowUuid = edgeFlowUuid || targetPortResolved.flowUuid || sourcePortResolved.flowUuid || "";
  if (!flowUuid || sourcePortResolved.flowUuid !== flowUuid || targetPortResolved.flowUuid !== flowUuid) {
    return undefined;
  }

  const sourcePort = sourcePortResolved;
  const targetPort = targetPortResolved;

  const quantityMode = edge.data?.quantityMode ?? resolveQuantityMode(sourceNode, targetNode);
  const consumerFromTarget = targetPort && Number.isFinite(targetPort.amount) ? targetPort.amount : undefined;
  const providerFromSource = sourcePort && Number.isFinite(sourcePort.amount) ? sourcePort.amount : undefined;

  let amount = 0;
  let providerAmount = 0;
  let consumerAmount = 0;

  if (quantityMode === "dual") {
    consumerAmount =
      consumerFromTarget ??
      edge.data?.consumerAmount ??
      edge.data?.amount ??
      providerFromSource ??
      edge.data?.providerAmount ??
      0;
    providerAmount = providerFromSource ?? edge.data?.providerAmount ?? edge.data?.amount ?? consumerAmount;
    amount = consumerAmount;
  } else {
    amount =
      consumerFromTarget ??
      edge.data?.amount ??
      edge.data?.consumerAmount ??
      providerFromSource ??
      edge.data?.providerAmount ??
      0;
    providerAmount = amount;
    consumerAmount = amount;
  }

  return {
    sourcePort,
    targetPort,
    data: {
      flowUuid,
      flowName: edge.data?.flowName ?? targetPort?.name ?? sourcePort?.name ?? flowUuid,
      quantityMode,
      amount,
      providerAmount,
      consumerAmount,
      unit: edge.data?.unit ?? sourcePort?.unit ?? targetPort?.unit ?? "kg",
      type: edge.data?.type ?? sourcePort?.type ?? targetPort?.type ?? "technosphere",
      allocation: edge.data?.allocation ?? "none",
      dbMapping: edge.data?.dbMapping,
    },
  };
};

const normalizeEdgeWithNodeMap = (
  edge: Edge<LcaEdgeData>,
  nodeById: Map<string, Node<LcaNodeData>>,
): Edge<LcaEdgeData> | undefined => {
  const resolved = resolveEdgeDataByNodes(edge, nodeById);
  if (!resolved) {
    return undefined;
  }
  return {
    ...edge,
    sourceHandle: buildHandleWithSameSide(edge.sourceHandle, "out:", resolved.sourcePort.id),
    targetHandle: buildHandleWithSameSide(edge.targetHandle, "in:", resolved.targetPort.id),
    data: resolved.data,
  };
};

const normalizeSingleQuantityEdgeData = (data: LcaEdgeData): LcaEdgeData => {
  if (data.quantityMode !== "single") {
    return data;
  }
  const amount = data.amount ?? data.consumerAmount ?? data.providerAmount ?? 0;
  return {
    ...data,
    amount,
    providerAmount: amount,
    consumerAmount: amount,
  };
};

const getEffectiveEdgeAmount = (edge: Edge<LcaEdgeData>): number => {
  if (edge.data?.quantityMode === "dual") {
    return edge.data.consumerAmount ?? edge.data.amount ?? 0;
  }
  return edge.data?.amount ?? edge.data?.consumerAmount ?? 0;
};

const getPtsPortIdentityKey = (
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
  return port.flowUuid;
};

const getPtsPortSourceIdentityKey = (
  direction: "input" | "output",
  port: Pick<FlowPort, "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId">,
): string =>
  direction === "output"
    ? `${port.flowUuid}@@${String(port.sourceProcessUuid ?? "").trim()}@@${String(port.sourceNodeId ?? "").trim()}@@${String(port.sourceProcessName ?? "").trim()}`
    : port.flowUuid;

const isSamePtsPortIdentity = (
  direction: "input" | "output",
  left: Pick<
    FlowPort,
    "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId" | "portKey" | "productKey"
  >,
  right: Pick<
    FlowPort,
    "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId" | "portKey" | "productKey"
  >,
): boolean =>
  getPtsPortIdentityKey(direction, left) === getPtsPortIdentityKey(direction, right) ||
  getPtsPortSourceIdentityKey(direction, left) === getPtsPortSourceIdentityKey(direction, right);

const isPtsPortExposedToRoot = (node: Node<LcaNodeData>, port: FlowPort): boolean =>
  node.data.nodeKind !== "pts_module" || ((port.internalExposed ?? true) && Boolean(port.showOnNode));

const enqueuePendingEdge = (
  queue: PendingEdgeItem[],
  item: Omit<PendingEdgeItem, "retries">,
): PendingEdgeItem[] => {
  const filtered = queue.filter((pending) => !(pending.canvasId === item.canvasId && pending.edge.id === item.edge.id));
  return [...filtered, { ...item, retries: 0 }];
};

const attachEdgeToCanvas = (
  canvas: CanvasGraph,
  edge: Edge<LcaEdgeData>,
): { canvas: CanvasGraph; attached: boolean } => {
  const nodeById = new Map(canvas.nodes.map((node) => [node.id, node]));
  const sourceNode = nodeById.get(edge.source);
  const targetNode = nodeById.get(edge.target);
  const flowUuid = edge.data?.flowUuid ?? "";
  if (!sourceNode || !targetNode || !flowUuid) {
    return { canvas, attached: false };
  }

  const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
  if (!sourcePortId) {
    return { canvas, attached: false };
  }
  const sourcePort = sourceNode.data.outputs.find((port) => port.id === sourcePortId && port.flowUuid === flowUuid);
  if (!sourcePort) {
    return { canvas, attached: false };
  }

  const targetHandle = edge.targetHandle ?? undefined;
  if (!targetHandle || !hasInputHandleId(targetNode, targetHandle)) {
    return { canvas, attached: false };
  }

  const targetPortId = parseHandlePortId(targetHandle, "in:");
  const targetPort =
    targetPortId && targetNode.data.inputs.find((port) => port.id === targetPortId && port.flowUuid === flowUuid);
  if (!targetPort) {
    return { canvas, attached: false };
  }

  const withReboundHandles: Edge<LcaEdgeData> = {
    ...edge,
    sourceHandle: buildHandleWithSameSide(edge.sourceHandle, "out:", sourcePort.id),
    targetHandle: buildHandleWithSameSide(targetHandle, "in:", targetPort.id),
  };
  const normalizedEdge = normalizeEdgeWithNodeMap(withReboundHandles, nodeById);
  if (!normalizedEdge) {
    return { canvas, attached: false };
  }

  const hasExisting = canvas.edges.some((item) => item.id === normalizedEdge.id);
  const nextEdges = hasExisting
    ? canvas.edges.map((item) => (item.id === normalizedEdge.id ? normalizedEdge : item))
    : [...canvas.edges, normalizedEdge];
  return {
    canvas: repairCanvasEdgeHandles({
      ...canvas,
      edges: nextEdges,
    }),
    attached: true,
  };
};

const repairCanvasEdgeHandles = (
  canvas: CanvasGraph,
  options?: {
    preservePtsProjectionNodeIds?: Set<string>;
  },
): CanvasGraph => {
  const nodeById = new Map(canvas.nodes.map((node) => [node.id, node]));
  const repairedEdges: Array<Edge<LcaEdgeData>> = [];
  const marketInputOccupied = new Set<string>();
  const marketSourceProcessOccupied = new Set<string>();
  const shouldPreservePtsProjectionEdge = (edge: Edge<LcaEdgeData>): boolean =>
    Boolean(
      options?.preservePtsProjectionNodeIds &&
        (options.preservePtsProjectionNodeIds.has(edge.source) || options.preservePtsProjectionNodeIds.has(edge.target)),
    );
  const resolveMarketSourceIdentity = (
    sourceNode: Node<LcaNodeData>,
    sourcePort: FlowPort,
  ): string => {
    if (sourceNode.data.nodeKind === "pts_module") {
      return `pts:${sourceNode.id}:${sourcePort.id}`;
    }
    const processUuid = (sourceNode.data.processUuid ?? "").trim();
    if (processUuid) {
      return `proc:${processUuid}`;
    }
    return `node:${sourceNode.id}`;
  };

  for (const edge of canvas.edges) {
    const source = nodeById.get(edge.source);
    const target = nodeById.get(edge.target);
    const flowUuid = edge.data?.flowUuid ?? "";
    if (!source || !target || !flowUuid) {
      repairedEdges.push(edge);
      continue;
    }

    const normalized = normalizeEdgeWithNodeMap(edge, nodeById);
    if (!normalized) {
      if (shouldPreservePtsProjectionEdge(edge)) {
        repairedEdges.push(edge);
        continue;
      }
      repairedEdges.push(edge);
      continue;
    }
    const normalizedSourcePortId = parseHandlePortId(normalized.sourceHandle ?? undefined, "out:");
    const normalizedTargetPortId = parseHandlePortId(normalized.targetHandle ?? undefined, "in:");
    const sourcePort =
      normalizedSourcePortId &&
      source.data.outputs.find((port) => port.id === normalizedSourcePortId && port.flowUuid === flowUuid);
    const targetPort =
      normalizedTargetPortId &&
      target.data.inputs.find((port) => port.id === normalizedTargetPortId && port.flowUuid === flowUuid);
    if (!sourcePort || !targetPort) {
      if (shouldPreservePtsProjectionEdge(edge)) {
        repairedEdges.push(edge);
        continue;
      }
      repairedEdges.push(edge);
      continue;
    }
    if (
      normalized.sourceHandle !== edge.sourceHandle ||
      normalized.targetHandle !== edge.targetHandle
    ) {
      debugPts("repairCanvasEdgeHandles:normalized", {
        canvasId: canvas.id,
        edgeId: edge.id,
        from: { sourceHandle: edge.sourceHandle, targetHandle: edge.targetHandle },
        to: { sourceHandle: normalized.sourceHandle, targetHandle: normalized.targetHandle },
        flowUuid,
      });
    }
      if (isMarketProcessNode(target)) {
        if (normalizedTargetPortId) {
          const marketKey = `${normalized.target}::${normalizedTargetPortId}`;
          if (marketInputOccupied.has(marketKey)) {
            if (shouldPreservePtsProjectionEdge(edge)) {
              repairedEdges.push(edge);
              continue;
            }
            repairedEdges.push(edge);
            continue;
          }
          marketInputOccupied.add(marketKey);
        }
        const sourceIdentity = resolveMarketSourceIdentity(source, sourcePort);
        const sourceKey = `${normalized.target}::${flowUuid}::${sourceIdentity}`;
        if (marketSourceProcessOccupied.has(sourceKey)) {
          if (shouldPreservePtsProjectionEdge(edge)) {
            repairedEdges.push(edge);
            continue;
          }
          repairedEdges.push(edge);
          continue;
        }
        marketSourceProcessOccupied.add(sourceKey);
      }
    repairedEdges.push(normalized);
  }

  return {
    ...canvas,
    edges: repairedEdges,
  };
};

const repairCanvasEdgeHandlesForNode = (canvas: CanvasGraph, nodeId: string): CanvasGraph => {
  const nodeById = new Map(canvas.nodes.map((node) => [node.id, node]));
  const nextEdges = canvas.edges.flatMap((edge) => {
    if (edge.source !== nodeId && edge.target !== nodeId) {
      return [edge];
    }
    const normalized = normalizeEdgeWithNodeMap(edge, nodeById);
    if (!normalized) {
      debugPts("repairCanvasEdgeHandlesForNode:dropped", {
        canvasId: canvas.id,
        nodeId,
        edgeId: edge.id,
        sourceHandle: edge.sourceHandle,
        targetHandle: edge.targetHandle,
        flowUuid: edge.data?.flowUuid,
      });
      return [];
    }
    if (normalized.sourceHandle !== edge.sourceHandle || normalized.targetHandle !== edge.targetHandle) {
      debugPts("repairCanvasEdgeHandlesForNode:normalized", {
        canvasId: canvas.id,
        nodeId,
        edgeId: edge.id,
        from: { sourceHandle: edge.sourceHandle, targetHandle: edge.targetHandle },
        to: { sourceHandle: normalized.sourceHandle, targetHandle: normalized.targetHandle },
        flowUuid: edge.data?.flowUuid,
      });
    }
    return [normalized];
  });
  return {
    ...canvas,
    edges: nextEdges,
  };
};

const pruneDetachedMarketInputs = (canvas: CanvasGraph): CanvasGraph => {
  const incomingHandleIdsByNode = new Map<string, Set<string>>();
  for (const edge of canvas.edges) {
    const targetHandleId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
    if (!targetHandleId) {
      continue;
    }
    const set = incomingHandleIdsByNode.get(edge.target) ?? new Set<string>();
    set.add(targetHandleId);
    incomingHandleIdsByNode.set(edge.target, set);
  }
  const nextNodes = canvas.nodes.map((node) => {
    if (!isMarketProcessNode(node)) {
      return node;
    }
    const attachedIds = incomingHandleIdsByNode.get(node.id) ?? new Set<string>();
    const nextInputs = node.data.inputs.filter((port) => port.type === "biosphere" || attachedIds.has(port.id));
    if (nextInputs.length === node.data.inputs.length) {
      return node;
    }
    return {
      ...node,
      data: {
        ...node.data,
        inputs: nextInputs,
      },
    };
  });
  return {
    ...canvas,
    nodes: nextNodes,
  };
};

const filterDetachedMarketIncomingEdgesForPtsPackaging = (
  edges: Array<Edge<LcaEdgeData>>,
  nodes: Array<Node<LcaNodeData>>,
): Array<Edge<LcaEdgeData>> => {
  const nodeById = new Map(nodes.map((node) => [node.id, node]));
  return edges.filter((edge) => {
    const targetNode = nodeById.get(edge.target);
    if (!targetNode || !isMarketProcessNode(targetNode)) {
      return true;
    }
    const targetPortId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
    if (!targetPortId) {
      return false;
    }
    return targetNode.data.inputs.some((port) => port.id === targetPortId);
  });
};

const updateNodePortAmountByFlow = (
  node: Node<LcaNodeData>,
  direction: "input" | "output",
  flowUuid: string,
  amount: number,
  preferredPortId?: string,
): Node<LcaNodeData> => {
  const key = direction === "input" ? "inputs" : "outputs";
  const ports = node.data[key];
  let targetIndex = -1;
  if (preferredPortId) {
    targetIndex = ports.findIndex((p) => p.id === preferredPortId);
  }
  if (targetIndex < 0) {
    targetIndex = ports.findIndex((p) => p.flowUuid === flowUuid);
  }
  if (targetIndex < 0) {
    return node;
  }
  const nextPorts = ports.map((p, idx) => (idx === targetIndex ? { ...p, amount } : p));
  return {
    ...node,
    data: {
      ...node.data,
      [key]: nextPorts,
    },
  };
};

const updateNodeOutputExternalSaleByFlow = (
  node: Node<LcaNodeData>,
  flowUuid: string,
  externalSaleAmount: number,
  preferredPortId?: string,
): Node<LcaNodeData> => {
  const ports = node.data.outputs;
  let targetIndex = -1;
  if (preferredPortId) {
    targetIndex = ports.findIndex((p) => p.id === preferredPortId);
  }
  if (targetIndex < 0) {
    targetIndex = ports.findIndex((p) => p.flowUuid === flowUuid);
  }
  if (targetIndex < 0) {
    return node;
  }
  const nextPorts = ports.map((p, idx) => (idx === targetIndex ? { ...p, externalSaleAmount } : p));
  return {
    ...node,
    data: {
      ...node.data,
      outputs: nextPorts,
    },
  };
};

const updateNodePortUnitByFlow = (
  node: Node<LcaNodeData>,
  direction: "input" | "output",
  flowUuid: string,
  unit: string,
  preferredPortId?: string,
): Node<LcaNodeData> => {
  const key = direction === "input" ? "inputs" : "outputs";
  const ports = node.data[key];
  let targetIndex = -1;
  if (preferredPortId) {
    targetIndex = ports.findIndex((p) => p.id === preferredPortId);
  }
  if (targetIndex < 0) {
    targetIndex = ports.findIndex((p) => p.flowUuid === flowUuid);
  }
  if (targetIndex < 0) {
    return node;
  }
  const nextPorts = ports.map((p, idx) => (idx === targetIndex ? { ...p, unit } : p));
  return {
    ...node,
    data: {
      ...node.data,
      [key]: nextPorts,
    },
  };
};

const ensureNodePortVisibleByFlow = (
  node: Node<LcaNodeData>,
  direction: "input" | "output",
  flowUuid: string,
  preferredPortId?: string,
): Node<LcaNodeData> => {
  const key = direction === "input" ? "inputs" : "outputs";
  const ports = node.data[key];
  let targetIndex = -1;
  if (preferredPortId) {
    targetIndex = ports.findIndex((p) => p.id === preferredPortId);
  }
  if (targetIndex < 0) {
    targetIndex = ports.findIndex((p) => p.flowUuid === flowUuid);
  }
  if (targetIndex < 0) {
    return node;
  }
  const nextPorts = ports.map((p, idx) => (idx === targetIndex ? { ...p, showOnNode: true } : p));
  return {
    ...node,
    data: {
      ...node.data,
      [key]: nextPorts,
    },
  };
};

const getFanoutEdges = (canvas: CanvasGraph, sourceNodeId: string, flowUuid: string): Array<Edge<LcaEdgeData>> =>
  canvas.edges.filter((edge) => edge.source === sourceNodeId && edge.data?.flowUuid === flowUuid);

const resolveNodeWidth = (node: Node<LcaNodeData>): number => {
  const width = typeof node.width === "number" && Number.isFinite(node.width) ? node.width : undefined;
  return width ?? 220;
};

const pickShortestAutoHandles = (
  sourceNode: Node<LcaNodeData>,
  sourcePortId: string,
  targetNode: Node<LcaNodeData>,
  targetPortId: string,
): { sourceHandle: string; targetHandle: string } => {
  const sourceLeftX = sourceNode.position.x;
  const sourceRightX = sourceNode.position.x + resolveNodeWidth(sourceNode);
  const targetLeftX = targetNode.position.x;
  const targetRightX = targetNode.position.x + resolveNodeWidth(targetNode);
  const sourceCenterX = (sourceLeftX + sourceRightX) / 2;
  const targetCenterX = (targetLeftX + targetRightX) / 2;
  const centerGap = Math.abs(sourceCenterX - targetCenterX);

  const inwardPenalty = (sourceHandle: string, targetHandle: string): number => {
    if (centerGap < 20) {
      return 0;
    }
    const sourceOnLeft = sourceCenterX < targetCenterX;
    const sourceUsesLeft = sourceHandle.startsWith("outl:");
    const targetUsesRight = targetHandle.startsWith("inr:");
    let penalty = 0;
    if (sourceOnLeft) {
      if (sourceUsesLeft) {
        penalty += 120;
      }
      if (targetUsesRight) {
        penalty += 120;
      }
    } else {
      if (!sourceUsesLeft) {
        penalty += 120;
      }
      if (!targetUsesRight) {
        penalty += 120;
      }
    }
    return penalty;
  };

  const candidates = [
    { sourceHandle: `out:${sourcePortId}`, targetHandle: `in:${targetPortId}`, baseDist: Math.abs(sourceRightX - targetLeftX) },
    { sourceHandle: `out:${sourcePortId}`, targetHandle: `inr:${targetPortId}`, baseDist: Math.abs(sourceRightX - targetRightX) },
    { sourceHandle: `outl:${sourcePortId}`, targetHandle: `in:${targetPortId}`, baseDist: Math.abs(sourceLeftX - targetLeftX) },
    { sourceHandle: `outl:${sourcePortId}`, targetHandle: `inr:${targetPortId}`, baseDist: Math.abs(sourceLeftX - targetRightX) },
  ].map((item) => ({
    ...item,
    dist: item.baseDist + inwardPenalty(item.sourceHandle, item.targetHandle),
  }));

  // Stable tie-break to avoid flicker between equivalent choices.
  candidates.sort((a, b) => {
    if (a.dist !== b.dist) {
      return a.dist - b.dist;
    }
    return a.baseDist - b.baseDist;
  });
  return { sourceHandle: candidates[0].sourceHandle, targetHandle: candidates[0].targetHandle };
};

const shouldOpenBalanceInspectorForEdge = (canvas: CanvasGraph, edge: Edge<LcaEdgeData>): boolean => {
  if (!edge.data) {
    return false;
  }
  if (edge.data.quantityMode === "dual") {
    return true;
  }

  const flowUuid = edge.data.flowUuid;
  if (!flowUuid) {
    return true;
  }
  const sourceNode = canvas.nodes.find((node) => node.id === edge.source);
  if (!sourceNode) {
    return true;
  }

  const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
  const sourcePort =
    sourceNode.data.outputs.find((p) => p.id === sourcePortId) ??
    sourceNode.data.outputs.find((p) => p.flowUuid === flowUuid);
  const outputTotal = sourcePort?.amount ?? edge.data.providerAmount ?? edge.data.amount ?? 0;
  const externalSale = sourcePort?.externalSaleAmount ?? 0;

  const inputTotal = canvas.edges
    .filter((item) => item.source === edge.source && item.data?.flowUuid === flowUuid)
    .reduce((sum, item) => {
      const targetNode = canvas.nodes.find((node) => node.id === item.target);
      const targetPortId = parseHandlePortId(item.targetHandle ?? undefined, "in:");
      const targetPort =
        targetNode?.data.inputs.find((p) => p.id === targetPortId) ??
        targetNode?.data.inputs.find((p) => p.flowUuid === flowUuid);
      return sum + (targetPort?.amount ?? item.data?.consumerAmount ?? item.data?.amount ?? 0);
    }, 0);

  return Math.abs(outputTotal - externalSale - inputTotal) > 1e-9;
};

const shouldAutoOpenBalanceEditorForNewLink = (params: {
  state: LcaGraphState;
  canvas: CanvasGraph;
  edge: Edge<LcaEdgeData>;
  sourceNode: Node<LcaNodeData>;
  didAutoCreateTargetInput: boolean;
}): boolean => {
  const { state, canvas, edge, sourceNode, didAutoCreateTargetInput } = params;
  if (!state.autoPopupEnabled) {
    return false;
  }
  if (didAutoCreateTargetInput) {
    return true;
  }
  if (sourceNode.data.mode === "normalized") {
    return false;
  }
  return shouldOpenBalanceInspectorForEdge(canvas, edge);
};

const rootCanvas: CanvasGraph = {
  id: ROOT_CANVAS_ID,
  name: "Product System",
  kind: "root",
  nodes: [],
  edges: [],
};

export const useLcaGraphStore = create<LcaGraphState>((set, get) => ({
  functionalUnit: "1 kg 对二甲苯",
  uiLanguage: resolveInitialUiLanguage(),
  flowAnimationEnabled: true,
  edgeRoutingStyle: "classic_curve",
  flowAnimationEpoch: Date.now(),
  autoPopupEnabled: true,
  unitAutoScaleEnabled: true,
  canvases: { [ROOT_CANVAS_ID]: rootCanvas },
  activeCanvasId: ROOT_CANVAS_ID,
  activeCanvasKind: "root",
  canvasPath: [rootCanvas.name],
  viewport: { x: 0, y: 0, zoom: 0.82 },
  nodes: [],
  edges: [],
  graphRelations: createEmptyGraphRelations(),
  selection: { nodeIds: [], edgeIds: [] },
  inspectorOpen: false,
  flowBalanceDialog: { open: false },
  ptsPortEditor: { open: false },
  connectionHint: undefined,
  connectionFix: undefined,
  pendingEdges: [],
  deferredBalanceEdgeId: undefined,
  pendingAutoConnect: false,
  pendingPtsCompileNodeId: undefined,
  unitProcessImportDialog: { open: false, targetKind: "unit_process" },
  setUiLanguage: (lang) =>
    set(() => {
      try {
        window.localStorage.setItem(UI_LANGUAGE_STORAGE_KEY, lang);
      } catch {
        // ignore storage access failures
      }
      return { uiLanguage: lang };
    }),
  setFlowAnimationEnabled: (enabled) => set(() => ({ flowAnimationEnabled: enabled })),
  setEdgeRoutingStyle: (style) => set(() => ({ edgeRoutingStyle: style })),
  setAutoPopupEnabled: (enabled) => set(() => ({ autoPopupEnabled: enabled })),
  setUnitAutoScaleEnabled: (enabled) => set(() => ({ unitAutoScaleEnabled: enabled })),
  setViewport: (viewport) =>
    set((state) => {
      const current = state.viewport;
      if (current.x === viewport.x && current.y === viewport.y && current.zoom === viewport.zoom) {
        return state;
      }
      return { viewport };
    }),
  onNodesChange: (changes) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => ({
        ...canvas,
        nodes: applyNodeChanges(changes, canvas.nodes),
      })),
    ),
  onEdgesChange: (changes) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const repaired = repairCanvasEdgeHandles({
          ...canvas,
          edges: applyEdgeChanges(changes, canvas.edges),
        });
        return pruneDetachedMarketInputs(repaired);
      }),
    ),
  onConnect: (connection) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      if (!connection.source || !connection.target) {
        return { connectionHint: RULE_HINTS.missingNode };
      }

      const sourceNode = active.nodes.find((node) => node.id === connection.source);
      const targetNode = active.nodes.find((node) => node.id === connection.target);
      if (!sourceNode || !targetNode) {
        return { connectionHint: RULE_HINTS.missingNode };
      }

      const sourceStartsFromInput = isInputHandleId(connection.sourceHandle);
      const providerNode = sourceStartsFromInput ? targetNode : sourceNode;
      const consumerNode = sourceStartsFromInput ? sourceNode : targetNode;

      const hint = getConnectionHint(providerNode, consumerNode);
      if (hint) {
        return { connectionHint: hint };
      }

      let nextProviderNode: Node<LcaNodeData> = providerNode;
      let nextConsumerNode: Node<LcaNodeData> = consumerNode;
      let didAutoCreateTargetInput = false;
      let autoPromotedInputAsProduct = false;

      let providerPort = parseOutputPort(
        providerNode,
        sourceStartsFromInput ? connection.targetHandle : connection.sourceHandle,
      );

      if (sourceStartsFromInput && !providerPort) {
        const consumerInputPort = parseInputPort(sourceNode, connection.sourceHandle);
        if (consumerInputPort) {
          if (shouldEnforceMarketSingleFlow(providerNode)) {
            const marketInput = providerNode.data.inputs[0];
            const marketOutput = providerNode.data.outputs[0];
            if (
              (marketInput && marketInput.flowUuid !== consumerInputPort.flowUuid) ||
              (marketOutput && marketOutput.flowUuid !== consumerInputPort.flowUuid)
            ) {
              return { connectionHint: RULE_HINTS.marketSingleFlowOnly };
            }
          }

          const existingOut = providerNode.data.outputs.find((p) => p.flowUuid === consumerInputPort.flowUuid);
          if (existingOut) {
            nextProviderNode = ensureNodePortVisibleByFlow(providerNode, "output", consumerInputPort.flowUuid, existingOut.id);
            providerPort =
              nextProviderNode.data.outputs.find((p) => p.id === existingOut.id) ??
              nextProviderNode.data.outputs.find((p) => p.flowUuid === consumerInputPort.flowUuid);
          } else {
            const fallbackAmount = Number.isFinite(consumerInputPort.amount) ? consumerInputPort.amount : 1;
            const autoOut: FlowPort = {
              ...buildAutoOutputPortFromInput(consumerInputPort),
              amount: fallbackAmount,
              isProduct: !Boolean(consumerInputPort.isProduct),
              externalSaleAmount: 0,
            };
            nextProviderNode = {
              ...providerNode,
              data: {
                ...providerNode.data,
                outputs: [...providerNode.data.outputs, autoOut],
                ...(isLciNode(providerNode) ? { lciRole: "provider" as const } : {}),
              },
            };
            providerPort = autoOut;
          }
        }
      }

      if (!providerPort) {
        return { connectionHint: RULE_HINTS.noVisibleOutput };
      }

      if (!isUuidLike(providerPort.flowUuid)) {
        return { connectionHint: RULE_HINTS.invalidFlowUuid };
      }
      if (isMarketProcessNode(nextConsumerNode) && !isProductIntermediate(providerPort)) {
        return { connectionHint: RULE_HINTS.marketInputRequiresProductSource };
      }

      const explicitConsumerHandle = sourceStartsFromInput ? connection.sourceHandle : connection.targetHandle;
      const explicitPortId = isInputHandleId(explicitConsumerHandle)
        ? parseHandlePortId(explicitConsumerHandle, "in:")
        : undefined;
      const explicitPort = explicitPortId ? nextConsumerNode.data.inputs.find((p) => p.id === explicitPortId) : undefined;
      const explicitOccupiedByAnyEdge =
        Boolean(explicitPortId) &&
        active.edges.some(
          (edge) =>
            edge.target === nextConsumerNode.id && parseHandlePortId(edge.targetHandle ?? undefined, "in:") === explicitPortId,
        );
      const explicitOccupied = isMarketProcessNode(nextConsumerNode)
        ? explicitOccupiedByAnyEdge
        : Boolean(explicitPortId) &&
          active.edges.some(
            (edge) =>
              edge.target === nextConsumerNode.id &&
              parseHandlePortId(edge.targetHandle ?? undefined, "in:") === explicitPortId &&
              edge.source !== nextProviderNode.id,
          );
      let shouldOpenBalanceEditor = true;

      const forceCreateMarketInputRow =
        isMarketProcessNode(nextConsumerNode) &&
        (!isInputHandleId(explicitConsumerHandle) || (Boolean(explicitPortId) && explicitOccupied));
      let targetHandle: string | undefined;
      if (isInputHandleId(explicitConsumerHandle)) {
        if (explicitPort?.flowUuid === providerPort.flowUuid && !(isMarketProcessNode(nextConsumerNode) && explicitOccupied)) {
          targetHandle = explicitConsumerHandle ?? undefined;
          nextConsumerNode = ensureNodePortVisibleByFlow(nextConsumerNode, "input", providerPort.flowUuid, explicitPort.id);
        }
      }
      if (!targetHandle && !forceCreateMarketInputRow) {
        targetHandle = getTargetHandle(nextConsumerNode, providerPort.flowUuid, active.edges);
        if (targetHandle) {
          const matchedPortId = parseHandlePortId(targetHandle, "in:");
          nextConsumerNode = ensureNodePortVisibleByFlow(nextConsumerNode, "input", providerPort.flowUuid, matchedPortId);
        }
      }
      if (shouldEnforceMarketSingleFlow(nextConsumerNode)) {
        const marketInput = nextConsumerNode.data.inputs[0];
        const marketOutput = nextConsumerNode.data.outputs[0];
        if (marketInput && marketInput.flowUuid !== providerPort.flowUuid) {
          return {
            connectionHint: RULE_HINTS.marketSingleFlowOnly,
          };
        }
        if (marketOutput && marketOutput.flowUuid !== providerPort.flowUuid) {
          return {
            connectionHint: RULE_HINTS.marketSingleFlowOnly,
          };
        }
      }
      if (!targetHandle) {
        const sourceHasSameFlowLinks = active.edges.some(
          (edge) =>
            (edge.source === nextProviderNode.id || edge.target === nextProviderNode.id) &&
            edge.data?.flowUuid === providerPort.flowUuid,
        );
        const consumerMarketOutput = isMarketProcessNode(nextConsumerNode) ? nextConsumerNode.data.outputs[0] : undefined;
        const autoPort: FlowPort = {
          ...buildAutoInputPort(providerPort),
          unit: consumerMarketOutput?.unit ?? providerPort.unit,
          unitGroup: consumerMarketOutput?.unitGroup ?? providerPort.unitGroup,
          amount: sourceHasSameFlowLinks ? 0 : (providerPort.amount ?? 0),
          isProduct: !Boolean(providerPort.isProduct),
        };
        const nextAutoPort = isMarketProcessNode(nextConsumerNode)
          ? withMarketInputSourceIdentity(autoPort, {
              nodeId: nextProviderNode.id,
              processUuid: nextProviderNode.data.processUuid,
              processName: nextProviderNode.data.name,
              portName: providerPort.name,
              nestedProcessName: providerPort.sourceProcessName,
            })
          : autoPort;
        autoPromotedInputAsProduct = !Boolean(providerPort.isProduct) && Boolean(autoPort.isProduct);
        nextConsumerNode = {
          ...nextConsumerNode,
          data: {
            ...nextConsumerNode.data,
            inputs: [...nextConsumerNode.data.inputs, nextAutoPort],
            ...(isLciNode(nextConsumerNode) ? { lciRole: "waste_sink" as const } : {}),
          },
        };
        if (isMarketProcessNode(nextConsumerNode) && nextConsumerNode.data.outputs.length === 0) {
        const autoOut: FlowPort = {
          ...buildAutoOutputPortFromInput(nextAutoPort),
          name: stripMarketSourceSuffix(providerPort.name),
          flowNameEn: providerPort.flowNameEn,
          amount: 1,
          isProduct: true,
          externalSaleAmount: 0,
          };
          nextConsumerNode = {
            ...nextConsumerNode,
            data: {
              ...nextConsumerNode.data,
              outputs: [autoOut],
              referenceProduct: autoOut.name,
            },
          };
        }
        targetHandle = `in:${nextAutoPort.id}`;
        didAutoCreateTargetInput = true;
      }
      if (!hasInputHandleId(nextConsumerNode, targetHandle)) {
        const fallbackHandle = getTargetHandle(nextConsumerNode, providerPort.flowUuid, active.edges);
        if (hasInputHandleId(nextConsumerNode, fallbackHandle)) {
          targetHandle = fallbackHandle;
        } else {
          const firstInput =
            nextConsumerNode.data.inputs.find((p) => p.flowUuid === providerPort.flowUuid) ?? nextConsumerNode.data.inputs[0];
          targetHandle = firstInput ? `in:${firstInput.id}` : undefined;
        }
      }
      if (!targetHandle || !hasInputHandleId(nextConsumerNode, targetHandle)) {
        return { connectionHint: RULE_HINTS.targetNoMatchingInputCanceled };
      }

      const targetPortId = parseHandlePortId(targetHandle, "in:");
      const targetPort =
        (targetPortId && nextConsumerNode.data.inputs.find((p) => p.id === targetPortId && p.flowUuid === providerPort.flowUuid)) ||
        nextConsumerNode.data.inputs.find((p) => p.flowUuid === providerPort.flowUuid);
      if (!targetPort) {
        return { connectionHint: RULE_HINTS.targetNoMatchingInputCanceled };
      }
      if (isMarketProcessNode(nextConsumerNode) && targetPortId) {
        nextConsumerNode = {
          ...nextConsumerNode,
          data: {
            ...nextConsumerNode.data,
            inputs: nextConsumerNode.data.inputs.map((port) =>
              port.id === targetPortId
                  ? withMarketInputSourceIdentity(port, {
                      nodeId: nextProviderNode.id,
                      processUuid: nextProviderNode.data.processUuid,
                      processName: nextProviderNode.data.name,
                      portName: providerPort.name,
                      nestedProcessName: providerPort.sourceProcessName,
                    })
                  : port,
            ),
          },
        };
      }

      const existingAtSameInputPort = active.edges.find(
        (edge) =>
          edge.target === nextConsumerNode.id &&
          parseHandlePortId(edge.targetHandle ?? undefined, "in:") === targetPort.id &&
          edge.source !== nextProviderNode.id,
      );

      const incomingSameFlowToTarget = active.edges.filter(
        (edge) =>
          edge.target === nextConsumerNode.id &&
          edge.data?.flowUuid === providerPort.flowUuid &&
          edge.source !== nextProviderNode.id,
      );
      const outgoingSameFlowFromProvider = active.edges.filter(
        (edge) =>
          edge.source === nextProviderNode.id &&
          edge.data?.flowUuid === providerPort.flowUuid &&
          edge.target !== nextConsumerNode.id,
      );
      const relationHint = getRelationViolationHint({
        sourcePort: providerPort,
        targetPort,
        targetNode: nextConsumerNode,
        existingIncomingFromOtherSources: incomingSameFlowToTarget.length,
        existingOutgoingToOtherTargets: outgoingSameFlowFromProvider.length,
        targetInputOccupiedByOtherSource: Boolean(existingAtSameInputPort),
      });
      if (relationHint) {
        return { connectionHint: relationHint };
      }

      if (isLciNode(nextProviderNode)) {
        nextProviderNode = {
          ...nextProviderNode,
          data: {
            ...nextProviderNode.data,
            lciRole: inferLciRoleByIntermediates(nextProviderNode),
          },
        };
      }
      if (isLciNode(nextConsumerNode)) {
        nextConsumerNode = {
          ...nextConsumerNode,
          data: {
            ...nextConsumerNode.data,
            lciRole: inferLciRoleByIntermediates(nextConsumerNode),
          },
        };
      }

      const quantityMode = resolveQuantityMode(nextProviderNode, nextConsumerNode);
      const amount = providerPort.amount ?? 0;
      const consumerAmount =
        isMarketProcessNode(nextConsumerNode) && nextConsumerNode.data.mode === "normalized" && quantityMode === "dual"
          ? resolveTargetInputAmount(nextConsumerNode, targetHandle, providerPort.flowUuid, amount)
          : amount;
      const existing = active.edges.find(
        (edge) =>
          edge.source === nextProviderNode.id &&
          edge.target === nextConsumerNode.id &&
          edge.data?.flowUuid === providerPort.flowUuid,
      );

      const edge: Edge<LcaEdgeData> = {
        id: existing?.id ?? `edge_${uid()}`,
        source: nextProviderNode.id,
        target: nextConsumerNode.id,
        sourceHandle:
          (sourceStartsFromInput ? connection.targetHandle : connection.sourceHandle) ?? `out:${providerPort.id}`,
        targetHandle,
        type: "lcaExchange",
        data: {
          flowUuid: providerPort.flowUuid,
          flowName: providerPort.name,
          quantityMode,
          amount: consumerAmount,
          providerAmount: amount,
          consumerAmount,
          unit: providerPort.unit,
          type: providerPort.type,
          allocation: "physical",
          dbMapping: "",
        },
      };

      const next = updateActiveCanvas(state, (canvas) => {
        const nextNodes = canvas.nodes.map((node) => {
          if (node.id === nextProviderNode.id) {
            return nextProviderNode;
          }
          if (node.id === nextConsumerNode.id) {
            return nextConsumerNode;
          }
          return node;
        });
        const nextEdgesPreview = existing
          ? canvas.edges.map((item) => (item.id === existing.id ? edge : item))
          : [...canvas.edges, edge];
        shouldOpenBalanceEditor = shouldAutoOpenBalanceEditorForNewLink({
          state,
          sourceNode: nextProviderNode,
          didAutoCreateTargetInput,
          canvas: {
            ...canvas,
            nodes: nextNodes,
            edges: nextEdgesPreview,
          },
          edge,
        });
        const nextCanvas = {
          ...canvas,
          nodes: nextNodes,
        };
        return repairCanvasEdgeHandles(nextCanvas);
      });

      return {
        ...next,
        pendingEdges: enqueuePendingEdge(next.pendingEdges ?? state.pendingEdges, {
          canvasId: state.activeCanvasId,
          edge,
          openBalanceEditor: shouldOpenBalanceEditor,
        }),
        flowBalanceDialog: { open: false },
        connectionHint: autoPromotedInputAsProduct
          ? "警告：输入端自动新增的中间流被定义为产品，请检查该过程产品定义。"
          : undefined,
        connectionFix: undefined,
      };
    }),
  flushPendingEdges: () =>
    set((state) => {
      if (state.pendingEdges.length === 0) {
        return state;
      }
      let nextCanvases = state.canvases;
      const nextPending: PendingEdgeItem[] = [];
      let deferredBalanceEdgeId: string | undefined;

      for (const pending of state.pendingEdges) {
        const canvas = nextCanvases[pending.canvasId];
        if (!canvas) {
          continue;
        }
        const attached = attachEdgeToCanvas(canvas, pending.edge);
        if (attached.attached) {
          nextCanvases = {
            ...nextCanvases,
            [pending.canvasId]: attached.canvas,
          };
          if (pending.openBalanceEditor && pending.canvasId === state.activeCanvasId) {
            deferredBalanceEdgeId = pending.edge.id;
          }
          continue;
        }
        if (pending.retries < 2) {
          nextPending.push({
            ...pending,
            retries: pending.retries + 1,
          });
        }
      }

      const next = setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
      return {
        ...next,
        pendingEdges: nextPending,
        deferredBalanceEdgeId: deferredBalanceEdgeId ?? state.deferredBalanceEdgeId,
      };
    }),
  applyHandleValidationIssues: (issues) =>
    set((state) => {
      if (!issues.length) {
        return state;
      }
      const patchByEdgeId = new Map(
        issues
          .filter((item) => item.edge_id)
          .map((item) => [
            String(item.edge_id),
            {
              sourcePortId: item.suggested_source_port_id ? String(item.suggested_source_port_id) : undefined,
              targetPortId: item.suggested_target_port_id ? String(item.suggested_target_port_id) : undefined,
            },
          ]),
      );
      if (patchByEdgeId.size === 0) {
        return state;
      }

      const nextCanvases: Record<string, CanvasGraph> = {};
      for (const [canvasId, canvas] of Object.entries(state.canvases)) {
        const nextEdges = canvas.edges.map((edge) => {
          const patch = patchByEdgeId.get(edge.id);
          if (!patch) {
            return edge;
          }
          return {
            ...edge,
            sourceHandle: patch.sourcePortId
              ? buildHandleWithSameSide(edge.sourceHandle, "out:", patch.sourcePortId)
              : edge.sourceHandle,
            targetHandle: patch.targetPortId
              ? buildHandleWithSameSide(edge.targetHandle, "in:", patch.targetPortId)
              : edge.targetHandle,
          };
        });
        nextCanvases[canvasId] = repairCanvasEdgeHandles({
          ...canvas,
          edges: nextEdges,
        });
      }
      return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
    }),
  rebindRootEdgeHandles: () =>
    set((state) => {
      const rootCanvas = state.canvases[ROOT_CANVAS_ID];
      if (!rootCanvas) {
        return state;
      }
      const repairedRoot = repairCanvasEdgeHandles(rootCanvas);
      const nextCanvases = {
        ...state.canvases,
        [ROOT_CANVAS_ID]: repairedRoot,
      };
      return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
    }),
  rebindRootEdgeHandlesForNode: (nodeId) =>
    set((state) => {
      const rootCanvas = state.canvases[ROOT_CANVAS_ID];
      if (!rootCanvas) {
        return state;
      }
      const repairedRoot = repairCanvasEdgeHandlesForNode(rootCanvas, nodeId);
      const nextCanvases = {
        ...state.canvases,
        [ROOT_CANVAS_ID]: repairedRoot,
      };
      return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
    }),
  rebindRootEdgeHandlesForPtsShell: (nodeId, ports, portIdMap) =>
    set((state) => {
      const rootCanvas = state.canvases[ROOT_CANVAS_ID];
      if (!rootCanvas) {
        return state;
      }
      const currentNode = rootCanvas.nodes.find((node) => node.id === nodeId && node.data.nodeKind === "pts_module");
      const outputPorts = (ports.outputs ?? []).filter((port) => Boolean(port?.id));
      const inputPorts = (ports.inputs ?? []).filter((port) => Boolean(port?.id));
      const currentOutputs = currentNode?.data.outputs ?? [];
      const currentInputs = currentNode?.data.inputs ?? [];
      const outputByLegacy = new Map(
        outputPorts
          .map((port) => [String(port.legacyPortId ?? "").trim(), String(port.id)] as const)
          .filter(([legacyId]) => legacyId.length > 0),
      );
      const inputByLegacy = new Map(
        inputPorts
          .map((port) => [String(port.legacyPortId ?? "").trim(), String(port.id)] as const)
          .filter(([legacyId]) => legacyId.length > 0),
      );
        const outputByIdentity = new Map<string, string>();
        for (const port of outputPorts) {
          const portId = String(port.id ?? "").trim();
          if (!portId) {
            continue;
          }
          const keys = [
            getPtsPortIdentityKey("output", port),
            getPtsPortSourceIdentityKey("output", port),
            `${String(port.flowUuid ?? "").trim()}@@${String(port.sourceProcessUuid ?? "").trim()}`,
            `${String(port.flowUuid ?? "").trim()}@@node:${String(port.sourceNodeId ?? "").trim()}`,
            `${String(port.flowUuid ?? "").trim()}@@name:${String(port.sourceProcessName ?? "").trim()}`,
          ]
            .map((item) => String(item ?? "").trim())
            .filter((item) => item.length > 0);
          keys.forEach((key) => {
            if (!outputByIdentity.has(key)) {
              outputByIdentity.set(key, portId);
            }
          });
        }
        const inputByIdentity = new Map<string, string>();
        for (const port of inputPorts) {
          const portId = String(port.id ?? "").trim();
          if (!portId) {
            continue;
          }
          const keys = [getPtsPortIdentityKey("input", port), getPtsPortSourceIdentityKey("input", port)]
            .map((item) => String(item ?? "").trim())
            .filter((item) => item.length > 0);
          keys.forEach((key) => {
            if (!inputByIdentity.has(key)) {
              inputByIdentity.set(key, portId);
            }
          });
        }
      const outputByFlow = new Map<string, string>();
      for (const port of outputPorts) {
        const flowUuid = String(port.flowUuid ?? "").trim();
        if (!flowUuid || outputByFlow.has(flowUuid)) {
          continue;
        }
        if (outputPorts.filter((item) => String(item.flowUuid ?? "").trim() === flowUuid).length === 1) {
          outputByFlow.set(flowUuid, String(port.id));
        }
      }
        const inputByFlow = new Map<string, string>();
        for (const port of inputPorts) {
          const flowUuid = String(port.flowUuid ?? "").trim();
          if (!flowUuid || inputByFlow.has(flowUuid)) {
            continue;
        }
        if (inputPorts.filter((item) => String(item.flowUuid ?? "").trim() === flowUuid).length === 1) {
            inputByFlow.set(flowUuid, String(port.id));
          }
        }
        const nodeById = new Map(rootCanvas.nodes.map((node) => [node.id, node]));
        const nextEdges = rootCanvas.edges.map((edge) => {
          const flowUuid = String(edge.data?.flowUuid ?? "").trim();
          if (!flowUuid) {
            return edge;
          }
        if (edge.source === nodeId) {
          const currentPortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
          const currentPort =
            (currentPortId ? currentOutputs.find((port) => String(port.id ?? "") === currentPortId) : undefined) ??
            (currentPortId
              ? currentOutputs.find((port) => String(port.legacyPortId ?? "").trim() === currentPortId)
              : undefined);
          const identityKey = currentPort ? getPtsPortIdentityKey("output", currentPort) : "";
            const portId =
              (currentPortId ? String(portIdMap?.[currentPortId] ?? "").trim() : "") ||
              (currentPortId ? outputByLegacy.get(currentPortId) : undefined) ||
              (identityKey ? outputByIdentity.get(identityKey) : undefined) ||
              outputByFlow.get(flowUuid);
            const targetNode = nodeById.get(edge.target);
            const targetPortId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
            const targetPort =
              targetNode && targetPortId
                ? targetNode.data.inputs.find(
                    (port) =>
                      (port.id === targetPortId || String(port.legacyPortId ?? "").trim() === targetPortId) &&
                      port.flowUuid === flowUuid,
                  )
                : undefined;
            const nestedSourceName = String(targetPort?.nestedSourceProcessName ?? "").trim();
            const sourceNameFallback = String(targetPort?.sourceProcessName ?? "").trim();
            const inferredOutputPort =
              !portId && targetNode && isMarketProcessNode(targetNode)
                ? outputPorts.find((port) => {
                    if (String(port.flowUuid ?? "").trim() !== flowUuid) {
                      return false;
                    }
                    const sourceProcessName = String(port.sourceProcessName ?? "").trim();
                    if (nestedSourceName) {
                      return sourceProcessName === nestedSourceName;
                    }
                    return sourceNameFallback ? sourceProcessName === sourceNameFallback : false;
                  })
                : undefined;
            const reboundPortId = portId || String(inferredOutputPort?.id ?? "").trim();
            if (reboundPortId) {
              return {
                ...edge,
                sourceHandle: buildHandleWithSameSide(edge.sourceHandle, "out:", reboundPortId),
              };
            }
          }
        if (edge.target === nodeId) {
          const currentPortId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
          const currentPort =
            (currentPortId ? currentInputs.find((port) => String(port.id ?? "") === currentPortId) : undefined) ??
            (currentPortId
              ? currentInputs.find((port) => String(port.legacyPortId ?? "").trim() === currentPortId)
              : undefined);
          const identityKey = currentPort ? getPtsPortIdentityKey("input", currentPort) : "";
          const portId =
            (currentPortId ? String(portIdMap?.[currentPortId] ?? "").trim() : "") ||
            (currentPortId ? inputByLegacy.get(currentPortId) : undefined) ||
            (identityKey ? inputByIdentity.get(identityKey) : undefined) ||
            inputByFlow.get(flowUuid);
          if (portId) {
            return {
              ...edge,
              targetHandle: buildHandleWithSameSide(edge.targetHandle, "in:", portId),
            };
          }
        }
        return edge;
      });
      const nextCanvases = {
        ...state.canvases,
          [ROOT_CANVAS_ID]: repairCanvasEdgeHandles(
            {
              ...rootCanvas,
              edges: nextEdges,
            },
            {
              preservePtsProjectionNodeIds: new Set([nodeId]),
            },
          ),
        };
        return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
      }),
  consumeDeferredBalanceEdge: () =>
    set((state) =>
      state.deferredBalanceEdgeId
        ? {
            deferredBalanceEdgeId: undefined,
          }
        : state,
    ),
  setSelection: (selection) =>
    set((state) => {
      const current = state.selection;
      const nextIsEmpty =
        selection.nodeIds.length === 0 &&
        selection.edgeIds.length === 0 &&
        !selection.nodeId &&
        !selection.edgeId;
      if (nextIsEmpty && state.inspectorOpen && (Boolean(current.edgeId) || Boolean(current.nodeId))) {
        return state;
      }
      if (
        current.nodeId === selection.nodeId &&
        current.edgeId === selection.edgeId &&
        current.nodeIds.join(",") === selection.nodeIds.join(",") &&
        current.edgeIds.join(",") === selection.edgeIds.join(",")
      ) {
        return state;
      }
      return { selection };
    }),
  openNodeInspector: (nodeId) =>
    set(() => ({
      selection: { nodeIds: [nodeId], edgeIds: [], nodeId, edgeId: undefined },
      inspectorOpen: true,
      flowBalanceDialog: { open: false },
    })),
  openEdgeInspector: (edgeId) =>
    set(() => ({
      selection: { nodeIds: [], edgeIds: [edgeId], nodeId: undefined, edgeId },
      inspectorOpen: true,
      flowBalanceDialog: { open: false },
    })),
  openFlowBalanceDialogForEdge: (edgeId) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const edge = active?.edges.find((item) => item.id === edgeId);
      const flowUuid = String(edge?.data?.flowUuid ?? "").trim();
      const sourceNodeId = String(edge?.source ?? "").trim();
      if (!edge || !flowUuid || !sourceNodeId) {
        return state;
      }
      return {
        selection: { nodeIds: [], edgeIds: [edgeId], edgeId },
        inspectorOpen: false,
        flowBalanceDialog: {
          open: true,
          edgeId,
          sourceNodeId,
          flowUuid,
        },
      };
    }),
  closeInspector: (options) =>
    set((state) => {
      const nodeTargetId = state.selection.nodeId ?? state.selection.nodeIds[0];
      const node = nodeTargetId ? state.nodes.find((item) => item.id === nodeTargetId) : undefined;
      if (
        options?.requireProductConfirm &&
        node &&
        node.data.nodeKind === "unit_process" &&
        !isMarketProcessNode(node) &&
        !hasIntermediateProductPort(node)
      ) {
        return {
          inspectorOpen: false,
          connectionHint: "警告：当前过程未定义产品，已关闭清单分析，可稍后继续补充。",
          connectionFix: undefined,
        };
      }
      return {
        inspectorOpen: false,
      };
    }),
  closeFlowBalanceDialog: () =>
    set(() => ({
      flowBalanceDialog: { open: false },
    })),
  openPtsPortEditor: (nodeId) =>
    set((state) => {
      let targetNodeId = nodeId;
      if (!targetNodeId) {
        if (state.activeCanvasKind === "pts_internal") {
          const active = state.canvases[state.activeCanvasId];
          targetNodeId = active.parentPtsNodeId;
        } else {
          const selectedNodeId = state.selection.nodeIds[0];
          const selectedNode = state.nodes.find((n) => n.id === selectedNodeId);
          if (selectedNode?.data.nodeKind === "pts_module") {
            targetNodeId = selectedNode.id;
          }
        }
      }
      if (!targetNodeId) {
        return { connectionHint: "请选择一个 PTS 节点后再编辑外部端口。" };
      }
      const root = state.canvases[ROOT_CANVAS_ID];
      const ptsNode = root.nodes.find((n) => n.id === targetNodeId && n.data.nodeKind === "pts_module");
      if (!ptsNode) {
        return { connectionHint: "未找到对应 PTS 节点，无法编辑外部端口。" };
      }
      return { ptsPortEditor: { open: true, ptsNodeId: targetNodeId }, connectionHint: undefined };
    }),
  closePtsPortEditor: () =>
    set(() => ({
      ptsPortEditor: { open: false },
    })),
  syncPtsPortsFromInternal: (_ptsNodeId) =>
    set((state) => state),
  setPtsPortExposureByFlow: (ptsNodeId, direction, flow, enabled) =>
    set((state) => {
      const root = state.canvases[ROOT_CANVAS_ID];
      const nextRoot: CanvasGraph = {
        ...root,
        nodes: root.nodes.map((node) => {
          if (node.id !== ptsNodeId || node.data.nodeKind !== "pts_module") {
            return node;
          }
          const list = direction === "input" ? node.data.inputs : node.data.outputs;
          const existing = list.find((p) => isSamePtsPortIdentity(direction, p, flow));
          let nextList: FlowPort[];
          if (existing) {
            nextList = list.map((p) =>
              isSamePtsPortIdentity(direction, p, flow)
                ? { ...p, internalExposed: enabled, showOnNode: enabled ? true : false }
                : p,
            );
          } else if (enabled) {
            nextList = [
              ...list,
              {
                id: `${direction}_${uid().slice(0, 8)}`,
                flowUuid: flow.flowUuid,
                name: flow.name,
                portKey: flow.portKey,
                productKey: flow.productKey,
                sourceProcessUuid: flow.sourceProcessUuid,
                sourceProcessName: flow.sourceProcessName,
                sourceNodeId: flow.sourceNodeId,
                exposureMode: flow.exposureMode ?? "boundary_only",
                unit: flow.unit || "kg",
                unitGroup: flow.unitGroup,
                amount: 0,
                isProduct: Boolean(flow.isProduct) && direction === "output",
                externalSaleAmount: 0,
                type: flow.type,
                direction,
                showOnNode: true,
                internalExposed: true,
              },
            ];
          } else {
            nextList = list;
          }
          return {
            ...node,
            data: {
              ...node.data,
              inputs: direction === "input" ? nextList : node.data.inputs,
              outputs: direction === "output" ? nextList : node.data.outputs,
            },
          };
        }),
      };
      const nextCanvases = Object.fromEntries(
        Object.entries(state.canvases).map(([canvasId, canvas]) => {
          if (canvasId === ROOT_CANVAS_ID) {
            return [canvasId, nextRoot];
          }
          if (canvas.kind !== "pts_internal" || canvas.parentPtsNodeId !== ptsNodeId) {
            return [canvasId, canvas];
          }
          const nextCanvas: CanvasGraph = {
            ...canvas,
            nodes: canvas.nodes.map((node) => {
              if (direction === "output" && flow.sourceNodeId && node.id !== flow.sourceNodeId) {
                return node;
              }
              const key = direction === "input" ? "inputs" : "outputs";
              const nextPorts = node.data[key].map((port) => {
                const sameFlow = String(port.flowUuid ?? "").trim() === String(flow.flowUuid ?? "").trim();
                if (!sameFlow) {
                  return port;
                }
                if (direction === "output" && flow.portKey && String(port.portKey ?? "").trim() && port.portKey !== flow.portKey) {
                  return port;
                }
                if (
                  direction === "output" &&
                  flow.productKey &&
                  String(port.productKey ?? "").trim() &&
                  port.productKey !== flow.productKey
                ) {
                  return port;
                }
                return {
                  ...port,
                  internalExposed: enabled,
                };
              });
              return {
                ...node,
                data: {
                  ...node.data,
                  [key]: nextPorts,
                },
              };
            }),
          };
          return [canvasId, nextCanvas];
        }),
      ) as Record<string, CanvasGraph>;
      return {
        ...setActiveCanvas(state, nextCanvases, state.activeCanvasId, true),
      };
    }),
  setPtsPortExposureMode: (ptsNodeId, portId, mode) =>
    set((state) => {
      const root = state.canvases[ROOT_CANVAS_ID];
      const nextRoot: CanvasGraph = {
        ...root,
        nodes: root.nodes.map((node) => {
          if (node.id !== ptsNodeId || node.data.nodeKind !== "pts_module") {
            return node;
          }
          return {
            ...node,
            data: {
              ...node.data,
              outputs: node.data.outputs.map((port) =>
                port.id === portId
                  ? {
                      ...port,
                      exposureMode: mode,
                    }
                  : port,
              ),
            },
          };
        }),
      };
      const nextCanvases = {
        ...state.canvases,
        [ROOT_CANVAS_ID]: nextRoot,
      };
      if (state.activeCanvasId === ROOT_CANVAS_ID) {
        return {
          ...setActiveCanvas(state, nextCanvases, ROOT_CANVAS_ID, true),
        };
      }
      return {
        canvases: nextCanvases,
      };
    }),
  setPtsPortVisibility: (ptsNodeId, direction, portId, visible) =>
    set((state) => {
      const root = state.canvases[ROOT_CANVAS_ID];
      const nextRoot: CanvasGraph = {
        ...root,
        nodes: root.nodes.map((node) => {
          if (node.id !== ptsNodeId || node.data.nodeKind !== "pts_module") {
            return node;
          }
          const patchPorts = (ports: FlowPort[]) =>
            ports.map((p) => {
              if (p.id !== portId) {
                return p;
              }
              if (p.internalExposed === false && visible) {
                return p;
              }
              return { ...p, showOnNode: visible };
            });
          return {
            ...node,
            data: {
              ...node.data,
              inputs: direction === "input" ? patchPorts(node.data.inputs) : node.data.inputs,
              outputs: direction === "output" ? patchPorts(node.data.outputs) : node.data.outputs,
            },
          };
        }),
      };
      const nextCanvases = {
        ...state.canvases,
        [ROOT_CANVAS_ID]: nextRoot,
      };
      if (state.activeCanvasId === ROOT_CANVAS_ID) {
        return {
          ...setActiveCanvas(state, nextCanvases, ROOT_CANVAS_ID, true),
        };
      }
      return {
        canvases: nextCanvases,
      };
    }),
  setFlowBalanceTotal: (sourceNodeId, flowUuid, nextTotal, externalSaleAmount = 0, preserveInputs = false) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const group = getFanoutEdges(canvas, sourceNodeId, flowUuid);
        if (group.length === 0) {
          return canvas;
        }
        const nextInternalTotal = nextTotal - externalSaleAmount;
        const oldTotal = group.reduce((sum, edge) => sum + (edge.data?.amount ?? 0), 0);
        const count = group.length;
        const nextAmountById = new Map<string, number>();
        for (const edge of group) {
          if (preserveInputs) {
            nextAmountById.set(edge.id, edge.data?.amount ?? 0);
          } else {
            const oldAmount = edge.data?.amount ?? 0;
            const nextAmount = oldTotal > 0 ? (oldAmount / oldTotal) * nextInternalTotal : nextInternalTotal / count;
            nextAmountById.set(edge.id, nextAmount);
          }
        }

        const nextEdges = preserveInputs
          ? canvas.edges
          : canvas.edges.map((edge) => {
              const patched = nextAmountById.get(edge.id);
              if (patched === undefined) {
                return edge;
              }
              const srcNode = canvas.nodes.find((n) => n.id === edge.source);
              const dstNode = canvas.nodes.find((n) => n.id === edge.target);
              const balancedPair = isBalancedUnitNode(srcNode) && isBalancedUnitNode(dstNode);
              return {
                ...edge,
                data: {
                  ...edge.data,
                  quantityMode: balancedPair ? "single" : edge.data?.quantityMode ?? "dual",
                  amount: patched,
                  providerAmount: patched,
                  consumerAmount: patched,
                } as LcaEdgeData,
              };
            });
        const nextNodes = canvas.nodes.map((node) => {
          if (node.id === sourceNodeId) {
            const sourcePortId = parseHandlePortId(group[0].sourceHandle ?? undefined, "out:");
            return updateNodeOutputExternalSaleByFlow(
              updateNodePortAmountByFlow(node, "output", flowUuid, nextTotal, sourcePortId),
              flowUuid,
              externalSaleAmount,
              sourcePortId,
            );
          }
          if (preserveInputs) {
            return node;
          }
          const incoming = group.filter((e) => e.target === node.id);
          if (incoming.length === 0) {
            return node;
          }
          const first = incoming[0];
          const amount = nextAmountById.get(first.id) ?? first.data?.amount ?? 0;
          const targetPortId = parseHandlePortId(first.targetHandle ?? undefined, "in:");
          return updateNodePortAmountByFlow(node, "input", flowUuid, amount, targetPortId);
        });
        return {
          ...canvas,
          nodes: nextNodes,
          edges: nextEdges,
        };
      }),
    ),
  setFlowBalanceEdgeAmount: (edgeId, nextAmount) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const changedEdge = canvas.edges.find((e) => e.id === edgeId);
        if (!changedEdge?.data?.flowUuid) {
          return canvas;
        }
        const group = getFanoutEdges(canvas, changedEdge.source, changedEdge.data.flowUuid);
        if (group.length === 0) {
          return canvas;
        }
        const nextEdges = canvas.edges.map((edge) =>
          edge.id === edgeId
            ? {
                ...edge,
                data: {
                  ...edge.data,
                  quantityMode:
                    isBalancedUnitNode(canvas.nodes.find((n) => n.id === edge.source)) &&
                    isBalancedUnitNode(canvas.nodes.find((n) => n.id === edge.target))
                      ? "single"
                      : edge.data?.quantityMode ?? "dual",
                  amount: nextAmount,
                  providerAmount: nextAmount,
                  consumerAmount: nextAmount,
                } as LcaEdgeData,
              }
            : edge,
        );
        const nextTotal = group.reduce(
          (sum, edge) => sum + (edge.id === edgeId ? nextAmount : edge.data?.amount ?? 0),
          0,
        );
        const sourcePortId = parseHandlePortId(changedEdge.sourceHandle ?? undefined, "out:");
        const targetPortId = parseHandlePortId(changedEdge.targetHandle ?? undefined, "in:");
        const sourceNode = canvas.nodes.find((node) => node.id === changedEdge.source);
        const sourcePort =
          sourceNode?.data.outputs.find((p) => p.id === sourcePortId) ??
          sourceNode?.data.outputs.find((p) => p.flowUuid === (changedEdge.data?.flowUuid ?? ""));
        const externalSaleAmount = sourcePort?.externalSaleAmount ?? 0;
        const nextNodes = canvas.nodes.map((node) => {
          if (node.id === changedEdge.source) {
            return updateNodePortAmountByFlow(
              node,
              "output",
              changedEdge.data?.flowUuid ?? "",
              nextTotal + externalSaleAmount,
              sourcePortId,
            );
          }
          if (node.id === changedEdge.target) {
            return updateNodePortAmountByFlow(node, "input", changedEdge.data?.flowUuid ?? "", nextAmount, targetPortId);
          }
          return node;
        });
        return {
          ...canvas,
          nodes: nextNodes,
          edges: nextEdges,
        };
      }),
    ),
  setFlowBalanceUnit: (sourceNodeId, flowUuid, nextUnit) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const group = getFanoutEdges(canvas, sourceNodeId, flowUuid);
        if (group.length === 0) {
          return canvas;
        }
        const nextEdges = canvas.edges.map((edge) =>
          group.some((item) => item.id === edge.id)
            ? {
                ...edge,
                data: {
                  ...edge.data,
                  unit: nextUnit,
                } as LcaEdgeData,
              }
            : edge,
        );
        const nextNodes = canvas.nodes.map((node) => {
          if (node.id === sourceNodeId) {
            const sourcePortId = parseHandlePortId(group[0].sourceHandle ?? undefined, "out:");
            return updateNodePortUnitByFlow(node, "output", flowUuid, nextUnit, sourcePortId);
          }
          const incoming = group.find((e) => e.target === node.id);
          if (!incoming) {
            return node;
          }
          const targetPortId = parseHandlePortId(incoming.targetHandle ?? undefined, "in:");
          return updateNodePortUnitByFlow(node, "input", flowUuid, nextUnit, targetPortId);
        });
        return {
          ...canvas,
          nodes: nextNodes,
          edges: nextEdges,
        };
      }),
    ),
  clearConnectionHint: () =>
    set(() => ({
      connectionHint: undefined,
      connectionFix: undefined,
    })),
  setConnectionHint: (hint) =>
    set(() => ({
      connectionHint: hint,
      connectionFix: undefined,
    })),
  consumePendingAutoConnect: () =>
    set(() => ({
      pendingAutoConnect: false,
    })),
  consumePendingPtsCompile: () =>
    set(() => ({
      pendingPtsCompileNodeId: undefined,
    })),
  applyConnectionFix: () =>
    set((state) => {
      const fix = state.connectionFix;
      if (!fix || fix.type !== "insert_market") {
        return state;
      }

      const next = updateActiveCanvas(state, (canvas) => {
        const sourceNode = canvas.nodes.find((node) => node.id === fix.sourceNodeId);
        const targetNode = canvas.nodes.find((node) => node.id === fix.targetNodeId);
        if (!sourceNode || !targetNode) {
          return canvas;
        }
        const sourcePort = sourceNode.data.outputs.find((port) => port.id === fix.sourcePortId);
        if (!sourcePort) {
          return canvas;
        }

        const marketIn: FlowPort = withMarketInputSourceIdentity({
          id: `in_${uid().slice(0, 8)}`,
          flowUuid: fix.flowUuid,
          name: fix.flowName,
          unit: sourcePort.unit,
          unitGroup: sourcePort.unitGroup,
          amount: 0,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
        }, {
          nodeId: sourceNode.id,
          processUuid: sourceNode.data.processUuid,
          processName: sourceNode.data.name,
        });
        const marketOut: FlowPort = {
          id: `out_${uid().slice(0, 8)}`,
          flowUuid: fix.flowUuid,
          name: fix.flowName,
          unit: sourcePort.unit,
          unitGroup: sourcePort.unitGroup,
          amount: 1,
          isProduct: true,
          externalSaleAmount: 0,
          type: "technosphere",
          direction: "output",
          showOnNode: true,
        };
        const marketNode: Node<LcaNodeData> = {
          id: `node_${uid()}`,
          type: "lcaProcess",
          position: {
            x: (sourceNode.position.x + targetNode.position.x) / 2,
            y: (sourceNode.position.y + targetNode.position.y) / 2,
          },
          data: {
            nodeKind: "market_process",
            mode: "normalized",
            marketAllowMixedFlows: false,
            processUuid: `market_auto_${uid().slice(0, 8)}`,
            name: `市场过程-${fix.flowName}`,
            location: "",
            referenceProduct: fix.flowName,
            inputs: [marketIn],
            outputs: [marketOut],
          },
        };

        const incomingSameFlow = canvas.edges.filter(
          (edge) => edge.target === targetNode.id && edge.data?.flowUuid === fix.flowUuid,
        );
        const untouchedEdges = canvas.edges.filter(
          (edge) => !(edge.target === targetNode.id && edge.data?.flowUuid === fix.flowUuid),
        );

        const rewiredIncoming: Array<Edge<LcaEdgeData>> = incomingSameFlow.map((edge) => ({
          ...edge,
          id: `edge_${uid()}`,
          target: marketNode.id,
          targetHandle: `in:${marketIn.id}`,
          data: {
            flowUuid: fix.flowUuid,
            flowName: fix.flowName,
            amount: edge.data?.amount ?? 1,
            providerAmount: edge.data?.providerAmount ?? edge.data?.amount ?? 1,
            consumerAmount: edge.data?.consumerAmount ?? edge.data?.amount ?? 1,
            unit: edge.data?.unit ?? sourcePort.unit,
            type: edge.data?.type ?? sourcePort.type,
            allocation: edge.data?.allocation ?? "physical",
            dbMapping: edge.data?.dbMapping,
            quantityMode: resolveQuantityMode(canvas.nodes.find((n) => n.id === edge.source), marketNode),
          },
        }));

        const hasSourceInIncoming = incomingSameFlow.some((edge) => edge.source === sourceNode.id);
        const sourceToMarket =
          hasSourceInIncoming
            ? []
            : [
                {
                  id: `edge_${uid()}`,
                  source: sourceNode.id,
                  target: marketNode.id,
                  sourceHandle: `out:${sourcePort.id}`,
                  targetHandle: `in:${marketIn.id}`,
                  type: "lcaExchange" as const,
                  data: {
                    flowUuid: fix.flowUuid,
                    flowName: fix.flowName,
                    quantityMode: resolveQuantityMode(sourceNode, marketNode),
                    amount: sourcePort.amount ?? 0,
                    providerAmount: sourcePort.amount ?? 0,
                    consumerAmount: sourcePort.amount ?? 0,
                    unit: sourcePort.unit,
                    type: sourcePort.type,
                    allocation: "physical" as const,
                    dbMapping: "",
                  },
                } satisfies Edge<LcaEdgeData>,
              ];

        let nextTargetNode = targetNode;
        let targetHandle = getTargetHandle(targetNode, fix.flowUuid, canvas.edges);
        if (!targetHandle) {
          const autoInput = buildAutoInputPort(marketOut);
          nextTargetNode = {
            ...targetNode,
            data: {
              ...targetNode.data,
              inputs: [...targetNode.data.inputs, autoInput],
            },
          };
          targetHandle = `in:${autoInput.id}`;
        }

        const marketToTarget: Edge<LcaEdgeData> = {
          id: `edge_${uid()}`,
          source: marketNode.id,
          target: nextTargetNode.id,
          sourceHandle: `out:${marketOut.id}`,
          targetHandle,
          type: "lcaExchange",
          data: {
            flowUuid: fix.flowUuid,
            flowName: fix.flowName,
            quantityMode: resolveQuantityMode(marketNode, nextTargetNode),
            amount: sourcePort.amount ?? 0,
            providerAmount: sourcePort.amount ?? 0,
            consumerAmount: sourcePort.amount ?? 0,
            unit: sourcePort.unit,
            type: sourcePort.type,
            allocation: "physical",
            dbMapping: "",
          },
        };

        return {
          ...canvas,
          nodes: [
            ...canvas.nodes.map((node) => (node.id === nextTargetNode.id ? nextTargetNode : node)),
            marketNode,
          ],
          edges: [...untouchedEdges, ...rewiredIncoming, ...sourceToMarket, marketToTarget],
        };
      });

      return {
        ...next,
        connectionHint: undefined,
        connectionFix: undefined,
      };
    }),
  addNodeFromTemplate: (templateId, position) =>
    set((state) => {
      const template = processLibrary.find((item) => item.id === templateId);
      if (!template) {
        return state;
      }

      const node: Node<LcaNodeData> = buildNodeFromTemplate(template, position);

      const next = updateActiveCanvas(state, (canvas) => ({
        ...canvas,
        nodes: [...canvas.nodes, node],
      }));

      return {
        ...next,
        connectionHint: undefined,
      };
    }),
  updateNode: (nodeId, updater) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const currentNode = active.nodes.find((node) => node.id === nodeId);
      debugFlowTypesForNode("updateNode:before", currentNode);
      const nextNodesInitial = active.nodes.map((node) => (node.id === nodeId ? sanitizeMarketNode(updater(node)) : node));
      let nextNodes = nextNodesInitial;
      let nextEdges = active.edges;
      let pairAutoPromotedInputAsProduct = false;

      const updatedNode = nextNodes.find((node) => node.id === nodeId);
      debugFlowTypesForNode("updateNode:after-updater", updatedNode);
      if (currentNode && updatedNode) {
        const nodeById = new Map(nextNodes.map((n) => [n.id, n]));
        const currentOutputById = new Map(currentNode.data.outputs.map((p) => [p.id, p]));
        const updatedOutputById = new Map(updatedNode.data.outputs.map((p) => [p.id, p]));
        const currentInputById = new Map(currentNode.data.inputs.map((p) => [p.id, p]));
        const updatedInputById = new Map(updatedNode.data.inputs.map((p) => [p.id, p]));

        const outgoingFlowToEdges = new Map<string, Edge<LcaEdgeData>[]>();
        const incomingFlowToEdges = new Map<string, Edge<LcaEdgeData>[]>();
        for (const edge of nextEdges) {
          if (edge.source === nodeId) {
            const key = edge.data?.flowUuid ?? "";
            if (!outgoingFlowToEdges.has(key)) {
              outgoingFlowToEdges.set(key, []);
            }
            outgoingFlowToEdges.get(key)?.push(edge);
          }
          if (edge.target === nodeId) {
            const key = edge.data?.flowUuid ?? "";
            if (!incomingFlowToEdges.has(key)) {
              incomingFlowToEdges.set(key, []);
            }
            incomingFlowToEdges.get(key)?.push(edge);
          }
        }

        const edgeAmountPatch = new Map<string, number>();
        const targetInputPatch = new Map<string, Array<{ flowUuid: string; amount: number; preferredPortId?: string }>>();
        const sourceOutputPatch = new Map<string, Array<{ flowUuid: string; amount: number; preferredPortId?: string }>>();

        for (const [portId, nextPort] of updatedOutputById.entries()) {
          const prev = currentOutputById.get(portId);
          if (!prev || Math.abs(prev.amount - nextPort.amount) < 1e-12) {
            continue;
          }
          const groupEdges = outgoingFlowToEdges.get(nextPort.flowUuid) ?? [];
          if (groupEdges.length !== 1) {
            continue;
          }
          const e = groupEdges[0];
          const s = nodeById.get(e.source);
          const t = nodeById.get(e.target);
          const quantityMode = e.data?.quantityMode ?? resolveQuantityMode(s, t);
          if (!isBalancedUnitNode(s) || !isBalancedUnitNode(t) || quantityMode !== "single") {
            continue;
          }
          edgeAmountPatch.set(e.id, nextPort.amount);
          const targetPortId = parseHandlePortId(e.targetHandle ?? undefined, "in:");
          const patches = targetInputPatch.get(e.target) ?? [];
          patches.push({ flowUuid: nextPort.flowUuid, amount: nextPort.amount, preferredPortId: targetPortId });
          targetInputPatch.set(e.target, patches);
        }

        for (const [portId, nextPort] of updatedInputById.entries()) {
          const prev = currentInputById.get(portId);
          if (!prev || Math.abs(prev.amount - nextPort.amount) < 1e-12) {
            continue;
          }
          const groupEdges = incomingFlowToEdges.get(nextPort.flowUuid) ?? [];
          if (groupEdges.length !== 1) {
            continue;
          }
          const targetEdge = groupEdges[0];
          const sourceNode = nodeById.get(targetEdge.source);
          const targetNode = nodeById.get(targetEdge.target);
          const quantityMode = targetEdge.data?.quantityMode ?? resolveQuantityMode(sourceNode, targetNode);
          if (!isBalancedUnitNode(sourceNode) || !isBalancedUnitNode(targetNode) || quantityMode !== "single") {
            continue;
          }
          edgeAmountPatch.set(targetEdge.id, nextPort.amount);
          const sourcePortId = parseHandlePortId(targetEdge.sourceHandle ?? undefined, "out:");
          const patches = sourceOutputPatch.get(targetEdge.source) ?? [];
          patches.push({ flowUuid: nextPort.flowUuid, amount: nextPort.amount, preferredPortId: sourcePortId });
          sourceOutputPatch.set(targetEdge.source, patches);
        }

        if (edgeAmountPatch.size > 0) {
          nextEdges = nextEdges.map((edge) => {
            const patched = edgeAmountPatch.get(edge.id);
            if (patched === undefined) {
              return edge;
            }
            return {
              ...edge,
              data: {
                flowUuid: edge.data?.flowUuid ?? "",
                flowName: edge.data?.flowName ?? "",
                quantityMode: "single",
                amount: patched,
                providerAmount: patched,
                consumerAmount: patched,
                unit: edge.data?.unit ?? "kg",
                type: edge.data?.type ?? "technosphere",
                allocation: edge.data?.allocation ?? "none",
                dbMapping: edge.data?.dbMapping,
              },
            };
          });
        }

        if (targetInputPatch.size > 0 || sourceOutputPatch.size > 0) {
          nextNodes = nextNodes.map((node) => {
            const inputPatches = targetInputPatch.get(node.id) ?? [];
            const outputPatches = sourceOutputPatch.get(node.id) ?? [];
            let nextNode = node;
            for (const patch of inputPatches) {
              nextNode = updateNodePortAmountByFlow(nextNode, "input", patch.flowUuid, patch.amount, patch.preferredPortId);
            }
            for (const patch of outputPatches) {
              nextNode = updateNodePortAmountByFlow(nextNode, "output", patch.flowUuid, patch.amount, patch.preferredPortId);
            }
            return nextNode;
          });
        }
      }

      const nodeByIdAfterUpdate = new Map(nextNodes.map((node) => [node.id, node]));
      nextEdges = nextEdges.filter((edge) => {
        const source = nodeByIdAfterUpdate.get(edge.source);
        const target = nodeByIdAfterUpdate.get(edge.target);
        const flowUuid = edge.data?.flowUuid ?? "";
        if (!source || !target || !flowUuid) {
          return false;
        }
        const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
        const targetPortId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
        if (!sourcePortId || !targetPortId) {
          return false;
        }
        const sourceValid = source.data.outputs.some((p) => p.id === sourcePortId && p.flowUuid === flowUuid);
        const targetValid = target.data.inputs.some((p) => p.id === targetPortId && p.flowUuid === flowUuid);
        return sourceValid && targetValid;
      });

      if (currentNode && updatedNode) {
        const currentInputByIdForSwap = new Map(currentNode.data.inputs.map((p) => [p.id, p]));
        const currentOutputByIdForSwap = new Map(currentNode.data.outputs.map((p) => [p.id, p]));
        const productChangedPorts: Array<{ direction: "input" | "output"; portId: string; flowUuid: string; isProduct: boolean }> = [];
        for (const port of updatedNode.data.inputs) {
          const prev = currentInputByIdForSwap.get(port.id);
          if (prev && Boolean(prev.isProduct) !== Boolean(port.isProduct)) {
            productChangedPorts.push({
              direction: "input",
              portId: port.id,
              flowUuid: port.flowUuid,
              isProduct: Boolean(port.isProduct),
            });
          }
        }
        for (const port of updatedNode.data.outputs) {
          const prev = currentOutputByIdForSwap.get(port.id);
          if (prev && Boolean(prev.isProduct) !== Boolean(port.isProduct)) {
            productChangedPorts.push({
              direction: "output",
              portId: port.id,
              flowUuid: port.flowUuid,
              isProduct: Boolean(port.isProduct),
            });
          }
        }
        if (productChangedPorts.length > 0) {
          const patchNodePortProduct = (targetNodeId: string, portDirection: "input" | "output", targetPortId: string, nextFlag: boolean) => {
            nextNodes = nextNodes.map((node) => {
              if (node.id !== targetNodeId) {
                return node;
              }
              const patchList = (ports: FlowPort[], thisDirection: "input" | "output") =>
                ports.map((port) =>
                  port.id === targetPortId && thisDirection === portDirection ? { ...port, isProduct: nextFlag } : port,
                );
              return {
                ...node,
                data: {
                  ...node.data,
                  inputs: patchList(node.data.inputs, "input"),
                  outputs: patchList(node.data.outputs, "output"),
                },
              };
            });
          };

          for (const changed of productChangedPorts) {
            const relatedEdges = nextEdges.filter((edge) => {
              const sameFlow = (edge.data?.flowUuid ?? "") === changed.flowUuid;
              if (!sameFlow) {
                return false;
              }
              if (changed.direction === "output" && edge.source === nodeId) {
                const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
                return sourcePortId === changed.portId;
              }
              if (changed.direction === "input" && edge.target === nodeId) {
                const targetPortId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
                return targetPortId === changed.portId;
              }
              return false;
            });
            if (relatedEdges.length !== 1) {
              continue;
            }
            const edge = relatedEdges[0];
            const sourceSideEdges = nextEdges.filter((e) => e.source === edge.source && (e.data?.flowUuid ?? "") === changed.flowUuid);
            const targetSideEdges = nextEdges.filter((e) => e.target === edge.target && (e.data?.flowUuid ?? "") === changed.flowUuid);
            const sourceNode = nodeByIdAfterUpdate.get(edge.source);
            const targetNode = nodeByIdAfterUpdate.get(edge.target);
            const quantityMode = edge.data?.quantityMode ?? resolveQuantityMode(sourceNode, targetNode);
            if (!isBalancedUnitNode(sourceNode) || !isBalancedUnitNode(targetNode) || quantityMode !== "single") {
              continue;
            }
            if (sourceSideEdges.length !== 1 || targetSideEdges.length !== 1) {
              continue;
            }
            const counterpartNodeId = changed.direction === "output" ? edge.target : edge.source;
            const counterpartPortId =
              changed.direction === "output"
                ? parseHandlePortId(edge.targetHandle ?? undefined, "in:")
                : parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
            const counterpartDirection: "input" | "output" = changed.direction === "output" ? "input" : "output";
            if (!counterpartPortId) {
              continue;
            }
            if (counterpartDirection === "input" && !changed.isProduct) {
              pairAutoPromotedInputAsProduct = true;
            }
            patchNodePortProduct(counterpartNodeId, counterpartDirection, counterpartPortId, !changed.isProduct);
          }
          nodeByIdAfterUpdate.clear();
          for (const node of nextNodes) {
            nodeByIdAfterUpdate.set(node.id, node);
          }
        }
      }

      // Enforce relation constraints after inventory edit to avoid lingering stale invalid edges.
      const occupiedInputHandles = new Set<string>();
      const incomingSourceByTargetFlow = new Map<string, Set<string>>();
      const outgoingTargetBySourceFlow = new Map<string, Set<string>>();
      nextEdges = nextEdges.filter((edge) => {
        const source = nodeByIdAfterUpdate.get(edge.source);
        const target = nodeByIdAfterUpdate.get(edge.target);
        const flowUuid = edge.data?.flowUuid ?? "";
        if (!source || !target || !flowUuid) {
          return false;
        }
        const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
        const targetPortId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
        if (!sourcePortId || !targetPortId) {
          return false;
        }
        const sourcePort = source.data.outputs.find((p) => p.id === sourcePortId && p.flowUuid === flowUuid);
        const targetPort = target.data.inputs.find((p) => p.id === targetPortId && p.flowUuid === flowUuid);
        if (!sourcePort || !targetPort) {
          return false;
        }

        const enforceSingleInboundForNonProduct = isNonProductIntermediate(targetPort);
        const inputHandleKey = `${edge.target}::${targetPort.id}`;
        const inputOccupiedByOtherSource = enforceSingleInboundForNonProduct && occupiedInputHandles.has(inputHandleKey);

        const targetFlowKey = `${edge.target}::${flowUuid}`;
        const sourceFlowKey = `${edge.source}::${flowUuid}`;
        const existingIncoming = incomingSourceByTargetFlow.get(targetFlowKey) ?? new Set<string>();
        const existingOutgoing = outgoingTargetBySourceFlow.get(sourceFlowKey) ?? new Set<string>();
        const relationHint = getRelationViolationHint({
          sourcePort,
          targetPort,
          targetNode: target,
          existingIncomingFromOtherSources: existingIncoming.has(edge.source)
            ? existingIncoming.size - 1
            : existingIncoming.size,
          existingOutgoingToOtherTargets: existingOutgoing.has(edge.target)
            ? existingOutgoing.size - 1
            : existingOutgoing.size,
          targetInputOccupiedByOtherSource: inputOccupiedByOtherSource,
        });
        if (relationHint) {
          return false;
        }

        if (enforceSingleInboundForNonProduct) {
          occupiedInputHandles.add(inputHandleKey);
        }
        existingIncoming.add(edge.source);
        incomingSourceByTargetFlow.set(targetFlowKey, existingIncoming);
        existingOutgoing.add(edge.target);
        outgoingTargetBySourceFlow.set(sourceFlowKey, existingOutgoing);
        return true;
      });

      // For normalized market targets in dual mode, consumer side must follow target input amount (share).
      nextEdges = nextEdges.map((edge) => {
        const target = nodeByIdAfterUpdate.get(edge.target);
        if (!target || !isMarketProcessNode(target)) {
          return edge;
        }
        const quantityMode = edge.data?.quantityMode ?? resolveQuantityMode(nodeByIdAfterUpdate.get(edge.source), target);
        if (target.data.mode !== "normalized" || quantityMode !== "dual") {
          return edge;
        }
        const flowUuid = edge.data?.flowUuid ?? "";
        if (!flowUuid) {
          return edge;
        }
        const source = nodeByIdAfterUpdate.get(edge.source);
        const sourcePortId = parseHandlePortId(edge.sourceHandle ?? undefined, "out:");
        const sourcePort = sourcePortId
          ? source?.data.outputs.find((p) => p.id === sourcePortId && p.flowUuid === flowUuid)
          : undefined;
        const providerAmount = Number.isFinite(edge.data?.providerAmount)
          ? (edge.data?.providerAmount as number)
          : (sourcePort?.amount ?? edge.data?.amount ?? 0);
        const consumerAmount = resolveTargetInputAmount(target, edge.targetHandle ?? undefined, flowUuid, edge.data?.amount ?? 0);
        return {
          ...edge,
          data: {
            flowUuid,
            flowName: edge.data?.flowName ?? "",
            quantityMode: "dual",
            amount: consumerAmount,
            providerAmount,
            consumerAmount,
            unit: edge.data?.unit ?? sourcePort?.unit ?? "kg",
            type: edge.data?.type ?? sourcePort?.type ?? "technosphere",
            allocation: edge.data?.allocation ?? "none",
            dbMapping: edge.data?.dbMapping,
          },
        };
      });

      const nextActiveCanvas = repairCanvasEdgeHandles({
        ...active,
        nodes: nextNodes,
        edges: nextEdges,
      });
      debugFlowTypesForNode(
        "updateNode:after-repair",
        nextActiveCanvas.nodes.find((node) => node.id === nodeId),
      );
      let nextCanvases = {
        ...state.canvases,
        [active.id]: nextActiveCanvas,
      };

      const finalUpdatedNode = nextActiveCanvas.nodes.find((node) => node.id === nodeId);
      if (currentNode?.data.ptsCanvasId && finalUpdatedNode) {
        const ptsCanvas = nextCanvases[currentNode.data.ptsCanvasId];
        if (ptsCanvas && ptsCanvas.name !== finalUpdatedNode.data.name) {
          nextCanvases = {
            ...nextCanvases,
            [ptsCanvas.id]: {
              ...ptsCanvas,
              name: finalUpdatedNode.data.name,
            },
          };
        }
      }

      const nextState = setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
      if (pairAutoPromotedInputAsProduct) {
        return {
          ...nextState,
          connectionHint: "警告：一对一连线已自动互换产品定义，输入端被设置为产品，请检查产品定义。",
          connectionFix: undefined,
        };
      }
      return nextState;
    }),
  setNodeMode: (nodeId, mode) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const nodeIds = new Set(active.nodes.map((n) => n.id));

      const nextNodes = active.nodes.map((node) => {
        if (node.id !== nodeId) {
          return node;
        }
        if (node.data.nodeKind !== "unit_process" || isMarketProcessNode(node)) {
          return {
            ...node,
            data: {
              ...node.data,
              mode: "normalized" as ProcessMode,
            },
          };
        }
        return {
          ...node,
          data: {
            ...node.data,
            mode: mode as ProcessMode,
          },
        };
      });

      const nodeById = new Map(nextNodes.map((n) => [n.id, n]));

      const nextEdges = active.edges.map((edge) => {
        if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) {
          return edge;
        }
        const sourceNode = nodeById.get(edge.source);
        const targetNode = nodeById.get(edge.target);
        const quantityMode = resolveQuantityMode(sourceNode, targetNode);
        const baseAmount = edge.data?.amount ?? edge.data?.providerAmount ?? edge.data?.consumerAmount ?? 1;

        if (quantityMode === "single") {
          return {
            ...edge,
            data: {
              flowUuid: edge.data?.flowUuid ?? "",
              flowName: edge.data?.flowName ?? "",
              quantityMode,
              amount: baseAmount,
              providerAmount: baseAmount,
              consumerAmount: baseAmount,
              unit: edge.data?.unit ?? "kg",
              type: edge.data?.type ?? "technosphere",
              allocation: edge.data?.allocation ?? "none",
              dbMapping: edge.data?.dbMapping,
            },
          };
        }

        const providerAmount = edge.data?.providerAmount ?? baseAmount;
        const consumerAmount = edge.data?.consumerAmount ?? baseAmount;
        return {
          ...edge,
          data: {
            flowUuid: edge.data?.flowUuid ?? "",
            flowName: edge.data?.flowName ?? "",
            quantityMode,
            amount: consumerAmount,
            providerAmount,
            consumerAmount,
            unit: edge.data?.unit ?? "kg",
            type: edge.data?.type ?? "technosphere",
            allocation: edge.data?.allocation ?? "none",
            dbMapping: edge.data?.dbMapping,
          },
        };
      });

      const nextCanvas: CanvasGraph = {
        ...active,
        nodes: nextNodes,
        edges: nextEdges,
      };
      const nextCanvases = {
        ...state.canvases,
        [active.id]: nextCanvas,
      };

      return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
    }),
  setMarketAllowMixedFlows: (nodeId, allow) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const nextCanvas: CanvasGraph = {
        ...active,
        nodes: active.nodes.map((node) => {
          if (node.id !== nodeId || !isMarketProcessNode(node)) {
            return node;
          }
          return sanitizeMarketNode({
            ...node,
            data: {
              ...node.data,
              marketAllowMixedFlows: allow,
            },
          });
        }),
      };
      const nextCanvases = {
        ...state.canvases,
        [active.id]: nextCanvas,
      };
      return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
    }),
  replaceMarketOutputFlow: (nodeId, flow) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const nextEdges = active.edges.filter((edge) => edge.source !== nodeId && edge.target !== nodeId);
      const nextCanvas: CanvasGraph = {
        ...active,
        nodes: active.nodes.map((node) => {
          if (node.id !== nodeId || !isMarketProcessNode(node)) {
            return node;
          }
          const currentOutput = node.data.outputs[0];
          const nextOutput: FlowPort = {
            id: currentOutput?.id ?? `out_${uid().slice(0, 8)}`,
            flowUuid: flow.flowUuid,
            name: flow.name,
            unit: flow.unit,
            unitGroup: flow.unitGroup,
            amount: 1,
            isProduct: true,
            externalSaleAmount: 0,
            type: flow.type,
            direction: "output",
            showOnNode: currentOutput?.showOnNode ?? true,
          };
          const nextInputs = node.data.inputs.filter((input) => input.type === "biosphere");
          return sanitizeMarketNode({
            ...node,
            data: {
              ...node.data,
              referenceProduct: flow.name,
              referenceProductFlowUuid: flow.flowUuid,
              referenceProductDirection: "output",
              outputs: [nextOutput],
              inputs: nextInputs,
            },
          });
        }),
        edges: nextEdges,
      };
      const nextCanvases = {
        ...state.canvases,
        [active.id]: nextCanvas,
      };
      return setActiveCanvas(state, nextCanvases, state.activeCanvasId, true);
    }),
  updateEdgeData: (edgeId, data) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const nextEdges = canvas.edges.map((edge) =>
          edge.id === edgeId
            ? (() => {
                const nextData = {
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
                  ...data,
                } as LcaEdgeData;
                return {
                  ...edge,
                  data: normalizeSingleQuantityEdgeData(nextData),
                };
              })()
            : edge,
        );

        const changedEdge = nextEdges.find((edge) => edge.id === edgeId);
        if (!changedEdge || !changedEdge.data) {
          return { ...canvas, edges: nextEdges };
        }

        let normalizedEdges = nextEdges;
        const sourceNode = canvas.nodes.find((n) => n.id === changedEdge.source);
        const targetNode = canvas.nodes.find((n) => n.id === changedEdge.target);
        const balancedPair = isBalancedUnitNode(sourceNode) && isBalancedUnitNode(targetNode);

        if (balancedPair) {
          const amount = changedEdge.data.amount ?? changedEdge.data.consumerAmount ?? changedEdge.data.providerAmount ?? 0;
          normalizedEdges = nextEdges.map((edge) =>
            edge.id === edgeId
              ? {
                  ...edge,
                  data: {
                    ...edge.data,
                    quantityMode: "single",
                    amount,
                    providerAmount: amount,
                    consumerAmount: amount,
                  } as LcaEdgeData,
                }
              : edge,
          );
        }

        const syncedEdge = normalizedEdges.find((edge) => edge.id === edgeId);
        if (!syncedEdge || !syncedEdge.data) {
          return { ...canvas, edges: normalizedEdges };
        }

        let nextNodes = canvas.nodes;
        if (balancedPair) {
          const amount = syncedEdge.data.amount ?? 0;
          const sourceFlowEdges = normalizedEdges.filter(
            (edge) => edge.source === syncedEdge.source && edge.data?.flowUuid === syncedEdge.data?.flowUuid,
          );
          const targetFlowEdges = normalizedEdges.filter(
            (edge) => edge.target === syncedEdge.target && edge.data?.flowUuid === syncedEdge.data?.flowUuid,
          );

          if (sourceFlowEdges.length === 1 && targetFlowEdges.length === 1) {
            const sourcePortId = parseHandlePortId(syncedEdge.sourceHandle ?? undefined, "out:");
            const targetPortId = parseHandlePortId(syncedEdge.targetHandle ?? undefined, "in:");
            nextNodes = nextNodes.map((node) => {
              if (node.id === syncedEdge.source) {
                return updateNodePortAmountByFlow(node, "output", syncedEdge.data?.flowUuid ?? "", amount, sourcePortId);
              }
              if (node.id === syncedEdge.target) {
                return updateNodePortAmountByFlow(node, "input", syncedEdge.data?.flowUuid ?? "", amount, targetPortId);
              }
              return node;
            });
          } else if (sourceFlowEdges.length > 1) {
            const sum = sourceFlowEdges.reduce((acc, edge) => acc + (edge.data?.amount ?? 0), 0);
            const sourcePortId = parseHandlePortId(syncedEdge.sourceHandle ?? undefined, "out:");
            nextNodes = nextNodes.map((node) =>
              node.id === syncedEdge.source
                ? updateNodePortAmountByFlow(node, "output", syncedEdge.data?.flowUuid ?? "", sum, sourcePortId)
                : node,
            );
          }
        }

        return {
          ...canvas,
          nodes: nextNodes,
          edges: normalizedEdges,
        };
      }),
    ),
  removeEdge: (edgeId) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const repaired = repairCanvasEdgeHandles({
          ...canvas,
          edges: canvas.edges.filter((edge) => edge.id !== edgeId),
        });
        return pruneDetachedMarketInputs(repaired);
      }),
    ),
  removeNode: (nodeId) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const repaired = repairCanvasEdgeHandles({
          ...canvas,
          nodes: canvas.nodes.filter((node) => node.id !== nodeId),
          edges: canvas.edges.filter((edge) => edge.source !== nodeId && edge.target !== nodeId),
        });
        return pruneDetachedMarketInputs(repaired);
      }),
    ),
  cloneNodeAt: (nodeId, position) =>
    set((state) =>
      updateActiveCanvas(state, (canvas) => {
        const source = canvas.nodes.find((n) => n.id === nodeId);
        if (!source) {
          return canvas;
        }
        const identityPatch = buildClonedProcessIdentity(source);
        const clone: Node<LcaNodeData> = {
          ...source,
          id: `node_${uid()}`,
          position,
          data: {
            ...source.data,
            ...identityPatch,
            inputs: source.data.inputs.map((p) => ({ ...p, id: `${p.direction}_${uid().slice(0, 8)}` })),
            outputs: source.data.outputs.map((p) => ({ ...p, id: `${p.direction}_${uid().slice(0, 8)}` })),
          },
        };
        return {
          ...canvas,
          nodes: [...canvas.nodes, clone],
        };
      }),
    ),
  upsertOutputLink: (sourceNodeId, outputPortId, targetNodeId, targetInputPortId) =>
    set((state) => {
      let queuedEdge: Edge<LcaEdgeData> | undefined;
      let shouldOpenBalanceEditor = false;
      let didAutoCreateTargetInput = false;
      const nextPartial = updateActiveCanvas(state, (canvas) => {
        const sourceNode = canvas.nodes.find((n) => n.id === sourceNodeId);
        const targetNode = canvas.nodes.find((n) => n.id === targetNodeId);
        if (!sourceNode || !targetNode || sourceNode.id === targetNode.id) {
          return canvas;
        }
        const outputPort = sourceNode.data.outputs.find((p) => p.id === outputPortId);
        if (!outputPort) {
          return canvas;
        }
        let nextTargetNode: Node<LcaNodeData> = targetNode;
        const forceCreateMarketInputRow = isMarketProcessNode(targetNode) && !targetInputPortId;
        const explicitOccupied =
          Boolean(targetInputPortId) &&
          canvas.edges.some(
            (edge) =>
              edge.target === targetNode.id &&
              parseHandlePortId(edge.targetHandle ?? undefined, "in:") === targetInputPortId &&
              edge.source !== sourceNodeId,
          );
        let targetHandle =
          targetInputPortId &&
          targetNode.data.inputs.some((port) => port.id === targetInputPortId && port.flowUuid === outputPort.flowUuid) &&
          !(isMarketProcessNode(targetNode) && explicitOccupied)
            ? `in:${targetInputPortId}`
            : !forceCreateMarketInputRow
              ? getTargetHandle(targetNode, outputPort.flowUuid, canvas.edges)
              : undefined;
        if (shouldEnforceMarketSingleFlow(targetNode)) {
          const marketInput = targetNode.data.inputs[0];
          const marketOutput = targetNode.data.outputs[0];
          if (marketInput && marketInput.flowUuid !== outputPort.flowUuid) {
            return canvas;
          }
          if (marketOutput && marketOutput.flowUuid !== outputPort.flowUuid) {
            return canvas;
          }
        }
        if (!targetHandle) {
          const autoPort: FlowPort = {
            ...buildAutoInputPort(outputPort),
          };
          const nextAutoPort = isMarketProcessNode(targetNode)
            ? withMarketInputSourceIdentity(autoPort, {
                nodeId: sourceNode.id,
                processUuid: sourceNode.data.processUuid,
                processName: sourceNode.data.name,
                portName: outputPort.name,
                nestedProcessName: outputPort.sourceProcessName,
              })
            : autoPort;
          nextTargetNode = {
            ...targetNode,
            data: {
              ...targetNode.data,
              inputs: [...targetNode.data.inputs, nextAutoPort],
            },
          };
          if (shouldEnforceMarketSingleFlow(targetNode) && nextTargetNode.data.outputs.length === 0) {
            const autoOut: FlowPort = {
              ...buildAutoOutputPortFromInput(nextAutoPort),
              name: stripMarketSourceSuffix(outputPort.name),
              flowNameEn: outputPort.flowNameEn,
              amount: 1,
              isProduct: true,
              externalSaleAmount: 0,
            };
            nextTargetNode = {
              ...nextTargetNode,
              data: {
                ...nextTargetNode.data,
                outputs: [autoOut],
                referenceProduct: autoOut.name,
              },
            };
          }
          targetHandle = `in:${nextAutoPort.id}`;
          didAutoCreateTargetInput = true;
        }
        const resolvedTargetPortId = parseHandlePortId(targetHandle, "in:");
        if (isMarketProcessNode(nextTargetNode) && resolvedTargetPortId) {
          nextTargetNode = {
            ...nextTargetNode,
            data: {
              ...nextTargetNode.data,
              inputs: nextTargetNode.data.inputs.map((port) =>
                port.id === resolvedTargetPortId
                  ? withMarketInputSourceIdentity(port, {
                      nodeId: sourceNode.id,
                      processUuid: sourceNode.data.processUuid,
                      processName: sourceNode.data.name,
                      portName: outputPort.name,
                      nestedProcessName: outputPort.sourceProcessName,
                    })
                  : port,
              ),
            },
          };
        }

        const quantityMode = resolveQuantityMode(sourceNode, nextTargetNode);
        const amount = outputPort.amount ?? 0;
        const targetPortId = parseHandlePortId(targetHandle, "in:");
        const targetPort =
          (targetPortId &&
            nextTargetNode.data.inputs.find((port) => port.id === targetPortId && port.flowUuid === outputPort.flowUuid)) ||
          nextTargetNode.data.inputs.find((port) => port.flowUuid === outputPort.flowUuid);
        if (!targetPort) {
          return canvas;
        }
        const relationHint = getConnectionRuleHint({
          sourceNode,
          sourcePort: outputPort,
          targetNode: nextTargetNode,
          targetPort,
          edges: canvas.edges,
        });
        if (relationHint) {
          return canvas;
        }
        const consumerAmount =
          isMarketProcessNode(nextTargetNode) && nextTargetNode.data.mode === "normalized" && quantityMode === "dual"
            ? resolveTargetInputAmount(nextTargetNode, targetHandle, outputPort.flowUuid, amount)
            : amount;
        const existing = canvas.edges.find((edge) => {
          if (edge.source !== sourceNodeId || edge.target !== targetNodeId || edge.data?.flowUuid !== outputPort.flowUuid) {
            return false;
          }
          if (!targetInputPortId) {
            return true;
          }
          const portId = parseHandlePortId(edge.targetHandle ?? undefined, "in:");
          return portId === targetInputPortId;
        });
        const nextEdge: Edge<LcaEdgeData> = {
          id: existing?.id ?? `edge_${uid()}`,
          source: sourceNodeId,
          target: targetNodeId,
          sourceHandle: `out:${outputPort.id}`,
          targetHandle,
          type: "lcaExchange",
          data: {
            flowUuid: outputPort.flowUuid,
            flowName: outputPort.name,
            quantityMode,
            amount: consumerAmount,
            providerAmount: amount,
            consumerAmount,
            unit: outputPort.unit,
            type: outputPort.type,
            allocation: "physical",
          },
        };

        const edgesPreview = existing
          ? canvas.edges.map((edge) => (edge.id === existing.id ? nextEdge : edge))
          : [...canvas.edges, nextEdge];

        shouldOpenBalanceEditor = shouldAutoOpenBalanceEditorForNewLink({
          state,
          sourceNode,
          didAutoCreateTargetInput,
          canvas: {
            ...canvas,
            nodes: canvas.nodes.map((node) => (node.id === targetNode.id ? nextTargetNode : node)),
            edges: edgesPreview,
          },
          edge: nextEdge,
        });
        queuedEdge = nextEdge;

        const nextCanvas = {
          ...canvas,
          nodes: canvas.nodes.map((node) => (node.id === targetNode.id ? nextTargetNode : node)),
        };
        return repairCanvasEdgeHandles(nextCanvas);
      });
      return {
        ...nextPartial,
        pendingEdges: queuedEdge
          ? enqueuePendingEdge(nextPartial.pendingEdges ?? state.pendingEdges, {
              canvasId: state.activeCanvasId,
              edge: queuedEdge,
              openBalanceEditor: shouldOpenBalanceEditor,
            })
          : nextPartial.pendingEdges,
        flowBalanceDialog: { open: false },
      };
    }),
  createAndLinkOutputTarget: (sourceNodeId, outputPortId, targetKind) =>
    set((state) => {
      let queuedEdge: Edge<LcaEdgeData> | undefined;
      const nextPartial = updateActiveCanvas(state, (canvas) => {
        const sourceNode = canvas.nodes.find((n) => n.id === sourceNodeId);
        if (!sourceNode) {
          return canvas;
        }
        const outputPort = sourceNode.data.outputs.find((p) => p.id === outputPortId);
        if (!outputPort) {
          return canvas;
        }

        const targetCreateKind =
          targetKind === "unit_process"
            ? "unit_process"
            : targetKind === "market_process"
              ? "market_process"
              : targetKind === "pts_module"
                ? "pts_module"
                : "lci_dataset";
        const node = buildBlankNode(targetCreateKind, {
          x: sourceNode.position.x + 340,
          y: sourceNode.position.y + Math.floor(Math.random() * 80) - 40,
        }, state.uiLanguage);
        let nextNode = node;
        let targetHandle = getTargetHandle(nextNode, outputPort.flowUuid, canvas.edges);
        if (!targetHandle) {
          const autoPort: FlowPort = {
            ...buildAutoInputPort(outputPort),
          };
          const nextAutoPort = isMarketProcessNode(nextNode)
            ? withMarketInputSourceIdentity(autoPort, {
                nodeId: sourceNode.id,
                processUuid: sourceNode.data.processUuid,
                processName: sourceNode.data.name,
                portName: outputPort.name,
                nestedProcessName: outputPort.sourceProcessName,
              })
            : autoPort;
          nextNode = {
            ...nextNode,
            data: {
              ...nextNode.data,
              inputs: [...nextNode.data.inputs, nextAutoPort],
            },
          };
          if (shouldEnforceMarketSingleFlow(nextNode) && nextNode.data.outputs.length === 0) {
            const autoOut: FlowPort = {
              ...buildAutoOutputPortFromInput(nextAutoPort),
              name: stripMarketSourceSuffix(outputPort.name),
              amount: 1,
              isProduct: true,
              externalSaleAmount: 0,
            };
            nextNode = {
              ...nextNode,
              data: {
                ...nextNode.data,
                outputs: [autoOut],
                referenceProduct: autoOut.name,
              },
            };
          }
          targetHandle = `in:${nextAutoPort.id}`;
        }
        const resolvedTargetPortId = parseHandlePortId(targetHandle, "in:");
        if (isMarketProcessNode(nextNode) && resolvedTargetPortId) {
          nextNode = {
            ...nextNode,
            data: {
              ...nextNode.data,
              inputs: nextNode.data.inputs.map((port) =>
                port.id === resolvedTargetPortId
                  ? withMarketInputSourceIdentity(port, {
                      nodeId: sourceNode.id,
                      processUuid: sourceNode.data.processUuid,
                      processName: sourceNode.data.name,
                      portName: outputPort.name,
                      nestedProcessName: outputPort.sourceProcessName,
                    })
                  : port,
              ),
            },
          };
        }
        const quantityMode = resolveQuantityMode(sourceNode, nextNode);
        const amount = outputPort.amount ?? 0;
        const edge: Edge<LcaEdgeData> = {
          id: `edge_${uid()}`,
          source: sourceNode.id,
          target: nextNode.id,
          sourceHandle: `out:${outputPort.id}`,
          targetHandle,
          type: "lcaExchange",
          data: {
            flowUuid: outputPort.flowUuid,
            flowName: outputPort.name,
            quantityMode,
            amount,
            providerAmount: amount,
            consumerAmount: amount,
            unit: outputPort.unit,
            type: outputPort.type,
            allocation: "physical",
          },
        };

        const nextCanvas = {
          ...canvas,
          nodes: [...canvas.nodes, nextNode],
        };
        queuedEdge = edge;
        return repairCanvasEdgeHandles(nextCanvas);
      });
      return {
        ...nextPartial,
        pendingEdges: queuedEdge
          ? enqueuePendingEdge(nextPartial.pendingEdges ?? state.pendingEdges, {
              canvasId: state.activeCanvasId,
              edge: queuedEdge,
              openBalanceEditor: false,
            })
          : nextPartial.pendingEdges,
      };
    }),
  addBlankUnitProcess: () =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const baseNode = buildBlankNode("unit_process", getAutoNodePosition(active), state.uiLanguage);
      const node = {
        ...baseNode,
        data: {
          ...baseNode.data,
          name: buildUniqueNodeName(active.nodes, baseNode.data.name, state.uiLanguage),
        },
      };
      return {
        ...updateActiveCanvas(state, (canvas) => ({
          ...canvas,
          nodes: [...canvas.nodes, node],
        })),
        selection: { nodeIds: [node.id], edgeIds: [], nodeId: node.id },
      };
    }),
  addBlankMarketProcess: () =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const baseNode = buildBlankNode("market_process", getAutoNodePosition(active), state.uiLanguage);
      const node = {
        ...baseNode,
        data: {
          ...baseNode.data,
          name: buildUniqueNodeName(active.nodes, baseNode.data.name, state.uiLanguage),
        },
      };
      return {
        ...updateActiveCanvas(state, (canvas) => ({
          ...canvas,
          nodes: [...canvas.nodes, node],
        })),
        selection: { nodeIds: [node.id], edgeIds: [], nodeId: node.id },
      };
    }),
  addBlankPts: () =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const baseNode = buildBlankNode("pts_module", getAutoNodePosition(active), state.uiLanguage);
      const node = {
        ...baseNode,
        data: {
          ...baseNode.data,
          name: buildUniqueNodeName(active.nodes, baseNode.data.name, state.uiLanguage),
        },
      };
      const ptsCanvasId = `canvas_${uid()}`;
      const ptsNode =
        node.data.nodeKind === "pts_module"
          ? {
              ...node,
              data: {
                ...node.data,
                ptsCanvasId,
              },
            }
          : node;
      const base = updateActiveCanvas(state, (canvas) => ({
        ...canvas,
        nodes: [...canvas.nodes, ptsNode],
      }));
      const nextCanvases: Record<string, CanvasGraph> = {
        ...base.canvases,
        [ptsCanvasId]: {
          id: ptsCanvasId,
          name: ptsNode.data.name,
          kind: "pts_internal",
          parentCanvasId: state.activeCanvasId,
          parentPtsNodeId: ptsNode.id,
          nodes: [],
          edges: [],
        },
      };
      return {
        ...base,
        canvases: nextCanvases,
        selection: { nodeIds: [ptsNode.id], edgeIds: [], nodeId: ptsNode.id },
      };
    }),
  addBlankLciDataset: () =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const baseNode = buildBlankNode("lci_dataset", getAutoNodePosition(active), state.uiLanguage);
      const node = {
        ...baseNode,
        data: {
          ...baseNode.data,
          name: buildUniqueNodeName(active.nodes, baseNode.data.name, state.uiLanguage),
        },
      };
      return {
        ...updateActiveCanvas(state, (canvas) => ({
          ...canvas,
          nodes: [...canvas.nodes, node],
        })),
        selection: { nodeIds: [node.id], edgeIds: [], nodeId: node.id },
      };
    }),
  addBlankNodeAt: (kind, position) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const baseNode = buildBlankNode(kind, position, state.uiLanguage);
      const node = {
        ...baseNode,
        data: {
          ...baseNode.data,
          name: buildUniqueNodeName(active.nodes, baseNode.data.name, state.uiLanguage),
        },
      };
      const ptsCanvasId = kind === "pts_module" ? `canvas_${uid()}` : undefined;
      const nextNode =
        kind === "pts_module" && ptsCanvasId
          ? {
              ...node,
              data: {
                ...node.data,
                ptsCanvasId,
              },
            }
          : node;
      const base = updateActiveCanvas(state, (canvas) => ({
        ...canvas,
        nodes: [...canvas.nodes, nextNode],
      }));
      const nextCanvases =
        kind === "pts_module" && ptsCanvasId
          ? {
              ...base.canvases,
              [ptsCanvasId]: {
                id: ptsCanvasId,
                name: nextNode.data.name,
                kind: "pts_internal" as const,
                parentCanvasId: state.activeCanvasId,
                parentPtsNodeId: nextNode.id,
                nodes: [],
                edges: [],
              },
            }
          : undefined;
      return {
        ...(nextCanvases
          ? {
              ...base,
              canvases: nextCanvases,
            }
          : base),
        selection: { nodeIds: [nextNode.id], edgeIds: [], nodeId: nextNode.id },
        connectionHint: undefined,
      };
    }),
  openUnitProcessImportDialog: (position, targetKind = "unit_process") =>
    set(() => ({
      unitProcessImportDialog: {
        open: true,
        position,
        targetKind,
      },
    })),
  closeUnitProcessImportDialog: () =>
    set(() => ({
      unitProcessImportDialog: {
        open: false,
        targetKind: "unit_process",
      },
    })),
  addImportedUnitProcesses: (rows, position) => {
    const createdIds: string[] = [];
    set((state) => {
      if (rows.length === 0) {
        return state;
      }
      const active = state.canvases[state.activeCanvasId];
      const base = position ?? getAutoNodePosition(active);
      const importedNodes = rows.map((row, idx) => {
        const nextPos =
          position || rows.length > 1
            ? { x: base.x + (idx % 3) * 40, y: base.y + Math.floor(idx / 3) * 26 }
            : { x: base.x, y: base.y };
        const node = buildImportedUnitProcessNode(row, nextPos);
        createdIds.push(node.id);
        return node;
      });
      return {
        ...updateActiveCanvas(state, (canvas) => ({
          ...canvas,
          nodes: [...canvas.nodes, ...importedNodes],
        })),
        selection:
          createdIds.length > 0
            ? { nodeIds: createdIds, edgeIds: [], nodeId: createdIds[0] }
            : state.selection,
        connectionHint: undefined,
      };
    });
    return createdIds;
  },
  autoConnectByUuid: (options) =>
    set((state) => {
      const silentNoCandidate = Boolean(options?.silentNoCandidate);
      const silentSuccess = Boolean(options?.silentSuccess);
      const active = state.canvases[state.activeCanvasId];
      const workingNodeById = new Map(active.nodes.map((node) => [node.id, node]));
      let workingEdges = [...active.edges];
      const createdEdges: Array<Edge<LcaEdgeData>> = [];

      const getAllNodes = (): Array<Node<LcaNodeData>> => active.nodes.map((node) => workingNodeById.get(node.id) ?? node);
      const getNode = (nodeId: string): Node<LcaNodeData> | undefined => workingNodeById.get(nodeId);
      const setNode = (node: Node<LcaNodeData>) => {
        workingNodeById.set(node.id, node);
      };

      const ensureMarketTargetInput = (
        marketNode: Node<LcaNodeData>,
        sourcePort: FlowPort,
        sourceNode: Node<LcaNodeData>,
        sourceNodeId: string,
        sourceProcessName?: string,
        sourceProcessUuid?: string,
      ): { targetNode: Node<LcaNodeData>; targetPort: FlowPort } | undefined => {
        let targetNode = getNode(marketNode.id) ?? marketNode;
        const resolveSourceIdentityKey = (
          node: Node<LcaNodeData>,
          port: FlowPort,
          processUuid?: string,
        ): string => {
          const kind = node.data.nodeKind;
          if (kind === "pts_module") {
            return `pts:${node.id}:${port.id}`;
          }
          const processKey = (processUuid ?? node.data.processUuid ?? "").trim();
          if (processKey) {
            return `proc:${processKey}`;
          }
          return `node:${node.id}`;
        };
        const sourceIdentityKey = resolveSourceIdentityKey(sourceNode, sourcePort, sourceProcessUuid);
        if (sourceIdentityKey) {
          const parseOutputPortAny = (handleId?: string): string | undefined => {
            if (!handleId || typeof handleId !== "string") {
              return undefined;
            }
            if (handleId.startsWith("out:") || handleId.startsWith("outl:") || handleId.startsWith("outr:")) {
              return handleId.slice(handleId.indexOf(":") + 1);
            }
            return undefined;
          };
          const existingEdge = workingEdges.find((edge) => {
            if (edge.target !== targetNode.id || edge.data?.flowUuid !== sourcePort.flowUuid) {
              return false;
            }
            const sourceNode = getNode(edge.source);
            if (!sourceNode) {
              return false;
            }
            const edgeSourcePortId = parseOutputPortAny(edge.sourceHandle ?? undefined);
            const edgeSourcePort = edgeSourcePortId
              ? sourceNode.data.outputs.find((p) => p.id === edgeSourcePortId)
              : undefined;
            if (!edgeSourcePort) {
              return false;
            }
            return resolveSourceIdentityKey(sourceNode, edgeSourcePort, (sourceNode.data.processUuid ?? "").trim()) === sourceIdentityKey;
          });
          if (existingEdge) {
            const existingPortId = parseHandlePortId(existingEdge.targetHandle ?? undefined, "in:");
            const existingPort = existingPortId
              ? targetNode.data.inputs.find((port) => port.id === existingPortId && port.flowUuid === sourcePort.flowUuid)
              : undefined;
            if (existingPort) {
              return { targetNode, targetPort: existingPort };
            }
          }
        }
        let targetHandle = getTargetHandle(targetNode, sourcePort.flowUuid, workingEdges);
        const shouldCreateNewMarketInputRow = isMarketProcessNode(targetNode) && !targetHandle;
        let targetPortId = parseHandlePortId(targetHandle, "in:");
        let targetPort =
          shouldCreateNewMarketInputRow || !targetPortId
            ? undefined
            : targetNode.data.inputs.find((port) => port.id === targetPortId && port.flowUuid === sourcePort.flowUuid);
        if (!targetPort || shouldCreateNewMarketInputRow) {
          const sourceNode = getNode(sourceNodeId);
          const sourceHasSameFlowLinks = workingEdges.some(
            (edge) =>
              (edge.source === sourceNodeId || edge.target === sourceNodeId) &&
              edge.data?.flowUuid === sourcePort.flowUuid,
          );
          const shouldPrefillAmount = Boolean(sourceNode && isBalancedUnitNode(sourceNode));
          const autoPort: FlowPort = {
            ...buildAutoInputPort(sourcePort),
            amount: shouldPrefillAmount && !sourceHasSameFlowLinks ? (sourcePort.amount ?? 0) : 0,
          };
          const nextAutoPort = withMarketInputSourceIdentity(autoPort, {
            nodeId: sourceNodeId,
            processUuid: sourceProcessUuid,
            processName: sourceProcessName,
            portName: sourcePort.name,
            nestedProcessName: sourcePort.sourceProcessName,
          });
          targetNode = {
            ...targetNode,
            data: {
              ...targetNode.data,
              inputs: [...targetNode.data.inputs, nextAutoPort],
            },
          };
          setNode(targetNode);
          targetHandle = `in:${nextAutoPort.id}`;
          targetPortId = parseHandlePortId(targetHandle, "in:");
          targetPort = targetNode.data.inputs.find((port) => port.id === targetPortId);
        }
        if (!targetPort) {
          return undefined;
        }
        targetNode = {
          ...targetNode,
          data: {
            ...targetNode.data,
            inputs: targetNode.data.inputs.map((port) =>
              port.id === targetPort?.id
                ? withMarketInputSourceIdentity(port, {
                    nodeId: sourceNodeId,
                    processUuid: sourceProcessUuid,
                    processName: sourceProcessName,
                    portName: sourcePort.name,
                    nestedProcessName: sourcePort.sourceProcessName,
                  })
                : port),
          },
        };
        setNode(targetNode);
        targetPort = targetNode.data.inputs.find((port) => port.id === targetPort?.id) ?? targetPort;
        return { targetNode, targetPort };
      };

      const tryCreateEdge = (params: {
        sourceNode: Node<LcaNodeData>;
        sourcePort: FlowPort;
        targetNode: Node<LcaNodeData>;
        targetPort: FlowPort;
      }): Edge<LcaEdgeData> | undefined => {
        const { sourceNode, sourcePort, targetNode, targetPort } = params;
        const sourceProcessUuid = (sourceNode.data.processUuid ?? "").trim();
        const resolveSourceIdentityKey = (
          node: Node<LcaNodeData>,
          port: FlowPort,
          processUuid?: string,
        ): string => {
          if (node.data.nodeKind === "pts_module") {
            return `pts:${node.id}:${port.id}`;
          }
          const processKey = (processUuid ?? node.data.processUuid ?? "").trim();
          if (processKey) {
            return `proc:${processKey}`;
          }
          return `node:${node.id}`;
        };
        const sourceIdentityKey = resolveSourceIdentityKey(sourceNode, sourcePort, sourceProcessUuid);
        if (isMarketProcessNode(targetNode) && sourceIdentityKey) {
          const parseOutputPortAny = (handleId?: string): string | undefined => {
            if (!handleId || typeof handleId !== "string") {
              return undefined;
            }
            if (handleId.startsWith("out:") || handleId.startsWith("outl:") || handleId.startsWith("outr:")) {
              return handleId.slice(handleId.indexOf(":") + 1);
            }
            return undefined;
          };
          const duplicatedSourceProcess = workingEdges.some((edge) => {
            if (edge.target !== targetNode.id || edge.data?.flowUuid !== sourcePort.flowUuid) {
              return false;
            }
            const existingSource = getNode(edge.source);
            if (!existingSource) {
              return false;
            }
            const edgeSourcePortId = parseOutputPortAny(edge.sourceHandle ?? undefined);
            const edgeSourcePort = edgeSourcePortId
              ? existingSource.data.outputs.find((p) => p.id === edgeSourcePortId)
              : undefined;
            if (!edgeSourcePort) {
              return false;
            }
            return (
              resolveSourceIdentityKey(existingSource, edgeSourcePort, (existingSource.data.processUuid ?? "").trim()) ===
              sourceIdentityKey
            );
          });
          if (duplicatedSourceProcess) {
            return undefined;
          }
        }
        if (
          workingEdges.some(
            (edge) =>
              edge.source === sourceNode.id &&
              edge.target === targetNode.id &&
              edge.data?.flowUuid === sourcePort.flowUuid &&
              parseHandlePortId(edge.sourceHandle ?? undefined, "out:") === sourcePort.id,
          )
        ) {
          return undefined;
        }
        const ruleHint = getConnectionRuleHint({
          sourceNode,
          sourcePort,
          targetNode,
          targetPort,
          edges: workingEdges,
        });
        if (ruleHint) {
          return undefined;
        }

        const quantityMode = resolveQuantityMode(sourceNode, targetNode);
        const sourceAmount = sourcePort.amount ?? 0;
        const consumerAmount =
          isMarketProcessNode(targetNode) && targetNode.data.mode === "normalized" && quantityMode === "dual"
            ? resolveTargetInputAmount(targetNode, `in:${targetPort.id}`, sourcePort.flowUuid, sourceAmount)
            : sourceAmount;
        const autoHandles = pickShortestAutoHandles(sourceNode, sourcePort.id, targetNode, targetPort.id);
        const nextEdge: Edge<LcaEdgeData> = {
          id: `edge_${uid()}`,
          source: sourceNode.id,
          target: targetNode.id,
          sourceHandle: autoHandles.sourceHandle,
          targetHandle: autoHandles.targetHandle,
          type: "lcaExchange",
          data: {
            flowUuid: sourcePort.flowUuid,
            flowName: sourcePort.name,
            quantityMode,
            amount: consumerAmount,
            providerAmount: sourceAmount,
            consumerAmount,
            unit: sourcePort.unit,
            type: sourcePort.type,
            allocation: "physical",
            dbMapping: "",
          },
        };
        const normalized = normalizeEdgeWithNodeMap(nextEdge, workingNodeById);
        if (!normalized) {
          return undefined;
        }
        createdEdges.push(normalized);
        workingEdges = [...workingEdges, normalized];
        return normalized;
      };

      // Stage 1: for each flow UUID, if there is exactly one market process handling this UUID,
      // aggregate all product providers to that market input.
      const allNodesStage1 = getAllNodes();
      const allProductOutputs = allNodesStage1.flatMap((node) =>
        node.data.outputs
          .filter((port) => isProductIntermediate(port) && isUuidLike(port.flowUuid) && isPtsPortExposedToRoot(node, port))
          .map((port) => ({ node, port })),
      );
      const allFlowUuids = Array.from(new Set(allProductOutputs.map((item) => item.port.flowUuid)));
      for (const flowUuid of allFlowUuids) {
        const marketCandidates = getAllNodes().filter(
          (node) =>
            isMarketProcessNode(node) &&
            node.data.outputs.some(
              (port) => isProductIntermediate(port) && port.flowUuid === flowUuid && isPtsPortExposedToRoot(node, port),
            ),
        );
        if (marketCandidates.length !== 1) {
          continue;
        }
        const marketNode = marketCandidates[0];
        const providers = getAllNodes().flatMap((node) =>
          node.data.outputs
            .filter((port) => isProductIntermediate(port) && port.flowUuid === flowUuid && isPtsPortExposedToRoot(node, port))
            .map((port) => ({ node, port })),
        );
        for (const provider of providers) {
          if (provider.node.id === marketNode.id) {
            continue;
          }
          const resolvedTarget = ensureMarketTargetInput(
            marketNode,
            provider.port,
            provider.node,
            provider.node.id,
            provider.node.data.name,
            (provider.node.data.processUuid ?? "").trim(),
          );
          if (!resolvedTarget) {
            continue;
          }
          void tryCreateEdge({
            sourceNode: provider.node,
            sourcePort: provider.port,
            targetNode: resolvedTarget.targetNode,
            targetPort: resolvedTarget.targetPort,
          });
        }
      }

      type PortRef = { node: Node<LcaNodeData>; port: FlowPort };
      type NonProductRef = PortRef & { direction: "input" | "output" };

      const nonProductPorts: NonProductRef[] = [];
      for (const node of getAllNodes()) {
        for (const port of node.data.inputs) {
          if (isNonProductIntermediate(port) && isPtsPortExposedToRoot(node, port)) {
            nonProductPorts.push({ node, port, direction: "input" });
          }
        }
        for (const port of node.data.outputs) {
          if (isNonProductIntermediate(port) && isPtsPortExposedToRoot(node, port)) {
            nonProductPorts.push({ node, port, direction: "output" });
          }
        }
      }

      const isNonProductUnconnected = (ref: NonProductRef, edges: Array<Edge<LcaEdgeData>>): boolean => {
        if (ref.direction === "input") {
          return !edges.some(
            (edge) =>
              edge.target === ref.node.id && parseHandlePortId(edge.targetHandle ?? undefined, "in:") === ref.port.id,
          );
        }
        return !edges.some(
          (edge) =>
            edge.source === ref.node.id && parseHandlePortId(edge.sourceHandle ?? undefined, "out:") === ref.port.id,
        );
      };

      const getProductCandidates = (ref: NonProductRef): Array<{
        sourceNode: Node<LcaNodeData>;
        sourcePort: FlowPort;
        targetNode: Node<LcaNodeData>;
        targetPort: FlowPort;
      }> => {
        const flowUuid = ref.port.flowUuid;
        if (!flowUuid) {
          return [];
        }
        const providers: Array<{
          sourceNode: Node<LcaNodeData>;
          sourcePort: FlowPort;
          targetNode: Node<LcaNodeData>;
          targetPort: FlowPort;
        }> = [];
        if (ref.direction === "input") {
          for (const node of getAllNodes()) {
            for (const port of node.data.outputs) {
              if (!isProductIntermediate(port) || port.flowUuid !== flowUuid) {
                continue;
              }
              if (!isPtsPortExposedToRoot(node, port)) {
                continue;
              }
              providers.push({
                sourceNode: node,
                sourcePort: port,
                targetNode: ref.node,
                targetPort: ref.port,
              });
            }
          }
          return providers;
        }
        for (const node of getAllNodes()) {
          for (const port of node.data.inputs) {
            if (!isProductIntermediate(port) || port.flowUuid !== flowUuid) {
              continue;
            }
            if (!isPtsPortExposedToRoot(node, port)) {
              continue;
            }
            providers.push({
              sourceNode: ref.node,
              sourcePort: ref.port,
              targetNode: node,
              targetPort: port,
            });
          }
        }
        return providers;
      };

      const pickUniqueCandidate = (ref: NonProductRef, edges: Array<Edge<LcaEdgeData>>) => {
        const candidates = getProductCandidates(ref).filter((candidate) => {
          if (candidate.sourceNode.id === candidate.targetNode.id) {
            return false;
          }
          return !getConnectionRuleHint({
            sourceNode: candidate.sourceNode,
            sourcePort: candidate.sourcePort,
            targetNode: candidate.targetNode,
            targetPort: candidate.targetPort,
            edges,
          });
        });
        if (candidates.length === 0) {
          return undefined;
        }

        if (ref.direction === "input") {
          const marketOutputCandidates = candidates.filter((candidate) => isMarketProcessNode(candidate.sourceNode));
          if (marketOutputCandidates.length === 1) {
            return marketOutputCandidates[0];
          }
          if (marketOutputCandidates.length > 1) {
            return undefined;
          }
        }
        return candidates.length === 1 ? candidates[0] : undefined;
      };

      for (const ref of nonProductPorts) {
        if (!isNonProductUnconnected(ref, workingEdges)) {
          continue;
        }
        const selected = pickUniqueCandidate(ref, workingEdges);
        if (!selected) {
          continue;
        }
        void tryCreateEdge(selected);
      }

      if (createdEdges.length === 0) {
        return {
          connectionHint: silentNoCandidate
            ? state.connectionHint
            : state.uiLanguage === "zh"
              ? "自动连线未找到可连接的唯一候选。"
              : "No unique auto-connect candidates found.",
          connectionFix: undefined,
        };
      }

      const nextCanvas = repairCanvasEdgeHandles({
        ...active,
        nodes: getAllNodes(),
        edges: [...active.edges, ...createdEdges],
      });
      const nextCanvases = {
        ...state.canvases,
        [active.id]: nextCanvas,
      };
      return {
        ...setActiveCanvas(state, nextCanvases, state.activeCanvasId, true),
        connectionHint: silentSuccess
          ? state.connectionHint
          : state.uiLanguage === "zh"
            ? `自动连线完成：新增 ${createdEdges.length} 条。`
            : `Auto-connect complete: ${createdEdges.length} edge(s) added.`,
        connectionFix: undefined,
      };
    }),
  packageSelectionAsPts: () =>
    set((state) => {
      if (state.activeCanvasKind === "pts_internal") {
        return { connectionHint: "Packing is only allowed on the root product-system canvas." };
      }

      const active = state.canvases[state.activeCanvasId];
      const selectedSet = new Set(state.selection.nodeIds);
      const selectedNodes = active.nodes.filter((node) => selectedSet.has(node.id));

      if (selectedNodes.length < 2) {
        return { connectionHint: "至少选择 2 个节点后再封装为 PTS。" };
      }
      const unsupportedNodes = selectedNodes.filter(
        (node) =>
          node.data.nodeKind !== "unit_process" &&
          node.data.nodeKind !== "market_process" &&
          node.data.nodeKind !== "lci_dataset",
      );
      if (unsupportedNodes.length > 0) {
        if (unsupportedNodes.some((node) => node.data.nodeKind === "pts_module")) {
          return { connectionHint: "PTS 封装不支持包含 PTS 节点（暂不支持嵌套 PTS）。" };
        }
        return { connectionHint: "PTS 仅支持由单元过程、市场过程或 LCI 节点封装。" };
      }

      const internalEdges = active.edges.filter(
        (edge) => selectedSet.has(edge.source) && selectedSet.has(edge.target),
      );
      const incomingEdges = active.edges.filter(
        (edge) => !selectedSet.has(edge.source) && selectedSet.has(edge.target),
      );
      const outgoingEdges = active.edges.filter(
        (edge) => selectedSet.has(edge.source) && !selectedSet.has(edge.target),
      );
      const remainingEdges = active.edges.filter(
        (edge) => !selectedSet.has(edge.source) && !selectedSet.has(edge.target),
      );
      const remainingNodes = active.nodes.filter((node) => !selectedSet.has(node.id));

      const nodeById = new Map(active.nodes.map((node) => [node.id, node]));
      const normalizedInternalEdges = internalEdges
        .map((edge) => normalizeEdgeWithNodeMap(edge, nodeById))
        .filter((edge): edge is Edge<LcaEdgeData> => Boolean(edge));
      const normalizedIncomingEdges = incomingEdges
        .map((edge) => normalizeEdgeWithNodeMap(edge, nodeById))
        .filter((edge): edge is Edge<LcaEdgeData> => Boolean(edge));
      const normalizedOutgoingEdges = outgoingEdges
        .map((edge) => normalizeEdgeWithNodeMap(edge, nodeById))
        .filter((edge): edge is Edge<LcaEdgeData> => Boolean(edge));

      const invalidInternalEdge = normalizedInternalEdges.find((edge) => {
        const effectiveAmount = getEffectiveEdgeAmount(edge);
        return !Number.isFinite(effectiveAmount) || effectiveAmount <= 0;
      });
      if (invalidInternalEdge) {
        const flowName = invalidInternalEdge.data?.flowName || invalidInternalEdge.data?.flowUuid || "unknown_flow";
        return {
          connectionHint: `PTS 封装失败：内部边 ${invalidInternalEdge.id} 数量无效（${flowName}<=0），请先在目标输入清单或连线中设置 > 0。`,
        };
      }

      const sanitizedSelectedCanvas = pruneDetachedMarketInputs({
        ...active,
        nodes: selectedNodes,
        edges: normalizedInternalEdges,
      });
      const sanitizedSelectedNodes = sanitizedSelectedCanvas.nodes;
      const sanitizedNodeById = new Map(sanitizedSelectedNodes.map((node) => [node.id, node]));
      const sanitizedIncomingEdges = filterDetachedMarketIncomingEdgesForPtsPackaging(
        normalizedIncomingEdges,
        sanitizedSelectedNodes,
      );

      const ptsInputByFlow = new Map<string, FlowPort>();
      const ptsInputs: FlowPort[] = [];
      sanitizedIncomingEdges.forEach((edge, index) => {
        const sourceNode = nodeById.get(edge.source);
        const sourcePort = resolveEdgeBoundaryPort(nodeById, edge, "source");
        const flowUuid = String(edge.data?.flowUuid ?? "").trim();
        const sourceProcessUuid = String(sourceNode?.data.processUuid ?? "").trim();
        const sourceNodeId = String(sourceNode?.id ?? "").trim();
        const sourceProcessName = String(sourceNode?.data.name ?? "").trim();
        const key =
          flowUuid && sourceProcessUuid
            ? `${flowUuid}@@${sourceProcessUuid}`
            : flowUuid && sourceNodeId
              ? `${flowUuid}@@node:${sourceNodeId}`
              : flowUuid && sourceProcessName
                ? `${flowUuid}@@name:${sourceProcessName}`
                : flowUuid;
        if (!key) {
          return;
        }
        let port = ptsInputByFlow.get(key);
        if (!port) {
          const base = toPtsPort(edge, "input", index + 1);
          port = {
            ...base,
            flowNameEn: sourcePort?.flowNameEn ?? base.flowNameEn,
            sourceProcessUuid: sourceProcessUuid || undefined,
            sourceProcessName: sourceProcessName || undefined,
            sourceNodeId: sourceNodeId || undefined,
          };
          ptsInputByFlow.set(key, port);
          ptsInputs.push(port);
        }
      });

      const ptsOutputByIdentity = new Map<string, FlowPort>();
      const ptsOutputs: FlowPort[] = [];
      normalizedOutgoingEdges.forEach((edge, index) => {
        const sourceNode = sanitizedNodeById.get(edge.source);
        const sourcePort = resolveEdgeBoundaryPort(nodeById, edge, "source");
        const flowUuid = String(edge.data?.flowUuid ?? "").trim();
        if (!flowUuid) {
          return;
        }
        const key = `${flowUuid}@@${String(sourceNode?.data.processUuid ?? "").trim()}`;
        let port = ptsOutputByIdentity.get(key);
        if (!port) {
          const base = toPtsPort(edge, "output", index + 1);
          port = {
            ...base,
            flowNameEn: sourcePort?.flowNameEn ?? base.flowNameEn,
            sourceProcessUuid: sourceNode?.data.processUuid,
            sourceProcessName: sourceNode?.data.name,
            sourceNodeId: sourceNode?.id,
          };
          ptsOutputByIdentity.set(key, port);
          ptsOutputs.push(port);
        }
      });

      const appendPtsBoundaryCandidates = (
        current: FlowPort[],
        direction: "input" | "output",
      ): FlowPort[] => {
        const byKey = new Map(current.map((port) => [getPtsPortIdentityKey(direction, port), port]));
        for (const node of sanitizedSelectedNodes) {
          const ports = direction === "input" ? node.data.inputs : node.data.outputs;
          for (const port of ports) {
            if (port.type === "biosphere" || !port.flowUuid) {
              continue;
            }
            const candidateKey =
              direction === "output"
                ? `${port.flowUuid}@@${node.data.processUuid || node.id}`
                : getPtsPortIdentityKey(direction, port);
            if (byKey.has(candidateKey)) {
              continue;
            }
            byKey.set(candidateKey, {
              id: `${direction}_${uid().slice(0, 8)}`,
              flowUuid: port.flowUuid,
              name: port.name,
              flowNameEn: port.flowNameEn,
              sourceProcessUuid: direction === "output" ? (node.data.processUuid || undefined) : undefined,
              sourceProcessName: direction === "output" ? node.data.name : undefined,
              sourceNodeId: direction === "output" ? node.id : undefined,
              exposureMode: "boundary_only",
              unit: port.unit || "kg",
              unitGroup: port.unitGroup,
              amount: 0,
              isProduct: Boolean(port.isProduct) && direction === "output",
              externalSaleAmount: 0,
              type: port.type,
              direction,
              showOnNode: false,
              internalExposed: true,
            });
          }
        }
        return Array.from(byKey.values());
      };

      const nextPtsInputs = appendPtsBoundaryCandidates(ptsInputs, "input");
      const nextPtsOutputs = appendPtsBoundaryCandidates(ptsOutputs, "output");

      const avgX = sanitizedSelectedNodes.reduce((sum, node) => sum + node.position.x, 0) / sanitizedSelectedNodes.length;
      const avgY = sanitizedSelectedNodes.reduce((sum, node) => sum + node.position.y, 0) / sanitizedSelectedNodes.length;

      const ptsCanvasId = `canvas_${uid()}`;
      const ptsNodeId = `node_${uid()}`;
      const ptsUuid = `pts_${uid().slice(0, 8)}`;

      const ptsNode: Node<LcaNodeData> = {
        id: ptsNodeId,
        type: "lcaProcess",
        position: { x: avgX, y: avgY },
        data: {
          nodeKind: "pts_module",
          mode: "normalized",
          ptsUuid,
          ptsCanvasId,
          processUuid: ptsUuid,
          name: buildUniqueNodeName(remainingNodes, "Packed PTS", state.uiLanguage),
          location: "Plant Internal",
          referenceProduct: "PTS模块输出",
          inputs: nextPtsInputs,
          outputs: nextPtsOutputs,
        },
      };

      if (typeof console !== "undefined") {
        console.debug("[PTS_DEBUG] packageSelectionAsPts:created", {
          ptsNodeId,
          ptsUuid,
          inputs: nextPtsInputs.map((port) => ({
            id: port.id,
            flowUuid: port.flowUuid,
            name: port.name,
            type: port.type,
            isProduct: port.isProduct,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
          })),
          outputs: nextPtsOutputs.map((port) => ({
            id: port.id,
            flowUuid: port.flowUuid,
            name: port.name,
            type: port.type,
            isProduct: port.isProduct,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
            sourceProcessUuid: port.sourceProcessUuid,
            sourceNodeId: port.sourceNodeId,
          })),
        });
      }

      const nextCanvases: Record<string, CanvasGraph> = {
        ...state.canvases,
        [state.activeCanvasId]: {
          ...active,
          nodes: [...remainingNodes, ptsNode],
          edges: remainingEdges,
        },
        [ptsCanvasId]: {
          id: ptsCanvasId,
          name: ptsNode.data.name,
          kind: "pts_internal",
          parentCanvasId: state.activeCanvasId,
          parentPtsNodeId: ptsNodeId,
          nodes: sanitizedSelectedNodes,
          edges: normalizedInternalEdges,
        },
      };

      const next = setActiveCanvas(state, nextCanvases, state.activeCanvasId, false);
      return {
        ...next,
        selection: { nodeIds: [ptsNodeId], edgeIds: [], nodeId: ptsNodeId },
        connectionHint: undefined,
        // Packaging already rewires boundary edges; triggering root auto-connect here
        // only re-enters the PTS refresh chain before the new resource is finalized.
        pendingAutoConnect: false,
        pendingPtsCompileNodeId: ptsNodeId,
      };
    }),
  unpackPtsNode: (nodeId) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const targetPtsNodeId = nodeId ?? state.selection.nodeId;
      if (!targetPtsNodeId) {
        return { connectionHint: "请选择一个 PTS 节点后再解封。" };
      }

      const ptsNode = active.nodes.find((n) => n.id === targetPtsNodeId);
      if (!ptsNode || ptsNode.data.nodeKind !== "pts_module") {
        return { connectionHint: "仅支持解封 PTS 节点。" };
      }
      if (!ptsNode.data.ptsCanvasId) {
        return { connectionHint: "PTS 未绑定内部画布，无法解封。" };
      }

      const internalCanvas = state.canvases[ptsNode.data.ptsCanvasId];
      if (!internalCanvas) {
        return { connectionHint: "PTS 内部画布不存在，无法解封。" };
      }

      const internalNodes = internalCanvas.nodes;
      const internalEdges = internalCanvas.edges;
      if (internalNodes.length === 0) {
        return { connectionHint: "PTS 内部没有节点，无法解封。" };
      }

      const avgX = internalNodes.reduce((sum, n) => sum + n.position.x, 0) / internalNodes.length;
      const avgY = internalNodes.reduce((sum, n) => sum + n.position.y, 0) / internalNodes.length;
      const dx = ptsNode.position.x - avgX;
      const dy = ptsNode.position.y - avgY;

      const restoredNodes = internalNodes.map((node) => ({
        ...node,
        position: {
          x: node.position.x + dx,
          y: node.position.y + dy,
        },
      }));
      const restoredNodeById = new Map(restoredNodes.map((node) => [node.id, node]));
      const outerNodesWithoutPts = active.nodes.filter((node) => node.id !== ptsNode.id);
      const rewireNodeById = new Map([...outerNodesWithoutPts, ...restoredNodes].map((node) => [node.id, node]));

      const ptsIncomingEdges = active.edges.filter((e) => e.target === ptsNode.id);
      const ptsOutgoingEdges = active.edges.filter((e) => e.source === ptsNode.id);
      const unaffectedEdges = active.edges.filter((e) => e.source !== ptsNode.id && e.target !== ptsNode.id);

      const normalizedInternalEdges = internalEdges
        .map((edge) => normalizeEdgeWithNodeMap(edge, restoredNodeById))
        .filter((edge): edge is Edge<LcaEdgeData> => Boolean(edge));
      let workingEdges: Array<Edge<LcaEdgeData>> = [...unaffectedEdges, ...normalizedInternalEdges];

      const rewiredIncoming: Array<Edge<LcaEdgeData>> = [];
      for (const edge of ptsIncomingEdges) {
        const flowUuid = edge.data?.flowUuid ?? "";
        if (!flowUuid) {
          continue;
        }
        const sourceNode = rewireNodeById.get(edge.source);
        const sourcePort = findOutputPortByHandleOrFlow(sourceNode, edge.sourceHandle ?? undefined, flowUuid);
        if (!sourceNode || !sourcePort) {
          continue;
        }

        const targetCandidates = restoredNodes.flatMap((node) =>
          node.data.inputs
            .filter((port) => port.flowUuid === flowUuid)
            .map((port) => ({
              node,
              port,
              incomingCount: workingEdges.filter(
                (item) => item.target === node.id && item.data?.flowUuid === flowUuid,
              ).length,
            })),
        );
        targetCandidates.sort((a, b) => a.incomingCount - b.incomingCount);
        const targetRef = targetCandidates.find(
          (candidate) =>
            !getConnectionRuleHint({
              sourceNode,
              sourcePort,
              targetNode: candidate.node,
              targetPort: candidate.port,
              edges: workingEdges,
            }),
        );
        if (!targetRef) {
          continue;
        }
        const resolvedHandles = pickShortestAutoHandles(
          sourceNode,
          sourcePort.id,
          targetRef.node,
          targetRef.port.id,
        );
        const rewiredEdge = normalizeEdgeWithNodeMap(
          {
            ...edge,
            id: `edge_${uid()}`,
            sourceHandle: resolvedHandles.sourceHandle,
            target: targetRef.node.id,
            targetHandle: resolvedHandles.targetHandle,
          },
          rewireNodeById,
        );
        if (!rewiredEdge) {
          continue;
        }
        rewiredIncoming.push(rewiredEdge);
        workingEdges.push(rewiredEdge);
      }

      const rewiredOutgoing: Array<Edge<LcaEdgeData>> = [];
      for (const edge of ptsOutgoingEdges) {
        const flowUuid = edge.data?.flowUuid ?? "";
        if (!flowUuid) {
          continue;
        }
        const targetNode = rewireNodeById.get(edge.target);
        const targetPort = findInputPortByHandleOrFlow(targetNode, edge.targetHandle ?? undefined, flowUuid);
        if (!targetNode || !targetPort) {
          continue;
        }
        const sourceCandidates = restoredNodes.flatMap((node) =>
          node.data.outputs
            .filter((port) => port.flowUuid === flowUuid)
            .map((port) => ({
              node,
              port,
              outgoingCount: workingEdges.filter(
                (item) => item.source === node.id && item.data?.flowUuid === flowUuid,
              ).length,
            })),
        );
        sourceCandidates.sort((a, b) => a.outgoingCount - b.outgoingCount);
        const sourceRef = sourceCandidates.find(
          (candidate) =>
            !getConnectionRuleHint({
              sourceNode: candidate.node,
              sourcePort: candidate.port,
              targetNode,
              targetPort,
              edges: workingEdges,
            }),
        );
        if (!sourceRef) {
          continue;
        }
        const resolvedHandles = pickShortestAutoHandles(
          sourceRef.node,
          sourceRef.port.id,
          targetNode,
          targetPort.id,
        );
        const rewiredEdge = normalizeEdgeWithNodeMap(
          {
            ...edge,
            id: `edge_${uid()}`,
            source: sourceRef.node.id,
            sourceHandle: resolvedHandles.sourceHandle,
            targetHandle: resolvedHandles.targetHandle,
          },
          rewireNodeById,
        );
        if (!rewiredEdge) {
          continue;
        }
        rewiredOutgoing.push(rewiredEdge);
        workingEdges.push(rewiredEdge);
      }

      const nextActive: CanvasGraph = {
        ...active,
        nodes: [...active.nodes.filter((node) => node.id !== ptsNode.id), ...restoredNodes],
        edges: [...unaffectedEdges, ...normalizedInternalEdges, ...rewiredIncoming, ...rewiredOutgoing],
      };

      const nextCanvases = { ...state.canvases };
      nextCanvases[active.id] = nextActive;
      delete nextCanvases[internalCanvas.id];

      const next = setActiveCanvas(state, nextCanvases, active.id, false);
      return {
        ...next,
        selection: {
          nodeIds: restoredNodes.map((n) => n.id),
          edgeIds: [],
          nodeId: restoredNodes[0]?.id,
        },
        connectionHint: "PTS解封完成。",
        pendingAutoConnect: true,
      };
    }),
  enterPtsNode: (nodeId) =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      const node = active.nodes.find((item) => item.id === nodeId);
      if (!node || node.data.nodeKind !== "pts_module") {
        return state;
      }

      const resolvedInternal = resolvePtsInternalCanvas(state.canvases, nodeId, node.data.ptsCanvasId);
      if (!resolvedInternal) {
        const ptsCanvasId = `canvas_${uid()}`;
        const nextCanvases: Record<string, CanvasGraph> = {
          ...state.canvases,
          [state.activeCanvasId]: {
            ...active,
            nodes: active.nodes.map((n) =>
              n.id === nodeId
                ? {
                    ...n,
                    data: {
                      ...n.data,
                      ptsCanvasId,
                    },
                  }
                : n,
            ),
          },
          [ptsCanvasId]: {
            id: ptsCanvasId,
            name: node.data.name,
            kind: "pts_internal",
            parentCanvasId: state.activeCanvasId,
            parentPtsNodeId: nodeId,
            nodes: [],
            edges: [],
          },
        };
        return {
          ...setActiveCanvas(state, nextCanvases, ptsCanvasId, false),
          inspectorOpen: false,
          connectionHint: undefined,
        };
      }

      const targetCanvas = resolvedInternal;
      const nextCanvases =
        node.data.ptsCanvasId === targetCanvas.id
          ? state.canvases
          : {
              ...state.canvases,
              [state.activeCanvasId]: {
                ...active,
                nodes: active.nodes.map((n) =>
                  n.id === nodeId
                    ? {
                        ...n,
                        data: {
                          ...n.data,
                          ptsCanvasId: targetCanvas.id,
                        },
                      }
                    : n,
                ),
              },
            };
      return {
        ...setActiveCanvas(state, nextCanvases, targetCanvas.id, false),
        inspectorOpen: false,
      };
    }),
  goToParentCanvas: () =>
    set((state) => {
      const active = state.canvases[state.activeCanvasId];
      if (!active.parentCanvasId) {
        return state;
      }
      return {
        ...setActiveCanvas(state, state.canvases, active.parentCanvasId, false),
        inspectorOpen: false,
      };
    }),
  updatePtsPublishedBinding: (ptsNodeId, binding) =>
    set((state) => {
      const root = state.canvases[ROOT_CANVAS_ID];
      const nextRoot: CanvasGraph = {
        ...root,
        nodes: root.nodes.map((node) =>
          node.id === ptsNodeId && node.data.nodeKind === "pts_module"
            ? {
                ...node,
                data: {
                  ...node.data,
                  ptsPublishedVersion: binding.publishedVersion,
                  ptsPublishedArtifactId: binding.publishedArtifactId,
                },
              }
            : node,
        ),
      };
      const nextCanvases = {
        ...state.canvases,
        [ROOT_CANVAS_ID]: nextRoot,
      };
      if (state.activeCanvasId === ROOT_CANVAS_ID) {
        return {
          ...setActiveCanvas(state, nextCanvases, ROOT_CANVAS_ID, true),
        };
      }
      return {
        canvases: nextCanvases,
      };
    }),
  replaceRootPtsShell: (ptsNodeId, shell) =>
    set((state) => {
      if (typeof console !== "undefined") {
        console.debug("[PTS_DEBUG] replaceRootPtsShell", {
          ptsNodeId,
          publishedVersion: shell.publishedVersion,
          publishedArtifactId: shell.publishedArtifactId,
          inputs: (shell.inputs ?? []).map((port) => ({
            id: port.id,
            flowUuid: port.flowUuid,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
          })),
          outputs: (shell.outputs ?? []).map((port) => ({
            id: port.id,
            flowUuid: port.flowUuid,
            showOnNode: port.showOnNode,
            internalExposed: port.internalExposed,
            isProduct: port.isProduct,
          })),
        });
      }
      const root = state.canvases[ROOT_CANVAS_ID];
      const nextRoot: CanvasGraph = {
        ...root,
        nodes: root.nodes.map((node) =>
          node.id === ptsNodeId && node.data.nodeKind === "pts_module"
            ? {
                ...node,
                data: {
                  ...node.data,
                  nodeKind: shell.nodeKind ?? node.data.nodeKind,
                  mode: shell.mode ?? node.data.mode,
                  ptsUuid: shell.ptsUuid ?? node.data.ptsUuid,
                  processUuid: shell.processUuid ?? node.data.processUuid,
                  name: shell.name ?? node.data.name,
                  location: shell.location ?? node.data.location,
                  referenceProduct: shell.referenceProduct ?? node.data.referenceProduct,
                  referenceProductFlowUuid:
                    shell.referenceProductFlowUuid ?? node.data.referenceProductFlowUuid,
                  referenceProductDirection:
                    shell.referenceProductDirection ?? node.data.referenceProductDirection,
                  inputs: shell.inputs,
                  outputs: shell.outputs,
                  ptsPublishedVersion: shell.publishedVersion ?? node.data.ptsPublishedVersion,
                  ptsPublishedArtifactId: shell.publishedArtifactId ?? node.data.ptsPublishedArtifactId,
                },
              }
            : node,
        ),
      };
      const nextCanvases = {
        ...state.canvases,
        [ROOT_CANVAS_ID]: nextRoot,
      };
      if (state.activeCanvasId === ROOT_CANVAS_ID) {
        return {
          ...setActiveCanvas(state, nextCanvases, ROOT_CANVAS_ID, true),
        };
      }
      return {
        canvases: nextCanvases,
      };
    }),
  replacePtsInternalCanvasGraph: (ptsNodeId, graph, options) =>
    set((state) => {
      const root = state.canvases[ROOT_CANVAS_ID];
      const ptsNode = root.nodes.find((node) => node.id === ptsNodeId && node.data.nodeKind === "pts_module");
      if (!ptsNode) {
        return state;
      }
      const uiLanguage = state.uiLanguage;
      const existingCanvas = resolvePtsInternalCanvas(state.canvases, ptsNodeId, ptsNode.data.ptsCanvasId);
      const canvasId = existingCanvas?.id ?? ptsNode.data.ptsCanvasId ?? `canvas_${uid()}`;
      const nodePositionsRaw = (graph.metadata as { node_positions?: Record<string, unknown> } | undefined)?.node_positions;
      const edgeFlowNameEnByUuid = new Map<string, string>();
      (graph.exchanges ?? []).forEach((edge) => {
        const rawEdge = edge as Record<string, unknown>;
        const flowUuid = String(edge.flowUuid ?? rawEdge.flow_uuid ?? "").trim();
        const flowNameEn = String(rawEdge.flow_name_en ?? rawEdge.flowNameEn ?? "").trim();
        if (flowUuid && flowNameEn && !edgeFlowNameEnByUuid.has(flowUuid)) {
          edgeFlowNameEnByUuid.set(flowUuid, flowNameEn);
        }
      });
      const deserializeNode = (
        node: {
          id: string;
          node_kind: LcaNodeData["nodeKind"];
          mode?: "balanced" | "normalized";
          market_allow_mixed_flows?: boolean;
          lci_role?: LcaNodeData["lciRole"];
          import_mode?: ProcessImportMode;
          source_process_uuid?: string;
          import_warnings?: string[];
          filtered_exchanges?: FilteredExchangeEvidence[];
          pts_uuid?: string;
          pts_canvas_id?: string;
          pts_published_version?: number;
          pts_published_artifact_id?: string;
          process_uuid?: string;
          name: string;
          location: string;
          reference_product: string;
          reference_product_flow_uuid?: string;
          reference_product_direction?: "input" | "output";
          inputs?: FlowPort[];
          outputs?: FlowPort[];
          position?: XYPosition;
        },
      ): Node<LcaNodeData> => {
        const rawNode = node as Record<string, unknown>;
        const normalized = normalizeProductFlags({
          nodeKind: node.node_kind,
          referenceProductFlowUuid: node.reference_product_flow_uuid,
          referenceProductDirection: node.reference_product_direction,
          inputs: node.inputs ?? [],
          outputs: node.outputs ?? [],
        });
        const patchFlowNameEn = (ports: FlowPort[]) =>
          ports.map((port) => {
            if (String(port.flowNameEn ?? "").trim()) {
              return port;
            }
            const flowUuid = String(port.flowUuid ?? "").trim();
            const flowNameEn = flowUuid ? String(edgeFlowNameEnByUuid.get(flowUuid) ?? "").trim() : "";
            return flowNameEn ? { ...port, flowNameEn } : port;
          });
        const nameZh = String(node.name ?? "").trim();
        const nameEn = String(rawNode.process_name_en ?? rawNode.name_en ?? rawNode.nameEn ?? "").trim();
        const referenceProductZh = String(node.reference_product ?? "").trim();
        const referenceProductEn = String(
          rawNode.reference_product_en ?? rawNode.referenceProductEn ?? "",
        ).trim();
        return sanitizeMarketNode({
          id: node.id,
          type: "lcaProcess",
          position: isValidPosition(node.position)
            ? node.position
            : isValidPosition(nodePositionsRaw?.[node.id])
              ? (nodePositionsRaw?.[node.id] as XYPosition)
              : toStablePosition(node.id),
          data: {
            nodeKind: node.node_kind,
            mode: node.mode ?? (node.node_kind === "unit_process" ? "balanced" : "normalized"),
            marketAllowMixedFlows: node.market_allow_mixed_flows,
            lciRole: node.lci_role,
            importMode: node.import_mode,
            sourceProcessUuid: node.source_process_uuid,
            importWarnings: node.import_warnings,
            filteredExchanges: node.filtered_exchanges,
            ptsUuid: node.pts_uuid,
            ptsCanvasId: node.pts_canvas_id,
            ptsPublishedVersion: node.pts_published_version,
            ptsPublishedArtifactId: node.pts_published_artifact_id,
            processUuid: node.process_uuid ?? node.pts_uuid ?? `proc_${node.id}`,
            name: uiLanguage === "en" ? nameEn || nameZh : nameZh || nameEn,
            location: node.location,
            referenceProduct: uiLanguage === "en" ? referenceProductEn || referenceProductZh : referenceProductZh || referenceProductEn,
            referenceProductFlowUuid: node.reference_product_flow_uuid,
            referenceProductDirection: node.reference_product_direction,
            inputs: patchFlowNameEn(normalized.inputs),
            outputs: patchFlowNameEn(normalized.outputs),
          },
        });
      };
      const deserializeEdge = (edge: {
          id: string;
          fromNode: string;
          toNode: string;
          source_port_id?: string;
          target_port_id?: string;
          sourceHandle?: string;
          targetHandle?: string;
          flowUuid: string;
        flowName: string;
        flowNameEn?: string;
        quantityMode?: "single" | "dual";
        amount: number;
        providerAmount?: number;
        consumerAmount?: number;
        unit: string;
        type: LcaEdgeData["type"];
        allocation: "physical" | "economic" | "none";
        dbMapping?: string;
          }): Edge<LcaEdgeData> => {
            const rawEdge = edge as Record<string, unknown>;
            return {
              id: edge.id,
              source: edge.fromNode,
              target: edge.toNode,
              sourceHandle: toCanvasHandleId(edge.sourceHandle ?? edge.source_port_id ?? undefined, "output"),
              targetHandle: toCanvasHandleId(edge.targetHandle ?? edge.target_port_id ?? undefined, "input"),
              type: "lcaExchange",
              data: {
                flowUuid: edge.flowUuid,
                flowName: edge.flowName,
                flowNameEn: String(edge.flowNameEn ?? rawEdge.flow_name_en ?? "").trim() || undefined,
                quantityMode: edge.quantityMode ?? "single",
                amount: edge.amount,
                providerAmount: edge.providerAmount,
                consumerAmount: edge.consumerAmount,
                unit: edge.unit,
                type: edge.type,
                allocation: edge.allocation,
                dbMapping: edge.dbMapping,
              },
            };
          };

      const nextCanvas = repairCanvasEdgeHandles({
        id: canvasId,
        name: options?.name ?? existingCanvas?.name ?? ptsNode.data.name,
        kind: "pts_internal",
        parentCanvasId: existingCanvas?.parentCanvasId ?? ROOT_CANVAS_ID,
        parentPtsNodeId: ptsNodeId,
        nodes: (graph.nodes ?? []).map((node) => deserializeNode(node as Parameters<typeof deserializeNode>[0])),
        edges: (graph.exchanges ?? []).map((edge) => deserializeEdge(edge as Parameters<typeof deserializeEdge>[0])),
      });
      const nextRoot: CanvasGraph = {
        ...root,
        nodes: root.nodes.map((node) =>
          node.id === ptsNodeId
            ? {
                ...node,
                data: {
                  ...node.data,
                  ptsCanvasId: canvasId,
                },
              }
            : node,
        ),
      };
      const nextCanvases = {
        ...state.canvases,
        [ROOT_CANVAS_ID]: nextRoot,
        [canvasId]: nextCanvas,
      };
      if (state.activeCanvasId === canvasId) {
        return {
          ...setActiveCanvas(state, nextCanvases, canvasId, true),
        };
      }
      return {
        canvases: nextCanvases,
      };
    }),
  exportGraph: () => {
    const state = get();
    const active = state.canvases[ROOT_CANVAS_ID];
    const toExportNodeMap = (
      serializedNodes: Array<{
        id: string;
        node_kind: LcaNodeData["nodeKind"];
        mode?: "balanced" | "normalized";
        market_allow_mixed_flows?: boolean;
        lci_role?: LcaNodeData["lciRole"];
        import_mode?: ProcessImportMode;
        source_process_uuid?: string;
        import_warnings?: string[];
        filtered_exchanges?: FilteredExchangeEvidence[];
        pts_uuid?: string;
        pts_canvas_id?: string;
        process_uuid?: string;
        name: string;
        location: string;
        reference_product: string;
        reference_product_flow_uuid?: string;
        reference_product_direction?: "input" | "output";
        inputs?: FlowPort[];
        outputs?: FlowPort[];
        position?: XYPosition;
      }>,
    ): Map<string, Node<LcaNodeData>> =>
      new Map(
        serializedNodes.map((node) => [
          node.id,
          {
            id: node.id,
            type: "lcaProcess",
            position: node.position ?? toStablePosition(node.id),
            data: {
              nodeKind: node.node_kind,
              mode: node.mode ?? (node.node_kind === "unit_process" ? "balanced" : "normalized"),
              marketAllowMixedFlows: node.market_allow_mixed_flows,
              lciRole: node.lci_role,
              importMode: node.import_mode,
              sourceProcessUuid: node.source_process_uuid,
              importWarnings: node.import_warnings,
              filteredExchanges: node.filtered_exchanges,
              ptsUuid: node.pts_uuid,
              ptsCanvasId: node.pts_canvas_id,
              processUuid: node.process_uuid ?? node.pts_uuid ?? `proc_${node.id}`,
              name: node.name,
              location: node.location,
              referenceProduct: node.reference_product,
              referenceProductFlowUuid: node.reference_product_flow_uuid,
              referenceProductDirection: node.reference_product_direction,
              inputs: node.inputs ?? [],
              outputs: node.outputs ?? [],
            },
          } satisfies Node<LcaNodeData>,
        ]),
      );

    const normalizeCanvasEdgesForExport = (
      canvas: CanvasGraph,
      serializedNodes?: Array<{
        id: string;
        node_kind: LcaNodeData["nodeKind"];
        mode?: "balanced" | "normalized";
        market_allow_mixed_flows?: boolean;
        lci_role?: LcaNodeData["lciRole"];
        import_mode?: ProcessImportMode;
        source_process_uuid?: string;
        import_warnings?: string[];
        filtered_exchanges?: FilteredExchangeEvidence[];
        pts_uuid?: string;
        pts_canvas_id?: string;
        process_uuid?: string;
        name: string;
        location: string;
        reference_product: string;
        reference_product_flow_uuid?: string;
        reference_product_direction?: "input" | "output";
        inputs?: FlowPort[];
        outputs?: FlowPort[];
        position?: XYPosition;
      }>,
    ): Array<Edge<LcaEdgeData>> => {
      const nodeById = serializedNodes ? toExportNodeMap(serializedNodes) : new Map(canvas.nodes.map((node) => [node.id, node]));
      return canvas.edges
        .map((edge) => normalizeEdgeWithNodeMap(edge, nodeById))
        .filter((edge): edge is Edge<LcaEdgeData> => Boolean(edge));
    };
    const nodePositions = Object.fromEntries(
      active.nodes.map((node) => [
        node.id,
        {
          x: node.position.x,
          y: node.position.y,
        },
      ]),
    );
      const serializePort = (port: FlowPort) => ({
        ...port,
        flow_name_en: port.flowNameEn,
        display_name_en: port.displayNameEn,
      });
    const serializeNode = (node: Node<LcaNodeData>) => {
      const exportInputs = node.data.inputs;
      const exportOutputs = node.data.outputs;
      const normalized = normalizeProductFlags({
        nodeKind: node.data.nodeKind,
        referenceProductFlowUuid: node.data.referenceProductFlowUuid,
        referenceProductDirection: node.data.referenceProductDirection,
        inputs: exportInputs,
        outputs: exportOutputs,
      });
      return {
        id: node.id,
        node_kind: node.data.nodeKind,
        mode: node.data.mode,
        market_allow_mixed_flows: node.data.marketAllowMixedFlows,
        lci_role: node.data.lciRole,
        import_mode: node.data.importMode,
        source_process_uuid: node.data.sourceProcessUuid,
        import_warnings: node.data.importWarnings,
        filtered_exchanges: node.data.filteredExchanges,
        pts_uuid: node.data.ptsUuid,
        pts_canvas_id: node.data.ptsCanvasId,
        pts_published_version: node.data.ptsPublishedVersion,
        pts_published_artifact_id: node.data.ptsPublishedArtifactId,
        process_uuid: node.data.processUuid,
        name: node.data.name,
        location: node.data.location,
        reference_product: node.data.referenceProduct,
        reference_product_flow_uuid: node.data.referenceProductFlowUuid,
        reference_product_direction: node.data.referenceProductDirection,
        inputs: normalized.inputs.map(serializePort),
        outputs: normalized.outputs.map(serializePort),
        position: {
          x: node.position.x,
          y: node.position.y,
        },
      };
    };
    const serializeEdge = (edge: Edge<LcaEdgeData>) => ({
        id: edge.id,
        fromNode: edge.source,
        toNode: edge.target,
        source_port_id: toBackendPortId(edge.sourceHandle ?? undefined, "output"),
        target_port_id: toBackendPortId(edge.targetHandle ?? undefined, "input"),
        sourceHandle: edge.sourceHandle ?? undefined,
        targetHandle: edge.targetHandle ?? undefined,
        flowUuid: edge.data?.flowUuid ?? "",
        flowName: edge.data?.flowName ?? "",
        flowNameEn: edge.data?.flowNameEn,
        flow_name_en: edge.data?.flowNameEn,
        quantityMode: edge.data?.quantityMode ?? "single",
      amount: edge.data?.amount ?? 0,
      providerAmount: edge.data?.providerAmount,
      consumerAmount: edge.data?.consumerAmount,
      unit: edge.data?.unit ?? "kg",
      type: edge.data?.type ?? "technosphere",
      allocation: edge.data?.allocation ?? "none",
      dbMapping: edge.data?.dbMapping,
    });
      const serializedRootNodes = active.nodes.map((node) => serializeNode(node));
      const rootExportEdges = normalizeCanvasEdgesForExport(active, serializedRootNodes);
      const exchanges: LcaExchange[] = rootExportEdges.map((edge) => ({
        id: edge.id,
        fromNode: edge.source,
        toNode: edge.target,
        source_port_id: toBackendPortId(edge.sourceHandle ?? undefined, "output"),
        target_port_id: toBackendPortId(edge.targetHandle ?? undefined, "input"),
        sourceHandle: edge.sourceHandle ?? undefined,
        targetHandle: edge.targetHandle ?? undefined,
        flowUuid: edge.data?.flowUuid ?? "",
        flowName: edge.data?.flowName ?? "",
        flowNameEn: edge.data?.flowNameEn,
        flow_name_en: edge.data?.flowNameEn,
        quantityMode: edge.data?.quantityMode ?? "single",
      amount: edge.data?.amount ?? 0,
      providerAmount: edge.data?.providerAmount,
      consumerAmount: edge.data?.consumerAmount,
      unit: edge.data?.unit ?? "kg",
      type: edge.data?.type ?? "technosphere",
      allocation: edge.data?.allocation ?? "none",
      dbMapping: edge.data?.dbMapping,
    }));
    const serializedCanvases = Object.values(state.canvases).map((canvas) => ({
      id: canvas.id,
      name: canvas.name,
      kind: canvas.kind,
      parentCanvasId: canvas.parentCanvasId,
      parentPtsNodeId: canvas.parentPtsNodeId,
      nodes: canvas.nodes.map(serializeNode),
      edges: normalizeCanvasEdgesForExport(canvas, canvas.nodes.map(serializeNode)).map(serializeEdge),
    }));

    return {
      functionalUnit: state.functionalUnit,
      metadata: {
        source: "lca-graph-editor",
        node_positions: nodePositions,
        viewport: state.viewport,
        canvases: serializedCanvases,
      },
      nodes: serializedRootNodes,
      exchanges,
    };
  },
  importGraph: (graph) =>
    set((state) => {
      const nodePositionsRaw = (graph.metadata as { node_positions?: Record<string, unknown> } | undefined)?.node_positions;
      const viewportRaw = (graph.metadata as { viewport?: unknown } | undefined)?.viewport;
      const deserializeNode = (
        node: {
          id: string;
          node_kind: LcaNodeData["nodeKind"];
          mode?: "balanced" | "normalized";
          market_allow_mixed_flows?: boolean;
          lci_role?: LcaNodeData["lciRole"];
          import_mode?: ProcessImportMode;
          source_process_uuid?: string;
          import_warnings?: string[];
          filtered_exchanges?: FilteredExchangeEvidence[];
          pts_uuid?: string;
          pts_canvas_id?: string;
          pts_published_version?: number;
          pts_published_artifact_id?: string;
          process_uuid?: string;
          name: string;
          location: string;
          reference_product: string;
          reference_product_flow_uuid?: string;
          reference_product_direction?: "input" | "output";
          inputs?: FlowPort[];
          outputs?: FlowPort[];
          position?: XYPosition;
        },
      ): Node<LcaNodeData> => {
        const normalized = normalizeProductFlags({
          nodeKind: node.node_kind,
          referenceProductFlowUuid: node.reference_product_flow_uuid,
          referenceProductDirection: node.reference_product_direction,
          inputs: node.inputs ?? [],
          outputs: node.outputs ?? [],
        });
        return sanitizeMarketNode({
          id: node.id,
          type: "lcaProcess",
          position: isValidPosition(node.position)
            ? node.position
            : isValidPosition(nodePositionsRaw?.[node.id])
              ? (nodePositionsRaw?.[node.id] as XYPosition)
              : toStablePosition(node.id),
          data: {
            nodeKind: node.node_kind,
            mode: node.mode ?? (node.node_kind === "unit_process" ? "balanced" : "normalized"),
            marketAllowMixedFlows: node.market_allow_mixed_flows,
            lciRole: node.lci_role,
            importMode: node.import_mode,
            sourceProcessUuid: node.source_process_uuid,
            importWarnings: node.import_warnings,
            filteredExchanges: node.filtered_exchanges,
            ptsUuid: node.pts_uuid,
            ptsCanvasId: node.pts_canvas_id,
            ptsPublishedVersion: node.pts_published_version,
            ptsPublishedArtifactId: node.pts_published_artifact_id,
            processUuid: node.process_uuid ?? node.pts_uuid ?? `proc_${node.id}`,
            name: node.name,
            location: node.location,
            referenceProduct: node.reference_product,
            referenceProductFlowUuid: node.reference_product_flow_uuid,
            referenceProductDirection: node.reference_product_direction,
            inputs: normalized.inputs,
            outputs: normalized.outputs,
          },
        });
      };

      const deserializeEdge = (edge: {
          id: string;
          fromNode: string;
          toNode: string;
          source_port_id?: string;
          target_port_id?: string;
          sourceHandle?: string;
          targetHandle?: string;
          flowUuid: string;
        flowName: string;
        flowNameEn?: string;
        quantityMode?: "single" | "dual";
        amount: number;
        providerAmount?: number;
        consumerAmount?: number;
        unit: string;
        type: LcaEdgeData["type"];
        allocation: "physical" | "economic" | "none";
        dbMapping?: string;
          }): Edge<LcaEdgeData> => {
            const rawEdge = edge as Record<string, unknown>;
            return {
              id: edge.id,
              source: edge.fromNode,
              target: edge.toNode,
              sourceHandle: toCanvasHandleId(edge.sourceHandle ?? edge.source_port_id ?? undefined, "output"),
              targetHandle: toCanvasHandleId(edge.targetHandle ?? edge.target_port_id ?? undefined, "input"),
              type: "lcaExchange",
              data: {
                flowUuid: edge.flowUuid,
                flowName: edge.flowName,
                flowNameEn: String(edge.flowNameEn ?? rawEdge.flow_name_en ?? "").trim() || undefined,
                quantityMode: edge.quantityMode ?? "single",
                amount: edge.amount,
                providerAmount: edge.providerAmount,
                consumerAmount: edge.consumerAmount,
                unit: edge.unit,
                type: edge.type,
                allocation: edge.allocation,
                dbMapping: edge.dbMapping,
              },
            };
          };

      const metadataCanvases = (graph.metadata as { canvases?: unknown } | undefined)?.canvases;
      let nextCanvases: Record<string, CanvasGraph> | null = null;
      if (Array.isArray(metadataCanvases) && metadataCanvases.length > 0) {
        const canvasEntries = metadataCanvases
          .map((raw) => (typeof raw === "object" && raw ? (raw as Record<string, unknown>) : null))
          .filter(Boolean) as Array<Record<string, unknown>>;
        const built: Record<string, CanvasGraph> = {};
        for (const item of canvasEntries) {
          const id = String(item.id ?? "");
          if (!id) {
            continue;
          }
          const rawNodes = Array.isArray(item.nodes) ? item.nodes : [];
          const rawEdges = Array.isArray(item.edges) ? item.edges : [];
          built[id] = {
            id,
            name: String(item.name ?? id),
            kind: item.kind === "pts_internal" ? "pts_internal" : "root",
            parentCanvasId: item.parentCanvasId ? String(item.parentCanvasId) : undefined,
            parentPtsNodeId: item.parentPtsNodeId ? String(item.parentPtsNodeId) : undefined,
            nodes: rawNodes.map((n) => deserializeNode(n as Parameters<typeof deserializeNode>[0])),
            edges: rawEdges.map((e) => deserializeEdge(e as Parameters<typeof deserializeEdge>[0])),
          };
        }
        if (built[ROOT_CANVAS_ID]) {
          const rootCanvas = built[ROOT_CANVAS_ID];
          const canonicalRootNodes: Array<Node<LcaNodeData>> = graph.nodes.map((node) =>
            deserializeNode(node as Parameters<typeof deserializeNode>[0]),
          );
          const canonicalRootEdges: Array<Edge<LcaEdgeData>> = graph.exchanges.map((edge) =>
            deserializeEdge(edge as Parameters<typeof deserializeEdge>[0]),
          );
          const internalByParent = new Map<string, CanvasGraph[]>();
          for (const canvas of Object.values(built)) {
            if (canvas.kind !== "pts_internal" || !canvas.parentPtsNodeId) {
              continue;
            }
            const arr = internalByParent.get(canvas.parentPtsNodeId) ?? [];
            arr.push(canvas);
            internalByParent.set(canvas.parentPtsNodeId, arr);
          }
          built[ROOT_CANVAS_ID] = {
            ...rootCanvas,
            nodes: canonicalRootNodes.map((node) => {
              if (node.data.nodeKind !== "pts_module" || node.data.ptsCanvasId) {
                return node;
              }
              const candidates = internalByParent.get(node.id) ?? [];
              if (candidates.length === 0) {
                return node;
              }
              const chosen = [...candidates].sort(
                (a, b) => b.nodes.length + b.edges.length - (a.nodes.length + a.edges.length),
              )[0];
              return {
                ...node,
                data: {
                  ...node.data,
                  ptsCanvasId: chosen.id,
                },
              };
            }),
            edges: canonicalRootEdges,
          };
          nextCanvases = built;
        }
      }

      if (!nextCanvases) {
        const nodes: Array<Node<LcaNodeData>> = graph.nodes.map((node) => deserializeNode(node as Parameters<typeof deserializeNode>[0]));
        const edges: Array<Edge<LcaEdgeData>> = graph.exchanges.map((edge) =>
          deserializeEdge(edge as Parameters<typeof deserializeEdge>[0]),
        );
        const nextRoot: CanvasGraph = {
          ...state.canvases[ROOT_CANVAS_ID],
          nodes,
          edges,
        };
        nextCanvases = {
          [ROOT_CANVAS_ID]: nextRoot,
        };
      }

      debugFlowTypesForNode(
        "importGraph:root",
        nextCanvases[ROOT_CANVAS_ID]?.nodes.find((node) => node.id === DEBUG_FLOW_TYPE_NODE_ID),
      );

      return {
        ...setActiveCanvas(state, nextCanvases, ROOT_CANVAS_ID, false),
        functionalUnit: graph.functionalUnit || state.functionalUnit,
        viewport: isValidViewport(viewportRaw) ? viewportRaw : state.viewport,
        inspectorOpen: false,
        ptsPortEditor: { open: false },
        connectionHint: undefined,
        pendingAutoConnect: false,
        pendingPtsCompileNodeId: undefined,
        deferredBalanceEdgeId: undefined,
      };
    }),
  getBalancedWarnings: () => {
    const state = get();
    const root = state.canvases[ROOT_CANVAS_ID];
    return collectBalancedWarningsForCanvas(root);
  },
  getBalancedWarningsForCanvas: (canvasId) => {
    const state = get();
    const canvas = state.canvases[canvasId];
    if (!canvas) {
      return [];
    }
    return collectBalancedWarningsForCanvas(canvas);
  },
  getMarketWarnings: () => {
    const state = get();
    const root = state.canvases[ROOT_CANVAS_ID];
    return collectMarketWarningsForCanvas(root);
  },
  getMarketWarningsForCanvas: (canvasId) => {
    const state = get();
    const canvas = state.canvases[canvasId];
    if (!canvas) {
      return [];
    }
    return collectMarketWarningsForCanvas(canvas);
  },
}));

