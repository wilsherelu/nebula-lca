import {
  Background,
  Controls,
  ReactFlow,
  ReactFlowProvider,
  type Connection,
  type Viewport,
  useReactFlow,
} from "@xyflow/react";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import type React from "react";
import { LcaExchangeEdge } from "./LcaExchangeEdge";
import { LcaProcessNode } from "./LcaProcessNode";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;
const ENABLE_LARGE_GRAPH_EDGE_STAGING = true;
const ENABLE_LARGE_GRAPH_VISIBLE_CULLING = false;
const LARGE_GRAPH_NODE_THRESHOLD = 40;
const LARGE_GRAPH_EDGE_THRESHOLD = 100;
const EDGE_RENDER_BATCH_SIZE = 120;

type HandleValidationIssue = {
  edge_id?: string;
  suggested_source_port_id?: string;
  suggested_target_port_id?: string;
};

type HandleValidationResponse = {
  ok?: boolean;
  issues?: HandleValidationIssue[];
};

type GraphCanvasPort = {
  id: string;
  legacyPortId?: string;
  flowUuid: string;
};

const parseHandlePortId = (handleId: string | null | undefined, prefix: "in:" | "out:"): string | undefined => {
  if (!handleId) {
    return undefined;
  }
  const candidates = prefix === "in:" ? ["in:", "inl:", "inr:"] : ["out:", "outl:", "outr:"];
  const matched = candidates.find((p) => handleId.startsWith(p));
  return matched ? handleId.slice(matched.length) : undefined;
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

const buildPortLookupKey = (nodeId: string, portId: string, flowUuid: string): string => `${nodeId}::${flowUuid}::${portId}`;
const buildSingleFlowLookupKey = (nodeId: string, flowUuid: string): string => `${nodeId}::${flowUuid}`;

const nodeTypes = {
  lcaProcess: LcaProcessNode,
};

const edgeTypes = {
  lcaExchange: LcaExchangeEdge,
};

function GraphCanvasInner(props: {
  onBeforeAutoConnect?: () => Promise<void> | void;
  onRequestUnpackPts?: (nodeId: string) => Promise<void> | void;
  onOpenProjectTarget?: () => void;
  canvasLoadKey?: string | number;
}) {
  const [interactionMode, setInteractionMode] = useState<"cursor" | "hand">("hand");
  const [isConnecting, setIsConnecting] = useState(false);
  const [hoverDropNodeId, setHoverDropNodeId] = useState<string | undefined>(undefined);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const rf = useReactFlow();
  const {
    nodes,
    edges,
    onConnect,
    onEdgesChange,
    onNodesChange,
    setSelection,
    openNodeInspector,
    openEdgeInspector,
    closeInspector,
    clearConnectionHint,
    enterPtsNode,
    packageSelectionAsPts,
    removeEdge,
    removeNode,
    cloneNodeAt,
    openFlowBalanceDialogForEdge,
    openPtsPortEditor,
    activeCanvasKind,
    selection,
    addBlankNodeAt,
    openUnitProcessImportDialog,
    autoConnectByUuid,
    pendingAutoConnect,
    consumePendingAutoConnect,
    viewport,
    setViewport,
    autoPopupEnabled,
    setAutoPopupEnabled,
    flowAnimationEnabled,
    setFlowAnimationEnabled,
    unitAutoScaleEnabled,
    setUnitAutoScaleEnabled,
    uiLanguage,
    pendingEdges,
    flushPendingEdges,
    applyHandleValidationIssues,
    exportGraph,
    deferredBalanceEdgeId,
    consumeDeferredBalanceEdge,
    activeCanvasId,
  } = useLcaGraphStore();
  const [menu, setMenu] = useState<
    | {
        x: number;
        y: number;
        kind: "pane";
        flowPos: { x: number; y: number };
      }
    | {
        x: number;
        y: number;
        kind: "node";
        nodeId: string;
      }
    | {
        x: number;
        y: number;
        kind: "edge";
        edgeId: string;
      }
    | null
  >(null);
  const [copiedNodeId, setCopiedNodeId] = useState<string | null>(null);
  const connectingRef = useRef<{ nodeId?: string | null; handleId?: string | null } | null>(null);
  const suppressNodeSelectionUntilRef = useRef(0);
  const validatingRef = useRef(false);
  const lastCanvasLoadKeyRef = useRef<string | number | undefined>(undefined);
  const edgeRenderLimitByCanvasRef = useRef<Record<string, number>>({});
  const hiddenDisplayMetricsRef = useRef<{ width: number; height: number; dpr: number } | null>(null);
  const [edgeRenderLimit, setEdgeRenderLimit] = useState(0);

  const resolveNodeIdFromEvent = useCallback((event: MouseEvent | TouchEvent): string | undefined => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return undefined;
    }
    const nodeEl = target.closest(".react-flow__node");
    const nodeId = nodeEl?.getAttribute("data-id") ?? undefined;
    return nodeId || undefined;
  }, []);

  const resolveNodeIdFromElement = useCallback((target: EventTarget | null): string | undefined => {
    if (!(target instanceof Element)) {
      return undefined;
    }
    return target.closest(".react-flow__node")?.getAttribute("data-id") ?? undefined;
  }, []);

  const resolveEdgeIdFromElement = useCallback((target: EventTarget | null): string | undefined => {
    if (!(target instanceof Element)) {
      return undefined;
    }
    return target.closest(".react-flow__edge")?.getAttribute("data-id") ?? undefined;
  }, []);

  const resolveHandleIdFromElement = useCallback((target: EventTarget | null): string | undefined => {
    if (!(target instanceof Element)) {
      return undefined;
    }
    const handleEl = target.closest(".react-flow__handle");
    if (!(handleEl instanceof Element)) {
      return undefined;
    }
    return (
      handleEl.getAttribute("data-handleid") ??
      handleEl.getAttribute("data-id") ??
      undefined
    ) || undefined;
  }, []);

  useEffect(() => {
    if (!pendingAutoConnect || activeCanvasKind !== "root") {
      return;
    }
    consumePendingAutoConnect();
    void (async () => {
      await props.onBeforeAutoConnect?.();
      autoConnectByUuid({ silentNoCandidate: true, silentSuccess: true });
    })();
  }, [activeCanvasKind, autoConnectByUuid, consumePendingAutoConnect, pendingAutoConnect, props]);

  useEffect(() => {
    const root = wrapperRef.current;
    if (!root) {
      return;
    }
    const nodeEls = root.querySelectorAll<HTMLElement>(".react-flow__node");
    nodeEls.forEach((el) => {
      const nodeId = el.getAttribute("data-id") ?? "";
      el.classList.toggle("drop-target-highlight", Boolean(isConnecting && hoverDropNodeId && nodeId === hoverDropNodeId));
    });
  }, [hoverDropNodeId, isConnecting, nodes, edges]);

  const onDrop = useCallback(
    (event: React.DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      const kind = event.dataTransfer.getData("application/lca-node-kind");
      if (!kind || !wrapperRef.current) {
        return;
      }
      const position = rf.screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });
      if (kind === "unit_process" || kind === "market_process" || kind === "pts_module" || kind === "lci_dataset") {
        addBlankNodeAt(kind, position);
      }
    },
    [addBlankNodeAt, rf],
  );

  useEffect(() => {
    // Apply persisted viewport only when a whole graph/canvas load cycle happens.
    void rf.setViewport(viewport, { duration: 0 });
  }, [activeCanvasId, props.canvasLoadKey, rf]);

  useEffect(() => {
    const readDisplayMetrics = () => {
      const host = wrapperRef.current;
      if (!host) {
        return null;
      }
      return {
        width: host.clientWidth,
        height: host.clientHeight,
        dpr: window.devicePixelRatio || 1,
      };
    };
    const snapshotHiddenMetrics = () => {
      const metrics = readDisplayMetrics();
      hiddenDisplayMetricsRef.current = metrics;
    };
    const restoreViewportIfDisplayMetricsChanged = () => {
      if (document.visibilityState === "hidden") {
        return;
      }
      const before = hiddenDisplayMetricsRef.current;
      hiddenDisplayMetricsRef.current = null;
      const after = readDisplayMetrics();
      if (!before || !after || after.width <= 0 || after.height <= 0) {
        return;
      }
      const metricsChanged =
        before.width !== after.width ||
        before.height !== after.height ||
        Math.abs(before.dpr - after.dpr) > 0.001;
      if (!metricsChanged) {
        return;
      }
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          const latestMetrics = readDisplayMetrics();
          if (!latestMetrics || latestMetrics.width <= 0 || latestMetrics.height <= 0) {
            return;
          }
          void rf.setViewport(viewport, { duration: 0 });
        });
      });
    };
    const onVisibilityChange = () => {
      if (document.visibilityState === "hidden") {
        snapshotHiddenMetrics();
        return;
      }
      restoreViewportIfDisplayMetricsChanged();
    };
    const onFocus = () => {
      restoreViewportIfDisplayMetricsChanged();
    };
    const onBlur = () => {
      snapshotHiddenMetrics();
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    window.addEventListener("focus", onFocus);
    window.addEventListener("blur", onBlur);
    return () => {
      document.removeEventListener("visibilitychange", onVisibilityChange);
      window.removeEventListener("focus", onFocus);
      window.removeEventListener("blur", onBlur);
    };
  }, [rf, viewport]);

  const edgeLookup = useMemo(() => {
    const nodeById = new Map(nodes.map((node) => [node.id, node]));
    const outputPortByNodeAndPortId = new Map<string, GraphCanvasPort>();
    const inputPortByNodeAndPortId = new Map<string, GraphCanvasPort>();
    const singleOutputByNodeAndFlowUuid = new Map<string, GraphCanvasPort>();
    const singleInputByNodeAndFlowUuid = new Map<string, GraphCanvasPort>();

    for (const node of nodes) {
      const outputCountByFlow = new Map<string, number>();
      const inputCountByFlow = new Map<string, number>();
      for (const port of node.data.outputs) {
        const flowUuid = String(port.flowUuid ?? "").trim();
        const portId = String(port.id ?? "").trim();
        const legacyPortId = String(port.legacyPortId ?? "").trim();
        if (portId && flowUuid) {
          outputPortByNodeAndPortId.set(buildPortLookupKey(node.id, portId, flowUuid), port);
        }
        if (legacyPortId && flowUuid) {
          outputPortByNodeAndPortId.set(buildPortLookupKey(node.id, legacyPortId, flowUuid), port);
        }
        if (flowUuid) {
          outputCountByFlow.set(flowUuid, (outputCountByFlow.get(flowUuid) ?? 0) + 1);
        }
      }
      for (const port of node.data.outputs) {
        const flowUuid = String(port.flowUuid ?? "").trim();
        if (flowUuid && outputCountByFlow.get(flowUuid) === 1) {
          singleOutputByNodeAndFlowUuid.set(buildSingleFlowLookupKey(node.id, flowUuid), port);
        }
      }

      for (const port of node.data.inputs) {
        const flowUuid = String(port.flowUuid ?? "").trim();
        const portId = String(port.id ?? "").trim();
        const legacyPortId = String(port.legacyPortId ?? "").trim();
        if (portId && flowUuid) {
          inputPortByNodeAndPortId.set(buildPortLookupKey(node.id, portId, flowUuid), port);
        }
        if (legacyPortId && flowUuid) {
          inputPortByNodeAndPortId.set(buildPortLookupKey(node.id, legacyPortId, flowUuid), port);
        }
        if (flowUuid) {
          inputCountByFlow.set(flowUuid, (inputCountByFlow.get(flowUuid) ?? 0) + 1);
        }
      }
      for (const port of node.data.inputs) {
        const flowUuid = String(port.flowUuid ?? "").trim();
        if (flowUuid && inputCountByFlow.get(flowUuid) === 1) {
          singleInputByNodeAndFlowUuid.set(buildSingleFlowLookupKey(node.id, flowUuid), port);
        }
      }
    }

    return {
      nodeById,
      outputPortByNodeAndPortId,
      inputPortByNodeAndPortId,
      singleOutputByNodeAndFlowUuid,
      singleInputByNodeAndFlowUuid,
    };
  }, [nodes]);

  const renderedEdges = useMemo((): typeof edges => {
    const {
      nodeById,
      outputPortByNodeAndPortId,
      inputPortByNodeAndPortId,
      singleOutputByNodeAndFlowUuid,
      singleInputByNodeAndFlowUuid,
    } = edgeLookup;
    const next: typeof edges = [];
    for (const edge of edges) {
      const sourceNode = nodeById.get(edge.source);
      const targetNode = nodeById.get(edge.target);
      const flowUuid = edge.data?.flowUuid ?? "";
      if (!sourceNode || !targetNode || !flowUuid) {
        continue;
      }

      const sourcePortId = parseHandlePortId(edge.sourceHandle, "out:");
      const sourcePort =
        (sourcePortId
          ? outputPortByNodeAndPortId.get(buildPortLookupKey(edge.source, sourcePortId, flowUuid))
          : undefined) ?? singleOutputByNodeAndFlowUuid.get(buildSingleFlowLookupKey(edge.source, flowUuid));
      if (!sourcePort) {
        continue;
      }

      const targetPortId = parseHandlePortId(edge.targetHandle, "in:");
      const targetPort =
        (targetPortId
          ? inputPortByNodeAndPortId.get(buildPortLookupKey(edge.target, targetPortId, flowUuid))
          : undefined) ?? singleInputByNodeAndFlowUuid.get(buildSingleFlowLookupKey(edge.target, flowUuid));
      if (!targetPort) {
        continue;
      }

      next.push({
        ...edge,
        sourceHandle: buildHandleWithSameSide(edge.sourceHandle, "out:", sourcePort.id),
        targetHandle: buildHandleWithSameSide(edge.targetHandle, "in:", targetPort.id),
      });
    }
    return next;
  }, [edgeLookup, edges]);

  const largeGraphMode =
    ENABLE_LARGE_GRAPH_EDGE_STAGING &&
    (nodes.length >= LARGE_GRAPH_NODE_THRESHOLD || renderedEdges.length >= LARGE_GRAPH_EDGE_THRESHOLD);

  useLayoutEffect(() => {
    const loadCycleChanged = lastCanvasLoadKeyRef.current !== props.canvasLoadKey;
    lastCanvasLoadKeyRef.current = props.canvasLoadKey;
    if (!ENABLE_LARGE_GRAPH_EDGE_STAGING || !largeGraphMode) {
      edgeRenderLimitByCanvasRef.current[activeCanvasId] = renderedEdges.length;
      setEdgeRenderLimit(renderedEdges.length);
      return;
    }
    const rememberedLimit = edgeRenderLimitByCanvasRef.current[activeCanvasId];
    if (loadCycleChanged || typeof rememberedLimit !== "number") {
      const initialLimit = Math.min(EDGE_RENDER_BATCH_SIZE, renderedEdges.length);
      edgeRenderLimitByCanvasRef.current[activeCanvasId] = initialLimit;
      setEdgeRenderLimit(initialLimit);
      return;
    }
    const nextLimit = Math.min(
      Math.max(rememberedLimit ?? edgeRenderLimit, Math.min(EDGE_RENDER_BATCH_SIZE, renderedEdges.length)),
      renderedEdges.length,
    );
    edgeRenderLimitByCanvasRef.current[activeCanvasId] = nextLimit;
    setEdgeRenderLimit(nextLimit);
  }, [activeCanvasId, edgeRenderLimit, largeGraphMode, props.canvasLoadKey, renderedEdges.length]);

  useEffect(() => {
    if (!ENABLE_LARGE_GRAPH_EDGE_STAGING || !largeGraphMode || edgeRenderLimit >= renderedEdges.length) {
      return;
    }
    const frame = window.requestAnimationFrame(() => {
      setEdgeRenderLimit((prev) => {
        const nextLimit = Math.min(prev + EDGE_RENDER_BATCH_SIZE, renderedEdges.length);
        edgeRenderLimitByCanvasRef.current[activeCanvasId] = nextLimit;
        return nextLimit;
      });
    });
    return () => {
      window.cancelAnimationFrame(frame);
    };
  }, [activeCanvasId, edgeRenderLimit, largeGraphMode, renderedEdges.length]);

  const mountedEdges = useMemo(
    () => (largeGraphMode ? renderedEdges.slice(0, edgeRenderLimit) : renderedEdges),
    [edgeRenderLimit, largeGraphMode, renderedEdges],
  );

  useEffect(() => {
    if (pendingEdges.length === 0) {
      return;
    }
    if (validatingRef.current) {
      return;
    }
    let canceled = false;
    const frame = window.requestAnimationFrame(() => {
      if (canceled) {
        return;
      }
      window.requestAnimationFrame(async () => {
        if (canceled) {
          return;
        }
        validatingRef.current = true;
        try {
          const graph = exportGraph();
          const resp = await fetch(`${API_BASE}/model/validate-handles`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ graph }),
          });
          if (resp.ok) {
            const payload = (await resp.json()) as HandleValidationResponse;
            const issues = Array.isArray(payload.issues) ? payload.issues : [];
            if (issues.length > 0) {
              applyHandleValidationIssues(issues);
            }
          }
        } catch {
          // ignore validation network errors and keep local fallback
        } finally {
          flushPendingEdges();
          validatingRef.current = false;
        }
      });
    });
    return () => {
      canceled = true;
      window.cancelAnimationFrame(frame);
    };
  }, [applyHandleValidationIssues, exportGraph, flushPendingEdges, pendingEdges.length]);

  useEffect(() => {
    if (!deferredBalanceEdgeId) {
      return;
    }
    const exists = mountedEdges.some((edge) => edge.id === deferredBalanceEdgeId);
    if (!exists) {
      return;
    }
    const frame = window.requestAnimationFrame(() => {
      openFlowBalanceDialogForEdge(deferredBalanceEdgeId);
      consumeDeferredBalanceEdge();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [consumeDeferredBalanceEdge, deferredBalanceEdgeId, mountedEdges, openFlowBalanceDialogForEdge]);

  const selectionNodes = useMemo(
    () => nodes.filter((node) => selection.nodeIds.includes(node.id)),
    [nodes, selection.nodeIds],
  );
  const canPackSelection =
    activeCanvasKind === "root" &&
    selectionNodes.length >= 2 &&
    selectionNodes.every(
      (node) =>
        node.data.nodeKind === "unit_process" ||
        node.data.nodeKind === "market_process" ||
        node.data.nodeKind === "lci_dataset",
    );
  const isSingleSelectedPts =
    activeCanvasKind === "root" &&
    selection.nodeIds.length === 1 &&
    Boolean(nodes.find((node) => node.id === selection.nodeIds[0] && node.data.nodeKind === "pts_module"));
  return (
    <div
      ref={wrapperRef}
      className={`graph-canvas ${interactionMode === "hand" ? "graph-canvas--hand" : "graph-canvas--cursor"}`}
      onMouseMove={(event) => {
        if (!isConnecting) {
          return;
        }
        const hovered = resolveNodeIdFromEvent(event.nativeEvent);
        const sourceId = connectingRef.current?.nodeId ?? undefined;
        setHoverDropNodeId(hovered && hovered !== sourceId ? hovered : undefined);
      }}
      onDrop={onDrop}
      onDragOver={(event) => event.preventDefault()}
      onContextMenu={(event) => {
        event.preventDefault();
        const targetNodeId = resolveNodeIdFromElement(event.target);
        if (targetNodeId) {
          const isInCurrentSelection = selection.nodeIds.includes(targetNodeId);
          if (!isInCurrentSelection) {
            setSelection({
              nodeIds: [targetNodeId],
              edgeIds: [],
              nodeId: targetNodeId,
              edgeId: undefined,
            });
          }
          setMenu({ x: event.clientX, y: event.clientY, kind: "node", nodeId: targetNodeId });
          return;
        }
        const targetEdgeId = resolveEdgeIdFromElement(event.target);
        if (targetEdgeId) {
          setSelection({
            nodeIds: [],
            edgeIds: [targetEdgeId],
            nodeId: undefined,
            edgeId: targetEdgeId,
          });
          setMenu({ x: event.clientX, y: event.clientY, kind: "edge", edgeId: targetEdgeId });
          return;
        }
        const flowPos = rf.screenToFlowPosition({ x: event.clientX, y: event.clientY });
        setMenu({ x: event.clientX, y: event.clientY, kind: "pane", flowPos });
      }}
    >
      <ReactFlow
        nodes={nodes}
        edges={mountedEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onlyRenderVisibleElements={ENABLE_LARGE_GRAPH_VISIBLE_CULLING && largeGraphMode}
        defaultViewport={viewport}
        minZoom={0.3}
        deleteKeyCode={["Backspace", "Delete"]}
        multiSelectionKeyCode="Shift"
        selectionOnDrag={interactionMode === "cursor"}
        panOnDrag={interactionMode === "hand"}
        nodesDraggable
        elementsSelectable
        onConnect={onConnect}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onSelectionChange={({ nodes: sNodes, edges: sEdges }) =>
          {
            const now = Date.now();
            if (now < suppressNodeSelectionUntilRef.current && sEdges.length === 0 && sNodes.length > 0) {
              return;
            }
            setSelection({
              nodeIds: sNodes.map((n) => n.id),
              edgeIds: [],
              nodeId: sNodes[0]?.id,
              edgeId: undefined,
            });
          }
        }
        onNodeDoubleClick={(event, node) => {
          if (node.data.nodeKind === "pts_module" && !event.altKey) {
            enterPtsNode(node.id);
            return;
          }
          openNodeInspector(node.id);
        }}
        onEdgeDoubleClick={(_, edge) => openEdgeInspector(edge.id)}
        onPaneClick={(event) => {
          if (event.detail === 2) {
            closeInspector();
          }
          setMenu(null);
        }}
        onNodeClick={(event, node) => {
          setMenu(null);
          if ((event.ctrlKey || event.metaKey) && node.data.nodeKind === "pts_module" && activeCanvasKind === "root") {
            openPtsPortEditor(node.id);
          }
        }}
        onConnectStart={(_, params) => {
          clearConnectionHint();
          closeInspector();
          setSelection({ nodeIds: [], edgeIds: [], nodeId: undefined, edgeId: undefined });
          suppressNodeSelectionUntilRef.current = Date.now() + 1200;
          setIsConnecting(true);
          connectingRef.current = {
            nodeId: params.nodeId,
            handleId: params.handleId ?? null,
          };
        }}
        onConnectEnd={(event, connectionState) => {
          suppressNodeSelectionUntilRef.current = Date.now() + 1200;
          const finalState = connectionState as {
            isValid?: boolean;
            fromNode?: { id: string };
            fromHandle?: { id: string | null };
          };
          if (finalState?.isValid) {
            connectingRef.current = null;
            setIsConnecting(false);
            setHoverDropNodeId(undefined);
            return;
          }

          const sourceNodeId = finalState?.fromNode?.id ?? connectingRef.current?.nodeId;
          const sourceHandle = finalState?.fromHandle?.id ?? connectingRef.current?.handleId ?? null;
          const targetNodeId = resolveNodeIdFromEvent(event);
          const targetHandle = resolveHandleIdFromElement(event.target);

          connectingRef.current = null;
          setIsConnecting(false);
          setHoverDropNodeId(undefined);
          if (!sourceNodeId || !targetNodeId || sourceNodeId === targetNodeId) {
            return;
          }

          const fallbackConnection: Connection = {
            source: sourceNodeId,
            sourceHandle,
            target: targetNodeId,
            targetHandle: targetHandle ?? null,
          };
          onConnect(fallbackConnection);
        }}
        onMoveEnd={(_, nextViewport: Viewport) => {
          setViewport(nextViewport);
        }}
      >
        <Background gap={18} size={1} color="#d6dde2" />
        <Controls />
      </ReactFlow>
      <div className="canvas-mode-switch" role="group" aria-label="canvas-interaction-mode">
        <div className="canvas-mode-switch-group">
          <button
            type="button"
            className={interactionMode === "hand" ? "active" : ""}
            onClick={() => setInteractionMode("hand")}
            title={uiLanguage === "zh" ? "拖手模式：平移画布视角" : "Hand mode: pan canvas"}
          >
            {uiLanguage === "zh" ? "拖手" : "Hand"}
          </button>
          <button
            type="button"
            className={interactionMode === "cursor" ? "active" : ""}
            onClick={() => setInteractionMode("cursor")}
            title={uiLanguage === "zh" ? "光标模式：框选/多选" : "Cursor mode: multi-select"}
          >
            {uiLanguage === "zh" ? "光标" : "Cursor"}
          </button>
        </div>
        <div className="canvas-mode-switch-divider" />
        <div className="canvas-mode-switch-group">
          <button
            type="button"
            className={autoPopupEnabled ? "active toggle-btn two-line-btn" : "toggle-btn two-line-btn"}
            onClick={() => setAutoPopupEnabled(!autoPopupEnabled)}
            title={uiLanguage === "zh" ? "自动弹窗开关" : "Auto popup"}
          >
            {uiLanguage === "zh" ? "配平\n弹窗" : "Popup"}
          </button>
          <button
            type="button"
            className={flowAnimationEnabled ? "active toggle-btn two-line-btn" : "toggle-btn two-line-btn"}
            onClick={() => setFlowAnimationEnabled(!flowAnimationEnabled)}
            title={uiLanguage === "zh" ? "流向动画开关" : "Flow animation"}
          >
            {uiLanguage === "zh" ? "流向\n动画" : "Flow\nAnim"}
          </button>
          <button
            type="button"
            className={unitAutoScaleEnabled ? "active toggle-btn two-line-btn" : "toggle-btn two-line-btn"}
            onClick={() => setUnitAutoScaleEnabled(!unitAutoScaleEnabled)}
            title={uiLanguage === "zh" ? "单位切换自动换算开关" : "Unit auto convert"}
          >
            {uiLanguage === "zh" ? "单位\n换算" : "Unit\nConvert"}
          </button>
        </div>
        <div className="canvas-mode-switch-divider" />
        <div className="canvas-mode-switch-group">
          <button
            type="button"
            className="action-btn"
            style={{ minWidth: 38, fontSize: 18, fontWeight: 700, lineHeight: 1, padding: "4px 8px" }}
            onClick={() => window.dispatchEvent(new Event("nebula:history-undo"))}
            title={uiLanguage === "zh" ? "回退到上一版本（Ctrl+Z）" : "Undo to previous version (Ctrl+Z)"}
          >
            ⟲
          </button>
          <button
            type="button"
            className="action-btn"
            style={{ minWidth: 38, fontSize: 18, fontWeight: 700, lineHeight: 1, padding: "4px 8px" }}
            onClick={() => window.dispatchEvent(new Event("nebula:history-redo"))}
            title={uiLanguage === "zh" ? "前进到下一版本（Ctrl+Shift+Z）" : "Redo to next version (Ctrl+Shift+Z)"}
          >
            ⟳
          </button>
          {activeCanvasKind === "pts_internal" && (
            <button
              type="button"
              className="action-btn two-line-btn"
              onClick={() => openPtsPortEditor()}
              title={uiLanguage === "zh" ? "编辑当前 PTS 的端口" : "Edit PTS ports"}
            >
              {uiLanguage === "zh" ? "暴露\n端口" : "Ports"}
            </button>
          )}
          {activeCanvasKind === "root" && (
            <button
              type="button"
              className="action-btn two-line-btn"
              onClick={() => props.onOpenProjectTarget?.()}
              title={uiLanguage === "zh" ? "设置项目目标产品与产量" : "Target product"}
            >
              {uiLanguage === "zh" ? "目标\n产品" : "Target"}
            </button>
          )}
          <button
            type="button"
            className="action-btn two-line-btn"
            onClick={() => {
              void (async () => {
                await props.onBeforeAutoConnect?.();
                autoConnectByUuid();
              })();
            }}
            title={uiLanguage === "zh" ? "按 UUID 自动连线（唯一市场产品优先）" : "Auto connect by UUID"}
          >
            {uiLanguage === "zh" ? "自动\n连线" : "Auto\nLink"}
          </button>
        </div>
      </div>
      {menu && (
        <div className="context-menu" style={{ left: menu.x, top: menu.y }} onClick={() => setMenu(null)}>
          {menu.kind === "edge" && (
            <>
              <button
                onClick={() => {
                  openFlowBalanceDialogForEdge(menu.edgeId);
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "配平编辑" : "Balance"}
              </button>
              <button
                onClick={() => {
                  removeEdge(menu.edgeId);
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "删除连线" : "Delete Edge"}
              </button>
            </>
          )}
            {menu.kind === "node" && (
              <>
                {!nodes.find((n) => n.id === menu.nodeId && n.data.nodeKind === "pts_module") && (
                  <button
                    onClick={() => {
                      openNodeInspector(menu.nodeId);
                      setMenu(null);
                    }}
                  >
                    {uiLanguage === "zh" ? "编辑" : "Edit"}
                  </button>
                )}
                <button
                  disabled={activeCanvasKind !== "root" || !nodes.find((n) => n.id === menu.nodeId && n.data.nodeKind === "pts_module")}
                  onClick={() => {
                  openPtsPortEditor(menu.nodeId);
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "编辑端口" : "Edit Ports"}
              </button>
              <button
                disabled={!canPackSelection}
                onClick={() => {
                  packageSelectionAsPts();
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "PTS封装" : "Pack as PTS"}
              </button>
              <button
                disabled={
                  activeCanvasKind !== "root" ||
                  !nodes.find((n) => n.id === menu.nodeId && n.data.nodeKind === "pts_module")
                }
                onClick={() => {
                  void props.onRequestUnpackPts?.(menu.nodeId);
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "解封PTS" : "Unpack PTS"}
              </button>
              <button
                onClick={() => {
                  setCopiedNodeId(menu.nodeId);
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "复制" : "Copy"}
              </button>
              <button
                onClick={() => {
                  removeNode(menu.nodeId);
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "删除" : "Delete"}
              </button>
            </>
          )}
          {menu.kind === "pane" && (
            <>
              {selection.nodeIds.length === 0 && selection.edgeIds.length === 0 && (
                <>
                  <div className="context-menu-submenu-wrap" onClick={(event) => event.stopPropagation()}>
                    <button
                      type="button"
                      className="context-menu-submenu-trigger"
                      onClick={(event) => event.stopPropagation()}
                    >
                      <span>{uiLanguage === "zh" ? "导入" : "Import"}</span>
                      <span className="context-menu-submenu-arrow">›</span>
                    </button>
                    <div className="context-menu-submenu">
                      <button
                        onClick={() => {
                          openUnitProcessImportDialog(menu.flowPos, "unit_process");
                          setMenu(null);
                        }}
                      >
                        {uiLanguage === "zh" ? "单元过程" : "Unit Process"}
                      </button>
                      <button
                        onClick={() => {
                          openUnitProcessImportDialog(menu.flowPos, "market_process");
                          setMenu(null);
                        }}
                      >
                        {uiLanguage === "zh" ? "市场过程" : "Market Process"}
                      </button>
                      <button
                        onClick={() => {
                          openUnitProcessImportDialog(menu.flowPos, "lci_dataset");
                          setMenu(null);
                        }}
                      >
                        {uiLanguage === "zh" ? "LCI数据集" : "LCI Dataset"}
                      </button>
                      {activeCanvasKind !== "pts_internal" && (
                        <button
                          onClick={() => {
                            openUnitProcessImportDialog(menu.flowPos, "pts_module");
                            setMenu(null);
                          }}
                        >
                          {uiLanguage === "zh" ? "PTS模块" : "PTS Module"}
                        </button>
                      )}
                    </div>
                  </div>
                  <button
                    onClick={() => {
                      addBlankNodeAt("unit_process", menu.flowPos);
                      setMenu(null);
                    }}
                  >
                    {uiLanguage === "zh" ? "新建单元过程" : "New Unit Process"}
                  </button>
                  <button
                    onClick={() => {
                      addBlankNodeAt("market_process", menu.flowPos);
                      setMenu(null);
                    }}
                  >
                    {uiLanguage === "zh" ? "新建市场过程" : "New Market Process"}
                  </button>
                  {activeCanvasKind !== "pts_internal" && (
                    <button
                      onClick={() => {
                        addBlankNodeAt("pts_module", menu.flowPos);
                        setMenu(null);
                      }}
                    >
                      {uiLanguage === "zh" ? "新建PTS模块" : "New PTS Module"}
                    </button>
                  )}
                  <button
                    onClick={() => {
                      addBlankNodeAt("lci_dataset", menu.flowPos);
                      setMenu(null);
                    }}
                  >
                    {uiLanguage === "zh" ? "新建LCI数据集" : "New LCI Dataset"}
                  </button>
                </>
              )}
              {selection.nodeIds.length > 0 && (
                <>
                  <button
                    disabled={!canPackSelection}
                    onClick={() => {
                      packageSelectionAsPts();
                      setMenu(null);
                    }}
                  >
                    {uiLanguage === "zh" ? "PTS封装" : "Pack as PTS"}
                  </button>
                  <button
                    disabled={!isSingleSelectedPts}
                    onClick={() => {
                      void props.onRequestUnpackPts?.(selection.nodeIds[0]);
                      setMenu(null);
                    }}
                  >
                    {uiLanguage === "zh" ? "解封PTS" : "Unpack PTS"}
                  </button>
                </>
              )}
              <button
                disabled={!copiedNodeId}
                onClick={() => {
                  if (copiedNodeId) {
                    cloneNodeAt(copiedNodeId, menu.flowPos);
                  }
                  setMenu(null);
                }}
              >
                {uiLanguage === "zh" ? "粘贴" : "Paste"}
              </button>
            </>
          )}
        </div>
      )}
    </div>
  );
}

export function GraphCanvas(props: {
  onBeforeAutoConnect?: () => Promise<void> | void;
  onRequestUnpackPts?: (nodeId: string) => Promise<void> | void;
  onOpenProjectTarget?: () => void;
  canvasLoadKey?: string | number;
}) {
  return (
    <ReactFlowProvider>
      <GraphCanvasInner
        canvasLoadKey={props.canvasLoadKey}
        onBeforeAutoConnect={props.onBeforeAutoConnect}
        onRequestUnpackPts={props.onRequestUnpackPts}
        onOpenProjectTarget={props.onOpenProjectTarget}
      />
    </ReactFlowProvider>
  );
}










