import { Handle, Position, useUpdateNodeInternals, type NodeProps } from "@xyflow/react";
import { useEffect, useMemo, useRef, useState } from "react";
import type React from "react";
import type { FlowPort, LcaNodeData } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;

const sectionTitleStyle: React.CSSProperties = {
  margin: "6px 0 3px",
  fontSize: 10,
  color: "#4f6d7a",
  fontWeight: 700,
  textTransform: "uppercase",
  letterSpacing: 0.2,
};

const hiddenHandleStyle = (side: "left" | "right", topPercent: number): React.CSSProperties => ({
  opacity: 0,
  width: 1,
  height: 1,
  pointerEvents: "none",
  top: `${topPercent}%`,
  transform: "translateY(-50%)",
  left: side === "left" ? -4 : undefined,
  right: side === "right" ? -4 : undefined,
});

const getHiddenHandleTop = (section: "inputs" | "outputs", index: number, total: number): number => {
  const safeTotal = Math.max(1, total);
  const start = section === "inputs" ? 32 : 72;
  const spread = section === "inputs" ? 14 : 14;
  const step = safeTotal === 1 ? 0 : spread / (safeTotal - 1);
  return start + index * step;
};

const normalizeNodeName = (value: string) => value.trim().toLowerCase();
const EMPTY_PORT_LABELS: ReadonlyMap<string, string> = new Map();

export function LcaProcessNode(props: NodeProps) {
  const data = props.data as LcaNodeData;
  const updateNodeInternals = useUpdateNodeInternals();
  const updateNode = useLcaGraphStore((state) => state.updateNode);
  const setConnectionHint = useLcaGraphStore((state) => state.setConnectionHint);
  const connectedSourceInputLabelByPortId = useLcaGraphStore(
    (state) => state.graphRelations.marketInputDisplayByNodeId.get(props.id) ?? EMPTY_PORT_LABELS,
  );
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const activeCanvasKind = useLcaGraphStore((state) => state.activeCanvasKind);
  const isNodeDragging = useLcaGraphStore((state) => state.isNodeDragging);
  const draggingNodeIds = useLcaGraphStore((state) => state.draggingNodeIds);
  const dragAdjacentNodeIds = useLcaGraphStore((state) => state.dragAdjacentNodeIds);
  const pendingPtsCompileNodeId = useLcaGraphStore((state) => state.pendingPtsCompileNodeId);
  const [editingName, setEditingName] = useState(false);
  const [draftName, setDraftName] = useState(data.name);
  const [flowNameEnByUuid, setFlowNameEnByUuid] = useState<Record<string, string>>({});
  const stableIoSectionsRef = useRef<{
    inputRows: React.ReactNode[];
    outputRows: React.ReactNode[];
    showEmptyInputs: boolean;
    showEmptyOutputs: boolean;
  } | null>(null);

  const isRootVisiblePort = (item: LcaNodeData["inputs"][number]) => {
    if (item.type === "biosphere") {
      return false;
    }
    return Boolean(item.showOnNode);
  };
  const suppressPtsShellPorts =
    activeCanvasKind === "root" &&
    data.nodeKind === "pts_module" &&
    pendingPtsCompileNodeId === props.id;
  const rawVisibleInputs = suppressPtsShellPorts ? [] : data.inputs.filter((item) => isRootVisiblePort(item));
  const rawVisibleOutputs = suppressPtsShellPorts ? [] : data.outputs.filter((item) => isRootVisiblePort(item));
  const shouldDelayPtsOutputs =
    activeCanvasKind === "root" &&
    data.nodeKind === "pts_module" &&
    uiLanguage === "en" &&
    rawVisibleOutputs.some((port) => !String(port.flowNameEn ?? "").trim());
  const visibleInputs = rawVisibleInputs;
  const visibleOutputs = shouldDelayPtsOutputs ? [] : rawVisibleOutputs;
  const isDragRelevant =
    !isNodeDragging || draggingNodeIds.has(props.id) || dragAdjacentNodeIds.has(props.id);
  const freezeNodeContent = isNodeDragging && !isDragRelevant;
  const outputFlowCount = rawVisibleOutputs.reduce<Map<string, number>>((acc, port) => {
    const key = String(port.flowUuid ?? "").trim();
    if (!key) {
      return acc;
    }
    acc.set(key, (acc.get(key) ?? 0) + 1);
    return acc;
  }, new Map());
  const hiddenInputs =
    data.nodeKind === "pts_module" || isNodeDragging
      ? []
      : data.inputs.filter((item) => item.type !== "biosphere" && !isRootVisiblePort(item));
  const hiddenOutputs =
    data.nodeKind === "pts_module" || isNodeDragging
      ? []
      : data.outputs.filter((item) => item.type !== "biosphere" && !isRootVisiblePort(item));
  const visibleFlowUuids = useMemo(
    () =>
      Array.from(
        new Set(
          [...rawVisibleInputs, ...rawVisibleOutputs]
            .map((port) => String(port.flowUuid ?? "").trim())
            .filter((uuid) => uuid.length > 0),
        ),
      ),
    [rawVisibleInputs, rawVisibleOutputs],
  );
  const marketProcess = data.nodeKind === "market_process" || (data.nodeKind === "unit_process" && data.processUuid.startsWith("market_"));
  const isLockedImport = data.importMode === "locked";
  const hasProduct = [...data.inputs, ...data.outputs].some((port) => port.type !== "biosphere" && Boolean(port.isProduct));
  const missingProduct =
    (data.nodeKind === "unit_process" || data.nodeKind === "market_process" || data.nodeKind === "lci_dataset") &&
    !hasProduct;
  const emptyPtsGraph =
    data.nodeKind === "pts_module" &&
    data.inputs.filter((port) => port.type !== "biosphere").length === 0 &&
    data.outputs.filter((port) => port.type !== "biosphere").length === 0 &&
    !String(data.referenceProduct ?? "").trim();
  const kindLabel =
    data.nodeKind === "unit_process" || data.nodeKind === "market_process"
      ? uiLanguage === "zh"
        ? marketProcess
          ? "市场过程"
          : "单元过程"
        : marketProcess
          ? "Market Process"
          : "Unit Process"
      : data.nodeKind === "pts_module"
        ? uiLanguage === "zh"
          ? "PTS模块"
          : "PTS Module"
        : uiLanguage === "zh"
          ? "LCI数据集"
          : "LCI Dataset";
  const roleLabel = "";
  const modeLabel =
    data.nodeKind === "unit_process"
      ? uiLanguage === "zh"
        ? data.mode === "balanced"
          ? "守恒"
          : "归一化"
        : data.mode
      : uiLanguage === "zh"
        ? "归一化"
        : "normalized";
  const ioLabels =
    uiLanguage === "zh"
      ? { inputs: "输入", outputs: "输出", none: "无" }
      : { inputs: "Inputs", outputs: "Outputs", none: "None" };
  const rowBg =
    marketProcess
      ? "#fffefe"
      : data.nodeKind === "lci_dataset"
        ? "#fffefd"
        : data.nodeKind === "pts_module"
          ? "#fcfffc"
          : "#fcfeff";
  const itemStyle: React.CSSProperties = {
    position: "relative",
    fontSize: 11,
    color: "#22313b",
    marginBottom: 2,
    padding: "2px 10px",
    borderRadius: 4,
    background: rowBg,
  };
  const outputItemStyle: React.CSSProperties = {
    ...itemStyle,
    textAlign: "right",
  };
  const inputLabelStyle: React.CSSProperties = {
    display: "block",
    overflow: "hidden",
    whiteSpace: "nowrap",
    textOverflow: "ellipsis",
  };
  const outputLabelStyle: React.CSSProperties = {
    ...inputLabelStyle,
    textAlign: "right",
  };
  const activeHandleStyle: React.CSSProperties = isNodeDragging
    ? { top: "50%", transform: "translateY(-50%)", width: 7, height: 7, pointerEvents: "none" }
    : { top: "50%", transform: "translateY(-50%)", width: 7, height: 7 };
  const formatPtsOutputLabel = (port: FlowPort): string => {
    const rawName = String(port.name ?? "").trim();
    const displayNameEn = String(port.displayNameEn ?? "").trim();
    const englishName = String(port.flowNameEn ?? "").trim();
    const sourceProcessName = String(port.sourceProcessName ?? "").trim();
    const sourceProcessUuid = String(port.sourceProcessUuid ?? "").trim();
    if (uiLanguage === "en" && displayNameEn) {
      return displayNameEn;
    }
    if (!rawName) {
      return sourceProcessName || sourceProcessUuid || "";
    }
    const atIndex = rawName.indexOf("@");
    const baseName = atIndex >= 0 ? rawName.slice(0, atIndex).trim() : rawName;
    const suffix = atIndex >= 0 ? rawName.slice(atIndex).trim() : "";
    const resolvedBaseName = uiLanguage === "en" && englishName ? englishName : baseName;
    if (suffix) {
      return `${resolvedBaseName} ${suffix}`.trim();
    }
    return `${resolvedBaseName} @ ${sourceProcessName || sourceProcessUuid || "unknown"}`;
  };
  const formatMarketInputLabel = (port: FlowPort): string => {
    const connectedLabel = connectedSourceInputLabelByPortId.get(port.id);
    if (connectedLabel) {
      return connectedLabel;
    }
    const rawName = String(port.name ?? "").trim();
    const ptsLabel = String(port.sourceProcessName ?? port.sourceProcessUuid ?? "").trim();
    const nestedLabel = String(port.nestedSourceProcessName ?? "").trim();
    const normalizedRaw = rawName.replace(/\s*@\s*/g, "@");
    const baseName = normalizedRaw.split("@")[0]?.trim() || normalizedRaw;
    if (!baseName) {
      return rawName;
    }
    if (nestedLabel) {
      const suffixParts = [ptsLabel, nestedLabel].filter(
        (part, index, arr) => part && arr.indexOf(part) === index,
      );
      return suffixParts.length > 0 ? `${baseName}@${suffixParts.join("@")}` : baseName;
    }
    return baseName;
  };
  const getDisplayFlowName = (name: string, flowUuid?: string, port?: FlowPort): string => {
    const baseName =
      data.nodeKind === "market_process" && port?.direction === "input"
        ? formatMarketInputLabel(port)
        : name;
    if (uiLanguage !== "en") {
      return baseName;
    }
      const portMatch =
        port ??
        (flowUuid ? visibleInputs.find((item) => item.flowUuid === flowUuid) : undefined) ??
        (flowUuid ? visibleOutputs.find((item) => item.flowUuid === flowUuid) : undefined);
      const englishDisplayName = String(portMatch?.displayNameEn ?? "").trim();
      const englishName =
        String(portMatch?.flowNameEn ?? "").trim() ||
        (flowUuid ? String(flowNameEnByUuid[flowUuid] ?? "").trim() : "");
      if (englishDisplayName) {
        return englishDisplayName;
      }
      if (!englishName) {
        return baseName;
      }
    const rawName = String(baseName ?? "");
    const atIndex = rawName.indexOf("@");
    if (atIndex >= 0) {
      const suffix = rawName.slice(atIndex).trim();
      return suffix ? `${englishName} ${suffix}` : englishName;
    }
    return englishName;
  };

  useEffect(() => {
    setDraftName(data.name);
  }, [data.name]);

  useEffect(() => {
    const shouldSkipFlowNameBackfill =
      uiLanguage !== "en" ||
      activeCanvasKind === "pts_internal" ||
      (activeCanvasKind === "root" && data.nodeKind === "pts_module");
    if (shouldSkipFlowNameBackfill) {
      return;
    }
    const existingEnglish = new Set(
      [...visibleInputs, ...visibleOutputs]
        .filter((port) => String(port.flowNameEn ?? "").trim())
        .map((port) => String(port.flowUuid ?? "").trim()),
    );
    const needFetch = visibleFlowUuids.filter((uuid) => !existingEnglish.has(uuid) && !flowNameEnByUuid[uuid]);
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
        const row = (await resp.json()) as { flow_uuid?: string; flow_name_en?: string | null };
        return row.flow_uuid ? row : null;
      }),
    )
      .then((rows) => {
        if (canceled) {
          return;
        }
        const patch: Record<string, string> = {};
        rows.forEach((row) => {
          if (!row?.flow_uuid) {
            return;
          }
          const englishName = String(row.flow_name_en ?? "").trim();
          if (englishName) {
            patch[row.flow_uuid] = englishName;
          }
        });
        if (Object.keys(patch).length > 0) {
          setFlowNameEnByUuid((prev) => ({ ...prev, ...patch }));
          updateNode(props.id, (node) => {
            const patchPorts = (ports: FlowPort[]) =>
              ports.map((port) => {
                const flowUuid = String(port.flowUuid ?? "").trim();
                const flowNameEn = flowUuid ? String(patch[flowUuid] ?? "").trim() : "";
                if (!flowNameEn || String(port.flowNameEn ?? "").trim() === flowNameEn) {
                  return port;
                }
                return {
                  ...port,
                  flowNameEn,
                };
              });
            return {
              ...node,
              data: {
                ...node.data,
                inputs: patchPorts(node.data.inputs),
                outputs: patchPorts(node.data.outputs),
              },
            };
          });
        }
      })
      .catch(() => {
        // ignore display-only lookup failures
      });
    return () => {
      canceled = true;
    };
  }, [activeCanvasKind, data.nodeKind, flowNameEnByUuid, props.id, uiLanguage, updateNode, visibleFlowUuids, visibleInputs, visibleOutputs]);

  useEffect(() => {
    // React Flow requires an internals refresh when dynamic handles change.
    updateNodeInternals(props.id);
    const rafId = window.requestAnimationFrame(() => {
      updateNodeInternals(props.id);
    });
    return () => {
      window.cancelAnimationFrame(rafId);
    };
  }, [data.inputs, data.outputs, props.id, updateNodeInternals]);

  const commitName = () => {
    const next = draftName.trim();
    if (!next || next === data.name) {
      setEditingName(false);
      setDraftName(data.name);
      return;
    }
    const duplicated = useLcaGraphStore
      .getState()
      .nodes.some(
      (node) => node.id !== props.id && normalizeNodeName(node.data.name) === normalizeNodeName(next),
    );
    if (duplicated) {
      setConnectionHint(uiLanguage === "zh" ? `过程名称重复：${next}` : `Duplicate process name: ${next}`);
      return;
    }
    updateNode(props.id, (node) => ({
      ...node,
      data: {
        ...node.data,
        name: next,
      },
    }));
    setConnectionHint(undefined);
    setEditingName(false);
  };

  const liveInputRows = useMemo(() => {
    if (freezeNodeContent && stableIoSectionsRef.current) {
      return stableIoSectionsRef.current.inputRows;
    }
    return visibleInputs.map((input) => {
      const inputLabel = getDisplayFlowName(input.name, input.flowUuid, input);
      return (
        <div key={input.id} style={itemStyle}>
          <Handle type="target" position={Position.Left} id={`in:${input.id}`} style={activeHandleStyle} />
          <Handle type="target" position={Position.Left} id={`inl:${input.id}`} style={{ ...activeHandleStyle, opacity: 0 }} />
          <Handle type="target" position={Position.Right} id={`inr:${input.id}`} style={activeHandleStyle} />
          <span style={inputLabelStyle} title={inputLabel}>
            {inputLabel}
          </span>
        </div>
      );
    });
  }, [activeHandleStyle, freezeNodeContent, inputLabelStyle, itemStyle, visibleInputs]);

  const liveOutputRows = useMemo(() => {
    if (freezeNodeContent && stableIoSectionsRef.current) {
      return stableIoSectionsRef.current.outputRows;
    }
    return visibleOutputs.map((output) => {
      const outputLabel =
        data.nodeKind === "pts_module" && (outputFlowCount.get(output.flowUuid) ?? 0) > 1
          ? formatPtsOutputLabel(output)
          : getDisplayFlowName(output.name, output.flowUuid, output);
      return (
        <div key={output.id} style={outputItemStyle}>
          <Handle type="source" position={Position.Left} id={`outl:${output.id}`} style={activeHandleStyle} />
          <span style={outputLabelStyle} title={outputLabel}>
            {outputLabel}
          </span>
          <Handle type="source" position={Position.Right} id={`out:${output.id}`} style={activeHandleStyle} />
          <Handle type="source" position={Position.Right} id={`outr:${output.id}`} style={{ ...activeHandleStyle, opacity: 0 }} />
        </div>
      );
    });
  }, [activeHandleStyle, data.nodeKind, freezeNodeContent, outputFlowCount, outputItemStyle, outputLabelStyle, visibleOutputs]);

  const showEmptyInputs = freezeNodeContent && stableIoSectionsRef.current
    ? stableIoSectionsRef.current.showEmptyInputs
    : visibleInputs.length === 0;
  const showEmptyOutputs = freezeNodeContent && stableIoSectionsRef.current
    ? stableIoSectionsRef.current.showEmptyOutputs
    : visibleOutputs.length === 0;

  useEffect(() => {
    if (freezeNodeContent) {
      return;
    }
    stableIoSectionsRef.current = {
      inputRows: liveInputRows,
      outputRows: liveOutputRows,
      showEmptyInputs: visibleInputs.length === 0,
      showEmptyOutputs: visibleOutputs.length === 0,
    };
  }, [freezeNodeContent, liveInputRows, liveOutputRows, visibleInputs.length, visibleOutputs.length]);

  return (
    <div className={`lca-node lca-node-${data.nodeKind}${marketProcess ? " lca-node-market" : ""}`}>
      <div className="lca-node-title nodrag" onClick={(event) => event.stopPropagation()}>
        {editingName ? (
          <input
            className="nodrag"
            value={draftName}
            autoFocus
            onChange={(event) => setDraftName(event.target.value)}
            onMouseDown={(event) => event.stopPropagation()}
            onBlur={commitName}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                commitName();
              } else if (event.key === "Escape") {
                setDraftName(data.name);
                setEditingName(false);
              }
            }}
          />
        ) : (
          <span className="lca-node-title-text" title={data.name} onClick={() => setEditingName(true)}>
            {data.name}
          </span>
        )}
      </div>
      <div className="lca-node-subtitle">{`${kindLabel}${roleLabel} | ${modeLabel}`}</div>
      {(isLockedImport || emptyPtsGraph || missingProduct) && (
        <div className="lca-node-badges">
          {isLockedImport && <span className="lca-node-badge lock">{uiLanguage === "zh" ? "只读导入" : "Read-only import"}</span>}
          {emptyPtsGraph && <span className="lca-node-badge warn">{uiLanguage === "zh" ? "内部为空" : "Empty graph"}</span>}
          {missingProduct && <span className="lca-node-badge warn">{uiLanguage === "zh" ? "未定义产品" : "No product"}</span>}
        </div>
      )}

      <div style={sectionTitleStyle}>{ioLabels.inputs}</div>
      {liveInputRows}
      {showEmptyInputs && <div style={itemStyle}>{ioLabels.none}</div>}

      <div style={sectionTitleStyle}>{ioLabels.outputs}</div>
      {liveOutputRows}
      {showEmptyOutputs && <div style={itemStyle}>{ioLabels.none}</div>}
      <div style={{ position: "absolute", inset: 0, overflow: "hidden", pointerEvents: "none" }}>
        {hiddenInputs.map((input, index) => {
          const top = getHiddenHandleTop("inputs", index, hiddenInputs.length);
          return (
          <div key={`hidden-in-${input.id}`}>
            <Handle type="target" position={Position.Left} id={`in:${input.id}`} style={hiddenHandleStyle("left", top)} />
            <Handle type="target" position={Position.Left} id={`inl:${input.id}`} style={hiddenHandleStyle("left", top)} />
            <Handle type="target" position={Position.Right} id={`inr:${input.id}`} style={hiddenHandleStyle("right", top)} />
          </div>
          );
        })}
        {hiddenOutputs.map((output, index) => {
          const top = getHiddenHandleTop("outputs", index, hiddenOutputs.length);
          return (
          <div key={`hidden-out-${output.id}`}>
            <Handle type="source" position={Position.Left} id={`outl:${output.id}`} style={hiddenHandleStyle("left", top)} />
            <Handle type="source" position={Position.Right} id={`out:${output.id}`} style={hiddenHandleStyle("right", top)} />
            <Handle type="source" position={Position.Right} id={`outr:${output.id}`} style={hiddenHandleStyle("right", top)} />
          </div>
          );
        })}
      </div>
    </div>
  );
}






