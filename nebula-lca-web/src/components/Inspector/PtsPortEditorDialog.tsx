import { useMemo } from "react";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import * as React from "react";

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;

type Candidate = {
  key: string;
  flowUuid: string;
  name: string;
  flowNameEn?: string;
  portKey?: string;
  productKey?: string;
  sourceProcessUuid?: string;
  sourceProcessName?: string;
  sourceNodeId?: string;
  isProduct?: boolean;
  unit: string;
  unitGroup?: string;
  type: "technosphere" | "biosphere";
  sourceNames: string[];
  defaultVisible: boolean;
  internalExposed: boolean;
  showOnNode: boolean;
  portId?: string;
};

const formatPortDisplayName = (
  name: string,
  flowNameEn: string | undefined,
  sourceProcessName: string | undefined,
  sourceProcessUuid: string | undefined,
  uiLanguage: "zh" | "en",
): string => {
  const trimmedName = String(name ?? "").trim();
  const englishName = String(flowNameEn ?? "").trim();
  const sourceLabel = String(sourceProcessName ?? sourceProcessUuid ?? "").trim();
  if (!trimmedName) {
    return sourceLabel;
  }
  const atIndex = trimmedName.indexOf("@");
  if (atIndex >= 0) {
    if (!(uiLanguage === "en" && englishName)) {
      return trimmedName;
    }
    const suffix = trimmedName.slice(atIndex).trim();
    return suffix ? `${englishName} ${suffix}` : englishName;
  }
  const baseLabel = uiLanguage === "en" && englishName ? englishName : trimmedName;
  if (!sourceLabel) {
    return baseLabel;
  }
  return `${baseLabel} @ ${sourceLabel}`;
};

const getCandidateKey = (
  direction: "input" | "output",
  data: Pick<Candidate, "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId" | "portKey" | "productKey">,
): string => {
  if (direction === "output") {
    const portKey = String(data.portKey ?? "").trim();
    if (portKey) {
      return `port:${portKey}`;
    }
    const productKey = String(data.productKey ?? "").trim();
    if (productKey) {
      return `product:${productKey}`;
    }
    const sourceProcessUuid = String(data.sourceProcessUuid ?? "").trim();
    if (sourceProcessUuid) {
      return `${data.flowUuid}@@${sourceProcessUuid}`;
    }
    const sourceNodeId = String(data.sourceNodeId ?? "").trim();
    if (sourceNodeId) {
      return `${data.flowUuid}@@node:${sourceNodeId}`;
    }
    const sourceProcessName = String(data.sourceProcessName ?? "").trim();
    if (sourceProcessName) {
      return `${data.flowUuid}@@name:${sourceProcessName}`;
    }
  }
  return data.flowUuid;
};

const getCandidateSourceKey = (
  direction: "input" | "output",
  data: Pick<Candidate, "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId">,
): string =>
  direction === "output"
    ? `${data.flowUuid}@@${String(data.sourceProcessUuid ?? "").trim()}@@${String(data.sourceNodeId ?? "").trim()}@@${String(data.sourceProcessName ?? "").trim()}`
    : data.flowUuid;

const isSameCandidateIdentity = (
  direction: "input" | "output",
  left: Pick<Candidate, "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId" | "portKey" | "productKey">,
  right: Pick<Candidate, "flowUuid" | "sourceProcessUuid" | "sourceProcessName" | "sourceNodeId" | "portKey" | "productKey">,
): boolean =>
  getCandidateKey(direction, left) === getCandidateKey(direction, right) ||
  getCandidateSourceKey(direction, left) === getCandidateSourceKey(direction, right);

const keyDataFromPort = (port: {
  flowUuid: string;
  sourceProcessUuid?: string;
  sourceProcessName?: string;
  sourceNodeId?: string;
  portKey?: string;
  productKey?: string;
}) => ({
  flowUuid: port.flowUuid,
  sourceProcessUuid: port.sourceProcessUuid,
  sourceProcessName: port.sourceProcessName,
  sourceNodeId: port.sourceNodeId,
  portKey: port.portKey,
  productKey: port.productKey,
});

const isIntermediateCandidate = (port: Pick<Candidate, "type">) => port.type === "technosphere";

export function PtsPortEditorDialog() {
  const ptsPortEditor = useLcaGraphStore((state) => state.ptsPortEditor);
  const canvases = useLcaGraphStore((state) => state.canvases);
  const activeCanvasKind = useLcaGraphStore((state) => state.activeCanvasKind);
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const closePtsPortEditor = useLcaGraphStore((state) => state.closePtsPortEditor);
  const setPtsPortExposureByFlow = useLcaGraphStore((state) => state.setPtsPortExposureByFlow);
  const setPtsPortVisibility = useLcaGraphStore((state) => state.setPtsPortVisibility);
  const isInternalEditor = activeCanvasKind === "pts_internal";
  const [flowNameEnByUuid, setFlowNameEnByUuid] = React.useState<Record<string, string>>({});

  const ptsNode = useMemo(() => {
    const ptsNodeId = ptsPortEditor.ptsNodeId;
    if (!ptsNodeId) {
      return undefined;
    }
    return canvases.root?.nodes.find((node) => node.id === ptsNodeId && node.data.nodeKind === "pts_module");
  }, [canvases, ptsPortEditor.ptsNodeId]);

  const internalCanvases = useMemo(() => {
    if (!ptsNode) {
      return [] as Array<(typeof canvases)[string]>;
    }
    return Object.values(canvases).filter(
      (canvas) => canvas.kind === "pts_internal" && canvas.parentPtsNodeId === ptsNode.id,
    );
  }, [canvases, ptsNode]);

  const collectCandidates = (direction: "input" | "output"): Candidate[] => {
    const ptsPorts = direction === "input" ? (ptsNode?.data.inputs ?? []) : (ptsNode?.data.outputs ?? []);
    if (!isInternalEditor) {
      return ptsPorts
        .filter((port) => port.type !== "biosphere")
        .map((port) => ({
          key: getCandidateKey(direction, keyDataFromPort(port)),
          flowUuid: port.flowUuid,
          name: port.name,
          flowNameEn: port.flowNameEn,
          portKey: port.portKey,
          productKey: port.productKey,
          sourceProcessUuid: port.sourceProcessUuid,
          sourceProcessName: port.sourceProcessName,
          sourceNodeId: port.sourceNodeId,
          isProduct: port.isProduct,
          unit: port.unit,
          unitGroup: port.unitGroup,
          type: (port.type === "biosphere" ? "biosphere" : "technosphere") as Candidate["type"],
          sourceNames: [port.sourceProcessName || port.sourceProcessUuid || "—"],
          defaultVisible: Boolean(port.showOnNode),
          internalExposed: port.internalExposed ?? true,
          showOnNode: Boolean(port.showOnNode),
          portId: port.id,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));
    }

    const byFlow = new Map<string, Candidate>();
    const ptsPortByFlow = new Map(
      ptsPorts.map((port) => [
        getCandidateKey(direction, keyDataFromPort(port)),
        port,
      ]),
    );
    if (internalCanvases.length === 0) {
      return [];
    }

    const parsePortId = (handleId: string | undefined, prefix: "in:" | "out:"): string | undefined => {
      if (!handleId || !handleId.startsWith(prefix)) {
        return undefined;
      }
      return handleId.slice(prefix.length);
    };

    for (const canvas of internalCanvases) {
      for (const node of canvas.nodes) {
        const ports = direction === "input" ? node.data.inputs : node.data.outputs;
        for (const port of ports) {
          if (port.type === "biosphere" || !port.flowUuid) {
            continue;
          }
          const relatedEdges = canvas.edges.filter((edge) => {
            if (direction === "input") {
              if (edge.target !== node.id) {
                return false;
              }
              const targetPortId = parsePortId(edge.targetHandle ?? undefined, "in:");
              if (targetPortId) {
                return targetPortId === port.id;
              }
              return (edge.data?.flowUuid ?? "") === port.flowUuid;
            }
            if (edge.source !== node.id) {
              return false;
            }
            const sourcePortId = parsePortId(edge.sourceHandle ?? undefined, "out:");
            if (sourcePortId) {
              return sourcePortId === port.id;
            }
            return (edge.data?.flowUuid ?? "") === port.flowUuid;
          });
          const connected = relatedEdges.length > 0;
          if (direction === "input" && connected) {
            continue;
          }
          if (direction === "output" && !port.isProduct && connected) {
            continue;
          }
          const key = getCandidateKey(direction, {
            flowUuid: port.flowUuid,
            portKey: port.portKey,
            productKey: port.productKey,
            sourceProcessUuid: direction === "output" ? (node.data.processUuid || undefined) : undefined,
          });
          const existing = byFlow.get(key);
          if (!existing) {
            const ptsPort =
              ptsPortByFlow.get(key) ??
              ptsPorts.find((p) =>
                isSameCandidateIdentity(direction, keyDataFromPort(p), {
                  flowUuid: port.flowUuid,
                  portKey: port.portKey,
                  productKey: port.productKey,
                  sourceProcessUuid: direction === "output" ? (node.data.processUuid || undefined) : undefined,
                  sourceProcessName: direction === "output" ? node.data.name : undefined,
                  sourceNodeId: direction === "output" ? node.id : undefined,
                }),
              );
            byFlow.set(key, {
              key,
              flowUuid: port.flowUuid,
              name: port.name,
              flowNameEn: port.flowNameEn,
              portKey: port.portKey,
              productKey: port.productKey,
              sourceProcessUuid: direction === "output" ? (node.data.processUuid || undefined) : undefined,
              sourceProcessName: direction === "output" ? node.data.name : undefined,
              sourceNodeId: direction === "output" ? node.id : undefined,
              isProduct: Boolean(port.isProduct) && direction === "output",
              unit: port.unit,
              unitGroup: port.unitGroup,
              type: "technosphere",
              sourceNames: [node.data.name],
              defaultVisible: Boolean(port.showOnNode),
              internalExposed: ptsPort ? (ptsPort.internalExposed ?? true) : true,
              showOnNode: ptsPort ? (ptsPort.showOnNode ?? true) : true,
              portId: ptsPort?.id,
            });
          } else {
            if (!existing.sourceNames.includes(node.data.name)) {
              existing.sourceNames.push(node.data.name);
            }
            if (port.showOnNode) {
              existing.defaultVisible = true;
            }
          }
        }
      }
    }

    return Array.from(byFlow.values())
      .filter(isIntermediateCandidate)
      .sort((a, b) => a.name.localeCompare(b.name));
  };

  const inputCandidates = collectCandidates("input").filter(isIntermediateCandidate);
  const outputCandidates = collectCandidates("output").filter(isIntermediateCandidate);
  const outputGridClass = "with-extra-column no-show-node";
  const dialogTitle = isInternalEditor
    ? uiLanguage === "zh"
      ? "PTS 内部暴露设置"
      : "PTS Internal Exposure"
    : uiLanguage === "zh"
      ? "PTS 外部端口"
      : "PTS External Ports";
  const dialogHint = isInternalEditor
    ? uiLanguage === "zh"
      ? "当前为内部编辑态：这里配置哪些内部端口允许投影到主图外部。"
      : "Internal edit mode: configure which internal ports may be projected to the root canvas."
    : uiLanguage === "zh"
      ? "这里控制当前已发布端口在主图 PTS 节点上的默认显示。"
      : "Control the default visibility of published ports on the root-canvas PTS node.";
  const inputSectionTitle = isInternalEditor
    ? uiLanguage === "zh"
      ? "可暴露输入端口"
      : "Exposable Input Ports"
    : uiLanguage === "zh"
      ? "外部输入端口"
      : "External Input Ports";
  const outputSectionTitle = isInternalEditor
    ? uiLanguage === "zh"
      ? "可暴露输出端口"
      : "Exposable Output Ports"
    : uiLanguage === "zh"
      ? "外部输出端口"
      : "External Output Ports";
  const exposureColumnLabel = isInternalEditor
    ? uiLanguage === "zh"
      ? "允许外部暴露"
      : "Expose to Root"
    : uiLanguage === "zh"
      ? "显示节点"
      : "Visible";
  const sourceColumnLabel = uiLanguage === "zh" ? (isInternalEditor ? "内部来源过程" : "来源过程") : "Source Process";
  const indexColumnLabel = uiLanguage === "zh" ? "序号" : "No.";
  const flowNameColumnLabel = uiLanguage === "zh" ? "流名称" : "Flow Name";
  const unitColumnLabel = uiLanguage === "zh" ? "单位" : "Unit";
  const closeLabel = uiLanguage === "zh" ? "关闭" : "Close";
  const missingPtsLabel = uiLanguage === "zh" ? "未找到 PTS 节点。" : "PTS node not found.";
  const emptyInputLabel = uiLanguage === "zh" ? "暂无可选中间流输入端口" : "No intermediate input ports available.";
  const emptyOutputLabel = uiLanguage === "zh" ? "暂无可选中间流输出端口" : "No intermediate output ports available.";
  const sourceNamesSeparator = uiLanguage === "zh" ? "，" : ", ";

  React.useEffect(() => {
    if (uiLanguage !== "en") {
      return;
    }
    const candidates = [...inputCandidates, ...outputCandidates];
    const needFetch = Array.from(
      new Set(
        candidates
          .map((port) => String(port.flowUuid ?? "").trim())
          .filter((flowUuid) => flowUuid.length > 0)
          .filter((flowUuid) => {
            const candidateHasEnglish = candidates.some(
              (port) => port.flowUuid === flowUuid && String(port.flowNameEn ?? "").trim(),
            );
            return !candidateHasEnglish && !String(flowNameEnByUuid[flowUuid] ?? "").trim();
          }),
      ),
    );
    if (needFetch.length === 0) {
      return;
    }
    let cancelled = false;
    Promise.all(
      needFetch.map(async (flowUuid) => {
        const resp = await fetch(`${API_BASE}/reference/flows/${encodeURIComponent(flowUuid)}`, { cache: "no-store" });
        if (!resp.ok) {
          return null;
        }
        const payload = (await resp.json()) as { flow_uuid?: string; flow_name_en?: string | null };
        return payload.flow_uuid ? payload : null;
      }),
    )
      .then((rows) => {
        if (cancelled) {
          return;
        }
        const patch: Record<string, string> = {};
        rows.forEach((row) => {
          const flowUuid = String(row?.flow_uuid ?? "").trim();
          const flowNameEn = String(row?.flow_name_en ?? "").trim();
          if (flowUuid && flowNameEn) {
            patch[flowUuid] = flowNameEn;
          }
        });
        if (Object.keys(patch).length > 0) {
          setFlowNameEnByUuid((prev) => ({ ...prev, ...patch }));
        }
      })
      .catch(() => {
        // ignore display-only lookup failures
      });
    return () => {
      cancelled = true;
    };
  }, [flowNameEnByUuid, inputCandidates, outputCandidates, uiLanguage]);

  const getExposed = (
    direction: "input" | "output",
    flowUuid: string,
    sourceProcessUuid?: string,
    sourceProcessName?: string,
    sourceNodeId?: string,
    portKey?: string,
    productKey?: string,
  ): boolean => {
    if (!ptsNode) {
      return false;
    }
    const ports = direction === "input" ? ptsNode.data.inputs : ptsNode.data.outputs;
    const hit = ports.find(
      (p) =>
        isSameCandidateIdentity(
          direction,
          {
            flowUuid: p.flowUuid,
            sourceProcessUuid: p.sourceProcessUuid,
            sourceProcessName: p.sourceProcessName,
            sourceNodeId: p.sourceNodeId,
            portKey: p.portKey,
            productKey: p.productKey,
          },
          { flowUuid, sourceProcessUuid, sourceProcessName, sourceNodeId, portKey, productKey },
        ),
    );
    return Boolean(hit ? (hit.internalExposed ?? true) : false);
  };

  const getNodeVisible = (
    direction: "input" | "output",
    flowUuid: string,
    sourceProcessUuid?: string,
    sourceProcessName?: string,
    sourceNodeId?: string,
    portKey?: string,
    productKey?: string,
  ): boolean => {
    if (!ptsNode) {
      return false;
    }
    const ports = direction === "input" ? ptsNode.data.inputs : ptsNode.data.outputs;
    const hit = ports.find(
      (p) =>
        isSameCandidateIdentity(
          direction,
          {
            flowUuid: p.flowUuid,
            sourceProcessUuid: p.sourceProcessUuid,
            sourceProcessName: p.sourceProcessName,
            sourceNodeId: p.sourceNodeId,
            portKey: p.portKey,
            productKey: p.productKey,
          },
          { flowUuid, sourceProcessUuid, sourceProcessName, sourceNodeId, portKey, productKey },
        ),
    );
    if (!hit) {
      return false;
    }
    return Boolean(hit.showOnNode);
  };

  if (!ptsPortEditor.open) {
    return null;
  }

  if (!ptsNode) {
    return (
      <div className="overlay-modal">
        <div className="overlay-panel">
          <div className="inspector-drawer-head">
            <div className="drawer-title">{dialogTitle}</div>
            <button className="drawer-close-btn" onClick={closePtsPortEditor}>
              {closeLabel}
            </button>
          </div>
          <div className="table-empty">{missingPtsLabel}</div>
        </div>
      </div>
    );
  }

  return (
    <div className="overlay-modal">
      <div className="overlay-panel">
        <div className="inspector-drawer-head">
          <div className="drawer-title">{dialogTitle}</div>
          <button className="drawer-close-btn" onClick={closePtsPortEditor}>
            {closeLabel}
          </button>
        </div>
        <div className="pts-port-scroll">
          <div className="pts-port-meta">
            <strong>{ptsNode.data.name}</strong>
            <span>{ptsNode.data.processUuid}</span>
          </div>
          <div className="table-empty" style={{ marginBottom: 12 }}>
            {dialogHint}
          </div>
          <section className="inventory-section">
            <div className="inventory-section-head">
              <h4>{inputSectionTitle}</h4>
            </div>
            <div className={`inventory-grid-header ${outputGridClass}`}>
              <div>{indexColumnLabel}</div>
              <div>{flowNameColumnLabel}</div>
              <div>{unitColumnLabel}</div>
              <div>{exposureColumnLabel}</div>
              <div>{sourceColumnLabel}</div>
            </div>
            {inputCandidates.map((port, idx) => {
              const checked = isInternalEditor
                ? getExposed(
                    "input",
                    port.flowUuid,
                    port.sourceProcessUuid,
                    port.sourceProcessName,
                    port.sourceNodeId,
                    port.portKey,
                    port.productKey,
                  )
                : getNodeVisible(
                  "input",
                  port.flowUuid,
                  port.sourceProcessUuid,
                  port.sourceProcessName,
                  port.sourceNodeId,
                  port.portKey,
                  port.productKey,
                );
              return (
                <div key={`in_${port.key}`} className="inventory-grid-row with-extra-column no-show-node">
                  <div>{idx + 1}</div>
                  <div className="flow-name-readonly">
                    {formatPortDisplayName(
                      port.name,
                      String(port.flowNameEn ?? "").trim() || flowNameEnByUuid[port.flowUuid],
                      undefined,
                      undefined,
                      uiLanguage,
                    )}
                  </div>
                  <div className="flow-name-readonly">{port.unit}</div>
                  <label className="inline-checkbox">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={(event) => {
                        if (isInternalEditor) {
                          setPtsPortExposureByFlow(
                            ptsNode.id,
                            "input",
                            {
                              flowUuid: port.flowUuid,
                              name: port.name,
                              portKey: port.portKey,
                              productKey: port.productKey,
                              unit: port.unit,
                              unitGroup: port.unitGroup,
                              type: port.type,
                            },
                            event.target.checked,
                          );
                          return;
                        }
                        if (port.portId) {
                          setPtsPortVisibility(ptsNode.id, "input", port.portId, event.target.checked);
                        }
                      }}
                      disabled={!isInternalEditor && !port.portId}
                    />
                  </label>
                  <div className="table-empty">{port.sourceNames.join(sourceNamesSeparator)}</div>
                </div>
              );
            })}
            {inputCandidates.length === 0 && <div className="table-empty">{emptyInputLabel}</div>}
          </section>
          <section className="inventory-section">
            <div className="inventory-section-head">
              <h4>{outputSectionTitle}</h4>
            </div>
            <div className="inventory-grid-header with-extra-column no-show-node">
              <div>{indexColumnLabel}</div>
              <div>{flowNameColumnLabel}</div>
              <div>{unitColumnLabel}</div>
              <div>{exposureColumnLabel}</div>
              <div>{sourceColumnLabel}</div>
            </div>
            {outputCandidates.map((port, idx) => {
              const checked = isInternalEditor
                ? getExposed(
                    "output",
                    port.flowUuid,
                    port.sourceProcessUuid,
                    port.sourceProcessName,
                    port.sourceNodeId,
                    port.portKey,
                    port.productKey,
                  )
                : getNodeVisible(
                  "output",
                  port.flowUuid,
                  port.sourceProcessUuid,
                  port.sourceProcessName,
                  port.sourceNodeId,
                  port.portKey,
                  port.productKey,
                );
              return (
                <div key={port.key} className={`inventory-grid-row ${outputGridClass}`}>
                  <div>{idx + 1}</div>
                  <div className="flow-name-readonly">
                    {formatPortDisplayName(
                      port.name,
                      String(port.flowNameEn ?? "").trim() || flowNameEnByUuid[port.flowUuid],
                      port.sourceProcessName,
                      port.sourceProcessUuid,
                      uiLanguage,
                    )}
                  </div>
                  <div className="flow-name-readonly">{port.unit}</div>
                  <label className="inline-checkbox">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={(event) => {
                        if (isInternalEditor) {
                          setPtsPortExposureByFlow(
                            ptsNode.id,
                            "output",
                            {
                              flowUuid: port.flowUuid,
                              name: port.name,
                              portKey: port.portKey,
                              productKey: port.productKey,
                              sourceProcessUuid: port.sourceProcessUuid,
                              sourceProcessName: port.sourceProcessName,
                              sourceNodeId: port.sourceNodeId,
                              isProduct: port.isProduct,
                              unit: port.unit,
                              unitGroup: port.unitGroup,
                              type: port.type,
                            },
                            event.target.checked,
                          );
                          return;
                        }
                        if (port.portId) {
                          setPtsPortVisibility(ptsNode.id, "output", port.portId, event.target.checked);
                        }
                      }}
                      disabled={!isInternalEditor && !port.portId}
                    />
                  </label>
                  <div className="table-empty">{port.sourceProcessName || port.sourceProcessUuid || "—"}</div>
                </div>
              );
            })}
            {outputCandidates.length === 0 && <div className="table-empty">{emptyOutputLabel}</div>}
          </section>
        </div>
      </div>
    </div>
  );
}

