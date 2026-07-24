import { useMemo, useState } from "react";
import type { Node } from "@xyflow/react";
import type { FlowPort, IntermediateFlowLink, LcaNodeData } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { getLocalizedText } from "../../utils/localizedText";
import {
  confirmL2IntermediateFlowLink,
  resolveIntermediateFlowPorts,
  toIntermediateFlowLink,
  type RawResolution,
  type ResolveItem,
} from "../../services/intermediateFlowLinks";
import {
  IntermediateFlowL2ReviewDialog,
  type IntermediateFlowL2ReviewItem,
} from "./IntermediateFlowL2ReviewDialog";
import { L3UserProxyModal } from "./L3UserProxyModal";

type Props = {
  node: Node<LcaNodeData>;
  onStatus?: (text: string) => void;
};

export function IntermediateFlowLinkPanel({ node, onStatus }: Props) {
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const edges = useLcaGraphStore((state) => state.edges);
  const updateNode = useLcaGraphStore((state) => state.updateNode);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [resolutionState, setResolutionState] = useState<"idle" | "loading" | "ready" | "error">("idle");
  const [candidateByPort, setCandidateByPort] = useState<Record<string, RawResolution>>({});
  const [statusByPort, setStatusByPort] = useState<Record<string, ResolveItem["status"]>>({});
  const [reasonByPort, setReasonByPort] = useState<Record<string, string>>({});
  const [l2ReviewOpen, setL2ReviewOpen] = useState(false);
  const [proxyPortId, setProxyPortId] = useState<string>();
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);
  const conversionLabel = (level: "L1" | "L2" | "L3") =>
    level === "L1"
      ? t("自动转换", "Automatic conversion")
      : level === "L2"
        ? t("需确认转换", "Conversion needs confirmation")
        : t("手动转换", "Manual conversion");

  const inputs = useMemo(
    () => node.data.inputs.filter((port) => port.type !== "biosphere"),
    [node.data.inputs],
  );
  const connectedPortIds = useMemo(
    () => new Set(edges.filter((edge) => edge.target === node.id).map((edge) => edge.targetHandle?.replace(/^in:/, ""))),
    [edges, node.id],
  );
  const linkedCount = inputs.filter((port) => (
    port.intermediateFlowLink && port.intermediateFlowLink.status !== "inactive"
  )).length;

  const resolveCandidates = async () => {
    setBusy(true);
    setResolutionState("loading");
    try {
      const payload = await resolveIntermediateFlowPorts(
        inputs.filter((port) => !connectedPortIds.has(port.id)),
      );
      const nextCandidates: Record<string, RawResolution> = {};
      const nextStatuses: Record<string, ResolveItem["status"]> = {};
      const nextReasons: Record<string, string> = {};
      for (const item of payload.items) {
        if (!item.port_id) continue;
        nextStatuses[item.port_id] = item.status;
        if (item.reason) nextReasons[item.port_id] = item.reason;
        if (
          (item.status === "L1" || item.status === "L2" || item.status === "L3")
          && item.resolution
          && "source_flow_uuid" in item.resolution
        ) {
          nextCandidates[item.port_id] = item.resolution;
        }
      }
      setCandidateByPort(nextCandidates);
      setStatusByPort(nextStatuses);
      setReasonByPort(nextReasons);
      setResolutionState("ready");
    } catch (error) {
      setResolutionState("error");
      onStatus?.(error instanceof Error ? error.message : t("中间流匹配失败", "Intermediate flow matching failed"));
    } finally {
      setBusy(false);
    }
  };

  const openDialog = () => {
    setOpen(true);
    void resolveCandidates();
  };

  const applyAllL1 = () => {
    const links = new Map<string, ReturnType<typeof toIntermediateFlowLink>>();
    for (const [portId, resolution] of Object.entries(candidateByPort)) {
      if (resolution.mapping_level === "L1") links.set(portId, toIntermediateFlowLink(resolution));
    }
    if (links.size === 0) return;
    updateNode(node.id, (current) => ({
      ...current,
      data: {
        ...current.data,
        inputs: current.data.inputs.map((port) => {
          const link = links.get(port.id);
          return link ? { ...port, intermediateFlowLink: link } : port;
        }),
      },
    }));
    setCandidateByPort((current) => {
      const next = { ...current };
      links.forEach((_, portId) => delete next[portId]);
      return next;
    });
    onStatus?.(t(`已自动转换 ${links.size} 条中间流。`, `Automatically converted ${links.size} intermediate flows.`));
  };

  const l2ReviewItems = useMemo<IntermediateFlowL2ReviewItem[]>(() => inputs.flatMap((port) => {
    const resolution = candidateByPort[port.id];
    return resolution?.mapping_level === "L2" ? [{ port, resolution }] : [];
  }), [candidateByPort, inputs]);

  const l1Count = Object.values(candidateByPort).filter((item) => item.mapping_level === "L1").length;
  const unmatchedCount = inputs.filter((port) => (
    !port.intermediateFlowLink
    && resolutionState === "ready"
    && ["unmatched", "blocked", "skipped"].includes(statusByPort[port.id] ?? "unmatched")
  )).length;

  const applySelectedL2 = async (portIds: string[]) => {
    const selections = portIds.flatMap((portId) => {
      const port = inputs.find((item) => item.id === portId);
      const resolution = candidateByPort[portId];
      return port && resolution?.mapping_level === "L2" ? [{ port, resolution }] : [];
    });
    if (selections.length === 0) return;
    setBusy(true);
    try {
      const confirmed = await Promise.all(selections.map(async ({ port, resolution }) => ({
        portId: port.id,
        link: await confirmL2IntermediateFlowLink(port.flowUuid, resolution.rule_id),
      })));
      const links = new Map(confirmed.map((item) => [item.portId, item.link]));
      updateNode(node.id, (current) => ({
        ...current,
        data: {
          ...current.data,
          inputs: current.data.inputs.map((port) => {
            const link = links.get(port.id);
            return link ? { ...port, intermediateFlowLink: link } : port;
          }),
        },
      }));
      setCandidateByPort((current) => {
        const next = { ...current };
        links.forEach((_, portId) => delete next[portId]);
        return next;
      });
      setL2ReviewOpen(false);
      onStatus?.(t(
        `已确认并转换 ${links.size} 条中间流；请继续选择具体背景 LCI。`,
        `Confirmed and converted ${links.size} intermediate flows; choose specific background LCI providers next.`,
      ));
    } catch (error) {
      onStatus?.(error instanceof Error ? error.message : t("批量确认转换失败", "Batch conversion confirmation failed"));
    } finally {
      setBusy(false);
    }
  };

  const applyReviewedLink = async (port: FlowPort, resolution: RawResolution) => {
    setBusy(true);
    try {
      const link = resolution.mapping_level === "L2"
        ? await confirmL2IntermediateFlowLink(port.flowUuid, resolution.rule_id)
        : toIntermediateFlowLink(resolution);
      updateNode(node.id, (current) => ({
        ...current,
        data: {
          ...current.data,
          inputs: current.data.inputs.map((item) =>
            item.id === port.id ? { ...item, intermediateFlowLink: link } : item,
          ),
        },
      }));
      setCandidateByPort((current) => {
        const next = { ...current };
        delete next[port.id];
        return next;
      });
      onStatus?.(resolution.mapping_level === "L2"
        ? t("已确认该转换；请继续选择具体背景 LCI。", "Confirmed this conversion; choose a background LCI next.")
        : t("已复用手动转换；请继续选择具体背景 LCI。", "Reused this manual conversion; choose a background LCI next."));
    } catch (error) {
      onStatus?.(error instanceof Error ? error.message : t("确认映射失败", "Link confirmation failed"));
    } finally {
      setBusy(false);
    }
  };

  const handleL3ProxyConfirm = (port: FlowPort, link: IntermediateFlowLink) => {
    updateNode(node.id, (current) => ({
      ...current,
      data: {
        ...current.data,
        inputs: current.data.inputs.map((item) =>
          item.id === port.id ? { ...item, intermediateFlowLink: link } : item,
        ),
      },
    }));
    setProxyPortId(undefined);
    onStatus?.(t(
      "已保存手动转换；该关系不表示环境或 provider 等价。请继续选择具体背景 LCI。",
      "Saved manual conversion; it does not imply environmental or provider equivalence. Choose a specific background LCI next.",
    ));
  };

  if (inputs.length === 0) return null;
  return (
    <>
      <button
        type="button"
        className="text-btn inspector-toolbar-btn intermediate-flow-link-trigger"
        onClick={openDialog}
      >
        {t("中间流转换", "Convert Flows")}
        <span>{linkedCount}/{inputs.length}</span>
      </button>
      {open && (
        <div className="overlay-modal" onMouseDown={() => setOpen(false)}>
          <section
            className="overlay-panel intermediate-flow-link-dialog"
            role="dialog"
            aria-modal="true"
            aria-label={t("中间流转换", "Intermediate Flow Conversion")}
            onMouseDown={(event) => event.stopPropagation()}
          >
            <header className="overlay-head intermediate-flow-link-dialog-head">
              <div className="intermediate-flow-link-title">
                <strong>{t("中间流转换", "Intermediate Flow Conversion")}</strong>
                <span>{inputs.length}</span>
              </div>
              <button type="button" className="drawer-close-btn" onClick={() => setOpen(false)}>
                {t("关闭", "Close")}
              </button>
            </header>
            <div className="intermediate-flow-link-dialog-toolbar">
              <div className="intermediate-flow-link-overview">
                <p>{t(
                  "在此完成中间流转换。可自动转换的流会直接处理；需要核对的流请确认后转换；没有自动结果时可手动转换。完成后回到清单分析，在对应流的“关联背景数据”中选择背景过程。",
                  "Convert intermediate flows here. Automatically convertible flows can be processed directly; review flows that need confirmation; use manual conversion when no automatic result is available. Then return to inventory analysis and use Link background data on the relevant flow to choose a background process.",
                )}</p>
                <div className="intermediate-flow-link-counts" aria-live="polite">
                  {resolutionState === "loading" ? (
                    <span>{t("正在检测可转换关系…", "Checking conversion candidates…")}</span>
                  ) : resolutionState === "error" ? (
                    <span className="error-text">{t("检测失败，请重试", "Candidate check failed; retry")}</span>
                  ) : (
                    <>
                      <span className="approved">{t("自动可转换", "Auto-convertible")} {l1Count}</span>
                      <span className="review">{t("需确认", "Needs review")} {l2ReviewItems.length}</span>
                      <span>{t("需手动转换", "Manual conversion needed")} {unmatchedCount}</span>
                    </>
                  )}
                </div>
              </div>
              <div className="intermediate-flow-link-toolbar-actions">
                {resolutionState === "error" && (
                  <button type="button" className="flow-link-button secondary" disabled={busy} onClick={resolveCandidates}>
                    {t("重新检测", "Retry")}
                  </button>
                )}
                <button type="button" className="flow-link-button primary" disabled={busy || l1Count === 0} onClick={applyAllL1}>
                  {t(`自动转换（${l1Count}）`, `Auto-convert (${l1Count})`)}
                </button>
                <button type="button" className="flow-link-button secondary" disabled={busy || l2ReviewItems.length === 0} onClick={() => setL2ReviewOpen(true)}>
                  {t(`确认转换（${l2ReviewItems.length}）`, `Confirm conversion (${l2ReviewItems.length})`)}
                </button>
              </div>
            </div>
            <div className="intermediate-flow-link-dialog-body">
              <div className="intermediate-flow-link-list">
      {inputs.map((port) => {
        const link = port.intermediateFlowLink;
        const review = candidateByPort[port.id];
        const resolutionStatus = statusByPort[port.id];
        return (
          <div className="intermediate-flow-link-row" key={port.id}>
            <div className="intermediate-flow-link-summary">
              <strong title={getLocalizedText(port.name, uiLanguage, port.name)}>
                {getLocalizedText(port.name, uiLanguage, port.name)}
              </strong>
              <span className={`intermediate-flow-level-badge ${link ? "approved" : review ? "review" : "unmatched"}`}>
                {link && link.status !== "inactive"
                  ? conversionLabel(link.mappingLevel)
                  : review?.mapping_level
                    ? conversionLabel(review.mapping_level)
                    : (resolutionState === "loading" || resolutionState === "idle"
                      ? t("检测中", "Checking")
                      : t("无候选", "No candidate"))}
              </span>
            </div>
            {link && link.status !== "inactive" ? (
              <>
                <div className="intermediate-flow-link-target">
                  <span>{t("已完成转换", "Conversion completed")}</span>
                  {link.applicationMode === "auto_compatible" && (
                    <span className="muted-text" title={(link.warnings ?? []).join(", ")}>
                      {t("语义泛化，计算前请核对", "Review semantic generalization before calculation")}
                    </span>
                  )}
                </div>
                <div className="intermediate-flow-link-actions">
                  <span className="muted-text">
                    {t("请回到清单分析，点击该流的“关联背景数据”。", "Return to inventory analysis and use Link background data on this flow.")}
                  </span>
                </div>
              </>
            ) : review ? (
              <div className="intermediate-flow-review-card">
                <span>
                  {t("目标：", "Target: ")}
                  {review.target_flow_name || review.target_flow_name_en || review.target_flow_uuid}
                  {` · ${review.target_unit}`}
                </span>
                <span className="muted-text">
                  {review.mapping_level === "L1"
                    ? t("可自动转换，可通过上方按钮批量处理。", "Ready for automatic conversion; use the action above to process it in bulk.")
                    : review.mapping_level === "L2"
                      ? t("需要核对转换目标，确认后才写入模型。", "Review the conversion target before writing it into the model.")
                      : t("这是已保存的手动转换，需逐条确认复用。", "This saved manual conversion must be reused per flow explicitly.")}
                </span>
                {(review.warnings ?? []).length > 0 && (
                  <span className="intermediate-flow-review-hint" title={(review.warnings ?? []).join(" · ")}>
                    {t("需核对产品范围和限定词", "Review product scope and qualifiers")}
                  </span>
                )}
                <div className="intermediate-flow-link-actions">
                  {review.mapping_level === "L1" ? (
                    <span className="muted-text">{t("等待自动转换", "Ready for automatic conversion")}</span>
                  ) : review.mapping_level === "L2" ? (
                    <span className="muted-text">{t("等待确认转换", "Ready for confirmation")}</span>
                  ) : (
                    <button type="button" className="flow-link-button primary compact" disabled={busy} onClick={() => applyReviewedLink(port, review)}>
                      {t("复用手动转换", "Reuse manual conversion")}
                    </button>
                  )}
                  {review.mapping_level === "L3" && (
                    <button type="button" className="flow-link-button ghost compact" disabled={busy} onClick={() => setProxyPortId(port.id)}>
                      {t("更换代理", "Change proxy")}
                    </button>
                  )}
                </div>
              </div>
            ) : null}
            {!link && !review && resolutionState === "ready" && (
              <div className="intermediate-flow-link-actions">
                <span className="muted-text" title={reasonByPort[port.id] ?? resolutionStatus}>
                  {t("未找到可自动转换的结果", "No automatic conversion is available")}
                </span>
                <button type="button" className="flow-link-button ghost compact" onClick={() => setProxyPortId(port.id)}>
                  {t("手动转换", "Manual conversion")}
                </button>
              </div>
            )}
          </div>
        );
      })}
              </div>
            </div>
          </section>
        </div>
      )}
      <L3UserProxyModal
        open={Boolean(proxyPortId)}
        busy={busy}
        port={inputs.find((item) => item.id === proxyPortId) ?? null}
        language={uiLanguage}
        onClose={() => setProxyPortId(undefined)}
        onConfirm={handleL3ProxyConfirm}
        onStatus={onStatus}
      />
      <IntermediateFlowL2ReviewDialog
        open={l2ReviewOpen}
        busy={busy}
        items={l2ReviewItems}
        language={uiLanguage}
        onClose={() => setL2ReviewOpen(false)}
        onConfirm={applySelectedL2}
      />
    </>
  );
}
