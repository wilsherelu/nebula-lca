import { useMemo, useState } from "react";
import type { Node } from "@xyflow/react";
import { getApiBase } from "../../apiBase";
import type { FlowPort, LcaNodeData } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { parseImportedRows } from "../NodePalette/UnitProcessImportDialog";
import { getLocalizedText } from "../../utils/localizedText";
import {
  confirmL2IntermediateFlowLink,
  fetchIntermediateFlowProviders,
  createUserProxyRule,
  resolveIntermediateFlowPorts,
  searchEcoIntermediateFlows,
  toIntermediateFlowLink,
  type EcoIntermediateFlow,
  type ProviderCandidate,
  type RawResolution,
  type ResolveItem,
} from "../../services/intermediateFlowLinks";
import {
  IntermediateFlowL2ReviewDialog,
  type IntermediateFlowL2ReviewItem,
} from "./IntermediateFlowL2ReviewDialog";

type Props = {
  node: Node<LcaNodeData>;
  onStatus?: (text: string) => void;
};

const API_BASE = getApiBase();

export function IntermediateFlowLinkPanel({ node, onStatus }: Props) {
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const edges = useLcaGraphStore((state) => state.edges);
  const updateNode = useLcaGraphStore((state) => state.updateNode);
  const connectProvider = useLcaGraphStore((state) => state.connectIntermediateProvider);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [resolutionState, setResolutionState] = useState<"idle" | "loading" | "ready" | "error">("idle");
  const [providersByPort, setProvidersByPort] = useState<Record<string, ProviderCandidate[]>>({});
  const [candidateByPort, setCandidateByPort] = useState<Record<string, RawResolution>>({});
  const [statusByPort, setStatusByPort] = useState<Record<string, ResolveItem["status"]>>({});
  const [reasonByPort, setReasonByPort] = useState<Record<string, string>>({});
  const [l2ReviewOpen, setL2ReviewOpen] = useState(false);
  const [proxyPortId, setProxyPortId] = useState<string>();
  const [proxyQuery, setProxyQuery] = useState("");
  const [proxyReason, setProxyReason] = useState("");
  const [proxyFlows, setProxyFlows] = useState<EcoIntermediateFlow[]>([]);
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);

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
    onStatus?.(t(`已转换 ${links.size} 条 L1 中间流。`, `Converted ${links.size} L1 intermediate flows.`));
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
        `已确认并转换 ${links.size} 条 L2 中间流；请继续选择具体背景 LCI。`,
        `Confirmed and converted ${links.size} L2 intermediate flows; choose specific background LCI providers next.`,
      ));
    } catch (error) {
      onStatus?.(error instanceof Error ? error.message : t("批量确认 L2 失败", "Batch L2 confirmation failed"));
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
        ? t("已确认该 L2 兼容映射；请继续选择具体背景 LCI。", "Confirmed this L2 compatibility link; choose a background LCI next.")
        : t("已复用该 L3 用户代理；请继续选择具体背景 LCI。", "Reused this L3 user proxy; choose a background LCI next."));
    } catch (error) {
      onStatus?.(error instanceof Error ? error.message : t("确认映射失败", "Link confirmation failed"));
    } finally {
      setBusy(false);
    }
  };

  const loadProviders = async (port: FlowPort) => {
    const target = port.intermediateFlowLink?.targetFlowUuid;
    if (!target) return;
    setBusy(true);
    try {
      const providers = await fetchIntermediateFlowProviders(target);
      setProvidersByPort((current) => ({ ...current, [port.id]: providers }));
    } finally {
      setBusy(false);
    }
  };

  const chooseProvider = async (port: FlowPort, provider: ProviderCandidate) => {
    setBusy(true);
    try {
      const response = await fetch(`${API_BASE}/reference/processes/import`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          import_mode: "locked",
          target_kind: "lci_dataset",
          process_uuids: [provider.process_uuid],
        }),
      });
      if (!response.ok) throw new Error(`Provider import failed (${response.status})`);
      const rows = parseImportedRows(await response.json(), "locked", uiLanguage, "lci_dataset");
      if (!rows[0] || !connectProvider(node.id, port.id, rows[0])) {
        throw new Error(t("provider 参考产品与链接目标不一致", "Provider reference product does not match the link target"));
      }
      onStatus?.(t(
        `已选择背景 LCI：${provider.process_name}；此选择不代表其他 provider 与其等价。`,
        `Selected background LCI: ${provider.process_name}; no equivalence with other providers is implied.`,
      ));
    } catch (error) {
      onStatus?.(error instanceof Error ? error.message : t("连接 provider 失败", "Provider connection failed"));
    } finally {
      setBusy(false);
    }
  };

  const chooseProxyFlow = async (target: EcoIntermediateFlow) => {
    const port = inputs.find((item) => item.id === proxyPortId);
    if (!port || proxyReason.trim().length < 3) return;
    setBusy(true);
    try {
      const link = await createUserProxyRule(port.flowUuid, target.flow_uuid, proxyReason.trim());
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
      setProxyFlows([]);
      setProxyQuery("");
      setProxyReason("");
      onStatus?.(t("已保存 L3 用户代理；该关系不表示环境或 provider 等价。", "Saved L3 user proxy; it does not imply environmental or provider equivalence."));
    } finally {
      setBusy(false);
    }
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
            aria-label={t("中间流转换与背景连接", "Intermediate Flow Conversion and Background Linking")}
            onMouseDown={(event) => event.stopPropagation()}
          >
            <header className="overlay-head intermediate-flow-link-dialog-head">
              <div className="intermediate-flow-link-title">
                <strong>{t("中间流转换与背景连接", "Intermediate Flow Conversion and Background Linking")}</strong>
                <span>{inputs.length}</span>
              </div>
              <button type="button" className="drawer-close-btn" onClick={() => setOpen(false)}>
                {t("关闭", "Close")}
              </button>
            </header>
            <div className="intermediate-flow-link-dialog-toolbar">
              <div className="intermediate-flow-link-overview">
                <p>{t("已自动检测全部中间流。L1 可直接批量转换；L2 需核对目标后批量确认；无 L1/L2 候选时才使用 L3。", "All intermediate flows are checked automatically. Convert L1 in bulk, review L2 targets in bulk, and use L3 only when no L1/L2 candidate exists.")}</p>
                <div className="intermediate-flow-link-counts" aria-live="polite">
                  {resolutionState === "loading" ? (
                    <span>{t("正在检测可转换关系…", "Checking conversion candidates…")}</span>
                  ) : resolutionState === "error" ? (
                    <span className="error-text">{t("检测失败，请重试", "Candidate check failed; retry")}</span>
                  ) : (
                    <>
                      <span className="approved">L1 {l1Count}</span>
                      <span className="review">L2 {l2ReviewItems.length}</span>
                      <span>{t("无候选", "No candidate")} {unmatchedCount}</span>
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
                  {t(`转换全部 L1（${l1Count}）`, `Convert all L1 (${l1Count})`)}
                </button>
                <button type="button" className="flow-link-button secondary" disabled={busy || l2ReviewItems.length === 0} onClick={() => setL2ReviewOpen(true)}>
                  {t(`批量确认 L2（${l2ReviewItems.length}）`, `Review L2 (${l2ReviewItems.length})`)}
                </button>
              </div>
            </div>
            <div className="intermediate-flow-link-dialog-body">
              <div className="intermediate-flow-link-list">
      {inputs.map((port) => {
        const link = port.intermediateFlowLink;
        const review = candidateByPort[port.id];
        const resolutionStatus = statusByPort[port.id];
        const providers = providersByPort[port.id] ?? [];
        return (
          <div className="intermediate-flow-link-row" key={port.id}>
            <div className="intermediate-flow-link-summary">
              <strong title={getLocalizedText(port.name, uiLanguage, port.name)}>
                {getLocalizedText(port.name, uiLanguage, port.name)}
              </strong>
              <span className={`intermediate-flow-level-badge ${link ? "approved" : review ? "review" : "unmatched"}`}>
                {link && link.status !== "inactive"
                  ? link.mappingLevel
                  : review?.mapping_level
                    ?? (resolutionState === "loading" || resolutionState === "idle"
                      ? t("检测中", "Checking")
                      : t("无候选", "No candidate"))}
              </span>
            </div>
            {link && link.status !== "inactive" ? (
              <>
                <div className="intermediate-flow-link-target">
                  <span>{t("已关联 eco reference product", "Linked to eco reference product")}</span>
                  {link.applicationMode === "auto_compatible" && (
                    <span className="muted-text" title={(link.warnings ?? []).join(", ")}>
                      {t("语义泛化，计算前请核对", "Review semantic generalization before calculation")}
                    </span>
                  )}
                </div>
                <div className="intermediate-flow-link-actions">
                  <button type="button" className="flow-link-button secondary compact" disabled={busy} onClick={() => loadProviders(port)}>
                    {t("选择背景 LCI", "Choose background LCI")}
                  </button>
                </div>
                {providers.map((provider) => (
                  <button
                    type="button"
                    className="provider-candidate-btn"
                    key={provider.process_uuid}
                    disabled={busy || !provider.has_lci_vector}
                    onClick={() => chooseProvider(port, provider)}
                  >
                    {provider.process_name} · {provider.location || "-"}
                  </button>
                ))}
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
                    ? t("严格匹配，可通过上方按钮批量转换。", "Strict match; use the action above to convert it in bulk.")
                    : review.mapping_level === "L2"
                      ? t("兼容但语义可能更宽或更窄，确认后才写入模型。", "Compatible but potentially broader or narrower; written only after confirmation.")
                      : t("这是已保存的用户代理，需逐条确认复用。", "This saved user proxy must be reused per flow explicitly.")}
                </span>
                {(review.warnings ?? []).length > 0 && (
                  <span className="intermediate-flow-review-hint" title={(review.warnings ?? []).join(" · ")}>
                    {t("需核对产品范围和限定词", "Review product scope and qualifiers")}
                  </span>
                )}
                <div className="intermediate-flow-link-actions">
                  {review.mapping_level === "L1" ? (
                    <span className="muted-text">{t("等待批量转换 L1", "Ready for bulk L1 conversion")}</span>
                  ) : review.mapping_level === "L2" ? (
                    <span className="muted-text">{t("等待批量确认 L2", "Ready for batch L2 review")}</span>
                  ) : (
                    <button type="button" className="flow-link-button primary compact" disabled={busy} onClick={() => applyReviewedLink(port, review)}>
                      {t("复用 L3", "Reuse L3")}
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
                  {t("未找到可用的 L1/L2 转换", "No usable L1/L2 conversion found")}
                </span>
                <button type="button" className="flow-link-button ghost compact" onClick={() => setProxyPortId(port.id)}>
                  {t("指定 L3 用户代理", "Assign L3 user proxy")}
                </button>
              </div>
            )}
          </div>
        );
      })}
              </div>
      {proxyPortId && (
        <div className="intermediate-flow-proxy-editor">
          <input
            value={proxyQuery}
            placeholder={t("搜索 eco 中间流", "Search eco intermediate flows")}
            onChange={(event) => setProxyQuery(event.target.value)}
          />
          <textarea
            value={proxyReason}
            placeholder={t("必填：代理原因", "Required: proxy reason")}
            onChange={(event) => setProxyReason(event.target.value)}
          />
          <button
            type="button"
            className="flow-link-button secondary"
            disabled={busy || !proxyQuery.trim()}
            onClick={async () => setProxyFlows(await searchEcoIntermediateFlows(proxyQuery.trim()))}
          >
            {t("搜索", "Search")}
          </button>
          {proxyFlows.map((flow) => (
            <button
              type="button"
              className="provider-candidate-btn flow-link-button ghost"
              key={flow.flow_uuid}
              disabled={busy || proxyReason.trim().length < 3}
              onClick={() => chooseProxyFlow(flow)}
            >
              {flow.flow_name} · {flow.default_unit}
            </button>
          ))}
        </div>
      )}
            </div>
          </section>
        </div>
      )}
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
