import { useMemo, useState } from "react";
import type { Node } from "@xyflow/react";
import { getApiBase } from "../../apiBase";
import type { FlowPort, LcaNodeData } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { parseImportedRows } from "../NodePalette/UnitProcessImportDialog";
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
} from "../../services/intermediateFlowLinks";

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
  const [busy, setBusy] = useState(false);
  const [providersByPort, setProvidersByPort] = useState<Record<string, ProviderCandidate[]>>({});
  const [reviewByPort, setReviewByPort] = useState<Record<string, RawResolution>>({});
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

  const runMatch = async () => {
    setBusy(true);
    try {
      const payload = await resolveIntermediateFlowPorts(
        inputs.filter((port) => !connectedPortIds.has(port.id)),
      );
      const links = new Map<string, ReturnType<typeof toIntermediateFlowLink>>();
      const nextReview: Record<string, RawResolution> = {};
      for (const item of payload.items) {
        if (item.port_id && item.status === "L1" && item.resolution && "source_flow_uuid" in item.resolution) {
          links.set(item.port_id, toIntermediateFlowLink(item.resolution));
        }
        if (
          item.port_id
          && (item.status === "L2" || item.status === "L3")
          && item.resolution
          && "source_flow_uuid" in item.resolution
        ) {
          nextReview[item.port_id] = item.resolution;
        }
      }
      if (links.size > 0) {
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
      }
      setReviewByPort(nextReview);
      onStatus?.(t(
        `已自动应用 ${links.size} 条 L1；${Object.keys(nextReview).length} 条 L2/L3 等待逐项确认。`,
        `Applied ${links.size} L1 links; ${Object.keys(nextReview).length} L2/L3 links await per-flow confirmation.`,
      ));
    } catch (error) {
      onStatus?.(error instanceof Error ? error.message : t("中间流匹配失败", "Intermediate flow matching failed"));
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
      setReviewByPort((current) => {
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
    <section className="intermediate-flow-link-panel">
      <div className="inspector-section-title">{t("ecoinvent 背景连接", "ecoinvent Background Linking")}</div>
      <p className="muted-text">
        {t(
          "L1 可一键转换；L2/L3 必须逐条确认。这里只建立天工流到 eco reference product 的单向兼容关系，provider 仍需单独选择。",
          "L1 can be applied in one click; L2/L3 require per-flow confirmation. This only links to an eco reference product; providers are selected separately.",
        )}
      </p>
      <button type="button" className="secondary-btn" disabled={busy} onClick={runMatch}>
        {busy ? t("处理中…", "Working…") : t("一键转换 L1", "Apply all L1 links")}
      </button>
      {inputs.map((port) => {
        const link = port.intermediateFlowLink;
        const review = reviewByPort[port.id];
        const providers = providersByPort[port.id] ?? [];
        return (
          <div className="intermediate-flow-link-row" key={port.id}>
            <strong>{port.name}</strong>
            {link && link.status !== "inactive" ? (
              <>
                <span>{link.mappingLevel} · {t("单向兼容", "one-way compatible")}</span>
                {link.applicationMode === "auto_compatible" && (
                  <span className="muted-text" title={(link.warnings ?? []).join(", ")}>
                    {t("语义泛化，计算前请核对", "Semantic generalization; review before calculation")}
                  </span>
                )}
                <button type="button" className="link-btn" disabled={busy} onClick={() => loadProviders(port)}>
                  {t("选择背景 LCI", "Choose background LCI")}
                </button>
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
                <span className="intermediate-flow-level-badge">{review.mapping_level}</span>
                <span>
                  {t("目标：", "Target: ")}
                  {review.target_flow_name || review.target_flow_name_en || review.target_flow_uuid}
                  {` · ${review.target_unit}`}
                </span>
                <span className="muted-text">
                  {review.mapping_level === "L2"
                    ? t("兼容但语义可能更宽或更窄，确认后才写入模型。", "Compatible but potentially broader or narrower; written only after confirmation.")
                    : t("这是已保存的用户代理，需逐条确认复用。", "This saved user proxy must be reused per flow explicitly.")}
                </span>
                {(review.warnings ?? []).length > 0 && (
                  <span className="muted-text">{(review.warnings ?? []).join(" · ")}</span>
                )}
                <button
                  type="button"
                  className="secondary-btn"
                  disabled={busy}
                  onClick={() => applyReviewedLink(port, review)}
                >
                  {review.mapping_level === "L2"
                    ? t("确认此 L2 映射", "Confirm this L2 link")
                    : t("复用此 L3 代理", "Reuse this L3 proxy")}
                </button>
                <button type="button" className="link-btn" disabled={busy} onClick={() => setProxyPortId(port.id)}>
                  {t("改用其他 L3 代理", "Choose another L3 proxy")}
                </button>
              </div>
            ) : null}
            {!link && !review && (
              <button type="button" className="link-btn" onClick={() => setProxyPortId(port.id)}>
                {t("指定 L3 用户代理", "Assign L3 user proxy")}
              </button>
            )}
          </div>
        );
      })}
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
            disabled={busy || !proxyQuery.trim()}
            onClick={async () => setProxyFlows(await searchEcoIntermediateFlows(proxyQuery.trim()))}
          >
            {t("搜索", "Search")}
          </button>
          {proxyFlows.map((flow) => (
            <button
              type="button"
              className="provider-candidate-btn"
              key={flow.flow_uuid}
              disabled={busy || proxyReason.trim().length < 3}
              onClick={() => chooseProxyFlow(flow)}
            >
              {flow.flow_name} · {flow.default_unit}
            </button>
          ))}
        </div>
      )}
    </section>
  );
}
