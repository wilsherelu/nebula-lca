import { useMemo, useState } from "react";
import type { Node } from "@xyflow/react";
import { getApiBase } from "../../apiBase";
import type { FlowPort, LcaNodeData } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { parseImportedRows } from "../NodePalette/UnitProcessImportDialog";
import {
  fetchIntermediateFlowProviders,
  createUserProxyRule,
  resolveIntermediateFlowPorts,
  searchEcoIntermediateFlows,
  toIntermediateFlowLink,
  type EcoIntermediateFlow,
  type LinkCandidate,
  type ProviderCandidate,
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
  const [l2ByPort, setL2ByPort] = useState<Record<string, LinkCandidate[]>>({});
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
      const nextL2: Record<string, LinkCandidate[]> = {};
      for (const item of payload.items) {
        if (item.port_id && item.status === "L1" && item.resolution && "source_flow_uuid" in item.resolution) {
          links.set(item.port_id, toIntermediateFlowLink(item.resolution));
        }
        if (item.port_id && item.status === "L2") {
          nextL2[item.port_id] = item.l2_candidates ?? [];
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
      setL2ByPort(nextL2);
      onStatus?.(t(
        `已建立 ${links.size} 条单向 eco reference product 兼容链接；provider 仍需逐项选择。`,
        `Created ${links.size} one-way eco reference-product links; providers still require selection.`,
      ));
    } catch (error) {
      onStatus?.(error instanceof Error ? error.message : t("中间流匹配失败", "Intermediate flow matching failed"));
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
          "这里只建立天工流到 eco reference product 的单向兼容关系。不同 provider 的地区、技术和 LCIA 结果可能不同。",
          "This creates one-way compatibility to an eco reference product. Providers may differ by region, technology, and LCIA results.",
        )}
      </p>
      <button type="button" className="secondary-btn" disabled={busy} onClick={runMatch}>
        {busy ? t("处理中…", "Working…") : t("一键匹配 eco 中间流", "Match eco intermediate flows")}
      </button>
      {inputs.map((port) => {
        const link = port.intermediateFlowLink;
        const providers = providersByPort[port.id] ?? [];
        return (
          <div className="intermediate-flow-link-row" key={port.id}>
            <strong>{port.name}</strong>
            {link && link.status !== "inactive" ? (
              <>
                <span>{link.mappingLevel} · {t("单向兼容", "one-way compatible")}</span>
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
            ) : l2ByPort[port.id]?.length ? (
              <span>{t("仅有 L2 候选，未修改模型", "L2 candidates only; model unchanged")}</span>
            ) : null}
            {!link && (
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
