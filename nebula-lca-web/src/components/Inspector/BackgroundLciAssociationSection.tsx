import { useEffect, useMemo, useState } from "react";
import { getApiBase } from "../../apiBase";
import type { FlowPort } from "../../model/node";
import {
  fetchIntermediateFlowProviders,
  type ProviderCandidate,
} from "../../services/intermediateFlowLinks";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { parseImportedRows } from "../NodePalette/UnitProcessImportDialog";
import { BackgroundLciPickerDialog } from "./BackgroundLciPickerDialog";
import { getLocalizedText } from "../../utils/localizedText";

type Props = {
  consumerNodeId: string;
  port: FlowPort;
  language: "zh" | "en";
  onLinked: () => void;
  onStatus?: (text: string) => void;
  sourceFlowName?: string;
};

const API_BASE = getApiBase();

export function BackgroundLciAssociationSection({
  consumerNodeId,
  port,
  language,
  onLinked,
  onStatus,
  sourceFlowName,
}: Props) {
  const connectProvider = useLcaGraphStore((state) => state.connectIntermediateProvider);
  const [providers, setProviders] = useState<ProviderCandidate[]>([]);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const t = (zh: string, en: string) => (language === "zh" ? zh : en);
  const link = port.intermediateFlowLink;
  const convertedTargetFlowUuid = link && link.status !== "inactive" ? link.targetFlowUuid : "";
  // An unchanged UUID is already a valid same-source target.  It does not
  // need a synthetic conversion record before an exact provider can be used.
  const targetFlowUuid = convertedTargetFlowUuid || port.flowUuid || "";

  useEffect(() => {
    setProviders([]);
    setPickerOpen(false);
    setError("");
    if (!targetFlowUuid) return;
    let canceled = false;
    setLoading(true);
    fetchIntermediateFlowProviders(targetFlowUuid)
      .then((rows) => {
        if (!canceled) setProviders(rows);
      })
      .catch((reason) => {
        if (!canceled) setError(reason instanceof Error ? reason.message : t("背景 LCI 加载失败", "Background LCI lookup failed"));
      })
      .finally(() => {
        if (!canceled) setLoading(false);
      });
    return () => {
      canceled = true;
    };
  }, [targetFlowUuid]);

  const selectableProviders = useMemo(
    () => providers.filter((provider) => provider.has_lci_vector),
    [providers],
  );

  const linkProvider = async (provider: ProviderCandidate) => {
    setLoading(true);
    setError("");
    try {
      if (
        provider.reference_product_flow_uuid
        && targetFlowUuid
        && provider.reference_product_flow_uuid !== targetFlowUuid
      ) {
        const sourceLabel = provider.source || t("所选数据源", "the selected source");
        const message = convertedTargetFlowUuid
          ? t(
            `${sourceLabel} 的参考产品与转换目标不匹配，无法关联。`,
            `${sourceLabel} reference product does not match the conversion target; cannot link.`,
          )
          : t(
            `当前输入与 ${sourceLabel} 的参考产品或单位不一致，需先完成转换。`,
            `The current input differs from ${sourceLabel} reference product or unit; conversion is required before linking.`,
          );
        setError(message);
        onStatus?.(message);
        setLoading(false);
        return;
      }
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
      const rows = parseImportedRows(await response.json(), "locked", language, "lci_dataset");
      if (rows[0] && provider.source) {
        rows[0].sourceSystem = provider.source;
      }
      if (!rows[0] || !connectProvider(consumerNodeId, port.id, rows[0])) {
        const sourceLabel = provider.source || t("所选数据源", "the selected source");
        const message = convertedTargetFlowUuid
          ? t(
            `${sourceLabel} 的参考产品与转换目标不匹配，无法关联。`,
            `${sourceLabel} reference product does not match the conversion target; cannot link.`,
          )
          : t(
            `当前输入与 ${sourceLabel} 的参考产品或单位不一致，需先完成转换。`,
            `The current input differs from ${sourceLabel} reference product or unit; conversion is required before linking.`,
          );
        throw new Error(message);
      }
      onStatus?.(t(
        `已关联背景 LCI：${provider.process_name}。该 provider 仅参与计算，不显示为画布节点。`,
        `Linked background LCI: ${provider.process_name}. This provider participates in calculation without a visible canvas node.`,
      ));
      setPickerOpen(false);
      onLinked();
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : t("背景 LCI 关联失败", "Background LCI linking failed");
      setError(message);
      onStatus?.(message);
    } finally {
      setLoading(false);
    }
  };

  const statusClass = !targetFlowUuid
    ? "disabled"
    : loading && providers.length === 0
      ? "loading"
      : error
        ? "error"
        : providers.length === 0
          ? "empty"
          : selectableProviders.length === 0
            ? "partial"
            : "ready";

  const statusLabel = !targetFlowUuid
    ? t("未就绪", "Not ready")
    : loading && providers.length === 0
      ? t("加载中", "Loading")
      : error
        ? t("加载失败", "Failed")
        : providers.length === 0
          ? t("无数据", "None")
          : selectableProviders.length === 0
            ? t("向量未导入", "No vector")
            : t(`${selectableProviders.length} 可用`, `${selectableProviders.length} available`);

  return (
    <section className="background-lci-association-section">
      <div className="background-lci-association-header">
        <div className="background-lci-association-title">
          <strong>{t("关联背景 LCI", "Link Background LCI")}</strong>
          <span className={`background-lci-status-badge background-lci-status-badge--${statusClass}`}>{statusLabel}</span>
        </div>
        {targetFlowUuid && <span className="background-lci-match-hint">{t(
          convertedTargetFlowUuid ? "按转换后的产品流精确匹配" : "按当前产品流精确匹配；跨库数据请先手动转换到目标产品流",
          convertedTargetFlowUuid ? "Matched exactly by converted product flow" : "Matched exactly by current product flow; convert cross-source data to its target product flow first",
        )}</span>}
      </div>
      {!targetFlowUuid ? (
        <div className="background-lci-hint">
          {t("请先完成中间流转换，再选择背景 LCI。", "Convert this input before choosing a background LCI.")}
        </div>
      ) : loading && providers.length === 0 ? (
        <div className="background-lci-hint">{t("正在查找可关联的背景 LCI…", "Loading background LCI providers…")}</div>
      ) : error ? (
        <div className="background-lci-hint error-text">{error}</div>
      ) : providers.length === 0 ? (
        <div className="background-lci-hint">{t(
          convertedTargetFlowUuid
            ? "本地数据库尚未导入该产品流对应的 LCI 数据集。"
            : "当前产品流尚无可关联背景 LCI；可先转换到目标产品流后再选择。",
          convertedTargetFlowUuid
            ? "No imported LCI dataset for this product flow in the local database."
            : "No background LCI available. Convert to a target product flow first.",
        )}</div>
      ) : selectableProviders.length === 0 ? (
        <div className="background-lci-hint">{t(
          `已找到 ${providers.length} 个 provider，但 LCI 向量尚未导入。`,
          `${providers.length} providers found, but LCI vectors are not imported.`,
        )}</div>
      ) : (
        <div className="background-lci-association-controls">
          <div className="background-lci-association-status">
            <strong className="background-lci-association-label">{t("背景 LCI 关联", "Background LCI association")}</strong>
            <span className="background-lci-association-count">{t(`${selectableProviders.length} 个可计算 provider`, `${selectableProviders.length} calculable providers`)}</span>
            <span className="background-lci-association-hint">{t("可按过程类型和地区筛选", "Filter by process type and location")}</span>
          </div>
          <div className="background-lci-association-action">
            <button type="button" className="flow-link-button primary" disabled={loading} onClick={() => setPickerOpen(true)}>
              {loading ? t("加载中…", "Loading…") : t("选择背景数据库", "Choose Background Database")}
            </button>
          </div>
        </div>
      )}
      <BackgroundLciPickerDialog
        open={pickerOpen}
        busy={loading}
        providers={providers}
        sourceFlowName={sourceFlowName || (language === "en"
          ? (port.flowNameEn || port.displayNameEn || getLocalizedText(port.name, "en", port.name))
          : getLocalizedText(port.name, language, port.name))}
        targetFlowUuid={targetFlowUuid}
        language={language}
        onClose={() => setPickerOpen(false)}
        onSelect={linkProvider}
      />
    </section>
  );
}
