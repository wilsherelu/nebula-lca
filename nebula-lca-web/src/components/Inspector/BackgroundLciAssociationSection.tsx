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

type Props = {
  consumerNodeId: string;
  port: FlowPort;
  language: "zh" | "en";
  onLinked: () => void;
  onStatus?: (text: string) => void;
};

const API_BASE = getApiBase();

export function BackgroundLciAssociationSection({
  consumerNodeId,
  port,
  language,
  onLinked,
  onStatus,
}: Props) {
  const connectProvider = useLcaGraphStore((state) => state.connectIntermediateProvider);
  const [providers, setProviders] = useState<ProviderCandidate[]>([]);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const t = (zh: string, en: string) => (language === "zh" ? zh : en);
  const link = port.intermediateFlowLink;
  const targetFlowUuid = link && link.status !== "inactive" ? link.targetFlowUuid : "";

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
      if (!rows[0] || !connectProvider(consumerNodeId, port.id, rows[0])) {
        throw new Error(t("背景 LCI 的参考产品与转换目标不一致", "Background LCI reference product does not match the converted target"));
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

  return (
    <section className="background-lci-association-section">
      <div className="background-lci-association-title">
        <strong>{t("背景 LCI", "Background LCI")}</strong>
        {targetFlowUuid && <span>{t("按转换后的 eco 中间流匹配", "Matched by converted eco flow")}</span>}
      </div>
      {!targetFlowUuid ? (
        <div className="mode-lock-hint">
          {t("该输入尚未完成中间流转换。请先执行 L1/L2/L3 转换，再选择背景 LCI。", "Convert this input through L1/L2/L3 before choosing a background LCI.")}
        </div>
      ) : loading && providers.length === 0 ? (
        <div className="table-empty">{t("正在加载可关联的背景 LCI…", "Loading linkable background LCI providers…")}</div>
      ) : error ? (
        <div className="error-text">{error}</div>
      ) : providers.length === 0 ? (
        <div className="table-empty">{t(
          "本地数据库尚未导入该 eco reference product 对应的 LCI 数据集",
          "The local database has no imported LCI dataset for this eco reference product",
        )}</div>
      ) : selectableProviders.length === 0 ? (
        <div className="table-empty">{t(
          `已找到 ${providers.length} 个 provider，但对应 LCI 向量尚未完成导入`,
          `${providers.length} providers were found, but their LCI vectors are not imported`,
        )}</div>
      ) : (
        <div className="background-lci-association-controls">
          <div>
            <strong>{t(`${selectableProviders.length} 个可计算 provider`, `${selectableProviders.length} calculable providers`)}</strong>
            <span>{t("可按过程类型和地区筛选", "Filter by process type and location")}</span>
          </div>
          <button type="button" className="flow-link-button primary" disabled={loading} onClick={() => setPickerOpen(true)}>
            {t("选择背景数据库", "Choose Background Database")}
          </button>
        </div>
      )}
      <BackgroundLciPickerDialog
        open={pickerOpen}
        busy={loading}
        providers={providers}
        language={language}
        onClose={() => setPickerOpen(false)}
        onSelect={linkProvider}
      />
    </section>
  );
}
