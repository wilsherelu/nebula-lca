import { useEffect, useState } from "react";
import type { FlowPort, IntermediateFlowLink } from "../../model/node";
import {
  createUserProxyRule,
  searchEcoIntermediateFlows,
  type EcoIntermediateFlow,
} from "../../services/intermediateFlowLinks";
import { getLocalizedText } from "../../utils/localizedText";

type Props = {
  open: boolean;
  busy: boolean;
  port: FlowPort | null;
  language: "zh" | "en";
  onClose: () => void;
  onConfirm: (port: FlowPort, link: IntermediateFlowLink) => void;
  onStatus?: (text: string) => void;
};

export function L3UserProxyModal({
  open,
  busy,
  port,
  language,
  onClose,
  onConfirm,
  onStatus,
}: Props) {
  const [query, setQuery] = useState("");
  const [reason, setReason] = useState("");
  const [flows, setFlows] = useState<EcoIntermediateFlow[]>([]);
  const [selectedFlowUuid, setSelectedFlowUuid] = useState("");
  const [searchState, setSearchState] = useState<"idle" | "loading" | "ready" | "error">("idle");
  const [searchError, setSearchError] = useState("");
  const [confirming, setConfirming] = useState(false);
  const t = (zh: string, en: string) => (language === "zh" ? zh : en);

  useEffect(() => {
    if (!open) return;
    setQuery("");
    setReason("");
    setFlows([]);
    setSelectedFlowUuid("");
    setSearchState("idle");
    setSearchError("");
    setConfirming(false);
  }, [open, port?.id]);

  const canSearch = query.trim().length > 0 && !busy && !confirming;
  const selectedFlow = flows.find((flow) => flow.flow_uuid === selectedFlowUuid) ?? null;
  const canConfirm = Boolean(selectedFlow) && reason.trim().length >= 3 && !busy && !confirming;

  const runSearch = async () => {
    if (!canSearch) return;
    setSearchState("loading");
    setSearchError("");
    try {
      const results = await searchEcoIntermediateFlows(query.trim());
      setFlows(results);
      setSelectedFlowUuid("");
      setSearchState("ready");
    } catch (error) {
      setSearchState("error");
      setSearchError(error instanceof Error ? error.message : t("搜索失败", "Search failed"));
      setFlows([]);
    }
  };

  const handleConfirm = async () => {
    if (!port || !selectedFlow || !canConfirm) return;
    setConfirming(true);
    try {
      const link = await createUserProxyRule(port.flowUuid, selectedFlow.flow_uuid, reason.trim());
      onConfirm(port, link);
    } catch (error) {
      const message = error instanceof Error ? error.message : t("保存手动转换失败", "Failed to save manual conversion");
      onStatus?.(message);
      setSearchError(message);
    } finally {
      setConfirming(false);
    }
  };

  const portLabel = port
    ? getLocalizedText(port.name, language, port.name)
    : "";

  if (!open || !port) return null;

  return (
    <div className="overlay-modal l3-proxy-modal-overlay" onMouseDown={() => !busy && !confirming && onClose()}>
      <section
        className="overlay-panel l3-proxy-modal-dialog"
        role="dialog"
        aria-modal="true"
        aria-label={t("手动转换", "Manual conversion")}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="overlay-head l3-proxy-modal-head">
          <div className="intermediate-flow-link-title">
            <strong>{t("手动转换", "Manual conversion")}</strong>
          </div>
          <button type="button" className="drawer-close-btn" disabled={busy || confirming} onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </header>

        <div className="l3-proxy-modal-context">
          <span className="l3-proxy-context-label">{t("源流", "Source flow")}</span>
          <strong title={portLabel}>{portLabel}</strong>
          <span className="muted-text">
            {t(
              "当没有可用的自动转换时，可手动选择一个 eco 中间流作为转换目标。",
              "When no automatic conversion is available, manually choose an eco intermediate flow as the conversion target.",
            )}
          </span>
        </div>

        <div className="l3-proxy-modal-search">
          <label>
            <span>{t("搜索 eco 中间流", "Search eco intermediate flows")}</span>
            <input
              value={query}
              disabled={busy || confirming}
              placeholder={t("输入流名称搜索", "Type flow name to search")}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  void runSearch();
                }
              }}
            />
          </label>
          <button
            type="button"
            className="flow-link-button secondary"
            disabled={!canSearch}
            onClick={() => void runSearch()}
          >
            {searchState === "loading" ? t("搜索中…", "Searching…") : t("搜索", "Search")}
          </button>
        </div>

        {searchState === "error" && (
          <div className="error-text">{searchError}</div>
        )}

        <div className="l3-proxy-modal-results">
          {searchState === "loading" && (
            <div className="table-empty">{t("正在搜索…", "Searching…")}</div>
          )}
          {searchState === "ready" && flows.length === 0 && (
            <div className="table-empty">{t("未找到匹配的中间流", "No matching intermediate flows found")}</div>
          )}
          {searchState === "ready" && flows.length > 0 && (
            <table className="l3-proxy-results-table">
              <thead>
                <tr>
                  <th>{t("流名称", "Flow name")}</th>
                  <th>{t("单位", "Unit")}</th>
                  <th>{t("单位组", "Unit group")}</th>
                  <th>{t("操作", "Action")}</th>
                </tr>
              </thead>
              <tbody>
                {flows.map((flow) => {
                  const selected = flow.flow_uuid === selectedFlowUuid;
                  const showEnglishName = Boolean(
                    flow.flow_name_en && flow.flow_name_en.trim().toLocaleLowerCase() !== flow.flow_name.trim().toLocaleLowerCase(),
                  );
                  return (
                    <tr key={flow.flow_uuid} className={selected ? "selected" : ""}>
                      <td title={flow.flow_name_en ? `${flow.flow_name} (${flow.flow_name_en})` : flow.flow_name}>
                        {flow.flow_name}
                        {showEnglishName && <span className="muted-text"> · {flow.flow_name_en}</span>}
                      </td>
                      <td>{flow.default_unit}</td>
                      <td>{flow.unit_group || "-"}</td>
                      <td>
                        <button
                          type="button"
                          className={`flow-link-button ${selected ? "primary" : "secondary"} compact`}
                          disabled={busy || confirming}
                          onClick={() => setSelectedFlowUuid(flow.flow_uuid)}
                        >
                          {selected ? t("已选择", "Selected") : t("选择", "Select")}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        <div className="l3-proxy-modal-reason">
          <label>
            <span>{t("选择依据（将记录到转换规则，至少 3 个字符）", "Selection rationale (saved with the conversion rule, at least 3 characters)")}</span>
            <textarea
              value={reason}
              disabled={busy || confirming}
              placeholder={t("请说明为何选择此流作为代理", "Explain why this flow is used as a proxy")}
              onChange={(event) => setReason(event.target.value)}
              rows={3}
            />
          </label>
        </div>

        <footer className="l3-proxy-modal-footer">
          <button
            type="button"
            className="flow-link-button secondary"
            disabled={busy || confirming}
            onClick={() => {
              setQuery("");
              setReason("");
              setFlows([]);
              setSelectedFlowUuid("");
              setSearchState("idle");
              setSearchError("");
            }}
          >
            {t("重置", "Reset")}
          </button>
          <button
            type="button"
            className="flow-link-button ghost"
            disabled={busy || confirming}
            onClick={onClose}
          >
            {t("取消", "Cancel")}
          </button>
          <button
            type="button"
            className="flow-link-button primary"
            disabled={!canConfirm}
            onClick={() => void handleConfirm()}
          >
            {confirming ? t("转换中…", "Converting…") : t("确认转换", "Confirm conversion")}
          </button>
        </footer>
      </section>
    </div>
  );
}
