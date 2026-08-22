import { useEffect, useMemo, useState } from "react";
import type { FlowPort } from "../../model/node";
import type { RawResolution } from "../../services/intermediateFlowLinks";
import { getLocalizedText } from "../../utils/localizedText";

export type IntermediateFlowL2ReviewItem = {
  port: FlowPort;
  resolution: RawResolution;
};

export type IntermediateFlowReviewSelection = {
  portId: string;
  amountFactor?: number;
};

type Props = {
  open: boolean;
  busy: boolean;
  items: IntermediateFlowL2ReviewItem[];
  language: "zh" | "en";
  onClose: () => void;
  onConfirm: (selections: IntermediateFlowReviewSelection[]) => void;
  getSourceDisplayName?: (port: FlowPort) => string;
};

export function IntermediateFlowL2ReviewDialog({
  open,
  busy,
  items,
  language,
  onClose,
  onConfirm,
  getSourceDisplayName,
}: Props) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [factorByPort, setFactorByPort] = useState<Record<string, string>>({});
  const t = (zh: string, en: string) => (language === "zh" ? zh : en);

  useEffect(() => {
    if (open) {
      setSelected(new Set(items.map((item) => item.port.id)));
      setFactorByPort({});
    }
  }, [items, open]);

  const allSelected = items.length > 0 && selected.size === items.length;
  const selectedIds = useMemo(() => Array.from(selected), [selected]);
  const invalidFactorIds = useMemo(() => selectedIds.filter((portId) => {
    const item = items.find((candidate) => candidate.port.id === portId);
    if (!item?.resolution.requires_manual_factor) return false;
    const factor = Number(factorByPort[portId]);
    return !Number.isFinite(factor) || factor <= 0;
  }), [factorByPort, items, selectedIds]);

  if (!open) return null;
  return (
    <div className="overlay-modal intermediate-flow-l2-review-overlay" onMouseDown={onClose}>
      <section
        className="overlay-panel intermediate-flow-l2-review-dialog"
        role="dialog"
        aria-modal="true"
        aria-label={t("确认转换", "Confirm conversions")}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="overlay-head intermediate-flow-link-dialog-head">
          <div className="intermediate-flow-link-title">
            <strong>{t("确认转换", "Confirm conversions")}</strong>
            <span>{items.length}</span>
          </div>
          <button type="button" className="drawer-close-btn" disabled={busy} onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </header>
        <div className="intermediate-flow-l2-review-note">
          {t(
            "请核对转换目标后确认。",
            "Review conversion targets before confirming.",
          )}
        </div>
        <div className="intermediate-flow-l2-review-select-all">
          <label className="intermediate-flow-l2-select-control">
            <span className="app-checkbox">
              <input
                type="checkbox"
                checked={allSelected}
                disabled={busy || items.length === 0}
                onChange={(event) => setSelected(event.target.checked
                  ? new Set(items.map((item) => item.port.id))
                  : new Set())}
              />
              <span className="app-checkbox-control" aria-hidden="true" />
            </span>
            <span>{t(`全选（${selected.size}/${items.length}）`, `Select all (${selected.size}/${items.length})`)}</span>
          </label>
        </div>
        <div className="intermediate-flow-l2-review-columns" aria-hidden="true">
          <span />
          <span>{t("来源流", "Source flow")}</span>
          <span>{t("转换目标", "Conversion target")}</span>
          <span>{t("单位", "Unit")}</span>
          <span>{t("确认信息", "Confirmation")}</span>
        </div>
        <div className="intermediate-flow-l2-review-list">
          {items.map(({ port, resolution }) => {
            const checked = selected.has(port.id);
            const sourceName = getSourceDisplayName
              ? getSourceDisplayName(port)
              : (language === "en"
                ? (port.flowNameEn || port.displayNameEn || getLocalizedText(port.name, "en", port.name))
                : getLocalizedText(port.name, language, port.name));
            const targetName = language === "zh"
              ? resolution.target_flow_name || resolution.target_flow_name_en || resolution.target_flow_uuid
              : resolution.target_flow_name_en || resolution.target_flow_name || resolution.target_flow_uuid;
            return (
              <div className="intermediate-flow-l2-review-row" key={port.id}>
                <label className="app-checkbox">
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={busy}
                    onChange={(event) => setSelected((current) => {
                      const next = new Set(current);
                      if (event.target.checked) next.add(port.id);
                      else next.delete(port.id);
                      return next;
                    })}
                  />
                  <span className="app-checkbox-control" aria-hidden="true" />
                </label>
                <span className="intermediate-flow-l2-review-flow" title={sourceName}>
                  <strong>{sourceName}</strong>
                </span>
                <span className="intermediate-flow-l2-review-target" title={targetName}>
                  <strong>{targetName}</strong>
                </span>
                <span className="intermediate-flow-l2-review-unit">{resolution.source_unit} → {resolution.target_unit}</span>
                {resolution.requires_manual_factor ? (
                  <span className="intermediate-flow-factor-editor">
                    <span>{t(
                      `1 ${resolution.source_unit} =`,
                      `1 ${resolution.source_unit} =`,
                    )}</span>
                    <input
                      type="number"
                      min="0"
                      step="any"
                      inputMode="decimal"
                      value={factorByPort[port.id] ?? ""}
                      disabled={busy}
                      aria-label={t(`${sourceName} 换算系数`, `${sourceName} conversion factor`)}
                      onChange={(event) => setFactorByPort((current) => ({
                        ...current,
                        [port.id]: event.target.value,
                      }))}
                    />
                    <span>{resolution.target_unit}</span>
                  </span>
                ) : (
                  <span className="intermediate-flow-l2-risk" title={(resolution.warnings ?? []).join(" · ")}>
                    {t("待核对", "Review needed")}
                  </span>
                )}
              </div>
            );
          })}
        </div>
        <footer className="intermediate-flow-l2-review-footer">
          <button type="button" className="flow-link-button secondary" disabled={busy} onClick={onClose}>
            {t("取消", "Cancel")}
          </button>
          <button
            type="button"
            className="flow-link-button primary"
            disabled={busy || selectedIds.length === 0 || invalidFactorIds.length > 0}
            onClick={() => onConfirm(selectedIds.map((portId) => {
              const item = items.find((candidate) => candidate.port.id === portId);
              return item?.resolution.requires_manual_factor
                ? { portId, amountFactor: Number(factorByPort[portId]) }
                : { portId };
            }))}
          >
            {busy
              ? t("确认中…", "Confirming…")
              : t(`确认并转换 ${selectedIds.length} 条`, `Confirm and convert ${selectedIds.length}`)}
          </button>
        </footer>
      </section>
    </div>
  );
}
