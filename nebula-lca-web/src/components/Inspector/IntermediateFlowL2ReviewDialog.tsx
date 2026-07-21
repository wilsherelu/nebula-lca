import { useEffect, useMemo, useState } from "react";
import type { FlowPort } from "../../model/node";
import type { RawResolution } from "../../services/intermediateFlowLinks";
import { getLocalizedText } from "../../utils/localizedText";

export type IntermediateFlowL2ReviewItem = {
  port: FlowPort;
  resolution: RawResolution;
};

type Props = {
  open: boolean;
  busy: boolean;
  items: IntermediateFlowL2ReviewItem[];
  language: "zh" | "en";
  onClose: () => void;
  onConfirm: (portIds: string[]) => void;
};

export function IntermediateFlowL2ReviewDialog({
  open,
  busy,
  items,
  language,
  onClose,
  onConfirm,
}: Props) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const t = (zh: string, en: string) => (language === "zh" ? zh : en);

  useEffect(() => {
    if (open) {
      setSelected(new Set(items.map((item) => item.port.id)));
    }
  }, [items, open]);

  const allSelected = items.length > 0 && selected.size === items.length;
  const selectedIds = useMemo(() => Array.from(selected), [selected]);

  if (!open) return null;
  return (
    <div className="overlay-modal intermediate-flow-l2-review-overlay" onMouseDown={onClose}>
      <section
        className="overlay-panel intermediate-flow-l2-review-dialog"
        role="dialog"
        aria-modal="true"
        aria-label={t("确认待核对的转换", "Confirm conversions needing review")}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="overlay-head intermediate-flow-link-dialog-head">
          <div className="intermediate-flow-link-title">
            <strong>{t("确认待核对的转换", "Confirm conversions needing review")}</strong>
            <span>{items.length}</span>
          </div>
          <button type="button" className="drawer-close-btn" disabled={busy} onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </header>
        <div className="intermediate-flow-l2-review-note">
          {t(
            "这些转换的单位兼容，但产品语义可能更宽或更窄。请核对目标后再批量确认。",
            "These conversions are unit-compatible, but product meaning may be broader or narrower. Review targets before confirming.",
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
        <div className="intermediate-flow-l2-review-list">
          {items.map(({ port, resolution }) => {
            const checked = selected.has(port.id);
            const sourceName = getLocalizedText(port.name, language, port.name);
            const targetName = language === "zh"
              ? resolution.target_flow_name || resolution.target_flow_name_en || resolution.target_flow_uuid
              : resolution.target_flow_name_en || resolution.target_flow_name || resolution.target_flow_uuid;
            return (
              <label className="intermediate-flow-l2-review-row" key={port.id}>
                <span className="app-checkbox">
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
                </span>
                <span className="intermediate-flow-l2-review-flow" title={sourceName}>
                  <strong>{sourceName}</strong>
                  <small>{t("天工中间流", "Tiangong intermediate flow")}</small>
                </span>
                <span className="intermediate-flow-l2-review-arrow" aria-hidden="true">→</span>
                <span className="intermediate-flow-l2-review-target" title={targetName}>
                  <strong>{targetName}</strong>
                  <small>{resolution.source_unit} → {resolution.target_unit}</small>
                </span>
                <span className="intermediate-flow-l2-risk" title={(resolution.warnings ?? []).join(" · ")}>
                  {t("需核对产品范围和限定词", "Review product scope and qualifiers")}
                </span>
              </label>
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
            disabled={busy || selectedIds.length === 0}
            onClick={() => onConfirm(selectedIds)}
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
