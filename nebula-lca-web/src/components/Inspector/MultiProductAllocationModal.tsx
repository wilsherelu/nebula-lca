import type { AllocationBasisMethod, FlowPort } from "../../model/node";

type AllocationPreview = {
  factors: Record<string, number> | null;
  weights: Record<string, number>;
  method: string;
  message: string;
  ok: boolean;
};

type Props = {
  open: boolean;
  uiLanguage: "zh" | "en";
  products: FlowPort[];
  preview: AllocationPreview;
  importedLocked?: boolean;
  lciNode?: boolean;
  getDisplayName: (port: FlowPort) => string;
  getUnitGroupLabel: (port: FlowPort) => string;
  onClose: () => void;
  onApplyFactors: (factors: Record<string, number> | null, method: AllocationBasisMethod) => void;
  onSetQuantityMode: () => void;
  onSetManualMode: () => void;
  onUpdateProduct: (portId: string, patch: Partial<FlowPort>) => void;
  onOpenFlowProperty: (port: FlowPort) => void;
};

export function MultiProductAllocationModal({
  open,
  uiLanguage,
  products,
  preview,
  importedLocked = false,
  lciNode = false,
  getDisplayName,
  getUnitGroupLabel,
  onClose,
  onApplyFactors,
  onSetQuantityMode,
  onSetManualMode,
  onUpdateProduct,
  onOpenFlowProperty,
}: Props) {
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);

  if (!open) {
    return null;
  }

  const manualMode = products.some((port) =>
    port.allocationBasis?.method === "manual_factor"
    || port.allocationFactor !== null && port.allocationFactor !== undefined
  );
  const baseProduct = products[0];
  const baseUnitGroup = baseProduct ? getUnitGroupLabel(baseProduct) : "";
  const mismatchedProducts = products.filter((port, index) => index > 0 && getUnitGroupLabel(port) !== baseUnitGroup);
  const hasUnitGroupMismatch = !manualMode && mismatchedProducts.length > 0;
  const canApply = products.length > 1 && Boolean(preview.factors);
  const applyMethod: AllocationBasisMethod = manualMode ? "manual_factor" : "quantity";

  const saveAllocation = () => {
    if (!canApply) {
      return;
    }
    onApplyFactors(preview.factors, applyMethod);
    onClose();
  };

  const formatNumber = (value: number | undefined): string => {
    if (!Number.isFinite(value ?? NaN)) {
      return "-";
    }
    return Number(value).toPrecision(6);
  };

  const statusText = () => {
    if (products.length <= 1) {
      return t("勾选至少两个输出为“定义产品”后，可配置多产品分配。", "Mark at least two outputs as products to configure allocation.");
    }
    if (manualMode) {
      return preview.message;
    }
    if (hasUnitGroupMismatch) {
      return t("单位组不一致，仅按单位换算后的数值预览分配；计算和导出前仍需统一单位组或手填系数。", "Unit groups differ; preview uses unit-converted amounts. Calculation and export still require one unit group or manual factors.");
    }
    return t("已按单位组默认单位预览分配。", "Previewing allocation by unit-group reference amounts.");
  };

  return (
    <div className="overlay-modal">
      <section className="pm-modal allocation-config-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{t("多产品分配配置", "Multi-product Allocation")}</strong>
          <button type="button" className="drawer-close-btn" onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </div>

        <div className="allocation-modal-toolbar">
          <div className={!hasUnitGroupMismatch && preview.ok ? "mode-lock-hint" : "mode-lock-hint warning"}>
            <span>{statusText()}</span>
            {hasUnitGroupMismatch && baseProduct && (
              <span className="allocation-inline-detail">
                {t("基准：", "Base: ")}{getDisplayName(baseProduct)} / {baseUnitGroup}
              </span>
            )}
          </div>
          <div className="allocation-actions">
            <button
              type="button"
              className="allocation-action-btn"
              disabled={products.length <= 1 || importedLocked || lciNode}
              onClick={onSetQuantityMode}
            >
              {t("使用按产量分配", "Use Quantity Allocation")}
            </button>
            <button
              type="button"
              className="allocation-action-btn"
              disabled={products.length <= 1 || importedLocked || lciNode}
              onClick={onSetManualMode}
            >
              {t("改为手填系数", "Use Manual Factors")}
            </button>
          </div>
        </div>

        {products.length > 1 && (
          <div className="allocation-modal-body">
            <div className="allocation-grid allocation-grid-head">
              <div>{t("产品", "Product")}</div>
              <div>{t("产量", "Amount")}</div>
              <div>{t("单位组", "Unit Group")}</div>
              <div>{t("分配方式", "Mode")}</div>
              <div>{t("分配系数", "Factor")}</div>
            </div>
            {products.map((port, index) => {
              const calculated = preview.factors?.[port.id];
              const unitGroup = getUnitGroupLabel(port);
              const mismatched = !manualMode && index > 0 && unitGroup !== baseUnitGroup;
              return (
                <div key={port.id} className="allocation-grid">
                  <div className="flow-name-readonly" title={getDisplayName(port)}>
                    {getDisplayName(port)}
                  </div>
                  <div>{`${port.amount} ${port.unit}`}</div>
                  <div className={mismatched ? "allocation-mismatch-cell" : ""}>
                    <span>{unitGroup || "-"}</span>
                    {mismatched && <span>{t("与基准不一致", "Differs from base")}</span>}
                  </div>
                  <div className="allocation-basis-cell">
                    <span>{manualMode ? t("手填系数", "Manual Factor") : index === 0 ? t("按产量：基准", "Quantity: Base") : t("按产量", "Quantity")}</span>
                    {mismatched && (
                      <button
                        type="button"
                        className="link-btn"
                        disabled={importedLocked || lciNode || !port.flowUuid}
                        onClick={() => onOpenFlowProperty(port)}
                      >
                        {t("去切换单位组", "Switch Unit Group")}
                      </button>
                    )}
                  </div>
                  <input
                    type="number"
                    min={0}
                    max={1}
                    step={0.0001}
                    disabled={importedLocked || lciNode || !manualMode}
                    value={manualMode ? Number(port.allocationFactor ?? 0) : formatNumber(calculated)}
                    onChange={(event) => {
                      const value = Number(event.target.value);
                      onUpdateProduct(port.id, {
                        allocationFactor: Number.isFinite(value) ? Math.max(0, value) : 0,
                        allocationBasis: { ...(port.allocationBasis ?? {}), method: "manual_factor" },
                      });
                    }}
                  />
                </div>
              );
            })}
          </div>
        )}
        <div className="allocation-modal-footer">
          <button type="button" className="allocation-action-btn" onClick={onClose}>
            {t("取消", "Cancel")}
          </button>
          <button type="button" className="allocation-action-btn primary" disabled={!canApply} onClick={saveAllocation}>
            {t("保存", "Save")}
          </button>
        </div>
      </section>
    </div>
  );
}
