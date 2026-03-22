import { useMemo } from "react";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { EdgeInspector } from "./EdgeInspector";

export function FlowBalanceDialog() {
  const dialog = useLcaGraphStore((state) => state.flowBalanceDialog);
  const edges = useLcaGraphStore((state) => state.edges);
  const closeDialog = useLcaGraphStore((state) => state.closeFlowBalanceDialog);
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);

  const edge = useMemo(() => {
    if (!dialog.open || !dialog.edgeId) {
      return undefined;
    }
    return edges.find((item) => item.id === dialog.edgeId);
  }, [dialog.edgeId, dialog.open, edges]);

  if (!dialog.open || !edge) {
    return null;
  }

  return (
    <div className="overlay-modal">
      <div className="overlay-panel edge-inspector-modal">
        <div className="overlay-head">
          <strong>{uiLanguage === "zh" ? "线配平 / 详情" : "Flow Balance / Details"}</strong>
          <button className="drawer-close-btn" onClick={closeDialog}>
            {uiLanguage === "zh" ? "关闭" : "Close"}
          </button>
        </div>
        <EdgeInspector edge={edge} onClose={closeDialog} />
      </div>
    </div>
  );
}
