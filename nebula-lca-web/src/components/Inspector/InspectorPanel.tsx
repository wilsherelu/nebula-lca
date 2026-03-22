import { EdgeInspector } from "./EdgeInspector";
import { NodeInspector } from "./NodeInspector";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

export function InspectorPanel() {
  const selection = useLcaGraphStore((state) => state.selection);
  const nodes = useLcaGraphStore((state) => state.nodes);
  const edges = useLcaGraphStore((state) => state.edges);
  const inspectorOpen = useLcaGraphStore((state) => state.inspectorOpen);
  const closeInspector = useLcaGraphStore((state) => state.closeInspector);
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);

  const nodeTargetId = selection.nodeId ?? selection.nodeIds[0];
  const edgeTargetId = selection.edgeId ?? selection.edgeIds[0];
  const node = nodeTargetId ? nodes.find((item) => item.id === nodeTargetId) : undefined;
  const edge = edgeTargetId ? edges.find((item) => item.id === edgeTargetId) : undefined;

  if (!inspectorOpen) {
    return null;
  }

  if (!node && !edge) {
    return null;
  }

  if (edge && !node) {
    return (
      <div className="overlay-modal">
        <div className="overlay-panel edge-inspector-modal">
          <div className="overlay-head">
            <strong>{t("线配平 / 详情", "Flow Balance / Details")}</strong>
            <button className="drawer-close-btn" onClick={() => closeInspector({ requireProductConfirm: true })}>{t("关闭", "Close")}</button>
          </div>
          <EdgeInspector edge={edge} />
        </div>
      </div>
    );
  }

  return (
    <aside className="inspector-drawer">
      <div className="inspector-drawer-head">
        <div>{t("清单分析", "Inventory Analysis")}</div>
        <button className="drawer-close-btn" onClick={() => closeInspector({ requireProductConfirm: true })}>{t("关闭", "Close")}</button>
      </div>
      {node && <NodeInspector node={node} />}
      {edge && <EdgeInspector edge={edge} />}
    </aside>
  );
}
