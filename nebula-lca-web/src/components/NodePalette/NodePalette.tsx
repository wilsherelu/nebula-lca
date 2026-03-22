import { useState } from "react";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

const normalizeNodeName = (value: string) => value.trim().toLowerCase();

export function NodePalette() {
  const nodes = useLcaGraphStore((state) => state.nodes);
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const selection = useLcaGraphStore((state) => state.selection);
  const openNodeInspector = useLcaGraphStore((state) => state.openNodeInspector);
  const enterPtsNode = useLcaGraphStore((state) => state.enterPtsNode);
  const updateNode = useLcaGraphStore((state) => state.updateNode);
  const setConnectionHint = useLcaGraphStore((state) => state.setConnectionHint);
  const activeCanvasKind = useLcaGraphStore((state) => state.activeCanvasKind);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [nameDraft, setNameDraft] = useState("");

  const lciTip =
    uiLanguage === "zh"
      ? "LCI数据集：单产品背景过程数据，通常只暴露一个中间流产品及其基本流清单。"
      : "LCI dataset: single-product background process with elementary flow inventory.";
  const ptsTip =
    uiLanguage === "zh"
      ? "PTS模块：由多个内部过程封装形成的产品系统模块，可暴露外部端口供主图调用。"
      : "PTS module: packaged internal process system with external ports for the main graph.";

  const grouped = [
    {
      key: "market",
      label: uiLanguage === "zh" ? "市场过程" : "Market Process",
      items: nodes.filter((node) => node.data.nodeKind === "market_process" || node.data.processUuid.startsWith("market_")),
    },
    {
      key: "unit",
      label: uiLanguage === "zh" ? "单元过程" : "Unit Process",
      items: nodes.filter(
        (node) =>
          node.data.nodeKind === "unit_process" &&
          !node.data.processUuid.startsWith("market_"),
      ),
    },
    ...(activeCanvasKind === "root"
      ? [{ key: "pts_module", label: uiLanguage === "zh" ? "PTS模块" : "PTS Module", items: nodes.filter((node) => node.data.nodeKind === "pts_module") }]
      : []),
    {
      key: "lci",
      label: uiLanguage === "zh" ? "LCI数据集" : "LCI Dataset",
      items: nodes.filter((node) => node.data.nodeKind === "lci_dataset"),
    },
  ];

  const commitRename = (nodeId: string) => {
    const next = nameDraft.trim();
    if (!next) {
      setEditingId(null);
      return;
    }
    const duplicated = nodes.some(
      (node) => node.id !== nodeId && normalizeNodeName(node.data.name) === normalizeNodeName(next),
    );
    if (duplicated) {
      setConnectionHint(uiLanguage === "zh" ? `过程名称重复：${next}` : `Duplicate process name: ${next}`);
      return;
    }
    updateNode(nodeId, (node) => ({
      ...node,
      data: {
        ...node.data,
        name: next,
      },
    }));
    setConnectionHint(undefined);
    setEditingId(null);
  };

  return (
    <aside className="panel panel-left">
      <div className="panel-title">
        {activeCanvasKind === "pts_internal"
          ? uiLanguage === "zh"
            ? "PTS模块编辑节点"
            : "PTS Module Nodes"
          : uiLanguage === "zh"
            ? "过程列表"
            : "Process List"}
      </div>
      {grouped.map(({ key, label, items }) => (
        <div key={key} className="palette-group">
          <div className="palette-group-title" title={key === "lci" ? lciTip : key === "pts_module" ? ptsTip : undefined}>
            {label}
          </div>
          {items.map((item) => (
            <div
              key={item.id}
              className={`palette-item palette-item-static ${selection.nodeIds.includes(item.id) ? "active" : ""}`}
              onClick={() => {
                if (item.data.nodeKind === "pts_module") {
                  enterPtsNode(item.id);
                  return;
                }
                openNodeInspector(item.id);
              }}
            >
              <div className="palette-item-name">
                {editingId === item.id ? (
                  <input
                    autoFocus
                    className="palette-name-input"
                    value={nameDraft}
                    onChange={(event) => setNameDraft(event.target.value)}
                    onBlur={() => commitRename(item.id)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        commitRename(item.id);
                      }
                      if (event.key === "Escape") {
                        setEditingId(null);
                      }
                    }}
                  />
                ) : (
                  <>
                    <span
                      className="palette-item-name-text"
                      title={`${item.data.name}${item.data.importMode === "locked" ? " [只读]" : ""}`}
                    >
                      {item.data.name}
                      {item.data.importMode === "locked" ? " [只读]" : ""}
                    </span>
                    <button
                      type="button"
                      className="link-btn"
                      disabled={item.data.importMode === "locked"}
                      onClick={(event) => {
                        event.stopPropagation();
                        setEditingId(item.id);
                        setNameDraft(item.data.name);
                      }}
                    >
                      {uiLanguage === "zh" ? "改名" : "Rename"}
                    </button>
                  </>
                )}
              </div>
            </div>
          ))}
          {items.length === 0 && <div className="table-empty">{uiLanguage === "zh" ? "暂无" : "None"}</div>}
        </div>
      ))}
      <div className="palette-legend">
        <div>{uiLanguage === "zh" ? "提示" : "Tips"}</div>
        <div>{uiLanguage === "zh" ? "单击过程名称可直接改名。" : "Click process name to rename."}</div>
        <div>{uiLanguage === "zh" ? "双击过程卡片打开清单分析。" : "Double click process card to open inspector."}</div>
        <div>{uiLanguage === "zh" ? "新建节点请使用画布底部抽屉。" : "Create nodes from the bottom drawer."}</div>
      </div>
    </aside>
  );
}

