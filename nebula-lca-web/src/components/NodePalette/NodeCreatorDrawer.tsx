import { useState } from "react";
import { useLcaGraphStore, type NodeCreateKind } from "../../store/lcaGraphStore";

type CreateCard = {
  id: NodeCreateKind;
  title: string;
  desc: string;
};

const getCardToneClass = (id: NodeCreateKind): string => {
  if (id === "market_process") return "node-creator-card--market";
  if (id === "pts_module") return "node-creator-card--pts";
  if (id === "lci_dataset") return "node-creator-card--lci";
  return "node-creator-card--unit";
};

export function NodeCreatorDrawer() {
  const activeCanvasKind = useLcaGraphStore((state) => state.activeCanvasKind);
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const openUnitProcessImportDialog = useLcaGraphStore((state) => state.openUnitProcessImportDialog);
  const [collapsed, setCollapsed] = useState(false);

  const lciTip =
    uiLanguage === "zh"
      ? "LCI数据集：单产品背景过程数据，通常只暴露一个中间流产品。"
      : "LCI dataset: single-product background process.";
  const ptsTip =
    uiLanguage === "zh"
      ? "PTS模块：多个过程封装后的模块，可对外暴露输入/输出端口。"
      : "PTS module: packaged process system with external ports.";

  const baseCards: CreateCard[] = [
    {
      id: "unit_process",
      title: uiLanguage === "zh" ? "单元过程" : "Unit Process",
      desc: uiLanguage === "zh" ? "空白单元过程" : "Blank Unit Process",
    },
    {
      id: "market_process",
      title: uiLanguage === "zh" ? "市场过程" : "Market Process",
      desc: uiLanguage === "zh" ? "空白市场过程" : "Blank Market Process",
    },
    {
      id: "lci_dataset",
      title: uiLanguage === "zh" ? "LCI数据集" : "LCI Dataset",
      desc: uiLanguage === "zh" ? "空白 LCI 数据集" : "Blank LCI Dataset",
    },
  ];
  const cards: CreateCard[] =
    activeCanvasKind === "root"
      ? [
          ...baseCards,
          {
            id: "pts_module",
            title: uiLanguage === "zh" ? "PTS模块" : "PTS Module",
            desc: uiLanguage === "zh" ? "空白 PTS 节点" : "Blank PTS Node",
          },
        ]
      : baseCards;

  return (
    <section className={`node-creator-drawer ${collapsed ? "collapsed" : ""}`}>
      <div className="node-creator-head">
        <div>
          <strong>
            {activeCanvasKind === "pts_internal"
              ? uiLanguage === "zh"
                ? "PTS 内部新建节点"
                : "Create Node In PTS"
              : uiLanguage === "zh"
                ? "新建节点"
                : "Create Node"}
          </strong>
          <span className="node-creator-hint">
            {uiLanguage === "zh" ? "拖拽卡片创建空白过程" : "Drag cards to create blank nodes"}
          </span>
        </div>
        <button type="button" onClick={() => setCollapsed((prev) => !prev)}>
          {collapsed ? (uiLanguage === "zh" ? "展开" : "Expand") : uiLanguage === "zh" ? "收起" : "Collapse"}
        </button>
      </div>
      {!collapsed && (
        <div className="node-creator-grid">
          {cards.map((card) => (
            <div
              key={card.id}
              className={`node-creator-card ${getCardToneClass(card.id)}`}
              draggable
              title={card.id === "lci_dataset" ? lciTip : card.id === "pts_module" ? ptsTip : undefined}
              onDragStart={(event) => {
                event.dataTransfer.setData("application/lca-node-kind", card.id);
                event.dataTransfer.effectAllowed = "copyMove";
              }}
            >
              <div className="node-creator-card-title-row">
                <div className="node-creator-card-text">
                  <div className="node-creator-card-title">{card.title}</div>
                  <div className="node-creator-card-desc">{card.desc}</div>
                </div>
                <button
                  type="button"
                  className="node-creator-import-btn"
                  onClick={(event) => {
                    event.preventDefault();
                    event.stopPropagation();
                    openUnitProcessImportDialog(undefined, card.id);
                  }}
                >
                  {uiLanguage === "zh" ? "导入" : "Import"}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

