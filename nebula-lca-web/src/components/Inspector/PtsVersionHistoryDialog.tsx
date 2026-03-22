type PtsVersionItem = {
  id: string;
  graph_hash: string;
  version?: number | null;
  created_at: string;
  updated_at: string;
  ok?: boolean | null;
  matrix_size?: number | null;
  invertible?: boolean | null;
  source_compile_id?: string | null;
  source_compile_version?: number | null;
};

type PtsVersionHistoryDialogProps = {
  open: boolean;
  loading: boolean;
  ptsName: string;
  ptsUuid: string;
  compileItems: PtsVersionItem[];
  publishedItems: PtsVersionItem[];
  activePublishedVersion?: number | null;
  onClose: () => void;
};

const formatDateTime = (value: string) => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
};

export function PtsVersionHistoryDialog({
  open,
  loading,
  ptsName,
  ptsUuid,
  compileItems,
  publishedItems,
  activePublishedVersion,
  onClose,
}: PtsVersionHistoryDialogProps) {
  if (!open) {
    return null;
  }

  return (
    <div className="overlay-modal">
      <div className="overlay-panel">
        <div className="inspector-drawer-head">
          <div className="drawer-title">PTS 版本历史</div>
          <button className="drawer-close-btn" onClick={onClose}>
            关闭
          </button>
        </div>
        <div className="pts-port-scroll">
          <div className="pts-port-meta">
            <strong>{ptsName}</strong>
            <span>{ptsUuid}</span>
          </div>
          <div className="status-bar status-bar--info">
            当前开源版主图始终消费 active/latest published PTS。当前 active 发布版本：{activePublishedVersion ?? "未设置"}
          </div>
          {loading ? (
            <div className="table-empty">正在读取版本历史...</div>
          ) : (
            <>
              <section className="inventory-section">
                <div className="inventory-section-head">
                  <h4>编译历史</h4>
                </div>
                <div className="run-analysis-table-wrap">
                  <table className="run-analysis-table">
                    <thead>
                      <tr>
                        <th>编译版本</th>
                        <th>图哈希</th>
                        <th>状态</th>
                        <th>矩阵</th>
                        <th>时间</th>
                      </tr>
                    </thead>
                    <tbody>
                      {compileItems.map((item) => (
                        <tr key={`compile-${item.id}`}>
                          <td>{item.version ?? "-"}</td>
                          <td className="run-analysis-method-cell">{item.graph_hash}</td>
                          <td>{item.ok ? "ok" : "failed"}</td>
                          <td>{item.matrix_size ?? "-"}</td>
                          <td>{formatDateTime(item.updated_at || item.created_at)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {compileItems.length === 0 && <div className="table-empty">暂无编译历史</div>}
                </div>
              </section>
              <section className="inventory-section">
                <div className="inventory-section-head">
                  <h4>发布历史</h4>
                </div>
                <div className="run-analysis-table-wrap">
                  <table className="run-analysis-table">
                    <thead>
                      <tr>
                        <th>发布版本</th>
                        <th>来源编译版本</th>
                        <th>图哈希</th>
                        <th>时间</th>
                      </tr>
                    </thead>
                    <tbody>
                      {publishedItems.map((item) => {
                        const isActive = activePublishedVersion != null && item.version === activePublishedVersion;
                        return (
                          <tr key={`published-${item.id}`}>
                            <td>
                              {item.version ?? "-"}
                              {isActive ? " (active)" : ""}
                            </td>
                            <td>{item.source_compile_version ?? "-"}</td>
                            <td className="run-analysis-method-cell">{item.graph_hash}</td>
                            <td>{formatDateTime(item.updated_at || item.created_at)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                  {publishedItems.length === 0 && <div className="table-empty">暂无发布历史</div>}
                </div>
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
