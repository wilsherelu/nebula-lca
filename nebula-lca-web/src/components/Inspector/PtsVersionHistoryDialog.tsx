import { useState } from "react";

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
  projectId: string;
  ptsName: string;
  ptsUuid: string;
  compileItems: PtsVersionItem[];
  publishedItems: PtsVersionItem[];
  activePublishedVersion?: number | null;
  onClose: () => void;
  uiLanguage: "zh" | "en";
};

type PtsArtifactDiagnostics = {
  artifact_kind: "compile" | "published";
  artifact_id: string;
  version?: number | null;
  graph_hash: string;
  resource_latest_graph_hash?: string | null;
  active_published_version?: number | null;
  source_compile_id?: string | null;
  source_compile_version?: number | null;
  stale_against_resource?: boolean;
  ok?: boolean | null;
  matrix_size?: number | null;
  invertible?: boolean | null;
  summary?: Record<string, unknown>;
  virtual_processes?: Array<Record<string, unknown>>;
  artifact?: Record<string, unknown>;
};

const formatDateTime = (value: string, uiLanguage: "zh" | "en") => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString(uiLanguage === "zh" ? "zh-CN" : "en-US", {
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
  projectId,
  ptsName,
  ptsUuid,
  compileItems,
  publishedItems,
  activePublishedVersion,
  onClose,
  uiLanguage,
}: PtsVersionHistoryDialogProps) {
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);
  const [diagnosticsLoading, setDiagnosticsLoading] = useState(false);
  const [diagnosticsError, setDiagnosticsError] = useState("");
  const [diagnostics, setDiagnostics] = useState<PtsArtifactDiagnostics | null>(null);

  const loadDiagnostics = async (kind: "compile" | "published", item: PtsVersionItem) => {
    if (!projectId || !ptsUuid) {
      return;
    }
    setDiagnosticsLoading(true);
    setDiagnosticsError("");
    try {
      const params = new URLSearchParams({
        project_id: projectId,
        kind,
        artifact_id: item.id,
      });
      const resp = await fetch(`/api/pts/${encodeURIComponent(ptsUuid)}/artifact-diagnostics?${params.toString()}`);
      if (!resp.ok) {
        throw new Error(await resp.text());
      }
      setDiagnostics((await resp.json()) as PtsArtifactDiagnostics);
    } catch (error) {
      setDiagnosticsError(error instanceof Error ? error.message : String(error));
    } finally {
      setDiagnosticsLoading(false);
    }
  };

  const formatHash = (value?: string | null) => {
    if (!value) return "-";
    return value.length > 16 ? `${value.slice(0, 10)}...${value.slice(-6)}` : value;
  };

  const renderValue = (value: unknown) => {
    if (value === undefined || value === null || value === "") return "-";
    if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toPrecision(6);
    if (typeof value === "boolean") return value ? "yes" : "no";
    return String(value);
  };

  const renderVirtualProcess = (vp: Record<string, unknown>, index: number) => {
    const referenceProduct = (vp.reference_product && typeof vp.reference_product === "object" ? vp.reference_product : {}) as Record<string, unknown>;
    const technosphere = Array.isArray(vp.technosphere_inputs) ? vp.technosphere_inputs as Array<Record<string, unknown>> : [];
    const elementary = Array.isArray(vp.elementary_flows) ? vp.elementary_flows as Array<Record<string, unknown>> : [];
    return (
      <details key={`${vp.process_uuid ?? index}`} className="inventory-section" open={index === 0}>
        <summary style={{ cursor: "pointer", fontWeight: 700 }}>
          {renderValue(vp.process_name)} · {renderValue(vp.product_key)}
        </summary>
        <div style={{ display: "grid", gap: 6, marginTop: 8, fontSize: 12 }}>
          <div>source_port_id: <b>{renderValue(vp.source_port_id)}</b></div>
          <div>source_process_uuid: <b>{renderValue(vp.source_process_uuid)}</b></div>
          <div>source_node_id: <b>{renderValue(vp.source_node_id)}</b></div>
          <div>allocation_fraction: <b>{renderValue(vp.allocation_fraction)}</b></div>
          <div>normalization_reference_amount: <b>{renderValue(vp.normalization_reference_amount)}</b></div>
          <div>unit / unitGroup: <b>{renderValue(vp.reference_unit)} / {renderValue(vp.reference_unit_group)}</b></div>
          <div>reference_product: <b>{renderValue(referenceProduct.name)} · {renderValue(referenceProduct.amount)} {renderValue(referenceProduct.unit)} · {renderValue(referenceProduct.flow_uuid)}</b></div>
          <div>{t("技术圈输入", "Technosphere inputs")}: <b>{technosphere.length}</b></div>
          <div>{t("基本流", "Elementary flows")}: <b>{elementary.length}</b></div>
        </div>
      </details>
    );
  };

  if (!open) {
    return null;
  }

  return (
    <div className="overlay-modal">
      <div className="overlay-panel">
        <div className="inspector-drawer-head">
          <div className="drawer-title">{t("PTS 版本历史", "PTS Version History")}</div>
          <button className="drawer-close-btn" onClick={onClose}>
            {t("关闭", "Close")}
          </button>
        </div>
        <div className="pts-port-scroll">
          <div className="pts-port-meta">
            <strong>{ptsName}</strong>
            <span>{ptsUuid}</span>
          </div>
          <div className="status-bar status-bar--info">
            {t(
              `当前开源版主图始终消费 active/latest published PTS。当前 active 发布版本：${activePublishedVersion ?? "未设置"}`,
              `The open-source main graph always consumes the active/latest published PTS. Current active published version: ${activePublishedVersion ?? "Not set"}`
            )}
          </div>
          {loading ? (
            <div className="table-empty">{t("正在读取版本历史...", "Loading version history...")}</div>
          ) : (
            <>
              <section className="inventory-section">
                <div className="inventory-section-head">
                  <h4>{t("编译历史", "Compile History")}</h4>
                </div>
                <div className="run-analysis-table-wrap">
                  <table className="run-analysis-table">
                    <thead>
                      <tr>
                        <th>{t("编译版本", "Compile Version")}</th>
                        <th>{t("图哈希", "Graph Hash")}</th>
                        <th>{t("状态", "Status")}</th>
                        <th>{t("矩阵", "Matrix")}</th>
                        <th>{t("时间", "Time")}</th>
                        <th>{t("诊断", "Diagnostics")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {compileItems.map((item) => (
                        <tr key={`compile-${item.id}`}>
                          <td>{item.version ?? "-"}</td>
                          <td className="run-analysis-method-cell">{item.graph_hash}</td>
                          <td>{item.ok ? "ok" : "failed"}</td>
                          <td>{item.matrix_size ?? "-"}</td>
                          <td>{formatDateTime(item.updated_at || item.created_at, uiLanguage)}</td>
                          <td>
                            <button className="pm-link-btn" type="button" onClick={() => void loadDiagnostics("compile", item)}>
                              {t("查看", "View")}
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {compileItems.length === 0 && <div className="table-empty">{t("暂无编译历史", "No compile history")}</div>}
                </div>
              </section>
              <section className="inventory-section">
                <div className="inventory-section-head">
                  <h4>{t("发布历史", "Publish History")}</h4>
                </div>
                <div className="run-analysis-table-wrap">
                  <table className="run-analysis-table">
                    <thead>
                      <tr>
                        <th>{t("发布版本", "Publish Version")}</th>
                        <th>{t("来源编译版本", "Source Compile Version")}</th>
                        <th>{t("图哈希", "Graph Hash")}</th>
                        <th>{t("时间", "Time")}</th>
                        <th>{t("诊断", "Diagnostics")}</th>
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
                            <td>{formatDateTime(item.updated_at || item.created_at, uiLanguage)}</td>
                            <td>
                              <button className="pm-link-btn" type="button" onClick={() => void loadDiagnostics("published", item)}>
                                {t("查看", "View")}
                              </button>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                  {publishedItems.length === 0 && <div className="table-empty">{t("暂无发布历史", "No publish history")}</div>}
                </div>
              </section>
              <section className="inventory-section">
                <div className="inventory-section-head">
                  <h4>{t("Artifact 诊断", "Artifact Diagnostics")}</h4>
                </div>
                {diagnosticsLoading && <div className="table-empty">{t("正在读取 artifact...", "Loading artifact...")}</div>}
                {diagnosticsError && <div className="status-bar status-bar--warning">{diagnosticsError}</div>}
                {!diagnosticsLoading && !diagnostics && !diagnosticsError && (
                  <div className="table-empty">{t("点击编译或发布历史中的“查看”读取 payload。", "Click View in compile or publish history to load a payload.")}</div>
                )}
                {diagnostics && (
                  <div style={{ display: "grid", gap: 10 }}>
                    <div className={diagnostics.stale_against_resource ? "status-bar status-bar--warning" : "status-bar status-bar--info"}>
                      {diagnostics.stale_against_resource
                        ? t("当前资源图哈希与 artifact 图哈希不一致，可能是过期发布。", "The resource graph hash differs from this artifact hash; the artifact may be stale.")
                        : t("当前资源图哈希与 artifact 图哈希一致或未记录资源图哈希。", "The resource graph hash matches this artifact hash, or no resource graph hash is recorded.")}
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 8, fontSize: 12 }}>
                      <div>{t("类型", "Kind")}: <b>{diagnostics.artifact_kind}</b></div>
                      <div>{t("版本", "Version")}: <b>{renderValue(diagnostics.version)}</b></div>
                      <div>artifact_id: <b>{diagnostics.artifact_id}</b></div>
                      <div>graph_hash: <b title={diagnostics.graph_hash}>{formatHash(diagnostics.graph_hash)}</b></div>
                      <div>resource_latest_graph_hash: <b title={diagnostics.resource_latest_graph_hash ?? ""}>{formatHash(diagnostics.resource_latest_graph_hash)}</b></div>
                      <div>active_published_version: <b>{renderValue(diagnostics.active_published_version)}</b></div>
                      <div>source_compile_version: <b>{renderValue(diagnostics.source_compile_version)}</b></div>
                      <div>matrix_size: <b>{renderValue(diagnostics.matrix_size)}</b></div>
                    </div>
                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap", fontSize: 12 }}>
                      <span>virtual_processes: <b>{renderValue(diagnostics.summary?.virtual_process_count)}</b></span>
                      <span>technosphere_inputs: <b>{renderValue(diagnostics.summary?.technosphere_input_count)}</b></span>
                      <span>elementary_flows: <b>{renderValue(diagnostics.summary?.elementary_flow_count)}</b></span>
                    </div>
                    <div style={{ display: "grid", gap: 8 }}>
                      {(diagnostics.virtual_processes ?? []).map(renderVirtualProcess)}
                    </div>
                    <details>
                      <summary style={{ cursor: "pointer", fontWeight: 700 }}>{t("原始 artifact JSON", "Raw artifact JSON")}</summary>
                      <pre style={{ maxHeight: 320, overflow: "auto", fontSize: 11, background: "#f5f9fc", padding: 12, borderRadius: 6 }}>
                        {JSON.stringify(diagnostics.artifact ?? {}, null, 2)}
                      </pre>
                    </details>
                  </div>
                )}
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
