import { getApiBase } from "../apiBase";
import { useEffect, useRef, useState } from "react";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface Ef31PreviewCounts {
  datasets?: number;
  exchanges?: number;
  missing_refs?: number;
  cf_rows_matched?: number;
  cf_rows_unmatched?: number;
  cf_rows_ambiguous?: number;
}

interface Ef31ArchiveFileDiscovery {
  master_data_files?: number;
  datasets_spold_files_total?: number;
  datasets_spold_files_selected?: number;
  lcia_excel_found?: number;
}

interface Ef31Foundation {
  units?: number;
  elementary_flows?: number;
  intermediate_flows?: number;
  unit_conversions?: number;
  indicators_total?: number;
  indicators_ef31?: number;
  cf_rows_total?: number;
  cf_rows_ef31?: number;
  cf_rows_matched?: number;
  cf_rows_unmatched?: number;
  cf_rows_ambiguous?: number;
}

interface Ef31PreviewResponse {
  job_id: string;
  can_commit: boolean;
  counts: Ef31PreviewCounts;
  archive_file_discovery: Ef31ArchiveFileDiscovery;
  foundation: Ef31Foundation;
  warnings: string[];
  errors: string[];
}

interface Ef31CommitCountBucket {
  new?: number;
  skipped?: number;
  error?: number;
}

interface Ef31CommitResponse {
  committed: boolean;
  job_id: string;
  counts: {
    flows?: Ef31CommitCountBucket;
    processes?: Ef31CommitCountBucket;
    units?: Ef31CommitCountBucket;
  };
  warnings: string[];
  errors: string[];
  catalog_target_kind?: string;
}

interface Ef31RuntimeCsvResponse {
  runtime_schema_version?: string;
  runtime_id?: string;
  job_id: string;
  output_dir: string;
  artifact_dir?: string;
  active?: boolean;
  files?: Record<string, string>;
  flows_count: number;
  indicators_count: number;
  factors_count: number;
  cf_matched: number;
  cf_unmatched: number;
  cf_ambiguous: number;
  env_var: string;
}

/* ------------------------------------------------------------------ */
/*  Constants                                                        */
/* ------------------------------------------------------------------ */

const API_BASE = getApiBase();

/* ------------------------------------------------------------------ */
/*  Component                                                        */
/* ------------------------------------------------------------------ */

export default function Ef31ImportDialog(props: {
  open: boolean;
  uiLanguage: "zh" | "en";
  onClose: () => void;
}) {
  const { open, uiLanguage, onClose } = props;
  const zh = uiLanguage === "zh";

  // --- State ---
  const [lciFile, setLciFile] = useState<File | null>(null);
  const [lciaFile, setLciaFile] = useState<File | null>(null);
  const [limit, setLimit] = useState<number>(100);
  const [busy, setBusy] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [phase, setPhase] = useState<"upload" | "preview" | "commit" | "done">("upload");
  const [preview, setPreview] = useState<Ef31PreviewResponse | null>(null);
  const [commitResult, setCommitResult] = useState<Ef31CommitResponse | null>(null);
  const [reportPayload, setReportPayload] = useState<Record<string, unknown> | null>(null);
  const [reportBusy, setReportBusy] = useState(false);
  const [reportJobId, setReportJobId] = useState<string>("");
  const [runtimeCsv, setRuntimeCsv] = useState<Ef31RuntimeCsvResponse | null>(null);
  const [runtimeBusy, setRuntimeBusy] = useState(false);

  const lciInputRef = useRef<HTMLInputElement | null>(null);
  const lciaInputRef = useRef<HTMLInputElement | null>(null);

  // --- Reset on open/close ---
  useEffect(() => {
    if (!open) {
      setLciFile(null);
      setLciaFile(null);
      setLimit(100);
      setBusy(false);
      setErrorText("");
      setPhase("upload");
      setPreview(null);
      setCommitResult(null);
      setReportPayload(null);
      setReportBusy(false);
      setReportJobId("");
      setRuntimeCsv(null);
      setRuntimeBusy(false);
    }
  }, [open]);

  /* ---------------------------------------------------------------- */
  /*  Handlers                                                       */
  /* ---------------------------------------------------------------- */

  const handlePreview = async () => {
    if (!lciFile) {
      setErrorText(zh ? "请选择 LCI .7z 文件" : "Please choose an LCI .7z file");
      return;
    }
    setErrorText("");
    setBusy(true);
    try {
      const form = new FormData();
      form.append("lci_archive", lciFile);
      if (lciaFile) {
        form.append("lcia_archive", lciaFile);
      }
      const resp = await fetch(`${API_BASE}/import/ef31/preview?limit=${limit}`, {
        method: "POST",
        body: form,
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { message?: string };
        throw new Error(err.message ?? `HTTP ${resp.status}`);
      }
      const data = (await resp.json()) as Ef31PreviewResponse;
      setPreview(data);
      setPhase("preview");
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : zh ? "预览失败" : "Preview failed");
    } finally {
      setBusy(false);
    }
  };

  const handleCommit = async () => {
    if (!preview) return;
    setErrorText("");
    setBusy(true);
    try {
      const resp = await fetch(`${API_BASE}/import/ef31/commit`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: preview.job_id, confirm: true }),
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { message?: string };
        throw new Error(err.message ?? `HTTP ${resp.status}`);
      }
      const data = (await resp.json()) as Ef31CommitResponse;
      setCommitResult(data);
      setPhase("done");
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : zh ? "确认导入失败" : "Commit failed");
    } finally {
      setBusy(false);
    }
  };

  const handleViewReport = async () => {
    const jobId = commitResult?.job_id ?? preview?.job_id;
    if (!jobId) return;
    setReportJobId(jobId);
    setReportBusy(true);
    setErrorText("");
    try {
      const resp = await fetch(`${API_BASE}/import/ef31/reports/${encodeURIComponent(jobId)}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const payload = (await resp.json()) as Record<string, unknown>;
      setReportPayload(payload);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : zh ? "报告加载失败" : "Report load failed");
    } finally {
      setReportBusy(false);
    }
  };

  const handleGenerateRuntimeCsv = async () => {
    const jobId = commitResult?.job_id ?? preview?.job_id;
    if (!jobId) return;
    setRuntimeBusy(true);
    setErrorText("");
    try {
      const resp = await fetch(`${API_BASE}/import/ef31/runtime-csv/${encodeURIComponent(jobId)}`, {
        method: "POST",
      });
      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { message?: string; detail?: { message?: string } };
        throw new Error(err.detail?.message ?? err.message ?? `HTTP ${resp.status}`);
      }
      const data = (await resp.json()) as Ef31RuntimeCsvResponse;
      setRuntimeCsv(data);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : "Runtime CSV generation failed");
    } finally {
      setRuntimeBusy(false);
    }
  };

  /* ---------------------------------------------------------------- */
  /*  Labels                                                         */
  /* ---------------------------------------------------------------- */

  const t = {
    title: zh ? "导入 LCI 数据库" : "Import LCI Database",
    step1: zh ? "1. 上传文件" : "1. Upload Files",
    step2: zh ? "2. 预览" : "2. Preview",
    step3: zh ? "3. 确认导入" : "3. Confirm Import",
    step4: zh ? "4. 结果" : "4. Result",
    lciLabel: zh ? "LCI 数据包（必填）" : "LCI Database Package (Required)",
    lciaLabel: zh ? "LCIA 因子文件（可选）" : "LCIA Factor File (Optional)",
    lciaHint: zh
      ? "支持 .xlsx 或 .7z 格式"
      : "Supports .xlsx or .7z format",
    limitLabel: zh
      ? "预览/导入前 N 个 LCI 数据集（用于小批量验证）"
      : "Preview/Import first N LCI datasets (for small-scale validation)",
    browse: zh ? "选择文件" : "Browse",
    preview: zh ? "预览" : "Preview",
    commit: zh ? "确认导入" : "Confirm Import",
    back: zh ? "返回" : "Back",
    done: zh ? "完成" : "Done",
    viewReport: zh ? "查看报告" : "View Report",
    close: zh ? "关闭" : "Close",
    canCommit: zh ? "可以导入" : "Ready to import",
    cantCommit: zh ? "存在错误，暂不可导入" : "Errors present, import blocked",
    warnings: zh ? "提醒" : "Warnings",
    errors: zh ? "错误" : "Errors",
    emptyLcia: zh ? "未提供 LCIA 档案" : "No LCIA archive provided",
  };

  /* ---------------------------------------------------------------- */
  /*  Render                                                         */
  /* ---------------------------------------------------------------- */

  if (!open) return null;

  const canProceedToCommit = preview?.can_commit ?? false;
  const hasErrors = (preview?.errors?.length ?? 0) > 0;
  const commitFlows = commitResult?.counts?.flows ?? {};
  const commitProcesses = commitResult?.counts?.processes ?? {};
  const commitUnits = commitResult?.counts?.units ?? {};

  return (
    <div className="pm-modal-mask" onClick={onClose}>
      <div className="pm-modal pm-tidas-modal" onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="pm-modal-head">
          <strong>{t.title}</strong>
          <button type="button" className="pm-link-btn" onClick={onClose}>
            {t.close}
          </button>
        </div>

        {/* Phase indicator */}
        <div style={{ padding: "12px 14px 0", display: "flex", gap: 6, flexWrap: "wrap" }}>
          {(["upload", "preview", "commit", "done"] as const).map((p, i) => {
            const idx = ["upload", "preview", "commit", "done"].indexOf(p);
            const active = idx <= ["upload", "preview", "commit", "done"].indexOf(phase);
            return (
              <span
                key={p}
                style={{
                  fontSize: 12,
                  fontWeight: active ? 600 : 400,
                  color: active ? "#0b66c5" : "#8fa8b5",
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: `1px solid ${active ? "#0d83ff" : "#c5d4dd"}`,
                  background: active ? "#e6f2ff" : "#f5f9fc",
                }}
              >
                {[t.step1, t.step2, t.step3, t.step4][i]}
              </span>
            );
          })}
        </div>

        {/* Body */}
        <div className="pm-modal-grid">
          {/* ========== Upload Phase ========== */}
          {(phase === "upload" || phase === "preview" || phase === "commit") && (
            <>
              <label className="span-2">
                <span>{t.lciLabel}</span>
                <div className="pm-file-picker-row">
                  <input
                    className="pm-file-picker-display"
                    value={lciFile?.name ?? ""}
                    readOnly
                    placeholder={zh ? "请选择 LCI 数据包 .7z 文件" : "Choose LCI database .7z file"}
                  />
                  <button type="button" className="pm-file-picker-btn" onClick={() => lciInputRef.current?.click()}>
                    {t.browse}
                  </button>
                  <input
                    ref={lciInputRef}
                    type="file"
                    accept=".7z,application/x-7z-compressed"
                    style={{ display: "none" }}
                    onChange={(e) => {
                      const f = e.target.files?.[0] ?? null;
                      setLciFile(f);
                      setErrorText("");
                    }}
                  />
                </div>
              </label>

              <label className="span-2">
                <span>{t.lciaLabel}</span>
                <span style={{ fontSize: 11, color: "#8fa8b5" }}>(.xlsx / .7z)</span>
                <div className="pm-file-picker-row">
                  <input
                    className="pm-file-picker-display"
                    value={lciaFile?.name ?? ""}
                    readOnly
                    placeholder={t.emptyLcia}
                  />
                  <button type="button" className="pm-file-picker-btn" onClick={() => lciaInputRef.current?.click()}>
                    {t.browse}
                  </button>
                  <input
                    ref={lciaInputRef}
                    type="file"
                    accept=".7z,.xlsx,application/x-7z-compressed,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    style={{ display: "none" }}
                    onChange={(e) => {
                      const f = e.target.files?.[0] ?? null;
                      setLciaFile(f);
                      setErrorText("");
                    }}
                  />
                </div>
              </label>

              <label>
                <span>{t.limitLabel}</span>
                <input
                  type="number"
                  min={1}
                  max={1000}
                  value={limit}
                  onChange={(e) => setLimit(Math.min(1000, Math.max(1, Number(e.target.value))))}
                />
              </label>
            </>
          )}

          {/* ========== Preview Phase ========== */}
          {phase === "preview" && preview && (
            <>
              <label className="span-2" style={{ cursor: "default" }}>
                <span style={{ fontWeight: 600 }}>{t.step2}</span>
                <div style={{ display: "flex", gap: 6, flexWrap: "wrap", fontSize: 12, color: "#2e5161" }}>
                  <span>
                    {zh ? "数据集" : "Datasets"}: <b>{preview.counts?.datasets ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "交换" : "Exchanges"}: <b>{preview.counts?.exchanges ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "缺失引用" : "Missing refs"}:{" "}
                    <b style={{ color: (preview.counts?.missing_refs ?? 0) > 0 ? "#c0392b" : "inherit" }}>
                      {preview.counts?.missing_refs ?? 0}
                    </b>
                  </span>
                </div>
              </label>

              {/* Archive discovery */}
              <label className="span-2" style={{ cursor: "default" }}>
                <span style={{ fontWeight: 600 }}>{t.lciLabel}</span>
                <div style={{ display: "flex", gap: 12, fontSize: 12, color: "#496675" }}>
                  <span>
                    {zh ? "MasterData XML" : "MasterData XML"}:{" "}
                    <b>{preview.archive_file_discovery?.master_data_files ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "SPOLD total" : "SPOLD total"}:{" "}
                    <b>{preview.archive_file_discovery?.datasets_spold_files_total ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "SPOLD selected" : "SPOLD selected"}:{" "}
                    <b>{preview.archive_file_discovery?.datasets_spold_files_selected ?? 0}</b>
                  </span>
                  <span>
                    LCIA:{" "}
                    <b>{preview.archive_file_discovery?.lcia_excel_found ? "✓" : "—"} </b>
                  </span>
                </div>
              </label>

              {/* Foundation summary */}
              <label className="span-2" style={{ cursor: "default" }}>
                <span style={{ fontWeight: 600 }}>
                  {zh ? "基础数据" : "Foundation"}
                </span>
                <div style={{ display: "flex", gap: 12, fontSize: 12, color: "#496675" }}>
                  <span>
                    {zh ? "单位" : "Units"}: <b>{preview.foundation?.units ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "基本流" : "Elementary"}: <b>{preview.foundation?.elementary_flows ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "中间流" : "Intermediate"}: <b>{preview.foundation?.intermediate_flows ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "EF3.1 指标" : "EF3.1 Indicators"}:{" "}
                    <b>{preview.foundation?.indicators_ef31 ?? 0}</b>
                  </span>
                  <span>
                    {zh ? "EF3.1 CF" : "EF3.1 CFs"}: <b>{preview.foundation?.cf_rows_ef31 ?? 0}</b>
                  </span>
                  <span>
                    CF matched: <b>{preview.foundation?.cf_rows_matched ?? preview.counts?.cf_rows_matched ?? 0}</b>
                  </span>
                  <span>
                    CF unmatched:{" "}
                    <b style={{ color: (preview.foundation?.cf_rows_unmatched ?? preview.counts?.cf_rows_unmatched ?? 0) > 0 ? "#c0392b" : "inherit" }}>
                      {preview.foundation?.cf_rows_unmatched ?? preview.counts?.cf_rows_unmatched ?? 0}
                    </b>
                  </span>
                  <span>
                    CF ambiguous:{" "}
                    <b style={{ color: (preview.foundation?.cf_rows_ambiguous ?? preview.counts?.cf_rows_ambiguous ?? 0) > 0 ? "#c0392b" : "inherit" }}>
                      {preview.foundation?.cf_rows_ambiguous ?? preview.counts?.cf_rows_ambiguous ?? 0}
                    </b>
                  </span>
                </div>
              </label>

              {runtimeCsv && (
                <label className="span-2" style={{ cursor: "default" }}>
                  <span style={{ fontWeight: 600 }}>Solver runtime CSV</span>
                  <div style={{ display: "grid", gap: 4, fontSize: 12, color: "#496675" }}>
                    <div>
                      runtime artifact: <b>{runtimeCsv.artifact_dir ?? runtimeCsv.output_dir}</b>
                    </div>
                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                      <span>flows: <b>{runtimeCsv.flows_count}</b></span>
                      <span>indicators: <b>{runtimeCsv.indicators_count}</b></span>
                      <span>factors: <b>{runtimeCsv.factors_count}</b></span>
                      <span>matched: <b>{runtimeCsv.cf_matched}</b></span>
                      <span>unmatched: <b>{runtimeCsv.cf_unmatched}</b></span>
                      <span>ambiguous: <b>{runtimeCsv.cf_ambiguous}</b></span>
                      <span>active: <b>{runtimeCsv.active === false ? "no" : "yes"}</b></span>
                    </div>
                  </div>
                </label>
              )}

              {/* Warnings / Errors */}
              {preview.warnings && preview.warnings.length > 0 && (
                <label className="span-2" style={{ cursor: "default" }}>
                  <span style={{ color: "#e67e22" }}>{t.warnings} ({preview.warnings.length})</span>
                  <div style={{ fontSize: 12, maxHeight: 100, overflowY: "auto", color: "#5a7584" }}>
                    {preview.warnings.slice(0, 10).map((w, i) => (
                      <div key={i}>• {w}</div>
                    ))}
                    {preview.warnings.length > 10 && <div>…</div>}
                  </div>
                </label>
              )}
              {preview.errors && preview.errors.length > 0 && (
                <label className="span-2" style={{ cursor: "default" }}>
                  <span style={{ color: "#c0392b" }}>{t.errors} ({preview.errors.length})</span>
                  <div style={{ fontSize: 12, maxHeight: 100, overflowY: "auto", color: "#5a7584" }}>
                    {preview.errors.slice(0, 10).map((e, i) => (
                      <div key={i}>• {e}</div>
                    ))}
                    {preview.errors.length > 10 && <div>…</div>}
                  </div>
                </label>
              )}
            </>
          )}

          {/* ========== Commit / Done Phase ========== */}
          {phase === "commit" && preview && (
            <label className="span-2" style={{ cursor: "default" }}>
              <span style={{ fontWeight: 600 }}>{t.step3}</span>
              <div style={{ fontSize: 13, color: "#2e5161" }}>
                <div>{zh ? "数据集预览" : "Dataset preview"}: {preview.counts?.datasets ?? 0}</div>
                <div>{t.canCommit}: ✓</div>
                <div style={{ fontSize: 12, color: "#8fa8b5" }}>job_id: {preview.job_id}</div>
              </div>
            </label>
          )}

          {phase === "done" && commitResult && (
            <label className="span-2" style={{ cursor: "default" }}>
              <span style={{ fontWeight: 600 }}>{t.step4}</span>
              <div style={{ display: "grid", gap: 4, fontSize: 13, color: "#2e5161" }}>
                <div style={{ color: "#27ae60", fontWeight: 600 }}>
                  {zh ? "✓ 导入成功" : "✓ Import successful"}
                </div>
                <div>
                  {zh ? "基本流" : "Flows"}:{" "}
                  <b>{commitFlows.new ?? 0}</b>
                  <span style={{ color: "#8fa8b5" }}>
                    {" "}{zh ? "新增" : "new"} / {commitFlows.skipped ?? 0} {zh ? "跳过" : "skipped"} / {commitFlows.error ?? 0} error
                  </span>
                </div>
                <div>
                  {zh ? "过程" : "Processes"}:{" "}
                  <b>{commitProcesses.new ?? 0}</b>
                  <span style={{ color: "#8fa8b5" }}>
                    {" "}{zh ? "新增" : "new"} / {commitProcesses.skipped ?? 0} {zh ? "跳过" : "skipped"} / {commitProcesses.error ?? 0} error
                  </span>
                </div>
                <div>
                  {zh ? "单位" : "Units"}:{" "}
                  <b>{commitUnits.new ?? 0}</b>
                  <span style={{ color: "#8fa8b5" }}>
                    {" "}{zh ? "新增" : "new"} / {commitUnits.skipped ?? 0} {zh ? "跳过" : "skipped"} / {commitUnits.error ?? 0} error
                  </span>
                </div>
                <div style={{ fontSize: 12, color: "#8fa8b5" }}>
                  catalog_target_kind: <b>{commitResult.catalog_target_kind ?? "lci_dataset"}</b>
                </div>
                {runtimeCsv && (
                  <div style={{ display: "grid", gap: 4, marginTop: 6, fontSize: 12, color: "#496675" }}>
                    <div>
                      runtime artifact: <b>{runtimeCsv.artifact_dir ?? runtimeCsv.output_dir}</b>
                    </div>
                    <div>
                      runtime CSV: {runtimeCsv.flows_count} flows / {runtimeCsv.indicators_count} indicators / {runtimeCsv.factors_count} factors
                    </div>
                  </div>
                )}
              </div>
            </label>
          )}
        </div>

        {/* Error display */}
        {errorText && <div className="pm-error">{errorText}</div>}

        {/* Actions */}
        <div className="pm-modal-actions">
          <button type="button" className="pm-ghost-btn" onClick={onClose}>
            {t.close}
          </button>

          {phase === "upload" && (
            <button
              type="button"
              className="pm-primary-btn"
              onClick={handlePreview}
              disabled={busy}
            >
              {busy ? (zh ? "加载中…" : "Loading…") : t.preview}
            </button>
          )}

          {phase === "preview" && (
            <>
              <button
                type="button"
                className="pm-ghost-btn"
                onClick={() => setPhase("upload")}
              >
                {t.back}
              </button>
              <button
                type="button"
                className="pm-ghost-btn"
                onClick={handleGenerateRuntimeCsv}
                disabled={runtimeBusy || busy}
              >
                {runtimeBusy ? "Generating runtime CSV..." : "Generate runtime CSV"}
              </button>
              <button
                type="button"
                className={hasErrors ? "pm-ghost-btn" : "pm-primary-btn"}
                onClick={handleCommit}
                disabled={!canProceedToCommit || hasErrors || busy}
              >
                {busy
                  ? (zh ? "导入中…" : "Importing…")
                  : hasErrors
                    ? t.cantCommit
                    : t.commit}
              </button>
            </>
          )}

          {phase === "commit" && (
            <>
              <button
                type="button"
                className="pm-ghost-btn"
                onClick={() => setPhase("preview")}
              >
                {t.back}
              </button>
              <button
                type="button"
                className="pm-primary-btn"
                onClick={handleCommit}
                disabled={busy}
              >
                {busy ? (zh ? "导入中…" : "Importing…") : t.commit}
              </button>
            </>
          )}

          {phase === "done" && (
            <>
              <label className="span-2" style={{ cursor: "default" }}>
                <span style={{ fontWeight: 600 }}>{zh ? "报告" : "Report"}</span>
                {reportBusy && <span style={{ fontSize: 12, color: "#8fa8b5" }}>{zh ? "加载中…" : "Loading…"}</span>}
                {reportPayload && !reportBusy && (() => {
                  const jobIdStr = typeof reportPayload.job_id === "string" ? reportPayload.job_id : reportJobId;
                  const countsObj = reportPayload.counts && typeof reportPayload.counts === "object" ? reportPayload.counts as Record<string, unknown> : null;
                  const warningsArr = Array.isArray(reportPayload.warnings) ? reportPayload.warnings as string[] : null;
                  const errorsArr = Array.isArray(reportPayload.errors) ? reportPayload.errors as string[] : null;
                  const hasStructuredContent = countsObj || warningsArr || errorsArr;

                  return (
                    <div style={{ fontSize: 12, color: "#496675", maxHeight: 200, overflowY: "auto" }}>
                      <div>
                        {zh ? "报告 job_id" : "Report job_id"}: <b>{jobIdStr}</b>
                      </div>
                      {countsObj && (
                        <div style={{ display: "flex", gap: 12, marginTop: 4, flexWrap: "wrap" }}>
                          {Object.entries(countsObj).map(([k, v]) => (
                            <span key={k}>
                              {k}: <b>{String(v)}</b>
                            </span>
                          ))}
                        </div>
                      )}
                      {warningsArr && warningsArr.length > 0 && (
                        <div style={{ marginTop: 4 }}>
                          <span style={{ color: "#e67e22" }}>{t.warnings}: </span>
                          {warningsArr.slice(0, 5).map((w, i) => (
                            <div key={i} style={{ color: "#5a7584" }}>• {w}</div>
                          ))}
                        </div>
                      )}
                      {errorsArr && errorsArr.length > 0 && (
                        <div style={{ marginTop: 4 }}>
                          <span style={{ color: "#c0392b" }}>{t.errors}: </span>
                          {errorsArr.slice(0, 5).map((e, i) => (
                            <div key={i} style={{ color: "#5a7584" }}>• {e}</div>
                          ))}
                        </div>
                      )}
                      {!hasStructuredContent && (
                        <pre style={{ fontSize: 11, overflowX: "auto", color: "#5a7584" }}>
                          {JSON.stringify(reportPayload, null, 2)}
                        </pre>
                      )}
                    </div>
                  );
                })()}
                {!reportPayload && !reportBusy && (
                  <span style={{ fontSize: 12, color: "#8fa8b5" }}>{zh ? "尚未加载报告" : "Report not loaded yet"}</span>
                )}
              </label>
            </>
          )}

          {phase === "done" && (
            <>
              <button
                type="button"
                className="pm-ghost-btn"
                onClick={handleViewReport}
                disabled={reportBusy}
              >
                {reportBusy
                  ? (zh ? "加载中…" : "Loading…")
                  : t.viewReport}
              </button>
              <button
                type="button"
                className="pm-ghost-btn"
                onClick={handleGenerateRuntimeCsv}
                disabled={runtimeBusy}
              >
                {runtimeBusy ? "Generating runtime CSV..." : "Generate runtime CSV"}
              </button>
              <button
                type="button"
                className="pm-primary-btn"
                onClick={onClose}
              >
                {t.done}
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
