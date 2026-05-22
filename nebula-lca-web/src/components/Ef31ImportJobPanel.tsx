/**
 * EF3.1 resumable import panel: chunked upload, background import job,
 * checkpoint resume, pause/cancel, and LCIA runtime generation.
 */

import { useCallback, useEffect, useRef, useState } from "react";

interface UploadSession {
  upload_id: string;
  chunk_size: number;
  uploaded_chunks: number[];
}

interface JobStatus {
  job_id: string;
  file_path: string;
  file_type: string;
  phase: string;
  progress_pct: number;
  workers: number;
  limit: number | null;
  status: string;
  error_summary: string | null;
  stats: Record<string, unknown> | null;
  failed_datasets: string[];
  skipped_global: number;
  overwrite_existing: boolean;
  created_at: string;
  updated_at: string;
}

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;
const RAW_IMPORT_API_BASE = ((import.meta.env.VITE_IMPORT_API_BASE_URL as string | undefined) ?? "").replace(/\/$/, "");
const IMPORT_API_BASE = RAW_IMPORT_API_BASE
  ? (RAW_IMPORT_API_BASE.endsWith("/api") ? RAW_IMPORT_API_BASE : `${RAW_IMPORT_API_BASE}/api`)
  : (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1") && window.location.port === "5173"
    ? "http://127.0.0.1:8001/api"
    : API_BASE;
const DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024;
const STATUS_POLL_INTERVAL_MS = 2000;
const CHUNK_UPLOAD_RETRY_LIMIT = 5;

const zhText = {
  title: "\u5bfc\u5165 LCI/LCIA \u6570\u636e\u5e93",
  upload: "\u4e0a\u4f20\u6587\u4ef6",
  createJob: "\u521b\u5efa\u5bfc\u5165\u4efb\u52a1",
  done: "\u5b8c\u6210",
  chooseFile: "\u9009\u62e9\u6587\u4ef6",
  packageLabel: "LCI/LCIA \u6570\u636e\u5305\uff08.7z / .xlsx\uff09",
  packagePlaceholder: "\u8bf7\u9009\u62e9 .7z \u6216 .xlsx \u6587\u4ef6",
  advanced: "\u9ad8\u7ea7\u8bbe\u7f6e",
  workers: "\u89e3\u6790\u5e76\u53d1\u6570",
  limit: "\u5bfc\u5165\u524d N \u4e2a\u6570\u636e\u96c6\uff08\u7559\u7a7a = \u5168\u91cf\uff09",
  full: "\u5168\u91cf",
  start: "\u5f00\u59cb\u5bfc\u5165",
  uploading: "\u4e0a\u4f20\u4e2d...",
  importing: "\u5bfc\u5165\u4e2d",
  pause: "\u6682\u505c",
  resume: "\u7ee7\u7eed",
  cancel: "\u53d6\u6d88",
  retry: "\u91cd\u8bd5\u5931\u8d25",
  retrying: "\u91cd\u8bd5\u4e2d...",
  close: "\u5173\u95ed",
  generateRuntime: "\u751f\u6210 LCIA Runtime",
  importComplete: "\u5bfc\u5165\u5b8c\u6210",
  selectFile: "\u8bf7\u9009\u62e9\u6587\u4ef6",
  processed: "\u5df2\u5904\u7406",
  failedFiles: "\u5931\u8d25\u6587\u4ef6",
  status: "\u72b6\u6001",
  processes: "\u8fc7\u7a0b",
  new: "\u65b0\u589e",
  skipped: "\u8df3\u8fc7",
  vectors: "\u5411\u91cf",
  nnz: "\u975e\u96f6\u9879",
  duration: "\u8017\u65f6",
  failed: "\u5931\u8d25",
  overwriteExisting: "\u8986\u76d6\u5df2\u5bfc\u5168 dataset",
  skippedGlobal: "\u5168\u5c40\u8df3\u8fc7",
};

const enText = {
  title: "Import LCI/LCIA Database",
  upload: "Upload File",
  createJob: "Create Import Job",
  done: "Done",
  chooseFile: "Browse",
  packageLabel: "LCI/LCIA Package (.7z / .xlsx)",
  packagePlaceholder: "Choose .7z or .xlsx file",
  advanced: "Advanced",
  workers: "Parser Workers",
  limit: "Import first N datasets (empty = full)",
  full: "full",
  start: "Start Import",
  uploading: "Uploading...",
  importing: "Importing",
  pause: "Pause",
  resume: "Resume",
  cancel: "Cancel",
  retry: "Retry Failed",
  retrying: "Retrying...",
  close: "Close",
  generateRuntime: "Generate LCIA Runtime",
  importComplete: "Import complete",
  selectFile: "Please select a file",
  processed: "Processed",
  failedFiles: "Failed files",
  status: "Status",
  processes: "Processes",
  new: "new",
  skipped: "skipped",
  vectors: "Vectors",
  nnz: "nnz",
  duration: "Duration",
  failed: "Failed",
  overwriteExisting: "Overwrite already imported datasets",
  skippedGlobal: "Global skipped",
};

async function requestJson<T>(url: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(url, init);
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    throw new Error(err.detail?.message ?? err.message ?? `HTTP ${resp.status}`);
  }
  return resp.json();
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function requestJsonWithRetry<T>(url: string, init: RequestInit | undefined, retries: number): Promise<T> {
  let lastError: unknown = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return await requestJson<T>(url, init);
    } catch (error) {
      lastError = error;
      if (attempt >= retries) break;
      await delay(Math.min(12000, 800 * 2 ** attempt));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function estimateTimeRemaining(progressPct: number, elapsedSeconds: number): string {
  if (progressPct <= 0 || progressPct >= 100) return "-";
  const totalSeconds = (elapsedSeconds / progressPct) * 100;
  const remaining = Math.max(0, totalSeconds - elapsedSeconds);
  const mins = Math.floor(remaining / 60);
  const secs = Math.floor(remaining % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

export default function Ef31ImportJobPanel(props: {
  open: boolean;
  uiLanguage: "zh" | "en";
  onClose: () => void;
}) {
  const { open, uiLanguage, onClose } = props;
  const t = uiLanguage === "zh" ? zhText : enText;

  const [phase, setPhase] = useState<"upload" | "job" | "done">("upload");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [fileType, setFileType] = useState<"lci" | "lcia">("lci");
  const [uploadBusy, setUploadBusy] = useState(false);
  const [jobBusy, setJobBusy] = useState(false);
  const [workers, setWorkers] = useState(4);
  const [limit, setLimit] = useState<number | null>(100);
  const [overwriteExisting, setOverwriteExisting] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [uploadProgress, setUploadProgress] = useState({ current: 0, total: 0 });
  const [uploadSession, setUploadSession] = useState<UploadSession | null>(null);
  const [job, setJob] = useState<JobStatus | null>(null);
  const [errorText, setErrorText] = useState("");
  const pollingRef = useRef<number | null>(null);
  const startTimeRef = useRef<number | null>(null);

  const stopPolling = useCallback(() => {
    if (pollingRef.current !== null) {
      window.clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  }, []);

  const startPolling = useCallback((jobId: string) => {
    stopPolling();
    pollingRef.current = window.setInterval(async () => {
      try {
        const next = await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(jobId)}`);
        setJob(next);
        if (["completed", "failed", "cancelled"].includes(next.status)) {
          stopPolling();
          if (next.status === "completed") setPhase("done");
        }
      } catch {
        // Keep polling; transient network errors should not lose the job state.
      }
    }, STATUS_POLL_INTERVAL_MS);
  }, [stopPolling]);

  useEffect(() => stopPolling, [stopPolling]);

  useEffect(() => {
    setUploadSession(null);
    setUploadProgress({ current: 0, total: 0 });
  }, [selectedFile]);

  const startUpload = useCallback(async () => {
    if (!selectedFile) {
      setErrorText(t.selectFile);
      return;
    }
    setErrorText("");
    setUploadBusy(true);
    setUploadProgress({ current: 0, total: 0 });
    try {
      let session = uploadSession;
      if (!session) {
        session = await requestJson<UploadSession>(
          `${IMPORT_API_BASE}/import/ef31/upload-session?file_name=${encodeURIComponent(selectedFile.name)}&file_type=${fileType}&expected_size=${selectedFile.size}`,
          { method: "POST" },
        );
        setUploadSession(session);
      } else {
        session = await requestJson<UploadSession>(
          `${IMPORT_API_BASE}/import/ef31/upload-session/${encodeURIComponent(session.upload_id)}`,
        );
        setUploadSession(session);
      }
      const chunkSize = session.chunk_size || DEFAULT_CHUNK_SIZE;
      const totalChunks = Math.ceil(selectedFile.size / chunkSize);
      const uploaded = new Set(session.uploaded_chunks ?? []);
      setUploadProgress({ current: uploaded.size, total: totalChunks });

      for (let i = 0; i < totalChunks; i += 1) {
        if (uploaded.has(i)) continue;
        const formData = new FormData();
        formData.append("file", selectedFile.slice(i * chunkSize, Math.min(selectedFile.size, (i + 1) * chunkSize)));
        let chunkResult: { uploaded_chunks?: number[] };
        try {
          chunkResult = await requestJsonWithRetry<{ uploaded_chunks?: number[] }>(
            `${IMPORT_API_BASE}/import/ef31/upload-session/${session.upload_id}/chunks/${i}`,
            {
              method: "PUT",
              body: formData,
            },
            CHUNK_UPLOAD_RETRY_LIMIT,
          );
        } catch (error) {
          throw new Error(`Chunk ${i + 1}/${totalChunks} upload failed after retry: ${error instanceof Error ? error.message : String(error)}`);
        }
        uploaded.add(i);
        if (Array.isArray(chunkResult.uploaded_chunks)) {
          chunkResult.uploaded_chunks.forEach((idx) => uploaded.add(idx));
        }
        setUploadProgress({ current: uploaded.size, total: totalChunks });
      }

      const completed = await requestJson<{ file_path: string }>(
        `${IMPORT_API_BASE}/import/ef31/upload-session/${session.upload_id}/complete?total_chunks=${totalChunks}&file_type=${fileType}`,
        { method: "POST" },
      );
      const newJob = await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_path: completed.file_path, file_type: fileType, workers, limit, overwrite_existing: overwriteExisting }),
      });
      const started = await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(newJob.job_id)}/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ resume_from_failed: false }),
      });
      setUploadSession(null);
      startTimeRef.current = Date.now();
      setJob(started);
      setPhase("job");
      startPolling(started.job_id);
    } catch (err) {
      setErrorText(err instanceof Error ? err.message : String(err));
    } finally {
      setUploadBusy(false);
    }
  }, [fileType, limit, overwriteExisting, selectedFile, startPolling, t.selectFile, uploadSession, workers]);

  const pause = useCallback(async () => {
    if (!job) return;
    await requestJson(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(job.job_id)}/pause`, { method: "POST" });
    setJob(await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(job.job_id)}`));
  }, [job]);

  const resume = useCallback(async () => {
    if (!job) return;
    const started = await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(job.job_id)}/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ resume_from_failed: false }),
    });
    setJob(started);
    startPolling(started.job_id);
  }, [job, startPolling]);

  const cancel = useCallback(async () => {
    if (!job) return;
    try {
      await requestJson(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(job.job_id)}/cancel`, { method: "POST" });
    } catch {
      // Ignore: signal file already written, background thread will pick it up
    }
    stopPolling();
    try {
      setJob(await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(job.job_id)}`));
    } catch {
      // Keep current state
    }
  }, [job, stopPolling]);

  const retryFailed = useCallback(async () => {
    if (!job) return;
    setJobBusy(true);
    try {
      const started = await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(job.job_id)}/retry-failed`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ resume_from_failed: true }),
      });
      setJob(started);
      setPhase("job");
      startPolling(started.job_id);
    } finally {
      setJobBusy(false);
    }
  }, [job, startPolling]);

  const generateRuntime = useCallback(async () => {
    if (!job) return;
    setErrorText("");
    try {
      await requestJson(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(job.job_id)}/lcia-runtime`, { method: "POST" });
    } catch (err) {
      setErrorText(err instanceof Error ? err.message : String(err));
    }
  }, [job]);

  if (!open) return null;

  const elapsedSeconds = startTimeRef.current ? (Date.now() - startTimeRef.current) / 1000 : 0;
  const stats = job?.stats ?? {};
  const processedCount = Number(stats.processes_inserted ?? 0) + Number(stats.processes_skipped ?? 0) + Number(stats.processes_failed ?? 0);
  const phaseItems = [t.upload, t.createJob, t.done];
  const phaseIndex = ["upload", "job", "done"].indexOf(phase);

  return (
    <div className="pm-modal-mask" onClick={(event) => event.stopPropagation()}>
      <div className="pm-modal pm-tidas-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{t.title}</strong>
          <button type="button" className="pm-link-btn" onClick={onClose}>{t.close}</button>
        </div>

        <div style={{ padding: "12px 14px 0", display: "flex", gap: 6, flexWrap: "wrap" }}>
          {phaseItems.map((label, index) => (
            <span
              key={label}
              style={{
                fontSize: 12,
                fontWeight: index <= phaseIndex ? 600 : 400,
                color: index <= phaseIndex ? "#0b66c5" : "#8fa8b5",
                padding: "4px 10px",
                borderRadius: 999,
                border: `1px solid ${index <= phaseIndex ? "#0d83ff" : "#c5d4dd"}`,
                background: index <= phaseIndex ? "#e6f2ff" : "#f5f9fc",
              }}
            >
              {index + 1}. {label}
            </span>
          ))}
        </div>

        <div className="pm-modal-grid">
          {phase === "upload" && (
            <>
              <label className="span-2">
                <span>{t.packageLabel}</span>
                <div className="pm-file-picker-row">
                  <input className="pm-file-picker-display" value={selectedFile?.name ?? ""} readOnly placeholder={t.packagePlaceholder} />
                  <button type="button" className="pm-file-picker-btn" onClick={() => document.getElementById("ef31-import-file-input")?.click()}>{t.chooseFile}</button>
                  <input
                    id="ef31-import-file-input"
                    type="file"
                    accept=".7z,.xlsx,application/x-7z-compressed,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    style={{ display: "none" }}
                    onChange={(event) => {
                      const file = event.target.files?.[0] ?? null;
                      setSelectedFile(file);
                      setFileType(file?.name.toLowerCase().endsWith(".xlsx") ? "lcia" : "lci");
                      setErrorText("");
                    }}
                  />
                </div>
              </label>
              <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 12, color: "#496675" }}>{selectedFile ? formatBytes(selectedFile.size) : ""}</span>
              </label>
              <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <button type="button" className="pm-link-btn" onClick={() => setShowAdvanced(!showAdvanced)}>
                  {t.advanced} {showAdvanced ? "?" : "?"}
                </button>
              </label>
              {showAdvanced && (
                <>
                  <label>
                    <span style={{ fontSize: 12 }}>{t.workers}</span>
                    <select value={workers} onChange={(event) => setWorkers(Number(event.target.value))}>
                      <option value={1}>1</option>
                      <option value={2}>2</option>
                      <option value={4}>4</option>
                      <option value={8}>8</option>
                    </select>
                  </label>
                  <label>
                    <span style={{ fontSize: 12 }}>{t.limit}</span>
                    <input type="number" min={1} value={limit ?? ""} placeholder={t.full} onChange={(event) => setLimit(event.target.value ? Number(event.target.value) : null)} />
                  </label>
                  <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <input type="checkbox" checked={overwriteExisting} onChange={(event) => setOverwriteExisting(event.target.checked)} />
                    <span style={{ fontSize: 12, color: "#c0392b" }}>{t.overwriteExisting}</span>
                  </label>
                </>
              )}
            </>
          )}

          {uploadBusy && uploadProgress.total > 0 && (
            <label className="span-2" style={{ cursor: "default" }}>
              <span style={{ fontWeight: 600, fontSize: 13 }}>{`${uploadProgress.current}/${uploadProgress.total}`}</span>
              <div style={{ height: 6, background: "#e5e7eb", borderRadius: 3, marginTop: 4 }}>
                <div style={{ height: "100%", width: `${(uploadProgress.current / uploadProgress.total) * 100}%`, background: "#0b66c5", borderRadius: 3 }} />
              </div>
            </label>
          )}

          {phase === "job" && job && (
            <label className="span-2" style={{ cursor: "default" }}>
              <span style={{ fontWeight: 600, fontSize: 13 }}>{job.status === "running" ? t.importing : job.status}</span>
              <div style={{ fontSize: 12, color: "#496675", marginTop: 4 }}>{`${Math.round(job.progress_pct)}%`}</div>
              <div style={{ height: 8, background: "#e5e7eb", borderRadius: 4, marginTop: 6 }}>
                <div style={{ height: "100%", width: `${Math.min(100, job.progress_pct)}%`, background: job.status === "failed" ? "#c0392b" : "#27ae60", borderRadius: 4 }} />
              </div>
              <div style={{ display: "flex", gap: 16, marginTop: 8, fontSize: 12, flexWrap: "wrap" }}>
                <span>{t.processed}: <b>{processedCount}</b></span>
                <span>{t.skippedGlobal}: <b>{job.skipped_global ?? 0}</b></span>
                <span>{t.vectors}: <b>{Number(stats.vectors_written ?? 0)}</b></span>
                <span>{t.failed}: <b>{Number(stats.processes_failed ?? 0)}</b></span>
                {(job.status === "running" || job.status === "paused") && <span>ETA {estimateTimeRemaining(job.progress_pct, elapsedSeconds)}</span>}
              </div>
              {job.failed_datasets?.length > 0 && (
                <div style={{ marginTop: 6, fontSize: 11, color: "#c0392b" }}>
                  {t.failedFiles}: {job.failed_datasets.slice(0, 5).join(", ")}{job.failed_datasets.length > 5 ? " ..." : ""}
                </div>
              )}
              {job.error_summary && <div style={{ marginTop: 4, fontSize: 11, color: "#c0392b" }}>{job.error_summary}</div>}
            </label>
          )}

          {phase === "done" && job && (
            <label className="span-2" style={{ cursor: "default" }}>
              <div style={{ color: "#27ae60", fontWeight: 600, fontSize: 14 }}>? {t.importComplete}</div>
              <div style={{ display: "grid", gap: 4, fontSize: 12, color: "#496675", marginTop: 8 }}>
                <div>{t.status}: <b>{job.status}</b></div>
                <div>{t.processes}: <b>{Number(stats.processes_inserted ?? 0)}</b> {t.new} / <b>{Number(stats.processes_skipped ?? 0)}</b> {t.skipped}</div>
                <div>{t.skippedGlobal}: <b>{job.skipped_global ?? 0}</b></div>
                <div>{t.vectors}: <b>{Number(stats.vectors_written ?? 0)}</b> ({t.nnz}) <b>{Number(stats.vector_nnz_total ?? 0)}</b></div>
                <div>{t.duration}: <b>{Number(stats.duration_seconds ?? 0).toFixed(1)}s</b></div>
                {job.failed_datasets?.length > 0 && <div style={{ color: "#e67e22" }}>{t.failed}: {job.failed_datasets.length}</div>}
              </div>
            </label>
          )}
        </div>

        {errorText && <div className="pm-error">{errorText}</div>}

        <div className="pm-modal-actions">
          <button type="button" className="pm-ghost-btn" onClick={onClose}>{t.close}</button>
          {phase === "upload" && (
            <button type="button" className="pm-primary-btn" onClick={startUpload} disabled={uploadBusy || !selectedFile}>
              {uploadBusy ? t.uploading : t.start}
            </button>
          )}
          {phase === "job" && job && (
            <>
              {job.status === "running" && <button type="button" className="pm-ghost-btn" onClick={pause}>{t.pause}</button>}
              {job.status === "paused" && <button type="button" className="pm-primary-btn" onClick={resume}>{t.resume}</button>}
              {(job.status === "running" || job.status === "paused") && <button type="button" className="pm-ghost-btn" onClick={cancel} style={{ color: "#c0392b" }}>{t.cancel}</button>}
              {job.status === "failed" && <button type="button" className="pm-primary-btn" onClick={retryFailed} disabled={jobBusy}>{jobBusy ? t.retrying : t.retry}</button>}
            </>
          )}
          {phase === "done" && job && (
            <button type="button" className="pm-ghost-btn" onClick={generateRuntime}>{t.generateRuntime}</button>
          )}
        </div>
      </div>
    </div>
  );
}
