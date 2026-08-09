import { getApiBase, getImportApiBase } from "../apiBase";
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

interface JobListResponse {
  jobs: JobStatus[];
  total: number;
}

interface LciaMethodStatus {
  ecoinvent_lcia_runtime_available?: boolean;
  ecoinvent_elementary_flow_count?: number;
  ecoinvent_lci_vector_count?: number;
}

type ImportFileType = "lci" | "lcia" | "masterdata";

const API_BASE = getApiBase();
const IMPORT_API_BASE = getImportApiBase();
const DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024;
const DEFAULT_IMPORT_WORKERS = 8;
const STATUS_POLL_INTERVAL_MS = 2000;
const CHUNK_UPLOAD_RETRY_LIMIT = 5;
const STALE_THRESHOLD_MS = 10 * 60 * 1000; // 10 minutes

function parseJobUpdatedAtMs(updatedAt: string | null | undefined): number {
  if (!updatedAt) return 0;
  const normalized = /(?:Z|[+-]\d{2}:?\d{2})$/.test(updatedAt) ? updatedAt : `${updatedAt}Z`;
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

const zhText = {
  title: "\u5bfc\u5165 LCI/LCIA \u6570\u636e\u5e93",
  upload: "\u4e0a\u4f20\u6587\u4ef6",
  createJob: "\u521b\u5efa\u5bfc\u5165\u4efb\u52a1",
  done: "\u5b8c\u6210",
  chooseFile: "\u9009\u62e9\u6587\u4ef6",
  packageLabel: "LCI/LCIA \u6570\u636e\u5305\uff08.7z / .xlsx\uff09",
  packagePlaceholder: "\u8bf7\u9009\u62e9 .7z \u6216 .xlsx \u6587\u4ef6",
  guide: "\u6570\u636e\u8d2d\u4e70\u4e0e\u4e0b\u8f7d\u6307\u5357",
  guideTitle: "ecoinvent \u6570\u636e\u5e93\u8d2d\u4e70\u4e0e\u4e0b\u8f7d",
  guideIntro: "请先购买 ecoinvent 授权，并从 ecoinvent 官网进入许可证与 ecoQuery 下载入口。",
  guideEnglish: "\u82f1\u6587\u5b98\u65b9\u9875\u9762",
  guidePackages: "\u9700\u8981\u4e0b\u8f7d\u7684\u4e24\u4e2a\u538b\u7f29\u5305",
  guideNote: "购买后进入下载区，通常会看到不同版本号与不同产品系统模型的压缩包。LCA 实务中一般推荐使用 cutoff 模型；实际文件名前面可能带有版本前缀，例如 ecoinvent 3.x。只要文件名结尾分别匹配 cutoff_lci_ecoSpold02.7z 与 LCIA_implementation.7z 即可。导入时先上传 LCI 包，LCIA 包用于生成 LCIA Runtime；请保留原始压缩包，不要先解压再上传。",
  advanced: "\u9ad8\u7ea7\u8bbe\u7f6e",
  hideAdvanced: "\u6536\u8d77\u9ad8\u7ea7\u8bbe\u7f6e",
  workers: "\u89e3\u6790\u5e76\u53d1\u6570",
  limit: "\u5bfc\u5165\u524d N \u4e2a\u6570\u636e\u96c6\uff08\u7559\u7a7a = \u5168\u91cf\uff09",
  fullImportHint: "完整使用需分别导入两个压缩包：先导入 LCI 包（cutoff_lci_ecoSpold02.7z）建立过程/清单数据库，再导入 LCIA 包（LCIA_implementation.7z）生成 LCIA Runtime。默认全量导入，8 并发解析；常见 Docker/开发机预估约 1 小时。可在高级设置中做分批导入、覆盖导入；上传与导入任务支持断点续传/续跑。",
  full: "\u5168\u91cf",
  fileSize: "\u6587\u4ef6\u5927\u5c0f",
  dataType: "\u6570\u636e\u7c7b\u578b",
  lciType: "LCI \u6570\u636e\u5305",
  lciaType: "LCIA \u56e0\u5b50\u5305",
  masterdataType: "MasterData \u5237\u65b0",
  masterdataHint: "\u53ea\u5237\u65b0 units\u3001elementary flows\u3001intermediate flows\uff0c\u4e0d\u5bfc\u5165 LCI \u8fc7\u7a0b\u6216\u5411\u91cf\u3002",
  masterdataRequiredTitle: "\u9700\u5148\u5bfc\u5165 MasterData",
  masterdataRequiredBody: "\u5f53\u524d\u672a\u68c0\u6d4b\u5230 ecoinvent MasterData \u76ee\u5f55\uff0c\u8bf7\u5148\u7528\u540c\u4e00\u4e2a LCI .7z \u5305\u6267\u884c MasterData \u5237\u65b0\u3002\u5b8c\u6210\u540e\u53ef\u518d\u5bfc\u5165 LCI/LCIA\uff0c\u540e\u7eed LCI \u4efb\u52a1\u4f1a\u590d\u7528\u5df2\u6709 MasterData\u3002",
  masterdataAvailable: "\u5df2\u68c0\u6d4b\u5230 MasterData\uff0cLCI \u5bfc\u5165\u53ef\u76f4\u63a5\u8df3\u8fc7\u76ee\u5f55\u5237\u65b0\u3002",
  masterdataStats: "MasterData \u7edf\u8ba1",
  masterdataReady: "MasterData 已刷新，可继续导入 LCI/LCIA 数据包。",
  units: "单位",
  unitGroups: "单位组",
  cacheInvalidated: "缓存已刷新",
  sqlitePragmas: "SQLite 写入参数",
  elementaryFlows: "\u57fa\u672c\u6d41",
  intermediateFlows: "\u4e2d\u95f4\u6d41",
  importTiming: "\u5bfc\u5165\u6027\u80fd",
  masterdataReused: "MasterData \u590d\u7528",
  avgBatchSize: "\u5e73\u5747\u6279\u5927\u5c0f",
  dbUpsert: "DB \u5199\u5165",
  globalFastSkip: "\u5168\u5c40\u5feb\u901f\u8df3\u8fc7",
  overwriteHint: "\u91cd\u65b0\u5bfc\u5165\u5df2\u5b58\u5728\u7684 dataset\uff0c\u7528\u4e8e\u4fee\u590d\u65e7\u5411\u91cf\u6216\u7248\u672c\u66f4\u65b0\u3002",
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
  lciaMissingWarningTitle: "已导入 ecoinvent LCI/基本流，但 LCIA Runtime 不可用",
  lciaMissingWarningBody: "请继续上传 LCIA_implementation.7z 并完成导入。否则 ecoinvent LCI 或 ecoinvent 基本流无法进行 LCIA 计算。",
  importComplete: "\u5bfc\u5165\u5b8c\u6210",
  selectFile: "\u8bf7\u9009\u62e9\u6587\u4ef6",
  processed: "\u5df2\u5904\u7406",
  failedFiles: "\u5931\u8d25\u6587\u4ef6",
  status: "\u72b6\u6001",
  processes: "\u8fc7\u7a0b",
  new: "\u65b0\u589e",
  updated: "\u66f4\u65b0",
  skipped: "\u8df3\u8fc7",
  vectors: "\u5411\u91cf",
  indicators: "LCIA \u6307\u6807",
  factors: "LCIA \u56e0\u5b50",
  matched: "CF \u5339\u914d",
  unmatched: "CF \u672a\u5339\u914d",
  reused: "\u590d\u7528",
  emptyVectors: "\u7a7a\u5411\u91cf",
  nnz: "\u975e\u96f6\u9879",
  duration: "\u8017\u65f6",
  failed: "\u5931\u8d25",
  overwriteExisting: "\u8986\u76d6\u5df2\u5bfc\u5165\u6570\u636e\u96c6",
  skippedGlobal: "\u5168\u5c40\u8df3\u8fc7",
  phase: "\u9636\u6bb5",
};

const enText = {
  title: "Import LCI/LCIA Database",
  upload: "Upload File",
  createJob: "Create Import Job",
  done: "Done",
  chooseFile: "Browse",
  packageLabel: "LCI/LCIA Package (.7z / .xlsx)",
  packagePlaceholder: "Choose .7z or .xlsx file",
  guide: "Purchase and download guide",
  guideTitle: "ecoinvent purchase and download",
  guideIntro: "Purchase an ecoinvent license first, then use the ecoinvent website for license and ecoQuery access.",
  guideEnglish: "English official page",
  guidePackages: "Required archive files",
  guideNote: "After purchase, the download area may contain archives for multiple versions and product system models. In LCA practice, the cutoff model is generally recommended. The actual archive names may include a version prefix such as ecoinvent 3.x; what matters is that the names end with cutoff_lci_ecoSpold02.7z and LCIA_implementation.7z. Upload the LCI archive first; the LCIA archive is used to generate the LCIA Runtime. Keep the original archives and do not unzip before upload.",
  advanced: "Advanced",
  hideAdvanced: "Hide advanced",
  workers: "Parser Workers",
  limit: "Import first N datasets (empty = full)",
  fullImportHint: "Full functionality requires importing two archives: import the LCI archive (cutoff_lci_ecoSpold02.7z) first to build the process/inventory database, then import the LCIA archive (LCIA_implementation.7z) to generate the LCIA Runtime. Default is full import with 8 parser workers; expect about 1 hour on a typical Docker or development machine. Advanced settings support batched import and overwrite import; upload and import jobs are resumable.",
  full: "full",
  fileSize: "File size",
  dataType: "Data type",
  lciType: "LCI package",
  lciaType: "LCIA factor package",
  masterdataType: "MasterData refresh",
  masterdataHint: "Refresh units, elementary flows, and intermediate flows only. Does not import LCI processes or vectors.",
  masterdataRequiredTitle: "Import MasterData first",
  masterdataRequiredBody: "No ecoinvent MasterData catalog is available. Run MasterData refresh first using the same LCI .7z package. After it completes, LCI/LCIA import can proceed and reuse the existing MasterData.",
  masterdataAvailable: "MasterData is available. LCI imports can skip catalog refresh.",
  masterdataStats: "MasterData stats",
  masterdataReady: "MasterData refreshed. LCI/LCIA imports are now available.",
  units: "Units",
  unitGroups: "Unit groups",
  cacheInvalidated: "Cache refreshed",
  sqlitePragmas: "SQLite write settings",
  elementaryFlows: "Elementary flows",
  intermediateFlows: "Intermediate flows",
  importTiming: "Import performance",
  masterdataReused: "MasterData reused",
  avgBatchSize: "Average batch size",
  dbUpsert: "DB upsert",
  globalFastSkip: "Global fast skip",
  overwriteHint: "Re-import existing datasets. Use this to repair old vectors or refresh a package version.",
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
  lciaMissingWarningTitle: "Ecoinvent LCI/elementary flows are imported, but the LCIA Runtime is unavailable",
  lciaMissingWarningBody: "Upload and import LCIA_implementation.7z next. Otherwise ecoinvent LCI datasets or ecoinvent elementary flows cannot be characterized.",
  importComplete: "Import complete",
  selectFile: "Please select a file",
  processed: "Processed",
  failedFiles: "Failed files",
  status: "Status",
  processes: "Processes",
  new: "new",
  updated: "updated",
  skipped: "skipped",
  vectors: "Vectors",
  indicators: "LCIA indicators",
  factors: "LCIA factors",
  matched: "CF matched",
  unmatched: "CF unmatched",
  reused: "reused",
  emptyVectors: "empty vectors",
  nnz: "nnz",
  duration: "Duration",
  failed: "Failed",
  overwriteExisting: "Overwrite already imported datasets",
  skippedGlobal: "Global skipped",
  phase: "Phase",
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

function formatStatValue(value: unknown): string {
  if (value === true) return "yes";
  if (value === false) return "no";
  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : value.toFixed(3);
  }
  if (value == null || value === "") return "-";
  return String(value);
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
  const [fileType, setFileType] = useState<ImportFileType>("lci");
  const [uploadBusy, setUploadBusy] = useState(false);
  const [jobBusy, setJobBusy] = useState(false);
  const [workers, setWorkers] = useState(DEFAULT_IMPORT_WORKERS);
  const [limit, setLimit] = useState<number | null>(null);
  const [overwriteExisting, setOverwriteExisting] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showGuide, setShowGuide] = useState(false);
  const [uploadProgress, setUploadProgress] = useState({ current: 0, total: 0 });
  const [uploadSession, setUploadSession] = useState<UploadSession | null>(null);
  const [job, setJob] = useState<JobStatus | null>(null);
  const [errorText, setErrorText] = useState("");
  const [lciaStatus, setLciaStatus] = useState<LciaMethodStatus | null>(null);
  const pollingRef = useRef<number | null>(null);
  const startTimeRef = useRef<number | null>(null);

  const stopPolling = useCallback(() => {
    if (pollingRef.current !== null) {
      window.clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  }, []);

  const resetPanel = useCallback(() => {
    stopPolling();
    setPhase("upload");
    setSelectedFile(null);
    setFileType("lci");
    setUploadBusy(false);
    setJobBusy(false);
    setWorkers(DEFAULT_IMPORT_WORKERS);
    setLimit(null);
    setOverwriteExisting(false);
    setShowAdvanced(false);
    setShowGuide(false);
    setUploadProgress({ current: 0, total: 0 });
    setUploadSession(null);
    setJob(null);
    setErrorText("");
    setLciaStatus(null);
    startTimeRef.current = null;
  }, [stopPolling]);

  const refreshLciaStatus = useCallback(async () => {
    try {
      const status = await requestJson<LciaMethodStatus>(`${API_BASE}/reference/lcia-methods`);
      setLciaStatus(status);
      return status;
    } catch {
      return null;
    }
  }, []);

  const closePanel = useCallback(() => {
    if (phase === "done" || (job && ["completed", "failed", "cancelled"].includes(job.status))) {
      resetPanel();
    }
    onClose();
  }, [job, onClose, phase, resetPanel]);

  const startPolling = useCallback((jobId: string) => {
    stopPolling();
    const pollOnce = async () => {
      try {
        const next = await requestJson<JobStatus>(`${IMPORT_API_BASE}/import/ef31/jobs/${encodeURIComponent(jobId)}`);
        setJob(next);
        if (["completed", "failed", "cancelled"].includes(next.status)) {
          stopPolling();
          setPhase("done");
          if (next.status === "completed") {
            void refreshLciaStatus();
          }
        }
      } catch {
        // Keep polling; transient network errors should not lose the job state.
      }
    };
    void pollOnce();
    pollingRef.current = window.setInterval(pollOnce, STATUS_POLL_INTERVAL_MS);
  }, [refreshLciaStatus, stopPolling]);

  useEffect(() => stopPolling, [stopPolling]);

  useEffect(() => {
    if (!open) return;
    void refreshLciaStatus();
  }, [open, refreshLciaStatus]);

  useEffect(() => {
    if (!open || !lciaStatus || job) return;
    const hasMasterData = Number(lciaStatus.ecoinvent_elementary_flow_count ?? 0) > 0;
    if (!hasMasterData && fileType !== "masterdata") {
      setFileType("masterdata");
      setShowAdvanced(false);
    }
  }, [fileType, job, lciaStatus, open]);

  useEffect(() => {
    if (!open || job || uploadBusy || jobBusy) return;
    let cancelled = false;
    const statuses = ["running", "pending", "paused"];
    const now = Date.now();
    const loadActiveJob = async () => {
      for (const status of statuses) {
        try {
          const result = await requestJson<JobListResponse>(`${IMPORT_API_BASE}/import/ef31/jobs?status=${status}&limit=1`);
          const active = result.jobs[0];
          if (!active || cancelled) continue;
          // Only auto-mount jobs updated within the stale threshold (10 min).
          // Older stale jobs are considered abandoned and left for manual recovery.
          const updatedMs = parseJobUpdatedAtMs(active.updated_at);
          if (updatedMs <= 0 || now - updatedMs > STALE_THRESHOLD_MS) continue;
          setJob(active);
          setPhase("job");
          if (active.status === "running") {
            startTimeRef.current = now;
            startPolling(active.job_id);
          }
          return;
        } catch {
          // Opening the panel should still work if job lookup has a transient error.
        }
      }
    };
    void loadActiveJob();
    return () => {
      cancelled = true;
    };
  }, [job, jobBusy, open, startPolling, uploadBusy]);

  useEffect(() => {
    setUploadSession(null);
    setUploadProgress({ current: 0, total: 0 });
  }, [selectedFile]);

  const startUpload = useCallback(async () => {
    if (!selectedFile) {
      setErrorText(t.selectFile);
      return;
    }
    const mustRefreshMasterData = lciaStatus !== null && Number(lciaStatus.ecoinvent_elementary_flow_count ?? 0) <= 0;
    if (mustRefreshMasterData && fileType !== "masterdata") {
      setFileType("masterdata");
      setShowAdvanced(false);
      setErrorText(t.masterdataRequiredTitle);
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
              method: "POST",
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
  }, [fileType, lciaStatus, limit, overwriteExisting, selectedFile, startPolling, t.masterdataRequiredTitle, t.selectFile, uploadSession, workers]);

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

  if (!open) return null;

  const elapsedSeconds = startTimeRef.current ? (Date.now() - startTimeRef.current) / 1000 : 0;
  const stats = job?.stats ?? {};
  const isMasterDataMode = fileType === "masterdata" || job?.file_type === "masterdata" || stats.masterdata_only === true;
  const timingItems = [
    [t.duration, stats.duration_seconds],
    [t.masterdataReused, stats.masterdata_reused],
    ["parse", stats.avg_parse_ms],
    ["prefetch", stats.batch_prefetch_wall_seconds],
    ["pack", stats.batch_pack_wall_seconds],
    [t.dbUpsert, stats.batch_db_upsert_wall_seconds],
    ["checkpoint", stats.batch_checkpoint_wall_seconds],
    [t.avgBatchSize, stats.avg_batch_size],
    [t.globalFastSkip, stats.global_skip_fast_count],
  ].filter(([, value]) => value !== undefined && value !== null);
  const importPragmas = stats.import_pragmas && typeof stats.import_pragmas === "object" && !Array.isArray(stats.import_pragmas)
    ? Object.entries(stats.import_pragmas as Record<string, unknown>)
    : [];
  const masterDataStatsItems = [
    [t.unitGroups, `${formatStatValue(stats.groups_inserted)} ${t.new}`],
    [t.units, `${formatStatValue(stats.units_inserted)} ${t.new}`],
    [t.elementaryFlows, `${formatStatValue(stats.elementary_inserted)} ${t.new} / ${formatStatValue(stats.elementary_updated)} ${t.updated} / ${formatStatValue(stats.elementary_skipped)} ${t.skipped}`],
    [t.intermediateFlows, `${formatStatValue(stats.intermediate_inserted)} ${t.new} / ${formatStatValue(stats.intermediate_updated)} ${t.updated} / ${formatStatValue(stats.intermediate_skipped)} ${t.skipped}`],
    [t.duration, `${formatStatValue(stats.duration_seconds)}s`],
    [t.cacheInvalidated, formatStatValue(stats.cache_invalidated)],
  ].filter(([, value]) => value !== undefined && value !== null);
  const processedCount = Number(
    stats.datasets_processed
      ?? (
        Number(stats.processes_inserted ?? 0)
        + Number(stats.processes_updated ?? 0)
        + Number(stats.processes_skipped ?? 0)
        + Number(stats.processes_failed ?? 0)
      )
  );
  const phaseItems = [t.upload, t.createJob, t.done];
  const phaseIndex = ["upload", "job", "done"].indexOf(phase);
  const hasImportedEcoContent = Number(lciaStatus?.ecoinvent_elementary_flow_count ?? 0) > 0
    || Number(lciaStatus?.ecoinvent_lci_vector_count ?? 0) > 0;
  const hasMasterData = Number(lciaStatus?.ecoinvent_elementary_flow_count ?? 0) > 0;
  const masterDataStatusKnown = lciaStatus !== null;
  const forceMasterDataRefresh = masterDataStatusKnown && !hasMasterData;
  const shouldPromptLciaRuntime = hasImportedEcoContent && !Boolean(lciaStatus?.ecoinvent_lcia_runtime_available);

  return (
    <div className="pm-modal-mask" onClick={(event) => event.stopPropagation()}>
      <div className="pm-modal pm-tidas-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{t.title}</strong>
          <button type="button" className="pm-link-btn" onClick={closePanel}>{t.close}</button>
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

        {shouldPromptLciaRuntime && (
          <div className="ef31-import-full-hint" style={{ margin: "12px 14px 0", borderColor: "#f1c27d", background: "#fff8ea", color: "#8a5a00" }}>
            <strong>{t.lciaMissingWarningTitle}</strong>
            <div style={{ marginTop: 4 }}>{t.lciaMissingWarningBody}</div>
          </div>
        )}
        {phase === "upload" && (
          <div
            className="ef31-import-full-hint"
            style={{ margin: "12px 14px 0", borderColor: forceMasterDataRefresh ? "#f1c27d" : "#a8d5ba", background: forceMasterDataRefresh ? "#fff8ea" : "#eefaf2", color: forceMasterDataRefresh ? "#8a5a00" : "#216b3a" }}
          >
            <strong>{forceMasterDataRefresh ? t.masterdataRequiredTitle : t.masterdataAvailable}</strong>
            {forceMasterDataRefresh && <div style={{ marginTop: 4 }}>{t.masterdataRequiredBody}</div>}
          </div>
        )}

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
                      const fileName = file?.name.toLowerCase() ?? "";
                      const detectedType: ImportFileType = fileName.includes("masterdata") ? "masterdata" : fileName.endsWith(".xlsx") || fileName.includes("lcia") ? "lcia" : "lci";
                      setFileType(forceMasterDataRefresh ? "masterdata" : detectedType);
                      setErrorText("");
                    }}
                  />
                </div>
              </label>
              <div className="ef31-import-file-meta span-2">
                <span>{t.fileSize}: <b>{selectedFile ? formatBytes(selectedFile.size) : "-"}</b></span>
                <label style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
                  <span>{t.dataType}:</span>
                  <select value={fileType} onChange={(event) => setFileType(event.target.value as ImportFileType)}>
                    <option value="lci" disabled={forceMasterDataRefresh}>{t.lciType}</option>
                    <option value="lcia" disabled={forceMasterDataRefresh}>{t.lciaType}</option>
                    <option value="masterdata">{t.masterdataType}</option>
                  </select>
                </label>
                <button type="button" className="ef31-import-guide-button" onClick={() => setShowGuide(true)}>
                  {t.guide}
                </button>
                {fileType !== "masterdata" && (
                  <button type="button" className="ef31-import-advanced-toggle" onClick={() => setShowAdvanced(!showAdvanced)}>
                    {showAdvanced ? t.hideAdvanced : t.advanced}
                  </button>
                )}
              </div>
              <div className="ef31-import-full-hint span-2">{fileType === "masterdata" ? t.masterdataHint : t.fullImportHint}</div>
              {showAdvanced && fileType !== "masterdata" && (
                <div className="ef31-import-advanced span-2">
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
                  <label className="ef31-import-overwrite">
                    <input type="checkbox" checked={overwriteExisting} onChange={(event) => setOverwriteExisting(event.target.checked)} />
                    <span>
                      <strong>{t.overwriteExisting}</strong>
                      <small>{t.overwriteHint}</small>
                    </span>
                  </label>
                </div>
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
                <span>{t.phase}: <b>{String(stats.phase ?? job.phase ?? "-")}</b></span>
                {isMasterDataMode ? (
                  <>
                    <span>{t.elementaryFlows}: <b>{Number(stats.elementary_inserted ?? 0)}</b> {t.new} / <b>{Number(stats.elementary_updated ?? 0)}</b> {t.updated}</span>
                    <span>{t.intermediateFlows}: <b>{Number(stats.intermediate_inserted ?? 0)}</b> {t.new} / <b>{Number(stats.intermediate_updated ?? 0)}</b> {t.updated}</span>
                    <span>{t.duration}: <b>{formatStatValue(stats.duration_seconds)}s</b></span>
                  </>
                ) : (
                  <>
                    <span>{t.processed}: <b>{processedCount}</b></span>
                    <span>{t.updated}: <b>{Number(stats.processes_updated ?? 0)}</b></span>
                    <span>{t.skippedGlobal}: <b>{job.skipped_global ?? 0}</b></span>
                    <span>{t.vectors}: <b>{Number(stats.vectors_written ?? 0)}</b></span>
                    <span>{t.reused}: <b>{Number(stats.vectors_reused ?? 0)}</b></span>
                    <span>{t.emptyVectors}: <b>{Number(stats.empty_vectors ?? 0)}</b></span>
                    <span>{t.failed}: <b>{Number(stats.processes_failed ?? 0)}</b></span>
                  </>
                )}
                {(job.status === "running" || job.status === "paused") && <span>ETA {estimateTimeRemaining(job.progress_pct, elapsedSeconds)}</span>}
              </div>
              {timingItems.length > 0 && (
                <div style={{ display: "flex", gap: 12, marginTop: 8, fontSize: 11, flexWrap: "wrap", color: "#607d8b" }}>
                  <strong>{t.importTiming}</strong>
                  {timingItems.map(([label, value]) => (
                    <span key={String(label)}>{String(label)}: <b>{formatStatValue(value)}</b></span>
                  ))}
                </div>
              )}
              {importPragmas.length > 0 && (
                <div style={{ display: "flex", gap: 12, marginTop: 8, fontSize: 11, flexWrap: "wrap", color: "#607d8b" }}>
                  <strong>{t.sqlitePragmas}</strong>
                  {importPragmas.map(([label, value]) => (
                    <span key={label}>{label}: <b>{formatStatValue(value)}</b></span>
                  ))}
                </div>
              )}
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
              <div style={{ color: job.status === "completed" ? "#27ae60" : "#c0392b", fontWeight: 600, fontSize: 14 }}>
                {job.status === "completed" ? t.importComplete : job.status}
              </div>
              <div style={{ display: "grid", gap: 4, fontSize: 12, color: "#496675", marginTop: 8 }}>
                <div>{t.status}: <b>{job.status}</b></div>
                {isMasterDataMode ? (
                  <>
                    {job.status === "completed" && <div style={{ color: "#216b3a" }}>{t.masterdataReady}</div>}
                    <div style={{ fontWeight: 600 }}>{t.masterdataStats}</div>
                    {masterDataStatsItems.map(([label, value]) => (
                      <div key={String(label)}>{String(label)}: <b>{String(value)}</b></div>
                    ))}
                    {importPragmas.length > 0 && (
                      <div>{t.sqlitePragmas}: {importPragmas.map(([label, value]) => `${label}=${formatStatValue(value)}`).join(", ")}</div>
                    )}
                  </>
                ) : job.file_type === "lcia" ? (
                  <>
                    <div>{t.indicators}: <b>{Number(stats.indicators_count ?? 0)}</b></div>
                    <div>{t.factors}: <b>{Number(stats.factors_count ?? 0)}</b></div>
                    <div>{t.matched}: <b>{Number(stats.cf_matched ?? 0)}</b></div>
                    <div>{t.unmatched}: <b>{Number(stats.cf_unmatched ?? 0)}</b></div>
                  </>
                ) : (
                  <>
                    <div>{t.processes}: <b>{Number(stats.processes_inserted ?? 0)}</b> {t.new} / <b>{Number(stats.processes_updated ?? 0)}</b> {t.updated}</div>
                    <div>{t.skippedGlobal}: <b>{job.skipped_global ?? 0}</b></div>
                    <div>{t.vectors}: <b>{Number(stats.vectors_written ?? 0)}</b> {t.new} / <b>{Number(stats.vectors_reused ?? 0)}</b> {t.reused} / <b>{Number(stats.empty_vectors ?? 0)}</b> {t.emptyVectors}</div>
                    <div>{t.nnz}: <b>{Number(stats.vector_nnz_total ?? 0)}</b></div>
                    <div>{t.duration}: <b>{Number(stats.duration_seconds ?? 0).toFixed(1)}s</b></div>
                    {timingItems.length > 0 && <div>{t.importTiming}: {timingItems.map(([label, value]) => `${String(label)}=${formatStatValue(value)}`).join(", ")}</div>}
                    {importPragmas.length > 0 && <div>{t.sqlitePragmas}: {importPragmas.map(([label, value]) => `${label}=${formatStatValue(value)}`).join(", ")}</div>}
                  </>
                )}
                {job.failed_datasets?.length > 0 && <div style={{ color: "#e67e22" }}>{t.failed}: {job.failed_datasets.length}</div>}
              </div>
            </label>
          )}
        </div>

        {showGuide && (
          <div className="ef31-guide-mask" onClick={() => setShowGuide(false)}>
            <div className="ef31-guide-modal" onClick={(event) => event.stopPropagation()}>
              <div className="ef31-guide-head">
                <strong>{t.guideTitle}</strong>
                <button type="button" className="pm-link-btn" onClick={() => setShowGuide(false)}>{t.close}</button>
              </div>
              <div className="ef31-guide-body">
                <p>{t.guideIntro}</p>
                <div className="ef31-guide-links">
                  <a href="https://ecoinvent.org/" target="_blank" rel="noreferrer">{t.guideEnglish}</a>
                </div>
                <div className="ef31-guide-packages">
                  <span>{t.guidePackages}</span>
                  <code>cutoff_lci_ecoSpold02.7z</code>
                  <code>LCIA_implementation.7z</code>
                </div>
                <p>{t.guideNote}</p>
              </div>
            </div>
          </div>
        )}

        {errorText && <div className="pm-error">{errorText}</div>}

        <div className="pm-modal-actions">
          <button type="button" className="pm-ghost-btn" onClick={closePanel}>{t.close}</button>
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
        </div>
      </div>
    </div>
  );
}
