import { getApiBase } from "../apiBase";
import { useRef, useState } from "react";

const API_BASE = getApiBase();

// ---------------------------------------------------------------------------
// API response shape matching TianGongFlowRefreshResponse
// ---------------------------------------------------------------------------

export type TianGongFlowRefreshResponse = {
  account_id: string;
  platform: string;
  flow_uuid: string;
  remote_id: string;
  remote_version: string | null;
  status: "refreshed" | "skipped" | "failed";
  tidas_import_job_id: string | null;
  tidas_import_report: Record<string, unknown> | null;
  warnings: string[];
  synced_record: Record<string, unknown> | null;
  error: string | null;
};

export type RefreshedCatalogFlow = {
  flow_uuid: string;
  flow_name: string;
  flow_name_en?: string | null;
  flow_type: string;
  default_unit: string;
  unit_group: string;
  compartment?: string | null;
  source?: string | null;
  is_custom?: boolean;
};

export type TianGongFlowRefreshResult = TianGongFlowRefreshResponse & {
  flow: RefreshedCatalogFlow;
};

// ---------------------------------------------------------------------------
// Error codes propagated from the backend
// ---------------------------------------------------------------------------

export type TianGongRefreshErrorCode =
  | "FLOW_NOT_FOUND"
  | "TIANGONG_REFRESH_INVALID_SOURCE"
  | "TIANGONG_REMOTE_FETCH_FAILED"
  | "TIANGONG_RATE_LIMITED"
  | "TIANGONG_UUID_MISMATCH"
  | "TIANGONG_IMPORT_FAILED"
  | string;

export interface TianGongRefreshError extends Error {
  code: TianGongRefreshErrorCode;
  retryAfterSeconds?: number;
  raw?: unknown;
}

async function parseRefreshError(resp: Response): Promise<TianGongRefreshError> {
  const payloadRaw = await resp.json().catch(() => ({})) as { detail?: { code?: string; message?: string; retry_after_seconds?: number } | string; message?: string };
  const detail = payloadRaw.detail;
  let code: TianGongRefreshErrorCode = "TIANGONG_IMPORT_FAILED";
  let message = resp.statusText || `HTTP ${resp.status}`;
  let retryAfter: number | undefined;

  if (typeof detail === "object" && detail !== null && "message" in detail) {
    message = String((detail as { message?: string }).message ?? message);
    code = ((detail as { code?: string }).code ?? code) as TianGongRefreshErrorCode;
    retryAfter = Number((detail as { retry_after_seconds?: number }).retry_after_seconds);
  } else if (typeof detail === "string") {
    message = detail;
  } else if (payloadRaw.message) {
    message = payloadRaw.message;
  }

  const err = new Error(message) as TianGongRefreshError;
  err.code = code;
  if (Number.isFinite(retryAfter) && retryAfter! > 0) {
    err.retryAfterSeconds = retryAfter!;
  }
  return err;
}

// ---------------------------------------------------------------------------
// Core fetch function — one busy flow UUID per call-site
// ---------------------------------------------------------------------------

/**
 * POST `/api/data-platforms/tiangong/flows/{flow_uuid}/refresh` with no request body.
 *
 * Returns the parsed `TianGongFlowRefreshResponse`.
 * Throws `TianGongRefreshError` on non-2xx, carrying `code` and optional
 * `retryAfterSeconds` for HTTP 429 cases.
 */
export async function refreshTianGongFlow(flowUuid: string): Promise<TianGongFlowRefreshResult> {
  const url = `${API_BASE}/data-platforms/tiangong/flows/${encodeURIComponent(flowUuid)}/refresh`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });

  if (!resp.ok) {
    throw await parseRefreshError(resp);
  }

  const refreshResult = (await resp.json()) as TianGongFlowRefreshResponse;
  const flowResp = await fetch(`${API_BASE}/reference/flows/${encodeURIComponent(flowUuid)}`, { cache: "no-store" });
  if (!flowResp.ok) {
    throw await parseRefreshError(flowResp);
  }
  const flow = (await flowResp.json()) as RefreshedCatalogFlow;
  if (flow.flow_uuid !== flowUuid) {
    const error = new Error(`Refreshed flow UUID mismatch: ${flow.flow_uuid}`) as TianGongRefreshError;
    error.code = "TIANGONG_UUID_MISMATCH";
    throw error;
  }
  return { ...refreshResult, flow };
}

// ---------------------------------------------------------------------------
// Hook: useTianGongFlowRefresh
// ---------------------------------------------------------------------------

export type UseTianGongFlowRefreshOptions = {
  /** Called on success with the refresh response; callers can update caches/lists. */
  onSuccess?: (result: TianGongFlowRefreshResult) => void;
  /** Called on error; receives the TianGongRefreshError. */
  onError?: (error: TianGongRefreshError) => void;
  /** Optional onStatus callback — forwards the backend message to the global status banner. */
  onStatus?: (text: string) => void;
  language?: "zh" | "en";
};

export type UseTianGongFlowRefreshReturn = {
  /** Currently refreshing flow UUID (empty string when idle). */
  busyFlowUuid: string;
  /** Overall loading state. */
  loading: boolean;
  /** Last known error (null when idle or after a successful refresh). */
  error: TianGongRefreshError | null;
  /** Last successful result (null when idle or after an error). */
  lastResult: TianGongFlowRefreshResult | null;
  /** Execute a refresh for the given flow UUID. Rejects if another flow is already refreshing. */
  refresh: (flowUuid: string) => Promise<TianGongFlowRefreshResult | null>;
  /** Clear current busy/error/result state. */
  reset: () => void;
};

/**
 * React hook for scoped TianGong flow refresh.
 *
 * - One busy flow UUID per hook instance — concurrent refreshes for different
 *   flows are rejected until the current one finishes or is reset.
 * - Real backend error messages are surfaced through `TianGongRefreshError`
 *   and optionally forwarded to `onStatus`.
 */
export function useTianGongFlowRefresh(
  options: UseTianGongFlowRefreshOptions = {},
): UseTianGongFlowRefreshReturn {
  const { onSuccess, onError, onStatus, language = "en" } = options;
  const activeFlowUuid = useRef("");
  const [busyFlowUuid, setBusyFlowUuid] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<TianGongRefreshError | null>(null);
  const [lastResult, setLastResult] = useState<TianGongFlowRefreshResult | null>(null);

  const refresh = async (flowUuid: string): Promise<TianGongFlowRefreshResult | null> => {
    if (activeFlowUuid.current) {
      const msg = onStatus
        ? (language === "zh" ? "正在更新中，请等待当前操作完成。" : "Refreshing, please wait for the current operation to complete.")
        : "";
      if (msg) onStatus?.(msg);
      return null;
    }
    activeFlowUuid.current = flowUuid;
    setLoading(true);
    setError(null);
    setLastResult(null);
    setBusyFlowUuid(flowUuid);

    try {
      const result = await refreshTianGongFlow(flowUuid);
      setLastResult(result);
      if (result.status === "failed" || result.error) {
        const err = new Error(result.error ?? "Refresh failed") as TianGongRefreshError;
        err.code = "TIANGONG_IMPORT_FAILED";
        setError(err);
        onError?.(err);
        onStatus?.(language === "zh" ? `更新失败：${result.error}` : `Refresh failed: ${result.error}`);
        return null;
      } else {
        onSuccess?.(result);
        onStatus?.(
          language === "zh"
            ? `Flow 已更新：${result.flow_uuid}`
            : `Flow refreshed: ${result.flow_uuid}`,
        );
        return result;
      }
    } catch (raw) {
      const err = raw instanceof Error && "code" in raw
        ? raw as TianGongRefreshError
        : Object.assign(
            raw instanceof Error ? raw : new Error(String(raw)),
            { code: "TIANGONG_IMPORT_FAILED" as TianGongRefreshErrorCode },
          );
      setError(err);
      onError?.(err);
      const codePart = err.code
        ? ` [${err.code}]`
        : "";
      onStatus?.(language === "zh" ? `更新失败：${err.message}${codePart}` : `Refresh failed: ${err.message}${codePart}`);
      return null;
    } finally {
      activeFlowUuid.current = "";
      setLoading(false);
      setBusyFlowUuid("");
    }
  };

  const reset = () => {
    activeFlowUuid.current = "";
    setBusyFlowUuid("");
    setLoading(false);
    setError(null);
    setLastResult(null);
  };

  return { busyFlowUuid, loading, error, lastResult, refresh, reset };
}
