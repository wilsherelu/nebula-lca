const RENDER_DEBUG_STORAGE_KEY = "nebula:render_debug";
const RENDER_DEBUG_GLOBAL_KEY = "__NEBULA_RENDER_DEBUG__";
const RENDER_DEBUG_BUFFER_STORAGE_KEY = "nebula:render_debug_buffer";
const RENDER_DEBUG_MAX_ENTRIES = 400;

type RenderDebugEntry = {
  ts: string;
  scope: string;
  payload?: unknown;
};

const parseEnabledFlag = (value: unknown): boolean => {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "on" || normalized === "yes";
};

export const isRenderDebugEnabled = (): boolean => {
  const envFlag = parseEnabledFlag(import.meta.env.VITE_RENDER_DEBUG);
  if (envFlag) {
    return true;
  }
  if (typeof window === "undefined") {
    return false;
  }
  const globalFlag = (window as typeof window & { [RENDER_DEBUG_GLOBAL_KEY]?: unknown })[RENDER_DEBUG_GLOBAL_KEY];
  if (typeof globalFlag !== "undefined") {
    return Boolean(globalFlag);
  }
  try {
    return parseEnabledFlag(window.localStorage.getItem(RENDER_DEBUG_STORAGE_KEY));
  } catch {
    return false;
  }
};

const readRenderDebugBuffer = (): RenderDebugEntry[] => {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const raw = window.sessionStorage.getItem(RENDER_DEBUG_BUFFER_STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw) as unknown;
    return Array.isArray(parsed) ? (parsed as RenderDebugEntry[]) : [];
  } catch {
    return [];
  }
};

const writeRenderDebugBuffer = (entries: RenderDebugEntry[]) => {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.setItem(RENDER_DEBUG_BUFFER_STORAGE_KEY, JSON.stringify(entries.slice(-RENDER_DEBUG_MAX_ENTRIES)));
  } catch {
    // Ignore storage quota or serialization errors in debug-only flow.
  }
};

const appendRenderDebugBuffer = (entry: RenderDebugEntry) => {
  const next = readRenderDebugBuffer();
  next.push(entry);
  writeRenderDebugBuffer(next);
};

export const getRenderDebugEntries = (): RenderDebugEntry[] => readRenderDebugBuffer();

export const clearRenderDebugEntries = () => {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.removeItem(RENDER_DEBUG_BUFFER_STORAGE_KEY);
  } catch {
    // Ignore storage cleanup failures in debug-only flow.
  }
};

export const downloadRenderDebugEntries = () => {
  if (typeof window === "undefined" || typeof document === "undefined") {
    return;
  }
  const entries = readRenderDebugBuffer();
  const payload = entries.map((entry) => JSON.stringify(entry)).join("\n");
  const blob = new Blob([payload], { type: "application/x-ndjson;charset=utf-8" });
  const url = window.URL.createObjectURL(blob);
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `nebula-render-debug-${stamp}.ndjson`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => window.URL.revokeObjectURL(url), 0);
};

export const debugRender = (scope: string, payload?: unknown) => {
  if (!isRenderDebugEnabled()) {
    return;
  }
  appendRenderDebugBuffer({
    ts: new Date().toISOString(),
    scope,
    payload,
  });
  if (payload === undefined) {
    console.info(`[RENDER_DEBUG] ${scope}`);
    return;
  }
  console.info(`[RENDER_DEBUG] ${scope}`, payload);
};

if (typeof window !== "undefined") {
  const globalWindow = window as typeof window & {
    __NEBULA_EXPORT_RENDER_DEBUG__?: () => void;
    __NEBULA_CLEAR_RENDER_DEBUG__?: () => void;
  };
  globalWindow.__NEBULA_EXPORT_RENDER_DEBUG__ = downloadRenderDebugEntries;
  globalWindow.__NEBULA_CLEAR_RENDER_DEBUG__ = clearRenderDebugEntries;
}

export { RENDER_DEBUG_BUFFER_STORAGE_KEY, RENDER_DEBUG_GLOBAL_KEY, RENDER_DEBUG_STORAGE_KEY };
