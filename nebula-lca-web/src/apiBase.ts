export type NebulaDesktopBridge = {
  apiBase?: string;
  openLogs?: () => Promise<void>;
  getDiagnostics?: () => Promise<Record<string, unknown>>;
  chooseImportFile?: () => Promise<string | null>;
};

declare global {
  interface Window {
    __NEBULA_DESKTOP__?: NebulaDesktopBridge;
  }
}

export function normalizeApiBase(raw: string | undefined, fallback = "/api"): string {
  const base = (raw || fallback).replace(/\/$/, "");
  return base.endsWith("/api") ? base : `${base}/api`;
}

export function getApiBase(): string {
  return normalizeApiBase(window.__NEBULA_DESKTOP__?.apiBase || import.meta.env.VITE_API_BASE_URL);
}

export function getImportApiBase(): string {
  return normalizeApiBase(
    window.__NEBULA_DESKTOP__?.apiBase || import.meta.env.VITE_IMPORT_API_BASE_URL || import.meta.env.VITE_API_BASE_URL,
  );
}
