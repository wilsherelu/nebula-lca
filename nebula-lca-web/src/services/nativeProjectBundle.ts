import { getApiBase } from "../apiBase";

const API_BASE = getApiBase();

const responseMessage = async (response: Response): Promise<string> => {
  const payload = (await response.json().catch(() => ({}))) as {
    detail?: string | { message?: string };
    message?: string;
  };
  return typeof payload.detail === "string"
    ? payload.detail
    : payload.detail?.message ?? payload.message ?? `HTTP ${response.status}`;
};

export const downloadNativeProjectBundle = async (projectId: string, projectName: string): Promise<void> => {
  const response = await fetch(`${API_BASE}/native-project-bundles/projects/${encodeURIComponent(projectId)}`);
  if (!response.ok) throw new Error(await responseMessage(response));
  const disposition = response.headers.get("Content-Disposition") ?? "";
  const match = disposition.match(/filename=["']?([^"';\n]+)/i);
  const filename = match?.[1] || `${projectName || projectId}.nebula.zip`;
  const url = URL.createObjectURL(await response.blob());
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
};

export type NativeProjectRestoreReceipt = {
  status: "restored" | "restored_with_issues";
  project_id: string;
  issues: Array<{ code?: string }>;
};

export const restoreNativeProjectBundle = async (file: File): Promise<NativeProjectRestoreReceipt> => {
  const body = new FormData();
  body.append("file", file);
  const response = await fetch(`${API_BASE}/native-project-bundles/import?conflict_policy=rename`, {
    method: "POST",
    body,
  });
  if (!response.ok) throw new Error(await responseMessage(response));
  return (await response.json()) as NativeProjectRestoreReceipt;
};
