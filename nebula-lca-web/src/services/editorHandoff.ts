export const NEBULA_ONE_RETURN_TARGET = "nebula_one" as const;

export type EditorHandoffMode = "new" | "existing";

export type EditorHandoffContext = {
  handoffId: string;
  state: string;
  returnTarget: typeof NEBULA_ONE_RETURN_TARGET;
  mode: EditorHandoffMode;
  baseProjectId: string | null;
  baseVersion: number | null;
};

export type EditorHandoffCandidate = {
  projectId: string;
  savedVersion: number;
  upstreamGraphHash: string;
};

type EditorHandoffSavedSession = {
  handoffId: string;
  state: string;
  mode: EditorHandoffMode;
  baseProjectId: string | null;
  baseVersion: number | null;
  candidate: EditorHandoffCandidate;
};

export type EditorHandoffParseResult = {
  context: EditorHandoffContext | null;
  requestedVersion: number | null;
  errorCode: string | null;
};

const positiveInteger = (raw: string | null): number | null => {
  if (!raw || !/^\d+$/.test(raw)) {
    return null;
  }
  const value = Number(raw);
  return Number.isSafeInteger(value) && value > 0 ? value : null;
};

const projectIdFromPathname = (pathname: string): string => {
  const match = String(pathname || "").match(/^\/projects\/([^/]+)\/?$/);
  if (!match) {
    return "";
  }
  try {
    return decodeURIComponent(match[1]).trim();
  } catch {
    return match[1].trim();
  }
};

const savedSessionKey = (handoffId: string): string => `nebula.editor-handoff.saved.${handoffId}`;

export const persistEditorHandoffCandidate = (
  storage: Storage,
  context: EditorHandoffContext,
  candidate: EditorHandoffCandidate,
): void => {
  const payload: EditorHandoffSavedSession = {
    handoffId: context.handoffId,
    state: context.state,
    mode: context.mode,
    baseProjectId: context.baseProjectId,
    baseVersion: context.baseVersion,
    candidate,
  };
  storage.setItem(savedSessionKey(context.handoffId), JSON.stringify(payload));
};

export const clearEditorHandoffCandidate = (storage: Storage, handoffId: string): void => {
  storage.removeItem(savedSessionKey(handoffId));
};

export const restoreEditorHandoffCandidate = (
  storage: Storage,
  context: EditorHandoffContext,
  exactProjectId: string,
  exactVersion: number,
  exactGraphHash: string,
): EditorHandoffCandidate | null => {
  try {
    const raw = storage.getItem(savedSessionKey(context.handoffId));
    if (!raw) {
      return null;
    }
    const saved = JSON.parse(raw) as Partial<EditorHandoffSavedSession>;
    const candidate = saved.candidate as EditorHandoffCandidate | undefined;
    if (
      saved.handoffId !== context.handoffId ||
      saved.state !== context.state ||
      saved.mode !== context.mode ||
      saved.baseProjectId !== context.baseProjectId ||
      saved.baseVersion !== context.baseVersion ||
      !candidate ||
      candidate.projectId !== exactProjectId ||
      candidate.savedVersion !== exactVersion ||
      candidate.upstreamGraphHash !== exactGraphHash
    ) {
      return null;
    }
    return candidate;
  } catch {
    return null;
  }
};

export const parseEditorHandoffRequest = (pathname: string, search: string): EditorHandoffParseResult => {
  const params = new URLSearchParams(search);
  const requestedVersionRaw = params.get("version");
  const requestedVersion = positiveInteger(requestedVersionRaw);
  if (requestedVersionRaw && requestedVersion == null) {
    return { context: null, requestedVersion: null, errorCode: "INVALID_VERSION" };
  }

  const hasHandoffParams = ["handoff_id", "state", "return_target", "handoff_mode", "return_url"].some((key) =>
    params.has(key),
  );
  if (!hasHandoffParams) {
    return { context: null, requestedVersion, errorCode: null };
  }
  if (params.has("return_url")) {
    return { context: null, requestedVersion, errorCode: "ARBITRARY_RETURN_URL_REJECTED" };
  }

  const handoffId = String(params.get("handoff_id") || "").trim();
  const state = String(params.get("state") || "").trim();
  const returnTarget = String(params.get("return_target") || "").trim();
  if (!handoffId || !state || !returnTarget) {
    return { context: null, requestedVersion, errorCode: "HANDOFF_PARAMETERS_INCOMPLETE" };
  }
  if (returnTarget !== NEBULA_ONE_RETURN_TARGET) {
    return { context: null, requestedVersion, errorCode: "RETURN_TARGET_NOT_ALLOWED" };
  }

  const routeProjectId = projectIdFromPathname(pathname);
  const explicitMode = params.get("handoff_mode");
  const mode: EditorHandoffMode = explicitMode === "new" || explicitMode === "existing"
    ? explicitMode
    : routeProjectId
      ? "existing"
      : "new";
  if (mode === "existing" && (!routeProjectId || requestedVersion == null)) {
    return { context: null, requestedVersion, errorCode: "EXACT_VERSION_REQUIRED" };
  }
  const baseVersionRaw = params.get("base_version");
  const baseVersion = baseVersionRaw == null ? requestedVersion : positiveInteger(baseVersionRaw);
  if (mode === "existing" && baseVersion == null) {
    return { context: null, requestedVersion, errorCode: "INVALID_BASE_VERSION" };
  }

  return {
    context: {
      handoffId,
      state,
      returnTarget: NEBULA_ONE_RETURN_TARGET,
      mode,
      baseProjectId: mode === "existing" ? routeProjectId : null,
      baseVersion: mode === "existing" ? baseVersion : null,
    },
    requestedVersion,
    errorCode: null,
  };
};

export const validateNebulaOneReturnOrigin = (rawOrigin: string | undefined): string | null => {
  const value = String(rawOrigin || "").trim();
  if (!value) {
    return null;
  }
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return null;
    }
    if (url.username || url.password || url.search || url.hash || (url.pathname && url.pathname !== "/")) {
      return null;
    }
    return url.origin;
  } catch {
    return null;
  }
};

export const buildEditorHandoffSearch = (
  context: EditorHandoffContext | null,
  version: number | null,
): string => {
  const params = new URLSearchParams();
  if (version != null) {
    params.set("version", String(version));
  }
  if (context) {
    params.set("handoff_id", context.handoffId);
    params.set("state", context.state);
    params.set("return_target", context.returnTarget);
    params.set("handoff_mode", context.mode);
    if (context.mode === "existing" && context.baseVersion != null) {
      params.set("base_version", String(context.baseVersion));
    }
  }
  const query = params.toString();
  return query ? `?${query}` : "";
};

export const buildEditorHandoffReturnUrl = (
  returnOrigin: string,
  context: EditorHandoffContext,
  result: { status: "cancelled" } | { status: "saved"; candidate: EditorHandoffCandidate },
): string => {
  const normalizedOrigin = validateNebulaOneReturnOrigin(returnOrigin);
  if (!normalizedOrigin) {
    throw new Error("Nebula One return origin is not configured correctly.");
  }
  const url = new URL("/modeling/return", normalizedOrigin);
  url.searchParams.set("handoff_id", context.handoffId);
  url.searchParams.set("state", context.state);
  url.searchParams.set("status", result.status);
  if (result.status === "saved") {
    url.searchParams.set("project_id", result.candidate.projectId);
    if (context.baseVersion != null) {
      url.searchParams.set("base_version", String(context.baseVersion));
    }
    url.searchParams.set("saved_version", String(result.candidate.savedVersion));
    url.searchParams.set("upstream_graph_hash", result.candidate.upstreamGraphHash);
  }
  return url.toString();
};
