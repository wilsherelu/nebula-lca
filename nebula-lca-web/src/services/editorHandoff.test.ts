import { describe, expect, it } from "vitest";

import {
  buildEditorHandoffReturnUrl,
  buildEditorHandoffSearch,
  clearEditorHandoffCandidate,
  parseEditorHandoffRequest,
  persistEditorHandoffCandidate,
  restoreEditorHandoffCandidate,
  validateNebulaOneReturnOrigin,
} from "./editorHandoff";

describe("editor handoff contract", () => {
  it("parses an exact existing-project handoff", () => {
    const parsed = parseEditorHandoffRequest(
      "/projects/project-1",
      "?version=7&handoff_id=h-1&state=s-1&return_target=nebula_one",
    );
    expect(parsed.errorCode).toBeNull();
    expect(parsed.requestedVersion).toBe(7);
    expect(parsed.context).toMatchObject({
      mode: "existing",
      baseProjectId: "project-1",
      baseVersion: 7,
      handoffId: "h-1",
    });
  });

  it("requires an exact version for an existing-project handoff", () => {
    const parsed = parseEditorHandoffRequest(
      "/projects/project-1",
      "?handoff_id=h-1&state=s-1&return_target=nebula_one",
    );
    expect(parsed.errorCode).toBe("EXACT_VERSION_REQUIRED");
  });

  it("accepts a new-project handoff on the editor root", () => {
    const parsed = parseEditorHandoffRequest(
      "/",
      "?handoff_id=h-2&state=s-2&return_target=nebula_one",
    );
    expect(parsed.errorCode).toBeNull();
    expect(parsed.context).toMatchObject({ mode: "new", baseProjectId: null, baseVersion: null });
  });

  it("rejects arbitrary return URLs and unknown targets", () => {
    expect(
      parseEditorHandoffRequest(
        "/",
        "?handoff_id=h&state=s&return_target=nebula_one&return_url=https://evil.example",
      ).errorCode,
    ).toBe("ARBITRARY_RETURN_URL_REJECTED");
    expect(parseEditorHandoffRequest("/", "?handoff_id=h&state=s&return_target=other").errorCode).toBe(
      "RETURN_TARGET_NOT_ALLOWED",
    );
  });

  it("accepts only a configured HTTP(S) origin", () => {
    expect(validateNebulaOneReturnOrigin("http://127.0.0.1:6121")).toBe("http://127.0.0.1:6121");
    expect(validateNebulaOneReturnOrigin("https://one.example/")).toBe("https://one.example");
    expect(validateNebulaOneReturnOrigin("https://one.example/other")).toBeNull();
    expect(validateNebulaOneReturnOrigin("javascript:alert(1)")).toBeNull();
  });

  it("builds saved and cancelled return URLs", () => {
    const context = parseEditorHandoffRequest(
      "/projects/project-1",
      "?version=7&handoff_id=h-1&state=s-1&return_target=nebula_one",
    ).context!;
    const saved = new URL(
      buildEditorHandoffReturnUrl("http://127.0.0.1:6121", context, {
        status: "saved",
        candidate: { projectId: "project-1", savedVersion: 8, upstreamGraphHash: "abc123" },
      }),
    );
    expect(saved.pathname).toBe("/modeling/return");
    expect(saved.searchParams.get("base_version")).toBe("7");
    expect(saved.searchParams.get("saved_version")).toBe("8");
    expect(saved.searchParams.get("upstream_graph_hash")).toBe("abc123");

    const cancelled = new URL(
      buildEditorHandoffReturnUrl("http://127.0.0.1:6121", context, { status: "cancelled" }),
    );
    expect(cancelled.searchParams.get("status")).toBe("cancelled");
    expect(cancelled.searchParams.has("project_id")).toBe(false);
  });

  it("preserves the opaque handoff state while changing the exact version", () => {
    const context = parseEditorHandoffRequest(
      "/projects/project-1",
      "?version=7&handoff_id=h-1&state=s-1&return_target=nebula_one",
    ).context!;
    const params = new URLSearchParams(buildEditorHandoffSearch(context, 8));
    expect(params.get("version")).toBe("8");
    expect(params.get("handoff_id")).toBe("h-1");
    expect(params.get("handoff_mode")).toBe("existing");
    expect(params.get("base_version")).toBe("7");

    const refreshed = parseEditorHandoffRequest("/projects/project-1", `?${params.toString()}`);
    expect(refreshed.requestedVersion).toBe(8);
    expect(refreshed.context?.baseVersion).toBe(7);
  });

  it("rejects an invalid persisted base version", () => {
    const parsed = parseEditorHandoffRequest(
      "/projects/project-1",
      "?version=8&base_version=bad&handoff_id=h-1&state=s-1&return_target=nebula_one&handoff_mode=existing",
    );
    expect(parsed.errorCode).toBe("INVALID_BASE_VERSION");
  });

  it("restores a saved candidate only for the same handoff and exact persisted snapshot", () => {
    const context = parseEditorHandoffRequest(
      "/projects/project-1",
      "?version=8&base_version=7&handoff_id=h-1&state=s-1&return_target=nebula_one&handoff_mode=existing",
    ).context!;
    const candidate = { projectId: "project-1", savedVersion: 8, upstreamGraphHash: "hash-8" };
    persistEditorHandoffCandidate(sessionStorage, context, candidate);

    expect(restoreEditorHandoffCandidate(sessionStorage, context, "project-1", 8, "hash-8")).toEqual(candidate);
    expect(restoreEditorHandoffCandidate(sessionStorage, context, "project-1", 7, "hash-8")).toBeNull();
    expect(restoreEditorHandoffCandidate(sessionStorage, context, "project-1", 8, "other-hash")).toBeNull();

    clearEditorHandoffCandidate(sessionStorage, context.handoffId);
    expect(restoreEditorHandoffCandidate(sessionStorage, context, "project-1", 8, "hash-8")).toBeNull();
  });
});
