import { describe, expect, it } from "vitest";
import { getApiBase, getImportApiBase, normalizeApiBase } from "../apiBase";

describe("api base resolution", () => {
  it("normalizes missing api suffix", () => {
    expect(normalizeApiBase("http://127.0.0.1:8765")).toBe("http://127.0.0.1:8765/api");
  });

  it("keeps an existing api suffix", () => {
    expect(normalizeApiBase("http://127.0.0.1:8765/api")).toBe("http://127.0.0.1:8765/api");
  });

  it("prefers the Electron desktop bridge", () => {
    window.__NEBULA_DESKTOP__ = { apiBase: "http://127.0.0.1:9001/api" };
    expect(getApiBase()).toBe("http://127.0.0.1:9001/api");
    expect(getImportApiBase()).toBe("http://127.0.0.1:9001/api");
    delete window.__NEBULA_DESKTOP__;
  });
});
