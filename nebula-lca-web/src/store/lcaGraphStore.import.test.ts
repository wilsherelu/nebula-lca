import { beforeEach, describe, expect, it } from "vitest";

import type { ImportedUnitProcessPayload } from "./lcaGraphStore";
import { useLcaGraphStore } from "./lcaGraphStore";

const importedProcess = (processUuid: string): ImportedUnitProcessPayload => ({
  processUuid,
  importMode: "locked",
  name: "重复过程",
  inputs: [],
  outputs: [],
});

describe("addImportedUnitProcesses", () => {
  beforeEach(() => {
    useLcaGraphStore.setState({ uiLanguage: "zh" });
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg test",
      nodes: [],
      exchanges: [],
      metadata: {},
    });
  });

  it("assigns visible numeric suffixes before duplicate imports reach save", () => {
    useLcaGraphStore.getState().addImportedUnitProcesses([
      importedProcess("process-1"),
      importedProcess("process-2"),
    ]);
    useLcaGraphStore.getState().addImportedUnitProcesses([importedProcess("process-3")]);

    expect(useLcaGraphStore.getState().nodes.map((node) => node.data.name)).toEqual([
      "重复过程",
      "重复过程（1）",
      "重复过程（2）",
    ]);
  });
});
