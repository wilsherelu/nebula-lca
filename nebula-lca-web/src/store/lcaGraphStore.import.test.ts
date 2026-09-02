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

describe("normalized market graph round-trip", () => {
  it("preserves elementary exchanges when a project is loaded and saved", () => {
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg product",
      nodes: [{
        id: "electricity",
        node_kind: "market_process",
        mode: "normalized",
        market_allow_mixed_flows: false,
        process_uuid: "electricity-supplier",
        name: "Electricity supplier",
        location: "GLO",
        reference_product: "electricity",
        reference_product_flow_uuid: "electricity-flow",
        inputs: [],
        outputs: [
          { id: "electricity-out", flowUuid: "electricity-flow", name: "electricity", amount: 1, unit: "MJ", unitGroup: "Energy", type: "technosphere", direction: "output", isProduct: true, showOnNode: true },
          { id: "co2-out", flowUuid: "co2-fossil", name: "carbon dioxide, fossil", amount: 0.004, unit: "kg", unitGroup: "Mass", type: "biosphere", direction: "output", showOnNode: true },
        ],
      }],
      exchanges: [],
      metadata: {},
    });

    const exported = useLcaGraphStore.getState().exportGraph();
    const outputs = exported.nodes[0]?.outputs ?? [];
    expect(outputs.filter((port) => port.type === "biosphere")).toEqual([
      expect.objectContaining({ flowUuid: "co2-fossil", amount: 0.004, unit: "kg" }),
    ]);
    expect(outputs.filter((port) => port.type === "technosphere")).toHaveLength(1);
  });
});
