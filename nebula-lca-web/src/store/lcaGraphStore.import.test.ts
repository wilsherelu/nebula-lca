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
  it.each([
    ["normalized", "balanced", "dual"],
    ["balanced", "normalized", "dual"],
    ["normalized", "normalized", "dual"],
    ["balanced", "balanced", "single"],
  ] as const)("derives %s -> %s quantity semantics without a repair prompt", (sourceMode, targetMode, expected) => {
    const port = (id: string, direction: "input" | "output", amount: number) => ({ id, flowUuid: "flow", name: "flow", amount, unit: "kg", unitGroup: "Mass", type: "technosphere" as const, direction, isProduct: direction === "output", showOnNode: true });
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg product", metadata: {},
      nodes: [
        { id: "source", node_kind: "unit_process", mode: sourceMode, process_uuid: "source", name: "source", location: "GLO", reference_product: "flow", inputs: [], outputs: [port("out", "output", 10)] },
        { id: "target", node_kind: "unit_process", mode: targetMode, process_uuid: "target", name: "target", location: "GLO", reference_product: "product", inputs: [port("in", "input", 3)], outputs: [] },
      ],
      exchanges: [{ id: "edge", fromNode: "source", toNode: "target", source_port_id: "out", target_port_id: "in", flowUuid: "flow", flowName: "flow", quantityMode: "single", amount: 3, providerAmount: 3, consumerAmount: 3, unit: "kg", type: "technosphere", allocation: "none" }],
    });
    expect(useLcaGraphStore.getState().edges[0].data?.quantityMode).toBe(expected);
    const edge = useLcaGraphStore.getState().exportGraph().exchanges[0];
    expect(edge.quantityMode).toBe(expected);
    if (expected === "dual") {
      expect(edge.providerAmount).toBe(10);
      expect(edge.consumerAmount).toBe(3);
    }
  });
  it("keeps a pinned source functional unit when editing and saving a project", () => {
    const functionalUnit = { amount: 1, flow_uuid: "steel-flow", unit: "kg", unit_group_uuid: "mass-group" };
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg crude steel", nodes: [], exchanges: [],
      metadata: { functional_unit: functionalUnit, reference_exchange_id: "caster::steel-out" },
    });
    const exported = useLcaGraphStore.getState().exportGraph();
    expect(exported.metadata?.functional_unit).toEqual(functionalUnit);
    expect(exported.metadata?.reference_exchange_id).toBe("caster::steel-out");
  });

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
