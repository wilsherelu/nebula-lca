import { describe, expect, it } from "vitest";
import type { FlowPort } from "./node";
import { flowPortsAreForegroundAliases, flowPortsAreVersionCompatible } from "./flowVersionIdentity";

const port = (overrides: Partial<FlowPort> = {}): FlowPort => ({
  id: "port-1",
  flowUuid: "flow-1",
  name: "Flow",
  unit: "kg",
  unitGroup: "Units of mass",
  amount: 1,
  type: "technosphere",
  direction: "output",
  showOnNode: true,
  ...overrides,
});

describe("Foreground Flow aliases", () => {
  it("accepts distinct TG 1.0 UUIDs only when their embedded semantics match", () => {
    expect(flowPortsAreForegroundAliases(
      port({ flowUuid: "provider-flow", name: "交流电", flowNameEn: "Alternating current", unit: "MJ", unitGroup: "Units of energy" }),
      port({ flowUuid: "consumer-flow", name: "交流电", flowNameEn: "alternating current", unit: "MJ", unitGroup: "Units of energy", direction: "input" }),
    )).toBe(true);
  });

  it("rejects aliases across Flow versions or unit groups", () => {
    const source = port({ flowUuid: "provider-flow", name: "交流电", unit: "MJ", unitGroup: "Units of energy" });
    expect(flowPortsAreForegroundAliases(source, port({
      flowUuid: "consumer-flow",
      name: "交流电",
      unit: "MJ",
      unitGroup: "Units of energy",
      flowSourceNamespace: "tiangong_open_data",
      flowVersion: "01.01.003",
    }))).toBe(false);
    expect(flowPortsAreForegroundAliases(source, port({
      flowUuid: "consumer-flow",
      name: "交流电",
      unit: "kg",
      unitGroup: "Units of mass",
    }))).toBe(false);
  });
});

describe("Flow version identity", () => {
  it("keeps legacy unversioned ports compatible with TG 1.0", () => {
    expect(flowPortsAreVersionCompatible(
      port(),
      port({ flowSourceNamespace: "tiangong_open_source", flowVersion: "TG-1.0" }),
    )).toBe(true);
  });

  it("rejects the same UUID when source versions differ", () => {
    const mass = port({
      flowSourceNamespace: "tiangong_open_data",
      flowVersion: "01.01.002",
    });
    const energy = port({
      flowSourceNamespace: "tiangong_open_data",
      flowVersion: "01.01.003",
      unit: "MJ",
      unitGroup: "Units of energy",
    });

    expect(flowPortsAreVersionCompatible(mass, energy)).toBe(false);
  });
});
