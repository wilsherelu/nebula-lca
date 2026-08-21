import { describe, expect, it } from "vitest";
import type { FlowPort } from "./node";
import { flowPortsAreVersionCompatible } from "./flowVersionIdentity";

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
