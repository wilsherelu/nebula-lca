import { describe, expect, it } from "vitest";
import type { FlowPort } from "./node";
import { uniquePinnedFlowIdentity } from "./pinnedFlowIdentity";

const pinned = (version: string): FlowPort => ({
  id: "co2-out", flowUuid: "co2-flow", name: "CO2", unit: "kg", amount: 1,
  type: "biosphere", direction: "output", showOnNode: false,
  flowSourceNamespace: "tiangong_open_data", flowVersion: version,
  flowPropertyUuid: "mass", flowPropertyVersion: "03.00.003",
  unitGroupUuid: "mass-units", unitGroupVersion: "03.00.003",
});

describe("uniquePinnedFlowIdentity", () => {
  it("reuses the exact identity of an existing graph flow", () => {
    expect(uniquePinnedFlowIdentity("co2-flow", [pinned("03.00.004"), pinned("03.00.004")]))
      .toEqual({
        flowSourceNamespace: "tiangong_open_data", flowVersion: "03.00.004",
        flowPropertyUuid: "mass", flowPropertyVersion: "03.00.003",
        unitGroupUuid: "mass-units", unitGroupVersion: "03.00.003",
      });
  });

  it("does not choose between different releases or invent a missing identity", () => {
    expect(uniquePinnedFlowIdentity("co2-flow", [pinned("03.00.004"), pinned("03.00.005")])).toBeNull();
    expect(uniquePinnedFlowIdentity("other-flow", [pinned("03.00.004")])).toBeNull();
  });
});
