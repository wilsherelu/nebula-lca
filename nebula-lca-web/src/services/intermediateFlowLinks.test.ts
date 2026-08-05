import { afterEach, describe, expect, it, vi } from "vitest";

import {
  searchEcoIntermediateFlows,
  toIntermediateFlowLink,
  type RawResolution,
} from "./intermediateFlowLinks";

const resolution = (mappingLevel: "L1" | "L2" | "L3"): RawResolution => ({
  source_flow_uuid: "source",
  target_flow_uuid: "target",
  amount_factor: 1,
  source_unit: "kg",
  target_unit: "kg",
  mapping_level: mappingLevel,
  mapping_reason: "reviewed",
  rule_id: "rule",
  rule_origin: mappingLevel === "L3" ? "user" : "builtin",
});

describe("toIntermediateFlowLink", () => {
  it("auto-applies only L1 mappings", () => {
    expect(toIntermediateFlowLink(resolution("L1")).status).toBe("auto");
    expect(toIntermediateFlowLink(resolution("L2")).status).toBe("user_confirmed");
    expect(toIntermediateFlowLink(resolution("L3")).status).toBe("user_confirmed");
  });

  it("normalizes an active user rule and preserves its unit-group mapping", () => {
    const link = toIntermediateFlowLink({
      ...resolution("L3"),
      source_unit: "MJ",
      target_unit: "kWh",
      source_unit_group: "Units of energy",
      target_unit_group: "energy",
      amount_factor: 1 / 3.6,
      status: "active",
    });

    expect(link.status).toBe("user_confirmed");
    expect(link.sourceUnitGroup).toBe("Units of energy");
    expect(link.targetUnitGroup).toBe("energy");
    expect(link.amountFactor).toBeCloseTo(1 / 3.6, 12);
  });
});

describe("searchEcoIntermediateFlows", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("normalizes the public flow-list fields for manual conversion", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        items: [{
          flow_id: "eco-flow",
          flow_name: "electricity, high voltage",
          flow_name_en: "electricity, high voltage",
          unit: "kWh",
          unit_group: "energy",
        }],
      }),
    }));

    await expect(searchEcoIntermediateFlows("electricity")).resolves.toEqual([{
      flow_uuid: "eco-flow",
      flow_name: "electricity, high voltage",
      flow_name_en: "electricity, high voltage",
      default_unit: "kWh",
      unit_group: "energy",
    }]);
  });
});
