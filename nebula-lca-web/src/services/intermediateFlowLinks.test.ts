import { describe, expect, it } from "vitest";

import { toIntermediateFlowLink, type RawResolution } from "./intermediateFlowLinks";

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
});
