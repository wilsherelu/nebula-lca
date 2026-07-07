import { describe, expect, it } from "vitest";

import { findClimateChangeIndicatorIndex, getRunProcessCount } from "../resultAnalysis";

describe("resultAnalysis", () => {
  it("recognizes GWP100 indicators without relying on fixed indicator_index", () => {
    const indicators = [
      { indicator_index: 177, indicator_en: "accumulated exceedance (AE)", method_en: "EF v3.1" },
      { indicator_index: 178, indicator_en: "global warming potential (GWP100)", method_en: "EF v3.1" },
    ];

    expect(findClimateChangeIndicatorIndex(indicators)).toBe(1);
  });

  it("uses backend process_count before display-row fallbacks", () => {
    expect(getRunProcessCount({ summary: { process_count: 4 }, lci_result: { process_index: [] } })).toBe(4);
  });
});
