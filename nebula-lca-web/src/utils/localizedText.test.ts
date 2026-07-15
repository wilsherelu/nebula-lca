import { describe, expect, it } from "vitest";
import { getLocalizedText } from "./localizedText";

describe("getLocalizedText", () => {
  it("selects the requested language from structured TIDAS text", () => {
    const value = [
      { "#text": "Electricity", "@xml:lang": "en" },
      { "#text": "电力", "@xml:lang": "zh" },
    ];
    expect(getLocalizedText(value, "zh")).toBe("电力");
    expect(getLocalizedText(value, "en")).toBe("Electricity");
  });

  it("recovers text from legacy Python-style serialized values", () => {
    expect(getLocalizedText("{'#text': 'waste wire', '@xml:lang': 'en'}", "zh")).toBe("waste wire");
    expect(getLocalizedText("[{'#text': 'Steam; industrial boilers', '@xml:lang': 'en'}]", "en"))
      .toBe("Steam; industrial boilers");
  });

  it("preserves ordinary names", () => {
    expect(getLocalizedText("hard coal", "zh")).toBe("hard coal");
  });
});
