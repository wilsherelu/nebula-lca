export function isClimateChangeIndicator(item: unknown): boolean {
  if (typeof item !== "object" || item === null) {
    return false;
  }
  const row = item as Record<string, unknown>;
  const parts = [
    row.canonical_indicator_key,
    row.ecoinvent_category,
    row.indicator_en,
    row.indicator_zh,
    row.indicator,
    row.method_en,
    row.method_zh,
    row.method,
  ]
    .map((value) => String(value ?? "").trim().toLowerCase())
    .filter(Boolean);
  return parts.some(
    (value) =>
      value === "climate change" ||
      value.includes("global warming potential") ||
      value.includes("gwp100") ||
      value.includes("kg co2"),
  );
}

export function findClimateChangeIndicatorIndex(indicators: unknown[]): number {
  return indicators.findIndex((item) => isClimateChangeIndicator(item));
}

export function getRunProcessCount(lastRun: { summary?: Record<string, unknown>; lci_result?: Record<string, unknown> } | null | undefined): number {
  const fromSummary = Number(lastRun?.summary?.process_count);
  if (Number.isFinite(fromSummary) && fromSummary >= 0) {
    return fromSummary;
  }
  const processIndex = lastRun?.lci_result?.process_index;
  return Array.isArray(processIndex) ? processIndex.length : 0;
}
