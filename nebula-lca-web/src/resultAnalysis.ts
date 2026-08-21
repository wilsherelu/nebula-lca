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

export const UNLINKED_TECHNOSPHERE_INPUT_CODE = "UNLINKED_POSITIVE_TECHNOSPHERE_INPUT";

export type RunIssueAssociationTarget = {
  nodeId: string;
  portId: string;
};

export function isUnlinkedTechnosphereInput(issue: unknown): boolean {
  return Boolean(
    issue
    && typeof issue === "object"
    && String((issue as Record<string, unknown>).code ?? "") === UNLINKED_TECHNOSPHERE_INPUT_CODE,
  );
}

export function getRunIssueAssociationTarget(issue: unknown): RunIssueAssociationTarget | null {
  if (!isUnlinkedTechnosphereInput(issue)) {
    return null;
  }
  const record = issue as Record<string, unknown>;
  const nodeId = String(record.node_id ?? "").trim();
  const portId = String(record.port_id ?? "").trim();
  return nodeId && portId ? { nodeId, portId } : null;
}

export function getUnlinkedTechnosphereInputCount(issues: unknown[]): number {
  return issues.filter(isUnlinkedTechnosphereInput).length;
}

export function formatRunIssue(issue: unknown, language: "zh" | "en"): string {
  if (isUnlinkedTechnosphereInput(issue)) {
    const record = issue as Record<string, unknown>;
    const nodeName = String(record.node_name ?? record.node_id ?? "");
    const flowName = String(record.flow_name ?? record.port_id ?? "");
    const amount = String(record.amount ?? "");
    const unit = String(record.unit ?? "");
    return language === "zh"
      ? `${nodeName} / ${flowName}（${amount} ${unit}）。该输入已从计算中省略。`
      : `${nodeName} / ${flowName} (${amount} ${unit}). This input was omitted from the calculation.`;
  }
  if (issue && typeof issue === "object") {
    return JSON.stringify(issue);
  }
  return String(issue ?? "");
}

export function getRunIssuePresentation(
  issue: unknown,
  language: "zh" | "en",
  localized?: { nodeName?: string; flowName?: string },
): { processName: string; flowName: string; amount: string; status: string } {
  const record = issue && typeof issue === "object" ? issue as Record<string, unknown> : {};
  const processName = String(localized?.nodeName ?? record.node_name ?? record.node_id ?? "—").trim() || "—";
  const flowName = String(localized?.flowName ?? record.flow_name ?? record.port_id ?? "—").trim() || "—";
  const amountValue = String(record.amount ?? "").trim();
  const unit = String(record.unit ?? "").trim();
  return {
    processName,
    flowName,
    amount: [amountValue, unit].filter(Boolean).join(" ") || "—",
    status: isUnlinkedTechnosphereInput(issue)
      ? (language === "zh" ? "已从计算中省略" : "Omitted from calculation")
      : (language === "zh" ? "请查看详情" : "Review details"),
  };
}

export function formatRunWarningBanner(issues: unknown[], language: "zh" | "en"): string {
  const unlinkedCount = getUnlinkedTechnosphereInputCount(issues);
  if (unlinkedCount > 0) {
    return language === "zh"
      ? `${unlinkedCount} 条中间流输入未关联背景数据集。`
      : `${unlinkedCount} intermediate-flow inputs have no linked background dataset.`;
  }
  if (issues.length === 0) {
    return "";
  }
  return language === "zh" ? `发现 ${issues.length} 条计算警告。` : `${issues.length} calculation warnings found.`;
}
