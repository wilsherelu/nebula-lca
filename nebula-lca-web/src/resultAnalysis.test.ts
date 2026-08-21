import { describe, expect, it } from "vitest";
import {
  formatRunIssue,
  formatRunWarningBanner,
  getRunIssueAssociationTarget,
  getUnlinkedTechnosphereInputCount,
} from "./resultAnalysis";

const unlinkedIssue = {
  code: "UNLINKED_POSITIVE_TECHNOSPHERE_INPUT",
  node_id: "node-1",
  node_name: "原煤生产",
  port_id: "port-1",
  flow_name: "交流电",
  amount: 0.06336,
  unit: "MJ",
};

describe("run issue presentation", () => {
  it("keeps the banner compact and counts only unlinked intermediate-flow inputs", () => {
    const issues = [unlinkedIssue, { code: "OTHER" }, { ...unlinkedIssue, port_id: "port-2" }];
    expect(getUnlinkedTechnosphereInputCount(issues)).toBe(2);
    expect(formatRunWarningBanner(issues, "zh")).toBe("2 条中间流输入未关联背景数据集。");
  });

  it("marks an unlinked input as omitted and exposes its association target", () => {
    expect(formatRunIssue(unlinkedIssue, "zh")).toContain("该输入已从计算中省略");
    expect(getRunIssueAssociationTarget(unlinkedIssue)).toEqual({ nodeId: "node-1", portId: "port-1" });
  });
});
