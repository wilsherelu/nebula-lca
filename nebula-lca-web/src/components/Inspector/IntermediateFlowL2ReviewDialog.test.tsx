import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { FlowPort } from "../../model/node";
import { IntermediateFlowL2ReviewDialog } from "./IntermediateFlowL2ReviewDialog";

const port: FlowPort = {
  id: "diesel-port",
  name: "柴油",
  flowNameEn: "Diesel oil",
  flowUuid: "source-flow",
  direction: "input",
  type: "technosphere",
  amount: 1,
  unit: "kg",
  showOnNode: true,
};

describe("IntermediateFlowL2ReviewDialog", () => {
  it("shows the concise review note and status badge instead of raw risk codes", () => {
    const onConfirm = vi.fn();
    render(
      <IntermediateFlowL2ReviewDialog
        open
        busy={false}
        language="zh"
        items={[{
          port,
          resolution: {
            source_flow_uuid: "source-flow",
            target_flow_uuid: "target-flow",
            amount_factor: 1,
            source_unit: "kg",
            target_unit: "kg",
            mapping_level: "L2",
            mapping_reason: "review",
            rule_id: "rule-1",
            rule_origin: "builtin",
            target_flow_name: "diesel",
            warnings: ["HEAD_NAME_MISMATCH", "SEMANTIC_GENERALIZATION"],
          },
        }]}
        onClose={vi.fn()}
        onConfirm={onConfirm}
      />,
    );

    expect(screen.getByText("确认转换")).toBeTruthy();
    expect(screen.getByText("请核对转换目标后确认。")).toBeTruthy();
    expect(screen.getByText("状态")).toBeTruthy();
    expect(screen.getByText("待核对")).toBeTruthy();
    expect(screen.queryByText("HEAD_NAME_MISMATCH")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "确认并转换 1 条" }));
    expect(onConfirm).toHaveBeenCalledWith(["diesel-port"]);
  });

  it("prefers flowNameEn for source display in English UI", () => {
    render(
      <IntermediateFlowL2ReviewDialog
        open
        busy={false}
        language="en"
        items={[{
          port,
          resolution: {
            source_flow_uuid: "source-flow",
            target_flow_uuid: "target-flow",
            amount_factor: 1,
            source_unit: "kg",
            target_unit: "kg",
            mapping_level: "L2",
            mapping_reason: "review",
            rule_id: "rule-1",
            rule_origin: "builtin",
            target_flow_name_en: "diesel",
            warnings: [],
          },
        }]}
        onClose={vi.fn()}
        onConfirm={vi.fn()}
      />,
    );

    expect(screen.getByText("Diesel oil")).toBeTruthy();
    expect(screen.queryByText("柴油")).toBeNull();
  });
});
