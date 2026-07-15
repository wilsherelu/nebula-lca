import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { FlowPort } from "../../model/node";
import { IntermediateFlowL2ReviewDialog } from "./IntermediateFlowL2ReviewDialog";

const port: FlowPort = {
  id: "diesel-port",
  name: "Diesel oil",
  flowUuid: "source-flow",
  direction: "input",
  type: "technosphere",
  amount: 1,
  unit: "kg",
  showOnNode: true,
};

describe("IntermediateFlowL2ReviewDialog", () => {
  it("shows a compact business warning instead of raw risk codes", () => {
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

    expect(screen.getByText("需核对产品范围和限定词")).toBeTruthy();
    expect(screen.queryByText("HEAD_NAME_MISMATCH")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "确认并转换 1 条 L2" }));
    expect(onConfirm).toHaveBeenCalledWith(["diesel-port"]);
  });
});
