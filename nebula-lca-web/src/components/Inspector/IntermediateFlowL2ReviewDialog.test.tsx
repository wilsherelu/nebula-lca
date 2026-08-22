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

const baseResolution = {
  source_flow_uuid: "source-flow",
  target_flow_uuid: "target-flow",
  amount_factor: 3.6,
  source_unit: "MJ",
  target_unit: "kWh",
  mapping_level: "L2" as const,
  mapping_reason: "review",
  rule_id: "rule-1",
  rule_origin: "builtin" as const,
  target_flow_name: "diesel",
  warnings: [] as string[],
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
            ...baseResolution,
            warnings: ["HEAD_NAME_MISMATCH", "SEMANTIC_GENERALIZATION"],
          },
        }]}
        onClose={vi.fn()}
        onConfirm={onConfirm}
      />,
    );

    expect(screen.getByText("确认转换")).toBeTruthy();
    expect(screen.getByText("请核对转换目标后确认。")).toBeTruthy();
    expect(screen.getByText("确认信息")).toBeTruthy();
    expect(screen.getByText("待核对")).toBeTruthy();
    expect(screen.queryByText("HEAD_NAME_MISMATCH")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "确认并转换 1 条" }));
    expect(onConfirm).toHaveBeenCalledWith([{ portId: "diesel-port" }]);
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
            ...baseResolution,
            target_flow_name_en: "diesel",
          },
        }]}
        onClose={vi.fn()}
        onConfirm={vi.fn()}
      />,
    );

    expect(screen.getByText("Diesel oil")).toBeTruthy();
    expect(screen.queryByText("柴油")).toBeNull();
  });

  it("renders source, target, unit, and status as separate semantic cells", () => {
    const { container } = render(
      <IntermediateFlowL2ReviewDialog
        open
        busy={false}
        language="zh"
        items={[{
          port,
          resolution: baseResolution,
        }]}
        onClose={vi.fn()}
        onConfirm={vi.fn()}
      />,
    );

    const rows = container.querySelectorAll(".intermediate-flow-l2-review-row");
    expect(rows.length).toBe(1);

    const cells = rows[0].children;
    expect(cells.length).toBe(5);
    expect(cells[1].textContent).toContain("柴油");
    expect(cells[2].textContent).toContain("diesel");
    expect(cells[3].textContent).toBe("MJ → kWh");
    expect(cells[4].textContent).toBe("待核对");

    const flowCell = cells[1] as HTMLElement;
    const targetCell = cells[2] as HTMLElement;
    expect(getComputedStyle(flowCell).display).not.toBe("grid");
    expect(getComputedStyle(targetCell).display).not.toBe("grid");
  });

  it("requires an independent positive factor for a cross-unit-group row", () => {
    const onConfirm = vi.fn();
    render(
      <IntermediateFlowL2ReviewDialog
        open
        busy={false}
        language="zh"
        items={[{
          port,
          resolution: {
            ...baseResolution,
            source_unit: "MJ",
            target_unit: "kg",
            source_unit_group: "Units of energy",
            target_unit_group: "Units of mass",
            requires_manual_factor: true,
          },
        }]}
        onClose={vi.fn()}
        onConfirm={onConfirm}
      />,
    );

    const confirmButton = screen.getByRole("button", { name: "确认并转换 1 条" }) as HTMLButtonElement;
    expect(confirmButton.disabled).toBe(true);
    expect(screen.getByText("1 MJ =")).toBeTruthy();

    fireEvent.change(screen.getByRole("spinbutton", { name: "柴油 换算系数" }), {
      target: { value: "0.0234" },
    });
    expect(confirmButton.disabled).toBe(false);
    fireEvent.click(confirmButton);
    expect(onConfirm).toHaveBeenCalledWith([{ portId: "diesel-port", amountFactor: 0.0234 }]);
  });
});
