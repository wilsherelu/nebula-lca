import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { FlowPort } from "../../model/node";
import { createUserProxyRule, searchEcoIntermediateFlows } from "../../services/intermediateFlowLinks";
import { L3UserProxyModal } from "./L3UserProxyModal";

vi.mock("../../services/intermediateFlowLinks", async (importOriginal) => {
  const original = await importOriginal<typeof import("../../services/intermediateFlowLinks")>();
  return {
    ...original,
    createUserProxyRule: vi.fn(),
    searchEcoIntermediateFlows: vi.fn(),
  };
});

const port: FlowPort = {
  id: "electricity-port",
  name: "电力",
  flowUuid: "source-flow",
  direction: "input",
  type: "technosphere",
  amount: 1,
  unit: "MJ",
  unitGroup: "Units of energy",
  showOnNode: true,
};

describe("L3UserProxyModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(searchEcoIntermediateFlows).mockResolvedValue([{
      flow_uuid: "eco-flow",
      flow_name: "electricity, high voltage",
      flow_name_en: "electricity, high voltage",
      default_unit: "kWh",
      unit_group: "energy",
    }]);
    vi.mocked(createUserProxyRule).mockResolvedValue({
      sourceFlowUuid: "source-flow",
      targetFlowUuid: "eco-flow",
      amountFactor: 1 / 3.6,
      sourceUnit: "MJ",
      targetUnit: "kWh",
      sourceUnitGroup: "Units of energy",
      targetUnitGroup: "energy",
      mappingLevel: "L3",
      mappingReason: "",
      ruleId: "rule-1",
      ruleOrigin: "user",
      status: "user_confirmed",
      warnings: [],
    });
  });

  it("confirms a selected result without requiring a note", async () => {
    const onConfirm = vi.fn();
    render(
      <L3UserProxyModal
        open
        busy={false}
        port={port}
        language="zh"
        onClose={vi.fn()}
        onConfirm={onConfirm}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("输入流名称搜索"), { target: { value: "electricity" } });
    fireEvent.click(screen.getByRole("button", { name: "搜索" }));
    await screen.findByText("electricity, high voltage");

    const selectButton = screen.getByRole("button", { name: "选择" });
    expect((selectButton as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(selectButton);
    expect(screen.getByRole("button", { name: "已选择" })).toBeTruthy();
    expect((screen.getByRole("button", { name: "确认转换" }) as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "确认转换" }));

    await waitFor(() => {
      expect(createUserProxyRule).toHaveBeenCalledWith("source-flow", "eco-flow", "", undefined);
      expect(onConfirm).toHaveBeenCalledTimes(1);
    });
  });

  it("requires an explicit factor for a cross-unit-group conversion", async () => {
    vi.mocked(searchEcoIntermediateFlows).mockResolvedValue([{
      flow_uuid: "eco-mass-flow",
      flow_name: "hard coal",
      flow_name_en: "hard coal",
      default_unit: "kg",
      unit_group: "mass",
    }]);
    const onConfirm = vi.fn();
    render(
      <L3UserProxyModal
        open
        busy={false}
        port={port}
        language="zh"
        onClose={vi.fn()}
        onConfirm={onConfirm}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("输入流名称搜索"), { target: { value: "coal" } });
    fireEvent.click(screen.getByRole("button", { name: "搜索" }));
    await screen.findByText("hard coal");
    fireEvent.click(screen.getByRole("button", { name: "选择" }));

    const confirmButton = screen.getByRole("button", { name: "确认转换" }) as HTMLButtonElement;
    expect(confirmButton.disabled).toBe(true);
    fireEvent.change(screen.getByPlaceholderText("请输入大于 0 的换算系数"), { target: { value: "0.04" } });
    expect(confirmButton.disabled).toBe(false);
    fireEvent.click(confirmButton);

    await waitFor(() => {
      expect(createUserProxyRule).toHaveBeenCalledWith("source-flow", "eco-mass-flow", "", 0.04);
      expect(onConfirm).toHaveBeenCalledTimes(1);
    });
  });
});
