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
      mappingLevel: "L3",
      mappingReason: "同类电力产品",
      ruleId: "rule-1",
      ruleOrigin: "user",
      status: "user_confirmed",
      warnings: [],
    });
  });

  it("lets the user select a result before entering the recorded rationale", async () => {
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
    expect((screen.getByRole("button", { name: "确认转换" }) as HTMLButtonElement).disabled).toBe(true);

    fireEvent.change(screen.getByPlaceholderText("请说明为何选择此流作为代理"), {
      target: { value: "同类电力产品" },
    });
    fireEvent.click(screen.getByRole("button", { name: "确认转换" }));

    await waitFor(() => {
      expect(createUserProxyRule).toHaveBeenCalledWith("source-flow", "eco-flow", "同类电力产品");
      expect(onConfirm).toHaveBeenCalledTimes(1);
    });
  });
});
