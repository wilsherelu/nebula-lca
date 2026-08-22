import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { FlowPort } from "../../model/node";
import { fetchIntermediateFlowProviders } from "../../services/intermediateFlowLinks";
import { BackgroundLciAssociationSection } from "./BackgroundLciAssociationSection";

vi.mock("../../services/intermediateFlowLinks", () => ({
  fetchIntermediateFlowProviders: vi.fn(),
}));

const mockFetchProviders = vi.mocked(fetchIntermediateFlowProviders);

const port: FlowPort = {
  id: "electricity-input",
  flowUuid: "tidas-electricity",
  name: "交流电",
  unit: "MJ",
  amount: 1,
  type: "technosphere",
  direction: "input",
  showOnNode: true,
};

describe("BackgroundLciAssociationSection", () => {
  beforeEach(() => {
    mockFetchProviders.mockReset();
  });

  it("shows a normal empty state and keeps the conversion entry available", async () => {
    const onRequestConversion = vi.fn();
    mockFetchProviders.mockResolvedValueOnce([]);

    render(
      <BackgroundLciAssociationSection
        consumerNodeId="consumer"
        port={port}
        language="zh"
        onLinked={vi.fn()}
        onRequestConversion={onRequestConversion}
      />,
    );

    await waitFor(() => expect(screen.getByText("无数据")).toBeTruthy());
    expect(screen.getByText("当前产品流尚无可关联背景 LCI；可先转换到目标产品流后再选择。")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "转换中间流" }));
    expect(onRequestConversion).toHaveBeenCalledTimes(1);
  });

  it("keeps raw HTTP diagnostics out of the visible error state", async () => {
    mockFetchProviders.mockRejectedValueOnce(new Error("Provider lookup failed (500)"));

    render(
      <BackgroundLciAssociationSection
        consumerNodeId="consumer"
        port={port}
        language="zh"
        onLinked={vi.fn()}
        onRequestConversion={vi.fn()}
      />,
    );

    await waitFor(() => expect(screen.getByText("背景供应查询暂不可用，请稍后重试。")).toBeTruthy());
    expect(screen.queryByText("Provider lookup failed (500)")).toBeNull();
  });
});
