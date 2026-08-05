import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { Node } from "@xyflow/react";
import { describe, expect, it, vi } from "vitest";
import type { LcaNodeData } from "../../model/node";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { IntermediateFlowLinkPanel } from "./IntermediateFlowLinkPanel";

vi.mock("../../services/intermediateFlowLinks", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../services/intermediateFlowLinks")>();
  return {
    ...actual,
    resolveIntermediateFlowPorts: vi.fn().mockResolvedValue({ items: [] }),
  };
});

const node: Node<LcaNodeData> = {
  id: "process-1",
  type: "lcaProcess",
  position: { x: 0, y: 0 },
  data: {
    nodeKind: "unit_process",
    mode: "balanced",
    processUuid: "process-1",
    name: "TIDAS process",
    location: "CN",
    referenceProduct: "product",
    inputs: [{
      id: "input-1",
      flowUuid: "tidas-flow-1",
      name: "天工中间流",
      unit: "kg",
      amount: 1,
      type: "technosphere",
      direction: "input",
      showOnNode: true,
    }],
    outputs: [],
  },
};

describe("IntermediateFlowLinkPanel", () => {
  it("closes the dialog without mutating ports when close button is clicked", async () => {
    useLcaGraphStore.setState({ uiLanguage: "zh", edges: [] });
    render(<IntermediateFlowLinkPanel node={node} />);

    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));
    await waitFor(() => expect(screen.getByRole("dialog", { name: "中间流转换" })).toBeTruthy());

    expect(screen.getByText("来源流")).toBeTruthy();
    expect(screen.getByText("转换目标")).toBeTruthy();
    expect(screen.getByText("状态")).toBeTruthy();
    expect(screen.getByText("操作")).toBeTruthy();
    expect(screen.queryByText("保留 TIDAS")).toBeNull();
    expect(screen.queryByText(/请回到清单分析/)).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "关闭" }));
    expect(node.data.inputs[0].intermediateFlowLink).toBeUndefined();
  });

  it("prefers flowNameEn for source display in English UI", async () => {
    useLcaGraphStore.setState({ uiLanguage: "en", edges: [] });
    const enNode: typeof node = {
      ...node,
      data: {
        ...node.data,
        inputs: [{
          ...node.data.inputs[0],
          name: "天工中间流",
          flowNameEn: "Tiangong intermediate flow",
        }],
      },
    };
    render(<IntermediateFlowLinkPanel node={enNode} />);
    fireEvent.click(screen.getByRole("button", { name: /Convert Flows/ }));
    await waitFor(() => expect(screen.getByRole("dialog")).toBeTruthy());
    expect(screen.getByText("Tiangong intermediate flow")).toBeTruthy();
  });
});
