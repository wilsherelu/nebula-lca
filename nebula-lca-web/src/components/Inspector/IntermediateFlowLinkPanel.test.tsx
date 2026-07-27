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
  it("keeps original TIDAS flows unchanged when conversion is skipped", async () => {
    useLcaGraphStore.setState({ uiLanguage: "zh", edges: [] });
    render(<IntermediateFlowLinkPanel node={node} />);

    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));
    await waitFor(() => expect(screen.getByRole("dialog", { name: "中间流转换" })).toBeTruthy());
    expect(screen.getByText("暂不转换")).toBeTruthy();
    expect(screen.getByText(/未转换的流会保留原始 TIDAS 标识/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "暂不转换" }));
    expect(screen.queryByRole("dialog", { name: "中间流转换" })).toBeNull();
    expect(node.data.inputs[0].intermediateFlowLink).toBeUndefined();
  });
});
