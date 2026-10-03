import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Position } from "@xyflow/react";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { LcaExchangeEdge } from "./LcaExchangeEdge";

vi.mock("@xyflow/react", async (importOriginal) => ({
  ...await importOriginal<typeof import("@xyflow/react")>(),
  BaseEdge: ({ style }: { style: React.CSSProperties }) => <path data-testid="edge" style={style} />,
}));

afterEach(cleanup);

describe("edge style follows process modes", () => {
  it.each([
    ["normalized", "balanced", "single", true],
    ["balanced", "normalized", "single", true],
    ["normalized", "normalized", "single", true],
    ["balanced", "balanced", "single", false],
    ["balanced", "balanced", "dual", false],
  ] as const)("renders %s -> %s despite a stored %s-mode edge", (sourceMode, targetMode, storedMode, dashed) => {
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg product", metadata: {}, exchanges: [],
      nodes: [
        { id: "source", node_kind: "unit_process", mode: sourceMode, process_uuid: "source", name: "source", location: "GLO", reference_product: "product", inputs: [], outputs: [] },
        { id: "target", node_kind: "unit_process", mode: targetMode, process_uuid: "target", name: "target", location: "GLO", reference_product: "product", inputs: [], outputs: [] },
      ],
    });
    const props = { id: "edge", source: "source", target: "target", sourceX: 0, sourceY: 0, targetX: 100, targetY: 0, sourcePosition: Position.Right, targetPosition: Position.Left, data: { quantityMode: storedMode }, selected: false };
    const { getByTestId } = render(<svg><LcaExchangeEdge {...props} /></svg>);
    expect(getByTestId("edge").style.strokeDasharray).toBe(dashed ? "7 5" : "");
  });
});
