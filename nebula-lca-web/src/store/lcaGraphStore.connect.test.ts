import { beforeEach, describe, expect, it } from "vitest";

import type { FlowPort } from "../model/node";
import { useLcaGraphStore } from "./lcaGraphStore";

const providerFlowUuid = "f1550a48-b171-4334-b3d3-cc9192a9cfa5";
const consumerFlowUuid = "3d76981f-964a-4865-b588-0e067a2a1163";

const electricityPort = (
  id: string,
  flowUuid: string,
  direction: "input" | "output",
  amount: number,
): FlowPort => ({
  id,
  flowUuid,
  flowSourceNamespace: "tiangong_open_source",
  flowVersion: "TG-1.0",
  flowPropertyUuid: "93a60a56-a3c8-11da-a746-0800200c9a66",
  name: "交流电",
  flowNameEn: "Alternating current",
  unit: "MJ",
  unitGroup: "Units of energy",
  amount,
  type: "technosphere",
  direction,
  showOnNode: true,
  isProduct: direction === "output",
});

describe("manual foreground connection", () => {
  beforeEach(() => {
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 unit",
      nodes: [
        {
          id: "provider",
          node_kind: "unit_process",
          mode: "balanced",
          process_uuid: "provider-process",
          name: "供给过程",
          location: "CN",
          reference_product: "交流电",
          inputs: [],
          outputs: [electricityPort("provider-output", providerFlowUuid, "output", 3.6)],
        },
        {
          id: "consumer",
          node_kind: "unit_process",
          mode: "balanced",
          process_uuid: "consumer-process",
          name: "消费过程",
          location: "CN",
          reference_product: "产品",
          inputs: [electricityPort("consumer-input", consumerFlowUuid, "input", 0.25)],
          outputs: [],
        },
      ],
      exchanges: [],
      metadata: {},
    });
  });

  it("rejects an explicitly selected port when the Flow UUID differs", () => {
    useLcaGraphStore.getState().onConnect({
      source: "provider",
      target: "consumer",
      sourceHandle: "out:provider-output",
      targetHandle: "in:consumer-input",
    });

    const stateAfterConnect = useLcaGraphStore.getState();
    expect(stateAfterConnect.nodes.find((node) => node.id === "consumer")?.data.inputs).toHaveLength(1);
    expect(stateAfterConnect.pendingEdges).toHaveLength(0);
    expect(stateAfterConnect.connectionHint).toContain("UUID");
    useLcaGraphStore.getState().flushPendingEdges();
    expect(useLcaGraphStore.getState().edges).toHaveLength(0);
  });
});
