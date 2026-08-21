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
  version: "legacy" | "tidas" = "legacy",
): FlowPort => ({
  id,
  flowUuid,
  flowSourceNamespace: version === "tidas" ? "tiangong_open_data" : "tiangong_open_source",
  flowVersion: version === "tidas" ? "01.01.000" : "TG-1.0",
  flowPropertyUuid: "93a60a56-a3c8-11da-a746-0800200c9a66",
  flowPropertyVersion: version === "tidas" ? "01.00.001" : undefined,
  unitGroupUuid: version === "tidas" ? "4d923ae4-a003-42ce-9f09-92c749af2104" : undefined,
  unitGroupVersion: version === "tidas" ? "01.00.002" : undefined,
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

  it("creates a new matching input instead of reusing a same-name port with another UUID", () => {
    useLcaGraphStore.getState().onConnect({
      source: "provider",
      target: "consumer",
      sourceHandle: "out:provider-output",
      targetHandle: "in:consumer-input",
    });

    const consumerInputs = useLcaGraphStore.getState().nodes.find((node) => node.id === "consumer")?.data.inputs ?? [];
    expect(consumerInputs).toHaveLength(2);
    expect(consumerInputs[0].flowUuid).toBe(consumerFlowUuid);
    expect(consumerInputs[1].flowUuid).toBe(providerFlowUuid);

    useLcaGraphStore.getState().flushPendingEdges();
    const edge = useLcaGraphStore.getState().edges[0];
    expect(edge?.data?.flowUuid).toBe(providerFlowUuid);
    expect(edge?.data?.consumerFlowUuid).toBeUndefined();
    expect(edge?.targetHandle).toBe(`in:${consumerInputs[1].id}`);
  });

  it("preserves the exact TIDAS Flow version identity on an auto-created input", () => {
    useLcaGraphStore.setState((state) => ({
      nodes: state.nodes.map((node) => node.id === "provider"
        ? {
            ...node,
            data: {
              ...node.data,
              outputs: [electricityPort("provider-output", providerFlowUuid, "output", 3.6, "tidas")],
            },
          }
        : node),
    }));

    useLcaGraphStore.getState().onConnect({
      source: "provider",
      target: "consumer",
      sourceHandle: "out:provider-output",
      targetHandle: "in:consumer-input",
    });

    const sourcePort = useLcaGraphStore.getState().nodes.find((node) => node.id === "provider")?.data.outputs[0];
    const autoInput = useLcaGraphStore.getState().nodes.find((node) => node.id === "consumer")?.data.inputs[1];
    expect(autoInput).toMatchObject({
      flowUuid: sourcePort?.flowUuid,
      flowSourceNamespace: sourcePort?.flowSourceNamespace,
      flowVersion: sourcePort?.flowVersion,
      flowPropertyUuid: sourcePort?.flowPropertyUuid,
      flowPropertyVersion: sourcePort?.flowPropertyVersion,
      unitGroupUuid: sourcePort?.unitGroupUuid,
      unitGroupVersion: sourcePort?.unitGroupVersion,
    });
  });

  it("does not reuse an orphaned same-UUID input from another Flow version", () => {
    const sourcePort = electricityPort("provider-output", providerFlowUuid, "output", 3.6, "tidas");
    const staleInput = electricityPort("in_oldbug", providerFlowUuid, "input", 3.6, "legacy");
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 unit",
      nodes: [
        {
          id: "provider",
          node_kind: "unit_process",
          process_uuid: "provider-process",
          name: "供给过程",
          location: "CN",
          reference_product: "交流电",
          inputs: [],
          outputs: [sourcePort],
        },
        {
          id: "consumer",
          node_kind: "unit_process",
          process_uuid: "consumer-process",
          name: "消费过程",
          location: "CN",
          reference_product: "产品",
          inputs: [staleInput],
          outputs: [],
        },
      ],
      exchanges: [],
      metadata: {},
    });

    useLcaGraphStore.getState().onConnect({
      source: "provider",
      target: "consumer",
      sourceHandle: "out:provider-output",
      targetHandle: "in:in_oldbug",
    });

    const consumerInputs = useLcaGraphStore.getState().nodes.find((node) => node.id === "consumer")?.data.inputs ?? [];
    expect(consumerInputs).toHaveLength(2);
    expect(consumerInputs[0]).toMatchObject({
      id: "in_oldbug",
      flowSourceNamespace: "tiangong_open_source",
      flowVersion: "TG-1.0",
    });
    expect(consumerInputs[1]).toMatchObject({
      flowUuid: sourcePort.flowUuid,
      flowSourceNamespace: sourcePort.flowSourceNamespace,
      flowVersion: sourcePort.flowVersion,
    });

    useLcaGraphStore.getState().flushPendingEdges();
    expect(useLcaGraphStore.getState().edges[0]?.targetHandle).toBe(`in:${consumerInputs[1].id}`);
  });

  it("repairs a version-mismatched auto input from the previous build without requiring reimport", () => {
    const sourcePort = electricityPort("provider-output", providerFlowUuid, "output", 3.6, "tidas");
    const staleAutoInput = {
      ...electricityPort("in_oldbug", providerFlowUuid, "input", 3.6),
      flowPropertyVersion: undefined,
      unitGroupUuid: undefined,
      unitGroupVersion: undefined,
    };
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 unit",
      nodes: [
        {
          id: "provider",
          node_kind: "unit_process",
          process_uuid: "provider-process",
          name: "供给过程",
          location: "CN",
          reference_product: "交流电",
          inputs: [],
          outputs: [sourcePort],
        },
        {
          id: "consumer",
          node_kind: "unit_process",
          process_uuid: "consumer-process",
          name: "消费过程",
          location: "CN",
          reference_product: "产品",
          inputs: [staleAutoInput],
          outputs: [],
        },
      ],
      exchanges: [{
        id: "edge-oldbug",
        fromNode: "provider",
        toNode: "consumer",
        source_port_id: sourcePort.id,
        target_port_id: staleAutoInput.id,
        sourceHandle: `out:${sourcePort.id}`,
        targetHandle: `in:${staleAutoInput.id}`,
        flowUuid: providerFlowUuid,
        flowName: sourcePort.name,
        quantityMode: "single",
        amount: 3.6,
        unit: "MJ",
        type: "technosphere",
        allocation: "none",
      }],
      metadata: {},
    });

    const repaired = useLcaGraphStore.getState().nodes.find((node) => node.id === "consumer")?.data.inputs[0];
    expect(repaired).toMatchObject({
      flowSourceNamespace: sourcePort.flowSourceNamespace,
      flowVersion: sourcePort.flowVersion,
      flowPropertyUuid: sourcePort.flowPropertyUuid,
      flowPropertyVersion: sourcePort.flowPropertyVersion,
      unitGroupUuid: sourcePort.unitGroupUuid,
      unitGroupVersion: sourcePort.unitGroupVersion,
    });
  });
});
