import { describe, expect, it } from "vitest";
import type { LcaGraphPayload } from "./model/exchange";
import { normalizeGraphPayload, prepareGraphForRun, reconcileProjectTargetProductConfig } from "./App";
import { useLcaGraphStore } from "./store/lcaGraphStore";

describe("normalizeGraphPayload", () => {
  it("preserves an exact converted-flow edge whose provider and consumer flow UUIDs differ", () => {
    const ruleId = "if-v2-consumer-flow";
    const graph = {
      functionalUnit: "1 kg product",
      nodes: [
        {
          id: "provider",
          node_kind: "lci_dataset",
          lci_role: "provider",
          process_uuid: "provider-process",
          name: "market group for electricity, high voltage",
          location: "CN",
          reference_product: "electricity, high voltage",
          hidden: true,
          inputs: [],
          outputs: [{
            id: "provider-output",
            name: "electricity, high voltage",
            flowUuid: "provider-flow",
            direction: "output",
            type: "technosphere",
            amount: 1,
            unit: "kWh",
            showOnNode: true,
          }],
        },
        {
          id: "foreground",
          node_kind: "unit_process",
          process_uuid: "foreground-process",
          name: "foreground",
          location: "CN",
          reference_product: "product",
          inputs: [{
            id: "consumer-input",
            name: "交流电",
            flowUuid: "consumer-flow",
            direction: "input",
            type: "technosphere",
            amount: 3.6,
            unit: "MJ",
            showOnNode: true,
            intermediateFlowLink: {
              sourceFlowUuid: "consumer-flow",
              targetFlowUuid: "provider-flow",
              amountFactor: 0.2777777777777778,
              sourceUnit: "MJ",
              targetUnit: "kWh",
              mappingLevel: "L2",
              mappingReason: "approved",
              ruleId,
              ruleOrigin: "builtin",
              status: "user_confirmed",
            },
          }],
          outputs: [],
        },
      ],
      exchanges: [{
        id: "edge-1",
        fromNode: "provider",
        toNode: "foreground",
        sourceHandle: "out:provider-output",
        targetHandle: "in:consumer-input",
        flowUuid: "provider-flow",
        flowName: "electricity, high voltage",
        consumerFlowUuid: "consumer-flow",
        quantityMode: "single",
        amount: 1,
        unit: "kWh",
        type: "technosphere",
        allocation: "none",
        intermediateFlowLinkRuleId: ruleId,
        intermediateFlowLinkFactor: 0.2777777777777778,
      }],
      metadata: {},
    } as LcaGraphPayload;

    const normalized = normalizeGraphPayload(graph);

    expect(normalized.exchanges).toHaveLength(1);
    expect(normalized.exchanges[0].source_port_id).toBe("provider-output");
    expect(normalized.exchanges[0].target_port_id).toBe("consumer-input");

    useLcaGraphStore.getState().importGraph(normalized);
    expect(useLcaGraphStore.getState().edges).toHaveLength(1);
    expect(useLcaGraphStore.getState().exportGraph().exchanges).toHaveLength(1);
  });

  it("preserves authoritative unit groups when preparing a converted graph for calculation", () => {
    const graph = {
      functionalUnit: "1 kg product",
      nodes: [
        {
          id: "provider",
          node_kind: "lci_dataset",
          process_uuid: "provider-process",
          name: "ecoinvent electricity provider",
          location: "GLO",
          reference_product: "electricity, low voltage",
          inputs: [],
          outputs: [{
            id: "provider-output",
            name: "electricity, low voltage",
            flowUuid: "ecoinvent-electricity",
            direction: "output",
            type: "technosphere",
            amount: 1,
            unit: "kWh",
            unitGroup: "energy",
            showOnNode: true,
            isProduct: true,
          }],
        },
        {
          id: "foreground",
          node_kind: "unit_process",
          process_uuid: "foreground-process",
          name: "foreground",
          location: "CN",
          reference_product: "product",
          inputs: [{
            id: "consumer-input",
            name: "直流电",
            flowUuid: "tidas-electricity",
            direction: "input",
            type: "technosphere",
            amount: 3.6,
            unit: "MJ",
            unitGroup: "Units of energy",
            showOnNode: true,
          }],
          outputs: [],
        },
      ],
      exchanges: [],
      metadata: {},
    } as LcaGraphPayload;

    const prepared = prepareGraphForRun(graph, null);

    expect(prepared.nodes[0].outputs?.[0].unitGroup).toBe("energy");
    expect(prepared.nodes[1].inputs?.[0].unitGroup).toBe("Units of energy");
  });

  it("rejects a hidden LCI product as the configured target", () => {
    const graph = {
      functionalUnit: "1 kg product",
      nodes: [
        {
          id: "foreground",
          node_kind: "unit_process",
          process_uuid: "foreground-process",
          name: "foreground",
          location: "CN",
          reference_product: "foreground product",
          inputs: [],
          outputs: [{
            id: "foreground-output",
            name: "foreground product",
            flowUuid: "foreground-flow",
            direction: "output",
            type: "technosphere",
            amount: 1,
            unit: "kg",
            showOnNode: true,
            isProduct: true,
          }],
        },
        {
          id: "background",
          node_kind: "lci_dataset",
          lci_role: "provider",
          process_uuid: "background-process",
          name: "background",
          location: "GLO",
          reference_product: "electricity",
          hidden: true,
          inputs: [],
          outputs: [{
            id: "background-output",
            name: "electricity",
            flowUuid: "background-flow",
            direction: "output",
            type: "technosphere",
            amount: 1,
            unit: "kWh",
            showOnNode: true,
            isProduct: true,
          }],
        },
      ],
      exchanges: [],
      metadata: {},
    } as LcaGraphPayload;

    expect(reconcileProjectTargetProductConfig(graph, {
      processUuid: "background-process",
      flowUuid: "background-flow",
      quantityMode: "custom",
      quantity: 1,
    })).toBeNull();
  });
});
