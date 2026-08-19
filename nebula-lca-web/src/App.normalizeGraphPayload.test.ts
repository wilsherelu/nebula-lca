import { describe, expect, it } from "vitest";
import type { LcaGraphPayload } from "./model/exchange";
import { normalizeGraphPayload, prepareGraphForRun, reconcileProjectTargetProductConfig } from "./App";
import { useLcaGraphStore } from "./store/lcaGraphStore";

describe("normalizeGraphPayload", () => {
  it("restores a persisted background LCI association from the slim storage shape", () => {
    const graph = {
      functionalUnit: "1 kg product",
      nodes: [
        {
          id: "provider",
          node_kind: "lci_dataset",
          lci_role: "provider",
          process_uuid: "provider-process",
          name: "market for diesel",
          location: "GLO",
          reference_product: "diesel",
          hidden: true,
          inputs: [],
          outputs: [{
            id: "provider-output",
            name: "diesel",
            flowUuid: "provider-flow",
            direction: "output",
            type: "technosphere",
            amount: 1,
            unit: "kg",
            unitGroup: "mass",
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
            name: "diesel input",
            flowUuid: "consumer-flow",
            direction: "input",
            type: "technosphere",
            amount: 1000,
            unit: "MJ",
            unitGroup: "Units of energy",
            showOnNode: true,
            intermediate_flow_link: {
              source_flow_uuid: "consumer-flow",
              target_flow_uuid: "provider-flow",
              amount_factor: 0.0234,
              source_unit: "MJ",
              target_unit: "kg",
              mapping_level: "L3",
              mapping_reason: "manual",
              rule_id: "manual-rule",
              rule_origin: "user",
              status: "user_confirmed",
            },
          }],
          outputs: [],
        },
      ],
      exchanges: [{
        id: "association-edge",
        fromNode: "provider",
        toNode: "foreground",
        sourceHandle: "out:provider-output",
        targetHandle: "in:consumer-input",
        source_port_id: "provider-output",
        target_port_id: "consumer-input",
        flowUuid: "provider-flow",
        flowName: "diesel",
        consumer_flow_uuid: "consumer-flow",
        quantityMode: "dual",
        amount: 23.4,
        providerAmount: 1,
        consumerAmount: 23.4,
        unit: "kg",
        type: "technosphere",
        allocation: "none",
        provider_unit: "kg",
        consumer_unit: "MJ",
        intermediate_flow_link_rule_id: "manual-rule",
        intermediate_flow_link_factor: 0.0234,
      }],
      metadata: {
        canvases: [{ id: "root", name: "Product System", kind: "root" }],
        storage_schema_version: "graph_slim_v1",
      },
    } as unknown as LcaGraphPayload;

    const normalized = normalizeGraphPayload(graph);
    expect(normalized.exchanges).toHaveLength(1);

    useLcaGraphStore.getState().importGraph(normalized);
    const state = useLcaGraphStore.getState();
    const provider = state.nodes.find((node) => node.id === "provider");
    const association = state.edges.find((edge) => edge.id === "association-edge");
    expect(provider?.hidden).toBe(true);
    expect(provider?.data.lciRole).toBe("provider");
    expect(association?.targetHandle).toBe("in:consumer-input");
    expect(state.graphRelations.sourcePortByEdgeId.get("association-edge")?.id).toBe("provider-output");
    expect(state.graphRelations.targetPortByEdgeId.get("association-edge")?.id).toBe("consumer-input");
    expect(state.exportGraph().exchanges).toHaveLength(1);

    useLcaGraphStore.getState().updateNode("foreground", (node) => ({
      ...node,
      data: {
        ...node.data,
        inputs: node.data.inputs.map((port) => ({ ...port, flowNameEn: "diesel input" })),
      },
    }));
    expect(useLcaGraphStore.getState().edges.find((edge) => edge.id === "association-edge")).toBeDefined();
  });

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
