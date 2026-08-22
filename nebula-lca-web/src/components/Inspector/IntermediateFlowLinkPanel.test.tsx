import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { Node } from "@xyflow/react";
import { describe, expect, it, vi } from "vitest";
import type { LcaNodeData } from "../../model/node";
import { createUserProxyRule, resolveIntermediateFlowPorts } from "../../services/intermediateFlowLinks";
import { useLcaGraphStore } from "../../store/lcaGraphStore";
import { IntermediateFlowLinkPanel } from "./IntermediateFlowLinkPanel";

vi.mock("../../services/intermediateFlowLinks", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../services/intermediateFlowLinks")>();
  return {
    ...actual,
    resolveIntermediateFlowPorts: vi.fn().mockResolvedValue({ items: [] }),
    createUserProxyRule: vi.fn(),
  };
});

const mockResolve = vi.mocked(resolveIntermediateFlowPorts);
const mockCreateUserProxyRule = vi.mocked(createUserProxyRule);

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
  it("opens when another inspector workflow requests intermediate-flow conversion", async () => {
    useLcaGraphStore.setState({ uiLanguage: "zh", edges: [] });
    const { rerender } = render(<IntermediateFlowLinkPanel node={node} openRequestKey={0} />);

    expect(screen.queryByRole("dialog", { name: "中间流转换" })).toBeNull();
    rerender(<IntermediateFlowLinkPanel node={node} openRequestKey={1} />);

    await waitFor(() => expect(screen.getByRole("dialog", { name: "中间流转换" })).toBeTruthy());
  });

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

  it("offers proxy replacement for an existing conversion and detaches its background LCI", async () => {
    const linkedNode: Node<LcaNodeData> = {
      ...node,
      data: {
        ...node.data,
        inputs: [{
          ...node.data.inputs[0],
          intermediateFlowLink: {
            sourceFlowUuid: "tidas-flow-1",
            targetFlowUuid: "eco-flow-1",
            targetFlowName: "目标产品流",
            targetFlowNameEn: "target product flow",
            amountFactor: 1,
            sourceUnit: "kg",
            targetUnit: "kg",
            mappingLevel: "L1",
            mappingReason: "exact",
            ruleId: "rule-existing",
            ruleOrigin: "builtin",
            status: "auto",
            warnings: [],
          },
        }],
      },
    };
    const providerNode: Node<LcaNodeData> = {
      id: "hidden-provider-replace",
      type: "lcaProcess",
      hidden: true,
      position: { x: -200, y: 0 },
      data: {
        nodeKind: "lci_dataset",
        mode: "normalized",
        processUuid: "provider-replace",
        name: "Background provider",
        location: "GLO",
        referenceProduct: "product",
        lciRole: "provider",
        inputs: [],
        outputs: [],
      },
    };
    const providerEdge = {
      id: "edge-provider-replace",
      source: providerNode.id,
      target: linkedNode.id,
      targetHandle: "in:input-1",
    };
    useLcaGraphStore.setState((state) => {
      const active = state.canvases[state.activeCanvasId];
      return {
        uiLanguage: "zh",
        canvases: {
          ...state.canvases,
          [state.activeCanvasId]: {
            ...active,
            nodes: [linkedNode, providerNode],
            edges: [providerEdge],
          },
        },
        nodes: [linkedNode, providerNode],
        edges: [providerEdge],
      };
    });

    render(<IntermediateFlowLinkPanel node={linkedNode} />);
    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));
    await waitFor(() => expect(screen.getByRole("button", { name: "更换代理" })).toBeTruthy());
    expect(screen.getByText("目标产品流 · kg")).toBeTruthy();
    expect(screen.getByText("ecoinvent")).toBeTruthy();
    expect(screen.queryByText("ecoinvent 产品流 · kg")).toBeNull();

    useLcaGraphStore.getState().disconnectIntermediateProvider(linkedNode.id, "input-1");
    const active = useLcaGraphStore.getState().canvases[useLcaGraphStore.getState().activeCanvasId];
    expect(active.edges).toHaveLength(0);
    expect(active.nodes.some((item) => item.id === providerNode.id)).toBe(false);
  });

  it("does not present a stale conversion as completed when exact Flow semantics are blocked", async () => {
    const staleNode: Node<LcaNodeData> = {
      ...node,
      data: {
        ...node.data,
        inputs: [{
          ...node.data.inputs[0],
          intermediateFlowLink: {
            sourceFlowUuid: "tidas-flow-1",
            targetFlowUuid: "eco-flow-1",
            amountFactor: 0.0234,
            sourceUnit: "MJ",
            targetUnit: "kg",
            mappingLevel: "L3",
            mappingReason: "legacy",
            ruleId: "legacy-rule",
            ruleOrigin: "user",
            status: "user_confirmed",
            warnings: [],
          },
        }],
      },
    };
    useLcaGraphStore.setState({ uiLanguage: "zh", edges: [] });
    mockResolve.mockResolvedValueOnce({
      counts: { blocked: 1 },
      items: [{
        port_id: "input-1",
        status: "blocked",
        reason: "SOURCE_UNIT_DRIFT",
      }],
    });

    render(<IntermediateFlowLinkPanel node={staleNode} />);
    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));

    await waitFor(() => expect(screen.getByText("原转换的单位证据与当前 Flow 不匹配")).toBeTruthy());
    expect(screen.queryByText("转换完成")).toBeNull();
    expect(screen.getByRole("button", { name: "手动转换" })).toBeTruthy();
  });

  it("rehydrates the target name for a legacy saved conversion", async () => {
    const legacyNode: Node<LcaNodeData> = {
      ...node,
      data: {
        ...node.data,
        inputs: [{
          ...node.data.inputs[0],
          intermediateFlowLink: {
            sourceFlowUuid: "tidas-flow-1",
            targetFlowUuid: "eco-flow-1",
            amountFactor: 1,
            sourceUnit: "kg",
            targetUnit: "kg",
            mappingLevel: "L1",
            mappingReason: "exact",
            ruleId: "legacy-link",
            ruleOrigin: "builtin",
            status: "auto",
          },
        }],
      },
    };
    useLcaGraphStore.setState({ uiLanguage: "zh", edges: [] });
    mockResolve.mockResolvedValueOnce({
      counts: {},
      items: [{
        port_id: "input-1",
        status: "explicit",
        resolution: legacyNode.data.inputs[0].intermediateFlowLink,
        target_flow_name: "低压电力",
        target_flow_name_en: "electricity, low voltage",
      }],
    });

    render(<IntermediateFlowLinkPanel node={legacyNode} />);
    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));

    await waitFor(() => expect(screen.getByText("低压电力 · kg")).toBeTruthy());
  });

  it("submits a cross-unit-group candidate with its reviewed factor", async () => {
    useLcaGraphStore.setState({ uiLanguage: "zh", edges: [] });
    mockResolve.mockResolvedValueOnce({
      counts: { L2: 1 },
      items: [{
        port_id: "input-1",
        status: "L2",
        resolution: {
          source_flow_uuid: "tidas-flow-1",
          target_flow_uuid: "eco-flow-1",
          amount_factor: 1,
          source_unit: "MJ",
          target_unit: "kg",
          source_unit_group: "Units of energy",
          target_unit_group: "Units of mass",
          mapping_level: "L1",
          mapping_reason: "package_mapping",
          rule_id: "cross-group-rule",
          rule_origin: "builtin",
          target_flow_name: "目标产品流",
          requires_manual_factor: true,
          warnings: ["CROSS_GROUP_FACTOR_REQUIRED"],
        },
      }],
    });
    mockCreateUserProxyRule.mockResolvedValueOnce({
      sourceFlowUuid: "tidas-flow-1",
      targetFlowUuid: "eco-flow-1",
      amountFactor: 0.0234,
      sourceUnit: "MJ",
      targetUnit: "kg",
      mappingLevel: "L3",
      mappingReason: "user_confirmed_cross_unit_group_conversion",
      ruleId: "user-rule-1",
      ruleOrigin: "user",
      status: "user_confirmed",
      warnings: [],
    });

    render(<IntermediateFlowLinkPanel node={node} />);
    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));
    await waitFor(() => expect(screen.getByText("需录入换算系数")).toBeTruthy());
    fireEvent.click(screen.getByRole("button", { name: "确认转换（1）" }));
    fireEvent.change(screen.getByRole("spinbutton", { name: "天工中间流 换算系数" }), {
      target: { value: "0.0234" },
    });
    fireEvent.click(screen.getByRole("button", { name: "确认并转换 1 条" }));

    await waitFor(() => expect(mockCreateUserProxyRule).toHaveBeenCalledWith(
      node.data.inputs[0],
      "eco-flow-1",
      "user_confirmed_cross_unit_group_conversion",
      0.0234,
    ));
  });

  it("sets the foreground port unit to sourceUnit after L1 auto-conversion", async () => {
    const localNode: Node<LcaNodeData> = {
      ...node,
      data: {
        ...node.data,
        inputs: [{
          ...node.data.inputs[0],
          id: "mj-port",
          flowUuid: "tidas-mj-flow",
          name: "热能",
          unit: "kg",
        }],
      },
    };
    useLcaGraphStore.setState((state) => {
      const active = state.canvases[state.activeCanvasId];
      return {
        uiLanguage: "zh",
        edges: [],
        canvases: {
          ...state.canvases,
          [state.activeCanvasId]: { ...active, nodes: [localNode], edges: [] },
        },
        nodes: [localNode],
      };
    });
    mockResolve.mockResolvedValueOnce({
      counts: {},
      items: [{
        port_id: "mj-port",
        status: "L1",
        resolution: {
          source_flow_uuid: "tidas-mj-flow",
          target_flow_uuid: "ecoinvent-heat",
          amount_factor: 3.6,
          source_unit: "MJ",
          target_unit: "kWh",
          source_unit_group: "Units of energy",
          target_unit_group: "energy",
          mapping_level: "L1",
          mapping_reason: "auto",
          rule_id: "rule-mj",
          rule_origin: "builtin",
          target_flow_name: "heat",
          warnings: [],
        },
      }],
    });

    render(<IntermediateFlowLinkPanel node={localNode} />);
    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));
    await waitFor(() => expect(screen.getByText("自动可转换 1")).toBeTruthy());
    expect(screen.queryByRole("button", { name: "更换代理" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /自动转换（1）/ }));

    await waitFor(() => {
      const storeNodes = useLcaGraphStore.getState().canvases[useLcaGraphStore.getState().activeCanvasId].nodes;
      const updated = storeNodes.find((n) => n.id === "process-1");
      expect(updated?.data.inputs[0].unit).toBe("MJ");
    });

    const storeNodes = useLcaGraphStore.getState().canvases[useLcaGraphStore.getState().activeCanvasId].nodes;
    const updated = storeNodes.find((n) => n.id === "process-1");
    expect(updated?.data.inputs[0].intermediateFlowLink).toBeDefined();
    expect(updated?.data.inputs[0].intermediateFlowLink?.sourceUnit).toBe("MJ");
    expect(updated?.data.inputs[0].intermediateFlowLink?.targetUnit).toBe("kWh");
    expect(updated?.data.inputs[0].unit).toBe("MJ");
    expect(updated?.data.inputs[0].unitGroup).toBe("Units of energy");
  });

  it("offers reuse and manual conversion for an L3 conversion memory", async () => {
    useLcaGraphStore.setState({ uiLanguage: "zh", edges: [] });
    mockResolve.mockResolvedValueOnce({
      counts: {},
      items: [{
        port_id: "input-1",
        status: "L3",
        resolution: {
          source_flow_uuid: "tidas-flow-1",
          target_flow_uuid: "ecoinvent-flow-1",
          amount_factor: 1,
          source_unit: "kg",
          target_unit: "kg",
          mapping_level: "L3",
          mapping_reason: "",
          rule_id: "remembered-rule",
          rule_origin: "user",
          target_flow_name: "remembered product",
          warnings: [],
        },
      }],
    });

    render(<IntermediateFlowLinkPanel node={node} />);
    fireEvent.click(screen.getByRole("button", { name: /中间流转换/ }));

    await waitFor(() => expect(screen.getByText("有转换记忆")).toBeTruthy());
    expect(screen.getByRole("button", { name: "复用" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "手动转换" })).toBeTruthy();
    expect(screen.queryByText("复用转换")).toBeNull();
  });

  it("restores source unit semantics and normalizes a persisted active L3 rule", () => {
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg test",
      nodes: [{
        id: "node-import-1",
        node_kind: "unit_process",
        mode: "balanced",
        process_uuid: "proc-import-1",
        name: "Imported Process",
        location: "CN",
        reference_product: "product",
        inputs: [{
          id: "port-restore",
          flowUuid: "tidas-energy-flow",
          name: "热能",
          unit: "kg",
          unitGroup: "Units of energy",
          amount: 1,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
          intermediate_flow_link: {
            source_flow_uuid: "tidas-energy-flow",
            target_flow_uuid: "ecoinvent-heat-kwh",
            amount_factor: 0.2777777777777778,
            source_unit: "MJ",
            target_unit: "kWh",
            source_unit_group: "Units of energy",
            target_unit_group: "energy",
            mapping_level: "L3",
            mapping_reason: "",
            rule_id: "rule-mj-kwh",
            rule_origin: "user",
            status: "active",
            warnings: [],
          },
        }] as unknown as import("../../model/node").FlowPort[],
        outputs: [],
      }],
      exchanges: [],
    });

    const storeNodes = useLcaGraphStore.getState().canvases[useLcaGraphStore.getState().activeCanvasId].nodes;
    const imported = storeNodes.find((n) => n.id === "node-import-1");
    expect(imported).toBeDefined();
    const port = imported!.data.inputs[0];
    expect(port.unit).toBe("MJ");
    expect(port.unitGroup).toBe("Units of energy");
    expect(port.intermediateFlowLink).toBeDefined();
    expect(port.intermediateFlowLink!.sourceUnit).toBe("MJ");
    expect(port.intermediateFlowLink!.targetUnit).toBe("kWh");
    expect(port.intermediateFlowLink!.targetUnitGroup).toBe("energy");
    expect(port.intermediateFlowLink!.status).toBe("user_confirmed");
  });

  it("preserves stored port unit when link status is inactive on import", () => {
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg test",
      nodes: [{
        id: "node-inactive",
        node_kind: "unit_process",
        mode: "balanced",
        process_uuid: "proc-inactive",
        name: "Inactive Link Process",
        location: "CN",
        reference_product: "product",
        inputs: [{
          id: "port-inactive",
          flowUuid: "tidas-flow-x",
          name: "中间流X",
          unit: "kg",
          amount: 1,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
          intermediate_flow_link: {
            source_flow_uuid: "tidas-flow-x",
            target_flow_uuid: "ecoinvent-flow-x",
            amount_factor: 1,
            source_unit: "MJ",
            target_unit: "kWh",
            mapping_level: "L2",
            mapping_reason: "test",
            rule_id: "rule-x",
            rule_origin: "builtin",
            status: "inactive",
            warnings: [],
          },
        }] as unknown as import("../../model/node").FlowPort[],
        outputs: [],
      }],
      exchanges: [],
    });

    const storeNodes = useLcaGraphStore.getState().canvases[useLcaGraphStore.getState().activeCanvasId].nodes;
    const imported = storeNodes.find((n) => n.id === "node-inactive");
    expect(imported).toBeDefined();
    expect(imported!.data.inputs[0].unit).toBe("kg");
    expect(imported!.data.inputs[0].intermediateFlowLink!.status).toBe("inactive");
  });

  it("repairs the missing subtype override on persisted public L2 evidence", () => {
    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 MJ electricity",
      nodes: [{
        id: "node-l2-repair",
        node_kind: "unit_process",
        mode: "balanced",
        process_uuid: "proc-l2-repair",
        name: "Imported Process",
        location: "CN",
        reference_product: "product",
        inputs: [{
          id: "port-l2-repair",
          flowUuid: "tidas-electricity",
          name: "Electricity",
          unit: "MJ",
          amount: 1,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
          intermediate_flow_link: {
            source_flow_uuid: "tidas-electricity",
            target_flow_uuid: "ecoinvent-electricity",
            amount_factor: 0.2777777777777778,
            source_unit: "MJ",
            target_unit: "kWh",
            mapping_level: "L2",
            mapping_reason: "approved",
            rule_id: "public-rule",
            rule_origin: "builtin",
            status: "user_confirmed",
            application_mode: "auto_compatible",
            flow_subtype_override: false,
            warnings: ["MANUAL_CONFIRMATION_RECOMMENDED"],
          },
        }] as unknown as import("../../model/node").FlowPort[],
        outputs: [],
      }],
      exchanges: [],
    });

    const store = useLcaGraphStore.getState();
    const imported = store.canvases[store.activeCanvasId].nodes.find((node) => node.id === "node-l2-repair");
    expect(imported?.data.inputs[0].intermediateFlowLink?.flowSubtypeOverride).toBe(true);
  });

  it("preserves converted provider edge through exportGraph with exact contract", () => {
    const consumerNodeId = "consumer-node";
    const consumerPortId = "consumer-port";
    const tidasFlowUuid = "tidas-energy-flow";
    const ecoinventFlowUuid = "ecoinvent-heat-kwh";
    const ruleId = "rule-mj-kwh";
    const amountFactor = 0.2777777777777778;
    const foregroundAmount = 3.6;

    const consumerNode: Node<LcaNodeData> = {
      id: consumerNodeId,
      type: "lcaProcess",
      position: { x: 0, y: 0 },
      data: {
        nodeKind: "unit_process",
        mode: "balanced",
        processUuid: "proc-consumer",
        name: "Consumer Process",
        location: "CN",
        referenceProduct: "product",
        inputs: [{
          id: consumerPortId,
          flowUuid: tidasFlowUuid,
          name: "热能",
          unit: "MJ",
          amount: foregroundAmount,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
          intermediateFlowLink: {
            sourceFlowUuid: tidasFlowUuid,
            targetFlowUuid: ecoinventFlowUuid,
            amountFactor,
            sourceUnit: "MJ",
            targetUnit: "kWh",
            mappingLevel: "L2",
            mappingReason: "unit_conversion",
            ruleId,
            ruleOrigin: "builtin",
            status: "user_confirmed",
            warnings: [],
          },
        }],
        outputs: [],
      },
    };

    useLcaGraphStore.setState((state) => {
      const active = state.canvases[state.activeCanvasId];
      return {
        uiLanguage: "zh",
        edges: [],
        canvases: {
          ...state.canvases,
          [state.activeCanvasId]: { ...active, nodes: [consumerNode], edges: [] },
        },
        nodes: [consumerNode],
      };
    });

    const providerPayload = {
      nodeKind: "lci_dataset" as const,
      processUuid: "proc-provider",
      importMode: "locked" as const,
      name: "Heat supply, kWh",
      location: "GLO",
      sourceSystem: "ecoinvent 4.5",
      referenceProduct: "Heat supply",
      referenceProductFlowUuid: ecoinventFlowUuid,
      inputs: [],
      outputs: [{
        id: "provider-out",
        flowUuid: ecoinventFlowUuid,
        name: "Heat supply",
        unit: "kWh",
        amount: 1,
        isProduct: true,
        type: "technosphere" as const,
        direction: "output" as const,
        showOnNode: true,
      }],
    };

    const createdNodeId = useLcaGraphStore.getState().connectIntermediateProvider(
      consumerNodeId,
      consumerPortId,
      providerPayload,
    );
    expect(createdNodeId).toBeDefined();

    const stateAfterConnect = useLcaGraphStore.getState();
    const canvasAfterConnect = stateAfterConnect.canvases[stateAfterConnect.activeCanvasId];
    expect(canvasAfterConnect.edges.length).toBe(1);
    const edge = canvasAfterConnect.edges[0];
    expect(edge.data?.flowUuid).toBe(ecoinventFlowUuid);
    expect(edge.data?.consumerFlowUuid).toBe(tidasFlowUuid);
    expect(edge.data?.intermediateFlowLinkRuleId).toBe(ruleId);
    expect(edge.data?.intermediateFlowLinkFactor).toBeCloseTo(amountFactor, 9);
    expect(edge.data?.providerUnit).toBe("kWh");
    expect(edge.data?.consumerUnit).toBe("MJ");

    const exported = useLcaGraphStore.getState().exportGraph();
    expect(exported.exchanges.length).toBe(1);
    const exchange = exported.exchanges[0];
    expect(exchange.flowUuid).toBe(ecoinventFlowUuid);
    expect(exchange.consumerFlowUuid).toBe(tidasFlowUuid);
    expect(exchange.intermediateFlowLinkRuleId).toBe(ruleId);
    expect(exchange.intermediateFlowLinkFactor).toBeCloseTo(amountFactor, 9);
    expect(exchange.providerUnit).toBe("kWh");
    expect(exchange.consumerUnit).toBe("MJ");
    const expectedConsumerAmount = foregroundAmount * amountFactor;
    expect(exchange.amount).toBeCloseTo(expectedConsumerAmount, 9);
    expect(exchange.consumerAmount).toBeCloseTo(expectedConsumerAmount, 9);
  });

  it("drops converted edge when link ruleId does not match edge ruleId", () => {
    const consumerNodeId = "consumer-mismatch";
    const consumerPortId = "port-mismatch";
    const tidasFlowUuid = "tidas-flow-m";
    const ecoinventFlowUuid = "ecoinvent-flow-m";

    const consumerNode: Node<LcaNodeData> = {
      id: consumerNodeId,
      type: "lcaProcess",
      position: { x: 0, y: 0 },
      data: {
        nodeKind: "unit_process",
        mode: "balanced",
        processUuid: "proc-mismatch",
        name: "Mismatch Process",
        location: "CN",
        referenceProduct: "product",
        inputs: [{
          id: consumerPortId,
          flowUuid: tidasFlowUuid,
          name: "Flow M",
          unit: "MJ",
          amount: 1,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
          intermediateFlowLink: {
            sourceFlowUuid: tidasFlowUuid,
            targetFlowUuid: ecoinventFlowUuid,
            amountFactor: 0.2777777777777778,
            sourceUnit: "MJ",
            targetUnit: "kWh",
            mappingLevel: "L2",
            mappingReason: "test",
            ruleId: "rule-correct",
            ruleOrigin: "builtin",
            status: "user_confirmed",
            warnings: [],
          },
        }],
        outputs: [],
      },
    };

    const providerNode: Node<LcaNodeData> = {
      id: "provider-mismatch",
      type: "lcaProcess",
      hidden: true,
      position: { x: -360, y: 0 },
      data: {
        nodeKind: "lci_dataset",
        mode: "normalized",
        lciRole: "provider",
        processUuid: "proc-provider-m",
        name: "Provider M",
        location: "GLO",
        referenceProduct: "Product M",
        inputs: [],
        outputs: [{
          id: "provider-out-m",
          flowUuid: ecoinventFlowUuid,
          name: "Product M",
          unit: "kWh",
          amount: 1,
          isProduct: true,
          type: "technosphere",
          direction: "output",
          showOnNode: true,
        }],
      },
    };

    const mismatchedEdge = {
      id: "edge-mismatch",
      source: "provider-mismatch",
      target: consumerNodeId,
      sourceHandle: `out:provider-out-m`,
      targetHandle: `in:${consumerPortId}`,
      data: {
        flowUuid: ecoinventFlowUuid,
        flowName: "Product M",
        quantityMode: "dual" as const,
        amount: 0.2777777777777778,
        providerAmount: 1,
        consumerAmount: 0.2777777777777778,
        unit: "kWh",
        type: "technosphere" as const,
        allocation: "none" as const,
        consumerFlowUuid: tidasFlowUuid,
        providerUnit: "kWh",
        consumerUnit: "MJ",
        intermediateFlowLinkRuleId: "rule-wrong",
        intermediateFlowLinkFactor: 0.2777777777777778,
      },
    };

    useLcaGraphStore.setState((state) => {
      const active = state.canvases[state.activeCanvasId];
      return {
        uiLanguage: "zh",
        canvases: {
          ...state.canvases,
          [state.activeCanvasId]: {
            ...active,
            nodes: [consumerNode, providerNode],
            edges: [mismatchedEdge],
          },
        },
        nodes: [consumerNode, providerNode],
        edges: [mismatchedEdge],
      };
    });

    const exported = useLcaGraphStore.getState().exportGraph();
    const matchingExchanges = exported.exchanges.filter(
      (e) => e.consumerFlowUuid === tidasFlowUuid && e.flowUuid === ecoinventFlowUuid,
    );
    expect(matchingExchanges.length).toBe(0);
  });

  it("reuses existing hidden provider node with same processUuid instead of creating duplicate", () => {
    const consumerNodeId = "consumer-reuse";
    const consumerPortId1 = "port-reuse-1";
    const consumerPortId2 = "port-reuse-2";
    const tidasFlow1 = "tidas-flow-1";
    const tidasFlow2 = "tidas-flow-2";
    const ecoinventFlow = "ecoinvent-heat";
    const providerProcessUuid = "proc-provider-reuse";

    const consumerNode: Node<LcaNodeData> = {
      id: consumerNodeId,
      type: "lcaProcess",
      position: { x: 0, y: 0 },
      data: {
        nodeKind: "unit_process",
        mode: "balanced",
        processUuid: "proc-consumer-reuse",
        name: "Consumer Reuse",
        location: "CN",
        referenceProduct: "product",
        inputs: [
          {
            id: consumerPortId1,
            flowUuid: tidasFlow1,
            name: "Flow 1",
            unit: "MJ",
            amount: 1,
            type: "technosphere",
            direction: "input",
            showOnNode: true,
            intermediateFlowLink: {
              sourceFlowUuid: tidasFlow1,
              targetFlowUuid: ecoinventFlow,
              amountFactor: 1,
              sourceUnit: "MJ",
              targetUnit: "kWh",
              mappingLevel: "L1",
              mappingReason: "auto",
              ruleId: "rule-1",
              ruleOrigin: "builtin",
              status: "auto",
              warnings: [],
            },
          },
          {
            id: consumerPortId2,
            flowUuid: tidasFlow2,
            name: "Flow 2",
            unit: "MJ",
            amount: 2,
            type: "technosphere",
            direction: "input",
            showOnNode: true,
            intermediateFlowLink: {
              sourceFlowUuid: tidasFlow2,
              targetFlowUuid: ecoinventFlow,
              amountFactor: 1,
              sourceUnit: "MJ",
              targetUnit: "kWh",
              mappingLevel: "L1",
              mappingReason: "auto",
              ruleId: "rule-2",
              ruleOrigin: "builtin",
              status: "auto",
              warnings: [],
            },
          },
        ],
        outputs: [],
      },
    };

    useLcaGraphStore.setState((state) => {
      const active = state.canvases[state.activeCanvasId];
      return {
        uiLanguage: "zh",
        edges: [],
        canvases: {
          ...state.canvases,
          [state.activeCanvasId]: { ...active, nodes: [consumerNode], edges: [] },
        },
        nodes: [consumerNode],
      };
    });

    const providerPayload = {
      nodeKind: "lci_dataset" as const,
      processUuid: providerProcessUuid,
      importMode: "locked" as const,
      name: "Heat supply",
      location: "GLO",
      sourceSystem: "ecoinvent",
      referenceProduct: "Heat",
      referenceProductFlowUuid: ecoinventFlow,
      inputs: [],
      outputs: [{
        id: "provider-out",
        flowUuid: ecoinventFlow,
        name: "Heat",
        unit: "kWh",
        amount: 1,
        isProduct: true,
        type: "technosphere" as const,
        direction: "output" as const,
        showOnNode: true,
      }],
    };

    const firstNodeId = useLcaGraphStore.getState().connectIntermediateProvider(
      consumerNodeId,
      consumerPortId1,
      providerPayload,
    );
    expect(firstNodeId).toBeDefined();

    const stateAfterFirst = useLcaGraphStore.getState();
    const canvasAfterFirst = stateAfterFirst.canvases[stateAfterFirst.activeCanvasId];
    expect(canvasAfterFirst.edges.length).toBe(1);
    const hiddenNodesAfterFirst = canvasAfterFirst.nodes.filter((n) => n.hidden);
    expect(hiddenNodesAfterFirst.length).toBe(1);
    expect(hiddenNodesAfterFirst[0].data.processUuid).toBe(providerProcessUuid);

    const secondNodeId = useLcaGraphStore.getState().connectIntermediateProvider(
      consumerNodeId,
      consumerPortId2,
      providerPayload,
    );
    expect(secondNodeId).toBeDefined();
    expect(secondNodeId).toBe(firstNodeId);

    const stateAfterSecond = useLcaGraphStore.getState();
    const canvasAfterSecond = stateAfterSecond.canvases[stateAfterSecond.activeCanvasId];
    expect(canvasAfterSecond.edges.length).toBe(2);

    const hiddenNodesAfterSecond = canvasAfterSecond.nodes.filter((n) => n.hidden);
    expect(hiddenNodesAfterSecond.length).toBe(1);
    expect(hiddenNodesAfterSecond[0].id).toBe(firstNodeId);
    expect(hiddenNodesAfterSecond[0].data.processUuid).toBe(providerProcessUuid);

    const allProcessUuids = canvasAfterSecond.nodes.map((n) => n.data.processUuid);
    const uniqueProcessUuids = new Set(allProcessUuids);
    expect(uniqueProcessUuids.size).toBe(allProcessUuids.length);

    const edgesFromProvider = canvasAfterSecond.edges.filter((e) => e.source === firstNodeId);
    expect(edgesFromProvider.length).toBe(2);
    const targetHandles = edgesFromProvider.map((e) => e.targetHandle);
    expect(targetHandles).toContain(`in:${consumerPortId1}`);
    expect(targetHandles).toContain(`in:${consumerPortId2}`);
  });

  it("preserves shared hidden provider when still referenced by another edge", () => {
    const consumerNode1Id = "consumer-shared-1";
    const consumerNode2Id = "consumer-shared-2";
    const consumerPort1Id = "port-shared-1";
    const consumerPort2Id = "port-shared-2";
    const tidasFlow1 = "tidas-shared-1";
    const tidasFlow2 = "tidas-shared-2";
    const ecoinventFlow = "ecoinvent-shared";
    const providerProcessUuid = "proc-provider-shared";

    const consumerNode1: Node<LcaNodeData> = {
      id: consumerNode1Id,
      type: "lcaProcess",
      position: { x: 0, y: 0 },
      data: {
        nodeKind: "unit_process",
        mode: "balanced",
        processUuid: "proc-consumer-1",
        name: "Consumer 1",
        location: "CN",
        referenceProduct: "product",
        inputs: [{
          id: consumerPort1Id,
          flowUuid: tidasFlow1,
          name: "Flow 1",
          unit: "MJ",
          amount: 1,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
          intermediateFlowLink: {
            sourceFlowUuid: tidasFlow1,
            targetFlowUuid: ecoinventFlow,
            amountFactor: 1,
            sourceUnit: "MJ",
            targetUnit: "kWh",
            mappingLevel: "L1",
            mappingReason: "auto",
            ruleId: "rule-shared-1",
            ruleOrigin: "builtin",
            status: "auto",
            warnings: [],
          },
        }],
        outputs: [],
      },
    };

    const consumerNode2: Node<LcaNodeData> = {
      id: consumerNode2Id,
      type: "lcaProcess",
      position: { x: 400, y: 0 },
      data: {
        nodeKind: "unit_process",
        mode: "balanced",
        processUuid: "proc-consumer-2",
        name: "Consumer 2",
        location: "CN",
        referenceProduct: "product",
        inputs: [{
          id: consumerPort2Id,
          flowUuid: tidasFlow2,
          name: "Flow 2",
          unit: "MJ",
          amount: 2,
          type: "technosphere",
          direction: "input",
          showOnNode: true,
          intermediateFlowLink: {
            sourceFlowUuid: tidasFlow2,
            targetFlowUuid: ecoinventFlow,
            amountFactor: 1,
            sourceUnit: "MJ",
            targetUnit: "kWh",
            mappingLevel: "L1",
            mappingReason: "auto",
            ruleId: "rule-shared-2",
            ruleOrigin: "builtin",
            status: "auto",
            warnings: [],
          },
        }],
        outputs: [],
      },
    };

    useLcaGraphStore.setState((state) => {
      const active = state.canvases[state.activeCanvasId];
      return {
        uiLanguage: "zh",
        edges: [],
        canvases: {
          ...state.canvases,
          [state.activeCanvasId]: { ...active, nodes: [consumerNode1, consumerNode2], edges: [] },
        },
        nodes: [consumerNode1, consumerNode2],
      };
    });

    const providerPayload = {
      nodeKind: "lci_dataset" as const,
      processUuid: providerProcessUuid,
      importMode: "locked" as const,
      name: "Shared Heat",
      location: "GLO",
      sourceSystem: "ecoinvent",
      referenceProduct: "Heat",
      referenceProductFlowUuid: ecoinventFlow,
      inputs: [],
      outputs: [{
        id: "provider-out-shared",
        flowUuid: ecoinventFlow,
        name: "Heat",
        unit: "kWh",
        amount: 1,
        isProduct: true,
        type: "technosphere" as const,
        direction: "output" as const,
        showOnNode: true,
      }],
    };

    const firstNodeId = useLcaGraphStore.getState().connectIntermediateProvider(
      consumerNode1Id,
      consumerPort1Id,
      providerPayload,
    );
    expect(firstNodeId).toBeDefined();

    const secondNodeId = useLcaGraphStore.getState().connectIntermediateProvider(
      consumerNode2Id,
      consumerPort2Id,
      providerPayload,
    );
    expect(secondNodeId).toBe(firstNodeId);

    const stateFinal = useLcaGraphStore.getState();
    const canvasFinal = stateFinal.canvases[stateFinal.activeCanvasId];
    const hiddenNodes = canvasFinal.nodes.filter((n) => n.hidden);
    expect(hiddenNodes.length).toBe(1);
    expect(hiddenNodes[0].data.processUuid).toBe(providerProcessUuid);

    const edgesFromProvider = canvasFinal.edges.filter((e) => e.source === firstNodeId);
    expect(edgesFromProvider.length).toBe(2);
    const edgeTargets = edgesFromProvider.map((e) => ({ target: e.target, targetHandle: e.targetHandle }));
    expect(edgeTargets).toContainEqual({ target: consumerNode1Id, targetHandle: `in:${consumerPort1Id}` });
    expect(edgeTargets).toContainEqual({ target: consumerNode2Id, targetHandle: `in:${consumerPort2Id}` });

    useLcaGraphStore.getState().disconnectIntermediateProvider(consumerNode1Id, consumerPort1Id);
    const stateAfterDetach = useLcaGraphStore.getState();
    const canvasAfterDetach = stateAfterDetach.canvases[stateAfterDetach.activeCanvasId];
    expect(canvasAfterDetach.edges).toHaveLength(1);
    expect(canvasAfterDetach.edges[0].target).toBe(consumerNode2Id);
    expect(canvasAfterDetach.nodes.some((item) => item.id === firstNodeId)).toBe(true);
  });

  it("exports converted edge after reconnecting to existing hidden provider from loaded snake_case graph", () => {
    const sourceFlowUuid = "4d0361a3-56cc-45f9-aa42-bb9103285bf9";
    const targetFlowUuid = "66c93e71-f32b-4591-901c-55395db5c132";
    const providerProcessUuid = "proc-existing-provider";
    const ruleId = "rule-loaded";
    const amountFactor = 0.2777777777777778;
    const foregroundAmount = 3.6;

    useLcaGraphStore.getState().importGraph({
      functionalUnit: "1 kg test",
      nodes: [
        {
          id: "consumer-loaded",
          node_kind: "unit_process",
          mode: "balanced",
          process_uuid: "proc-consumer-loaded",
          name: "Consumer Loaded",
          location: "CN",
          reference_product: "product",
          inputs: [{
            id: "port-loaded",
            flowUuid: sourceFlowUuid,
            name: "热能",
            unit: "MJ",
            amount: foregroundAmount,
            type: "technosphere",
            direction: "input",
            showOnNode: true,
            intermediate_flow_link: {
              source_flow_uuid: sourceFlowUuid,
              target_flow_uuid: targetFlowUuid,
              amount_factor: amountFactor,
              source_unit: "MJ",
              target_unit: "kWh",
              mapping_level: "L2",
              mapping_reason: "unit_conversion",
              rule_id: ruleId,
              rule_origin: "builtin",
              status: "user_confirmed",
              warnings: [],
            },
          }] as unknown as import("../../model/node").FlowPort[],
          outputs: [],
        },
        {
          id: "hidden-provider",
          node_kind: "lci_dataset",
          mode: "normalized",
          lci_role: "provider",
          hidden: true,
          process_uuid: providerProcessUuid,
          name: "Heat supply",
          location: "GLO",
          reference_product: "Heat",
          inputs: [],
          outputs: [{
            id: "provider-out-loaded",
            flowUuid: targetFlowUuid,
            name: "Heat",
            unit: "kWh",
            amount: 1,
            isProduct: true,
            type: "technosphere",
            direction: "output",
            showOnNode: true,
          }],
        },
      ],
      exchanges: [],
    });

    const stateAfterImport = useLcaGraphStore.getState();
    const canvasAfterImport = stateAfterImport.canvases[stateAfterImport.activeCanvasId];
    expect(canvasAfterImport.edges.length).toBe(0);
    const hiddenNodesAfterImport = canvasAfterImport.nodes.filter((n) => n.hidden);
    expect(hiddenNodesAfterImport.length).toBe(1);
    expect(hiddenNodesAfterImport[0].data.processUuid).toBe(providerProcessUuid);

    const consumerPort = canvasAfterImport.nodes
      .find((n) => n.id === "consumer-loaded")!
      .data.inputs[0];
    expect(consumerPort.intermediateFlowLink).toBeDefined();
    expect(consumerPort.intermediateFlowLink!.status).toBe("user_confirmed");
    expect(consumerPort.unit).toBe("MJ");

    const providerPayload = {
      nodeKind: "lci_dataset" as const,
      processUuid: providerProcessUuid,
      importMode: "locked" as const,
      name: "Heat supply",
      location: "GLO",
      sourceSystem: "ecoinvent",
      referenceProduct: "Heat",
      referenceProductFlowUuid: targetFlowUuid,
      inputs: [],
      outputs: [{
        id: "provider-out-new",
        flowUuid: targetFlowUuid,
        name: "Heat",
        unit: "kWh",
        amount: 1,
        isProduct: true,
        type: "technosphere" as const,
        direction: "output" as const,
        showOnNode: true,
      }],
    };

    const reusedNodeId = useLcaGraphStore.getState().connectIntermediateProvider(
      "consumer-loaded",
      "port-loaded",
      providerPayload,
    );
    expect(reusedNodeId).toBe("hidden-provider");

    const stateAfterConnect = useLcaGraphStore.getState();
    const canvasAfterConnect = stateAfterConnect.canvases[stateAfterConnect.activeCanvasId];
    expect(canvasAfterConnect.edges.length).toBe(1);
    const edge = canvasAfterConnect.edges[0];
    expect(edge.source).toBe("hidden-provider");
    expect(edge.target).toBe("consumer-loaded");
    expect(edge.data?.flowUuid).toBe(targetFlowUuid);
    expect(edge.data?.consumerFlowUuid).toBe(sourceFlowUuid);
    expect(edge.data?.intermediateFlowLinkRuleId).toBe(ruleId);
    expect(edge.data?.intermediateFlowLinkFactor).toBeCloseTo(amountFactor, 9);

    const exported = useLcaGraphStore.getState().exportGraph();
    expect(exported.exchanges.length).toBe(1);
    const exchange = exported.exchanges[0];
    expect(exchange.flowUuid).toBe(targetFlowUuid);
    expect(exchange.consumerFlowUuid).toBe(sourceFlowUuid);
    expect(exchange.intermediateFlowLinkRuleId).toBe(ruleId);
    expect(exchange.intermediateFlowLinkFactor).toBeCloseTo(amountFactor, 9);
    expect(exchange.providerUnit).toBe("kWh");
    expect(exchange.consumerUnit).toBe("MJ");
    const expectedConsumerAmount = foregroundAmount * amountFactor;
    expect(exchange.amount).toBeCloseTo(expectedConsumerAmount, 9);
    expect(exchange.consumerAmount).toBeCloseTo(expectedConsumerAmount, 9);
  });
});
