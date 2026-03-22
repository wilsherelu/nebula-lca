import type { ExchangeType } from "./node";
import type { FlowPort, LcaNodeKind, LciRole } from "./node";

export type LcaEdgeData = {
  flowUuid: string;
  flowName: string;
  flowNameEn?: string;
  quantityMode: "single" | "dual";
  amount: number;
  providerAmount?: number;
  consumerAmount?: number;
  unit: string;
  type: ExchangeType;
  allocation: "physical" | "economic" | "none";
  dbMapping?: string;
};

export type LcaExchange = {
  id: string;
  fromNode: string;
  toNode: string;
  source_port_id?: string;
  target_port_id?: string;
  sourceHandle?: string;
  targetHandle?: string;
  flowUuid: string;
  flowName: string;
  flowNameEn?: string;
  quantityMode: "single" | "dual";
  amount: number;
  providerAmount?: number;
  consumerAmount?: number;
  unit: string;
  type: ExchangeType;
  allocation: "physical" | "economic" | "none";
  dbMapping?: string;
};

export type LcaGraphPayload = {
  functionalUnit: string;
  metadata?: Record<string, unknown>;
  nodes: Array<{
    id: string;
    node_kind: LcaNodeKind;
    mode?: "balanced" | "normalized";
    market_allow_mixed_flows?: boolean;
    lci_role?: LciRole;
    pts_uuid?: string;
    pts_published_version?: number;
    pts_published_artifact_id?: string;
    process_uuid: string;
    name: string;
    location: string;
    reference_product: string;
    reference_product_flow_uuid?: string;
    reference_product_direction?: "input" | "output";
    inputs: FlowPort[];
    outputs: FlowPort[];
  }>;
  exchanges: LcaExchange[];
};
