export type FlowDirection = "input" | "output";
export type ExchangeType = "technosphere" | "biosphere" | "energy";
export type LcaNodeKind = "unit_process" | "market_process" | "pts_module" | "lci_dataset";
export type LciRole = "provider" | "waste_sink";
export type ProcessMode = "balanced" | "normalized";

export type FlowPort = {
  id: string;
  legacyPortId?: string;
  flowUuid: string;
  name: string;
  flowNameEn?: string;
  displayNameEn?: string;
  portKey?: string;
  productKey?: string;
  sourceProcessUuid?: string;
  sourceProcessName?: string;
  nestedSourceProcessName?: string;
  sourceNodeId?: string;
  unit: string;
  unitGroup?: string;
  amount: number;
  isProduct?: boolean;
  externalSaleAmount?: number;
  type: ExchangeType;
  direction: FlowDirection;
  showOnNode: boolean;
  internalExposed?: boolean;
  exposureMode?: "boundary_only" | "force_product_expose";
  dbMapping?: string;
};

export type LcaNodeData = {
  nodeKind: LcaNodeKind;
  mode: ProcessMode;
  marketAllowMixedFlows?: boolean;
  lciRole?: LciRole;
  importMode?: "locked" | "editable_clone";
  sourceProcessUuid?: string;
  importWarnings?: string[];
  filteredExchanges?: Array<{
    process_uuid?: string;
    exchange_internal_id?: string;
    flow_uuid?: string;
    reason?: string;
  }>;
  ptsUuid?: string;
  ptsCanvasId?: string;
  ptsPublishedVersion?: number;
  ptsPublishedArtifactId?: string;
  processUuid: string;
  name: string;
  location: string;
  referenceProduct: string;
  referenceProductFlowUuid?: string;
  referenceProductDirection?: FlowDirection;
  inputs: FlowPort[];
  outputs: FlowPort[];
};

export type LcaProcessTemplate = {
  id: string;
  category: string;
  nodeKind: LcaNodeKind;
  mode?: ProcessMode;
  lciRole?: LciRole;
  processUuid: string;
  name: string;
  location: string;
  referenceProduct: string;
  inputs: Array<Omit<FlowPort, "amount" | "direction" | "showOnNode"> & { amount?: number }>;
  outputs: Array<Omit<FlowPort, "amount" | "direction" | "showOnNode"> & { amount?: number }>;
};

