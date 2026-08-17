export type FlowDirection = "input" | "output";
export type ExchangeType = "technosphere" | "biosphere" | "energy";
export type LcaNodeKind = "unit_process" | "market_process" | "pts_module" | "lci_dataset";
export type LciRole = "provider" | "waste_sink";
export type ProcessMode = "balanced" | "normalized";
export type AllocationBasisMethod = "manual_factor" | "quantity" | "density" | "heating_value" | "custom_conversion";

export type AllocationBasis = {
  method: AllocationBasisMethod;
  propertyType?: string;
  sourceFlowUuid?: string;
  sourcePropertyType?: string;
  value?: number;
  factor?: number;
  conversionFactor?: number;
  targetUnitGroup?: string;
  targetUnit?: string;
  basisUnit?: string;
  source?: string;
  note?: string;
};

export type UnitGroupSwitchSnapshot = {
  sourceFlowUuid?: string;
  sourceUnitGroup?: string;
  sourceUnit?: string;
  sourceReferenceUnit?: string;
  sourceAmount?: number;
  sourceExternalSaleAmount?: number;
  targetUnitGroup: string;
  targetUnit: string;
  targetReferenceUnit: string;
  factor: number;
  source?: string;
  note?: string;
};

export type IntermediateFlowLink = {
  sourceFlowUuid: string;
  targetFlowUuid: string;
  amountFactor: number;
  sourceUnit: string;
  targetUnit: string;
  sourceUnitGroup?: string;
  targetUnitGroup?: string;
  mappingLevel: "L1" | "L2" | "L3";
  mappingReason: string;
  ruleId: string;
  ruleOrigin: "builtin" | "user" | "explicit";
  status: "auto" | "user_confirmed" | "inactive";
  packageId?: string;
  packageVersion?: string;
  packageHash?: string;
  applicationMode?: "strict_identity" | "auto_compatible";
  flowSubtypeOverride?: boolean;
  warnings?: string[];
};

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
  allocationFactor?: number | null;
  allocationBasis?: AllocationBasis | null;
  unitGroupSwitch?: UnitGroupSwitchSnapshot | null;
  externalSaleAmount?: number;
  type: ExchangeType;
  direction: FlowDirection;
  showOnNode: boolean;
  internalExposed?: boolean;
  exposureMode?: "boundary_only" | "force_product_expose";
  dbMapping?: string;
  sourceSystem?: string;
  intermediateFlowLink?: IntermediateFlowLink;
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
  sourceSystem?: string;
  referenceProduct: string;
  referenceProductFlowUuid?: string;
  referenceProductDirection?: FlowDirection;
  referenceYear?: number;
  timeRepresentativeness?: string;
  technologyDescription?: string;
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
