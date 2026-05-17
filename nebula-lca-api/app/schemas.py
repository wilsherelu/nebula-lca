from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator


NodeKind = Literal["unit_process", "market_process", "lci_dataset", "pts_module"]
LciRole = Literal["provider", "waste_sink"]
ExchangeType = Literal["technosphere", "biosphere"]
ProcessMode = Literal["balanced", "normalized"]
QuantityMode = Literal["single", "dual"]
ProcessImportMode = Literal["locked", "editable_clone"]
ProcessTargetKind = Literal["unit_process", "market_process", "lci_dataset", "pts_module"]
TidasUpsertMode = Literal["skip", "update"]


_FLOW_SEMANTIC_ALIAS_MAP: dict[str, str] = {
    "elementary flow": "elementary_flow",
    "elementary_flow": "elementary_flow",
    "basic flow": "elementary_flow",
    "basic_flow": "elementary_flow",
    "biosphere": "elementary_flow",
    "product flow": "product_flow",
    "product_flow": "product_flow",
    "technosphere": "product_flow",
    "waste flow": "waste_flow",
    "waste_flow": "waste_flow",
    "intermediate_flow": "intermediate_flow",
}


def normalize_flow_semantic(value: object) -> str:
    token = str(value or "").strip().lower()
    if not token:
        return ""
    return _FLOW_SEMANTIC_ALIAS_MAP.get(token, token.replace(" ", "_"))


def is_elementary_flow_semantic(value: object) -> bool:
    return normalize_flow_semantic(value) == "elementary_flow"


def is_product_flow_semantic(value: object) -> bool:
    return normalize_flow_semantic(value) == "product_flow"


def is_waste_flow_semantic(value: object) -> bool:
    return normalize_flow_semantic(value) == "waste_flow"


def graph_exchange_type_to_flow_semantic(value: object) -> str:
    normalized = normalize_flow_semantic(value)
    if normalized == "elementary_flow":
        return "elementary_flow"
    if normalized == "waste_flow":
        return "waste_flow"
    if normalized in {"product_flow", "intermediate_flow"}:
        return "product_flow"
    return "product_flow"


def flow_semantic_to_exchange_type(value: object) -> ExchangeType:
    normalized = normalize_flow_semantic(value)
    if normalized == "elementary_flow":
        return "biosphere"
    return "technosphere"


class FlowPort(BaseModel):
    id: str
    legacy_port_id: str | None = Field(default=None, alias="legacyPortId")
    flowUuid: str
    name: str
    flow_name_en: str | None = None
    display_name_en: str | None = None
    unit: str
    unitGroup: str | None = None
    amount: float
    externalSaleAmount: float | None = None
    type: ExchangeType
    direction: Literal["input", "output"]
    showOnNode: bool = True
    internalExposed: bool | None = None
    dbMapping: str | None = None
    source_process_uuid: str | None = Field(default=None, alias="sourceProcessUuid")
    source_process_name: str | None = Field(default=None, alias="sourceProcessName")
    source_node_id: str | None = Field(default=None, alias="sourceNodeId")
    isProduct: bool | None = False
    allocationFactor: float | None = None
    allocationBasis: dict | None = None
    unitGroupSwitch: dict | None = None
    product_key: str | None = None
    port_key: str | None = None
    reference_product_flow_uuid: str | None = None
    product_name: str | None = None
    product_name_en: str | None = None
    model_config = ConfigDict(populate_by_name=True)

    @field_validator("isProduct", mode="before")
    @classmethod
    def normalize_is_product(cls, value: object) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        return bool(value)


def _normalize_node_kind_value(value: object) -> object:
    if not isinstance(value, str):
        return value
    return value.strip()


def _normalize_process_target_kind_value(value: object) -> object:
    return _normalize_node_kind_value(value)


class HybridNode(BaseModel):
    id: str
    node_kind: NodeKind = Field(alias="node_kind")
    mode: ProcessMode
    lci_role: LciRole | None = Field(default=None, alias="lci_role")
    pts_uuid: str | None = Field(default=None, alias="pts_uuid")
    pts_published_version: int | None = Field(default=None, alias="pts_published_version")
    pts_published_artifact_id: str | None = Field(default=None, alias="pts_published_artifact_id")
    process_uuid: str = Field(alias="process_uuid")
    name: str
    location: str
    reference_product: str = Field(alias="reference_product")
    allocation_method: Literal["unit_group_physical_v1", "custom_factor_v1"] | None = Field(
        default=None,
        alias="allocation_method",
    )
    inputs: list[FlowPort]
    outputs: list[FlowPort]
    emissions: list[FlowPort] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("node_kind", mode="before")
    @classmethod
    def normalize_node_kind_aliases(cls, value: object) -> object:
        return _normalize_node_kind_value(value)

    @model_validator(mode="after")
    def normalize_mode_default(self) -> "HybridNode":
        merged_inputs = list(self.inputs or [])
        merged_outputs = list(self.outputs or [])
        for port in list(self.emissions or []):
            if str(port.direction or "") == "input":
                merged_inputs.append(port)
            else:
                port.direction = "output"
                merged_outputs.append(port)
        self.inputs = merged_inputs
        self.outputs = merged_outputs
        self.emissions = []

        if self.node_kind in {"unit_process", "market_process"}:
            return self
        if self.node_kind == "pts_module":
            self.mode = "normalized"
            if not self.pts_uuid:
                self.pts_uuid = self.process_uuid
            if not self.process_uuid:
                self.process_uuid = self.pts_uuid
            return self
        if self.node_kind == "lci_dataset":
            self.mode = "normalized"
        return self


class HybridEdge(BaseModel):
    id: str
    fromNode: str
    toNode: str
    sourceHandle: str | None = None
    targetHandle: str | None = None
    source_port_id: str | None = Field(default=None, alias="sourcePortId")
    target_port_id: str | None = Field(default=None, alias="targetPortId")
    flowUuid: str
    flowName: str
    flow_name_en: str | None = None
    quantityMode: QuantityMode
    amount: float
    providerAmount: float | None = None
    consumerAmount: float | None = None
    unit: str
    type: ExchangeType
    allocation: Literal["physical", "economic", "none"] = "none"
    dbMapping: str | None = None
    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def normalize_quantities(self) -> "HybridEdge":
        if self.quantityMode == "single":
            if self.providerAmount is None:
                self.providerAmount = self.amount
            if self.consumerAmount is None:
                self.consumerAmount = self.amount
            if self.providerAmount != self.amount or self.consumerAmount != self.amount:
                raise ValueError("single quantityMode requires providerAmount == consumerAmount == amount")
            return self

        if self.providerAmount is None:
            raise ValueError("dual quantityMode requires providerAmount")
        if self.consumerAmount is None:
            raise ValueError("dual quantityMode requires consumerAmount")
        self.amount = self.consumerAmount
        return self


class HybridGraph(BaseModel):
    functionalUnit: str
    nodes: list[HybridNode]
    exchanges: list[HybridEdge]
    metadata: dict = Field(default_factory=dict)


def normalize_same_flow_uuid_opposite_direction_ports(graph: HybridGraph, *, tol: float = 1e-12) -> int:
    changed = 0
    for node in graph.nodes:
        if str(node.node_kind or "") == "market_process":
            continue
        input_groups: dict[str, list[FlowPort]] = {}
        output_groups: dict[str, list[FlowPort]] = {}

        for port in node.inputs:
            if is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)):
                continue
            flow_uuid = str(port.flowUuid or "").strip()
            if not flow_uuid:
                continue
            input_groups.setdefault(flow_uuid, []).append(port)

        for port in node.outputs:
            if is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)):
                continue
            flow_uuid = str(port.flowUuid or "").strip()
            if not flow_uuid:
                continue
            output_groups.setdefault(flow_uuid, []).append(port)

        common_flow_uuids = set(input_groups).intersection(output_groups)
        if not common_flow_uuids:
            continue

        remove_input_ids: set[int] = set()
        remove_output_ids: set[int] = set()

        for flow_uuid in common_flow_uuids:
            inputs = input_groups.get(flow_uuid) or []
            outputs = output_groups.get(flow_uuid) or []
            if not inputs or not outputs:
                continue

            total_input = sum(float(port.amount or 0.0) for port in inputs)
            total_output = sum(float(port.amount or 0.0) for port in outputs)
            net = total_output - total_input

            for port in inputs:
                remove_input_ids.add(id(port))
            for port in outputs:
                remove_output_ids.add(id(port))

            if abs(net) <= tol:
                changed += 1
                continue

            if net > 0:
                survivor = outputs[0]
                survivor.amount = net
                survivor.isProduct = any(bool(port.isProduct) for port in outputs)
                remove_output_ids.discard(id(survivor))
            else:
                survivor = inputs[0]
                survivor.amount = -net
                survivor.isProduct = any(bool(port.isProduct) for port in inputs)
                remove_input_ids.discard(id(survivor))
            changed += 1

        if remove_input_ids:
            node.inputs = [port for port in node.inputs if id(port) not in remove_input_ids]
        if remove_output_ids:
            node.outputs = [port for port in node.outputs if id(port) not in remove_output_ids]

    return changed


class ModelCreateRequest(BaseModel):
    name: str
    graph: HybridGraph


class ModelCreateResponse(BaseModel):
    project_id: str
    version: int
    created_at: datetime
    created_new_version: bool = True
    graph_hash: str | None = None
    message: str | None = None
    pts_compile_count: int = 0
    pts_compiled_uuids: list[str] = Field(default_factory=list)
    pts_failed_count: int = 0
    pts_failed_items: list[dict] = Field(default_factory=list)


class PtsValidationItem(BaseModel):
    node_id: str
    node_name: str | None = None
    pts_uuid: str
    reason: str
    has_resource: bool = False
    active_published_version: int | None = None
    latest_published_version: int | None = None
    has_active_artifact: bool = False
    auto_repairable: bool = False


class PtsValidationSummary(BaseModel):
    ok: bool = True
    invalid_count: int = 0
    auto_repairable: bool = False
    items: list[PtsValidationItem] = Field(default_factory=list)


class ProjectIntegrityIssue(BaseModel):
    kind: Literal["pts_publication", "flow_name_sync"]
    severity: Literal["warning", "error"] = "warning"
    code: str
    message: str
    auto_repairable: bool = False
    details: dict = Field(default_factory=dict)


class ProjectIntegritySummary(BaseModel):
    ok: bool = True
    issue_count: int = 0
    auto_repairable: bool = False
    issues: list[ProjectIntegrityIssue] = Field(default_factory=list)


class ProjectCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    reference_product: str | None = None
    functional_unit: str | None = None
    system_boundary: str | None = None
    time_representativeness: str | None = None
    geography: str | None = None
    description: str | None = None
    source_policy: Literal["open_mixed", "tidas_compliant", "ecoinvent_strict", "explicit_mapped_mixed"] | None = None
    allowed_lcia_scope: Literal["ef31_only", "ecoinvent_runtime", "mapped_runtime"] | None = None


class ProjectOut(BaseModel):
    project_id: str
    name: str
    reference_product: str | None = None
    functional_unit: str | None = None
    system_boundary: str | None = None
    time_representativeness: str | None = None
    geography: str | None = None
    description: str | None = None
    source_policy: str = "open_mixed"
    allowed_lcia_scope: str = "ef31_only"
    status: str = "active"
    process_count: int = 0
    flow_count: int = 0
    created_at: datetime
    updated_at: datetime | None = None
    latest_version: int | None = None
    latest_version_created_at: datetime | None = None
    flow_name_sync_needed: bool = False
    outdated_flow_refs_count: int = 0
    outdated_flow_ref_examples: list[dict] = Field(default_factory=list)


class ProjectFlowNameSyncResponse(BaseModel):
    project_id: str
    synced: bool
    latest_version: int | None = None
    synced_port_count: int = 0
    synced_edge_count: int = 0
    cleared_pts_compile_count: int = 0
    cleared_pts_external_count: int = 0
    cleared_pts_definition_count: int = 0


class ProjectUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    reference_product: str | None = None
    functional_unit: str | None = None
    system_boundary: str | None = None
    time_representativeness: str | None = None
    geography: str | None = None
    description: str | None = None
    source_policy: Literal["open_mixed", "tidas_compliant", "ecoinvent_strict", "explicit_mapped_mixed"] | None = None
    allowed_lcia_scope: Literal["ef31_only", "ecoinvent_runtime", "mapped_runtime"] | None = None
    status: str | None = None


class ProjectDuplicateRequest(BaseModel):
    name: str | None = None


class PaginatedProjectsResponse(BaseModel):
    items: list[ProjectOut]
    total: int
    page: int
    page_size: int


class ProcessListItem(BaseModel):
    process_name: str
    process_name_en: str | None = None
    process_uuid: str
    type: str
    reference_flow_uuid: str | None = None
    reference_flow_internal_id: str | None = None
    reference_flow_name: str | None = None
    input_count: int = 0
    output_count: int = 0
    used_in_projects: int = 0
    balance_status: str = "unchecked"
    last_modified: datetime | None = None


class PaginatedProcessesResponse(BaseModel):
    items: list[ProcessListItem]
    total: int
    page: int
    page_size: int


class ReferenceProcessCatalogItem(BaseModel):
    process_uuid: str
    process_name: str
    process_name_en: str | None = None
    process_kind: ProcessTargetKind = "unit_process"
    source_kind: str | None = None
    suggested_kind: ProcessTargetKind = "unit_process"
    reference_flow_uuid: str | None = None
    reference_flow_name: str | None = None
    reference_flow_internal_id: str | None = None
    exchange_count: int = 0
    unmatched_exchange_count: int = 0

    @field_validator("process_kind", "suggested_kind", mode="before")
    @classmethod
    def normalize_process_target_kind_aliases(cls, value: object) -> object:
        return _normalize_process_target_kind_value(value)


class ReferenceProcessCatalogResponse(BaseModel):
    items: list[ReferenceProcessCatalogItem]
    total: int
    page: int
    page_size: int
    query_echo: dict[str, str | int | None] | None = None


class ProcessDetailResponse(BaseModel):
    process_uuid: str
    process_name: str
    process_name_zh: str | None = None
    process_name_en: str | None = None
    type: str
    reference_flow_uuid: str | None = None
    reference_flow_internal_id: str | None = None
    reference_flow_name: str | None = None
    process_json: dict | None = None
    source_file: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DeleteProcessResponse(BaseModel):
    process_uuid: str
    deleted: int


class DeleteProcessesBatchRequest(BaseModel):
    process_uuids: list[str] = Field(min_length=1)


class DeleteProcessesBatchResponse(BaseModel):
    requested: int
    deleted: int
    not_found: list[str] = Field(default_factory=list)


class FlowListItem(BaseModel):
    flow_id: str
    flow_name: str
    flow_name_en: str | None = None
    type: str
    unit: str | None = None
    category: str | None = None
    source: str | None = None
    is_custom: bool = False
    tidas_compatible: bool = False
    tidas_unit_group: str | None = None
    tidas_flow_property_uuid: str | None = None
    tidas_reference_source: str | None = None
    allocation_properties: list[dict] = Field(default_factory=list)
    used_in_processes: int = 0
    last_modified: str | None = None


class PaginatedFlowsResponse(BaseModel):
    items: list[FlowListItem]
    total: int
    page: int
    page_size: int


class FlowCategoryItem(BaseModel):
    category: str
    count: int


class FlowCategoriesResponse(BaseModel):
    items: list[FlowCategoryItem]
    total: int


class StatsResponse(BaseModel):
    projects: int
    processes: int
    flows: int
    flows_latest_graph: int | None = None
    flows_library_total: int | None = None
    graph_processes: int | None = None
    graph_flows: int | None = None


class ModelVersionCreateRequest(BaseModel):
    graph: HybridGraph


class ModelVersionOut(BaseModel):
    project_id: str
    version: int
    created_at: datetime
    graph: HybridGraph
    source_policy: str = "open_mixed"
    allowed_lcia_scope: str = "ef31_only"
    handle_validation: dict | None = None
    flow_name_sync_needed: bool = False
    outdated_flow_refs_count: int = 0
    outdated_flow_ref_examples: list[dict] = Field(default_factory=list)
    pts_validation: PtsValidationSummary = Field(default_factory=PtsValidationSummary)
    project_integrity: ProjectIntegritySummary = Field(default_factory=ProjectIntegritySummary)


class RepairPtsPublicationsResponse(BaseModel):
    project_id: str
    repaired_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    items: list[dict] = Field(default_factory=list)


class RepairProjectIntegrityResponse(BaseModel):
    project_id: str
    repaired_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    items: list[dict] = Field(default_factory=list)


class DeleteProjectResponse(BaseModel):
    project_id: str
    deleted_models: int
    deleted_versions: int
    cleared_run_job_refs: int


class RunRequest(BaseModel):
    graph: HybridGraph
    model_version_id: str | None = None
    project_id: str | None = None
    model_id: str | None = None
    force_recompile: bool = False
    lcia_methods: list[str] = Field(default_factory=lambda: ["EF v3.1"])


class RunResponse(BaseModel):
    run_id: str
    status: str
    summary: dict
    tiangong_like_input: dict
    lci_result: dict


class ReferenceProcessOut(BaseModel):
    process_uuid: str
    process_name: str
    process_name_en: str | None = None


class FlowOut(BaseModel):
    flow_uuid: str
    flow_name: str
    flow_name_en: str | None = None
    flow_type: str
    default_unit: str
    unit_group: str
    compartment: str | None = None
    source: str | None = None
    is_custom: bool = False
    tidas_compatible: bool = False
    tidas_unit_group: str | None = None
    tidas_flow_property_uuid: str | None = None
    tidas_reference_source: str | None = None
    allocation_properties: list[dict] = Field(default_factory=list)
    source_updated_at: str | None = None


class ImportFlowsRequest(BaseModel):
    file_path: str
    sheet_name: str | int | None = None
    mapping: dict[str, str] | None = None
    replace_existing: bool = False
    default_flow_type: str | None = None
    ef31_flow_index_path: str | None = None


class ImportFlowsResponse(BaseModel):
    inserted: int
    updated: int
    skipped: int
    filtered_out: int = 0
    ef31_allow_uuid_count: int = 0
    ef31_flow_index_path: str | None = None
    errors: list[str]
    resolved_columns: dict[str, str]


class ImportProcessesRequest(BaseModel):
    path: str
    replace_existing: bool = True
    strict_reference_flow: bool = False


class ImportProcessIssue(BaseModel):
    file: str
    process_uuid: str | None = None
    reason: str


class ImportProcessesResponse(BaseModel):
    total_files: int
    total_processes: int
    inserted: int
    updated: int
    skipped: int
    failed: int
    errors: list[ImportProcessIssue]
    warnings: list[ImportProcessIssue] = Field(default_factory=list)


class FilteredExchangeEvidence(BaseModel):
    process_uuid: str
    exchange_internal_id: str | None = None
    flow_uuid: str | None = None
    reason: str


class ImportedProcessPortItem(BaseModel):
    flow_uuid: str | None = None
    flow_name: str | None = None
    unit: str | None = None
    unit_group: str | None = None
    type: ExchangeType = "technosphere"
    amount: float = 0.0
    direction: str
    is_product: bool = False


class ImportedProcessDetail(BaseModel):
    process_uuid: str
    source_process_uuid: str | None = None
    import_mode: ProcessImportMode
    process_kind: ProcessTargetKind = "unit_process"
    process_name: str
    location: str | None = None
    reference_flow_uuid: str | None = None
    reference_flow_internal_id: str | None = None
    inputs: list[ImportedProcessPortItem] = Field(default_factory=list)
    outputs: list[ImportedProcessPortItem] = Field(default_factory=list)

    @field_validator("process_kind", mode="before")
    @classmethod
    def normalize_process_kind_aliases(cls, value: object) -> object:
        return _normalize_process_target_kind_value(value)


class ProcessImportWarning(BaseModel):
    process_uuid: str
    reasons: list[str] = Field(default_factory=list)


class ImportReferenceProcessesRequest(BaseModel):
    process_uuids: list[str] = Field(min_length=1)
    import_mode: ProcessImportMode = "locked"
    target_kind: ProcessTargetKind = "unit_process"
    replace_existing: bool = True

    @field_validator("target_kind", mode="before")
    @classmethod
    def normalize_target_kind_aliases(cls, value: object) -> object:
        return _normalize_process_target_kind_value(value)


class ImportReferenceProcessesResponse(BaseModel):
    target_kind: ProcessTargetKind = "unit_process"
    imported_process_count: int
    filtered_exchange_count: int
    filtered_process_uuid_basis: str = "imported_process_uuid"
    filtered_exchanges: list[FilteredExchangeEvidence] = Field(default_factory=list)
    warnings: list[ProcessImportWarning] = Field(default_factory=list)
    imported_processes: list[ImportedProcessDetail] = Field(default_factory=list)

    @field_validator("target_kind", mode="before")
    @classmethod
    def normalize_target_kind_aliases(cls, value: object) -> object:
        return _normalize_process_target_kind_value(value)


class ProcessImportReportResponse(BaseModel):
    process_uuid: str
    source_process_uuid: str | None = None
    import_mode: ProcessImportMode | None = None
    imported_process_count: int = 0
    filtered_exchange_count: int = 0
    filtered_exchanges: list[FilteredExchangeEvidence] = Field(default_factory=list)
    warnings: list[ProcessImportWarning] = Field(default_factory=list)
    updated_at: datetime | None = None


class TidasImportRequest(BaseModel):
    dry_run: bool = False
    upsert_mode: TidasUpsertMode = "update"
    strict_mode: bool = False


class TidasModelImportRequest(BaseModel):
    dry_run: bool = False
    strict_mode: bool = False


class TidasMissingFlowSummaryItem(BaseModel):
    flow_uuid: str
    missing_count: int
    process_count: int


class TidasImportReportResponse(BaseModel):
    job_id: str
    import_type: Literal["flows", "processes", "models", "bundle"]
    source_path: str
    dry_run: bool
    upsert_mode: TidasUpsertMode
    strict_mode: bool
    total_files: int = 0
    total_records: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    imported_process_count: int = 0
    imported_exchange_count: int = 0
    filtered_exchange_count: int = 0
    filtered_exchanges: list[FilteredExchangeEvidence] = Field(default_factory=list)
    top_missing_flow_uuids: list[str] = Field(default_factory=list)
    imported_count: int = 0
    filtered_count: int = 0
    warning_count: int = 0
    failed_count: int = 0
    unresolved_count: int = 0
    summary: dict = Field(default_factory=dict)
    unresolved: list[dict] = Field(default_factory=list)
    unresolved_items: list[dict] = Field(default_factory=list)
    model_topology_empty_count: int = 0
    created_projects: list[dict] = Field(default_factory=list)
    created_at: datetime


class ProcessFilteredExchangesResponse(BaseModel):
    process_uuid: str
    filtered_exchange_count: int = 0
    filtered_exchanges: list[FilteredExchangeEvidence] = Field(default_factory=list)


class MissingFlowSummaryResponse(BaseModel):
    items: list[TidasMissingFlowSummaryItem] = Field(default_factory=list)


class DeleteFlowsResponse(BaseModel):
    deleted: int
    by_flow_uuid: str | None = None
    by_flow_type: str | None = None
    only_non_ef31: bool = False


class ResetModelsResponse(BaseModel):
    deleted_models: int
    deleted_versions: int
    cleared_run_job_refs: int
    project_id: str
    project_name: str


class UnitGroupOut(BaseModel):
    name: str
    reference_unit: str | None = None
    source_uuid: str | None = None
    source_version: str | None = None
    source_package_version: str | None = None
    source_file: str | None = None


class UnitDefinitionOut(BaseModel):
    unit_group: str
    unit_name: str
    factor_to_reference: float
    is_reference: bool
    source_uuid: str | None = None
    source_version: str | None = None
    source_package_version: str | None = None
    source_file: str | None = None


class ImportUnitGroupsRequest(BaseModel):
    file_path: str
    replace_existing: bool = False


class ImportUnitGroupsResponse(BaseModel):
    groups_inserted: int
    groups_updated: int
    units_inserted: int
    units_updated: int
    errors: list[str]


class UnitConvertRequest(BaseModel):
    value: float
    from_unit: str
    to_unit: str
    unit_group: str | None = None


class UnitConvertResponse(BaseModel):
    value: float
    from_unit: str
    to_unit: str
    unit_group: str
    converted_value: float
    from_factor_to_reference: float
    to_factor_to_reference: float


class PtsValidateRequest(BaseModel):
    graph: HybridGraph
    internal_node_ids: list[str]
    product_node_ids: list[str] | None = None


class PtsValidateResponse(BaseModel):
    ok: bool
    errors: list[str]
    warnings: list[str]
    matrix_size: int
    invertible: bool


class PtsCompileRequest(BaseModel):
    graph: HybridGraph
    pts_uuid: str
    project_id: str = "test"
    force_recompile: bool = False


class PtsCompileResponse(BaseModel):
    compile_id: str
    project_id: str
    pts_node_id: str
    pts_uuid: str
    graph_hash: str
    compile_version: int | None = None
    cached: bool
    ok: bool
    errors: list[str]
    warnings: list[str]
    matrix_size: int
    invertible: bool
    artifact: dict
    external_preview: dict | None = None


class PtsCompiledGetResponse(BaseModel):
    project_id: str
    pts_uuid: str
    pts_node_id: str
    graph_hash: str
    compile_version: int | None = None
    ok: bool
    errors: list[str]
    warnings: list[str]
    matrix_size: int
    invertible: bool
    definition: dict
    artifact: dict


class PtsCompiledExternalResponse(BaseModel):
    project_id: str
    pts_uuid: str
    pts_node_id: str
    graph_hash: str
    published_version: int | None = None
    source_compile_id: str | None = None
    source_compile_version: int | None = None
    ok: bool
    errors: list[str]
    warnings: list[str]
    matrix_size: int
    invertible: bool
    external_boundary: dict
    virtual_processes: list[dict]


class PtsResourceOut(BaseModel):
    project_id: str
    pts_uuid: str
    name: str | None = None
    pts_node_id: str | None = None
    latest_graph_hash: str | None = None
    compiled_graph_hash: str | None = None
    latest_compile_version: int | None = None
    latest_published_version: int | None = None
    active_published_version: int | None = None
    published_at: datetime | None = None
    ports_policy: dict = Field(default_factory=dict)
    shell_node: dict = Field(default_factory=dict)
    pts_graph: dict = Field(default_factory=dict)


class PtsResourceUpdateRequest(BaseModel):
    project_id: str
    name: str | None = None
    pts_node_id: str | None = None
    latest_graph_hash: str | None = None
    active_published_version: int | None = None
    pts_graph: dict = Field(default_factory=dict)
    ports_policy: dict = Field(default_factory=dict)
    shell_node: dict = Field(default_factory=dict)


class PtsPublishRequest(BaseModel):
    project_id: str
    compile_id: str | None = None
    compile_version: int | None = None
    graph_hash: str | None = None
    set_active: bool = True


class PtsModelWarning(BaseModel):
    code: str
    severity: Literal["warning", "info"] = "warning"
    message: str
    pts_uuid: str
    pts_node_id: str | None = None
    node_name: str | None = None
    expected_total: float | None = None
    actual_total: float | None = None
    evidence: list[dict] = Field(default_factory=list)


class PtsBoundaryPortHint(BaseModel):
    direction: Literal["input", "output"]
    flow_uuid: str = Field(alias="flowUuid")
    name: str | None = None
    source_process_uuid: str | None = Field(default=None, alias="sourceProcessUuid")
    source_process_name: str | None = Field(default=None, alias="sourceProcessName")
    source_node_id: str | None = Field(default=None, alias="sourceNodeId")

    model_config = ConfigDict(populate_by_name=True)


class PtsPublishResponse(BaseModel):
    project_id: str
    pts_uuid: str
    pts_node_id: str
    published_artifact_id: str
    published_version: int
    source_compile_id: str
    source_compile_version: int | None = None
    graph_hash: str
    active_published_version: int | None = None
    published_at: datetime | None = None
    external_preview: dict = Field(default_factory=dict)
    warnings: list[PtsModelWarning] = Field(default_factory=list)


class PtsPackFinalizeRequest(BaseModel):
    project_id: str
    name: str | None = None
    pts_node_id: str | None = None
    latest_graph_hash: str | None = None
    pts_graph: dict = Field(default_factory=dict)
    ports_policy: dict = Field(default_factory=dict)
    shell_node: dict = Field(default_factory=dict)
    default_visible_port_hints: list[PtsBoundaryPortHint] = Field(default_factory=list, alias="defaultVisiblePortHints")
    force_recompile: bool = False
    set_active: bool = True

    model_config = ConfigDict(populate_by_name=True)


class PtsPackFinalizeResponse(BaseModel):
    project_id: str
    pts_uuid: str
    pts_node_id: str | None = None
    compile_id: str
    compile_version: int | None = None
    published_artifact_id: str
    published_version: int
    active_published_version: int | None = None
    graph_hash: str
    shell_node: dict = Field(default_factory=dict)
    port_id_map: dict[str, str] = Field(default_factory=dict)
    default_visible_port_ids: list[str] = Field(default_factory=list, alias="defaultVisiblePortIds")
    warnings: list[PtsModelWarning] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


class PtsUnpackRequest(BaseModel):
    project_id: str | None = None
    pts_node_id: str | None = None


class PtsUnpackPortBinding(BaseModel):
    shell_port_id: str
    flow_uuid: str
    direction: Literal["input", "output"]
    source_node_id: str | None = None
    source_process_uuid: str | None = None
    internal_port_id: str | None = None
    internal_port_ids: list[str] = Field(default_factory=list)


class PtsUnpackResponse(BaseModel):
    project_id: str
    pts_uuid: str
    pts_node_id: str | None = None
    shell_node: dict = Field(default_factory=dict)
    pts_graph: dict = Field(default_factory=dict)
    port_bindings: list[PtsUnpackPortBinding] = Field(default_factory=list)
    resource: PtsResourceOut


class PtsVersionItem(BaseModel):
    id: str
    graph_hash: str
    version: int | None = None
    created_at: datetime
    updated_at: datetime
    ok: bool | None = None
    matrix_size: int | None = None
    invertible: bool | None = None
    source_compile_id: str | None = None
    source_compile_version: int | None = None


class PtsCompileHistoryResponse(BaseModel):
    project_id: str
    pts_uuid: str
    items: list[PtsVersionItem] = Field(default_factory=list)


class PtsPublishedHistoryResponse(BaseModel):
    project_id: str
    pts_uuid: str
    active_published_version: int | None = None
    items: list[PtsVersionItem] = Field(default_factory=list)


class HandleValidationRequest(BaseModel):
    graph: HybridGraph


class HandleValidationResponse(BaseModel):
    ok: bool
    issue_count: int
    issues: list[dict]


# ---------------------------------------------------------------------------
# Custom flow creation (Stage 1 — open-source)
# ---------------------------------------------------------------------------

_ALLOWED_CREATE_FLOW_TYPES: set[str] = frozenset(
    {
        "product_flow",
        "intermediate_flow",
        "waste_flow",
        # Accept common user-facing aliases and let backend normalize
        "Product flow",
        "Intermediate flow",
        "Waste flow",
    }
)


class CreateFlowRequest(BaseModel):
    """Minimal client request for creating a custom product/waste flow."""

    flow_name: str = Field(min_length=1, max_length=255, description="Display name of the flow")
    flow_name_en: str | None = Field(default=None, max_length=255, description="English display name")
    flow_type: str = Field(description="Semantic flow type; accepted values: product_flow, intermediate_flow, waste_flow (and compatible aliases)")
    unit_group_uuid: str = Field(alias="unitGroupUuid", min_length=1, description="UUID / name of an existing unit group")
    default_unit: str = Field(min_length=1, max_length=64, description="Unit name belonging to unit_group_uuid")
    category: str | None = Field(default=None, max_length=255, description="Compartment / category path (e.g. 'Emission; Air; GHG')")
    confirm_create: bool = Field(default=False, alias="confirmCreate", description="If true, allow creation even when duplicate names exist")
    tidas_compatible: bool = Field(default=False, alias="tidasCompatible")
    tidas_unit_group: str | None = Field(default=None, alias="tidasUnitGroup", max_length=128)
    tidas_flow_property_uuid: str | None = Field(default=None, alias="tidasFlowPropertyUuid", max_length=64)
    tidas_reference_source: str | None = Field(default=None, alias="tidasReferenceSource", max_length=128)
    source_policy: str | None = Field(default=None, alias="sourcePolicy", max_length=32)

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("flow_type", mode="before")
    @classmethod
    def validate_flow_type(cls, value: object) -> str:
        normalized = normalize_flow_semantic(value)
        if normalized not in {"product_flow", "intermediate_flow", "waste_flow"}:
            raise ValueError(
                f"Unsupported flow_type '{value}'. Allowed: product_flow, intermediate_flow, waste_flow (and compatible aliases)"
            )
        return str(value)  # keep original string; DB stores canonical form like "Product flow"


class TidasFlowCompatibilityUpdateRequest(BaseModel):
    tidas_compatible: bool = Field(alias="tidasCompatible")
    tidas_unit_group: str | None = Field(default=None, alias="tidasUnitGroup", max_length=128)
    tidas_flow_property_uuid: str | None = Field(default=None, alias="tidasFlowPropertyUuid", max_length=64)
    tidas_reference_source: str | None = Field(default=None, alias="tidasReferenceSource", max_length=128)

    model_config = ConfigDict(populate_by_name=True)


class FlowAllocationProperty(BaseModel):
    property_type: str = Field(alias="propertyType", min_length=1, max_length=64)
    value: float = Field(gt=0)
    basis_unit: str | None = Field(default=None, alias="basisUnit", max_length=64)
    target_unit_group: str = Field(alias="targetUnitGroup", min_length=1, max_length=128)
    target_unit: str | None = Field(default=None, alias="targetUnit", max_length=64)
    source: str | None = Field(default=None, max_length=128)
    note: str | None = Field(default=None, max_length=512)

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("property_type", mode="before")
    @classmethod
    def validate_property_type(cls, value: object) -> str:
        normalized = str(value or "").strip()
        allowed = {
            "density",
            "heating_value_lhv",
            "heating_value_hhv",
            "dry_matter",
            "purity",
            "carbon_content",
            "economic_value",
            "custom_conversion",
        }
        if normalized not in allowed:
            raise ValueError(f"Unsupported allocation property type '{value}'.")
        return normalized


class FlowAllocationPropertiesUpdateRequest(BaseModel):
    properties: list[FlowAllocationProperty] = Field(default_factory=list, max_length=32)

    model_config = ConfigDict(populate_by_name=True)


class FlowAllocationPropertiesResponse(BaseModel):
    flow_uuid: str
    properties: list[FlowAllocationProperty] = Field(default_factory=list)


class FlowOutExtended(FlowOut):
    """FlowOut with added custom-flow metadata fields."""

    source: str | None = None
    is_custom: bool = False
    tidas_compatible: bool = False
    tidas_unit_group: str | None = None
    tidas_flow_property_uuid: str | None = None
    tidas_reference_source: str | None = None


class FlowCandidate(BaseModel):
    """Candidate flow details returned for duplicate checks."""

    flow_uuid: str
    flow_name: str
    flow_name_en: str | None = None
    flow_type: str
    unit_group: str
    default_unit: str
    source: str | None = None
    is_custom: bool = False


class CreateFlowResponse(BaseModel):
    """Unified response for custom flow creation.

    - ``flow``: the newly created or reused flow record
    - ``warnings``: optional warnings for weak duplicates
    - ``reuse_candidates``: existing flows with same name+type but different unit
    """

    flow: FlowOutExtended
    warnings: list[str] = Field(default_factory=list)
    reuse_candidates: list[dict] = Field(default_factory=list, alias="reuseCandidates")

    model_config = ConfigDict(populate_by_name=True)


# ==================== TIDAS Export Schemas ====================


class TidasExportPreviewRequest(BaseModel):
    """Request for TIDAS bundle export preview."""

    project_id: str = Field(..., description="Project/model ID to export")
    version: int | None = Field(default=None, description="Version number (None = latest)")


class TidasExportPreviewResponse(BaseModel):
    """Response for TIDAS bundle export preview."""

    can_export: bool = Field(..., description="Whether export can proceed")
    flow_count: int = Field(default=0, description="Number of flows to be exported")
    process_count: int = Field(default=0, description="Number of processes to be exported")
    exported_model_count: int = Field(default=0, description="Number of models to be exported")
    multi_product_process_count: int = Field(default=0, description="Number of multi-product processes")
    allocation_warnings: list[dict] = Field(default_factory=list, description="Allocation-related warnings")
    manual_allocation_required_processes: list[str] = Field(default_factory=list, description="Processes requiring manual allocation")
    reference_flow_by_process: dict[str, str] = Field(default_factory=dict, description="Reference flow by process UUID")
    warnings: list[dict] = Field(default_factory=list, description="Non-blocking warnings")
    errors: list[str] = Field(default_factory=list, description="Blocking errors")
    missing_flows: list[str] = Field(default_factory=list, description="List of missing flow UUIDs")
    missing_processes: list[str] = Field(default_factory=list, description="List of missing process UUIDs")

    model_config = ConfigDict(protected_namespaces=())


class TidasExportRequest(BaseModel):
    """Request for TIDAS bundle export."""

    project_id: str = Field(..., description="Project/model ID to export")
    version: int | None = Field(default=None, description="Version number (None = latest)")
    display_lang: str = Field(default="zh", description="Display language preference (zh/en)")


class TidasExportReadinessResponse(BaseModel):
    """Response for TIDAS export readiness check.

    Reports blocking issues (prevent export), warnings (non-blocking),
    and informational summary.  can_export is True only when blocking
    is empty and source policy is satisfied.
    """
    can_export: bool = Field(..., description="Whether export can proceed")
    source_policy: str = Field(default="open_mixed", description="Project source policy")
    blocking: list[dict] = Field(default_factory=list, description="Blocking issues that prevent export")
    warnings: list[dict] = Field(default_factory=list, description="Non-blocking warnings")
    info: list[dict] = Field(default_factory=list, description="Informational summary")
    # Export counts (same as preview)
    flow_count: int = Field(default=0, description="Number of flows to be exported")
    process_count: int = Field(default=0, description="Number of processes to be exported")
    exported_model_count: int = Field(default=0, description="Number of models to be exported")
    multi_product_process_count: int = Field(default=0, description="Number of multi-product processes")
    # Diagnostic lists (same as preview)
    allocation_warnings: list[dict] = Field(default_factory=list, description="Allocation-related warnings")
    manual_allocation_required_processes: list[str] = Field(default_factory=list, description="Processes requiring manual allocation")
    reference_flow_by_process: dict[str, str] = Field(default_factory=dict, description="Reference flow by process UUID")
    missing_flows: list[str] = Field(default_factory=list, description="List of missing flow UUIDs")
    missing_processes: list[str] = Field(default_factory=list, description="List of missing process UUIDs")

    model_config = ConfigDict(protected_namespaces=())


# ==================== EF 3.1 Import Schemas ====================

_EF31_DIAGNOSTIC_TYPE = "ef31.import.report.v1"


class Ef31ImportPreviewResponse(BaseModel):
    """Response from EF 3.1 LCI preview import.

    Contains archive discovery info, parsed counts, foundation data,
    dry-run summary, warnings, and missing refs.
    """
    job_id: str = Field(..., description="Unique job ID for subsequent commit/report calls")
    can_commit: bool = Field(..., description="True if preview has no blocking errors")
    limit: int = Field(..., description="Max datasets parsed in this preview")
    counts: dict = Field(..., description="Parsed counts: datasets, flows, exchanges, missing_refs, units, indicators, cfs")
    dry_run_summary: dict = Field(default_factory=dict, description="DbDryRunResult summary dict")
    warnings: list[str] = Field(default_factory=list, description="Non-blocking warnings from preview")
    errors: list[str] = Field(default_factory=list, description="Blocking errors (missing refs, parse failures)")
    expires_at: str = Field(..., description="ISO-8601 timestamp when job artifacts expire (default 24h)")
    # Archive / file discovery
    archive_name: str | None = Field(default=None, description="Original uploaded LCI archive filename")
    archive_file_discovery: dict | None = Field(default=None, description="Counts of MasterData XMLs, datasets/ SPOLDs, LCIA Excel found in archive")
    # Foundation data
    foundation: dict | None = Field(default=None, description="Parsed MasterData + LCIA Excel foundation (units, flows, indicators, CFs)")
    # Preview counts
    preview_counts: dict | None = Field(default=None, description="Detailed preview counts: spold_count, parsed_datasets, master_data fields")


class Ef31ImportCommitRequest(BaseModel):
    """Request to commit a previewed EF 3.1 LCI import job."""
    job_id: str = Field(..., description="Job ID from a successful preview")
    confirm: Literal[True] = Field(..., description="Must be true to allow commit")


class Ef31ImportCommitResponse(BaseModel):
    """Response after committing an EF 3.1 LCI import job."""
    job_id: str = Field(..., description="Job ID of the committed import")
    committed: bool = Field(..., description="True if commit succeeded")
    counts: dict = Field(default_factory=dict, description="Commit counts: flows_new, units_new, processes_new, skips, errors")
    warnings: list[str] = Field(default_factory=list, description="Non-blocking warnings from commit")
    errors: list[str] = Field(default_factory=list, description="Blocking errors from commit")
    catalog_target_kind: Literal["lci_dataset"] = Field(default="lci_dataset", description="Catalog target kind")


class Ef31RuntimeCsvResponse(BaseModel):
    """Response after generating solver runtime CSVs for an EF 3.1 job."""
    runtime_schema_version: str = Field(default="ef31-runtime-artifact-v1", description="Runtime artifact schema version")
    runtime_id: str | None = Field(default=None, description="Runtime artifact ID")
    job_id: str = Field(..., description="EF 3.1 import job ID")
    output_dir: str = Field(..., description="Directory containing solver runtime CSVs")
    artifact_dir: str | None = Field(default=None, description="Managed runtime artifact directory")
    active: bool = Field(default=True, description="Whether this runtime is the active generated runtime")
    files: dict = Field(default_factory=dict, description="Runtime artifact file names")
    flows_count: int = Field(..., description="Rows written to flow_index.csv")
    indicators_count: int = Field(..., description="Rows written to indicator_index.csv")
    factors_count: int = Field(..., description="Rows written to lcia_factors.csv")
    cf_matched: int = Field(default=0, description="Matched EF 3.1 CF rows")
    cf_unmatched: int = Field(default=0, description="Unmatched EF 3.1 CF rows")
    cf_ambiguous: int = Field(default=0, description="Ambiguous EF 3.1 CF rows")
    env_var: str = Field(default="NEBULA_LCA_EF31_DIR", description="Solver environment variable")


class Ef31ImportReportResponse(BaseModel):
    """Unified EF 3.1 import report response for preview, commit, and runtime CSV reports."""
    job_id: str = Field(..., description="EF 3.1 import job ID")
    status: str = Field(default="unknown", description="Report status, e.g. preview or committed")
    diagnostic_type: str | None = Field(default=None, description="DebugDiagnostic type")
    payload: dict = Field(default_factory=dict, description="Raw persisted report payload")
    can_commit: bool | None = None
    limit: int | None = None
    counts: dict = Field(default_factory=dict)
    dry_run_summary: dict = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    expires_at: str | None = None
    archive_name: str | None = None
    archive_file_discovery: dict | None = None
    foundation: dict | None = None
    preview_counts: dict | None = None
    committed: bool | None = None
    catalog_target_kind: str | None = None
    runtime_csv: dict | None = None
