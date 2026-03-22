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


class ProjectCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    reference_product: str | None = None
    functional_unit: str | None = None
    system_boundary: str | None = None
    time_representativeness: str | None = None
    geography: str | None = None
    description: str | None = None


class ProjectOut(BaseModel):
    project_id: str
    name: str
    reference_product: str | None = None
    functional_unit: str | None = None
    system_boundary: str | None = None
    time_representativeness: str | None = None
    geography: str | None = None
    description: str | None = None
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
    handle_validation: dict | None = None


class DeleteProjectResponse(BaseModel):
    project_id: str
    deleted_models: int
    deleted_versions: int
    cleared_run_job_refs: int


class RunRequest(BaseModel):
    graph: HybridGraph
    model_version_id: str | None = None
    project_id: str | None = None
    force_recompile: bool = False


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
    emissions: list[ImportedProcessPortItem] = Field(default_factory=list)

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


class UnitDefinitionOut(BaseModel):
    unit_group: str
    unit_name: str
    factor_to_reference: float
    is_reference: bool


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


