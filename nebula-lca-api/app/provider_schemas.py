from __future__ import annotations

import re
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from .schemas import HybridGraph


class ProviderIssue(BaseModel):
    code: str
    message: str
    severity: Literal["info", "warning", "error"] = "warning"
    path: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ProviderEngineIdentity(BaseModel):
    version: str
    commit: str | None = None


class ProviderFunctionalUnit(BaseModel):
    display_text: str
    amount: float | None = None
    flow_uuid: str | None = None
    flow_source_namespace: str | None = None
    flow_version: str | None = None
    unit: str | None = None
    unit_group_uuid: str | None = None
    unit_group_version: str | None = None


class ProviderSnapshotRef(BaseModel):
    project_id: str
    version: int
    graph_hash: str | None = None


class ProviderInlineSnapshot(BaseModel):
    schema_version: str = "provider.snapshot.v1"
    base_snapshot_ref: ProviderSnapshotRef | None = None
    graph_hash: str
    functional_unit: ProviderFunctionalUnit
    graph: HybridGraph
    source_policy: str | None = None
    allowed_lcia_scope: str | None = None
    issues: list[ProviderIssue] = Field(default_factory=list)


class ProviderModelSnapshot(ProviderInlineSnapshot):
    engine: ProviderEngineIdentity
    project_id: str
    version: int


class ProviderDemand(BaseModel):
    process_uuid: str | None = None
    reference_exchange_id: str | None = None
    reference_flow_uuid: str | None = None
    amount: float = Field(gt=0)
    unit: str
    unit_group_uuid: str | None = None
    unit_group_version: str | None = None

    @model_validator(mode="after")
    def require_one_selector(self) -> "ProviderDemand":
        selectors = [self.process_uuid, self.reference_exchange_id, self.reference_flow_uuid]
        if sum(bool(str(value or "").strip()) for value in selectors) != 1:
            raise ValueError(
                "exactly one of process_uuid, reference_exchange_id, or reference_flow_uuid is required"
            )
        return self


class ProviderElementaryFlowRef(BaseModel):
    exchange_id: str
    source_namespace: str
    flow_uuid: str
    version: str
    flow_property_uuid: str
    flow_property_version: str
    unit_group_uuid: str
    unit_group_version: str
    unit: str
    direction: Literal["input", "output"]
    compartment: str


class ProviderBackgroundProcessPin(BaseModel):
    schema_version: Literal["provider.background-process-pin.v1"] = (
        "provider.background-process-pin.v1"
    )
    consumer_exchange_id: str = Field(min_length=1)
    source_namespace: str
    process_uuid: str
    version: str
    expected_process_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_process_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    quantitative_reference_exchange_internal_id: str = Field(min_length=1)
    claim_role: Literal["partial_background_leaf"] = "partial_background_leaf"

    @model_validator(mode="after")
    def validate_exact_process_identity(self) -> "ProviderBackgroundProcessPin":
        try:
            normalized_uuid = str(uuid.UUID(self.process_uuid))
        except ValueError as exc:
            raise ValueError("process_uuid must be a canonical UUID") from exc
        if self.process_uuid != normalized_uuid:
            raise ValueError("process_uuid must be a canonical lowercase UUID")
        if not re.fullmatch(r"\d{2}\.\d{2}\.\d{3}", self.version):
            raise ValueError("version must be an exact TIDAS version such as 01.01.000")
        return self


class ProviderMappedBackgroundProcessPin(BaseModel):
    schema_version: Literal["provider.mapped-background-process-pin.v1"] = (
        "provider.mapped-background-process-pin.v1"
    )
    consumer_exchange_id: str = Field(min_length=1)
    source_flow_namespace: str = Field(min_length=1)
    source_flow_uuid: str = Field(min_length=1)
    source_flow_version: str = Field(min_length=1)
    target_flow_uuid: str = Field(min_length=1)
    source_unit: str = Field(min_length=1)
    target_unit: str = Field(min_length=1)
    amount_factor: float = Field(gt=0)
    mapping_level: Literal["L1", "L2"]
    mapping_status: Literal["auto", "user_confirmed"]
    mapping_rule_id: str = Field(min_length=1)
    mapping_package_id: str = Field(min_length=1)
    mapping_package_version: str = Field(min_length=1)
    mapping_package_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    database_release: str = Field(min_length=1)
    process_uuid: str = Field(min_length=1)
    expected_process_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_vector_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_role: Literal["partial_background_leaf"] = "partial_background_leaf"


class ProviderProjectBackgroundPin(BaseModel):
    schema_version: Literal["provider.project-background-pin.v1"] = "provider.project-background-pin.v1"
    consumer_exchange_id: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    expected_graph_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_node_id: str = Field(min_length=1)
    reference_port_id: str = Field(min_length=1)
    provider_amount_per_consumer_unit: float = Field(gt=0, allow_inf_nan=False)
    claim_role: Literal["user_declared_partial_background_leaf", "user_declared_partial_background_network"] = "user_declared_partial_background_leaf"


class ProviderProcessIdentityRef(BaseModel):
    process_uuid: str = Field(min_length=1)
    source_namespace: str = Field(min_length=1)
    version: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class ProviderNativeBackgroundClimateRequest(BaseModel):
    schema_version: Literal["provider.native-background-climate-total.request.v1"] = (
        "provider.native-background-climate-total.request.v1"
    )
    database_release: Literal["ecoinvent_3.11"]
    method_package_id: str = Field(min_length=1)
    method_package_version: str = Field(min_length=1)
    method_package_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    flow_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reference_dependency_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ProviderNativeBackgroundInventoryRequest(BaseModel):
    schema_version: Literal["provider.native-background-inventory.request.v1"] = (
        "provider.native-background-inventory.request.v1"
    )
    database_release: Literal["ecoinvent_3.11"]
    inventory_runtime_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    flow_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reference_dependency_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ProviderSolveRequest(BaseModel):
    schema_version: str = "provider.solve.request.v1"
    snapshot_ref: ProviderSnapshotRef | None = None
    inline_snapshot: ProviderInlineSnapshot | None = None
    demand: list[ProviderDemand] = Field(min_length=1)
    scenario_id: str | None = None
    operation_hash: str | None = None
    lcia_methods: list[str] | None = None
    elementary_flows: list[ProviderElementaryFlowRef] = Field(default_factory=list)
    background_process_pins: list[ProviderBackgroundProcessPin] = Field(default_factory=list)
    mapped_background_process_pins: list[ProviderMappedBackgroundProcessPin] = Field(default_factory=list)
    project_background_pins: list[ProviderProjectBackgroundPin] = Field(default_factory=list)
    native_background_climate: ProviderNativeBackgroundClimateRequest | None = None
    native_background_inventory: ProviderNativeBackgroundInventoryRequest | None = None
    process_identities: list[ProviderProcessIdentityRef] = Field(default_factory=list)
    idempotency_key: str | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def require_one_snapshot_source(self) -> "ProviderSolveRequest":
        if (self.snapshot_ref is None) == (self.inline_snapshot is None):
            raise ValueError("exactly one of snapshot_ref or inline_snapshot is required")
        if self.native_background_inventory is not None and (
            self.native_background_climate is not None or self.lcia_methods
        ):
            raise ValueError("native inventory cannot select a climate answer or LCIA method")
        if self.idempotency_key is not None:
            normalized = self.idempotency_key.strip()
            if not normalized:
                raise ValueError("idempotency_key must not be blank")
            self.idempotency_key = normalized
        return self


class ProviderActivity(BaseModel):
    process_uuid: str
    activity_amount: float
    reference_exchange_id: str
    reference_flow_uuid: str
    unit: str


class ProviderScaledExchange(BaseModel):
    exchange_id: str
    process_uuid: str
    flow_uuid: str
    flow_source_namespace: str | None = None
    flow_version: str | None = None
    flow_property_uuid: str | None = None
    flow_property_version: str | None = None
    unit_group_uuid: str | None = None
    unit_group_version: str | None = None
    unit: str
    direction: Literal["input", "output"]
    exchange_type: Literal["technosphere", "elementary"]
    boundary_role: Literal["internal", "boundary"]
    raw_amount: float
    allocation_total: float
    coefficient: float
    activity_amount: float
    scaled_amount: float


class ProviderInventoryTotal(BaseModel):
    flow_uuid: str
    flow_source_namespace: str | None = None
    flow_version: str | None = None
    flow_property_uuid: str | None = None
    flow_property_version: str | None = None
    unit_group_uuid: str | None = None
    unit_group_version: str | None = None
    unit: str
    direction: Literal["input", "output"]
    amount: float


class ProviderElementaryFlowReceipt(BaseModel):
    exchange_id: str
    source_namespace: str
    flow_uuid: str
    version: str
    flow_property_uuid: str
    flow_property_version: str
    unit_group_uuid: str
    unit_group_version: str
    unit: str
    direction: Literal["input", "output"]
    compartment: str
    content_hash: str
    snapshot_hash: str | None = None
    flow_property_content_hash: str | None = None
    unit_group_content_hash: str | None = None
    unit_content_hash: str | None = None
    reference_dependency_snapshot_hash: str | None = None
    runtime_flow_index: int | None = None
    method: str
    factor_count: int
    factor_hash: str


class ProviderTechnosphereFlowReceipt(BaseModel):
    exchange_id: str
    process_uuid: str
    resolution: Literal["tidas_exact_snapshot", "inline_custom"]
    source_namespace: str
    flow_uuid: str
    version: str
    flow_type: str | None = None
    content_hash: str | None = None
    snapshot_hash: str | None = None
    flow_property_uuid: str
    flow_property_version: str
    flow_property_content_hash: str | None = None
    unit_group_uuid: str
    unit_group_version: str
    unit_group_content_hash: str | None = None
    unit: str
    unit_content_hash: str | None = None
    reference_dependency_resolution_source: str | None = None
    reference_dependency_snapshot_hash: str | None = None


class ProviderBackgroundExchangeReceipt(BaseModel):
    exchange_internal_id: str
    expanded_exchange_id: str
    role: Literal["quantitative_reference", "elementary"]
    source_namespace: str
    flow_uuid: str
    version: str
    flow_name: str | None = None
    flow_content_hash: str
    flow_snapshot_hash: str | None = None
    flow_property_uuid: str
    flow_property_version: str
    unit_group_uuid: str
    unit_group_version: str
    unit: str
    direction: Literal["input", "output"]
    raw_amount: float
    scaled_amount: float


class ProviderBackgroundProcessReceipt(BaseModel):
    schema_version: Literal["provider.background-process-receipt.v1"] = (
        "provider.background-process-receipt.v1"
    )
    claim_role: Literal["partial_background_leaf"] = "partial_background_leaf"
    claim_limit: Literal["partial_background_leaf_only_not_complete_cradle_to_gate"] = (
        "partial_background_leaf_only_not_complete_cradle_to_gate"
    )
    consumer_exchange_id: str
    source_namespace: str
    process_uuid: str
    version: str
    process_name: str | None = None
    process_type: str
    process_content_hash: str
    process_snapshot_hash: str
    quantitative_reference_exchange_internal_id: str
    quantitative_reference_flow_uuid: str
    quantitative_reference_flow_version: str
    quantitative_reference_amount: float
    activity_amount: float
    process_scale: float
    exchanges: list[ProviderBackgroundExchangeReceipt]


class ProviderMappingDependencyReceipt(BaseModel):
    package_id: str
    package_version: str
    package_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    role: Literal[
        "public_interoperability_projection",
        "certified_elementary_identity_adjudication",
    ]


class ProviderMappedBackgroundProcessReceipt(BaseModel):
    schema_version: Literal["provider.mapped-background-process-receipt.v1"] = (
        "provider.mapped-background-process-receipt.v1"
    )
    claim_role: Literal["partial_background_leaf"] = "partial_background_leaf"
    claim_limit: Literal["partial_background_leaf_only_not_complete_cradle_to_gate"] = (
        "partial_background_leaf_only_not_complete_cradle_to_gate"
    )
    consumer_exchange_id: str
    source_flow_namespace: str
    source_flow_uuid: str
    source_flow_version: str
    target_flow_uuid: str
    source_unit: str
    target_unit: str
    amount_factor: float
    mapping_level: Literal["L1", "L2"]
    mapping_status: Literal["auto", "user_confirmed"]
    mapping_rule_id: str
    mapping_package_id: str
    mapping_package_version: str
    mapping_package_hash: str
    elementary_mapping_dependencies: list[ProviderMappingDependencyReceipt] = Field(default_factory=list)
    database_release: str
    process_uuid: str
    process_content_hash: str
    vector_checksum: str
    vector_nnz: int
    quantitative_reference_amount: float
    activity_amount: float
    process_scale: float
    shared_activity_schema_version: Literal["provider.mapped-background-shared-activity.v1"] | None = None
    provider_instance_id: str | None = None
    consumer_process_uuid: str | None = None
    consumer_converted_amount: float | None = None
    consumer_attributed_activity_amount: float | None = None
    shared_process_total_activity_amount: float | None = None
    attributed_process_scale: float | None = None


class ProviderPathWeightedContributionTerm(BaseModel):
    schema_version: Literal["provider.lcia.path-weighted-contribution-term.v1"] = (
        "provider.lcia.path-weighted-contribution-term.v1"
    )
    attribution_basis: Literal[
        "foreground_direct",
        "background_consumer_path",
        "background_unattributed",
    ]
    consumer_exchange_id: str | None = None
    consumer_process_uuid: str | None = None
    provider_instance_id: str | None = None
    background_process_content_hash: str | None = None
    mapping_rule_id: str | None = None
    mapping_package_id: str | None = None
    mapping_package_version: str | None = None
    mapping_package_hash: str | None = None
    source_flow_uuid: str | None = None
    source_flow_version: str | None = None
    target_flow_uuid: str | None = None
    amount_factor: float | None = None
    mapping_level: Literal["L1", "L2"] | None = None
    mapping_status: Literal["auto", "user_confirmed"] | None = None
    process_uuid: str
    exchange_id: str
    source_namespace: str
    flow_uuid: str
    flow_version: str
    flow_property_uuid: str | None = None
    flow_property_version: str | None = None
    unit_group_uuid: str | None = None
    unit_group_version: str | None = None
    amount: float
    amount_unit: str
    direction: Literal["input", "output"]
    compartment: str | None = None
    cf_method: str
    cf_database_release: str
    cf_indicator_key: str
    cf_version: str
    cf_presence: Literal["explicit_nonzero", "implicit_zero"]
    cf_value: float
    cf_hash: str
    path_share: float = Field(ge=0, le=1)
    consumer_attributed_activity_amount: float | None = None
    shared_process_total_activity_amount: float | None = None
    contribution_value: float
    path_contribution_value: float
    contribution_unit: str
    flow_snapshot_hash: str | None = None
    snapshot_hash: str
    provider_graph_hash: str
    solve_hash: str
    provider_commit: str | None = None
    claim_role: Literal["partial_background_leaf"] | None = None
    claim_limit: str | None = None


class ProviderPathWeightedContributionReceipt(BaseModel):
    schema_version: Literal["provider.lcia.path-weighted-contribution-receipt.v1"] = (
        "provider.lcia.path-weighted-contribution-receipt.v1"
    )
    method: str
    database_release: str
    canonical_indicator_key: str
    indicator_index: int | None = None
    indicator_unit: str
    indicator_value: float
    source_contribution_total: float
    path_weighted_total: float
    reconciliation_delta: float
    attribution_coverage: Literal["complete", "partial", "missing", "foreground_only"]
    unattributed_contribution_value: float
    terms: list[ProviderPathWeightedContributionTerm] = Field(default_factory=list)
    processes_without_path_attribution: list[dict[str, Any]] = Field(default_factory=list)
    snapshot_hash: str
    provider_graph_hash: str
    solve_hash: str
    provider_commit: str | None = None
    runtime_assets_hash: str | None = None
    content_hash: str | None = None


class ProviderProcessIdentityReceipt(BaseModel):
    process_uuid: str
    source_namespace: str
    version: str
    content_hash: str
    snapshot_hash: str | None = None
    resolution: Literal["tidas_exact_snapshot", "inline_custom"]
    verification_scope: Literal["provider_exact_snapshot", "consumer_asserted_hash"]
    process_name: str | None = None
    process_type: str | None = None


class ProviderSolveProvenance(BaseModel):
    engine: ProviderEngineIdentity
    snapshot_ref: ProviderSnapshotRef | None = None
    inline_graph_hash: str | None = None
    scenario_id: str | None = None
    operation_hash: str | None = None
    solver: str = "nebula_hybrid_matrix_v1"
    solver_version: str = "nebula_hybrid_matrix_v1"
    solver_build: str | None = None
    database_release: str
    foreground_source_release: str | None = None
    system_revision_hash: str
    consumer_graph_hash: str
    provider_graph_hash: str
    background_process_pins_hash: str | None = None
    mapped_background_process_pins_hash: str | None = None
    project_background_pins_hash: str | None = None
    background_claim_scope: str | None = None
    process_identities_hash: str | None = None
    flow_snapshot_hash: str | None = None
    reference_dependency_snapshot_hash: str | None = None
    elementary_mapping_dependencies: list[ProviderMappingDependencyReceipt] = Field(default_factory=list)
    native_climate_method_package_sha256: str | None = None
    native_climate_runtime_manifest_sha256: str | None = None
    native_inventory_runtime_manifest_sha256: str | None = None
    native_unit_binding_sha256: str | None = None
    idempotency_key: str | None = None
    request_hash: str | None = None
    activity_vector_semantics: str = "x in A*x=f"
    inventory_scope: str = "boundary elementary exchanges"


class ProviderNativeClimateTerm(BaseModel):
    exchange_id: str
    process_uuid: str
    source_namespace: Literal["ecoinvent", "tiangong_open_data"]
    flow_uuid: str
    flow_version: str
    direction: Literal["input", "output"]
    compartment: str
    subcompartment: str | None = None
    amount: float
    amount_unit: str
    cf_presence: Literal["explicit_nonzero", "explicit_zero", "no_cf"]
    cf_value: float | None = None
    cf_source: str
    cf_method: str
    cf_database_release: str
    method_package_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    contribution_value: float | None = None


class ProviderNativeClimateDomain(BaseModel):
    source_namespace: Literal["ecoinvent", "tiangong_open_data"]
    contribution_value: float
    indicator_unit: Literal["kg CO2-Eq"]
    coverage: dict[str, int]
    negative_cf_exchange_count: int
    terms: list[ProviderNativeClimateTerm]


class ProviderNativeClimateIndicator(BaseModel):
    method: Literal["EF v3.1"]
    category: Literal["climate change"]
    name: Literal["GWP100"]
    unit: Literal["kg CO2-Eq"]


class ProviderNativeBackgroundClimateResponse(BaseModel):
    schema_version: Literal["provider.native-background-climate-total.response.v1"]
    method_package_id: str
    method_package_version: str
    method_package_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    indicator: ProviderNativeClimateIndicator
    merge_status: Literal["merged", "split_diagnostic"]
    merge_blockers: list[dict[str, Any]]
    unified_climate_total: float | None = None
    domains: dict[str, ProviderNativeClimateDomain]
    provider_system_model_evidence: Literal["not_asserted_by_local_vector_metadata"]
    carbon_storage_permanence: Literal["not_assessed"]
    claim_limit: Literal["partial_background_leaf_only_not_complete_product_carbon_footprint"]


class ProviderSolveResponse(BaseModel):
    schema_version: str = "provider.solve.response.v1"
    run_id: str
    status: Literal["completed"] = "completed"
    idempotency_key: str | None = None
    request_hash: str | None = None
    snapshot_ref: ProviderSnapshotRef | None = None
    demand: list[ProviderDemand]
    activity_vector: list[ProviderActivity]
    scaled_exchanges: list[ProviderScaledExchange]
    inventory_totals: list[ProviderInventoryTotal]
    technosphere_flow_receipts: list[ProviderTechnosphereFlowReceipt] = Field(default_factory=list)
    elementary_flow_receipts: list[ProviderElementaryFlowReceipt] = Field(default_factory=list)
    background_process_receipts: list[ProviderBackgroundProcessReceipt] = Field(default_factory=list)
    mapped_background_process_receipts: list[ProviderMappedBackgroundProcessReceipt] = Field(default_factory=list)
    project_background_process_receipts: list[dict[str, Any]] = Field(default_factory=list)
    process_identity_receipts: list[ProviderProcessIdentityReceipt] = Field(default_factory=list)
    lcia: dict[str, Any] | None = None
    native_background_climate: ProviderNativeBackgroundClimateResponse | None = None
    native_background_inventory: dict[str, Any] | None = None
    process_residuals: list[dict[str, Any]] = Field(default_factory=list)
    contribution_graph: dict[str, Any] = Field(default_factory=dict)
    issues: list[ProviderIssue] = Field(default_factory=list)
    provenance: ProviderSolveProvenance


class ExactFlowRef(BaseModel):
    source_namespace: str
    flow_uuid: str
    version: str
    correlation_id: str | None = None


class ExactProcessRef(BaseModel):
    source_namespace: str
    process_uuid: str
    version: str
    correlation_id: str | None = None

    @model_validator(mode="after")
    def validate_exact_identity(self):
        try:
            normalized_uuid = str(uuid.UUID(self.process_uuid))
        except ValueError as exc:
            raise ValueError("process_uuid must be a canonical UUID") from exc
        if self.process_uuid != normalized_uuid:
            raise ValueError("process_uuid must be a canonical lowercase UUID")
        if not re.fullmatch(r"\d{2}\.\d{2}\.\d{3}", self.version):
            raise ValueError("version must be an exact TIDAS version such as 01.01.000")
        return self


class ExactFlowPropertyRef(BaseModel):
    flow_property_uuid: str
    version: str
    correlation_id: str | None = None


class ExactUnitGroupRef(BaseModel):
    unit_group_uuid: str
    version: str
    correlation_id: str | None = None


class ExactUnitRef(ExactUnitGroupRef):
    unit: str


class ProviderFlowCandidateQuery(BaseModel):
    query: str = Field(min_length=2)
    flow_type: str | None = None
    unit: str | None = None
    limit: int = Field(default=20, ge=1, le=100)
    correlation_id: str | None = None


class ProviderCatalogResolveRequest(BaseModel):
    schema_version: str = "provider.catalog.resolve.request.v1"
    processes: list[ExactProcessRef] = Field(default_factory=list)
    flows: list[ExactFlowRef] = Field(default_factory=list)
    flow_properties: list[ExactFlowPropertyRef] = Field(default_factory=list)
    unit_groups: list[ExactUnitGroupRef] = Field(default_factory=list)
    units: list[ExactUnitRef] = Field(default_factory=list)
    flow_candidates: list[ProviderFlowCandidateQuery] = Field(default_factory=list)


class ProviderCatalogResolution(BaseModel):
    kind: Literal["process", "flow", "flow_property", "unit_group", "unit"]
    key: dict[str, Any]
    status: Literal["resolved", "not_found", "unsupported"]
    value: dict[str, Any] | None = None
    code: str | None = None
    message: str | None = None


class ProviderCatalogResolveResponse(BaseModel):
    schema_version: str = "provider.catalog.resolve.response.v1"
    items: list[ProviderCatalogResolution]
    candidate_sets: list[dict[str, Any]] = Field(default_factory=list)
    issues: list[ProviderIssue] = Field(default_factory=list)
