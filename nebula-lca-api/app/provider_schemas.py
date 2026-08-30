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


class ProviderProcessIdentityRef(BaseModel):
    process_uuid: str = Field(min_length=1)
    source_namespace: str = Field(min_length=1)
    version: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


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
    process_identities: list[ProviderProcessIdentityRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_one_snapshot_source(self) -> "ProviderSolveRequest":
        if (self.snapshot_ref is None) == (self.inline_snapshot is None):
            raise ValueError("exactly one of snapshot_ref or inline_snapshot is required")
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
    runtime_flow_index: int
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
    system_revision_hash: str
    consumer_graph_hash: str
    provider_graph_hash: str
    background_process_pins_hash: str | None = None
    background_claim_scope: str | None = None
    process_identities_hash: str | None = None
    activity_vector_semantics: str = "x in A*x=f"
    inventory_scope: str = "boundary elementary exchanges"


class ProviderSolveResponse(BaseModel):
    schema_version: str = "provider.solve.response.v1"
    run_id: str
    status: Literal["completed"] = "completed"
    snapshot_ref: ProviderSnapshotRef | None = None
    demand: list[ProviderDemand]
    activity_vector: list[ProviderActivity]
    scaled_exchanges: list[ProviderScaledExchange]
    inventory_totals: list[ProviderInventoryTotal]
    technosphere_flow_receipts: list[ProviderTechnosphereFlowReceipt] = Field(default_factory=list)
    elementary_flow_receipts: list[ProviderElementaryFlowReceipt] = Field(default_factory=list)
    background_process_receipts: list[ProviderBackgroundProcessReceipt] = Field(default_factory=list)
    process_identity_receipts: list[ProviderProcessIdentityReceipt] = Field(default_factory=list)
    lcia: dict[str, Any] | None = None
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
