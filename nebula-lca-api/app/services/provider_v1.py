from __future__ import annotations

import importlib
import os
import subprocess
import uuid
from collections import defaultdict
from functools import lru_cache
from typing import Any

import numpy as np
from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..config import WORKSPACE_ROOT
from ..models import FlowVersionRecord, Model, ModelVersion, ReferenceProcess, UnitDefinition, UnitGroup
from ..provider_schemas import (
    ProviderActivity,
    ProviderCatalogResolution,
    ProviderCatalogResolveRequest,
    ProviderEngineIdentity,
    ProviderFunctionalUnit,
    ProviderInventoryTotal,
    ProviderIssue,
    ProviderModelSnapshot,
    ProviderScaledExchange,
    ProviderSnapshotRef,
    ProviderSolveProvenance,
    ProviderSolveRequest,
    ProviderSolveResponse,
    ProviderTechnosphereFlowReceipt,
)
from ..schemas import FlowPort, HybridGraph
from ..solver import to_tiangong_like
from ..solver_adapter import _ensure_embedded_solver_core
from .graph_storage import (
    compute_graph_hash_from_graph,
    compute_graph_hash_from_slim_graph,
    hydrate_graph_for_api,
)
from .provider_contract import ProviderContractError
from .provider_ef31 import (
    EF31_DATABASE_RELEASE,
    ProviderEf31Error,
    characterize_scaled_inventory,
    resolve_standard_flow,
    resolve_standard_flow_property,
    resolve_standard_unit,
    resolve_standard_unit_group,
)
from .provider_background_leaf import (
    CLAIM_LIMIT as BACKGROUND_CLAIM_LIMIT,
    ProviderBackgroundLeafError,
    build_background_process_receipts,
    expand_background_process_pins,
)
from .provider_process_identity import build_process_identity_receipts
from .provider_runtime_identity import build_solve_runtime_fingerprint
from .provider_tidas_snapshot import (
    SOURCE_NAMESPACE as TIDAS_SOURCE_NAMESPACE,
    ProviderTidasSnapshotError,
    TidasFlowSnapshot,
    configured_tidas_flow_snapshot,
)
from .provider_tidas_process_snapshot import (
    ProviderTidasProcessSnapshotError,
    TidasProcessSnapshot,
    canonical_hash as canonical_process_hash,
    configured_tidas_process_snapshot,
)
from .provider_tidas_reference_snapshot import (
    ProviderTidasReferenceSnapshotError,
    TidasReferenceDependencySnapshot,
    configured_tidas_reference_dependency_snapshot,
)


@lru_cache(maxsize=1)
def engine_identity() -> ProviderEngineIdentity:
    version = str(os.getenv("NEBULA_LCA_ENGINE_VERSION") or "0.1.0").strip()
    commit = str(os.getenv("NEBULA_LCA_ENGINE_COMMIT") or "").strip() or None
    if commit is None:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=str(WORKSPACE_ROOT),
                check=True,
                capture_output=True,
                text=True,
                timeout=3,
            )
            commit = result.stdout.strip() or None
        except (OSError, subprocess.SubprocessError):
            commit = None
    return ProviderEngineIdentity(version=version, commit=commit)


def provider_solve_runtime_fingerprint(
    db: Session,
    request: ProviderSolveRequest,
) -> str:
    return build_solve_runtime_fingerprint(
        db,
        request,
        engine=engine_identity(),
        resolve_snapshot=_resolve_solve_snapshot,
        load_flow_snapshot=_configured_tidas_snapshot,
        load_process_snapshot=_configured_tidas_process_snapshot,
    )


def _functional_unit(graph: HybridGraph, display_fallback: str | None = None) -> tuple[ProviderFunctionalUnit, list[ProviderIssue]]:
    metadata = graph.metadata or {}
    raw = None
    for key in ("functional_unit", "functionalUnitStructured", "functional_unit_structured"):
        if isinstance(metadata.get(key), dict):
            raw = metadata[key]
            break
    display_text = str(graph.functionalUnit or display_fallback or "").strip()
    if raw is None:
        return ProviderFunctionalUnit(display_text=display_text), [
            ProviderIssue(
                code="FUNCTIONAL_UNIT_NOT_STRUCTURED",
                message="The graph contains only a display functional unit; exact amount, Flow, and unit identity are unavailable.",
                path="graph.functionalUnit",
            )
        ]
    value = ProviderFunctionalUnit(
        display_text=str(raw.get("display_text") or raw.get("displayText") or display_text),
        amount=raw.get("amount"),
        flow_uuid=raw.get("flow_uuid") or raw.get("flowUuid"),
        flow_source_namespace=raw.get("flow_source_namespace") or raw.get("flowSourceNamespace"),
        flow_version=raw.get("flow_version") or raw.get("flowVersion"),
        unit=raw.get("unit"),
        unit_group_uuid=raw.get("unit_group_uuid") or raw.get("unitGroupUuid"),
        unit_group_version=raw.get("unit_group_version") or raw.get("unitGroupVersion"),
    )
    missing = [
        field
        for field in (
            "amount",
            "flow_uuid",
            "flow_source_namespace",
            "flow_version",
            "unit",
            "unit_group_uuid",
            "unit_group_version",
        )
        if getattr(value, field) is None
    ]
    issues = []
    if missing:
        issues.append(
            ProviderIssue(
                code="FUNCTIONAL_UNIT_EXACT_IDENTITY_INCOMPLETE",
                message="The structured functional unit is missing exact identity fields.",
                path="graph.metadata.functional_unit",
                details={"missing_fields": missing},
            )
        )
    return value, issues


def _graph_identity_issues(
    graph: HybridGraph,
    identified_processes: set[str] | None = None,
) -> list[ProviderIssue]:
    issues: list[ProviderIssue] = []
    identified_processes = identified_processes or set()
    required = {
        "flow_source_namespace": "flowSourceNamespace",
        "flow_version": "flowVersion",
        "flow_property_uuid": "flowPropertyUuid",
        "flow_property_version": "flowPropertyVersion",
        "unit_group_uuid": "unitGroupUuid",
        "unit_group_version": "unitGroupVersion",
    }
    for node_index, node in enumerate(graph.nodes):
        if node.process_uuid not in identified_processes:
            issues.append(
                ProviderIssue(
                    code="PROCESS_EXACT_VERSION_UNAVAILABLE",
                    message="HybridGraph identifies the process UUID but has no exact process-version field.",
                    path=f"graph.nodes[{node_index}]",
                    details={"process_uuid": node.process_uuid},
                )
            )
        for bucket in ("inputs", "outputs"):
            for port_index, port in enumerate(getattr(node, bucket)):
                missing = [alias for field, alias in required.items() if not str(getattr(port, field) or "").strip()]
                if missing:
                    issues.append(
                        ProviderIssue(
                            code="EXCHANGE_EXACT_IDENTITY_INCOMPLETE",
                            message="The exchange remains numerically usable but lacks exact catalog identity fields.",
                            path=f"graph.nodes[{node_index}].{bucket}[{port_index}]",
                            details={"exchange_id": f"{node.id}::{port.id}", "missing_fields": missing},
                        )
                    )
    return issues


def get_model_snapshot(db: Session, project_id: str, version: int) -> ProviderModelSnapshot:
    model = db.query(Model).filter(Model.id == project_id).one_or_none()
    if model is None:
        raise ProviderContractError(404, "PROJECT_NOT_FOUND", "Project was not found.", project_id=project_id)
    row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == project_id, ModelVersion.version == version)
        .one_or_none()
    )
    if row is None:
        raise ProviderContractError(
            404,
            "MODEL_VERSION_NOT_FOUND",
            "The requested exact model version was not found.",
            project_id=project_id,
            version=version,
        )
    graph_json = hydrate_graph_for_api(row.hybrid_graph_json, db)
    graph = HybridGraph.model_validate(graph_json)
    graph_hash = row.graph_hash or compute_graph_hash_from_slim_graph(row.hybrid_graph_json)
    functional_unit, issues = _functional_unit(graph, model.functional_unit)
    issues.extend(_graph_identity_issues(graph))
    identity = engine_identity()
    if identity.commit is None:
        issues.append(
            ProviderIssue(
                code="ENGINE_COMMIT_UNAVAILABLE",
                message="The runtime could not determine its source commit.",
                path="engine.commit",
            )
        )
    return ProviderModelSnapshot(
        engine=identity,
        project_id=project_id,
        version=version,
        base_snapshot_ref=ProviderSnapshotRef(project_id=project_id, version=version, graph_hash=graph_hash),
        graph_hash=graph_hash,
        functional_unit=functional_unit,
        graph=graph,
        source_policy=model.source_policy,
        allowed_lcia_scope=model.allowed_lcia_scope,
        issues=issues,
    )


def _resolve_solve_snapshot(
    db: Session,
    request: ProviderSolveRequest,
) -> tuple[HybridGraph, str, str, ProviderSnapshotRef | None, list[ProviderIssue]]:
    identified_processes = {item.process_uuid for item in request.process_identities}
    if request.snapshot_ref is not None:
        snapshot = get_model_snapshot(db, request.snapshot_ref.project_id, request.snapshot_ref.version)
        if request.snapshot_ref.graph_hash and request.snapshot_ref.graph_hash != snapshot.graph_hash:
            raise ProviderContractError(
                409,
                "SNAPSHOT_HASH_MISMATCH",
                "The exact stored model version does not match the requested graph hash.",
                expected=request.snapshot_ref.graph_hash,
                actual=snapshot.graph_hash,
            )
        issues = [
            issue
            for issue in snapshot.issues
            if not (
                issue.code == "PROCESS_EXACT_VERSION_UNAVAILABLE"
                and str(issue.details.get("process_uuid") or "") in identified_processes
            )
        ]
        return snapshot.graph, snapshot.graph_hash, snapshot.graph_hash, snapshot.base_snapshot_ref, issues

    assert request.inline_snapshot is not None
    inline = request.inline_snapshot
    provider_graph_hash = compute_graph_hash_from_graph(inline.graph)
    issues = list(inline.issues) + _graph_identity_issues(
        inline.graph,
        identified_processes=identified_processes,
    )
    if inline.graph_hash != provider_graph_hash:
        issues.append(
            ProviderIssue(
                code="GRAPH_HASH_DOMAINS_DISTINCT",
                message="The consumer exact-JSON hash and provider normalized graph hash are both retained as separate evidence.",
                severity="info",
                path="provenance",
            )
        )
    return inline.graph, inline.graph_hash, provider_graph_hash, inline.base_snapshot_ref, issues


def _dense_matrix(matrix: dict[str, Any]) -> np.ndarray:
    rows, cols = matrix.get("shape") or [0, 0]
    dense = np.zeros((int(rows), int(cols)), dtype=float)
    for entry in matrix.get("data") or []:
        dense[int(entry["row_index"]), int(entry["col_index"])] += float(entry["value"])
    return dense


def _port_from_exchange_id(graph: HybridGraph) -> dict[str, tuple[FlowPort, str]]:
    result: dict[str, tuple[FlowPort, str]] = {}
    for node in graph.nodes:
        for port in node.inputs + node.outputs:
            result[f"{node.id}::{port.id}"] = (port, node.process_uuid)
    return result


def _handle_port_id(handle: str | None) -> str:
    token = str(handle or "")
    return token.split(":", 1)[1] if ":" in token else token


def _internal_exchange_ids(graph: HybridGraph) -> set[str]:
    linked: set[str] = set()
    node_by_id = {node.id: node for node in graph.nodes}
    for edge in graph.exchanges:
        source_node = node_by_id.get(edge.fromNode)
        target_node = node_by_id.get(edge.toNode)
        source_port_id = _handle_port_id(edge.source_port_id or edge.sourceHandle)
        target_port_id = _handle_port_id(edge.target_port_id or edge.targetHandle)
        if source_node and source_port_id:
            linked.add(f"{source_node.id}::{source_port_id}")
        if target_node and target_port_id:
            linked.add(f"{target_node.id}::{target_port_id}")
    return linked


def _validate_inline_foreground(graph: HybridGraph) -> None:
    ports_by_node_and_id: dict[tuple[str, str], FlowPort] = {}
    required_identity_fields = (
        "flow_source_namespace",
        "flow_version",
        "flow_property_uuid",
        "flow_property_version",
        "unit_group_uuid",
        "unit_group_version",
    )
    for node in graph.nodes:
        for port in node.inputs + node.outputs:
            if port.type == "technosphere":
                missing = [field for field in required_identity_fields if not str(getattr(port, field) or "").strip()]
                if missing:
                    raise ProviderContractError(
                        422,
                        "CUSTOM_TECHNOSPHERE_IDENTITY_INCOMPLETE",
                        "Inline technosphere Flows must carry complete namespace/version/property/unit-group identity.",
                        exchange_id=f"{node.id}::{port.id}",
                        missing_fields=missing,
                    )
            ports_by_node_and_id[(node.id, port.id)] = port
    for edge in graph.exchanges:
        source_id = _handle_port_id(edge.source_port_id or edge.sourceHandle)
        target_id = _handle_port_id(edge.target_port_id or edge.targetHandle)
        source = ports_by_node_and_id.get((edge.fromNode, source_id))
        target = ports_by_node_and_id.get((edge.toNode, target_id))
        if source is None or target is None:
            raise ProviderContractError(
                422,
                "INLINE_EDGE_PORT_NOT_FOUND",
                "An inline edge cannot be tied to both exact graph ports.",
                edge_id=edge.id,
            )
        if source.type != "technosphere" or target.type != "technosphere":
            raise ProviderContractError(
                422,
                "INLINE_EDGE_ELEMENTARY_CONNECTION_FORBIDDEN",
                "Elementary exchanges cannot be connected as internal technosphere edges.",
                edge_id=edge.id,
            )
        if source.direction != "output" or target.direction != "input":
            raise ProviderContractError(
                422,
                "INLINE_EDGE_DIRECTION_MISMATCH",
                "Internal edges must connect an output port to an input port.",
                edge_id=edge.id,
            )
        source_identity = (
            source.flow_source_namespace,
            source.flowUuid,
            source.flow_version,
            source.flow_property_uuid,
            source.flow_property_version,
            source.unit_group_uuid,
            source.unit_group_version,
        )
        target_identity = (
            target.flow_source_namespace,
            target.flowUuid,
            target.flow_version,
            target.flow_property_uuid,
            target.flow_property_version,
            target.unit_group_uuid,
            target.unit_group_version,
        )
        has_explicit_conversion = bool(
            edge.intermediate_flow_link_rule_id
            and edge.intermediate_flow_link_factor
            and edge.intermediate_flow_link_factor > 0
        )
        if source_identity != target_identity and not has_explicit_conversion:
            raise ProviderContractError(
                422,
                "INLINE_EDGE_FLOW_IDENTITY_MISMATCH",
                "Internal ports must share exact Flow identity unless an explicit conversion rule is pinned.",
                edge_id=edge.id,
            )
        provider_unit = edge.provider_unit or edge.unit
        consumer_unit = edge.consumer_unit or edge.unit
        if source.unit != provider_unit or target.unit != consumer_unit:
            raise ProviderContractError(
                422,
                "INLINE_EDGE_UNIT_MISMATCH",
                "Edge units must match the connected provider and consumer port units.",
                edge_id=edge.id,
            )


def _configured_tidas_snapshot() -> TidasFlowSnapshot | None:
    try:
        return configured_tidas_flow_snapshot()
    except ProviderTidasSnapshotError as exc:
        raise ProviderContractError(422, exc.code, exc.message, **exc.details) from exc


def _configured_tidas_process_snapshot() -> TidasProcessSnapshot | None:
    try:
        return configured_tidas_process_snapshot()
    except ProviderTidasProcessSnapshotError as exc:
        raise ProviderContractError(422, exc.code, exc.message, **exc.details) from exc


def _configured_tidas_reference_snapshot() -> TidasReferenceDependencySnapshot | None:
    try:
        return configured_tidas_reference_dependency_snapshot()
    except ProviderTidasReferenceSnapshotError as exc:
        raise ProviderContractError(422, exc.code, exc.message, **exc.details) from exc


def _process_snapshot_database_conflicts(
    row: ReferenceProcess | None,
    value: dict[str, Any],
) -> dict[str, Any]:
    if row is None or not isinstance(row.import_report_json, dict):
        return {}
    report = row.import_report_json
    database_namespace = str(report.get("source_namespace") or "").strip()
    database_version = str(report.get("source_version") or "").strip()
    if (
        database_namespace != str(value.get("source_namespace") or "").strip()
        or database_version != str(value.get("version") or "").strip()
    ):
        return {}
    database_hash = _pinned_process_database_content_hash(row)
    qref = value.get("quantitative_reference") if isinstance(value.get("quantitative_reference"), dict) else {}
    qref_flow = qref.get("flow") if isinstance(qref.get("flow"), dict) else {}
    comparisons = {
        "content_hash": (database_hash, value.get("content_hash")),
        "process_type": (report.get("type_of_data_set"), value.get("process_type")),
        "reference_flow_uuid": (row.reference_flow_uuid, qref_flow.get("flow_uuid")),
        "reference_flow_internal_id": (row.reference_flow_internal_id, qref.get("exchange_internal_id")),
    }
    return {
        field: {"database": database_value, "snapshot": snapshot_value}
        for field, (database_value, snapshot_value) in comparisons.items()
        if str(database_value or "").strip()
        and str(snapshot_value or "").strip()
        and str(database_value).strip() != str(snapshot_value).strip()
    }


def _pinned_process_database_content_hash(row: ReferenceProcess) -> str:
    report = row.import_report_json if isinstance(row.import_report_json, dict) else {}
    database_hash = str(report.get("content_hash") or "").strip()
    if not database_hash and isinstance(row.process_json, dict) and "processDataSet" in row.process_json:
        database_hash = canonical_process_hash(row.process_json)
    return database_hash


def _snapshot_database_conflicts(row: FlowVersionRecord | None, value: dict[str, Any]) -> dict[str, Any]:
    if row is None:
        return {}
    comparisons = {
        "content_hash": (row.content_hash, value.get("content_hash")),
        "flow_property_uuid": (row.flow_property_uuid, value.get("flow_property_uuid")),
        "flow_property_version": (row.flow_property_version, value.get("flow_property_version")),
        "unit_group_uuid": (row.unit_group_uuid, value.get("unit_group_uuid")),
        "unit_group_version": (row.unit_group_version, value.get("unit_group_version")),
        "default_unit": (row.default_unit, value.get("default_unit")),
        "flow_type": (row.flow_type, value.get("flow_type")),
    }
    return {
        field: {"database": database_value, "snapshot": snapshot_value}
        for field, (database_value, snapshot_value) in comparisons.items()
        if str(database_value or "").strip()
        and str(snapshot_value or "").strip()
        and str(database_value).strip() != str(snapshot_value).strip()
    }


def _exact_flow_version_row(
    db: Session,
    *,
    source_namespace: str,
    flow_uuid: str,
    version: str,
) -> FlowVersionRecord | None:
    return (
        db.query(FlowVersionRecord)
        .filter(
            FlowVersionRecord.source_namespace == source_namespace,
            FlowVersionRecord.flow_uuid == flow_uuid,
            FlowVersionRecord.source_version == version,
        )
        .one_or_none()
    )


def _resolve_tidas_snapshot_value(
    snapshot: TidasFlowSnapshot,
    *,
    flow_uuid: str,
    version: str,
) -> dict[str, Any] | None:
    try:
        return snapshot.resolve(flow_uuid, version)
    except ProviderTidasSnapshotError as exc:
        raise ProviderContractError(422, exc.code, exc.message, **exc.details) from exc


def _technosphere_flow_receipts(
    db: Session,
    graph: HybridGraph,
    issues: list[ProviderIssue],
    snapshot: TidasFlowSnapshot | None,
) -> list[ProviderTechnosphereFlowReceipt]:
    receipts: list[ProviderTechnosphereFlowReceipt] = []
    for node in graph.nodes:
        for port in node.inputs + node.outputs:
            if port.type != "technosphere":
                continue
            exchange_id = f"{node.id}::{port.id}"
            source_namespace = str(port.flow_source_namespace or "")
            version = str(port.flow_version or "")
            resolution = "inline_custom"
            value: dict[str, Any] | None = None
            if source_namespace == TIDAS_SOURCE_NAMESPACE:
                if snapshot is None:
                    raise ProviderContractError(
                        422,
                        "TIDAS_FLOW_SNAPSHOT_REQUIRED",
                        "An inline TIDAS technosphere Flow requires a configured exact read-only snapshot.",
                        exchange_id=exchange_id,
                        flow_uuid=port.flowUuid,
                        version=version,
                    )
                else:
                    value = _resolve_tidas_snapshot_value(snapshot, flow_uuid=port.flowUuid, version=version)
                    if value is None:
                        raise ProviderContractError(
                            422,
                            "TIDAS_FLOW_EXACT_VERSION_NOT_IN_SNAPSHOT",
                            "An inline TIDAS technosphere Flow is not present at the exact version in the configured snapshot.",
                            exchange_id=exchange_id,
                            flow_uuid=port.flowUuid,
                            version=version,
                            snapshot_hash=snapshot.snapshot_hash,
                        )
                    if value.get("flow_type") not in {"Product flow", "Waste flow"}:
                        raise ProviderContractError(
                            422,
                            "TIDAS_TECHNOSPHERE_FLOW_TYPE_MISMATCH",
                            "A technosphere exchange must resolve to an exact Product or Waste Flow.",
                            exchange_id=exchange_id,
                            flow_uuid=port.flowUuid,
                            version=version,
                            flow_type=value.get("flow_type"),
                        )
                    row = _exact_flow_version_row(
                        db,
                        source_namespace=source_namespace,
                        flow_uuid=port.flowUuid,
                        version=version,
                    )
                    conflicts = _snapshot_database_conflicts(row, value)
                    if conflicts:
                        raise ProviderContractError(
                            422,
                            "TIDAS_FLOW_DATABASE_CONTENT_CONFLICT",
                            "The configured exact TIDAS Flow snapshot conflicts with the database row for the same identity.",
                            exchange_id=exchange_id,
                            flow_uuid=port.flowUuid,
                            version=version,
                            conflicts=conflicts,
                        )
                    graph_identity = {
                        "flow_property_uuid": port.flow_property_uuid,
                        "flow_property_version": port.flow_property_version,
                        "unit_group_uuid": port.unit_group_uuid,
                        "unit_group_version": port.unit_group_version,
                        "default_unit": port.unit,
                    }
                    mismatches = {
                        field: {"graph": graph_value, "snapshot": value.get(field)}
                        for field, graph_value in graph_identity.items()
                        if str(graph_value or "").strip() != str(value.get(field) or "").strip()
                    }
                    if mismatches:
                        raise ProviderContractError(
                            422,
                            "TIDAS_FLOW_GRAPH_IDENTITY_MISMATCH",
                            "An inline graph TIDAS Flow identity does not match the configured exact snapshot.",
                            exchange_id=exchange_id,
                            flow_uuid=port.flowUuid,
                            version=version,
                            mismatches=mismatches,
                        )
                    resolution = "tidas_exact_snapshot"
            else:
                issues.append(
                    ProviderIssue(
                        code="INLINE_CUSTOM_TECHNOSPHERE_FLOW",
                        message="The technosphere Flow is an inline custom identity, not a provider catalog record.",
                        severity="info",
                        path=f"scaled_exchanges.{exchange_id}",
                        details={
                            "source_namespace": source_namespace,
                            "flow_uuid": port.flowUuid,
                            "version": version,
                        },
                    )
                )
            receipts.append(
                ProviderTechnosphereFlowReceipt(
                    exchange_id=exchange_id,
                    process_uuid=node.process_uuid,
                    resolution=resolution,
                    source_namespace=source_namespace,
                    flow_uuid=port.flowUuid,
                    version=version,
                    flow_type=value.get("flow_type") if value else None,
                    content_hash=value.get("content_hash") if value else None,
                    snapshot_hash=value.get("snapshot_hash") if value else None,
                    flow_property_uuid=str(port.flow_property_uuid or ""),
                    flow_property_version=str(port.flow_property_version or ""),
                    flow_property_content_hash=(value.get("flow_property_content_hash") if value else None),
                    unit_group_uuid=str(port.unit_group_uuid or ""),
                    unit_group_version=str(port.unit_group_version or ""),
                    unit_group_content_hash=(value.get("unit_group_content_hash") if value else None),
                    unit=port.unit,
                    unit_content_hash=value.get("unit_content_hash") if value else None,
                    reference_dependency_resolution_source=(
                        value.get("reference_dependency_resolution_source") if value else None
                    ),
                    reference_dependency_snapshot_hash=(
                        value.get("reference_dependency_snapshot_hash") if value else None
                    ),
                )
            )
    return receipts


def _resolve_demand_process(demand: Any, base: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    refs: dict[str, dict[str, Any]] = base["reference_products"]
    if demand.process_uuid:
        ref = refs.get(demand.process_uuid)
        if ref is None:
            raise ProviderContractError(422, "DEMAND_PROCESS_NOT_FOUND", "Demand process is not in the matrix.")
        return demand.process_uuid, ref
    matches = []
    selector_name = "reference_exchange_id" if demand.reference_exchange_id else "reference_flow_uuid"
    selector = demand.reference_exchange_id or demand.reference_flow_uuid
    for process_uuid, ref in refs.items():
        ref_value = ref.get("exchange_id" if demand.reference_exchange_id else "flow_uuid")
        if ref_value == selector:
            matches.append((process_uuid, ref))
    if not matches:
        raise ProviderContractError(422, "DEMAND_REFERENCE_NOT_FOUND", "Demand reference is not in the matrix.")
    if len(matches) != 1:
        raise ProviderContractError(
            422,
            "DEMAND_REFERENCE_AMBIGUOUS",
            f"{selector_name} does not identify exactly one process.",
            match_count=len(matches),
        )
    return matches[0]


def solve_provider(
    db: Session,
    request: ProviderSolveRequest,
    *,
    run_id: str | None = None,
) -> ProviderSolveResponse:
    graph, consumer_graph_hash, provider_graph_hash, snapshot_ref, issues = _resolve_solve_snapshot(db, request)
    tidas_flow_snapshot = (
        _configured_tidas_snapshot()
        if (
            request.inline_snapshot is not None
            or request.background_process_pins
            or request.elementary_flows
            or request.lcia_methods
        )
        else None
    )
    process_identity_receipts, process_identities_hash = build_process_identity_receipts(
        db,
        graph,
        request.process_identities,
        issues,
        process_snapshot=(
            _configured_tidas_process_snapshot()
            if any(
                item.source_namespace == TIDAS_SOURCE_NAMESPACE
                for item in request.process_identities
            )
            else None
        ),
        database_content_hash=_pinned_process_database_content_hash,
        database_conflicts=_process_snapshot_database_conflicts,
    )
    technosphere_receipts: list[ProviderTechnosphereFlowReceipt] = []
    if request.inline_snapshot is not None:
        _validate_inline_foreground(graph)
        technosphere_receipts = _technosphere_flow_receipts(
            db,
            graph,
            issues,
            tidas_flow_snapshot,
        )
    try:
        background_expansion = expand_background_process_pins(
            graph=graph,
            pins=request.background_process_pins,
            process_snapshot=(
                _configured_tidas_process_snapshot()
                if request.background_process_pins
                else None
            ),
            flow_snapshot=(
                tidas_flow_snapshot
                if request.background_process_pins
                else None
            ),
        )
    except ProviderBackgroundLeafError as exc:
        raise ProviderContractError(422, exc.code, exc.message, **exc.details) from exc
    graph = background_expansion.graph
    provider_graph_hash = compute_graph_hash_from_graph(graph)
    if background_expansion.receipt_drafts:
        issues.append(
            ProviderIssue(
                code="PARTIAL_BACKGROUND_LEAF_CLAIM",
                message=(
                    "Pinned background Processes are closed leaves for this solve only and do not establish "
                    "a complete cradle-to-gate background system."
                ),
                severity="info",
                path="background_process_receipts",
                details={"claim_limit": BACKGROUND_CLAIM_LIMIT},
            )
        )
    raw_database_release = (graph.metadata or {}).get("database_release")
    database_release = raw_database_release.strip() if isinstance(raw_database_release, str) else ""
    if not database_release:
        database_release = "unpinned"
        issues.append(
            ProviderIssue(
                code="DATABASE_RELEASE_UNPINNED",
                message="The graph does not declare an authoritative database_release.",
                path="provenance.database_release",
            )
        )
    if engine_identity().commit is None:
        issues.append(
            ProviderIssue(
                code="SOLVER_BUILD_UNAVAILABLE",
                message="The runtime could not determine the solver build commit.",
                path="provenance.solver_build",
            )
        )
    solver_snapshot = to_tiangong_like(graph)
    _ensure_embedded_solver_core()
    matrix_builder = importlib.import_module("app.core.matrix_builder")
    base = matrix_builder.build_matrices_from_snapshot(solver_snapshot)
    for message in base.get("issues") or []:
        issues.append(ProviderIssue(code="MATRIX_BUILDER_ISSUE", message=str(message)))

    a_matrix = _dense_matrix(base["A"])
    process_index = list(base["A"].get("rows") or [])
    if a_matrix.shape != (len(process_index), len(process_index)) or not process_index:
        raise ProviderContractError(422, "UNSOLVABLE_MATRIX", "The existing matrix builder did not produce a non-empty square A matrix.")

    port_by_exchange = _port_from_exchange_id(graph)
    f_vector = np.zeros(len(process_index), dtype=float)
    for demand in request.demand:
        process_uuid, reference = _resolve_demand_process(demand, base)
        reference_exchange_id = str(reference.get("exchange_id") or "")
        port_entry = port_by_exchange.get(reference_exchange_id)
        if port_entry is None:
            raise ProviderContractError(
                422,
                "REFERENCE_EXCHANGE_METADATA_UNAVAILABLE",
                "The selected reference exchange has no matching graph port.",
                exchange_id=reference_exchange_id,
            )
        port, _ = port_entry
        if demand.unit != port.unit:
            raise ProviderContractError(
                422,
                "DEMAND_UNIT_CONVERSION_REQUIRED",
                "Provider v1 accepts demand only in the exact reference-port unit.",
                requested_unit=demand.unit,
                reference_unit=port.unit,
            )
        if demand.unit_group_uuid and demand.unit_group_uuid != port.unit_group_uuid:
            raise ProviderContractError(422, "DEMAND_UNIT_GROUP_MISMATCH", "Demand unit-group UUID does not match the reference port.")
        if demand.unit_group_version and demand.unit_group_version != port.unit_group_version:
            raise ProviderContractError(422, "DEMAND_UNIT_GROUP_VERSION_MISMATCH", "Demand unit-group version does not match the reference port.")
        f_vector[process_index.index(process_uuid)] += float(demand.amount)

    try:
        x_vector = np.linalg.solve(a_matrix, f_vector)
    except np.linalg.LinAlgError as exc:
        raise ProviderContractError(422, "SINGULAR_TECHNOSPHERE_MATRIX", "The existing A matrix cannot be solved.") from exc
    if not np.isfinite(x_vector).all():
        raise ProviderContractError(422, "NON_FINITE_ACTIVITY_VECTOR", "The solved activity vector contains non-finite values.")

    activity_by_process = dict(zip(process_index, (float(value) for value in x_vector), strict=True))
    activities: list[ProviderActivity] = []
    for process_uuid in process_index:
        reference = base["reference_products"][process_uuid]
        exchange_id = str(reference.get("exchange_id") or "")
        port_entry = port_by_exchange.get(exchange_id)
        if port_entry is None:
            raise ProviderContractError(422, "REFERENCE_EXCHANGE_METADATA_UNAVAILABLE", "A process reference exchange has no matching graph port.")
        port, _ = port_entry
        activities.append(
            ProviderActivity(
                process_uuid=process_uuid,
                activity_amount=activity_by_process[process_uuid],
                reference_exchange_id=exchange_id,
                reference_flow_uuid=str(reference.get("flow_uuid") or ""),
                unit=port.unit,
            )
        )

    internal_ids = _internal_exchange_ids(graph)
    scaled: list[ProviderScaledExchange] = []
    totals: dict[tuple[Any, ...], float] = defaultdict(float)
    allocation_totals = base.get("allocation_total") or {}
    for exchange in solver_snapshot.get("exchanges") or []:
        exchange_id = str(exchange.get("exchange_id") or "")
        port_entry = port_by_exchange.get(exchange_id)
        if port_entry is None:
            raise ProviderContractError(
                422,
                "EXCHANGE_METADATA_UNAVAILABLE",
                "A solver exchange has no matching graph port; scaled exchange output would be ambiguous.",
                exchange_id=exchange_id,
            )
        port, process_uuid = port_entry
        raw_amount = float(exchange.get("amount") or 0.0)
        allocation_total = float(allocation_totals.get(process_uuid) or 0.0)
        if allocation_total <= 0:
            raise ProviderContractError(422, "ALLOCATION_TOTAL_UNAVAILABLE", "A process has no positive allocation total.")
        coefficient = raw_amount / allocation_total
        activity_amount = activity_by_process[process_uuid]
        scaled_amount = coefficient * activity_amount
        exchange_type = "elementary" if port.type == "biosphere" else "technosphere"
        boundary_role = "internal" if exchange_type == "technosphere" and exchange_id in internal_ids else "boundary"
        item = ProviderScaledExchange(
            exchange_id=exchange_id,
            process_uuid=process_uuid,
            flow_uuid=port.flowUuid,
            flow_source_namespace=port.flow_source_namespace,
            flow_version=port.flow_version,
            flow_property_uuid=port.flow_property_uuid,
            flow_property_version=port.flow_property_version,
            unit_group_uuid=port.unit_group_uuid,
            unit_group_version=port.unit_group_version,
            unit=port.unit,
            direction=port.direction,
            exchange_type=exchange_type,
            boundary_role=boundary_role,
            raw_amount=raw_amount,
            allocation_total=allocation_total,
            coefficient=coefficient,
            activity_amount=activity_amount,
            scaled_amount=scaled_amount,
        )
        scaled.append(item)
        if exchange_type == "elementary" and boundary_role == "boundary":
            key = (
                item.flow_uuid,
                item.flow_source_namespace,
                item.flow_version,
                item.flow_property_uuid,
                item.flow_property_version,
                item.unit_group_uuid,
                item.unit_group_version,
                item.unit,
                item.direction,
            )
            totals[key] += scaled_amount

    inventory = [
        ProviderInventoryTotal(
            flow_uuid=key[0],
            flow_source_namespace=key[1],
            flow_version=key[2],
            flow_property_uuid=key[3],
            flow_property_version=key[4],
            unit_group_uuid=key[5],
            unit_group_version=key[6],
            unit=key[7],
            direction=key[8],
            amount=amount,
        )
        for key, amount in sorted(totals.items(), key=lambda item: tuple(str(value or "") for value in item[0]))
    ]
    issues.extend(
        [
            ProviderIssue(
                code="PROCESS_RESIDUALS_UNSUPPORTED",
                message="The existing solver path does not expose authoritative per-process residuals.",
                severity="info",
                path="process_residuals",
            ),
            ProviderIssue(
                code="CONTRIBUTION_GRAPH_UNSUPPORTED",
                message="The existing solver path does not expose an authoritative contribution graph.",
                severity="info",
                path="contribution_graph",
            ),
        ]
    )
    solved_snapshot_ref = (
        snapshot_ref.model_copy(update={"graph_hash": consumer_graph_hash})
        if snapshot_ref is not None
        else None
    )
    response = ProviderSolveResponse(
        run_id=run_id or str(uuid.uuid4()),
        snapshot_ref=solved_snapshot_ref,
        demand=list(request.demand),
        activity_vector=activities,
        scaled_exchanges=scaled,
        inventory_totals=inventory,
        technosphere_flow_receipts=technosphere_receipts,
        background_process_receipts=build_background_process_receipts(
            drafts=background_expansion.receipt_drafts,
            activity_by_process=activity_by_process,
            scaled_exchanges=scaled,
        ),
        process_identity_receipts=process_identity_receipts,
        process_residuals=[],
        contribution_graph={},
        issues=issues,
        provenance=ProviderSolveProvenance(
            engine=engine_identity(),
            snapshot_ref=solved_snapshot_ref,
            inline_graph_hash=consumer_graph_hash if request.inline_snapshot is not None else None,
            scenario_id=request.scenario_id,
            operation_hash=request.operation_hash,
            solver_build=engine_identity().commit,
            database_release=database_release,
            system_revision_hash=consumer_graph_hash,
            consumer_graph_hash=consumer_graph_hash,
            provider_graph_hash=provider_graph_hash,
            background_process_pins_hash=background_expansion.pins_hash,
            background_claim_scope=(
                BACKGROUND_CLAIM_LIMIT if background_expansion.receipt_drafts else None
            ),
            process_identities_hash=process_identities_hash,
            flow_snapshot_hash=(
                tidas_flow_snapshot.snapshot_hash
                if tidas_flow_snapshot is not None
                else None
            ),
            reference_dependency_snapshot_hash=(
                tidas_flow_snapshot.reference_dependencies.snapshot_hash
                if tidas_flow_snapshot is not None
                and tidas_flow_snapshot.reference_dependencies is not None
                else None
            ),
        ),
    )
    elementary_refs = list(request.elementary_flows) + list(
        background_expansion.elementary_refs
    )
    if request.lcia_methods or elementary_refs:
        def exact_elementary_flow(ref):
            if tidas_flow_snapshot is None or ref.source_namespace != TIDAS_SOURCE_NAMESPACE:
                return None
            value = _resolve_tidas_snapshot_value(
                tidas_flow_snapshot,
                flow_uuid=ref.flow_uuid,
                version=ref.version,
            )
            if value is not None and value.get("flow_type") != "Elementary flow":
                raise ProviderContractError(
                    422,
                    "ELEMENTARY_FLOW_TYPE_MISMATCH",
                    "An elementary reference must resolve to an exact Elementary Flow snapshot record.",
                    flow_uuid=ref.flow_uuid,
                    version=ref.version,
                    flow_type=value.get("flow_type"),
                )
            if value is None and tidas_flow_snapshot.contains_uuid(ref.flow_uuid):
                raise ProviderContractError(
                    422,
                    "ELEMENTARY_FLOW_EXACT_VERSION_NOT_IN_SNAPSHOT",
                    "The configured snapshot contains this elementary Flow UUID, but not the requested version.",
                    flow_uuid=ref.flow_uuid,
                    version=ref.version,
                    snapshot_hash=tidas_flow_snapshot.snapshot_hash,
                )
            return value

        try:
            elementary_exchange_ids = [item.exchange_id for item in elementary_refs]
            if len(elementary_exchange_ids) != len(set(elementary_exchange_ids)):
                raise ProviderContractError(
                    422,
                    "ELEMENTARY_FLOW_REFERENCE_DUPLICATE",
                    "An elementary exchange may have only one exact reference.",
                )
            lcia, receipts = characterize_scaled_inventory(
                methods=request.lcia_methods or ["EF v3.1"],
                elementary_refs=elementary_refs,
                scaled_exchanges=response.scaled_exchanges,
                inventory_totals=response.inventory_totals,
                exact_flow_resolver=exact_elementary_flow,
            )
        except ProviderEf31Error as exc:
            raise ProviderContractError(422, exc.code, exc.message, **exc.details) from exc
        if request.lcia_methods:
            response.lcia = lcia
        response.elementary_flow_receipts = receipts
        response.provenance.database_release = EF31_DATABASE_RELEASE
    return response


def resolve_catalog(db: Session, request: ProviderCatalogResolveRequest) -> list[ProviderCatalogResolution]:
    items: list[ProviderCatalogResolution] = []
    tidas_snapshot = _configured_tidas_snapshot()
    process_snapshot = _configured_tidas_process_snapshot() if request.processes else None
    reference_snapshot = (
        _configured_tidas_reference_snapshot()
        if request.flow_properties or request.unit_groups or request.units
        else None
    )
    for ref in request.processes:
        key = ref.model_dump(mode="python")
        if ref.source_namespace != TIDAS_SOURCE_NAMESPACE:
            items.append(
                ProviderCatalogResolution(
                    kind="process",
                    key=key,
                    status="unsupported",
                    code="PROCESS_CATALOG_NAMESPACE_UNSUPPORTED",
                    message="Provider v1 currently resolves only exact open TianGong Process snapshots.",
                )
            )
            continue
        if process_snapshot is None:
            items.append(
                ProviderCatalogResolution(
                    kind="process",
                    key=key,
                    status="unsupported",
                    code="TIDAS_PROCESS_SNAPSHOT_REQUIRED",
                    message=(
                        "Exact TIDAS Process resolution requires a configured read-only snapshot; "
                        "the database is not a substitute receipt."
                    ),
                )
            )
            continue
        value = process_snapshot.resolve(ref.process_uuid, ref.version)
        if value is None:
            items.append(
                ProviderCatalogResolution(
                    kind="process",
                    key=key,
                    status="not_found",
                    value={"snapshot_hash": process_snapshot.snapshot_hash},
                    code="EXACT_PROCESS_VERSION_NOT_FOUND",
                    message="The exact Process UUID and version are absent from the configured snapshot.",
                )
            )
            continue
        row = db.get(ReferenceProcess, ref.process_uuid)
        same_identity = (
            row is not None
            and isinstance(row.import_report_json, dict)
            and row.import_report_json.get("source_namespace") == ref.source_namespace
            and row.import_report_json.get("source_version") == ref.version
        )
        if same_identity and not _pinned_process_database_content_hash(row):
            items.append(
                ProviderCatalogResolution(
                    kind="process",
                    key=key,
                    status="unsupported",
                    value={**value, "database_identity_status": "same_identity_unverifiable"},
                    code="TIDAS_PROCESS_DATABASE_IDENTITY_INCOMPLETE",
                    message=(
                        "The database row claims the same exact Process identity but has no canonical "
                        "content hash or complete raw payload for comparison."
                    ),
                )
            )
            continue
        conflicts = _process_snapshot_database_conflicts(row, value)
        if conflicts:
            items.append(
                ProviderCatalogResolution(
                    kind="process",
                    key=key,
                    status="unsupported",
                    value={**value, "database_conflicts": conflicts},
                    code="TIDAS_PROCESS_DATABASE_CONTENT_CONFLICT",
                    message=(
                        "The configured exact TIDAS Process snapshot conflicts with the database row "
                        "for the same pinned identity."
                    ),
                )
            )
            continue
        value["database_identity_status"] = (
            "same_identity_consistent" if same_identity else "not_pinned_or_absent"
        )
        items.append(
            ProviderCatalogResolution(
                kind="process",
                key=key,
                status="resolved",
                value=value,
            )
        )
    for ref in request.flows:
        key = ref.model_dump(mode="python")
        if ref.source_namespace == TIDAS_SOURCE_NAMESPACE and tidas_snapshot is not None:
            try:
                snapshot_value = tidas_snapshot.resolve(ref.flow_uuid, ref.version)
            except ProviderTidasSnapshotError as exc:
                items.append(
                    ProviderCatalogResolution(
                        kind="flow",
                        key=key,
                        status="unsupported",
                        value={"snapshot_hash": tidas_snapshot.snapshot_hash, "details": exc.details},
                        code=exc.code,
                        message=exc.message,
                    )
                )
                continue
            if snapshot_value is not None:
                row = _exact_flow_version_row(
                    db,
                    source_namespace=ref.source_namespace,
                    flow_uuid=ref.flow_uuid,
                    version=ref.version,
                )
                conflicts = _snapshot_database_conflicts(row, snapshot_value)
                if conflicts:
                    items.append(
                        ProviderCatalogResolution(
                            kind="flow",
                            key=key,
                            status="unsupported",
                            value={**snapshot_value, "database_conflicts": conflicts},
                            code="TIDAS_FLOW_DATABASE_CONTENT_CONFLICT",
                            message=(
                                "The configured exact TIDAS Flow snapshot conflicts with the database row "
                                "for the same identity."
                            ),
                        )
                    )
                else:
                    items.append(
                        ProviderCatalogResolution(
                            kind="flow",
                            key=key,
                            status="resolved",
                            value=snapshot_value,
                        )
                    )
                continue
            if tidas_snapshot.contains_uuid(ref.flow_uuid):
                items.append(
                    ProviderCatalogResolution(
                        kind="flow",
                        key=key,
                        status="not_found",
                        value={"snapshot_hash": tidas_snapshot.snapshot_hash},
                        code="EXACT_FLOW_VERSION_NOT_FOUND",
                        message="The configured snapshot contains this Flow UUID, but not the requested version.",
                    )
                )
                continue
        try:
            standard = resolve_standard_flow(ref)
        except ProviderEf31Error as exc:
            items.append(
                ProviderCatalogResolution(
                    kind="flow",
                    key=key,
                    status="unsupported",
                    code=exc.code,
                    message=exc.message,
                )
            )
            continue
        if standard is not None:
            items.append(ProviderCatalogResolution(kind="flow", key=key, status="resolved", value=standard))
            continue
        if ref.source_namespace == TIDAS_SOURCE_NAMESPACE:
            if tidas_snapshot is None:
                items.append(
                    ProviderCatalogResolution(
                        kind="flow",
                        key=key,
                        status="unsupported",
                        code="TIDAS_FLOW_SNAPSHOT_REQUIRED",
                        message=(
                            "Exact non-built-in TIDAS Flow resolution requires a configured read-only snapshot; "
                            "the database is not a substitute receipt."
                        ),
                    )
                )
            else:
                items.append(
                    ProviderCatalogResolution(
                        kind="flow",
                        key=key,
                        status="not_found",
                        value={"snapshot_hash": tidas_snapshot.snapshot_hash},
                        code="EXACT_FLOW_VERSION_NOT_FOUND",
                    )
                )
            continue
        row = _exact_flow_version_row(
            db,
            source_namespace=ref.source_namespace,
            flow_uuid=ref.flow_uuid,
            version=ref.version,
        )
        if row is None:
            items.append(ProviderCatalogResolution(kind="flow", key=key, status="not_found", code="EXACT_FLOW_VERSION_NOT_FOUND"))
            continue
        value = {
            "source_namespace": row.source_namespace,
            "flow_uuid": row.flow_uuid,
            "version": row.source_version,
            "version_label": row.version_label,
            "name": row.flow_name,
            "name_en": row.flow_name_en,
            "flow_type": row.flow_type,
            "default_unit": row.default_unit,
            "unit_group": row.unit_group,
            "flow_property_uuid": row.flow_property_uuid,
            "flow_property_version": row.flow_property_version,
            "unit_group_uuid": row.unit_group_uuid,
            "unit_group_version": row.unit_group_version,
            "content_hash": row.content_hash,
        }
        missing = [
            field
            for field in (
                "flow_property_uuid",
                "flow_property_version",
                "unit_group_uuid",
                "unit_group_version",
                "content_hash",
            )
            if not str(value.get(field) or "").strip()
        ]
        items.append(
            ProviderCatalogResolution(
                kind="flow",
                key=key,
                status="unsupported" if missing else "resolved",
                value=value,
                code="FLOW_VERSION_IDENTITY_INCOMPLETE" if missing else None,
                message=(
                    "The exact Flow version row exists but lacks fields required for a complete reusable identity."
                    if missing
                    else None
                ),
            )
        )
    for ref in request.flow_properties:
        if reference_snapshot is not None:
            exact = reference_snapshot.resolve_flow_property(ref.flow_property_uuid, ref.version)
            if exact is not None:
                items.append(
                    ProviderCatalogResolution(
                        kind="flow_property",
                        key=ref.model_dump(mode="python"),
                        status="resolved",
                        value=exact,
                    )
                )
                continue
        standard = resolve_standard_flow_property(ref)
        if standard is not None:
            items.append(
                ProviderCatalogResolution(
                    kind="flow_property",
                    key=ref.model_dump(mode="python"),
                    status="resolved",
                    value=standard,
                )
            )
            continue
        items.append(
            ProviderCatalogResolution(
                kind="flow_property",
                key=ref.model_dump(mode="python"),
                status="unsupported",
                code="FLOW_PROPERTY_RESOURCE_UNAVAILABLE",
                message="The current database stores Flow Property identity on Flow versions but has no exact-version Flow Property resource table.",
            )
        )
    for ref in request.unit_groups:
        key = ref.model_dump(mode="python")
        if reference_snapshot is not None:
            exact = reference_snapshot.resolve_unit_group(ref.unit_group_uuid, ref.version)
            if exact is not None:
                items.append(
                    ProviderCatalogResolution(
                        kind="unit_group",
                        key=key,
                        status="resolved",
                        value=exact,
                    )
                )
                continue
        standard = resolve_standard_unit_group(ref)
        if standard is not None:
            items.append(ProviderCatalogResolution(kind="unit_group", key=key, status="resolved", value=standard))
            continue
        rows = (
            db.query(UnitGroup)
            .filter(UnitGroup.source_uuid == ref.unit_group_uuid, UnitGroup.source_version == ref.version)
            .all()
        )
        if not rows:
            items.append(ProviderCatalogResolution(kind="unit_group", key=key, status="not_found", code="EXACT_UNIT_GROUP_VERSION_NOT_FOUND"))
        elif len(rows) > 1:
            items.append(ProviderCatalogResolution(kind="unit_group", key=key, status="unsupported", code="EXACT_UNIT_GROUP_IDENTITY_AMBIGUOUS"))
        else:
            row = rows[0]
            items.append(
                ProviderCatalogResolution(
                    kind="unit_group",
                    key=key,
                    status="resolved",
                    value={
                        "unit_group_uuid": row.source_uuid,
                        "version": row.source_version,
                        "name": row.name,
                        "reference_unit": row.reference_unit,
                        "source_package_version": row.source_package_version,
                    },
                )
            )
    for ref in request.units:
        key = ref.model_dump(mode="python")
        if reference_snapshot is not None:
            exact = reference_snapshot.resolve_unit(ref.unit_group_uuid, ref.version, ref.unit)
            if exact is not None:
                items.append(
                    ProviderCatalogResolution(
                        kind="unit",
                        key=key,
                        status="resolved",
                        value=exact,
                    )
                )
                continue
        standard = resolve_standard_unit(ref)
        if standard is not None:
            items.append(ProviderCatalogResolution(kind="unit", key=key, status="resolved", value=standard))
            continue
        groups = (
            db.query(UnitGroup)
            .filter(UnitGroup.source_uuid == ref.unit_group_uuid, UnitGroup.source_version == ref.version)
            .all()
        )
        if len(groups) != 1:
            status = "not_found" if not groups else "unsupported"
            code = "EXACT_UNIT_GROUP_VERSION_NOT_FOUND" if not groups else "EXACT_UNIT_GROUP_IDENTITY_AMBIGUOUS"
            items.append(ProviderCatalogResolution(kind="unit", key=key, status=status, code=code))
            continue
        unit = (
            db.query(UnitDefinition)
            .filter(UnitDefinition.unit_group == groups[0].name, UnitDefinition.unit_name == ref.unit)
            .one_or_none()
        )
        if unit is None:
            items.append(ProviderCatalogResolution(kind="unit", key=key, status="not_found", code="EXACT_UNIT_NOT_FOUND"))
            continue
        items.append(
            ProviderCatalogResolution(
                kind="unit",
                key=key,
                status="resolved",
                value={
                    "unit_group_uuid": ref.unit_group_uuid,
                    "unit_group_version": ref.version,
                    "unit_group": groups[0].name,
                    "unit": unit.unit_name,
                    "factor_to_reference": unit.factor_to_reference,
                    "is_reference": unit.is_reference,
                },
            )
        )
    return items


def resolve_flow_candidates(db: Session, request: ProviderCatalogResolveRequest) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for query in request.flow_candidates:
        token = query.query.strip()
        statement = db.query(FlowVersionRecord).filter(
            or_(
                FlowVersionRecord.flow_name.ilike(f"%{token}%"),
                FlowVersionRecord.flow_name_en.ilike(f"%{token}%"),
            )
        )
        if query.flow_type:
            statement = statement.filter(FlowVersionRecord.flow_type == query.flow_type)
        if query.unit:
            statement = statement.filter(FlowVersionRecord.default_unit == query.unit)
        rows = statement.order_by(
            FlowVersionRecord.flow_name_en.asc(),
            FlowVersionRecord.flow_uuid.asc(),
            FlowVersionRecord.source_version.asc(),
        ).limit(query.limit).all()
        candidates = []
        for row in rows:
            missing = [
                field
                for field in (
                    "flow_property_uuid",
                    "flow_property_version",
                    "unit_group_uuid",
                    "unit_group_version",
                    "content_hash",
                )
                if not str(getattr(row, field) or "").strip()
            ]
            candidates.append(
                {
                    "source_namespace": row.source_namespace,
                    "flow_uuid": row.flow_uuid,
                    "version": row.source_version,
                    "name": row.flow_name,
                    "name_en": row.flow_name_en,
                    "flow_type": row.flow_type,
                    "default_unit": row.default_unit,
                    "unit_group": row.unit_group,
                    "flow_property_uuid": row.flow_property_uuid,
                    "flow_property_version": row.flow_property_version,
                    "unit_group_uuid": row.unit_group_uuid,
                    "unit_group_version": row.unit_group_version,
                    "content_hash": row.content_hash,
                    "exact_identity_complete": not missing,
                    "missing_exact_fields": missing,
                }
            )
        result.append(
            {
                "correlation_id": query.correlation_id,
                "query": query.query,
                "flow_type": query.flow_type,
                "unit": query.unit,
                "candidates": candidates,
            }
        )
    return result
