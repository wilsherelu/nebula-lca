from __future__ import annotations

import importlib
import os
import subprocess
import uuid
from collections import defaultdict
from functools import lru_cache
from typing import Any

import numpy as np
from sqlalchemy.orm import Session

from ..config import WORKSPACE_ROOT
from ..models import FlowVersionRecord, Model, ModelVersion, UnitDefinition, UnitGroup
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
)
from ..schemas import FlowPort, HybridGraph
from ..solver import to_tiangong_like
from ..solver_adapter import _ensure_embedded_solver_core
from .graph_storage import (
    compute_graph_hash_from_graph,
    compute_graph_hash_from_slim_graph,
    hydrate_graph_for_api,
)


class ProviderContractError(RuntimeError):
    def __init__(self, status_code: int, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


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


def _graph_identity_issues(graph: HybridGraph) -> list[ProviderIssue]:
    issues: list[ProviderIssue] = []
    required = {
        "flow_source_namespace": "flowSourceNamespace",
        "flow_version": "flowVersion",
        "flow_property_uuid": "flowPropertyUuid",
        "flow_property_version": "flowPropertyVersion",
        "unit_group_uuid": "unitGroupUuid",
        "unit_group_version": "unitGroupVersion",
    }
    for node_index, node in enumerate(graph.nodes):
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
        return snapshot.graph, snapshot.graph_hash, snapshot.graph_hash, snapshot.base_snapshot_ref, list(snapshot.issues)

    assert request.inline_snapshot is not None
    inline = request.inline_snapshot
    provider_graph_hash = compute_graph_hash_from_graph(inline.graph)
    issues = list(inline.issues) + _graph_identity_issues(inline.graph)
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


def solve_provider(db: Session, request: ProviderSolveRequest) -> ProviderSolveResponse:
    graph, consumer_graph_hash, provider_graph_hash, snapshot_ref, issues = _resolve_solve_snapshot(db, request)
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
    return ProviderSolveResponse(
        run_id=str(uuid.uuid4()),
        snapshot_ref=solved_snapshot_ref,
        demand=list(request.demand),
        activity_vector=activities,
        scaled_exchanges=scaled,
        inventory_totals=inventory,
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
        ),
    )


def resolve_catalog(db: Session, request: ProviderCatalogResolveRequest) -> list[ProviderCatalogResolution]:
    items: list[ProviderCatalogResolution] = []
    for ref in request.flows:
        key = ref.model_dump(mode="python")
        row = (
            db.query(FlowVersionRecord)
            .filter(
                FlowVersionRecord.source_namespace == ref.source_namespace,
                FlowVersionRecord.flow_uuid == ref.flow_uuid,
                FlowVersionRecord.source_version == ref.version,
            )
            .one_or_none()
        )
        if row is None:
            items.append(ProviderCatalogResolution(kind="flow", key=key, status="not_found", code="EXACT_FLOW_VERSION_NOT_FOUND"))
            continue
        items.append(
            ProviderCatalogResolution(
                kind="flow",
                key=key,
                status="resolved",
                value={
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
                },
            )
        )
    for ref in request.flow_properties:
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
