import json
import hashlib
import csv
import uuid
import io
import zipfile
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import time
import traceback
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import case, func, inspect, text
from sqlalchemy.orm import Session
from .config import settings
from .database import Base, SessionLocal, engine, get_db
from .models import (
    DebugDiagnostic,
    FlowRecord,
    Model,
    ModelVersion,
    PtsCompileArtifact,
    PtsDefinition,
    PtsExternalArtifact,
    PtsResource,
    ReferenceProcess,
    RunJob,
    UnitDefinition,
    UnitGroup,
)
from .schemas import (
    ImportUnitGroupsRequest,
    ImportUnitGroupsResponse,
    DeleteFlowsResponse,
    DeleteProjectResponse,
    FlowCategoriesResponse,
    FlowCategoryItem,
    FlowPort,
    ImportFlowsRequest,
    ImportFlowsResponse,
    ImportProcessesRequest,
    ImportProcessesResponse,
    ImportReferenceProcessesRequest,
    ImportReferenceProcessesResponse,
    ImportedProcessDetail,
    ImportedProcessPortItem,
    ModelCreateRequest,
    ModelCreateResponse,
    ModelVersionCreateRequest,
    ModelVersionOut,
    ProjectIntegrityIssue,
    ProjectIntegritySummary,
    PtsValidationItem,
    PtsValidationSummary,
    PaginatedFlowsResponse,
    PaginatedProcessesResponse,
    PaginatedProjectsResponse,
    DeleteProcessResponse,
    DeleteProcessesBatchRequest,
    DeleteProcessesBatchResponse,
    ProcessDetailResponse,
    ProcessListItem,
    ProcessImportReportResponse,
    ProcessImportWarning,
    ProcessFilteredExchangesResponse,
    ProjectDuplicateRequest,
    ProjectCreateRequest,
    ProjectUpdateRequest,
    ProjectOut,
    ProjectFlowNameSyncResponse,
    RepairProjectIntegrityResponse,
    RepairPtsPublicationsResponse,
    StatsResponse,
    FlowListItem,
    FlowOut,
    HybridGraph,
    HybridEdge,
    HybridNode,
    graph_exchange_type_to_flow_semantic,
    flow_semantic_to_exchange_type,
    is_elementary_flow_semantic,
    is_product_flow_semantic,
    is_waste_flow_semantic,
    normalize_same_flow_uuid_opposite_direction_ports,
    normalize_flow_semantic,
    ReferenceProcessOut,
    ReferenceProcessCatalogItem,
    ReferenceProcessCatalogResponse,
    RunRequest,
    RunResponse,
    UnitConvertRequest,
    UnitConvertResponse,
    UnitDefinitionOut,
    UnitGroupOut,
    FilteredExchangeEvidence,
    PtsValidateRequest,
    PtsValidateResponse,
    PtsCompileRequest,
    PtsCompileResponse,
    PtsCompiledGetResponse,
    PtsCompiledExternalResponse,
    PtsCompileHistoryResponse,
    PtsBoundaryPortHint,
    PtsModelWarning,
    PtsPublishRequest,
    PtsPublishResponse,
    PtsPackFinalizeRequest,
    PtsPackFinalizeResponse,
    PtsUnpackRequest,
    PtsUnpackResponse,
    PtsUnpackPortBinding,
    PtsPublishedHistoryResponse,
    PtsResourceOut,
    PtsResourceUpdateRequest,
    PtsVersionItem,
    TidasImportReportResponse,
    TidasImportRequest,
    TidasModelImportRequest,
    TidasMissingFlowSummaryItem,
    MissingFlowSummaryResponse,
    HybridNode,
    HandleValidationRequest,
    HandleValidationResponse,
)
from .ingest import convert_unit_value, import_flows_from_file, import_unit_groups_from_excel, load_ef31_flow_uuid_set
from .ingest import import_processes_from_json
from .preprocess import normalize_graph_units_to_reference
from .solver import to_tiangong_like
from .solver_adapter import run_tiangong_lcia
from .pts_validate import validate_pts_compile
from .pts_compile import PTS_COMPILE_SCHEMA_VERSION, compile_pts, compute_pts_graph_hash

app = FastAPI(title=settings.app_name, version="0.1.0")

_CACHE_TTL_SECONDS = 30.0
_CACHE_TTL_PROJECTS_SECONDS = 3600.0
_CACHE_TTL_PROCESSES_SECONDS = 3600.0
_CACHE_TTL_FLOWS_SECONDS = 3600.0
_CACHE_TTL_FLOW_CATEGORIES_SECONDS = 3600.0
_CACHE_TTL_STATS_SECONDS = 3600.0
_CACHE_TTL_REFERENCE_PROCESS_CATALOG_SECONDS = 30.0
_CACHE_TTL_REFERENCE_PROCESS_REPORT_SECONDS = 1800.0
_CACHE_TTL_FLOW_META_SECONDS = 300.0
_api_cache: dict[str, tuple[float, object]] = {}
_api_cache_revisions: dict[str, int] = defaultdict(int)
_indicator_meta_by_index_cache: dict[int, dict[str, str]] | None = None
_indicator_units_cache_path: str | None = None
_indicator_units_cache_mtime: float | None = None


def is_graph_non_empty(graph: dict | None) -> bool:
    if not isinstance(graph, dict):
        return False
    nodes = graph.get("nodes")
    exchanges = graph.get("exchanges")
    return bool((isinstance(nodes, list) and len(nodes) > 0) or (isinstance(exchanges, list) and len(exchanges) > 0))


def get_model_or_404(db: Session, model_id: str) -> Model:
    model = db.query(Model).filter(Model.id == model_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Project not found")
    return model


def resolve_project_id_for_run(payload: RunRequest, db: Session) -> str:
    if payload.project_id:
        return get_model_or_404(db, payload.project_id).id

    if payload.model_version_id:
        if ":" in payload.model_version_id:
            project_id = payload.model_version_id.split(":", 1)[0].strip()
            if project_id:
                return get_model_or_404(db, project_id).id
        version_row = db.query(ModelVersion).filter(ModelVersion.id == payload.model_version_id).first()
        if version_row:
            return version_row.model_id

    raise HTTPException(status_code=400, detail="project_id is required for PTS compile/save context")


def normalize_graph_node_kinds(graph: HybridGraph) -> None:
    for node in graph.nodes:
        node_kind = str(node.node_kind or "").strip()
        if node_kind == "unit_process":
            process_uuid = str(node.process_uuid or "")
            if node.mode == "normalized" and process_uuid.startswith("market_"):
                node.node_kind = "market_process"


def _normalize_pts_shell_node_kind(value: object) -> str:
    raw = str(value or "").strip()
    if raw == "pts":
        return "pts_module"
    return raw or "pts_module"


def normalize_graph_product_flags(graph: HybridGraph) -> None:
    normalize_graph_node_kinds(graph)
    for node in graph.nodes:
        for port in node.inputs:
            port.isProduct = bool(port.isProduct)
        for port in node.outputs:
            port.isProduct = bool(port.isProduct)


def validate_product_unit_group_consistency(graph: HybridGraph) -> None:
    violations: list[dict] = []
    for node in graph.nodes:
        if node.node_kind not in {"unit_process", "market_process"}:
            continue

        product_outputs = [port for port in node.outputs if bool(port.isProduct)]
        if len(product_outputs) <= 1:
            continue

        distinct_groups = {str(port.unitGroup or "").strip() for port in product_outputs}
        if len(distinct_groups) <= 1:
            continue

        violations.append(
            {
                "node_id": node.id,
                "node_name": node.name,
                "product_flows": [
                    {
                        "port_id": port.id,
                        "flow_uuid": port.flowUuid,
                        "flow_name": port.name,
                        "unit": port.unit,
                        "unit_group": str(port.unitGroup or "").strip(),
                    }
                    for port in product_outputs
                ],
            }
        )

    if violations:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "UNIT_GROUP_MISMATCH_FOR_PRODUCTS",
                "message": "Current version supports multi-product allocation only within the same unit group.",
                "violations": violations,
            },
        )


def validate_unique_process_uuid(graph: HybridGraph) -> None:
    process_uuid_to_nodes: dict[str, list[HybridNode]] = {}
    for node in graph.nodes:
        pid = str(node.process_uuid or "").strip()
        if not pid:
            continue
        process_uuid_to_nodes.setdefault(pid, []).append(node)

    duplicates = {pid: nodes for pid, nodes in process_uuid_to_nodes.items() if len(nodes) > 1}
    if not duplicates:
        return

    evidence: list[dict] = []
    for pid, nodes in duplicates.items():
        evidence.append(
            {
                "duplicate_process_uuid": pid,
                "node_ids": [node.id for node in nodes],
                "process_names": [node.name for node in nodes],
            }
        )

    raise HTTPException(
        status_code=400,
        detail={
            "code": "DUPLICATE_PROCESS_UUID",
            "message": "process_uuid must be unique within one graph.",
            "evidence": evidence,
        },
    )


def normalize_graph_edge_port_ids(graph: HybridGraph) -> None:
    for edge in graph.exchanges:
        if not edge.source_port_id and edge.sourceHandle:
            edge.source_port_id = edge.sourceHandle
        if not edge.target_port_id and edge.targetHandle:
            edge.target_port_id = edge.targetHandle
        if not edge.sourceHandle and edge.source_port_id:
            edge.sourceHandle = edge.source_port_id
        if not edge.targetHandle and edge.target_port_id:
            edge.targetHandle = edge.target_port_id


def _resolve_edge_port_id(raw_port_or_handle: str | None, prefix: str) -> str:
    return _port_id_from_handle(raw_port_or_handle, prefix)


def _preserve_or_default_handle(
    *,
    raw_handle: str | None,
    resolved_port_id: str,
    prefix: str,
) -> str:
    handle = str(raw_handle or "").strip()
    allowed_prefixes = {prefix, f"{prefix}l", f"{prefix}r"}
    handle_prefix = handle.split(":", 1)[0].strip().lower() if ":" in handle else ""
    if handle and handle_prefix in allowed_prefixes and _port_id_from_handle(handle, prefix) == resolved_port_id:
        return handle
    return f"{prefix}:{resolved_port_id}"


def _resolve_edge_port_id_for_node(
    *,
    node: HybridNode | None,
    raw_port_or_handle: str | None,
    prefix: str,
    flow_uuid: str,
    direction: str,
) -> str:
    resolved = _resolve_edge_port_id(raw_port_or_handle, prefix)
    if node is None:
        return resolved

    candidate_ports = list(node.outputs) if direction == "output" else list(node.inputs)
    if resolved and any(str(port.id or "") == resolved for port in candidate_ports):
        return resolved

    flow_uuid = str(flow_uuid or "").strip()
    if not flow_uuid:
        return resolved

    matching_ports = [port for port in candidate_ports if str(port.flowUuid or "").strip() == flow_uuid]
    if len(matching_ports) == 1:
        return str(matching_ports[0].id or "")

    compact_resolved = str(resolved or "").strip()
    if compact_resolved:
        suffix_matches = [port for port in matching_ports if str(port.id or "").strip().endswith(compact_resolved)]
        if len(suffix_matches) == 1:
            return str(suffix_matches[0].id or "")

    return resolved


def validate_port_bucket_direction_consistency(graph: HybridGraph) -> None:
    violations: list[dict] = []
    for node in graph.nodes:
        for bucket_name, expected_direction in (("inputs", "input"), ("outputs", "output")):
            ports = getattr(node, bucket_name, []) or []
            for port in ports:
                actual_direction = str(port.direction or "").strip()
                if actual_direction == expected_direction:
                    continue
                violations.append(
                    {
                        "node_id": node.id,
                        "node_kind": str(node.node_kind or ""),
                        "port_id": str(port.id or ""),
                        "bucket": bucket_name,
                        "expected_direction": expected_direction,
                        "actual_direction": actual_direction or None,
                        "flow_uuid": str(port.flowUuid or ""),
                        "flow_name": _truncate_text_preview(str(port.name or "")),
                    }
                )
                if len(violations) >= 200:
                    break
            if len(violations) >= 200:
                break
        if len(violations) >= 200:
            break

    if violations:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "PORT_DIRECTION_BUCKET_MISMATCH",
                "message": "Port direction must match its container bucket.",
                "evidence": violations,
            },
        )


def validate_edge_binding_and_uniqueness(graph: HybridGraph) -> None:
    node_by_id = {node.id: node for node in graph.nodes}
    source_port_index: dict[tuple[str, str], object] = {}
    target_port_index: dict[tuple[str, str], object] = {}

    for node in graph.nodes:
        for port in node.outputs:
            if port.id:
                source_port_index[(node.id, port.id)] = port
        for port in node.inputs:
            if port.id:
                target_port_index[(node.id, port.id)] = port

    binding_issues: list[dict] = []
    duplicate_edges: dict[tuple[str, str, str, str, str], list[str]] = {}

    for edge in graph.exchanges:
        source_node = node_by_id.get(edge.fromNode)
        target_node = node_by_id.get(edge.toNode)
        source_port_id = _resolve_edge_port_id_for_node(
            node=source_node,
            raw_port_or_handle=edge.source_port_id or edge.sourceHandle,
            prefix="out",
            flow_uuid=str(edge.flowUuid or ""),
            direction="output",
        )
        target_port_id = _resolve_edge_port_id_for_node(
            node=target_node,
            raw_port_or_handle=edge.target_port_id or edge.targetHandle,
            prefix="in",
            flow_uuid=str(edge.flowUuid or ""),
            direction="input",
        )

        if source_port_id:
            edge.source_port_id = source_port_id
            edge.sourceHandle = _preserve_or_default_handle(
                raw_handle=edge.sourceHandle or edge.source_port_id,
                resolved_port_id=source_port_id,
                prefix="out",
            )
        if target_port_id:
            edge.target_port_id = target_port_id
            edge.targetHandle = _preserve_or_default_handle(
                raw_handle=edge.targetHandle or edge.target_port_id,
                resolved_port_id=target_port_id,
                prefix="in",
            )

        source_port = source_port_index.get((edge.fromNode, source_port_id))
        target_port = target_port_index.get((edge.toNode, target_port_id))

        issues: list[str] = []
        if source_node is None:
            issues.append("source node not found")
        if target_node is None:
            issues.append("target node not found")
        if not source_port_id:
            issues.append("source port missing")
        if not target_port_id:
            issues.append("target port missing")
        if source_port_id and source_port is None:
            issues.append("source port is not an output port on source node")
        if target_port_id and target_port is None:
            issues.append("target port is not an input port on target node")
        if source_port is not None and str(source_port.flowUuid or "") != str(edge.flowUuid or ""):
            issues.append("edge.flowUuid does not match source port flowUuid")
        if target_port is not None and str(target_port.flowUuid or "") != str(edge.flowUuid or ""):
            issues.append("edge.flowUuid does not match target port flowUuid")

        if issues:
            binding_issues.append(
                {
                    "edge_id": edge.id,
                    "from_node_id": edge.fromNode,
                    "to_node_id": edge.toNode,
                    "source_port_id": source_port_id,
                    "target_port_id": target_port_id,
                    "flow_uuid": edge.flowUuid,
                    "issues": issues,
                }
            )
            continue

        dedupe_key = (edge.fromNode, edge.toNode, source_port_id, target_port_id, str(edge.flowUuid or ""))
        duplicate_edges.setdefault(dedupe_key, []).append(edge.id)

    if binding_issues:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_EDGE_PORT_BINDING",
                "message": "Edge endpoints must bind source output ports to target input ports with matching flowUuid.",
                "evidence": binding_issues[:200],
            },
        )

    duplicated = [
        {
            "from_node_id": key[0],
            "to_node_id": key[1],
            "source_port_id": key[2],
            "target_port_id": key[3],
            "flow_uuid": key[4],
            "edge_ids": edge_ids,
        }
        for key, edge_ids in duplicate_edges.items()
        if len(edge_ids) > 1
    ]
    if duplicated:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "DUPLICATE_EDGE_BINDING",
                "message": "Duplicate edges are not allowed for the same source/target port binding and flowUuid.",
                "evidence": duplicated[:200],
            },
        )


def validate_market_input_constraints(graph: HybridGraph) -> None:
    node_by_id = {node.id: node for node in graph.nodes}
    duplicate_provider_hits: dict[tuple[str, str, str, str], list[str]] = {}

    for edge in graph.exchanges:
        target_node = node_by_id.get(edge.toNode)
        source_node = node_by_id.get(edge.fromNode)
        if target_node is None or source_node is None:
            continue
        if str(target_node.node_kind or "") != "market_process":
            continue

        target_port_id = _resolve_edge_port_id(edge.target_port_id or edge.targetHandle, "in")
        source_process_uuid = str(source_node.process_uuid or "").strip()
        key = (target_node.id, target_port_id, source_process_uuid, str(edge.flowUuid or ""))
        duplicate_provider_hits.setdefault(key, []).append(edge.id)

    violations = [
        {
            "market_node_id": key[0],
            "target_port_id": key[1],
            "source_process_uuid": key[2],
            "flow_uuid": key[3],
            "edge_ids": edge_ids,
        }
        for key, edge_ids in duplicate_provider_hits.items()
        if len(edge_ids) > 1
    ]
    if violations:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "DUPLICATE_MARKET_PORT_SOURCE_PROCESS",
                "message": "For market_process inputs, one target port cannot connect the same source process more than once.",
                "evidence": violations[:200],
            },
        )


def validate_non_product_input_constraints(graph: HybridGraph) -> None:
    constrained_target_kinds = {"unit_process", "pts_module"}
    node_by_id = {node.id: node for node in graph.nodes}
    source_port_index: dict[tuple[str, str], object] = {}
    target_port_index: dict[tuple[str, str], object] = {}

    for node in graph.nodes:
        for port in node.outputs:
            if port.id:
                source_port_index[(node.id, port.id)] = port
        for port in node.inputs:
            if port.id:
                target_port_index[(node.id, port.id)] = port

    incoming_product_edges: dict[tuple[str, str], list[dict]] = {}
    for edge in graph.exchanges:
        target_port_id = _resolve_edge_port_id(edge.target_port_id or edge.targetHandle, "in")
        source_port_id = _resolve_edge_port_id(edge.source_port_id or edge.sourceHandle, "out")
        target_node = node_by_id.get(edge.toNode)
        source_node = node_by_id.get(edge.fromNode)
        if target_node is None or source_node is None:
            continue
        if str(target_node.node_kind or "") not in constrained_target_kinds:
            continue

        target_port = target_port_index.get((edge.toNode, target_port_id))
        source_port = source_port_index.get((edge.fromNode, source_port_id))
        if target_port is None or source_port is None:
            continue
        if str(target_port.type or "") == "biosphere":
            continue
        if bool(target_port.isProduct):
            continue
        if str(source_port.type or "") == "biosphere":
            continue
        if not bool(source_port.isProduct):
            continue

        incoming_product_edges.setdefault((target_node.id, target_port.id), []).append(
            {
                "edge_id": edge.id,
                "source_node_id": source_node.id,
                "source_process_uuid": str(source_node.process_uuid or ""),
                "source_port_id": source_port.id,
                "flow_uuid": str(edge.flowUuid or ""),
            }
        )

    violations = [
        {
            "target_node_id": target_key[0],
            "target_port_id": target_key[1],
            "incoming_edges": rows,
        }
        for target_key, rows in incoming_product_edges.items()
        if len(rows) > 1
    ]
    if violations:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "NON_PRODUCT_INPUT_MULTI_PRODUCT_SOURCE",
                "message": "For unit_process/pts, a non-product intermediate input port cannot connect to multiple product intermediate sources.",
                "evidence": violations[:200],
            },
        )


def validate_edge_product_role_alignment(graph: HybridGraph) -> None:
    node_by_id = {node.id: node for node in graph.nodes}
    source_port_index: dict[tuple[str, str], object] = {}
    target_port_index: dict[tuple[str, str], object] = {}

    for node in graph.nodes:
        for port in node.outputs:
            if port.id:
                source_port_index[(node.id, port.id)] = port
        for port in node.inputs:
            if port.id:
                target_port_index[(node.id, port.id)] = port

    violations: list[dict] = []
    for edge in graph.exchanges:
        source_port_id = _resolve_edge_port_id(edge.source_port_id or edge.sourceHandle, "out")
        target_port_id = _resolve_edge_port_id(edge.target_port_id or edge.targetHandle, "in")
        source_node = node_by_id.get(edge.fromNode)
        target_node = node_by_id.get(edge.toNode)
        if source_node is None or target_node is None:
            continue
        source_port = source_port_index.get((edge.fromNode, source_port_id))
        target_port = target_port_index.get((edge.toNode, target_port_id))
        if source_port is None or target_port is None:
            continue
        if str(source_port.type or "") == "biosphere" or str(target_port.type or "") == "biosphere":
            continue

        source_is_product = bool(source_port.isProduct)
        target_is_product = bool(target_port.isProduct)
        if source_is_product != target_is_product:
            continue

        violations.append(
            {
                "edge_id": edge.id,
                "from_node_id": source_node.id,
                "to_node_id": target_node.id,
                "from_process_uuid": str(source_node.process_uuid or ""),
                "to_process_uuid": str(target_node.process_uuid or ""),
                "source_port_id": str(source_port.id or ""),
                "target_port_id": str(target_port.id or ""),
                "flow_uuid": str(edge.flowUuid or ""),
                "source_is_product": source_is_product,
                "target_is_product": target_is_product,
                "mismatch_type": "product_to_product" if source_is_product else "non_product_to_non_product",
            }
        )

    if violations:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_EDGE_PRODUCT_ROLE_ALIGNMENT",
                "message": "Technosphere edges must connect product-to-non-product or non-product-to-product ports.",
                "evidence": violations[:500],
            },
        )


def validate_graph_contract(
    graph: HybridGraph,
    *,
    require_non_empty: bool = False,
    allow_pts_nodes: bool = True,
) -> None:
    normalize_graph_product_flags(graph)
    normalize_graph_edge_port_ids(graph)
    validate_port_bucket_direction_consistency(graph)
    validate_unique_process_uuid(graph)
    validate_product_unit_group_consistency(graph)
    validate_edge_binding_and_uniqueness(graph)
    validate_edge_product_role_alignment(graph)
    validate_market_input_constraints(graph)
    validate_non_product_input_constraints(graph)

    if require_non_empty and not is_graph_non_empty(graph.model_dump()):
        raise HTTPException(status_code=400, detail="Empty graph is not allowed")
    if not allow_pts_nodes and any(node.node_kind == "pts_module" for node in graph.nodes):
        raise HTTPException(
            status_code=400,
            detail="PTS nodes detected. Use /model/run with published PTS artifacts.",
        )


def validate_graph_flow_type_contract(
    graph: HybridGraph,
    *,
    db: Session,
    stage: str,
) -> None:
    flow_meta = _flow_meta_by_uuid_cached(db)
    flow_type_by_uuid: dict[str, str] = {}
    flow_type_by_uuid_lower: dict[str, str] = {}
    for flow_uuid, meta in flow_meta.items():
        flow_type = str((meta or (None, None, None, None))[2] or "").strip()
        if not flow_uuid or not flow_type:
            continue
        flow_type_by_uuid[flow_uuid] = flow_type
        flow_type_by_uuid_lower[flow_uuid.lower()] = flow_type

    not_found_violations: list[dict] = []
    bucket_specs = ["inputs", "outputs"]
    for node in graph.nodes:
        for bucket_name in bucket_specs:
            ports = getattr(node, bucket_name, []) or []
            for port in ports:
                flow_uuid = str(port.flowUuid or "").strip()
                if not flow_uuid:
                    continue

                flow_type = flow_type_by_uuid.get(flow_uuid) or flow_type_by_uuid_lower.get(flow_uuid.lower())
                if not flow_type:
                    not_found_violations.append(
                        {
                            "node_id": node.id,
                            "port_id": str(port.id or ""),
                            "flow_uuid": flow_uuid,
                            "expected_type": None,
                            "actual_type": str(port.type or ""),
                            "stage": stage,
                            "reason": "FLOW_UUID_NOT_FOUND",
                            "bucket": bucket_name,
                            "actual_direction": str(port.direction or ""),
                        }
                    )
                    continue

                expected_type = flow_semantic_to_exchange_type(flow_type)
                # Backend is authoritative for flow type classification.
                port.type = expected_type

    if not_found_violations:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "FLOW_UUID_NOT_FOUND",
                "message": "flow_uuid must exist in flow catalog before save/run.",
                "stage": stage,
                "evidence": not_found_violations[:500],
            },
        )


def _normalize_port_display_name(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " ".join(text.split())


def validate_graph_port_names_against_flow_catalog(
    graph: HybridGraph,
    *,
    db: Session,
    stage: str,
) -> None:
    flow_meta = _flow_meta_by_uuid_cached(db)
    evidence: list[dict] = []
    sample_limit = 10
    sampled = 0

    for node in graph.nodes:
        if str(node.node_kind or "") == "pts_module":
            continue
        for bucket_name in ("inputs", "outputs"):
            for port in getattr(node, bucket_name, []) or []:
                flow_uuid = str(port.flowUuid or "").strip()
                if not flow_uuid:
                    continue
                meta = flow_meta.get(flow_uuid) or flow_meta.get(flow_uuid.lower())
                if not meta:
                    continue
                expected_name = _normalize_port_display_name(meta[0])
                actual_name = _normalize_port_display_name(port.name)
                if "@" in actual_name or "@" in expected_name:
                    continue
                if stage == "import_model" and expected_name:
                    if actual_name == expected_name:
                        if sampled >= sample_limit:
                            break
                        continue
                    if actual_name.startswith(f"{expected_name};"):
                        if sampled >= sample_limit:
                            break
                        continue
                if not expected_name or not actual_name:
                    continue
                sampled += 1
                if not expected_name or not actual_name or actual_name == expected_name:
                    if sampled >= sample_limit:
                        break
                    continue
                evidence.append(
                    {
                        "node_id": node.id,
                        "node_kind": str(node.node_kind or ""),
                        "port_id": str(port.id or ""),
                        "bucket": bucket_name,
                        "flow_uuid": flow_uuid,
                        "expected_flow_name": _truncate_text_preview(expected_name),
                        "actual_port_name": _truncate_text_preview(actual_name),
                        "stage": stage,
                    }
                )
                if sampled >= sample_limit:
                    break
            if sampled >= sample_limit:
                break
        if sampled >= sample_limit:
            break

    if evidence and sampled > 0 and len(evidence) == sampled:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "FLOW_NAME_MISMATCH",
                "message": "Sampled port display names all differ from catalog flow names; save/run rejected.",
                "stage": stage,
                "sampled_count": sampled,
                "evidence": evidence,
            },
        )


_MOJIBAKE_LATIN1_RE = re.compile(r"[\u00C0-\u00FF\u0080-\u009F]")


def _truncate_text_preview(value: str, limit: int = 80) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _looks_like_utf8_latin1_mojibake(value: str) -> tuple[bool, str | None]:
    if not value:
        return False, None
    if "\ufffd" in value:
        return True, "contains replacement character"
    if not _MOJIBAKE_LATIN1_RE.search(value):
        return False, None

    try:
        repaired = value.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return False, None

    if repaired == value:
        return False, None

    original_cjk = sum(1 for ch in value if "\u4e00" <= ch <= "\u9fff")
    repaired_cjk = sum(1 for ch in repaired if "\u4e00" <= ch <= "\u9fff")
    if repaired_cjk <= 0 or repaired_cjk <= original_cjk:
        return False, None
    return True, f"looks like UTF-8 bytes decoded as Latin-1; candidate repair={_truncate_text_preview(repaired)}"


def validate_graph_text_encoding(graph: HybridGraph, *, stage: str) -> None:
    evidence: list[dict] = []

    def _check(field: str, value: str | None, *, node_id: str | None = None, port_id: str | None = None) -> None:
        text = str(value or "").strip()
        if not text:
            return
        suspicious, reason = _looks_like_utf8_latin1_mojibake(text)
        if not suspicious:
            return
        evidence.append(
            {
                "node_id": node_id,
                "port_id": port_id,
                "field": field,
                "value_preview": _truncate_text_preview(text),
                "reason": reason,
                "stage": stage,
            }
        )

    _check("functionalUnit", graph.functionalUnit)
    for node in graph.nodes:
        _check("node.name", node.name, node_id=node.id)
        _check("node.location", node.location, node_id=node.id)
        _check("node.reference_product", node.reference_product, node_id=node.id)
        for bucket_name in ("inputs", "outputs"):
            for port in getattr(node, bucket_name, []) or []:
                _check(f"{bucket_name}.name", port.name, node_id=node.id, port_id=str(port.id or ""))
                _check(f"{bucket_name}.unit", port.unit, node_id=node.id, port_id=str(port.id or ""))
                _check(f"{bucket_name}.unitGroup", port.unitGroup, node_id=node.id, port_id=str(port.id or ""))
        if len(evidence) >= 200:
            break

    if evidence:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "TEXT_ENCODING_MISMATCH",
                "message": "Graph contains suspicious mojibake text; save/run rejected.",
                "stage": stage,
                "evidence": evidence[:200],
            },
        )


def analyze_handle_consistency(graph: HybridGraph) -> dict:
    node_port_ids: dict[str, set[str]] = {}
    source_candidates: dict[tuple[str, str], list[str]] = {}
    target_candidates: dict[tuple[str, str], list[str]] = {}

    for node in graph.nodes:
        all_ports = [*node.inputs, *node.outputs]
        node_port_ids[node.id] = {port.id for port in all_ports if port.id}

        for port in node.outputs:
            if port.id and port.flowUuid:
                source_candidates.setdefault((node.id, port.flowUuid), []).append(port.id)
        for port in node.inputs:
            if port.id and port.flowUuid:
                target_candidates.setdefault((node.id, port.flowUuid), []).append(port.id)

    issues: list[dict] = []
    for edge in graph.exchanges:
        source_handle = edge.source_port_id or edge.sourceHandle
        target_handle = edge.target_port_id or edge.targetHandle

        source_ok = bool(source_handle and source_handle in node_port_ids.get(edge.fromNode, set()))
        target_ok = bool(target_handle and target_handle in node_port_ids.get(edge.toNode, set()))
        if source_ok and target_ok:
            continue

        source_options = source_candidates.get((edge.fromNode, edge.flowUuid), [])
        target_options = target_candidates.get((edge.toNode, edge.flowUuid), [])
        source_guess = source_options[0] if len(source_options) == 1 else None
        target_guess = target_options[0] if len(target_options) == 1 else None

        issues.append(
            {
                "edge_id": edge.id,
                "flow_uuid": edge.flowUuid,
                "from_node_id": edge.fromNode,
                "to_node_id": edge.toNode,
                "source_handle": source_handle,
                "target_handle": target_handle,
                "source_ok": source_ok,
                "target_ok": target_ok,
                "suggested_source_port_id": source_guess,
                "suggested_target_port_id": target_guess,
                "suggested_source_handle": source_guess,
                "suggested_target_handle": target_guess,
            }
        )

    return {
        "ok": len(issues) == 0,
        "issue_count": len(issues),
        "issues": issues,
    }


def analyze_handle_consistency_from_graph_json(graph_json: dict) -> dict:
    try:
        graph = HybridGraph.model_validate(graph_json)
    except Exception as exc:
        return {
            "ok": False,
            "issue_count": 1,
            "issues": [{"code": "GRAPH_PARSE_ERROR", "message": str(exc)}],
        }
    return analyze_handle_consistency(graph)


def safe_handle_validation_from_graph_json(graph_json: dict) -> dict:
    try:
        return analyze_handle_consistency_from_graph_json(graph_json)
    except Exception as exc:
        return {
            "ok": False,
            "issue_count": 1,
            "issues": [{"code": "HANDLE_VALIDATION_RUNTIME_ERROR", "message": str(exc)}],
        }


def _port_id_from_handle(handle_id: str | None, prefix: str) -> str:
    if not handle_id:
        return ""
    token = f"{prefix}:"
    if handle_id.startswith(token):
        return handle_id[len(token) :]
    if ":" in handle_id:
        return handle_id.split(":", 1)[1]
    return handle_id


def _split_port_source_suffix(name: str | None) -> tuple[str, str]:
    text = str(name or "").strip()
    if "@" not in text:
        return text, ""
    base, suffix = text.split("@", 1)
    return base.strip(), suffix.strip()


def _market_input_display_name(flow_name: str | None, source_name: str | None) -> str:
    base_name, _ = _split_port_source_suffix(flow_name)
    source = str(source_name or "").strip()
    if not source:
        return base_name
    return f"{base_name}@{source}" if base_name else source


def _enrich_market_process_input_sources_on_canvas(*, nodes: list[dict], exchanges: list[dict]) -> None:
    if not isinstance(nodes, list) or not isinstance(exchanges, list):
        return

    node_by_id = {
        str(node.get("id") or "").strip(): node
        for node in nodes
        if isinstance(node, dict) and str(node.get("id") or "").strip()
    }
    incoming_by_port: dict[tuple[str, str, str], list[dict]] = {}
    incoming_by_flow: dict[tuple[str, str], list[dict]] = {}

    for edge in exchanges:
        if not isinstance(edge, dict):
            continue
        target_node_id = str(edge.get("toNode") or "").strip()
        source_node_id = str(edge.get("fromNode") or "").strip()
        flow_uuid = str(edge.get("flowUuid") or "").strip()
        if not target_node_id or not source_node_id or not flow_uuid:
            continue
        provider_node = node_by_id.get(source_node_id)
        if provider_node is None:
            continue
        provider = {
            "source_process_uuid": str(provider_node.get("process_uuid") or "").strip(),
            "source_process_name": str(provider_node.get("name") or "").strip(),
            "source_node_id": source_node_id,
        }
        target_port_id = _port_id_from_handle(
            str(edge.get("targetPortId") or edge.get("target_port_id") or edge.get("targetHandle") or ""),
            "in",
        )
        if target_port_id:
            incoming_by_port.setdefault((target_node_id, target_port_id, flow_uuid), []).append(provider)
        incoming_by_flow.setdefault((target_node_id, flow_uuid), []).append(provider)

    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("node_kind") or "").strip() != "market_process":
            continue
        node_id = str(node.get("id") or "").strip()
        inputs = node.get("inputs")
        if not isinstance(inputs, list):
            continue
        for port in inputs:
            if not isinstance(port, dict):
                continue
            if str(port.get("type") or "").strip() == "biosphere":
                continue
            flow_uuid = str(port.get("flowUuid") or "").strip()
            port_id = str(port.get("id") or "").strip()
            if not flow_uuid:
                continue
            candidates = incoming_by_port.get((node_id, port_id, flow_uuid), [])
            if len(candidates) != 1:
                flow_candidates = incoming_by_flow.get((node_id, flow_uuid), [])
                if len(flow_candidates) == 1:
                    candidates = flow_candidates
            if len(candidates) != 1:
                continue
            provider = candidates[0]
            source_process_uuid = str(provider.get("source_process_uuid") or "").strip()
            source_process_name = str(provider.get("source_process_name") or "").strip()
            source_node_id = str(provider.get("source_node_id") or "").strip()
            if source_process_uuid:
                port["sourceProcessUuid"] = source_process_uuid
                port["source_process_uuid"] = source_process_uuid
            if source_process_name:
                port["sourceProcessName"] = source_process_name
                port["source_process_name"] = source_process_name
            if source_node_id:
                port["sourceNodeId"] = source_node_id
                port["source_node_id"] = source_node_id

            current_name = str(port.get("name") or "").strip()
            if source_process_name and "@" not in current_name:
                port["name"] = _market_input_display_name(current_name or flow_uuid, source_process_name)


def _enrich_market_process_input_sources_in_graph_json(graph_json: dict) -> dict:
    if not isinstance(graph_json, dict):
        return graph_json

    nodes = graph_json.get("nodes")
    exchanges = graph_json.get("exchanges")
    if isinstance(nodes, list) and isinstance(exchanges, list):
        _enrich_market_process_input_sources_on_canvas(nodes=nodes, exchanges=exchanges)

    metadata = graph_json.get("metadata")
    canvases = metadata.get("canvases") if isinstance(metadata, dict) else None
    if isinstance(canvases, list):
        for canvas in canvases:
            if not isinstance(canvas, dict):
                continue
            canvas_nodes = canvas.get("nodes")
            canvas_edges = canvas.get("edges")
            if isinstance(canvas_nodes, list) and isinstance(canvas_edges, list):
                _enrich_market_process_input_sources_on_canvas(nodes=canvas_nodes, exchanges=canvas_edges)

    return graph_json


def _raise_pts_compile_value_error_http(exc: ValueError, pts_node_id: str) -> None:
    raw = str(exc)
    code = "INVALID_PTS_EXPORT_PORTS"
    message = raw
    evidence: list[dict] = [{"pts_node_id": pts_node_id, "error": raw}]
    if "|" in raw:
        parts = raw.split("|", 2)
        if len(parts) >= 2:
            code = parts[0] or code
            message = parts[1] or message
        if len(parts) == 3:
            try:
                parsed = json.loads(parts[2])
                parsed_evidence = parsed.get("evidence")
                if isinstance(parsed_evidence, list):
                    evidence = parsed_evidence
            except Exception:
                pass
    raise HTTPException(status_code=400, detail={"code": code, "message": message, "evidence": evidence}) from exc


def _build_process_unit_map_from_snapshot(snapshot: dict) -> dict[str, dict]:
    processes = snapshot.get("processes") if isinstance(snapshot, dict) else []
    exchanges = snapshot.get("exchanges") if isinstance(snapshot, dict) else []
    flows = snapshot.get("flows") if isinstance(snapshot, dict) else []
    process_rows = [p for p in processes if isinstance(p, dict)]
    exchange_rows = [e for e in exchanges if isinstance(e, dict)]
    flow_rows = [f for f in flows if isinstance(f, dict)]

    exchange_by_id = {str(row.get("exchange_id") or ""): row for row in exchange_rows}
    flow_by_uuid = {str(row.get("flow_uuid") or ""): row for row in flow_rows}
    process_unit_map: dict[str, dict] = {}

    for process in process_rows:
        process_uuid = str(process.get("process_uuid") or "")
        ref_exchange_id = str(process.get("reference_product_flow_uuid") or "")
        ref_exchange = exchange_by_id.get(ref_exchange_id, {})
        flow_uuid = str(ref_exchange.get("flow_uuid") or "")
        flow_row = flow_by_uuid.get(flow_uuid, {})
        process_unit_map[process_uuid] = {
            "reference_flow_uuid": flow_uuid,
            "reference_unit": str(flow_row.get("default_unit_uuid") or ""),
            "reference_unit_group": str(flow_row.get("unit_group_uuid") or ""),
        }
    return process_unit_map


def _build_process_unit_map_from_graph(graph: HybridGraph) -> dict[str, dict]:
    process_unit_map: dict[str, dict] = {}
    for node in graph.nodes:
        process_uuid = str(node.process_uuid or "")
        if not process_uuid:
            continue

        ref_port = next((port for port in node.outputs if bool(port.isProduct) and port.type != "biosphere"), None)
        if ref_port is None:
            ref_port = next((port for port in node.outputs if port.type != "biosphere"), None)
        if ref_port is None:
            ref_port = next((port for port in node.inputs if bool(port.isProduct) and port.type != "biosphere"), None)
        if ref_port is None:
            ref_port = next((port for port in node.inputs if port.type != "biosphere"), None)
        if ref_port is None:
            continue

        process_unit_map[process_uuid] = {
            "reference_flow_uuid": str(ref_port.flowUuid or ""),
            "reference_unit": str(ref_port.unit or ""),
            "reference_unit_group": str(ref_port.unitGroup or ""),
        }
    return process_unit_map


def _build_product_result_view_from_graph(
    *,
    graph: HybridGraph,
    process_index: object,
    values: object,
) -> tuple[list[dict], dict[str, dict], object]:
    if not isinstance(process_index, list) or not process_index:
        return [], {}, values

    products_by_process: dict[str, list[dict]] = {}
    seen_keys: set[str] = set()
    for node in graph.nodes:
        process_uuid = str(node.process_uuid or "")
        process_name = str(node.name or process_uuid or node.id)
        if not process_uuid:
            continue
        reference_port = next((port for port in node.outputs if port.type != "biosphere" and bool(port.isProduct)), None)
        for port in node.outputs:
            if port.type == "biosphere" or not bool(port.isProduct):
                continue
            product_port_id = str(port.id or "")
            product_flow_uuid = str(port.flowUuid or "")
            product_key = f"{process_uuid}::{product_port_id or product_flow_uuid}"
            if not product_flow_uuid or product_key in seen_keys:
                continue
            seen_keys.add(product_key)
            products_by_process.setdefault(process_uuid, []).append(
                {
                    "product_key": product_key,
                    "process_uuid": process_uuid,
                    "process_name": process_name,
                    "product_port_id": product_port_id,
                    "product_flow_uuid": product_flow_uuid,
                    "product_name": str(port.name or product_flow_uuid),
                    "is_reference_product": bool(reference_port is not None and str(reference_port.id or "") == product_port_id),
                    "unit": str(port.unit or ""),
                    "unit_group": str(port.unitGroup or ""),
                    "flow_uuid": product_flow_uuid,
                }
            )

    product_result_index: list[dict] = []
    product_unit_map: dict[str, dict] = {}
    process_positions: list[list[int]] = []
    for pid in process_index:
        process_uuid = str(pid)
        product_rows = products_by_process.get(process_uuid) or []
        positions_for_process: list[int] = []
        for item in product_rows:
            pos = len(product_result_index)
            positions_for_process.append(pos)
            product_result_index.append(
                {
                    "product_key": item["product_key"],
                    "process_uuid": process_uuid,
                    "process_name": item["process_name"],
                    "product_port_id": item["product_port_id"],
                    "product_flow_uuid": item["product_flow_uuid"],
                    "product_name": item["product_name"],
                    "is_reference_product": bool(item["is_reference_product"]),
                }
            )
            product_unit_map[item["product_key"]] = {
                "unit": item["unit"],
                "unit_group": item["unit_group"],
                "flow_uuid": item["flow_uuid"],
            }
        process_positions.append(positions_for_process)

    if not product_result_index:
        return [], {}, values

    product_values = values
    if isinstance(values, list) and values:
        if all(isinstance(row, list) for row in values):
            expanded_rows: list[list[object]] = []
            for row in values:
                expanded_row: list[object] = []
                for idx, positions in enumerate(process_positions):
                    source_value = row[idx] if idx < len(row) else None
                    for _ in positions:
                        expanded_row.append(source_value)
                expanded_rows.append(expanded_row)
            product_values = expanded_rows
        elif len(values) == len(process_index):
            expanded_values: list[object] = []
            for idx, positions in enumerate(process_positions):
                source_value = values[idx] if idx < len(values) else None
                for _ in positions:
                    expanded_values.append(source_value)
            product_values = expanded_values

    return product_result_index, product_unit_map, product_values


def _rescale_lci_values_to_inventory_units(
    *,
    values: object,
    process_index: object,
    process_unit_map: dict[str, dict],
    unit_factor_by_group_and_name: dict[tuple[str, str], float],
) -> object:
    if not isinstance(process_index, list) or not process_index:
        return values
    if not isinstance(values, list) or not values:
        return values

    scale_by_position: list[float] = []
    for pid in process_index:
        meta = process_unit_map.get(str(pid), {}) if isinstance(process_unit_map, dict) else {}
        unit_group = str(meta.get("reference_unit_group") or "")
        unit_name = str(meta.get("reference_unit") or "")
        factor = unit_factor_by_group_and_name.get((unit_group, unit_name))
        scale_by_position.append(float(factor) if factor is not None else 1.0)

    if all(isinstance(row, list) for row in values):
        scaled_rows: list[list] = []
        for row in values:
            scaled_row: list = []
            for idx, val in enumerate(row):
                if idx >= len(scale_by_position):
                    scaled_row.append(val)
                    continue
                try:
                    scaled_row.append(float(val) * scale_by_position[idx])
                except (TypeError, ValueError):
                    scaled_row.append(val)
            scaled_rows.append(scaled_row)
        return scaled_rows

    if len(values) == len(process_index):
        scaled: list = []
        for idx, val in enumerate(values):
            try:
                scaled.append(float(val) * scale_by_position[idx])
            except (TypeError, ValueError):
                scaled.append(val)
        return scaled

    return values


def _compile_and_persist_pts_for_node(
    *,
    db: Session,
    project_id: str,
    graph: HybridGraph,
    node_id: str,
    force_recompile: bool,
) -> PtsCompileArtifact:
    try:
        graph_hash = compute_pts_graph_hash(graph, node_id)
        definition = extract_pts_definition(
            graph=graph,
            pts_node_id=node_id,
            graph_hash=graph_hash,
        )
        definition = _apply_pts_resource_policy_override(
            db=db,
            project_id=project_id,
            definition=definition,
        )
    except ValueError as exc:
        _raise_pts_compile_value_error_http(exc, node_id)
    definition_row = upsert_pts_definition(
        db=db,
        project_id=project_id,
        definition=definition,
    )
    pts_node = next((node for node in graph.nodes if node.id == node_id and node.node_kind == "pts_module"), None)
    pts_uuid = str(pts_node.pts_uuid or pts_node.process_uuid or pts_node.id).strip() if pts_node is not None else ""
    ports_policy = _get_pts_resource_ports_policy(db=db, project_id=project_id, pts_uuid=pts_uuid) if pts_uuid else None

    cached_row = (
        db.query(PtsCompileArtifact)
        .filter(
            PtsCompileArtifact.project_id == project_id,
            PtsCompileArtifact.pts_node_id == node_id,
            PtsCompileArtifact.graph_hash == graph_hash,
        )
        .first()
    )
    if cached_row is not None and not force_recompile:
        cached_artifact = cached_row.artifact_json if isinstance(cached_row.artifact_json, dict) else {}
        cached_version = str(cached_artifact.get("compile_schema_version") or "")
        if cached_version == PTS_COMPILE_SCHEMA_VERSION:
            compile_row = cached_row
        else:
            try:
                compile_result = compile_pts(graph, node_id, ports_policy=ports_policy)
            except ValueError as exc:
                _raise_pts_compile_value_error_http(exc, node_id)
            compile_row, _ = upsert_pts_compile_artifact(
                db=db,
                project_id=project_id,
                pts_node_id=node_id,
                force_recompile=True,
                compile_result=compile_result,
            )
    else:
        try:
            compile_result = compile_pts(graph, node_id, ports_policy=ports_policy)
        except ValueError as exc:
            _raise_pts_compile_value_error_http(exc, node_id)
        compile_row, _ = upsert_pts_compile_artifact(
            db=db,
            project_id=project_id,
            pts_node_id=node_id,
            force_recompile=force_recompile,
            compile_result=compile_result,
        )

    return compile_row


def _compile_pts_on_save_if_needed(
    *,
    db: Session,
    project_id: str,
    graph: HybridGraph,
    compile_on_save: bool,
) -> dict:
    # Main-graph save no longer performs implicit PTS compile/publish.
    # PTS lifecycle is handled by explicit /api/pts/compile and /api/pts/{pts_uuid}/publish.
    if not compile_on_save:
        return {
            "pts_compile_count": 0,
            "pts_compiled_uuids": [],
            "pts_failed_count": 0,
            "pts_failed_items": [],
        }
    return {
        "pts_compile_count": 0,
        "pts_compiled_uuids": [],
        "pts_failed_count": 0,
        "pts_failed_items": [],
    }


def _find_pts_internal_canvas(graph: HybridGraph, pts_node_id: str) -> dict | None:
    metadata = graph.metadata if isinstance(graph.metadata, dict) else {}
    canvases = metadata.get("canvases")
    if not isinstance(canvases, list):
        return None
    candidates: list[dict] = []
    for item in canvases:
        if not isinstance(item, dict):
            continue
        if item.get("kind") != "pts_internal":
            continue
        if str(item.get("parentPtsNodeId") or "") != pts_node_id:
            continue
        candidates.append(item)
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda c: (len(c.get("nodes") or []), len(c.get("edges") or [])),
        reverse=True,
    )[0]


def _build_pts_ports_policy_from_node(pts_node: HybridNode) -> dict:
    return {
        "inputs": [
            {
                "flow_uuid": port.flowUuid,
                "name": port.name,
                "internal_exposed": bool(port.internalExposed),
                "external_visible": bool(port.showOnNode),
                "source_process_uuid": str(port.source_process_uuid or ""),
                "source_node_id": str(port.source_node_id or ""),
            }
            for port in pts_node.inputs
            if port.type != "biosphere"
        ],
        "outputs": [
            {
                "flow_uuid": port.flowUuid,
                "name": port.name,
                "internal_exposed": bool(port.internalExposed),
                "external_visible": bool(port.showOnNode),
                "source_process_uuid": str(port.source_process_uuid or ""),
                "source_node_id": str(port.source_node_id or ""),
                "is_product": bool(port.isProduct),
            }
            for port in pts_node.outputs
            if port.type != "biosphere"
        ],
    }


def _is_pts_port_exposed_like(port: object) -> bool:
    internal_exposed = getattr(port, "internalExposed", None)
    if internal_exposed is not None:
        return bool(internal_exposed)
    external_visible = getattr(port, "externalVisible", None)
    if external_visible is not None:
        return bool(external_visible)
    show_on_node = getattr(port, "showOnNode", None)
    if show_on_node is not None:
        return bool(show_on_node)
    return False


def _normalize_pts_ports_policy_from_graph(*, pts_graph: dict, fallback_policy: dict | None = None) -> dict:
    if not isinstance(pts_graph, dict):
        return dict(fallback_policy or {})
    nodes = pts_graph.get("nodes")
    exchanges = pts_graph.get("exchanges")
    if not isinstance(nodes, list) or not isinstance(exchanges, list):
        return dict(fallback_policy or {})

    graph = HybridGraph.model_validate(
        {
            "functionalUnit": str(pts_graph.get("functionalUnit") or "PTS"),
            "nodes": nodes,
            "exchanges": exchanges,
            "metadata": pts_graph.get("metadata") or {},
        }
    )

    def _policy_rows(policy: dict | None, key: str) -> list[dict]:
        if not isinstance(policy, dict):
            return []
        rows = policy.get(key)
        if rows is None or not isinstance(rows, list):
            return []
        normalized_rows: list[dict] = []
        for row in rows:
            if isinstance(row, dict):
                normalized_rows.append(dict(row))
        return normalized_rows

    def _policy_bool(row: dict | None, *keys: str, default: bool = False) -> bool:
        if not isinstance(row, dict):
            return default
        for key in keys:
            if key in row and row.get(key) is not None:
                return bool(row.get(key))
        return default

    def _policy_row_keys(row: dict) -> list[tuple[str, str, str, str]]:
        flow_uuid = str(row.get("flow_uuid") or row.get("flowUuid") or "").strip()
        source_process_uuid = str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip()
        source_node_id = str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip()
        name = str(row.get("name") or "").strip()
        keys: list[tuple[str, str, str, str]] = []
        if flow_uuid:
            keys.append((flow_uuid, source_process_uuid, source_node_id, name))
            keys.append((flow_uuid, source_process_uuid, "", name))
            keys.append((flow_uuid, "", source_node_id, name))
            keys.append((flow_uuid, "", "", name))
            keys.append((flow_uuid, source_process_uuid, source_node_id, ""))
            keys.append((flow_uuid, "", "", ""))
        return keys

    def _derived_row_key(row: dict) -> tuple[str, str, str, str]:
        return (
            str(row.get("flow_uuid") or "").strip(),
            str(row.get("source_process_uuid") or "").strip(),
            str(row.get("source_node_id") or "").strip(),
            str(row.get("name") or "").strip(),
        )

    fallback_inputs = _policy_rows(fallback_policy or {}, "inputs")
    fallback_outputs = _policy_rows(fallback_policy or {}, "outputs")

    fallback_input_map: dict[tuple[str, str, str, str], dict] = {}
    fallback_output_map: dict[tuple[str, str, str, str], dict] = {}
    for row in fallback_inputs:
        for key in _policy_row_keys(row):
            fallback_input_map.setdefault(key, row)
    for row in fallback_outputs:
        for key in _policy_row_keys(row):
            fallback_output_map.setdefault(key, row)

    inbound_pairs: set[tuple[str, str]] = set()
    outbound_pairs: set[tuple[str, str]] = set()
    for edge in graph.exchanges:
        to_node = str(edge.toNode or "").strip()
        from_node = str(edge.fromNode or "").strip()
        flow_uuid = str(edge.flowUuid or "").strip()
        if not to_node or not flow_uuid:
            if not from_node or not flow_uuid:
                continue
        target_port_id = _port_id_from_handle(
            str(getattr(edge, "target_port_id", "") or getattr(edge, "targetHandle", "") or ""),
            "in",
        )
        source_port_id = _port_id_from_handle(
            str(getattr(edge, "source_port_id", "") or getattr(edge, "sourceHandle", "") or ""),
            "out",
        )
        if to_node and flow_uuid:
            inbound_pairs.add((to_node, flow_uuid))
            if target_port_id:
                inbound_pairs.add((f"{to_node}::{target_port_id}", flow_uuid))
        if from_node and flow_uuid:
            outbound_pairs.add((from_node, flow_uuid))
            if source_port_id:
                outbound_pairs.add((f"{from_node}::{source_port_id}", flow_uuid))

    inputs: list[dict] = []
    outputs: list[dict] = []

    for node in graph.nodes:
        node_id = str(node.id or "")
        process_uuid = str(node.process_uuid or "")
        process_name = str(node.name or "")
        for port in node.inputs:
            flow_uuid = str(port.flowUuid or "")
            if not flow_uuid or port.type == "biosphere":
                continue
            specific_key = (f"{node_id}::{str(port.id or '')}", flow_uuid)
            broad_key = (node_id, flow_uuid)
            if specific_key in inbound_pairs or broad_key in inbound_pairs:
                continue
            derived = {
                "flow_uuid": flow_uuid,
                "name": str(port.name or ""),
                "source_process_uuid": process_uuid,
                "source_process_name": process_name,
                "source_node_id": node_id,
            }
            fallback_row = fallback_input_map.get(_derived_row_key(derived))
            derived["internal_exposed"] = _policy_bool(
                fallback_row,
                "internal_exposed",
                "internalExposed",
                default=_is_pts_port_exposed_like(port),
            )
            derived["external_visible"] = _policy_bool(
                fallback_row,
                "external_visible",
                "externalVisible",
                "showOnNode",
                default=bool(getattr(port, "showOnNode", False)),
            )
            inputs.append(derived)
        for port in node.outputs:
            flow_uuid = str(port.flowUuid or "")
            if not flow_uuid or port.type == "biosphere":
                continue
            specific_key = (f"{node_id}::{str(port.id or '')}", flow_uuid)
            broad_key = (node_id, flow_uuid)
            is_product = bool(port.isProduct)
            if not is_product and (specific_key in outbound_pairs or broad_key in outbound_pairs):
                continue
            derived = {
                "flow_uuid": flow_uuid,
                "name": str(port.name or ""),
                "source_process_uuid": process_uuid,
                "source_process_name": process_name,
                "source_node_id": node_id,
                "is_product": is_product,
            }
            fallback_row = fallback_output_map.get(_derived_row_key(derived))
            derived["internal_exposed"] = _policy_bool(
                fallback_row,
                "internal_exposed",
                "internalExposed",
                default=_is_pts_port_exposed_like(port),
            )
            derived["external_visible"] = _policy_bool(
                fallback_row,
                "external_visible",
                "externalVisible",
                "showOnNode",
                default=bool(getattr(port, "showOnNode", False)),
            )
            if fallback_row and "is_product" in fallback_row and fallback_row.get("is_product") is not None:
                derived["is_product"] = bool(fallback_row.get("is_product"))
            outputs.append(derived)

    if inputs or outputs:
        return {"inputs": inputs, "outputs": outputs}
    return dict(fallback_policy or {})


def _sanitize_pts_ports_policy(*, ports_policy: dict | None, pts_graph: dict | None = None) -> dict:
    policy = dict(ports_policy or {})
    inputs = policy.get("inputs") if isinstance(policy.get("inputs"), list) else []
    outputs = policy.get("outputs") if isinstance(policy.get("outputs"), list) else []

    allowed_inputs: set[tuple[str, str, str, str]] | None = None
    allowed_outputs: set[tuple[str, str, str, str]] | None = None
    if isinstance(pts_graph, dict) and pts_graph:
        normalized = _normalize_pts_ports_policy_from_graph(pts_graph=pts_graph, fallback_policy=None)
        allowed_inputs = {
            (
                str(row.get("flow_uuid") or "").strip(),
                str(row.get("source_process_uuid") or "").strip(),
                str(row.get("source_node_id") or "").strip(),
                str(row.get("name") or "").strip(),
            )
            for row in (normalized.get("inputs") or [])
            if isinstance(row, dict)
        }
        allowed_outputs = {
            (
                str(row.get("flow_uuid") or "").strip(),
                str(row.get("source_process_uuid") or "").strip(),
                str(row.get("source_node_id") or "").strip(),
                str(row.get("name") or "").strip(),
            )
            for row in (normalized.get("outputs") or [])
            if isinstance(row, dict)
        }

    def _norm_key(row: dict) -> tuple[str, str, str, str]:
        return (
            str(row.get("flow_uuid") or row.get("flowUuid") or "").strip(),
            str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
            str(row.get("name") or "").strip(),
        )

    sanitized_inputs: list[dict] = []
    for row in inputs:
        if not isinstance(row, dict):
            continue
        norm = {
            "flow_uuid": str(row.get("flow_uuid") or row.get("flowUuid") or "").strip(),
            "name": str(row.get("name") or "").strip(),
            "source_process_uuid": str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            "source_process_name": str(row.get("source_process_name") or row.get("sourceProcessName") or "").strip(),
            "source_node_id": str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
            "internal_exposed": bool(row.get("internal_exposed") if "internal_exposed" in row else row.get("internalExposed")),
            "external_visible": bool(
                row.get("external_visible")
                if "external_visible" in row
                else row.get("externalVisible", row.get("showOnNode"))
            ),
        }
        if not norm["flow_uuid"]:
            continue
        if allowed_inputs is not None and _norm_key(norm) not in allowed_inputs:
            continue
        sanitized_inputs.append(norm)

    sanitized_outputs: list[dict] = []
    for row in outputs:
        if not isinstance(row, dict):
            continue
        norm = {
            "flow_uuid": str(row.get("flow_uuid") or row.get("flowUuid") or "").strip(),
            "name": str(row.get("name") or "").strip(),
            "source_process_uuid": str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            "source_process_name": str(row.get("source_process_name") or row.get("sourceProcessName") or "").strip(),
            "source_node_id": str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
            "internal_exposed": bool(row.get("internal_exposed") if "internal_exposed" in row else row.get("internalExposed")),
            "external_visible": bool(
                row.get("external_visible")
                if "external_visible" in row
                else row.get("externalVisible", row.get("showOnNode"))
            ),
            "is_product": bool(row.get("is_product") if "is_product" in row else row.get("isProduct")),
        }
        if not norm["flow_uuid"]:
            continue
        if allowed_outputs is not None and _norm_key(norm) not in allowed_outputs:
            continue
        sanitized_outputs.append(norm)

    return {"inputs": sanitized_inputs, "outputs": sanitized_outputs}


def _get_pts_resource_ports_policy(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
) -> dict | None:
    row = (
        db.query(PtsResource)
        .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == pts_uuid)
        .first()
    )
    if row is None:
        return None
    return _sanitize_pts_ports_policy(
        ports_policy=dict(row.ports_policy_json or {}),
        pts_graph=dict(row.pts_graph_json or {}),
    )


def _apply_pts_resource_policy_override(
    *,
    db: Session,
    project_id: str,
    definition: dict,
) -> dict:
    pts_uuid = str(definition.get("pts_uuid") or "").strip()
    if not pts_uuid:
        return definition
    resource_policy = _get_pts_resource_ports_policy(db=db, project_id=project_id, pts_uuid=pts_uuid)
    if resource_policy:
        definition = dict(definition)
        definition["ports_policy"] = resource_policy
    return definition


def _build_pts_shell_node_snapshot(pts_node: HybridNode) -> dict:
    return {
        "id": pts_node.id,
        "node_kind": pts_node.node_kind,
        "mode": pts_node.mode,
        "lci_role": pts_node.lci_role,
        "pts_uuid": pts_node.pts_uuid,
        "pts_published_version": pts_node.pts_published_version,
        "pts_published_artifact_id": pts_node.pts_published_artifact_id,
        "process_uuid": pts_node.process_uuid,
        "name": pts_node.name,
        "location": pts_node.location,
        "reference_product": pts_node.reference_product,
        "allocation_method": pts_node.allocation_method,
        "inputs": [port.model_dump(mode="python", by_alias=True) for port in pts_node.inputs],
        "outputs": [port.model_dump(mode="python", by_alias=True) for port in pts_node.outputs],
        "emissions": [port.model_dump(mode="python", by_alias=True) for port in pts_node.emissions],
    }


def _upsert_pts_resource_from_graph(*, db: Session, project_id: str, graph: HybridGraph, pts_node: HybridNode) -> PtsResource | None:
    internal_canvas = _find_pts_internal_canvas(graph, pts_node.id)
    if internal_canvas is None:
        return None
    pts_uuid = str(pts_node.pts_uuid or pts_node.process_uuid or pts_node.id).strip()
    pts_graph = {
        "functionalUnit": graph.functionalUnit,
        "nodes": internal_canvas.get("nodes") if isinstance(internal_canvas.get("nodes"), list) else [],
        "exchanges": internal_canvas.get("edges") if isinstance(internal_canvas.get("edges"), list) else [],
        "metadata": {
            "kind": "pts_internal",
            "canvas_id": str(internal_canvas.get("id") or ""),
            "parentPtsNodeId": str(internal_canvas.get("parentPtsNodeId") or pts_node.id),
            "name": str(internal_canvas.get("name") or ""),
        },
    }
    normalized_ports_policy = _normalize_pts_ports_policy_from_graph(
        pts_graph=pts_graph,
        fallback_policy=None,
    )
    resource = (
        db.query(PtsResource)
        .filter(PtsResource.pts_uuid == pts_uuid)
        .first()
    )
    if resource is None:
        resource = PtsResource(
            project_id=project_id,
            pts_uuid=pts_uuid,
            name=str(pts_node.name or ""),
            pts_node_id=str(pts_node.id or ""),
            latest_graph_hash=str(compute_pts_graph_hash(graph, pts_node.id)),
            pts_graph_json=pts_graph,
            ports_policy_json=normalized_ports_policy,
            shell_node_json=_build_pts_shell_node_snapshot(pts_node),
        )
        db.add(resource)
    else:
        resource.project_id = project_id
        resource.name = str(pts_node.name or "")
        resource.pts_node_id = str(pts_node.id or "")
        resource.latest_graph_hash = str(compute_pts_graph_hash(graph, pts_node.id))
        resource.pts_graph_json = pts_graph
        resource.ports_policy_json = normalized_ports_policy
        resource.shell_node_json = _build_pts_shell_node_snapshot(pts_node)
    latest_external = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .first()
    )
    latest_compile = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    if latest_compile is not None:
        resource.compiled_graph_hash = str(latest_compile.graph_hash or "")
        resource.latest_compile_version = int(latest_compile.compile_version or 0) or resource.latest_compile_version
    if latest_external is not None:
        resource.latest_published_version = int(latest_external.published_version or 0) or resource.latest_published_version
        if resource.active_published_version is None:
            resource.active_published_version = resource.latest_published_version
        resource.published_at = latest_external.updated_at or latest_external.created_at
    return resource


def _next_pts_compile_version(*, db: Session, project_id: str, pts_uuid: str) -> int:
    current_max = (
        db.query(func.max(PtsCompileArtifact.compile_version))
        .filter(PtsCompileArtifact.project_id == project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .scalar()
    )
    return int(current_max or 0) + 1


def _next_pts_published_version(*, db: Session, project_id: str, pts_uuid: str) -> int:
    current_max = (
        db.query(func.max(PtsExternalArtifact.published_version))
        .filter(PtsExternalArtifact.project_id == project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .scalar()
    )
    return int(current_max or 0) + 1


def _sync_pts_resources_from_graph(*, db: Session, project_id: str, graph: HybridGraph) -> list[str]:
    synced: list[str] = []
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        resource = _upsert_pts_resource_from_graph(db=db, project_id=project_id, graph=graph, pts_node=node)
        if resource is not None:
            synced.append(str(resource.pts_uuid))
    return synced


def _migrate_pts_resources(
    *,
    db: Session,
    project_id: str | None = None,
    dry_run: bool = True,
    latest_only: bool = True,
) -> dict:
    scanned_projects = 0
    scanned_versions = 0
    migrated_projects = 0
    migrated_resources = 0
    skipped_projects = 0
    failures: list[dict] = []
    items: list[dict] = []

    project_query = db.query(Model).order_by(Model.created_at.asc(), Model.id.asc())
    if project_id:
        project_query = project_query.filter(Model.id == project_id)
    projects = project_query.all()

    for project in projects:
        scanned_projects += 1
        version_query = db.query(ModelVersion).filter(ModelVersion.model_id == project.id)
        if latest_only:
            version_rows = [version_query.order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc()).first()]
        else:
            version_rows = version_query.order_by(ModelVersion.version.asc(), ModelVersion.created_at.asc()).all()

        pts_uuids: set[str] = set()
        project_failed = False

        for row in [item for item in version_rows if item is not None]:
            scanned_versions += 1
            source = row.hybrid_graph_json if isinstance(row.hybrid_graph_json, dict) else {}
            try:
                graph = HybridGraph.model_validate(source)
            except Exception as exc:
                project_failed = True
                failures.append(
                    {
                        "project_id": str(project.id),
                        "project_name": str(project.name or ""),
                        "version": int(row.version),
                        "error": str(exc),
                    }
                )
                continue

            current_pts = [node for node in graph.nodes if node.node_kind == "pts_module" and str(node.pts_uuid or "").strip()]
            if not current_pts:
                continue

            pts_uuids.update(str(node.pts_uuid or "") for node in current_pts)
            if not dry_run:
                _sync_pts_resources_from_graph(db=db, project_id=str(project.id), graph=graph)

        if not pts_uuids:
            skipped_projects += 1
            continue

        if not project_failed:
            migrated_projects += 1
        migrated_resources += len(pts_uuids)
        if len(items) < 200:
            items.append(
                {
                    "project_id": str(project.id),
                    "project_name": str(project.name or ""),
                    "pts_uuids": sorted(pts_uuids),
                }
            )

    if not dry_run:
        db.commit()

    return {
        "migration": "pts-resources-v1",
        "dry_run": dry_run,
        "latest_only": latest_only,
        "project_id": project_id,
        "scanned_projects": scanned_projects,
        "scanned_versions": scanned_versions,
        "migrated_projects": migrated_projects,
        "migrated_resources": migrated_resources,
        "skipped_projects": skipped_projects,
        "items": items,
        "failures": failures[:50],
    }


def _migrate_pts_published_version_bindings(
    *,
    db: Session,
    project_id: str | None = None,
    dry_run: bool = True,
    latest_only: bool = True,
) -> dict:
    scanned_projects = 0
    scanned_versions = 0
    migrated_projects = 0
    updated_versions = 0
    bound_nodes = 0
    skipped_projects = 0
    failures: list[dict] = []
    items: list[dict] = []

    project_query = db.query(Model).order_by(Model.created_at.asc(), Model.id.asc())
    if project_id:
        project_query = project_query.filter(Model.id == project_id)
    projects = project_query.all()

    for project in projects:
        scanned_projects += 1
        version_query = db.query(ModelVersion).filter(ModelVersion.model_id == project.id)
        if latest_only:
            version_rows = [version_query.order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc()).first()]
        else:
            version_rows = version_query.order_by(ModelVersion.version.asc(), ModelVersion.created_at.asc()).all()

        project_updates = 0
        project_bound_nodes = 0
        project_versions: list[int] = []

        for row in [item for item in version_rows if item is not None]:
            scanned_versions += 1
            source = row.hybrid_graph_json if isinstance(row.hybrid_graph_json, dict) else {}
            try:
                graph = HybridGraph.model_validate(source)
            except Exception as exc:
                failures.append(
                    {
                        "project_id": str(project.id),
                        "project_name": str(project.name or ""),
                        "version": int(row.version),
                        "error": str(exc),
                    }
                )
                continue

            pts_nodes = [node for node in graph.nodes if node.node_kind == "pts_module" and str(node.pts_uuid or "").strip()]
            if not pts_nodes:
                continue

            before_bindings = {
                str(node.id): (
                    int(node.pts_published_version) if node.pts_published_version is not None else None,
                    str(node.pts_published_artifact_id or ""),
                )
                for node in pts_nodes
            }

            _bind_pts_published_versions_for_graph(db=db, project_id=str(project.id), graph=graph)
            _project_pts_external_ports_into_graph(db=db, project_id=str(project.id), graph=graph)

            after_bindings = {
                str(node.id): (
                    int(node.pts_published_version) if node.pts_published_version is not None else None,
                    str(node.pts_published_artifact_id or ""),
                )
                for node in pts_nodes
            }
            changed_nodes = [
                node_id
                for node_id, binding in after_bindings.items()
                if before_bindings.get(node_id) != binding
            ]

            normalized_graph_json = _normalize_graph_json_for_storage(graph.model_dump(mode="python"))
            normalized_hash = _compute_graph_hash_from_graph_json(normalized_graph_json)
            current_hash = str(row.graph_hash or "")
            current_graph = row.hybrid_graph_json if isinstance(row.hybrid_graph_json, dict) else {}
            changed = bool(changed_nodes) or current_graph != normalized_graph_json or current_hash != normalized_hash
            if not changed:
                continue

            project_updates += 1
            project_bound_nodes += len(changed_nodes)
            project_versions.append(int(row.version))

            if not dry_run:
                row.hybrid_graph_json = normalized_graph_json
                row.graph_hash = normalized_hash

        if project_updates == 0:
            skipped_projects += 1
            continue

        migrated_projects += 1
        updated_versions += project_updates
        bound_nodes += project_bound_nodes
        if len(items) < 200:
            items.append(
                {
                    "project_id": str(project.id),
                    "project_name": str(project.name or ""),
                    "updated_versions": project_versions,
                    "bound_nodes": project_bound_nodes,
                }
            )

    if not dry_run:
        db.commit()

    return {
        "migration": "pts-published-version-bindings-v1",
        "dry_run": dry_run,
        "latest_only": latest_only,
        "project_id": project_id,
        "scanned_projects": scanned_projects,
        "scanned_versions": scanned_versions,
        "migrated_projects": migrated_projects,
        "updated_versions": updated_versions,
        "bound_nodes": bound_nodes,
        "skipped_projects": skipped_projects,
        "items": items,
        "failures": failures[:50],
    }


def _canonical_json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _normalize_node_json_for_storage(node_json: dict) -> dict:
    raw = dict(node_json or {})
    normalized = HybridNode.model_validate(raw).model_dump(mode="python")
    raw["node_kind"] = normalized.get("node_kind", raw.get("node_kind"))
    raw["inputs"] = normalized.get("inputs", [])
    raw["outputs"] = normalized.get("outputs", [])
    raw["emissions"] = normalized.get("emissions", [])
    raw["mode"] = normalized.get("mode", raw.get("mode"))
    raw["process_uuid"] = normalized.get("process_uuid", raw.get("process_uuid"))
    raw["pts_uuid"] = normalized.get("pts_uuid", raw.get("pts_uuid"))
    return raw


def _restore_canvas_node_positions(*, source_canvas: dict, normalized_canvas: dict) -> dict:
    if not isinstance(normalized_canvas, dict):
        return normalized_canvas
    source_nodes = source_canvas.get("nodes") if isinstance(source_canvas.get("nodes"), list) else []
    normalized_nodes = normalized_canvas.get("nodes") if isinstance(normalized_canvas.get("nodes"), list) else []
    source_by_id = {
        str(node.get("id") or "").strip(): node
        for node in source_nodes
        if isinstance(node, dict) and str(node.get("id") or "").strip()
    }
    restored_nodes: list[dict] = []
    for node in normalized_nodes:
        if not isinstance(node, dict):
            restored_nodes.append(node)
            continue
        restored = dict(node)
        source_node = source_by_id.get(str(restored.get("id") or "").strip())
        if isinstance(source_node, dict):
            position = source_node.get("position") if isinstance(source_node.get("position"), dict) else None
            if isinstance(position, dict):
                restored["position"] = {
                    "x": float(position.get("x", 0.0)),
                    "y": float(position.get("y", 0.0)),
                }
        restored_nodes.append(restored)
    updated = dict(normalized_canvas)
    updated["nodes"] = restored_nodes
    source_meta = source_canvas.get("metadata") if isinstance(source_canvas.get("metadata"), dict) else {}
    normalized_meta = updated.get("metadata") if isinstance(updated.get("metadata"), dict) else {}
    source_positions = source_meta.get("node_positions") if isinstance(source_meta.get("node_positions"), dict) else None
    if isinstance(source_positions, dict):
        updated["metadata"] = {
            **dict(normalized_meta),
            "node_positions": dict(source_positions),
        }
    return updated


def _restore_graph_node_positions(*, source_graph_json: dict, normalized_graph_json: dict) -> dict:
    if not isinstance(source_graph_json, dict) or not isinstance(normalized_graph_json, dict):
        return normalized_graph_json

    source_nodes = source_graph_json.get("nodes") if isinstance(source_graph_json.get("nodes"), list) else []
    normalized_nodes = normalized_graph_json.get("nodes") if isinstance(normalized_graph_json.get("nodes"), list) else []
    source_by_id = {
        str(node.get("id") or "").strip(): node
        for node in source_nodes
        if isinstance(node, dict) and str(node.get("id") or "").strip()
    }

    restored_nodes: list[dict] = []
    for node in normalized_nodes:
        if not isinstance(node, dict):
            restored_nodes.append(node)
            continue
        restored = dict(node)
        source_node = source_by_id.get(str(restored.get("id") or "").strip())
        if isinstance(source_node, dict):
            position = source_node.get("position") if isinstance(source_node.get("position"), dict) else None
            if isinstance(position, dict):
                restored["position"] = {
                    "x": float(position.get("x", 0.0)),
                    "y": float(position.get("y", 0.0)),
                }
        restored_nodes.append(restored)

    updated = dict(normalized_graph_json)
    updated["nodes"] = restored_nodes

    source_meta = source_graph_json.get("metadata") if isinstance(source_graph_json.get("metadata"), dict) else {}
    normalized_meta = updated.get("metadata") if isinstance(updated.get("metadata"), dict) else {}
    source_positions = source_meta.get("node_positions") if isinstance(source_meta.get("node_positions"), dict) else None
    if isinstance(source_positions, dict):
        updated["metadata"] = {
            **dict(normalized_meta),
            "node_positions": dict(source_positions),
        }

    source_canvases = source_meta.get("canvases") if isinstance(source_meta.get("canvases"), list) else []
    normalized_canvases = normalized_meta.get("canvases") if isinstance(normalized_meta.get("canvases"), list) else []
    if normalized_canvases:
        source_canvas_by_id = {
            str(canvas.get("id") or "").strip(): canvas
            for canvas in source_canvases
            if isinstance(canvas, dict) and str(canvas.get("id") or "").strip()
        }
        restored_canvases: list[dict] = []
        for idx, canvas in enumerate(normalized_canvases):
            if not isinstance(canvas, dict):
                restored_canvases.append(canvas)
                continue
            source_canvas = source_canvas_by_id.get(str(canvas.get("id") or "").strip())
            if source_canvas is None and idx < len(source_canvases) and isinstance(source_canvases[idx], dict):
                source_canvas = source_canvases[idx]
            restored_canvases.append(
                _restore_canvas_node_positions(
                    source_canvas=source_canvas if isinstance(source_canvas, dict) else {},
                    normalized_canvas=canvas,
                )
            )
        updated["metadata"] = {
            **dict(updated.get("metadata") or {}),
            "canvases": restored_canvases,
        }

    return updated


def _build_default_graph_viewport(
    *,
    node_positions: dict[str, dict[str, float]] | None = None,
    nodes: list[dict] | None = None,
) -> dict | None:
    position_rows: list[tuple[float, float]] = []

    if isinstance(node_positions, dict):
        for row in node_positions.values():
            if not isinstance(row, dict):
                continue
            position_rows.append((_safe_float(row.get("x")), _safe_float(row.get("y"))))

    if not position_rows and isinstance(nodes, list):
        grid_columns = 4
        grid_dx = 360.0
        grid_dy = 240.0
        for idx, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            pos = node.get("position") if isinstance(node.get("position"), dict) else None
            if isinstance(pos, dict):
                position_rows.append((_safe_float(pos.get("x")), _safe_float(pos.get("y"))))
            else:
                position_rows.append(((idx % grid_columns) * grid_dx, (idx // grid_columns) * grid_dy))

    if not position_rows:
        return None

    node_width = 260.0
    node_height = 180.0
    padding = 120.0
    viewport_width = 1400.0
    viewport_height = 900.0

    min_x = min(x for x, _ in position_rows)
    min_y = min(y for _, y in position_rows)
    max_x = max(x for x, _ in position_rows) + node_width
    max_y = max(y for _, y in position_rows) + node_height

    bounds_width = max(max_x - min_x, node_width)
    bounds_height = max(max_y - min_y, node_height)
    zoom_x = (viewport_width - padding * 2.0) / bounds_width
    zoom_y = (viewport_height - padding * 2.0) / bounds_height
    zoom = max(0.2, min(1.2, zoom_x, zoom_y))

    center_x = min_x + bounds_width / 2.0
    center_y = min_y + bounds_height / 2.0
    return {
        "x": viewport_width / 2.0 - center_x * zoom,
        "y": viewport_height / 2.0 - center_y * zoom,
        "zoom": zoom,
    }


def _normalize_graph_canvases_for_storage(graph_json: dict) -> dict:
    metadata = graph_json.get("metadata")
    if not isinstance(metadata, dict):
        return graph_json
    canvases = metadata.get("canvases")
    if not isinstance(canvases, list):
        return graph_json

    normalized_canvases: list[dict] = []
    for canvas in canvases:
        if not isinstance(canvas, dict):
            normalized_canvases.append(canvas)
            continue
        canvas_copy = dict(canvas)
        nodes = canvas_copy.get("nodes")
        if isinstance(nodes, list):
            normalized_nodes: list[dict] = []
            for node in nodes:
                if not isinstance(node, dict):
                    normalized_nodes.append(node)
                    continue
                if "inputs" in node or "outputs" in node or "emissions" in node:
                    normalized_nodes.append(_normalize_node_json_for_storage(node))
                else:
                    normalized_nodes.append(node)
            canvas_copy["nodes"] = normalized_nodes
        normalized_canvases.append(canvas_copy)

    metadata_copy = dict(metadata)
    metadata_copy["canvases"] = normalized_canvases
    graph_json["metadata"] = metadata_copy
    return graph_json


_NON_PTS_UNIQUE_NAME_NODE_KINDS: set[str] = {"unit_process", "market_process", "lci_dataset"}


def _process_name_uniqueness_group(node_kind: str) -> str | None:
    normalized = str(node_kind or "").strip()
    if normalized in _NON_PTS_UNIQUE_NAME_NODE_KINDS:
        return "process"
    if normalized == "pts_module":
        return "pts_module"
    return None


def _raise_if_duplicate_process_names_in_graph(*, graph: HybridGraph, scope_label: str) -> None:
    buckets: dict[tuple[str, str], list[HybridNode]] = defaultdict(list)
    for node in list(graph.nodes or []):
        group = _process_name_uniqueness_group(str(node.node_kind or ""))
        if not group:
            continue
        name = str(node.name or "").strip()
        if not name:
            continue
        buckets[(group, name.casefold())].append(node)

    duplicates: list[dict] = []
    for (group, _normalized_name), nodes in buckets.items():
        if len(nodes) < 2:
            continue
        duplicates.append(
            {
                "scope": scope_label,
                "group": group,
                "name": str(nodes[0].name or "").strip(),
                "node_ids": [str(node.id or "") for node in nodes],
                "node_kinds": [str(node.node_kind or "") for node in nodes],
            }
        )

    if not duplicates:
        return

    duplicates.sort(key=lambda item: (str(item.get("scope") or ""), str(item.get("group") or ""), str(item.get("name") or "")))
    first = duplicates[0]
    raise HTTPException(
        status_code=409,
        detail={
            "code": "DUPLICATE_PROCESS_NAME",
            "message": f"Duplicate process name is not allowed in {scope_label}: {first['name']}",
            "scope": scope_label,
            "duplicates": duplicates,
        },
    )


def _validate_process_name_uniqueness_for_graph_json(*, graph_json: dict, scope_label: str) -> None:
    graph = HybridGraph.model_validate(graph_json)
    _raise_if_duplicate_process_names_in_graph(graph=graph, scope_label=scope_label)

    metadata = graph_json.get("metadata") if isinstance(graph_json, dict) else None
    canvases = metadata.get("canvases") if isinstance(metadata, dict) else None
    if not isinstance(canvases, list):
        return

    for idx, canvas in enumerate(canvases):
        if not isinstance(canvas, dict):
            continue
        canvas_graph = HybridGraph.model_validate(
            {
                "functionalUnit": str(canvas.get("functionalUnit") or graph.functionalUnit or "product system"),
                "nodes": canvas.get("nodes") or [],
                "exchanges": canvas.get("exchanges") or [],
                "metadata": canvas.get("metadata") or {},
            }
        )
        canvas_label = str(canvas.get("name") or canvas.get("id") or f"canvas[{idx}]")
        _raise_if_duplicate_process_names_in_graph(graph=canvas_graph, scope_label=f"{scope_label}.{canvas_label}")


def _normalize_graph_json_for_storage(graph_json: dict) -> dict:
    _enrich_market_process_input_sources_in_graph_json(graph_json)
    _validate_process_name_uniqueness_for_graph_json(graph_json=graph_json, scope_label="main_graph")
    graph = HybridGraph.model_validate(graph_json)
    normalize_graph_product_flags(graph)
    normalize_graph_edge_port_ids(graph)
    normalized = graph.model_dump(mode="python")
    normalized = _normalize_graph_canvases_for_storage(normalized)
    _validate_process_name_uniqueness_for_graph_json(graph_json=normalized, scope_label="main_graph")
    normalized = _enrich_market_process_input_sources_in_graph_json(normalized)
    return _restore_graph_node_positions(source_graph_json=graph_json, normalized_graph_json=normalized)



_UI_HASH_NOISE_KEYS: set[str] = {
    "selected",
    "dragging",
    "resizing",
    "positionAbsolute",
    "width",
    "height",
    "__rf",
    "updatedAt",
    "createdAt",
    "lastSavedAt",
    "lastModifiedAt",
    "uiTimestamp",
    "clientTimestamp",
    "tempId",
    "ephemeral",
}


def _resolve_indicator_index_csv_path() -> Path | None:
    local = Path(__file__).resolve().parent.parent / "data" / "EF3.1" / "indicator_index.csv"
    if local.exists():
        return local

    base = Path(settings.nebula_lca_ef31_dir)
    if base.is_file():
        return base
    candidate = base / "indicator_index.csv"
    if candidate.exists():
        return candidate
    return None


def _load_indicator_meta_by_index() -> dict[int, dict[str, str]]:
    global _indicator_meta_by_index_cache, _indicator_units_cache_path, _indicator_units_cache_mtime

    csv_path = _resolve_indicator_index_csv_path()
    path_str = str(csv_path) if csv_path is not None else ""
    current_mtime: float | None = None
    if csv_path is not None and csv_path.exists():
        try:
            current_mtime = float(csv_path.stat().st_mtime)
        except OSError:
            current_mtime = None

    if (
        _indicator_meta_by_index_cache is not None
        and _indicator_units_cache_path == path_str
        and _indicator_units_cache_mtime == current_mtime
    ):
        return _indicator_meta_by_index_cache

    if csv_path is None or not csv_path.exists():
        _indicator_meta_by_index_cache = {}
        _indicator_units_cache_path = path_str
        _indicator_units_cache_mtime = None
        return _indicator_meta_by_index_cache

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(2048)
        handle.seek(0)
        delimiter = ";" if sample.count(";") >= sample.count(",") else ","
        reader = csv.DictReader(handle, delimiter=delimiter)
        mapping: dict[int, dict[str, str]] = {}
        for row in reader:
            idx_raw = str(row.get("indicator_index") or "").strip()
            if not idx_raw:
                continue
            try:
                idx = int(idx_raw)
            except ValueError:
                continue
            mapping[idx] = {
                "method_en": str(row.get("method_en") or "").strip(),
                "method_zh": str(row.get("method_zh") or "").strip(),
                "indicator_en": str(row.get("indicator_en") or "").strip(),
                "indicator_zh": str(row.get("indicator_zh") or "").strip(),
                "indicator_unit": str(row.get("LCIA_unit") or row.get("lcia_unit") or "").strip(),
            }

    _indicator_meta_by_index_cache = mapping
    _indicator_units_cache_path = path_str
    _indicator_units_cache_mtime = current_mtime
    return mapping


def _infer_indicator_unit(entry: dict) -> str:
    unit = str(entry.get("indicator_unit") or entry.get("unit") or "").strip()
    if unit and unit not in {"-", "--", "N/A", "n/a"}:
        return unit
    idx_value = entry.get("indicator_index")
    idx_raw = "" if idx_value is None else str(idx_value).strip()
    if idx_raw:
        try:
            idx = int(idx_raw)
            meta = _load_indicator_meta_by_index().get(idx, {})
            return str(meta.get("indicator_unit") or "").strip()
        except ValueError:
            return ""
    return ""


def _enrich_indicator_index_with_units(indicator_index: object) -> list:
    rows = indicator_index if isinstance(indicator_index, list) else []
    enriched: list = []
    meta_by_index = _load_indicator_meta_by_index()
    for item in rows:
        if not isinstance(item, dict):
            enriched.append(item)
            continue
        row = dict(item)
        idx_value = row.get("indicator_index")
        idx_raw = "" if idx_value is None else str(idx_value).strip()
        csv_meta: dict[str, str] = {}
        if idx_raw:
            try:
                csv_meta = meta_by_index.get(int(idx_raw), {})
            except ValueError:
                csv_meta = {}

        for key in ("method_zh", "indicator_zh"):
            cur = str(row.get(key) or "").strip()
            if cur:
                continue
            fallback = str(csv_meta.get(key) or "").strip()
            if fallback:
                row[key] = fallback

        row.pop("ecoinvent_category", None)

        unit = _infer_indicator_unit(row)
        if unit:
            row["indicator_unit"] = unit
            # Keep a generic field for frontend fallback compatibility.
            row["unit"] = unit
        enriched.append(row)
    return enriched


def _round_float_for_hash(value: object) -> object:
    if isinstance(value, float):
        return round(value, 12)
    return value


def _canonicalize_value_for_hash(value: object) -> object:
    if isinstance(value, dict):
        sanitized: dict[str, object] = {}
        for key in sorted(value.keys()):
            if key in _UI_HASH_NOISE_KEYS:
                continue
            sanitized[key] = _canonicalize_value_for_hash(value[key])
        return sanitized
    if isinstance(value, list):
        items = [_canonicalize_value_for_hash(item) for item in value]
        if items and all(isinstance(item, dict) for item in items):
            def _item_key(item: dict) -> tuple:
                return (
                    str(item.get("id") or ""),
                    str(item.get("nodeId") or ""),
                    str(item.get("kind") or ""),
                    str(item.get("parentPtsNodeId") or ""),
                    str(item.get("parentNodeId") or ""),
                    str(item.get("fromNode") or ""),
                    str(item.get("toNode") or ""),
                    str(item.get("flowUuid") or ""),
                    str(item.get("process_uuid") or item.get("processUuid") or ""),
                )

            items = sorted(items, key=lambda item: _item_key(item))
        return items
    return _round_float_for_hash(value)


def _compute_graph_hash_from_graph_json(graph_json: dict) -> str:
    normalized = _normalize_graph_json_for_storage(graph_json)
    canonical_value = _canonicalize_value_for_hash(normalized)
    canonical_json = _canonical_json(canonical_value)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _compute_graph_hash_from_graph(graph: HybridGraph) -> str:
    return _compute_graph_hash_from_graph_json(graph.model_dump(mode="python"))


def _iter_chunks(values: list[str], size: int = 500):
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _ensure_model_versions_hash_schema() -> dict:
    added_columns: list[str] = []
    created_indexes: list[str] = []

    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table("model_versions"):
            return {
                "table": "model_versions",
                "column_added": added_columns,
                "index_created": created_indexes,
                "status": "skipped_table_missing",
            }

        columns = {col["name"] for col in inspector.get_columns("model_versions")}
        if "graph_hash" not in columns:
            conn.execute(text("ALTER TABLE model_versions ADD COLUMN graph_hash VARCHAR(64)"))
            added_columns.append("graph_hash")

        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_model_versions_graph_hash ON model_versions (graph_hash)"))
        created_indexes.append("ix_model_versions_graph_hash")
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_model_versions_model_id_graph_hash ON model_versions (model_id, graph_hash)")
        )
        created_indexes.append("ix_model_versions_model_id_graph_hash")

    return {
        "table": "model_versions",
        "column_added": added_columns,
        "index_created": created_indexes,
        "status": "ok",
    }


def _migrate_flow_catalog_table_name() -> dict:
    with engine.begin() as conn:
        inspector = inspect(conn)
        has_new = inspector.has_table("flow_catalog")
        has_old = inspector.has_table("reference_flows")

        if has_new:
            return {"from": "reference_flows", "to": "flow_catalog", "status": "already_new"}
        if not has_old:
            return {"from": "reference_flows", "to": "flow_catalog", "status": "no_legacy_table"}

        conn.execute(text("ALTER TABLE reference_flows RENAME TO flow_catalog"))
        return {"from": "reference_flows", "to": "flow_catalog", "status": "renamed"}


def _ensure_projects_management_schema() -> dict:
    added_columns: list[str] = []
    created_indexes: list[str] = []

    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table("models"):
            return {
                "table": "models",
                "column_added": added_columns,
                "index_created": created_indexes,
                "status": "skipped_table_missing",
            }

        columns = {col["name"] for col in inspector.get_columns("models")}
        expected_columns = {
            "reference_product": "TEXT",
            "functional_unit": "TEXT",
            "system_boundary": "TEXT",
            "time_representativeness": "TEXT",
            "geography": "TEXT",
            "description": "TEXT",
            "status": "VARCHAR(32) DEFAULT 'active'",
            "updated_at": "DATETIME",
        }
        for name, ddl_type in expected_columns.items():
            if name in columns:
                continue
            conn.execute(text(f"ALTER TABLE models ADD COLUMN {name} {ddl_type}"))
            added_columns.append(name)

        conn.execute(text("UPDATE models SET status='active' WHERE status IS NULL OR TRIM(status)=''"))
        conn.execute(text("UPDATE models SET updated_at=created_at WHERE updated_at IS NULL"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_models_name ON models (name)"))
        created_indexes.append("ix_models_name")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_models_status ON models (status)"))
        created_indexes.append("ix_models_status")

    return {
        "table": "models",
        "column_added": added_columns,
        "index_created": created_indexes,
        "status": "ok",
    }


def _ensure_reference_processes_schema() -> dict:
    added_columns: list[str] = []
    created_indexes: list[str] = []

    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table("reference_processes"):
            return {
                "table": "reference_processes",
                "column_added": added_columns,
                "index_created": created_indexes,
                "status": "skipped_table_missing",
            }

        columns = {col["name"] for col in inspector.get_columns("reference_processes")}
        expected_columns = {
            "process_name_zh": "VARCHAR(255)",
            "process_name_en": "VARCHAR(255)",
            "process_type": "VARCHAR(64) DEFAULT 'unit_process'",
            "reference_flow_uuid": "VARCHAR(64)",
            "reference_flow_internal_id": "VARCHAR(64)",
            "process_json": "JSON",
            "source_file": "VARCHAR(1024)",
            "source_process_uuid": "VARCHAR(64)",
            "import_mode": "VARCHAR(32)",
            "import_report_json": "JSON",
            "created_at": "DATETIME",
            "updated_at": "DATETIME",
        }
        for name, ddl_type in expected_columns.items():
            if name in columns:
                continue
            conn.execute(text(f"ALTER TABLE reference_processes ADD COLUMN {name} {ddl_type}"))
            added_columns.append(name)

        conn.execute(text("UPDATE reference_processes SET process_type='unit_process' WHERE process_type IS NULL OR TRIM(process_type)=''"))
        conn.execute(text("UPDATE reference_processes SET created_at=CURRENT_TIMESTAMP WHERE created_at IS NULL"))
        conn.execute(text("UPDATE reference_processes SET updated_at=created_at WHERE updated_at IS NULL"))
        conn.execute(
            text(
                "UPDATE reference_processes "
                "SET process_name_zh = COALESCE(process_name_zh, process_name), "
                "process_name_en = COALESCE(process_name_en, process_name) "
                "WHERE process_name IS NOT NULL"
            )
        )
        conn.execute(
            text(
                "UPDATE reference_processes "
                "SET reference_flow_internal_id = COALESCE("
                "reference_flow_internal_id, "
                "json_extract(process_json, '$.reference_flow_internal_id')"
                ") "
                "WHERE process_json IS NOT NULL"
            )
        )

        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reference_processes_process_name_zh ON reference_processes (process_name_zh)"))
        created_indexes.append("ix_reference_processes_process_name_zh")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reference_processes_process_name_en ON reference_processes (process_name_en)"))
        created_indexes.append("ix_reference_processes_process_name_en")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reference_processes_process_type ON reference_processes (process_type)"))
        created_indexes.append("ix_reference_processes_process_type")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reference_processes_reference_flow_uuid ON reference_processes (reference_flow_uuid)"))
        created_indexes.append("ix_reference_processes_reference_flow_uuid")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reference_processes_source_process_uuid ON reference_processes (source_process_uuid)"))
        created_indexes.append("ix_reference_processes_source_process_uuid")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reference_processes_import_mode ON reference_processes (import_mode)"))
        created_indexes.append("ix_reference_processes_import_mode")

    return {
        "table": "reference_processes",
        "column_added": added_columns,
        "index_created": created_indexes,
        "status": "ok",
    }


def _bootstrap_reference_data_if_needed(*, db: Session) -> None:
    if not settings.auto_bootstrap_reference_data_on_startup:
        return

    data_root = Path(__file__).resolve().parents[1] / "data" / "Tiangong"
    unit_groups_path = data_root / "ILCD_Unit_Groups.xlsx"
    elementary_flows_path = data_root / "elementary_flows_sample.csv"
    intermediate_flows_path = data_root / "intermediate_flows_sample.csv"
    processes_path = data_root / "tiangong_processes.zip"

    if not unit_groups_path.exists():
        print(f"[startup-bootstrap] unit group source not found: {unit_groups_path}")
    else:
        result = import_unit_groups_from_excel(
            db,
            file_path=str(unit_groups_path),
            replace_existing=False,
        )
        print(
            "[startup-bootstrap] imported unit groups: "
            f"groups_inserted={result.get('groups_inserted', 0)} "
            f"units_inserted={result.get('units_inserted', 0)}"
        )

    if not elementary_flows_path.exists():
        print(f"[startup-bootstrap] elementary flow source not found: {elementary_flows_path}")
    else:
        result = import_flows_from_file(
            db,
            file_path=str(elementary_flows_path),
            sheet_name=None,
            mapping=None,
            replace_existing=False,
            default_flow_type="Elementary flow",
        ef31_flow_index_path=settings.nebula_lca_ef31_dir,
        )
        print(
            "[startup-bootstrap] imported elementary flows: "
            f"inserted={result.get('inserted', 0)} updated={result.get('updated', 0)}"
        )

    if not intermediate_flows_path.exists():
        print(f"[startup-bootstrap] intermediate flow source not found: {intermediate_flows_path}")
    else:
        result = import_flows_from_file(
            db,
            file_path=str(intermediate_flows_path),
            sheet_name=None,
            mapping=None,
            replace_existing=False,
            default_flow_type="Product flow",
            ef31_flow_index_path=None,
        )
        print(
            "[startup-bootstrap] imported intermediate flows: "
            f"inserted={result.get('inserted', 0)} updated={result.get('updated', 0)}"
        )

    if not processes_path.exists():
        print(f"[startup-bootstrap] process source not found: {processes_path}")
    else:
        result = import_processes_from_json(
            db,
            path_like=str(processes_path),
            replace_existing=False,
            strict_reference_flow=False,
        )
        print(
            "[startup-bootstrap] imported reference processes: "
            f"inserted={result.get('inserted', 0)} "
            f"updated={result.get('updated', 0)} "
            f"failed={result.get('failed', 0)}"
        )


def _ensure_pts_uuid_schema() -> dict:
    added_columns: list[str] = []
    created_indexes: list[str] = []

    with engine.begin() as conn:
        inspector = inspect(conn)

        if inspector.has_table("pts_definitions"):
            columns = {col["name"] for col in inspector.get_columns("pts_definitions")}
            if "pts_uuid" not in columns:
                conn.execute(text("ALTER TABLE pts_definitions ADD COLUMN pts_uuid VARCHAR(64)"))
                added_columns.append("pts_definitions.pts_uuid")
            if "pts_id" in columns:
                conn.execute(
                    text(
                        "UPDATE pts_definitions "
                        "SET pts_uuid = pts_id "
                        "WHERE (pts_uuid IS NULL OR TRIM(pts_uuid)='') AND pts_id IS NOT NULL"
                    )
                )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_definitions_pts_uuid ON pts_definitions (pts_uuid)"))
            created_indexes.append("ix_pts_definitions_pts_uuid")
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pts_definition_project_pts_uuid "
                    "ON pts_definitions (project_id, pts_uuid)"
                )
            )
            created_indexes.append("uq_pts_definition_project_pts_uuid")

        if inspector.has_table("pts_external_artifacts"):
            columns = {col["name"] for col in inspector.get_columns("pts_external_artifacts")}
            if "pts_uuid" not in columns:
                conn.execute(text("ALTER TABLE pts_external_artifacts ADD COLUMN pts_uuid VARCHAR(64)"))
                added_columns.append("pts_external_artifacts.pts_uuid")
            if "pts_id" in columns:
                conn.execute(
                    text(
                        "UPDATE pts_external_artifacts "
                        "SET pts_uuid = pts_id "
                        "WHERE (pts_uuid IS NULL OR TRIM(pts_uuid)='') AND pts_id IS NOT NULL"
                    )
                )
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_pts_external_artifacts_pts_uuid ON pts_external_artifacts (pts_uuid)")
            )
            created_indexes.append("ix_pts_external_artifacts_pts_uuid")
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pts_external_project_pts_uuid_hash "
                    "ON pts_external_artifacts (project_id, pts_uuid, graph_hash)"
                )
            )
            created_indexes.append("uq_pts_external_project_pts_uuid_hash")

    return {
        "tables": ["pts_definitions", "pts_external_artifacts"],
        "column_added": added_columns,
        "index_created": created_indexes,
        "status": "ok",
    }


def _ensure_pts_resources_schema() -> dict:
    created_indexes: list[str] = []
    added_columns: list[str] = []
    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table("pts_resources"):
            PtsResource.__table__.create(bind=conn)
        existing_columns = {col["name"] for col in inspector.get_columns("pts_resources")}
        pts_resource_columns = {
            "latest_compile_version": "ALTER TABLE pts_resources ADD COLUMN latest_compile_version INTEGER",
            "latest_published_version": "ALTER TABLE pts_resources ADD COLUMN latest_published_version INTEGER",
            "active_published_version": "ALTER TABLE pts_resources ADD COLUMN active_published_version INTEGER",
        }
        for column_name, ddl in pts_resource_columns.items():
            if column_name in existing_columns:
                continue
            conn.execute(text(ddl))
            added_columns.append(column_name)
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_resources_project_id ON pts_resources (project_id)"))
        created_indexes.append("ix_pts_resources_project_id")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_resources_pts_node_id ON pts_resources (pts_node_id)"))
        created_indexes.append("ix_pts_resources_pts_node_id")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_resources_latest_graph_hash ON pts_resources (latest_graph_hash)"))
        created_indexes.append("ix_pts_resources_latest_graph_hash")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_resources_compiled_graph_hash ON pts_resources (compiled_graph_hash)"))
        created_indexes.append("ix_pts_resources_compiled_graph_hash")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_resources_latest_compile_version ON pts_resources (latest_compile_version)"))
        created_indexes.append("ix_pts_resources_latest_compile_version")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_resources_latest_published_version ON pts_resources (latest_published_version)"))
        created_indexes.append("ix_pts_resources_latest_published_version")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_resources_active_published_version ON pts_resources (active_published_version)"))
        created_indexes.append("ix_pts_resources_active_published_version")
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_pts_resource_pts_uuid "
                "ON pts_resources (pts_uuid)"
            )
        )
        created_indexes.append("uq_pts_resource_pts_uuid")
        compile_columns = {col["name"] for col in inspector.get_columns("pts_compile_artifacts")}
        compile_column_ddls = {
            "compile_version": "ALTER TABLE pts_compile_artifacts ADD COLUMN compile_version INTEGER",
        }
        for column_name, ddl in compile_column_ddls.items():
            if column_name in compile_columns:
                continue
            conn.execute(text(ddl))
            added_columns.append(f"pts_compile_artifacts.{column_name}")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_compile_artifacts_compile_version ON pts_compile_artifacts (compile_version)"))
        created_indexes.append("ix_pts_compile_artifacts_compile_version")

        external_columns = {col["name"] for col in inspector.get_columns("pts_external_artifacts")}
        external_column_ddls = {
            "published_version": "ALTER TABLE pts_external_artifacts ADD COLUMN published_version INTEGER",
            "source_compile_id": "ALTER TABLE pts_external_artifacts ADD COLUMN source_compile_id VARCHAR(36)",
            "source_compile_version": "ALTER TABLE pts_external_artifacts ADD COLUMN source_compile_version INTEGER",
        }
        for column_name, ddl in external_column_ddls.items():
            if column_name in external_columns:
                continue
            conn.execute(text(ddl))
            added_columns.append(f"pts_external_artifacts.{column_name}")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_external_artifacts_published_version ON pts_external_artifacts (published_version)"))
        created_indexes.append("ix_pts_external_artifacts_published_version")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_external_artifacts_source_compile_id ON pts_external_artifacts (source_compile_id)"))
        created_indexes.append("ix_pts_external_artifacts_source_compile_id")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_pts_external_artifacts_source_compile_version ON pts_external_artifacts (source_compile_version)"))
        created_indexes.append("ix_pts_external_artifacts_source_compile_version")
    return {"table": "pts_resources", "column_added": added_columns, "index_created": created_indexes, "status": "ok"}


def _backfill_pts_artifact_versions(*, db: Session, dry_run: bool = False) -> dict:
    compile_updated = 0
    external_updated = 0
    resource_updated = 0

    compile_groups: dict[tuple[str, str], list[PtsCompileArtifact]] = defaultdict(list)
    for row in db.query(PtsCompileArtifact).order_by(PtsCompileArtifact.created_at.asc(), PtsCompileArtifact.updated_at.asc()).all():
        compile_groups[(str(row.project_id), str(row.pts_uuid))].append(row)
    for rows in compile_groups.values():
        for idx, row in enumerate(rows, start=1):
            if row.compile_version == idx:
                continue
            compile_updated += 1
            if not dry_run:
                row.compile_version = idx

    external_groups: dict[tuple[str, str], list[PtsExternalArtifact]] = defaultdict(list)
    for row in db.query(PtsExternalArtifact).order_by(PtsExternalArtifact.created_at.asc(), PtsExternalArtifact.updated_at.asc()).all():
        external_groups[(str(row.project_id), str(row.pts_uuid))].append(row)
    for rows in external_groups.values():
        for idx, row in enumerate(rows, start=1):
            if row.published_version != idx:
                external_updated += 1
                if not dry_run:
                    row.published_version = idx
            if row.source_compile_version is None and row.source_compile_id:
                source = db.query(PtsCompileArtifact).filter(PtsCompileArtifact.id == row.source_compile_id).first()
                if source is not None and source.compile_version is not None:
                    external_updated += 1
                    if not dry_run:
                        row.source_compile_version = source.compile_version

    resources = db.query(PtsResource).all()
    for resource in resources:
        latest_compile = (
            db.query(PtsCompileArtifact)
            .filter(PtsCompileArtifact.project_id == resource.project_id, PtsCompileArtifact.pts_uuid == resource.pts_uuid)
            .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
            .first()
        )
        latest_external = (
            db.query(PtsExternalArtifact)
            .filter(PtsExternalArtifact.project_id == resource.project_id, PtsExternalArtifact.pts_uuid == resource.pts_uuid)
            .order_by(PtsExternalArtifact.published_version.desc(), PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
            .first()
        )
        desired_latest_compile = int(latest_compile.compile_version) if latest_compile and latest_compile.compile_version is not None else None
        desired_latest_published = int(latest_external.published_version) if latest_external and latest_external.published_version is not None else None
        desired_active = resource.active_published_version
        if desired_active is None:
            desired_active = desired_latest_published
        desired_compiled_hash = str(latest_compile.graph_hash or "") if latest_compile is not None else resource.compiled_graph_hash
        desired_published_at = (latest_external.updated_at or latest_external.created_at) if latest_external is not None else resource.published_at
        if (
            resource.latest_compile_version != desired_latest_compile
            or resource.latest_published_version != desired_latest_published
            or resource.active_published_version != desired_active
            or resource.compiled_graph_hash != desired_compiled_hash
            or resource.published_at != desired_published_at
        ):
            resource_updated += 1
            if not dry_run:
                resource.latest_compile_version = desired_latest_compile
                resource.latest_published_version = desired_latest_published
                resource.active_published_version = desired_active
                resource.compiled_graph_hash = desired_compiled_hash
                resource.published_at = desired_published_at

    if not dry_run and (compile_updated or external_updated or resource_updated):
        db.commit()

    return {
        "compile_versions_updated": compile_updated,
        "published_versions_updated": external_updated,
        "resource_versions_updated": resource_updated,
        "dry_run": dry_run,
    }


def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _is_output_direction(value: object) -> bool:
    return str(value or "").strip().lower() == "output"


def _to_stripped(value: object) -> str:
    return str(value or "").strip()


def _normalize_import_mode_value(value: object) -> str | None:
    mode = _to_stripped(value)
    if mode in {"locked", "editable_clone"}:
        return mode
    return None


def _normalize_process_kind(value: object) -> str:
    raw = _to_stripped(value).lower()
    if raw in {"unit_process", "market_process", "lci_dataset", "pts_module"}:
        return raw
    if raw == "lci":
        return "lci_dataset"
    if raw == "pts":
        return "pts_module"
    return "unit_process"


def _validate_target_kind_or_400(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _normalize_process_kind(value)
    if normalized != value:
        allowed = ["unit_process", "market_process", "lci_dataset", "pts_module"]
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_TARGET_KIND",
                "message": f"target_kind must be one of: {', '.join(allowed)}",
            },
        )
    return normalized


def _flow_uuid_set(db: Session) -> set[str]:
    return {str(row.flow_uuid).strip() for row in db.query(FlowRecord.flow_uuid).all() if str(row.flow_uuid).strip()}


def _flow_uuid_set_cached(db: Session) -> set[str]:
    cache_key = f"flow_uuid_set:v1:rev={_cache_revision('flow_meta')}"
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOW_META_SECONDS)
    if isinstance(cached, set):
        return cached
    value = _flow_uuid_set(db)
    _cache_set(cache_key, value)
    return value


def _flow_meta_by_uuid_cached(db: Session) -> dict[str, tuple[str | None, str | None, str | None, str | None]]:
    cache_key = f"flow_meta_by_uuid:v1:rev={_cache_revision('flow_meta')}"
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOW_META_SECONDS)
    if isinstance(cached, dict):
        return cached
    value = {
        str(row.flow_uuid).strip(): (
            _safe_str(row.flow_name),
            _safe_str(row.default_unit),
            _safe_str(row.flow_type),
            _safe_str(row.unit_group),
        )
        for row in db.query(
            FlowRecord.flow_uuid,
            FlowRecord.flow_name,
            FlowRecord.default_unit,
            FlowRecord.flow_type,
            FlowRecord.unit_group,
        ).all()
    }
    _cache_set(cache_key, value)
    return value


def _flow_name_en_by_uuid_cached(db: Session) -> dict[str, str]:
    cache_key = f"flow_name_en_by_uuid:v1:rev={_cache_revision('flow_meta')}"
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOW_META_SECONDS)
    if isinstance(cached, dict):
        return cached
    value = {
        str(row.flow_uuid).strip(): _safe_str(row.flow_name_en)
        for row in db.query(FlowRecord.flow_uuid, FlowRecord.flow_name_en).all()
        if str(row.flow_uuid or "").strip() and _safe_str(row.flow_name_en)
    }
    _cache_set(cache_key, value)
    return value


def _solver_flow_type_by_uuid_cached(db: Session) -> dict[str, str]:
    return {
        flow_uuid: str(meta[2]).strip()
        for flow_uuid, meta in _flow_meta_by_uuid_cached(db).items()
        if flow_uuid and meta and str(meta[2] or "").strip()
    }


def _filter_exchanges_with_evidence(
    *,
    process_uuid: str,
    exchanges: list[dict],
    valid_flow_uuids: set[str],
) -> tuple[list[dict], list[FilteredExchangeEvidence]]:
    kept: list[dict] = []
    filtered: list[FilteredExchangeEvidence] = []
    for ex in exchanges:
        if not isinstance(ex, dict):
            filtered.append(
                FilteredExchangeEvidence(
                    process_uuid=process_uuid,
                    exchange_internal_id=None,
                    flow_uuid=None,
                    reason="invalid exchange object",
                )
            )
            continue
        flow_uuid = _to_stripped(ex.get("flow_uuid"))
        exchange_internal_id = _safe_str(ex.get("exchange_internal_id"))
        if not flow_uuid:
            filtered.append(
                FilteredExchangeEvidence(
                    process_uuid=process_uuid,
                    exchange_internal_id=exchange_internal_id,
                    flow_uuid=None,
                    reason="missing flow_uuid",
                )
            )
            continue
        if flow_uuid not in valid_flow_uuids:
            filtered.append(
                FilteredExchangeEvidence(
                    process_uuid=process_uuid,
                    exchange_internal_id=exchange_internal_id,
                    flow_uuid=flow_uuid,
                    reason="flow_uuid not found in flow catalog",
                )
            )
            continue
        kept.append(ex)
    return kept, filtered


def _mark_reference_product_exchange(
    *,
    process_uuid: str,
    process_json: dict,
    exchanges: list[dict],
) -> tuple[str | None, list[str]]:
    warnings: list[str] = []
    reference_flow_internal_id = _to_stripped(process_json.get("reference_flow_internal_id"))
    matched_output: dict | None = None
    allocation_outputs = [ex for ex in exchanges if _is_output_direction(ex.get("direction")) and bool(ex.get("is_allocated_product"))]

    if reference_flow_internal_id:
        for ex in exchanges:
            if _to_stripped(ex.get("exchange_internal_id")) != reference_flow_internal_id:
                continue
            if not _is_output_direction(ex.get("direction")):
                warnings.append("reference_flow_internal_id matched non-output exchange; product not set")
                return None, warnings
            matched_output = ex
            break
        if matched_output is None:
            warnings.append("reference output exchange not found after flow filtering; product requires manual completion")

    if matched_output is None and len(allocation_outputs) == 1:
        matched_output = allocation_outputs[0]
    elif matched_output is None and len(allocation_outputs) > 1:
        warnings.append("multiple output exchanges have allocatedFraction > 0; product requires manual completion")
        return None, warnings
    elif matched_output is None:
        warnings.append("reference_flow_internal_id missing and no allocated product output found; product cannot be auto-detected")
        return None, warnings

    for ex in exchanges:
        ex["is_reference_flow"] = ex is matched_output
        ex["isProduct"] = bool(ex is matched_output or ex.get("is_allocated_product"))

    flow_uuid = _to_stripped(matched_output.get("flow_uuid"))
    if not flow_uuid:
        warnings.append("reference output flow_uuid missing; product requires manual completion")
        return None, warnings
    return flow_uuid, warnings


def _derive_reference_flow_display(
    *,
    process_json: dict | None,
    reference_flow_uuid: str | None,
    reference_flow_name: str | None,
    reference_flow_internal_id: str | None,
) -> tuple[str | None, str | None]:
    ref_uuid = _safe_str(reference_flow_uuid)
    ref_name = _safe_str(reference_flow_name)
    if ref_uuid or ref_name:
        return ref_uuid or None, ref_name or None

    if isinstance(process_json, dict):
        source_uuid = _safe_str(process_json.get("reference_flow_source_uuid"))
        source_name = _safe_str(process_json.get("reference_flow_source_name"))
        if source_uuid or source_name:
            return source_uuid or None, source_name or None

        report_json = process_json.get("import_report_json")
        if isinstance(report_json, dict):
            warnings = report_json.get("warnings")
            if isinstance(warnings, list):
                for item in warnings:
                    if not isinstance(item, dict):
                        continue
                    reasons = item.get("reasons")
                    if not isinstance(reasons, list):
                        continue
                    if any("reference output exchange not found after flow filtering" in str(reason) for reason in reasons):
                        return None, "Reference Flow Missing"

    if _safe_str(reference_flow_internal_id):
        return None, "Reference Flow Missing"
    return None, None


def _build_imported_process_ports(
    *,
    exchanges: list[dict],
    flow_meta_by_uuid: dict[str, tuple[str | None, str | None, str | None, str | None]],
) -> tuple[list[ImportedProcessPortItem], list[ImportedProcessPortItem]]:
    inputs: list[ImportedProcessPortItem] = []
    outputs: list[ImportedProcessPortItem] = []

    for ex in exchanges:
        if not isinstance(ex, dict):
            continue
        flow_uuid = _safe_str(ex.get("flow_uuid"))
        flow_name = _safe_str(ex.get("flow_name"))
        unit = _safe_str(ex.get("unit"))
        unit_group = ""
        flow_type = _safe_str(ex.get("flow_type"))
        if flow_uuid and flow_uuid in flow_meta_by_uuid:
            db_flow_name, db_unit, db_flow_type, db_unit_group = flow_meta_by_uuid.get(flow_uuid) or (None, None, None, None)
            flow_name = flow_name or db_flow_name
            unit = unit or db_unit
            flow_type = flow_type or db_flow_type
            unit_group = _safe_str(db_unit_group)

        try:
            amount = float(ex.get("amount") or 0.0)
        except Exception:  # noqa: BLE001
            amount = 0.0

        direction = "output" if _is_output_direction(ex.get("direction")) else "input"
        item = ImportedProcessPortItem(
            flow_uuid=flow_uuid,
            flow_name=flow_name,
            unit=unit,
            unit_group=unit_group or None,
            type=flow_semantic_to_exchange_type(flow_type),
            amount=amount,
            direction=direction,
            is_product=bool(ex.get("isProduct") or ex.get("is_reference_flow")),
        )

        if direction == "input":
            inputs.append(item)
        else:
            outputs.append(item)

    return inputs, outputs


_TIDAS_IMPORT_DIAGNOSTIC_TYPE = "tidas.import.report.v1"


def _as_list(value: object) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _pick_localized_text(value: object, *, preferred_langs: tuple[str, ...] = ("zh", "en")) -> str | None:
    rows = _as_list(value)
    if not rows:
        return _safe_str(value)

    candidates: list[tuple[int, str]] = []
    for row in rows:
        if isinstance(row, str):
            text_value = _safe_str(row)
            if text_value:
                candidates.append((999, text_value))
            continue
        if not isinstance(row, dict):
            continue
        text_value = _safe_str(row.get("#text"))
        if not text_value:
            continue
        lang = _safe_str(row.get("@xml:lang")) or ""
        rank = preferred_langs.index(lang) if lang in preferred_langs else 998
        candidates.append((rank, text_value))

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _extract_ilcd_name(name_obj: object) -> tuple[str | None, str | None]:
    if not isinstance(name_obj, dict):
        return None, None
    zh = _pick_localized_text(name_obj.get("baseName"), preferred_langs=("zh", "en"))
    en = _pick_localized_text(name_obj.get("baseName"), preferred_langs=("en", "zh"))
    if not zh:
        zh = _pick_localized_text(name_obj.get("common:name"), preferred_langs=("zh", "en"))
    if not en:
        en = _pick_localized_text(name_obj.get("common:name"), preferred_langs=("en", "zh"))
    return zh, en


def _extract_ilcd_process_name(name_obj: object) -> tuple[str | None, str | None]:
    if not isinstance(name_obj, dict):
        return None, None

    def _compose(preferred_langs: tuple[str, ...]) -> str | None:
        parts: list[str] = []
        for key in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes"):
            text_value = _pick_localized_text(name_obj.get(key), preferred_langs=preferred_langs)
            text_value = str(text_value or "").strip()
            if text_value:
                parts.append(text_value)
        if parts:
            return "; ".join(parts)
        return _pick_localized_text(name_obj.get("common:name"), preferred_langs=preferred_langs)

    return _compose(("zh", "en")), _compose(("en", "zh"))


def _extract_ilcd_flow_compartment(classification_obj: object) -> str | None:
    if not isinstance(classification_obj, dict):
        return None

    elementary = (
        classification_obj.get("common:elementaryFlowCategorization")
        if isinstance(classification_obj.get("common:elementaryFlowCategorization"), dict)
        else None
    )
    if elementary is not None:
        categories = _as_list(elementary.get("common:category"))
        parts: list[str] = []
        for row in categories:
            text_value = _safe_str(row.get("#text")) if isinstance(row, dict) else _safe_str(row)
            if text_value:
                parts.append(text_value)
        if parts:
            return ";".join(parts)

    legacy = classification_obj.get("common:classification") if isinstance(classification_obj, dict) else {}
    classes = _as_list(legacy.get("common:class")) if isinstance(legacy, dict) else []
    parts: list[str] = []
    for row in classes:
        text_value = _safe_str(row.get("#text")) if isinstance(row, dict) else _safe_str(row)
        if text_value:
            parts.append(text_value)
    if parts:
        return ";".join(parts)
    return None


def _infer_unit_defaults_from_flow_dataset(flow_dataset: dict) -> tuple[str, str]:
    flow_props = flow_dataset.get("flowProperties") if isinstance(flow_dataset.get("flowProperties"), dict) else {}
    flow_prop = flow_props.get("flowProperty") if isinstance(flow_props, dict) else {}
    rows = _as_list(flow_prop)
    hint_texts: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ref = row.get("referenceToFlowPropertyDataSet") if isinstance(row.get("referenceToFlowPropertyDataSet"), dict) else {}
        short_desc = ref.get("common:shortDescription") if isinstance(ref, dict) else None
        text_value = _pick_localized_text(short_desc, preferred_langs=("en", "zh")) or ""
        if text_value:
            hint_texts.append(text_value.lower())
    hint_blob = " ".join(hint_texts)
    if "energy" in hint_blob or "能量" in hint_blob:
        return "MJ", "Units of energy"
    if "volume" in hint_blob or "体积" in hint_blob:
        return "m3", "Units of volume"
    if "item" in hint_blob or "count" in hint_blob or "数量" in hint_blob:
        return "item", "dimensionless"
    return "kg", "Units of mass"


def _parse_tidas_json_documents(file_path: Path) -> tuple[list[dict], list[str]]:
    return _parse_tidas_json_payload(source_name=file_path.name, raw_text=file_path.read_text(encoding="utf-8"))


def _parse_tidas_json_payload(*, source_name: str, raw_text: str) -> tuple[list[dict], list[str]]:
    errors: list[str] = []
    try:
        payload = json.loads(raw_text)
    except Exception as exc:  # noqa: BLE001
        return [], [f"{source_name}: invalid JSON ({exc})"]
    if isinstance(payload, list):
        rows = [item for item in payload if isinstance(item, dict)]
        if not rows:
            errors.append(f"{source_name}: root list contains no object records")
        return rows, errors
    if isinstance(payload, dict):
        return [payload], errors
    return [], [f"{source_name}: unsupported root type {type(payload).__name__}"]


async def _parse_tidas_uploaded_json(file: UploadFile) -> tuple[str, list[dict], list[str]]:
    source_name = str(file.filename or "upload.json")
    try:
        raw = await file.read()
    finally:
        await file.close()
    try:
        raw_text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        return source_name, [], [f"{source_name}: invalid UTF-8 ({exc})"]
    rows, errors = _parse_tidas_json_payload(source_name=source_name, raw_text=raw_text)
    return source_name, rows, errors


async def _read_uploaded_bytes(file: UploadFile) -> tuple[str, bytes]:
    source_name = str(file.filename or "upload.bin")
    try:
        raw = await file.read()
    finally:
        await file.close()
    return source_name, raw


def _parse_tidas_bundle_zip(
    *,
    source_name: str,
    raw_bytes: bytes,
    require_model_file: bool = True,
) -> tuple[dict, list[tuple[str, list[dict], list[str]]], list[tuple[str, list[dict], list[str]]], list[tuple[str, list[dict], list[str]]], list[str]]:
    errors: list[str] = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw_bytes))
    except Exception as exc:  # noqa: BLE001
        return {}, [], [], [], [f"{source_name}: invalid zip ({exc})"]

    with zf:
        names = [name for name in zf.namelist() if not name.endswith("/")]
        manifest_name = next((name for name in names if name.lower() == "manifest.json"), None)
        if manifest_name is None:
            manifest_name = next((name for name in names if name.lower().endswith("/manifest.json")), None)
        if not manifest_name:
            return {}, [], [], [], [f"{source_name}: missing manifest.json"]
        try:
            manifest = json.loads(zf.read(manifest_name).decode("utf-8-sig"))
        except Exception as exc:  # noqa: BLE001
            return {}, [], [], [], [f"{source_name}: invalid manifest.json ({exc})"]

        bundle_root = manifest_name[: -len("manifest.json")].rstrip("/\\")

        bundle_version = str(manifest.get("bundle_schema_version") or "").strip()
        manifest_format = str(manifest.get("format") or "").strip()
        manifest_version = str(manifest.get("version") or "").strip()
        is_legacy_bundle = bundle_version == "tidas-lca-bundle-v1"
        is_package_v2 = manifest_format == "tiangong-tidas-package" and manifest_version == "2"

        if not is_legacy_bundle and not is_package_v2:
            errors.append(
                f"{source_name}: unsupported bundle manifest "
                f"(bundle_schema_version={bundle_version or '<empty>'}, format={manifest_format or '<empty>'}, version={manifest_version or '<empty>'})"
            )

        model_file = str(manifest.get("model_file") or "").strip()
        model_files: list[str] = [model_file] if model_file else []
        process_dir = str(manifest.get("process_dir") or "process").strip().strip("/\\")
        flow_dir = str(manifest.get("flow_dir") or "flow").strip().strip("/\\")
        if is_package_v2:
            process_dir = "processes"
            flow_dir = "flows"
            if require_model_file:
                entries = _as_list(manifest.get("entries"))
                entry_model_files: list[str] = []
                for row in entries:
                    if not isinstance(row, dict):
                        continue
                    table_name = _safe_str(row.get("table")).lower()
                    file_path = _safe_str(row.get("file_path"))
                    if table_name == "lifecyclemodels" and file_path:
                        entry_model_files.append(file_path)
                if entry_model_files:
                    model_files = entry_model_files
                    model_file = entry_model_files[0]
        if require_model_file and not model_files:
            errors.append(f"{source_name}: manifest missing model_file")

        def _resolve_bundle_path(path_value: str) -> str:
            norm = str(path_value or "").strip().strip("/\\")
            if not norm:
                return ""
            if bundle_root and not norm.lower().startswith((bundle_root + "/").lower()):
                return f"{bundle_root}/{norm}"
            return norm

        def _collect_json_entries(prefix: str) -> list[str]:
            norm_prefix = _resolve_bundle_path(prefix)
            if not norm_prefix:
                return []
            match_prefix = norm_prefix + "/"
            return sorted(
                [
                    name
                    for name in names
                    if name.lower().startswith(match_prefix.lower()) and name.lower().endswith(".json")
                ]
            )

        def _read_rows(entry_name: str) -> tuple[str, list[dict], list[str]]:
            try:
                raw_text = zf.read(entry_name).decode("utf-8-sig")
            except Exception as exc:  # noqa: BLE001
                return entry_name, [], [f"{entry_name}: unable to read zip entry ({exc})"]
            rows, parse_errors = _parse_tidas_json_payload(source_name=entry_name, raw_text=raw_text)
            return entry_name, rows, parse_errors

        model_items: list[tuple[str, list[dict], list[str]]] = []
        for raw_model_file in model_files:
            resolved_model_file = _resolve_bundle_path(raw_model_file)
            if resolved_model_file not in names:
                if require_model_file:
                    errors.append(f"{source_name}: manifest model_file not found in zip: {raw_model_file}")
            else:
                model_items.append(_read_rows(resolved_model_file))

        process_items = [_read_rows(name) for name in _collect_json_entries(process_dir)]
        flow_items = [_read_rows(name) for name in _collect_json_entries(flow_dir)]

        if not process_items:
            errors.append(f"{source_name}: no process json found under {process_dir}/")
        if not flow_items:
            errors.append(f"{source_name}: no flow json found under {flow_dir}/")
        if require_model_file and not model_items:
            errors.append(f"{source_name}: no model json found")

        return manifest, flow_items, process_items, model_items, errors


def _coerce_form_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _build_tidas_base_report(*, import_type: str, payload: object) -> dict:
    upsert_mode_value = _safe_str(getattr(payload, "upsert_mode", None)) or "update"
    return {
        "job_id": str(uuid.uuid4()),
        "import_type": import_type,
        "source_path": "",
        "dry_run": bool(getattr(payload, "dry_run", False)),
        "upsert_mode": upsert_mode_value,
        "strict_mode": bool(getattr(payload, "strict_mode", False)),
        "total_files": 0,
        "total_records": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "warnings": [],
        "errors": [],
        "imported_process_count": 0,
        "imported_exchange_count": 0,
        "filtered_exchange_count": 0,
        "filtered_exchanges": [],
        "top_missing_flow_uuids": [],
        "imported_count": 0,
        "filtered_count": 0,
        "warning_count": 0,
        "failed_count": 0,
        "unresolved_count": 0,
        "summary": {},
        "unresolved": [],
        "unresolved_items": [],
        "model_topology_empty_count": 0,
        "created_projects": [],
        "created_at": datetime.utcnow(),
    }


def _finalize_tidas_report(report: dict) -> dict:
    imported_count = int(report.get("imported_process_count") or 0)
    if imported_count <= 0:
        imported_count = int(report.get("inserted") or 0) + int(report.get("updated") or 0)
    filtered_count = int(report.get("filtered_exchange_count") or 0)
    warning_count = len(list(report.get("warnings") or []))
    failed_count = int(report.get("failed") or 0)
    unresolved_items = list(report.get("unresolved_items") or [])
    unresolved_count = int(report.get("unresolved_count") or len(unresolved_items))
    report["imported_count"] = imported_count
    report["filtered_count"] = filtered_count
    report["warning_count"] = warning_count
    report["failed_count"] = failed_count
    report["unresolved_count"] = unresolved_count
    report["unresolved"] = unresolved_items
    report["summary"] = {
        "job_id": report.get("job_id"),
        "import_type": report.get("import_type"),
        "source_path": report.get("source_path"),
        "dry_run": bool(report.get("dry_run")),
        "upsert_mode": report.get("upsert_mode"),
        "strict_mode": bool(report.get("strict_mode")),
        "total_files": int(report.get("total_files") or 0),
        "total_records": int(report.get("total_records") or 0),
        "imported_count": imported_count,
        "filtered_count": filtered_count,
        "warning_count": warning_count,
        "failed_count": failed_count,
        "unresolved_count": unresolved_count,
        "created_projects": list(report.get("created_projects") or []),
    }
    return report


def _persist_tidas_import_report(db: Session, report_payload: dict) -> TidasImportReportResponse:
    report_payload = _finalize_tidas_report(report_payload)
    report_model = TidasImportReportResponse.model_validate(report_payload)
    report_json = report_model.model_dump(mode="json")
    row = db.get(DebugDiagnostic, report_model.job_id)
    if row is None:
        row = DebugDiagnostic(
            id=report_model.job_id,
            diagnostic_type=_TIDAS_IMPORT_DIAGNOSTIC_TYPE,
            payload_json={
                "import_type": report_model.import_type,
                "source_path": report_model.source_path,
                "dry_run": report_model.dry_run,
                "upsert_mode": report_model.upsert_mode,
                "strict_mode": report_model.strict_mode,
            },
            result_json=report_json,
            created_at=datetime.utcnow(),
        )
        db.add(row)
    else:
        row.diagnostic_type = _TIDAS_IMPORT_DIAGNOSTIC_TYPE
        row.result_json = report_json
    db.commit()
    return report_model


def _extract_tidas_process_record(raw: dict) -> tuple[dict | None, str | None]:
    process_dataset = raw.get("processDataSet") if isinstance(raw.get("processDataSet"), dict) else None
    if process_dataset is None:
        return None, "missing processDataSet"
    process_info = process_dataset.get("processInformation") if isinstance(process_dataset.get("processInformation"), dict) else {}
    dsi = process_info.get("dataSetInformation") if isinstance(process_info.get("dataSetInformation"), dict) else {}
    process_uuid = _safe_str(dsi.get("common:UUID"))
    if not process_uuid:
        return None, "missing process UUID (processInformation.dataSetInformation.common:UUID)"

    name_zh, name_en = _extract_ilcd_process_name(dsi.get("name"))
    process_name = name_zh or name_en or process_uuid
    qref = process_info.get("quantitativeReference") if isinstance(process_info.get("quantitativeReference"), dict) else {}
    reference_flow_internal_id = _safe_str(qref.get("referenceToReferenceFlow"))
    reference_flow_source_uuid: str | None = None
    reference_flow_source_name: str | None = None
    geography = process_info.get("geography") if isinstance(process_info.get("geography"), dict) else {}
    location_obj = (
        geography.get("locationOfOperationSupplyOrProduction")
        if isinstance(geography.get("locationOfOperationSupplyOrProduction"), dict)
        else {}
    )
    location = _safe_str(location_obj.get("@location")) or "GLO"

    exchanges_obj = process_dataset.get("exchanges") if isinstance(process_dataset.get("exchanges"), dict) else {}
    exchanges_raw = _as_list(exchanges_obj.get("exchange")) if isinstance(exchanges_obj, dict) else []
    exchanges: list[dict] = []

    def _has_positive_allocated_fraction(value: object) -> bool:
        for row in _as_list(value):
            if not isinstance(row, dict):
                continue
            try:
                fraction = float(row.get("@allocatedFraction") or 0.0)
            except Exception:  # noqa: BLE001
                fraction = 0.0
            if fraction > 0:
                return True
        return False
    for ex in exchanges_raw:
        if not isinstance(ex, dict):
            continue
        flow_ref = ex.get("referenceToFlowDataSet") if isinstance(ex.get("referenceToFlowDataSet"), dict) else {}
        flow_uuid = _safe_str(flow_ref.get("@refObjectId"))
        flow_name = _pick_localized_text(flow_ref.get("common:shortDescription"), preferred_langs=("zh", "en"))
        amount_raw = ex.get("meanAmount")
        if amount_raw is None:
            amount_raw = ex.get("resultingAmount")
        try:
            amount = float(amount_raw or 0.0)
        except Exception:  # noqa: BLE001
            amount = 0.0
        allocations_obj = ex.get("allocations") if isinstance(ex.get("allocations"), dict) else {}
        exchanges.append(
            {
                "exchange_internal_id": _safe_str(ex.get("@dataSetInternalID")),
                "flow_uuid": flow_uuid,
                "flow_name": flow_name,
                "direction": ("output" if str(ex.get("exchangeDirection") or "").strip().lower() == "output" else "input"),
                "amount": amount,
                "unit": None,
                "is_allocated_product": _has_positive_allocated_fraction(
                    allocations_obj.get("allocation") if isinstance(allocations_obj, dict) else None
                ),
            }
        )
        if (
            reference_flow_internal_id
            and _safe_str(ex.get("@dataSetInternalID")) == reference_flow_internal_id
        ):
            reference_flow_source_uuid = flow_uuid or None
            reference_flow_source_name = flow_name or None

    return {
        "process_uuid": process_uuid,
        "process_name": process_name,
        "process_name_zh": name_zh,
        "process_name_en": name_en,
        "location": location,
        "reference_flow_internal_id": reference_flow_internal_id,
        "reference_flow_source_uuid": reference_flow_source_uuid,
        "reference_flow_source_name": reference_flow_source_name,
        "exchanges": exchanges,
        "process_type": "unit_process",
    }, None


def _extract_tidas_flow_record(raw: dict) -> tuple[dict | None, str | None]:
    flow_dataset = raw.get("flowDataSet") if isinstance(raw.get("flowDataSet"), dict) else None
    if flow_dataset is None:
        return None, "missing flowDataSet"
    flow_info = flow_dataset.get("flowInformation") if isinstance(flow_dataset.get("flowInformation"), dict) else {}
    dsi = flow_info.get("dataSetInformation") if isinstance(flow_info.get("dataSetInformation"), dict) else {}
    flow_uuid = _safe_str(dsi.get("common:UUID"))
    if not flow_uuid:
        return None, "missing flow UUID (flowInformation.dataSetInformation.common:UUID)"
    name_zh, name_en = _extract_ilcd_name(dsi.get("name"))
    flow_name = name_zh or name_en or flow_uuid
    flow_type = _safe_str(
        (((flow_dataset.get("modellingAndValidation") or {}).get("LCIMethod") or {}).get("typeOfDataSet"))
    ) or "Product flow"
    default_unit, unit_group = _infer_unit_defaults_from_flow_dataset(flow_dataset)
    classification = dsi.get("classificationInformation") if isinstance(dsi.get("classificationInformation"), dict) else {}
    compartment = _extract_ilcd_flow_compartment(classification)
    return {
        "flow_uuid": flow_uuid,
        "flow_name": flow_name,
        "flow_name_en": name_en,
        "flow_type": flow_type,
        "default_unit": default_unit,
        "unit_group": unit_group,
        "compartment": compartment,
        "source_updated_at": datetime.utcnow().isoformat(),
    }, None


def _extract_tidas_model_record(raw: dict) -> tuple[dict | None, str | None]:
    model_dataset = raw.get("lifeCycleModelDataSet") if isinstance(raw.get("lifeCycleModelDataSet"), dict) else None
    if model_dataset is None:
        return None, "missing lifeCycleModelDataSet"
    model_info = (
        model_dataset.get("lifeCycleModelInformation")
        if isinstance(model_dataset.get("lifeCycleModelInformation"), dict)
        else {}
    )
    dsi = model_info.get("dataSetInformation") if isinstance(model_info.get("dataSetInformation"), dict) else {}
    model_uuid = _safe_str(dsi.get("common:UUID"))
    if not model_uuid:
        return None, "missing model UUID (lifeCycleModelInformation.dataSetInformation.common:UUID)"
    name_zh, name_en = _extract_ilcd_name(dsi.get("name"))
    model_name = name_zh or name_en or model_uuid
    tech = model_info.get("technology") if isinstance(model_info.get("technology"), dict) else {}
    processes_obj = tech.get("processes") if isinstance(tech.get("processes"), dict) else {}
    instances = _as_list(processes_obj.get("processInstance")) if isinstance(processes_obj, dict) else []
    process_refs: list[str] = []
    process_instances: list[dict] = []
    derived_connections: list[dict] = []
    for inst in instances:
        if not isinstance(inst, dict):
            continue
        internal_id = _safe_str(inst.get("@dataSetInternalID"))
        ref = inst.get("referenceToProcess") if isinstance(inst.get("referenceToProcess"), dict) else {}
        process_uuid = _safe_str(ref.get("@refObjectId"))
        if process_uuid:
            process_refs.append(process_uuid)
        process_instances.append(
            {
                "internal_id": internal_id,
                "process_uuid": process_uuid,
                "multiplication_factor": _safe_str(inst.get("@multiplicationFactor")),
            }
        )
        connections_obj = inst.get("connections") if isinstance(inst.get("connections"), dict) else {}
        output_exchanges = _as_list(connections_obj.get("outputExchange")) if isinstance(connections_obj, dict) else []
        for output_exchange in output_exchanges:
            if not isinstance(output_exchange, dict):
                continue
            flow_uuid = _safe_str(output_exchange.get("@flowUUID"))
            downstream_rows = _as_list(output_exchange.get("downstreamProcess"))
            for downstream in downstream_rows:
                if not isinstance(downstream, dict):
                    continue
                derived_connections.append(
                    {
                        "source_internal_id": internal_id,
                        "target_internal_id": _safe_str(downstream.get("@id")),
                        "flow_uuid": _safe_str(downstream.get("@flowUUID")) or flow_uuid,
                    }
                )

    json_tg = raw.get("json_tg") if isinstance(raw.get("json_tg"), dict) else {}
    xflow = json_tg.get("xflow") if isinstance(json_tg.get("xflow"), dict) else {}
    xflow_nodes = _as_list(xflow.get("nodes")) if isinstance(xflow, dict) else []
    xflow_edges = _as_list(xflow.get("edges")) if isinstance(xflow, dict) else []
    submodels = _as_list(json_tg.get("submodels")) if isinstance(json_tg, dict) else []

    connections_obj = tech.get("connections") if isinstance(tech.get("connections"), dict) else {}
    connections = _as_list(connections_obj.get("connection")) if isinstance(connections_obj, dict) else []
    topology_process_count = len(xflow_nodes) if xflow_nodes else len(instances)
    topology_connection_count = len(xflow_edges) if xflow_edges else (len(derived_connections) if derived_connections else len(connections))
    topology_empty = topology_process_count == 0 or topology_connection_count == 0
    return {
        "model_uuid": model_uuid,
        "model_name": model_name,
        "model_name_zh": name_zh,
        "model_name_en": name_en,
        "process_refs": process_refs,
        "process_instances": process_instances,
        "derived_connections": derived_connections,
        "xflow_nodes": xflow_nodes,
        "xflow_edges": xflow_edges,
        "submodels": submodels,
        "has_xflow": bool(xflow_nodes),
        "topology_empty": topology_empty,
        "topology_process_count": topology_process_count,
        "topology_connection_count": topology_connection_count,
    }, None


def _top_missing_flow_uuids(filtered: list[FilteredExchangeEvidence], top_n: int = 10) -> list[str]:
    counter: Counter[str] = Counter()
    for row in filtered:
        if row.reason != "flow_uuid not found in flow catalog":
            continue
        flow_uuid = _safe_str(row.flow_uuid)
        if flow_uuid:
            counter[flow_uuid] += 1
    return [flow_uuid for flow_uuid, _ in counter.most_common(top_n)]


def _build_tidas_graph_port_lists(
    *,
    exchanges: list[dict],
    flow_meta_by_uuid: dict[str, tuple[str | None, str | None, str | None]],
    unresolved: list[dict] | None = None,
    model_uuid: str | None = None,
    process_uuid: str | None = None,
    node_id: str | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    inputs: list[dict] = []
    outputs: list[dict] = []
    emissions: list[dict] = []
    counters = {"input": 0, "output": 0}

    for ex in exchanges:
        if not isinstance(ex, dict):
            continue
        flow_uuid = _safe_str(ex.get("flow_uuid"))
        if not flow_uuid:
            continue
        flow_name = _safe_str(ex.get("flow_name"))
        unit = _safe_str(ex.get("unit")) or "kg"
        flow_type = _safe_str(ex.get("flow_type"))
        db_meta = flow_meta_by_uuid.get(flow_uuid)
        if db_meta is None:
            if unresolved is not None:
                unresolved.append(
                    {
                        "model_uuid": _safe_str(model_uuid),
                        "type": "missing_flow_reference",
                        "process_uuid": _safe_str(process_uuid),
                        "node_id": _safe_str(node_id),
                        "flow_uuid": flow_uuid,
                        "exchange_internal_id": _safe_str(ex.get("exchange_internal_id")),
                        "direction": "output" if _is_output_direction(ex.get("direction")) else "input",
                        "action": "filtered",
                        "reason": "flow_uuid not found in flow catalog; import graph port skipped",
                    }
                )
            continue
        if db_meta:
            flow_name = flow_name or _safe_str(db_meta[0])
            unit = unit or _safe_str(db_meta[1]) or "kg"
            flow_type = flow_type or _safe_str(db_meta[2])
        unit_group = _safe_str(db_meta[3]) if db_meta and len(db_meta) > 3 else ""

        amount = float(ex.get("amount") or 0.0)
        direction = "output" if _is_output_direction(ex.get("direction")) else "input"
        is_elementary = is_elementary_flow_semantic(flow_type)
        bucket = direction
        counters[bucket] += 1
        port_id = _safe_str(ex.get("exchange_internal_id")) or f"{bucket}_{counters[bucket]}"
        port = {
            "id": port_id,
            "flowUuid": flow_uuid,
            "name": flow_name or flow_uuid,
            "unit": unit or "kg",
            "unitGroup": unit_group or None,
            "amount": amount,
            "externalSaleAmount": float(ex.get("externalSaleAmount") or 0.0),
            "type": flow_semantic_to_exchange_type(flow_type),
            "direction": direction,
            "showOnNode": not is_elementary,
            "internalExposed": not is_elementary,
            "isProduct": bool(ex.get("isProduct") or ex.get("is_reference_flow")),
        }
        if bucket == "input":
            inputs.append(port)
        else:
            outputs.append(port)

    return inputs, outputs, emissions


def _make_unique_graph_node_name(base_name: str, seen_names: dict[str, int]) -> str:
    normalized = str(base_name or "").strip() or "Process"
    count = int(seen_names.get(normalized, 0))
    if count <= 0:
        seen_names[normalized] = 1
        return normalized
    seen_names[normalized] = count + 1
    return f"{normalized}({count + 1})"


def _pick_process_display_name_for_import(source_json: dict | None, process_uuid: str | None, *, display_lang: str) -> str:
    data = source_json if isinstance(source_json, dict) else {}
    process_name_zh = _safe_str(data.get("process_name_zh"))
    process_name_en = _safe_str(data.get("process_name_en"))
    process_name = _safe_str(data.get("process_name"))
    lang = (_safe_str(display_lang) or "").lower()
    if lang == "en":
        return process_name_en or process_name_zh or process_name or _safe_str(process_uuid) or "Process"
    return process_name_zh or process_name_en or process_name or _safe_str(process_uuid) or "Process"


def _limit_graph_port_visibility_to_connected_edges(*, nodes: list[dict], edges: list[dict]) -> None:
    connected_pairs: set[tuple[str, str]] = set()
    connected_flow_pairs: set[tuple[str, str]] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source_node = _safe_str(edge.get("fromNode"))
        target_node = _safe_str(edge.get("toNode"))
        source_port_id = _safe_str(edge.get("sourcePortId"))
        target_port_id = _safe_str(edge.get("targetPortId"))
        flow_uuid = _safe_str(edge.get("flowUuid"))
        if source_node and source_port_id:
            connected_pairs.add((source_node, source_port_id))
        if target_node and target_port_id:
            connected_pairs.add((target_node, target_port_id))
        if flow_uuid:
            if source_node:
                connected_flow_pairs.add((source_node, flow_uuid))
            if target_node:
                connected_flow_pairs.add((target_node, flow_uuid))

    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = _safe_str(node.get("id"))
        if not node_id:
            continue
        for bucket in ("inputs", "outputs"):
            ports = node.get(bucket)
            if not isinstance(ports, list):
                continue
            for port in ports:
                if not isinstance(port, dict):
                    continue
                port_id = _safe_str(port.get("id"))
                flow_uuid = _safe_str(port.get("flowUuid"))
                is_connected = (
                    (node_id, port_id) in connected_pairs
                    or (flow_uuid and (node_id, flow_uuid) in connected_flow_pairs)
                )
                port["showOnNode"] = bool(is_connected)
                port["internalExposed"] = bool(is_connected)


def _prune_import_graph_invalid_product_role_edges(*, graph_json: dict, unresolved: list[dict], model_uuid: str) -> dict:
    if not isinstance(graph_json, dict):
        return graph_json

    nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
    edges = graph_json.get("exchanges") if isinstance(graph_json.get("exchanges"), list) else []
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return graph_json

    source_port_index: dict[tuple[str, str], dict] = {}
    target_port_index: dict[tuple[str, str], dict] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = _safe_str(node.get("id"))
        for port in node.get("outputs") if isinstance(node.get("outputs"), list) else []:
            if isinstance(port, dict):
                port_id = _safe_str(port.get("id"))
                if node_id and port_id:
                    source_port_index[(node_id, port_id)] = port
        for port in node.get("inputs") if isinstance(node.get("inputs"), list) else []:
            if isinstance(port, dict):
                port_id = _safe_str(port.get("id"))
                if node_id and port_id:
                    target_port_index[(node_id, port_id)] = port

    kept_edges: list[dict] = []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        from_node = _safe_str(edge.get("fromNode"))
        to_node = _safe_str(edge.get("toNode"))
        source_port_id = _safe_str(edge.get("sourcePortId")) or _safe_str(edge.get("source_port_id")) or _port_id_from_handle(_safe_str(edge.get("sourceHandle")), "out")
        target_port_id = _safe_str(edge.get("targetPortId")) or _safe_str(edge.get("target_port_id")) or _port_id_from_handle(_safe_str(edge.get("targetHandle")), "in")
        source_port = source_port_index.get((from_node, source_port_id))
        target_port = target_port_index.get((to_node, target_port_id))
        if source_port is None or target_port is None:
            kept_edges.append(edge)
            continue
        if _safe_str(source_port.get("type")) == "biosphere" or _safe_str(target_port.get("type")) == "biosphere":
            kept_edges.append(edge)
            continue

        source_is_product = bool(source_port.get("isProduct"))
        target_is_product = bool(target_port.get("isProduct"))
        if source_is_product != target_is_product:
            kept_edges.append(edge)
            continue

        unresolved.append(
            {
                "model_uuid": model_uuid,
                "type": "invalid_edge_product_role_alignment",
                "edge_id": _safe_str(edge.get("id")),
                "flow_uuid": _safe_str(edge.get("flowUuid")),
                "from_node_id": from_node,
                "to_node_id": to_node,
                "source_port_id": source_port_id,
                "target_port_id": target_port_id,
                "source_is_product": source_is_product,
                "target_is_product": target_is_product,
                "action": "disconnected",
                "reason": "import auto-disconnected product/product or non-product/non-product edge",
            }
        )

    updated = dict(graph_json)
    updated["exchanges"] = kept_edges
    return updated


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return default


def _xflow_port_text(port_item: dict) -> str:
    data = port_item.get("data") if isinstance(port_item.get("data"), dict) else {}
    text_lang = data.get("textLang")
    text_value = _pick_localized_text(text_lang, preferred_langs=("zh", "en"))
    if text_value:
        return text_value
    attrs = port_item.get("attrs") if isinstance(port_item.get("attrs"), dict) else {}
    attrs_text = attrs.get("text") if isinstance(attrs.get("text"), dict) else {}
    return _safe_str(attrs_text.get("title")) or _safe_str(attrs_text.get("text"))


def _apply_xflow_ports_to_node(
    *,
    inputs: list[dict],
    outputs: list[dict],
    xflow_node: dict,
    flow_meta_by_uuid: dict[str, tuple[str | None, str | None, str | None]],
    unresolved: list[dict] | None = None,
    model_uuid: str | None = None,
    process_uuid: str | None = None,
    node_id: str | None = None,
) -> tuple[list[dict], list[dict]]:
    port_items = (
        xflow_node.get("ports", {}).get("items")
        if isinstance(xflow_node.get("ports"), dict)
        else []
    )
    rows = [item for item in port_items if isinstance(item, dict)]
    if not rows:
        return inputs, outputs

    def _match_and_update(existing_ports: list[dict], port_item: dict, direction: str) -> None:
        data = port_item.get("data") if isinstance(port_item.get("data"), dict) else {}
        flow_uuid = _safe_str(data.get("flowId"))
        if not flow_uuid:
            return
        db_meta = flow_meta_by_uuid.get(flow_uuid)
        if db_meta is None:
            if unresolved is not None:
                unresolved.append(
                    {
                        "model_uuid": _safe_str(model_uuid),
                        "type": "missing_flow_reference",
                        "process_uuid": _safe_str(process_uuid),
                        "node_id": _safe_str(node_id),
                        "flow_uuid": flow_uuid,
                        "port_id": _safe_str(port_item.get("id")),
                        "direction": direction,
                        "action": "filtered",
                        "reason": "xflow port flow_uuid not found in flow catalog; import graph port skipped",
                    }
                )
            return
        quantitative_reference = bool(data.get("quantitativeReference"))
        matched = next(
            (
                port
                for port in existing_ports
                if _safe_str(port.get("flowUuid")) == flow_uuid and not bool(port.get("_xflow_bound"))
            ),
            None,
        )
        if matched is None:
            matched = {
                "id": _safe_str(port_item.get("id")) or f"{direction.upper()}:{flow_uuid}",
                "flowUuid": flow_uuid,
                "name": _xflow_port_text(port_item) or _safe_str(db_meta[0]) or flow_uuid,
                "unit": _safe_str(db_meta[1]) or "kg",
                "unitGroup": (_safe_str(db_meta[3]) if len(db_meta) > 3 else "") or None,
                "amount": 0.0,
                "externalSaleAmount": 0.0,
                "type": flow_semantic_to_exchange_type(_safe_str(db_meta[2])),
                "direction": direction,
                "showOnNode": True,
                "internalExposed": True,
                "isProduct": quantitative_reference,
            }
            existing_ports.append(matched)
        matched["id"] = _safe_str(port_item.get("id")) or _safe_str(matched.get("id"))
        matched["name"] = _xflow_port_text(port_item) or _safe_str(matched.get("name")) or flow_uuid
        matched["showOnNode"] = True
        matched["internalExposed"] = True
        if quantitative_reference:
            matched["isProduct"] = True
        matched["_xflow_bound"] = True

    for port_item in rows:
        group = _safe_str(port_item.get("group")).lower()
        if "input" in group:
            _match_and_update(inputs, port_item, "input")
        elif "output" in group:
            _match_and_update(outputs, port_item, "output")

    for port in inputs:
        port.pop("_xflow_bound", None)
    for port in outputs:
        port.pop("_xflow_bound", None)
    return inputs, outputs


def _build_tidas_graph_from_model_record(
    *,
    db: Session,
    model_record: dict,
    process_json_by_uuid: dict[str, dict] | None = None,
    display_lang: str = "zh",
) -> tuple[dict | None, list[dict]]:
    xflow_nodes = [item for item in list(model_record.get("xflow_nodes") or []) if isinstance(item, dict)]
    xflow_edges = [item for item in list(model_record.get("xflow_edges") or []) if isinstance(item, dict)]
    if xflow_nodes:
        return _build_tidas_graph_from_xflow(
            db=db,
            model_record=model_record,
            process_json_by_uuid=process_json_by_uuid,
            xflow_nodes=xflow_nodes,
            xflow_edges=xflow_edges,
            display_lang=display_lang,
        )

    instances = [item for item in list(model_record.get("process_instances") or []) if isinstance(item, dict)]
    if not instances:
        return None, []

    flow_meta_by_uuid = _flow_meta_by_uuid_cached(db)
    process_cache = process_json_by_uuid or {}
    nodes: list[dict] = []
    unresolved: list[dict] = []
    node_id_by_internal: dict[str, str] = {}
    port_index: dict[tuple[str, str, str], dict] = {}

    seen_process_uuids_in_graph: set[str] = set()
    seen_process_names_in_graph: dict[str, int] = {}
    for idx, inst in enumerate(instances):
        internal_id = _safe_str(inst.get("internal_id")) or str(idx)
        process_uuid = _safe_str(inst.get("process_uuid"))
        if process_uuid and process_uuid in seen_process_uuids_in_graph:
            continue
        source_json = process_cache.get(process_uuid)
        if source_json is None:
            row = db.get(ReferenceProcess, process_uuid) if process_uuid else None
            source_json = row.process_json if row is not None and isinstance(row.process_json, dict) else None
        if source_json is None:
            unresolved.append(
                {
                    "model_uuid": model_record.get("model_uuid"),
                    "type": "missing_process_reference",
                    "process_uuid": process_uuid,
                    "reason": "referenced process not found in reference_processes",
                }
            )
            continue
        if process_uuid:
            seen_process_uuids_in_graph.add(process_uuid)

        node_id = f"node_tidas_{internal_id}"
        exchanges = list(source_json.get("exchanges") or [])
        inputs, outputs, emissions = _build_tidas_graph_port_lists(
            exchanges=exchanges,
            flow_meta_by_uuid=flow_meta_by_uuid,
            unresolved=unresolved,
            model_uuid=_safe_str(model_record.get("model_uuid")),
            process_uuid=process_uuid,
            node_id=node_id,
        )
        reference_port = (
            next((port for port in outputs if bool(port.get("isProduct"))), None)
            or next((port for port in inputs if bool(port.get("isProduct"))), None)
            or (outputs[0] if outputs else None)
            or (inputs[0] if inputs else None)
        )
        node_name = _make_unique_graph_node_name(
            _pick_process_display_name_for_import(source_json, process_uuid, display_lang=display_lang)
            or f"node_tidas_{internal_id}",
            seen_process_names_in_graph,
        )
        node_id_by_internal[internal_id] = node_id
        node = {
            "id": node_id,
            "node_kind": "unit_process",
            "mode": "normalized",
            "lci_role": "waste_sink" if not any(bool(port.get("isProduct")) for port in outputs) and any(bool(port.get("isProduct")) for port in inputs) else None,
            "process_uuid": process_uuid,
            "name": node_name,
            "location": _safe_str(source_json.get("location")) or "GLO",
            "reference_product": _safe_str(reference_port.get("name") if isinstance(reference_port, dict) else "") or "",
            "inputs": inputs,
            "outputs": outputs,
            "emissions": emissions,
        }
        nodes.append(node)
        for bucket_name, ports in (("output", outputs), ("input", inputs)):
            for port in ports:
                port_index[(internal_id, bucket_name, _safe_str(port.get("flowUuid")))] = port
    edges: list[dict] = []
    for idx, conn in enumerate(list(model_record.get("derived_connections") or []), start=1):
        if not isinstance(conn, dict):
            continue
        source_internal_id = _safe_str(conn.get("source_internal_id"))
        target_internal_id = _safe_str(conn.get("target_internal_id"))
        flow_uuid = _safe_str(conn.get("flow_uuid"))
        source_node_id = node_id_by_internal.get(source_internal_id)
        target_node_id = node_id_by_internal.get(target_internal_id)
        if not source_node_id or not target_node_id or not flow_uuid:
            continue
        source_port = port_index.get((source_internal_id, "output", flow_uuid))
        if source_port is None:
            source_port = port_index.get((source_internal_id, "input", flow_uuid))
        target_port = port_index.get((target_internal_id, "input", flow_uuid))
        if target_port is None:
            target_port = port_index.get((target_internal_id, "output", flow_uuid))
        if source_port is None or target_port is None:
            unresolved.append(
                {
                    "model_uuid": model_record.get("model_uuid"),
                    "type": "missing_connection_port",
                    "flow_uuid": flow_uuid,
                    "source_internal_id": source_internal_id,
                    "target_internal_id": target_internal_id,
                    "reason": "connection flow not found on source/target process ports",
                }
            )
            continue
        amount = float(target_port.get("amount") or source_port.get("amount") or 1.0)
        edges.append(
            {
                "id": f"edge_tidas_{idx}",
                "fromNode": source_node_id,
                "toNode": target_node_id,
                "sourceHandle": f"out:{source_port['id']}",
                "targetHandle": f"in:{target_port['id']}",
                "sourcePortId": str(source_port["id"]),
                "targetPortId": str(target_port["id"]),
                "flowUuid": flow_uuid,
                "flowName": _safe_str(source_port.get("name")) or flow_uuid,
                "quantityMode": "dual",
                "amount": amount,
                "providerAmount": amount,
                "consumerAmount": amount,
                "unit": _safe_str(target_port.get("unit") or source_port.get("unit")) or "kg",
                "type": "technosphere",
            }
        )

    if not nodes:
        return None, unresolved

    _limit_graph_port_visibility_to_connected_edges(nodes=nodes, edges=edges)
    default_viewport = _build_default_graph_viewport(nodes=nodes)

    graph = {
        "functionalUnit": _safe_str(model_record.get("model_name")) or _safe_str(model_record.get("model_uuid")) or "1 unit",
        "nodes": nodes,
        "exchanges": edges,
        "metadata": {
            "source": "tidas_model_import",
            "tidas_model_uuid": _safe_str(model_record.get("model_uuid")),
            "viewport": default_viewport,
        },
    }
    graph = _prune_import_graph_invalid_product_role_edges(
        graph_json=graph,
        unresolved=unresolved,
        model_uuid=_safe_str(model_record.get("model_uuid")),
    )
    _limit_graph_port_visibility_to_connected_edges(
        nodes=graph.get("nodes") if isinstance(graph.get("nodes"), list) else [],
        edges=graph.get("exchanges") if isinstance(graph.get("exchanges"), list) else [],
    )
    return graph, unresolved


def _build_tidas_graph_from_xflow(
    *,
    db: Session,
    model_record: dict,
    process_json_by_uuid: dict[str, dict] | None,
    xflow_nodes: list[dict],
    xflow_edges: list[dict],
    display_lang: str = "zh",
) -> tuple[dict | None, list[dict]]:
    flow_meta_by_uuid = _flow_meta_by_uuid_cached(db)
    process_cache = process_json_by_uuid or {}
    nodes: list[dict] = []
    edges: list[dict] = []
    unresolved: list[dict] = []
    node_positions: dict[str, dict[str, float]] = {}
    node_ids_in_graph: set[str] = set()

    seen_process_uuids_in_graph: set[str] = set()
    seen_process_names_in_graph: dict[str, int] = {}
    for idx, xnode in enumerate(xflow_nodes):
        node_id = _safe_str(xnode.get("id")) or f"node_tidas_xflow_{idx}"
        data = xnode.get("data") if isinstance(xnode.get("data"), dict) else {}
        process_uuid = _safe_str(data.get("id"))
        if process_uuid and process_uuid in seen_process_uuids_in_graph:
            continue
        source_json = process_cache.get(process_uuid)
        if source_json is None:
            row = db.get(ReferenceProcess, process_uuid) if process_uuid else None
            source_json = row.process_json if row is not None and isinstance(row.process_json, dict) else None
        if source_json is None:
            unresolved.append(
                {
                    "model_uuid": model_record.get("model_uuid"),
                    "type": "missing_process_reference",
                    "process_uuid": process_uuid,
                    "node_id": node_id,
                    "reason": "xflow node process not found in reference_processes",
                }
            )
            continue
        if process_uuid:
            seen_process_uuids_in_graph.add(process_uuid)

        exchanges = list(source_json.get("exchanges") or [])
        inputs, outputs, emissions = _build_tidas_graph_port_lists(
            exchanges=exchanges,
            flow_meta_by_uuid=flow_meta_by_uuid,
            unresolved=unresolved,
            model_uuid=_safe_str(model_record.get("model_uuid")),
            process_uuid=process_uuid,
            node_id=node_id,
        )
        inputs, outputs = _apply_xflow_ports_to_node(
            inputs=inputs,
            outputs=outputs,
            xflow_node=xnode,
            flow_meta_by_uuid=flow_meta_by_uuid,
            unresolved=unresolved,
            model_uuid=_safe_str(model_record.get("model_uuid")),
            process_uuid=process_uuid,
            node_id=node_id,
        )

        xflow_qref = _safe_str(data.get("quantitativeReference"))
        if xflow_qref.isdigit():
            qref_index = int(xflow_qref)
            if 0 <= qref_index < len(outputs):
                outputs[qref_index]["isProduct"] = True

        reference_port = (
            next((port for port in outputs if bool(port.get("isProduct"))), None)
            or next((port for port in inputs if bool(port.get("isProduct"))), None)
            or (outputs[0] if outputs else None)
            or (inputs[0] if inputs else None)
        )
        position = xnode.get("position") if isinstance(xnode.get("position"), dict) else {}
        if position:
            node_positions[node_id] = {
                "x": _safe_float(position.get("x")),
                "y": _safe_float(position.get("y")),
            }

        label = data.get("label") if isinstance(data.get("label"), dict) else {}
        preferred_langs = ("en", "zh") if (_safe_str(display_lang) or "").lower() == "en" else ("zh", "en")
        node_name = _make_unique_graph_node_name(
            _pick_localized_text(label.get("baseName"), preferred_langs=preferred_langs)
            or _pick_process_display_name_for_import(source_json, process_uuid, display_lang=display_lang)
            or process_uuid
            or node_id,
            seen_process_names_in_graph,
        )
        node = {
            "id": node_id,
            "node_kind": "unit_process",
            "mode": "normalized",
            "lci_role": "waste_sink" if not any(bool(port.get("isProduct")) for port in outputs) and any(bool(port.get("isProduct")) for port in inputs) else None,
            "process_uuid": process_uuid,
            "name": node_name,
            "location": _safe_str(source_json.get("location")) or "GLO",
            "reference_product": _safe_str(reference_port.get("name") if isinstance(reference_port, dict) else "") or "",
            "inputs": inputs,
            "outputs": outputs,
            "emissions": emissions,
        }
        nodes.append(node)
        node_ids_in_graph.add(node_id)
    for idx, xedge in enumerate(xflow_edges, start=1):
        source = xedge.get("source") if isinstance(xedge.get("source"), dict) else {}
        target = xedge.get("target") if isinstance(xedge.get("target"), dict) else {}
        source_node_id = _safe_str(source.get("cell"))
        target_node_id = _safe_str(target.get("cell"))
        if source_node_id not in node_ids_in_graph or target_node_id not in node_ids_in_graph:
            continue
        source_port_id = _safe_str(source.get("port"))
        target_port_id = _safe_str(target.get("port"))
        data = xedge.get("data") if isinstance(xedge.get("data"), dict) else {}
        connection = data.get("connection") if isinstance(data.get("connection"), dict) else {}
        output_exchange = connection.get("outputExchange") if isinstance(connection.get("outputExchange"), dict) else {}
        flow_uuid = _safe_str(output_exchange.get("@flowUUID"))
        if not flow_uuid and ":" in source_port_id:
            flow_uuid = source_port_id.split(":", 1)[1]
        if not flow_uuid:
            continue
        if flow_meta_by_uuid.get(flow_uuid) is None:
            unresolved.append(
                {
                    "model_uuid": _safe_str(model_record.get("model_uuid")),
                    "type": "missing_flow_reference",
                    "node_id": source_node_id,
                    "flow_uuid": flow_uuid,
                    "edge_id": _safe_str(xedge.get("id")) or f"edge_tidas_xflow_{idx}",
                    "action": "filtered",
                    "reason": "xflow edge flow_uuid not found in flow catalog; import graph edge skipped",
                }
            )
            continue
        amount = _safe_float(connection.get("exchangeAmount"), default=0.0)
        if amount <= 0:
            amount = 1.0
        edges.append(
            {
                "id": _safe_str(xedge.get("id")) or f"edge_tidas_xflow_{idx}",
                "fromNode": source_node_id,
                "toNode": target_node_id,
                "sourceHandle": f"out:{source_port_id}" if source_port_id else None,
                "targetHandle": f"in:{target_port_id}" if target_port_id else None,
                "sourcePortId": source_port_id or None,
                "targetPortId": target_port_id or None,
                "flowUuid": flow_uuid,
                "flowName": "",
                "quantityMode": "dual",
                "amount": amount,
                "providerAmount": amount,
                "consumerAmount": amount,
                "unit": "kg",
                "type": "technosphere",
            }
        )

    if not nodes:
        return None, unresolved

    _limit_graph_port_visibility_to_connected_edges(nodes=nodes, edges=edges)
    default_viewport = _build_default_graph_viewport(node_positions=node_positions, nodes=nodes)

    graph = {
        "functionalUnit": _safe_str(model_record.get("model_name")) or _safe_str(model_record.get("model_uuid")) or "1 unit",
        "nodes": nodes,
        "exchanges": edges,
        "metadata": {
            "source": "tidas_model_import",
            "tidas_model_uuid": _safe_str(model_record.get("model_uuid")),
            "node_positions": node_positions,
            "viewport": default_viewport,
        },
    }
    graph = _prune_import_graph_invalid_product_role_edges(
        graph_json=graph,
        unresolved=unresolved,
        model_uuid=_safe_str(model_record.get("model_uuid")),
    )
    _limit_graph_port_visibility_to_connected_edges(
        nodes=graph.get("nodes") if isinstance(graph.get("nodes"), list) else [],
        edges=graph.get("exchanges") if isinstance(graph.get("exchanges"), list) else [],
    )
    return graph, unresolved


def _create_project_version_from_graph_json(*, db: Session, project_id: str, graph_json: dict) -> ModelVersion:
    graph = HybridGraph.model_validate(graph_json)
    normalize_graph_product_flags(graph)
    validate_graph_contract(graph, require_non_empty=True, allow_pts_nodes=True)
    validate_graph_flow_type_contract(graph, db=db, stage="import_model")
    validate_graph_port_names_against_flow_catalog(graph, db=db, stage="import_model")
    _bind_pts_published_versions_for_graph(db=db, project_id=project_id, graph=graph)
    latest_version = db.query(func.max(ModelVersion.version)).filter(ModelVersion.model_id == project_id).scalar()
    next_version = (latest_version or 0) + 1
    persisted_graph_json = graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(persisted_graph_json, db=db)
    normalized_graph_json = _normalize_graph_json_for_storage(persisted_graph_json)
    version = ModelVersion(
        model_id=project_id,
        version=next_version,
        graph_hash=_compute_graph_hash_from_graph_json(normalized_graph_json),
        hybrid_graph_json=normalized_graph_json,
    )
    db.add(version)
    _sync_pts_resources_from_graph(db=db, project_id=project_id, graph=graph)
    return version


def _replace_flow_display_name(current_name: str | None, standard_name: str | None) -> str:
    standard = _safe_str(standard_name) or ""
    current = _safe_str(current_name) or ""
    if not standard:
        return current
    if not current:
        return standard
    if "@" in current:
        _, suffix = current.split("@", 1)
        suffix = suffix.strip()
        return f"{standard}@{suffix}" if suffix else standard
    return standard


def _sync_graph_flow_names(graph_json: dict, *, updated_flow_names: dict[str, str]) -> tuple[int, int]:
    changed_port_count = 0
    changed_edge_count = 0
    nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
    exchanges = graph_json.get("exchanges") if isinstance(graph_json.get("exchanges"), list) else []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        for bucket_name in ("inputs", "outputs"):
            ports = node.get(bucket_name) if isinstance(node.get(bucket_name), list) else []
            for port in ports:
                if not isinstance(port, dict):
                    continue
                flow_uuid = _safe_str(port.get("flowUuid"))
                standard_name = updated_flow_names.get(flow_uuid) or updated_flow_names.get(flow_uuid.lower())
                if not standard_name:
                    continue
                new_name = _replace_flow_display_name(port.get("name"), standard_name)
                if _safe_str(port.get("name")) != new_name:
                    port["name"] = new_name
                    changed_port_count += 1

    for edge in exchanges:
        if not isinstance(edge, dict):
            continue
        flow_uuid = _safe_str(edge.get("flowUuid"))
        standard_name = updated_flow_names.get(flow_uuid) or updated_flow_names.get(flow_uuid.lower())
        if not standard_name:
            continue
        new_name = _replace_flow_display_name(edge.get("flowName"), standard_name)
        if _safe_str(edge.get("flowName")) != new_name:
            edge["flowName"] = new_name
            changed_edge_count += 1

    return changed_port_count, changed_edge_count


def _enrich_graph_flow_name_en(graph_json: dict, *, db: Session) -> None:
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return

    nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
    exchanges = graph_json.get("exchanges") if isinstance(graph_json.get("exchanges"), list) else []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        for bucket_name in ("inputs", "outputs"):
            ports = node.get(bucket_name) if isinstance(node.get(bucket_name), list) else []
            for port in ports:
                if not isinstance(port, dict):
                    continue
                flow_uuid = _safe_str(port.get("flowUuid"))
                if not flow_uuid:
                    continue
                flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
                if flow_name_en:
                    port["flow_name_en"] = flow_name_en

    for edge in exchanges:
        if not isinstance(edge, dict):
            continue
        flow_uuid = _safe_str(edge.get("flowUuid"))
        if not flow_uuid:
            continue
        flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
        if flow_name_en:
            edge["flow_name_en"] = flow_name_en


def _enrich_node_ports_flow_name_en(node_json: dict, *, db: Session) -> None:
    if not isinstance(node_json, dict):
        return
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return
    for bucket_name in ("inputs", "outputs"):
        ports = node_json.get(bucket_name) if isinstance(node_json.get(bucket_name), list) else []
        for port in ports:
            if not isinstance(port, dict):
                continue
            flow_uuid = _safe_str(port.get("flowUuid"))
            if not flow_uuid:
                continue
            flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
            if flow_name_en:
                port["flow_name_en"] = flow_name_en


def _detect_project_flow_name_outdated_refs(
    *,
    db: Session,
    graph_json: dict | None,
) -> tuple[int, list[dict]]:
    if not isinstance(graph_json, dict):
        return 0, []
    flow_meta = _flow_meta_by_uuid_cached(db)
    evidence: list[dict] = []
    nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
    for node in nodes:
        if not isinstance(node, dict) or str(node.get("node_kind") or "") == "pts_module":
            continue
        for bucket_name in ("inputs", "outputs"):
            ports = node.get(bucket_name) if isinstance(node.get(bucket_name), list) else []
            for port in ports:
                if not isinstance(port, dict):
                    continue
                actual_name = _normalize_port_display_name(port.get("name"))
                if "@" in actual_name:
                    continue
                flow_uuid = _safe_str(port.get("flowUuid"))
                if not flow_uuid:
                    continue
                meta = flow_meta.get(flow_uuid) or flow_meta.get(flow_uuid.lower())
                if not meta:
                    continue
                expected_name = _normalize_port_display_name(meta[0])
                if not expected_name or not actual_name or actual_name == expected_name:
                    continue
                evidence.append(
                    {
                        "node_id": _safe_str(node.get("id")),
                        "node_name": _safe_str(node.get("name")),
                        "port_id": _safe_str(port.get("id")),
                        "bucket": bucket_name,
                        "flow_uuid": flow_uuid,
                        "expected_flow_name": _truncate_text_preview(expected_name),
                        "actual_port_name": _truncate_text_preview(actual_name),
                    }
                )
                if len(evidence) >= 20:
                    return len(evidence), evidence
    return len(evidence), evidence


def _sync_project_latest_version_flow_names(
    *,
    db: Session,
    project_id: str,
) -> tuple[int | None, int, int, int, int, int]:
    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == project_id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is None or not isinstance(latest_row.hybrid_graph_json, dict):
        return None, 0, 0, 0, 0, 0

    flow_meta = _flow_meta_by_uuid_cached(db)
    updated_flow_names = {
        flow_uuid: str(meta[0] or "")
        for flow_uuid, meta in flow_meta.items()
        if flow_uuid and meta and str(meta[0] or "")
    }
    cloned_graph = json.loads(json.dumps(latest_row.hybrid_graph_json, ensure_ascii=False))
    synced_port_count, synced_edge_count = _sync_graph_flow_names(cloned_graph, updated_flow_names=updated_flow_names)
    if synced_port_count == 0 and synced_edge_count == 0:
        return latest_row.version, 0, 0, 0, 0, 0

    latest_row.hybrid_graph_json = cloned_graph
    latest_row.graph_hash = _compute_graph_hash_from_graph_json(cloned_graph)
    db.query(Model).filter(Model.id == project_id).update({Model.updated_at: datetime.utcnow()}, synchronize_session=False)

    cleared_pts_compile_count = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == project_id)
        .delete(synchronize_session=False)
    )
    cleared_pts_external_count = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == project_id)
        .delete(synchronize_session=False)
    )
    cleared_pts_definition_count = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == project_id)
        .delete(synchronize_session=False)
    )
    return (
        latest_row.version,
        synced_port_count,
        synced_edge_count,
        cleared_pts_compile_count,
        cleared_pts_external_count,
        cleared_pts_definition_count,
    )


def _first_category_segment(value: str | None) -> str | None:
    raw = _safe_str(value)
    if not raw:
        return None
    return _safe_str(raw.split(";", 1)[0])


def _cache_get(key: str, ttl_seconds: float = _CACHE_TTL_SECONDS) -> object | None:
    entry = _api_cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    if (time.time() - ts) > ttl_seconds:
        _api_cache.pop(key, None)
        return None
    return value


def _cache_set(key: str, value: object) -> None:
    _api_cache[key] = (time.time(), value)


def _cache_invalidate_prefix(prefix: str) -> int:
    removed = 0
    for key in list(_api_cache.keys()):
        if key.startswith(prefix):
            _api_cache.pop(key, None)
            removed += 1
    return removed


def _cache_revision(domain: str) -> int:
    return int(_api_cache_revisions.get(domain, 0))


def _cache_bump_revision(domain: str) -> int:
    next_value = _cache_revision(domain) + 1
    _api_cache_revisions[domain] = next_value
    return next_value


def _invalidate_management_caches(
    *,
    projects: bool = False,
    flows: bool = False,
    stats: bool = False,
    reference_processes: bool = False,
) -> None:
    if projects:
        _cache_bump_revision("projects")
        _cache_invalidate_prefix("projects:v1:")
    if flows:
        _cache_bump_revision("flows")
        _cache_bump_revision("flow_categories")
        _cache_bump_revision("flow_meta")
        _cache_invalidate_prefix("flows:v1:")
        _cache_invalidate_prefix("flows:v2:")
        _cache_invalidate_prefix("flow_categories:v1:")
        _cache_invalidate_prefix("flow_uuid_set:v1")
        _cache_invalidate_prefix("flow_meta_by_uuid:v1")
        _cache_invalidate_prefix("flow_name_en_by_uuid:v1")
    if stats:
        _cache_bump_revision("stats")
        _cache_invalidate_prefix("stats:v2")
    if reference_processes:
        _cache_bump_revision("processes")
        _cache_bump_revision("reference_processes_catalog")
        _cache_bump_revision("reference_process_report")
        _cache_invalidate_prefix("processes:v1:")
        _cache_invalidate_prefix("reference_processes_catalog:v1:")
        _cache_invalidate_prefix("reference_process_report:v1:")


def _refresh_flow_runtime_caches_for_current_request() -> None:
    _cache_bump_revision("flows")
    _cache_bump_revision("flow_categories")
    _cache_bump_revision("flow_meta")
    _cache_invalidate_prefix("flows:v1:")
    _cache_invalidate_prefix("flows:v2:")
    _cache_invalidate_prefix("flow_categories:v1:")
    _cache_invalidate_prefix("flow_uuid_set:v1")
    _cache_invalidate_prefix("flow_meta_by_uuid:v1")
    _cache_invalidate_prefix("flow_name_en_by_uuid:v1")


def _build_etag_for_payload(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"\"{digest}\""


def _is_if_none_match_hit(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match:
        return False
    candidates = [item.strip() for item in if_none_match.split(",") if item.strip()]
    return etag in candidates or "*" in candidates


_PROJECT_STATUS_SET = {"active", "draft", "archived"}


def _normalize_project_status(value: object | None, *, default: str = "active") -> str:
    raw = _safe_str(value)
    if not raw:
        return default
    normalized = raw.lower()
    if normalized not in _PROJECT_STATUS_SET:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REQUEST", "message": f"Invalid status: {raw}. Allowed: active|draft|archived"},
        )
    return normalized


def _latest_version_by_project_id(db: Session) -> dict[str, ModelVersion]:
    rows = db.query(ModelVersion).order_by(ModelVersion.model_id.asc(), ModelVersion.version.desc(), ModelVersion.created_at.desc()).all()
    latest_by_project: dict[str, ModelVersion] = {}
    for row in rows:
        pid = str(row.model_id)
        if pid not in latest_by_project:
            latest_by_project[pid] = row
    return latest_by_project


def _graph_process_and_flow_counts(graph_json: dict | None) -> tuple[int, int]:
    if not isinstance(graph_json, dict):
        return 0, 0
    nodes = graph_json.get("nodes")
    process_count = len(nodes) if isinstance(nodes, list) else 0
    flow_keys: set[str] = set()
    for node in nodes if isinstance(nodes, list) else []:
        if not isinstance(node, dict):
            continue
        for bucket in ("inputs", "outputs"):
            ports = node.get(bucket)
            if not isinstance(ports, list):
                continue
            for port in ports:
                if not isinstance(port, dict):
                    continue
                flow_uuid = _safe_str(port.get("flowUuid") or port.get("flow_uuid"))
                if flow_uuid:
                    flow_keys.add(flow_uuid)
    return process_count, len(flow_keys)


def _build_project_out(model: Model, latest: ModelVersion | None = None) -> ProjectOut:
    process_count = 0
    flow_count = 0
    flow_name_sync_needed = False
    outdated_flow_refs_count = 0
    outdated_flow_ref_examples: list[dict] = []
    if latest is not None:
        graph_json = latest.hybrid_graph_json if isinstance(latest.hybrid_graph_json, dict) else {}
        process_count, flow_count = _graph_process_and_flow_counts(graph_json)
        tmp_db = SessionLocal()
        try:
            outdated_flow_refs_count, outdated_flow_ref_examples = _detect_project_flow_name_outdated_refs(
                db=tmp_db,
                graph_json=graph_json,
            )
        finally:
            tmp_db.close()
        flow_name_sync_needed = outdated_flow_refs_count > 0
    return ProjectOut(
        project_id=model.id,
        name=model.name,
        reference_product=_safe_str(model.reference_product),
        functional_unit=_safe_str(model.functional_unit),
        system_boundary=_safe_str(model.system_boundary),
        time_representativeness=_safe_str(model.time_representativeness),
        geography=_safe_str(model.geography),
        description=_safe_str(model.description),
        status=_safe_str(model.status) or "active",
        process_count=process_count,
        flow_count=flow_count,
        created_at=model.created_at,
        updated_at=model.updated_at,
        latest_version=latest.version if latest else None,
        latest_version_created_at=latest.created_at if latest else None,
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples[:5],
    )


def _build_flow_sync_state_for_graph_json(*, db: Session, graph_json: dict) -> tuple[bool, int, list[dict]]:
    outdated_flow_refs_count, outdated_flow_ref_examples = _detect_project_flow_name_outdated_refs(
        db=db,
        graph_json=graph_json,
    )
    return outdated_flow_refs_count > 0, outdated_flow_refs_count, outdated_flow_ref_examples[:5]


def _is_pts_publication_auto_repairable(*, resource: PtsResource | None, external: PtsExternalArtifact | None) -> bool:
    if resource is None or external is not None:
        return False
    if resource.active_published_version is None:
        return False
    shell_node = dict(resource.shell_node_json or {})
    return bool(shell_node.get("inputs") or shell_node.get("outputs"))


def _build_pts_validation_summary(*, db: Session, project_id: str, graph: HybridGraph) -> PtsValidationSummary:
    items: list[PtsValidationItem] = []
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        resource = (
            db.query(PtsResource)
            .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == pts_uuid)
            .first()
        )
        external = _load_pts_active_external_artifact(db=db, project_id=project_id, pts_uuid=pts_uuid)
        if external is not None:
            continue
        reason = "pts_resource_missing"
        if resource is not None and resource.active_published_version is not None:
            reason = "published_resource_missing_artifact"
        elif resource is not None:
            reason = "pts_resource_unpublished"
        auto_repairable = _is_pts_publication_auto_repairable(resource=resource, external=external)
        items.append(
            PtsValidationItem(
                node_id=str(node.id or ""),
                node_name=str(node.name or "") or None,
                pts_uuid=pts_uuid,
                reason=reason,
                has_resource=resource is not None,
                active_published_version=(
                    int(resource.active_published_version)
                    if resource is not None and resource.active_published_version is not None
                    else None
                ),
                latest_published_version=(
                    int(resource.latest_published_version)
                    if resource is not None and resource.latest_published_version is not None
                    else None
                ),
                has_active_artifact=False,
                auto_repairable=auto_repairable,
            )
        )
    return PtsValidationSummary(
        ok=not items,
        invalid_count=len(items),
        auto_repairable=any(item.auto_repairable for item in items),
        items=items,
    )


def _repair_pts_publication_from_resource(*, db: Session, resource: PtsResource) -> tuple[bool, str]:
    existing = _load_pts_active_external_artifact(
        db=db,
        project_id=str(resource.project_id or ""),
        pts_uuid=str(resource.pts_uuid or ""),
    )
    if existing is not None:
        return False, "already_has_active_artifact"
    if resource.active_published_version is None:
        return False, "resource_has_no_active_published_version"
    shell_node = dict(resource.shell_node_json or {})
    if not shell_node:
        return False, "resource_shell_node_missing"

    def _rows(rows: object, direction: str) -> list[dict]:
        result: list[dict] = []
        for item in rows if isinstance(rows, list) else []:
            slim = _slim_exchange_row(item if isinstance(item, dict) else None, direction, True)
            if slim is not None:
                result.append(slim)
        return result

    frontend_ports = {
        "inputs": _rows(shell_node.get("inputs"), "input"),
        "outputs": _rows(shell_node.get("outputs"), "output"),
    }
    payload = {
        "project_id": str(resource.project_id or ""),
        "pts_uuid": str(resource.pts_uuid or ""),
        "pts_node_id": str(resource.pts_node_id or shell_node.get("id") or ""),
        "graph_hash": str(resource.compiled_graph_hash or resource.latest_graph_hash or ""),
        "ok": True,
        "errors": [],
        "warnings": [],
        "matrix_size": 0,
        "invertible": True,
        "external_boundary": {
            "inputs": list(frontend_ports["inputs"]),
            "outputs": list(frontend_ports["outputs"]),
            "elementary": [],
        },
        "virtual_processes": [],
        "frontend_ports": frontend_ports,
        "output_virtual_process_bindings": [],
        "definition_summary": {
            "pts_uuid": str(resource.pts_uuid or ""),
            "pts_node_id": str(resource.pts_node_id or ""),
            "internal_node_count": len((dict(resource.pts_graph_json or {})).get("nodes") or []),
            "product_ref_count": 0,
        },
    }
    _enrich_pts_external_payload_flow_name_en(payload, db=db)
    artifact = PtsExternalArtifact(
        project_id=str(resource.project_id or ""),
        pts_id=str(resource.pts_uuid or ""),
        pts_uuid=str(resource.pts_uuid or ""),
        pts_node_id=str(resource.pts_node_id or shell_node.get("id") or ""),
        graph_hash=str(resource.compiled_graph_hash or resource.latest_graph_hash or ""),
        published_version=int(resource.active_published_version or resource.latest_published_version or 1),
        source_compile_id=None,
        source_compile_version=(
            int(resource.latest_compile_version) if resource.latest_compile_version is not None else None
        ),
        artifact_json=payload,
    )
    db.add(artifact)
    db.flush()
    resource.latest_published_version = (
        int(resource.latest_published_version)
        if resource.latest_published_version is not None
        else int(artifact.published_version or 0) or None
    )
    resource.active_published_version = int(artifact.published_version or 0) or resource.active_published_version
    resource.published_at = artifact.updated_at or artifact.created_at
    resource.shell_node_json = _build_pts_shell_snapshot_from_external(row=resource, external=artifact)
    return True, "repaired_missing_active_artifact"


def _build_project_integrity_summary(
    *,
    pts_validation: PtsValidationSummary,
    flow_name_sync_needed: bool,
    outdated_flow_refs_count: int,
    outdated_flow_ref_examples: list[dict],
) -> ProjectIntegritySummary:
    issues: list[ProjectIntegrityIssue] = []
    for item in pts_validation.items:
        issues.append(
            ProjectIntegrityIssue(
                kind="pts_publication",
                severity="error",
                code="PTS_PUBLICATION_INVALID",
                message=f"PTS {item.pts_uuid} published state is invalid for save/run.",
                auto_repairable=item.auto_repairable,
                details=item.model_dump(mode="python"),
            )
        )
    if flow_name_sync_needed:
        issues.append(
            ProjectIntegrityIssue(
                kind="flow_name_sync",
                severity="warning",
                code="FLOW_NAME_SYNC_NEEDED",
                message="Some node/port flow names are outdated compared with the flow catalog.",
                auto_repairable=outdated_flow_refs_count > 0,
                details={
                    "outdated_count": int(outdated_flow_refs_count or 0),
                    "examples": list(outdated_flow_ref_examples or []),
                },
            )
        )
    return ProjectIntegritySummary(
        ok=not issues,
        issue_count=len(issues),
        auto_repairable=any(issue.auto_repairable for issue in issues),
        issues=issues,
    )


def _latest_graphs_with_project_meta(db: Session) -> list[tuple[Model, ModelVersion]]:
    latest_by_project = _latest_version_by_project_id(db)
    if not latest_by_project:
        return []
    models = db.query(Model).all()
    result: list[tuple[Model, ModelVersion]] = []
    for model in models:
        latest = latest_by_project.get(str(model.id))
        if latest is not None:
            result.append((model, latest))
    return result


def _build_process_items_from_latest_graphs(db: Session) -> list[ProcessListItem]:
    latest_rows = _latest_graphs_with_project_meta(db)
    by_process: dict[str, dict] = {}
    used_projects: dict[str, set[str]] = defaultdict(set)

    for model, version in latest_rows:
        graph_json = version.hybrid_graph_json if isinstance(version.hybrid_graph_json, dict) else {}
        nodes = graph_json.get("nodes")
        if not isinstance(nodes, list):
            continue
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_kind = _safe_str(node.get("node_kind")) or "unit_process"
            if node_kind not in {"unit_process", "market_process"}:
                continue
            process_key = _safe_str(node.get("process_uuid")) or _safe_str(node.get("id"))
            if not process_key:
                continue
            inputs = node.get("inputs")
            outputs = node.get("outputs")
            output_ports = outputs if isinstance(outputs, list) else []
            reference_flow = None
            for port in output_ports:
                if not isinstance(port, dict):
                    continue
                if bool(port.get("isProduct")):
                    reference_flow = _safe_str(port.get("name")) or _safe_str(port.get("flowUuid"))
                    break

            if process_key not in by_process:
                by_process[process_key] = {
                    "process_uuid": process_key,
                    "process_name": _safe_str(node.get("name")) or process_key,
                    "type": node_kind,
                    "reference_flow_uuid": None,
                    "reference_flow_name": reference_flow,
                    "input_count": len(inputs) if isinstance(inputs, list) else 0,
                    "output_count": len(output_ports),
                    "balance_status": "unchecked",
                    "last_modified": version.created_at,
                }
            used_projects[process_key].add(str(model.id))

    items: list[ProcessListItem] = []
    for process_uuid, payload in by_process.items():
        items.append(
            ProcessListItem(
                process_uuid=payload["process_uuid"],
                process_name=payload["process_name"],
                type=payload["type"],
                reference_flow_uuid=payload["reference_flow_uuid"],
                reference_flow_name=payload["reference_flow_name"],
                input_count=int(payload["input_count"]),
                output_count=int(payload["output_count"]),
                used_in_projects=len(used_projects.get(process_uuid) or set()),
                balance_status=payload["balance_status"],
                last_modified=payload["last_modified"],
            )
        )
    return items


def _build_flow_used_in_processes_map(db: Session) -> dict[str, int]:
    latest_rows = _latest_graphs_with_project_meta(db)
    used_by_flow: dict[str, set[str]] = defaultdict(set)
    for _, version in latest_rows:
        graph_json = version.hybrid_graph_json if isinstance(version.hybrid_graph_json, dict) else {}
        nodes = graph_json.get("nodes")
        if not isinstance(nodes, list):
            continue
        for node in nodes:
            if not isinstance(node, dict):
                continue
            process_key = _safe_str(node.get("process_uuid")) or _safe_str(node.get("id"))
            if not process_key:
                continue
            for bucket in ("inputs", "outputs"):
                ports = node.get(bucket)
                if not isinstance(ports, list):
                    continue
                for port in ports:
                    if not isinstance(port, dict):
                        continue
                    flow_uuid = _safe_str(port.get("flowUuid") or port.get("flow_uuid"))
                    if flow_uuid:
                        used_by_flow[flow_uuid].add(process_key)
    return {flow_uuid: len(processes) for flow_uuid, processes in used_by_flow.items()}


def _resolve_model_version_graph_hash(row: ModelVersion, *, assign_if_missing: bool = False) -> str:
    resolved = str(row.graph_hash or "").strip()
    if resolved:
        return resolved
    source = row.hybrid_graph_json if isinstance(row.hybrid_graph_json, dict) else {}
    resolved = _compute_graph_hash_from_graph_json(source)
    if assign_if_missing:
        row.graph_hash = resolved
    return resolved


def _is_sqlite_database() -> bool:
    return settings.database_url.strip().lower().startswith("sqlite")


def _run_sqlite_vacuum() -> dict:
    if not _is_sqlite_database():
        return {"executed": False, "reason": "database is not sqlite"}
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        checkpoint = cursor.fetchall() if cursor.description else []
        cursor.execute("VACUUM")
        raw_conn.commit()
        return {"executed": True, "checkpoint": checkpoint}
    finally:
        raw_conn.close()


def _prune_model_versions_retention(
    *,
    db: Session,
    keep_latest: int,
    dry_run: bool,
    project_id: str | None = None,
    vacuum_after_cleanup: bool = False,
) -> dict:
    if keep_latest < 1:
        raise ValueError("keep_latest must be >= 1")

    query = db.query(ModelVersion)
    if project_id:
        query = query.filter(ModelVersion.model_id == project_id)
    rows = query.order_by(ModelVersion.model_id.asc(), ModelVersion.version.desc(), ModelVersion.created_at.desc()).all()

    scanned_versions = 0
    scanned_projects = 0
    redundant_version_ids: list[str] = []
    redundant_examples: list[dict] = []
    current_project: str | None = None
    seen_per_project = 0

    for row in rows:
        scanned_versions += 1
        row_project_id = str(row.model_id)
        if row_project_id != current_project:
            current_project = row_project_id
            scanned_projects += 1
            seen_per_project = 0
        seen_per_project += 1
        if seen_per_project <= keep_latest:
            continue
        redundant_version_ids.append(str(row.id))
        if len(redundant_examples) < 200:
            redundant_examples.append(
                {
                    "model_version_id": str(row.id),
                    "project_id": row_project_id,
                    "version": int(row.version),
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                }
            )

    cleared_run_job_refs = 0
    deleted_versions = 0
    if not dry_run and redundant_version_ids:
        for batch in _iter_chunks(redundant_version_ids):
            cleared_run_job_refs += (
                db.query(RunJob)
                .filter(RunJob.model_version_id.in_(batch))
                .update({RunJob.model_version_id: None}, synchronize_session=False)
            )
            deleted_versions += db.query(ModelVersion).filter(ModelVersion.id.in_(batch)).delete(synchronize_session=False)
        db.commit()
    elif not dry_run:
        db.commit()

    vacuum_result = {"executed": False, "reason": "disabled"}
    if not dry_run and vacuum_after_cleanup:
        db.close()
        vacuum_result = _run_sqlite_vacuum()

    return {
        "keep_latest": keep_latest,
        "project_id": project_id,
        "dry_run": dry_run,
        "scanned_projects": scanned_projects,
        "scanned_versions": scanned_versions,
        "redundant_versions": len(redundant_version_ids),
        "deleted_versions": deleted_versions,
        "cleared_run_job_refs": cleared_run_job_refs,
        "redundant_model_version_ids": redundant_version_ids[:500],
        "redundant_examples": redundant_examples,
        "vacuum": vacuum_result,
    }


def require_debug_access(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")) -> None:
    if settings.debug:
        return
    if settings.admin_token and x_admin_token == settings.admin_token:
        return
    raise HTTPException(status_code=403, detail="Debug endpoints require DEBUG=true or admin token")


def persist_debug_diagnostic(
    *,
    db: Session,
    persist: bool,
    diagnostic_type: str,
    payload: dict,
    result: dict,
    run_id: str | None = None,
    project_id: str | None = None,
    graph_hash: str | None = None,
) -> str | None:
    if not persist:
        return None
    row = DebugDiagnostic(
        diagnostic_type=diagnostic_type,
        run_id=run_id,
        project_id=project_id,
        graph_hash=graph_hash,
        payload_json=payload,
        result_json=result,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row.id


def _matrix_rank_and_determinant(matrix: list[list[float]], tol: float = 1e-12) -> tuple[int, float]:
    n = len(matrix)
    if n == 0:
        return 0, 0.0
    m = [row[:] for row in matrix]
    rank = 0
    det = 1.0
    sign = 1.0
    for col in range(n):
        pivot = max(range(rank, n), key=lambda r: abs(m[r][col]))
        if abs(m[pivot][col]) <= tol:
            det = 0.0
            continue
        if pivot != rank:
            m[rank], m[pivot] = m[pivot], m[rank]
            sign *= -1.0
        pivot_val = m[rank][col]
        det *= pivot_val
        for r in range(rank + 1, n):
            factor = m[r][col] / pivot_val
            if abs(factor) <= tol:
                continue
            for c in range(col, n):
                m[r][c] -= factor * m[rank][c]
        rank += 1
    if rank < n:
        det = 0.0
    else:
        det *= sign
    return rank, det


def _build_snapshot_matrix(snapshot: dict) -> dict:
    processes = snapshot.get("processes") if isinstance(snapshot, dict) else []
    exchanges = snapshot.get("exchanges") if isinstance(snapshot, dict) else []
    links = snapshot.get("links") if isinstance(snapshot, dict) else []
    process_rows = [p for p in processes if isinstance(p, dict)]
    exchange_rows = [e for e in exchanges if isinstance(e, dict)]
    link_rows = [l for l in links if isinstance(l, dict)]

    process_ids = [str(p.get("process_uuid") or "") for p in process_rows]
    idx = {pid: i for i, pid in enumerate(process_ids) if pid}
    n = len(process_ids)
    a = [[0.0 for _ in range(n)] for _ in range(n)]

    exchange_by_id = {str(e.get("exchange_id") or ""): e for e in exchange_rows}
    ref_amount_by_process: dict[str, float] = {}
    ref_exchange_by_process: dict[str, str] = {}
    for p in process_rows:
        pid = str(p.get("process_uuid") or "")
        ref_exchange_id = str(p.get("reference_product_flow_uuid") or "")
        ref_exchange_by_process[pid] = ref_exchange_id
        ref_exchange = exchange_by_id.get(ref_exchange_id)
        ref_amount_by_process[pid] = float((ref_exchange or {}).get("amount") or 0.0)

    for link in link_rows:
        provider = str(link.get("provider_process_uuid") or "")
        consumer = str(link.get("consumer_process_uuid") or "")
        if provider not in idx or consumer not in idx:
            continue
        denom = ref_amount_by_process.get(consumer, 0.0)
        if denom <= 1e-12:
            continue
        amount = float(link.get("consumer_amount") or link.get("amount") or 0.0)
        if amount == 0.0:
            continue
        a[idx[provider]][idx[consumer]] += amount / denom

    i_minus_a = [[(1.0 if i == j else 0.0) - a[i][j] for j in range(n)] for i in range(n)]
    rank, det = _matrix_rank_and_determinant(i_minus_a)
    invertible = rank == n and abs(det) > 1e-12

    non_zero_entries: list[dict] = []
    for i in range(n):
        for j in range(n):
            value = a[i][j]
            if abs(value) <= 1e-12:
                continue
            non_zero_entries.append(
                {
                    "row": i,
                    "col": j,
                    "provider_process_uuid": process_ids[i],
                    "consumer_process_uuid": process_ids[j],
                    "value": value,
                }
            )

    adjacency: dict[str, set[str]] = {}
    for link in link_rows:
        provider = str(link.get("provider_process_uuid") or "")
        consumer = str(link.get("consumer_process_uuid") or "")
        if not provider or not consumer:
            continue
        adjacency.setdefault(provider, set()).add(consumer)

    suspect_cycles: list[list[str]] = []
    visited: set[str] = set()
    stack: list[str] = []
    in_stack: set[str] = set()

    def dfs(node: str) -> None:
        visited.add(node)
        stack.append(node)
        in_stack.add(node)
        for nxt in adjacency.get(node, set()):
            if nxt not in visited:
                dfs(nxt)
            elif nxt in in_stack:
                start_idx = stack.index(nxt)
                cycle = stack[start_idx:] + [nxt]
                if cycle not in suspect_cycles:
                    suspect_cycles.append(cycle)
        stack.pop()
        in_stack.remove(node)

    for pid in process_ids:
        if pid and pid not in visited:
            dfs(pid)

    evidences: list[dict] = []
    reason_counts = {
        "self_dependency": 0,
        "zero_reference": 0,
        "duplicate_reference_flow": 0,
        "unresolved_provider_cycle": 0,
    }
    for i, pid in enumerate(process_ids):
        if abs(a[i][i]) > 1e-12:
            reason_counts["self_dependency"] += 1
            evidences.append({"category": "self_dependency", "process_uuid": pid, "value": a[i][i]})
    for pid, ref_amount in ref_amount_by_process.items():
        if ref_amount <= 1e-12:
            reason_counts["zero_reference"] += 1
            evidences.append(
                {
                    "category": "zero_reference",
                    "process_uuid": pid,
                    "reference_exchange_id": ref_exchange_by_process.get(pid),
                    "amount": ref_amount,
                }
            )
    ref_exchange_ids = [ref_exchange_by_process.get(pid, "") for pid in process_ids if pid]
    if len([item for item in ref_exchange_ids if item]) != len(set([item for item in ref_exchange_ids if item])):
        reason_counts["duplicate_reference_flow"] += 1
        evidences.append({"category": "duplicate_reference_flow", "reference_exchange_ids": ref_exchange_ids})
    if suspect_cycles:
        reason_counts["unresolved_provider_cycle"] += len(suspect_cycles)
        for cycle in suspect_cycles:
            evidences.append({"category": "unresolved_provider_cycle", "cycle": cycle})

    return {
        "invertible": invertible,
        "rank": rank,
        "determinant": det,
        "a_matrix_preview": {
            "rows": n,
            "cols": n,
            "non_zero_count": len(non_zero_entries),
            "non_zero_entries": non_zero_entries[:2000],
        },
        "suspect_cycles": suspect_cycles[:100],
        "singular_reasons": reason_counts,
        "evidence": evidences[:2000],
    }


def run_solver_and_persist(
    *,
    payload: RunRequest,
    db: Session,
) -> tuple[str, str, dict, dict]:
    # Harden product flags at backend entry to avoid stale/null frontend payloads.
    normalize_graph_product_flags(payload.graph)
    normalize_same_flow_uuid_opposite_direction_ports(payload.graph)

    unit_rows = db.query(UnitDefinition).all()
    unit_factor_by_group_and_name: dict[tuple[str, str], float] = {}
    reference_unit_by_group: dict[str, str] = {}
    for row in unit_rows:
        unit_factor_by_group_and_name[(row.unit_group, row.unit_name)] = float(row.factor_to_reference)
        if row.is_reference and row.unit_group not in reference_unit_by_group:
            reference_unit_by_group[row.unit_group] = row.unit_name
    for group in db.query(UnitGroup).all():
        if group.reference_unit and group.name not in reference_unit_by_group:
            reference_unit_by_group[group.name] = group.reference_unit

    display_process_unit_map = _build_process_unit_map_from_graph(payload.graph)
    flow_type_by_uuid = _solver_flow_type_by_uuid_cached(db)

    normalized_graph = normalize_graph_units_to_reference(
        payload.graph,
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
        reference_unit_by_group=reference_unit_by_group,
    )

    try:
        tiangong_like = to_tiangong_like(normalized_graph, flow_type_by_uuid=flow_type_by_uuid)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_DUAL_EDGE_FOR_NORMALIZED_MARKET",
                "message": str(exc),
                "evidence": [{"stage": "to_tiangong_like", "error": str(exc)}],
            },
        ) from exc
    status = "completed"
    message = "Run completed"

    try:
        adapter_result = run_tiangong_lcia(normalized_graph, flow_type_by_uuid=flow_type_by_uuid)
    except Exception as exc:
        try:
            debug_dir = Path(__file__).resolve().parent.parent / "tmp"
            debug_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
            debug_path = debug_dir / f"solver_failed_snapshot_{ts}.json"
            debug_path.write_text(json.dumps(tiangong_like, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        raise HTTPException(status_code=502, detail=f"tiangong solver api failed: {exc}") from exc

    solver_output = adapter_result["solver_output"]
    snapshot_process_unit_map = _build_process_unit_map_from_snapshot(adapter_result.get("tiangong_like_input", {}))
    merged_process_unit_map = dict(snapshot_process_unit_map)
    for pid, meta in display_process_unit_map.items():
        if not isinstance(meta, dict):
            continue
        prev = merged_process_unit_map.get(pid, {})
        merged_process_unit_map[pid] = {
            "reference_flow_uuid": str(meta.get("reference_flow_uuid") or prev.get("reference_flow_uuid") or ""),
            "reference_unit": str(meta.get("reference_unit") or prev.get("reference_unit") or ""),
            "reference_unit_group": str(meta.get("reference_unit_group") or prev.get("reference_unit_group") or ""),
        }

    scaled_values = _rescale_lci_values_to_inventory_units(
        values=solver_output.get("values", []),
        process_index=solver_output.get("process_index", []),
        process_unit_map=merged_process_unit_map,
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
    )
    product_result_index, product_unit_map, product_values = _build_product_result_view_from_graph(
        graph=payload.graph,
        process_index=solver_output.get("process_index", []),
        values=scaled_values,
    )

    solved = {
        "summary": solver_output.get("summary", {}),
        "lci_result": {
            "issues": solver_output.get("issues", []),
            "missing_ef31_flow_uuids": solver_output.get("missing_ef31_flow_uuids", []),
            "missing_ef31_flows": solver_output.get("missing_ef31_flows", []),
            "indicator_index": _enrich_indicator_index_with_units(solver_output.get("indicator_index", [])),
            "process_index": solver_output.get("process_index", []),
            "values": scaled_values,
            "process_unit_map": merged_process_unit_map,
            "product_result_index": product_result_index,
            "product_values": product_values,
            "product_unit_map": product_unit_map,
        },
    }
    tiangong_like = adapter_result["tiangong_like_input"]

    run_job = RunJob(
        model_version_id=payload.model_version_id,
        status=status,
        request_json=payload.graph.model_dump(),
        result_json=solved,
        message=message,
        created_at=datetime.utcnow(),
        finished_at=datetime.utcnow(),
    )
    db.add(run_job)
    db.commit()
    db.refresh(run_job)

    return status, run_job.id, solved, tiangong_like


def upsert_pts_compile_artifact(
    *,
    db: Session,
    project_id: str,
    pts_node_id: str,
    force_recompile: bool,
    compile_result: dict | None = None,
    graph: HybridGraph | None = None,
) -> tuple[PtsCompileArtifact, bool]:
    if compile_result is None:
        if graph is None:
            raise ValueError("graph is required when compile_result is not provided")
        pts_node = next((node for node in graph.nodes if node.id == pts_node_id and node.node_kind == "pts_module"), None)
        pts_uuid = str(pts_node.pts_uuid or pts_node.process_uuid or pts_node.id).strip() if pts_node is not None else ""
        ports_policy = _get_pts_resource_ports_policy(db=db, project_id=project_id, pts_uuid=pts_uuid) if pts_uuid else None
        compile_result = compile_pts(graph, pts_node_id, ports_policy=ports_policy)
    graph_hash = compile_result["graph_hash"]
    cached = (
        db.query(PtsCompileArtifact)
        .filter(
            PtsCompileArtifact.project_id == project_id,
            PtsCompileArtifact.pts_node_id == pts_node_id,
            PtsCompileArtifact.graph_hash == graph_hash,
        )
        .first()
    )
    validation = compile_result["validation"]

    if cached is not None and not force_recompile:
        return cached, True

    if cached is None:
        compile_version = _next_pts_compile_version(
            db=db,
            project_id=project_id,
            pts_uuid=str(compile_result["pts_uuid"]),
        )
        cached = PtsCompileArtifact(
            project_id=project_id,
            pts_node_id=pts_node_id,
            pts_uuid=compile_result["pts_uuid"],
            graph_hash=graph_hash,
            compile_version=compile_version,
            ok=validation.ok,
            matrix_size=validation.matrix_size,
            invertible=validation.invertible,
            errors_json=validation.errors,
            warnings_json=validation.warnings,
            artifact_json=compile_result["artifact"],
        )
        db.add(cached)
    else:
        cached.pts_uuid = compile_result["pts_uuid"]
        cached.graph_hash = graph_hash
        if cached.compile_version is None:
            cached.compile_version = _next_pts_compile_version(
                db=db,
                project_id=project_id,
                pts_uuid=str(compile_result["pts_uuid"]),
            )
        cached.ok = validation.ok
        cached.matrix_size = validation.matrix_size
        cached.invertible = validation.invertible
        cached.errors_json = validation.errors
        cached.warnings_json = validation.warnings
        cached.artifact_json = compile_result["artifact"]

    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == str(compile_result["pts_uuid"])).first()
    if resource is not None:
        resource.project_id = project_id
        resource.pts_node_id = pts_node_id
        resource.compiled_graph_hash = graph_hash
        resource.latest_compile_version = int(cached.compile_version or 0) or resource.latest_compile_version

    db.commit()
    db.refresh(cached)
    return cached, False


def extract_pts_definition(*, graph: HybridGraph, pts_node_id: str, graph_hash: str) -> dict:
    pts_node = next((node for node in graph.nodes if node.id == pts_node_id and node.node_kind == "pts_module"), None)
    if pts_node is None:
        raise ValueError(f"PTS node not found: {pts_node_id}")

    internal_node_ids: list[str] = []
    internal_canvas = _find_pts_internal_canvas(graph, pts_node_id)
    pts_graph: dict = {}
    if isinstance(internal_canvas, dict):
        nodes = internal_canvas.get("nodes")
        if isinstance(nodes, list):
            internal_node_ids = [str(node.get("id")) for node in nodes if isinstance(node, dict) and node.get("id")]
        pts_graph = {
            "functionalUnit": graph.functionalUnit,
            "nodes": nodes if isinstance(nodes, list) else [],
            "exchanges": internal_canvas.get("edges") if isinstance(internal_canvas.get("edges"), list) else [],
            "metadata": {
                "kind": "pts_internal",
                "canvas_id": str(internal_canvas.get("id") or ""),
                "parentPtsNodeId": str(internal_canvas.get("parentPtsNodeId") or pts_node.id),
                "name": str(internal_canvas.get("name") or ""),
            },
        }

    product_refs: list[dict] = []
    for port in pts_node.outputs:
        if bool(port.isProduct):
            product_refs.append(
                {
                    "flow_uuid": port.flowUuid,
                    "flow_name": port.name,
                    "unit": port.unit,
                    "direction": "output",
                }
            )

    ports_policy = _normalize_pts_ports_policy_from_graph(
        pts_graph=pts_graph,
        fallback_policy=None,
    )

    return {
        "pts_uuid": pts_node.pts_uuid or pts_node.process_uuid or pts_node_id,
        "pts_node_id": pts_node_id,
        "internal_node_ids": internal_node_ids,
        "product_refs": product_refs,
        "ports_policy": ports_policy,
        "latest_graph_hash": graph_hash,
        "name": str(pts_node.name or ""),
        "shell_node": _build_pts_shell_node_snapshot(pts_node),
        "pts_graph": pts_graph,
    }


def upsert_pts_definition(*, db: Session, project_id: str, definition: dict) -> PtsDefinition:
    pts_uuid = str(definition["pts_uuid"])
    row = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == project_id, PtsDefinition.pts_uuid == pts_uuid)
        .first()
    )
    if row is None:
        row = PtsDefinition(
            project_id=project_id,
            pts_id=pts_uuid,
            pts_uuid=pts_uuid,
            pts_node_id=str(definition["pts_node_id"]),
            internal_node_ids_json=list(definition.get("internal_node_ids", [])),
            product_refs_json=list(definition.get("product_refs", [])),
            ports_policy_json=dict(definition.get("ports_policy", {})),
            latest_graph_hash=str(definition.get("latest_graph_hash") or ""),
            definition_json=dict(definition),
        )
        db.add(row)
    else:
        row.pts_id = pts_uuid
        row.pts_uuid = pts_uuid
        row.pts_node_id = str(definition["pts_node_id"])
        row.internal_node_ids_json = list(definition.get("internal_node_ids", []))
        row.product_refs_json = list(definition.get("product_refs", []))
        row.ports_policy_json = dict(definition.get("ports_policy", {}))
        row.latest_graph_hash = str(definition.get("latest_graph_hash") or "")
        row.definition_json = dict(definition)
    db.commit()
    db.refresh(row)
    return row


def _slim_exchange_row(item: dict | None, fallback_direction: str, include_amount: bool) -> dict | None:
    if not isinstance(item, dict):
        return None
    raw_direction = str(item.get("direction") or "").lower()
    direction = raw_direction if raw_direction in {"input", "output"} else fallback_direction
    result = {
        "flowUuid": str(item.get("flowUuid") or ""),
        "name": str(item.get("name") or ""),
        "flow_name_en": str(item.get("flow_name_en") or item.get("flowNameEn") or ""),
        "unit": str(item.get("unit") or ""),
        "unitGroup": str(item.get("unitGroup") or ""),
        "sourceProcessUuid": str(item.get("source_process_uuid") or item.get("sourceProcessUuid") or ""),
        "sourceProcessName": str(item.get("source_process_name") or item.get("sourceProcessName") or ""),
        "sourceNodeId": str(item.get("source_node_id") or item.get("sourceNodeId") or ""),
        "direction": direction,
        "isProduct": bool(item.get("isProduct") if "isProduct" in item else item.get("is_product")),
        "internalExposed": item.get("internalExposed", item.get("internal_exposed")),
        "showOnNode": bool(item.get("showOnNode", item.get("show_on_node", False))),
        "product_key": str(item.get("product_key") or ""),
        "port_key": str(item.get("port_key") or ""),
        "display_name": str(item.get("display_name") or ""),
        "reference_product_flow_uuid": str(item.get("reference_product_flow_uuid") or ""),
        "product_name": str(item.get("product_name") or ""),
    }
    if include_amount:
        result["amount"] = float(item.get("amount") or 0.0)
    return result


def _enrich_frontend_ports_flow_name_en(frontend_ports: dict, *, db: Session) -> None:
    if not isinstance(frontend_ports, dict):
        return
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return
    for bucket in ("inputs", "outputs"):
        rows = frontend_ports.get(bucket) if isinstance(frontend_ports.get(bucket), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("flow_name_en") or row.get("flowNameEn") or "").strip():
                continue
            flow_uuid = str(row.get("flowUuid") or "").strip()
            if not flow_uuid:
                continue
            flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
            if not flow_name_en:
                continue
            row["flow_name_en"] = flow_name_en


def _enrich_pts_external_payload_flow_name_en(payload: dict, *, db: Session) -> None:
    if not isinstance(payload, dict):
        return
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return

    def _lookup(flow_uuid: object) -> str:
        token = str(flow_uuid or "").strip()
        if not token:
            return ""
        return flow_name_en_by_uuid.get(token) or flow_name_en_by_uuid.get(token.lower()) or ""

    def _enrich_exchange_rows(rows: object) -> None:
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            flow_name_en = str(row.get("flow_name_en") or row.get("flowNameEn") or "").strip() or _lookup(row.get("flowUuid"))
            if flow_name_en:
                row["flow_name_en"] = flow_name_en
                row.setdefault("flowNameEn", flow_name_en)
            product_name_en = str(row.get("product_name_en") or row.get("productNameEn") or "").strip()
            if not product_name_en and flow_name_en:
                row["product_name_en"] = flow_name_en

    external_boundary = payload.get("external_boundary") if isinstance(payload.get("external_boundary"), dict) else {}
    _enrich_exchange_rows(external_boundary.get("inputs"))
    _enrich_exchange_rows(external_boundary.get("outputs"))
    _enrich_exchange_rows(external_boundary.get("elementary"))

    virtual_processes = payload.get("virtual_processes")
    if isinstance(virtual_processes, list):
        for vp in virtual_processes:
            if not isinstance(vp, dict):
                continue
            ref = vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else None
            ref_flow_uuid = ""
            ref_flow_name_en = ""
            if ref is not None:
                ref_flow_uuid = str(ref.get("flowUuid") or "").strip()
                ref_flow_name_en = str(ref.get("flow_name_en") or ref.get("flowNameEn") or "").strip() or _lookup(ref_flow_uuid)
                if ref_flow_name_en:
                    ref["flow_name_en"] = ref_flow_name_en
                    ref.setdefault("flowNameEn", ref_flow_name_en)
                    ref.setdefault("name_en", ref_flow_name_en)
            if ref_flow_name_en:
                vp.setdefault("product_name_en", ref_flow_name_en)
            _enrich_exchange_rows(vp.get("outputs"))
            _enrich_exchange_rows(vp.get("technosphere_inputs"))
            _enrich_exchange_rows(vp.get("elementary_flows"))

    frontend_ports = payload.get("frontend_ports")
    if isinstance(frontend_ports, dict):
        _enrich_frontend_ports_flow_name_en(frontend_ports, db=db)
        for bucket in ("inputs", "outputs"):
            rows = frontend_ports.get(bucket) if isinstance(frontend_ports.get(bucket), list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                flow_name_en = str(row.get("flow_name_en") or row.get("flowNameEn") or "").strip()
                if not flow_name_en:
                    continue
                base_name_en = str(row.get("product_name_en") or row.get("productNameEn") or "").strip() or flow_name_en
                row.setdefault("product_name_en", base_name_en)
                source_name = str(row.get("sourceProcessName") or row.get("source_process_name") or "").strip()
                if source_name:
                    row.setdefault("display_name_en", f"{base_name_en} @ {source_name}")
                else:
                    row.setdefault("display_name_en", base_name_en)


def _build_frontend_ports_from_external_payload(payload: dict) -> dict:
    pts_uuid = str(payload.get("pts_uuid") or payload.get("ptsUuid") or "").strip() if isinstance(payload, dict) else ""
    external_boundary = payload.get("external_boundary") if isinstance(payload, dict) else {}
    boundary_inputs_raw = external_boundary.get("inputs") if isinstance(external_boundary, dict) else []
    boundary_outputs_raw = external_boundary.get("outputs") if isinstance(external_boundary, dict) else []
    virtuals_raw = payload.get("virtual_processes") if isinstance(payload, dict) else []

    boundary_inputs = [row for row in (boundary_inputs_raw if isinstance(boundary_inputs_raw, list) else []) if isinstance(row, dict)]
    boundary_outputs = [row for row in (boundary_outputs_raw if isinstance(boundary_outputs_raw, list) else []) if isinstance(row, dict)]
    virtuals = [row for row in (virtuals_raw if isinstance(virtuals_raw, list) else []) if isinstance(row, dict)]

    vp_by_flow: dict[str, list[dict]] = {}
    for vp in virtuals:
        ref = vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else {}
        flow_uuid = str(ref.get("flowUuid") or "")
        if not flow_uuid:
            continue
        source_process_uuid = str(vp.get("source_process_uuid") or vp.get("sourceProcessUuid") or "")
        source_process_name = str(vp.get("source_process_name") or vp.get("sourceProcessName") or "")
        source_node_id = str(vp.get("source_node_id") or vp.get("sourceNodeId") or "")
        process_name = str(vp.get("process_name") or "")
        if not source_process_name and "@" in process_name:
            source_process_name = process_name.split("@", 1)[1].strip()
        vp_by_flow.setdefault(flow_uuid, []).append(
            {
                "sourceProcessUuid": source_process_uuid,
                "sourceProcessName": source_process_name,
                "sourceNodeId": source_node_id,
            }
        )

    outputs: list[dict] = []
    for row in boundary_outputs:
        flow_uuid = str(row.get("flowUuid") or "")
        base_name = str(row.get("name") or flow_uuid or "")
        source_process_uuid = str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or "")
        source_process_name = str(row.get("sourceProcessName") or row.get("source_process_name") or "")
        source_node_id = str(row.get("sourceNodeId") or row.get("source_node_id") or "")

        if source_process_uuid or source_node_id:
            if not source_process_name and flow_uuid:
                for candidate in vp_by_flow.get(flow_uuid, []):
                    if str(candidate.get("sourceProcessUuid") or "") == source_process_uuid:
                        source_process_name = str(candidate.get("sourceProcessName") or "")
                        break
            display_source = source_process_name or source_process_uuid
            display_name = f"{base_name} @ {display_source}" if display_source else base_name
            normalized = dict(row)
            normalized["sourceProcessUuid"] = source_process_uuid
            normalized["sourceProcessName"] = source_process_name
            normalized["sourceNodeId"] = source_node_id
            normalized["product_key"] = str(row.get("product_key") or "") or (
                f"{source_process_uuid}::{flow_uuid}" if source_process_uuid else flow_uuid
            )
            normalized["port_key"] = str(row.get("port_key") or "") or str(normalized["product_key"] or "")
            normalized["display_name"] = str(row.get("display_name") or "") or display_name
            outputs.append(normalized)
            continue

        candidates = vp_by_flow.get(flow_uuid, [])
        if len(candidates) <= 1:
            candidate = candidates[0] if candidates else {}
            candidate_source = str(candidate.get("sourceProcessUuid") or "")
            candidate_source_name = str(candidate.get("sourceProcessName") or "")
            candidate_node = str(candidate.get("sourceNodeId") or "")
            display_source = candidate_source_name or candidate_source
            normalized = dict(row)
            normalized["sourceProcessUuid"] = candidate_source
            normalized["sourceProcessName"] = candidate_source_name
            normalized["sourceNodeId"] = candidate_node
            normalized["product_key"] = str(row.get("product_key") or "") or (
                f"{candidate_source}::{flow_uuid}" if candidate_source else flow_uuid
            )
            normalized["port_key"] = str(row.get("port_key") or "") or str(normalized["product_key"] or "")
            normalized["display_name"] = (
                str(row.get("display_name") or "") or (f"{base_name} @ {display_source}" if display_source else base_name)
            )
            outputs.append(normalized)
            continue

        for candidate in candidates:
            candidate_source = str(candidate.get("sourceProcessUuid") or "")
            candidate_source_name = str(candidate.get("sourceProcessName") or "")
            candidate_node = str(candidate.get("sourceNodeId") or "")
            display_source = candidate_source_name or candidate_source
            normalized = dict(row)
            normalized["sourceProcessUuid"] = candidate_source
            normalized["sourceProcessName"] = candidate_source_name
            normalized["sourceNodeId"] = candidate_node
            normalized["product_key"] = f"{candidate_source}::{flow_uuid}" if candidate_source else flow_uuid
            normalized["port_key"] = str(normalized["product_key"] or "")
            normalized["display_name"] = f"{base_name} @ {display_source}" if display_source else base_name
            outputs.append(normalized)

    deduped_outputs: list[dict] = []
    seen_output_keys: set[tuple[str, str, str, str]] = set()
    for row in outputs:
        dedupe_key = (
            str(row.get("flowUuid") or ""),
            str(row.get("sourceProcessUuid") or ""),
            str(row.get("sourceNodeId") or ""),
            str(row.get("port_key") or row.get("product_key") or ""),
        )
        if dedupe_key in seen_output_keys:
            continue
        seen_output_keys.add(dedupe_key)
        deduped_outputs.append(row)

    outputs_by_flow: dict[str, list[dict]] = {}
    for row in deduped_outputs:
        flow_uuid = str(row.get("flowUuid") or "")
        if not flow_uuid:
            continue
        outputs_by_flow.setdefault(flow_uuid, []).append(row)
    for flow_uuid, rows in outputs_by_flow.items():
        if len(rows) <= 1:
            continue
        missing_binding = [
            row
            for row in rows
            if not str(row.get("sourceProcessUuid") or "").strip() and not str(row.get("sourceNodeId") or "").strip()
        ]
        if missing_binding:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "INVALID_PORTS_OUTPUT_SOURCE_BINDING",
                    "message": f"ports.outputs contains duplicated flowUuid without source binding: flowUuid={flow_uuid}",
                    "evidence": missing_binding,
                },
            )

    return {
        "inputs": [
            {
                **row,
                "pts_uuid": pts_uuid,
                "id": str(row.get("id") or _stable_shell_port_id(row={**row, "pts_uuid": pts_uuid}, idx=idx, direction="input", flow_type="technosphere")),
                "port_key": str(row.get("port_key") or f"in::{str(row.get('flowUuid') or '')}::{idx}"),
                "display_name": str(row.get("display_name") or row.get("name") or row.get("flowUuid") or ""),
            }
            for idx, row in enumerate(boundary_inputs, start=1)
        ],
        "outputs": [
            {
                **row,
                "pts_uuid": pts_uuid,
                "id": str(
                    row.get("id")
                    or _stable_shell_port_id(
                        row={**row, "pts_uuid": pts_uuid},
                        idx=idx,
                        direction="output",
                        flow_type="technosphere",
                    )
                ),
            }
            for idx, row in enumerate(deduped_outputs, start=1)
        ],
    }


def _external_row_to_flow_port(row: dict, *, idx: int, direction: str, flow_type: str) -> dict:
    port_id = _stable_shell_port_id(row=row, idx=idx, direction=direction, flow_type=flow_type)
    legacy_port_id = _legacy_shell_port_id(row=row, idx=idx, direction=direction, flow_type=flow_type)
    return {
        "id": port_id,
        "legacyPortId": legacy_port_id,
        "flowUuid": str(row.get("flowUuid") or ""),
        "name": str(row.get("display_name") or row.get("name") or row.get("flowUuid") or ""),
        "flow_name_en": str(row.get("flow_name_en") or row.get("flowNameEn") or ""),
        "display_name_en": str(row.get("display_name_en") or row.get("displayNameEn") or ""),
        "unit": str(row.get("unit") or ""),
        "unitGroup": row.get("unitGroup"),
        "amount": float(row.get("amount") or 0.0),
        "externalSaleAmount": float(row.get("externalSaleAmount") or 0.0),
        "type": flow_type,
        "direction": direction,
        "showOnNode": bool(row.get("showOnNode", True)),
        "internalExposed": row.get("internalExposed"),
        "dbMapping": row.get("dbMapping"),
        "sourceProcessUuid": str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or ""),
        "sourceProcessName": str(row.get("sourceProcessName") or row.get("source_process_name") or ""),
        "sourceNodeId": str(row.get("sourceNodeId") or row.get("source_node_id") or ""),
        "isProduct": bool(row.get("isProduct", False)),
        "allocationFactor": row.get("allocationFactor"),
        "product_key": str(row.get("product_key") or ""),
        "port_key": str(row.get("port_key") or row.get("product_key") or ""),
        "reference_product_flow_uuid": str(row.get("reference_product_flow_uuid") or ""),
        "product_name": str(row.get("product_name") or ""),
        "product_name_en": str(row.get("product_name_en") or row.get("productNameEn") or ""),
    }


def _legacy_shell_port_id(*, row: dict, idx: int, direction: str, flow_type: str) -> str:
    if direction == "input":
        return str(row.get("port_key") or row.get("id") or f"in::{str(row.get('flowUuid') or '')}::{idx}")
    return str(row.get("port_key") or row.get("product_key") or row.get("id") or f"{flow_type}_{idx}")


def _stable_shell_port_id(*, row: dict, idx: int, direction: str, flow_type: str) -> str:
    legacy_port_id = _legacy_shell_port_id(row=row, idx=idx, direction=direction, flow_type=flow_type)
    pts_uuid = str(row.get("pts_uuid") or row.get("ptsUuid") or "").strip()
    digest = hashlib.sha256(f"{pts_uuid}|{direction}|{legacy_port_id}".encode("utf-8")).hexdigest()[:16]
    prefix = "ptsin" if direction == "input" else "ptsout"
    return f"{prefix}_{digest}"


def _build_pts_port_id_map(*, shell_node: dict) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for port in list(shell_node.get("inputs") or []) + list(shell_node.get("outputs") or []):
        if not isinstance(port, dict):
            continue
        legacy_port_id = str(port.get("legacyPortId") or port.get("legacy_port_id") or "").strip()
        port_id = str(port.get("id") or "").strip()
        if legacy_port_id and port_id:
            mapping[legacy_port_id] = port_id
    return mapping


def _resolve_default_visible_port_ids(
    *,
    shell_node: dict,
    hints: list[PtsBoundaryPortHint] | None,
) -> list[str]:
    if not isinstance(shell_node, dict) or not hints:
        return []

    buckets = {
        "input": list(shell_node.get("inputs") or []),
        "output": list(shell_node.get("outputs") or []),
    }
    matched_ids: list[str] = []
    seen_ids: set[str] = set()

    for hint in hints:
        if not isinstance(hint, PtsBoundaryPortHint):
            continue
        direction = str(hint.direction or "").strip()
        flow_uuid = str(hint.flow_uuid or "").strip()
        if direction not in buckets or not flow_uuid:
            continue

        candidates = [
            port
            for port in buckets[direction]
            if isinstance(port, dict) and str(port.get("flowUuid") or "").strip() == flow_uuid
        ]
        if not candidates:
            continue

        source_process_uuid = str(hint.source_process_uuid or "").strip()
        source_node_id = str(hint.source_node_id or "").strip()
        source_process_name = str(hint.source_process_name or "").strip()
        hint_name = str(hint.name or "").strip()

        def _unique_match(rows: list[dict], predicate) -> dict | None:
            matched = [row for row in rows if predicate(row)]
            return matched[0] if len(matched) == 1 else None

        chosen: dict | None = None
        if source_process_uuid:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or "").strip() == source_process_uuid,
            )
        if chosen is None and source_node_id:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("sourceNodeId") or row.get("source_node_id") or "").strip() == source_node_id,
            )
        if chosen is None and source_process_name:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("sourceProcessName") or row.get("source_process_name") or "").strip() == source_process_name,
            )
        if chosen is None and hint_name and "@" in hint_name:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("name") or "").strip() == hint_name,
            )
        if chosen is None and len(candidates) == 1:
            chosen = candidates[0]

        if chosen is None:
            continue

        port_id = str(chosen.get("id") or "").strip()
        if port_id and port_id not in seen_ids:
            seen_ids.add(port_id)
            matched_ids.append(port_id)

    return matched_ids


def _apply_default_visible_port_ids_to_shell_node(*, shell_node: dict, default_visible_port_ids: list[str]) -> dict:
    if not isinstance(shell_node, dict):
        return shell_node
    selected_ids = {str(item).strip() for item in default_visible_port_ids if str(item).strip()}
    if not selected_ids:
        return shell_node

    updated = dict(shell_node)
    outputs = []
    for port in list(shell_node.get("outputs") or []):
        if not isinstance(port, dict):
            outputs.append(port)
            continue
        normalized = dict(port)
        port_id = str(normalized.get("id") or "").strip()
        normalized["showOnNode"] = port_id in selected_ids
        outputs.append(normalized)
    updated["outputs"] = outputs
    return updated


def _pts_port_identity_key(row: dict) -> tuple[str, str, str, str, str, str]:
    if not isinstance(row, dict):
        return ("", "", "", "", "", "")
    return (
        str(row.get("flowUuid") or row.get("flow_uuid") or "").strip(),
        str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or "").strip(),
        str(row.get("sourceNodeId") or row.get("source_node_id") or "").strip(),
        str(row.get("sourceProcessName") or row.get("source_process_name") or "").strip(),
        str(row.get("direction") or "").strip(),
        str(row.get("port_key") or row.get("product_key") or "").strip(),
    )


def _apply_default_visible_port_ids_to_external_payload(*, payload: dict, shell_node: dict, default_visible_port_ids: list[str]) -> dict:
    if not isinstance(payload, dict):
        return payload
    selected_ids = {str(item).strip() for item in default_visible_port_ids if str(item).strip()}
    if not selected_ids:
        return payload

    selected_identity_keys = {
        _pts_port_identity_key(port)
        for port in list(shell_node.get("outputs") or [])
        if isinstance(port, dict) and str(port.get("id") or "").strip() in selected_ids
    }

    updated = dict(payload)
    frontend_ports = updated.get("frontend_ports") if isinstance(updated.get("frontend_ports"), dict) else {}
    outputs = frontend_ports.get("outputs") if isinstance(frontend_ports.get("outputs"), list) else []
    normalized_outputs: list[dict] = []
    for row in outputs:
        if not isinstance(row, dict):
            continue
        normalized = dict(row)
        port_id = str(normalized.get("id") or "").strip()
        normalized["showOnNode"] = (
            _pts_port_identity_key(normalized) in selected_identity_keys
            if selected_identity_keys
            else port_id in selected_ids
        )
        normalized_outputs.append(normalized)
    updated["frontend_ports"] = {
        **dict(frontend_ports),
        "outputs": normalized_outputs,
    }
    return updated


def _is_effectively_zero(value: float, *, tolerance: float = 1e-9) -> bool:
    return abs(float(value)) <= tolerance


def _is_total_significantly_below_target(
    actual_total: float,
    expected_total: float,
    *,
    tolerance: float = 1e-6,
) -> bool:
    if expected_total <= 0:
        return False
    return actual_total < (expected_total - tolerance)


def _build_pts_publish_warnings(
    *,
    project_id: str,
    pts_uuid: str,
    pts_node_id: str | None,
    pts_graph: dict | None,
    external_payload: dict | None,
) -> list[PtsModelWarning]:
    _ = project_id
    graph_nodes = list((pts_graph or {}).get("nodes") or []) if isinstance(pts_graph, dict) else []
    if not graph_nodes:
        return []

    warnings: list[PtsModelWarning] = []
    for node in graph_nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("node_kind") or "").strip() != "market_process":
            continue

        source_node_id = str(node.get("id") or "").strip()
        if not source_node_id:
            continue

        raw_inputs = node.get("inputs") if isinstance(node.get("inputs"), list) else []
        actual_total = 0.0
        expected_total = 1.0 if str(node.get("mode") or "").strip() == "normalized" else 0.0
        actual_rows = 0
        evidence: list[dict] = []
        for port in raw_inputs:
            if not isinstance(port, dict):
                continue
            if is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.get("type"))):
                continue
            try:
                amount = float(port.get("amount") or 0.0)
            except (TypeError, ValueError):
                amount = 0.0
            actual_total += amount
            actual_rows += 1
            evidence.append(
                {
                    "flow_uuid": str(port.get("flowUuid") or ""),
                    "flow_name": str(port.get("name") or ""),
                    "unit": str(port.get("unit") or ""),
                    "amount": amount,
                    "source_process_uuid": str(port.get("sourceProcessUuid") or port.get("source_process_uuid") or ""),
                    "source_process_name": str(port.get("sourceProcessName") or port.get("source_process_name") or ""),
                    "source_node_id": str(port.get("sourceNodeId") or port.get("source_node_id") or ""),
                }
            )

        if actual_rows <= 0 and _is_effectively_zero(actual_total):
            warning_code = "PTS_INPUT_SHARE_ZERO"
        elif _is_effectively_zero(actual_total):
            warning_code = "PTS_INPUT_SHARE_ZERO"
        elif expected_total > 0 and _is_total_significantly_below_target(actual_total, expected_total):
            warning_code = "PTS_INPUT_SHARE_BELOW_TARGET"
        else:
            continue

        warnings.append(
            PtsModelWarning(
                code=warning_code,
                severity="warning",
                message=f"Input share total is {actual_total:.6f} (target {expected_total:g})",
                pts_uuid=pts_uuid,
                pts_node_id=pts_node_id or source_node_id or None,
                node_name=str(node.get("name") or "").strip() or None,
                expected_total=expected_total,
                actual_total=actual_total,
                evidence=evidence[:50],
            )
        )

    return warnings


def _load_pts_external_artifact(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
    published_version: int | None = None,
) -> PtsExternalArtifact | None:
    query = db.query(PtsExternalArtifact).filter(
        PtsExternalArtifact.project_id == project_id,
        PtsExternalArtifact.pts_uuid == pts_uuid,
    )
    if published_version is not None:
        query = query.filter(PtsExternalArtifact.published_version == published_version)
    return query.order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc()).first()


def _load_pts_active_external_artifact(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
) -> PtsExternalArtifact | None:
    resource = db.query(PtsResource).filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == pts_uuid).first()
    if resource is not None and resource.active_published_version is not None:
        active = _load_pts_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
            published_version=int(resource.active_published_version),
        )
        if active is not None:
            return active
    if resource is not None and resource.latest_published_version is not None:
        latest = _load_pts_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
            published_version=int(resource.latest_published_version),
        )
        if latest is not None:
            return latest
    return (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.published_version.desc(), PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .first()
    )


def _build_projected_pts_ports_from_external(external: PtsExternalArtifact) -> tuple[list[FlowPort], list[FlowPort]]:
    payload = external.artifact_json if isinstance(external.artifact_json, dict) else {}
    frontend_ports = payload.get("frontend_ports") if isinstance(payload.get("frontend_ports"), dict) else None
    if not isinstance(frontend_ports, dict):
        frontend_ports = _build_frontend_ports_from_external_payload(payload)
    inputs_rows = frontend_ports.get("inputs") if isinstance(frontend_ports.get("inputs"), list) else []
    outputs_rows = frontend_ports.get("outputs") if isinstance(frontend_ports.get("outputs"), list) else []
    projected_inputs = [
        FlowPort.model_validate(
            _external_row_to_flow_port(
                {**item, "pts_uuid": str(external.pts_uuid or "")},
                idx=idx,
                direction="input",
                flow_type="technosphere",
            )
        )
        for idx, item in enumerate(inputs_rows, start=1)
        if isinstance(item, dict)
    ]
    projected_outputs = [
        FlowPort.model_validate(
            _external_row_to_flow_port(
                {**item, "pts_uuid": str(external.pts_uuid or "")},
                idx=idx,
                direction="output",
                flow_type="technosphere",
            )
        )
        for idx, item in enumerate(outputs_rows, start=1)
        if isinstance(item, dict)
    ]
    return projected_inputs, projected_outputs


def _flow_port_projection_signature(port: FlowPort | dict) -> tuple:
    data = port.model_dump(mode="python", by_alias=True) if isinstance(port, FlowPort) else dict(port)
    return (
        str(data.get("id") or ""),
        str(data.get("flowUuid") or data.get("flow_uuid") or ""),
        str(data.get("type") or ""),
        str(data.get("direction") or ""),
        bool(data.get("internalExposed") if "internalExposed" in data else data.get("internal_exposed")),
        bool(data.get("showOnNode")),
        bool(data.get("isProduct") if "isProduct" in data else data.get("is_product")),
        str(data.get("sourceProcessUuid") or data.get("source_process_uuid") or ""),
        str(data.get("sourceNodeId") or data.get("source_node_id") or ""),
    )


def _is_unpublished_empty_pts_draft(*, graph: HybridGraph, pts_node: HybridNode) -> bool:
    internal_canvas = _find_pts_internal_canvas(graph, pts_node.id)
    if internal_canvas is None:
        return False
    internal_nodes = internal_canvas.get("nodes") if isinstance(internal_canvas.get("nodes"), list) else []
    internal_edges = internal_canvas.get("edges") if isinstance(internal_canvas.get("edges"), list) else []
    return len(internal_nodes) == 0 and len(internal_edges) == 0


def _validate_pts_nodes_are_synced_to_active_publish(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        external = _load_pts_active_external_artifact(db=db, project_id=project_id, pts_uuid=pts_uuid)
        if external is None:
            if _is_unpublished_empty_pts_draft(graph=graph, pts_node=node):
                continue
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PTS_PUBLISHED_ARTIFACT_REQUIRED_FOR_MAIN_GRAPH_SAVE",
                    "message": (
                        f"PTS {pts_uuid} has no published artifact. Publish the PTS first, then refresh the main graph before saving."
                    ),
                    "evidence": [
                        {
                            "project_id": project_id,
                            "pts_uuid": pts_uuid,
                            "pts_node_id": node.id,
                        }
                    ],
                },
            )
        expected_inputs, expected_outputs = _build_projected_pts_ports_from_external(external)
        actual_inputs = [_flow_port_projection_signature(port) for port in node.inputs]
        actual_outputs = [_flow_port_projection_signature(port) for port in node.outputs]
        expected_input_sig = [_flow_port_projection_signature(port) for port in expected_inputs]
        expected_output_sig = [_flow_port_projection_signature(port) for port in expected_outputs]
        if actual_inputs != expected_input_sig or actual_outputs != expected_output_sig:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PTS_MAIN_GRAPH_STALE",
                    "message": (
                        f"PTS node {pts_uuid} is not synced to the latest published artifact. Refresh/update the PTS node in the main graph before saving."
                    ),
                    "evidence": [
                        {
                            "project_id": project_id,
                            "pts_uuid": pts_uuid,
                            "pts_node_id": node.id,
                            "active_published_version": int(external.published_version or 0) if external.published_version is not None else None,
                            "active_published_artifact_id": str(external.id),
                            "actual_input_count": len(actual_inputs),
                            "expected_input_count": len(expected_input_sig),
                            "actual_output_count": len(actual_outputs),
                            "expected_output_count": len(expected_output_sig),
                            "actual_outputs_preview": [str(port.name or "") for port in node.outputs[:5]],
                            "expected_outputs_preview": [str(port.name or "") for port in expected_outputs[:5]],
                        }
                    ],
                },
            )


def _project_pts_external_ports_into_graph(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    pts_nodes = [node for node in graph.nodes if node.node_kind == "pts_module"]
    if not pts_nodes:
        return
    for node in pts_nodes:
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        external = _load_pts_active_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
        )
        if external is None:
            continue
        node.pts_published_version = int(external.published_version) if external.published_version is not None else node.pts_published_version
        node.pts_published_artifact_id = str(external.id)
        projected_inputs, projected_outputs = _build_projected_pts_ports_from_external(external)
        connected_port_ids = _collect_connected_port_ids_for_pts_node(graph=graph, node_id=str(node.id))
        node.inputs = _overlay_pts_port_visibility(
            submitted_ports=list(node.inputs),
            projected_ports=projected_inputs,
            connected_port_ids=connected_port_ids,
        )
        node.outputs = _overlay_pts_port_visibility(
            submitted_ports=list(node.outputs),
            projected_ports=projected_outputs,
            connected_port_ids=connected_port_ids,
        )
        node.emissions = []


def _flow_port_identity_key(port: FlowPort | dict) -> tuple[str, str, str, str]:
    data = port.model_dump(mode="python", by_alias=True) if isinstance(port, FlowPort) else dict(port)
    return (
        str(data.get("flowUuid") or data.get("flow_uuid") or "").strip(),
        str(data.get("direction") or "").strip(),
        str(data.get("sourceProcessUuid") or data.get("source_process_uuid") or "").strip(),
        str(data.get("sourceNodeId") or data.get("source_node_id") or "").strip(),
    )


def _collect_connected_port_ids_for_pts_node(*, graph: HybridGraph, node_id: str) -> set[str]:
    connected: set[str] = set()
    for edge in graph.exchanges:
        if str(edge.fromNode or "") == node_id:
            port_id = _port_id_from_handle(edge.source_port_id or edge.sourceHandle, "out")
            if port_id:
                connected.add(port_id)
        if str(edge.toNode or "") == node_id:
            port_id = _port_id_from_handle(edge.target_port_id or edge.targetHandle, "in")
            if port_id:
                connected.add(port_id)
    return connected


def _overlay_pts_port_visibility(
    *,
    submitted_ports: list[FlowPort],
    projected_ports: list[FlowPort],
    connected_port_ids: set[str],
) -> list[FlowPort]:
    submitted_by_id: dict[str, FlowPort] = {}
    submitted_by_key: dict[tuple[str, str, str, str], FlowPort] = {}
    submitted_by_flow_direction: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    for port in submitted_ports:
        if str(port.id or "").strip():
            submitted_by_id[str(port.id)] = port
        submitted_by_key[_flow_port_identity_key(port)] = port
        flow_uuid = str(port.flowUuid or "").strip()
        direction = str(port.direction or "").strip()
        if flow_uuid and direction:
            submitted_by_flow_direction[(flow_uuid, direction)].append(port)

    projected_by_flow_direction: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    for port in projected_ports:
        flow_uuid = str(port.flowUuid or "").strip()
        direction = str(port.direction or "").strip()
        if flow_uuid and direction:
            projected_by_flow_direction[(flow_uuid, direction)].append(port)

    merged: list[FlowPort] = []
    for port in projected_ports:
        submitted = submitted_by_id.get(str(port.id)) or submitted_by_key.get(_flow_port_identity_key(port))
        if submitted is None:
            flow_uuid = str(port.flowUuid or "").strip()
            direction = str(port.direction or "").strip()
            candidates = submitted_by_flow_direction.get((flow_uuid, direction), [])
            projected_candidates = projected_by_flow_direction.get((flow_uuid, direction), [])
            if len(candidates) == 1 and len(projected_candidates) == 1:
                submitted = candidates[0]
        show_on_node = bool(port.showOnNode)
        if submitted is not None:
            show_on_node = bool(submitted.showOnNode)
        if str(port.id or "") in connected_port_ids:
            show_on_node = True
        merged.append(port.model_copy(update={"showOnNode": show_on_node}))
    return merged


def _canonicalize_pts_nodes_for_main_graph_save(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        submitted_pts_uuid = str(node.pts_uuid or "").strip()
        submitted_process_uuid = str(node.process_uuid or "").strip()
        submitted_node_id = str(node.id or "").strip()
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        external = _load_pts_active_external_artifact(db=db, project_id=project_id, pts_uuid=pts_uuid)
        if external is None:
            if _is_unpublished_empty_pts_draft(graph=graph, pts_node=node):
                _upsert_pts_resource_from_graph(db=db, project_id=project_id, graph=graph, pts_node=node)
                node.pts_published_version = None
                node.pts_published_artifact_id = None
                node.emissions = []
                continue
            submitted_pts_resource = (
                db.query(PtsResource)
                .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == submitted_pts_uuid)
                .first()
                if submitted_pts_uuid
                else None
            )
            submitted_process_resource = (
                db.query(PtsResource)
                .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == submitted_process_uuid)
                .first()
                if submitted_process_uuid and submitted_process_uuid != submitted_pts_uuid
                else None
            )
            submitted_node_resource = (
                db.query(PtsResource)
                .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == submitted_node_id)
                .first()
                if submitted_node_id and submitted_node_id not in {submitted_pts_uuid, submitted_process_uuid}
                else None
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PTS_PUBLISHED_ARTIFACT_REQUIRED_FOR_MAIN_GRAPH_SAVE",
                    "message": (
                        f"PTS {pts_uuid} has no published artifact. Publish the PTS first, then save the main graph."
                    ),
                    "evidence": [
                        {
                            "project_id": project_id,
                            "pts_uuid": pts_uuid,
                            "pts_node_id": node.id,
                            "node_name": str(node.name or "").strip() or None,
                            "submitted_pts_uuid": submitted_pts_uuid or None,
                            "submitted_process_uuid": submitted_process_uuid or None,
                            "submitted_node_id": submitted_node_id or None,
                            "lookup_basis": pts_uuid,
                            "submitted_pts_uuid_lookup_hit": submitted_pts_resource is not None,
                            "submitted_process_uuid_lookup_hit": submitted_process_resource is not None,
                            "submitted_node_id_lookup_hit": submitted_node_resource is not None,
                            "submitted_pts_uuid_active_published_version": (
                                int(submitted_pts_resource.active_published_version)
                                if submitted_pts_resource is not None
                                and submitted_pts_resource.active_published_version is not None
                                else None
                            ),
                            "submitted_process_uuid_active_published_version": (
                                int(submitted_process_resource.active_published_version)
                                if submitted_process_resource is not None
                                and submitted_process_resource.active_published_version is not None
                                else None
                            ),
                            "submitted_node_id_active_published_version": (
                                int(submitted_node_resource.active_published_version)
                                if submitted_node_resource is not None
                                and submitted_node_resource.active_published_version is not None
                                else None
                            ),
                        }
                    ],
                },
            )
        projected_inputs, projected_outputs = _build_projected_pts_ports_from_external(external)
        connected_port_ids = _collect_connected_port_ids_for_pts_node(graph=graph, node_id=str(node.id))
        node.pts_published_version = (
            int(external.published_version) if external.published_version is not None else node.pts_published_version
        )
        node.pts_published_artifact_id = str(external.id)
        node.inputs = _overlay_pts_port_visibility(
            submitted_ports=list(node.inputs),
            projected_ports=projected_inputs,
            connected_port_ids=connected_port_ids,
        )
        node.outputs = _overlay_pts_port_visibility(
            submitted_ports=list(node.outputs),
            projected_ports=projected_outputs,
            connected_port_ids=connected_port_ids,
        )
        node.emissions = []


def build_pts_external_payload(*, project_id: str, pts_uuid: str, definition: dict, compile_row: PtsCompileArtifact) -> dict:
    artifact = compile_row.artifact_json or {}
    virtual_processes_raw = artifact.get("virtual_processes") if isinstance(artifact, dict) else []
    ports_policy = definition.get("ports_policy") if isinstance(definition.get("ports_policy"), dict) else {}
    policy_inputs = ports_policy.get("inputs") if isinstance(ports_policy.get("inputs"), list) else []
    policy_outputs = ports_policy.get("outputs") if isinstance(ports_policy.get("outputs"), list) else []

    def _policy_flow_uuid(row: dict) -> str:
        return str(row.get("flow_uuid") or row.get("flowUuid") or "").strip()

    def _policy_is_exposed(row: dict) -> bool:
        return bool(
            row.get("internal_exposed")
            or row.get("internalExposed")
            or row.get("show_on_node")
            or row.get("showOnNode")
        )

    def _policy_key(row: dict) -> tuple[str, str, str]:
        return (
            _policy_flow_uuid(row),
            str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
        )

    def _policy_allowed(rows: list[dict]) -> set[tuple[str, str, str]]:
        allowed: set[tuple[str, str, str]] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            if not _policy_is_exposed(row):
                continue
            allowed.add(_policy_key(row))
        return allowed

    exposed_input_policies = [
        row
        for row in policy_inputs
        if isinstance(row, dict)
        and not bool(row.get("is_product") or row.get("isProduct"))
        and _policy_is_exposed(row)
    ]
    allowed_input_keys = _policy_allowed(exposed_input_policies)
    allowed_input_flow_uuids = {_policy_flow_uuid(row) for row in exposed_input_policies if _policy_flow_uuid(row)}
    preferred_input_policy_by_flow: dict[str, dict] = {}
    for row in exposed_input_policies:
        flow_uuid = _policy_flow_uuid(row)
        if flow_uuid and flow_uuid not in preferred_input_policy_by_flow:
            preferred_input_policy_by_flow[flow_uuid] = row
    exposed_output_policies = [
        row
        for row in policy_outputs
        if isinstance(row, dict)
        and bool(row.get("is_product") or row.get("isProduct"))
        and _policy_is_exposed(row)
    ]

    def _boundary_rows(rows: object, fallback_direction: str) -> list[dict]:
        source = rows if isinstance(rows, list) else []
        return [
            row
            for row in (
                _slim_exchange_row(item if isinstance(item, dict) else None, fallback_direction, False)
                for item in source
            )
            if row is not None
        ]

    all_virtual_processes: list[dict] = []
    for idx, vp in enumerate((virtual_processes_raw if isinstance(virtual_processes_raw, list) else []), start=1):
        vp_obj = vp if isinstance(vp, dict) else {}
        ref_obj = vp_obj.get("reference_product") if isinstance(vp_obj.get("reference_product"), dict) else {}
        inputs_raw = vp_obj.get("technosphere_inputs") if isinstance(vp_obj, dict) else None
        emissions_raw = vp_obj.get("elementary_flows") if isinstance(vp_obj, dict) else None
        process_uuid = str(vp_obj.get("process_uuid") or "")
        flow_uuid = str(ref_obj.get("flowUuid") or "")
        technosphere_inputs = [
            row
            for row in (
                _slim_exchange_row(item if isinstance(item, dict) else None, "input", True)
                for item in (inputs_raw if isinstance(inputs_raw, list) else [])
            )
            if row is not None
        ]
        elementary_flows = [
            row
            for row in (
                _slim_exchange_row(item if isinstance(item, dict) else None, "output", True)
                for item in (emissions_raw if isinstance(emissions_raw, list) else [])
            )
            if row is not None
        ]
        all_virtual_processes.append(
            {
                "process_uuid": process_uuid,
                "process_name": str(vp_obj.get("process_name") or ""),
                "product_key": str(vp_obj.get("product_key") or ""),
                "product_name": str(vp_obj.get("product_name") or ref_obj.get("name") or ""),
                "is_product": bool(vp_obj.get("is_product", True)),
                "reference_unit": str(vp_obj.get("reference_unit") or ref_obj.get("unit") or ""),
                "reference_unit_group": (
                    vp_obj.get("reference_unit_group")
                    or ref_obj.get("unitGroup")
                    or None
                ),
                "reference_product_flow_uuid": str(
                    vp_obj.get("reference_product_flow_uuid") or ref_obj.get("flowUuid") or ""
                ),
                "virtual_process_key": f"{process_uuid or 'vp'}::{idx}",
                "source_process_uuid": str(vp_obj.get("source_process_uuid") or vp_obj.get("sourceProcessUuid") or ""),
                "source_process_name": str(vp_obj.get("source_process_name") or vp_obj.get("sourceProcessName") or ""),
                "source_node_id": str(vp_obj.get("source_node_id") or vp_obj.get("sourceNodeId") or ""),
                "source_port_id": str(vp_obj.get("source_port_id") or vp_obj.get("sourcePortId") or ""),
                "source_port_name": str(vp_obj.get("source_port_name") or vp_obj.get("sourcePortName") or ""),
                "reference_product": {
                    "flowUuid": flow_uuid,
                    "name": str(ref_obj.get("name") or ""),
                    "unit": str(ref_obj.get("unit") or ""),
                    "unitGroup": ref_obj.get("unitGroup") if isinstance(ref_obj, dict) else None,
                },
                "outputs": [
                    dict(item)
                    for item in (vp_obj.get("outputs") if isinstance(vp_obj.get("outputs"), list) else [])
                    if isinstance(item, dict)
                ],
                "technosphere_inputs": technosphere_inputs,
                "elementary_flows": elementary_flows,
            }
        )

    def _policy_product_key(row: dict) -> str:
        flow_uuid = str(row.get("flow_uuid") or row.get("flowUuid") or "").strip()
        source_process_uuid = str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip()
        return f"{source_process_uuid}::{flow_uuid}" if source_process_uuid and flow_uuid else flow_uuid

    def _match_virtual_process(policy_row: dict) -> dict | None:
        policy_product_key = _policy_product_key(policy_row)
        if policy_product_key:
            exact = [vp for vp in all_virtual_processes if str(vp.get("product_key") or "") == policy_product_key]
            if len(exact) == 1:
                return exact[0]
        flow_uuid = str(policy_row.get("flow_uuid") or policy_row.get("flowUuid") or "").strip()
        source_process_uuid = str(policy_row.get("source_process_uuid") or policy_row.get("sourceProcessUuid") or "").strip()
        source_node_id = str(policy_row.get("source_node_id") or policy_row.get("sourceNodeId") or "").strip()
        candidates = [
            vp for vp in all_virtual_processes
            if str(vp.get("reference_product_flow_uuid") or "") == flow_uuid
        ]
        if source_process_uuid:
            exact = [
                vp for vp in candidates
                if str(vp.get("source_process_uuid") or vp.get("sourceProcessUuid") or "") == source_process_uuid
            ]
            if len(exact) == 1:
                return exact[0]
        if source_node_id:
            exact = [
                vp for vp in candidates
                if str(vp.get("source_node_id") or vp.get("sourceNodeId") or "") == source_node_id
            ]
            if len(exact) == 1:
                return exact[0]
        if len(candidates) == 1:
            return candidates[0]
        return None

    selected_virtual_processes: list[dict] = []
    boundary_outputs: list[dict] = []
    output_virtual_process_bindings: list[dict] = []
    matched_virtual_process_keys: set[str] = set()

    for policy_row in exposed_output_policies:
        vp = _match_virtual_process(policy_row)
        flow_uuid = str(policy_row.get("flow_uuid") or policy_row.get("flowUuid") or "").strip()
        source_process_uuid = str(policy_row.get("source_process_uuid") or policy_row.get("sourceProcessUuid") or "").strip()
        source_process_name = str(policy_row.get("source_process_name") or policy_row.get("sourceProcessName") or "").strip()
        source_node_id = str(policy_row.get("source_node_id") or policy_row.get("sourceNodeId") or "").strip()
        base_name = str(policy_row.get("name") or flow_uuid or "")
        product_key = _policy_product_key(policy_row)
        if vp is None:
            output_virtual_process_bindings.append(
                {
                    "product_key": product_key,
                    "flowUuid": flow_uuid,
                    "virtual_process_key": None,
                    "process_uuid": None,
                }
            )
            continue

        if not source_process_name:
            source_process_name = str(vp.get("source_process_name") or vp.get("sourceProcessName") or "").strip()
        if not source_node_id:
            source_node_id = str(vp.get("source_node_id") or vp.get("sourceNodeId") or "").strip()
        display_source = source_process_name or source_process_uuid
        display_name = f"{base_name} @ {display_source}" if display_source else base_name
        if "@" in base_name and display_source:
            existing_source = base_name.split("@", 1)[1].strip()
            display_name = base_name if existing_source == display_source else f"{base_name} @ {display_source}"

        boundary_outputs.append(
            {
                "flowUuid": flow_uuid,
                "name": base_name,
                "unit": str(
                    vp.get("reference_unit")
                    or ((vp.get("reference_product") or {}).get("unit") if isinstance(vp.get("reference_product"), dict) else "")
                    or ""
                ),
                "unitGroup": (
                    vp.get("reference_unit_group")
                    or ((vp.get("reference_product") or {}).get("unitGroup") if isinstance(vp.get("reference_product"), dict) else None)
                    or ""
                ),
                "sourceProcessUuid": source_process_uuid or str(vp.get("source_process_uuid") or ""),
                "sourceProcessName": source_process_name,
                "sourceNodeId": source_node_id,
                "direction": "output",
                "isProduct": True,
                "internalExposed": True,
                "showOnNode": True,
                "product_key": str(vp.get("product_key") or product_key),
                "port_key": str(vp.get("product_key") or product_key),
                "display_name": display_name,
                "reference_product_flow_uuid": str(vp.get("reference_product_flow_uuid") or flow_uuid),
                "product_name": str(vp.get("product_name") or base_name or flow_uuid),
            }
        )

        process_uuid = str(vp.get("process_uuid") or "")
        virtual_process_key = str(vp.get("virtual_process_key") or str(vp.get("product_key") or product_key))
        if virtual_process_key and virtual_process_key not in matched_virtual_process_keys:
            matched_virtual_process_keys.add(virtual_process_key)
            selected_virtual_processes.append(vp)

        output_virtual_process_bindings.append(
            {
                "product_key": str(vp.get("product_key") or product_key),
                "flowUuid": flow_uuid,
                "virtual_process_key": vp.get("virtual_process_key"),
                "process_uuid": process_uuid,
            }
        )

    def _aggregate_rows(rows: list[dict], *, collapse_source: bool) -> list[dict]:
        grouped: dict[tuple[str, str, str, str, str, str, str], dict] = {}
        for row in rows:
            key = (
                str(row.get("flowUuid") or ""),
                str(row.get("unit") or ""),
                str(row.get("unitGroup") or ""),
                str(row.get("direction") or ""),
                str(row.get("name") or ""),
                "" if collapse_source else str(row.get("sourceProcessUuid") or ""),
                "" if collapse_source else str(row.get("sourceNodeId") or ""),
            )
            amount = float(row.get("amount") or 0.0)
            if key not in grouped:
                grouped[key] = dict(row)
                if collapse_source:
                    grouped[key]["sourceProcessUuid"] = ""
                    grouped[key]["sourceProcessName"] = ""
                    grouped[key]["sourceNodeId"] = ""
                grouped[key]["amount"] = 0.0
            grouped[key]["amount"] = float(grouped[key]["amount"] or 0.0) + amount
        return list(grouped.values())

    boundary_inputs: list[dict] = []
    boundary_elementary: list[dict] = []
    for vp in selected_virtual_processes:
        for row in vp.get("technosphere_inputs") or []:
            if not isinstance(row, dict):
                continue
            slim = _slim_exchange_row(row, "input", True)
            if slim is None:
                continue
            if allowed_input_flow_uuids and str(slim.get("flowUuid") or "") not in allowed_input_flow_uuids:
                continue
            if allowed_input_keys:
                row_key = _policy_key(slim)
                if row_key not in allowed_input_keys and not any(
                    _policy_flow_uuid(item) == str(slim.get("flowUuid") or "") for item in policy_inputs if isinstance(item, dict) and _policy_is_exposed(item)
                ):
                    continue
            preferred_policy = preferred_input_policy_by_flow.get(str(slim.get("flowUuid") or ""))
            if preferred_policy is not None:
                slim["name"] = str(preferred_policy.get("name") or slim.get("name") or "")
                slim["unit"] = str(preferred_policy.get("unit") or slim.get("unit") or "")
                slim["unitGroup"] = str(preferred_policy.get("unit_group") or preferred_policy.get("unitGroup") or slim.get("unitGroup") or "")
                slim["sourceProcessUuid"] = ""
                slim["sourceProcessName"] = ""
                slim["sourceNodeId"] = ""
                slim["internalExposed"] = True
                slim["showOnNode"] = True
            boundary_inputs.append(slim)
        for row in vp.get("elementary_flows") or []:
            if not isinstance(row, dict):
                continue
            slim = _slim_exchange_row(row, "output", True)
            if slim is None:
                continue
            boundary_elementary.append(slim)

    boundary_inputs = _aggregate_rows(boundary_inputs, collapse_source=True)
    boundary_outputs = _aggregate_rows(boundary_outputs, collapse_source=False)
    boundary_elementary = _aggregate_rows(boundary_elementary, collapse_source=True)
    virtual_processes = selected_virtual_processes

    return {
        "project_id": project_id,
        "pts_uuid": pts_uuid,
        "pts_node_id": str(compile_row.pts_node_id),
        "graph_hash": str(compile_row.graph_hash),
        "ok": bool(compile_row.ok),
        "errors": list(compile_row.errors_json or []),
        "warnings": list(compile_row.warnings_json or []),
        "matrix_size": int(compile_row.matrix_size or 0),
        "invertible": bool(compile_row.invertible),
        "external_boundary": {
            "inputs": boundary_inputs,
            "outputs": boundary_outputs,
            "elementary": boundary_elementary,
        },
        "virtual_processes": virtual_processes,
        "frontend_ports": _build_frontend_ports_from_external_payload(
            {
                "external_boundary": {
                    "inputs": boundary_inputs,
                    "outputs": boundary_outputs,
                    "elementary": boundary_elementary,
                },
                "virtual_processes": virtual_processes,
            }
        ),
        "output_virtual_process_bindings": output_virtual_process_bindings,
        "definition_summary": {
            "pts_uuid": pts_uuid,
            "pts_node_id": str(definition.get("pts_node_id") or ""),
            "internal_node_count": len(definition.get("internal_node_ids") or []),
            "product_ref_count": len(definition.get("product_refs") or []),
        },
    }


def upsert_pts_external_artifact(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
    pts_node_id: str,
    graph_hash: str,
    payload: dict,
    source_compile_id: str | None = None,
    source_compile_version: int | None = None,
    set_active: bool = True,
) -> PtsExternalArtifact:
    row = (
        db.query(PtsExternalArtifact)
        .filter(
            PtsExternalArtifact.project_id == project_id,
            PtsExternalArtifact.pts_uuid == pts_uuid,
            PtsExternalArtifact.graph_hash == graph_hash,
        )
        .first()
    )
    if row is None:
        published_version = _next_pts_published_version(db=db, project_id=project_id, pts_uuid=pts_uuid)
        row = PtsExternalArtifact(
            project_id=project_id,
            pts_id=pts_uuid,
            pts_uuid=pts_uuid,
            pts_node_id=pts_node_id,
            graph_hash=graph_hash,
            published_version=published_version,
            source_compile_id=source_compile_id,
            source_compile_version=source_compile_version,
            artifact_json=payload,
        )
        db.add(row)
    else:
        row.pts_id = pts_uuid
        row.pts_uuid = pts_uuid
        row.pts_node_id = pts_node_id
        if row.published_version is None:
            row.published_version = _next_pts_published_version(db=db, project_id=project_id, pts_uuid=pts_uuid)
        row.source_compile_id = source_compile_id or row.source_compile_id
        row.source_compile_version = source_compile_version if source_compile_version is not None else row.source_compile_version
        row.artifact_json = payload
    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    if resource is not None:
        resource.project_id = project_id
        resource.pts_node_id = pts_node_id
        resource.compiled_graph_hash = graph_hash
        resource.latest_published_version = int(row.published_version or 0) or resource.latest_published_version
        if set_active:
            resource.active_published_version = int(row.published_version or 0) or resource.active_published_version
        resource.published_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return row


def build_flattened_graph_for_run_pts(*, graph: HybridGraph, compile_rows: list[PtsCompileArtifact]) -> HybridGraph:
    if not compile_rows:
        return graph

    node_by_id = {node.id: node for node in graph.nodes}
    pts_node_ids = {row.pts_node_id for row in compile_rows}

    new_nodes = [node for node in graph.nodes if node.id not in pts_node_ids]
    new_edges = [edge for edge in graph.exchanges if edge.fromNode not in pts_node_ids and edge.toNode not in pts_node_ids]

    virtual_nodes_map: dict[str, list[dict]] = {}
    for row in compile_rows:
        artifact = row.artifact_json or {}
        virtual_nodes_map[row.pts_node_id] = list(artifact.get("virtual_processes") or [])

    def vp_ref(vp: dict) -> tuple[str, str, str]:
        ref = vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else {}
        flow_uuid = str(ref.get("flowUuid") or "")
        flow_name = str(ref.get("name") or "")
        flow_unit = str(ref.get("unit") or "")
        return flow_uuid, flow_name, flow_unit

    def as_flow_port(item: dict, *, idx: int, fallback_direction: str, fallback_type: str) -> dict:
        direction = str(item.get("direction") or fallback_direction or "input")
        flow_type = str(item.get("type") or fallback_type)
        return {
            "id": str(item.get("id") or f"{fallback_type}_{idx}"),
            "flowUuid": str(item.get("flowUuid") or ""),
            "name": str(item.get("name") or ""),
            "unit": str(item.get("unit") or ""),
            "unitGroup": item.get("unitGroup"),
            "amount": float(item.get("amount") or 0.0),
            "externalSaleAmount": float(item.get("externalSaleAmount") or 0.0),
            "type": flow_type,
            "direction": direction,
            "showOnNode": bool(item.get("showOnNode", True)),
            "internalExposed": item.get("internalExposed"),
            "dbMapping": item.get("dbMapping"),
            "isProduct": bool(item.get("isProduct", False)),
            "allocationFactor": item.get("allocationFactor"),
        }

    def vp_inputs(vp: dict) -> list[dict]:
        raw = vp.get("technosphere_inputs")
        if isinstance(raw, list):
            return [
                as_flow_port(item, idx=idx, fallback_direction="input", fallback_type="technosphere")
                for idx, item in enumerate(raw, start=1)
                if isinstance(item, dict)
            ]
        return []

    def vp_emissions(vp: dict) -> list[dict]:
        raw = vp.get("elementary_flows")
        if isinstance(raw, list):
            return [
                as_flow_port(item, idx=idx, fallback_direction="output", fallback_type="biosphere")
                for idx, item in enumerate(raw, start=1)
                if isinstance(item, dict)
            ]
        return []

    def pick_virtual_node(pts_node_id: str, flow_uuid: str, direction: str, source_port_id: str | None = None) -> dict | None:
        virtuals = virtual_nodes_map.get(pts_node_id, [])
        if not virtuals:
            return None
        if direction == "output":
            candidates = [vp for vp in virtuals if vp_ref(vp)[0] == flow_uuid]
            if not candidates:
                return virtuals[0]
            if len(candidates) == 1:
                return candidates[0]

            published_candidates = [
                vp for vp in candidates if "::published::" in str(vp.get("process_uuid") or "")
            ]
            if len(published_candidates) == 1:
                return published_candidates[0]

            if source_port_id:
                by_source_port = [
                    vp
                    for vp in candidates
                    if (
                        str(vp.get("source_port_id") or vp.get("sourcePortId") or "") == source_port_id
                        or str(vp.get("source_port_id") or vp.get("sourcePortId") or "").endswith(f"::{source_port_id}")
                    )
                ]
                if len(by_source_port) == 1:
                    return by_source_port[0]

            pts_node = node_by_id.get(pts_node_id)
            bound_source_process_uuid = ""
            bound_source_node_id = ""
            source_port_name = ""
            if pts_node is not None and source_port_id:
                source_port = next((port for port in pts_node.outputs if port.id == source_port_id), None)
                if source_port is not None:
                    bound_source_process_uuid = str(source_port.source_process_uuid or "").strip()
                    bound_source_node_id = str(source_port.source_node_id or "").strip()
                    source_port_name = str(source_port.name or "").strip()

            if bound_source_process_uuid:
                exact = [
                    vp
                    for vp in candidates
                    if str(vp.get("source_process_uuid") or vp.get("sourceProcessUuid") or "").strip()
                    == bound_source_process_uuid
                    or str(vp.get("process_uuid") or "").strip() == bound_source_process_uuid
                ]
                if len(exact) == 1:
                    return exact[0]

            if bound_source_node_id:
                by_node_hint = [
                    vp
                    for vp in candidates
                    if bound_source_node_id in str(vp.get("process_uuid") or "")
                    or bound_source_node_id in str(vp.get("process_name") or "")
                ]
                if len(by_node_hint) == 1:
                    return by_node_hint[0]

            if source_port_name:
                by_ref_name = [
                    vp
                    for vp in candidates
                    if source_port_name
                    == str((vp.get("reference_product") or {}).get("name") if isinstance(vp.get("reference_product"), dict) else "")
                ]
                if len(by_ref_name) == 1:
                    return by_ref_name[0]

                if "@" in source_port_name:
                    process_hint = source_port_name.split("@", 1)[1].strip()
                    if process_hint:
                        by_process_hint = [
                            vp for vp in candidates if process_hint in str(vp.get("process_name") or "")
                        ]
                        if len(by_process_hint) == 1:
                            return by_process_hint[0]

            raise ValueError(
                f"AMBIGUOUS_PTS_OUTPUT_PRODUCER|Multiple virtual producers match flowUuid={flow_uuid} "
                f"for pts_node_id={pts_node_id}; provide sourceProcessUuid/sourceNodeId binding on output port "
                f"and ensure published artifact includes source_port_id/source_process_uuid (recompile if needed)"
            )
        for vp in virtuals:
            inputs = vp_inputs(vp)
            if any(str(item.get("flowUuid") or "") == flow_uuid for item in inputs if isinstance(item, dict)):
                return vp
        return virtuals[0]

    def pick_virtual_input_targets(pts_node_id: str, flow_uuid: str) -> list[tuple[dict, float, str | None]]:
        virtuals = virtual_nodes_map.get(pts_node_id, [])
        if not virtuals:
            return []
        targets: list[tuple[dict, float, str | None]] = []
        for vp in virtuals:
            inputs = vp_inputs(vp)
            amount = 0.0
            matched_port_id: str | None = None
            for item in inputs:
                if not isinstance(item, dict):
                    continue
                if str(item.get("flowUuid") or "") != flow_uuid:
                    continue
                if matched_port_id is None:
                    raw_port_id = str(item.get("id") or "").strip()
                    matched_port_id = raw_port_id or None
                try:
                    amount += float(item.get("amount") or 0.0)
                except (TypeError, ValueError):
                    continue
            if amount > 0:
                targets.append((vp, amount, matched_port_id))
        return targets

    process_uuid_to_node_id: dict[str, str] = {}
    virtual_key_to_node_id: dict[str, str] = {}
    used_process_uuids: set[str] = set()

    def vp_key(vp: dict, fallback_idx: int) -> str:
        key = str(vp.get("virtual_process_key") or "").strip()
        if key:
            return key
        process_uuid = str(vp.get("process_uuid") or "").strip()
        return process_uuid or f"vp::{fallback_idx}"

    def resolve_vp_node_id(vp: dict) -> str | None:
        key = str(vp.get("virtual_process_key") or "").strip()
        if key and key in virtual_key_to_node_id:
            return virtual_key_to_node_id[key]
        process_uuid = str(vp.get("process_uuid") or "").strip()
        if process_uuid:
            return process_uuid_to_node_id.get(process_uuid)
        return None

    for pts_node_id in pts_node_ids:
        source_pts = node_by_id.get(pts_node_id)
        if source_pts is None:
            continue
        virtuals = virtual_nodes_map.get(pts_node_id, [])
        for idx, vp in enumerate(virtuals, start=1):
            ref_uuid, ref_name, ref_unit = vp_ref(vp)
            ref_product = vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else {}
            ref_unit_group = str(
                vp.get("reference_unit_group")
                or ref_product.get("unitGroup")
                or ref_product.get("unit_group")
                or ""
            ).strip()
            if not ref_name:
                ref_name = f"{source_pts.name}-product-{idx}"
            if not ref_unit:
                ref_unit = "kg"
            inputs = vp_inputs(vp)
            emissions = vp_emissions(vp)
            vn_id = f"{source_pts.id}::vp::{idx}"
            base_process_uuid = str(vp.get("process_uuid") or f"{source_pts.process_uuid}::vp::{idx}")
            unique_process_uuid = base_process_uuid
            if unique_process_uuid in used_process_uuids:
                unique_process_uuid = f"{base_process_uuid}::v{idx}"
                suffix = 1
                while unique_process_uuid in used_process_uuids:
                    suffix += 1
                    unique_process_uuid = f"{base_process_uuid}::v{idx}_{suffix}"
            used_process_uuids.add(unique_process_uuid)

            new_node = {
                "id": vn_id,
                "node_kind": "unit_process",
                "mode": "normalized",
                "lci_role": None,
                "pts_uuid": None,
                "process_uuid": unique_process_uuid,
                "name": str(vp.get("process_name") or f"{source_pts.name}::{idx}"),
                "location": source_pts.location,
                "reference_product": ref_name,
                "inputs": inputs,
                "outputs": [
                    {
                        "id": f"out_{idx}",
                        "flowUuid": ref_uuid,
                        "name": ref_name,
                        "unit": ref_unit,
                        "unitGroup": ref_unit_group or None,
                        "amount": 1.0,
                        "isProduct": True,
                        "externalSaleAmount": 0.0,
                        "type": "technosphere",
                        "direction": "output",
                        "showOnNode": True,
                    }
                ],
                "emissions": emissions,
            }
            validated = HybridNode.model_validate(new_node)
            new_nodes.append(validated)
            process_uuid_to_node_id[validated.process_uuid] = validated.id
            key = vp_key(vp, idx)
            virtual_key_to_node_id[key] = validated.id

    for pts_node_id in pts_node_ids:
        virtuals = virtual_nodes_map.get(pts_node_id, [])
        if not virtuals:
            continue

        provider_candidates_by_flow: dict[str, list[dict]] = {}
        for vp in virtuals:
            ref_uuid, _, _ = vp_ref(vp)
            if not ref_uuid:
                continue
            provider_candidates_by_flow.setdefault(ref_uuid, []).append(vp)

        for target_idx, vp in enumerate(virtuals, start=1):
            target_node_id = resolve_vp_node_id(vp)
            if not target_node_id:
                continue
            for input_idx, item in enumerate(vp_inputs(vp), start=1):
                if not isinstance(item, dict):
                    continue
                flow_uuid = str(item.get("flowUuid") or "").strip()
                if not flow_uuid:
                    continue
                candidates = provider_candidates_by_flow.get(flow_uuid, [])
                if not candidates:
                    continue

                source_process_uuid = str(item.get("sourceProcessUuid") or item.get("source_process_uuid") or "").strip()
                source_node_id = str(item.get("sourceNodeId") or item.get("source_node_id") or "").strip()

                provider_vp: dict | None = None
                if source_process_uuid:
                    exact = [
                        candidate
                        for candidate in candidates
                        if str(candidate.get("source_process_uuid") or candidate.get("sourceProcessUuid") or "").strip()
                        == source_process_uuid
                    ]
                    if len(exact) == 1:
                        provider_vp = exact[0]
                if provider_vp is None and source_node_id:
                    exact = [
                        candidate
                        for candidate in candidates
                        if str(candidate.get("source_node_id") or candidate.get("sourceNodeId") or "").strip()
                        == source_node_id
                    ]
                    if len(exact) == 1:
                        provider_vp = exact[0]
                if provider_vp is None and len(candidates) == 1:
                    provider_vp = candidates[0]
                if provider_vp is None:
                    continue

                provider_node_id = resolve_vp_node_id(provider_vp)
                if not provider_node_id or provider_node_id == target_node_id:
                    continue

                target_port_id = str(item.get("id") or "").strip()
                target_handle = f"in:{target_port_id}" if target_port_id else None
                amount = float(item.get("amount") or 0.0)
                if amount <= 0:
                    continue
                new_edges.append(
                    HybridEdge.model_validate(
                        {
                            "id": f"{pts_node_id}::internal::{target_idx}::{input_idx}",
                            "fromNode": provider_node_id,
                            "toNode": target_node_id,
                            "sourceHandle": None,
                            "source_port_id": None,
                            "targetHandle": target_handle,
                            "target_port_id": target_handle,
                            "flowUuid": flow_uuid,
                            "flowName": str(item.get("name") or flow_uuid),
                            "quantityMode": "dual",
                            "amount": amount,
                            "providerAmount": amount,
                            "consumerAmount": amount,
                            "unit": str(item.get("unit") or ""),
                            "type": "technosphere",
                            "allocation": "physical",
                            "dbMapping": "",
                        }
                    )
                )

    for edge in graph.exchanges:
        if edge.fromNode in pts_node_ids and edge.toNode in pts_node_ids:
            source_port_id = _port_id_from_handle(edge.sourceHandle or edge.source_port_id, "out")
            source_vp = pick_virtual_node(edge.fromNode, edge.flowUuid, "output", source_port_id)
            if source_vp is None:
                continue
            source_node_id = resolve_vp_node_id(source_vp)
            if not source_node_id:
                continue

            input_targets = pick_virtual_input_targets(edge.toNode, edge.flowUuid)
            if input_targets:
                for idx, (vp, vp_amount, vp_input_port_id) in enumerate(input_targets, start=1):
                    target_node_id = resolve_vp_node_id(vp)
                    if not target_node_id:
                        continue
                    target_handle = f"in:{vp_input_port_id}" if vp_input_port_id else edge.targetHandle
                    patched = edge.model_copy(
                        update={
                            "id": f"{edge.id}::vp2vp::{idx}",
                            "fromNode": source_node_id,
                            "toNode": target_node_id,
                            "targetHandle": target_handle,
                            "target_port_id": target_handle,
                            "quantityMode": "dual",
                            "amount": vp_amount,
                            "providerAmount": vp_amount,
                            "consumerAmount": vp_amount,
                        }
                    )
                    new_edges.append(patched)
                continue

            target_vp = pick_virtual_node(edge.toNode, edge.flowUuid, "input")
            if target_vp is None:
                continue
            target_node_id = resolve_vp_node_id(target_vp)
            if not target_node_id:
                continue
            fallback_amount = float(edge.consumerAmount or edge.amount or 0.0)
            if fallback_amount <= 0:
                fallback_amount = 1.0
            patched = edge.model_copy(
                update={
                    "id": f"{edge.id}::vp2vp::fallback",
                    "fromNode": source_node_id,
                    "toNode": target_node_id,
                    "quantityMode": "dual",
                    "amount": fallback_amount,
                    "providerAmount": fallback_amount,
                    "consumerAmount": fallback_amount,
                }
            )
            new_edges.append(patched)
            continue

        if edge.fromNode in pts_node_ids:
            edge_source_port_id = _port_id_from_handle(edge.sourceHandle or edge.source_port_id, "out")
            vp = pick_virtual_node(edge.fromNode, edge.flowUuid, "output", edge_source_port_id)
            if vp is None:
                continue
            target_node = node_by_id.get(edge.toNode)
            source_node_id = resolve_vp_node_id(vp)
            if not source_node_id:
                continue
            patched = edge.model_copy(update={"fromNode": source_node_id})
            if target_node is not None:
                patched.quantityMode = "single" if target_node.mode == "balanced" else "dual"
            new_edges.append(patched)
            continue

        if edge.toNode in pts_node_ids:
            input_targets = pick_virtual_input_targets(edge.toNode, edge.flowUuid)
            if not input_targets:
                vp = pick_virtual_node(edge.toNode, edge.flowUuid, "input")
                if vp is None:
                    continue
                target_node_id = resolve_vp_node_id(vp)
                if not target_node_id:
                    continue
                patched = edge.model_copy(update={"toNode": target_node_id})
                new_edges.append(patched)
                continue

            for idx, (vp, vp_amount, vp_input_port_id) in enumerate(input_targets, start=1):
                target_node_id = resolve_vp_node_id(vp)
                if not target_node_id:
                    continue
                target_handle = f"in:{vp_input_port_id}" if vp_input_port_id else edge.targetHandle
                patched = edge.model_copy(
                    update={
                        "id": f"{edge.id}::vp_in::{idx}",
                        "toNode": target_node_id,
                        "targetHandle": target_handle,
                        "target_port_id": target_handle,
                        "quantityMode": "dual",
                        "amount": vp_amount,
                        "providerAmount": vp_amount,
                        "consumerAmount": vp_amount,
                    }
                )
                new_edges.append(patched)

    return graph.model_copy(
        update={
            "nodes": new_nodes,
            "exchanges": new_edges,
        }
    )


def _filter_lci_result_by_process_uuids(lci_result: dict, keep_process_uuids: set[str]) -> dict:
    process_index = lci_result.get("process_index")
    if not isinstance(process_index, list):
        return lci_result
    kept_positions = [idx for idx, item in enumerate(process_index) if str(item) in keep_process_uuids]
    filtered_process_index = [process_index[idx] for idx in kept_positions]

    values = lci_result.get("values")
    filtered_values = values
    if isinstance(values, list) and values:
        if all(isinstance(row, list) for row in values):
            filtered_values = [[row[idx] for idx in kept_positions if idx < len(row)] for row in values]
        elif len(values) == len(process_index):
            filtered_values = [values[idx] for idx in kept_positions]

    filtered = dict(lci_result)
    filtered["process_index"] = filtered_process_index
    filtered["values"] = filtered_values
    process_unit_map = lci_result.get("process_unit_map")
    if isinstance(process_unit_map, dict):
        filtered["process_unit_map"] = {
            str(pid): meta
            for pid, meta in process_unit_map.items()
            if str(pid) in keep_process_uuids
        }
    product_result_index = lci_result.get("product_result_index")
    if isinstance(product_result_index, list):
        kept_product_positions = [
            idx
            for idx, item in enumerate(product_result_index)
            if isinstance(item, dict) and str(item.get("process_uuid") or "") in keep_process_uuids
        ]
        filtered["product_result_index"] = [product_result_index[idx] for idx in kept_product_positions]
        product_values = lci_result.get("product_values")
        if isinstance(product_values, list) and product_values:
            if all(isinstance(row, list) for row in product_values):
                filtered["product_values"] = [
                    [row[idx] for idx in kept_product_positions if idx < len(row)]
                    for row in product_values
                ]
            elif len(product_values) == len(product_result_index):
                filtered["product_values"] = [product_values[idx] for idx in kept_product_positions]
        product_unit_map = lci_result.get("product_unit_map")
        if isinstance(product_unit_map, dict):
            kept_product_keys = {
                str(item.get("product_key") or "")
                for item in filtered.get("product_result_index", [])
                if isinstance(item, dict)
            }
            filtered["product_unit_map"] = {
                str(product_key): meta
                for product_key, meta in product_unit_map.items()
                if str(product_key) in kept_product_keys
            }
    return filtered


def _collect_published_vp_process_uuids(
    *,
    graph: HybridGraph,
    compile_rows: list[PtsCompileArtifact],
    flattened_graph: HybridGraph,
) -> set[str]:
    published_keys: set[tuple[str, str, str]] = set()
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        for port in node.outputs:
            if port.type == "biosphere":
                continue
            if not bool(port.showOnNode):
                continue
            flow_uuid = str(port.flowUuid or "").strip()
            source_process_uuid = str(port.source_process_uuid or "").strip()
            if not flow_uuid:
                continue
            published_keys.add((node.id, source_process_uuid, flow_uuid))

    vp_by_key: dict[tuple[str, str, str], str] = {}
    vp_by_flow: dict[tuple[str, str], list[str]] = {}
    for row in compile_rows:
        artifact = row.artifact_json if isinstance(row.artifact_json, dict) else {}
        virtuals = artifact.get("virtual_processes") if isinstance(artifact, dict) else []
        for vp in virtuals if isinstance(virtuals, list) else []:
            if not isinstance(vp, dict):
                continue
            process_uuid = str(vp.get("process_uuid") or "").strip()
            source_process_uuid = str(vp.get("source_process_uuid") or "").strip()
            ref = vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else {}
            flow_uuid = str(ref.get("flowUuid") or "").strip()
            if not process_uuid or not flow_uuid:
                continue
            vp_by_key[(row.pts_node_id, source_process_uuid, flow_uuid)] = process_uuid
            vp_by_flow.setdefault((row.pts_node_id, flow_uuid), []).append(process_uuid)

    keep: set[str] = set()
    for pts_node_id, source_process_uuid, flow_uuid in published_keys:
        exact = vp_by_key.get((pts_node_id, source_process_uuid, flow_uuid))
        if exact:
            keep.add(exact)
            continue
        fallback = vp_by_flow.get((pts_node_id, flow_uuid), [])
        if len(fallback) == 1:
            keep.add(fallback[0])

    # Keep non-VP nodes in mixed graphs.
    for node in flattened_graph.nodes:
        if "::vp::" not in node.id:
            keep.add(node.process_uuid)
    return keep


def _load_published_compile_rows_for_graph(
    *,
    db: Session,
    project_id: str,
    graph: HybridGraph,
    graph_hash: str | None = None,
) -> list[PtsCompileArtifact]:
    rows: list[PtsCompileArtifact] = []
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id)
        bound_published_version = None
        expected_pts_graph_hash = compute_pts_graph_hash(graph, node.id)
        if graph_hash and graph_hash != expected_pts_graph_hash:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PTS_PUBLISHED_ARTIFACT_HASH_MISMATCH",
                    "message": (
                        f"Provided artifact_graph_hash does not match current PTS graph hash for pts_uuid={pts_uuid}. "
                        "Please republish current graph or omit artifact_graph_hash."
                    ),
                    "evidence": [
                        {
                            "project_id": project_id,
                            "pts_uuid": pts_uuid,
                            "provided_graph_hash": graph_hash,
                            "expected_pts_graph_hash": expected_pts_graph_hash,
                        }
                    ],
                },
            )
        resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
        external = _load_pts_active_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
        )
        bound_published_version = int(external.published_version) if external is not None and external.published_version is not None else None

        effective_hash = graph_hash or expected_pts_graph_hash
        if external is None:
            if _is_unpublished_empty_pts_draft(graph=graph, pts_node=node):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "PTS_DRAFT_EMPTY_NOT_RUNNABLE",
                        "message": (
                            f"PTS {pts_uuid} is still an empty draft. Open the PTS, add internal nodes/edges, publish it, "
                            "then run the main graph."
                        ),
                        "evidence": [
                            {
                                "project_id": project_id,
                                "pts_uuid": pts_uuid,
                                "pts_node_id": node.id,
                                "draft_state": "empty_unpublished_pts",
                                "expected_pts_graph_hash": expected_pts_graph_hash,
                            }
                        ],
                    },
                )
            latest = (
                db.query(PtsExternalArtifact)
                .filter(
                    PtsExternalArtifact.project_id == project_id,
                    PtsExternalArtifact.pts_uuid == pts_uuid,
                )
                .order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
                .first()
            )
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "PTS_PUBLISHED_ARTIFACT_NOT_FOUND",
                    "message": (
                        f"Published external artifact not found for pts_uuid={pts_uuid} and graph_hash={graph_hash}. "
                        "Please publish current graph first."
                        if effective_hash
                        else f"Published external artifact not found for pts_uuid={pts_uuid}"
                    ),
                    "evidence": [
                        {
                            "project_id": project_id,
                            "pts_uuid": pts_uuid,
                            "graph_hash": effective_hash,
                            "published_version": bound_published_version,
                            "expected_pts_graph_hash": expected_pts_graph_hash,
                            "provided_graph_hash": graph_hash,
                            "latest_published_graph_hash": (latest.graph_hash if latest is not None else None),
                        }
                    ],
                },
            )
        payload = external.artifact_json if isinstance(external.artifact_json, dict) else {}
        rows.append(
            PtsCompileArtifact(
                project_id=project_id,
                pts_node_id=str(payload.get("pts_node_id") or node.id),
                pts_uuid=pts_uuid,
                graph_hash=str(payload.get("graph_hash") or external.graph_hash),
                compile_version=(int(external.source_compile_version) if external.source_compile_version is not None else None),
                ok=bool(payload.get("ok", True)),
                matrix_size=int(payload.get("matrix_size") or 0),
                invertible=bool(payload.get("invertible", True)),
                errors_json=list(payload.get("errors") or []),
                warnings_json=list(payload.get("warnings") or []),
                artifact_json=dict(payload),
            )
        )
    return rows

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    _migrate_flow_catalog_table_name()
    should_bootstrap_reference_data = len(inspect(engine).get_table_names()) == 0
    Base.metadata.create_all(bind=engine)
    _ensure_reference_processes_schema()
    _ensure_model_versions_hash_schema()
    _ensure_projects_management_schema()
    _ensure_pts_uuid_schema()
    _ensure_pts_resources_schema()
    db = SessionLocal()
    try:
        if should_bootstrap_reference_data:
            _bootstrap_reference_data_if_needed(db=db)
        _backfill_pts_artifact_versions(db=db, dry_run=False)
    finally:
        db.close()
    if settings.auto_prune_on_startup:
        db = SessionLocal()
        try:
            _prune_model_versions_retention(
                db=db,
                keep_latest=max(1, int(settings.keep_latest_versions_per_project)),
                dry_run=False,
                project_id=None,
                vacuum_after_cleanup=bool(settings.auto_vacuum_after_prune_on_startup),
            )
        except Exception as exc:
            print(f"[startup-maintenance] prune failed: {exc}")
        finally:
            db.close()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": settings.app_name}


def list_reference_processes(db: Session = Depends(get_db)) -> list[ReferenceProcess]:
    return db.query(ReferenceProcess).order_by(ReferenceProcess.process_name.asc()).all()


def import_reference_processes_json(
    payload: ImportProcessesRequest,
    db: Session = Depends(get_db),
) -> ImportProcessesResponse:
    try:
        result = import_processes_from_json(
            db,
            path_like=payload.path,
            replace_existing=payload.replace_existing,
            strict_reference_flow=payload.strict_reference_flow,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ImportProcessesResponse(**result)


@app.post("/api/processes/import-json", response_model=ImportProcessesResponse)
def import_processes_json_api(
    payload: ImportProcessesRequest,
    db: Session = Depends(get_db),
) -> ImportProcessesResponse:
    result = import_reference_processes_json(payload=payload, db=db)
    _invalidate_management_caches(stats=True, reference_processes=True)
    return result


@app.post("/api/import/tidas/flows", response_model=TidasImportReportResponse)
@app.post("/import/tidas/flows", response_model=TidasImportReportResponse)
async def import_tidas_flows(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    upsert_mode: str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    payload = TidasImportRequest(
        dry_run=_coerce_form_bool(dry_run, default=False),
        upsert_mode=str(upsert_mode or "update").strip() or "update",
        strict_mode=_coerce_form_bool(strict_mode, default=False),
    )
    report = _build_tidas_base_report(import_type="flows", payload=payload)
    source_name, rows, parse_errors = await _parse_tidas_uploaded_json(file)
    source_items: list[tuple[str, list[dict], list[str]]] = [(source_name, rows, parse_errors)]
    report["source_path"] = f"upload://{source_name}"
    report["total_files"] = 1
    for source_name, rows, parse_errors in source_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            flow_record, err = _extract_tidas_flow_record(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_name}: {err}")
                continue
            flow_uuid = str(flow_record["flow_uuid"])
            existing = db.get(FlowRecord, flow_uuid)
            if existing is not None and payload.upsert_mode == "skip":
                report["skipped"] += 1
                continue

            if existing is None:
                report["inserted"] += 1
                if payload.dry_run:
                    continue
                db.add(FlowRecord(**flow_record))
                continue

            report["updated"] += 1
            if payload.dry_run:
                continue
            existing.flow_name = str(flow_record.get("flow_name") or existing.flow_name)
            existing.flow_name_en = _safe_str(flow_record.get("flow_name_en"))
            existing.flow_type = str(flow_record.get("flow_type") or existing.flow_type)
            existing.default_unit = str(flow_record.get("default_unit") or existing.default_unit)
            existing.unit_group = str(flow_record.get("unit_group") or existing.unit_group)
            new_compartment = _safe_str(flow_record.get("compartment"))
            if new_compartment and new_compartment != "[]":
                existing.compartment = new_compartment
            existing.source_updated_at = _safe_str(flow_record.get("source_updated_at"))

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist_tidas_import_report(db, report)
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIDAS_IMPORT_STRICT_FAILED",
                "job_id": persisted.job_id,
                "message": "TIDAS flow import failed in strict_mode",
                "failed": report["failed"],
                "errors": report["errors"][:50],
            },
        )

    if payload.dry_run:
        db.rollback()
    else:
        db.commit()
        _invalidate_management_caches(flows=True, stats=True, reference_processes=True)
    return _persist_tidas_import_report(db, report)


@app.post("/api/import/tidas/processes", response_model=TidasImportReportResponse)
@app.post("/import/tidas/processes", response_model=TidasImportReportResponse)
async def import_tidas_processes(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    upsert_mode: str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    payload = TidasImportRequest(
        dry_run=_coerce_form_bool(dry_run, default=False),
        upsert_mode=str(upsert_mode or "update").strip() or "update",
        strict_mode=_coerce_form_bool(strict_mode, default=False),
    )
    report = _build_tidas_base_report(import_type="processes", payload=payload)
    source_name, raw_bytes = await _read_uploaded_bytes(file)
    report["source_path"] = f"upload://{source_name}"
    is_zip_upload = source_name.lower().endswith(".zip") or raw_bytes[:4] == b"PK\x03\x04"

    if is_zip_upload:
        manifest, flow_items, process_items, _model_items, bundle_errors = _parse_tidas_bundle_zip(
            source_name=source_name,
            raw_bytes=raw_bytes,
            require_model_file=False,
        )
        report["total_files"] = len(flow_items) + len(process_items)
        report["errors"].extend(bundle_errors)
        report["failed"] += len(bundle_errors)
        report["warnings"].extend([f"bundle missing process: {item}" for item in list(manifest.get("missing_processes") or [])])
        report["warnings"].extend([f"bundle missing flow: {item}" for item in list(manifest.get("missing_flows") or [])])

        if bundle_errors and payload.strict_mode:
            db.rollback()
            report["warnings"].append("strict_mode rollback: bundle manifest/structure invalid")
            persisted = _persist_tidas_import_report(db, report)
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "TIDAS_BUNDLE_INVALID",
                    "job_id": persisted.job_id,
                    "message": "TIDAS process bundle invalid in strict_mode",
                    "errors": report["errors"][:50],
                },
            )

        bundle_flow_uuids: set[str] = set()
        seen_flow_uuids_in_batch: set[str] = set()
        for source_entry, rows, parse_errors in flow_items:
            report["errors"].extend(parse_errors)
            report["failed"] += len(parse_errors)
            for row in rows:
                report["total_records"] += 1
                flow_record, err = _extract_tidas_flow_record(row)
                if err:
                    report["failed"] += 1
                    report["errors"].append(f"{source_entry}: {err}")
                    continue
                flow_uuid = str(flow_record["flow_uuid"])
                if flow_uuid in seen_flow_uuids_in_batch:
                    report["skipped"] += 1
                    continue
                seen_flow_uuids_in_batch.add(flow_uuid)
                bundle_flow_uuids.add(flow_uuid)
                existing = db.get(FlowRecord, flow_uuid)
                if existing is not None and payload.upsert_mode == "skip":
                    report["skipped"] += 1
                    continue
                if existing is None:
                    report["inserted"] += 1
                    if not payload.dry_run:
                        db.add(FlowRecord(**flow_record))
                    continue
                report["updated"] += 1
                if payload.dry_run:
                    continue
                existing.flow_name = str(flow_record.get("flow_name") or existing.flow_name)
                existing.flow_name_en = _safe_str(flow_record.get("flow_name_en"))
                existing.flow_type = str(flow_record.get("flow_type") or existing.flow_type)
                existing.default_unit = str(flow_record.get("default_unit") or existing.default_unit)
                existing.unit_group = str(flow_record.get("unit_group") or existing.unit_group)
                new_compartment = _safe_str(flow_record.get("compartment"))
                if new_compartment and new_compartment != "[]":
                    existing.compartment = new_compartment
                existing.source_updated_at = _safe_str(flow_record.get("source_updated_at"))

        valid_flow_uuids = _flow_uuid_set_cached(db).union(bundle_flow_uuids)
        if not payload.dry_run:
            db.flush()
            _refresh_flow_runtime_caches_for_current_request()

        source_items: list[tuple[str, list[dict], list[str], str]] = [
            (entry_name, rows, parse_errors, f"zip://{source_name}/{entry_name}")
            for entry_name, rows, parse_errors in process_items
        ]
    else:
        try:
            raw_text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            source_items = [(source_name, [], [f"{source_name}: invalid UTF-8 ({exc})"], f"upload://{source_name}")]
        else:
            rows, parse_errors = _parse_tidas_json_payload(source_name=source_name, raw_text=raw_text)
            source_items = [(source_name, rows, parse_errors, f"upload://{source_name}")]
        report["total_files"] = 1
        valid_flow_uuids = _flow_uuid_set_cached(db)

    for source_name, rows, parse_errors, source_file in source_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            process_record, err = _extract_tidas_process_record(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_name}: {err}")
                continue
            process_uuid = str(process_record["process_uuid"])
            existing = db.get(ReferenceProcess, process_uuid)
            if existing is not None and payload.upsert_mode == "skip":
                report["skipped"] += 1
                continue

            kept_exchanges, filtered = _filter_exchanges_with_evidence(
                process_uuid=process_uuid,
                exchanges=list(process_record.get("exchanges") or []),
                valid_flow_uuids=valid_flow_uuids,
            )
            reference_flow_uuid, product_warnings = _mark_reference_product_exchange(
                process_uuid=process_uuid,
                process_json=process_record,
                exchanges=kept_exchanges,
            )
            report["warnings"].extend([f"{process_uuid}: {msg}" for msg in product_warnings])
            report["filtered_exchanges"].extend([item.model_dump(mode="python") for item in filtered])
            report["filtered_exchange_count"] = len(report["filtered_exchanges"])
            report["imported_exchange_count"] += len(kept_exchanges)

            normalized_json = {
                "process_uuid": process_uuid,
                "process_name": process_record.get("process_name"),
                "process_name_zh": process_record.get("process_name_zh"),
                "process_name_en": process_record.get("process_name_en"),
                "location": process_record.get("location"),
                "process_type": "unit_process",
                "reference_flow_internal_id": process_record.get("reference_flow_internal_id"),
                "reference_flow_source_uuid": process_record.get("reference_flow_source_uuid"),
                "reference_flow_source_name": process_record.get("reference_flow_source_name"),
                "exchanges": kept_exchanges,
                "source": "tidas_ref_process_import" if not is_zip_upload else "tidas_bundle_process_import",
            }
            process_report = ProcessImportReportResponse(
                process_uuid=process_uuid,
                source_process_uuid=None,
                import_mode="locked",
                imported_process_count=1,
                filtered_exchange_count=len(filtered),
                filtered_exchanges=filtered,
                warnings=(
                    [ProcessImportWarning(process_uuid=process_uuid, reasons=product_warnings)]
                    if product_warnings
                    else []
                ),
                updated_at=datetime.utcnow(),
            )
            if existing is None:
                report["inserted"] += 1
                report["imported_process_count"] += 1
                if payload.dry_run:
                    continue
                db.add(
                    ReferenceProcess(
                        process_uuid=process_uuid,
                        process_name=str(process_record.get("process_name") or process_uuid),
                        process_name_zh=_safe_str(process_record.get("process_name_zh")),
                        process_name_en=_safe_str(process_record.get("process_name_en")),
                        process_type="unit_process",
                        reference_flow_uuid=reference_flow_uuid,
                        reference_flow_internal_id=_safe_str(process_record.get("reference_flow_internal_id")),
                        process_json=normalized_json,
                        source_file=source_file,
                        source_process_uuid=None,
                        import_mode="locked",
                        import_report_json=process_report.model_dump(mode="json"),
                    )
                )
                continue

            report["updated"] += 1
            report["imported_process_count"] += 1
            if payload.dry_run:
                continue
            existing.process_name = str(process_record.get("process_name") or existing.process_name)
            existing.process_name_zh = _safe_str(process_record.get("process_name_zh"))
            existing.process_name_en = _safe_str(process_record.get("process_name_en"))
            existing.process_type = "unit_process"
            existing.reference_flow_uuid = reference_flow_uuid
            existing.reference_flow_internal_id = _safe_str(process_record.get("reference_flow_internal_id"))
            existing.process_json = normalized_json
            existing.source_file = source_file
            existing.source_process_uuid = None
            existing.import_mode = "locked"
            existing.import_report_json = process_report.model_dump(mode="json")

    filtered_models = [FilteredExchangeEvidence.model_validate(item) for item in report["filtered_exchanges"]]
    report["top_missing_flow_uuids"] = _top_missing_flow_uuids(filtered_models, top_n=10)

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist_tidas_import_report(db, report)
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIDAS_IMPORT_STRICT_FAILED",
                "job_id": persisted.job_id,
                "message": "TIDAS process import failed in strict_mode",
                "failed": report["failed"],
                "errors": report["errors"][:50],
            },
        )

    if payload.dry_run:
        db.rollback()
    else:
        db.commit()
        _invalidate_management_caches(flows=is_zip_upload, stats=True, reference_processes=True)
    return _persist_tidas_import_report(db, report)


@app.post("/api/import/tidas/models", response_model=TidasImportReportResponse)
@app.post("/import/tidas/models", response_model=TidasImportReportResponse)
async def import_tidas_models(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    display_lang: str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    payload = TidasModelImportRequest(
        dry_run=_coerce_form_bool(dry_run, default=False),
        strict_mode=_coerce_form_bool(strict_mode, default=False),
    )
    report = _build_tidas_base_report(import_type="models", payload=payload)
    source_name, rows, parse_errors = await _parse_tidas_uploaded_json(file)
    source_items: list[tuple[str, list[dict], list[str]]] = [(source_name, rows, parse_errors)]
    report["source_path"] = f"upload://{source_name}"
    report["total_files"] = 1

    for source_name, rows, parse_errors in source_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            model_record, err = _extract_tidas_model_record(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_name}: {err}")
                continue

            model_uuid = str(model_record["model_uuid"])

            if bool(model_record.get("topology_empty")):
                report["model_topology_empty_count"] += 1
                report["warnings"].append(f"{model_uuid}: MODEL_TOPOLOGY_EMPTY")

            unresolved_items = []
            for process_uuid in list(model_record.get("process_refs") or []):
                if db.get(ReferenceProcess, process_uuid) is None:
                    unresolved_items.append(
                        {
                            "model_uuid": model_uuid,
                            "type": "missing_process_reference",
                            "process_uuid": process_uuid,
                            "reason": "referenced process not found in reference_processes",
                        }
                    )
            if unresolved_items:
                report["unresolved_items"].extend(unresolved_items)
                report["unresolved_count"] = len(report["unresolved_items"])

            report["inserted"] += 1
            if payload.dry_run:
                continue
            now = datetime.utcnow()
            model_row = Model(
                name=str(model_record.get("model_name") or model_uuid),
                description=f"Imported from TIDAS lifecycle model JSON (source_model_uuid={model_uuid})",
                updated_at=now,
            )
            db.add(model_row)
            db.flush()
            report["created_projects"].append({"project_id": str(model_row.id), "name": str(model_row.name)})
            graph_json, graph_unresolved = _build_tidas_graph_from_model_record(
                db=db,
                model_record=model_record,
                display_lang=(_safe_str(display_lang) or "").lower() or "zh",
            )
            if graph_unresolved:
                report["unresolved_items"].extend(graph_unresolved)
                report["unresolved_count"] = len(report["unresolved_items"])
            if graph_json is not None:
                _create_project_version_from_graph_json(
                    db=db,
                    project_id=model_row.id,
                    graph_json=graph_json,
                )

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist_tidas_import_report(db, report)
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIDAS_IMPORT_STRICT_FAILED",
                "job_id": persisted.job_id,
                "message": "TIDAS model import failed in strict_mode",
                "failed": report["failed"],
                "errors": report["errors"][:50],
            },
        )

    if payload.dry_run:
        db.rollback()
    else:
        db.commit()
        _invalidate_management_caches(projects=True, stats=True)
    return _persist_tidas_import_report(db, report)


@app.post("/api/import/tidas/bundle", response_model=TidasImportReportResponse)
@app.post("/import/tidas/bundle", response_model=TidasImportReportResponse)
async def import_tidas_bundle(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    upsert_mode: str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    display_lang: str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    payload = TidasImportRequest(
        dry_run=_coerce_form_bool(dry_run, default=False),
        upsert_mode=str(upsert_mode or "update").strip() or "update",
        strict_mode=_coerce_form_bool(strict_mode, default=False),
    )
    report = _build_tidas_base_report(import_type="bundle", payload=payload)
    source_name, raw_bytes = await _read_uploaded_bytes(file)
    report["source_path"] = f"upload://{source_name}"
    manifest, flow_items, process_items, model_items, bundle_errors = _parse_tidas_bundle_zip(
        source_name=source_name,
        raw_bytes=raw_bytes,
    )
    report["errors"].extend(bundle_errors)
    report["failed"] += len(bundle_errors)
    report["total_files"] = len(flow_items) + len(process_items) + len(model_items)
    report["warnings"].extend([f"bundle missing process: {item}" for item in list(manifest.get("missing_processes") or [])])
    report["warnings"].extend([f"bundle missing flow: {item}" for item in list(manifest.get("missing_flows") or [])])

    if bundle_errors and payload.strict_mode:
        db.rollback()
        report["warnings"].append("strict_mode rollback: bundle manifest/structure invalid")
        persisted = _persist_tidas_import_report(db, report)
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIDAS_BUNDLE_INVALID",
                "job_id": persisted.job_id,
                "message": "TIDAS bundle invalid in strict_mode",
                "errors": report["errors"][:50],
            },
        )

    bundle_flow_uuids: set[str] = set()
    seen_flow_uuids_in_batch: set[str] = set()
    for source_entry, rows, parse_errors in flow_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            flow_record, err = _extract_tidas_flow_record(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_entry}: {err}")
                continue
            flow_uuid = str(flow_record["flow_uuid"])
            if flow_uuid in seen_flow_uuids_in_batch:
                report["skipped"] += 1
                continue
            seen_flow_uuids_in_batch.add(flow_uuid)
            bundle_flow_uuids.add(flow_uuid)
            existing = db.get(FlowRecord, flow_uuid)
            if existing is not None and payload.upsert_mode == "skip":
                report["skipped"] += 1
                continue
            if existing is None:
                report["inserted"] += 1
                if not payload.dry_run:
                    db.add(FlowRecord(**flow_record))
                continue
            report["updated"] += 1
            if payload.dry_run:
                continue
            existing.flow_name = str(flow_record.get("flow_name") or existing.flow_name)
            existing.flow_name_en = _safe_str(flow_record.get("flow_name_en"))
            existing.flow_type = str(flow_record.get("flow_type") or existing.flow_type)
            existing.default_unit = str(flow_record.get("default_unit") or existing.default_unit)
            existing.unit_group = str(flow_record.get("unit_group") or existing.unit_group)
            new_compartment = _safe_str(flow_record.get("compartment"))
            if new_compartment and new_compartment != "[]":
                existing.compartment = new_compartment
            existing.source_updated_at = _safe_str(flow_record.get("source_updated_at"))

    valid_flow_uuids = _flow_uuid_set_cached(db).union(bundle_flow_uuids)
    if not payload.dry_run:
        db.flush()
        _refresh_flow_runtime_caches_for_current_request()

    bundle_process_uuids: set[str] = set()
    bundle_process_json_by_uuid: dict[str, dict] = {}
    for source_entry, rows, parse_errors in process_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            process_record, err = _extract_tidas_process_record(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_entry}: {err}")
                continue
            process_uuid = str(process_record["process_uuid"])
            bundle_process_uuids.add(process_uuid)
            existing = db.get(ReferenceProcess, process_uuid)
            if existing is not None and payload.upsert_mode == "skip":
                report["skipped"] += 1
                continue

            kept_exchanges, filtered = _filter_exchanges_with_evidence(
                process_uuid=process_uuid,
                exchanges=list(process_record.get("exchanges") or []),
                valid_flow_uuids=valid_flow_uuids,
            )
            reference_flow_uuid, product_warnings = _mark_reference_product_exchange(
                process_uuid=process_uuid,
                process_json=process_record,
                exchanges=kept_exchanges,
            )
            report["warnings"].extend([f"{process_uuid}: {msg}" for msg in product_warnings])
            report["filtered_exchanges"].extend([item.model_dump(mode="python") for item in filtered])
            report["filtered_exchange_count"] = len(report["filtered_exchanges"])
            report["imported_exchange_count"] += len(kept_exchanges)

            normalized_json = {
                "process_uuid": process_uuid,
                "process_name": process_record.get("process_name"),
                "process_name_zh": process_record.get("process_name_zh"),
                "process_name_en": process_record.get("process_name_en"),
                "location": process_record.get("location"),
                "process_type": "unit_process",
                "reference_flow_internal_id": process_record.get("reference_flow_internal_id"),
                "reference_flow_source_uuid": process_record.get("reference_flow_source_uuid"),
                "reference_flow_source_name": process_record.get("reference_flow_source_name"),
                "exchanges": kept_exchanges,
                "source": "tidas_bundle_process_import",
            }
            bundle_process_json_by_uuid[process_uuid] = normalized_json
            process_report = ProcessImportReportResponse(
                process_uuid=process_uuid,
                source_process_uuid=None,
                import_mode="locked",
                imported_process_count=1,
                filtered_exchange_count=len(filtered),
                filtered_exchanges=filtered,
                warnings=([ProcessImportWarning(process_uuid=process_uuid, reasons=product_warnings)] if product_warnings else []),
                updated_at=datetime.utcnow(),
            )
            if existing is None:
                report["imported_process_count"] += 1
                report["inserted"] += 1
                if not payload.dry_run:
                    db.add(
                        ReferenceProcess(
                            process_uuid=process_uuid,
                            process_name=str(process_record.get("process_name") or process_uuid),
                            process_name_zh=_safe_str(process_record.get("process_name_zh")),
                            process_name_en=_safe_str(process_record.get("process_name_en")),
                            process_type="unit_process",
                            reference_flow_uuid=reference_flow_uuid,
                            reference_flow_internal_id=_safe_str(process_record.get("reference_flow_internal_id")),
                            process_json=normalized_json,
                            source_file=f"zip://{source_name}/{source_entry}",
                            source_process_uuid=None,
                            import_mode="locked",
                            import_report_json=process_report.model_dump(mode="json"),
                        )
                    )
                continue

            report["imported_process_count"] += 1
            report["updated"] += 1
            if payload.dry_run:
                continue
            existing.process_name = str(process_record.get("process_name") or existing.process_name)
            existing.process_name_zh = _safe_str(process_record.get("process_name_zh"))
            existing.process_name_en = _safe_str(process_record.get("process_name_en"))
            existing.process_type = "unit_process"
            existing.reference_flow_uuid = reference_flow_uuid
            existing.reference_flow_internal_id = _safe_str(process_record.get("reference_flow_internal_id"))
            existing.process_json = normalized_json
            existing.source_file = f"zip://{source_name}/{source_entry}"
            existing.source_process_uuid = None
            existing.import_mode = "locked"
            existing.import_report_json = process_report.model_dump(mode="json")

    filtered_models = [FilteredExchangeEvidence.model_validate(item) for item in report["filtered_exchanges"]]
    report["top_missing_flow_uuids"] = _top_missing_flow_uuids(filtered_models, top_n=10)

    for source_entry, rows, parse_errors in model_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            model_record, err = _extract_tidas_model_record(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_entry}: {err}")
                continue

            model_uuid = str(model_record["model_uuid"])
            if bool(model_record.get("topology_empty")):
                report["model_topology_empty_count"] += 1
                report["warnings"].append(f"{model_uuid}: MODEL_TOPOLOGY_EMPTY")

            unresolved_items = []
            for process_uuid in list(model_record.get("process_refs") or []):
                if process_uuid in bundle_process_uuids:
                    continue
                if db.get(ReferenceProcess, process_uuid) is None:
                    unresolved_items.append(
                        {
                            "model_uuid": model_uuid,
                            "type": "missing_process_reference",
                            "process_uuid": process_uuid,
                            "reason": "referenced process not found in reference_processes",
                        }
                    )
            if unresolved_items:
                report["unresolved_items"].extend(unresolved_items)
                report["unresolved_count"] = len(report["unresolved_items"])

            report["inserted"] += 1
            if payload.dry_run:
                continue
            now = datetime.utcnow()
            model_row = Model(
                name=str(model_record.get("model_name") or model_uuid),
                description=f"Imported from TIDAS bundle ZIP (source_model_uuid={model_uuid})",
                updated_at=now,
            )
            db.add(model_row)
            db.flush()
            report["created_projects"].append({"project_id": str(model_row.id), "name": str(model_row.name)})
            graph_json, graph_unresolved = _build_tidas_graph_from_model_record(
                db=db,
                model_record=model_record,
                process_json_by_uuid=bundle_process_json_by_uuid,
                display_lang=(_safe_str(display_lang) or "").lower() or "zh",
            )
            if graph_unresolved:
                report["unresolved_items"].extend(graph_unresolved)
                report["unresolved_count"] = len(report["unresolved_items"])
            if graph_json is not None:
                _create_project_version_from_graph_json(
                    db=db,
                    project_id=model_row.id,
                    graph_json=graph_json,
                )

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist_tidas_import_report(db, report)
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIDAS_IMPORT_STRICT_FAILED",
                "job_id": persisted.job_id,
                "message": "TIDAS bundle import failed in strict_mode",
                "failed": report["failed"],
                "errors": report["errors"][:50],
            },
        )

    if payload.dry_run:
        db.rollback()
    else:
        db.commit()
        _invalidate_management_caches(projects=True, flows=True, stats=True, reference_processes=True)
    return _persist_tidas_import_report(db, report)


@app.get("/api/import/reports/{job_id}", response_model=TidasImportReportResponse)
@app.get("/import/reports/{job_id}", response_model=TidasImportReportResponse)
def get_tidas_import_report(job_id: str, db: Session = Depends(get_db)) -> TidasImportReportResponse:
    row = db.get(DebugDiagnostic, job_id)
    if row is None or row.diagnostic_type != _TIDAS_IMPORT_DIAGNOSTIC_TYPE:
        raise HTTPException(status_code=404, detail={"code": "IMPORT_REPORT_NOT_FOUND", "message": f"report not found: {job_id}"})
    result_json = row.result_json if isinstance(row.result_json, dict) else {}
    return TidasImportReportResponse.model_validate(result_json)


def list_elementary_flows(db: Session = Depends(get_db)) -> list[FlowRecord]:
    return (
        db.query(FlowRecord)
        .filter(FlowRecord.flow_type == "Elementary flow")
        .order_by(FlowRecord.flow_name.asc())
        .all()
    )


def list_intermediate_flows(db: Session = Depends(get_db)) -> list[FlowRecord]:
    return (
        db.query(FlowRecord)
        .filter(FlowRecord.flow_type.in_(["Product flow", "Waste flow"]))
        .order_by(FlowRecord.flow_name.asc())
        .all()
    )


@app.get("/api/reference/flows/{flow_uuid}", response_model=FlowOut)
def get_flow(flow_uuid: str, db: Session = Depends(get_db)) -> FlowRecord:
    normalized_uuid = (flow_uuid or "").strip().lower()
    item = db.get(FlowRecord, normalized_uuid) if normalized_uuid else None
    if item is None and normalized_uuid:
        item = db.query(FlowRecord).filter(func.lower(FlowRecord.flow_uuid) == normalized_uuid).first()
    if item is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return item


def import_elementary_flows(payload: ImportFlowsRequest, db: Session = Depends(get_db)) -> ImportFlowsResponse:
    result = import_flows_from_file(
        db,
        file_path=payload.file_path,
        sheet_name=payload.sheet_name,
        mapping=payload.mapping,
        replace_existing=payload.replace_existing,
        default_flow_type=payload.default_flow_type or "Elementary flow",
        ef31_flow_index_path=payload.ef31_flow_index_path,
    )
    return ImportFlowsResponse(**result)


def import_intermediate_flows(payload: ImportFlowsRequest, db: Session = Depends(get_db)) -> ImportFlowsResponse:
    result = import_flows_from_file(
        db,
        file_path=payload.file_path,
        sheet_name=payload.sheet_name,
        mapping=payload.mapping,
        replace_existing=payload.replace_existing,
        default_flow_type=payload.default_flow_type or "Product flow",
        ef31_flow_index_path=payload.ef31_flow_index_path,
    )
    return ImportFlowsResponse(**result)


def import_unit_groups(payload: ImportUnitGroupsRequest, db: Session = Depends(get_db)) -> ImportUnitGroupsResponse:
    result = import_unit_groups_from_excel(
        db,
        file_path=payload.file_path,
        replace_existing=payload.replace_existing,
    )
    return ImportUnitGroupsResponse(**result)


def list_unit_groups(db: Session = Depends(get_db)) -> list[UnitGroup]:
    return db.query(UnitGroup).order_by(UnitGroup.name.asc()).all()


@app.get("/api/reference/units", response_model=list[UnitDefinitionOut])
@app.get("/reference/units", response_model=list[UnitDefinitionOut])
def list_units(unit_group: str | None = None, db: Session = Depends(get_db)) -> list[UnitDefinition]:
    query = db.query(UnitDefinition)
    if unit_group:
        query = query.filter(UnitDefinition.unit_group == unit_group)
    return query.order_by(UnitDefinition.unit_group.asc(), UnitDefinition.factor_to_reference.asc()).all()


@app.post("/api/units/convert", response_model=UnitConvertResponse)
@app.post("/units/convert", response_model=UnitConvertResponse)
def convert_units(payload: UnitConvertRequest, db: Session = Depends(get_db)) -> UnitConvertResponse:
    try:
        result = convert_unit_value(
            db,
            value=payload.value,
            from_unit=payload.from_unit,
            to_unit=payload.to_unit,
            unit_group=payload.unit_group,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return UnitConvertResponse(**result)


@app.post("/pts/validate", response_model=PtsValidateResponse)
def validate_pts(payload: PtsValidateRequest) -> PtsValidateResponse:
    validate_graph_contract(payload.graph, require_non_empty=False, allow_pts_nodes=True)
    result = validate_pts_compile(
        graph=payload.graph,
        internal_node_ids=payload.internal_node_ids,
        product_node_ids=payload.product_node_ids,
    )
    return PtsValidateResponse(
        ok=result.ok,
        errors=result.errors,
        warnings=result.warnings,
        matrix_size=result.matrix_size,
        invertible=result.invertible,
    )


@app.post("/api/model/validate-handles", response_model=HandleValidationResponse)
@app.post("/model/validate-handles", response_model=HandleValidationResponse)
def validate_model_handles(payload: HandleValidationRequest) -> HandleValidationResponse:
    normalize_graph_product_flags(payload.graph)
    normalize_graph_edge_port_ids(payload.graph)
    result = analyze_handle_consistency(payload.graph)
    return HandleValidationResponse(
        ok=bool(result.get("ok")),
        issue_count=int(result.get("issue_count") or 0),
        issues=list(result.get("issues") or []),
    )


@app.post("/debug/solver/check-snapshot", dependencies=[Depends(require_debug_access)])
def debug_check_snapshot(
    payload: dict,
    persist: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    snapshot = payload.get("snapshot") if isinstance(payload, dict) else None
    if not isinstance(snapshot, dict):
        raise HTTPException(status_code=422, detail="snapshot is required")
    matrix_diag = _build_snapshot_matrix(snapshot)
    result = {
        "diagnostic_schema_version": "debug-diagnostics-v1",
        "matrix": {
            "invertible": matrix_diag["invertible"],
            "rank": matrix_diag["rank"],
            "determinant": matrix_diag["determinant"],
            "a_matrix_preview": matrix_diag["a_matrix_preview"],
        },
        "singular_reason_classification": matrix_diag["singular_reasons"],
        "suspect_cycles": matrix_diag["suspect_cycles"],
        "evidence": matrix_diag["evidence"],
    }
    persisted_id = persist_debug_diagnostic(
        db=db,
        persist=persist,
        diagnostic_type="solver.check_snapshot",
        payload=payload,
        result=result,
    )
    if persisted_id:
        result["persisted_diagnostic_id"] = persisted_id
    return result


@app.post("/debug/solver/inspect-run-pts", dependencies=[Depends(require_debug_access)])
def debug_inspect_run_pts(
    payload: RunRequest,
    persist: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    normalize_graph_product_flags(payload.graph)
    normalize_graph_edge_port_ids(payload.graph)

    unit_rows = db.query(UnitDefinition).all()
    unit_factor_by_group_and_name: dict[tuple[str, str], float] = {}
    reference_unit_by_group: dict[str, str] = {}
    for row in unit_rows:
        unit_factor_by_group_and_name[(row.unit_group, row.unit_name)] = float(row.factor_to_reference)
        if row.is_reference and row.unit_group not in reference_unit_by_group:
            reference_unit_by_group[row.unit_group] = row.unit_name
    for group in db.query(UnitGroup).all():
        if group.reference_unit and group.name not in reference_unit_by_group:
            reference_unit_by_group[group.name] = group.reference_unit

    normalized_graph = normalize_graph_units_to_reference(
        payload.graph,
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
        reference_unit_by_group=reference_unit_by_group,
    )

    pts_nodes = [node for node in normalized_graph.nodes if node.node_kind == "pts_module"]
    compile_rows: list[PtsCompileArtifact] = []
    compile_summaries: list[dict] = []
    for node in pts_nodes:
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        ports_policy = _get_pts_resource_ports_policy(db=db, project_id=payload.project_id or "", pts_uuid=pts_uuid) if pts_uuid and payload.project_id else None
        try:
            compile_result = compile_pts(normalized_graph, node.id, ports_policy=ports_policy)
        except ValueError as exc:
            _raise_pts_compile_value_error_http(exc, node.id)
        row = PtsCompileArtifact(
            project_id=payload.project_id or "",
            pts_node_id=node.id,
            pts_uuid=str(compile_result.get("pts_uuid") or ""),
            graph_hash=str(compile_result.get("graph_hash") or ""),
            ok=bool(compile_result["validation"].ok),
            matrix_size=int(compile_result.get("matrix_size") or 0),
            invertible=bool(compile_result.get("invertible")),
            errors_json=list(compile_result["validation"].errors),
            warnings_json=list(compile_result["validation"].warnings),
            artifact_json=dict(compile_result.get("artifact") or {}),
        )
        compile_rows.append(row)
        compile_summaries.append(
            {
                "pts_node_id": node.id,
                "ok": row.ok,
                "matrix_size": row.matrix_size,
                "invertible": row.invertible,
                "errors": row.errors_json,
                "warnings": row.warnings_json,
            }
        )

    flattened_graph = build_flattened_graph_for_run_pts(graph=normalized_graph, compile_rows=compile_rows)
    snapshot = to_tiangong_like(flattened_graph)
    matrix_diag = _build_snapshot_matrix(snapshot)

    result = {
        "diagnostic_schema_version": "debug-diagnostics-v1",
        "normalized_graph": normalized_graph.model_dump(mode="python", by_alias=True),
        "flattened_graph": flattened_graph.model_dump(mode="python", by_alias=True),
        "tiangong_like_snapshot": snapshot,
        "a_matrix_preview": matrix_diag["a_matrix_preview"],
        "invertible": matrix_diag["invertible"],
        "determinant": matrix_diag["determinant"],
        "rank": matrix_diag["rank"],
        "suspect_cycles": matrix_diag["suspect_cycles"],
        "pts_compile_summary": compile_summaries,
        "evidence": matrix_diag["evidence"],
    }
    persisted_id = persist_debug_diagnostic(
        db=db,
        persist=persist,
        diagnostic_type="solver.inspect_run_pts",
        payload=payload.model_dump(mode="python", by_alias=True),
        result=result,
        project_id=payload.project_id,
    )
    if persisted_id:
        result["persisted_diagnostic_id"] = persisted_id
    return result


@app.post("/debug/pts/compile-preview", dependencies=[Depends(require_debug_access)])
def debug_pts_compile_preview(
    payload: PtsCompileRequest,
    persist: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    normalize_graph_product_flags(payload.graph)
    normalize_graph_edge_port_ids(payload.graph)
    pts_uuid = str(payload.pts_uuid or "").strip()
    matched_pts_nodes = [
        node
        for node in payload.graph.nodes
        if node.node_kind == "pts_module" and str(node.pts_uuid or node.process_uuid or "").strip() == pts_uuid
    ]
    if not matched_pts_nodes:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "PTS_NODE_NOT_FOUND_BY_UUID",
                "message": f"No PTS node found in graph for pts_uuid={pts_uuid}",
                "evidence": [{"pts_uuid": pts_uuid}],
            },
        )
    if len(matched_pts_nodes) > 1:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "PTS_UUID_NOT_UNIQUE_IN_GRAPH",
                "message": f"Multiple PTS nodes found in graph for pts_uuid={pts_uuid}",
                "evidence": [{"pts_uuid": pts_uuid, "matched_node_ids": [node.id for node in matched_pts_nodes]}],
            },
        )
    pts_node_id = matched_pts_nodes[0].id
    ports_policy = _get_pts_resource_ports_policy(db=db, project_id=payload.project_id, pts_uuid=pts_uuid) if pts_uuid else None
    try:
        compile_result = compile_pts(payload.graph, pts_node_id, ports_policy=ports_policy)
    except ValueError as exc:
        _raise_pts_compile_value_error_http(exc, pts_node_id)
    validation = compile_result["validation"]
    tmp_row = PtsCompileArtifact(
        project_id=payload.project_id,
        pts_node_id=pts_node_id,
        pts_uuid=str(compile_result.get("pts_uuid") or ""),
        graph_hash=str(compile_result.get("graph_hash") or ""),
        ok=bool(validation.ok),
        matrix_size=int(compile_result.get("matrix_size") or 0),
        invertible=bool(compile_result.get("invertible") or False),
        errors_json=list(validation.errors),
        warnings_json=list(validation.warnings),
        artifact_json=dict(compile_result.get("artifact") or {}),
    )
    definition = extract_pts_definition(
        graph=payload.graph,
        pts_node_id=pts_node_id,
        graph_hash=str(compile_result.get("graph_hash") or ""),
    )
    external_preview = build_pts_external_payload(
        project_id=payload.project_id,
        pts_uuid=str(definition.get("pts_uuid") or pts_node_id),
        definition=definition,
        compile_row=tmp_row,
    )

    result = {
        "diagnostic_schema_version": "debug-diagnostics-v1",
        "pts_node_id": pts_node_id,
        "solver_payload": compile_result.get("solver_payload", {}),
        "validation": {
            "ok": validation.ok,
            "errors": validation.errors,
            "warnings": validation.warnings,
            "matrix_size": validation.matrix_size,
            "invertible": validation.invertible,
        },
        "allowed_inventory_input_flow_uuids": compile_result.get("allowed_inventory_input_flow_uuids", []),
        "virtual_processes_preview": (compile_result.get("artifact") or {}).get("virtual_processes", []),
        "external_preview": external_preview,
        "evidence": [
            {
                "pts_node_id": pts_node_id,
                "graph_hash": compile_result.get("graph_hash"),
                "matrix_size": compile_result.get("matrix_size"),
            }
        ],
    }
    persisted_id = persist_debug_diagnostic(
        db=db,
        persist=persist,
        diagnostic_type="pts.compile_preview",
        payload=payload.model_dump(mode="python", by_alias=True),
        result=result,
        project_id=payload.project_id,
        graph_hash=str(compile_result.get("graph_hash") or ""),
    )
    if persisted_id:
        result["persisted_diagnostic_id"] = persisted_id
    return result


@app.get("/debug/pts/{pts_node_id}/artifacts/latest", dependencies=[Depends(require_debug_access)])
def debug_latest_pts_artifacts(
    pts_node_id: str,
    project_id: str,
    persist: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    compile_row = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == project_id, PtsCompileArtifact.pts_node_id == pts_node_id)
        .order_by(PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    if compile_row is None:
        raise HTTPException(status_code=404, detail="PTS compile artifact not found")

    definition_row = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == project_id, PtsDefinition.pts_node_id == pts_node_id)
        .order_by(PtsDefinition.updated_at.desc(), PtsDefinition.created_at.desc())
        .first()
    )
    external_row = None
    if definition_row is not None:
        external_row = (
            db.query(PtsExternalArtifact)
            .filter(
                PtsExternalArtifact.project_id == project_id,
                PtsExternalArtifact.pts_uuid == definition_row.pts_uuid,
                PtsExternalArtifact.graph_hash == compile_row.graph_hash,
            )
            .order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
            .first()
        )

    result = {
        "diagnostic_schema_version": "debug-diagnostics-v1",
        "project_id": project_id,
        "pts_node_id": pts_node_id,
        "graph_hash": compile_row.graph_hash,
        "compile_artifact": compile_row.artifact_json or {},
        "external_artifact": (external_row.artifact_json if external_row is not None else {}),
        "evidence": [
            {
                "pts_node_id": pts_node_id,
                "pts_uuid": (definition_row.pts_uuid if definition_row is not None else None),
                "compile_id": compile_row.id,
                "external_id": (external_row.id if external_row is not None else None),
            }
        ],
    }
    persisted_id = persist_debug_diagnostic(
        db=db,
        persist=persist,
        diagnostic_type="pts.latest_artifacts",
        payload={"project_id": project_id, "pts_node_id": pts_node_id},
        result=result,
        project_id=project_id,
        graph_hash=compile_row.graph_hash,
    )
    if persisted_id:
        result["persisted_diagnostic_id"] = persisted_id
    return result


@app.post("/debug/graph/trace-flow", dependencies=[Depends(require_debug_access)])
def debug_trace_flow(
    payload: dict,
    persist: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    graph_raw = payload.get("graph") if isinstance(payload, dict) else None
    flow_uuid = str(payload.get("flow_uuid") or payload.get("flowUuid") or "") if isinstance(payload, dict) else ""
    if not isinstance(graph_raw, dict) or not flow_uuid:
        raise HTTPException(status_code=422, detail="graph and flow_uuid are required")
    graph = HybridGraph.model_validate(graph_raw)

    node_map = {node.id: node for node in graph.nodes}
    port_rows: list[dict] = []
    for node in graph.nodes:
        for port in [*node.inputs, *node.outputs]:
            if port.flowUuid != flow_uuid:
                continue
            port_rows.append(
                {
                    "node_id": node.id,
                    "node_name": node.name,
                    "direction": port.direction,
                    "port_id": port.id,
                    "unit": port.unit,
                    "amount": port.amount,
                    "type": port.type,
                }
            )

    matching_links = [edge for edge in graph.exchanges if edge.flowUuid == flow_uuid]
    link_rows: list[dict] = []
    provider_count_by_consumer: dict[str, int] = {}
    self_loop = False
    for edge in matching_links:
        if edge.fromNode == edge.toNode:
            self_loop = True
        consumer_key = f"{edge.toNode}::{flow_uuid}"
        provider_count_by_consumer[consumer_key] = provider_count_by_consumer.get(consumer_key, 0) + 1
        link_rows.append(
            {
                "edge_id": edge.id,
                "provider_node_id": edge.fromNode,
                "provider_process_uuid": node_map.get(edge.fromNode).process_uuid if edge.fromNode in node_map else None,
                "consumer_node_id": edge.toNode,
                "consumer_process_uuid": node_map.get(edge.toNode).process_uuid if edge.toNode in node_map else None,
                "flow_uuid": edge.flowUuid,
                "source_handle": edge.sourceHandle,
                "target_handle": edge.targetHandle,
                "source_port_id": edge.source_port_id,
                "target_port_id": edge.target_port_id,
            }
        )

    multi_provider_conflicts = [k for k, v in provider_count_by_consumer.items() if v > 1]
    result = {
        "diagnostic_schema_version": "debug-diagnostics-v1",
        "flow_uuid": flow_uuid,
        "flow_occurrences": port_rows,
        "link_mapping": link_rows,
        "self_supply_cycle_detected": self_loop,
        "multi_provider_conflict_detected": len(multi_provider_conflicts) > 0,
        "evidence": [
            {
                "flow_uuid": flow_uuid,
                "self_supply_cycle_detected": self_loop,
                "multi_provider_conflicts": multi_provider_conflicts,
            }
        ],
    }
    persisted_id = persist_debug_diagnostic(
        db=db,
        persist=persist,
        diagnostic_type="graph.trace_flow",
        payload=payload if isinstance(payload, dict) else {},
        result=result,
    )
    if persisted_id:
        result["persisted_diagnostic_id"] = persisted_id
    return result


@app.get("/debug/run-jobs/{run_id}/diagnostics", dependencies=[Depends(require_debug_access)])
def debug_run_job_diagnostics(
    run_id: str,
    persist: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    run_job = db.get(RunJob, run_id)
    if run_job is None:
        raise HTTPException(status_code=404, detail="Run job not found")

    request_graph_raw = run_job.request_json if isinstance(run_job.request_json, dict) else {}
    request_graph = None
    snapshot = {}
    matrix_diag = {
        "invertible": False,
        "rank": 0,
        "determinant": 0.0,
        "a_matrix_preview": {"rows": 0, "cols": 0, "non_zero_count": 0, "non_zero_entries": []},
        "suspect_cycles": [],
        "evidence": [],
    }
    try:
        request_graph = HybridGraph.model_validate(request_graph_raw)
        snapshot = to_tiangong_like(request_graph)
        matrix_diag = _build_snapshot_matrix(snapshot)
    except Exception:
        pass

    result_json = run_job.result_json if isinstance(run_job.result_json, dict) else {}
    lci_result = result_json.get("lci_result") if isinstance(result_json, dict) else {}
    solver_issues = lci_result.get("issues") if isinstance(lci_result, dict) else []
    failed_classification = []
    if any((isinstance(msg, str) and "invertible" in msg.lower()) for msg in (solver_issues or [])):
        failed_classification.append("matrix_not_invertible")
    if matrix_diag.get("suspect_cycles"):
        failed_classification.append("possible_provider_cycle")
    if not failed_classification and run_job.status != "completed":
        failed_classification.append("unknown_runtime_failure")

    result = {
        "diagnostic_schema_version": "debug-diagnostics-v1",
        "run_id": run_id,
        "status": run_job.status,
        "request_graph": request_graph_raw,
        "flattened_graph": request_graph_raw,
        "snapshot_summary": {
            "process_count": len(snapshot.get("processes", [])) if isinstance(snapshot, dict) else 0,
            "flow_count": len(snapshot.get("flows", [])) if isinstance(snapshot, dict) else 0,
            "link_count": len(snapshot.get("links", [])) if isinstance(snapshot, dict) else 0,
        },
        "solver_error_raw": solver_issues,
        "failure_classification": failed_classification,
        "suggested_fixes": [
            "Check handles and provider mapping with /model/validate-handles",
            "Use /debug/solver/check-snapshot to inspect singular reasons",
        ],
        "a_matrix_preview": matrix_diag.get("a_matrix_preview"),
        "evidence": matrix_diag.get("evidence", []),
    }
    persisted_id = persist_debug_diagnostic(
        db=db,
        persist=persist,
        diagnostic_type="run_jobs.diagnostics",
        payload={"run_id": run_id},
        result=result,
        run_id=run_id,
    )
    if persisted_id:
        result["persisted_diagnostic_id"] = persisted_id
    return result


@app.post("/api/pts/compile", response_model=PtsCompileResponse)
@app.post("/pts/compile", response_model=PtsCompileResponse)
def compile_pts_endpoint(payload: PtsCompileRequest, db: Session = Depends(get_db)) -> PtsCompileResponse:
    validate_graph_contract(payload.graph, require_non_empty=False, allow_pts_nodes=True)
    pts_uuid = str(payload.pts_uuid or "").strip()
    compile_graph = payload.graph
    matched_pts_nodes = [
        node
        for node in compile_graph.nodes
        if node.node_kind == "pts_module" and str(node.pts_uuid or node.process_uuid or "").strip() == pts_uuid
    ]
    if not matched_pts_nodes:
        resource = (
            db.query(PtsResource)
            .filter(PtsResource.project_id == payload.project_id, PtsResource.pts_uuid == pts_uuid)
            .first()
        )
        compile_graph = _build_compile_graph_from_pts_resource(resource) if resource is not None else None
        matched_pts_nodes = [
            node
            for node in (compile_graph.nodes if compile_graph is not None else [])
            if node.node_kind == "pts_module" and str(node.pts_uuid or node.process_uuid or "").strip() == pts_uuid
        ]
        if not matched_pts_nodes:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "PTS_NODE_NOT_FOUND_BY_UUID",
                    "message": f"No PTS node found in graph for pts_uuid={pts_uuid}",
                    "evidence": [{"pts_uuid": pts_uuid}],
                },
            )
    if len(matched_pts_nodes) > 1:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "PTS_UUID_NOT_UNIQUE_IN_GRAPH",
                "message": f"Multiple PTS nodes found in graph for pts_uuid={pts_uuid}",
                "evidence": [{"pts_uuid": pts_uuid, "matched_node_ids": [node.id for node in matched_pts_nodes]}],
            },
        )
    normalize_graph_product_flags(compile_graph)
    normalize_same_flow_uuid_opposite_direction_ports(compile_graph)
    resolved_pts_node_id = matched_pts_nodes[0].id
    ports_policy = _get_pts_resource_ports_policy(db=db, project_id=payload.project_id, pts_uuid=pts_uuid) if pts_uuid else None
    try:
        compile_result = compile_pts(compile_graph, resolved_pts_node_id, ports_policy=ports_policy)
    except ValueError as exc:
        _raise_pts_compile_value_error_http(exc, resolved_pts_node_id)
    definition = extract_pts_definition(
        graph=compile_graph,
        pts_node_id=resolved_pts_node_id,
        graph_hash=compile_result["graph_hash"],
    )
    definition = _apply_pts_resource_policy_override(
        db=db,
        project_id=payload.project_id,
        definition=definition,
    )
    definition_row = upsert_pts_definition(
        db=db,
        project_id=payload.project_id,
        definition=definition,
    )
    row, cached = upsert_pts_compile_artifact(
        db=db,
        project_id=payload.project_id,
        pts_node_id=resolved_pts_node_id,
        force_recompile=payload.force_recompile,
        compile_result=compile_result,
    )
    try:
        external_payload = build_pts_external_payload(
            project_id=payload.project_id,
            pts_uuid=definition_row.pts_uuid,
            definition=definition_row.definition_json or definition,
            compile_row=row,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "PTS_EXTERNAL_PAYLOAD_BUILD_ERROR",
                "message": str(exc),
                "evidence": [{"pts_node_id": resolved_pts_node_id, "compile_id": row.id}],
            },
        ) from exc
    return PtsCompileResponse(
        compile_id=row.id,
        project_id=row.project_id,
        pts_node_id=row.pts_node_id,
        pts_uuid=row.pts_uuid,
        graph_hash=row.graph_hash,
        compile_version=row.compile_version,
        cached=cached,
        ok=row.ok,
        errors=list(row.errors_json or []),
        warnings=list(row.warnings_json or []),
        matrix_size=row.matrix_size,
        invertible=row.invertible,
        artifact=row.artifact_json or {},
        external_preview=external_payload,
    )


@app.get("/api/pts/{pts_uuid}/compiled", response_model=PtsCompiledGetResponse)
@app.get("/pts/{pts_uuid}/compiled", response_model=PtsCompiledGetResponse)
def get_pts_compiled(pts_uuid: str, project_id: str, db: Session = Depends(get_db)) -> PtsCompiledGetResponse:
    definition = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == project_id, PtsDefinition.pts_uuid == pts_uuid)
        .first()
    )
    if definition is None:
        raise HTTPException(status_code=404, detail="PTS definition not found")

    compiled = (
        db.query(PtsCompileArtifact)
        .filter(
            PtsCompileArtifact.project_id == project_id,
            PtsCompileArtifact.pts_node_id == definition.pts_node_id,
        )
        .order_by(PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    if compiled is None:
        raise HTTPException(status_code=404, detail="PTS compiled artifact not found")

    return PtsCompiledGetResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compiled.pts_node_id,
        graph_hash=compiled.graph_hash,
        compile_version=compiled.compile_version,
        ok=compiled.ok,
        errors=list(compiled.errors_json or []),
        warnings=list(compiled.warnings_json or []),
        matrix_size=compiled.matrix_size,
        invertible=compiled.invertible,
        definition=definition.definition_json or {},
        artifact=compiled.artifact_json or {},
    )


@app.get("/api/pts/{pts_uuid}/compiled-external", response_model=PtsCompiledExternalResponse)
@app.get("/pts/{pts_uuid}/compiled-external", response_model=PtsCompiledExternalResponse)
def get_pts_compiled_external(
    pts_uuid: str,
    project_id: str,
    published_version: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> PtsCompiledExternalResponse:
    effective_published_version = published_version
    if effective_published_version is None:
        resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
        if resource is not None and resource.active_published_version is not None:
            effective_published_version = int(resource.active_published_version)
    row = _load_pts_external_artifact(
        db=db,
        project_id=project_id,
        pts_uuid=pts_uuid,
        published_version=effective_published_version,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="PTS external artifact not found")
    payload = row.artifact_json if isinstance(row.artifact_json, dict) else {}
    if not payload.get("pts_uuid"):
        payload = dict(payload)
        payload["pts_uuid"] = str(payload.get("pts_id") or pts_uuid)
    payload = dict(payload)
    payload["published_version"] = int(row.published_version or 0) if row.published_version is not None else None
    payload["source_compile_id"] = str(row.source_compile_id or "") or None
    payload["source_compile_version"] = int(row.source_compile_version or 0) if row.source_compile_version is not None else None
    return PtsCompiledExternalResponse.model_validate(payload)


def _build_pts_resource_out(*, row: PtsResource, db: Session) -> PtsResourceOut:
    pts_graph = dict(row.pts_graph_json or {})
    if pts_graph:
        _enrich_graph_flow_name_en(pts_graph, db=db)
    ports_policy = _sanitize_pts_ports_policy(
        ports_policy=dict(row.ports_policy_json or {}),
        pts_graph=pts_graph,
    )
    shell_node = dict(row.shell_node_json or {})
    if shell_node:
        _enrich_node_ports_flow_name_en(shell_node, db=db)
    return PtsResourceOut(
        project_id=str(row.project_id),
        pts_uuid=str(row.pts_uuid),
        name=str(row.name or "") or None,
        pts_node_id=str(row.pts_node_id or "") or None,
        latest_graph_hash=str(row.latest_graph_hash or "") or None,
        compiled_graph_hash=str(row.compiled_graph_hash or "") or None,
        latest_compile_version=(int(row.latest_compile_version) if row.latest_compile_version is not None else None),
        latest_published_version=(int(row.latest_published_version) if row.latest_published_version is not None else None),
        active_published_version=(int(row.active_published_version) if row.active_published_version is not None else None),
        published_at=row.published_at,
        ports_policy=ports_policy,
        shell_node=shell_node,
        pts_graph=pts_graph,
    )


def _build_pts_shell_snapshot_from_external(*, row: PtsResource, external: PtsExternalArtifact) -> dict:
    shell_node = dict(row.shell_node_json or {})
    projected_inputs, projected_outputs = _build_projected_pts_ports_from_external(external)
    return {
        "id": str(shell_node.get("id") or row.pts_node_id or external.pts_node_id or ""),
        "node_kind": _normalize_pts_shell_node_kind(shell_node.get("node_kind")),
        "mode": str(shell_node.get("mode") or "normalized"),
        "lci_role": shell_node.get("lci_role"),
        "pts_uuid": str(shell_node.get("pts_uuid") or row.pts_uuid or external.pts_uuid),
        "pts_published_version": (int(external.published_version) if external.published_version is not None else None),
        "pts_published_artifact_id": str(external.id),
        "process_uuid": str(shell_node.get("process_uuid") or row.pts_uuid or external.pts_uuid),
        "name": str(shell_node.get("name") or row.name or "PTS"),
        "location": str(shell_node.get("location") or "Plant Internal"),
        "reference_product": str(shell_node.get("reference_product") or "PTS模块输出"),
        "allocation_method": shell_node.get("allocation_method"),
        "inputs": [port.model_dump(mode="python", by_alias=True) for port in projected_inputs],
        "outputs": [port.model_dump(mode="python", by_alias=True) for port in projected_outputs],
        "emissions": [],
    }


def _get_or_materialize_pts_resource_row(*, db: Session, pts_uuid: str) -> PtsResource:
    row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    if row is not None:
        return row
    definition_row = db.query(PtsDefinition).filter(PtsDefinition.pts_uuid == pts_uuid).first()
    if definition_row is None:
        raise HTTPException(status_code=404, detail="PTS resource not found")
    latest_compile = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == definition_row.project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    latest_external = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == definition_row.project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.published_version.desc(), PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .first()
    )
    row = _upsert_pts_resource_from_definition(
        db=db,
        definition_row=definition_row,
        compile_row=latest_compile,
        external_row=latest_external,
    )
    db.commit()
    db.refresh(row)
    return row


def _resolve_pts_shell_snapshot_for_resource(*, db: Session, row: PtsResource) -> dict:
    project_id = str(row.project_id or "").strip()
    pts_uuid = str(row.pts_uuid or "").strip()
    if project_id and pts_uuid:
        external = _load_pts_active_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
        )
        if external is not None:
            return _build_pts_shell_snapshot_from_external(row=row, external=external)
    return dict(row.shell_node_json or {})


def _build_pts_unpack_port_bindings(*, pts_graph: dict, ports_policy: dict, shell_node: dict) -> list[PtsUnpackPortBinding]:
    if not isinstance(pts_graph, dict) or not pts_graph:
        return []
    try:
        graph = HybridGraph.model_validate(
            {
                "functionalUnit": str(pts_graph.get("functionalUnit") or "PTS"),
                "nodes": pts_graph.get("nodes") or [],
                "exchanges": pts_graph.get("exchanges") or [],
                "metadata": pts_graph.get("metadata") or {},
            }
        )
    except Exception:
        return []

    input_ports_by_node_flow: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    output_ports_by_node_flow: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    product_output_ports_by_node_flow: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    for node in graph.nodes:
        node_id = str(node.id or "")
        for port in node.inputs:
            flow_uuid = str(port.flowUuid or "")
            if not node_id or not flow_uuid:
                continue
            input_ports_by_node_flow[(node_id, flow_uuid)].append(port)
        for port in node.outputs:
            flow_uuid = str(port.flowUuid or "")
            if not node_id or not flow_uuid:
                continue
            output_ports_by_node_flow[(node_id, flow_uuid)].append(port)
            if bool(port.isProduct):
                product_output_ports_by_node_flow[(node_id, flow_uuid)].append(port)

    def _unique_ports(ports: list[FlowPort]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for port in ports:
            port_id = str(port.id or "").strip()
            if not port_id or port_id in seen:
                continue
            seen.add(port_id)
            result.append(port_id)
        return result

    def _match_input_candidates(shell_port: dict) -> list[str]:
        flow_uuid = str(shell_port.get("flowUuid") or "").strip()
        source_node_id = str(shell_port.get("sourceNodeId") or "").strip()
        candidates: list[FlowPort] = []
        if source_node_id:
            candidates.extend(input_ports_by_node_flow.get((source_node_id, flow_uuid), []))
        else:
            for row in (ports_policy.get("inputs") or []):
                if not isinstance(row, dict):
                    continue
                if str(row.get("flow_uuid") or row.get("flowUuid") or "").strip() != flow_uuid:
                    continue
                row_node_id = str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip()
                if not row_node_id:
                    continue
                candidates.extend(input_ports_by_node_flow.get((row_node_id, flow_uuid), []))
        return _unique_ports(candidates)

    def _match_output_candidates(shell_port: dict) -> list[str]:
        flow_uuid = str(shell_port.get("flowUuid") or "").strip()
        source_node_id = str(shell_port.get("sourceNodeId") or "").strip()
        prefer_product = bool(shell_port.get("isProduct"))
        candidates: list[FlowPort] = []
        if source_node_id:
            indexed = product_output_ports_by_node_flow if prefer_product else output_ports_by_node_flow
            candidates.extend(indexed.get((source_node_id, flow_uuid), []))
            if not candidates and prefer_product:
                candidates.extend(output_ports_by_node_flow.get((source_node_id, flow_uuid), []))
        else:
            for row in (ports_policy.get("outputs") or []):
                if not isinstance(row, dict):
                    continue
                if str(row.get("flow_uuid") or row.get("flowUuid") or "").strip() != flow_uuid:
                    continue
                row_node_id = str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip()
                if not row_node_id:
                    continue
                indexed = product_output_ports_by_node_flow if prefer_product else output_ports_by_node_flow
                matches = indexed.get((row_node_id, flow_uuid), [])
                if not matches and prefer_product:
                    matches = output_ports_by_node_flow.get((row_node_id, flow_uuid), [])
                candidates.extend(matches)
        return _unique_ports(candidates)

    bindings: list[PtsUnpackPortBinding] = []
    for direction, shell_ports in (
        ("input", list(shell_node.get("inputs") or [])),
        ("output", list(shell_node.get("outputs") or [])),
    ):
        for shell_port in shell_ports:
            if not isinstance(shell_port, dict):
                continue
            shell_port_id = str(shell_port.get("id") or shell_port.get("port_key") or shell_port.get("product_key") or "").strip()
            flow_uuid = str(shell_port.get("flowUuid") or "").strip()
            if not shell_port_id or not flow_uuid:
                continue
            internal_port_ids = _match_input_candidates(shell_port) if direction == "input" else _match_output_candidates(shell_port)
            bindings.append(
                PtsUnpackPortBinding(
                    shell_port_id=shell_port_id,
                    flow_uuid=flow_uuid,
                    direction=direction,
                    source_node_id=str(shell_port.get("sourceNodeId") or "").strip() or None,
                    source_process_uuid=str(shell_port.get("sourceProcessUuid") or "").strip() or None,
                    internal_port_id=(internal_port_ids[0] if len(internal_port_ids) == 1 else None),
                    internal_port_ids=internal_port_ids,
                )
            )
    return bindings


def _pts_ports_policy_has_rows(policy: dict | None) -> bool:
    if not isinstance(policy, dict):
        return False
    inputs = policy.get("inputs")
    outputs = policy.get("outputs")
    return bool((isinstance(inputs, list) and len(inputs) > 0) or (isinstance(outputs, list) and len(outputs) > 0))


def _count_pts_policy_rows(policy: dict | None) -> tuple[int, int]:
    if not isinstance(policy, dict):
        return 0, 0
    inputs = policy.get("inputs")
    outputs = policy.get("outputs")
    return (
        len(inputs) if isinstance(inputs, list) else 0,
        len(outputs) if isinstance(outputs, list) else 0,
    )


def _count_shell_ports(shell_node: dict | None) -> tuple[int, int]:
    if not isinstance(shell_node, dict):
        return 0, 0
    inputs = shell_node.get("inputs")
    outputs = shell_node.get("outputs")
    return (
        len(inputs) if isinstance(inputs, list) else 0,
        len(outputs) if isinstance(outputs, list) else 0,
    )


def _raise_if_pack_finalize_obviously_reentered(
    *,
    pts_uuid: str,
    payload: PtsPackFinalizeRequest,
    derived_ports_policy: dict,
    existing_row: PtsResource | None,
) -> None:
    if existing_row is None:
        return
    existing_published_version = (
        int(existing_row.active_published_version)
        if existing_row.active_published_version is not None
        else (
            int(existing_row.latest_published_version)
            if existing_row.latest_published_version is not None
            else None
        )
    )
    if existing_published_version is None:
        return
    existing_shell = dict(existing_row.shell_node_json or {})
    existing_shell_inputs, existing_shell_outputs = _count_shell_ports(existing_shell)
    if existing_shell_inputs == 0 and existing_shell_outputs == 0:
        return

    incoming_policy_inputs, incoming_policy_outputs = _count_pts_policy_rows(derived_ports_policy)
    same_graph_hash = bool(
        str(payload.latest_graph_hash or "").strip()
        and str(existing_row.latest_graph_hash or "").strip()
        and str(payload.latest_graph_hash or "").strip() == str(existing_row.latest_graph_hash or "").strip()
    )
    if not same_graph_hash:
        return

    # Conservative rule:
    # if a packed PTS already has a published shell and the incoming wrap payload is
    # no broader than the current shell, it is very likely a mistaken re-entry from
    # the root shell instead of the original wrap source.
    if (
        incoming_policy_inputs <= existing_shell_inputs
        and incoming_policy_outputs <= existing_shell_outputs
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "PTS_PACK_FINALIZE_REENTRY_FORBIDDEN",
                "message": (
                    f"pack-finalize for pts_uuid={pts_uuid} looks like a second pass built from the current "
                    "published shell instead of the original wrap source. Do not re-run pack-finalize for an "
                    "already packed node on ordinary root refresh/click flows."
                ),
                "evidence": [
                    {
                        "pts_uuid": pts_uuid,
                        "latest_graph_hash": str(existing_row.latest_graph_hash or "") or None,
                        "existing_published_version": existing_published_version,
                        "existing_shell_input_count": existing_shell_inputs,
                        "existing_shell_output_count": existing_shell_outputs,
                        "incoming_policy_input_count": incoming_policy_inputs,
                        "incoming_policy_output_count": incoming_policy_outputs,
                    }
                ],
            },
        )


def _upsert_pts_resource_from_definition(
    *,
    db: Session,
    definition_row: PtsDefinition,
    compile_row: PtsCompileArtifact | None = None,
    external_row: PtsExternalArtifact | None = None,
) -> PtsResource:
    row = db.query(PtsResource).filter(PtsResource.pts_uuid == definition_row.pts_uuid).first()
    definition = dict(definition_row.definition_json or {})
    pts_graph = dict(definition.get("pts_graph") or {})
    ports_policy = _sanitize_pts_ports_policy(
        ports_policy=dict(definition_row.ports_policy_json or {}),
        pts_graph=pts_graph,
    )
    shell_node = dict(definition.get("shell_node") or {})
    if row is None:
        row = PtsResource(
            project_id=str(definition_row.project_id),
            pts_uuid=str(definition_row.pts_uuid),
            name=str(definition.get("name") or shell_node.get("name") or "") or None,
            pts_node_id=str(definition_row.pts_node_id or "") or None,
            latest_graph_hash=str(definition_row.latest_graph_hash or "") or None,
            pts_graph_json=pts_graph,
            ports_policy_json=ports_policy,
            shell_node_json=shell_node,
        )
        db.add(row)
    else:
        row.project_id = str(definition_row.project_id)
        row.name = str(definition.get("name") or shell_node.get("name") or row.name or "") or None
        row.pts_node_id = str(definition_row.pts_node_id or "") or None
        row.latest_graph_hash = str(definition_row.latest_graph_hash or "") or None
        row.pts_graph_json = pts_graph
        row.ports_policy_json = ports_policy
        row.shell_node_json = shell_node

    if compile_row is not None:
        row.compiled_graph_hash = str(compile_row.graph_hash or "") or None
        row.latest_compile_version = int(compile_row.compile_version or 0) or row.latest_compile_version
    if external_row is not None:
        row.latest_published_version = int(external_row.published_version or 0) or row.latest_published_version
        row.published_at = external_row.updated_at or external_row.created_at
        if row.active_published_version is None:
            row.active_published_version = row.latest_published_version
    return row


def _build_compile_graph_from_pts_resource(row: PtsResource) -> HybridGraph | None:
    pts_graph = dict(row.pts_graph_json or {})
    shell_node = dict(row.shell_node_json or {})
    if not pts_graph or not shell_node:
        return None
    shell_payload = {
        "id": str(shell_node.get("id") or row.pts_node_id or ""),
        "node_kind": _normalize_pts_shell_node_kind(shell_node.get("node_kind")),
        "mode": str(shell_node.get("mode") or "normalized"),
        "lci_role": shell_node.get("lci_role"),
        "pts_uuid": str(shell_node.get("pts_uuid") or row.pts_uuid),
        "process_uuid": str(shell_node.get("process_uuid") or row.pts_uuid),
        "name": str(shell_node.get("name") or row.name or "PTS"),
        "location": str(shell_node.get("location") or "Plant Internal"),
        "reference_product": str(shell_node.get("reference_product") or "PTS模块输出"),
        "allocation_method": shell_node.get("allocation_method"),
        "inputs": shell_node.get("inputs") if isinstance(shell_node.get("inputs"), list) else [],
        "outputs": shell_node.get("outputs") if isinstance(shell_node.get("outputs"), list) else [],
        "emissions": shell_node.get("emissions") if isinstance(shell_node.get("emissions"), list) else [],
    }
    internal_nodes = pts_graph.get("nodes") if isinstance(pts_graph.get("nodes"), list) else []
    internal_edges = pts_graph.get("exchanges") if isinstance(pts_graph.get("exchanges"), list) else []
    canvas_name = str((pts_graph.get("metadata") or {}).get("name") or shell_node.get("name") or "PTS")
    graph_payload = {
        "functionalUnit": str(pts_graph.get("functionalUnit") or "PTS"),
        "nodes": [shell_payload],
        "exchanges": [],
        "metadata": {
            "canvases": [
                {
                    "id": str((pts_graph.get("metadata") or {}).get("canvas_id") or f"canvas::{row.pts_uuid}"),
                    "kind": "pts_internal",
                    "parentPtsNodeId": str((pts_graph.get("metadata") or {}).get("parentPtsNodeId") or row.pts_node_id or shell_node.get("id") or ""),
                    "name": canvas_name,
                    "nodes": internal_nodes,
                    "edges": internal_edges,
                }
            ]
        },
    }
    try:
        return HybridGraph.model_validate(graph_payload)
    except Exception:
        return None


@app.get("/api/pts/{pts_uuid}", response_model=PtsResourceOut)
@app.get("/pts/{pts_uuid}", response_model=PtsResourceOut)
def get_pts_resource(pts_uuid: str, db: Session = Depends(get_db)) -> PtsResourceOut:
    row = _get_or_materialize_pts_resource_row(db=db, pts_uuid=pts_uuid)
    return _build_pts_resource_out(row=row, db=db)


@app.post("/api/pts/{pts_uuid}/unpack", response_model=PtsUnpackResponse)
@app.post("/pts/{pts_uuid}/unpack", response_model=PtsUnpackResponse)
def unpack_pts_resource(
    pts_uuid: str,
    payload: PtsUnpackRequest | None = None,
    db: Session = Depends(get_db),
) -> PtsUnpackResponse:
    row = _get_or_materialize_pts_resource_row(db=db, pts_uuid=pts_uuid)
    resource = _build_pts_resource_out(row=row, db=db)
    project_id = str((payload.project_id if payload else None) or resource.project_id or row.project_id or "").strip()
    pts_graph = dict(resource.pts_graph or {})
    if not is_graph_non_empty(pts_graph):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "PTS_UNPACK_GRAPH_EMPTY",
                "message": f"PTS pts_uuid={pts_uuid} has no internal graph to unpack.",
                "evidence": {
                    "pts_uuid": pts_uuid,
                    "project_id": project_id or None,
                },
            },
        )
    shell_node = _resolve_pts_shell_snapshot_for_resource(db=db, row=row)
    port_bindings = _build_pts_unpack_port_bindings(
        pts_graph=pts_graph,
        ports_policy=dict(resource.ports_policy or {}),
        shell_node=shell_node,
    )
    resolved_pts_node_id = str((payload.pts_node_id if payload else None) or resource.pts_node_id or row.pts_node_id or shell_node.get("id") or "").strip() or None
    return PtsUnpackResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        pts_node_id=resolved_pts_node_id,
        shell_node=shell_node,
        pts_graph=pts_graph,
        port_bindings=port_bindings,
        resource=resource,
    )


@app.put("/api/pts/{pts_uuid}", response_model=PtsResourceOut)
@app.put("/pts/{pts_uuid}", response_model=PtsResourceOut)
def put_pts_resource(pts_uuid: str, payload: PtsResourceUpdateRequest, db: Session = Depends(get_db)) -> PtsResourceOut:
    pts_graph = dict(payload.pts_graph or {})
    _enrich_market_process_input_sources_in_graph_json(pts_graph)
    normalized_shell_node = dict(payload.shell_node or {})
    if normalized_shell_node:
        normalized_shell_node["node_kind"] = _normalize_pts_shell_node_kind(normalized_shell_node.get("node_kind"))
    explicit_ports_policy = payload.ports_policy if "ports_policy" in payload.model_fields_set else None
    normalized_ports_policy = (
        dict(explicit_ports_policy or {})
        if explicit_ports_policy is not None
        else _normalize_pts_ports_policy_from_graph(
            pts_graph=pts_graph,
            fallback_policy=None,
        )
    )
    normalized_ports_policy = _sanitize_pts_ports_policy(
        ports_policy=normalized_ports_policy,
        pts_graph=pts_graph,
    )
    if pts_graph:
        source_pts_graph = json.loads(json.dumps(pts_graph, ensure_ascii=False))
        validated_pts_graph = {
            "functionalUnit": str(pts_graph.get("functionalUnit") or "PTS"),
            "nodes": pts_graph.get("nodes") or [],
            "exchanges": pts_graph.get("exchanges") or [],
            "metadata": pts_graph.get("metadata") or {},
        }
        _validate_process_name_uniqueness_for_graph_json(
            graph_json=validated_pts_graph,
            scope_label=f"pts_graph:{pts_uuid}",
        )
        validated_graph = HybridGraph.model_validate(validated_pts_graph)
        normalize_graph_product_flags(validated_graph)
        normalize_graph_edge_port_ids(validated_graph)
        pts_graph = _restore_graph_node_positions(
            source_graph_json=source_pts_graph,
            normalized_graph_json=validated_graph.model_dump(mode="python"),
        )
    row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    if row is None:
        row = PtsResource(
            project_id=payload.project_id,
            pts_uuid=pts_uuid,
            name=payload.name,
            pts_node_id=payload.pts_node_id,
            latest_graph_hash=payload.latest_graph_hash,
            active_published_version=payload.active_published_version,
            pts_graph_json=pts_graph,
            ports_policy_json=normalized_ports_policy,
            shell_node_json=normalized_shell_node,
        )
        db.add(row)
    else:
        row.project_id = payload.project_id
        row.name = payload.name
        row.pts_node_id = payload.pts_node_id
        row.latest_graph_hash = payload.latest_graph_hash
        row.active_published_version = payload.active_published_version
        row.pts_graph_json = pts_graph
        row.ports_policy_json = normalized_ports_policy
        row.shell_node_json = normalized_shell_node
    latest_external = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == payload.project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .first()
    )
    latest_compile = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == payload.project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    if latest_compile is not None:
        row.compiled_graph_hash = str(latest_compile.graph_hash or "")
        row.latest_compile_version = int(latest_compile.compile_version or 0) or row.latest_compile_version
    if latest_external is not None:
        row.published_at = latest_external.updated_at or latest_external.created_at
        row.latest_published_version = int(latest_external.published_version or 0) or row.latest_published_version
        if row.active_published_version is None:
            row.active_published_version = row.latest_published_version
    db.commit()
    db.refresh(row)
    return _build_pts_resource_out(row=row, db=db)


@app.post("/api/pts/{pts_uuid}/pack-finalize", response_model=PtsPackFinalizeResponse)
@app.post("/pts/{pts_uuid}/pack-finalize", response_model=PtsPackFinalizeResponse)
def pack_finalize_pts_resource(
    pts_uuid: str,
    payload: PtsPackFinalizeRequest,
    db: Session = Depends(get_db),
) -> PtsPackFinalizeResponse:
    derived_ports_policy = _normalize_pts_ports_policy_from_graph(
        pts_graph=dict(payload.pts_graph or {}),
        fallback_policy=None,
    )
    existing_row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    _raise_if_pack_finalize_obviously_reentered(
        pts_uuid=pts_uuid,
        payload=payload,
        derived_ports_policy=derived_ports_policy,
        existing_row=existing_row,
    )
    save_payload = PtsResourceUpdateRequest(
        project_id=payload.project_id,
        name=payload.name,
        pts_node_id=payload.pts_node_id,
        latest_graph_hash=payload.latest_graph_hash,
        pts_graph=dict(payload.pts_graph or {}),
        ports_policy=derived_ports_policy,
        shell_node=dict(payload.shell_node or {}),
    )
    put_pts_resource(pts_uuid=pts_uuid, payload=save_payload, db=db)

    compile_request = PtsCompileRequest(
        graph=HybridGraph.model_validate(
            {
                "functionalUnit": "PTS",
                "nodes": [],
                "exchanges": [],
                "metadata": {},
            }
        ),
        pts_uuid=pts_uuid,
        project_id=payload.project_id,
        force_recompile=payload.force_recompile,
    )
    compile_response = compile_pts_endpoint(payload=compile_request, db=db)

    publish_request = PtsPublishRequest(
        project_id=payload.project_id,
        compile_id=compile_response.compile_id,
        set_active=payload.set_active,
    )
    publish_response = publish_pts_artifact(pts_uuid=pts_uuid, payload=publish_request, db=db)

    row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    external = db.query(PtsExternalArtifact).filter(PtsExternalArtifact.id == publish_response.published_artifact_id).first()
    if row is None or external is None:
        raise HTTPException(status_code=500, detail="PTS pack/finalize completed but resource refresh failed")

    shell_snapshot = _build_pts_shell_snapshot_from_external(row=row, external=external)
    port_id_map = _build_pts_port_id_map(shell_node=shell_snapshot)
    default_visible_port_ids = _resolve_default_visible_port_ids(
        shell_node=shell_snapshot,
        hints=list(payload.default_visible_port_hints or []),
    )
    if default_visible_port_ids:
        shell_snapshot = _apply_default_visible_port_ids_to_shell_node(
            shell_node=shell_snapshot,
            default_visible_port_ids=default_visible_port_ids,
        )
        external_payload = _apply_default_visible_port_ids_to_external_payload(
            payload=dict(external.artifact_json or {}),
            shell_node=shell_snapshot,
            default_visible_port_ids=default_visible_port_ids,
        )
        external.artifact_json = external_payload
    row.shell_node_json = dict(shell_snapshot)
    db.commit()
    db.refresh(row)
    if default_visible_port_ids:
        db.refresh(external)

    return PtsPackFinalizeResponse(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=str(row.pts_node_id or "") or None,
        compile_id=str(compile_response.compile_id),
        compile_version=(int(compile_response.compile_version) if compile_response.compile_version is not None else None),
        published_artifact_id=str(publish_response.published_artifact_id),
        published_version=int(publish_response.published_version),
        active_published_version=(int(publish_response.active_published_version) if publish_response.active_published_version is not None else None),
        graph_hash=str(publish_response.graph_hash),
        shell_node=shell_snapshot,
        port_id_map=port_id_map,
        default_visible_port_ids=default_visible_port_ids,
        warnings=list(publish_response.warnings or []),
    )


def _resolve_compile_row_for_publish(*, db: Session, pts_uuid: str, payload: PtsPublishRequest) -> PtsCompileArtifact:
    query = db.query(PtsCompileArtifact).filter(
        PtsCompileArtifact.project_id == payload.project_id,
        PtsCompileArtifact.pts_uuid == pts_uuid,
    )
    if payload.compile_id:
        row = query.filter(PtsCompileArtifact.id == payload.compile_id).first()
    elif payload.compile_version is not None:
        row = query.filter(PtsCompileArtifact.compile_version == payload.compile_version).first()
    elif payload.graph_hash:
        row = query.filter(PtsCompileArtifact.graph_hash == payload.graph_hash).first()
    else:
        row = query.order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc()).first()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "PTS_COMPILE_ARTIFACT_NOT_FOUND",
                "message": f"No compile artifact found for pts_uuid={pts_uuid}",
            },
        )
    return row


def _bind_pts_published_versions_for_graph(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        external = _load_pts_active_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
        )
        if external is None:
            node.pts_published_version = None
            node.pts_published_artifact_id = None
            continue
        node.pts_published_version = int(external.published_version) if external.published_version is not None else None
        node.pts_published_artifact_id = str(external.id)


@app.post("/api/pts/{pts_uuid}/publish", response_model=PtsPublishResponse)
@app.post("/pts/{pts_uuid}/publish", response_model=PtsPublishResponse)
def publish_pts_artifact(pts_uuid: str, payload: PtsPublishRequest, db: Session = Depends(get_db)) -> PtsPublishResponse:
    definition_row = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == payload.project_id, PtsDefinition.pts_uuid == pts_uuid)
        .first()
    )
    if definition_row is None:
        raise HTTPException(status_code=404, detail="PTS definition not found")

    compile_row = _resolve_compile_row_for_publish(db=db, pts_uuid=pts_uuid, payload=payload)
    definition = dict(definition_row.definition_json or {})
    definition = _apply_pts_resource_policy_override(
        db=db,
        project_id=payload.project_id,
        definition=definition,
    )
    external_payload = build_pts_external_payload(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        definition=definition,
        compile_row=compile_row,
    )
    _enrich_pts_external_payload_flow_name_en(external_payload, db=db)
    published_row = upsert_pts_external_artifact(
        db=db,
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        graph_hash=compile_row.graph_hash,
        payload=external_payload,
        source_compile_id=str(compile_row.id),
        source_compile_version=(int(compile_row.compile_version) if compile_row.compile_version is not None else None),
        set_active=payload.set_active,
    )
    resource = _upsert_pts_resource_from_definition(
        db=db,
        definition_row=definition_row,
        compile_row=compile_row,
        external_row=published_row,
    )
    resource.shell_node_json = _build_pts_shell_snapshot_from_external(row=resource, external=published_row)
    if payload.set_active:
        resource.active_published_version = int(published_row.published_version or 0) or resource.active_published_version
    db.commit()
    db.refresh(resource)
    active_version = int(resource.active_published_version) if resource and resource.active_published_version is not None else None
    publish_warnings = _build_pts_publish_warnings(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        pts_graph=dict(resource.pts_graph_json or {}) if resource is not None else {},
        external_payload=external_payload,
    )
    return PtsPublishResponse(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        published_artifact_id=str(published_row.id),
        published_version=int(published_row.published_version or 0),
        source_compile_id=str(compile_row.id),
        source_compile_version=(int(compile_row.compile_version) if compile_row.compile_version is not None else None),
        graph_hash=compile_row.graph_hash,
        active_published_version=active_version,
        published_at=published_row.updated_at or published_row.created_at,
        external_preview=external_payload,
        warnings=publish_warnings,
    )


@app.get("/api/pts/{pts_uuid}/compile-history", response_model=PtsCompileHistoryResponse)
@app.get("/pts/{pts_uuid}/compile-history", response_model=PtsCompileHistoryResponse)
def get_pts_compile_history(pts_uuid: str, project_id: str, db: Session = Depends(get_db)) -> PtsCompileHistoryResponse:
    rows = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .all()
    )
    return PtsCompileHistoryResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        items=[
            PtsVersionItem(
                id=str(row.id),
                graph_hash=str(row.graph_hash),
                version=(int(row.compile_version) if row.compile_version is not None else None),
                created_at=row.created_at,
                updated_at=row.updated_at,
                ok=bool(row.ok),
                matrix_size=int(row.matrix_size),
                invertible=bool(row.invertible),
            )
            for row in rows
        ],
    )


@app.get("/api/pts/{pts_uuid}/published-history", response_model=PtsPublishedHistoryResponse)
@app.get("/pts/{pts_uuid}/published-history", response_model=PtsPublishedHistoryResponse)
def get_pts_published_history(pts_uuid: str, project_id: str, db: Session = Depends(get_db)) -> PtsPublishedHistoryResponse:
    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    rows = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.published_version.desc(), PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .all()
    )
    return PtsPublishedHistoryResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        active_published_version=(int(resource.active_published_version) if resource and resource.active_published_version is not None else None),
        items=[
            PtsVersionItem(
                id=str(row.id),
                graph_hash=str(row.graph_hash),
                version=(int(row.published_version) if row.published_version is not None else None),
                created_at=row.created_at,
                updated_at=row.updated_at,
                source_compile_id=(str(row.source_compile_id) if row.source_compile_id else None),
                source_compile_version=(int(row.source_compile_version) if row.source_compile_version is not None else None),
            )
            for row in rows
        ],
    )


@app.get("/api/pts/{pts_uuid}/ports")
@app.get("/pts/{pts_uuid}/ports")
def get_pts_ports(
    pts_uuid: str,
    project_id: str,
    published_version: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    # Open-source contract: main-graph and PTS shell reads must always consume the
    # current active/latest published artifact. Historical published-version binding
    # is reserved for future SaaS/commercial version management and is intentionally
    # ignored here to prevent stale frontend callers from pulling old shell ports.
    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    effective_published_version = None
    if resource is not None and resource.active_published_version is not None:
        effective_published_version = int(resource.active_published_version)
    row = _load_pts_external_artifact(
        db=db,
        project_id=project_id,
        pts_uuid=pts_uuid,
        published_version=effective_published_version,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="PTS external artifact not found")
    payload = dict(row.artifact_json or {}) if isinstance(row.artifact_json, dict) else {}
    _enrich_pts_external_payload_flow_name_en(payload, db=db)
    frontend_ports = _build_frontend_ports_from_external_payload(payload)
    _enrich_frontend_ports_flow_name_en(frontend_ports, db=db)
    return {
        "project_id": project_id,
        "pts_uuid": pts_uuid,
        "pts_node_id": payload.get("pts_node_id"),
        "graph_hash": payload.get("graph_hash"),
        "published_version": (int(row.published_version) if row.published_version is not None else None),
        "source_compile_version": (int(row.source_compile_version) if row.source_compile_version is not None else None),
        "ports": frontend_ports,
        "output_virtual_process_bindings": payload.get("output_virtual_process_bindings", []),
        "virtual_processes": payload.get("virtual_processes", []),
    }


def delete_elementary_flow(flow_uuid: str, db: Session = Depends(get_db)) -> DeleteFlowsResponse:
    item = db.get(FlowRecord, flow_uuid)
    if item is None:
        return DeleteFlowsResponse(deleted=0, by_flow_uuid=flow_uuid)
    if item.flow_type != "Elementary flow":
        raise HTTPException(status_code=400, detail=f"Flow {flow_uuid} is not Elementary flow")
    db.delete(item)
    db.commit()
    return DeleteFlowsResponse(deleted=1, by_flow_uuid=flow_uuid)


def delete_intermediate_flow(flow_uuid: str, db: Session = Depends(get_db)) -> DeleteFlowsResponse:
    item = db.get(FlowRecord, flow_uuid)
    if item is None:
        return DeleteFlowsResponse(deleted=0, by_flow_uuid=flow_uuid)
    if item.flow_type not in {"Product flow", "Waste flow"}:
        raise HTTPException(status_code=400, detail=f"Flow {flow_uuid} is not Intermediate flow")
    db.delete(item)
    db.commit()
    return DeleteFlowsResponse(deleted=1, by_flow_uuid=flow_uuid)


def delete_elementary_flows(
    only_non_ef31: bool = False,
    ef31_flow_index_path: str | None = None,
    db: Session = Depends(get_db),
) -> DeleteFlowsResponse:
    query = db.query(FlowRecord).filter(FlowRecord.flow_type == "Elementary flow")

    candidates = query.all()
    if not candidates:
        return DeleteFlowsResponse(
            deleted=0,
            by_flow_type="Elementary flow",
            only_non_ef31=only_non_ef31,
        )

    allow_uuids: set[str] | None = None
    if only_non_ef31:
        allow_uuids = load_ef31_flow_uuid_set(ef31_flow_index_path or settings.nebula_lca_ef31_dir)

    deleted = 0
    for item in candidates:
        if allow_uuids is not None and item.flow_uuid in allow_uuids:
            continue
        db.delete(item)
        deleted += 1
    db.commit()

    return DeleteFlowsResponse(
        deleted=deleted,
        by_flow_type="Elementary flow",
        only_non_ef31=only_non_ef31,
    )


def delete_intermediate_flows(
    db: Session = Depends(get_db),
) -> DeleteFlowsResponse:
    deleted = (
        db.query(FlowRecord)
        .filter(FlowRecord.flow_type.in_(["Product flow", "Waste flow"]))
        .delete(synchronize_session=False)
    )
    db.commit()

    return DeleteFlowsResponse(
        deleted=deleted,
        by_flow_type="Intermediate flow",
        only_non_ef31=False,
    )


def create_model(payload: ModelCreateRequest, db: Session = Depends(get_db)) -> ModelCreateResponse:
    normalize_graph_product_flags(payload.graph)

    normalized_name = payload.name.strip()
    if not normalized_name:
        raise HTTPException(status_code=400, detail="Project name cannot be empty")
    model = db.query(Model).filter(Model.name == normalized_name).first()
    if model is None:
        model = Model(name=normalized_name)
        db.add(model)
        db.flush()

    _canonicalize_pts_nodes_for_main_graph_save(db=db, project_id=model.id, graph=payload.graph)
    validate_graph_contract(payload.graph, require_non_empty=True, allow_pts_nodes=True)
    validate_graph_flow_type_contract(payload.graph, db=db, stage="save_model")
    validate_graph_port_names_against_flow_catalog(payload.graph, db=db, stage="save_model")

    graph_hash = _compute_graph_hash_from_graph(payload.graph)
    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is not None:
        latest_has_hash = bool(str(latest_row.graph_hash or "").strip())
        latest_hash = _resolve_model_version_graph_hash(latest_row, assign_if_missing=True)
        if not latest_has_hash:
            db.commit()
        if latest_hash == graph_hash:
            pts_compile_summary = _compile_pts_on_save_if_needed(
                db=db,
                project_id=model.id,
                graph=payload.graph,
                compile_on_save=True,
            )
            return ModelCreateResponse(
                project_id=model.id,
                version=latest_row.version,
                created_at=latest_row.created_at,
                created_new_version=False,
                graph_hash=graph_hash,
                message="内容未变化，未创建新版本",
                **pts_compile_summary,
            )

    latest_version = (
        db.query(func.max(ModelVersion.version))
        .filter(ModelVersion.model_id == model.id)
        .scalar()
    )
    next_version = (latest_version or 0) + 1

    persisted_graph_json = payload.graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(persisted_graph_json, db=db)
    normalized_graph_json = _normalize_graph_json_for_storage(persisted_graph_json)
    version = ModelVersion(
        model_id=model.id,
        version=next_version,
        graph_hash=graph_hash,
        hybrid_graph_json=normalized_graph_json,
    )
    model.updated_at = datetime.utcnow()
    db.add(version)
    db.commit()
    db.refresh(version)
    pts_compile_summary = _compile_pts_on_save_if_needed(
        db=db,
        project_id=model.id,
        graph=payload.graph,
        compile_on_save=True,
    )

    return ModelCreateResponse(
        project_id=model.id,
        version=version.version,
        created_at=version.created_at,
        created_new_version=True,
        graph_hash=graph_hash,
        **pts_compile_summary,
    )


@app.post("/projects", response_model=ProjectOut)
def create_project(payload: ProjectCreateRequest, db: Session = Depends(get_db)) -> ProjectOut:
    _ensure_projects_management_schema()
    project_name = payload.name.strip()
    if not project_name:
        raise HTTPException(status_code=400, detail="Project name cannot be empty")

    existing = db.query(Model).filter(Model.name == project_name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Project name already exists")

    now = datetime.utcnow()
    model = Model(
        name=project_name,
        reference_product=_safe_str(payload.reference_product),
        functional_unit=_safe_str(payload.functional_unit),
        system_boundary=_safe_str(payload.system_boundary),
        time_representativeness=_safe_str(payload.time_representativeness),
        geography=_safe_str(payload.geography),
        description=_safe_str(payload.description),
        status=_normalize_project_status("active"),
        updated_at=now,
    )
    db.add(model)
    db.commit()
    db.refresh(model)
    return _build_project_out(model, latest=None)


@app.get("/projects", response_model=list[ProjectOut])
def list_projects(db: Session = Depends(get_db)) -> list[ProjectOut]:
    models = db.query(Model).order_by(Model.created_at.desc()).all()
    latest_by_project = _latest_version_by_project_id(db)
    return [_build_project_out(model, latest_by_project.get(str(model.id))) for model in models]


@app.get("/api/projects/{project_id}", response_model=ProjectOut)
@app.get("/projects/{project_id}", response_model=ProjectOut)
def get_project(project_id: str, db: Session = Depends(get_db)) -> ProjectOut:
    model = get_model_or_404(db, project_id)
    latest = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    return _build_project_out(model, latest)


@app.post("/api/projects/{project_id}/sync-flow-names", response_model=ProjectFlowNameSyncResponse)
@app.post("/projects/{project_id}/sync-flow-names", response_model=ProjectFlowNameSyncResponse)
def sync_project_flow_names(project_id: str, db: Session = Depends(get_db)) -> ProjectFlowNameSyncResponse:
    model = get_model_or_404(db, project_id)
    (
        latest_version,
        synced_port_count,
        synced_edge_count,
        cleared_pts_compile_count,
        cleared_pts_external_count,
        cleared_pts_definition_count,
    ) = _sync_project_latest_version_flow_names(db=db, project_id=model.id)
    db.commit()
    _invalidate_management_caches(projects=True, stats=True)
    return ProjectFlowNameSyncResponse(
        project_id=model.id,
        synced=bool(synced_port_count or synced_edge_count),
        latest_version=latest_version,
        synced_port_count=synced_port_count,
        synced_edge_count=synced_edge_count,
        cleared_pts_compile_count=cleared_pts_compile_count,
        cleared_pts_external_count=cleared_pts_external_count,
        cleared_pts_definition_count=cleared_pts_definition_count,
    )


@app.get("/api/projects", response_model=PaginatedProjectsResponse)
def list_projects_api(
    search: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    recent: bool = Query(default=False),
    limit: int | None = Query(default=None, ge=1, le=200),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> PaginatedProjectsResponse:
    search_key = (search or "").strip().lower()
    cache_key = (
        f"projects:v1:rev={_cache_revision('projects')}:search={search_key}:"
        f"page={page}:page_size={page_size}:recent={int(bool(recent))}:limit={limit or ''}"
    )
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_PROJECTS_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _is_if_none_match_hit(if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    query = db.query(Model)
    if search and search.strip():
        token = f"%{search.strip().lower()}%"
        query = query.filter(
            func.lower(Model.name).like(token)
            | func.lower(func.coalesce(Model.reference_product, "")).like(token)
            | func.lower(func.coalesce(Model.functional_unit, "")).like(token)
            | func.lower(func.coalesce(Model.system_boundary, "")).like(token)
            | func.lower(func.coalesce(Model.time_representativeness, "")).like(token)
            | func.lower(func.coalesce(Model.geography, "")).like(token)
            | func.lower(func.coalesce(Model.description, "")).like(token)
        )

    ordered = query.order_by(Model.updated_at.desc(), Model.created_at.desc())
    if recent:
        effective_limit = limit or page_size
        models = ordered.limit(effective_limit).all()
        total = len(models)
        page = 1
        page_size = effective_limit
    else:
        total = query.count()
        models = (
            ordered
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
    latest_by_project = _latest_version_by_project_id(db)
    items = [_build_project_out(model, latest_by_project.get(str(model.id))) for model in models]
    result = PaginatedProjectsResponse(items=items, total=total, page=page, page_size=page_size)
    payload = result.model_dump(mode="json")
    etag = _build_etag_for_payload(payload)
    _cache_set(cache_key, {"payload": payload, "etag": etag})
    if _is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})


@app.post("/api/projects", response_model=ProjectOut)
def create_project_api(payload: ProjectCreateRequest, db: Session = Depends(get_db)) -> ProjectOut:
    project_name = payload.name.strip()
    if not project_name:
        raise HTTPException(status_code=400, detail={"code": "INVALID_REQUEST", "message": "Project name cannot be empty"})
    existing = db.query(Model).filter(Model.name == project_name).first()
    if existing:
        raise HTTPException(
            status_code=409,
            detail={"code": "DUPLICATE_PROJECT_NAME", "message": f"Project name already exists: {project_name}"},
        )
    created = create_project(payload=payload, db=db)
    _invalidate_management_caches(projects=True, stats=True)
    return created


@app.patch("/api/projects/{project_id}", response_model=ProjectOut)
def update_project_api(project_id: str, payload: ProjectUpdateRequest, db: Session = Depends(get_db)) -> ProjectOut:
    model = db.query(Model).filter(Model.id == project_id).first()
    if model is None:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": f"Project not found: {project_id}"})

    if payload.name is not None:
        new_name = payload.name.strip()
        if not new_name:
            raise HTTPException(status_code=400, detail={"code": "INVALID_REQUEST", "message": "Project name cannot be empty"})
        existing = db.query(Model).filter(Model.name == new_name, Model.id != project_id).first()
        if existing:
            raise HTTPException(
                status_code=409,
                detail={"code": "DUPLICATE_PROJECT_NAME", "message": f"Project name already exists: {new_name}"},
            )
        model.name = new_name

    for field_name in (
        "reference_product",
        "functional_unit",
        "system_boundary",
        "time_representativeness",
        "geography",
        "description",
    ):
        value = getattr(payload, field_name)
        if value is not None:
            setattr(model, field_name, _safe_str(value))
    if payload.status is not None:
        model.status = _normalize_project_status(payload.status)
    model.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(model)
    _invalidate_management_caches(projects=True, stats=True)
    latest = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    return _build_project_out(model, latest)


@app.post("/api/projects/{project_id}/duplicate", response_model=ProjectOut)
def duplicate_project_api(
    project_id: str,
    payload: ProjectDuplicateRequest | None = None,
    db: Session = Depends(get_db),
) -> ProjectOut:
    source = db.query(Model).filter(Model.id == project_id).first()
    if source is None:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": f"Project not found: {project_id}"})
    source_latest = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == source.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    base_name = _safe_str(payload.name) if payload else None
    if not base_name:
        base_name = f"{source.name}_copy"
    candidate = base_name
    suffix = 1
    while db.query(Model).filter(Model.name == candidate).first() is not None:
        suffix += 1
        candidate = f"{base_name}_{suffix}"

    now = datetime.utcnow()
    duplicated = Model(
        name=candidate,
        reference_product=source.reference_product,
        functional_unit=source.functional_unit,
        system_boundary=source.system_boundary,
        time_representativeness=source.time_representativeness,
        geography=source.geography,
        description=source.description,
        status=source.status or "active",
        updated_at=now,
    )
    db.add(duplicated)
    db.flush()
    if source_latest is not None:
        new_version = ModelVersion(
            model_id=duplicated.id,
            version=1,
            graph_hash=_resolve_model_version_graph_hash(source_latest, assign_if_missing=False),
            hybrid_graph_json=source_latest.hybrid_graph_json if isinstance(source_latest.hybrid_graph_json, dict) else {},
        )
        db.add(new_version)
    db.commit()
    db.refresh(duplicated)
    _invalidate_management_caches(projects=True, stats=True)
    latest = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == duplicated.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    return _build_project_out(duplicated, latest)


@app.delete("/api/projects/{project_id}", response_model=DeleteProjectResponse)
def delete_project_api(project_id: str, db: Session = Depends(get_db)) -> DeleteProjectResponse:
    source = db.query(Model).filter(Model.id == project_id).first()
    if source is None:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": f"Project not found: {project_id}"})
    return delete_project(project_id=project_id, db=db)


@app.delete("/projects/{project_id}", response_model=DeleteProjectResponse)
def delete_project(project_id: str, db: Session = Depends(get_db)) -> DeleteProjectResponse:
    model = get_model_or_404(db, project_id)
    version_rows = db.query(ModelVersion.id).filter(ModelVersion.model_id == model.id).all()
    version_ids = [row[0] for row in version_rows]

    cleared = 0
    if version_ids:
        cleared = (
            db.query(RunJob)
            .filter(RunJob.model_version_id.in_(version_ids))
            .update({RunJob.model_version_id: None}, synchronize_session=False)
        )

    deleted_versions = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .delete(synchronize_session=False)
    )
    db.delete(model)
    db.commit()
    _invalidate_management_caches(projects=True, stats=True)

    return DeleteProjectResponse(
        project_id=project_id,
        deleted_models=1,
        deleted_versions=deleted_versions,
        cleared_run_job_refs=cleared,
    )


@app.get("/api/reference/processes/catalog", response_model=ReferenceProcessCatalogResponse)
@app.get("/reference/processes/catalog", response_model=ReferenceProcessCatalogResponse)
def list_reference_processes_catalog(
    search: str | None = Query(default=None),
    q: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    target_kind: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> ReferenceProcessCatalogResponse:
    normalized_target_kind = _validate_target_kind_or_400(target_kind)
    effective_keyword = (keyword or q or search or "").strip()
    search_key = effective_keyword.lower()
    cache_key = (
        f"reference_processes_catalog:v1:rev={_cache_revision('reference_processes_catalog')}:"
        f"target_kind={normalized_target_kind or ''}:search={search_key}:page={page}:page_size={page_size}"
    )
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_REFERENCE_PROCESS_CATALOG_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _is_if_none_match_hit(if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    query = db.query(ReferenceProcess).filter(ReferenceProcess.process_json.is_not(None))
    if normalized_target_kind == "unit_process":
        query = query.filter(
            (ReferenceProcess.process_type.is_(None))
            | (func.trim(ReferenceProcess.process_type) == "")
            | (ReferenceProcess.process_type == "unit_process")
        )
    elif normalized_target_kind == "market_process":
        query = query.filter(ReferenceProcess.process_type == "market_process")
    elif normalized_target_kind == "lci_dataset":
        query = query.filter(ReferenceProcess.process_type.in_(["lci", "lci_dataset"]))
    elif normalized_target_kind == "pts_module":
        query = query.filter(ReferenceProcess.process_type.in_(["pts", "pts_module"]))

    if effective_keyword:
        token = f"%{effective_keyword.lower()}%"
        query = query.filter(
            func.lower(ReferenceProcess.process_uuid).like(token)
            | func.lower(func.coalesce(ReferenceProcess.process_name, "")).like(token)
            | func.lower(func.coalesce(ReferenceProcess.process_name_zh, "")).like(token)
            | func.lower(func.coalesce(ReferenceProcess.process_name_en, "")).like(token)
        )

    total = query.count()
    rows = (
        query.order_by(ReferenceProcess.updated_at.desc(), ReferenceProcess.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    flow_names = {row.flow_uuid: row.flow_name for row in db.query(FlowRecord.flow_uuid, FlowRecord.flow_name).all()}
    valid_flow_uuids = _flow_uuid_set_cached(db)

    items: list[ReferenceProcessCatalogItem] = []
    for row in rows:
        process_json = row.process_json if isinstance(row.process_json, dict) else {}
        exchanges_raw = process_json.get("exchanges")
        exchanges = [ex for ex in exchanges_raw if isinstance(ex, dict)] if isinstance(exchanges_raw, list) else []
        unmatched = 0
        for ex in exchanges:
            flow_uuid = _to_stripped(ex.get("flow_uuid"))
            if not flow_uuid or flow_uuid not in valid_flow_uuids:
                unmatched += 1
        ref_uuid = _safe_str(row.reference_flow_uuid)
        process_kind = _normalize_process_kind(row.process_type)
        items.append(
            ReferenceProcessCatalogItem(
                process_uuid=row.process_uuid,
                process_name=_safe_str(row.process_name_zh) or _safe_str(row.process_name_en) or row.process_name,
                process_name_en=_safe_str(row.process_name_en),
                process_kind=process_kind,
                source_kind=_safe_str(row.process_type),
                suggested_kind=process_kind,
                reference_flow_uuid=ref_uuid,
                reference_flow_name=flow_names.get(ref_uuid or ""),
                reference_flow_internal_id=_safe_str(process_json.get("reference_flow_internal_id")),
                exchange_count=len(exchanges),
                unmatched_exchange_count=unmatched,
            )
        )
    result = ReferenceProcessCatalogResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        query_echo={
            "keyword": effective_keyword or None,
            "target_kind": normalized_target_kind,
            "page": page,
            "page_size": page_size,
        },
    )
    payload = result.model_dump(mode="json")
    etag = _build_etag_for_payload(payload)
    _cache_set(cache_key, {"payload": payload, "etag": etag})
    if _is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})


@app.post("/api/reference/processes/import", response_model=ImportReferenceProcessesResponse)
@app.post("/reference/processes/import", response_model=ImportReferenceProcessesResponse)
def import_reference_processes(
    payload: ImportReferenceProcessesRequest,
    db: Session = Depends(get_db),
) -> ImportReferenceProcessesResponse:
    target_kind = _validate_target_kind_or_400(payload.target_kind)
    if target_kind != "unit_process":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TARGET_KIND_NOT_IMPLEMENTED",
                "message": f"target_kind={target_kind} is reserved but not implemented yet for Tiangong source processes.",
            },
        )

    source_ids = [str(pid).strip() for pid in payload.process_uuids if str(pid).strip()]
    if not source_ids:
        raise HTTPException(status_code=400, detail={"code": "INVALID_REQUEST", "message": "process_uuids cannot be empty"})

    valid_flow_uuids = _flow_uuid_set_cached(db)
    flow_meta_by_uuid = _flow_meta_by_uuid_cached(db)
    imported_process_count = 0
    filtered_exchanges: list[FilteredExchangeEvidence] = []
    warning_by_process: dict[str, list[str]] = defaultdict(list)
    imported_processes: list[ImportedProcessDetail] = []

    for source_process_uuid in source_ids:
        source_row = db.get(ReferenceProcess, source_process_uuid)
        if source_row is None:
            warning_by_process[source_process_uuid].append("source process not found")
            continue
        source_json = source_row.process_json if isinstance(source_row.process_json, dict) else None
        if source_json is None:
            warning_by_process[source_process_uuid].append("process_json missing; skipped")
            continue

        process_name_zh = _safe_str(source_json.get("process_name_zh")) or _safe_str(source_row.process_name_zh)
        process_name_en = _safe_str(source_json.get("process_name_en")) or _safe_str(source_row.process_name_en)
        process_name = process_name_zh or process_name_en or source_row.process_name or source_process_uuid
        reference_flow_internal_id = _safe_str(source_json.get("reference_flow_internal_id")) or _safe_str(source_row.reference_flow_internal_id)

        cloned_json = json.loads(json.dumps(source_json))
        raw_exchanges = cloned_json.get("exchanges")
        exchanges = [ex for ex in raw_exchanges if isinstance(ex, dict)] if isinstance(raw_exchanges, list) else []

        if payload.import_mode == "locked":
            if not payload.replace_existing:
                warning_by_process[source_process_uuid].append("replace_existing=false in locked mode; skipped")
                continue
            target_uuid = source_process_uuid
        else:
            target_uuid = str(uuid.uuid4())

        kept_exchanges, filtered = _filter_exchanges_with_evidence(
            process_uuid=target_uuid,
            exchanges=exchanges,
            valid_flow_uuids=valid_flow_uuids,
        )
        filtered_exchanges.extend(filtered)
        reference_flow_uuid, product_warnings = _mark_reference_product_exchange(
            process_uuid=target_uuid,
            process_json=cloned_json,
            exchanges=kept_exchanges,
        )
        warning_by_process[target_uuid].extend(product_warnings)

        cloned_json["process_uuid"] = target_uuid
        cloned_json["process_type"] = target_kind
        cloned_json["exchanges"] = kept_exchanges

        target_row: ReferenceProcess
        if payload.import_mode == "locked":
            target_row = source_row
        else:
            target_row = ReferenceProcess(
                process_uuid=target_uuid,
                process_name=process_name,
                process_name_zh=process_name_zh,
                process_name_en=process_name_en,
                process_type=target_kind,
                reference_flow_uuid=reference_flow_uuid,
                reference_flow_internal_id=reference_flow_internal_id,
                process_json=cloned_json,
                source_file=source_row.source_file,
                source_process_uuid=source_process_uuid,
                import_mode=payload.import_mode,
            )
            db.add(target_row)

        if payload.import_mode == "locked":
            target_row.process_name = process_name
            target_row.process_name_zh = process_name_zh
            target_row.process_name_en = process_name_en
            target_row.process_type = target_kind
            target_row.reference_flow_uuid = reference_flow_uuid
            target_row.reference_flow_internal_id = reference_flow_internal_id
            target_row.process_json = cloned_json
            target_row.source_process_uuid = None
            target_row.import_mode = payload.import_mode

        process_warnings = warning_by_process.get(target_uuid, [])
        report = ProcessImportReportResponse(
            process_uuid=target_uuid,
            source_process_uuid=(source_process_uuid if payload.import_mode == "editable_clone" else None),
            import_mode=payload.import_mode,
            imported_process_count=1,
            filtered_exchange_count=len(filtered),
            filtered_exchanges=filtered,
            warnings=(
                [ProcessImportWarning(process_uuid=target_uuid, reasons=process_warnings)]
                if process_warnings
                else []
            ),
            updated_at=datetime.utcnow(),
        )
        target_row.import_report_json = report.model_dump(mode="json")
        inputs, outputs = _build_imported_process_ports(
            exchanges=kept_exchanges,
            flow_meta_by_uuid=flow_meta_by_uuid,
        )
        imported_processes.append(
            ImportedProcessDetail(
                process_uuid=target_uuid,
                source_process_uuid=(source_process_uuid if payload.import_mode == "editable_clone" else None),
                import_mode=payload.import_mode,
                process_kind=target_kind,
                process_name=process_name,
                location=_safe_str(cloned_json.get("location")) or "GLO",
                reference_flow_uuid=reference_flow_uuid,
                reference_flow_internal_id=reference_flow_internal_id,
                inputs=inputs,
                outputs=outputs,
            )
        )
        imported_process_count += 1

    db.commit()
    _invalidate_management_caches(stats=True, reference_processes=True)

    warnings: list[ProcessImportWarning] = []
    for process_uuid, reasons in warning_by_process.items():
        dedup_reasons = sorted({str(reason).strip() for reason in reasons if str(reason).strip()})
        if not dedup_reasons:
            continue
        warnings.append(ProcessImportWarning(process_uuid=process_uuid, reasons=dedup_reasons))

    return ImportReferenceProcessesResponse(
        target_kind=target_kind,
        imported_process_count=imported_process_count,
        filtered_exchange_count=len(filtered_exchanges),
        filtered_process_uuid_basis="imported_process_uuid",
        filtered_exchanges=filtered_exchanges,
        warnings=warnings,
        imported_processes=imported_processes,
    )


@app.get("/api/reference/processes/{process_uuid}/import-report", response_model=ProcessImportReportResponse)
@app.get("/reference/processes/{process_uuid}/import-report", response_model=ProcessImportReportResponse)
def get_reference_process_import_report(
    process_uuid: str,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> ProcessImportReportResponse:
    cache_key = f"reference_process_report:v1:rev={_cache_revision('reference_process_report')}:process_uuid={process_uuid}"
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_REFERENCE_PROCESS_REPORT_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _is_if_none_match_hit(if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    row = db.get(ReferenceProcess, process_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})

    report_json = row.import_report_json if isinstance(row.import_report_json, dict) else {}
    result: ProcessImportReportResponse
    if not report_json:
        result = ProcessImportReportResponse(
            process_uuid=row.process_uuid,
            source_process_uuid=_safe_str(row.source_process_uuid),
            import_mode=_normalize_import_mode_value(row.import_mode),
            imported_process_count=0,
            filtered_exchange_count=0,
            filtered_exchanges=[],
            warnings=[],
            updated_at=row.updated_at,
        )
        payload = result.model_dump(mode="json")
        etag = _build_etag_for_payload(payload)
        _cache_set(cache_key, {"payload": payload, "etag": etag})
        if _is_if_none_match_hit(if_none_match, etag):
            return Response(status_code=304, headers={"ETag": etag})
        return JSONResponse(content=payload, headers={"ETag": etag})

    filtered_items_raw = report_json.get("filtered_exchanges")
    warnings_raw = report_json.get("warnings")
    filtered_items: list[FilteredExchangeEvidence] = []
    warnings_items: list[ProcessImportWarning] = []

    if isinstance(filtered_items_raw, list):
        for item in filtered_items_raw:
            if not isinstance(item, dict):
                continue
            try:
                filtered_items.append(FilteredExchangeEvidence.model_validate(item))
            except Exception:  # noqa: BLE001
                continue

    if isinstance(warnings_raw, list):
        for item in warnings_raw:
            if not isinstance(item, dict):
                continue
            try:
                warnings_items.append(ProcessImportWarning.model_validate(item))
            except Exception:  # noqa: BLE001
                continue

    result = ProcessImportReportResponse(
        process_uuid=_safe_str(report_json.get("process_uuid")) or row.process_uuid,
        source_process_uuid=_safe_str(report_json.get("source_process_uuid")) or _safe_str(row.source_process_uuid),
        import_mode=_normalize_import_mode_value(report_json.get("import_mode")) or _normalize_import_mode_value(row.import_mode),
        imported_process_count=int(report_json.get("imported_process_count") or 0),
        filtered_exchange_count=int(report_json.get("filtered_exchange_count") or len(filtered_items)),
        filtered_exchanges=filtered_items,
        warnings=warnings_items,
        updated_at=row.updated_at,
    )
    payload = result.model_dump(mode="json")
    etag = _build_etag_for_payload(payload)
    _cache_set(cache_key, {"payload": payload, "etag": etag})
    if _is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})


@app.get("/api/reference/processes/{process_uuid}/filtered-exchanges", response_model=ProcessFilteredExchangesResponse)
@app.get("/reference/processes/{process_uuid}/filtered-exchanges", response_model=ProcessFilteredExchangesResponse)
def get_reference_process_filtered_exchanges(
    process_uuid: str,
    db: Session = Depends(get_db),
) -> ProcessFilteredExchangesResponse:
    row = db.get(ReferenceProcess, process_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})
    report_json = row.import_report_json if isinstance(row.import_report_json, dict) else {}
    filtered_raw = report_json.get("filtered_exchanges") if isinstance(report_json, dict) else []
    filtered: list[FilteredExchangeEvidence] = []
    if isinstance(filtered_raw, list):
        for item in filtered_raw:
            if not isinstance(item, dict):
                continue
            try:
                filtered.append(FilteredExchangeEvidence.model_validate(item))
            except Exception:  # noqa: BLE001
                continue
    return ProcessFilteredExchangesResponse(
        process_uuid=process_uuid,
        filtered_exchange_count=len(filtered),
        filtered_exchanges=filtered,
    )


@app.get("/api/reference/flows/missing/summary", response_model=MissingFlowSummaryResponse)
@app.get("/reference/flows/missing/summary", response_model=MissingFlowSummaryResponse)
def get_reference_flow_missing_summary(
    top: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> MissingFlowSummaryResponse:
    missing_count: Counter[str] = Counter()
    process_set_by_flow: dict[str, set[str]] = defaultdict(set)
    rows = db.query(ReferenceProcess.process_uuid, ReferenceProcess.import_report_json).all()
    for process_uuid, import_report_json in rows:
        report_json = import_report_json if isinstance(import_report_json, dict) else {}
        filtered_raw = report_json.get("filtered_exchanges")
        if not isinstance(filtered_raw, list):
            continue
        for item in filtered_raw:
            if not isinstance(item, dict):
                continue
            reason = _safe_str(item.get("reason"))
            flow_uuid = _safe_str(item.get("flow_uuid"))
            if not flow_uuid:
                continue
            if reason and reason != "flow_uuid not found in flow catalog":
                continue
            missing_count[flow_uuid] += 1
            process_set_by_flow[flow_uuid].add(str(process_uuid))
    items = [
        TidasMissingFlowSummaryItem(
            flow_uuid=flow_uuid,
            missing_count=count,
            process_count=len(process_set_by_flow.get(flow_uuid, set())),
        )
        for flow_uuid, count in missing_count.most_common(top)
    ]
    return MissingFlowSummaryResponse(items=items)


@app.get("/api/processes", response_model=PaginatedProcessesResponse)
def list_processes_api(
    search: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    type: str | None = Query(default=None),
    include_legacy: bool = Query(default=False),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> PaginatedProcessesResponse:
    allowed_types = {None, "unit_process", "market_process"}
    if type not in allowed_types:
        raise HTTPException(status_code=400, detail={"code": "INVALID_REQUEST", "message": "Invalid process type"})

    search_key = (search or "").strip().lower()
    cache_key = (
        f"processes:v1:rev={_cache_revision('processes')}:search={search_key}:page={page}:page_size={page_size}:"
        f"type={type or ''}:include_legacy={str(include_legacy).lower()}"
    )
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_PROCESSES_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _is_if_none_match_hit(if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    query = db.query(ReferenceProcess)
    if not include_legacy:
        query = query.filter(ReferenceProcess.process_json.is_not(None))
    if type:
        query = query.filter(ReferenceProcess.process_type == type)
    if search and search.strip():
        token = f"%{search.strip().lower()}%"
        query = query.filter(
            func.lower(ReferenceProcess.process_uuid).like(token)
            | func.lower(func.coalesce(ReferenceProcess.process_name, "")).like(token)
            | func.lower(func.coalesce(ReferenceProcess.process_name_zh, "")).like(token)
            | func.lower(func.coalesce(ReferenceProcess.process_name_en, "")).like(token)
        )

    total = query.count()
    rows = (
        query.order_by(ReferenceProcess.updated_at.desc(), ReferenceProcess.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    flow_names = {
        row.flow_uuid: row.flow_name
        for row in db.query(FlowRecord.flow_uuid, FlowRecord.flow_name).all()
    }

    used_in_projects_by_process: dict[str, int] = defaultdict(int)
    project_usage: dict[str, set[str]] = defaultdict(set)
    for model, latest in _latest_graphs_with_project_meta(db):
        graph_json = latest.hybrid_graph_json if isinstance(latest.hybrid_graph_json, dict) else {}
        nodes = graph_json.get("nodes")
        if not isinstance(nodes, list):
            continue
        for node in nodes:
            if not isinstance(node, dict):
                continue
            pid = _safe_str(node.get("process_uuid"))
            if pid:
                project_usage[pid].add(str(model.id))
    used_in_projects_by_process = {pid: len(projects) for pid, projects in project_usage.items()}

    items: list[ProcessListItem] = []
    for row in rows:
        process_json = row.process_json if isinstance(row.process_json, dict) else {}
        exchanges = process_json.get("exchanges")
        input_count = 0
        output_count = 0
        if isinstance(exchanges, list):
            for ex in exchanges:
                if not isinstance(ex, dict):
                    continue
                if str(ex.get("direction") or "").strip().lower() == "input":
                    input_count += 1
                else:
                    output_count += 1
        ref_uuid, ref_name = _derive_reference_flow_display(
            process_json=process_json,
            reference_flow_uuid=_safe_str(row.reference_flow_uuid),
            reference_flow_name=flow_names.get(_safe_str(row.reference_flow_uuid) or "", None),
            reference_flow_internal_id=_safe_str(row.reference_flow_internal_id)
            or _safe_str(process_json.get("reference_flow_internal_id")),
        )
        items.append(
            ProcessListItem(
                process_uuid=row.process_uuid,
                process_name=_safe_str(row.process_name_zh) or _safe_str(row.process_name_en) or row.process_name,
                process_name_en=_safe_str(row.process_name_en),
                type=_safe_str(row.process_type) or "unit_process",
                reference_flow_uuid=ref_uuid,
                reference_flow_internal_id=_safe_str(row.reference_flow_internal_id)
                or _safe_str(process_json.get("reference_flow_internal_id")),
                reference_flow_name=ref_name,
                input_count=input_count,
                output_count=output_count,
                used_in_projects=int(used_in_projects_by_process.get(row.process_uuid, 0)),
                balance_status="unchecked",
                last_modified=row.updated_at,
            )
        )
    result = PaginatedProcessesResponse(items=items, total=total, page=page, page_size=page_size)
    payload = result.model_dump(mode="json")
    etag = _build_etag_for_payload(payload)
    _cache_set(cache_key, {"payload": payload, "etag": etag})
    if _is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})


@app.get("/api/processes/{process_uuid}", response_model=ProcessDetailResponse)
def get_process_detail_api(process_uuid: str, db: Session = Depends(get_db)) -> ProcessDetailResponse:
    row = db.get(ReferenceProcess, process_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})
    process_json = row.process_json if isinstance(row.process_json, dict) else None
    ref_name = None
    raw_ref_uuid = _safe_str(row.reference_flow_uuid)
    if raw_ref_uuid:
        flow_row = db.get(FlowRecord, raw_ref_uuid)
        if flow_row is not None:
            ref_name = flow_row.flow_name
    ref_uuid, ref_name = _derive_reference_flow_display(
        process_json=process_json,
        reference_flow_uuid=raw_ref_uuid,
        reference_flow_name=ref_name,
        reference_flow_internal_id=_safe_str(row.reference_flow_internal_id)
        or _safe_str((process_json or {}).get("reference_flow_internal_id") if isinstance(process_json, dict) else None),
    )
    return ProcessDetailResponse(
        process_uuid=row.process_uuid,
        process_name=_safe_str(row.process_name_zh) or _safe_str(row.process_name_en) or row.process_name,
        process_name_zh=_safe_str(row.process_name_zh),
        process_name_en=_safe_str(row.process_name_en),
        type=_safe_str(row.process_type) or "unit_process",
        reference_flow_uuid=ref_uuid,
        reference_flow_internal_id=_safe_str(row.reference_flow_internal_id)
        or _safe_str((row.process_json or {}).get("reference_flow_internal_id") if isinstance(row.process_json, dict) else None),
        reference_flow_name=ref_name,
        process_json=process_json,
        source_file=_safe_str(row.source_file),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@app.delete("/api/processes/{process_uuid}", response_model=DeleteProcessResponse)
def delete_process_api(process_uuid: str, db: Session = Depends(get_db)) -> DeleteProcessResponse:
    row = db.get(ReferenceProcess, process_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})
    db.delete(row)
    db.commit()
    _invalidate_management_caches(stats=True, reference_processes=True)
    return DeleteProcessResponse(process_uuid=process_uuid, deleted=1)


@app.post("/api/processes/delete-batch", response_model=DeleteProcessesBatchResponse)
def delete_processes_batch_api(
    payload: DeleteProcessesBatchRequest,
    db: Session = Depends(get_db),
) -> DeleteProcessesBatchResponse:
    ids = [str(pid).strip() for pid in payload.process_uuids if str(pid).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail={"code": "INVALID_REQUEST", "message": "process_uuids cannot be empty"})

    existing_rows = db.query(ReferenceProcess.process_uuid).filter(ReferenceProcess.process_uuid.in_(ids)).all()
    existing_ids = {row[0] for row in existing_rows}
    to_delete = [pid for pid in ids if pid in existing_ids]
    not_found = [pid for pid in ids if pid not in existing_ids]

    deleted = 0
    if to_delete:
        deleted = db.query(ReferenceProcess).filter(ReferenceProcess.process_uuid.in_(to_delete)).delete(synchronize_session=False)
    db.commit()
    _invalidate_management_caches(stats=True, reference_processes=True)
    return DeleteProcessesBatchResponse(
        requested=len(ids),
        deleted=deleted,
        not_found=not_found,
    )


@app.get("/api/flows", response_model=PaginatedFlowsResponse)
def list_flows_api(
    search: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    type: str | None = Query(default=None),
    category: str | None = Query(default=None),
    category_level_1: str | None = Query(default=None),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> PaginatedFlowsResponse:
    search_key = (search or "").strip().lower()
    category_key = (category or "").strip().lower()
    level1_key = (category_level_1 or "").strip().lower()
    cache_key = (
        f"flows:v2:rev={_cache_revision('flows')}:search={search_key}:page={page}:page_size={page_size}:"
        f"type={type or ''}:category={category_key}:level1={level1_key}"
    )
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOWS_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _is_if_none_match_hit(if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    type_map: dict[str, set[str]] = {
        "intermediate_flow": {"Product flow", "Waste flow"},
        "elementary_flow": {"Elementary flow"},
        "product_flow": {"Product flow"},
        "waste_flow": {"Waste flow"},
    }
    if type is not None and type not in type_map:
        raise HTTPException(status_code=400, detail={"code": "INVALID_REQUEST", "message": "Invalid flow type"})

    query = db.query(FlowRecord)
    if type:
        query = query.filter(FlowRecord.flow_type.in_(sorted(type_map[type])))
    flow_name_expr = func.lower(func.coalesce(FlowRecord.flow_name, ""))
    flow_name_en_expr = func.lower(func.coalesce(FlowRecord.flow_name_en, ""))
    flow_uuid_expr = func.lower(func.coalesce(FlowRecord.flow_uuid, ""))
    if search and search.strip():
        normalized_search = search.strip().lower()
        token = f"%{normalized_search}%"
        query = query.filter(
            flow_name_expr.like(token)
            | flow_name_en_expr.like(token)
            | flow_uuid_expr.like(token)
        )
    if category and category.strip():
        category_token = f"%{category.strip().lower()}%"
        query = query.filter(func.lower(func.coalesce(FlowRecord.compartment, "")).like(category_token))
    if category_level_1 and category_level_1.strip():
        level1 = category_level_1.strip().lower()
        compartment_expr = func.lower(func.coalesce(FlowRecord.compartment, ""))
        query = query.filter((compartment_expr == level1) | (compartment_expr.like(f"{level1};%")))

    total = query.count()
    if search_key:
        prioritize_carbon_dioxide = ("二氧化碳" in search_key) or ("carbon dioxide" in search_key)
        exact_rank = case(
            (flow_name_expr == search_key, 0),
            (flow_name_en_expr == search_key, 0),
            (flow_uuid_expr == search_key, 0),
            else_=1,
        )
        prefix_rank = case(
            (flow_name_expr.like(f"{search_key}%"), 0),
            (flow_name_en_expr.like(f"{search_key}%"), 0),
            (flow_uuid_expr.like(f"{search_key}%"), 0),
            else_=1,
        )
        order_by_clauses = []
        if prioritize_carbon_dioxide:
            carbon_dioxide_rank = case(
                (
                    flow_name_expr.like("%二氧化碳%")
                    | flow_name_en_expr.like("%carbon dioxide%"),
                    0,
                ),
                else_=1,
            )
            order_by_clauses.append(carbon_dioxide_rank.asc())
        order_by_clauses.extend(
            [
                exact_rank.asc(),
                prefix_rank.asc(),
                func.length(FlowRecord.flow_name).asc(),
                FlowRecord.flow_name.asc(),
            ]
        )
        rows = (
            query.order_by(*order_by_clauses)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
    else:
        default_order_by = []
        if type == "elementary_flow":
            carbon_dioxide_rank = case(
                (
                    flow_name_expr.like("%二氧化碳%")
                    | flow_name_en_expr.like("%carbon dioxide%"),
                    0,
                ),
                else_=1,
            )
            default_order_by.append(carbon_dioxide_rank.asc())
        default_order_by.append(FlowRecord.flow_name.asc())
        rows = (
            query.order_by(*default_order_by)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
    used_in_processes = _build_flow_used_in_processes_map(db)
    items: list[FlowListItem] = []
    for row in rows:
        normalized_type = normalize_flow_semantic(row.flow_type) or "intermediate_flow"
        if normalized_type not in {"elementary_flow", "product_flow", "waste_flow"}:
            normalized_type = "intermediate_flow"
        items.append(
            FlowListItem(
                flow_id=row.flow_uuid,
                flow_name=row.flow_name,
                flow_name_en=row.flow_name_en,
                type=normalized_type,
                unit=row.default_unit,
                category=row.compartment,
                used_in_processes=int(used_in_processes.get(row.flow_uuid, 0)),
                last_modified=row.source_updated_at,
            )
        )
    result = PaginatedFlowsResponse(items=items, total=total, page=page, page_size=page_size)
    payload = result.model_dump(mode="json")
    etag = _build_etag_for_payload(payload)
    _cache_set(cache_key, {"payload": payload, "etag": etag})
    if _is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})


@app.get("/api/flows/categories", response_model=FlowCategoriesResponse)
def list_flow_categories_api(
    type: str | None = Query(default=None),
    search: str | None = Query(default=None),
    level: int = Query(default=1, ge=1, le=3),
    db: Session = Depends(get_db),
) -> FlowCategoriesResponse:
    type_map: dict[str, set[str]] = {
        "intermediate_flow": {"Product flow", "Waste flow"},
        "elementary_flow": {"Elementary flow"},
        "product_flow": {"Product flow"},
        "waste_flow": {"Waste flow"},
    }
    if type is not None and type not in type_map:
        raise HTTPException(status_code=400, detail={"code": "INVALID_REQUEST", "message": "Invalid flow type"})
    if level != 1:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REQUEST", "message": "Only level=1 is supported currently"},
        )

    cache_key = (
        f"flow_categories:v1:rev={_cache_revision('flow_categories')}:"
        f"type={type or ''}:level={level}:search={search or ''}"
    )
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOW_CATEGORIES_SECONDS)
    if isinstance(cached, FlowCategoriesResponse):
        return cached

    query = db.query(FlowRecord.compartment)
    if type:
        query = query.filter(FlowRecord.flow_type.in_(sorted(type_map[type])))
    if search and search.strip():
        token = f"%{search.strip().lower()}%"
        query = query.filter(func.lower(func.coalesce(FlowRecord.compartment, "")).like(token))
    query = query.filter(FlowRecord.compartment.is_not(None), func.trim(FlowRecord.compartment) != "")
    rows = query.all()

    counts: dict[str, int] = defaultdict(int)
    for (compartment,) in rows:
        first = _first_category_segment(str(compartment))
        if not first:
            continue
        counts[first] += 1

    items = [
        FlowCategoryItem(category=category_name, count=count)
        for category_name, count in sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    ]
    result = FlowCategoriesResponse(items=items, total=len(items))
    _cache_set(cache_key, result)
    return result


@app.get("/api/stats", response_model=StatsResponse)
def get_stats_api(db: Session = Depends(get_db)) -> StatsResponse:
    cache_key = f"stats:v2:rev={_cache_revision('stats')}"
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_STATS_SECONDS)
    if isinstance(cached, StatsResponse):
        return cached

    projects = db.query(Model).count()
    flows_library_total = db.query(FlowRecord).count()
    process_library_total = db.query(ReferenceProcess).count()
    latest_rows = _latest_graphs_with_project_meta(db)
    graph_process_total = 0
    graph_flow_total = 0
    for _, latest in latest_rows:
        process_count, flow_count = _graph_process_and_flow_counts(
            latest.hybrid_graph_json if isinstance(latest.hybrid_graph_json, dict) else {}
        )
        graph_process_total += process_count
        graph_flow_total += flow_count

    # Library-oriented stats for management dashboard.
    flows_latest_graph = graph_flow_total
    flow_total = flows_library_total

    result = StatsResponse(
        projects=projects,
        processes=process_library_total,
        flows=flow_total,
        flows_latest_graph=flows_latest_graph,
        flows_library_total=flows_library_total,
        flow_library_total=flows_library_total,
        graph_processes=graph_process_total,
        graph_flows=graph_flow_total,
    )
    _cache_set(cache_key, result)
    return result


@app.post("/projects/{project_id}/versions", response_model=ModelCreateResponse)
def create_project_version(
    project_id: str,
    payload: ModelVersionCreateRequest,
    compile_pts_on_save: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> ModelCreateResponse:
    normalize_graph_product_flags(payload.graph)
    _canonicalize_pts_nodes_for_main_graph_save(db=db, project_id=project_id, graph=payload.graph)
    validate_graph_contract(payload.graph, require_non_empty=True, allow_pts_nodes=True)
    validate_graph_flow_type_contract(payload.graph, db=db, stage="save_version")
    validate_graph_port_names_against_flow_catalog(payload.graph, db=db, stage="save_version")

    model = get_model_or_404(db, project_id)
    graph_hash = _compute_graph_hash_from_graph(payload.graph)

    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is not None:
        latest_has_hash = bool(str(latest_row.graph_hash or "").strip())
        latest_hash = _resolve_model_version_graph_hash(latest_row, assign_if_missing=True)
        if not latest_has_hash:
            db.commit()
        if latest_hash == graph_hash:
            pts_compile_summary = _compile_pts_on_save_if_needed(
                db=db,
                project_id=model.id,
                graph=payload.graph,
                compile_on_save=compile_pts_on_save,
            )
            return ModelCreateResponse(
                project_id=model.id,
                version=latest_row.version,
                created_at=latest_row.created_at,
                created_new_version=False,
                graph_hash=graph_hash,
                message="内容未变化，未创建新版本",
                **pts_compile_summary,
            )

    latest_version = (
        db.query(func.max(ModelVersion.version))
        .filter(ModelVersion.model_id == model.id)
        .scalar()
    )
    next_version = (latest_version or 0) + 1

    normalized_graph_json = _normalize_graph_json_for_storage(payload.graph.model_dump(mode="python"))
    version = ModelVersion(
        model_id=model.id,
        version=next_version,
        graph_hash=graph_hash,
        hybrid_graph_json=normalized_graph_json,
    )
    model.updated_at = datetime.utcnow()
    db.add(version)
    db.commit()
    db.refresh(version)
    pts_compile_summary = _compile_pts_on_save_if_needed(
        db=db,
        project_id=model.id,
        graph=payload.graph,
        compile_on_save=compile_pts_on_save,
    )
    _invalidate_management_caches(projects=True, stats=True)
    return ModelCreateResponse(
        project_id=model.id,
        version=version.version,
        created_at=version.created_at,
        created_new_version=True,
        graph_hash=graph_hash,
        **pts_compile_summary,
    )


@app.post("/api/projects/{project_id}/versions", response_model=ModelCreateResponse)
def create_project_version_api(
    project_id: str,
    payload: ModelVersionCreateRequest,
    compile_pts_on_save: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> ModelCreateResponse:
    return create_project_version(
        project_id=project_id,
        payload=payload,
        compile_pts_on_save=compile_pts_on_save,
        db=db,
    )


@app.get("/projects/{project_id}/versions", response_model=list[ModelCreateResponse])
def list_project_versions(project_id: str, db: Session = Depends(get_db)) -> list[ModelCreateResponse]:
    model = get_model_or_404(db, project_id)
    versions = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .all()
    )
    return [
        ModelCreateResponse(
            project_id=model.id,
            version=v.version,
            created_at=v.created_at,
            graph_hash=_resolve_model_version_graph_hash(v, assign_if_missing=False),
        )
        for v in versions
    ]


@app.get("/api/projects/{project_id}/versions/{version}", response_model=ModelVersionOut)
@app.get("/projects/{project_id}/versions/{version}", response_model=ModelVersionOut)
def get_project_version(project_id: str, version: int, db: Session = Depends(get_db)) -> ModelVersionOut:
    model = get_model_or_404(db, project_id)
    record = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id, ModelVersion.version == version)
        .first()
    )
    if not record:
        raise HTTPException(status_code=404, detail="Model version not found")
    graph = HybridGraph.model_validate(record.hybrid_graph_json)
    normalize_graph_product_flags(graph)
    _project_pts_external_ports_into_graph(db=db, project_id=model.id, graph=graph)
    pts_validation = _build_pts_validation_summary(db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(graph_json, db=db)
    flow_name_sync_needed, outdated_flow_refs_count, outdated_flow_ref_examples = _build_flow_sync_state_for_graph_json(
        db=db,
        graph_json=graph_json,
    )
    project_integrity = _build_project_integrity_summary(
        pts_validation=pts_validation,
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples,
    )
    return ModelVersionOut(
        project_id=model.id,
        version=record.version,
        created_at=record.created_at,
        graph=graph_json,
        handle_validation=safe_handle_validation_from_graph_json(graph_json),
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples,
        pts_validation=pts_validation,
        project_integrity=project_integrity,
    )


@app.get("/api/projects/{project_id}/latest", response_model=ModelVersionOut)
@app.get("/projects/{project_id}/latest", response_model=ModelVersionOut)
def get_project_latest_by_id(
    project_id: str,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> ModelVersionOut | Response:
    model = get_model_or_404(db, project_id)
    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is None:
        raise HTTPException(status_code=404, detail="No model version found")
    cache_key = (
        f"project_latest:v1:rev={_cache_revision('projects')}:project_id={model.id}:"
        f"version={latest_row.version}:graph_hash={str(latest_row.graph_hash or '')}"
    )
    cached = _cache_get(cache_key, ttl_seconds=_CACHE_TTL_PROJECTS_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _is_if_none_match_hit(if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    graph = HybridGraph.model_validate(latest_row.hybrid_graph_json)
    normalize_graph_product_flags(graph)
    _project_pts_external_ports_into_graph(db=db, project_id=model.id, graph=graph)
    pts_validation = _build_pts_validation_summary(db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(graph_json, db=db)
    flow_name_sync_needed, outdated_flow_refs_count, outdated_flow_ref_examples = _build_flow_sync_state_for_graph_json(
        db=db,
        graph_json=graph_json,
    )
    project_integrity = _build_project_integrity_summary(
        pts_validation=pts_validation,
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples,
    )
    payload = ModelVersionOut(
        project_id=model.id,
        version=latest_row.version,
        created_at=latest_row.created_at,
        graph=graph_json,
        handle_validation=safe_handle_validation_from_graph_json(graph_json),
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples,
        pts_validation=pts_validation,
        project_integrity=project_integrity,
    )
    payload_json = payload.model_dump(mode="json")
    etag = _build_etag_for_payload(payload_json)
    _cache_set(cache_key, {"payload": payload_json, "etag": etag})
    if _is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload_json, headers={"ETag": etag})


@app.post("/api/projects/{project_id}/repair-pts-publications", response_model=RepairPtsPublicationsResponse)
@app.post("/projects/{project_id}/repair-pts-publications", response_model=RepairPtsPublicationsResponse)
def repair_pts_publications_for_project(project_id: str, db: Session = Depends(get_db)) -> RepairPtsPublicationsResponse:
    model = get_model_or_404(db, project_id)
    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is None:
        raise HTTPException(status_code=404, detail="No model version found")
    graph = HybridGraph.model_validate(latest_row.hybrid_graph_json)
    normalize_graph_product_flags(graph)
    _project_pts_external_ports_into_graph(db=db, project_id=model.id, graph=graph)
    validation = _build_pts_validation_summary(db=db, project_id=model.id, graph=graph)

    repaired_count = 0
    skipped_count = 0
    failed_count = 0
    items: list[dict] = []
    for item in validation.items:
        if not item.auto_repairable:
            skipped_count += 1
            items.append(
                {
                    "pts_uuid": item.pts_uuid,
                    "node_id": item.node_id,
                    "node_name": item.node_name,
                    "status": "skipped",
                    "reason": item.reason,
                }
            )
            continue
        resource = (
            db.query(PtsResource)
            .filter(PtsResource.project_id == model.id, PtsResource.pts_uuid == item.pts_uuid)
            .first()
        )
        if resource is None:
            failed_count += 1
            items.append(
                {
                    "pts_uuid": item.pts_uuid,
                    "node_id": item.node_id,
                    "node_name": item.node_name,
                    "status": "failed",
                    "reason": "pts_resource_missing",
                }
            )
            continue
        try:
            repaired, reason = _repair_pts_publication_from_resource(db=db, resource=resource)
            if repaired:
                repaired_count += 1
                items.append(
                    {
                        "pts_uuid": item.pts_uuid,
                        "node_id": item.node_id,
                        "node_name": item.node_name,
                        "status": "repaired",
                        "reason": reason,
                    }
                )
            else:
                skipped_count += 1
                items.append(
                    {
                        "pts_uuid": item.pts_uuid,
                        "node_id": item.node_id,
                        "node_name": item.node_name,
                        "status": "skipped",
                        "reason": reason,
                    }
                )
        except Exception as exc:
            failed_count += 1
            items.append(
                {
                    "pts_uuid": item.pts_uuid,
                    "node_id": item.node_id,
                    "node_name": item.node_name,
                    "status": "failed",
                    "reason": str(exc),
                }
            )
    db.commit()
    _invalidate_management_caches(projects=True, stats=True)
    return RepairPtsPublicationsResponse(
        project_id=model.id,
        repaired_count=repaired_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        items=items,
    )


@app.post("/api/projects/{project_id}/repair-integrity", response_model=RepairProjectIntegrityResponse)
@app.post("/projects/{project_id}/repair-integrity", response_model=RepairProjectIntegrityResponse)
def repair_project_integrity(project_id: str, db: Session = Depends(get_db)) -> RepairProjectIntegrityResponse:
    model = get_model_or_404(db, project_id)
    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is None:
        raise HTTPException(status_code=404, detail="No model version found")

    graph = HybridGraph.model_validate(latest_row.hybrid_graph_json)
    normalize_graph_product_flags(graph)
    _project_pts_external_ports_into_graph(db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    pts_validation = _build_pts_validation_summary(db=db, project_id=model.id, graph=graph)
    flow_name_sync_needed, outdated_flow_refs_count, outdated_flow_ref_examples = _build_flow_sync_state_for_graph_json(
        db=db,
        graph_json=graph_json,
    )

    repaired_count = 0
    skipped_count = 0
    failed_count = 0
    items: list[dict] = []

    for item in pts_validation.items:
        if not item.auto_repairable:
            skipped_count += 1
            items.append(
                {
                    "kind": "pts_publication",
                    "pts_uuid": item.pts_uuid,
                    "node_id": item.node_id,
                    "node_name": item.node_name,
                    "status": "skipped",
                    "reason": item.reason,
                }
            )
            continue
        resource = (
            db.query(PtsResource)
            .filter(PtsResource.project_id == model.id, PtsResource.pts_uuid == item.pts_uuid)
            .first()
        )
        if resource is None:
            failed_count += 1
            items.append(
                {
                    "kind": "pts_publication",
                    "pts_uuid": item.pts_uuid,
                    "node_id": item.node_id,
                    "node_name": item.node_name,
                    "status": "failed",
                    "reason": "pts_resource_missing",
                }
            )
            continue
        try:
            repaired, reason = _repair_pts_publication_from_resource(db=db, resource=resource)
            if repaired:
                repaired_count += 1
                items.append(
                    {
                        "kind": "pts_publication",
                        "pts_uuid": item.pts_uuid,
                        "node_id": item.node_id,
                        "node_name": item.node_name,
                        "status": "repaired",
                        "reason": reason,
                    }
                )
            else:
                skipped_count += 1
                items.append(
                    {
                        "kind": "pts_publication",
                        "pts_uuid": item.pts_uuid,
                        "node_id": item.node_id,
                        "node_name": item.node_name,
                        "status": "skipped",
                        "reason": reason,
                    }
                )
        except Exception as exc:
            failed_count += 1
            items.append(
                {
                    "kind": "pts_publication",
                    "pts_uuid": item.pts_uuid,
                    "node_id": item.node_id,
                    "node_name": item.node_name,
                    "status": "failed",
                    "reason": str(exc),
                }
            )

    if flow_name_sync_needed:
        try:
            (
                latest_version,
                synced_port_count,
                synced_edge_count,
                updated_flow_count,
                _before_count,
                _after_count,
            ) = _sync_project_latest_version_flow_names(db=db, project_id=model.id)
            if synced_port_count > 0 or synced_edge_count > 0:
                repaired_count += 1
                items.append(
                    {
                        "kind": "flow_name_sync",
                        "status": "repaired",
                        "reason": "flow_names_synced",
                        "version": latest_version,
                        "synced_port_count": synced_port_count,
                        "synced_edge_count": synced_edge_count,
                        "updated_flow_count": updated_flow_count,
                    }
                )
            else:
                skipped_count += 1
                items.append(
                    {
                        "kind": "flow_name_sync",
                        "status": "skipped",
                        "reason": "already_synced",
                        "outdated_count": int(outdated_flow_refs_count or 0),
                        "examples": list(outdated_flow_ref_examples or []),
                    }
                )
        except Exception as exc:
            failed_count += 1
            items.append(
                {
                    "kind": "flow_name_sync",
                    "status": "failed",
                    "reason": str(exc),
                    "outdated_count": int(outdated_flow_refs_count or 0),
                }
            )

    db.commit()
    _invalidate_management_caches(projects=True, stats=True)
    return RepairProjectIntegrityResponse(
        project_id=model.id,
        repaired_count=repaired_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        items=items,
    )


def get_model_version(project_id: str, version: int, db: Session = Depends(get_db)) -> dict:
    record = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == project_id, ModelVersion.version == version)
        .first()
    )
    if not record:
        raise HTTPException(status_code=404, detail="Model version not found")
    graph = HybridGraph.model_validate(record.hybrid_graph_json)
    normalize_graph_product_flags(graph)
    _project_pts_external_ports_into_graph(db=db, project_id=project_id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(graph_json, db=db)
    return {
        "project_id": project_id,
        "version": version,
        "graph": graph_json,
        "created_at": record.created_at,
        "handle_validation": safe_handle_validation_from_graph_json(graph_json),
    }


def get_project_latest(project_name: str, db: Session = Depends(get_db)) -> dict:
    normalized_name = project_name.strip()
    if not normalized_name:
        raise HTTPException(status_code=400, detail="Project name cannot be empty")
    record = (
        db.query(ModelVersion, Model)
        .join(Model, ModelVersion.model_id == Model.id)
        .filter(Model.name == normalized_name)
        .order_by(ModelVersion.created_at.desc(), ModelVersion.version.desc())
        .first()
    )
    if not record:
        raise HTTPException(status_code=404, detail="Project not found")

    version, model = record
    graph = HybridGraph.model_validate(version.hybrid_graph_json)
    normalize_graph_product_flags(graph)
    _project_pts_external_ports_into_graph(db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(graph_json, db=db)
    return {
        "project_name": normalized_name,
        "project_id": model.id,
        "version": version.version,
        "graph": graph_json,
        "created_at": version.created_at,
        "handle_validation": safe_handle_validation_from_graph_json(graph_json),
    }


def get_fixed_project_latest(db: Session = Depends(get_db)) -> dict:
    latest = (
        db.query(ModelVersion, Model)
        .join(Model, ModelVersion.model_id == Model.id)
        .order_by(ModelVersion.created_at.desc(), ModelVersion.version.desc())
        .first()
    )
    if not latest:
        raise HTTPException(status_code=404, detail="No project version found")
    version, model = latest
    graph = HybridGraph.model_validate(version.hybrid_graph_json)
    normalize_graph_product_flags(graph)
    _project_pts_external_ports_into_graph(db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(graph_json, db=db)
    return {
        "project_name": model.name,
        "project_id": model.id,
        "version": version.version,
        "graph": graph_json,
        "created_at": version.created_at,
        "handle_validation": safe_handle_validation_from_graph_json(graph_json),
    }


@app.post("/admin/migrations/node-kinds", dependencies=[Depends(require_debug_access)])
def migrate_node_kinds(
    project_id: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
    invalidate_pts_cache: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> dict:
    query = db.query(ModelVersion)
    if project_id:
        query = query.filter(ModelVersion.model_id == project_id)

    rows = query.order_by(ModelVersion.created_at.asc(), ModelVersion.version.asc()).all()

    scanned = 0
    updated = 0
    failed = 0
    updated_version_ids: list[str] = []
    updated_resource_ids: list[str] = []
    updated_definition_ids: list[str] = []
    failures: list[dict] = []

    for row in rows:
        scanned += 1
        source = row.hybrid_graph_json if isinstance(row.hybrid_graph_json, dict) else {}
        try:
            normalized = _normalize_graph_json_for_storage(source)
        except Exception as exc:
            failed += 1
            failures.append(
                {
                    "model_version_id": str(row.id),
                    "project_id": str(row.model_id),
                    "version": int(row.version),
                    "error": str(exc),
                }
            )
            continue

        if _canonical_json(source) == _canonical_json(normalized):
            continue

        updated += 1
        updated_version_ids.append(str(row.id))
        if not dry_run:
            row.hybrid_graph_json = normalized

    resource_rows = db.query(PtsResource).all() if not project_id else db.query(PtsResource).filter(PtsResource.project_id == project_id).all()
    for row in resource_rows:
        source_pts_graph = row.pts_graph_json if isinstance(row.pts_graph_json, dict) else {}
        source_shell = row.shell_node_json if isinstance(row.shell_node_json, dict) else {}
        try:
            normalized_pts_graph = dict(source_pts_graph or {})
            if normalized_pts_graph:
                _enrich_market_process_input_sources_in_graph_json(normalized_pts_graph)
                validated_pts_graph = {
                    "functionalUnit": str(normalized_pts_graph.get("functionalUnit") or "PTS"),
                    "nodes": normalized_pts_graph.get("nodes") or [],
                    "exchanges": normalized_pts_graph.get("exchanges") or [],
                    "metadata": normalized_pts_graph.get("metadata") or {},
                }
                _validate_process_name_uniqueness_for_graph_json(
                    graph_json=validated_pts_graph,
                    scope_label=f"pts_graph:{row.pts_uuid}",
                )
                validated_graph = HybridGraph.model_validate(validated_pts_graph)
                normalize_graph_product_flags(validated_graph)
                normalize_graph_edge_port_ids(validated_graph)
                normalized_pts_graph = validated_graph.model_dump(mode="python")
            normalized_shell = dict(source_shell or {})
            if normalized_shell:
                normalized_shell["node_kind"] = _normalize_pts_shell_node_kind(normalized_shell.get("node_kind"))
        except Exception as exc:
            failed += 1
            failures.append(
                {
                    "pts_resource_id": str(row.id),
                    "project_id": str(row.project_id),
                    "pts_uuid": str(row.pts_uuid),
                    "error": str(exc),
                }
            )
            continue

        if _canonical_json(source_pts_graph) == _canonical_json(normalized_pts_graph) and _canonical_json(source_shell) == _canonical_json(normalized_shell):
            continue

        updated += 1
        updated_resource_ids.append(str(row.id))
        if not dry_run:
            (
                db.query(PtsResource)
                .filter(PtsResource.id == row.id)
                .update(
                    {
                        PtsResource.pts_graph_json: normalized_pts_graph,
                        PtsResource.shell_node_json: normalized_shell,
                    },
                    synchronize_session=False,
                )
            )

    definition_rows = db.query(PtsDefinition).all() if not project_id else db.query(PtsDefinition).filter(PtsDefinition.project_id == project_id).all()
    for row in definition_rows:
        source_definition = row.definition_json if isinstance(row.definition_json, dict) else {}
        try:
            normalized_definition = dict(source_definition or {})
            definition_pts_graph = dict(normalized_definition.get("pts_graph") or {})
            if definition_pts_graph:
                _enrich_market_process_input_sources_in_graph_json(definition_pts_graph)
                validated_pts_graph = {
                    "functionalUnit": str(definition_pts_graph.get("functionalUnit") or "PTS"),
                    "nodes": definition_pts_graph.get("nodes") or [],
                    "exchanges": definition_pts_graph.get("exchanges") or [],
                    "metadata": definition_pts_graph.get("metadata") or {},
                }
                _validate_process_name_uniqueness_for_graph_json(
                    graph_json=validated_pts_graph,
                    scope_label=f"pts_definition:{row.pts_uuid or row.pts_id}",
                )
                validated_graph = HybridGraph.model_validate(validated_pts_graph)
                normalize_graph_product_flags(validated_graph)
                normalize_graph_edge_port_ids(validated_graph)
                normalized_definition["pts_graph"] = validated_graph.model_dump(mode="python")
            shell_node = dict(normalized_definition.get("shell_node") or {})
            if shell_node:
                shell_node["node_kind"] = _normalize_pts_shell_node_kind(shell_node.get("node_kind"))
                normalized_definition["shell_node"] = shell_node
        except Exception as exc:
            failed += 1
            failures.append(
                {
                    "pts_definition_id": str(row.id),
                    "project_id": str(row.project_id),
                    "pts_uuid": str(row.pts_uuid or row.pts_id),
                    "error": str(exc),
                }
            )
            continue

        if _canonical_json(source_definition) == _canonical_json(normalized_definition):
            continue

        updated += 1
        updated_definition_ids.append(str(row.id))
        if not dry_run:
            (
                db.query(PtsDefinition)
                .filter(PtsDefinition.id == row.id)
                .update(
                    {
                        PtsDefinition.definition_json: normalized_definition,
                    },
                    synchronize_session=False,
                )
            )

    invalidated_pts_compile = 0
    invalidated_pts_external = 0
    invalidated_pts_definition = 0

    if not dry_run and invalidate_pts_cache and updated > 0:
        touched_project_ids = sorted({str(r.model_id) for r in rows}) if not project_id else [project_id]
        if touched_project_ids:
            invalidated_pts_compile = (
                db.query(PtsCompileArtifact)
                .filter(PtsCompileArtifact.project_id.in_(touched_project_ids))
                .delete(synchronize_session=False)
            )
            invalidated_pts_external = (
                db.query(PtsExternalArtifact)
                .filter(PtsExternalArtifact.project_id.in_(touched_project_ids))
                .delete(synchronize_session=False)
            )
            invalidated_pts_definition = (
                db.query(PtsDefinition)
                .filter(PtsDefinition.project_id.in_(touched_project_ids))
                .delete(synchronize_session=False)
            )

    if not dry_run:
        db.commit()

    return {
        "migration": "node-kinds-v2",
        "dry_run": dry_run,
        "project_id": project_id,
        "scanned_versions": scanned,
        "updated_versions": updated,
        "failed_versions": failed,
        "updated_model_version_ids": updated_version_ids[:200],
        "updated_pts_resource_ids": updated_resource_ids[:200],
        "updated_pts_definition_ids": updated_definition_ids[:200],
        "failures": failures[:50],
        "pts_cache_invalidated": {
            "compile_artifacts": invalidated_pts_compile,
            "external_artifacts": invalidated_pts_external,
            "definitions": invalidated_pts_definition,
        },
    }


@app.post("/admin/migrations/model-version-hashes", dependencies=[Depends(require_debug_access)])
def migrate_model_version_hashes(
    project_id: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
    deduplicate_redundant_versions: bool = Query(default=True),
    invalidate_pts_cache: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> dict:
    schema_report = _ensure_model_versions_hash_schema()

    query = db.query(ModelVersion)
    if project_id:
        query = query.filter(ModelVersion.model_id == project_id)

    rows = query.order_by(ModelVersion.model_id.asc(), ModelVersion.version.asc(), ModelVersion.created_at.asc()).all()

    scanned = 0
    hash_backfilled = 0
    failed = 0
    failures: list[dict] = []
    duplicate_version_ids: list[str] = []
    duplicate_examples: list[dict] = []
    touched_project_ids: set[str] = set()
    last_hash_by_project: dict[str, str] = {}

    for row in rows:
        scanned += 1
        source = row.hybrid_graph_json if isinstance(row.hybrid_graph_json, dict) else {}
        try:
            computed_hash = _compute_graph_hash_from_graph_json(source)
        except Exception as exc:
            failed += 1
            failures.append(
                {
                    "model_version_id": str(row.id),
                    "project_id": str(row.model_id),
                    "version": int(row.version),
                    "error": str(exc),
                }
            )
            continue

        current_hash = str(row.graph_hash or "").strip()
        if current_hash != computed_hash:
            hash_backfilled += 1
            touched_project_ids.add(str(row.model_id))
            if not dry_run:
                row.graph_hash = computed_hash

        if deduplicate_redundant_versions:
            prev_hash = last_hash_by_project.get(str(row.model_id))
            if prev_hash == computed_hash:
                duplicate_version_ids.append(str(row.id))
                touched_project_ids.add(str(row.model_id))
                if len(duplicate_examples) < 200:
                    duplicate_examples.append(
                        {
                            "model_version_id": str(row.id),
                            "project_id": str(row.model_id),
                            "version": int(row.version),
                            "graph_hash": computed_hash,
                        }
                    )
            else:
                last_hash_by_project[str(row.model_id)] = computed_hash

    cleared_run_job_refs = 0
    deleted_versions = 0
    if not dry_run and duplicate_version_ids:
        # Flush pending graph_hash updates first, then delete duplicates.
        # Otherwise SQLAlchemy may raise StaleDataError when a row is updated
        # in-session and then removed by bulk delete before commit.
        db.flush()
        for batch in _iter_chunks(duplicate_version_ids):
            cleared_run_job_refs += (
                db.query(RunJob)
                .filter(RunJob.model_version_id.in_(batch))
                .update({RunJob.model_version_id: None}, synchronize_session=False)
            )
            deleted_versions += db.query(ModelVersion).filter(ModelVersion.id.in_(batch)).delete(synchronize_session=False)

    invalidated_pts_compile = 0
    invalidated_pts_external = 0
    invalidated_pts_definition = 0
    touched_project_id_list = sorted(touched_project_ids)
    if not dry_run and invalidate_pts_cache and touched_project_id_list:
        for batch in _iter_chunks(touched_project_id_list):
            invalidated_pts_compile += (
                db.query(PtsCompileArtifact)
                .filter(PtsCompileArtifact.project_id.in_(batch))
                .delete(synchronize_session=False)
            )
            invalidated_pts_external += (
                db.query(PtsExternalArtifact)
                .filter(PtsExternalArtifact.project_id.in_(batch))
                .delete(synchronize_session=False)
            )
            invalidated_pts_definition += (
                db.query(PtsDefinition)
                .filter(PtsDefinition.project_id.in_(batch))
                .delete(synchronize_session=False)
            )

    if not dry_run:
        db.commit()

    return {
        "migration": "model-version-hashes-v1",
        "dry_run": dry_run,
        "project_id": project_id,
        "schema": schema_report,
        "scanned_versions": scanned,
        "hash_backfilled_versions": hash_backfilled,
        "redundant_duplicate_versions": len(duplicate_version_ids),
        "deleted_versions": deleted_versions,
        "cleared_run_job_refs": cleared_run_job_refs,
        "failed_versions": failed,
        "redundant_model_version_ids": duplicate_version_ids[:500],
        "duplicate_examples": duplicate_examples,
        "failures": failures[:50],
        "pts_cache_invalidated": {
            "compile_artifacts": invalidated_pts_compile,
            "external_artifacts": invalidated_pts_external,
            "definitions": invalidated_pts_definition,
            "project_ids": touched_project_id_list[:200],
        },
    }


@app.post("/admin/migrations/pts-resources", dependencies=[Depends(require_debug_access)])
def migrate_pts_resources_endpoint(
    project_id: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
    latest_only: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> dict:
    schema_report = _ensure_pts_resources_schema()
    report = _migrate_pts_resources(
        db=db,
        project_id=project_id,
        dry_run=dry_run,
        latest_only=latest_only,
    )
    report["schema"] = schema_report
    return report


@app.post("/admin/migrations/pts-published-bindings", dependencies=[Depends(require_debug_access)])
def migrate_pts_published_bindings_endpoint(
    project_id: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
    latest_only: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> dict:
    report = _migrate_pts_published_version_bindings(
        db=db,
        project_id=project_id,
        dry_run=dry_run,
        latest_only=latest_only,
    )
    return report


@app.post("/admin/maintenance/prune-model-versions", dependencies=[Depends(require_debug_access)])
def prune_model_versions_maintenance(
    keep_latest: int = Query(default=max(1, int(settings.keep_latest_versions_per_project)), ge=1, le=20000),
    project_id: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
    vacuum_after_cleanup: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict:
    try:
        result = _prune_model_versions_retention(
            db=db,
            keep_latest=keep_latest,
            dry_run=dry_run,
            project_id=project_id,
            vacuum_after_cleanup=vacuum_after_cleanup,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "maintenance": "prune-model-versions-v1",
        **result,
    }


@app.post("/api/model/run", response_model=RunResponse)
@app.post("/model/run", response_model=RunResponse)
def run_model(payload: RunRequest, db: Session = Depends(get_db)) -> RunResponse:
    validate_graph_contract(payload.graph, require_non_empty=False, allow_pts_nodes=True)
    validate_graph_flow_type_contract(payload.graph, db=db, stage="run_model")
    validate_graph_port_names_against_flow_catalog(payload.graph, db=db, stage="run_model")
    try:
        pts_nodes = [node for node in payload.graph.nodes if node.node_kind == "pts_module"]
        effective_payload = payload
        if pts_nodes:
            project_id = resolve_project_id_for_run(payload, db)
            compile_rows = _load_published_compile_rows_for_graph(
                db=db,
                project_id=project_id,
                graph=payload.graph,
                graph_hash=None,
            )
            failed_rows = [row for row in compile_rows if not row.ok]
            if failed_rows:
                errors: list[str] = []
                for row in failed_rows:
                    row_errors = row.errors_json or []
                    if not row_errors:
                        errors.append(f"PTS published artifact invalid: {row.pts_node_id}")
                        continue
                    errors.extend([f"[{row.pts_node_id}] {msg}" for msg in row_errors])
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "PTS_PUBLISHED_ARTIFACT_INVALID",
                        "message": "Published artifact invalid",
                        "errors": errors,
                    },
                )

            flattened_graph = build_flattened_graph_for_run_pts(graph=payload.graph, compile_rows=compile_rows)
            effective_payload = RunRequest(
                graph=flattened_graph,
                model_version_id=payload.model_version_id,
                project_id=project_id,
                force_recompile=False,
            )

        status, run_id, solved, tiangong_like = run_solver_and_persist(payload=effective_payload, db=db)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"run_model unexpected error: {exc}\n{traceback.format_exc()}") from exc

    return RunResponse(
        run_id=run_id,
        status=status,
        summary=solved["summary"],
        tiangong_like_input=tiangong_like,
        lci_result=solved["lci_result"],
    )
