import json
import hashlib
import csv
import uuid
import io
import zipfile
import re
from collections import Counter, defaultdict
from datetime import datetime
from itertools import chain
from pathlib import Path
import time
import traceback
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import case, func, inspect, text
from sqlalchemy.orm import Session
from .config import settings
from .allocation import raise_for_multi_product_unit_group_violations
from .database import Base, SessionLocal, engine, get_db
from .flow_unit_semantics import (
    build_unit_reference_maps,
    collect_flow_default_unit_conversion_violations,
    resolve_flow_port_unit_semantics,
)
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
    TidasMissingFlowSummaryItem,
    MissingFlowSummaryResponse,
    HybridNode,
    HandleValidationRequest,
    HandleValidationResponse,
    CreateFlowRequest,
    FlowOutExtended,
    CreateFlowResponse,
    FlowCandidate,
)
from .ingest import convert_unit_value, import_flows_from_file, import_unit_groups_from_excel, load_ef31_flow_uuid_set
from .ingest import import_processes_from_json
from .preprocess import normalize_graph_units_to_reference
from .solver import to_tiangong_like
from .solver_adapter import run_tiangong_lcia
from .schema_maintenance import (
    backfill_tidas_unit_group_sources,
    ensure_flow_catalog_tidas_columns,
    ensure_unit_group_source_columns,
)
from .pts_validate import validate_pts_compile
from .pts_compile import PTS_COMPILE_SCHEMA_VERSION, compile_pts, compute_pts_graph_hash
from .services import graph_contract as _gc
from .services import graph_storage as _gs
from .services import catalog_cache as _cc
from .services import pts_resources as _pr
from .api.projects import _base_router, _api_router as _api_projects_router
from .api.paginated_projects import _router as _paginated_projects_router
from .api.export_tidas import _base_router as _export_tidas_base_router, _api_router as _export_tidas_api_router
from .api.ef31_import import _base_router as _ef31_import_base_router, _api_router as _ef31_import_api_router
from .api.tidas_import import _base_router as _tidas_import_base_router, _api_router as _tidas_import_api_router
from .api.reference_catalog import _base_router as _ref_catalog_base_router, _api_router as _ref_catalog_api_router

# Re-export graph contract functions (authoritative implementations live in
# ``app.services.graph_contract``; keep aliases so the rest of main.py and
# existing tests can move incrementally.
is_graph_non_empty = _gc.is_graph_non_empty
normalize_graph_node_kinds = _gc.normalize_graph_node_kinds
normalize_graph_product_flags = _gc.normalize_graph_product_flags
validate_product_unit_group_consistency = _gc.validate_product_unit_group_consistency
validate_unique_process_uuid = _gc.validate_unique_process_uuid
normalize_graph_edge_port_ids = _gc.normalize_graph_edge_port_ids
_resolve_edge_port_id = _gc._resolve_edge_port_id
_preserve_or_default_handle = _gc._preserve_or_default_handle
_resolve_edge_port_id_for_node = _gc._resolve_edge_port_id_for_node
validate_port_bucket_direction_consistency = _gc.validate_port_bucket_direction_consistency
validate_edge_binding_and_uniqueness = _gc.validate_edge_binding_and_uniqueness
validate_market_input_constraints = _gc.validate_market_input_constraints
validate_non_product_input_constraints = _gc.validate_non_product_input_constraints
validate_edge_product_role_alignment = _gc.validate_edge_product_role_alignment
validate_graph_contract = _gc.validate_graph_contract
validate_graph_flow_type_contract = _gc.validate_graph_flow_type_contract
_normalize_port_display_name = _gc._normalize_port_display_name
validate_graph_port_names_against_flow_catalog = _gc.validate_graph_port_names_against_flow_catalog
_truncate_text_preview = _gc._truncate_text_preview
_looks_like_utf8_latin1_mojibake = _gc._looks_like_utf8_latin1_mojibake
validate_graph_text_encoding = _gc.validate_graph_text_encoding
_port_id_from_handle = _gc._port_id_from_handle

# Re-export graph storage functions (authoritative implementations live in
# ``app.services.graph_storage``; keep aliases so the rest of main.py and
# the test suite don't need to change their import paths.
_compute_graph_hash_from_graph_json = _gs.compute_graph_hash_from_graph_json
_compute_graph_hash_from_slim_graph = _gs.compute_graph_hash_from_slim_graph
_compute_graph_hash_from_graph = _gs.compute_graph_hash_from_graph
_slim_graph_for_storage = _gs.slim_graph_for_storage
_hydrate_graph_for_api = _gs.hydrate_graph_for_api
STORAGE_SLIM_VERSION = _gs._STORAGE_SLIM_VERSION
_FLOWPORT_DERIVED_KEYS = _gs._FLOWPORT_DERIVED_KEYS
_slim_flowport_for_storage = _gs.slim_flowport_for_storage
_slim_node_for_storage = _gs.slim_node_for_storage
_slim_pts_node_for_storage = _gs.slim_pts_node_for_storage

# Re-export shared catalog/cache helpers for legacy routes that still live in
# main.py and for incremental route modules that lazy-import old names.
_cache_get = _cc.cache_get
_cache_set = _cc.cache_set
_cache_revision = _cc.cache_revision
_cache_invalidate_prefix = _cc.cache_invalidate_prefix
_cache_bump_revision = _cc.cache_bump_revision
_build_etag_for_payload = _cc.build_etag_for_payload
_is_if_none_match_hit = _cc.is_if_none_match_hit
_invalidate_management_caches = _cc.invalidate_management_caches

# PTS resource helpers live in ``app.services.pts_resources`` after Stage 6C.
# Keep these aliases for legacy main.py helpers that still run during project
# save/load until the remaining PTS code is fully migrated.
_load_pts_active_external_artifact = _pr._load_pts_active_external_artifact
_build_projected_pts_ports_from_external = _pr._build_projected_pts_ports_from_external
_normalize_pts_shell_node_kind = _pr._normalize_pts_shell_node_kind
_enrich_node_ports_flow_name_en = _pr._enrich_node_ports_flow_name_en
_collect_connected_port_ids_for_pts_node = _pr._collect_connected_port_ids_for_pts_node
_overlay_pts_port_visibility = _pr._overlay_pts_port_visibility
_load_published_compile_rows_for_graph = _pr._load_published_compile_rows_for_graph
build_flattened_graph_for_run_pts = _pr.build_flattened_graph_for_run_pts
_build_compile_graph_from_pts_resource = _pr._build_compile_graph_from_pts_resource
extract_pts_definition = _pr.extract_pts_definition
upsert_pts_definition = _pr.upsert_pts_definition
upsert_pts_compile_artifact = _pr.upsert_pts_compile_artifact
build_pts_external_payload = _pr.build_pts_external_payload
_enrich_pts_external_payload_flow_name_en = _pr._enrich_pts_external_payload_flow_name_en
upsert_pts_external_artifact = _pr.upsert_pts_external_artifact
_upsert_pts_resource_from_definition = _pr._upsert_pts_resource_from_definition
_build_pts_shell_snapshot_from_external = _pr._build_pts_shell_snapshot_from_external
_get_pts_resource_ports_policy = _pr._get_pts_resource_ports_policy

# Re-export PTS operations helpers (migrated from main.py to pts_operations)
from .services.pts_operations import (
    _compile_pts_on_save_if_needed,
    _sync_pts_resources_from_graph,
    _bind_pts_published_versions_for_graph,
    _enrich_graph_flow_name_en,
    _build_pts_validation_summary,
    _project_pts_external_ports_into_graph,
    _canonicalize_pts_nodes_for_main_graph_save,
    _repair_pts_publication_from_resource,
    _is_pts_publication_auto_repairable,
)

# Re-export graph normalization helpers (migrated from main.py to graph_contract)
from .services.graph_contract import (
    _normalize_graph_json_for_storage,
    _enrich_market_process_input_sources_in_graph_json,
    _validate_process_name_uniqueness_for_graph_json,
    safe_handle_validation_from_graph_json,
    analyze_handle_consistency,
    analyze_handle_consistency_from_graph_json,
)

# Re-export project/version helpers from services
from .services.project_versions import (
    _build_project_out,
    _create_project_version_from_graph_json,
    _latest_version_by_project_id,
    _resolve_model_version_graph_hash,
    _auto_prune_versions,
    _prune_model_versions_retention,
)
app = FastAPI(title=settings.app_name, version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Include modular routers ──
app.include_router(_base_router)
app.include_router(_api_projects_router)
app.include_router(_paginated_projects_router, prefix="/api/projects")
app.include_router(_export_tidas_base_router)
app.include_router(_export_tidas_api_router)
app.include_router(_ef31_import_base_router)
app.include_router(_ef31_import_api_router)
app.include_router(_tidas_import_base_router)
app.include_router(_tidas_import_api_router)
app.include_router(_ref_catalog_base_router)
# ── reference_data router (Stage 6B-lite: stats only) ──
from .api.reference_data import _api_router as _reference_data_api_router
from .api.pts import _pts_base_router, _pts_api_router
from .api.reference_data import _base_router as _reference_data_base_router

app.include_router(_ref_catalog_api_router)
app.include_router(_reference_data_base_router)
app.include_router(_reference_data_api_router)
app.include_router(_pts_base_router)
app.include_router(_pts_api_router)


def get_model_or_404(db: Session, model_id: str) -> Model:
    model = db.query(Model).filter(Model.id == model_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Project not found")
    return model

def resolve_project_id_for_run(payload: RunRequest, db: Session) -> str:
    if payload.project_id:
        return get_model_or_404(db, payload.project_id).id
    if payload.model_id:
        return get_model_or_404(db, payload.model_id).id

    if payload.model_version_id:
        if ":" in payload.model_version_id:
            project_id = payload.model_version_id.split(":", 1)[0].strip()
            if project_id:
                return get_model_or_404(db, project_id).id
        version_row = db.query(ModelVersion).filter(ModelVersion.id == payload.model_version_id).first()
        if version_row:
            return version_row.model_id

    raise HTTPException(status_code=400, detail="project_id is required for PTS compile/save context")

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
            row["unit"] = unit
        enriched.append(row)
    return enriched


def _build_run_job_request_json(payload) -> dict:
    """Build a lightweight request_json for RunJob storage.

    By default stores only metadata instead of the full graph with all nodes/edges.
    Set env NEBULA_DEBUG_STORE_RUN_GRAPH=1 to restore full graph storage temporarily.
    """
    import os
    if os.getenv("NEBULA_DEBUG_STORE_RUN_GRAPH", "0").lower() in ("1", "true", "yes"):
        return payload.graph.model_dump()

    graph = payload.graph
    nodes = graph.nodes if hasattr(graph, "nodes") else []
    edges = graph.edges if hasattr(graph, "edges") else []

    functional_unit = None
    reference_product = None
    for node in nodes:
        meta = getattr(node, "metadata", None)
        if meta and isinstance(meta, dict):
            if meta.get("functionalUnit") and not functional_unit:
                functional_unit = meta["functionalUnit"]
            if meta.get("referenceProduct") and not reference_product:
                reference_product = meta["referenceProduct"]
            break

    return {
        "model_version_id": payload.model_version_id,
        "project_id": getattr(payload, "project_id", None),
        "force_recompile": getattr(payload, "force_recompile", False),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "functional_unit": functional_unit,
        "reference_product": reference_product,
        "graph_hash": getattr(graph, "graph_hash", None)
            if hasattr(graph, "graph_hash") and graph.graph_hash
            else None,
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
    db: Session,
    graph: HybridGraph,
    process_index: object,
    values: object,
    unit_factor_by_group_and_name: dict[tuple[str, str], float],
    reference_unit_by_group: dict[str, str],
) -> tuple[list[dict], dict[str, dict], object]:
    if not isinstance(process_index, list) or not process_index:
        return [], {}, values

    products_by_process: dict[str, list[dict]] = {}
    seen_keys: set[str] = set()
    for node in graph.nodes:
        process_uuid = str(node.process_uuid or "")
        process_name = str(node.name or process_uuid or node.id)
        process_location = str(node.location or "")
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
            unit_semantics = resolve_flow_port_unit_semantics(
                db,
                port,
                unit_factor_by_group_and_name=unit_factor_by_group_and_name,
                reference_unit_by_group=reference_unit_by_group,
            ).as_dict()
            products_by_process.setdefault(process_uuid, []).append(
                {
                    "product_key": product_key,
                    "process_uuid": process_uuid,
                    "process_name": process_name,
                    "process_location": process_location,
                    "product_port_id": product_port_id,
                    "product_flow_uuid": product_flow_uuid,
                    "product_name": str(port.name or product_flow_uuid),
                    "is_reference_product": bool(reference_port is not None and str(reference_port.id or "") == product_port_id),
                    "unit": str(port.unit or ""),
                    "unit_group": str(port.unitGroup or ""),
                    "unit_group_switch": dict(port.unitGroupSwitch or {}),
                    "unit_semantics": unit_semantics,
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
                    "process_location": item["process_location"],
                    "product_port_id": item["product_port_id"],
                    "product_flow_uuid": item["product_flow_uuid"],
                    "product_name": item["product_name"],
                    "is_reference_product": bool(item["is_reference_product"]),
                }
            )
            unit_semantics = item.get("unit_semantics") or {}
            product_unit_map[item["product_key"]] = {
                "unit": item["unit"],
                "unit_group": item["unit_group"],
                "current_unit": unit_semantics.get("current_unit") or item["unit"],
                "current_unit_group": unit_semantics.get("current_unit_group") or item["unit_group"],
                "flow_default_unit": unit_semantics.get("flow_default_unit") or item["unit"],
                "flow_default_unit_group": unit_semantics.get("flow_default_unit_group") or item["unit_group"],
                "result_factor_to_flow_default_unit": unit_semantics.get("result_factor_to_flow_default_unit"),
                "amount_in_flow_default_unit": unit_semantics.get("amount_in_flow_default_unit"),
                "unit_semantics_ok": unit_semantics.get("ok"),
                "unit_semantics_reason": unit_semantics.get("reason"),
                "unit_group_switch": item.get("unit_group_switch") or {},
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
            "source_policy": "VARCHAR(32) DEFAULT 'open_mixed'",
            "allowed_lcia_scope": "VARCHAR(32) DEFAULT 'ef31_only'",
            "status": "VARCHAR(32) DEFAULT 'active'",
            "updated_at": "DATETIME",
        }
        for name, ddl_type in expected_columns.items():
            if name in columns:
                continue
            conn.execute(text(f"ALTER TABLE models ADD COLUMN {name} {ddl_type}"))
            added_columns.append(name)

        conn.execute(text("UPDATE models SET status='active' WHERE status IS NULL OR TRIM(status)=''"))
        conn.execute(text("UPDATE models SET source_policy='open_mixed' WHERE source_policy IS NULL OR TRIM(source_policy)=''"))
        conn.execute(text("UPDATE models SET allowed_lcia_scope='ef31_only' WHERE allowed_lcia_scope IS NULL OR TRIM(allowed_lcia_scope)=''"))
        conn.execute(text("UPDATE models SET updated_at=created_at WHERE updated_at IS NULL"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_models_name ON models (name)"))
        created_indexes.append("ix_models_name")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_models_status ON models (status)"))
        created_indexes.append("ix_models_status")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_models_source_policy ON models (source_policy)"))
        created_indexes.append("ix_models_source_policy")
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_models_allowed_lcia_scope ON models (allowed_lcia_scope)"))
        created_indexes.append("ix_models_allowed_lcia_scope")

    return {
        "table": "models",
        "column_added": added_columns,
        "index_created": created_indexes,
        "status": "ok",
    }

# ---------------------------------------------------------------------------
# Custom flow creation (Stage 1 — open-source)
# ---------------------------------------------------------------------------

BUILTIN_ELEMENTARY_FLOW_SOURCE = "ef3.1"
BUILTIN_INTERMEDIATE_FLOW_SOURCE = "tiangong"
EXTERNAL_ELEMENTARY_FLOW_SOURCE = "external_import"
TIDAS_FLOW_IMPORT_SOURCE = "tidas_import"
TIDAS_BUNDLE_FLOW_IMPORT_SOURCE = "tidas_bundle_import"
PROTECTED_BUILTIN_FLOW_SOURCES = {
    BUILTIN_ELEMENTARY_FLOW_SOURCE,
    BUILTIN_INTERMEDIATE_FLOW_SOURCE,
}

def _is_protected_builtin_flow(row: FlowRecord) -> bool:
    return not bool(row.is_custom) and _safe_str(row.source) in PROTECTED_BUILTIN_FLOW_SOURCES

def _ensure_custom_flow_columns() -> dict:
    """Ensure ``source`` / ``is_custom`` columns exist on flow_catalog.

    Runs at startup so ordinary FlowRecord queries never hit missing-columns
    errors before POST /api/flows is ever called.

    Uses ``engine.begin()`` which runs in autocommit mode for DDL.
    - On PostgreSQL: uses ``ADD COLUMN IF NOT EXISTS`` (PG >= 9.2) which never
      aborts the transaction.
    - On SQLite: does not support ``IF NOT EXISTS`` on ``ADD COLUMN``, so
      pre-inspects columns and only emits ALTER when needed.
    - Unknown dialects: fail fast with a clear error — only SQLite and
      PostgreSQL are supported.
    """
    added_columns: list[str] = []
    table_exists = False

    with engine.begin() as conn:
        inspector = inspect(conn)
        table_exists = inspector.has_table("flow_catalog")
        if not table_exists:
            return {"table": "flow_catalog", "added_columns": added_columns, "status": "skipped_table_missing"}

        columns = {col["name"] for col in inspector.get_columns("flow_catalog")}
        dialect_name = conn.engine.dialect.name

        for col_name, col_type in [
            ("source", "VARCHAR(64)"),
            ("is_custom", "BOOLEAN NOT NULL DEFAULT false"),
            ("tidas_compatible", "BOOLEAN NOT NULL DEFAULT false"),
            ("tidas_unit_group", "VARCHAR(128)"),
            ("tidas_flow_property_uuid", "VARCHAR(64)"),
            ("tidas_reference_source", "VARCHAR(128)"),
            ("allocation_properties", "JSONB" if engine.dialect.name == "postgresql" else "JSON"),
        ]:
            if col_name in columns:
                continue
            # Dialect-specific safe-add strategy
            if dialect_name == "postgresql":
                conn.execute(text(f"ALTER TABLE flow_catalog ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            elif dialect_name == "sqlite":
                conn.execute(text(f"ALTER TABLE flow_catalog ADD COLUMN {col_name} {col_type}"))
            else:
                # Fail fast for unknown dialects — only SQLite and PostgreSQL are supported.
                raise RuntimeError(
                    f"Unsupported database dialect '{dialect_name}'. "
                    "Nebula LCA supports only SQLite and PostgreSQL."
                )
            added_columns.append(col_name)

    if not added_columns:
        status = "already_complete"
    else:
        status = "ok" if table_exists else "skipped_table_missing"
    return {"table": "flow_catalog", "added_columns": added_columns, "status": status}


def _ensure_unit_group_source_columns() -> dict:
    return ensure_unit_group_source_columns(engine)


@app.on_event("startup")
def _ensure_source_compliance_schema_on_startup() -> None:
    _ensure_custom_flow_columns()
    _ensure_unit_group_source_columns()
    db = SessionLocal()
    try:
        backfill_tidas_unit_group_sources(db)
    finally:
        db.close()

def _ensure_flow_catalog_fts_triggers(*, db: Session | None = None) -> dict:
    """Ensure SQLite triggers exist to keep flow_catalog_fts in sync with flow_catalog.

    Triggers fire after INSERT/UPDATE/DELETE on flow_catalog and maintain
    the FTS5 virtual table automatically. This means new custom flows
    created via POST /api/flows will immediately appear in search results.

    Safe to call at startup — idempotent (IF NOT EXISTS on triggers).
    """
    from sqlalchemy import text as sa_text

    url = settings.database_url
    if not url.strip().lower().startswith("sqlite"):
        return {"executed": False, "reason": "not sqlite"}

    try:
        with engine.begin() as conn:
            fts_exists = conn.execute(sa_text(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flow_catalog_fts'"
            )).fetchone() is not None
            if not fts_exists:
                return {"executed": False, "reason": "flow_catalog_fts_missing"}

            # Create triggers only if they don't exist
            # SQLite doesn't support IF NOT EXISTS for triggers, so we check first
            cursor = conn.execute(sa_text(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'fts_flow_catalog%'"
            ))
            existing = {row[0] for row in cursor.fetchall()}

            needed = {
                "fts_flow_catalog_ai": "CREATE TRIGGER fts_flow_catalog_ai AFTER INSERT ON flow_catalog BEGIN "
                    "INSERT INTO flow_catalog_fts(rowid, flow_name, flow_name_en, flow_uuid) "
                    "VALUES (new.rowid, new.flow_name, new.flow_name_en, new.flow_uuid); END",
                "fts_flow_catalog_ad": "CREATE TRIGGER fts_flow_catalog_ad AFTER DELETE ON flow_catalog BEGIN "
                    "DELETE FROM flow_catalog_fts WHERE rowid = old.rowid; END",
                "fts_flow_catalog_au": "CREATE TRIGGER fts_flow_catalog_au AFTER UPDATE ON flow_catalog BEGIN "
                    "DELETE FROM flow_catalog_fts WHERE rowid = old.rowid; "
                    "INSERT INTO flow_catalog_fts(rowid, flow_name, flow_name_en, flow_uuid) "
                    "VALUES (new.rowid, new.flow_name, new.flow_name_en, new.flow_uuid); END",
            }

            created = []
            for name, sql in needed.items():
                if name not in existing:
                    conn.execute(sa_text(sql))
                    created.append(name)

            return {
                "executed": True,
                "created_triggers": created,
                "total_triggers": len(needed),
            }
    except Exception as exc:
        return {"executed": False, "error": str(exc)}

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
            default_source=BUILTIN_ELEMENTARY_FLOW_SOURCE,
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
            replace_existing=True,
            default_flow_type="Product flow",
            ef31_flow_index_path=None,
            default_source=BUILTIN_INTERMEDIATE_FLOW_SOURCE,
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


def _has_non_ecoinvent_elementary_flows(graph: HybridGraph, db: Session) -> bool:
    """Check if any biosphere port references a non-ecoinvent elementary flow.

    Returns True if any elementary flow in the graph has source != 'ecoinvent'.
    Flows with source=None or source in {'tiangong', 'ef3.1', 'tidas', 'user_custom'}
    are all considered non-ecoinvent.
    """
    # Collect all biosphere flow UUIDs from the graph
    biosphere_uuids: set[str] = set()
    for node in (graph.nodes or []):
        for port in chain(getattr(node, "inputs", []) or [], getattr(node, "outputs", []) or []):
            port_type = getattr(port, "type", None)
            if port_type == "biosphere":
                uuid_val = str(getattr(port, "flowUuid", getattr(port, "flow_uuid", "") or ""))
                if uuid_val:
                    biosphere_uuids.add(uuid_val.lower().strip())

    if not biosphere_uuids:
        return False

    # Batch lookup sources from DB
    uuid_list = list(biosphere_uuids)
    queries = [
        db.query(FlowRecord.source).filter(func.lower(FlowRecord.flow_uuid) == uuid).first()
        for uuid in uuid_list
    ]
    for source_val in queries:
        source = str(source_val.source if source_val else "").strip().lower() if source_val else ""
        # Only "ecoinvent" is considered eco-compatible
        if not source.startswith("ecoinvent"):
            return True
    return False


def _solver_flow_type_by_uuid_cached(db: Session) -> dict[str, str]:
    return {
        flow_uuid: str(meta[2]).strip()
        for flow_uuid, meta in _flow_meta_by_uuid_cached(db).items()
        if flow_uuid and meta and str(meta[2] or "").strip()
    }


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
    # Priority 1: Read from flowInformation.referenceUnit/unitGroup if present
    flow_info = flow_dataset.get("flowInformation") if isinstance(flow_dataset.get("flowInformation"), dict) else {}
    ref_unit = _safe_str(flow_info.get("referenceUnit"))
    unit_group = _safe_str(flow_info.get("unitGroup"))
    if ref_unit and unit_group:
        return ref_unit, unit_group

    # Priority 2: Infer from flowProperties
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
        # Allow empty lists for optional datasets (processes/flows may be empty in some exports)
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

        # Check for process/flow files - but allow empty datasets (processDataSet.json with [])
        # Only error if no files found AND no processDataSet.json exists
        process_dataset_path = _resolve_bundle_path("processes/processDataSet.json")
        flow_dataset_path = _resolve_bundle_path("flows/flowDataSet.json")

        if not process_items and process_dataset_path not in names:
            errors.append(f"{source_name}: no process json found under {process_dir}/")
        if not flow_items and flow_dataset_path not in names:
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

def import_elementary_flows(payload: ImportFlowsRequest, db: Session = Depends(get_db)) -> ImportFlowsResponse:
    result = import_flows_from_file(
        db,
        file_path=payload.file_path,
        sheet_name=payload.sheet_name,
        mapping=payload.mapping,
        replace_existing=payload.replace_existing,
        default_flow_type=payload.default_flow_type or "Elementary flow",
        ef31_flow_index_path=payload.ef31_flow_index_path,
        default_source=EXTERNAL_ELEMENTARY_FLOW_SOURCE,
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

    unit_factor_by_group_and_name, reference_unit_by_group = build_unit_reference_maps(db)

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

    normalized_graph = _normalize_graph_json_for_storage(payload.graph.model_dump(mode="python"))
    slim_graph = _slim_graph_for_storage(normalized_graph)
    graph_hash = _compute_graph_hash_from_slim_graph(slim_graph)
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
    slim_graph_json = _slim_graph_for_storage(normalized_graph_json)
    version = ModelVersion(
        model_id=model.id,
        version=next_version,
        graph_hash=graph_hash,
        hybrid_graph_json=slim_graph_json,
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
    raw_graph = _hydrate_graph_for_api(latest_row.hybrid_graph_json if isinstance(latest_row.hybrid_graph_json, dict) else {}, db=db)
    graph = HybridGraph.model_validate(raw_graph)
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


def get_model_version(project_id: str, version: int, db: Session = Depends(get_db)) -> dict:
    record = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == project_id, ModelVersion.version == version)
        .first()
    )
    if not record:
        raise HTTPException(status_code=404, detail="Model version not found")
    raw_graph = _hydrate_graph_for_api(record.hybrid_graph_json if isinstance(record.hybrid_graph_json, dict) else {}, db=db)
    graph = HybridGraph.model_validate(raw_graph)
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
    raw_graph = _hydrate_graph_for_api(version.hybrid_graph_json if isinstance(version.hybrid_graph_json, dict) else {}, db=db)
    graph = HybridGraph.model_validate(raw_graph)
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
    raw_graph = _hydrate_graph_for_api(version.hybrid_graph_json if isinstance(version.hybrid_graph_json, dict) else {}, db=db)
    graph = HybridGraph.model_validate(raw_graph)
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
        adapter_result = run_tiangong_lcia(
            normalized_graph,
            flow_type_by_uuid=flow_type_by_uuid,
            lcia_methods=payload.lcia_methods,
        )
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
        db=db,
        graph=payload.graph,
        process_index=solver_output.get("process_index", []),
        values=scaled_values,
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
        reference_unit_by_group=reference_unit_by_group,
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

    request_json = _build_run_job_request_json(payload)
    run_job = RunJob(
        model_version_id=payload.model_version_id,
        status=status,
        request_json=request_json,
        result_json=solved,
        message=message,
        created_at=datetime.utcnow(),
        finished_at=datetime.utcnow(),
    )
    db.add(run_job)
    db.commit()
    db.refresh(run_job)

    return status, run_job.id, solved, tiangong_like


@app.post("/api/model/run", response_model=RunResponse)
@app.post("/model/run", response_model=RunResponse)
def run_model(payload: RunRequest, db: Session = Depends(get_db)) -> RunResponse:
    validate_graph_contract(payload.graph, require_non_empty=False, allow_pts_nodes=True)
    validate_graph_flow_type_contract(payload.graph, db=db, stage="run_model")
    validate_graph_port_names_against_flow_catalog(payload.graph, db=db, stage="run_model")
    flow_default_unit_violations = collect_flow_default_unit_conversion_violations(payload.graph, db)
    if flow_default_unit_violations:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "FLOW_DEFAULT_UNIT_CONVERSION_REQUIRED",
                "message": "Flow current modelling units must be convertible back to each flow default unit before calculation.",
                "violations": flow_default_unit_violations,
            },
        )
    raise_for_multi_product_unit_group_violations(payload.graph)

    # Resolve project_id for source-policy checks (prefer payload project_id, fall back to model_version lookup)
    project_id = resolve_project_id_for_run(payload, db) if payload.model_version_id else getattr(payload, "project_id", None)

    # Phase 1: source-policy LCIA scope validation
    if project_id:
        try:
            from .source_policy import validate_lcia_scope_compatibility
            lcia_methods = payload.lcia_methods or ["EF v3.1"]
            validate_lcia_scope_compatibility(
                model_id=project_id,
                graph=payload.graph.model_dump(mode="python"),
                lcia_methods=lcia_methods,
                db=db,
            )
        except HTTPException:
            raise
        except Exception:
            # If source-policy validation fails, fall through to existing legacy check
            pass

    # Legacy LCIA method compatibility check (still active for open_mixed projects
    # and when source-policy validation didn't catch the issue).
    lcia_methods = payload.lcia_methods or ["EF v3.1"]
    has_non_eco_elementary = _has_non_ecoinvent_elementary_flows(payload.graph, db)
    if has_non_eco_elementary:
        non_ef31 = [m for m in lcia_methods if m != "EF v3.1"]
        if non_ef31:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "LCIA_METHOD_INCOMPATIBLE_WITH_ELEMENTARY_FLOWS",
                    "message": "模型包含天工/TIDAS/EF 来源基本流，只能使用 EF v3.1。非 ecoinvent 基本流无法匹配 ecoinvent 专属 LCIA 方法。",
                    "evidence": {
                        "requested_methods": non_ef31,
                        "allowed_methods": ["EF v3.1"],
                    },
                },
            )

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
                lcia_methods=["EF v3.1"],
            )

        flow_default_unit_violations = collect_flow_default_unit_conversion_violations(effective_payload.graph, db)
        if flow_default_unit_violations:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "FLOW_DEFAULT_UNIT_CONVERSION_REQUIRED",
                    "message": "Flow current modelling units must be convertible back to each flow default unit before calculation.",
                    "violations": flow_default_unit_violations,
                },
            )
        raise_for_multi_product_unit_group_violations(effective_payload.graph)
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

# Stage 3: include catalog routers after legacy routes so specific legacy
# paths (for example /api/reference/flows/missing/summary) keep priority over
# dynamic catalog paths.
from .api.flows import api_router as _flows_router
from .api.processes import api_router as _processes_router

app.include_router(_flows_router)
app.include_router(_processes_router)
