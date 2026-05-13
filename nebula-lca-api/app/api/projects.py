"""Project/Version API routes (GET, POST, PATCH, DELETE).

Extracted from ``app.main`` for Stage 4A. Uses ``APIRouter`` pattern;
the router is included in main.py via ``app.include_router(_router)``.

URL paths preserved to match the original ``@app.xxx`` registrations.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from sqlalchemy import func as sqla_func
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import Model, ModelVersion, PtsResource, RunJob
from ..schemas import (
    DeleteProjectResponse,
    ModelCreateResponse,
    ModelVersionCreateRequest,
    ModelVersionOut,
    PaginatedProjectsResponse,
    ProjectCreateRequest,
    ProjectDuplicateRequest,
    ProjectFlowNameSyncResponse,
    ProjectOut,
    ProjectUpdateRequest,
    RepairProjectIntegrityResponse,
)
from ..services.project_versions import (
    _auto_prune_versions,
    _build_flow_sync_state_for_graph_json,
    _build_project_integrity_summary,
    _build_project_out,
    _latest_version_by_project_id,
    _resolve_model_version_graph_hash,
    _sync_project_latest_version_flow_names,
)
from ..services.graph_contract import (
    normalize_graph_product_flags,
    validate_graph_contract,
    validate_graph_flow_type_contract,
    validate_graph_port_names_against_flow_catalog,
)
from ..services.graph_storage import (
    compute_graph_hash_from_slim_graph,
    hydrate_graph_for_api,
    slim_graph_for_storage,
)
from ..schemas import HybridGraph

# ── Cache helpers ────────────────────────────────────────────────────────

_cache_helpers = None


def _ensure_cache_helpers():
    global _cache_helpers
    if _cache_helpers is not None:
        return
    # fmt: off
    from ..services.catalog_cache import (
        cache_get, cache_set, cache_revision,
        build_etag_for_payload, is_if_none_match_hit,
        invalidate_management_caches,
    )
    # fmt: on
    _cache_helpers = {
        "_cache_get": cache_get,
        "_cache_set": cache_set,
        "_cache_revision": cache_revision,
        "_build_etag_for_payload": build_etag_for_payload,
        "_is_if_none_match_hit": is_if_none_match_hit,
        "_invalidate_management_caches": invalidate_management_caches,
    }


# ── Inline helpers extracted from main.py ─────────────────────────────────

def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


_PROJECT_STATUS_SET = {"active", "draft", "archived"}


def _normalize_project_status(value: str | None) -> str:
    raw = _safe_str(value)
    if not raw:
        return "active"
    normalized = raw.lower()
    if normalized not in _PROJECT_STATUS_SET:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REQUEST", "message": f"Invalid status: {raw}. Allowed: active|draft|archived"},
        )
    return normalized


def _get_model_or_404(db: Session, model_id: str) -> Model:
    model = db.query(Model).filter(Model.id == model_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Project not found")
    return model


def _safe_handle_validation(graph_json: dict) -> dict:
    from ..services.graph_contract import safe_handle_validation_from_graph_json
    return safe_handle_validation_from_graph_json(graph_json)


def _ensure_projects_management_schema() -> dict:
    from ..database import engine as _db_engine
    from sqlalchemy import inspect as _sqla_inspect, text as _sqla_text

    added_columns: list[str] = []
    created_indexes: list[str] = []

    with _db_engine.begin() as conn:
        inspector = _sqla_inspect(conn)
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
            conn.execute(_sqla_text(f"ALTER TABLE models ADD COLUMN {name} {ddl_type}"))
            added_columns.append(name)

        conn.execute(_sqla_text("UPDATE models SET status='active' WHERE status IS NULL OR TRIM(status)=''"))
        conn.execute(_sqla_text("UPDATE models SET updated_at=created_at WHERE updated_at IS NULL"))

        conn.execute(_sqla_text("CREATE INDEX IF NOT EXISTS ix_models_name ON models (name)"))
        created_indexes.append("ix_models_name")
        conn.execute(_sqla_text("CREATE INDEX IF NOT EXISTS ix_models_status ON models (status)"))
        created_indexes.append("ix_models_status")

    return {
        "table": "models",
        "column_added": added_columns,
        "index_created": created_indexes,
        "status": "ensured",
    }


# ── Lazily-load project-version PTS helpers from pts_operations ─────────

_get_pts_helpers = None


def _ensure_pts_helpers():
    global _get_pts_helpers
    if _get_pts_helpers is not None:
        return
    # fmt: off
    from ..services.pts_operations import (
        _compile_pts_on_save_if_needed,
        _project_pts_external_ports_into_graph,
        _build_pts_validation_summary,
        _enrich_graph_flow_name_en,
        _canonicalize_pts_nodes_for_main_graph_save,
        _repair_pts_publication_from_resource,
    )
    from ..services.graph_contract import _normalize_graph_json_for_storage
    # fmt: on
    _get_pts_helpers = {
        "_compile_pts_on_save_if_needed": _compile_pts_on_save_if_needed,
        "_normalize_graph_json_for_storage": _normalize_graph_json_for_storage,
        "_project_pts_external_ports_into_graph": _project_pts_external_ports_into_graph,
        "_build_pts_validation_summary": _build_pts_validation_summary,
        "_enrich_graph_flow_name_en": _enrich_graph_flow_name_en,
        "_build_flow_sync_state_for_graph_json": _build_flow_sync_state_for_graph_json,
        "_build_project_integrity_summary": _build_project_integrity_summary,
        "_canonicalize_pts_nodes_for_main_graph_save": _canonicalize_pts_nodes_for_main_graph_save,
        "_repair_pts_publication_from_resource": _repair_pts_publication_from_resource,
    }


# ── Routers ───────────────────────────────────────────────────────────────
# Two routers for the two URL namespaces:
#   _base_router  →  /projects  (no /api prefix)
#   _api_router   →  /api/projects  (with /api prefix)


_base_router = APIRouter(prefix="/projects")
_api_router = APIRouter(prefix="/api/projects")


# ══════════════════════════════════════════════════════════════════════════
#  /projects  (base) routes
# ══════════════════════════════════════════════════════════════════════════


@_base_router.post("", response_model=ProjectOut)
def create_project(
    payload: ProjectCreateRequest, db: Session = Depends(get_db)
) -> ProjectOut:
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


@_base_router.get("", response_model=list[ProjectOut])
def list_projects(db: Session = Depends(get_db)) -> list[ProjectOut]:
    models = db.query(Model).order_by(Model.created_at.desc()).all()
    latest_by_project = _latest_version_by_project_id(db)
    return [_build_project_out(model, latest_by_project.get(str(model.id))) for model in models]


@_base_router.get("/{project_id}", response_model=ProjectOut)
def get_project(project_id: str, db: Session = Depends(get_db)) -> ProjectOut:
    model = db.query(Model).filter(Model.id == project_id).first()
    if not model:
        raise HTTPException(status_code=404, detail="Project not found")
    latest = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    return _build_project_out(model, latest)


@_base_router.delete("/{project_id}", response_model=DeleteProjectResponse)
def delete_project(
    project_id: str, db: Session = Depends(get_db)
) -> DeleteProjectResponse:
    _ensure_cache_helpers()
    model = db.query(Model).filter(Model.id == project_id).first()
    if model is None:
        raise HTTPException(status_code=404, detail="Project not found")
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
    _cache_helpers["_invalidate_management_caches"](projects=True, stats=True)

    return DeleteProjectResponse(
        project_id=project_id,
        deleted_models=1,
        deleted_versions=deleted_versions,
        cleared_run_job_refs=cleared,
    )


@_base_router.post("/{project_id}/versions", response_model=ModelCreateResponse)
def create_project_version(
    project_id: str,
    payload: ModelVersionCreateRequest,
    compile_pts_on_save: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> ModelCreateResponse:
    _ensure_cache_helpers()
    _ensure_pts_helpers()
    normalize_graph_product_flags(payload.graph)
    _get_pts_helpers["_canonicalize_pts_nodes_for_main_graph_save"](
        db=db, project_id=project_id, graph=payload.graph
    )
    validate_graph_contract(payload.graph, require_non_empty=True, allow_pts_nodes=True)
    validate_graph_flow_type_contract(payload.graph, db=db, stage="save_version")
    validate_graph_port_names_against_flow_catalog(payload.graph, db=db, stage="save_version")

    model = db.query(Model).filter(Model.id == project_id).first()
    if model is None:
        raise HTTPException(status_code=404, detail="Project not found")

    normalized_graph = _get_pts_helpers["_normalize_graph_json_for_storage"](
        payload.graph.model_dump(mode="python")
    )
    slim_graph = slim_graph_for_storage(normalized_graph)
    graph_hash = compute_graph_hash_from_slim_graph(slim_graph)

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
            pts_compile_summary = _get_pts_helpers["_compile_pts_on_save_if_needed"](
                db=db, project_id=model.id, graph=payload.graph, compile_on_save=compile_pts_on_save,
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
        db.query(sqla_func.max(ModelVersion.version))
        .filter(ModelVersion.model_id == model.id)
        .scalar()
    )
    next_version = (latest_version or 0) + 1

    version = ModelVersion(
        model_id=model.id,
        version=next_version,
        graph_hash=graph_hash,
        hybrid_graph_json=slim_graph,
    )
    model.updated_at = datetime.utcnow()
    db.add(version)
    db.commit()
    db.refresh(version)
    pts_compile_summary = _get_pts_helpers["_compile_pts_on_save_if_needed"](
        db=db, project_id=model.id, graph=payload.graph, compile_on_save=compile_pts_on_save,
    )
    try:
        _auto_prune_versions(db=db, model_id=model.id)
        db.commit()
    except Exception as e:
        print(f"DEBUG: _auto_prune_versions failed: {e}")
        pass
    _cache_helpers["_invalidate_management_caches"](projects=True, stats=True)
    return ModelCreateResponse(
        project_id=model.id,
        version=version.version,
        created_at=version.created_at,
        created_new_version=True,
        graph_hash=graph_hash,
        **pts_compile_summary,
    )


@_base_router.get("/{project_id}/versions", response_model=list[ModelCreateResponse])
def list_project_versions(
    project_id: str, db: Session = Depends(get_db)
) -> list[ModelCreateResponse]:
    model = db.query(Model).filter(Model.id == project_id).first()
    if model is None:
        raise HTTPException(status_code=404, detail="Project not found")
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


@_base_router.get("/{project_id}/versions/{version}", response_model=ModelVersionOut)
def get_project_version(
    project_id: str, version: int, db: Session = Depends(get_db)
) -> ModelVersionOut:
    _ensure_pts_helpers()
    model = db.query(Model).filter(Model.id == project_id).first()
    if model is None:
        raise HTTPException(status_code=404, detail="Project not found")
    record = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id, ModelVersion.version == version)
        .first()
    )
    if not record:
        raise HTTPException(status_code=404, detail="Model version not found")
    raw_graph = hydrate_graph_for_api(
        record.hybrid_graph_json if isinstance(record.hybrid_graph_json, dict) else {}, db=db
    )
    graph = HybridGraph.model_validate(raw_graph)
    normalize_graph_product_flags(graph)
    _get_pts_helpers["_project_pts_external_ports_into_graph"](db=db, project_id=model.id, graph=graph)
    pts_validation = _get_pts_helpers["_build_pts_validation_summary"](db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    _get_pts_helpers["_enrich_graph_flow_name_en"](graph_json, db=db)
    flow_name_sync_needed, outdated_flow_refs_count, outdated_flow_ref_examples = _get_pts_helpers[
        "_build_flow_sync_state_for_graph_json"
    ](db=db, graph_json=graph_json)
    project_integrity = _get_pts_helpers["_build_project_integrity_summary"](
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
        handle_validation=_safe_handle_validation(graph_json),
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples,
        pts_validation=pts_validation,
        project_integrity=project_integrity,
    )


@_base_router.get("/{project_id}/latest", response_model=ModelVersionOut)
def get_project_latest_by_id(
    project_id: str,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> ModelVersionOut | Response:
    _ensure_cache_helpers()
    _ensure_pts_helpers()
    from ..config import settings as _settings
    CACHE_TTL_PROJECTS_SECONDS = getattr(_settings, "cache_ttl_projects_seconds", 300.0)

    model = db.query(Model).filter(Model.id == project_id).first()
    if model is None:
        raise HTTPException(status_code=404, detail="Project not found")

    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is None:
        raise HTTPException(status_code=404, detail="No model version found")

    cache_key = (
        f"project_latest:v2:rev={_cache_helpers['_cache_revision']('projects')}:project_id={model.id}:"
        f"version={latest_row.version}:graph_hash={str(latest_row.graph_hash or '')}"
    )
    cached = _cache_helpers["_cache_get"](cache_key, ttl_seconds=CACHE_TTL_PROJECTS_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _cache_helpers["_is_if_none_match_hit"](if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    raw_graph = hydrate_graph_for_api(
        latest_row.hybrid_graph_json if isinstance(latest_row.hybrid_graph_json, dict) else {}, db=db
    )
    graph = HybridGraph.model_validate(raw_graph)
    normalize_graph_product_flags(graph)
    _get_pts_helpers["_project_pts_external_ports_into_graph"](db=db, project_id=model.id, graph=graph)
    pts_validation = _get_pts_helpers["_build_pts_validation_summary"](db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    _get_pts_helpers["_enrich_graph_flow_name_en"](graph_json, db=db)
    flow_name_sync_needed, outdated_flow_refs_count, outdated_flow_ref_examples = _get_pts_helpers[
        "_build_flow_sync_state_for_graph_json"
    ](db=db, graph_json=graph_json)
    project_integrity = _get_pts_helpers["_build_project_integrity_summary"](
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
        handle_validation=_safe_handle_validation(graph_json),
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples,
        pts_validation=pts_validation,
        project_integrity=project_integrity,
    )
    payload_json = payload.model_dump(mode="json")
    etag = _cache_helpers["_build_etag_for_payload"](payload_json)
    _cache_helpers["_cache_set"](cache_key, {"payload": payload_json, "etag": etag})
    if _cache_helpers["_is_if_none_match_hit"](if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload_json, headers={"ETag": etag})


# ══════════════════════════════════════════════════════════════════════════
#  /api/projects  routes
# ══════════════════════════════════════════════════════════════════════════


@_api_router.get("/{project_id}", response_model=ProjectOut)
def get_project_api(project_id: str, db: Session = Depends(get_db)) -> ProjectOut:
    return get_project(project_id=project_id, db=db)


@_api_router.post("", response_model=ProjectOut)
def create_project_api(
    payload: ProjectCreateRequest, db: Session = Depends(get_db)
) -> ProjectOut:
    _ensure_cache_helpers()
    project_name = payload.name.strip()
    if not project_name:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REQUEST", "message": "Project name cannot be empty"},
        )
    existing = db.query(Model).filter(Model.name == project_name).first()
    if existing:
        raise HTTPException(
            status_code=409,
            detail={"code": "DUPLICATE_PROJECT_NAME", "message": f"Project name already exists: {project_name}"},
        )
    created = create_project(payload=payload, db=db)
    _cache_helpers["_invalidate_management_caches"](projects=True, stats=True)
    return created


@_api_router.patch("/{project_id}", response_model=ProjectOut)
def update_project_api(
    project_id: str, payload: ProjectUpdateRequest, db: Session = Depends(get_db)
) -> ProjectOut:
    _ensure_cache_helpers()
    model = db.query(Model).filter(Model.id == project_id).first()
    if model is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "PROJECT_NOT_FOUND", "message": f"Project not found: {project_id}"},
        )

    if payload.name is not None:
        new_name = payload.name.strip()
        if not new_name:
            raise HTTPException(
                status_code=400,
                detail={"code": "INVALID_REQUEST", "message": "Project name cannot be empty"},
            )
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
    _cache_helpers["_invalidate_management_caches"](projects=True, stats=True)
    latest = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    return _build_project_out(model, latest)


@_api_router.post("/{project_id}/duplicate", response_model=ProjectOut)
def duplicate_project_api(
    project_id: str,
    payload: ProjectDuplicateRequest | None = None,
    db: Session = Depends(get_db),
) -> ProjectOut:
    _ensure_cache_helpers()
    source = db.query(Model).filter(Model.id == project_id).first()
    if source is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "PROJECT_NOT_FOUND", "message": f"Project not found: {project_id}"},
        )
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
    _cache_helpers["_invalidate_management_caches"](projects=True, stats=True)
    latest = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == duplicated.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    return _build_project_out(duplicated, latest)


@_api_router.delete("/{project_id}", response_model=DeleteProjectResponse)
def delete_project_api(project_id: str, db: Session = Depends(get_db)) -> DeleteProjectResponse:
    return delete_project(project_id=project_id, db=db)


@_api_router.post("/{project_id}/versions", response_model=ModelCreateResponse)
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


@_api_router.get("/{project_id}/versions", response_model=list[ModelCreateResponse])
def list_project_versions_api(
    project_id: str, db: Session = Depends(get_db)
) -> list[ModelCreateResponse]:
    return list_project_versions(project_id=project_id, db=db)


@_api_router.get("/{project_id}/versions/{version}", response_model=ModelVersionOut)
def get_project_version_api(
    project_id: str, version: int, db: Session = Depends(get_db)
) -> ModelVersionOut:
    return get_project_version(project_id=project_id, version=version, db=db)


@_api_router.get("/{project_id}/latest", response_model=ModelVersionOut)
def get_project_latest_api(
    project_id: str,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> ModelVersionOut | Response:
    return get_project_latest_by_id(project_id=project_id, if_none_match=if_none_match, db=db)


# ── Paginated projects list (main.py has this under /api/projects query) ─

# NOTE: The original main.py has `@app.get("/api/projects", ...)` which is a
# standalone route (not under /projects prefix). We'll keep it as a
# separate router included in main.py with its own prefix.
# See main.py for the router include.


# ══════════════════════════════════════════════════════════════════════════
#  Sync flow names & repair integrity (Stage 4B)
# ══════════════════════════════════════════════════════════════════════════


@_api_router.post(
    "/{project_id}/sync-flow-names",
    response_model=ProjectFlowNameSyncResponse,
)
@_base_router.post(
    "/{project_id}/sync-flow-names",
    response_model=ProjectFlowNameSyncResponse,
)
def sync_project_flow_names(
    project_id: str, db: Session = Depends(get_db),
) -> "ProjectFlowNameSyncResponse":
    _ensure_cache_helpers()

    model = _get_model_or_404(db, project_id)
    (
        latest_version,
        synced_port_count,
        synced_edge_count,
        cleared_pts_compile_count,
        cleared_pts_external_count,
        cleared_pts_definition_count,
    ) = _sync_project_latest_version_flow_names(db=db, project_id=model.id)
    db.commit()
    _cache_helpers["_invalidate_management_caches"](projects=True, stats=True)
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


@_api_router.post(
    "/{project_id}/repair-integrity",
    response_model=RepairProjectIntegrityResponse,
)
@_base_router.post(
    "/{project_id}/repair-integrity",
    response_model=RepairProjectIntegrityResponse,
)
def repair_project_integrity(
    project_id: str, db: Session = Depends(get_db),
) -> "RepairProjectIntegrityResponse":
    _ensure_cache_helpers()
    _ensure_pts_helpers()

    model = _get_model_or_404(db, project_id)
    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == model.id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is None:
        raise HTTPException(status_code=404, detail="No model version found")

    from ..services.graph_storage import hydrate_graph_for_api as _hydrate_graph_for_api
    from ..services.graph_contract import normalize_graph_product_flags as _normalize_graph_product_flags

    raw_graph = _hydrate_graph_for_api(
        latest_row.hybrid_graph_json if isinstance(latest_row.hybrid_graph_json, dict) else {}, db=db
    )
    graph = HybridGraph.model_validate(raw_graph)
    _normalize_graph_product_flags(graph)
    _get_pts_helpers["_project_pts_external_ports_into_graph"](db=db, project_id=model.id, graph=graph)
    graph_json = graph.model_dump(mode="python")
    pts_validation = _get_pts_helpers["_build_pts_validation_summary"](db=db, project_id=model.id, graph=graph)
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
            repaired, reason = _get_pts_helpers["_repair_pts_publication_from_resource"](db=db, resource=resource)
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
    _cache_helpers["_invalidate_management_caches"](projects=True, stats=True)
    return RepairProjectIntegrityResponse(
        project_id=model.id,
        repaired_count=repaired_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        items=items,
    )


# ── Router registration ─────────────────────────────────────────────────
# main.py includes _base_router and _api_router separately. Do not nest the
# /api router under /projects, otherwise invalid /projects/api/projects paths
# are registered.
