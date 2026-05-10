"""Process catalog API routes.

Standalone ``APIRouter`` for:
- ``GET /api/processes``              (paginated list)
- ``GET /api/processes/{process_uuid}``
- ``DELETE /api/processes/{process_uuid}``
- ``POST /api/processes/delete-batch``

Routes are included in ``app.main`` via ``app.include_router()``.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from sqlalchemy import func as sqla_func
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import FlowRecord, ReferenceProcess
from ..schemas import (
    DeleteProcessResponse,
    DeleteProcessesBatchRequest,
    DeleteProcessesBatchResponse,
    PaginatedProcessesResponse,
    ProcessDetailResponse,
    ProcessListItem,
)
from ..services.catalog_cache import (
    build_etag_for_payload,
    cache_get,
    cache_set,
    cache_revision,
    invalidate_management_caches,
    is_if_none_match_hit,
)

api_router = APIRouter()

# TTL constants (mirror main.py)
_CACHE_TTL_PROCESSES_SECONDS: float = 3600.0


def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _derive_reference_flow_display(
    process_json: dict | None,
    reference_flow_uuid: str,
    reference_flow_name: str | None,
    reference_flow_internal_id: str | None,
) -> tuple[str, str | None]:
    """Mirror the original helper from main.py."""
    ref_uuid = _safe_str(reference_flow_uuid) or (
        _safe_str(process_json.get("reference_flow_uuid")) if isinstance(process_json, dict) else None
    )
    ref_name = reference_flow_name
    if ref_name is None:
        if isinstance(process_json, dict):
            ref_name = _safe_str(process_json.get("reference_flow_name"))
    if ref_name is None:
        if isinstance(process_json, dict):
            ref_name = _safe_str(process_json.get("reference_flow_internal_id"))
        ref_name = _safe_str(reference_flow_internal_id) or ref_name
    return (ref_uuid or ""), ref_name


# ── Paginated processes list ────────────────────────────────────────────


@api_router.get("/api/processes", response_model=PaginatedProcessesResponse)
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
        f"processes:v1:rev={cache_revision('processes')}:search={search_key}:page={page}"
        f":page_size={page_size}:type={type or ''}:include_legacy={str(include_legacy).lower()}"
    )
    cached = cache_get(cache_key, ttl_seconds=_CACHE_TTL_PROCESSES_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if is_if_none_match_hit(if_none_match, etag):
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
            sqla_func.lower(ReferenceProcess.process_uuid).like(token)
            | sqla_func.lower(sqla_func.coalesce(ReferenceProcess.process_name, "")).like(token)
            | sqla_func.lower(sqla_func.coalesce(ReferenceProcess.process_name_zh, "")).like(token)
            | sqla_func.lower(sqla_func.coalesce(ReferenceProcess.process_name_en, "")).like(token)
        )

    total = query.count()
    rows = (
        query.order_by(ReferenceProcess.updated_at.desc(), ReferenceProcess.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    # Optimized: only query reference flows needed by current page
    page_flow_uuids = set()
    for row in rows:
        rfu = _safe_str(row.reference_flow_uuid)
        if rfu:
            page_flow_uuids.add(rfu)
    flow_names = {}
    if page_flow_uuids:
        for row in db.query(FlowRecord.flow_uuid, FlowRecord.flow_name).filter(
            FlowRecord.flow_uuid.in_(sorted(page_flow_uuids))
        ).all():
            flow_names[row.flow_uuid] = row.flow_name

    # Cache used_in_projects computation per-request via static attr
    projects_cache_key = "_used_in_projects_by_process"
    used_in_projects_by_process = getattr(db, projects_cache_key, None)
    if used_in_projects_by_process is None:
        from ..services.catalog_cache import _latest_graphs_with_project_meta
        project_usage: dict[str, set[str]] = {}
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
                    project_usage.setdefault(pid, set()).add(str(model.id))
        used_in_projects_by_process = {pid: len(projs) for pid, projs in project_usage.items()}
        setattr(db, projects_cache_key, used_in_projects_by_process)

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
    etag = build_etag_for_payload(payload)
    cache_set(cache_key, {"payload": payload, "etag": etag})
    if is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})


# ── Process detail ──────────────────────────────────────────────────────


@api_router.get("/api/processes/{process_uuid}", response_model=ProcessDetailResponse)
def get_process_detail_api(
    process_uuid: str,
    db: Session = Depends(get_db),
) -> ProcessDetailResponse:
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


# ── Process deletion ────────────────────────────────────────────────────


@api_router.delete("/api/processes/{process_uuid}", response_model=DeleteProcessResponse)
def delete_process_api(
    process_uuid: str,
    db: Session = Depends(get_db),
) -> DeleteProcessResponse:
    row = db.get(ReferenceProcess, process_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})
    db.delete(row)
    db.commit()
    invalidate_management_caches(stats=True, reference_processes=True)
    return DeleteProcessResponse(process_uuid=process_uuid, deleted=1)


@api_router.post("/api/processes/delete-batch", response_model=DeleteProcessesBatchResponse)
def delete_processes_batch_api(
    payload: DeleteProcessesBatchRequest,
    db: Session = Depends(get_db),
) -> DeleteProcessesBatchResponse:
    ids = [str(pid).strip() for pid in payload.process_uuids if str(pid).strip()]
    if not ids:
        raise HTTPException(
            status_code=400, detail={"code": "INVALID_REQUEST", "message": "process_uuids cannot be empty"}
        )

    existing_rows = db.query(ReferenceProcess.process_uuid).filter(
        ReferenceProcess.process_uuid.in_(ids)
    ).all()
    existing_ids = {row[0] for row in existing_rows}
    to_delete = [pid for pid in ids if pid in existing_ids]
    not_found = [pid for pid in ids if pid not in existing_ids]

    deleted = 0
    if to_delete:
        deleted = db.query(ReferenceProcess).filter(
            ReferenceProcess.process_uuid.in_(to_delete)
        ).delete(synchronize_session=False)
    db.commit()
    invalidate_management_caches(stats=True, reference_processes=True)
    return DeleteProcessesBatchResponse(
        requested=len(ids),
        deleted=deleted,
        not_found=not_found,
    )
