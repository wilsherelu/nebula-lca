"""Paginated /api/projects list route.

Extracted from ``app.main`` for Stage 4A. This is a standalone route that
doesn't fit the standard ``/projects`` namespace, so it gets its own router.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Query, Response
from fastapi.responses import JSONResponse
from sqlalchemy import func as sqla_func
from sqlalchemy.orm import Session

from ..config import settings as _settings
from ..database import get_db
from ..models import Model
from ..schemas import PaginatedProjectsResponse, ProjectOut
from ..services.project_versions import _build_project_out, _latest_version_by_project_id

# Cache helpers
_cache_helpers = None


def _ensure_cache_helpers():
    global _cache_helpers
    if _cache_helpers is not None:
        return
    # fmt: off
    from ..services.catalog_cache import (
        cache_get, cache_set, cache_revision,
        build_etag_for_payload, is_if_none_match_hit,
    )
    # fmt: on
    _cache_helpers = {
        "_cache_get": cache_get,
        "_cache_set": cache_set,
        "_cache_revision": cache_revision,
        "_build_etag_for_payload": build_etag_for_payload,
        "_is_if_none_match_hit": is_if_none_match_hit,
    }


_router = APIRouter()


@_router.get("", response_model=PaginatedProjectsResponse)
def list_projects_api(
    search: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    recent: bool = Query(default=False),
    limit: int | None = Query(default=None, ge=1, le=200),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> PaginatedProjectsResponse:
    _ensure_cache_helpers()
    from ..config import settings

    CACHE_TTL_PROJECTS_SECONDS = getattr(settings, "cache_ttl_projects_seconds", 300.0)

    search_key = (search or "").strip().lower()
    cache_key = (
        f"projects:v1:rev={_cache_helpers['_cache_revision']('projects')}:search={search_key}:"
        f"page={page}:page_size={page_size}:recent={int(bool(recent))}:limit={limit or ''}"
    )
    cached = _cache_helpers["_cache_get"](cache_key, ttl_seconds=CACHE_TTL_PROJECTS_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if _cache_helpers["_is_if_none_match_hit"](if_none_match, etag):
                return Response(status_code=304, headers={"ETag": etag})
            return JSONResponse(content=payload, headers={"ETag": etag})

    query = db.query(Model)
    if search and search.strip():
        token = f"%{search.strip().lower()}%"
        query = query.filter(
            sqla_func.lower(Model.name).like(token)
            | sqla_func.lower(sqla_func.coalesce(Model.reference_product, "")).like(token)
            | sqla_func.lower(sqla_func.coalesce(Model.functional_unit, "")).like(token)
            | sqla_func.lower(sqla_func.coalesce(Model.system_boundary, "")).like(token)
            | sqla_func.lower(sqla_func.coalesce(Model.time_representativeness, "")).like(token)
            | sqla_func.lower(sqla_func.coalesce(Model.geography, "")).like(token)
            | sqla_func.lower(sqla_func.coalesce(Model.description, "")).like(token)
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
    etag = _cache_helpers["_build_etag_for_payload"](payload)
    _cache_helpers["_cache_set"](cache_key, {"payload": payload, "etag": etag})
    if _cache_helpers["_is_if_none_match_hit"](if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})
