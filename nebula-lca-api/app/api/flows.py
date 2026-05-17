"""Flow catalog API routes.

Standalone ``APIRouter`` for:
- ``GET /api/flows``              (paginated list + FTS/LIKE)
- ``GET /api/flows/categories``   (compartment counts)
- ``GET /api/reference/flows/{flow_uuid}``
- ``POST /api/flows``             (custom flow creation)

Routes are included in ``app.main`` via ``app.include_router()``.
All function names and signatures match the original ``main.py`` implementations
so that no URL, response schema, or error code changes.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from sqlalchemy import case, func as sqla_func
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import FlowRecord, UnitDefinition, UnitGroup
from ..source_policy import get_tidas_allowed_unit_groups
from ..schemas import (
    CreateFlowRequest,
    CreateFlowResponse,
    FlowAllocationPropertiesResponse,
    FlowAllocationPropertiesUpdateRequest,
    FlowCategoriesResponse,
    FlowCategoryItem,
    FlowListItem,
    FlowOut,
    FlowOutExtended,
    PaginatedFlowsResponse,
    TidasFlowCompatibilityUpdateRequest,
    normalize_flow_semantic,
)
from ..services.catalog_cache import (
    build_etag_for_payload,
    cache_get,
    cache_set,
    cache_revision,
    first_category_segment,
    invalidate_management_caches,
    is_if_none_match_hit,
)
from ..services.catalog_cache import fts5_flow_search_query as _fts5_flow_search_query

api_router = APIRouter()

# TTL constants (mirror main.py)
_CACHE_TTL_SECONDS: float = 30.0
_CACHE_TTL_FLOWS_SECONDS: float = 3600.0
_CACHE_TTL_FLOW_CATEGORIES_SECONDS: float = 3600.0


def _flow_out_extended(row: FlowRecord) -> FlowOutExtended:
    return FlowOutExtended(
        flow_uuid=row.flow_uuid,
        flow_name=row.flow_name,
        flow_name_en=row.flow_name_en,
        flow_type=row.flow_type,
        default_unit=row.default_unit,
        unit_group=row.unit_group,
        compartment=row.compartment,
        source_updated_at=row.source_updated_at,
        source=row.source,
        is_custom=bool(row.is_custom),
        tidas_compatible=bool(getattr(row, "tidas_compatible", False)),
        tidas_unit_group=getattr(row, "tidas_unit_group", None),
        tidas_flow_property_uuid=getattr(row, "tidas_flow_property_uuid", None),
        tidas_reference_source=getattr(row, "tidas_reference_source", None),
        allocation_properties=list(getattr(row, "allocation_properties", None) or []),
    )


def _find_flow_or_404(flow_uuid: str, db: Session) -> FlowRecord:
    normalized_uuid = (flow_uuid or "").strip().lower()
    row = db.get(FlowRecord, normalized_uuid) if normalized_uuid else None
    if row is None and normalized_uuid:
        row = db.query(FlowRecord).filter(sqla_func.lower(FlowRecord.flow_uuid) == normalized_uuid).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return row


def _ensure_allocation_properties_editable(row: FlowRecord) -> None:
    if normalize_flow_semantic(row.flow_type) == "elementary_flow":
        raise HTTPException(
            status_code=422,
            detail={
                "code": "ALLOCATION_PROPERTIES_NOT_EDITABLE",
                "message": "Only non-elementary flows can define allocation conversion properties.",
            },
        )


# ── Reference flow lookup ───────────────────────────────────────────────


@api_router.get("/api/reference/flows/{flow_uuid}", response_model=FlowOut)
def get_reference_flow(flow_uuid: str, db: Session = Depends(get_db)) -> FlowOut:
    return _flow_out_extended(_find_flow_or_404(flow_uuid, db))


# ── Custom flow creation ────────────────────────────────────────────────


@api_router.post("/api/flows", response_model=CreateFlowResponse, status_code=201)
def create_flow(payload: CreateFlowRequest, db: Session = Depends(get_db)) -> CreateFlowResponse:
    """Create a custom non-elementary flow (product / waste)."""
    normalized_semantic = normalize_flow_semantic(payload.flow_type)
    _SEMANTIC_TO_DB_TYPE: dict[str, str] = {
        "product_flow": "Product flow",
        "intermediate_flow": "Product flow",
        "waste_flow": "Waste flow",
    }
    db_flow_type = _SEMANTIC_TO_DB_TYPE.get(normalized_semantic, "Product flow")
    requested_tidas_policy = str(payload.source_policy or "").strip() == "tidas_compliant"
    tidas_compatible = bool(payload.tidas_compatible or requested_tidas_policy)
    tidas_unit_group = (payload.tidas_unit_group or payload.unit_group_uuid).strip()

    if tidas_compatible:
        from ..tidas_reference import normalize_tidas_unit_group

        allowed = set(get_tidas_allowed_unit_groups())
        if allowed and normalize_tidas_unit_group(tidas_unit_group) not in allowed:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "TIDAS_UNIT_GROUP_NOT_ALLOWED",
                    "message": f"Unit group '{tidas_unit_group}' is not allowed for TIDAS-compatible custom flows.",
                    "unit_group": tidas_unit_group,
                },
            )

    unit_group = db.query(UnitGroup).filter(UnitGroup.name == payload.unit_group_uuid).first()
    if not unit_group:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "INVALID_UNIT_GROUP",
                "message": f"Unit group '{payload.unit_group_uuid}' does not exist.",
            },
        )

    unit_def = (
        db.query(UnitDefinition)
        .filter(
            UnitDefinition.unit_group == payload.unit_group_uuid,
            UnitDefinition.unit_name == payload.default_unit,
        )
        .first()
    )
    if not unit_def:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "INVALID_DEFAULT_UNIT",
                "message": (
                    f"Default unit '{payload.default_unit}' is not a member of "
                    f"unit group '{payload.unit_group_uuid}'."
                ),
            },
        )

    name_conditions = [sqla_func.lower(FlowRecord.flow_name) == payload.flow_name.strip().lower()]
    if payload.flow_name_en:
        name_conditions.append(
            sqla_func.lower(FlowRecord.flow_name_en) == payload.flow_name_en.strip().lower()
        )
    name_filter = name_conditions[0] if len(name_conditions) == 1 else (name_conditions[0] | name_conditions[1])

    candidate_flows = (
        db.query(FlowRecord)
        .filter(name_filter, FlowRecord.flow_type == db_flow_type)
        .limit(10)
        .all()
    )

    if candidate_flows and not payload.confirm_create:
        candidates = [{
            "flow_uuid": f.flow_uuid,
            "flow_name": f.flow_name,
            "flow_name_en": f.flow_name_en,
            "flow_type": f.flow_type,
            "unit_group": f.unit_group,
            "default_unit": f.default_unit,
            "source": f.source,
            "is_custom": f.is_custom,
            "tidas_compatible": bool(getattr(f, "tidas_compatible", False)),
            "tidas_unit_group": getattr(f, "tidas_unit_group", None),
            "tidas_flow_property_uuid": getattr(f, "tidas_flow_property_uuid", None),
            "tidas_reference_source": getattr(f, "tidas_reference_source", None),
            "allocation_properties": list(getattr(f, "allocation_properties", None) or []),
        } for f in candidate_flows]
        raise HTTPException(
            status_code=409,
            detail={
                "code": "FLOW_REUSE_RECOMMENDED",
                "message": (
                    "Existing flows with the same name or English name were found. "
                    "We recommend reusing an existing flow to avoid confusion in model "
                    "connections, imports/exports, and statistics. If you still need to "
                    "create a new flow, please add distinguishing information such as "
                    "specification, source, grade, boundary, or purpose to the name, "
                    "or confirm creation."
                ),
                "candidates": candidates,
            },
        )

    flow_uuid = str(uuid.uuid4())
    flow_record = FlowRecord(
        flow_uuid=flow_uuid,
        flow_name=payload.flow_name.strip(),
        flow_name_en=payload.flow_name_en.strip() if payload.flow_name_en else None,
        flow_type=db_flow_type,
        default_unit=payload.default_unit.strip(),
        unit_group=payload.unit_group_uuid.strip(),
        compartment=payload.category.strip() if payload.category else None,
        source_updated_at=None,
        source="user_custom",
        is_custom=True,
        tidas_compatible=tidas_compatible,
        tidas_unit_group=tidas_unit_group if tidas_compatible else None,
        tidas_flow_property_uuid=(payload.tidas_flow_property_uuid or "").strip() or None,
        tidas_reference_source=(payload.tidas_reference_source or "user_declared").strip() if tidas_compatible else None,
    )
    db.add(flow_record)
    db.commit()
    db.refresh(flow_record)

    invalidate_management_caches(flows=True, stats=True)

    flow_out = _flow_out_extended(flow_record)

    warnings = []
    reuse_candidates = []
    if candidate_flows:
        warnings.append(
            "Existing flows with the same name or English name were found. "
            "We recommend reusing an existing flow to avoid confusion in model "
            "connections, imports/exports, and statistics. This new flow was created "
            "per user confirmation."
        )
        reuse_candidates = [{
            "flow_uuid": f.flow_uuid,
            "flow_name": f.flow_name,
            "flow_name_en": f.flow_name_en,
            "flow_type": f.flow_type,
            "unit_group": f.unit_group,
            "default_unit": f.default_unit,
            "source": f.source,
            "is_custom": f.is_custom,
            "tidas_compatible": bool(getattr(f, "tidas_compatible", False)),
            "tidas_unit_group": getattr(f, "tidas_unit_group", None),
            "tidas_flow_property_uuid": getattr(f, "tidas_flow_property_uuid", None),
            "tidas_reference_source": getattr(f, "tidas_reference_source", None),
            "allocation_properties": list(getattr(f, "allocation_properties", None) or []),
        } for f in candidate_flows]

    return CreateFlowResponse(flow=flow_out, warnings=warnings, reuse_candidates=reuse_candidates)


@api_router.patch("/api/flows/{flow_uuid}/tidas-compatibility", response_model=FlowOutExtended)
def update_flow_tidas_compatibility(
    flow_uuid: str,
    payload: TidasFlowCompatibilityUpdateRequest,
    db: Session = Depends(get_db),
) -> FlowOutExtended:
    normalized_uuid = (flow_uuid or "").strip().lower()
    row = db.get(FlowRecord, normalized_uuid) if normalized_uuid else None
    if row is None and normalized_uuid:
        row = db.query(FlowRecord).filter(sqla_func.lower(FlowRecord.flow_uuid) == normalized_uuid).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Flow not found")

    semantic_type = normalize_flow_semantic(row.flow_type)
    if not bool(row.is_custom) or semantic_type == "elementary_flow":
        raise HTTPException(
            status_code=422,
            detail={
                "code": "TIDAS_COMPATIBILITY_NOT_EDITABLE",
                "message": "Only custom non-elementary flows can be marked TIDAS compatible.",
            },
        )

    if payload.tidas_compatible:
        tidas_unit_group = (payload.tidas_unit_group or row.unit_group or "").strip()
        from ..tidas_reference import normalize_tidas_unit_group

        allowed = set(get_tidas_allowed_unit_groups())
        if allowed and normalize_tidas_unit_group(tidas_unit_group) not in allowed:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "TIDAS_UNIT_GROUP_NOT_ALLOWED",
                    "message": f"Unit group '{tidas_unit_group}' is not allowed for TIDAS-compatible custom flows.",
                    "unit_group": tidas_unit_group,
                },
            )
        row.tidas_compatible = True
        row.tidas_unit_group = tidas_unit_group
        row.tidas_flow_property_uuid = (payload.tidas_flow_property_uuid or "").strip() or None
        row.tidas_reference_source = (payload.tidas_reference_source or "user_declared").strip() or "user_declared"
    else:
        row.tidas_compatible = False
        row.tidas_unit_group = None
        row.tidas_flow_property_uuid = None
        row.tidas_reference_source = None

    db.commit()
    db.refresh(row)
    invalidate_management_caches(flows=True, stats=True)
    return _flow_out_extended(row)


@api_router.get("/api/flows/{flow_uuid}/allocation-properties", response_model=FlowAllocationPropertiesResponse)
def get_flow_allocation_properties(
    flow_uuid: str,
    db: Session = Depends(get_db),
) -> FlowAllocationPropertiesResponse:
    row = _find_flow_or_404(flow_uuid, db)
    return FlowAllocationPropertiesResponse(
        flow_uuid=row.flow_uuid,
        properties=list(getattr(row, "allocation_properties", None) or []),
    )


@api_router.patch("/api/flows/{flow_uuid}/allocation-properties", response_model=FlowAllocationPropertiesResponse)
def update_flow_allocation_properties(
    flow_uuid: str,
    payload: FlowAllocationPropertiesUpdateRequest,
    db: Session = Depends(get_db),
) -> FlowAllocationPropertiesResponse:
    row = _find_flow_or_404(flow_uuid, db)
    _ensure_allocation_properties_editable(row)
    row.allocation_properties = [
        item.model_dump(mode="json", by_alias=True, exclude_none=True)
        for item in payload.properties
    ]
    db.commit()
    db.refresh(row)
    invalidate_management_caches(flows=True, stats=True)
    return FlowAllocationPropertiesResponse(
        flow_uuid=row.flow_uuid,
        properties=list(row.allocation_properties or []),
    )


# ── Paginated flows list ────────────────────────────────────────────────


@api_router.get("/api/flows", response_model=PaginatedFlowsResponse)
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
        f"flows:v2:rev={cache_revision('flows')}:search={search_key}:page={page}:"
        f"page_size={page_size}:type={type or ''}:category={category_key}:level1={level1_key}"
    )
    cached = cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOWS_SECONDS)
    if isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        if isinstance(payload, dict) and isinstance(etag, str):
            if is_if_none_match_hit(if_none_match, etag):
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

    flow_name_expr = sqla_func.lower(sqla_func.coalesce(FlowRecord.flow_name, ""))
    flow_name_en_expr = sqla_func.lower(sqla_func.coalesce(FlowRecord.flow_name_en, ""))
    flow_uuid_expr = sqla_func.lower(sqla_func.coalesce(FlowRecord.flow_uuid, ""))
    query = db.query(FlowRecord)

    if search and search.strip():
        normalized_search = search.strip().lower()
        fts_query, used_fts = _fts5_flow_search_query(db, normalized_search)
        if used_fts:
            query = fts_query
        else:
            token = f"%{normalized_search}%"
            query = db.query(FlowRecord).filter(
                flow_name_expr.like(token)
                | flow_name_en_expr.like(token)
                | flow_uuid_expr.like(token)
            )

    if type:
        query = query.filter(FlowRecord.flow_type.in_(sorted(type_map[type])))

    if category and category.strip():
        category_token = f"%{category.strip().lower()}%"
        query = query.filter(sqla_func.lower(sqla_func.coalesce(FlowRecord.compartment, "")).like(category_token))
    if category_level_1 and category_level_1.strip():
        level1 = category_level_1.strip().lower()
        compartment_expr = sqla_func.lower(sqla_func.coalesce(FlowRecord.compartment, ""))
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
        order_by_clauses: list[Any] = []
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
        order_by_clauses.extend([
            exact_rank.asc(),
            prefix_rank.asc(),
            sqla_func.length(FlowRecord.flow_name).asc(),
            FlowRecord.flow_name.asc(),
        ])
        rows = (
            query.order_by(*order_by_clauses)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
    else:
        default_order_by: list[Any] = []
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

    from ..services.catalog_cache import build_flow_used_in_processes_map_cached
    used_in_processes = build_flow_used_in_processes_map_cached(db)

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
                source=row.source,
                is_custom=bool(row.is_custom),
                tidas_compatible=bool(getattr(row, "tidas_compatible", False)),
                tidas_unit_group=getattr(row, "tidas_unit_group", None),
                tidas_flow_property_uuid=getattr(row, "tidas_flow_property_uuid", None),
                tidas_reference_source=getattr(row, "tidas_reference_source", None),
                allocation_properties=list(getattr(row, "allocation_properties", None) or []),
                used_in_processes=int(used_in_processes.get(row.flow_uuid, 0)),
                last_modified=row.source_updated_at,
            )
        )

    result = PaginatedFlowsResponse(items=items, total=total, page=page, page_size=page_size)
    payload = result.model_dump(mode="json")
    etag = build_etag_for_payload(payload)
    cache_set(cache_key, {"payload": payload, "etag": etag})
    if is_if_none_match_hit(if_none_match, etag):
        return Response(status_code=304, headers={"ETag": etag})
    return JSONResponse(content=payload, headers={"ETag": etag})


# ── Flow categories ─────────────────────────────────────────────────────


@api_router.get("/api/flows/categories", response_model=FlowCategoriesResponse)
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
        f"flow_categories:v1:rev={cache_revision('flow_categories')}:"
        f"type={type or ''}:level={level}:search={search or ''}"
    )
    cached = cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOW_CATEGORIES_SECONDS)
    if isinstance(cached, FlowCategoriesResponse):
        return cached

    query = db.query(FlowRecord.compartment)
    if type:
        query = query.filter(FlowRecord.flow_type.in_(sorted(type_map[type])))
    if search and search.strip():
        token = f"%{search.strip().lower()}%"
        query = query.filter(sqla_func.lower(sqla_func.coalesce(FlowRecord.compartment, "")).like(token))
    query = query.filter(FlowRecord.compartment.is_not(None), sqla_func.trim(FlowRecord.compartment) != "")
    rows = query.all()

    from collections import defaultdict
    counts: dict[str, int] = defaultdict(int)
    for (compartment,) in rows:
        first = first_category_segment(str(compartment))
        if not first:
            continue
        counts[first] += 1

    items = [
        FlowCategoryItem(category=category_name, count=count)
        for category_name, count in sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    ]
    result = FlowCategoriesResponse(items=items, total=len(items))
    cache_set(cache_key, result)
    return result
