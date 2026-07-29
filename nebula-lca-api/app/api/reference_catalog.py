"""Reference Process / Catalog API routes.

Extracted from ``app.main`` for Stage 6A. Uses ``APIRouter`` pattern;
the router is included in main.py via ``app.include_router()``.

URL paths preserved to match the original ``@app.xxx`` registrations.
"""

from __future__ import annotations

import json
import uuid
from collections import Counter
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import func as func_raw
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import ExternalDataSyncRecord, FlowRecord, LciProcessVector, ReferenceProcess
from ..schemas import (
    FilteredExchangeEvidence,
    ImportedProcessDetail,
    ImportReferenceProcessesRequest,
    ImportReferenceProcessesResponse,
    ProcessExchangeSummaryGroup,
    ProcessExchangeSummaryItem,
    ProcessExchangeSummaryResponse,
    ProcessVectorDiagnosticInfo,
    LciVectorTopExchangesResponse,
    MissingFlowSummaryResponse,
    ProcessFilteredExchangesResponse,
    ProcessImportReportResponse,
    ProcessImportWarning,
    ReferenceProcessCatalogItem,
    ReferenceProcessCatalogResponse,
    TidasMissingFlowSummaryItem,
)
from ..services import reference_catalog as _rc

# ── Routers ────────────────────────────────────────────────────────────────

_base_router = APIRouter(tags=["reference-processes"])
_api_router = APIRouter(tags=["api-reference-processes"])


# ═══════════════════════════════════════════════════════════════════════════
# Catalog
# ═══════════════════════════════════════════════════════════════════════════

@_api_router.get("/api/reference/processes/catalog", response_model=ReferenceProcessCatalogResponse)
@_base_router.get("/reference/processes/catalog", response_model=ReferenceProcessCatalogResponse)
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
    from ..config import settings

    _safe_str = _rc._safe_str
    _to_stripped = _rc._to_stripped
    _validate_target_kind_or_400 = _rc._validate_target_kind_or_400
    _normalize_process_kind = _rc._normalize_process_kind
    _cache_revision = _rc._cache_revision
    _cache_get = _rc._cache_get
    _cache_set = _rc._cache_set
    _build_etag_for_payload = _rc._build_etag_for_payload
    _is_if_none_match_hit = _rc._is_if_none_match_hit
    _flow_uuid_set_cached = _rc._flow_uuid_set_cached

    _CACHE_TTL_REFERENCE_PROCESS_CATALOG_SECONDS = _rc._CACHE_TTL_REFERENCE_PROCESS_CATALOG_SECONDS

    normalized_target_kind = _validate_target_kind_or_400(target_kind)
    effective_keyword = (keyword or q or search or "").strip()
    search_key = effective_keyword.lower()
    cache_key = (
        f"reference_processes_catalog:v2:rev={_cache_revision('reference_processes_catalog')}:"
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

    query = db.query(ReferenceProcess).filter(
        ReferenceProcess.process_json.is_not(None),
        (ReferenceProcess.import_mode.is_(None)) | (ReferenceProcess.import_mode != "editable_clone"),
    )
    if normalized_target_kind == "unit_process":
        query = query.filter(
            (ReferenceProcess.process_type.is_(None))
            | (func_raw.trim(ReferenceProcess.process_type) == "")
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
            func_raw.lower(ReferenceProcess.process_uuid).like(token)
            | func_raw.lower(func_raw.coalesce(ReferenceProcess.process_name, "")).like(token)
            | func_raw.lower(func_raw.coalesce(ReferenceProcess.process_name_zh, "")).like(token)
            | func_raw.lower(func_raw.coalesce(ReferenceProcess.process_name_en, "")).like(token)
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


# ═══════════════════════════════════════════════════════════════════════════
# Import
# ═══════════════════════════════════════════════════════════════════════════

@_api_router.post("/api/reference/processes/import", response_model=ImportReferenceProcessesResponse)
@_base_router.post("/reference/processes/import", response_model=ImportReferenceProcessesResponse)
def import_reference_processes(
    payload: ImportReferenceProcessesRequest,
    db: Session = Depends(get_db),
) -> ImportReferenceProcessesResponse:
    _safe_str = _rc._safe_str
    _to_stripped = _rc._to_stripped
    _validate_target_kind_or_400 = _rc._validate_target_kind_or_400
    _filter_exchanges_with_evidence = _rc._filter_exchanges_with_evidence
    _mark_reference_product_exchange = _rc._mark_reference_product_exchange
    _build_imported_process_ports = _rc._build_imported_process_ports
    _restore_exchange_amounts_from_lineage = _rc._restore_exchange_amounts_from_lineage
    _flow_meta_by_uuid_cached = _rc._flow_meta_by_uuid_cached
    _normalize_import_mode_value = _rc._normalize_import_mode_value
    invalidate_management_caches = _rc.invalidate_management_caches

    target_kind = _validate_target_kind_or_400(payload.target_kind)
    if target_kind not in {"unit_process", "lci_dataset"}:
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

    valid_flow_uuids = _rc._flow_uuid_set_cached(db)
    flow_meta_by_uuid = _flow_meta_by_uuid_cached(db)
    imported_process_count = 0
    filtered_exchanges: list[FilteredExchangeEvidence] = []
    warning_by_process: dict[str, list[str]] = {}
    imported_processes: list[ImportedProcessDetail] = []

    for source_process_uuid in source_ids:
        source_row = db.get(ReferenceProcess, source_process_uuid)
        if source_row is None:
            warning_by_process.setdefault(source_process_uuid, []).append("source process not found")
            continue
        source_json = source_row.process_json if isinstance(source_row.process_json, dict) else None
        if source_json is None:
            warning_by_process.setdefault(source_process_uuid, []).append("process_json missing; skipped")
            continue

        process_name_zh = _safe_str(source_json.get("process_name_zh")) or _safe_str(source_row.process_name_zh)
        process_name_en = _safe_str(source_json.get("process_name_en")) or _safe_str(source_row.process_name_en)
        process_name = process_name_zh or process_name_en or source_row.process_name or source_process_uuid
        reference_flow_internal_id = _safe_str(source_json.get("reference_flow_internal_id")) or _safe_str(source_row.reference_flow_internal_id)

        cloned_json = json.loads(json.dumps(source_json))
        raw_exchanges = cloned_json.get("exchanges")
        exchanges = [ex for ex in raw_exchanges if isinstance(ex, dict)] if isinstance(raw_exchanges, list) else []
        lineage = (
            db.query(ExternalDataSyncRecord)
            .filter(
                ExternalDataSyncRecord.local_kind == "process",
                ExternalDataSyncRecord.local_uuid == source_process_uuid,
            )
            .order_by(ExternalDataSyncRecord.synced_at.desc())
            .first()
        )
        restored_amount_count = _restore_exchange_amounts_from_lineage(
            exchanges,
            lineage.metadata_json if lineage is not None else None,
        )
        if restored_amount_count > 0:
            repaired_source_json = json.loads(json.dumps(source_json))
            repaired_source_json["exchanges"] = json.loads(json.dumps(exchanges))
            source_row.process_json = repaired_source_json
        if target_kind == "lci_dataset":
            reference_flow_uuid = _safe_str(source_json.get("reference_flow_uuid")) or _safe_str(source_row.reference_flow_uuid)
            has_reference_flow = bool(
                reference_flow_uuid
                and any(_safe_str(ex.get("flow_uuid")) == reference_flow_uuid for ex in exchanges)
            )
            if reference_flow_uuid and not has_reference_flow:
                exchanges.insert(
                    0,
                    {
                        "exchange_id": reference_flow_uuid,
                        "exchange_internal_id": reference_flow_uuid,
                        "flow_uuid": reference_flow_uuid,
                        "flow_name": _safe_str(source_json.get("reference_product")) or reference_flow_uuid,
                        "unit": _safe_str(source_json.get("reference_product_unit")),
                        "amount": source_json.get("reference_product_amount") or 1,
                        "direction": "output",
                        "flow_type": "Product flow",
                        "is_allocated_product": True,
                        "is_reference_flow": True,
                        "isProduct": True,
                    },
                )

        if payload.import_mode == "locked":
            target_uuid = source_process_uuid
        else:
            target_uuid = str(uuid.uuid4())

        kept_exchanges, filtered = _filter_exchanges_with_evidence(
            process_uuid=target_uuid,
            exchanges=exchanges,
            valid_flow_uuids=valid_flow_uuids,
        )
        filtered_exchanges.extend(filtered)
        declared_reference_flow_uuid = _safe_str(cloned_json.get("reference_flow_uuid")) or _safe_str(source_row.reference_flow_uuid)
        marked_reference_flow_uuid, product_warnings = _mark_reference_product_exchange(
            process_uuid=target_uuid,
            process_json=cloned_json,
            exchanges=kept_exchanges,
        )
        reference_flow_uuid = marked_reference_flow_uuid or declared_reference_flow_uuid
        if reference_flow_uuid:
            for exchange in kept_exchanges:
                if _safe_str(exchange.get("flow_uuid")) == reference_flow_uuid:
                    exchange["is_reference_flow"] = True
                    exchange["isProduct"] = True
                    break
        if target_kind == "lci_dataset":
            scoped_exchanges: list[dict] = []
            for ex in kept_exchanges:
                flow_uuid = _safe_str(ex.get("flow_uuid"))
                flow_type = _safe_str(ex.get("flow_type"))
                if flow_uuid and flow_uuid in flow_meta_by_uuid:
                    _, _, db_flow_type, _ = flow_meta_by_uuid.get(flow_uuid) or (None, None, None, None)
                    flow_type = flow_type or _safe_str(db_flow_type)
                normalized_flow_type = (flow_type or "").strip().lower()
                is_reference_product = bool(ex.get("isProduct") or ex.get("is_reference_flow"))
                is_elementary = normalized_flow_type == "elementary flow"
                if is_reference_product or is_elementary:
                    scoped_exchanges.append(ex)
            kept_exchanges = scoped_exchanges
        warning_by_process.setdefault(target_uuid, []).extend(product_warnings)

        cloned_json["process_uuid"] = target_uuid
        cloned_json["process_type"] = target_kind
        cloned_json["exchanges"] = kept_exchanges

        target_row: ReferenceProcess | None = None
        if payload.import_mode != "locked":
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
        if target_row is not None:
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
    invalidate_management_caches(stats=True, reference_processes=True)

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


# ═══════════════════════════════════════════════════════════════════════════
# Import Report
# ═══════════════════════════════════════════════════════════════════════════

@_api_router.get("/api/reference/processes/{process_uuid}/import-report", response_model=ProcessImportReportResponse)
@_base_router.get("/reference/processes/{process_uuid}/import-report", response_model=ProcessImportReportResponse)
def get_reference_process_import_report(
    process_uuid: str,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
    db: Session = Depends(get_db),
) -> ProcessImportReportResponse:
    _safe_str = _rc._safe_str
    _cache_revision = _rc._cache_revision
    _cache_get = _rc._cache_get
    _cache_set = _rc._cache_set
    _build_etag_for_payload = _rc._build_etag_for_payload
    _is_if_none_match_hit = _rc._is_if_none_match_hit
    _normalize_import_mode_value = _rc._normalize_import_mode_value

    _CACHE_TTL_REFERENCE_PROCESS_REPORT_SECONDS = _rc._CACHE_TTL_REFERENCE_PROCESS_REPORT_SECONDS

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


# ═══════════════════════════════════════════════════════════════════════════
# Filtered Exchanges
# ═══════════════════════════════════════════════════════════════════════════

@_api_router.get("/api/reference/processes/{process_uuid}/filtered-exchanges", response_model=ProcessFilteredExchangesResponse)
@_base_router.get("/reference/processes/{process_uuid}/filtered-exchanges", response_model=ProcessFilteredExchangesResponse)
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


_EXCHANGE_GROUP_KEYS = ("in_intermediate", "out_intermediate", "in_elementary", "out_elementary")


def _exchange_direction(value: Any) -> str:
    return "output" if _rc._is_output_direction(value) else "input"


def _is_elementary_exchange(item: dict[str, Any]) -> bool:
    raw = " ".join(
        str(item.get(key) or "").lower()
        for key in ("flow_type", "type", "exchange_type", "flowType")
    )
    return "elementary" in raw or "basic" in raw or "biosphere" in raw


def _exchange_group_key(item: dict[str, Any]) -> str:
    prefix = "out" if _exchange_direction(item.get("direction")) == "output" else "in"
    suffix = "elementary" if _is_elementary_exchange(item) else "intermediate"
    return f"{prefix}_{suffix}"


def _as_float(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:  # noqa: BLE001
        return 0.0
    return parsed if parsed == parsed and parsed not in {float("inf"), float("-inf")} else 0.0


def _matches_exchange_query(item: ProcessExchangeSummaryItem, query: str) -> bool:
    if not query:
        return True
    haystack = " ".join(
        str(part or "").lower()
        for part in (
            item.flow_uuid,
            item.flow_name,
            item.flow_name_en,
            item.flow_type,
            item.unit,
            item.unit_group,
            item.source,
            item.category,
        )
    )
    return query.lower() in haystack


def _paginate_exchange_items(
    *,
    key: str,
    items: list[ProcessExchangeSummaryItem],
    page: int,
    page_size: int,
    q: str | None,
) -> ProcessExchangeSummaryGroup:
    query = str(q or "").strip()
    filtered = [item for item in items if _matches_exchange_query(item, query)]
    offset = (page - 1) * page_size
    return ProcessExchangeSummaryGroup(
        key=key,  # type: ignore[arg-type]
        total=len(filtered),
        page=page,
        page_size=page_size,
        items=filtered[offset: offset + page_size],
    )


def _process_json_exchange_items(row: ReferenceProcess, db: Session) -> dict[str, list[ProcessExchangeSummaryItem]]:
    process_json = row.process_json if isinstance(row.process_json, dict) else {}
    exchanges_raw = process_json.get("exchanges")
    if not isinstance(exchanges_raw, list):
        exchanges_raw = []
    flow_uuids = sorted(
        {
            str(item.get("flow_uuid") or item.get("flowUuid") or "").strip()
            for item in exchanges_raw
            if isinstance(item, dict) and str(item.get("flow_uuid") or item.get("flowUuid") or "").strip()
        }
    )
    flow_rows = {
        item.flow_uuid: item
        for item in db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(flow_uuids)).all()
    } if flow_uuids else {}

    groups: dict[str, list[ProcessExchangeSummaryItem]] = {key: [] for key in _EXCHANGE_GROUP_KEYS}
    for raw in exchanges_raw:
        if not isinstance(raw, dict):
            continue
        flow_uuid = str(raw.get("flow_uuid") or raw.get("flowUuid") or "").strip()
        flow = flow_rows.get(flow_uuid)
        flow_type = str(raw.get("flow_type") or raw.get("type") or raw.get("flowType") or getattr(flow, "flow_type", "") or "").strip()
        group_key = _exchange_group_key({**raw, "flow_type": flow_type})
        if group_key not in groups:
            continue
        groups[group_key].append(
            ProcessExchangeSummaryItem(
                flow_uuid=flow_uuid or None,
                flow_name=str(raw.get("flow_name") or raw.get("flowName") or getattr(flow, "flow_name", "") or "").strip() or None,
                flow_name_en=str(raw.get("flow_name_en") or raw.get("flowNameEn") or getattr(flow, "flow_name_en", "") or "").strip() or None,
                flow_type=flow_type or None,
                direction=_exchange_direction(raw.get("direction")),
                unit=str(raw.get("unit") or getattr(flow, "default_unit", "") or "").strip() or None,
                unit_group=str(raw.get("unit_group") or raw.get("unitGroup") or getattr(flow, "unit_group", "") or "").strip() or None,
                amount=_as_float(raw.get("amount")),
                is_product=bool(raw.get("isProduct") or raw.get("is_product") or raw.get("is_reference_flow")),
                source=str(raw.get("source") or getattr(flow, "source", "") or "").strip() or None,
            )
        )
    return groups


def _vector_diagnostic_info(vector: LciProcessVector | None) -> ProcessVectorDiagnosticInfo:
    if vector is None:
        return ProcessVectorDiagnosticInfo(available=False)
    return ProcessVectorDiagnosticInfo(
        available=True,
        nnz=int(vector.nnz or 0),
        axis_id=vector.axis_id,
        checksum=vector.checksum,
        canonicalized=bool(vector.canonicalized),
        compression=vector.compression,
        index_dtype=vector.index_dtype,
        amount_dtype=vector.amount_dtype,
        source=vector.source,
        source_package_version=vector.source_package_version,
    )


@_api_router.get("/api/reference/processes/{process_uuid}/exchange-summary", response_model=ProcessExchangeSummaryResponse)
@_base_router.get("/reference/processes/{process_uuid}/exchange-summary", response_model=ProcessExchangeSummaryResponse)
def get_reference_process_exchange_summary(
    process_uuid: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    group: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> ProcessExchangeSummaryResponse:
    row = db.get(ReferenceProcess, process_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})
    if group is not None and group not in _EXCHANGE_GROUP_KEYS:
        raise HTTPException(status_code=422, detail={"code": "INVALID_GROUP", "message": f"group must be one of: {', '.join(_EXCHANGE_GROUP_KEYS)}"})

    grouped_items = _process_json_exchange_items(row, db)
    vector = db.get(LciProcessVector, process_uuid)

    if group in {None, "in_elementary", "out_elementary"}:
        from ..services.lci_runtime import top_process_vector_exchanges

        for key, direction in (("in_elementary", "input"), ("out_elementary", "output")):
            if group is not None and group != key:
                continue
            total, vector_items = top_process_vector_exchanges(
                db,
                process_uuid,
                page=page,
                page_size=page_size,
                direction=direction,
                q=q,
            )
            grouped_items[key] = [
                ProcessExchangeSummaryItem(
                    flow_key_id=item.get("flow_key_id"),
                    flow_uuid=item.get("flow_uuid"),
                    flow_name=item.get("flow_name"),
                    direction=direction,
                    unit=item.get("unit"),
                    amount=_as_float(item.get("amount")),
                    flow_type="Elementary flow",
                    category=" / ".join(part for part in [item.get("compartment"), item.get("subcompartment")] if part) or None,
                    source="ecoinvent",
                )
                for item in vector_items
            ]
            grouped_items[f"{key}__total"] = [ProcessExchangeSummaryItem(direction=direction, amount=float(total))]

    groups: dict[str, ProcessExchangeSummaryGroup] = {}
    for key in _EXCHANGE_GROUP_KEYS:
        if group is not None and key != group:
            continue
        if key in {"in_elementary", "out_elementary"} and f"{key}__total" in grouped_items:
            total_marker = grouped_items.pop(f"{key}__total")[0]
            groups[key] = ProcessExchangeSummaryGroup(
                key=key,  # type: ignore[arg-type]
                total=int(total_marker.amount),
                page=page,
                page_size=page_size,
                items=grouped_items.get(key, []),
            )
        else:
            groups[key] = _paginate_exchange_items(
                key=key,
                items=grouped_items.get(key, []),
                page=page,
                page_size=page_size,
                q=q,
            )

    report_json = row.import_report_json if isinstance(row.import_report_json, dict) else {}
    return ProcessExchangeSummaryResponse(
        process_uuid=process_uuid,
        process_name=row.process_name,
        process_type=row.process_type,
        reference_flow_uuid=row.reference_flow_uuid,
        source_file=row.source_file,
        source_process_uuid=row.source_process_uuid,
        import_mode=row.import_mode,
        vector=_vector_diagnostic_info(vector),
        import_report=report_json,
        groups=groups,
    )


@_api_router.get("/api/reference/processes/{process_uuid}/lci-vector/top-exchanges", response_model=LciVectorTopExchangesResponse)
@_base_router.get("/reference/processes/{process_uuid}/lci-vector/top-exchanges", response_model=LciVectorTopExchangesResponse)
def get_reference_process_lci_vector_top_exchanges(
    process_uuid: str,
    limit: int = Query(default=10, ge=1, le=100),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=100),
    direction: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> LciVectorTopExchangesResponse:
    row = db.get(ReferenceProcess, process_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})
    if direction is not None and direction not in {"input", "output"}:
        raise HTTPException(status_code=422, detail={"code": "INVALID_DIRECTION", "message": "direction must be input or output"})
    from ..services.lci_runtime import top_process_vector_exchanges

    effective_page_size = page_size or limit
    nnz, items = top_process_vector_exchanges(
        db,
        process_uuid,
        limit=limit,
        page=page,
        page_size=effective_page_size,
        direction=direction,
        q=q,
    )
    return LciVectorTopExchangesResponse(
        process_uuid=process_uuid,
        nnz=nnz,
        page=page,
        page_size=effective_page_size,
        items=items,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Missing Flow Summary
# ═══════════════════════════════════════════════════════════════════════════

@_api_router.get("/api/reference/flows/missing/summary", response_model=MissingFlowSummaryResponse)
@_base_router.get("/reference/flows/missing/summary", response_model=MissingFlowSummaryResponse)
def get_reference_flow_missing_summary(
    top: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> MissingFlowSummaryResponse:
    _safe_str = _rc._safe_str

    missing_count: Counter[str] = Counter()
    process_set_by_flow: dict[str, set[str]] = {}
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
            process_set_by_flow.setdefault(flow_uuid, set()).add(str(process_uuid))
    items = [
        TidasMissingFlowSummaryItem(
            flow_uuid=flow_uuid,
            missing_count=count,
            process_count=len(process_set_by_flow.get(flow_uuid, set())),
        )
        for flow_uuid, count in missing_count.most_common(top)
    ]
    return MissingFlowSummaryResponse(items=items)
