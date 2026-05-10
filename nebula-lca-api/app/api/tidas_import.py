"""TIDAS Import API routes (flows/processes/models/bundle + report).

Extracted from ``app.main`` for Stage 5C. Uses ``APIRouter`` pattern;
the router is included in main.py via ``app.include_router(_base_router)``.

URL paths preserved to match the original ``@app.xxx`` registrations.

Complex parsing/graph helpers are imported lazily from main.py via
``_ensure_tidas_helpers()`` to avoid circular dependencies.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from ..database import SessionLocal, get_db
from ..models import DebugDiagnostic, FlowRecord, Model, ReferenceProcess
from ..schemas import (
    FilteredExchangeEvidence,
    ProcessImportReportResponse,
    ProcessImportWarning,
    TidasImportReportResponse,
    TidasImportRequest,
    TidasModelImportRequest,
)

_TIDAS_IMPORT_DIAGNOSTIC_TYPE = "tidas.import.report.v1"

# ── Lazy helpers from main.py (circular avoidance) ────────────────────────

_tidas_helpers = None


def _ensure_tidas_helpers():
    """Lazily import shared helpers from main.py to break circular deps."""
    global _tidas_helpers
    if _tidas_helpers is not None:
        return
    # fmt: off
    from ..main import (
        TIDAS_FLOW_IMPORT_SOURCE,
        TIDAS_BUNDLE_FLOW_IMPORT_SOURCE,
        _safe_str,
        _coerce_form_bool,
        _is_protected_builtin_flow,
        _label_imported_elementary_flow_source,
        _extract_tidas_flow_record,
        _extract_tidas_process_record,
        _extract_tidas_model_record,
        _parse_tidas_uploaded_json,
        _parse_tidas_json_payload,
        _parse_tidas_bundle_zip,
        _read_uploaded_bytes,
        _build_tidas_graph_from_model_record,
        _build_tidas_base_report as _btbr,
        _finalize_tidas_report as _ftr,
        _persist_tidas_import_report as _ptir,
        _filter_exchanges_with_evidence,
        _mark_reference_product_exchange,
        _flow_uuid_set_cached,
        _refresh_flow_runtime_caches_for_current_request,
        _top_missing_flow_uuids,
        _create_project_version_from_graph_json,
        _invalidate_management_caches,
    )
    # fmt: on
    _tidas_helpers = {
        "TIDAS_FLOW_IMPORT_SOURCE": TIDAS_FLOW_IMPORT_SOURCE,
        "TIDAS_BUNDLE_FLOW_IMPORT_SOURCE": TIDAS_BUNDLE_FLOW_IMPORT_SOURCE,
        "_safe_str": _safe_str,
        "_coerce_form_bool": _coerce_form_bool,
        "_is_protected_builtin_flow": _is_protected_builtin_flow,
        "_label_imported_elementary_flow_source": _label_imported_elementary_flow_source,
        "_extract_tidas_flow_record": _extract_tidas_flow_record,
        "_extract_tidas_process_record": _extract_tidas_process_record,
        "_extract_tidas_model_record": _extract_tidas_model_record,
        "_parse_tidas_uploaded_json": _parse_tidas_uploaded_json,
        "_parse_tidas_json_payload": _parse_tidas_json_payload,
        "_parse_tidas_bundle_zip": _parse_tidas_bundle_zip,
        "_read_uploaded_bytes": _read_uploaded_bytes,
        "_build_tidas_graph_from_model_record": _build_tidas_graph_from_model_record,
        "_build_tidas_base_report": _btbr,
        "_finalize_tidas_report": _ftr,
        "_persist_tidas_import_report": _ptir,
        "_filter_exchanges_with_evidence": _filter_exchanges_with_evidence,
        "_mark_reference_product_exchange": _mark_reference_product_exchange,
        "_flow_uuid_set_cached": _flow_uuid_set_cached,
        "_refresh_flow_runtime_caches_for_current_request": _refresh_flow_runtime_caches_for_current_request,
        "_top_missing_flow_uuids": _top_missing_flow_uuids,
        "_create_project_version_from_graph_json": _create_project_version_from_graph_json,
        "_invalidate_management_caches": _invalidate_management_caches,
    }


def _h(name: str):
    """Get a helper from the lazy-imported dict."""
    _ensure_tidas_helpers()
    return _tidas_helpers[name]


# ── Routers ────────────────────────────────────────────────────────────────
_base_router = APIRouter(prefix="/import")
_api_router = APIRouter(prefix="/api/import")


# ── Helpers ────────────────────────────────────────────────────────────────

def _build_tidas_base_report(*, import_type: str, payload: object) -> dict:
    upsert_mode_value = _h("_safe_str")(getattr(payload, "upsert_mode", None)) or "update"
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


# ── Import flows route ────────────────────────────────────────────────────

@_api_router.post("/tidas/flows", response_model=TidasImportReportResponse)
@_base_router.post("/tidas/flows", response_model=TidasImportReportResponse)
async def import_tidas_flows(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    upsert_mode: str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    _coerce = _h("_coerce_form_bool")
    _safe = _h("_safe_str")
    _build_base = _h("_build_tidas_base_report")
    _parse = _h("_parse_tidas_uploaded_json")
    _extract = _h("_extract_tidas_flow_record")
    _label = _h("_label_imported_elementary_flow_source")
    _persist = _h("_persist_tidas_import_report")
    _is_protected = _h("_is_protected_builtin_flow")
    _invalidate = _h("_invalidate_management_caches")
    TIDAS_SRC = _h("TIDAS_FLOW_IMPORT_SOURCE")

    payload = TidasImportRequest(
        dry_run=_coerce(dry_run, default=False),
        upsert_mode=str(upsert_mode or "update").strip() or "update",
        strict_mode=_coerce(strict_mode, default=False),
    )
    report = _build_base(import_type="flows", payload=payload)
    source_name, rows, parse_errors = await _parse(file)
    source_items: list[tuple[str, list[dict], list[str]]] = [(source_name, rows, parse_errors)]
    report["source_path"] = f"upload://{source_name}"
    report["total_files"] = 1
    for source_name, rows, parse_errors in source_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            flow_record, err = _extract(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_name}: {err}")
                continue
            _label(flow_record, TIDAS_SRC)
            flow_uuid = str(flow_record["flow_uuid"])
            existing = db.get(FlowRecord, flow_uuid)
            if existing is not None and payload.upsert_mode == "skip":
                report["skipped"] += 1
                continue
            if existing is not None and _is_protected(existing):
                report["skipped"] += 1
                report["warnings"].append(
                    f"{flow_uuid}: built-in flow source={existing.source}; skipped overwrite from TIDAS flow import"
                )
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
            existing.flow_name_en = _safe(flow_record.get("flow_name_en"))
            existing.flow_type = str(flow_record.get("flow_type") or existing.flow_type)
            existing.default_unit = str(flow_record.get("default_unit") or existing.default_unit)
            existing.unit_group = str(flow_record.get("unit_group") or existing.unit_group)
            new_compartment = _safe(flow_record.get("compartment"))
            if new_compartment and new_compartment != "[]":
                existing.compartment = new_compartment
            existing.source_updated_at = _safe(flow_record.get("source_updated_at"))
            existing.source = _safe(flow_record.get("source")) or existing.source

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist(db, report)
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
        _invalidate(flows=True, stats=True, reference_processes=True)
    return _persist(db, report)


# ── Import processes route ────────────────────────────────────────────────

@_api_router.post("/tidas/processes", response_model=TidasImportReportResponse)
@_base_router.post("/tidas/processes", response_model=TidasImportReportResponse)
async def import_tidas_processes(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    upsert_mode: str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    _coerce = _h("_coerce_form_bool")
    _safe = _h("_safe_str")
    _build_base = _h("_build_tidas_base_report")
    _read = _h("_read_uploaded_bytes")
    _bundle = _h("_parse_tidas_bundle_zip")
    _extract_flow = _h("_extract_tidas_flow_record")
    _extract_proc = _h("_extract_tidas_process_record")
    _label = _h("_label_imported_elementary_flow_source")
    _is_protected = _h("_is_protected_builtin_flow")
    _flow_uuid = _h("_flow_uuid_set_cached")
    _refresh = _h("_refresh_flow_runtime_caches_for_current_request")
    _persist = _h("_persist_tidas_import_report")
    _filter = _h("_filter_exchanges_with_evidence")
    _mark = _h("_mark_reference_product_exchange")
    _top_miss = _h("_top_missing_flow_uuids")
    _invalidate = _h("_invalidate_management_caches")
    TIDAS_BUNDLE_SRC = _h("TIDAS_BUNDLE_FLOW_IMPORT_SOURCE")

    payload = TidasImportRequest(
        dry_run=_coerce(dry_run, default=False),
        upsert_mode=str(upsert_mode or "update").strip() or "update",
        strict_mode=_coerce(strict_mode, default=False),
    )
    report = _build_base(import_type="processes", payload=payload)
    source_name, raw_bytes = await _read(file)
    report["source_path"] = f"upload://{source_name}"
    is_zip_upload = source_name.lower().endswith(".zip") or raw_bytes[:4] == b"PK\x03\x04"

    if is_zip_upload:
        manifest, flow_items, process_items, _model_items, bundle_errors = _bundle(
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
            persisted = _persist(db, report)
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
                flow_record, err = _extract_flow(row)
                if err:
                    report["failed"] += 1
                    report["errors"].append(f"{source_entry}: {err}")
                    continue
                _label(flow_record, TIDAS_BUNDLE_SRC)
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
                if existing is not None and _is_protected(existing):
                    report["skipped"] += 1
                    report["warnings"].append(
                        f"{flow_uuid}: built-in flow source={existing.source}; skipped overwrite from TIDAS bundle import"
                    )
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
                existing.flow_name_en = _safe(flow_record.get("flow_name_en"))
                existing.flow_type = str(flow_record.get("flow_type") or existing.flow_type)
                existing.default_unit = str(flow_record.get("default_unit") or existing.default_unit)
                existing.unit_group = str(flow_record.get("unit_group") or existing.unit_group)
                new_compartment = _safe(flow_record.get("compartment"))
                if new_compartment and new_compartment != "[]":
                    existing.compartment = new_compartment
                existing.source_updated_at = _safe(flow_record.get("source_updated_at"))
                existing.source = _safe(flow_record.get("source")) or existing.source

        valid_flow_uuids = _flow_uuid(db).union(bundle_flow_uuids)
        if not payload.dry_run:
            db.flush()
            _refresh()

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
            rows, parse_errors = _h("_parse_tidas_json_payload")(source_name=source_name, raw_text=raw_text)
            source_items = [(source_name, rows, parse_errors, f"upload://{source_name}")]
        report["total_files"] = 1
        valid_flow_uuids = _flow_uuid(db)

    for source_name, rows, parse_errors, source_file in source_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            process_record, err = _extract_proc(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_name}: {err}")
                continue
            process_uuid = str(process_record["process_uuid"])
            existing = db.get(ReferenceProcess, process_uuid)
            if existing is not None and payload.upsert_mode == "skip":
                report["skipped"] += 1
                continue

            kept_exchanges, filtered = _filter(
                process_uuid=process_uuid,
                exchanges=list(process_record.get("exchanges") or []),
                valid_flow_uuids=valid_flow_uuids,
            )
            reference_flow_uuid, product_warnings = _mark(
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
                        process_name_zh=_safe(process_record.get("process_name_zh")),
                        process_name_en=_safe(process_record.get("process_name_en")),
                        process_type="unit_process",
                        reference_flow_uuid=reference_flow_uuid,
                        reference_flow_internal_id=_safe(process_record.get("reference_flow_internal_id")),
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
            existing.process_name_zh = _safe(process_record.get("process_name_zh"))
            existing.process_name_en = _safe(process_record.get("process_name_en"))
            existing.process_type = "unit_process"
            existing.reference_flow_uuid = reference_flow_uuid
            existing.reference_flow_internal_id = _safe(process_record.get("reference_flow_internal_id"))
            existing.process_json = normalized_json
            existing.source_file = source_file
            existing.source_process_uuid = None
            existing.import_mode = "locked"
            existing.import_report_json = process_report.model_dump(mode="json")

    filtered_models = [FilteredExchangeEvidence.model_validate(item) for item in report["filtered_exchanges"]]
    report["top_missing_flow_uuids"] = _top_miss(filtered_models, top_n=10)

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist(db, report)
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
        _invalidate(flows=is_zip_upload, stats=True, reference_processes=True)
    return _persist(db, report)


# ── Import models route ───────────────────────────────────────────────────

@_api_router.post("/tidas/models", response_model=TidasImportReportResponse)
@_base_router.post("/tidas/models", response_model=TidasImportReportResponse)
async def import_tidas_models(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    display_lang: str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    _coerce = _h("_coerce_form_bool")
    _safe = _h("_safe_str")
    _build_base = _h("_build_tidas_base_report")
    _parse = _h("_parse_tidas_uploaded_json")
    _extract = _h("_extract_tidas_model_record")
    _persist = _h("_persist_tidas_import_report")
    _build_graph = _h("_build_tidas_graph_from_model_record")
    _create_version = _h("_create_project_version_from_graph_json")
    _invalidate = _h("_invalidate_management_caches")

    payload = TidasModelImportRequest(
        dry_run=_coerce(dry_run, default=False),
        strict_mode=_coerce(strict_mode, default=False),
    )
    report = _build_base(import_type="models", payload=payload)
    source_name, rows, parse_errors = await _parse(file)
    source_items: list[tuple[str, list[dict], list[str]]] = [(source_name, rows, parse_errors)]
    report["source_path"] = f"upload://{source_name}"
    report["total_files"] = 1

    for source_name, rows, parse_errors in source_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            model_record, err = _extract(row)
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
            graph_json, graph_unresolved = _build_graph(
                db=db,
                model_record=model_record,
                display_lang=(_safe(display_lang) or "").lower() or "zh",
            )
            if graph_unresolved:
                report["unresolved_items"].extend(graph_unresolved)
                report["unresolved_count"] = len(report["unresolved_items"])
            if graph_json is not None:
                _create_version(
                    db=db,
                    project_id=model_row.id,
                    graph_json=graph_json,
                )

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist(db, report)
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
        _invalidate(projects=True, stats=True)
    return _persist(db, report)


# ── Import bundle route ───────────────────────────────────────────────────

@_api_router.post("/tidas/bundle", response_model=TidasImportReportResponse)
@_base_router.post("/tidas/bundle", response_model=TidasImportReportResponse)
async def import_tidas_bundle(
    file: UploadFile = File(...),
    dry_run: bool | str | None = Form(default=None),
    upsert_mode: str | None = Form(default=None),
    strict_mode: bool | str | None = Form(default=None),
    display_lang: str | None = Form(default=None),
    db: Session = Depends(get_db),
) -> TidasImportReportResponse:
    _coerce = _h("_coerce_form_bool")
    _safe = _h("_safe_str")
    _build_base = _h("_build_tidas_base_report")
    _read = _h("_read_uploaded_bytes")
    _bundle = _h("_parse_tidas_bundle_zip")
    _extract_flow = _h("_extract_tidas_flow_record")
    _extract_proc = _h("_extract_tidas_process_record")
    _extract_model = _h("_extract_tidas_model_record")
    _label = _h("_label_imported_elementary_flow_source")
    _is_protected = _h("_is_protected_builtin_flow")
    _flow_uuid = _h("_flow_uuid_set_cached")
    _refresh = _h("_refresh_flow_runtime_caches_for_current_request")
    _persist = _h("_persist_tidas_import_report")
    _filter = _h("_filter_exchanges_with_evidence")
    _mark = _h("_mark_reference_product_exchange")
    _top_miss = _h("_top_missing_flow_uuids")
    _build_graph = _h("_build_tidas_graph_from_model_record")
    _create_version = _h("_create_project_version_from_graph_json")
    _invalidate = _h("_invalidate_management_caches")
    TIDAS_BUNDLE_SRC = _h("TIDAS_BUNDLE_FLOW_IMPORT_SOURCE")

    payload = TidasImportRequest(
        dry_run=_coerce(dry_run, default=False),
        upsert_mode=str(upsert_mode or "update").strip() or "update",
        strict_mode=_coerce(strict_mode, default=False),
    )
    report = _build_base(import_type="bundle", payload=payload)
    source_name, raw_bytes = await _read(file)
    report["source_path"] = f"upload://{source_name}"
    manifest, flow_items, process_items, model_items, bundle_errors = _bundle(
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
        persisted = _persist(db, report)
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
            flow_record, err = _extract_flow(row)
            if err:
                report["failed"] += 1
                report["errors"].append(f"{source_entry}: {err}")
                continue
            _label(flow_record, TIDAS_BUNDLE_SRC)
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
            existing.flow_name_en = _safe(flow_record.get("flow_name_en"))
            existing.flow_type = str(flow_record.get("flow_type") or existing.flow_type)
            existing.default_unit = str(flow_record.get("default_unit") or existing.default_unit)
            existing.unit_group = str(flow_record.get("unit_group") or existing.unit_group)
            new_compartment = _safe(flow_record.get("compartment"))
            if new_compartment and new_compartment != "[]":
                existing.compartment = new_compartment
            existing.source_updated_at = _safe(flow_record.get("source_updated_at"))
            existing.source = _safe(flow_record.get("source")) or existing.source

    valid_flow_uuids = _flow_uuid(db).union(bundle_flow_uuids)
    if not payload.dry_run:
        db.flush()
        _refresh()

    bundle_process_uuids: set[str] = set()
    bundle_process_json_by_uuid: dict[str, dict] = {}
    for source_entry, rows, parse_errors in process_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            process_record, err = _extract_proc(row)
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

            kept_exchanges, filtered = _filter(
                process_uuid=process_uuid,
                exchanges=list(process_record.get("exchanges") or []),
                valid_flow_uuids=valid_flow_uuids,
            )
            reference_flow_uuid, product_warnings = _mark(
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
                            process_name_zh=_safe(process_record.get("process_name_zh")),
                            process_name_en=_safe(process_record.get("process_name_en")),
                            process_type="unit_process",
                            reference_flow_uuid=reference_flow_uuid,
                            reference_flow_internal_id=_safe(process_record.get("reference_flow_internal_id")),
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
            existing.process_name_zh = _safe(process_record.get("process_name_zh"))
            existing.process_name_en = _safe(process_record.get("process_name_en"))
            existing.process_type = "unit_process"
            existing.reference_flow_uuid = reference_flow_uuid
            existing.reference_flow_internal_id = _safe(process_record.get("reference_flow_internal_id"))
            existing.process_json = normalized_json
            existing.source_file = f"zip://{source_name}/{source_entry}"
            existing.source_process_uuid = None
            existing.import_mode = "locked"
            existing.import_report_json = process_report.model_dump(mode="json")

    filtered_models = [FilteredExchangeEvidence.model_validate(item) for item in report["filtered_exchanges"]]
    report["top_missing_flow_uuids"] = _top_miss(filtered_models, top_n=10)

    for source_entry, rows, parse_errors in model_items:
        report["errors"].extend(parse_errors)
        report["failed"] += len(parse_errors)
        for row in rows:
            report["total_records"] += 1
            model_record, err = _extract_model(row)
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
            graph_json, graph_unresolved = _build_graph(
                db=db,
                model_record=model_record,
                process_json_by_uuid=bundle_process_json_by_uuid,
                display_lang=(_safe(display_lang) or "").lower() or "zh",
            )
            if graph_unresolved:
                report["unresolved_items"].extend(graph_unresolved)
                report["unresolved_count"] = len(report["unresolved_items"])
            if graph_json is not None:
                _create_version(
                    db=db,
                    project_id=model_row.id,
                    graph_json=graph_json,
                )

    if payload.strict_mode and report["failed"] > 0:
        db.rollback()
        report["warnings"].append("strict_mode rollback: import aborted due to structural errors")
        persisted = _persist(db, report)
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
        _invalidate(projects=True, flows=True, stats=True, reference_processes=True)
    return _persist(db, report)


# ── Report route ──────────────────────────────────────────────────────────

@_api_router.get("/reports/{job_id}", response_model=TidasImportReportResponse)
@_base_router.get("/reports/{job_id}", response_model=TidasImportReportResponse)
def get_tidas_import_report(job_id: str, db: Session = Depends(get_db)) -> TidasImportReportResponse:
    row = db.get(DebugDiagnostic, job_id)
    if row is None or row.diagnostic_type != _TIDAS_IMPORT_DIAGNOSTIC_TYPE:
        raise HTTPException(status_code=404, detail={"code": "IMPORT_REPORT_NOT_FOUND", "message": f"report not found: {job_id}"})
    result_json = row.result_json if isinstance(row.result_json, dict) else {}
    return TidasImportReportResponse.model_validate(result_json)
