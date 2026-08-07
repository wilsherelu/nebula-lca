"""Pure helpers for reference process / catalog operations.

These functions do NOT depend on FastAPI or main.py.  They are imported by
``app/api/reference_catalog.py`` and can also be used by other modules
(e.g. ``app/api/tidas_import.py``) to avoid code duplication.

Public API
----------
* ``_filter_exchanges_with_evidence``
* ``_mark_reference_product_exchange``
* ``_build_imported_process_ports``
* ``_flow_uuid_set``
* ``_flow_uuid_set_cached``
* ``_flow_meta_by_uuid_cached``
* ``_normalize_process_kind``
* ``_validate_target_kind_or_400`` (HTTPException version)

Note: ``_derive_reference_flow_display`` exists in main.py but is dead code
(never called) and has been intentionally left there.
"""

from __future__ import annotations

import ast
import copy
import uuid
from collections import Counter
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from ..models import FlowRecord, ReferenceProcess
from ..schemas import (
    FilteredExchangeEvidence,
    ImportedProcessDetail,
    ImportedProcessPortItem,
    ProcessImportReportResponse,
    ProcessImportWarning,
)
from . import catalog_cache as _cc

# ---------------------------------------------------------------------------
# Cache TTL constants (mirrored from main.py)
# ---------------------------------------------------------------------------
_CACHE_TTL_FLOW_META_SECONDS = 300.0
_CACHE_TTL_REFERENCE_PROCESS_CATALOG_SECONDS = 30.0
_CACHE_TTL_REFERENCE_PROCESS_REPORT_SECONDS = 1800.0


class TidasAllocationImportError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        message_zh: str,
        process_uuid: str,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.message_zh = message_zh
        self.process_uuid = process_uuid
        self.details = details or {}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _localized_display_text(value: object) -> str | None:
    parsed = value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                parsed = ast.literal_eval(stripped)
            except (SyntaxError, ValueError):
                return stripped or None
        else:
            return stripped or None
    if isinstance(parsed, list):
        rows = [item for item in parsed if isinstance(item, dict)]
        for language in ("zh", "en"):
            for row in rows:
                if str(row.get("@xml:lang") or "").lower().startswith(language):
                    text = _safe_str(row.get("#text") or row.get("text") or row.get("value"))
                    if text:
                        return text
        for item in parsed:
            text = _localized_display_text(item)
            if text:
                return text
        return None
    if isinstance(parsed, dict):
        return _safe_str(parsed.get("#text") or parsed.get("text") or parsed.get("value"))
    return _safe_str(parsed)


def _restore_exchange_amounts_from_lineage(
    exchanges: list[dict],
    metadata: dict | None,
) -> int:
    row = metadata.get("row") if isinstance(metadata, dict) else None
    raw_payload = row.get("json") if isinstance(row, dict) and isinstance(row.get("json"), dict) else row
    process_dataset = raw_payload.get("processDataSet") if isinstance(raw_payload, dict) else None
    exchange_root = process_dataset.get("exchanges") if isinstance(process_dataset, dict) else None
    raw_exchanges = exchange_root.get("exchange") if isinstance(exchange_root, dict) else None
    if isinstance(raw_exchanges, dict):
        raw_exchanges = [raw_exchanges]
    if not isinstance(raw_exchanges, list):
        return 0

    raw_by_internal_id: dict[str, dict] = {}
    for raw in raw_exchanges:
        if not isinstance(raw, dict):
            continue
        internal_id = _to_stripped(
            raw.get("@dataSetInternalID")
            or raw.get("dataSetInternalID")
            or raw.get("exchange_internal_id")
        )
        if internal_id:
            raw_by_internal_id[internal_id] = raw

    restored = 0
    for exchange in exchanges:
        internal_id = _to_stripped(exchange.get("exchange_internal_id"))
        raw = raw_by_internal_id.get(internal_id)
        if raw is None:
            continue
        raw_amount = next(
            (raw.get(key) for key in ("amount", "meanAmount", "resultingAmount", "meanValue") if raw.get(key) is not None),
            None,
        )
        if raw_amount is None:
            continue
        try:
            amount = float(raw_amount)
        except (TypeError, ValueError):
            continue
        if exchange.get("amount") != amount:
            exchange["amount"] = amount
            restored += 1
    return restored


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


# ---------------------------------------------------------------------------
# Flow UUID / meta caching helpers
# ---------------------------------------------------------------------------

def _flow_uuid_set(db: Session) -> set[str]:
    return {str(row.flow_uuid).strip() for row in db.query(FlowRecord.flow_uuid).all() if str(row.flow_uuid).strip()}


def _flow_uuid_set_cached(db: Session) -> set[str]:
    cache_key = f"flow_uuid_set:v1:rev={_cc.cache_revision('flow_meta')}"
    cached = _cc.cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOW_META_SECONDS)
    if isinstance(cached, set):
        return cached
    value = _flow_uuid_set(db)
    _cc.cache_set(cache_key, value)
    return value


def _flow_meta_by_uuid_cached(db: Session) -> dict[str, tuple[str | None, str | None, str | None, str | None, str | None]]:
    cache_key = f"flow_meta_by_uuid:v2:rev={_cc.cache_revision('flow_meta')}"
    cached = _cc.cache_get(cache_key, ttl_seconds=_CACHE_TTL_FLOW_META_SECONDS)
    if isinstance(cached, dict):
        return cached
    value = {
        str(row.flow_uuid).strip(): (
            _safe_str(row.flow_name),
            _safe_str(row.flow_name_en),
            _safe_str(row.default_unit),
            _safe_str(row.flow_type),
            _safe_str(row.unit_group),
        )
        for row in db.query(
            FlowRecord.flow_uuid,
            FlowRecord.flow_name,
            FlowRecord.flow_name_en,
            FlowRecord.default_unit,
            FlowRecord.flow_type,
            FlowRecord.unit_group,
        ).all()
    }
    _cc.cache_set(cache_key, value)
    return value


# ---------------------------------------------------------------------------
# Exchange filtering & reference product detection
# ---------------------------------------------------------------------------

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
    allocation_policy: str = "quantity",
) -> tuple[str | None, list[str]]:
    warnings: list[str] = []
    reference_flow_internal_id = _to_stripped(process_json.get("reference_flow_internal_id"))
    matched_output: dict | None = None
    explicit_allocation_outputs = [
        ex for ex in exchanges
        if _is_output_direction(ex.get("direction"))
        and (ex.get("allocationFactor") is not None or ex.get("allocation_factor") is not None)
    ]
    tidas_allocation_outputs = [
        ex for ex in exchanges
        if _is_output_direction(ex.get("direction"))
        and bool(ex.get("tidasAllocationPresent"))
        and str(ex.get("flow_type") or "").strip().casefold() == "product flow"
    ]
    allocation_outputs = tidas_allocation_outputs or explicit_allocation_outputs

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

    product_outputs: list[dict] = [matched_output]
    for ex in allocation_outputs:
        if all(ex is not product for product in product_outputs):
            product_outputs.append(ex)

    ignored_nested_count = sum(
        1
        for ex in exchanges
        if bool(ex.get("tidasAllocationPresent"))
        and all(ex is not product for product in tidas_allocation_outputs)
    )
    if ignored_nested_count:
        warnings.append(
            f"ignored {ignored_nested_count} allocatedFraction exchange(s) that are not output Product flows"
        )

    if len(product_outputs) > 1 and tidas_allocation_outputs:
        unit_groups = {str(ex.get("unit_group") or "").strip() for ex in product_outputs}
        if "" in unit_groups:
            raise TidasAllocationImportError(
                "TIDAS product allocation is missing authoritative Flow unit-group metadata",
                message_zh="共同产品缺少权威 Flow 单位组，无法确定分配方式。",
                process_uuid=process_uuid,
                details={"product_flow_uuids": [ex.get("flow_uuid") for ex in product_outputs]},
            )
        effective_policy = "tidas" if len(unit_groups) > 1 else allocation_policy
        if len(unit_groups) > 1 and allocation_policy != "tidas":
            warnings.append("product unit groups differ; TIDAS allocation factors were required automatically")
        if effective_policy == "tidas":
            raw_factors: dict[str, float] = {}
            for ex in product_outputs:
                raw = ex.get("tidasAllocatedFraction")
                if raw is None:
                    raise TidasAllocationImportError(
                        "TIDAS product allocation factors are incomplete",
                        message_zh="TIDAS 包内共同产品分配系数不完整，已停止导入。",
                        process_uuid=process_uuid,
                        details={"flow_uuid": ex.get("flow_uuid")},
                    )
                raw_factors[str(ex.get("exchange_internal_id") or ex.get("flow_uuid") or "")] = float(raw)
            total = sum(raw_factors.values())
            if total <= 0:
                raise TidasAllocationImportError(
                    "TIDAS product allocation factors must contain a positive value",
                    message_zh="TIDAS 包内共同产品分配系数没有有效正值，已停止导入。",
                    process_uuid=process_uuid,
                    details={"raw_factors": raw_factors},
                )
            for ex in product_outputs:
                raw = float(ex.get("tidasAllocatedFraction") or 0.0)
                ex["allocationFactor"] = raw / total
                ex["allocationBasis"] = {
                    "method": "manual_factor",
                    "source": "tidas_allocated_fraction",
                    "rawValue": raw,
                }
        else:
            for ex in product_outputs:
                ex["allocationFactor"] = None
                ex["allocationBasis"] = {
                    "method": "quantity",
                    "source": "tidas_import_policy",
                }

    product_ids = {id(ex) for ex in product_outputs}
    for ex in exchanges:
        ex["is_reference_flow"] = ex is matched_output
        ex["isProduct"] = id(ex) in product_ids

    flow_uuid = _to_stripped(matched_output.get("flow_uuid"))
    if not flow_uuid:
        warnings.append("reference output flow_uuid missing; product requires manual completion")
        return None, warnings
    return flow_uuid, warnings


def _materialize_process_exchanges_for_graph(
    *,
    process_uuid: str,
    process_json: dict,
    exchanges: list[dict],
    allocation_policy: str = "quantity",
) -> tuple[list[dict], str | None, list[str]]:
    """Clone raw catalog exchanges and apply graph-only product semantics."""
    graph_exchanges = copy.deepcopy(exchanges)
    reference_flow_uuid, warnings = _mark_reference_product_exchange(
        process_uuid=process_uuid,
        process_json=process_json,
        exchanges=graph_exchanges,
        allocation_policy=allocation_policy,
    )
    return graph_exchanges, reference_flow_uuid, warnings


def _build_imported_process_ports(
    *,
    exchanges: list[dict],
    flow_meta_by_uuid: dict[str, tuple[str | None, str | None, str | None, str | None, str | None]],
) -> tuple[list[ImportedProcessPortItem], list[ImportedProcessPortItem]]:
    from ..schemas import flow_semantic_to_exchange_type

    inputs: list[ImportedProcessPortItem] = []
    outputs: list[ImportedProcessPortItem] = []

    for ex in exchanges:
        if not isinstance(ex, dict):
            continue
        flow_uuid = _safe_str(ex.get("flow_uuid"))
        exchange_flow_name = _localized_display_text(ex.get("flow_name"))
        flow_name = exchange_flow_name
        flow_name_en = ""
        unit = _safe_str(ex.get("unit"))
        unit_group = ""
        flow_type = _safe_str(ex.get("flow_type"))
        if flow_uuid and flow_uuid in flow_meta_by_uuid:
            db_flow_name, db_flow_name_en, db_unit, db_flow_type, db_unit_group = flow_meta_by_uuid.get(flow_uuid) or (None, None, None, None, None)
            flow_name = db_flow_name or flow_name
            flow_name_en = db_flow_name_en or ""
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
            flow_name_en=flow_name_en or None,
            unit=unit,
            unit_group=unit_group or None,
            type=flow_semantic_to_exchange_type(flow_type),
            amount=amount,
            direction=direction,
            is_product=bool(ex.get("isProduct") or ex.get("is_reference_flow")),
            allocation_factor=ex.get("allocationFactor"),
            allocation_basis=ex.get("allocationBasis"),
        )

        if direction == "input":
            inputs.append(item)
        else:
            outputs.append(item)

    return inputs, outputs


# ---------------------------------------------------------------------------
# Cache / invalidation helpers (thin wrappers for import by API module)
# ---------------------------------------------------------------------------

_cache_get = _cc.cache_get
_cache_set = _cc.cache_set
_cache_revision = _cc.cache_revision
_build_etag_for_payload = _cc.build_etag_for_payload
_is_if_none_match_hit = _cc.is_if_none_match_hit
invalidate_management_caches = _cc.invalidate_management_caches
