"""Shared TIDAS import core for upload routes and remote platform sync."""

from __future__ import annotations

import io
import json
import uuid
import zipfile
from collections import Counter
from datetime import datetime
from typing import Any

from fastapi import UploadFile
from sqlalchemy.orm import Session

from ..models import DebugDiagnostic, FlowRecord, Model, ReferenceProcess
from ..schemas import (
    FilteredExchangeEvidence,
    HybridGraph,
    ProcessImportReportResponse,
    ProcessImportWarning,
    TidasImportReportResponse,
)
from .catalog_cache import invalidate_management_caches
from .project_versions import _create_project_version_from_graph_json
from .reference_catalog import (
    _filter_exchanges_with_evidence,
    _flow_uuid_set_cached,
    _mark_reference_product_exchange,
)

TIDAS_FLOW_IMPORT_SOURCE = "tidas_import"
TIDAS_BUNDLE_FLOW_IMPORT_SOURCE = "tidas_bundle_import"
TIDAS_IMPORT_DIAGNOSTIC_TYPE = "tidas.import.report.v1"
PROTECTED_BUILTIN_FLOW_SOURCES = {"ef3.1", "tiangong"}


def _safe_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


def _as_list(value: object) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _pick_localized_text(value: object, *, preferred_langs: tuple[str, ...] = ("zh", "en")) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return _safe_str(value) or None
    rows = _as_list(value)
    fallback: str | None = None
    by_lang: dict[str, str] = {}
    for row in rows:
        if isinstance(row, dict):
            text = _safe_str(row.get("#text") or row.get("text") or row.get("value"))
            lang = _safe_str(row.get("@xml:lang") or row.get("xml:lang") or row.get("lang")).lower()
        else:
            text = _safe_str(row)
            lang = ""
        if not text:
            continue
        fallback = fallback or text
        if lang:
            by_lang[lang] = text
    for lang in preferred_langs:
        if lang in by_lang:
            return by_lang[lang]
    return fallback


def _payload_from_row(row: dict) -> dict:
    if not isinstance(row, dict):
        return {}
    for key in ("json_tg", "json_ordered", "json"):
        value = row.get(key)
        if isinstance(value, dict):
            return value
    return row


def _extract_dataset(row: dict, dataset_key: str) -> dict:
    payload = _payload_from_row(row)
    dataset = payload.get(dataset_key) if isinstance(payload, dict) else None
    if isinstance(dataset, dict):
        return dataset
    if dataset_key in row and isinstance(row.get(dataset_key), dict):
        return row[dataset_key]
    return payload if isinstance(payload, dict) else {}


def _extract_ilcd_name(name_obj: object) -> tuple[str | None, str | None]:
    if not isinstance(name_obj, dict):
        text = _safe_str(name_obj)
        return (text or None), (text or None)
    zh = _pick_localized_text(name_obj.get("baseName"), preferred_langs=("zh", "en"))
    en = _pick_localized_text(name_obj.get("baseName"), preferred_langs=("en", "zh"))
    if not zh:
        zh = _pick_localized_text(name_obj.get("common:name"), preferred_langs=("zh", "en"))
    if not en:
        en = _pick_localized_text(name_obj.get("common:name"), preferred_langs=("en", "zh"))
    return zh, en


def _extract_ilcd_process_name(name_obj: object) -> tuple[str | None, str | None]:
    if not isinstance(name_obj, dict):
        text = _safe_str(name_obj)
        return (text or None), (text or None)

    def _compose(preferred_langs: tuple[str, ...]) -> str | None:
        parts: list[str] = []
        for key in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes"):
            text_value = _pick_localized_text(name_obj.get(key), preferred_langs=preferred_langs)
            if text_value:
                parts.append(text_value)
        return "; ".join(parts) if parts else _pick_localized_text(name_obj.get("common:name"), preferred_langs=preferred_langs)

    return _compose(("zh", "en")), _compose(("en", "zh"))


def _extract_ilcd_flow_compartment(classification_obj: object) -> str | None:
    if not isinstance(classification_obj, dict):
        return None
    elementary = classification_obj.get("common:elementaryFlowCategorization")
    if isinstance(elementary, dict):
        parts = [_safe_str(row.get("#text") if isinstance(row, dict) else row) for row in _as_list(elementary.get("common:category"))]
        parts = [part for part in parts if part]
        if parts:
            return ";".join(parts)
    legacy = classification_obj.get("common:classification")
    classes = _as_list(legacy.get("common:class")) if isinstance(legacy, dict) else []
    parts = [_safe_str(row.get("#text") if isinstance(row, dict) else row) for row in classes]
    parts = [part for part in parts if part]
    return ";".join(parts) if parts else None


def _infer_unit_defaults_from_flow_dataset(flow_dataset: dict) -> tuple[str, str]:
    flow_info = flow_dataset.get("flowInformation") if isinstance(flow_dataset.get("flowInformation"), dict) else {}
    ref_unit = _safe_str(flow_info.get("referenceUnit"))
    unit_group = _safe_str(flow_info.get("unitGroup"))
    if ref_unit and unit_group:
        return ref_unit, unit_group
    props = flow_dataset.get("flowProperties") if isinstance(flow_dataset.get("flowProperties"), dict) else {}
    hint_texts: list[str] = []
    for row in _as_list(props.get("flowProperty") if isinstance(props, dict) else None):
        if not isinstance(row, dict):
            continue
        ref = row.get("referenceToFlowPropertyDataSet")
        short_desc = ref.get("common:shortDescription") if isinstance(ref, dict) else None
        text_value = _pick_localized_text(short_desc, preferred_langs=("en", "zh")) or ""
        if text_value:
            hint_texts.append(text_value.lower())
    hint_blob = " ".join(hint_texts)
    if "energy" in hint_blob:
        return "MJ", "Units of energy"
    if "volume" in hint_blob:
        return "m3", "Units of volume"
    if "item" in hint_blob or "count" in hint_blob:
        return "item", "dimensionless"
    return "kg", "Units of mass"


def _numeric_value(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _exchange_unit_text(row: dict) -> str:
    for key in ("unit", "referenceToUnit"):
        value = row.get(key)
        if isinstance(value, dict):
            text = _pick_localized_text(
                value.get("common:shortDescription")
                or value.get("shortDescription")
                or value.get("name")
                or value.get("common:name"),
                preferred_langs=("en", "zh"),
            )
            if text:
                return text
            ref = _safe_str(value.get("@refObjectId") or value.get("refObjectId") or value.get("@dataSetInternalID") or value.get("dataSetInternalID"))
            if ref and not ref.isdigit():
                return ref
        else:
            text = _safe_str(value)
            if text and not text.isdigit():
                return text
    return "kg"


def _exchange_is_allocated_product(row: dict) -> bool:
    return bool(
        row.get("is_allocated_product")
        or row.get("is_product")
        or row.get("isProduct")
        or row.get("productOutput")
        or _numeric_value(row.get("allocatedFraction"), 0.0) > 0
    )


def _is_protected_builtin_flow(row: FlowRecord) -> bool:
    return not bool(row.is_custom) and _safe_str(row.source) in PROTECTED_BUILTIN_FLOW_SOURCES


def _label_imported_elementary_flow_source(flow_record: dict, source: str) -> None:
    flow_record["source"] = source
    flow_record.setdefault("is_custom", False)
    flow_record["tidas_compatible"] = True
    flow_record.setdefault("tidas_reference_source", source)


def _extract_tidas_flow_record(row: dict) -> tuple[dict | None, str | None]:
    flow_dataset = _extract_dataset(row, "flowDataSet")
    info = flow_dataset.get("flowInformation") if isinstance(flow_dataset.get("flowInformation"), dict) else {}
    data_info = info.get("dataSetInformation") if isinstance(info.get("dataSetInformation"), dict) else {}
    flow_uuid = _safe_str(data_info.get("UUID") or data_info.get("common:UUID") or row.get("uuid") or row.get("id"))
    if not flow_uuid:
        return None, "flow missing UUID/id"
    name_zh, name_en = _extract_ilcd_name(data_info.get("name") or data_info.get("common:name"))
    flow_name = _safe_str(name_zh or name_en or row.get("name") or flow_uuid)
    flow_name_en = _safe_str(name_en or row.get("name_en"))
    classification = info.get("classificationInformation") if isinstance(info.get("classificationInformation"), dict) else {}
    compartment = _extract_ilcd_flow_compartment(classification)
    flow_type = _safe_str(row.get("flow_type"))
    if not flow_type:
        flow_type = "Elementary flow" if compartment else "Product flow"
    default_unit = _safe_str(row.get("default_unit"))
    unit_group = _safe_str(row.get("unit_group"))
    inferred_unit, inferred_group = _infer_unit_defaults_from_flow_dataset(flow_dataset)
    if not default_unit or not unit_group:
        default_unit = default_unit or inferred_unit
        unit_group = unit_group or inferred_group
    elif default_unit == inferred_unit and unit_group != inferred_group:
        # TianGong may localize a dimension label while the ILCD payload still
        # provides enough evidence for the runtime's canonical unit group.
        unit_group = inferred_group
    elif (
        default_unit == "kg"
        and unit_group == "Units of mass"
        and (inferred_unit, inferred_group) != ("kg", "Units of mass")
    ):
        default_unit, unit_group = inferred_unit, inferred_group
    props = flow_dataset.get("flowProperties") if isinstance(flow_dataset.get("flowProperties"), dict) else {}
    flow_property_uuid = ""
    for prop in _as_list(props.get("flowProperty") if isinstance(props, dict) else None):
        if not isinstance(prop, dict):
            continue
        ref = prop.get("referenceToFlowPropertyDataSet")
        if isinstance(ref, dict):
            flow_property_uuid = _safe_str(ref.get("@refObjectId") or ref.get("refObjectId"))
            if flow_property_uuid:
                break
    return {
        "flow_uuid": flow_uuid,
        "flow_name": flow_name,
        "flow_name_en": flow_name_en or None,
        "flow_type": flow_type,
        "default_unit": default_unit or "kg",
        "unit_group": unit_group or "Units of mass",
        "compartment": compartment,
        "source_updated_at": _safe_str(row.get("modified_at") or row.get("updated_at") or row.get("version")),
        "source": _safe_str(row.get("source")) or TIDAS_FLOW_IMPORT_SOURCE,
        "is_custom": False,
        "tidas_compatible": True,
        "tidas_unit_group": unit_group or "Units of mass",
        "tidas_flow_property_uuid": flow_property_uuid or None,
        "tidas_reference_source": _safe_str(row.get("source")) or TIDAS_FLOW_IMPORT_SOURCE,
    }, None


def _normalize_exchange(row: dict) -> dict:
    ref = row.get("referenceToFlowDataSet") if isinstance(row.get("referenceToFlowDataSet"), dict) else {}
    flow_uuid = _safe_str(row.get("flow_uuid") or row.get("flowUuid") or ref.get("@refObjectId") or ref.get("refObjectId"))

    # Resolve flow_name: support localized dict/list structures (e.g. common:shortDescription with #text/@xml:lang)
    raw_name = row.get("flow_name") or row.get("flowName") or row.get("name")
    if not raw_name and isinstance(ref, dict):
        raw_name = ref.get("common:shortDescription")
    flow_name = _pick_localized_text(raw_name, preferred_langs=("zh", "en")) or flow_uuid

    direction = _safe_str(row.get("direction") or row.get("exchangeDirection")).lower() or "input"
    if direction in {"outputs", "output"}:
        direction = "output"
    else:
        direction = "input"

    # Resolve amount: prefer amount, then meanAmount, then resultingAmount, then meanValue
    amount = row.get("amount")
    if amount is None:
        amount = row.get("meanAmount")
    if amount is None:
        amount = row.get("resultingAmount")
    if amount is None:
        amount = row.get("meanValue", 0)
    # Ensure amount is numeric
    amount = _numeric_value(amount)

    return {
        "exchange_internal_id": _safe_str(row.get("exchange_internal_id") or row.get("@dataSetInternalID") or row.get("dataSetInternalID")),
        "flow_uuid": flow_uuid,
        "flow_name": flow_name,
        "direction": direction,
        "amount": amount,
        "unit": _exchange_unit_text(row),
        "is_allocated_product": _exchange_is_allocated_product(row),
        "is_reference_flow": bool(row.get("is_reference_flow")),
        "isProduct": bool(row.get("isProduct") or row.get("is_reference_flow") or _exchange_is_allocated_product(row)),
    }


def _extract_tidas_process_record(row: dict) -> tuple[dict | None, str | None]:
    payload = _payload_from_row(row)
    if "process_uuid" in payload or "exchanges" in payload:
        process_uuid = _safe_str(payload.get("process_uuid") or payload.get("id") or row.get("id"))
        if not process_uuid:
            return None, "process missing UUID/id"
        exchanges = [_normalize_exchange(ex) for ex in _as_list(payload.get("exchanges")) if isinstance(ex, dict)]
        return {
            "process_uuid": process_uuid,
            "process_name": _safe_str(payload.get("process_name") or payload.get("name") or row.get("name") or process_uuid),
            "process_name_zh": _safe_str(payload.get("process_name_zh") or payload.get("process_name") or row.get("name")),
            "process_name_en": _safe_str(payload.get("process_name_en") or payload.get("process_name") or row.get("name")),
            "location": _safe_str(payload.get("location")),
            "reference_flow_internal_id": _safe_str(payload.get("reference_flow_internal_id")),
            "reference_flow_source_uuid": _safe_str(payload.get("reference_flow_uuid") or payload.get("reference_flow_source_uuid")),
            "reference_flow_source_name": _safe_str(payload.get("reference_flow_source_name")),
            "exchanges": exchanges,
        }, None
    process_dataset = _extract_dataset(row, "processDataSet")
    info = process_dataset.get("processInformation") if isinstance(process_dataset.get("processInformation"), dict) else {}
    data_info = info.get("dataSetInformation") if isinstance(info.get("dataSetInformation"), dict) else {}
    process_uuid = _safe_str(data_info.get("UUID") or data_info.get("common:UUID") or row.get("uuid") or row.get("id"))
    if not process_uuid:
        return None, "process missing UUID/id"
    name_zh, name_en = _extract_ilcd_process_name(data_info.get("name") or data_info.get("common:name"))
    geography = info.get("geography") if isinstance(info.get("geography"), dict) else {}
    location = _safe_str(geography.get("locationOfOperationSupplyOrProduction") or row.get("location"))
    exchanges_root = process_dataset.get("exchanges") if isinstance(process_dataset.get("exchanges"), dict) else {}
    exchanges = [_normalize_exchange(ex) for ex in _as_list(exchanges_root.get("exchange")) if isinstance(ex, dict)]
    quant = info.get("quantitativeReference") if isinstance(info.get("quantitativeReference"), dict) else {}
    ref_id = _safe_str(quant.get("referenceToReferenceFlow") or quant.get("reference_flow_internal_id"))
    return {
        "process_uuid": process_uuid,
        "process_name": _safe_str(name_zh or name_en or row.get("name") or process_uuid),
        "process_name_zh": _safe_str(name_zh or row.get("name")),
        "process_name_en": _safe_str(name_en or row.get("name")),
        "location": location,
        "reference_flow_internal_id": ref_id,
        "reference_flow_source_uuid": _safe_str(row.get("reference_flow_uuid")),
        "reference_flow_source_name": _safe_str(row.get("reference_flow_name")),
        "exchanges": exchanges,
    }, None


def _graph_from_payload(payload: dict) -> dict | None:
    candidates: list[Any] = [payload, payload.get("graph"), payload.get("hybrid_graph")]
    for key in ("json_tg", "json_ordered", "json"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidates.extend([nested, nested.get("graph"), nested.get("hybrid_graph")])
    for candidate in candidates:
        if isinstance(candidate, dict) and {"functionalUnit", "nodes", "exchanges"}.issubset(candidate.keys()):
            return dict(candidate)
    return None


def _extract_tidas_model_record(row: dict) -> tuple[dict | None, str | None]:
    payload = _payload_from_row(row)
    graph = _graph_from_payload(row) or _graph_from_payload(payload)
    model_dataset = _extract_dataset(row, "lifeCycleModelDataSet")
    model_uuid = _safe_str(payload.get("model_uuid") or model_dataset.get("UUID") or row.get("uuid") or row.get("id"))
    if not model_uuid:
        return None, "model missing UUID/id"
    model_name = _safe_str(payload.get("model_name") or payload.get("name") or row.get("name") or model_uuid)
    process_refs: set[str] = set()
    if graph:
        for node in _as_list(graph.get("nodes")):
            if isinstance(node, dict):
                ref = _safe_str(node.get("process_uuid") or node.get("processUuid") or node.get("processId"))
                if ref:
                    process_refs.add(ref)
    for ref in _as_list(payload.get("process_refs")):
        text = _safe_str(ref)
        if text:
            process_refs.add(text)
    return {
        "model_uuid": model_uuid,
        "model_name": model_name,
        "process_refs": sorted(process_refs),
        "topology_empty": not bool(graph and graph.get("nodes")),
        "graph_json": graph,
        "raw": payload,
    }, None


def _build_tidas_graph_from_model_record(
    *,
    db: Session,
    model_record: dict,
    process_json_by_uuid: dict[str, dict] | None = None,
    display_lang: str = "zh",
) -> tuple[dict | None, list[dict]]:
    graph_json = model_record.get("graph_json") if isinstance(model_record.get("graph_json"), dict) else None
    if graph_json is not None:
        return graph_json, []
    unresolved: list[dict] = []
    nodes: list[dict] = []
    process_json_by_uuid = process_json_by_uuid or {}
    for process_uuid in list(model_record.get("process_refs") or []):
        source = process_json_by_uuid.get(process_uuid)
        row = db.get(ReferenceProcess, process_uuid)
        process_json = source or (row.process_json if row is not None and isinstance(row.process_json, dict) else {})
        if not process_json:
            unresolved.append({"type": "missing_process_reference", "process_uuid": process_uuid, "reason": "referenced process not found"})
            continue
        name = process_json.get("process_name_zh") if display_lang == "zh" else process_json.get("process_name_en")
        nodes.append(
            {
                "id": f"node-{process_uuid}",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": process_uuid,
                "name": _safe_str(name or process_json.get("process_name") or process_uuid),
                "location": _safe_str(process_json.get("location")),
                "inputs": [],
                "outputs": [],
                "emissions": [],
            }
        )
    if not nodes:
        return None, unresolved
    return {"functionalUnit": "1 unit", "nodes": nodes, "exchanges": [], "metadata": {"source": "tidas_import"}}, unresolved


def _parse_tidas_json_payload(*, source_name: str, raw_text: str) -> tuple[list[dict], list[str]]:
    try:
        payload = json.loads(raw_text)
    except Exception as exc:  # noqa: BLE001
        return [], [f"{source_name}: invalid JSON ({exc})"]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], []
    if isinstance(payload, dict):
        return [payload], []
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
        bundle_version = _safe_str(manifest.get("bundle_schema_version"))
        manifest_format = _safe_str(manifest.get("format"))
        manifest_version = _safe_str(manifest.get("version"))
        is_legacy_bundle = bundle_version == "tidas-lca-bundle-v1"
        is_package_v2 = manifest_format == "tiangong-tidas-package" and manifest_version == "2"
        if not is_legacy_bundle and not is_package_v2:
            errors.append(f"{source_name}: unsupported bundle manifest (bundle_schema_version={bundle_version or '<empty>'}, format={manifest_format or '<empty>'}, version={manifest_version or '<empty>'})")
        model_file = _safe_str(manifest.get("model_file"))
        model_files = [model_file] if model_file else []
        process_dir = _safe_str(manifest.get("process_dir") or "process").strip("/\\")
        flow_dir = _safe_str(manifest.get("flow_dir") or "flow").strip("/\\")
        if is_package_v2:
            process_dir = "processes"
            flow_dir = "flows"
            if require_model_file:
                entry_model_files = [
                    _safe_str(row.get("file_path"))
                    for row in _as_list(manifest.get("entries"))
                    if isinstance(row, dict) and _safe_str(row.get("table")) == "lifecyclemodels" and _safe_str(row.get("file_path"))
                ]
                if entry_model_files:
                    model_files = entry_model_files
        if require_model_file and not model_files:
            errors.append(f"{source_name}: manifest missing model_file")

        def _resolve_bundle_path(path_value: str) -> str:
            norm = _safe_str(path_value).strip("/\\")
            if not norm:
                return ""
            if bundle_root and not norm.lower().startswith((bundle_root + "/").lower()):
                return f"{bundle_root}/{norm}"
            return norm

        def _collect_json_entries(prefix: str) -> list[str]:
            norm_prefix = _resolve_bundle_path(prefix)
            if not norm_prefix:
                return []
            return sorted([name for name in names if name.lower().startswith((norm_prefix + "/").lower()) and name.lower().endswith(".json")])

        def _read_rows(entry_name: str) -> tuple[str, list[dict], list[str]]:
            try:
                raw_text = zf.read(entry_name).decode("utf-8-sig")
            except Exception as exc:  # noqa: BLE001
                return entry_name, [], [f"{entry_name}: unable to read zip entry ({exc})"]
            rows, parse_errors = _parse_tidas_json_payload(source_name=entry_name, raw_text=raw_text)
            return entry_name, rows, parse_errors

        model_items: list[tuple[str, list[dict], list[str]]] = []
        for raw_model_file in model_files:
            resolved = _resolve_bundle_path(raw_model_file)
            if resolved not in names:
                if require_model_file:
                    errors.append(f"{source_name}: manifest model_file not found in zip: {raw_model_file}")
            else:
                model_items.append(_read_rows(resolved))
        process_items = [_read_rows(name) for name in _collect_json_entries(process_dir)]
        flow_items = [_read_rows(name) for name in _collect_json_entries(flow_dir)]
        if not process_items and _resolve_bundle_path("processes/processDataSet.json") not in names:
            errors.append(f"{source_name}: no process json found under {process_dir}/")
        if not flow_items and _resolve_bundle_path("flows/flowDataSet.json") not in names:
            errors.append(f"{source_name}: no flow json found under {flow_dir}/")
        if require_model_file and not model_items:
            errors.append(f"{source_name}: no model json found")
        return manifest, flow_items, process_items, model_items, errors


def _build_tidas_base_report(*, import_type: str, payload: object, source_path: str = "") -> dict:
    upsert_mode_value = _safe_str(getattr(payload, "upsert_mode", None)) or "update"
    return {
        "job_id": str(uuid.uuid4()),
        "import_type": import_type,
        "source_path": source_path,
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


def _top_missing_flow_uuids(filtered: list[FilteredExchangeEvidence], top_n: int = 10) -> list[str]:
    counts = Counter(item.flow_uuid for item in filtered if item.flow_uuid)
    return [flow_uuid for flow_uuid, _count in counts.most_common(top_n)]


def _finalize_tidas_report(report: dict) -> dict:
    imported_count = int(report.get("imported_process_count") or 0)
    if imported_count <= 0:
        imported_count = int(report.get("inserted") or 0) + int(report.get("updated") or 0)
    filtered_count = int(report.get("filtered_exchange_count") or 0)
    unresolved_items = list(report.get("unresolved_items") or [])
    report["imported_count"] = imported_count
    report["filtered_count"] = filtered_count
    report["warning_count"] = len(list(report.get("warnings") or []))
    report["failed_count"] = int(report.get("failed") or 0)
    report["unresolved_count"] = int(report.get("unresolved_count") or len(unresolved_items))
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
        "warning_count": report["warning_count"],
        "failed_count": report["failed_count"],
        "unresolved_count": report["unresolved_count"],
        "created_projects": list(report.get("created_projects") or []),
    }
    return report


def _persist_tidas_import_report(db: Session, report_payload: dict) -> TidasImportReportResponse:
    report_payload = _finalize_tidas_report(report_payload)
    report_model = TidasImportReportResponse.model_validate(report_payload)
    report_json = report_model.model_dump(mode="json")
    row = db.get(DebugDiagnostic, report_model.job_id)
    if row is None:
        db.add(
            DebugDiagnostic(
                id=report_model.job_id,
                diagnostic_type=TIDAS_IMPORT_DIAGNOSTIC_TYPE,
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
        )
    else:
        row.diagnostic_type = TIDAS_IMPORT_DIAGNOSTIC_TYPE
        row.result_json = report_json
    db.commit()
    return report_model


def _refresh_flow_runtime_caches_for_current_request() -> None:
    invalidate_management_caches(flows=True, stats=True, reference_processes=True)


def _invalidate_management_caches(**kwargs: Any) -> None:
    invalidate_management_caches(**kwargs)


def _upsert_flow_record(db: Session, flow_record: dict, report: dict, *, dry_run: bool, upsert_mode: str) -> str | None:
    flow_uuid = str(flow_record["flow_uuid"])
    existing = db.get(FlowRecord, flow_uuid)
    if existing is not None and upsert_mode == "skip":
        report["skipped"] += 1
        return flow_uuid
    incoming_source = _safe_str(flow_record.get("source"))
    if existing is not None and _is_protected_builtin_flow(existing) and incoming_source != _safe_str(existing.source):
        report["skipped"] += 1
        report["warnings"].append(f"{flow_uuid}: built-in flow source={existing.source}; skipped overwrite from TIDAS flow import")
        return flow_uuid
    if existing is None:
        report["inserted"] += 1
        if not dry_run:
            db.add(FlowRecord(**flow_record))
        return flow_uuid
    report["updated"] += 1
    if not dry_run:
        previous_unit = existing.default_unit
        previous_unit_group = existing.unit_group
        existing.flow_name = str(flow_record.get("flow_name") or existing.flow_name)
        existing.flow_name_en = _safe_str(flow_record.get("flow_name_en")) or None
        existing.flow_type = str(flow_record.get("flow_type") or existing.flow_type)
        incoming_unit = str(flow_record.get("default_unit") or existing.default_unit)
        incoming_unit_group = str(flow_record.get("unit_group") or existing.unit_group)
        existing.default_unit = incoming_unit
        existing.unit_group = previous_unit_group if incoming_unit == previous_unit and previous_unit_group else incoming_unit_group
        compartment = _safe_str(flow_record.get("compartment"))
        if compartment and compartment != "[]":
            existing.compartment = compartment
        existing.source_updated_at = _safe_str(flow_record.get("source_updated_at")) or None
        existing.source = _safe_str(flow_record.get("source")) or existing.source
        existing.tidas_compatible = bool(flow_record.get("tidas_compatible"))
        existing.tidas_unit_group = existing.unit_group or _safe_str(flow_record.get("tidas_unit_group")) or existing.tidas_unit_group
        existing.tidas_flow_property_uuid = _safe_str(flow_record.get("tidas_flow_property_uuid")) or existing.tidas_flow_property_uuid
        existing.tidas_reference_source = _safe_str(flow_record.get("tidas_reference_source")) or existing.tidas_reference_source
    return flow_uuid


def import_tidas_flow_rows(
    db: Session,
    rows: list[dict],
    *,
    source_path: str,
    dry_run: bool = False,
    upsert_mode: str = "update",
    strict_mode: bool = False,
    source_label: str = TIDAS_FLOW_IMPORT_SOURCE,
    persist_report: bool = True,
) -> TidasImportReportResponse:
    payload = type("Payload", (), {"dry_run": dry_run, "upsert_mode": upsert_mode, "strict_mode": strict_mode})()
    report = _build_tidas_base_report(import_type="flows", payload=payload, source_path=source_path)
    report["total_files"] = 1
    for row in rows:
        report["total_records"] += 1
        flow_record, err = _extract_tidas_flow_record(row)
        if err or flow_record is None:
            report["failed"] += 1
            report["errors"].append(f"{source_path}: {err}")
            continue
        _label_imported_elementary_flow_source(flow_record, source_label)
        _upsert_flow_record(db, flow_record, report, dry_run=dry_run, upsert_mode=upsert_mode)
    if strict_mode and report["failed"] > 0:
        db.rollback()
    elif dry_run:
        db.rollback()
    else:
        db.commit()
        invalidate_management_caches(flows=True, stats=True, reference_processes=True)
    return _persist_tidas_import_report(db, report) if persist_report else TidasImportReportResponse.model_validate(_finalize_tidas_report(report))


def import_tidas_process_rows(
    db: Session,
    rows: list[dict],
    *,
    source_path: str,
    dry_run: bool = False,
    upsert_mode: str = "update",
    strict_mode: bool = False,
    valid_flow_uuids: set[str] | None = None,
    persist_report: bool = True,
) -> TidasImportReportResponse:
    payload = type("Payload", (), {"dry_run": dry_run, "upsert_mode": upsert_mode, "strict_mode": strict_mode})()
    report = _build_tidas_base_report(import_type="processes", payload=payload, source_path=source_path)
    report["total_files"] = 1
    valid_flow_uuids = set(valid_flow_uuids or _flow_uuid_set_cached(db))
    for row in rows:
        report["total_records"] += 1
        process_record, err = _extract_tidas_process_record(row)
        if err or process_record is None:
            report["failed"] += 1
            report["errors"].append(f"{source_path}: {err}")
            continue
        process_uuid = str(process_record["process_uuid"])
        existing = db.get(ReferenceProcess, process_uuid)
        if existing is not None and upsert_mode == "skip":
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
            "source": "tidas_ref_process_import",
        }
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
            report["inserted"] += 1
            report["imported_process_count"] += 1
            if not dry_run:
                db.add(
                    ReferenceProcess(
                        process_uuid=process_uuid,
                        process_name=str(process_record.get("process_name") or process_uuid),
                        process_name_zh=_safe_str(process_record.get("process_name_zh")) or None,
                        process_name_en=_safe_str(process_record.get("process_name_en")) or None,
                        process_type="unit_process",
                        reference_flow_uuid=reference_flow_uuid,
                        reference_flow_internal_id=_safe_str(process_record.get("reference_flow_internal_id")) or None,
                        process_json=normalized_json,
                        source_file=source_path,
                        source_process_uuid=None,
                        import_mode="locked",
                        import_report_json=process_report.model_dump(mode="json"),
                    )
                )
            continue
        report["updated"] += 1
        report["imported_process_count"] += 1
        if not dry_run:
            existing.process_name = str(process_record.get("process_name") or existing.process_name)
            existing.process_name_zh = _safe_str(process_record.get("process_name_zh")) or None
            existing.process_name_en = _safe_str(process_record.get("process_name_en")) or None
            existing.process_type = "unit_process"
            existing.reference_flow_uuid = reference_flow_uuid
            existing.reference_flow_internal_id = _safe_str(process_record.get("reference_flow_internal_id")) or None
            existing.process_json = normalized_json
            existing.source_file = source_path
            existing.source_process_uuid = None
            existing.import_mode = "locked"
            existing.import_report_json = process_report.model_dump(mode="json")
    filtered_models = [FilteredExchangeEvidence.model_validate(item) for item in report["filtered_exchanges"]]
    report["top_missing_flow_uuids"] = _top_missing_flow_uuids(filtered_models, top_n=10)
    if strict_mode and report["failed"] > 0:
        db.rollback()
    elif dry_run:
        db.rollback()
    else:
        db.commit()
        invalidate_management_caches(stats=True, reference_processes=True)
    return _persist_tidas_import_report(db, report) if persist_report else TidasImportReportResponse.model_validate(_finalize_tidas_report(report))


def import_tidas_model_rows(
    db: Session,
    rows: list[dict],
    *,
    source_path: str,
    dry_run: bool = False,
    strict_mode: bool = False,
    display_lang: str = "zh",
    process_json_by_uuid: dict[str, dict] | None = None,
    project_name: str | None = None,
    persist_report: bool = True,
) -> TidasImportReportResponse:
    payload = type("Payload", (), {"dry_run": dry_run, "upsert_mode": "update", "strict_mode": strict_mode})()
    report = _build_tidas_base_report(import_type="models", payload=payload, source_path=source_path)
    report["total_files"] = 1
    for row in rows:
        report["total_records"] += 1
        model_record, err = _extract_tidas_model_record(row)
        if err or model_record is None:
            report["failed"] += 1
            report["errors"].append(f"{source_path}: {err}")
            continue
        model_uuid = str(model_record["model_uuid"])
        if bool(model_record.get("topology_empty")):
            report["model_topology_empty_count"] += 1
            report["warnings"].append(f"{model_uuid}: MODEL_TOPOLOGY_EMPTY")
        for process_uuid in list(model_record.get("process_refs") or []):
            if db.get(ReferenceProcess, process_uuid) is None:
                report["unresolved_items"].append(
                    {
                        "model_uuid": model_uuid,
                        "type": "missing_process_reference",
                        "process_uuid": process_uuid,
                        "reason": "referenced process not found in reference_processes",
                    }
                )
        graph_json, graph_unresolved = _build_tidas_graph_from_model_record(
            db=db,
            model_record=model_record,
            process_json_by_uuid=process_json_by_uuid,
            display_lang=(_safe_str(display_lang) or "zh").lower(),
        )
        if graph_unresolved:
            report["unresolved_items"].extend(graph_unresolved)
        if graph_json is None:
            report["failed"] += 1
            report["errors"].append(f"{model_uuid}: HybridGraph payload is missing or cannot be derived from TIDAS model")
            continue
        try:
            graph = HybridGraph.model_validate(graph_json)
        except Exception as exc:  # noqa: BLE001
            report["failed"] += 1
            report["errors"].append(f"{model_uuid}: HybridGraph validation failed ({exc})")
            continue
        report["inserted"] += 1
        if dry_run:
            continue
        model_row = Model(
            name=_safe_str(project_name) or str(model_record.get("model_name") or model_uuid),
            functional_unit=graph.functionalUnit,
            description=f"Imported from TIDAS lifecycle model JSON (source_model_uuid={model_uuid})",
            source_policy="open_mixed",
            allowed_lcia_scope="ef31_only",
            status="active",
            updated_at=datetime.utcnow(),
        )
        db.add(model_row)
        db.flush()
        version = _create_project_version_from_graph_json(db=db, project_id=model_row.id, graph_json=graph.model_dump(mode="python"))
        report["created_projects"].append({"project_id": str(model_row.id), "name": str(model_row.name), "version": version.version})
    report["unresolved_count"] = len(report["unresolved_items"])
    if strict_mode and report["failed"] > 0:
        db.rollback()
    elif dry_run:
        db.rollback()
    elif report["failed"] > 0:
        db.rollback()
    else:
        db.commit()
        invalidate_management_caches(projects=True, stats=True)
    return _persist_tidas_import_report(db, report) if persist_report else TidasImportReportResponse.model_validate(_finalize_tidas_report(report))
