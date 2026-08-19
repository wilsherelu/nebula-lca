"""Shared TIDAS import core for upload routes and remote platform sync."""

from __future__ import annotations

import io
import json
import copy
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
    flow_semantic_to_exchange_type,
    is_elementary_flow_semantic,
    normalize_flow_semantic,
)
from .catalog_cache import invalidate_management_caches
from .project_versions import _create_project_version_from_graph_json
from .graph_storage import repair_tidas_product_flags
from .reference_catalog import (
    TidasAllocationImportError,
    _filter_exchanges_with_evidence,
    _flow_uuid_set_cached,
    _materialize_process_exchanges_for_graph,
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


def _extract_ilcd_flow_type(flow_dataset: dict) -> str:
    modelling = flow_dataset.get("modellingAndValidation")
    lci_method = modelling.get("LCIMethod") if isinstance(modelling, dict) else None
    raw_type = lci_method.get("typeOfDataSet") if isinstance(lci_method, dict) else None
    canonical_by_semantic = {
        "elementary_flow": "Elementary flow",
        "product_flow": "Product flow",
        "waste_flow": "Waste flow",
    }
    return canonical_by_semantic.get(normalize_flow_semantic(raw_type), "")


def _misclassified_elementary_port_uuids(graph_json: dict, elementary_flow_uuids: set[str]) -> list[str]:
    mismatched: set[str] = set()
    for node in list(graph_json.get("nodes") or []):
        if not isinstance(node, dict):
            continue
        for port in [*list(node.get("inputs") or []), *list(node.get("outputs") or [])]:
            if not isinstance(port, dict):
                continue
            flow_uuid = _safe_str(port.get("flowUuid") or port.get("flow_uuid"))
            if flow_uuid in elementary_flow_uuids and _safe_str(port.get("type")) != "biosphere":
                mismatched.add(flow_uuid)
    return sorted(mismatched)


def _reference_flow_property(flow_dataset: dict) -> dict | None:
    flow_info = flow_dataset.get("flowInformation") if isinstance(flow_dataset.get("flowInformation"), dict) else {}
    quantitative_reference = (
        flow_info.get("quantitativeReference")
        if isinstance(flow_info.get("quantitativeReference"), dict)
        else {}
    )
    reference_id = _safe_str(quantitative_reference.get("referenceToReferenceFlowProperty"))
    props = flow_dataset.get("flowProperties") if isinstance(flow_dataset.get("flowProperties"), dict) else {}
    rows = [row for row in _as_list(props.get("flowProperty")) if isinstance(row, dict)]
    if reference_id:
        for row in rows:
            internal_id = _safe_str(row.get("@dataSetInternalID") or row.get("dataSetInternalID"))
            if internal_id == reference_id:
                return row
    return rows[0] if rows else None


def _infer_unit_defaults_from_flow_dataset(flow_dataset: dict) -> tuple[str, str]:
    flow_info = flow_dataset.get("flowInformation") if isinstance(flow_dataset.get("flowInformation"), dict) else {}
    ref_unit = _safe_str(flow_info.get("referenceUnit"))
    unit_group = _safe_str(flow_info.get("unitGroup"))
    if ref_unit and unit_group:
        return ref_unit, unit_group
    reference_property = _reference_flow_property(flow_dataset)
    ref = reference_property.get("referenceToFlowPropertyDataSet") if isinstance(reference_property, dict) else None
    short_desc = ref.get("common:shortDescription") if isinstance(ref, dict) else None
    hint_blob = (_pick_localized_text(short_desc, preferred_langs=("en", "zh")) or "").lower()
    if "energy" in hint_blob or "calorific" in hint_blob:
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
    return row.get("allocationFactor") is not None or row.get("allocation_factor") is not None


def _tidas_allocated_fraction(row: dict) -> float | None:
    allocations = row.get("allocations")
    if not isinstance(allocations, dict):
        return None
    candidates = _as_list(allocations.get("allocation"))
    values: list[float] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        raw = candidate.get("@allocatedFraction")
        if raw is None:
            raw = candidate.get("allocatedFraction")
        if raw is None:
            continue
        try:
            parsed = float(raw)
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            values.append(parsed)
    if not values:
        return None
    return sum(values)


def _has_tidas_allocation(row: dict) -> bool:
    allocations = row.get("allocations")
    return isinstance(allocations, dict) and allocations.get("allocation") is not None


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
    # The standard ILCD type is authoritative. Product classifications also
    # use common:classification, so a non-empty classification path alone must
    # not turn a product flow into an elementary flow.
    flow_type = _extract_ilcd_flow_type(flow_dataset)
    if not flow_type:
        flow_type = _safe_str(row.get("flow_type"))
    if not flow_type:
        elementary = classification.get("common:elementaryFlowCategorization")
        flow_type = "Elementary flow" if isinstance(elementary, dict) else "Product flow"
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
    flow_property_uuid = ""
    reference_property = _reference_flow_property(flow_dataset)
    ref = reference_property.get("referenceToFlowPropertyDataSet") if isinstance(reference_property, dict) else None
    if isinstance(ref, dict):
        flow_property_uuid = _safe_str(ref.get("@refObjectId") or ref.get("refObjectId"))
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
    allocation_factor = row.get("allocationFactor")
    if allocation_factor is None:
        allocation_factor = row.get("allocation_factor")
    tidas_allocated_fraction = _tidas_allocated_fraction(row)

    return {
        "exchange_internal_id": _safe_str(row.get("exchange_internal_id") or row.get("@dataSetInternalID") or row.get("dataSetInternalID")),
        "flow_uuid": flow_uuid,
        "flow_name": flow_name,
        "direction": direction,
        "amount": amount,
        "unit": _exchange_unit_text(row),
        "is_allocated_product": _exchange_is_allocated_product(row),
        "is_reference_flow": bool(row.get("is_reference_flow")),
        "isProduct": bool(row.get("is_reference_flow") or allocation_factor is not None),
        "allocationFactor": (_numeric_value(allocation_factor) if allocation_factor is not None else None),
        "tidasAllocationPresent": _has_tidas_allocation(row),
        "tidasAllocatedFraction": tidas_allocated_fraction,
    }


def _hydrate_exchange_flow_semantics(
    db: Session,
    exchanges: list[dict],
    staged_flow_records: dict[str, dict] | None = None,
) -> None:
    staged = staged_flow_records or {}
    flow_uuids = {
        _safe_str(exchange.get("flow_uuid"))
        for exchange in exchanges
        if isinstance(exchange, dict) and _safe_str(exchange.get("flow_uuid"))
    }
    stored = {
        str(row.flow_uuid): row
        for row in db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(flow_uuids)).all()
    } if flow_uuids else {}
    for exchange in exchanges:
        if not isinstance(exchange, dict):
            continue
        flow_uuid = _safe_str(exchange.get("flow_uuid"))
        staged_row = staged.get(flow_uuid) or {}
        stored_row = stored.get(flow_uuid)
        flow_type = staged_row.get("flow_type") or getattr(stored_row, "flow_type", None)
        unit_group = staged_row.get("unit_group") or getattr(stored_row, "unit_group", None)
        default_unit = staged_row.get("default_unit") or getattr(stored_row, "default_unit", None)
        if flow_type:
            exchange["flow_type"] = str(flow_type)
        if unit_group:
            exchange["unit_group"] = str(unit_group)
        if not _safe_str(exchange.get("unit")) and default_unit:
            exchange["unit"] = str(default_unit)


def _reference_flow_uuid_from_quantitative_reference(
    process_json: dict,
    exchanges: list[dict],
) -> tuple[str | None, list[str]]:
    warnings: list[str] = []
    reference_internal_id = _safe_str(process_json.get("reference_flow_internal_id"))
    if not reference_internal_id:
        return _safe_str(process_json.get("reference_flow_source_uuid")) or None, warnings
    for exchange in exchanges:
        if not isinstance(exchange, dict):
            continue
        if _safe_str(exchange.get("exchange_internal_id")) != reference_internal_id:
            continue
        if _safe_str(exchange.get("direction")).lower() != "output":
            warnings.append("reference_flow_internal_id matched non-output exchange")
            return None, warnings
        return _safe_str(exchange.get("flow_uuid")) or None, warnings
    warnings.append("reference output exchange not found after flow filtering")
    return None, warnings


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
    payload = next(
        (
            candidate
            for candidate in (row.get("json_ordered"), row.get("json"), row)
            if isinstance(candidate, dict) and isinstance(candidate.get("lifeCycleModelDataSet"), dict)
        ),
        _payload_from_row(row),
    )
    graph = _graph_from_payload(row) or _graph_from_payload(payload)
    model_dataset = payload.get("lifeCycleModelDataSet") if isinstance(payload.get("lifeCycleModelDataSet"), dict) else {}
    model_info = model_dataset.get("lifeCycleModelInformation") if isinstance(model_dataset.get("lifeCycleModelInformation"), dict) else {}
    data_info = model_info.get("dataSetInformation") if isinstance(model_info.get("dataSetInformation"), dict) else {}
    model_uuid = _safe_str(
        payload.get("model_uuid")
        or data_info.get("common:UUID")
        or model_dataset.get("UUID")
        or row.get("uuid")
        or row.get("id")
    )
    if not model_uuid:
        return None, "model missing UUID/id"
    name_zh, name_en = _extract_ilcd_name(data_info.get("name"))
    model_name = _safe_str(payload.get("model_name") or payload.get("name") or row.get("name") or name_zh or name_en or model_uuid)
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
    technology = model_info.get("technology") if isinstance(model_info.get("technology"), dict) else {}
    processes = technology.get("processes") if isinstance(technology.get("processes"), dict) else {}
    model_instances: list[dict] = []
    for instance in _as_list(processes.get("processInstance")):
        if not isinstance(instance, dict):
            continue
        ref = instance.get("referenceToProcess") if isinstance(instance.get("referenceToProcess"), dict) else {}
        process_uuid = _safe_str(ref.get("@refObjectId") or ref.get("refObjectId"))
        if process_uuid:
            process_refs.add(process_uuid)
        output_connections: list[dict] = []
        connections = instance.get("connections") if isinstance(instance.get("connections"), dict) else {}
        for output in _as_list(connections.get("outputExchange")):
            if not isinstance(output, dict):
                continue
            flow_uuid = _safe_str(output.get("@flowUUID") or output.get("flowUUID"))
            for downstream in _as_list(output.get("downstreamProcess")):
                if not isinstance(downstream, dict):
                    continue
                downstream_id = _safe_str(downstream.get("@id") or downstream.get("id"))
                if flow_uuid and downstream_id:
                    output_connections.append({
                        "flow_uuid": flow_uuid,
                        "downstream_instance_id": downstream_id,
                        "downstream_flow_uuid": _safe_str(downstream.get("@flowUUID") or downstream.get("flowUUID")) or flow_uuid,
                    })
        model_instances.append({
            "instance_id": _safe_str(instance.get("@dataSetInternalID") or instance.get("dataSetInternalID")),
            "process_uuid": process_uuid,
            "output_connections": output_connections,
        })
    json_tg = row.get("json_tg") if isinstance(row.get("json_tg"), dict) else {}
    xflow = json_tg.get("xflow") if isinstance(json_tg.get("xflow"), dict) else {}
    xflow_nodes = [item for item in _as_list(xflow.get("nodes")) if isinstance(item, dict)]
    xflow_edges = [item for item in _as_list(xflow.get("edges")) if isinstance(item, dict)]
    return {
        "model_uuid": model_uuid,
        "model_name": model_name,
        "process_refs": sorted(process_refs),
        "topology_empty": not bool((graph and graph.get("nodes")) or xflow_nodes or model_instances),
        "graph_json": graph,
        "xflow_nodes": xflow_nodes,
        "xflow_edges": xflow_edges,
        "model_instances": model_instances,
        "raw": payload,
    }, None


def _build_tidas_graph_from_model_record(
    *,
    db: Session,
    model_record: dict,
    process_json_by_uuid: dict[str, dict] | None = None,
    display_lang: str = "zh",
    allocation_policy: str = "quantity",
) -> tuple[dict | None, list[dict]]:
    graph_json = model_record.get("graph_json") if isinstance(model_record.get("graph_json"), dict) else None
    if graph_json is not None:
        repair_tidas_product_flags(graph_json)
        return graph_json, []
    xflow_nodes = [item for item in list(model_record.get("xflow_nodes") or []) if isinstance(item, dict)]
    if xflow_nodes:
        return _build_tidas_graph_from_xflow_record(
            db=db,
            model_record=model_record,
            process_json_by_uuid=process_json_by_uuid or {},
            display_lang=display_lang,
            allocation_policy=allocation_policy,
        )
    model_instances = [item for item in list(model_record.get("model_instances") or []) if isinstance(item, dict)]
    if model_instances:
        node_id_by_instance = {
            _safe_str(item.get("instance_id")): f"node-tidas-instance-{_safe_str(item.get('instance_id'))}"
            for item in model_instances
            if _safe_str(item.get("instance_id"))
        }
        synthesized_nodes = [
            {
                "id": node_id_by_instance[instance_id],
                "data": {"id": _safe_str(item.get("process_uuid"))},
            }
            for item in model_instances
            if (instance_id := _safe_str(item.get("instance_id"))) in node_id_by_instance
        ]
        synthesized_edges: list[dict] = []
        for item in model_instances:
            source_id = node_id_by_instance.get(_safe_str(item.get("instance_id")))
            if not source_id:
                continue
            for connection in list(item.get("output_connections") or []):
                if not isinstance(connection, dict):
                    continue
                target_id = node_id_by_instance.get(_safe_str(connection.get("downstream_instance_id")))
                source_flow_uuid = _safe_str(connection.get("flow_uuid"))
                target_flow_uuid = _safe_str(connection.get("downstream_flow_uuid")) or source_flow_uuid
                if not target_id or not source_flow_uuid or source_flow_uuid != target_flow_uuid:
                    continue
                synthesized_edges.append({
                    "id": f"edge-tidas-standard-{len(synthesized_edges) + 1}",
                    "source": {"cell": source_id, "port": f"OUTPUT:{source_flow_uuid}"},
                    "target": {"cell": target_id, "port": f"INPUT:{target_flow_uuid}"},
                })
        synthesized_record = dict(model_record)
        synthesized_record["xflow_nodes"] = synthesized_nodes
        synthesized_record["xflow_edges"] = synthesized_edges
        return _build_tidas_graph_from_xflow_record(
            db=db,
            model_record=synthesized_record,
            process_json_by_uuid=process_json_by_uuid or {},
            display_lang=display_lang,
            allocation_policy=allocation_policy,
        )
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
                "reference_product": "",
                "inputs": [],
                "outputs": [],
                "emissions": [],
            }
        )
    if not nodes:
        return None, unresolved
    return {"functionalUnit": "1 unit", "nodes": nodes, "exchanges": [], "metadata": {"source": "tidas_import"}}, unresolved


def _build_tidas_graph_from_xflow_record(
    *,
    db: Session,
    model_record: dict,
    process_json_by_uuid: dict[str, dict],
    display_lang: str,
    allocation_policy: str,
) -> tuple[dict | None, list[dict]]:
    """Rebuild a Nebula graph from the TIDAS XFlow extension.

    Process JSON remains authoritative for exchanges and quantitative
    references. XFlow contributes node identity, layout, and topology only.
    """
    xflow_nodes = [item for item in list(model_record.get("xflow_nodes") or []) if isinstance(item, dict)]
    xflow_edges = [item for item in list(model_record.get("xflow_edges") or []) if isinstance(item, dict)]
    resolved_process_json_by_uuid = dict(process_json_by_uuid)
    for xnode in xflow_nodes:
        data = xnode.get("data") if isinstance(xnode.get("data"), dict) else {}
        process_uuid = _safe_str(data.get("id") or xnode.get("process_uuid"))
        if not process_uuid or process_uuid in resolved_process_json_by_uuid:
            continue
        row = db.get(ReferenceProcess, process_uuid)
        if row is not None and isinstance(row.process_json, dict):
            resolved_process_json_by_uuid[process_uuid] = row.process_json

    flow_uuids = {
        _safe_str(exchange.get("flow_uuid"))
        for process_json in resolved_process_json_by_uuid.values()
        for exchange in list(process_json.get("exchanges") or [])
        if isinstance(exchange, dict) and _safe_str(exchange.get("flow_uuid"))
    }
    flow_meta: dict[str, tuple[str, str, str, str | None]] = {}
    if flow_uuids:
        rows = db.query(
            FlowRecord.flow_uuid,
            FlowRecord.flow_name,
            FlowRecord.default_unit,
            FlowRecord.unit_group,
            FlowRecord.flow_type,
        ).filter(FlowRecord.flow_uuid.in_(flow_uuids)).all()
        flow_meta = {
            str(row.flow_uuid): (
                str(row.flow_name or row.flow_uuid),
                str(row.default_unit or "kg"),
                str(row.unit_group or "Units of mass"),
                row.flow_type,
            )
            for row in rows
        }

    nodes: list[dict] = []
    node_ids: set[str] = set()
    positions: dict[str, dict[str, float]] = {}
    unresolved: list[dict] = []
    materialization_warnings: list[str] = []
    seen_names: dict[str, int] = {}
    process_instance_counts: dict[str, int] = {}
    for index, xnode in enumerate(xflow_nodes):
        data = xnode.get("data") if isinstance(xnode.get("data"), dict) else {}
        node_id = _safe_str(xnode.get("id")) or f"node_tidas_xflow_{index}"
        source_process_uuid = _safe_str(data.get("id") or xnode.get("process_uuid"))
        source = resolved_process_json_by_uuid.get(source_process_uuid)
        if source is None:
            unresolved.append({
                "model_uuid": model_record.get("model_uuid"),
                "type": "missing_process_reference",
                "process_uuid": source_process_uuid,
                "node_id": node_id,
                "reason": "xflow node process not found",
            })
            continue

        instance_count = process_instance_counts.get(source_process_uuid, 0) + 1
        process_instance_counts[source_process_uuid] = instance_count
        process_uuid = source_process_uuid
        if instance_count > 1:
            process_uuid = str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"nebula:tidas-model:{_safe_str(model_record.get('model_uuid'))}:node:{node_id}:process:{source_process_uuid}",
            ))
            instance_source = copy.deepcopy(source)
            instance_source["process_uuid"] = process_uuid
            instance_source["source_process_uuid"] = source_process_uuid
            resolved_process_json_by_uuid[process_uuid] = instance_source
            source = instance_source
            if db.get(ReferenceProcess, process_uuid) is None:
                source_row = db.get(ReferenceProcess, source_process_uuid)
                if source_row is not None:
                    import_report = copy.deepcopy(source_row.import_report_json or {})
                    import_report["process_uuid"] = process_uuid
                    import_report["source_process_uuid"] = source_process_uuid
                    db.add(ReferenceProcess(
                        process_uuid=process_uuid,
                        process_name=f"{source_row.process_name}({instance_count})",
                        process_name_zh=(f"{source_row.process_name_zh}({instance_count})" if source_row.process_name_zh else None),
                        process_name_en=(f"{source_row.process_name_en}({instance_count})" if source_row.process_name_en else None),
                        process_type=source_row.process_type,
                        reference_flow_uuid=source_row.reference_flow_uuid,
                        reference_flow_internal_id=source_row.reference_flow_internal_id,
                        process_json=instance_source,
                        source_file=source_row.source_file,
                        source_process_uuid=source_process_uuid,
                        import_mode=source_row.import_mode,
                        import_report_json=import_report,
                    ))

        source_exchanges = copy.deepcopy(
            [exchange for exchange in list(source.get("exchanges") or []) if isinstance(exchange, dict)]
        )
        for exchange in source_exchanges:
            meta = flow_meta.get(_safe_str(exchange.get("flow_uuid")))
            if meta is None:
                continue
            exchange["flow_type"] = meta[3]
            exchange["unit_group"] = meta[2]
            if not _safe_str(exchange.get("unit")):
                exchange["unit"] = meta[1]
        graph_exchanges, materialized_reference_flow_uuid, product_warnings = _materialize_process_exchanges_for_graph(
            process_uuid=process_uuid,
            process_json=source,
            exchanges=source_exchanges,
            allocation_policy=allocation_policy,
        )
        materialization_warnings.extend(f"{process_uuid}: {warning}" for warning in product_warnings)

        inputs: list[dict] = []
        outputs: list[dict] = []
        seen_port_ids: set[str] = set()
        reference_flow_uuid = materialized_reference_flow_uuid or ""
        for exchange in graph_exchanges:
            if not isinstance(exchange, dict):
                continue
            flow_uuid = _safe_str(exchange.get("flow_uuid"))
            meta = flow_meta.get(flow_uuid)
            if not flow_uuid or meta is None:
                continue
            direction = "output" if _safe_str(exchange.get("direction")).lower() == "output" else "input"
            prefix = "OUTPUT" if direction == "output" else "INPUT"
            base_port_id = f"{prefix}:{flow_uuid}"
            port_id = base_port_id
            if port_id in seen_port_ids:
                internal_id = _safe_str(exchange.get("exchange_internal_id")) or str(len(seen_port_ids) + 1)
                port_id = f"{base_port_id}:{internal_id}"
            seen_port_ids.add(port_id)
            is_product = bool(exchange.get("isProduct"))
            port = {
                "id": port_id,
                "flowUuid": flow_uuid,
                "name": meta[0],
                "unit": meta[1],
                "unitGroup": meta[2],
                "amount": float(exchange.get("amount") or 0.0),
                "type": flow_semantic_to_exchange_type(meta[3]),
                "direction": direction,
                "showOnNode": not is_elementary_flow_semantic(meta[3]),
                "internalExposed": not is_elementary_flow_semantic(meta[3]),
                "isProduct": is_product,
                "allocationFactor": exchange.get("allocationFactor"),
                "allocationBasis": exchange.get("allocationBasis"),
            }
            (outputs if direction == "output" else inputs).append(port)

        label = data.get("label") if isinstance(data.get("label"), dict) else {}
        preferred = ("en", "zh") if _safe_str(display_lang).lower() == "en" else ("zh", "en")
        name = (
            _pick_localized_text(label.get("baseName"), preferred_langs=preferred)
            or _safe_str(source.get("process_name_en") if preferred[0] == "en" else source.get("process_name_zh"))
            or _safe_str(source.get("process_name"))
            or process_uuid
        )
        name_count = seen_names.get(name, 0) + 1
        seen_names[name] = name_count
        if name_count > 1:
            name = f"{name}({name_count})"
        reference_port = next((port for port in outputs if port.get("flowUuid") == reference_flow_uuid), None)
        node = {
            "id": node_id,
            "node_kind": "unit_process",
            "mode": "normalized",
            "process_uuid": process_uuid,
            "name": name,
            "location": _safe_str(source.get("location")) or "GLO",
            "reference_product": _safe_str((reference_port or {}).get("name")),
            "reference_product_flow_uuid": reference_flow_uuid or None,
            "reference_product_direction": "output" if reference_flow_uuid else None,
            "inputs": inputs,
            "outputs": outputs,
            "emissions": [],
        }
        nodes.append(node)
        node_ids.add(node_id)
        position = xnode.get("position") if isinstance(xnode.get("position"), dict) else {}
        if not position and (xnode.get("x") is not None or xnode.get("y") is not None):
            position = {"x": xnode.get("x"), "y": xnode.get("y")}
        if position:
            positions[node_id] = {
                "x": _numeric_value(position.get("x")),
                "y": _numeric_value(position.get("y")),
            }

    if len(positions) < len(nodes):
        column_count = 6
        column_y = [100.0] * column_count
        for index, node in enumerate(nodes):
            node_id = _safe_str(node.get("id"))
            if node_id in positions:
                continue
            column = min(range(column_count), key=column_y.__getitem__)
            position = {"x": 120.0 + column * 430.0, "y": column_y[column]}
            positions[node_id] = position
            node["position"] = dict(position)
            visible_port_count = sum(
                1
                for port in [*list(node.get("inputs") or []), *list(node.get("outputs") or [])]
                if isinstance(port, dict) and bool(port.get("showOnNode"))
            )
            estimated_height = max(220.0, 150.0 + visible_port_count * 30.0)
            column_y[column] += estimated_height + 100.0

    output_port_ids_by_node = {
        str(node.get("id")): {str(port.get("id")) for port in list(node.get("outputs") or [])}
        for node in nodes
    }
    input_port_ids_by_node = {
        str(node.get("id")): {str(port.get("id")) for port in list(node.get("inputs") or [])}
        for node in nodes
    }
    edges: list[dict] = []
    for index, xedge in enumerate(xflow_edges, start=1):
        source = xedge.get("source") if isinstance(xedge.get("source"), dict) else {}
        target = xedge.get("target") if isinstance(xedge.get("target"), dict) else {}
        from_node = _safe_str(source.get("cell"))
        to_node = _safe_str(target.get("cell"))
        if from_node not in node_ids or to_node not in node_ids:
            continue
        source_port = _safe_str(source.get("port"))
        target_port = _safe_str(target.get("port"))
        data = xedge.get("data") if isinstance(xedge.get("data"), dict) else {}
        connection = data.get("connection") if isinstance(data.get("connection"), dict) else {}
        output_exchange = connection.get("outputExchange") if isinstance(connection.get("outputExchange"), dict) else {}
        connection_flow_uuid = _safe_str(output_exchange.get("@flowUUID") or output_exchange.get("flowUUID"))
        if connection_flow_uuid and not source_port:
            source_port = next(
                (
                    port_id
                    for port_id in output_port_ids_by_node.get(from_node, set())
                    if port_id == f"OUTPUT:{connection_flow_uuid}" or port_id.startswith(f"OUTPUT:{connection_flow_uuid}:")
                ),
                "",
            )
        if connection_flow_uuid and not target_port:
            target_port = next(
                (
                    port_id
                    for port_id in input_port_ids_by_node.get(to_node, set())
                    if port_id == f"INPUT:{connection_flow_uuid}" or port_id.startswith(f"INPUT:{connection_flow_uuid}:")
                ),
                "",
            )
        if (
            source_port not in output_port_ids_by_node.get(from_node, set())
            or target_port not in input_port_ids_by_node.get(to_node, set())
        ):
            unresolved.append({
                "model_uuid": model_record.get("model_uuid"),
                "type": "invalid_process_connection",
                "edge_id": _safe_str(xedge.get("id")) or f"edge_tidas_xflow_{index}",
                "reason": "connection flow is not exposed by the referenced source output and target input",
            })
            continue
        flow_uuid = source_port.split(":", 1)[1] if ":" in source_port else ""
        meta = flow_meta.get(flow_uuid)
        if not flow_uuid or meta is None:
            continue
        amount = _numeric_value(connection.get("exchangeAmount"), default=1.0)
        if amount <= 0:
            amount = 1.0
        edges.append({
            "id": _safe_str(xedge.get("id")) or f"edge_tidas_xflow_{index}",
            "fromNode": from_node,
            "toNode": to_node,
            "sourceHandle": f"out:{source_port}",
            "targetHandle": f"in:{target_port}",
            "sourcePortId": source_port,
            "targetPortId": target_port,
            "flowUuid": flow_uuid,
            "flowName": meta[0],
            "quantityMode": "dual",
            "amount": amount,
            "providerAmount": amount,
            "consumerAmount": amount,
            "unit": meta[1],
            "type": "technosphere",
        })

    if not nodes:
        return None, unresolved
    graph = {
        "functionalUnit": _safe_str(model_record.get("model_name")) or "1 unit",
        "nodes": nodes,
        "exchanges": edges,
        "metadata": {
            "source": "tidas_model_import",
            "tidas_model_uuid": _safe_str(model_record.get("model_uuid")),
            "node_positions": positions,
            "product_allocation_warnings": materialization_warnings,
        },
    }
    repair_tidas_product_flags(graph)
    return graph, unresolved


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


def _persist_tidas_import_report(
    db: Session,
    report_payload: dict,
    *,
    commit: bool = True,
) -> TidasImportReportResponse:
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
    if commit:
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
    with_transaction: bool = False,
) -> TidasImportReportResponse:
    """Import flow rows from TIDAS/ILCD format.

    Args:
        db: Database session.
        rows: List of flow record dicts.
        source_path: Source identifier for reporting.
        dry_run: If True, rollback all changes.
        upsert_mode: "update" or "skip" for existing records.
        strict_mode: If True, rollback on any failure.
        source_label: Source label for imported flows.
        persist_report: If True, persist diagnostic report to DB.
        with_transaction: If True, caller manages transaction (no internal commit/rollback).
                         Caller must commit or rollback after calling.

    Returns:
        TidasImportReportResponse with import statistics.
    """
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
    if with_transaction:
        return (
            _persist_tidas_import_report(db, report, commit=False)
            if persist_report
            else TidasImportReportResponse.model_validate(_finalize_tidas_report(report))
        )
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
    with_transaction: bool = False,
) -> TidasImportReportResponse:
    """Import process rows from TIDAS/ILCD format.

    Args:
        db: Database session.
        rows: List of process record dicts.
        source_path: Source identifier for reporting.
        dry_run: If True, rollback all changes.
        upsert_mode: "update" or "skip" for existing records.
        strict_mode: If True, rollback on any failure.
        valid_flow_uuids: Pre-computed set of valid flow UUIDs for exchange filtering.
        persist_report: If True, persist diagnostic report to DB.
        with_transaction: If True, caller manages transaction (no internal commit/rollback).

    Returns:
        TidasImportReportResponse with import statistics.
    """
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
        _hydrate_exchange_flow_semantics(db, kept_exchanges)
        reference_flow_uuid, product_warnings = _reference_flow_uuid_from_quantitative_reference(process_record, kept_exchanges)
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
    if with_transaction:
        return (
            _persist_tidas_import_report(db, report, commit=False)
            if persist_report
            else TidasImportReportResponse.model_validate(_finalize_tidas_report(report))
        )
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
    with_transaction: bool = False,
    allocation_policy: str = "quantity",
) -> TidasImportReportResponse:
    """Import model rows from TIDAS/ILCD format.

    Args:
        db: Database session.
        rows: List of model record dicts.
        source_path: Source identifier for reporting.
        dry_run: If True, rollback all changes.
        strict_mode: If True, rollback on any failure.
        display_lang: Language for model names (default "zh").
        process_json_by_uuid: Pre-loaded process JSON for graph building.
        project_name: Override project name.
        persist_report: If True, persist diagnostic report to DB.
        with_transaction: If True, caller manages transaction (no internal commit/rollback).

    Returns:
        TidasImportReportResponse with import statistics.
    """
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
            allocation_policy=allocation_policy,
        )
        if graph_unresolved:
            report["unresolved_items"].extend(graph_unresolved)
        if graph_json is None:
            report["failed"] += 1
            report["errors"].append(f"{model_uuid}: HybridGraph payload is missing or cannot be derived from TIDAS model")
            continue
        repair_tidas_product_flags(graph_json)
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
    if with_transaction:
        return (
            _persist_tidas_import_report(db, report, commit=False)
            if persist_report
            else TidasImportReportResponse.model_validate(_finalize_tidas_report(report))
        )
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
