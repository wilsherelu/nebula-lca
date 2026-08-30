from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import settings


SCHEMA_VERSION = "tiangong-open-dataset-snapshot.v1"
SOURCE_NAMESPACE = "tiangong_open_data"
DATASET_KIND = "process"
STATE_SCOPE = "open"
VERSION_RE = re.compile(r"^\d{2}\.\d{2}\.\d{3}$")
SNAPSHOT_BODY_FIELDS = (
    "schema_version",
    "source_namespace",
    "dataset_kind",
    "state_scope",
    "filters",
    "declared_total",
    "records",
)


class ProviderTidasProcessSnapshotError(RuntimeError):
    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _exact_uuid(value: Any, *, field: str) -> str:
    raw = str(value or "").strip()
    try:
        normalized = str(uuid.UUID(raw))
    except ValueError as exc:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_IDENTITY_INVALID",
            "The configured TIDAS Process snapshot contains an invalid UUID.",
            field=field,
            value=raw,
        ) from exc
    if raw != normalized:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_IDENTITY_NONCANONICAL",
            "TIDAS Process snapshot UUIDs must use canonical lowercase form.",
            field=field,
            value=raw,
        )
    return normalized


def _exact_version(value: Any, *, field: str) -> str:
    version = str(value or "").strip()
    if not VERSION_RE.fullmatch(version):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_VERSION_INVALID",
            "The configured TIDAS Process snapshot contains an invalid exact version.",
            field=field,
            value=version,
        )
    return version


def _localized_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        text = value.get("#text") or value.get("text")
        return str(text).strip() if text is not None and str(text).strip() else None
    rows = _as_list(value)
    for row in rows:
        if isinstance(row, dict) and row.get("@xml:lang") == "en":
            return _localized_text(row)
    for row in rows:
        text = _localized_text(row)
        if text:
            return text
    return None


def _finite_number(value: Any, *, field: str, positive: bool = False) -> float:
    try:
        amount = float(value)
    except (TypeError, ValueError) as exc:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_EXCHANGE_AMOUNT_INVALID",
            "A TIDAS Process exchange amount must be finite.",
            field=field,
            value=value,
        ) from exc
    if not math.isfinite(amount) or (positive and amount <= 0):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_EXCHANGE_AMOUNT_INVALID",
            "A TIDAS Process exchange amount must be finite and the quantitative reference must be positive.",
            field=field,
            value=value,
        )
    return amount


def _exchange_receipt(exchange: dict[str, Any], *, index: int) -> dict[str, Any]:
    internal_id = str(
        exchange.get("@dataSetInternalID") or exchange.get("dataSetInternalID") or ""
    ).strip()
    if not internal_id:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_EXCHANGE_IDENTITY_INCOMPLETE",
            "Every TIDAS Process exchange must have an internal ID.",
            exchange_index=index,
        )
    direction_raw = str(exchange.get("exchangeDirection") or "").strip()
    if direction_raw not in {"Input", "Output"}:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_EXCHANGE_DIRECTION_INVALID",
            "Every TIDAS Process exchange must have an explicit Input or Output direction.",
            exchange_internal_id=internal_id,
            direction=direction_raw,
        )
    reference = exchange.get("referenceToFlowDataSet")
    if not isinstance(reference, dict):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_EXCHANGE_FLOW_IDENTITY_INCOMPLETE",
            "Every TIDAS Process exchange must reference an exact Flow dataset.",
            exchange_internal_id=internal_id,
        )
    flow_uuid = _exact_uuid(
        reference.get("@refObjectId") or reference.get("refObjectId"),
        field=f"exchanges[{index}].referenceToFlowDataSet.@refObjectId",
    )
    flow_version = _exact_version(
        reference.get("@version") or reference.get("version"),
        field=f"exchanges[{index}].referenceToFlowDataSet.@version",
    )
    mean_raw = exchange.get("meanAmount")
    resulting_raw = exchange.get("resultingAmount")
    amount = _finite_number(
        mean_raw if mean_raw is not None else resulting_raw,
        field=f"exchanges[{index}].meanAmount",
    )
    resulting_amount = None
    if resulting_raw is not None:
        resulting_amount = _finite_number(
            resulting_raw,
            field=f"exchanges[{index}].resultingAmount",
        )
    return {
        "exchange_internal_id": internal_id,
        "direction": direction_raw.lower(),
        "amount": amount,
        "mean_amount_source": str(mean_raw) if mean_raw is not None else None,
        "resulting_amount": resulting_amount,
        "resulting_amount_source": str(resulting_raw) if resulting_raw is not None else None,
        "flow": {
            "source_namespace": SOURCE_NAMESPACE,
            "flow_uuid": flow_uuid,
            "version": flow_version,
            "name": _localized_text(reference.get("common:shortDescription")),
        },
        "flow_catalog_binding_status": "requires_exact_flow_resolution",
        "unit_identity_status": "requires_exact_flow_resolution",
    }


@dataclass(frozen=True)
class TidasProcessSnapshotRecord:
    process_uuid: str
    version: str
    process_type: str
    name: str | None
    quantitative_reference: dict[str, Any]
    exchanges: tuple[dict[str, Any], ...]
    included_process_refs: tuple[dict[str, str], ...]
    content_hash: str
    source_modified_at: str | None


@dataclass(frozen=True)
class TidasProcessSnapshot:
    snapshot_hash: str
    records: dict[tuple[str, str], TidasProcessSnapshotRecord]

    def resolve(self, process_uuid: str, version: str) -> dict[str, Any] | None:
        record = self.records.get((process_uuid, version))
        if record is None:
            return None
        return {
            "resolution_source": "tidas_exact_process_snapshot",
            "source_namespace": SOURCE_NAMESPACE,
            "process_uuid": record.process_uuid,
            "version": record.version,
            "process_type": record.process_type,
            "name": record.name,
            "content_hash": record.content_hash,
            "snapshot_hash": self.snapshot_hash,
            "snapshot_schema_version": SCHEMA_VERSION,
            "snapshot_state_scope": STATE_SCOPE,
            "source_modified_at": record.source_modified_at,
            "quantitative_reference": record.quantitative_reference,
            "exchanges": list(record.exchanges),
            "included_process_refs": list(record.included_process_refs),
            "input_dependencies": [
                row for row in record.exchanges if row["direction"] == "input"
            ],
            "other_outputs": [
                row
                for row in record.exchanges
                if row["direction"] == "output"
                and row["exchange_internal_id"]
                != record.quantitative_reference["exchange_internal_id"]
            ],
            "dependency_closure_status": "unresolved",
            "unresolved_exact_flow_refs": [
                row["flow"] for row in record.exchanges if row["direction"] == "input"
            ],
            "background_solve_supported": False,
            "calculation_role": "catalog_only",
        }


def _parse_process_record(raw_record: dict[str, Any], *, index: int) -> TidasProcessSnapshotRecord:
    if raw_record.get("dataset_kind") != DATASET_KIND or raw_record.get("source_namespace") != SOURCE_NAMESPACE:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_RECORD_SCOPE_INVALID",
            "A TIDAS Process snapshot record has the wrong dataset kind or source namespace.",
            record_index=index,
        )
    process_uuid = _exact_uuid(raw_record.get("source_object_id"), field="source_object_id")
    version = _exact_version(raw_record.get("source_version"), field="source_version")
    payload = raw_record.get("payload")
    if not isinstance(payload, dict):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_PAYLOAD_INVALID",
            "A TIDAS Process snapshot record has no JSON payload.",
            process_uuid=process_uuid,
            version=version,
        )
    root = payload.get("processDataSet")
    if not isinstance(root, dict):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_PAYLOAD_INVALID",
            "A TIDAS Process snapshot record has no processDataSet root.",
            process_uuid=process_uuid,
            version=version,
        )
    information = root.get("processInformation")
    data_information = information.get("dataSetInformation") if isinstance(information, dict) else None
    payload_uuid = str(data_information.get("common:UUID") if isinstance(data_information, dict) else "").strip()
    if payload_uuid != process_uuid:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_PAYLOAD_IDENTITY_MISMATCH",
            "A TIDAS Process payload UUID does not match its exact snapshot identity.",
            process_uuid=process_uuid,
            version=version,
            payload_uuid=payload_uuid,
        )
    expected_hash = canonical_hash(payload)
    content_hash = str(raw_record.get("content_hash") or "").strip()
    if content_hash != expected_hash:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_RECORD_HASH_MISMATCH",
            "A TIDAS Process snapshot record content hash does not match its canonical payload.",
            process_uuid=process_uuid,
            version=version,
            expected_content_hash=expected_hash,
            actual_content_hash=content_hash,
        )
    modelling = root.get("modellingAndValidation")
    method = modelling.get("LCIMethodAndAllocation") if isinstance(modelling, dict) else None
    process_type = str(method.get("typeOfDataSet") if isinstance(method, dict) else "").strip()
    if not process_type or process_type != str(raw_record.get("type_of_data_set") or "").strip():
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_TYPE_MISMATCH",
            "A TIDAS Process snapshot record type does not match its payload.",
            process_uuid=process_uuid,
            version=version,
        )
    name_block = data_information.get("name") if isinstance(data_information, dict) else None
    name = _localized_text(name_block.get("baseName") if isinstance(name_block, dict) else None)
    exchanges_block = root.get("exchanges")
    raw_exchanges = exchanges_block.get("exchange") if isinstance(exchanges_block, dict) else None
    exchanges = [row for row in _as_list(raw_exchanges) if isinstance(row, dict)]
    if not exchanges:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_EXCHANGES_MISSING",
            "A TIDAS Process snapshot record has no exchanges.",
            process_uuid=process_uuid,
            version=version,
        )
    parsed_exchanges = tuple(_exchange_receipt(row, index=row_index) for row_index, row in enumerate(exchanges))
    ids = [row["exchange_internal_id"] for row in parsed_exchanges]
    if len(ids) != len(set(ids)):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_EXCHANGE_IDENTITY_DUPLICATE",
            "TIDAS Process exchange internal IDs must be unique.",
            process_uuid=process_uuid,
            version=version,
        )
    quantitative = information.get("quantitativeReference") if isinstance(information, dict) else None
    reference_ids = [str(item).strip() for item in _as_list(
        quantitative.get("referenceToReferenceFlow") if isinstance(quantitative, dict) else None
    ) if str(item).strip()]
    if len(reference_ids) != 1:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_QUANTITATIVE_REFERENCE_AMBIGUOUS",
            "A TIDAS Process must identify exactly one quantitative-reference exchange.",
            process_uuid=process_uuid,
            version=version,
            reference_ids=reference_ids,
        )
    matches = [row for row in parsed_exchanges if row["exchange_internal_id"] == reference_ids[0]]
    if len(matches) != 1:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_QUANTITATIVE_REFERENCE_AMBIGUOUS",
            "The quantitative-reference internal ID must select exactly one exchange.",
            process_uuid=process_uuid,
            version=version,
            reference_internal_id=reference_ids[0],
            match_count=len(matches),
        )
    qref = dict(matches[0])
    if qref["direction"] != "output":
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_QUANTITATIVE_REFERENCE_NOT_OUTPUT",
            "The TIDAS Process quantitative reference must be an output exchange.",
            process_uuid=process_uuid,
            version=version,
            reference_internal_id=reference_ids[0],
        )
    _finite_number(qref["amount"], field="quantitative_reference.amount", positive=True)
    qref["functional_unit_text"] = _localized_text(
        quantitative.get("functionalUnitOrOther") if isinstance(quantitative, dict) else None
    )
    qref["quantitative_reference_type"] = str(
        quantitative.get("@type") if isinstance(quantitative, dict) else ""
    ).strip() or None
    technology = information.get("technology") if isinstance(information, dict) else None
    included_raw = (
        technology.get("referenceToIncludedProcesses")
        if isinstance(technology, dict)
        else None
    )
    included_process_refs = tuple(
        {
            "source_namespace": SOURCE_NAMESPACE,
            "process_uuid": _exact_uuid(
                row.get("@refObjectId") or row.get("refObjectId"),
                field="technology.referenceToIncludedProcesses.@refObjectId",
            ),
            "version": _exact_version(
                row.get("@version") or row.get("version"),
                field="technology.referenceToIncludedProcesses.@version",
            ),
        }
        for row in _as_list(included_raw)
        if isinstance(row, dict)
    )
    return TidasProcessSnapshotRecord(
        process_uuid=process_uuid,
        version=version,
        process_type=process_type,
        name=name,
        quantitative_reference=qref,
        exchanges=parsed_exchanges,
        included_process_refs=included_process_refs,
        content_hash=content_hash,
        source_modified_at=str(raw_record.get("source_modified_at") or "").strip() or None,
    )


def _load_snapshot(path: Path) -> TidasProcessSnapshot:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_UNREADABLE",
            "The configured TIDAS Process snapshot cannot be read as JSON.",
        ) from exc
    if not isinstance(document, dict):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_INVALID",
            "The configured TIDAS Process snapshot must be a JSON object.",
        )
    expected_header = {
        "schema_version": SCHEMA_VERSION,
        "source_namespace": SOURCE_NAMESPACE,
        "dataset_kind": DATASET_KIND,
        "state_scope": STATE_SCOPE,
    }
    actual_header = {key: document.get(key) for key in expected_header}
    if actual_header != expected_header:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_SCOPE_INVALID",
            "The configured snapshot is not an open TianGong Process dataset snapshot.",
            expected=expected_header,
            actual=actual_header,
        )
    declared_total = document.get("declared_total")
    records_raw = document.get("records")
    if (
        not isinstance(document.get("filters"), dict)
        or not isinstance(records_raw, list)
        or not records_raw
        or "declared_total" not in document
        or (declared_total is not None and (isinstance(declared_total, bool) or not isinstance(declared_total, int)))
    ):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_INVALID",
            "The configured TIDAS Process snapshot lacks deterministic filters or records.",
        )
    body = {field: document.get(field) for field in SNAPSHOT_BODY_FIELDS}
    expected_hash = canonical_hash(body)
    snapshot_hash = str(document.get("snapshot_hash") or "").strip()
    if snapshot_hash != expected_hash:
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_HASH_MISMATCH",
            "The configured TIDAS Process snapshot hash does not match its canonical body.",
            expected_snapshot_hash=expected_hash,
            actual_snapshot_hash=snapshot_hash,
        )
    records: dict[tuple[str, str], TidasProcessSnapshotRecord] = {}
    for index, raw_record in enumerate(records_raw):
        if not isinstance(raw_record, dict):
            raise ProviderTidasProcessSnapshotError(
                "TIDAS_PROCESS_SNAPSHOT_RECORD_INVALID",
                "A TIDAS Process snapshot record is not a JSON object.",
                record_index=index,
            )
        parsed = _parse_process_record(raw_record, index=index)
        identity = (parsed.process_uuid, parsed.version)
        if identity in records:
            raise ProviderTidasProcessSnapshotError(
                "TIDAS_PROCESS_SNAPSHOT_IDENTITY_DUPLICATE",
                "The configured TIDAS Process snapshot contains a duplicate exact identity.",
                process_uuid=parsed.process_uuid,
                version=parsed.version,
            )
        records[identity] = parsed
    if list(records) != sorted(records):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_ORDER_INVALID",
            "The configured TIDAS Process snapshot records are not in canonical exact-identity order.",
        )
    counts = document.get("counts")
    if not isinstance(counts, dict) or counts.get(DATASET_KIND) != len(records):
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_COUNT_MISMATCH",
            "The configured TIDAS Process snapshot count does not match its records.",
            declared_count=counts.get(DATASET_KIND) if isinstance(counts, dict) else None,
            actual_count=len(records),
        )
    return TidasProcessSnapshot(snapshot_hash=snapshot_hash, records=records)


def configured_tidas_process_snapshot() -> TidasProcessSnapshot | None:
    configured_path = str(settings.provider_tidas_process_snapshot_path or "").strip()
    if not configured_path:
        return None
    path = Path(configured_path)
    if not path.is_file():
        raise ProviderTidasProcessSnapshotError(
            "TIDAS_PROCESS_SNAPSHOT_FILE_NOT_FOUND",
            "The configured TIDAS Process snapshot file does not exist.",
        )
    return _load_snapshot(path)
