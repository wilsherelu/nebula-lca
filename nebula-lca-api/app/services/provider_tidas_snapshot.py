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
from ..provider_schemas import ExactFlowPropertyRef
from .provider_ef31 import resolve_standard_flow_property_binding


SCHEMA_VERSION = "tiangong-open-dataset-snapshot.v1"
SOURCE_NAMESPACE = "tiangong_open_data"
DATASET_KIND = "flow"
STATE_SCOPE = "open"
VERSION_RE = re.compile(r"^\d{2}\.\d{2}\.\d{3}$")
TECHNOSPHERE_FLOW_TYPES = {"Product flow", "Waste flow"}
SNAPSHOT_BODY_FIELDS = (
    "schema_version",
    "source_namespace",
    "dataset_kind",
    "state_scope",
    "filters",
    "declared_total",
    "records",
)


class ProviderTidasSnapshotError(RuntimeError):
    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _exact_uuid(value: Any, *, field: str) -> str:
    raw = str(value or "").strip()
    try:
        normalized = str(uuid.UUID(raw))
    except ValueError as exc:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_IDENTITY_INVALID",
            "The configured TIDAS Flow snapshot contains an invalid UUID.",
            field=field,
            value=raw,
        ) from exc
    if raw != normalized:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_IDENTITY_NONCANONICAL",
            "TIDAS Flow snapshot UUIDs must use canonical lowercase form.",
            field=field,
            value=raw,
        )
    return normalized


def _exact_version(value: Any, *, field: str) -> str:
    version = str(value or "").strip()
    if not VERSION_RE.fullmatch(version):
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_VERSION_INVALID",
            "The configured TIDAS Flow snapshot contains an invalid exact version.",
            field=field,
            value=version,
        )
    return version


def _extract_reference_flow_property(payload: dict[str, Any]) -> tuple[str, str, float]:
    flow_dataset = payload.get("flowDataSet")
    if not isinstance(flow_dataset, dict):
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_PAYLOAD_INVALID",
            "A TIDAS Flow snapshot record has no flowDataSet root.",
        )
    information = flow_dataset.get("flowInformation")
    quantitative = information.get("quantitativeReference") if isinstance(information, dict) else None
    internal_id = str(
        quantitative.get("referenceToReferenceFlowProperty")
        if isinstance(quantitative, dict)
        else ""
    ).strip()
    properties = flow_dataset.get("flowProperties")
    raw_properties = properties.get("flowProperty") if isinstance(properties, dict) else None
    rows = [row for row in _as_list(raw_properties) if isinstance(row, dict)]
    matches = [
        row
        for row in rows
        if str(row.get("@dataSetInternalID") or row.get("dataSetInternalID") or "").strip()
        == internal_id
    ]
    if not internal_id or len(matches) != 1:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_REFERENCE_PROPERTY_AMBIGUOUS",
            "A TIDAS Flow must select exactly one reference Flow Property by internal ID.",
            reference_internal_id=internal_id,
            match_count=len(matches),
        )
    row = matches[0]
    reference = row.get("referenceToFlowPropertyDataSet")
    if not isinstance(reference, dict):
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_PROPERTY_REFERENCE_MISSING",
            "The selected TIDAS reference Flow Property has no exact dataset reference.",
        )
    flow_property_uuid = _exact_uuid(
        reference.get("@refObjectId") or reference.get("refObjectId"),
        field="referenceToFlowPropertyDataSet.@refObjectId",
    )
    flow_property_version = _exact_version(
        reference.get("@version") or reference.get("version"),
        field="referenceToFlowPropertyDataSet.@version",
    )
    try:
        mean_value = float(row.get("meanValue"))
    except (TypeError, ValueError) as exc:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_PROPERTY_VALUE_INVALID",
            "The selected TIDAS reference Flow Property has no finite positive mean value.",
        ) from exc
    if not math.isfinite(mean_value) or mean_value <= 0:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_PROPERTY_VALUE_INVALID",
            "The selected TIDAS reference Flow Property has no finite positive mean value.",
            mean_value=mean_value,
        )
    return flow_property_uuid, flow_property_version, mean_value


def _flow_type(payload: dict[str, Any]) -> str:
    root = payload.get("flowDataSet")
    modelling = root.get("modellingAndValidation") if isinstance(root, dict) else None
    method = modelling.get("LCIMethod") if isinstance(modelling, dict) else None
    return str(method.get("typeOfDataSet") if isinstance(method, dict) else "").strip()


@dataclass(frozen=True)
class TidasFlowSnapshotRecord:
    source_namespace: str
    flow_uuid: str
    version: str
    flow_type: str
    flow_property_uuid: str
    flow_property_version: str
    flow_property_mean_value: float
    content_hash: str
    source_modified_at: str | None


@dataclass(frozen=True)
class TidasFlowSnapshot:
    snapshot_hash: str
    records: dict[tuple[str, str], TidasFlowSnapshotRecord]

    def resolve(self, flow_uuid: str, version: str) -> dict[str, Any] | None:
        record = self.records.get((flow_uuid, version))
        if record is None:
            return None
        binding = resolve_standard_flow_property_binding(
            ExactFlowPropertyRef(
                flow_property_uuid=record.flow_property_uuid,
                version=record.flow_property_version,
            )
        )
        if binding is None:
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_PROPERTY_DEPENDENCY_UNAVAILABLE",
                "The exact TIDAS Flow Property cannot be bound to a complete provider Unit Group dependency.",
                flow_uuid=record.flow_uuid,
                version=record.version,
                flow_property_uuid=record.flow_property_uuid,
                flow_property_version=record.flow_property_version,
            )
        flow_property = binding["flow_property"]
        unit_group = binding["unit_group"]
        unit = binding["unit"]
        return {
            "resolution_source": "tidas_exact_snapshot",
            "source_namespace": record.source_namespace,
            "flow_uuid": record.flow_uuid,
            "version": record.version,
            "flow_type": record.flow_type,
            "flow_property_uuid": record.flow_property_uuid,
            "flow_property_version": record.flow_property_version,
            "flow_property_mean_value": record.flow_property_mean_value,
            "flow_property_content_hash": flow_property["content_hash"],
            "unit_group_uuid": unit_group["unit_group_uuid"],
            "unit_group_version": unit_group["version"],
            "unit_group": unit_group["name"],
            "unit_group_content_hash": unit_group["content_hash"],
            "default_unit": unit["unit"],
            "unit_content_hash": unit["content_hash"],
            "content_hash": record.content_hash,
            "snapshot_hash": self.snapshot_hash,
            "snapshot_schema_version": SCHEMA_VERSION,
            "snapshot_state_scope": STATE_SCOPE,
            "source_modified_at": record.source_modified_at,
        }


def _load_snapshot(path: Path) -> TidasFlowSnapshot:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_UNREADABLE",
            "The configured TIDAS Flow snapshot cannot be read as JSON.",
        ) from exc
    if not isinstance(document, dict):
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_INVALID",
            "The configured TIDAS Flow snapshot must be a JSON object.",
        )
    expected_header = {
        "schema_version": SCHEMA_VERSION,
        "source_namespace": SOURCE_NAMESPACE,
        "dataset_kind": DATASET_KIND,
        "state_scope": STATE_SCOPE,
    }
    actual_header = {key: document.get(key) for key in expected_header}
    if actual_header != expected_header:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_SCOPE_INVALID",
            "The configured snapshot is not an open TianGong Flow dataset snapshot.",
            expected=expected_header,
            actual=actual_header,
        )
    declared_total = document.get("declared_total")
    if (
        not isinstance(document.get("filters"), dict)
        or not isinstance(document.get("records"), list)
        or "declared_total" not in document
        or (declared_total is not None and (isinstance(declared_total, bool) or not isinstance(declared_total, int)))
    ):
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_INVALID",
            "The configured TIDAS Flow snapshot lacks deterministic filters or records.",
        )
    if not document["records"]:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_EMPTY",
            "The configured TIDAS Flow snapshot contains no exact records.",
        )
    body = {field: document.get(field) for field in SNAPSHOT_BODY_FIELDS}
    expected_snapshot_hash = _sha256(body)
    actual_snapshot_hash = str(document.get("snapshot_hash") or "").strip()
    if actual_snapshot_hash != expected_snapshot_hash:
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_HASH_MISMATCH",
            "The configured TIDAS Flow snapshot hash does not match its canonical body.",
            expected_snapshot_hash=expected_snapshot_hash,
            actual_snapshot_hash=actual_snapshot_hash,
        )

    records: dict[tuple[str, str], TidasFlowSnapshotRecord] = {}
    for index, raw_record in enumerate(document["records"]):
        if not isinstance(raw_record, dict):
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_SNAPSHOT_RECORD_INVALID",
                "A TIDAS Flow snapshot record is not a JSON object.",
                record_index=index,
            )
        if raw_record.get("dataset_kind") != DATASET_KIND or raw_record.get("source_namespace") != SOURCE_NAMESPACE:
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_SNAPSHOT_RECORD_SCOPE_INVALID",
                "A TIDAS Flow snapshot record has the wrong dataset kind or source namespace.",
                record_index=index,
            )
        flow_uuid = _exact_uuid(raw_record.get("source_object_id"), field="source_object_id")
        version = _exact_version(raw_record.get("source_version"), field="source_version")
        identity = (flow_uuid, version)
        if identity in records:
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_SNAPSHOT_IDENTITY_DUPLICATE",
                "The configured TIDAS Flow snapshot contains a duplicate exact identity.",
                flow_uuid=flow_uuid,
                version=version,
            )
        payload = raw_record.get("payload")
        if not isinstance(payload, dict):
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_PAYLOAD_INVALID",
                "A TIDAS Flow snapshot record has no JSON payload.",
                flow_uuid=flow_uuid,
                version=version,
            )
        flow_dataset = payload.get("flowDataSet")
        flow_information = flow_dataset.get("flowInformation") if isinstance(flow_dataset, dict) else None
        data_information = (
            flow_information.get("dataSetInformation")
            if isinstance(flow_information, dict)
            else None
        )
        payload_uuid = str(
            data_information.get("common:UUID")
            if isinstance(data_information, dict)
            else ""
        ).strip()
        if payload_uuid and payload_uuid != flow_uuid:
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_PAYLOAD_IDENTITY_MISMATCH",
                "A TIDAS Flow payload UUID does not match its exact snapshot identity.",
                flow_uuid=flow_uuid,
                version=version,
                payload_uuid=payload_uuid,
            )
        expected_content_hash = _sha256(payload)
        actual_content_hash = str(raw_record.get("content_hash") or "").strip()
        if actual_content_hash != expected_content_hash:
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_RECORD_HASH_MISMATCH",
                "A TIDAS Flow snapshot record content hash does not match its canonical payload.",
                flow_uuid=flow_uuid,
                version=version,
                expected_content_hash=expected_content_hash,
                actual_content_hash=actual_content_hash,
            )
        flow_type = _flow_type(payload)
        if flow_type != str(raw_record.get("type_of_data_set") or "").strip():
            raise ProviderTidasSnapshotError(
                "TIDAS_FLOW_TYPE_MISMATCH",
                "A TIDAS Flow snapshot record type does not match its payload.",
                flow_uuid=flow_uuid,
                version=version,
            )
        if flow_type not in TECHNOSPHERE_FLOW_TYPES:
            raise ProviderTidasSnapshotError(
                "TIDAS_TECHNOSPHERE_FLOW_TYPE_UNSUPPORTED",
                "Provider technosphere resolution accepts only exact Product or Waste Flows.",
                flow_uuid=flow_uuid,
                version=version,
                flow_type=flow_type,
            )
        flow_property_uuid, flow_property_version, mean_value = _extract_reference_flow_property(payload)
        records[identity] = TidasFlowSnapshotRecord(
            source_namespace=SOURCE_NAMESPACE,
            flow_uuid=flow_uuid,
            version=version,
            flow_type=flow_type,
            flow_property_uuid=flow_property_uuid,
            flow_property_version=flow_property_version,
            flow_property_mean_value=mean_value,
            content_hash=actual_content_hash,
            source_modified_at=(
                str(raw_record.get("source_modified_at") or "").strip() or None
            ),
        )
    identities = list(records)
    if identities != sorted(identities):
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_ORDER_INVALID",
            "The configured TIDAS Flow snapshot records are not in canonical exact-identity order.",
        )
    counts = document.get("counts")
    if not isinstance(counts, dict) or counts.get(DATASET_KIND) != len(records):
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_COUNT_MISMATCH",
            "The configured TIDAS Flow snapshot count does not match its records.",
            declared_count=counts.get(DATASET_KIND) if isinstance(counts, dict) else None,
            actual_count=len(records),
        )
    return TidasFlowSnapshot(snapshot_hash=actual_snapshot_hash, records=records)


def configured_tidas_flow_snapshot() -> TidasFlowSnapshot | None:
    configured_path = str(settings.provider_tidas_flow_snapshot_path or "").strip()
    if not configured_path:
        return None
    path = Path(configured_path)
    if not path.is_file():
        raise ProviderTidasSnapshotError(
            "TIDAS_FLOW_SNAPSHOT_FILE_NOT_FOUND",
            "The configured TIDAS Flow snapshot file does not exist.",
        )
    return _load_snapshot(path)
