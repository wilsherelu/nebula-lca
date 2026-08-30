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


SCHEMA_VERSION = "tiangong-exact-reference-dependency.v1"
SOURCE_NAMESPACE = "tiangong_open_data"
STATE_SCOPE = "open"
DATASET_KINDS = {"flow_property", "unit_group"}
VERSION_RE = re.compile(r"^\d{2}\.\d{2}\.\d{3}$")


class ProviderTidasReferenceSnapshotError(RuntimeError):
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
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_IDENTITY_INVALID",
            "The reference dependency snapshot contains an invalid UUID.",
            field=field,
            value=raw,
        ) from exc
    if raw != normalized:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_IDENTITY_NONCANONICAL",
            "Reference dependency UUIDs must use canonical lowercase form.",
            field=field,
            value=raw,
        )
    return normalized


def _exact_version(value: Any, *, field: str) -> str:
    version = str(value or "").strip()
    if not VERSION_RE.fullmatch(version):
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_VERSION_INVALID",
            "The reference dependency snapshot contains an invalid exact version.",
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


def _finite_positive(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_UNIT_FACTOR_INVALID",
            "Every Unit Group conversion factor must be finite and positive.",
            field=field,
            value=value,
        ) from exc
    if not math.isfinite(number) or number <= 0:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_UNIT_FACTOR_INVALID",
            "Every Unit Group conversion factor must be finite and positive.",
            field=field,
            value=value,
        )
    return number


@dataclass(frozen=True)
class TidasFlowPropertyRecord:
    flow_property_uuid: str
    version: str
    name: str
    unit_group_uuid: str
    unit_group_version: str
    content_hash: str
    source_modified_at: str | None


@dataclass(frozen=True)
class TidasUnitGroupRecord:
    unit_group_uuid: str
    version: str
    name: str
    reference_unit_internal_id: str
    reference_unit: str
    units: tuple[dict[str, Any], ...]
    content_hash: str
    source_modified_at: str | None


@dataclass(frozen=True)
class TidasReferenceDependencySnapshot:
    snapshot_hash: str
    flow_properties: dict[tuple[str, str], TidasFlowPropertyRecord]
    unit_groups: dict[tuple[str, str], TidasUnitGroupRecord]

    def resolve_flow_property(self, flow_property_uuid: str, version: str) -> dict[str, Any] | None:
        record = self.flow_properties.get((flow_property_uuid, version))
        if record is None:
            return None
        return {
            "resolution_source": "tidas_exact_reference_snapshot",
            "source_namespace": SOURCE_NAMESPACE,
            "flow_property_uuid": record.flow_property_uuid,
            "version": record.version,
            "name": record.name,
            "reference_unit_group_uuid": record.unit_group_uuid,
            "reference_unit_group_version": record.unit_group_version,
            "content_hash": record.content_hash,
            "snapshot_hash": self.snapshot_hash,
            "snapshot_schema_version": SCHEMA_VERSION,
            "snapshot_state_scope": STATE_SCOPE,
            "source_modified_at": record.source_modified_at,
        }

    def resolve_unit_group(self, unit_group_uuid: str, version: str) -> dict[str, Any] | None:
        record = self.unit_groups.get((unit_group_uuid, version))
        if record is None:
            return None
        return {
            "resolution_source": "tidas_exact_reference_snapshot",
            "source_namespace": SOURCE_NAMESPACE,
            "unit_group_uuid": record.unit_group_uuid,
            "version": record.version,
            "name": record.name,
            "reference_unit_internal_id": record.reference_unit_internal_id,
            "reference_unit": record.reference_unit,
            "units": list(record.units),
            "content_hash": record.content_hash,
            "snapshot_hash": self.snapshot_hash,
            "snapshot_schema_version": SCHEMA_VERSION,
            "snapshot_state_scope": STATE_SCOPE,
            "source_modified_at": record.source_modified_at,
        }

    def resolve_unit(self, unit_group_uuid: str, version: str, unit_name: str) -> dict[str, Any] | None:
        group = self.resolve_unit_group(unit_group_uuid, version)
        if group is None:
            return None
        matches = [row for row in group["units"] if row["unit"] == unit_name]
        if len(matches) != 1:
            return None
        unit = matches[0]
        value = {
            "resolution_source": "tidas_exact_reference_snapshot",
            "source_namespace": SOURCE_NAMESPACE,
            "unit_group_uuid": unit_group_uuid,
            "unit_group_version": version,
            "unit_group": group["name"],
            **unit,
            "unit_group_content_hash": group["content_hash"],
            "snapshot_hash": self.snapshot_hash,
        }
        value["content_hash"] = canonical_hash(
            {
                "unit_group_uuid": unit_group_uuid,
                "unit_group_version": version,
                **unit,
            }
        )
        return value

    def resolve_flow_property_binding(
        self,
        flow_property_uuid: str,
        version: str,
    ) -> dict[str, Any] | None:
        flow_property = self.resolve_flow_property(flow_property_uuid, version)
        if flow_property is None:
            return None
        unit_group = self.resolve_unit_group(
            flow_property["reference_unit_group_uuid"],
            flow_property["reference_unit_group_version"],
        )
        if unit_group is None:
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_UNIT_GROUP_NOT_IN_SNAPSHOT",
                "The exact Unit Group referenced by the Flow Property is absent from the snapshot.",
                flow_property_uuid=flow_property_uuid,
                flow_property_version=version,
                unit_group_uuid=flow_property["reference_unit_group_uuid"],
                unit_group_version=flow_property["reference_unit_group_version"],
            )
        unit = self.resolve_unit(
            unit_group["unit_group_uuid"],
            unit_group["version"],
            unit_group["reference_unit"],
        )
        if unit is None:
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_UNIT_GROUP_REFERENCE_UNIT_UNAVAILABLE",
                "The exact Unit Group has no uniquely resolvable reference unit.",
                unit_group_uuid=unit_group["unit_group_uuid"],
                unit_group_version=unit_group["version"],
            )
        return {
            "flow_property": flow_property,
            "unit_group": unit_group,
            "unit": unit,
            "resolution_source": "tidas_exact_reference_snapshot",
            "snapshot_hash": self.snapshot_hash,
        }


def _validate_record_envelope(raw: dict[str, Any], *, index: int) -> tuple[str, str, str, dict[str, Any], str]:
    kind = str(raw.get("dataset_kind") or "").strip()
    if kind not in DATASET_KINDS or raw.get("source_namespace") != SOURCE_NAMESPACE:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_RECORD_SCOPE_INVALID",
            "A reference dependency record has the wrong dataset kind or source namespace.",
            record_index=index,
        )
    state_code = raw.get("state_code")
    if isinstance(state_code, bool) or not isinstance(state_code, int) or state_code < 100:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_RECORD_STATE_INVALID",
            "Every reference dependency record must be an open TianGong record.",
            record_index=index,
            state_code=state_code,
        )
    object_uuid = _exact_uuid(raw.get("source_object_id"), field="source_object_id")
    version = _exact_version(raw.get("source_version"), field="source_version")
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_PAYLOAD_INVALID",
            "A reference dependency record has no JSON payload.",
            dataset_kind=kind,
            source_object_id=object_uuid,
            source_version=version,
        )
    expected_hash = canonical_hash(payload)
    content_hash = str(raw.get("content_hash") or "").strip()
    if content_hash != expected_hash:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_RECORD_HASH_MISMATCH",
            "A reference dependency record content hash does not match its canonical payload.",
            dataset_kind=kind,
            source_object_id=object_uuid,
            source_version=version,
            expected_content_hash=expected_hash,
            actual_content_hash=content_hash,
        )
    return kind, object_uuid, version, payload, content_hash


def _parse_flow_property(
    object_uuid: str,
    version: str,
    payload: dict[str, Any],
    content_hash: str,
    source_modified_at: str | None,
) -> TidasFlowPropertyRecord:
    root = payload.get("flowPropertyDataSet")
    information = root.get("flowPropertiesInformation") if isinstance(root, dict) else None
    data_information = information.get("dataSetInformation") if isinstance(information, dict) else None
    payload_uuid = str(data_information.get("common:UUID") if isinstance(data_information, dict) else "").strip()
    if payload_uuid != object_uuid:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_PAYLOAD_IDENTITY_MISMATCH",
            "The Flow Property payload UUID does not match its snapshot identity.",
            source_object_id=object_uuid,
            payload_uuid=payload_uuid,
        )
    quantitative = information.get("quantitativeReference") if isinstance(information, dict) else None
    reference = quantitative.get("referenceToReferenceUnitGroup") if isinstance(quantitative, dict) else None
    if not isinstance(reference, dict):
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_FLOW_PROPERTY_UNIT_GROUP_MISSING",
            "The exact Flow Property has no exact reference Unit Group.",
            flow_property_uuid=object_uuid,
            version=version,
        )
    unit_group_uuid = _exact_uuid(
        reference.get("@refObjectId") or reference.get("refObjectId"),
        field="referenceToReferenceUnitGroup.@refObjectId",
    )
    unit_group_version = _exact_version(
        reference.get("@version") or reference.get("version"),
        field="referenceToReferenceUnitGroup.@version",
    )
    name = _localized_text(data_information.get("common:name") if isinstance(data_information, dict) else None)
    if not name:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_NAME_MISSING",
            "The exact Flow Property has no verifiable name.",
            flow_property_uuid=object_uuid,
            version=version,
        )
    return TidasFlowPropertyRecord(
        flow_property_uuid=object_uuid,
        version=version,
        name=name,
        unit_group_uuid=unit_group_uuid,
        unit_group_version=unit_group_version,
        content_hash=content_hash,
        source_modified_at=source_modified_at,
    )


def _parse_unit_group(
    object_uuid: str,
    version: str,
    payload: dict[str, Any],
    content_hash: str,
    source_modified_at: str | None,
) -> TidasUnitGroupRecord:
    root = payload.get("unitGroupDataSet")
    information = root.get("unitGroupInformation") if isinstance(root, dict) else None
    data_information = information.get("dataSetInformation") if isinstance(information, dict) else None
    payload_uuid = str(data_information.get("common:UUID") if isinstance(data_information, dict) else "").strip()
    if payload_uuid != object_uuid:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_PAYLOAD_IDENTITY_MISMATCH",
            "The Unit Group payload UUID does not match its snapshot identity.",
            source_object_id=object_uuid,
            payload_uuid=payload_uuid,
        )
    quantitative = information.get("quantitativeReference") if isinstance(information, dict) else None
    reference_ids = [
        str(item).strip()
        for item in _as_list(
            quantitative.get("referenceToReferenceUnit") if isinstance(quantitative, dict) else None
        )
        if str(item).strip()
    ]
    if len(reference_ids) != 1:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_UNIT_GROUP_REFERENCE_UNIT_AMBIGUOUS",
            "The exact Unit Group must select exactly one reference unit internal ID.",
            unit_group_uuid=object_uuid,
            version=version,
            reference_ids=reference_ids,
        )
    units_block = root.get("units") if isinstance(root, dict) else None
    raw_units = units_block.get("unit") if isinstance(units_block, dict) else None
    units: list[dict[str, Any]] = []
    internal_ids: set[str] = set()
    names: set[str] = set()
    for index, raw_unit in enumerate(_as_list(raw_units)):
        if not isinstance(raw_unit, dict):
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_UNIT_INVALID",
                "Every Unit Group unit must be a JSON object.",
                unit_group_uuid=object_uuid,
                unit_index=index,
            )
        internal_id = str(raw_unit.get("@dataSetInternalID") or "").strip()
        name = str(raw_unit.get("name") or "").strip()
        if not internal_id or not name:
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_UNIT_IDENTITY_INCOMPLETE",
                "Every Unit Group unit must have an internal ID and exact name.",
                unit_group_uuid=object_uuid,
                unit_index=index,
            )
        if internal_id in internal_ids or name.casefold() in names:
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_UNIT_DUPLICATE",
                "Unit Group unit internal IDs and names must be unique.",
                unit_group_uuid=object_uuid,
                unit_internal_id=internal_id,
                unit=name,
            )
        internal_ids.add(internal_id)
        names.add(name.casefold())
        units.append(
            {
                "unit_internal_id": internal_id,
                "unit": name,
                "factor_to_reference": _finite_positive(
                    raw_unit.get("meanValue"),
                    field=f"units[{index}].meanValue",
                ),
                "is_reference": internal_id == reference_ids[0],
            }
        )
    reference_units = [row for row in units if row["is_reference"]]
    if len(reference_units) != 1:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_UNIT_GROUP_REFERENCE_UNIT_AMBIGUOUS",
            "The Unit Group reference internal ID must select exactly one unit.",
            unit_group_uuid=object_uuid,
            version=version,
            reference_internal_id=reference_ids[0],
            match_count=len(reference_units),
        )
    if reference_units[0]["factor_to_reference"] != 1.0:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_UNIT_GROUP_REFERENCE_FACTOR_INVALID",
            "The Unit Group reference unit conversion factor must be exactly 1.",
            unit_group_uuid=object_uuid,
            version=version,
            reference_unit=reference_units[0]["unit"],
            factor=reference_units[0]["factor_to_reference"],
        )
    name = _localized_text(data_information.get("common:name") if isinstance(data_information, dict) else None)
    if not name:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_NAME_MISSING",
            "The exact Unit Group has no verifiable name.",
            unit_group_uuid=object_uuid,
            version=version,
        )
    return TidasUnitGroupRecord(
        unit_group_uuid=object_uuid,
        version=version,
        name=name,
        reference_unit_internal_id=reference_ids[0],
        reference_unit=reference_units[0]["unit"],
        units=tuple(units),
        content_hash=content_hash,
        source_modified_at=source_modified_at,
    )


def _load_snapshot(path: Path) -> TidasReferenceDependencySnapshot:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_SNAPSHOT_UNREADABLE",
            "The configured TIDAS reference dependency snapshot cannot be read as JSON.",
        ) from exc
    if not isinstance(document, dict):
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_SNAPSHOT_INVALID",
            "The configured TIDAS reference dependency snapshot must be a JSON object.",
        )
    if document.get("schema_version") != SCHEMA_VERSION or document.get("state_scope") != STATE_SCOPE:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_SNAPSHOT_SCOPE_INVALID",
            "The configured dependency snapshot is not an exact open TianGong reference snapshot.",
        )
    raw_records = document.get("records")
    if not isinstance(raw_records, list) or not raw_records:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_SNAPSHOT_EMPTY",
            "The configured TIDAS reference dependency snapshot contains no records.",
        )
    expected_snapshot_hash = canonical_hash(
        {key: value for key, value in document.items() if key != "snapshot_hash"}
    )
    snapshot_hash = str(document.get("snapshot_hash") or "").strip()
    if snapshot_hash != expected_snapshot_hash:
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_SNAPSHOT_HASH_MISMATCH",
            "The configured reference dependency snapshot hash does not match its canonical body.",
            expected_snapshot_hash=expected_snapshot_hash,
            actual_snapshot_hash=snapshot_hash,
        )
    flow_properties: dict[tuple[str, str], TidasFlowPropertyRecord] = {}
    unit_groups: dict[tuple[str, str], TidasUnitGroupRecord] = {}
    identities: list[tuple[str, str, str]] = []
    for index, raw in enumerate(raw_records):
        if not isinstance(raw, dict):
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_RECORD_INVALID",
                "A reference dependency snapshot record is not a JSON object.",
                record_index=index,
            )
        kind, object_uuid, version, payload, content_hash = _validate_record_envelope(raw, index=index)
        identity = (kind, object_uuid, version)
        if identity in identities:
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_IDENTITY_DUPLICATE",
                "The reference dependency snapshot contains a duplicate exact identity.",
                dataset_kind=kind,
                source_object_id=object_uuid,
                source_version=version,
            )
        identities.append(identity)
        source_modified_at = str(raw.get("source_modified_at") or "").strip() or None
        if kind == "flow_property":
            flow_properties[(object_uuid, version)] = _parse_flow_property(
                object_uuid,
                version,
                payload,
                content_hash,
                source_modified_at,
            )
        else:
            unit_groups[(object_uuid, version)] = _parse_unit_group(
                object_uuid,
                version,
                payload,
                content_hash,
                source_modified_at,
            )
    if identities != sorted(identities):
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_SNAPSHOT_ORDER_INVALID",
            "Reference dependency records are not in canonical exact-identity order.",
        )
    for record in flow_properties.values():
        if (record.unit_group_uuid, record.unit_group_version) not in unit_groups:
            raise ProviderTidasReferenceSnapshotError(
                "TIDAS_REFERENCE_UNIT_GROUP_NOT_IN_SNAPSHOT",
                "An exact Flow Property references a Unit Group absent from the same snapshot.",
                flow_property_uuid=record.flow_property_uuid,
                flow_property_version=record.version,
                unit_group_uuid=record.unit_group_uuid,
                unit_group_version=record.unit_group_version,
            )
    return TidasReferenceDependencySnapshot(
        snapshot_hash=snapshot_hash,
        flow_properties=flow_properties,
        unit_groups=unit_groups,
    )


def configured_tidas_reference_dependency_snapshot() -> TidasReferenceDependencySnapshot | None:
    configured_path = str(settings.provider_tidas_reference_dependency_snapshot_path or "").strip()
    if not configured_path:
        return None
    path = Path(configured_path)
    if not path.is_file():
        raise ProviderTidasReferenceSnapshotError(
            "TIDAS_REFERENCE_SNAPSHOT_FILE_NOT_FOUND",
            "The configured TIDAS reference dependency snapshot file does not exist.",
        )
    return _load_snapshot(path)
