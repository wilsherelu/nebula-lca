from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy.orm import Session

from ..models import FlowRecord, FlowVersionRecord


TG_LEGACY_NAMESPACE = "tiangong_open_source"
TG_LEGACY_VERSION = "TG-1.0"
TG_LEGACY_LABEL = "TG 1.0"
TIDAS_NAMESPACE = "tiangong_open_data"


def normalized_flow_identity(
    *,
    source_namespace: str | None,
    source_version: str | None,
) -> tuple[str, str]:
    namespace = str(source_namespace or "").strip()
    version = str(source_version or "").strip()
    if not namespace and not version:
        return TG_LEGACY_NAMESPACE, TG_LEGACY_VERSION
    if not namespace:
        namespace = TIDAS_NAMESPACE
    if not version:
        version = TG_LEGACY_VERSION if namespace == TG_LEGACY_NAMESPACE else ""
    return namespace, version


def port_flow_identity(port: Any) -> tuple[str, str, str, str]:
    def value(snake: str, camel: str) -> str:
        if isinstance(port, dict):
            return str(port.get(camel) or port.get(snake) or "").strip()
        return str(getattr(port, snake, None) or getattr(port, camel, None) or "").strip()

    namespace, version = normalized_flow_identity(
        source_namespace=value("flow_source_namespace", "flowSourceNamespace"),
        source_version=value("flow_version", "flowVersion"),
    )
    return value("flow_uuid", "flowUuid"), namespace, version, value("unit_group", "unitGroup")


def ports_are_version_compatible(source: Any, target: Any) -> bool:
    return port_flow_identity(source)[:3] == port_flow_identity(target)[:3]


def flow_version_label(source_namespace: str, source_version: str) -> str:
    if source_namespace == TG_LEGACY_NAMESPACE and source_version == TG_LEGACY_VERSION:
        return TG_LEGACY_LABEL
    return f"TIDAS {source_version}"


def flow_snapshot_content_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def get_flow_version(
    db: Session,
    *,
    flow_uuid: str,
    source_namespace: str | None,
    source_version: str | None,
) -> FlowVersionRecord | None:
    namespace, version = normalized_flow_identity(
        source_namespace=source_namespace,
        source_version=source_version,
    )
    if not flow_uuid or not version:
        return None
    return (
        db.query(FlowVersionRecord)
        .filter(
            FlowVersionRecord.source_namespace == namespace,
            FlowVersionRecord.flow_uuid == flow_uuid,
            FlowVersionRecord.source_version == version,
        )
        .one_or_none()
    )


def create_flow_version_snapshot(
    db: Session,
    *,
    flow_record: dict[str, Any],
    source_namespace: str,
    source_version: str,
    metadata: dict[str, Any] | None = None,
) -> tuple[FlowVersionRecord, bool]:
    if not source_version:
        raise ValueError("TIDAS Flow source version is required for a versioned snapshot.")
    flow_uuid = str(flow_record.get("flow_uuid") or "").strip()
    existing = get_flow_version(
        db,
        flow_uuid=flow_uuid,
        source_namespace=source_namespace,
        source_version=source_version,
    )
    semantic_payload = {
        "flow_uuid": flow_uuid,
        "source_namespace": source_namespace,
        "source_version": source_version,
        "flow_name": str(flow_record.get("flow_name") or flow_uuid),
        "flow_name_en": flow_record.get("flow_name_en"),
        "flow_type": str(flow_record.get("flow_type") or "Product flow"),
        "default_unit": str(flow_record.get("default_unit") or ""),
        "unit_group": str(flow_record.get("unit_group") or ""),
        "flow_property_uuid": flow_record.get("tidas_flow_property_uuid"),
        "flow_property_version": flow_record.get("tidas_flow_property_version"),
        "unit_group_uuid": flow_record.get("tidas_unit_group_uuid"),
        "unit_group_version": flow_record.get("tidas_unit_group_version"),
    }
    content_hash = flow_snapshot_content_hash(semantic_payload)
    if existing is not None:
        if existing.content_hash and existing.content_hash != content_hash:
            raise ValueError(
                f"Flow snapshot conflict for {flow_uuid}@{source_version}; immutable version already exists."
            )
        return existing, False
    row = FlowVersionRecord(
        source_namespace=source_namespace,
        flow_uuid=flow_uuid,
        source_version=source_version,
        version_label=flow_version_label(source_namespace, source_version),
        flow_name=semantic_payload["flow_name"],
        flow_name_en=semantic_payload["flow_name_en"],
        flow_type=semantic_payload["flow_type"],
        default_unit=semantic_payload["default_unit"],
        unit_group=semantic_payload["unit_group"],
        flow_property_uuid=semantic_payload["flow_property_uuid"],
        flow_property_version=semantic_payload["flow_property_version"],
        unit_group_uuid=semantic_payload["unit_group_uuid"],
        unit_group_version=semantic_payload["unit_group_version"],
        source_updated_at=flow_record.get("source_updated_at"),
        content_hash=content_hash,
        metadata_json=metadata or None,
    )
    db.add(row)
    return row, True


def backfill_tg_legacy_flow_versions(db: Session) -> int:
    """Pin existing TianGong catalog rows as the immutable TG 1.0 baseline."""
    rows = (
        db.query(FlowRecord)
        .filter(FlowRecord.source.in_(("tiangong", "tidas_import", "tidas_bundle")))
        .all()
    )
    versioned_flow_uuids = {
        flow_uuid
        for (flow_uuid,) in db.query(FlowVersionRecord.flow_uuid).distinct().all()
    }
    created = 0
    for row in rows:
        if row.flow_uuid in versioned_flow_uuids:
            continue
        row.source_namespace = TG_LEGACY_NAMESPACE
        row.source_version = TG_LEGACY_VERSION
        row.version_label = TG_LEGACY_LABEL
        _, inserted = create_flow_version_snapshot(
            db,
            flow_record={
                "flow_uuid": row.flow_uuid,
                "flow_name": row.flow_name,
                "flow_name_en": row.flow_name_en,
                "flow_type": row.flow_type,
                "default_unit": row.default_unit,
                "unit_group": row.unit_group,
                "tidas_flow_property_uuid": row.tidas_flow_property_uuid,
                "source_updated_at": row.source_updated_at,
            },
            source_namespace=TG_LEGACY_NAMESPACE,
            source_version=TG_LEGACY_VERSION,
            metadata={"migration": "existing_open_source_catalog"},
        )
        created += int(inserted)
        versioned_flow_uuids.add(row.flow_uuid)
    return created
