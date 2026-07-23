"""One-way Tiangong intermediate-flow links to ecoinvent reference products."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from ..flow_unit_semantics import _unit_group_key
from fastapi import HTTPException

from ..models import (
    FlowRecord,
    IntermediateFlowLinkRule,
    LciProcessVector,
    ReferenceProcess,
    UnitDefinition,
)
from ..schemas import HybridGraph, IntermediateFlowLink
from ..source_policy import SOURCE_SPACE_TIANGONG, classify_flow_source


DEFAULT_PACKAGE_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "flow_mappings"
    / "intermediate_tiangong_to_ecoinvent_v2.json"
)


def _normalized_name(value: object) -> str:
    text = str(value or "").casefold().strip()
    text = re.sub(r"[\s,;:/_\-()\[\]]+", " ", text)
    return " ".join(text.split())


def _flow_type_key(value: object) -> str:
    text = str(value or "").replace("_", " ").strip().casefold()
    if "waste" in text:
        return "waste"
    if "product" in text or "intermediate" in text:
        return "product"
    return text


@dataclass(frozen=True)
class IntermediateFlowResolution:
    source_flow_uuid: str
    target_flow_uuid: str
    amount_factor: float
    source_unit: str
    target_unit: str
    source_unit_group: str | None
    target_unit_group: str | None
    source_flow_type: str | None
    target_flow_type: str | None
    mapping_level: str
    mapping_reason: str
    rule_id: str
    rule_origin: str
    package_id: str | None = None
    package_version: str | None = None
    package_hash: str | None = None
    application_mode: str | None = None
    flow_subtype_override: bool = False
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_flow_uuid": self.source_flow_uuid,
            "target_flow_uuid": self.target_flow_uuid,
            "amount_factor": self.amount_factor,
            "source_unit": self.source_unit,
            "target_unit": self.target_unit,
            "source_unit_group": self.source_unit_group,
            "target_unit_group": self.target_unit_group,
            "source_flow_type": self.source_flow_type,
            "target_flow_type": self.target_flow_type,
            "mapping_level": self.mapping_level,
            "mapping_reason": self.mapping_reason,
            "rule_id": self.rule_id,
            "rule_origin": self.rule_origin,
            "package_id": self.package_id,
            "package_version": self.package_version,
            "package_hash": self.package_hash,
            "application_mode": self.application_mode,
            "flow_subtype_override": self.flow_subtype_override,
            "warnings": list(self.warnings),
            "link_direction": "tiangong_to_ecoinvent",
        }


class IntermediateFlowLinkRegistry:
    def __init__(self, path: Path = DEFAULT_PACKAGE_PATH):
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        direction = payload.get("link_direction") or payload.get("direction")
        if direction != "tiangong_to_ecoinvent":
            raise ValueError("intermediate-flow package has an unsupported direction")
        rows = payload.get("rules") or payload.get("mappings")
        if not isinstance(rows, list) or not rows:
            raise ValueError("intermediate-flow package contains no rules")
        unit_group_contracts = payload.get("unit_group_contracts")
        if not isinstance(unit_group_contracts, dict) or not unit_group_contracts:
            raise ValueError("intermediate-flow package contains no unit-group contracts")
        indexed: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("review_status") not in {"approved", "approved_with_warning"}:
                raise ValueError("intermediate-flow package contains an unapproved rule")
            mapping_level = str(row.get("mapping_level") or "")
            application_mode = str(row.get("application_mode") or "strict_identity")
            if mapping_level == "L1" and application_mode != "strict_identity":
                raise ValueError("L1 intermediate-flow rules must use strict_identity")
            if mapping_level == "L2" and application_mode != "auto_compatible":
                raise ValueError("L2 intermediate-flow rules must use auto_compatible")
            if mapping_level not in {"L1", "L2"}:
                raise ValueError("built-in intermediate-flow rules must be L1 or L2")
            if mapping_level == "L2" and not row.get("warnings"):
                raise ValueError("auto-compatible intermediate-flow rules require warnings")
            source_uuid = str(row.get("source_flow_uuid") or "").strip()
            target_uuid = str(row.get("target_flow_uuid") or "").strip()
            if not source_uuid or not target_uuid or source_uuid in indexed:
                raise ValueError("intermediate-flow package has invalid or duplicate UUIDs")
            source_flow_type = _flow_type_key(row.get("source_flow_type"))
            target_flow_type = _flow_type_key(row.get("target_flow_type"))
            flow_subtype_override = bool(row.get("flow_subtype_override"))
            if source_flow_type != target_flow_type:
                if (
                    mapping_level != "L2"
                    or not flow_subtype_override
                    or "FLOW_SUBTYPE_OVERRIDE" not in (row.get("warnings") or [])
                    or not re.fullmatch(
                        r"[0-9a-f]{64}",
                        str(row.get("evidence_sha256") or ""),
                    )
                ):
                    raise ValueError(f"flow type mismatch in rule {row.get('rule_id')}")
            elif flow_subtype_override:
                raise ValueError(f"unnecessary flow subtype override in rule {row.get('rule_id')}")
            unit_dimension = str(row.get("unit_dimension") or "").strip()
            contract = unit_group_contracts.get(unit_dimension)
            if not isinstance(contract, dict):
                raise ValueError(f"unit-group contract missing in rule {row.get('rule_id')}")
            source_unit_group = str(contract.get("source_unit_group") or "").strip()
            target_unit_group = str(contract.get("target_unit_group") or "").strip()
            if not source_unit_group or not target_unit_group:
                raise ValueError(f"unit group missing in rule {row.get('rule_id')}")
            if _unit_group_key(source_unit_group) != _unit_group_key(target_unit_group):
                raise ValueError(f"unit group mismatch in rule {row.get('rule_id')}")
            if float(row.get("amount_factor") or 0) <= 0:
                raise ValueError(f"invalid amount factor in rule {row.get('rule_id')}")
            indexed[source_uuid] = {
                **row,
                "source_unit_group": source_unit_group,
                "target_unit_group": target_unit_group,
            }
        self.path = path
        self.package_id = str(payload["package_id"])
        self.package_version = str(payload.get("package_version") or payload.get("version"))
        self.package_hash = hashlib.sha256(raw).hexdigest()
        self.rules = indexed

    def resolve(self, flow_uuid: str) -> IntermediateFlowResolution | None:
        row = self.rules.get(str(flow_uuid or "").strip())
        if row is None:
            return None
        return IntermediateFlowResolution(
            source_flow_uuid=str(row["source_flow_uuid"]),
            target_flow_uuid=str(row["target_flow_uuid"]),
            amount_factor=float(row["amount_factor"]),
            source_unit=str(row["source_unit"]),
            target_unit=str(row["target_unit"]),
            source_unit_group=str(row["source_unit_group"]),
            target_unit_group=str(row["target_unit_group"]),
            source_flow_type=str(row["source_flow_type"]),
            target_flow_type=str(row["target_flow_type"]),
            mapping_level=str(row["mapping_level"]),
            mapping_reason=(
                "approved_one_way_reference_product_link"
                if row["mapping_level"] == "L1"
                else "approved_one_way_compatible_reference_product_link"
            ),
            rule_id=str(row["rule_id"]),
            rule_origin="builtin",
            package_id=self.package_id,
            package_version=self.package_version,
            package_hash=self.package_hash,
            application_mode=str(row.get("application_mode") or "strict_identity"),
            flow_subtype_override=bool(row.get("flow_subtype_override")),
            warnings=tuple(str(item) for item in row.get("warnings") or []),
        )


@lru_cache(maxsize=1)
def get_intermediate_flow_link_registry() -> IntermediateFlowLinkRegistry:
    return IntermediateFlowLinkRegistry()


def clear_intermediate_flow_link_registry_cache() -> None:
    get_intermediate_flow_link_registry.cache_clear()


def _validate_resolution_records(
    source: FlowRecord | None,
    target: FlowRecord | None,
    resolution: IntermediateFlowResolution,
) -> str | None:
    if source is None:
        return "SOURCE_FLOW_NOT_FOUND"
    if target is None:
        return "TARGET_FLOW_NOT_FOUND"
    if classify_flow_source(str(source.source or "")) != SOURCE_SPACE_TIANGONG:
        return "SOURCE_FLOW_NOT_TIANGONG"
    if "ecoinvent" not in str(target.source or "").casefold():
        return "TARGET_FLOW_NOT_ECOINVENT"
    if resolution.source_flow_type and _flow_type_key(source.flow_type) != _flow_type_key(resolution.source_flow_type):
        return "SOURCE_FLOW_TYPE_DRIFT"
    if resolution.target_flow_type and _flow_type_key(target.flow_type) != _flow_type_key(resolution.target_flow_type):
        return "TARGET_FLOW_TYPE_DRIFT"
    if (
        _flow_type_key(source.flow_type) != _flow_type_key(target.flow_type)
        and not resolution.flow_subtype_override
    ):
        return "FLOW_TYPE_MISMATCH"
    if resolution.source_unit_group and _unit_group_key(source.unit_group) != _unit_group_key(resolution.source_unit_group):
        return "SOURCE_UNIT_GROUP_DRIFT"
    if resolution.target_unit_group and _unit_group_key(target.unit_group) != _unit_group_key(resolution.target_unit_group):
        return "TARGET_UNIT_GROUP_DRIFT"
    if _unit_group_key(source.unit_group) != _unit_group_key(target.unit_group):
        return "UNIT_GROUP_MISMATCH"
    if str(source.default_unit or "") != resolution.source_unit:
        return "SOURCE_UNIT_DRIFT"
    if str(target.default_unit or "") != resolution.target_unit:
        return "TARGET_UNIT_DRIFT"
    return None


def deterministic_default_unit_factor(db: Session, source: FlowRecord, target: FlowRecord) -> float | None:
    """Return target-default units per source-default unit for one physical unit group."""
    if _unit_group_key(source.unit_group) != _unit_group_key(target.unit_group):
        return None
    if source.default_unit == target.default_unit:
        return 1.0
    factors: dict[tuple[str, str], float] = {}
    for row in db.query(UnitDefinition).all():
        factors[(_unit_group_key(row.unit_group), row.unit_name)] = float(row.factor_to_reference)
    group = _unit_group_key(source.unit_group)
    source_factor = factors.get((group, source.default_unit))
    target_factor = factors.get((group, target.default_unit))
    if source_factor is None or target_factor is None or target_factor == 0:
        return None
    return source_factor / target_factor


def resolve_intermediate_flow(db: Session, flow_uuid: str) -> tuple[IntermediateFlowResolution | None, str | None]:
    source_uuid = str(flow_uuid or "").strip()
    user_rule = (
        db.query(IntermediateFlowLinkRule)
        .filter(
            IntermediateFlowLinkRule.source_flow_uuid == source_uuid,
            IntermediateFlowLinkRule.status == "active",
        )
        .order_by(IntermediateFlowLinkRule.updated_at.desc())
        .first()
    )
    if user_rule is not None:
        resolution = IntermediateFlowResolution(
            source_flow_uuid=user_rule.source_flow_uuid,
            target_flow_uuid=user_rule.target_flow_uuid,
            amount_factor=float(user_rule.amount_factor),
            source_unit=user_rule.source_unit,
            target_unit=user_rule.target_unit,
            source_unit_group=None,
            target_unit_group=None,
            source_flow_type=None,
            target_flow_type=None,
            mapping_level="L3",
            mapping_reason=user_rule.mapping_reason,
            rule_id=user_rule.id,
            rule_origin="user",
        )
    else:
        resolution = get_intermediate_flow_link_registry().resolve(source_uuid)
    if resolution is None:
        return None, None
    source = db.get(FlowRecord, source_uuid)
    target = db.get(FlowRecord, resolution.target_flow_uuid)
    return resolution, _validate_resolution_records(source, target, resolution)


def validate_intermediate_flow_link(
    db: Session,
    source_flow_uuid: str,
    link: IntermediateFlowLink,
) -> str | None:
    """Validate persisted link evidence against the current DB and rule source."""
    if link.status not in {"auto", "user_confirmed"}:
        return None
    if str(link.source_flow_uuid or "") != str(source_flow_uuid or ""):
        return "SOURCE_FLOW_UUID_MISMATCH"

    expected = None
    if link.mapping_level in {"L1", "L2"}:
        expected = get_intermediate_flow_link_registry().resolve(source_flow_uuid)
        if expected is None:
            return f"{link.mapping_level}_RULE_NOT_FOUND"
    source = db.get(FlowRecord, source_flow_uuid)
    target = db.get(FlowRecord, link.target_flow_uuid)
    record_issue = _validate_resolution_records(
        source,
        target,
        expected or IntermediateFlowResolution(
            source_flow_uuid=link.source_flow_uuid,
            target_flow_uuid=link.target_flow_uuid,
            amount_factor=link.amount_factor,
            source_unit=link.source_unit,
            target_unit=link.target_unit,
            source_unit_group=None,
            target_unit_group=None,
            source_flow_type=None,
            target_flow_type=None,
            mapping_level=link.mapping_level,
            mapping_reason=link.mapping_reason,
            rule_id=link.rule_id,
            rule_origin=link.rule_origin,
        ),
    )
    if record_issue:
        return record_issue

    if link.mapping_level in {"L1", "L2"}:
        assert expected is not None
        if expected.mapping_level != link.mapping_level:
            return f"{link.mapping_level}_EVIDENCE_MISMATCH"
        allowed_statuses = {"auto"} if link.mapping_level == "L1" else {"auto", "user_confirmed"}
        if link.status not in allowed_statuses or link.rule_origin != "builtin":
            return f"{link.mapping_level}_STATUS_OR_ORIGIN_MISMATCH"
        expected_fields = (
            expected.target_flow_uuid,
            expected.rule_id,
            expected.package_id,
            expected.package_version,
            expected.package_hash,
        )
        actual_fields = (
            link.target_flow_uuid,
            link.rule_id,
            link.package_id,
            link.package_version,
            link.package_hash,
        )
        if actual_fields != expected_fields or abs(link.amount_factor - expected.amount_factor) > 1e-12:
            return f"{link.mapping_level}_EVIDENCE_MISMATCH"
        if link.mapping_level == "L2" and (
            link.application_mode != expected.application_mode
            or link.flow_subtype_override != expected.flow_subtype_override
            or (
                expected.flow_subtype_override
                and (
                    link.source_flow_type != expected.source_flow_type
                    or link.target_flow_type != expected.target_flow_type
                )
            )
            or tuple(link.warnings) != expected.warnings
        ):
            return "L2_EVIDENCE_MISMATCH"
        return None

    if link.mapping_level == "L3":
        if link.status != "user_confirmed" or link.rule_origin != "user":
            return "L3_STATUS_OR_ORIGIN_MISMATCH"
        rule = db.get(IntermediateFlowLinkRule, link.rule_id)
        if rule is None or rule.status != "active":
            return "L3_RULE_NOT_ACTIVE"
        if (
            rule.source_flow_uuid != source_flow_uuid
            or rule.target_flow_uuid != link.target_flow_uuid
            or abs(float(rule.amount_factor) - link.amount_factor) > 1e-12
        ):
            return "L3_EVIDENCE_MISMATCH"
        return None
    return "UNSUPPORTED_MAPPING_LEVEL"


def validate_graph_intermediate_flow_links(db: Session, graph: HybridGraph) -> None:
    issues: list[dict[str, Any]] = []
    for node in graph.nodes:
        for port in node.inputs:
            link = port.intermediate_flow_link
            if link is None or link.status == "inactive":
                continue
            issue = validate_intermediate_flow_link(db, port.flowUuid, link)
            if issue:
                issues.append({
                    "node_id": node.id,
                    "port_id": port.id,
                    "source_flow_uuid": port.flowUuid,
                    "target_flow_uuid": link.target_flow_uuid,
                    "rule_id": link.rule_id,
                    "reason": issue,
                })
    if issues:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "INVALID_INTERMEDIATE_FLOW_LINK",
                "message": "Intermediate-flow link evidence does not match the current database or rule package.",
                "evidence": issues,
            },
        )


def list_l2_candidates(db: Session, source: FlowRecord, limit: int = 5) -> list[dict[str, Any]]:
    source_names = [_normalized_name(source.flow_name_en), _normalized_name(source.flow_name)]
    source_names = [name for name in source_names if name]
    if not source_names:
        return []
    rows = (
        db.query(FlowRecord)
        .filter(
            FlowRecord.source.ilike("%ecoinvent%"),
            FlowRecord.flow_type.in_(["Product flow", "Waste flow"]),
        )
        .all()
    )
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if _flow_type_key(row.flow_type) != _flow_type_key(source.flow_type):
            continue
        if _unit_group_key(row.unit_group) != _unit_group_key(source.unit_group):
            continue
        target_names = [_normalized_name(row.flow_name_en), _normalized_name(row.flow_name)]
        target_names = [name for name in target_names if name]
        if not target_names:
            continue
        score = max(SequenceMatcher(None, left, right).ratio() for left in source_names for right in target_names)
        if score < 0.55:
            continue
        candidates.append({
            "target_flow_uuid": row.flow_uuid,
            "target_flow_name": row.flow_name,
            "target_flow_name_en": row.flow_name_en,
            "target_unit": row.default_unit,
            "target_unit_group": row.unit_group,
            "score": round(score, 6),
            "mapping_level": "L2",
            "applicable": False,
        })
    candidates.sort(key=lambda item: (-float(item["score"]), str(item["target_flow_name"])))
    return candidates[: max(1, min(limit, 20))]


def list_provider_candidates(db: Session, target_flow_uuid: str) -> list[dict[str, Any]]:
    rows = db.query(ReferenceProcess).filter(ReferenceProcess.process_type == "lci_dataset").all()
    vector_ids = {
        row.process_uuid
        for row in db.query(LciProcessVector.process_uuid).filter(LciProcessVector.nnz > 0).all()
    }
    providers: list[dict[str, Any]] = []
    for row in rows:
        process_json = row.process_json if isinstance(row.process_json, dict) else {}
        reference_uuid = str(
            row.reference_flow_uuid
            or process_json.get("reference_product_id")
            or process_json.get("reference_flow_uuid")
            or ""
        ).strip()
        if reference_uuid != target_flow_uuid:
            continue
        providers.append({
            "process_uuid": row.process_uuid,
            "process_name": row.process_name,
            "process_name_en": row.process_name_en,
            "location": str(process_json.get("location") or ""),
            "reference_product_flow_uuid": reference_uuid,
            "reference_product_name": str(process_json.get("reference_product") or ""),
            "reference_product_unit": str(process_json.get("reference_product_unit") or ""),
            "has_lci_vector": row.process_uuid in vector_ids,
            "vector_nnz": int(db.get(LciProcessVector, row.process_uuid).nnz) if row.process_uuid in vector_ids else 0,
        })
    providers.sort(key=lambda item: (not item["has_lci_vector"], item["location"], item["process_name"]))
    return providers


def backfill_ecoinvent_reference_flow_uuids(db: Session, *, commit: bool = False) -> dict[str, Any]:
    updated = 0
    unchanged = 0
    missing_target: list[dict[str, str]] = []
    conflicts: list[dict[str, str]] = []
    rows = db.query(ReferenceProcess).filter(ReferenceProcess.process_type == "lci_dataset").all()
    for row in rows:
        process_json = row.process_json if isinstance(row.process_json, dict) else {}
        target_uuid = str(process_json.get("reference_product_id") or process_json.get("reference_flow_uuid") or "").strip()
        if not target_uuid:
            missing_target.append({"process_uuid": row.process_uuid, "reason": "reference_product_id_missing"})
            continue
        flow = db.get(FlowRecord, target_uuid)
        if flow is None or "ecoinvent" not in str(flow.source or "").casefold() or _flow_type_key(flow.flow_type) not in {"product", "waste"}:
            missing_target.append({"process_uuid": row.process_uuid, "target_flow_uuid": target_uuid})
            continue
        current = str(row.reference_flow_uuid or "").strip()
        if current and current != target_uuid:
            conflicts.append({
                "process_uuid": row.process_uuid,
                "current_flow_uuid": current,
                "target_flow_uuid": target_uuid,
            })
            continue
        if current == target_uuid:
            unchanged += 1
            continue
        row.reference_flow_uuid = target_uuid
        updated += 1
    if commit:
        db.commit()
    else:
        db.rollback()
    return {
        "commit": commit,
        "scanned": len(rows),
        "updated": updated,
        "unchanged": unchanged,
        "missing_target_count": len(missing_target),
        "conflict_count": len(conflicts),
        "missing_targets": missing_target[:100],
        "conflicts": conflicts[:100],
    }
