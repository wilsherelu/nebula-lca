"""One-way Tiangong intermediate-flow links to ecoinvent reference products."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
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
    IntermediateFlowLinkRuleVersion,
    LciProcessVector,
    ReferenceProcess,
    UnitDefinition,
)
from ..schemas import FlowPort, HybridGraph, IntermediateFlowLink
from ..source_policy import SOURCE_SPACE_TIANGONG, classify_flow_source
from .flow_versions import (
    TG_LEGACY_NAMESPACE,
    TG_LEGACY_VERSION,
    get_flow_version,
    normalized_flow_identity,
)
from .public_flow_mapping_service import (
    DEFAULT_PUBLIC_MAPPING_ROOT,
    PublicFlowMappingRegistry,
)


DEFAULT_PACKAGE_PATH = DEFAULT_PUBLIC_MAPPING_ROOT


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
    source_flow_namespace: str | None = None
    source_flow_version: str | None = None
    source_flow_property_uuid: str | None = None
    source_flow_property_version: str | None = None
    source_unit_group_uuid: str | None = None
    source_unit_group_version: str | None = None
    package_id: str | None = None
    package_version: str | None = None
    package_hash: str | None = None
    application_mode: str | None = None
    flow_subtype_override: bool = False
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_flow_uuid": self.source_flow_uuid,
            "source_flow_namespace": self.source_flow_namespace,
            "source_flow_version": self.source_flow_version,
            "source_flow_property_uuid": self.source_flow_property_uuid,
            "source_flow_property_version": self.source_flow_property_version,
            "source_unit_group_uuid": self.source_unit_group_uuid,
            "source_unit_group_version": self.source_unit_group_version,
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
        public = PublicFlowMappingRegistry(path)
        self.path = path
        self.package_id = public.package_id
        self.package_version = public.package_version
        self.package_hash = public.package_hash
        self.rules = public.intermediate
        self._public = public

    def resolve(self, flow_uuid: str) -> IntermediateFlowResolution | None:
        row = self._public.resolve_intermediate(flow_uuid)
        if row is None:
            return None
        is_l2 = row.mapping_level == "L2"
        return IntermediateFlowResolution(
            source_flow_uuid=row.tiangong_flow_uuid,
            target_flow_uuid=row.ecoinvent_flow_uuid,
            amount_factor=row.amount_factor,
            source_unit=row.source_unit,
            target_unit=row.target_unit,
            source_unit_group=None,
            target_unit_group=None,
            source_flow_type=None,
            target_flow_type=None,
            mapping_level=row.mapping_level,
            mapping_reason=(
                "approved_one_way_reference_product_link"
                if row.mapping_level == "L1"
                else "approved_one_way_compatible_reference_product_link"
            ),
            rule_id=f"nebula-flow-mapping-v1:intermediate:{row.tiangong_flow_uuid}",
            rule_origin="builtin",
            package_id=self.package_id,
            package_version=self.package_version,
            package_hash=self.package_hash,
            application_mode="auto_compatible" if is_l2 else "strict_identity",
            flow_subtype_override=is_l2,
            warnings=("MANUAL_CONFIRMATION_RECOMMENDED",) if is_l2 else (),
        )


@lru_cache(maxsize=1)
def get_intermediate_flow_link_registry() -> IntermediateFlowLinkRegistry:
    return IntermediateFlowLinkRegistry()


def clear_intermediate_flow_link_registry_cache() -> None:
    get_intermediate_flow_link_registry.cache_clear()


@dataclass(frozen=True)
class SourceFlowContext:
    record: Any
    namespace: str
    version: str
    inferred: bool = False


@dataclass(frozen=True)
class EcoinventTargetFlowContext:
    flow_uuid: str
    flow_name: str
    flow_name_en: str | None
    flow_type: str
    default_unit: str
    unit_group: str
    source: str = "ecoinvent_3.11"


def resolve_ecoinvent_target_flow(
    db: Session,
    target_flow_uuid: str,
    *,
    source: Any | None = None,
) -> FlowRecord | EcoinventTargetFlowContext | None:
    """Resolve an ecoinvent product independently of the shared Flow catalog.

    A TianGong Flow can legitimately reuse an ecoinvent UUID. In that case the
    compatibility catalog contains the TianGong record, while the imported LCI
    reference process remains the authoritative ecoinvent product definition.
    """
    catalog = db.get(FlowRecord, target_flow_uuid)
    if catalog is not None and "ecoinvent" in str(catalog.source or "").casefold():
        return catalog

    providers = (
        db.query(ReferenceProcess)
        .filter(
            ReferenceProcess.process_type == "lci_dataset",
            ReferenceProcess.reference_flow_uuid == target_flow_uuid,
        )
        .all()
    )
    definitions: dict[tuple[str, str], dict[str, str]] = {}
    for provider in providers:
        payload = provider.process_json if isinstance(provider.process_json, dict) else {}
        unit = str(payload.get("reference_product_unit") or "").strip()
        name = str(payload.get("reference_product") or "").strip()
        if unit and name:
            definitions[(unit, name.casefold())] = {"unit": unit, "name": name}
    units = {item["unit"] for item in definitions.values()}
    if not definitions or len(units) != 1:
        return None

    definition = next(iter(definitions.values()))
    target_unit = definition["unit"]
    target_group = ""
    if source is not None:
        source_group = str(getattr(source, "unit_group", None) or "")
        unit_definitions = db.query(UnitDefinition).filter(
            UnitDefinition.unit_name == target_unit,
        ).all()
        if any(_unit_group_key(item.unit_group) == _unit_group_key(source_group) for item in unit_definitions):
            target_group = source_group
        elif target_unit == str(getattr(source, "default_unit", None) or ""):
            target_group = source_group
        else:
            candidate_groups = {
                str(item.unit_group or "").strip()
                for item in unit_definitions
                if str(item.unit_group or "").strip()
            }
            if len(candidate_groups) == 1:
                target_group = next(iter(candidate_groups))

    return EcoinventTargetFlowContext(
        flow_uuid=target_flow_uuid,
        flow_name=definition["name"],
        flow_name_en=definition["name"],
        flow_type="Product flow",
        default_unit=target_unit,
        unit_group=target_group,
    )


def resolve_source_flow_context(
    db: Session,
    *,
    flow_uuid: str,
    source_namespace: str | None = None,
    source_version: str | None = None,
    source_unit: str | None = None,
    source_unit_group: str | None = None,
) -> tuple[SourceFlowContext | None, str | None]:
    """Resolve immutable Flow semantics, inferring an old version only when unique."""
    namespace, version = normalized_flow_identity(
        source_namespace=source_namespace,
        source_version=source_version,
    )
    explicit_identity = bool(str(source_namespace or "").strip() or str(source_version or "").strip())
    record = get_flow_version(
        db,
        flow_uuid=flow_uuid,
        source_namespace=namespace,
        source_version=version,
    )
    expected_unit = str(source_unit or "").strip()
    expected_group = str(source_unit_group or "").strip()
    if record is not None and (
        (expected_unit and expected_unit != str(record.default_unit or ""))
        or (expected_group and _unit_group_key(expected_group) != _unit_group_key(record.unit_group))
    ):
        if explicit_identity:
            return None, "SOURCE_FLOW_VERSION_SEMANTICS_MISMATCH"
        candidates = db.query(type(record)).filter(type(record).flow_uuid == flow_uuid).all()
        matches = [
            item for item in candidates
            if (not expected_unit or expected_unit == str(item.default_unit or ""))
            and (not expected_group or _unit_group_key(expected_group) == _unit_group_key(item.unit_group))
        ]
        if len(matches) == 1:
            match = matches[0]
            return SourceFlowContext(match, match.source_namespace, match.source_version, True), None
        return None, "SOURCE_FLOW_VERSION_AMBIGUOUS"
    if record is not None:
        return SourceFlowContext(record, namespace, version), None
    if explicit_identity and not (
        namespace == TG_LEGACY_NAMESPACE and version == TG_LEGACY_VERSION
    ):
        return None, "SOURCE_FLOW_VERSION_NOT_FOUND"
    legacy = db.get(FlowRecord, flow_uuid)
    if legacy is None:
        return None, "SOURCE_FLOW_NOT_FOUND"
    if expected_unit and expected_unit != str(legacy.default_unit or ""):
        return None, "SOURCE_UNIT_DRIFT"
    if expected_group and _unit_group_key(expected_group) != _unit_group_key(legacy.unit_group):
        return None, "SOURCE_UNIT_GROUP_DRIFT"
    return SourceFlowContext(legacy, TG_LEGACY_NAMESPACE, TG_LEGACY_VERSION), None


def _bind_source_context(
    resolution: IntermediateFlowResolution,
    context: SourceFlowContext,
) -> IntermediateFlowResolution:
    record = context.record
    return replace(
        resolution,
        source_flow_namespace=context.namespace,
        source_flow_version=context.version,
        source_flow_property_uuid=getattr(record, "flow_property_uuid", None),
        source_flow_property_version=getattr(record, "flow_property_version", None),
        source_unit_group_uuid=getattr(record, "unit_group_uuid", None),
        source_unit_group_version=getattr(record, "unit_group_version", None),
        source_unit=str(getattr(record, "default_unit", None) or resolution.source_unit),
        source_unit_group=str(getattr(record, "unit_group", None) or resolution.source_unit_group or "") or None,
    )


def _validate_resolution_records(
    source: Any | None,
    target: FlowRecord | None,
    resolution: IntermediateFlowResolution,
) -> str | None:
    if source is None:
        return "SOURCE_FLOW_NOT_FOUND"
    if target is None:
        return "TARGET_FLOW_NOT_FOUND"
    if resolution.rule_origin == "builtin":
        source_origin = getattr(source, "source", None) or getattr(source, "source_namespace", None)
        if classify_flow_source(str(source_origin or "")) != SOURCE_SPACE_TIANGONG:
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
    if (
        _unit_group_key(source.unit_group) != _unit_group_key(target.unit_group)
        and resolution.rule_origin != "user"
    ):
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


def repair_legacy_intermediate_flow_links(db: Session, graph: HybridGraph) -> list[dict[str, Any]]:
    """Pin uniquely resolvable legacy L3 links to their immutable Flow version.

    Older projects could retain a TG 1.0 rule after a port had been repaired to
    the semantics of a newer TIDAS Flow version.  Preserve the reviewed target,
    but only migrate when the current port uniquely identifies that version and
    the source/target conversion is deterministic within one unit group.
    """
    repairs: list[dict[str, Any]] = []
    edges_by_target: dict[tuple[str, str], list[Any]] = {}
    for edge in graph.exchanges:
        target_port_id = str(edge.target_port_id or "").strip()
        if not target_port_id and str(edge.targetHandle or "").startswith("in:"):
            target_port_id = str(edge.targetHandle)[3:]
        edges_by_target.setdefault((str(edge.toNode), target_port_id), []).append(edge)

    for node in graph.nodes:
        for port in node.inputs:
            link = port.intermediate_flow_link
            if (
                link is None
                or link.status not in {"auto", "user_confirmed"}
                or link.mapping_level not in {"L1", "L2"}
                or link.rule_origin != "builtin"
                or str(link.source_flow_version or "").strip()
            ):
                continue
            resolution, issue = resolve_intermediate_flow(
                db,
                port.flowUuid,
                source_namespace=port.flow_source_namespace,
                source_version=port.flow_version,
                source_unit=port.unit,
                source_unit_group=port.unitGroup,
            )
            if (
                issue
                or resolution is None
                or resolution.mapping_level != link.mapping_level
                or resolution.target_flow_uuid != link.target_flow_uuid
                or resolution.rule_id != link.rule_id
            ):
                continue
            port.flow_source_namespace = resolution.source_flow_namespace
            port.flow_version = resolution.source_flow_version
            port.flow_property_uuid = resolution.source_flow_property_uuid
            port.flow_property_version = resolution.source_flow_property_version
            port.unit_group_uuid = resolution.source_unit_group_uuid
            port.unit_group_version = resolution.source_unit_group_version
            port.unit = resolution.source_unit
            port.unitGroup = resolution.source_unit_group
            port.unitGroupSwitch = None
            port.intermediate_flow_link = link.model_copy(update={
                "source_flow_namespace": resolution.source_flow_namespace,
                "source_flow_version": resolution.source_flow_version,
                "source_flow_property_uuid": resolution.source_flow_property_uuid,
                "source_flow_property_version": resolution.source_flow_property_version,
                "source_unit_group_uuid": resolution.source_unit_group_uuid,
                "source_unit_group_version": resolution.source_unit_group_version,
                "amount_factor": resolution.amount_factor,
                "source_unit": resolution.source_unit,
                "target_unit": resolution.target_unit,
                "source_unit_group": resolution.source_unit_group,
                "target_unit_group": resolution.target_unit_group,
                "mapping_reason": resolution.mapping_reason,
                "package_id": resolution.package_id,
                "package_version": resolution.package_version,
                "package_hash": resolution.package_hash,
                "application_mode": resolution.application_mode,
                "source_flow_type": resolution.source_flow_type,
                "target_flow_type": resolution.target_flow_type,
                "flow_subtype_override": resolution.flow_subtype_override,
                "warnings": list(resolution.warnings),
            })
            linked_edges = edges_by_target.get((str(node.id), str(port.id)), [])
            converted_amount = float(port.amount) * resolution.amount_factor
            updated_edges = 0
            for edge in linked_edges:
                if str(edge.consumer_flow_uuid or "") != str(port.flowUuid):
                    continue
                edge.amount = converted_amount
                edge.consumerAmount = converted_amount
                edge.unit = resolution.target_unit
                edge.provider_unit = resolution.target_unit
                edge.consumer_unit = resolution.source_unit
                edge.intermediate_flow_link_rule_id = resolution.rule_id
                edge.intermediate_flow_link_factor = resolution.amount_factor
                updated_edges += 1
            repairs.append({
                "node_id": node.id,
                "port_id": port.id,
                "flow_uuid": port.flowUuid,
                "source_namespace": resolution.source_flow_namespace,
                "source_version": resolution.source_flow_version,
                "rule_id": resolution.rule_id,
                "amount_factor": resolution.amount_factor,
                "updated_edges": updated_edges,
            })

    # A package's publication metadata may change without changing any mapping
    # row. Refresh only the content fingerprint when every other piece of the
    # persisted builtin evidence still validates against the current rule.
    for node in graph.nodes:
        for port in node.inputs:
            link = port.intermediate_flow_link
            if (
                link is None
                or link.status not in {"auto", "user_confirmed"}
                or link.mapping_level not in {"L1", "L2"}
                or link.rule_origin != "builtin"
            ):
                continue
            resolution, issue = resolve_intermediate_flow(
                db,
                port.flowUuid,
                source_namespace=port.flow_source_namespace,
                source_version=port.flow_version,
                source_unit=port.unit,
                source_unit_group=port.unitGroup,
            )
            if issue or resolution is None or link.package_hash == resolution.package_hash:
                continue
            refreshed = link.model_copy(update={"package_hash": resolution.package_hash})
            if validate_intermediate_flow_link(db, port.flowUuid, refreshed, port=port) is None:
                port.intermediate_flow_link = refreshed

    for node in graph.nodes:
        for port in node.inputs:
            link = port.intermediate_flow_link
            if (
                link is None
                or link.status != "user_confirmed"
                or link.mapping_level != "L3"
                or link.rule_origin != "user"
                or str(link.source_flow_version or "").strip()
            ):
                continue
            legacy_rule = db.get(IntermediateFlowLinkRule, link.rule_id)
            if (
                legacy_rule is None
                or legacy_rule.status != "active"
                or legacy_rule.source_flow_uuid != port.flowUuid
                or legacy_rule.target_flow_uuid != link.target_flow_uuid
            ):
                continue
            context, issue = resolve_source_flow_context(
                db,
                flow_uuid=port.flowUuid,
                source_namespace=port.flow_source_namespace,
                source_version=port.flow_version,
                source_unit=port.unit,
                source_unit_group=port.unitGroup,
            )
            if issue or context is None or (
                not context.inferred and not str(port.flow_version or "").strip()
            ):
                continue
            target = resolve_ecoinvent_target_flow(db, link.target_flow_uuid, source=context.record)
            if target is None:
                continue
            factor = deterministic_default_unit_factor(db, context.record, target)
            if factor is None:
                continue

            versioned_rule = (
                db.query(IntermediateFlowLinkRuleVersion)
                .filter(
                    IntermediateFlowLinkRuleVersion.source_namespace == context.namespace,
                    IntermediateFlowLinkRuleVersion.source_flow_uuid == port.flowUuid,
                    IntermediateFlowLinkRuleVersion.source_version == context.version,
                    IntermediateFlowLinkRuleVersion.target_flow_uuid == target.flow_uuid,
                )
                .one_or_none()
            )
            if versioned_rule is None:
                versioned_rule = IntermediateFlowLinkRuleVersion(
                    id=str(uuid.uuid4()),
                    source_flow_uuid=port.flowUuid,
                    source_namespace=context.namespace,
                    source_version=context.version,
                    target_flow_uuid=target.flow_uuid,
                )
                db.add(versioned_rule)
            source = context.record
            versioned_rule.amount_factor = factor
            versioned_rule.source_unit = str(source.default_unit or "")
            versioned_rule.source_unit_group = str(source.unit_group or "")
            versioned_rule.target_unit = str(target.default_unit or "")
            versioned_rule.target_unit_group = str(target.unit_group or "")
            versioned_rule.mapping_level = "L3"
            versioned_rule.mapping_reason = legacy_rule.mapping_reason
            versioned_rule.rule_origin = "user"
            versioned_rule.status = "active"
            versioned_rule.updated_at = datetime.utcnow()
            db.flush()

            port.flow_source_namespace = context.namespace
            port.flow_version = context.version
            port.flow_property_uuid = getattr(source, "flow_property_uuid", None)
            port.flow_property_version = getattr(source, "flow_property_version", None)
            port.unit_group_uuid = getattr(source, "unit_group_uuid", None)
            port.unit_group_version = getattr(source, "unit_group_version", None)
            port.unit = str(source.default_unit or port.unit)
            port.unitGroup = str(source.unit_group or port.unitGroup or "") or None
            port.unitGroupSwitch = None
            port.intermediate_flow_link = link.model_copy(update={
                "source_flow_namespace": context.namespace,
                "source_flow_version": context.version,
                "source_flow_property_uuid": getattr(source, "flow_property_uuid", None),
                "source_flow_property_version": getattr(source, "flow_property_version", None),
                "source_unit_group_uuid": getattr(source, "unit_group_uuid", None),
                "source_unit_group_version": getattr(source, "unit_group_version", None),
                "amount_factor": factor,
                "source_unit": str(source.default_unit or ""),
                "target_unit": str(target.default_unit or ""),
                "source_unit_group": str(source.unit_group or "") or None,
                "target_unit_group": str(target.unit_group or "") or None,
                "rule_id": versioned_rule.id,
            })

            linked_edges = edges_by_target.get((str(node.id), str(port.id)), [])
            converted_amount = float(port.amount) * factor
            updated_edges = 0
            for edge in linked_edges:
                if str(edge.consumer_flow_uuid or "") != str(port.flowUuid):
                    continue
                edge.amount = converted_amount
                edge.consumerAmount = converted_amount
                edge.unit = str(target.default_unit or edge.unit)
                edge.provider_unit = str(target.default_unit or "")
                edge.consumer_unit = str(source.default_unit or "")
                edge.intermediate_flow_link_rule_id = versioned_rule.id
                edge.intermediate_flow_link_factor = factor
                updated_edges += 1
            repairs.append({
                "node_id": node.id,
                "port_id": port.id,
                "flow_uuid": port.flowUuid,
                "source_namespace": context.namespace,
                "source_version": context.version,
                "rule_id": versioned_rule.id,
                "amount_factor": factor,
                "updated_edges": updated_edges,
            })
    return repairs


def _resolution_with_catalog_units(
    db: Session,
    source: FlowRecord | None,
    target: FlowRecord | None,
    resolution: IntermediateFlowResolution,
) -> IntermediateFlowResolution:
    """Use current catalog spellings when they preserve the reviewed factor."""
    if source is None or target is None:
        return resolution
    factor = deterministic_default_unit_factor(db, source, target)
    if factor is None or abs(factor - resolution.amount_factor) > 1e-12:
        return resolution
    return replace(
        resolution,
        amount_factor=factor,
        source_unit=str(source.default_unit or ""),
        target_unit=str(target.default_unit or ""),
        source_unit_group=str(source.unit_group or "") or None,
        target_unit_group=str(target.unit_group or "") or None,
    )


def resolve_intermediate_flow(
    db: Session,
    flow_uuid: str,
    *,
    source_namespace: str | None = None,
    source_version: str | None = None,
    source_unit: str | None = None,
    source_unit_group: str | None = None,
) -> tuple[IntermediateFlowResolution | None, str | None]:
    source_uuid = str(flow_uuid or "").strip()
    context, context_issue = resolve_source_flow_context(
        db,
        flow_uuid=source_uuid,
        source_namespace=source_namespace,
        source_version=source_version,
        source_unit=source_unit,
        source_unit_group=source_unit_group,
    )
    if context_issue or context is None:
        return None, context_issue
    versioned_rule = (
        db.query(IntermediateFlowLinkRuleVersion)
        .filter(
            IntermediateFlowLinkRuleVersion.source_flow_uuid == source_uuid,
            IntermediateFlowLinkRuleVersion.source_namespace == context.namespace,
            IntermediateFlowLinkRuleVersion.source_version == context.version,
            IntermediateFlowLinkRuleVersion.status == "active",
        )
        .order_by(IntermediateFlowLinkRuleVersion.updated_at.desc())
        .first()
    )
    legacy_rule = None
    if versioned_rule is None and (
        context.namespace == TG_LEGACY_NAMESPACE and context.version == TG_LEGACY_VERSION
    ):
        legacy_rule = (
            db.query(IntermediateFlowLinkRule)
            .filter(
                IntermediateFlowLinkRule.source_flow_uuid == source_uuid,
                IntermediateFlowLinkRule.status == "active",
            )
            .order_by(IntermediateFlowLinkRule.updated_at.desc())
            .first()
        )
    user_rule = versioned_rule or legacy_rule
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
    target = resolve_ecoinvent_target_flow(db, resolution.target_flow_uuid, source=context.record)
    resolution = _bind_source_context(resolution, context)
    resolution = _resolution_with_catalog_units(db, context.record, target, resolution)
    return resolution, _validate_resolution_records(context.record, target, resolution)


def validate_intermediate_flow_link(
    db: Session,
    source_flow_uuid: str,
    link: IntermediateFlowLink,
    *,
    port: FlowPort | None = None,
) -> str | None:
    """Validate persisted link evidence against the current DB and rule source."""
    if link.status not in {"auto", "user_confirmed"}:
        return None
    if str(link.source_flow_uuid or "") != str(source_flow_uuid or ""):
        return "SOURCE_FLOW_UUID_MISMATCH"

    port_namespace = port.flow_source_namespace if port is not None else None
    port_version = port.flow_version if port is not None else None
    link_namespace, link_version = normalized_flow_identity(
        source_namespace=link.source_flow_namespace,
        source_version=link.source_flow_version,
    )
    if port is not None:
        expected_namespace, expected_version = normalized_flow_identity(
            source_namespace=port_namespace,
            source_version=port_version,
        )
        if (link_namespace, link_version) != (expected_namespace, expected_version):
            return "SOURCE_FLOW_VERSION_MISMATCH"
    context, context_issue = resolve_source_flow_context(
        db,
        flow_uuid=source_flow_uuid,
        source_namespace=link.source_flow_namespace,
        source_version=link.source_flow_version,
    )
    if context_issue or context is None:
        return context_issue
    if port is not None and not str(port.flow_version or "").strip():
        switch = port.unitGroupSwitch if isinstance(port.unitGroupSwitch, dict) else {}
        saved_default_unit = str(
            switch.get("sourceUnit") or switch.get("source_unit") or port.unit or ""
        )
        saved_default_group = str(
            switch.get("sourceUnitGroup") or switch.get("source_unit_group") or port.unitGroup or ""
        )
        if saved_default_unit and saved_default_unit != str(getattr(context.record, "default_unit", None) or ""):
            return "SOURCE_UNIT_DRIFT"
        if saved_default_group and (
            _unit_group_key(saved_default_group)
            != _unit_group_key(getattr(context.record, "unit_group", None))
        ):
            return "SOURCE_UNIT_GROUP_DRIFT"
    if str(link.source_unit or "") != str(getattr(context.record, "default_unit", None) or ""):
        return "SOURCE_UNIT_DRIFT"
    if link.source_unit_group and (
        _unit_group_key(link.source_unit_group)
        != _unit_group_key(getattr(context.record, "unit_group", None))
    ):
        return "SOURCE_UNIT_GROUP_DRIFT"

    expected = None
    if link.mapping_level in {"L1", "L2"}:
        expected = get_intermediate_flow_link_registry().resolve(source_flow_uuid)
        if expected is None:
            return f"{link.mapping_level}_RULE_NOT_FOUND"
    source = context.record
    target = resolve_ecoinvent_target_flow(db, link.target_flow_uuid, source=source)
    if expected is not None:
        expected = _resolution_with_catalog_units(db, source, target, expected)
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
        )
        actual_fields = (
            link.target_flow_uuid,
            link.rule_id,
            link.package_id,
            link.package_version,
        )
        if actual_fields != expected_fields or abs(link.amount_factor - expected.amount_factor) > 1e-12:
            return f"{link.mapping_level}_EVIDENCE_MISMATCH"
        if link.mapping_level == "L2" and (
            link.application_mode != expected.application_mode
            or link.flow_subtype_override != expected.flow_subtype_override
            or (
                expected.flow_subtype_override
                and (
                    (
                        expected.source_flow_type is not None
                        and link.source_flow_type != expected.source_flow_type
                    )
                    or (
                        expected.target_flow_type is not None
                        and link.target_flow_type != expected.target_flow_type
                    )
                )
            )
            or tuple(link.warnings) != expected.warnings
        ):
            return "L2_EVIDENCE_MISMATCH"
        if link.package_hash != expected.package_hash:
            return "PACKAGE_HASH_DRIFT"
        return None

    if link.mapping_level == "L3":
        if link.status != "user_confirmed" or link.rule_origin != "user":
            return "L3_STATUS_OR_ORIGIN_MISMATCH"
        rule = db.get(IntermediateFlowLinkRuleVersion, link.rule_id)
        if rule is None:
            rule = db.get(IntermediateFlowLinkRule, link.rule_id)
        if rule is None or rule.status != "active":
            return "L3_RULE_NOT_ACTIVE"
        if (
            rule.source_flow_uuid != source_flow_uuid
            or rule.target_flow_uuid != link.target_flow_uuid
            or abs(float(rule.amount_factor) - link.amount_factor) > 1e-12
        ):
            return "L3_EVIDENCE_MISMATCH"
        if isinstance(rule, IntermediateFlowLinkRuleVersion) and (
            rule.source_namespace != link_namespace
            or rule.source_version != link_version
            or rule.source_unit != link.source_unit
            or _unit_group_key(rule.source_unit_group) != _unit_group_key(link.source_unit_group)
        ):
            return "L3_FLOW_VERSION_EVIDENCE_MISMATCH"
        if isinstance(rule, IntermediateFlowLinkRule) and (
            link_namespace != TG_LEGACY_NAMESPACE or link_version != TG_LEGACY_VERSION
        ):
            return "L3_FLOW_VERSION_EVIDENCE_MISMATCH"
        return None
    return "UNSUPPORTED_MAPPING_LEVEL"


def validate_graph_intermediate_flow_links(db: Session, graph: HybridGraph) -> None:
    issues: list[dict[str, Any]] = []
    for node in graph.nodes:
        for port in node.inputs:
            link = port.intermediate_flow_link
            if link is None or link.status == "inactive":
                continue
            issue = validate_intermediate_flow_link(db, port.flowUuid, link, port=port)
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


def list_provider_candidates(
    db: Session,
    target_flow_uuid: str,
) -> list[dict[str, Any]]:
    rows = db.query(ReferenceProcess).filter(ReferenceProcess.process_type == "lci_dataset").all()
    vector_ids = {
        row.process_uuid
        for row in db.query(LciProcessVector.process_uuid).filter(LciProcessVector.nnz > 0).all()
    }
    providers: list[dict[str, Any]] = []
    for row in rows:
        process_json = row.process_json if isinstance(row.process_json, dict) else {}
        source_file = str(row.source_file or "")
        source = source_file.split("://", 1)[0] if "://" in source_file else str(process_json.get("source") or source_file)
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
            "source": source,
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
