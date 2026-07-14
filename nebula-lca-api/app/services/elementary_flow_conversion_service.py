"""Map reviewed elementary flows to canonical EF flows for TIDAS export."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from ..elementary_flow_conversion.package_loader import load_mapping_package
from ..flow_unit_semantics import build_unit_reference_maps, resolve_flow_port_unit_semantics
from ..models import FlowRecord, ReferenceProcess
from ..source_policy import SOURCE_SPACE_TIANGONG, classify_flow_source


MAPPING_MISSING = "ELEMENTARY_FLOW_MAPPING_MISSING"
MAPPING_CONTEXT_MISMATCH = "ELEMENTARY_FLOW_MAPPING_CONTEXT_MISMATCH"
MAPPING_UNIT_MISMATCH = "ELEMENTARY_FLOW_MAPPING_UNIT_MISMATCH"
MAPPING_DIRECTION_MISMATCH = "ELEMENTARY_FLOW_MAPPING_DIRECTION_MISMATCH"
MAPPING_PACKAGE_UNAVAILABLE = "ELEMENTARY_FLOW_MAPPING_PACKAGE_UNAVAILABLE"

DEFAULT_MAPPING_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "flow_mappings"
    / "ghg_ef31_v1.json"
)


def _flow_uuid(row: dict[str, Any]) -> str:
    return str(
        row.get("flowUuid")
        or row.get("flow_uuid")
        or row.get("@flowUUID")
        or ""
    ).strip()


def _is_elementary(record: FlowRecord | None) -> bool:
    normalized = str(getattr(record, "flow_type", "") or "").replace("_", " ").strip().lower()
    return normalized == "elementary flow"


@dataclass(frozen=True)
class EfResolution:
    source_flow_uuid: str
    target_flow_uuid: str
    amount_factor: float
    mode: str
    direction: str
    source_context: dict[str, Any] | None
    target_context: dict[str, Any] | None
    unit: str | None
    warning_codes: tuple[str, ...] = ()


@dataclass
class EfShadowExportView:
    graph: dict[str, Any]
    process_exchanges: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    traces: list[dict[str, Any]] = field(default_factory=list)
    blocking: list[dict[str, Any]] = field(default_factory=list)
    package_id: str = ""
    package_version: str = ""
    package_hash: str = ""

    def summary(self) -> dict[str, Any]:
        modes: dict[str, int] = {}
        for trace in self.traces:
            mode = str(trace.get("mode") or "")
            modes[mode] = modes.get(mode, 0) + 1
        return {
            "package_id": self.package_id,
            "package_version": self.package_version,
            "package_hash": self.package_hash,
            "target_namespace": "EF",
            "total_elementary_exchange_count": len(self.traces) + len(self.blocking),
            "native_ef_count": modes.get("native_ef", 0),
            "canonical_ef_count": modes.get("canonical", 0),
            "ecoinvent_to_ef_count": modes.get("bidirectional", 0),
            "one_way_canonicalization_count": modes.get("one_way_canonicalization", 0),
            "unmapped_count": sum(item.get("code") == MAPPING_MISSING for item in self.blocking),
            "context_mismatch_count": sum(item.get("code") == MAPPING_CONTEXT_MISMATCH for item in self.blocking),
            "unit_mismatch_count": sum(item.get("code") == MAPPING_UNIT_MISMATCH for item in self.blocking),
            "direction_mismatch_count": sum(item.get("code") == MAPPING_DIRECTION_MISMATCH for item in self.blocking),
            "trace_count": len(self.traces),
            "traces": self.traces,
        }


class ElementaryFlowMappingRegistry:
    """Certified package plus indexes needed by search and EF export."""

    def __init__(self, path: Path = DEFAULT_MAPPING_PATH):
        self.path = path
        payload = json.loads(path.read_text(encoding="utf-8"))
        compiled = load_mapping_package(path)
        if compiled.rejected:
            raise ValueError("mapping package contains rejected rows")
        self.package_id = str(payload["package_id"])
        self.package_version = str(payload["package_version"])
        self.package_hash = compiled.package.package_hash
        self._eco_rows = {
            str(row["ecoinvent_flow_uuid"]): row for row in payload["mappings"]
        }
        self._canonical_ef_rows = {
            str(row["ef_flow_uuid"]): row for row in payload["mappings"]
        }
        self._alias_ef_rows = {
            str(row["ef_flow_uuid"]): row
            for row in payload.get("one_way_mappings", [])
            if row.get("source_namespace") == "EF"
        }
        self.compatible_flow_uuids = frozenset(
            {*self._eco_rows, *self._canonical_ef_rows, *self._alias_ef_rows}
        )

    def resolve_mapped(self, flow_uuid: str) -> EfResolution | None:
        normalized = str(flow_uuid or "").strip()
        row = self._eco_rows.get(normalized)
        if row is not None:
            return EfResolution(
                source_flow_uuid=normalized,
                target_flow_uuid=str(row["ef_flow_uuid"]),
                amount_factor=float(row["eco_to_ef_factor"]),
                mode="bidirectional",
                direction=str(row["direction"]),
                source_context=dict(row["ecoinvent_context"]),
                target_context=dict(row["ef_context"]),
                unit=str(row["unit"]),
            )
        row = self._canonical_ef_rows.get(normalized)
        if row is not None:
            return EfResolution(
                source_flow_uuid=normalized,
                target_flow_uuid=normalized,
                amount_factor=1.0,
                mode="canonical",
                direction=str(row["direction"]),
                source_context=dict(row["ef_context"]),
                target_context=dict(row["ef_context"]),
                unit=str(row["unit"]),
            )
        alias = self._alias_ef_rows.get(normalized)
        if alias is None:
            return None
        canonical = self._eco_rows.get(str(alias["ecoinvent_flow_uuid"]))
        if canonical is None:
            raise ValueError(f"one-way alias lacks canonical EF target: {normalized}")
        return EfResolution(
            source_flow_uuid=normalized,
            target_flow_uuid=str(canonical["ef_flow_uuid"]),
            amount_factor=float(alias["amount_factor"]) * float(canonical["eco_to_ef_factor"]),
            mode="one_way_canonicalization",
            direction=str(alias["direction"]),
            source_context=dict(alias["ef_context"]),
            target_context=dict(canonical["ef_context"]),
            unit=str(alias["unit"]),
            warning_codes=("ONE_WAY_CANONICALIZATION",),
        )

    def compatibility(self, flow_uuid: str) -> dict[str, Any] | None:
        resolution = self.resolve_mapped(flow_uuid)
        if resolution is None:
            return None
        return {
            "conversion_compatible": True,
            "conversion_mode": resolution.mode,
            "conversion_target_flow_uuid": resolution.target_flow_uuid,
            "conversion_package_version": self.package_version,
        }


@lru_cache(maxsize=1)
def get_elementary_flow_mapping_registry() -> ElementaryFlowMappingRegistry:
    return ElementaryFlowMappingRegistry()


def clear_elementary_flow_mapping_registry_cache() -> None:
    get_elementary_flow_mapping_registry.cache_clear()


def _catalog_context(record: FlowRecord, context: dict[str, Any] | None) -> tuple[str, str]:
    if context and "catalog_compartment" in context:
        return (
            str(context.get("catalog_compartment") or ""),
            str(context.get("catalog_subcompartment") or ""),
        )
    return (
        str(context.get("compartment") or "") if context else "",
        str(context.get("subcompartment") or "") if context else "",
    )


def _validate_resolution(
    source: FlowRecord,
    target: FlowRecord,
    resolution: EfResolution,
    direction: str,
) -> dict[str, Any] | None:
    if direction != resolution.direction:
        return {
            "code": MAPPING_DIRECTION_MISMATCH,
            "message": f"{MAPPING_DIRECTION_MISMATCH}: Flow {source.flow_uuid} is mapped only for {resolution.direction} exchanges.",
            "details": {"flow_uuid": source.flow_uuid, "direction": direction},
        }
    source_expected = _catalog_context(source, resolution.source_context)
    target_expected = _catalog_context(target, resolution.target_context)
    source_actual = (str(source.compartment or ""), str(source.subcompartment or ""))
    target_actual = (str(target.compartment or ""), str(target.subcompartment or ""))
    if source_actual != source_expected or target_actual != target_expected:
        return {
            "code": MAPPING_CONTEXT_MISMATCH,
            "message": f"{MAPPING_CONTEXT_MISMATCH}: Flow mapping context does not match the current catalog: {source.flow_uuid}",
            "details": {
                "flow_uuid": source.flow_uuid,
                "target_flow_uuid": target.flow_uuid,
                "source_expected": source_expected,
                "source_actual": source_actual,
                "target_expected": target_expected,
                "target_actual": target_actual,
            },
        }
    if resolution.unit and (
        str(source.default_unit or "") != resolution.unit
        or str(target.default_unit or "") != resolution.unit
    ):
        return {
            "code": MAPPING_UNIT_MISMATCH,
            "message": f"{MAPPING_UNIT_MISMATCH}: Flow mapping unit does not match the current catalog: {source.flow_uuid}",
            "details": {
                "flow_uuid": source.flow_uuid,
                "target_flow_uuid": target.flow_uuid,
                "mapping_unit": resolution.unit,
                "source_unit": source.default_unit,
                "target_unit": target.default_unit,
            },
        }
    return None


def _resolve_for_export(
    db: Session,
    registry: ElementaryFlowMappingRegistry,
    flow_uuid: str,
    direction: str,
) -> tuple[EfResolution | None, FlowRecord | None, FlowRecord | None, dict[str, Any] | None]:
    source = db.get(FlowRecord, flow_uuid)
    if source is None:
        return None, None, None, None
    mapped = registry.resolve_mapped(flow_uuid)
    if mapped is not None and mapped.mode == "canonical":
        mapped = EfResolution(
            source_flow_uuid=mapped.source_flow_uuid,
            target_flow_uuid=mapped.target_flow_uuid,
            amount_factor=1.0,
            mode=mapped.mode,
            direction=direction,
            source_context=mapped.source_context,
            target_context=mapped.target_context,
            unit=mapped.unit,
        )
    if mapped is None:
        if classify_flow_source(source.source, bool(source.is_custom)) == SOURCE_SPACE_TIANGONG:
            mapped = EfResolution(
                source_flow_uuid=flow_uuid,
                target_flow_uuid=flow_uuid,
                amount_factor=1.0,
                mode="native_ef",
                direction=direction,
                source_context=None,
                target_context=None,
                unit=str(source.default_unit or ""),
            )
        else:
            return None, source, None, {
                "code": MAPPING_MISSING,
                "message": f"{MAPPING_MISSING}: Elementary flow cannot be resolved to EF: {flow_uuid}",
                "details": {"flow_uuid": flow_uuid, "source": source.source},
            }
    target = db.get(FlowRecord, mapped.target_flow_uuid)
    if target is None:
        return mapped, source, None, {
            "code": MAPPING_MISSING,
            "message": f"{MAPPING_MISSING}: Mapped EF target is absent from the catalog: {mapped.target_flow_uuid}",
            "details": {"flow_uuid": flow_uuid, "target_flow_uuid": mapped.target_flow_uuid},
        }
    issue = None if mapped.mode in {"native_ef", "canonical"} else _validate_resolution(source, target, mapped, direction)
    return mapped, source, target, issue


def _convert_exchange(
    db: Session,
    registry: ElementaryFlowMappingRegistry,
    exchange: dict[str, Any],
    *,
    direction: str,
    location: dict[str, Any],
    unit_factor_by_group_and_name: dict[tuple[str, str], float],
    reference_unit_by_group: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    converted = copy.deepcopy(exchange)
    source_uuid = _flow_uuid(exchange)
    source_record = db.get(FlowRecord, source_uuid) if source_uuid else None
    if not _is_elementary(source_record):
        return converted, None, None
    resolution, source, target, issue = _resolve_for_export(db, registry, source_uuid, direction)
    if issue is not None:
        issue = copy.deepcopy(issue)
        issue.setdefault("details", {}).update(location)
        return converted, None, issue
    assert resolution is not None and source is not None and target is not None
    semantics = resolve_flow_port_unit_semantics(
        db,
        exchange,
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
        reference_unit_by_group=reference_unit_by_group,
    )
    if not semantics.ok or semantics.amount_in_flow_default_unit is None:
        return converted, None, {
            "code": MAPPING_UNIT_MISMATCH,
            "message": f"{MAPPING_UNIT_MISMATCH}: Flow amount cannot be converted to its default unit: {source_uuid}",
            "details": {"flow_uuid": source_uuid, "reason": semantics.reason, **location},
        }
    target_amount = float(semantics.amount_in_flow_default_unit) * resolution.amount_factor
    for key in ("flowUuid", "flow_uuid", "@flowUUID"):
        if key in converted or key == "flowUuid":
            converted[key] = resolution.target_flow_uuid
            if key == "flowUuid":
                break
    if "flow_uuid" in exchange:
        converted["flow_uuid"] = resolution.target_flow_uuid
    if "@flowUUID" in exchange:
        converted["@flowUUID"] = resolution.target_flow_uuid
    converted["name"] = target.flow_name
    if "port_name" in converted:
        converted["port_name"] = target.flow_name
    if "flow_name" in converted:
        converted["flow_name"] = target.flow_name
    converted["flow_name_en"] = target.flow_name_en
    converted["unit"] = target.default_unit
    if "unitGroup" in converted or "unit_group" not in converted:
        converted["unitGroup"] = target.unit_group
    if "unit_group" in converted:
        converted["unit_group"] = target.unit_group
    converted["amount"] = target_amount
    converted.pop("unitGroupSwitch", None)
    converted.pop("unit_group_switch", None)
    trace = {
        "source_flow_uuid": source_uuid,
        "target_flow_uuid": resolution.target_flow_uuid,
        "mode": resolution.mode,
        "direction": direction,
        "source_amount": semantics.current_amount,
        "source_unit": semantics.current_unit,
        "target_amount": target_amount,
        "target_unit": target.default_unit,
        "amount_factor": resolution.amount_factor,
        "warning_codes": list(resolution.warning_codes),
        **location,
    }
    return converted, trace, None


def build_ef_shadow_export_view(
    db: Session,
    graph_json: dict[str, Any],
    registry: ElementaryFlowMappingRegistry | None = None,
) -> EfShadowExportView:
    try:
        active_registry = registry or get_elementary_flow_mapping_registry()
    except Exception as exc:
        return EfShadowExportView(
            graph=copy.deepcopy(graph_json),
            blocking=[{
                "code": MAPPING_PACKAGE_UNAVAILABLE,
                "message": f"{MAPPING_PACKAGE_UNAVAILABLE}: Elementary flow mapping package is unavailable: {exc}",
                "details": {},
            }],
        )
    result = EfShadowExportView(
        graph=copy.deepcopy(graph_json),
        package_id=active_registry.package_id,
        package_version=active_registry.package_version,
        package_hash=active_registry.package_hash,
    )
    unit_factors, reference_units = build_unit_reference_maps(db)
    process_uuids: set[str] = set()
    for node in result.graph.get("nodes", []) or []:
        if not isinstance(node, dict):
            continue
        process_uuid = str(node.get("process_uuid") or node.get("id") or "").strip()
        if process_uuid:
            process_uuids.add(process_uuid)
        for bucket, direction in (("inputs", "input"), ("outputs", "output")):
            ports = node.get(bucket) or []
            for index, port in enumerate(ports):
                if not isinstance(port, dict):
                    continue
                converted, trace, issue = _convert_exchange(
                    db,
                    active_registry,
                    port,
                    direction=direction,
                    location={"node_id": node.get("id"), "port_id": port.get("id"), "bucket": bucket},
                    unit_factor_by_group_and_name=unit_factors,
                    reference_unit_by_group=reference_units,
                )
                ports[index] = converted
                if trace:
                    result.traces.append(trace)
                if issue:
                    result.blocking.append(issue)
    for index, exchange in enumerate(result.graph.get("exchanges", []) or []):
        if not isinstance(exchange, dict):
            continue
        direction = str(exchange.get("direction") or "output").strip().lower()
        converted, trace, issue = _convert_exchange(
            db,
            active_registry,
            exchange,
            direction=direction,
            location={"graph_exchange_index": index},
            unit_factor_by_group_and_name=unit_factors,
            reference_unit_by_group=reference_units,
        )
        result.graph["exchanges"][index] = converted
        if trace:
            result.traces.append(trace)
        if issue:
            result.blocking.append(issue)

    for process_uuid in sorted(process_uuids):
        reference = db.get(ReferenceProcess, process_uuid)
        process_json = reference.process_json if reference is not None else None
        if not isinstance(process_json, dict):
            continue
        root = process_json.get("processDataSet") if isinstance(process_json.get("processDataSet"), dict) else process_json
        exchanges = root.get("exchanges") if isinstance(root, dict) else None
        if not isinstance(exchanges, list):
            continue
        converted_exchanges: list[dict[str, Any]] = []
        for index, exchange in enumerate(exchanges):
            if not isinstance(exchange, dict):
                converted_exchanges.append(exchange)
                continue
            direction = str(exchange.get("direction") or "output").strip().lower()
            converted, trace, issue = _convert_exchange(
                db,
                active_registry,
                exchange,
                direction=direction,
                location={"process_uuid": process_uuid, "process_exchange_index": index},
                unit_factor_by_group_and_name=unit_factors,
                reference_unit_by_group=reference_units,
            )
            converted_exchanges.append(converted)
            if trace:
                result.traces.append(trace)
            if issue:
                result.blocking.append(issue)
        result.process_exchanges[process_uuid] = converted_exchanges
    return result
