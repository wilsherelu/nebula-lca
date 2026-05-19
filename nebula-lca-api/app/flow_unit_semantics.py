from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from .models import FlowRecord, UnitDefinition, UnitGroup


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _set(obj: Any, key: str, value: Any) -> None:
    if isinstance(obj, dict):
        obj[key] = value
    else:
        setattr(obj, key, value)


def _switch(port: Any) -> dict[str, Any]:
    value = _get(port, "unitGroupSwitch", None)
    if value is None:
        value = _get(port, "unit_group_switch", None)
    return value if isinstance(value, dict) else {}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _same_group(left: str, right: str) -> bool:
    return _clean(left).lower() == _clean(right).lower()


def _prop_get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        if key in row:
            return row.get(key, default)
        snake = []
        for char in key:
            if char.isupper():
                snake.append("_")
                snake.append(char.lower())
            else:
                snake.append(char)
        return row.get("".join(snake), default)
    return getattr(row, key, default)


def _flow_allocation_properties(flow_record: FlowRecord | None) -> list[Any]:
    if flow_record is None:
        return []
    rows = getattr(flow_record, "allocation_properties", None) or []
    return rows if isinstance(rows, list) else []


def _infer_switch_from_flow_properties(
    *,
    flow_uuid: str,
    flow_record: FlowRecord | None,
    current_unit_group: str,
    current_unit: str,
    flow_default_unit_group: str,
    flow_default_unit: str,
    reference_unit_by_group: dict[str, str],
) -> dict[str, Any]:
    for row in _flow_allocation_properties(flow_record):
        target_group = _clean(_prop_get(row, "targetUnitGroup"))
        if not target_group or not _same_group(target_group, current_unit_group):
            continue
        try:
            factor = float(_prop_get(row, "value"))
        except (TypeError, ValueError):
            factor = 0.0
        if factor <= 0:
            continue
        target_unit = _clean(_prop_get(row, "targetUnit") or reference_unit_by_group.get(target_group) or current_unit)
        basis_unit = _clean(_prop_get(row, "basisUnit") or reference_unit_by_group.get(flow_default_unit_group) or flow_default_unit)
        return {
            "sourceFlowUuid": flow_uuid,
            "sourceUnitGroup": flow_default_unit_group,
            "sourceUnit": flow_default_unit,
            "sourceReferenceUnit": basis_unit,
            "targetUnitGroup": target_group,
            "targetUnit": target_unit,
            "targetReferenceUnit": target_unit,
            "factor": factor,
            "source": _clean(_prop_get(row, "source")) or "flow_allocation_properties",
            "note": _clean(_prop_get(row, "note")),
            "inferredFromFlowAllocationProperties": True,
        }
    return {}


def _canonical_switch(sem: "FlowPortUnitSemantics") -> dict[str, Any]:
    switch = sem.unit_group_switch or {}
    return {
        "sourceFlowUuid": sem.flow_uuid,
        "sourceUnitGroup": sem.flow_default_unit_group,
        "sourceUnit": sem.flow_default_unit,
        "sourceReferenceUnit": sem.flow_default_unit,
        "targetUnitGroup": sem.current_unit_group,
        "targetUnit": _clean(switch.get("targetUnit") or switch.get("target_unit") or sem.current_unit),
        "targetReferenceUnit": _clean(
            switch.get("targetReferenceUnit")
            or switch.get("target_reference_unit")
            or switch.get("targetUnit")
            or switch.get("target_unit")
            or sem.current_unit
        ),
        "factor": float(switch.get("factor")),
        "source": _clean(switch.get("source")) or "user_declared",
        "note": _clean(switch.get("note")),
    }


def build_unit_reference_maps(
    db: Session,
) -> tuple[dict[tuple[str, str], float], dict[str, str]]:
    unit_factor_by_group_and_name: dict[tuple[str, str], float] = {}
    reference_unit_by_group: dict[str, str] = {}
    for row in db.query(UnitDefinition).all():
        unit_factor_by_group_and_name[(row.unit_group, row.unit_name)] = float(row.factor_to_reference)
        if row.is_reference and row.unit_group not in reference_unit_by_group:
            reference_unit_by_group[row.unit_group] = row.unit_name
    for group in db.query(UnitGroup).all():
        if group.reference_unit and group.name not in reference_unit_by_group:
            reference_unit_by_group[group.name] = group.reference_unit
    return unit_factor_by_group_and_name, reference_unit_by_group


def _unit_factor(
    unit_factor_by_group_and_name: dict[tuple[str, str], float],
    reference_unit_by_group: dict[str, str],
    unit_group: str,
    unit: str,
) -> float | None:
    unit_group = _clean(unit_group)
    unit = _clean(unit)
    if not unit_group or not unit:
        return None
    factor = unit_factor_by_group_and_name.get((unit_group, unit))
    if factor is not None:
        return float(factor)
    if reference_unit_by_group.get(unit_group) == unit:
        return 1.0
    return None


@dataclass(frozen=True)
class FlowPortUnitSemantics:
    flow_uuid: str
    flow_default_unit_group: str
    flow_default_unit: str
    current_unit_group: str
    current_unit: str
    current_amount: float
    unit_group_switch: dict[str, Any]
    result_factor_to_flow_default_unit: float | None
    amount_in_flow_default_unit: float | None
    ok: bool
    inferred_source_amount: float | None = None
    snapshot_source_amount: float | None = None
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "flow_uuid": self.flow_uuid,
            "flow_default_unit_group": self.flow_default_unit_group,
            "flow_default_unit": self.flow_default_unit,
            "current_unit_group": self.current_unit_group,
            "current_unit": self.current_unit,
            "current_amount": self.current_amount,
            "unit_group_switch": self.unit_group_switch,
            "result_factor_to_flow_default_unit": self.result_factor_to_flow_default_unit,
            "amount_in_flow_default_unit": self.amount_in_flow_default_unit,
            "inferred_source_amount": self.inferred_source_amount,
            "snapshot_source_amount": self.snapshot_source_amount,
            "ok": self.ok,
            "reason": self.reason,
        }


def resolve_flow_port_unit_semantics(
    db: Session,
    port: Any,
    *,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None = None,
    reference_unit_by_group: dict[str, str] | None = None,
) -> FlowPortUnitSemantics:
    if unit_factor_by_group_and_name is None or reference_unit_by_group is None:
        unit_factor_by_group_and_name, reference_unit_by_group = build_unit_reference_maps(db)

    flow_uuid = _clean(_get(port, "flowUuid", None) or _get(port, "flow_uuid", None))
    flow_record = db.get(FlowRecord, flow_uuid) if flow_uuid else None
    switch = _switch(port)

    flow_default_unit_group = _clean(
        getattr(flow_record, "unit_group", None)
        or switch.get("sourceUnitGroup")
        or switch.get("source_unit_group")
        or _get(port, "unitGroup", None)
        or _get(port, "unit_group", None)
    )
    flow_default_unit = _clean(
        getattr(flow_record, "default_unit", None)
        or switch.get("sourceUnit")
        or switch.get("source_unit")
        or switch.get("sourceReferenceUnit")
        or switch.get("source_reference_unit")
        or _get(port, "unit", None)
    )

    current_unit_group = _clean(
        _get(port, "unitGroup", None)
        or _get(port, "unit_group", None)
        or switch.get("targetUnitGroup")
        or switch.get("target_unit_group")
        or flow_default_unit_group
    )
    current_unit = _clean(
        _get(port, "unit", None)
        or switch.get("targetUnit")
        or switch.get("target_unit")
        or flow_default_unit
    )
    try:
        current_amount = float(_get(port, "amount", 0) or 0)
    except (TypeError, ValueError):
        current_amount = 0.0

    if (
        current_unit
        and flow_default_unit
        and current_unit == flow_default_unit
        and (
            not current_unit_group
            or not flow_default_unit_group
            or _same_group(current_unit_group, flow_default_unit_group)
        )
    ):
        return FlowPortUnitSemantics(
            flow_uuid=flow_uuid,
            flow_default_unit_group=flow_default_unit_group or current_unit_group,
            flow_default_unit=flow_default_unit,
            current_unit_group=current_unit_group or flow_default_unit_group,
            current_unit=current_unit,
            current_amount=current_amount,
            unit_group_switch=switch,
            result_factor_to_flow_default_unit=1.0,
            amount_in_flow_default_unit=current_amount,
            ok=True,
        )

    current_factor = _unit_factor(
        unit_factor_by_group_and_name,
        reference_unit_by_group,
        current_unit_group,
        current_unit,
    )
    default_factor = _unit_factor(
        unit_factor_by_group_and_name,
        reference_unit_by_group,
        flow_default_unit_group,
        flow_default_unit,
    )
    if current_factor is None or current_factor <= 0:
        return FlowPortUnitSemantics(
            flow_uuid,
            flow_default_unit_group,
            flow_default_unit,
            current_unit_group,
            current_unit,
            current_amount,
            switch,
            None,
            None,
            False,
            reason="missing_current_unit_factor",
        )
    if default_factor is None or default_factor <= 0:
        return FlowPortUnitSemantics(
            flow_uuid,
            flow_default_unit_group,
            flow_default_unit,
            current_unit_group,
            current_unit,
            current_amount,
            switch,
            None,
            None,
            False,
            reason="missing_flow_default_unit_factor",
        )

    if _same_group(current_unit_group, flow_default_unit_group):
        result_factor = default_factor / current_factor
    else:
        raw_switch_factor = switch.get("factor")
        try:
            switch_factor = float(raw_switch_factor)
        except (TypeError, ValueError):
            switch_factor = 0.0
        target_group = _clean(switch.get("targetUnitGroup") or switch.get("target_unit_group"))
        if switch_factor <= 0 or not _same_group(target_group, current_unit_group):
            inferred_switch = _infer_switch_from_flow_properties(
                flow_uuid=flow_uuid,
                flow_record=flow_record,
                current_unit_group=current_unit_group,
                current_unit=current_unit,
                flow_default_unit_group=flow_default_unit_group,
                flow_default_unit=flow_default_unit,
                reference_unit_by_group=reference_unit_by_group,
            )
            if inferred_switch:
                switch = inferred_switch
                switch_factor = float(inferred_switch["factor"])
                target_group = _clean(inferred_switch["targetUnitGroup"])
            else:
                return FlowPortUnitSemantics(
                    flow_uuid,
                    flow_default_unit_group,
                    flow_default_unit,
                    current_unit_group,
                    current_unit,
                    current_amount,
                    switch,
                    None,
                    None,
                    False,
                    reason="missing_or_inconsistent_unit_group_switch",
                )
        result_factor = default_factor * switch_factor / current_factor

    if result_factor <= 0:
        return FlowPortUnitSemantics(
            flow_uuid,
            flow_default_unit_group,
            flow_default_unit,
            current_unit_group,
            current_unit,
            current_amount,
            switch,
            None,
            None,
            False,
            reason="invalid_result_factor",
        )

    amount_in_flow_default_unit = current_amount / result_factor
    inferred_source_amount: float | None = None
    snapshot_source_amount: float | None = None
    if switch and not _same_group(current_unit_group, flow_default_unit_group):
        raw_source_amount = switch.get("sourceAmount")
        if raw_source_amount is None:
            raw_source_amount = switch.get("source_amount")
        try:
            snapshot_source_amount = float(raw_source_amount)
        except (TypeError, ValueError):
            snapshot_source_amount = None
        source_reference_unit = _clean(
            switch.get("sourceReferenceUnit")
            or switch.get("source_reference_unit")
            or flow_default_unit
        )
        source_reference_factor = _unit_factor(
            unit_factor_by_group_and_name,
            reference_unit_by_group,
            flow_default_unit_group,
            source_reference_unit,
        )
        if source_reference_factor and source_reference_factor > 0:
            inferred_source_amount = amount_in_flow_default_unit * default_factor / source_reference_factor

    return FlowPortUnitSemantics(
        flow_uuid=flow_uuid,
        flow_default_unit_group=flow_default_unit_group,
        flow_default_unit=flow_default_unit,
        current_unit_group=current_unit_group,
        current_unit=current_unit,
        current_amount=current_amount,
        unit_group_switch=switch,
        result_factor_to_flow_default_unit=result_factor,
        amount_in_flow_default_unit=amount_in_flow_default_unit,
        ok=True,
        inferred_source_amount=inferred_source_amount,
        snapshot_source_amount=snapshot_source_amount,
    )


def collect_flow_default_unit_conversion_violations(
    graph: Any,
    db: Session,
    *,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None = None,
    reference_unit_by_group: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    if unit_factor_by_group_and_name is None or reference_unit_by_group is None:
        unit_factor_by_group_and_name, reference_unit_by_group = build_unit_reference_maps(db)

    nodes = graph.get("nodes", []) if isinstance(graph, dict) else getattr(graph, "nodes", [])
    violations: list[dict[str, Any]] = []
    for node in nodes or []:
        node_id = _clean(_get(node, "id", ""))
        node_name = _clean(_get(node, "name", ""))
        process_uuid = _clean(_get(node, "process_uuid", None) or _get(node, "processUuid", ""))
        for bucket in ("inputs", "outputs", "emissions"):
            ports = _get(node, bucket, []) or []
            for port in ports:
                sem = resolve_flow_port_unit_semantics(
                    db,
                    port,
                    unit_factor_by_group_and_name=unit_factor_by_group_and_name,
                    reference_unit_by_group=reference_unit_by_group,
                )
                if sem.ok:
                    continue
                if _same_group(sem.current_unit_group, sem.flow_default_unit_group) and sem.reason not in {
                    "missing_current_unit_factor",
                    "missing_flow_default_unit_factor",
                }:
                    continue
                violations.append({
                    "node_id": node_id,
                    "node_name": node_name,
                    "process_uuid": process_uuid,
                    "port_id": _clean(_get(port, "id", "")),
                    "flow_uuid": sem.flow_uuid,
                    "flow_name": _clean(_get(port, "name", "")),
                    "direction": bucket,
                    "current_unit": sem.current_unit,
                    "current_unit_group": sem.current_unit_group,
                    "flow_default_unit": sem.flow_default_unit,
                    "flow_default_unit_group": sem.flow_default_unit_group,
                    "amount_in_flow_default_unit": sem.amount_in_flow_default_unit,
                    "inferred_source_amount": sem.inferred_source_amount,
                    "snapshot_source_amount": sem.snapshot_source_amount,
                    "reason": sem.reason,
                    "repair_target": "unit_group",
                })
    return violations


def normalize_graph_flow_unit_switches(graph: Any, db: Session) -> Any:
    """Persist backend-owned unit-group switch snapshots where resolvable.

    The port amount/unit/unitGroup remains the current modelling fact. This
    only canonicalizes the audit/conversion rule needed to derive the Flow
    default-unit amount for calculation and TIDAS export.
    """
    unit_factor_by_group_and_name, reference_unit_by_group = build_unit_reference_maps(db)
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else getattr(graph, "nodes", [])
    for node in nodes or []:
        for bucket in ("inputs", "outputs", "emissions"):
            ports = _get(node, bucket, []) or []
            for port in ports:
                sem = resolve_flow_port_unit_semantics(
                    db,
                    port,
                    unit_factor_by_group_and_name=unit_factor_by_group_and_name,
                    reference_unit_by_group=reference_unit_by_group,
                )
                if not sem.ok or _same_group(sem.current_unit_group, sem.flow_default_unit_group):
                    continue
                if not sem.unit_group_switch:
                    continue
                _set(port, "unitGroupSwitch", _canonical_switch(sem))
    return graph
