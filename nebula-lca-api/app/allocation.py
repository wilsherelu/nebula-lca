from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ALLOCATION_TOLERANCE = 0.01


@dataclass
class AllocationWarning:
    message: str
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class AllocationResult:
    factors: dict[str, float] | None
    warnings: list[AllocationWarning] = field(default_factory=list)
    manual_required: bool = False
    method: str = "none"
    weights: dict[str, float] = field(default_factory=dict)


def _port_id(port: Any) -> str:
    if isinstance(port, dict):
        return str(port.get("id") or "")
    return str(getattr(port, "id", "") or "")


def _port_get(port: Any, key: str, default: Any = None) -> Any:
    if isinstance(port, dict):
        return port.get(key, default)
    return getattr(port, key, default)


def _allocation_basis(port: Any) -> dict[str, Any]:
    raw = _port_get(port, "allocationBasis", None)
    if raw is None:
        raw = _port_get(port, "allocation_basis", None)
    if isinstance(raw, dict):
        return raw
    return {}


def _basis_method(port: Any) -> str:
    basis = _allocation_basis(port)
    value = (
        basis.get("method")
        or basis.get("propertyType")
        or basis.get("property_type")
        or basis.get("type")
        or basis.get("basis")
    )
    raw = str(value or "").strip()
    if raw in {"heating_value_lhv", "heating_value_hhv"}:
        return "heating_value"
    if raw in {"dry_matter", "purity", "carbon_content", "economic_value"}:
        return "custom_conversion"
    return raw


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _amount(port: Any) -> float:
    try:
        return abs(float(_port_get(port, "amount", 0) or 0))
    except (TypeError, ValueError):
        return 0.0


def _allocation_weight(port: Any) -> float | None:
    try:
        parsed = float(_port_get(port, "allocationWeight", None))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _unit_group(port: Any) -> str:
    return str(_port_get(port, "unitGroup", None) or _port_get(port, "unit_group", "") or "").strip()


def _unit(port: Any) -> str:
    return str(_port_get(port, "unit", "") or "").strip()


def _current_unit(port: Any) -> str:
    switch = _port_get(port, "unitGroupSwitch", None)
    if switch is None:
        switch = _port_get(port, "unit_group_switch", None)
    if isinstance(switch, dict):
        target = switch.get("targetUnit") or switch.get("target_unit")
        if str(target or "").strip():
            return str(target).strip()
    return _unit(port)


_PREFERRED_UNIT_GROUP_BY_UNIT = {
    "pg": "Units of mass",
    "ng": "Units of mass",
    "ug": "Units of mass",
    "mg": "Units of mass",
    "g": "Units of mass",
    "kg": "Units of mass",
    "t": "Units of mass",
    "m": "Units of length",
    "km": "Units of length",
    "mm": "Units of length",
    "cm": "Units of length",
    "m2": "Units of area",
    "m²": "Units of area",
    "l": "Units of volume",
    "ml": "Units of volume",
    "m3": "Units of volume",
    "m³": "Units of volume",
    "j": "Units of energy",
    "kj": "Units of energy",
    "mj": "Units of energy",
    "gj": "Units of energy",
    "wh": "Units of energy",
    "kwh": "Units of energy",
}


def _current_unit_group(port: Any) -> str:
    switch = _port_get(port, "unitGroupSwitch", None)
    if switch is None:
        switch = _port_get(port, "unit_group_switch", None)
    if isinstance(switch, dict):
        target = switch.get("targetUnitGroup") or switch.get("target_unit_group")
        if str(target or "").strip():
            return str(target).strip()
    explicit_unit_group = _unit_group(port)
    if explicit_unit_group:
        return explicit_unit_group
    return _PREFERRED_UNIT_GROUP_BY_UNIT.get(_unit(port).lower(), "")


def collect_multi_product_unit_group_violations(graph: Any) -> list[dict[str, Any]]:
    """Find multi-product nodes whose current product unit groups differ.

    This is a runtime/export gate. Project versions may still save these drafts.
    """
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else getattr(graph, "nodes", [])
    violations: list[dict[str, Any]] = []
    for node in nodes or []:
        node_kind = str(_port_get(node, "node_kind", _port_get(node, "nodeKind", "")) or "")
        if node_kind not in {"unit_process", "market_process"}:
            continue
        outputs = _port_get(node, "outputs", []) or []
        product_outputs = [
            port
            for port in outputs
            if bool(_port_get(port, "isProduct", False)) and str(_port_get(port, "type", "") or "") != "biosphere"
        ]
        if len(product_outputs) <= 1:
            continue
        groups = [_current_unit_group(port) or "unknown" for port in product_outputs]
        if len(set(groups)) <= 1:
            continue
        user_factors: list[float] = []
        user_factors_valid = True
        for port in product_outputs:
            raw_factor = _port_get(port, "allocationFactor", None)
            if raw_factor is None:
                user_factors_valid = False
                break
            try:
                parsed_factor = float(raw_factor)
            except (TypeError, ValueError):
                user_factors_valid = False
                break
            if parsed_factor < 0:
                user_factors_valid = False
                break
            user_factors.append(parsed_factor)
        if user_factors_valid and abs(sum(user_factors) - 1.0) < ALLOCATION_TOLERANCE:
            continue
        base = product_outputs[0]
        base_group = groups[0]
        violations.append({
            "node_id": _port_get(node, "id", ""),
            "process_uuid": _port_get(node, "process_uuid", _port_get(node, "processUuid", "")),
            "node_name": _port_get(node, "name", ""),
            "base_flow": {
                "port_id": _port_id(base),
                "flow_uuid": _port_get(base, "flowUuid", ""),
                "flow_name": _port_get(base, "name", ""),
                "unit": _unit(base),
                "unit_group": base_group,
            },
            "mismatched_flows": [
                {
                    "port_id": _port_id(port),
                    "flow_uuid": _port_get(port, "flowUuid", ""),
                    "flow_name": _port_get(port, "name", ""),
                    "unit": _unit(port),
                    "unit_group": group,
                }
                for port, group in zip(product_outputs[1:], groups[1:])
                if group != base_group
            ],
        })
    return violations


def raise_for_multi_product_unit_group_violations(graph: Any) -> None:
    from fastapi import HTTPException

    violations = collect_multi_product_unit_group_violations(graph)
    if not violations:
        return
    raise HTTPException(
        status_code=400,
        detail={
            "code": "MULTI_PRODUCT_UNIT_GROUP_MISMATCH",
            "message": "Multi-product processes must use one current product unit group before calculation or export.",
            "violations": violations,
        },
    )


def _conversion_factor(
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None,
    unit_group: str,
    unit: str,
) -> float | None:
    if not unit_group or not unit:
        return 1.0
    if not unit_factor_by_group_and_name:
        return 1.0
    return unit_factor_by_group_and_name.get((unit_group, unit))


def _converted_amount(
    port: Any,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None,
) -> float | None:
    factor = _conversion_factor(unit_factor_by_group_and_name, _current_unit_group(port), _current_unit(port))
    if factor is None:
        return None
    return _amount(port) * factor


def _manual_result(process_uuid: str, message: str, context: dict[str, Any]) -> AllocationResult:
    return AllocationResult(
        factors=None,
        warnings=[AllocationWarning(message, {"process_uuid": process_uuid, **context})],
        manual_required=True,
    )


def _normalize_factors(weights: dict[str, float]) -> dict[str, float] | None:
    total = sum(value for value in weights.values() if value > 0)
    if total <= 0:
        return None
    return {port_id: value / total for port_id, value in weights.items() if value > 0}


def _manual_factor_allocation(
    product_outputs: list[Any],
    process_uuid: str,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None,
) -> AllocationResult | None:
    user_allocation: dict[str, float] = {}
    for port in product_outputs:
        factor = _port_get(port, "allocationFactor", None)
        if factor is None:
            continue
        try:
            parsed = float(factor)
        except (TypeError, ValueError):
            parsed = -1.0
        user_allocation[_port_id(port)] = parsed

    if not user_allocation:
        return None

    if len(user_allocation) != len(product_outputs) or any(value < 0 for value in user_allocation.values()):
        return _manual_result(
            process_uuid,
            f"Multi-product process {process_uuid}: user-specified allocation factors are incomplete or invalid, manual allocation required",
            {"user_allocation": user_allocation, "product_count": len(product_outputs)},
        )

    user_sum = sum(user_allocation.values())
    if abs(user_sum - 1.0) < ALLOCATION_TOLERANCE:
        quantity_weights = {
            _port_id(port): weight
            for port in product_outputs
            if (weight := _allocation_weight(port)) is not None
        }
        if len(quantity_weights) != len(product_outputs):
            quantity_result = _same_unit_group_allocation(product_outputs, process_uuid, unit_factor_by_group_and_name)
            quantity_weights = quantity_result.weights if quantity_result and quantity_result.weights else {}
        return AllocationResult(factors=user_allocation, method="manual_factor", weights=quantity_weights)

    return _manual_result(
        process_uuid,
        f"Multi-product process {process_uuid}: user-specified allocation factors sum to {user_sum:.4f} (expected 1.0), manual allocation required",
        {"user_allocation": user_allocation, "sum": user_sum},
    )


def _same_unit_group_allocation(
    product_outputs: list[Any],
    process_uuid: str,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None,
) -> AllocationResult | None:
    unit_groups = {_current_unit_group(port) for port in product_outputs if _current_unit_group(port)}
    if len(unit_groups) != 1:
        return None

    weights: dict[str, float] = {}
    for port in product_outputs:
        converted = _converted_amount(port, unit_factor_by_group_and_name)
        if converted is None:
            return _manual_result(
                process_uuid,
                f"Multi-product process {process_uuid}: unit conversion failed for one or more products, manual allocation required",
                {"unit_groups": list(unit_groups)},
            )
        weights[_port_id(port)] = converted

    factors = _normalize_factors(weights)
    if factors:
        return AllocationResult(factors=factors, method="quantity", weights=weights)
    return None


def _basis_number(basis: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _positive_float(basis.get(key))
        if value is not None:
            return value
    return None


def _basis_target_group(basis: dict[str, Any], fallback: str) -> str:
    return str(
        basis.get("targetUnitGroup")
        or basis.get("target_unit_group")
        or basis.get("target")
        or fallback
    ).strip()


def _explicit_basis_target_group(basis: dict[str, Any]) -> str:
    return str(
        basis.get("targetUnitGroup")
        or basis.get("target_unit_group")
        or basis.get("target")
        or ""
    ).strip()


def _basis_weight(
    port: Any,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None,
    preferred_method: str | None,
    preferred_target_group: str | None = None,
) -> tuple[float | None, str | None, str | None]:
    basis = _allocation_basis(port)
    method = _basis_method(port) or preferred_method or ""
    unit_group = _current_unit_group(port)
    converted = _converted_amount(port, unit_factor_by_group_and_name)
    if converted is None:
        return None, method, None

    if method == "density":
        target_group = _basis_target_group(basis, "Units of mass")
        if unit_group == target_group:
            return converted, method, target_group
        factor = _basis_number(basis, "value", "density", "factor", "conversionFactor", "conversion_factor")
        if factor is None:
            return None, method, target_group
        return converted * factor, method, target_group

    if method == "heating_value":
        target_group = _basis_target_group(basis, "Units of energy")
        if unit_group == target_group:
            return converted, method, target_group
        factor = _basis_number(basis, "value", "heatingValue", "heating_value", "factor", "conversionFactor", "conversion_factor")
        if factor is None:
            return None, method, target_group
        return converted * factor, method, target_group

    if method == "custom_conversion":
        target_group = _explicit_basis_target_group(basis) or str(preferred_target_group or "").strip()
        if not target_group:
            return None, method, None
        if unit_group == target_group:
            return converted, method, target_group
        factor = _basis_number(basis, "value", "factor", "conversionFactor", "conversion_factor")
        if factor is None:
            return None, method, target_group
        return converted * factor, method, target_group

    return None, method, None


def _basis_allocation(
    product_outputs: list[Any],
    process_uuid: str,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None,
) -> AllocationResult | None:
    methods = {_basis_method(port) for port in product_outputs if _basis_method(port)}
    preferred_method = None
    if "density" in methods:
        preferred_method = "density"
    elif "heating_value" in methods:
        preferred_method = "heating_value"
    elif "custom_conversion" in methods:
        preferred_method = "custom_conversion"
    if preferred_method is None:
        return None
    preferred_target_group = ""
    if preferred_method == "custom_conversion":
        preferred_target_group = next(
            (
                _explicit_basis_target_group(_allocation_basis(port))
                for port in product_outputs
                if _explicit_basis_target_group(_allocation_basis(port))
            ),
            "",
        )

    weights: dict[str, float] = {}
    target_groups: set[str] = set()
    for port in product_outputs:
        weight, method, target_group = _basis_weight(
            port,
            unit_factor_by_group_and_name,
            preferred_method,
            preferred_target_group,
        )
        if weight is None or not target_group:
            return _manual_result(
                process_uuid,
                f"Multi-product process {process_uuid}: allocation basis is incomplete, manual allocation required",
                {
                    "port_id": _port_id(port),
                    "method": method or preferred_method,
                    "unit_group": _current_unit_group(port),
                    "unit": _current_unit(port),
                },
            )
        weights[_port_id(port)] = weight
        target_groups.add(target_group)

    if len(target_groups) != 1:
        return _manual_result(
            process_uuid,
            f"Multi-product process {process_uuid}: allocation basis targets differ, manual allocation required",
            {"target_unit_groups": sorted(target_groups)},
        )

    factors = _normalize_factors(weights)
    if factors:
        return AllocationResult(factors=factors, method=preferred_method, weights=weights)
    return None


def calculate_product_allocation(
    product_outputs: list[Any],
    *,
    process_uuid: str,
    unit_factor_by_group_and_name: dict[tuple[str, str], float] | None = None,
) -> AllocationResult:
    if not product_outputs:
        return AllocationResult(factors={}, method="none")
    if len(product_outputs) == 1:
        return AllocationResult(factors={_port_id(product_outputs[0]): 1.0}, method="single_product")

    manual = _manual_factor_allocation(product_outputs, process_uuid, unit_factor_by_group_and_name)
    if manual is not None:
        return manual

    same_group = _same_unit_group_allocation(product_outputs, process_uuid, unit_factor_by_group_and_name)
    if same_group is not None:
        return same_group

    basis = _basis_allocation(product_outputs, process_uuid, unit_factor_by_group_and_name)
    if basis is not None:
        return basis

    unit_groups = sorted({_unit_group(port) or "unknown" for port in product_outputs})
    return _manual_result(
        process_uuid,
        f"Multi-product process {process_uuid}: products have different unit groups, manual allocation required",
        {"unit_groups": unit_groups, "product_count": len(product_outputs)},
    )
