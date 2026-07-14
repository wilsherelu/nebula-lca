"""Deterministic inventory conversion with complete exchange traces."""

from __future__ import annotations

import math
from collections.abc import Sequence

from .contracts import (
    ConversionPackage,
    ConversionResult,
    ConversionTrace,
    DirectionalMapping,
    InventoryExchange,
    MappingStatus,
)


def _select_mappings(
    package: ConversionPackage,
    source_namespace: str,
    target_namespace: str,
) -> tuple[DirectionalMapping, ...]:
    selected: tuple[DirectionalMapping, ...] = ()
    if package.forward_mappings:
        first = package.forward_mappings[0]
        if first.source.namespace == source_namespace and first.target.namespace == target_namespace:
            selected = package.forward_mappings
    if package.reverse_mappings:
        first = package.reverse_mappings[0]
        if first.source.namespace == source_namespace and first.target.namespace == target_namespace:
            selected = package.reverse_mappings
    one_way = tuple(
        mapping
        for mapping in package.one_way_mappings
        if mapping.source.namespace == source_namespace
        and mapping.target.namespace == target_namespace
    )
    if not selected and not one_way:
        raise ValueError("package does not contain the requested conversion direction")
    return (*selected, *one_way)


def convert_inventory(
    inventory: Sequence[InventoryExchange],
    *,
    package: ConversionPackage,
    source_namespace: str,
    target_namespace: str,
) -> ConversionResult:
    """Convert all certified exchanges and retain every unmatched exchange."""

    mappings = _select_mappings(package, source_namespace, target_namespace)
    mapping_by_source = {mapping.source.key: mapping for mapping in mappings}
    target_amounts: dict[object, list[float]] = {}
    residual: list[InventoryExchange] = []
    traces: list[ConversionTrace] = []

    for exchange in inventory:
        mapping = mapping_by_source.get(exchange.flow.key)
        if mapping is None:
            residual.append(exchange)
            traces.append(
                ConversionTrace(
                    source=exchange,
                    status=MappingStatus.RESIDUAL,
                    warning_codes=("FLOW_OUTSIDE_CERTIFIED_CORE",),
                )
            )
            continue
        converted_exchange = InventoryExchange(
            flow=mapping.target,
            amount=exchange.amount * mapping.amount_factor,
        )
        target_amounts.setdefault(mapping.target, []).append(converted_exchange.amount)
        traces.append(
            ConversionTrace(
                source=exchange,
                status=MappingStatus.MAPPED,
                target=converted_exchange,
                mapping_evidence_ids=mapping.evidence_ids,
                warning_codes=("ONE_WAY_CANONICALIZATION",)
                if mapping in package.one_way_mappings
                else (),
            )
        )

    converted = tuple(
        InventoryExchange(flow=flow, amount=math.fsum(amounts))
        for flow, amounts in sorted(target_amounts.items(), key=lambda item: item[0].key)
    )
    return ConversionResult(
        converted=converted,
        residual=tuple(residual),
        traces=tuple(traces),
        package_hash=package.package_hash,
    )
