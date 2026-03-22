from __future__ import annotations

from .schemas import HybridGraph


def normalize_graph_units_to_reference(
    graph: HybridGraph,
    *,
    unit_factor_by_group_and_name: dict[tuple[str, str], float],
    reference_unit_by_group: dict[str, str],
) -> HybridGraph:
    graph_dict = graph.model_dump(mode="python")
    flow_unit_group: dict[str, str] = {}

    for node in graph_dict.get("nodes", []):
        is_market = (
            str(node.get("node_kind") or "") == "unit_process"
            and str(node.get("process_uuid") or "").startswith("market_")
        )
        if not is_market:
            continue
        techno_inputs = [port for port in node.get("inputs", []) if port.get("type") != "biosphere"]
        techno_outputs = [port for port in node.get("outputs", []) if port.get("type") != "biosphere"]
        canonical = (techno_outputs[0] if techno_outputs else None) or (techno_inputs[0] if techno_inputs else None)
        if not canonical:
            continue
        canonical_unit = canonical.get("unit")
        canonical_group = canonical.get("unitGroup")
        if not canonical_group:
            for candidate in techno_outputs + techno_inputs:
                candidate_group = candidate.get("unitGroup")
                if candidate_group:
                    canonical_group = candidate_group
                    break
        if not canonical_group:
            flow_uuid = canonical.get("flowUuid")
            if flow_uuid:
                canonical_group = flow_unit_group.get(flow_uuid)
        for port in techno_inputs + techno_outputs:
            if canonical_unit:
                port["unit"] = canonical_unit
            if canonical_group:
                port["unitGroup"] = canonical_group

    for node in graph_dict.get("nodes", []):
        for port_key in ("inputs", "outputs", "emissions"):
            for port in node.get(port_key, []):
                flow_uuid = port.get("flowUuid")
                unit_group = port.get("unitGroup")
                if flow_uuid and unit_group:
                    flow_unit_group[flow_uuid] = unit_group

    # Backfill missing unitGroup by flow UUID so same flow is converted consistently.
    # This fixes mixed rows like market inputs where one row lost unitGroup in UI state.
    for node in graph_dict.get("nodes", []):
        for port_key in ("inputs", "outputs", "emissions"):
            for port in node.get(port_key, []):
                flow_uuid = port.get("flowUuid")
                if not flow_uuid:
                    continue
                if port.get("unitGroup"):
                    continue
                inferred = flow_unit_group.get(flow_uuid)
                if inferred:
                    port["unitGroup"] = inferred

    def convert_amount(value: float, from_unit: str, unit_group: str) -> float:
        src_factor = unit_factor_by_group_and_name.get((unit_group, from_unit))
        if src_factor is None:
            return value
        return value * src_factor

    for node in graph_dict.get("nodes", []):
        for port_key in ("inputs", "outputs", "emissions"):
            for port in node.get(port_key, []):
                unit_group = port.get("unitGroup")
                unit = port.get("unit")
                if not unit_group or not unit:
                    continue
                reference_unit = reference_unit_by_group.get(unit_group)
                if not reference_unit:
                    continue
                amount = float(port.get("amount", 0) or 0)
                port["amount"] = convert_amount(amount, unit, unit_group)
                if port_key == "outputs":
                    sale_amount = float(port.get("externalSaleAmount", 0) or 0)
                    port["externalSaleAmount"] = convert_amount(sale_amount, unit, unit_group)
                port["unit"] = reference_unit

    for edge in graph_dict.get("exchanges", []):
        flow_uuid = edge.get("flowUuid")
        unit_group = flow_unit_group.get(flow_uuid or "")
        edge_unit = edge.get("unit")
        if not unit_group or not edge_unit:
            if str(edge.get("quantityMode") or "") == "single":
                amount = float(edge.get("amount", 0) or 0)
                edge["providerAmount"] = amount
                edge["consumerAmount"] = amount
            continue
        reference_unit = reference_unit_by_group.get(unit_group)
        if not reference_unit:
            if str(edge.get("quantityMode") or "") == "single":
                amount = float(edge.get("amount", 0) or 0)
                edge["providerAmount"] = amount
                edge["consumerAmount"] = amount
            continue
        amount = float(edge.get("amount", 0) or 0)
        provider_amount = float(edge.get("providerAmount", amount) or 0)
        consumer_amount = float(edge.get("consumerAmount", amount) or 0)
        edge["amount"] = convert_amount(amount, edge_unit, unit_group)
        edge["providerAmount"] = convert_amount(provider_amount, edge_unit, unit_group)
        edge["consumerAmount"] = convert_amount(consumer_amount, edge_unit, unit_group)
        if str(edge.get("quantityMode") or "") == "single":
            # Single-mode edges are modeled as one quantity; keep all three fields identical
            # so later HybridEdge validation cannot be tripped by stale UI-carried values.
            edge["providerAmount"] = edge["amount"]
            edge["consumerAmount"] = edge["amount"]
        edge["unit"] = reference_unit

    return HybridGraph.model_validate(graph_dict)
