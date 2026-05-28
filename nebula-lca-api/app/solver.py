from collections import defaultdict

from .schemas import (
    HybridGraph,
    graph_exchange_type_to_flow_semantic,
    is_elementary_flow_semantic,
    is_waste_flow_semantic,
    normalize_flow_semantic,
)
from .allocation import calculate_product_allocation


def _solver_runtime_flow_type(value: object) -> str:
    normalized = normalize_flow_semantic(value)
    if normalized == "elementary_flow":
        return "Elementary flow"
    if normalized == "waste_flow":
        return "Waste flow"
    return "Product flow"


def _port_float_attr(port: object, name: str) -> float | None:
    try:
        value = getattr(port, name)
    except AttributeError:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def to_tiangong_like(
    graph: HybridGraph,
    *,
    flow_type_by_uuid: dict[str, str] | None = None,
    flow_source_by_uuid: dict[str, str] | None = None,
) -> dict:
    processes = []
    flows_map: dict[str, dict] = {}
    exchanges = []
    links = []
    normalized_flow_type_by_uuid = {
        str(flow_uuid).strip().lower(): str(flow_type).strip()
        for flow_uuid, flow_type in (flow_type_by_uuid or {}).items()
        if str(flow_uuid).strip() and str(flow_type).strip()
    }
    normalized_flow_source_by_uuid = {
        str(flow_uuid).strip().lower(): str(source).strip()
        for flow_uuid, source in (flow_source_by_uuid or {}).items()
        if str(flow_uuid).strip() and str(source).strip()
    }

    node_by_id = {node.id: node for node in graph.nodes}
    input_port_amount_by_node_and_port: dict[tuple[str, str], float] = {}
    input_port_flow_uuid_by_node_and_port: dict[tuple[str, str], str] = {}
    input_port_is_product_by_node_and_port: dict[tuple[str, str], bool] = {}
    output_port_is_product_by_node_and_port: dict[tuple[str, str], bool] = {}
    exchange_id_by_node_and_port: dict[tuple[str, str], str] = {}
    product_meta_by_node_and_port: dict[tuple[str, str], dict[str, float]] = {}
    for node in graph.nodes:
        for port in node.inputs:
            input_port_amount_by_node_and_port[(node.id, port.id)] = float(port.amount or 0.0)
            input_port_flow_uuid_by_node_and_port[(node.id, port.id)] = str(port.flowUuid or "").strip()
            input_port_is_product_by_node_and_port[(node.id, port.id)] = bool(port.isProduct)
        for port in node.outputs:
            output_port_is_product_by_node_and_port[(node.id, port.id)] = bool(port.isProduct)

    edge_input_amount_by_target_and_flow: dict[tuple[str, str], float] = defaultdict(float)
    for edge in graph.exchanges:
        target_handle_or_port = edge.targetHandle or edge.target_port_id
        target_port_id = _port_id_from_handle(target_handle_or_port, "in")
        target_port_amount = 0.0
        target_port_flow_uuid = ""
        if target_port_id:
            target_port_amount = input_port_amount_by_node_and_port.get((edge.toNode, target_port_id), 0.0)
            target_port_flow_uuid = input_port_flow_uuid_by_node_and_port.get((edge.toNode, target_port_id), "")
        flow_uuid_key = str(target_port_flow_uuid or edge.flowUuid or "").strip()
        if not flow_uuid_key:
            continue
        if edge.quantityMode == "dual":
            input_amount = float(edge.consumerAmount or 0.0)
            if input_amount <= 0 and target_port_amount > 0:
                input_amount = target_port_amount
        else:
            # Solver contract: single-mode links are defined by consumer-side input amount.
            input_amount = target_port_amount if target_port_amount > 0 else float(edge.amount or edge.consumerAmount or 0.0)
        if input_amount > 0:
            edge_input_amount_by_target_and_flow[(edge.toNode, flow_uuid_key)] += input_amount

    for node in graph.nodes:
        allocation_fraction_by_port_id: dict[str, float] = {}
        product_conversion_factor_by_port_id: dict[str, float] = {}
        product_result_scale_by_port_id: dict[str, float] = {}
        baseline_fraction_by_port_id: dict[str, float] = {}
        allocation_weight_by_port_id: dict[str, float] = {}
        explicit_product_outputs = [
            port
            for port in node.outputs
            if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)) and bool(port.isProduct)
        ]
        product_outputs = explicit_product_outputs

        if product_outputs:
            allocation = calculate_product_allocation(
                product_outputs,
                process_uuid=str(node.process_uuid or node.id or ""),
            )
            if allocation.factors:
                allocation_fraction_by_port_id.update(allocation.factors)
            baseline_weights = allocation.weights or {}
            if not baseline_weights:
                baseline_weights = {
                    port.id: weight
                    for port in product_outputs
                    if (weight := _port_float_attr(port, "allocationWeight")) is not None
                }
            allocation_weight_by_port_id.update(baseline_weights)
            default_amounts = {
                port.id: float(port.amount or 0.0)
                for port in product_outputs
                if float(port.amount or 0.0) > 0
            }
            total_weight = sum(value for value in default_amounts.values() if value > 0)
            if total_weight > 0:
                for port in product_outputs:
                    amount = default_amounts.get(port.id)
                    if amount is not None and amount > 0:
                        baseline_fraction_by_port_id[port.id] = amount / total_weight
            for port in product_outputs:
                baseline = baseline_fraction_by_port_id.get(port.id)
                fraction = allocation_fraction_by_port_id.get(port.id)
                if baseline is not None and baseline > 0 and fraction is not None:
                    if fraction > 0:
                        product_conversion_factor_by_port_id[port.id] = baseline / fraction
                        product_result_scale_by_port_id[port.id] = fraction / baseline

        # Internal reference exchange selection for matrix builder compatibility.
        # Prefer explicit output product, then normal output, and finally a product-marked
        # input port so waste-treatment style processes can use waste input as reference.
        reference_port = (
            next(
                (
                    port
                    for port in explicit_product_outputs
                    if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type))
                ),
                None,
            )
            or next(
                (
                    port
                    for port in node.outputs
                    if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type))
                ),
                None,
            )
            or next(
                (
                    port
                    for port in node.inputs
                    if bool(port.isProduct)
                    and not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type))
                ),
                None,
            )
            or (node.outputs[0] if node.outputs else None)
            or (node.inputs[0] if node.inputs else None)
        )

        for port in node.inputs + node.outputs + node.emissions:
            if port.flowUuid not in flows_map:
                flow_type = normalized_flow_type_by_uuid.get(str(port.flowUuid).strip().lower())
                if not flow_type:
                    flow_type = graph_exchange_type_to_flow_semantic(port.type)
                source_system = str(getattr(port, "sourceSystem", "") or "").strip()
                if not source_system:
                    source_system = normalized_flow_source_by_uuid.get(str(port.flowUuid).strip().lower(), "")
                flows_map[port.flowUuid] = {
                    "flow_uuid": port.flowUuid,
                    "flow_name": port.name,
                    "flow_type": _solver_runtime_flow_type(flow_type),
                    "default_unit_uuid": port.unit,
                    "unit_group_uuid": f"unit_group::{port.unitGroup or port.unit}",
                    "source_system": source_system,
                }

        node_ports = node.inputs + node.outputs + node.emissions
        node_exchange_ids: dict[str, str] = {}

        for idx, port in enumerate(node_ports):
            exchange_id = f"{node.id}::{port.id or idx}"
            amount = port.amount
            if port.direction == "input":
                linked_amount = edge_input_amount_by_target_and_flow.get((node.id, port.flowUuid))
                if linked_amount is not None and linked_amount > 0:
                    amount = linked_amount
            exchanges.append(
                {
                    "exchange_id": exchange_id,
                    "process_uuid": node.process_uuid,
                    "flow_uuid": port.flowUuid,
                    "direction": port.direction,
                    "amount": amount,
                    "is_reference_product": bool(
                        reference_port is not None
                        and port.id == reference_port.id
                    ),
                    "allocation_fraction": allocation_fraction_by_port_id.get(port.id)
                    if port.direction == "output"
                    else None,
                    "allocation_weight": allocation_weight_by_port_id.get(port.id)
                    if port.direction == "output"
                    else None,
                    "allocation_weight_unit_group": getattr(port, "allocationWeightUnitGroup", None) or port.unitGroup
                    if port.direction == "output" and allocation_weight_by_port_id.get(port.id) is not None
                    else None,
                    "product_output_amount": float(port.amount or 0.0)
                    if port.direction == "output" and bool(port.isProduct)
                    else None,
                    "baseline_quantity_fraction": baseline_fraction_by_port_id.get(port.id)
                    if port.direction == "output"
                    else None,
                    "product_conversion_factor": product_conversion_factor_by_port_id.get(port.id)
                    if port.direction == "output"
                    else None,
                    "allocation_scale": product_result_scale_by_port_id.get(port.id)
                    if port.direction == "output"
                    else None,
                }
            )
            node_exchange_ids[port.id] = exchange_id
            exchange_id_by_node_and_port[(node.id, port.id)] = exchange_id
            if port.direction == "output" and port.id in product_conversion_factor_by_port_id:
                product_meta_by_node_and_port[(node.id, port.id)] = {
                    "allocation_fraction": allocation_fraction_by_port_id.get(port.id, 1.0),
                    "baseline_quantity_fraction": baseline_fraction_by_port_id.get(port.id, 1.0),
                    "allocation_weight": allocation_weight_by_port_id.get(port.id),
                    "product_conversion_factor": product_conversion_factor_by_port_id.get(port.id, 1.0),
                    "allocation_scale": product_result_scale_by_port_id.get(port.id, 1.0),
                    "product_output_amount": float(port.amount or 0.0),
                }

        reference_exchange_id = node_exchange_ids.get(reference_port.id, "") if reference_port else ""

        processes.append(
            {
                "process_uuid": node.process_uuid,
                "process_name": node.name,
                "reference_product_flow_uuid": reference_exchange_id,
                "node_kind": node.node_kind,
                "mode": node.mode,
                "lci_role": node.lci_role,
            }
        )

    for edge in graph.exchanges:
        target_port_amount = 0.0
        target_port_amount_present = False
        target_port_flow_uuid = ""
        target_handle_or_port = edge.targetHandle or edge.target_port_id
        target_port_id = _port_id_from_handle(target_handle_or_port, "in")
        source_handle_or_port = edge.sourceHandle or edge.source_port_id
        source_port_id = _port_id_from_handle(source_handle_or_port, "out")
        if target_port_id:
            target_key = (edge.toNode, target_port_id)
            target_port_amount_present = target_key in input_port_amount_by_node_and_port
            target_port_amount = input_port_amount_by_node_and_port.get(target_key, 0.0)
            target_port_flow_uuid = input_port_flow_uuid_by_node_and_port.get(target_key, "")

        consumer_amount = float(edge.consumerAmount or 0.0)
        provider_amount = float(edge.providerAmount or 0.0)
        amount = float(edge.amount or 0.0)
        edge_flow_uuid = str(target_port_flow_uuid or edge.flowUuid or "").strip()
        to_node = node_by_id.get(edge.toNode)
        is_normalized_market = bool(
            to_node is not None
            and to_node.node_kind == "market_process"
            and to_node.mode == "normalized"
        )

        if edge.quantityMode == "dual":
            # For normalized market nodes, consumer side must use target input-row normalized amount.
            if is_normalized_market:
                if not target_port_amount_present:
                    raise ValueError(
                        f"normalized market dual edge missing target input amount: edge_id={edge.id}, "
                        f"to_node={edge.toNode}, target_handle={target_handle_or_port or ''}"
                    )
                consumer_amount = target_port_amount
                if provider_amount <= 0:
                    provider_amount = consumer_amount
                if amount <= 0:
                    amount = consumer_amount
            else:
                if consumer_amount <= 0 and target_port_amount > 0:
                    consumer_amount = target_port_amount
                if provider_amount <= 0 and consumer_amount > 0:
                    provider_amount = consumer_amount
                if amount <= 0 and consumer_amount > 0:
                    amount = consumer_amount
        else:
            # Solver contract: single-mode links use consumer-side input amount.
            if target_port_amount > 0:
                amount = target_port_amount
            elif amount <= 0 and consumer_amount > 0:
                amount = consumer_amount
            if consumer_amount <= 0 and amount > 0:
                consumer_amount = amount
            if provider_amount <= 0 and amount > 0:
                provider_amount = amount

        edge_flow_type = normalized_flow_type_by_uuid.get(edge_flow_uuid.lower(), "") if edge_flow_uuid else ""
        is_waste_flow = is_waste_flow_semantic(edge_flow_type)
        source_port_is_product = bool(
            source_port_id
            and output_port_is_product_by_node_and_port.get((edge.fromNode, source_port_id))
        )
        target_port_is_product = bool(
            target_port_id
            and input_port_is_product_by_node_and_port.get((edge.toNode, target_port_id))
        )
        provider_process_uuid = _node_process_uuid(graph, edge.fromNode)
        consumer_process_uuid = _node_process_uuid(graph, edge.toNode)
        provider_product_exchange_id = (
            exchange_id_by_node_and_port.get((edge.fromNode, source_port_id))
            if source_port_id
            else None
        )
        provider_product_meta = (
            product_meta_by_node_and_port.get((edge.fromNode, source_port_id), {})
            if source_port_id
            else {}
        )
        if is_waste_flow and not source_port_is_product and not target_port_is_product and edge.quantityMode != "single":
            # Waste output means the upstream generator demands downstream treatment.
            provider_process_uuid = _node_process_uuid(graph, edge.toNode)
            consumer_process_uuid = _node_process_uuid(graph, edge.fromNode)
            provider_product_exchange_id = (
                exchange_id_by_node_and_port.get((edge.toNode, target_port_id))
                if target_port_id
                else None
            )
            provider_product_meta = (
                product_meta_by_node_and_port.get((edge.toNode, target_port_id), {})
                if target_port_id
                else {}
            )

        links.append(
            {
                "consumer_process_uuid": consumer_process_uuid,
                "provider_process_uuid": provider_process_uuid,
                "provider_product_port_id": source_port_id,
                "provider_product_exchange_id": provider_product_exchange_id,
                "provider_allocation_fraction": provider_product_meta.get("allocation_fraction"),
                "provider_baseline_quantity_fraction": provider_product_meta.get("baseline_quantity_fraction"),
                "provider_product_conversion_factor": provider_product_meta.get("product_conversion_factor"),
                "provider_allocation_scale": provider_product_meta.get("allocation_scale"),
                "provider_product_output_amount": provider_product_meta.get("product_output_amount"),
                "flow_uuid": edge_flow_uuid or edge.flowUuid,
                "flow_type": _solver_runtime_flow_type(edge_flow_type) if edge_flow_type else None,
                "is_waste_flow": is_waste_flow,
                "quantity_mode": edge.quantityMode,
                "provider_amount": provider_amount,
                "consumer_amount": consumer_amount,
                "amount": amount,
            }
        )

    return {
        "model": {
            "schema_version": "hybrid-0.2",
            "functional_unit": graph.functionalUnit,
        },
        "processes": processes,
        "flows": list(flows_map.values()),
        "exchanges": exchanges,
        "links": links,
    }


def _node_process_uuid(graph: HybridGraph, node_id: str) -> str:
    node = next((n for n in graph.nodes if n.id == node_id), None)
    return node.process_uuid if node else node_id


def _port_id_from_handle(handle_id: str | None, prefix: str) -> str:
    if not handle_id:
        return ""
    token = f"{prefix}:"
    if handle_id.startswith(token):
        return handle_id[len(token) :]
    if ":" in handle_id:
        return handle_id.split(":", 1)[1]
    return handle_id
