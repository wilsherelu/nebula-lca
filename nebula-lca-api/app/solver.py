from collections import defaultdict

from .schemas import (
    HybridGraph,
    graph_exchange_type_to_flow_semantic,
    is_elementary_flow_semantic,
    is_waste_flow_semantic,
    normalize_flow_semantic,
)


def _solver_runtime_flow_type(value: object) -> str:
    normalized = normalize_flow_semantic(value)
    if normalized == "elementary_flow":
        return "Elementary flow"
    if normalized == "waste_flow":
        return "Waste flow"
    return "Product flow"


def to_tiangong_like(graph: HybridGraph, *, flow_type_by_uuid: dict[str, str] | None = None) -> dict:
    processes = []
    flows_map: dict[str, dict] = {}
    exchanges = []
    links = []
    normalized_flow_type_by_uuid = {
        str(flow_uuid).strip().lower(): str(flow_type).strip()
        for flow_uuid, flow_type in (flow_type_by_uuid or {}).items()
        if str(flow_uuid).strip() and str(flow_type).strip()
    }

    node_by_id = {node.id: node for node in graph.nodes}
    input_port_amount_by_node_and_port: dict[tuple[str, str], float] = {}
    input_port_flow_uuid_by_node_and_port: dict[tuple[str, str], str] = {}
    input_port_is_product_by_node_and_port: dict[tuple[str, str], bool] = {}
    output_port_is_product_by_node_and_port: dict[tuple[str, str], bool] = {}
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
        explicit_product_outputs = [
            port
            for port in node.outputs
            if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)) and bool(port.isProduct)
        ]
        product_outputs = explicit_product_outputs

        if product_outputs:
            # v1: unit-group physical allocation.
            unit_groups = {port.unitGroup for port in product_outputs if port.unitGroup}
            if len(unit_groups) <= 1:
                custom_factors = [port.allocationFactor for port in product_outputs]
                if any(f is not None for f in custom_factors):
                    positive_factor_sum = sum(float(f or 0.0) for f in custom_factors if float(f or 0.0) > 0)
                    if positive_factor_sum > 0:
                        for port in product_outputs:
                            factor = float(port.allocationFactor or 0.0)
                            if factor > 0:
                                allocation_fraction_by_port_id[port.id] = factor / positive_factor_sum
                else:
                    total_amount = sum(float(port.amount or 0.0) for port in product_outputs if float(port.amount or 0.0) > 0)
                    if total_amount > 0:
                        for port in product_outputs:
                            amount = float(port.amount or 0.0)
                            if amount > 0:
                                allocation_fraction_by_port_id[port.id] = amount / total_amount

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
                flows_map[port.flowUuid] = {
                    "flow_uuid": port.flowUuid,
                    "flow_name": port.name,
                    "flow_type": _solver_runtime_flow_type(flow_type),
                    "default_unit_uuid": port.unit,
                    "unit_group_uuid": f"unit_group::{port.unitGroup or port.unit}",
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
                }
            )
            node_exchange_ids[port.id] = exchange_id

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
        target_port_flow_uuid = ""
        target_handle_or_port = edge.targetHandle or edge.target_port_id
        target_port_id = _port_id_from_handle(target_handle_or_port, "in")
        source_handle_or_port = edge.sourceHandle or edge.source_port_id
        source_port_id = _port_id_from_handle(source_handle_or_port, "out")
        if target_port_id:
            target_port_amount = input_port_amount_by_node_and_port.get((edge.toNode, target_port_id), 0.0)
            target_port_flow_uuid = input_port_flow_uuid_by_node_and_port.get((edge.toNode, target_port_id), "")

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
                if target_port_amount <= 0:
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
        if is_waste_flow and not source_port_is_product and not target_port_is_product and edge.quantityMode != "single":
            # Waste output means the upstream generator demands downstream treatment.
            provider_process_uuid = _node_process_uuid(graph, edge.toNode)
            consumer_process_uuid = _node_process_uuid(graph, edge.fromNode)

        links.append(
            {
                "consumer_process_uuid": consumer_process_uuid,
                "provider_process_uuid": provider_process_uuid,
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
