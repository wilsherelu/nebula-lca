"""Validate a closed, exact saved supply network before attaching it to a consumer."""
from __future__ import annotations

from ..schemas import HybridGraph
from .provider_background_leaf import ProviderBackgroundLeafError


def validate_supply_network(graph: HybridGraph, source_node_id: str, reference_port_id: str):
    def refuse(code, message, **details):
        raise ProviderBackgroundLeafError(code, message, **details)

    if len(graph.nodes) > 256 or len(graph.exchanges) > 2048:
        refuse("PROJECT_BACKGROUND_RESOURCE_LIMIT", "The saved supply network exceeds expansion limits.")
    nodes = {node.id: node for node in graph.nodes}
    if len(nodes) != len(graph.nodes) or len({node.process_uuid for node in graph.nodes}) != len(nodes):
        refuse("PROJECT_BACKGROUND_SOURCE_INVALID", "Supply node and process identities must be unique.")
    root = nodes.get(source_node_id)
    if root is None:
        refuse("PROJECT_BACKGROUND_SOURCE_INVALID", "The selected source process is absent.")
    products = [port for port in root.outputs if port.type == "technosphere" and port.isProduct]
    if len(products) != 1 or products[0].id != reference_port_id:
        refuse("PROJECT_BACKGROUND_REFERENCE_INVALID", "The source must have one exact reference product output.")
    incoming = {}
    used_outputs = set()
    upstream = {key: set() for key in nodes}
    identity = ("flowUuid", "flow_source_namespace", "flow_version", "flow_property_uuid",
                "flow_property_version", "unit_group_uuid", "unit_group_version")
    for edge in graph.exchanges:
        source_id = edge.source_port_id or (edge.sourceHandle or "").removeprefix("out:")
        target_id = edge.target_port_id or (edge.targetHandle or "").removeprefix("in:")
        source = nodes.get(edge.fromNode)
        target = nodes.get(edge.toNode)
        output = next((p for p in source.outputs if p.id == source_id), None) if source else None
        input_port = next((p for p in target.inputs if p.id == target_id), None) if target else None
        if (output is None or input_port is None or output.type != "technosphere"
                or input_port.type != "technosphere"
                or any(getattr(output, f) != getattr(input_port, f) for f in identity)):
            refuse("PROJECT_BACKGROUND_LINK_INVALID", "An internal supply link lacks matching exact port identities.", edge_id=edge.id)
        key = (edge.toNode, target_id)
        if key in incoming:
            refuse("PROJECT_BACKGROUND_LINK_AMBIGUOUS", "A supply input has more than one provider.", edge_id=edge.id)
        incoming[key] = edge
        used_outputs.add((edge.fromNode, source_id))
        upstream[edge.toNode].add(edge.fromNode)
    for node in graph.nodes:
        for port in node.inputs:
            if port.type == "technosphere" and (node.id, port.id) not in incoming:
                refuse("PROJECT_BACKGROUND_NOT_CLOSED_LEAF", "The source has an open supply dependency.", exchange_id=f"{node.id}::{port.id}")
        for port in node.outputs:
            if port.type == "technosphere" and (node.id, port.id) not in used_outputs and (node.id, port.id) != (source_node_id, reference_port_id):
                refuse("PROJECT_BACKGROUND_UNALLOCATED_OUTPUT", "A supply network has another unresolved product output.", exchange_id=f"{node.id}::{port.id}")
    reached = set()
    pending = [source_node_id]
    while pending:
        node_id = pending.pop()
        if node_id not in reached:
            reached.add(node_id)
            pending.extend(upstream[node_id] - reached)
    if reached != set(nodes):
        refuse("PROJECT_BACKGROUND_DISCONNECTED", "The source version includes processes outside the selected supply network.", node_ids=sorted(set(nodes) - reached))
    return root, products[0]
