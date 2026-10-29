"""Attach an exact user-owned closed supply network to one consumption path."""
from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass
from typing import Callable

from sqlalchemy.orm import Session

from ..ingest import convert_unit_value
from ..provider_schemas import ProviderElementaryFlowRef, ProviderProjectBackgroundPin
from ..schemas import HybridEdge, HybridGraph
from .graph_storage import compute_graph_hash_from_graph
from .provider_background_leaf import ProviderBackgroundLeafError
from .provider_tidas_process_snapshot import canonical_hash
from .provider_tidas_snapshot import TidasFlowSnapshot
from .provider_project_network import validate_supply_network


@dataclass(frozen=True)
class ProjectBackgroundExpansion:
    graph: HybridGraph
    elementary_refs: tuple[ProviderElementaryFlowRef, ...]
    receipt_drafts: tuple[dict, ...]
    pins_hash: str | None


def _refuse(code: str, message: str, **details: object) -> None:
    raise ProviderBackgroundLeafError(code, message, **details)


def expand_project_background_pins(
    db: Session,
    *,
    graph: HybridGraph,
    pins: list[ProviderProjectBackgroundPin],
    get_snapshot: Callable[[Session, str, int], object],
    flow_snapshot: TidasFlowSnapshot | None,
) -> ProjectBackgroundExpansion:
    if not pins:
        return ProjectBackgroundExpansion(graph, (), (), None)
    ordered = sorted(pins, key=lambda pin: pin.consumer_exchange_id)
    if len({pin.consumer_exchange_id for pin in ordered}) != len(ordered):
        _refuse("PROJECT_BACKGROUND_DUPLICATE", "One consumer input may have only one project supply pin.")
    expanded = graph.model_copy(deep=True)
    original_ports = {
        f"{node.id}::{port.id}": (node, port)
        for node in graph.nodes for port in node.inputs
        if port.type == "technosphere" and port.direction == "input"
    }
    connected = {
        f"{edge.toNode}::{(edge.target_port_id or edge.targetHandle or '').removeprefix('in:')}"
        for edge in graph.exchanges
    }
    consumer_processes = {node.process_uuid for node in graph.nodes}
    existing_processes = set(consumer_processes)
    existing_names = {node.name for node in graph.nodes}
    elementary_refs: list[ProviderElementaryFlowRef] = []
    drafts: list[dict] = []

    for pin in ordered:
        entry = original_ports.get(pin.consumer_exchange_id)
        if entry is None or pin.consumer_exchange_id in connected:
            _refuse("PROJECT_BACKGROUND_PORT_INVALID", "Supply pin must target one open technosphere input.", consumer_exchange_id=pin.consumer_exchange_id)
        consumer_node, consumer = entry
        if not math.isfinite(consumer.amount) or consumer.amount <= 0:
            _refuse("PROJECT_BACKGROUND_PORT_INVALID", "Supply pin must target a positive input.")
        snapshot = get_snapshot(db, pin.project_id, pin.version)
        if snapshot.graph_hash != pin.expected_graph_hash:
            _refuse("PROJECT_BACKGROUND_VERSION_CHANGED", "The selected source model version changed.", expected=pin.expected_graph_hash, actual=snapshot.graph_hash)
        source_graph = snapshot.graph
        source, reference = validate_supply_network(source_graph, pin.source_node_id, pin.reference_port_id)
        if any(node.process_uuid in consumer_processes for node in source_graph.nodes):
            _refuse("PROJECT_BACKGROUND_SOURCE_INVALID", "The selected source process is absent or duplicates a consumer process.")
        identity_fields = (
            "flowUuid", "flow_source_namespace", "flow_version", "flow_property_uuid",
            "flow_property_version", "unit_group_uuid", "unit_group_version",
        )
        mismatches = {field: {"consumer": getattr(consumer, field), "provider": getattr(reference, field)}
                      for field in identity_fields if getattr(consumer, field) != getattr(reference, field)}
        if mismatches:
            _refuse("PROJECT_BACKGROUND_FLOW_MISMATCH", "The supply reference Flow identity differs from the input.", mismatches=mismatches)
        try:
            converted = convert_unit_value(db, value=1.0, from_unit=consumer.unit,
                                           to_unit=reference.unit, unit_group=consumer.unitGroup)
        except ValueError as exc:
            _refuse("PROJECT_BACKGROUND_UNIT_MISMATCH", "Supply and input units are not convertible.", reason=str(exc))
        factor = float(converted["converted_value"])
        if not math.isclose(factor, pin.provider_amount_per_consumer_unit, rel_tol=1e-12):
            _refuse("PROJECT_BACKGROUND_CONVERSION_CHANGED", "The pinned unit conversion differs from the engine unit catalog.", expected=pin.provider_amount_per_consumer_unit, actual=factor)
        token = canonical_hash(pin.model_dump(mode="json"))[:16]
        added_nodes = []
        node_ids = {node.id: f"project-background-{token}" if node.id == source.id
                    else f"project-background-{token}-{index}"
                    for index, node in enumerate(source_graph.nodes)}
        # The same saved supply version can serve several open inputs. Keep each edge's activity
        # distinct in the solver while retaining the exact source process identity in the receipt.
        for original in source_graph.nodes:
            added = deepcopy(original)
            added.id = node_ids[original.id]
            if added.process_uuid in existing_processes:
                added.process_uuid = f"{original.process_uuid}::project-background::{token}"
            if added.process_uuid in existing_processes or added.id in {n.id for n in expanded.nodes}:
                _refuse("PROJECT_BACKGROUND_SOURCE_INVALID", "The selected source process instance is ambiguous.")
            if added.name in existing_names:
                added.name = f"{original.name} ({token})"
            added.hidden = True
            added_nodes.append((original, added))
            expanded.nodes.append(added)
            existing_processes.add(added.process_uuid)
            existing_names.add(added.name)
        for index, original_edge in enumerate(source_graph.exchanges):
            edge = deepcopy(original_edge)
            edge.id = f"project-background-internal-{token}-{index}"
            edge.fromNode = node_ids[original_edge.fromNode]
            edge.toNode = node_ids[original_edge.toNode]
            expanded.exchanges.append(edge)
        supply_amount = float(consumer.amount) * factor
        expanded.exchanges.append(HybridEdge(
            id=f"project-background-link-{token}", fromNode=node_ids[source.id], toNode=consumer_node.id,
            sourcePortId=reference.id, targetPortId=consumer.id,
            sourceHandle=f"out:{reference.id}", targetHandle=f"in:{consumer.id}",
            flowUuid=reference.flowUuid, flowName=reference.name,
            quantityMode="single" if factor == 1.0 else "dual",
            amount=float(consumer.amount), providerAmount=supply_amount,
            consumerAmount=float(consumer.amount), unit=consumer.unit,
            providerUnit=reference.unit, consumerUnit=consumer.unit, type="technosphere",
        ))
        for original, added in added_nodes:
            _append_elementary_refs(original, added, flow_snapshot, elementary_refs)
            drafts.append({"pin": pin.model_dump(mode="json"), "source_graph_hash": snapshot.graph_hash,
                       "source_process_uuid": added.process_uuid,
                       "source_model_process_uuid": original.process_uuid, "expanded_node_id": added.id,
                       "source_node_id": original.id, "expanded_network_node_ids": list(node_ids.values()),
                       "expansion_mode": "closed_leaf" if len(added_nodes) == 1 else "closed_network",
                       "reference_flow_uuid": reference.flowUuid, "reference_unit": reference.unit,
                       "consumer_unit": consumer.unit, "unit_conversion_factor": factor})
    return ProjectBackgroundExpansion(expanded, tuple(elementary_refs), tuple(drafts),
                                      canonical_hash([pin.model_dump(mode="json") for pin in ordered]))


def _append_elementary_refs(source, added, flow_snapshot, elementary_refs):
        for port in [*source.inputs, *source.outputs]:
            if port.type != "biosphere":
                continue
            if not all((port.flow_source_namespace, port.flow_version, port.flow_property_uuid,
                        port.flow_property_version, port.unit_group_uuid, port.unit_group_version)):
                _refuse("PROJECT_BACKGROUND_ELEMENTARY_IDENTITY_INCOMPLETE", "A source elementary Flow has no exact identity.")
            if flow_snapshot is None or port.flow_source_namespace != "tiangong_open_data":
                _refuse("PROJECT_BACKGROUND_ELEMENTARY_UNRESOLVED", "The source elementary Flow needs an exact configured catalog snapshot.")
            flow = flow_snapshot.resolve(port.flowUuid, port.flow_version)
            if flow is None or flow.get("source_namespace") != port.flow_source_namespace or not flow.get("compartment"):
                _refuse("PROJECT_BACKGROUND_ELEMENTARY_UNRESOLVED", "The source elementary Flow is absent from the exact catalog snapshot.")
            elementary_refs.append(ProviderElementaryFlowRef(
                exchange_id=f"{added.id}::{port.id}", source_namespace=port.flow_source_namespace,
                flow_uuid=port.flowUuid, version=port.flow_version,
                flow_property_uuid=port.flow_property_uuid,
                flow_property_version=port.flow_property_version,
                unit_group_uuid=port.unit_group_uuid,
                unit_group_version=port.unit_group_version, unit=port.unit,
                direction=port.direction, compartment=flow["compartment"],
            ))


def build_project_background_receipts(expansion: ProjectBackgroundExpansion, activity_by_process: dict[str, float]) -> list[dict]:
    return [{**draft, "activity_amount": activity_by_process[draft["source_process_uuid"]],
             "claim_limit": ("user_declared_partial_background_closed_network"
                             if draft["expansion_mode"] == "closed_network"
                             else "user_declared_partial_background_leaf_only")}
            for draft in expansion.receipt_drafts]
