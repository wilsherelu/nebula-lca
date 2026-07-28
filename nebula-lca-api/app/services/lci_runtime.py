"""Runtime helpers for compressed ecoinvent LCI process vectors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from ..lci_vector_codec import unpack_lci_flow_key_ids, unpack_lci_vector
from ..models import FlowRecord, LciBiosphereFlowKey, LciProcessVector, LciVectorAxis, ReferenceProcess, UnitDefinition
from ..schemas import FlowPort, HybridGraph


@dataclass(frozen=True)
class ExpandedLciInventory:
    inventory: dict[int, float]
    provenance: list[dict[str, Any]]
    missing_vectors: list[str]


@dataclass(frozen=True)
class ExpandedLciGraph:
    graph: HybridGraph
    expanded_process_count: int
    expanded_port_count: int
    missing_vectors: list[str]
    provenance: list[dict[str, Any]]


def expand_graph_lci_inventory(db: Session, graph: Any) -> ExpandedLciInventory:
    """Expand lci_dataset graph nodes into a sparse flow_key_id inventory."""
    nodes = _get_graph_nodes(graph)
    lci_nodes = [
        node
        for node in nodes
        if _get_value(node, "node_kind", "nodeKind") == "lci_dataset"
        and _is_vector_backed_process_uuid(_get_value(node, "process_uuid", "processUuid"))
    ]
    process_uuids = sorted({_get_value(node, "process_uuid", "processUuid") for node in lci_nodes})
    vectors = load_process_vectors(db, process_uuids)
    processes = {
        row.process_uuid: row
        for row in db.query(ReferenceProcess).filter(ReferenceProcess.process_uuid.in_(process_uuids)).all()
    } if process_uuids else {}

    inventory: dict[int, float] = {}
    provenance: list[dict[str, Any]] = []
    missing_vectors: list[str] = []

    for node in lci_nodes:
        process_uuid = _get_value(node, "process_uuid", "processUuid")
        if not process_uuid:
            continue
        vector = vectors.get(process_uuid)
        if vector is None:
            missing_vectors.append(process_uuid)
            continue
        process = processes.get(process_uuid)
        demand_amount, demand_unit = _resolve_node_reference_demand(node)
        ref_amount, ref_unit = _resolve_process_reference(process)
        demand_in_ref_unit = _convert_unit_amount(db, demand_amount, demand_unit, ref_unit)
        scale = demand_in_ref_unit / abs(ref_amount or 1.0)

        for flow_key_id, amount in vector.items():
            inventory[flow_key_id] = inventory.get(flow_key_id, 0.0) + amount * scale
        provenance.append(
            {
                "process_uuid": process_uuid,
                "node_id": _get_value(node, "id"),
                "demand_amount": demand_amount,
                "demand_unit": demand_unit,
                "reference_amount": ref_amount,
                "reference_unit": ref_unit,
                "scale": scale,
                "nnz": len(vector),
            }
        )

    return ExpandedLciInventory(inventory=inventory, provenance=provenance, missing_vectors=missing_vectors)


def expand_lci_vectors_into_graph(db: Session, graph: HybridGraph) -> ExpandedLciGraph:
    """Return a graph copy with compressed LCI vectors expanded as biosphere ports."""
    expanded_graph = graph.model_copy(deep=True)
    process_uuids = sorted(
        {
            node.process_uuid
            for node in expanded_graph.nodes
            if node.node_kind == "lci_dataset" and _is_vector_backed_process_uuid(node.process_uuid)
        }
    )
    vectors = load_process_vectors(db, process_uuids)
    processes = {
        row.process_uuid: row
        for row in db.query(ReferenceProcess).filter(ReferenceProcess.process_uuid.in_(process_uuids)).all()
    } if process_uuids else {}
    needed_flow_key_ids = sorted({flow_key_id for vector in vectors.values() for flow_key_id in vector})
    flow_key_rows = {
        int(row.flow_key_id): row
        for row in db.query(LciBiosphereFlowKey)
        .filter(LciBiosphereFlowKey.flow_key_id.in_(needed_flow_key_ids))
        .all()
    } if needed_flow_key_ids else {}
    flow_rows = {
        row.flow_uuid: row
        for row in db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_([row.flow_uuid for row in flow_key_rows.values()])).all()
    } if flow_key_rows else {}

    expanded_process_count = 0
    expanded_port_count = 0
    missing_vectors: list[str] = []
    provenance: list[dict[str, Any]] = []

    for node in expanded_graph.nodes:
        if node.node_kind != "lci_dataset" or not _is_vector_backed_process_uuid(node.process_uuid):
            continue
        has_existing_biosphere = any(
            str(port.type or "") == "biosphere"
            for port in [*(node.inputs or []), *(node.outputs or [])]
        )
        vector = vectors.get(node.process_uuid)
        if vector is None:
            if not has_existing_biosphere:
                missing_vectors.append(node.process_uuid)
            continue

        process = processes.get(node.process_uuid)
        demand_amount, demand_unit = _resolve_node_reference_demand(node)
        ref_amount, ref_unit = _resolve_process_reference(process)
        demand_in_ref_unit = _convert_unit_amount(db, demand_amount, demand_unit, ref_unit)
        scale = demand_in_ref_unit / abs(ref_amount or 1.0)

        for flow_key_id, amount in sorted(vector.items()):
            flow_key = flow_key_rows.get(flow_key_id)
            if flow_key is None:
                continue
            flow = flow_rows.get(flow_key.flow_uuid)
            direction = "input" if flow_key.direction == "input" else "output"
            port = FlowPort(
                id=f"lci-vector::{node.id}::{flow_key_id}",
                flowUuid=flow_key.flow_uuid,
                name=flow.flow_name if flow is not None else flow_key.flow_uuid,
                unit=flow_key.canonical_unit,
                unitGroup=flow.unit_group if flow is not None else None,
                amount=amount * scale,
                type="biosphere",
                direction=direction,
                showOnNode=False,
                isProduct=False,
                sourceSystem=flow.source if flow is not None else None,
            )
            if direction == "input":
                node.inputs.append(port)
            else:
                node.outputs.append(port)
            expanded_port_count += 1

        expanded_process_count += 1
        provenance.append(
            {
                "process_uuid": node.process_uuid,
                "node_id": node.id,
                "scale": scale,
                "nnz": len(vector),
            }
        )

    if expanded_process_count or missing_vectors:
        metadata = dict(expanded_graph.metadata or {})
        metadata["lci_vector_runtime"] = {
            "expanded_process_count": expanded_process_count,
            "expanded_port_count": expanded_port_count,
            "missing_vectors": missing_vectors,
            "provenance": provenance,
        }
        expanded_graph.metadata = metadata

    return ExpandedLciGraph(
        graph=expanded_graph,
        expanded_process_count=expanded_process_count,
        expanded_port_count=expanded_port_count,
        missing_vectors=missing_vectors,
        provenance=provenance,
    )


def load_process_vectors(db: Session, process_uuids: list[str]) -> dict[str, dict[int, float]]:
    if not process_uuids:
        return {}
    rows = db.query(LciProcessVector).filter(LciProcessVector.process_uuid.in_(process_uuids)).all()
    axes: dict[int, list[int]] = {}
    axis_ids = sorted({int(row.axis_id) for row in rows if row.axis_id is not None})
    if axis_ids:
        for axis in db.query(LciVectorAxis).filter(LciVectorAxis.axis_id.in_(axis_ids)).all():
            flow_key_ids = unpack_lci_flow_key_ids(
                flow_key_ids_blob=axis.flow_key_ids_blob,
                nnz=axis.nnz,
                compression=axis.compression,
            )
            axes[int(axis.axis_id)] = flow_key_ids

    result: dict[str, dict[int, float]] = {}
    for row in rows:
        if row.axis_id is not None:
            flow_key_ids = axes.get(int(row.axis_id))
            if flow_key_ids is None:
                continue
            if row.flow_key_ids_blob is None:
                amounts = _unpack_amounts_only(row.amounts_blob, row.nnz, row.compression)
            else:
                _, amounts = unpack_lci_vector(
                    flow_key_ids_blob=row.flow_key_ids_blob,
                    amounts_blob=row.amounts_blob,
                    nnz=row.nnz,
                    compression=row.compression,
                )
        else:
            if row.flow_key_ids_blob is None:
                continue
            flow_key_ids, amounts = unpack_lci_vector(
                flow_key_ids_blob=row.flow_key_ids_blob,
                amounts_blob=row.amounts_blob,
                nnz=row.nnz,
                compression=row.compression,
            )
        result[row.process_uuid] = dict(zip(flow_key_ids, amounts))
    return result


def _unpack_amounts_only(amounts_blob: bytes, nnz: int, compression: str) -> list[float]:
    # Reuse the vector codec by pairing the amounts with a temporary zero axis.
    # Axis-backed vectors are not written by the current importer, but this keeps
    # the runtime tolerant if a future importer starts using lci_vector_axes.
    from array import array
    import zlib

    if compression == "zlib":
        raw = zlib.decompress(amounts_blob)
    elif compression == "none":
        raw = amounts_blob
    else:
        raise ValueError(f"Unsupported LCI vector compression: {compression}")
    arr = array("d")
    arr.frombytes(raw)
    amounts = list(arr)
    if len(amounts) != nnz:
        raise ValueError("LCI amounts blob length does not match nnz")
    return amounts


def inventory_with_flow_metadata(db: Session, inventory: dict[int, float]) -> list[dict[str, Any]]:
    if not inventory:
        return []
    rows = db.query(LciBiosphereFlowKey).filter(LciBiosphereFlowKey.flow_key_id.in_(sorted(inventory))).all()
    by_id = {int(row.flow_key_id): row for row in rows}
    flow_uuids = sorted({row.flow_uuid for row in rows if row.flow_uuid})
    flow_rows = {
        row.flow_uuid: row
        for row in db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(flow_uuids)).all()
    } if flow_uuids else {}
    result: list[dict[str, Any]] = []
    for flow_key_id in sorted(inventory):
        row = by_id.get(flow_key_id)
        if row is None:
            continue
        flow = flow_rows.get(row.flow_uuid)
        result.append(
            {
                "flow_key_id": flow_key_id,
                "flow_uuid": row.flow_uuid,
                "direction": row.direction,
                "unit": row.canonical_unit,
                "amount": inventory[flow_key_id],
                "compartment": row.compartment or (flow.compartment if flow is not None else None),
                "subcompartment": row.subcompartment or (flow.subcompartment if flow is not None else None),
            }
        )
    return result


def top_process_vector_exchanges(
    db: Session,
    process_uuid: str,
    limit: int = 10,
    page: int = 1,
    page_size: int | None = None,
    direction: str | None = None,
    q: str | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Return the largest elementary exchanges in a compressed LCI vector."""
    effective_page_size = max(1, min(page_size or limit, 100))
    effective_page = max(1, page)
    vectors = load_process_vectors(db, [process_uuid])
    vector = vectors.get(process_uuid)
    if not vector:
        return 0, []

    flow_key_ids = list(vector.keys())
    flow_key_rows = {
        int(row.flow_key_id): row
        for row in db.query(LciBiosphereFlowKey)
        .filter(LciBiosphereFlowKey.flow_key_id.in_(flow_key_ids))
        .all()
    }
    normalized_direction = direction if direction in {"input", "output"} else None
    search_text = str(q or "").strip().lower()
    candidates: list[tuple[int, float]] = []
    for flow_key_id, amount in vector.items():
        flow_key = flow_key_rows.get(flow_key_id)
        if flow_key is None:
            continue
        row_direction = "input" if str(flow_key.direction or "").lower() == "input" else "output"
        if normalized_direction is not None and row_direction != normalized_direction:
            continue
        candidates.append((flow_key_id, amount))

    offset = (effective_page - 1) * effective_page_size
    selected = sorted(candidates, key=lambda item: abs(item[1]), reverse=True)[offset: offset + effective_page_size]
    flow_uuids = sorted({row.flow_uuid for row in flow_key_rows.values() if row.flow_uuid})
    flow_rows = {
        row.flow_uuid: row
        for row in db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(flow_uuids)).all()
    } if flow_uuids else {}
    if search_text:
        filtered: list[tuple[int, float]] = []
        for flow_key_id, amount in candidates:
            flow_key = flow_key_rows.get(flow_key_id)
            flow = flow_rows.get(flow_key.flow_uuid) if flow_key is not None else None
            name_haystack = " ".join(
                str(part or "").lower()
                for part in (
                    flow.flow_name if flow is not None else None,
                    getattr(flow, "flow_name_en", None) if flow is not None else None,
                    flow_key.flow_uuid if flow_key is not None else None,
                    (
                        flow_key.compartment
                        or (flow.compartment if flow is not None else None)
                    ) if flow_key is not None else None,
                    (
                        flow_key.subcompartment
                        or (flow.subcompartment if flow is not None else None)
                    ) if flow_key is not None else None,
                )
            )
            if search_text in name_haystack:
                filtered.append((flow_key_id, amount))
        candidates = filtered
        offset = (effective_page - 1) * effective_page_size
        selected = sorted(candidates, key=lambda item: abs(item[1]), reverse=True)[offset: offset + effective_page_size]

    items: list[dict[str, Any]] = []
    for flow_key_id, amount in selected:
        flow_key = flow_key_rows.get(flow_key_id)
        if flow_key is None:
            continue
        flow = flow_rows.get(flow_key.flow_uuid)
        row_direction = "input" if str(flow_key.direction or "").lower() == "input" else "output"
        items.append(
            {
                "flow_key_id": flow_key_id,
                "flow_uuid": flow_key.flow_uuid,
                "flow_name": flow.flow_name if flow is not None else flow_key.flow_uuid,
                "direction": row_direction,
                "unit": flow_key.canonical_unit,
                "amount": amount,
                "compartment": flow_key.compartment or (flow.compartment if flow is not None else None),
                "subcompartment": flow_key.subcompartment or (flow.subcompartment if flow is not None else None),
            }
        )
    return len(candidates), items


def _get_graph_nodes(graph: Any) -> list[Any]:
    if isinstance(graph, dict):
        return list(graph.get("nodes") or [])
    return list(getattr(graph, "nodes", []) or [])


def _is_vector_backed_process_uuid(process_uuid: Any) -> bool:
    value = str(process_uuid or "").strip()
    return bool(value) and not value.startswith("lci_")


def _get_value(obj: Any, *keys: str) -> Any:
    if isinstance(obj, dict):
        for key in keys:
            if key in obj:
                return obj.get(key)
        return None
    for key in keys:
        if hasattr(obj, key):
            return getattr(obj, key)
    return None


def _resolve_node_reference_demand(node: Any) -> tuple[float, str]:
    outputs = _get_value(node, "outputs") or []
    for port in outputs:
        if _get_value(port, "isProduct", "is_product") or _get_value(port, "type") == "technosphere":
            return float(_get_value(port, "amount") or 1.0), str(_get_value(port, "unit") or "")
    return 1.0, ""


def _resolve_process_reference(process: ReferenceProcess | None) -> tuple[float, str]:
    if process is None or not isinstance(process.process_json, dict):
        return 1.0, ""
    data = process.process_json
    amount = float(data.get("reference_product_amount") or 1.0)
    unit = str(data.get("reference_product_unit") or "")
    return amount, unit


def _convert_unit_amount(db: Session, amount: float, from_unit: str, to_unit: str) -> float:
    if not from_unit or not to_unit or from_unit == to_unit:
        return amount
    from_defs = db.query(UnitDefinition).filter(UnitDefinition.unit_name == from_unit).all()
    to_defs = db.query(UnitDefinition).filter(UnitDefinition.unit_name == to_unit).all()
    for from_def in from_defs:
        for to_def in to_defs:
            if from_def.unit_group == to_def.unit_group:
                return amount * float(from_def.factor_to_reference) / float(to_def.factor_to_reference)
    raise ValueError(f"Cannot convert demand unit {from_unit} to reference unit {to_unit}")
