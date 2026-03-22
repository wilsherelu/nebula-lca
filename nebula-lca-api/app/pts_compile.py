from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from typing import Any

from .pts_validate import PtsValidationResult, validate_pts_compile
from .schemas import (
    HybridGraph,
    HybridNode,
    graph_exchange_type_to_flow_semantic,
    is_elementary_flow_semantic,
)
from .solver_adapter import run_tiangong_pts_compile

PTS_COMPILE_SCHEMA_VERSION = "pts-compile-v6"


def _is_exposed(port: object) -> bool:
    internal_exposed = getattr(port, "internalExposed", None)
    if internal_exposed is not None:
        return bool(internal_exposed)
    external_visible = getattr(port, "externalVisible", None)
    if external_visible is not None:
        return bool(external_visible)
    # Backward-compatible fallback for payloads that only set visibility flag.
    show_on_node = getattr(port, "showOnNode", None)
    if show_on_node is not None:
        return bool(show_on_node)
    # If no exposure flags exist in payload, do not block export by default.
    return True


def _to_canonical_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compute_graph_hash(pts_node_id: str, pts_node: HybridNode, internal_canvas: dict[str, Any]) -> str:
    payload = {
        "pts_node_id": pts_node_id,
        "pts_node": pts_node.model_dump(mode="python"),
        "internal_canvas": internal_canvas,
    }
    raw = _to_canonical_json(payload).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _pick_internal_canvas(graph: HybridGraph, pts_node_id: str) -> dict[str, Any] | None:
    metadata = graph.metadata if isinstance(graph.metadata, dict) else {}
    canvases = metadata.get("canvases")
    if not isinstance(canvases, list):
        return None
    candidates: list[dict[str, Any]] = []
    for item in canvases:
        if not isinstance(item, dict):
            continue
        if item.get("kind") != "pts_internal":
            continue
        if str(item.get("parentPtsNodeId") or "") != pts_node_id:
            continue
        candidates.append(item)
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda c: (len(c.get("nodes") or []), len(c.get("edges") or [])),
        reverse=True,
    )[0]


def _reference_product_ports(node: HybridNode) -> list:
    explicit_product_outputs = [
        port
        for port in node.outputs
        if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)) and bool(port.isProduct)
    ]
    if explicit_product_outputs:
        return explicit_product_outputs
    return [
        port
        for port in node.inputs
        if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)) and bool(port.isProduct)
    ]


def _extract_product_node_ids(internal_graph: HybridGraph) -> list[str]:
    product_node_ids: list[str] = []
    for node in internal_graph.nodes:
        if _reference_product_ports(node):
            product_node_ids.append(node.id)
    return product_node_ids


def _find_reference_port_for_solver_output(
    *,
    source_node: HybridNode | None,
    out: dict[str, Any],
    flow_uuid: str,
) -> Any | None:
    if source_node is None or not flow_uuid:
        return None

    source_port_id = str(out.get("sourcePortId") or out.get("source_port_id") or out.get("id") or "").strip()
    candidate_ports = list(source_node.outputs) + list(source_node.inputs)
    explicit_match = next(
        (
            port
            for port in candidate_ports
            if str(port.flowUuid or "").strip() == flow_uuid
            and source_port_id
            and str(port.id or "").strip() == source_port_id
        ),
        None,
    )
    if explicit_match is not None:
        return explicit_match

    reference_match = next(
        (
            port
            for port in _reference_product_ports(source_node)
            if str(port.flowUuid or "").strip() == flow_uuid
        ),
        None,
    )
    if reference_match is not None:
        return reference_match

    return next((port for port in candidate_ports if str(port.flowUuid or "").strip() == flow_uuid), None)


def _to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _port_amount_map(node: HybridNode, direction: str) -> dict[str, float]:
    ports = node.inputs if direction == "input" else node.outputs
    result: dict[str, float] = {}
    for port in ports:
        if is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)) or not port.flowUuid:
            continue
        result[port.flowUuid] = result.get(port.flowUuid, 0.0) + _to_float(port.amount)
    return result


def _build_internal_flow_totals(internal_graph: HybridGraph) -> tuple[dict[str, float], dict[str, float]]:
    total_inputs: dict[str, float] = {}
    total_outputs: dict[str, float] = {}
    for node in internal_graph.nodes:
        in_map = _port_amount_map(node, "input")
        out_map = _port_amount_map(node, "output")
        for flow_uuid, amount in in_map.items():
            total_inputs[flow_uuid] = total_inputs.get(flow_uuid, 0.0) + max(amount, 0.0)
        for flow_uuid, amount in out_map.items():
            total_outputs[flow_uuid] = total_outputs.get(flow_uuid, 0.0) + max(amount, 0.0)
    return total_inputs, total_outputs


def _internal_product_producers_by_flow(internal_graph: HybridGraph) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for node in internal_graph.nodes:
        process_uuid = str(node.process_uuid or "").strip()
        for port in _reference_product_ports(node):
            flow_uuid = str(port.flowUuid or "").strip()
            if not flow_uuid:
                continue
            result.setdefault(flow_uuid, set()).add(process_uuid)
    return result


def _build_solver_pts_outputs_from_internal_graph(internal_graph: HybridGraph) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for node in internal_graph.nodes:
        for port in _reference_product_ports(node):
            flow_uuid = str(port.flowUuid or "").strip()
            if not flow_uuid:
                continue
            amount = float(port.amount or 0.0)
            if amount <= 0:
                continue
            outputs.append(
                {
                    "id": f"{node.id}::{port.id}",
                    "flowUuid": flow_uuid,
                    "name": str(port.name or ""),
                    "unit": str(port.unit or ""),
                    "unitGroup": port.unitGroup,
                    "amount": amount,
                    "externalSaleAmount": float(port.externalSaleAmount or 0.0),
                    "type": str(port.type or "technosphere"),
                    "direction": "output",
                    "showOnNode": True,
                    "internalExposed": True,
                    "isProduct": True,
                    "sourceProcessUuid": str(node.process_uuid or ""),
                    "sourceProcessName": str(node.name or ""),
                    "sourceNodeId": str(node.id or ""),
                    "sourcePortId": str(port.id or ""),
                }
            )
    return outputs


def _build_solver_pts_outputs_with_binding(
    *,
    pts_node: HybridNode,
    internal_graph: HybridGraph,
) -> list[dict[str, Any]]:
    candidates_by_flow: dict[str, list[dict[str, Any]]] = {}
    node_by_id = {str(node.id): node for node in internal_graph.nodes}
    for node in internal_graph.nodes:
        for port in _reference_product_ports(node):
            flow_uuid = str(port.flowUuid or "").strip()
            if not flow_uuid:
                continue
            candidates_by_flow.setdefault(flow_uuid, []).append(
                {
                    "source_process_uuid": str(node.process_uuid or "").strip(),
                    "source_node_id": str(node.id or "").strip(),
                    "port_id": str(port.id or "").strip(),
                    "name": str(port.name or ""),
                    "unit": str(port.unit or ""),
                    "unitGroup": port.unitGroup,
                    "amount": float(port.amount or 0.0),
                    "node_mode": str(node.mode or ""),
                }
            )

    targets = [
        port
        for port in pts_node.outputs
        if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type))
        and bool(port.isProduct)
        and bool(str(port.flowUuid or "").strip())
    ]
    if not targets:
        # fallback: keep previous behavior if product flags are absent in outer ports
        return _build_solver_pts_outputs_from_internal_graph(internal_graph)

    selected: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    ambiguities: list[dict[str, Any]] = []

    for port in targets:
        flow_uuid = str(port.flowUuid or "").strip()
        candidates = candidates_by_flow.get(flow_uuid, [])
        if not candidates:
            continue

        bound_process = str(port.source_process_uuid or "").strip()
        bound_node = str(port.source_node_id or "").strip()

        chosen: dict[str, Any] | None = None
        if bound_process:
            exact = [item for item in candidates if item["source_process_uuid"] == bound_process]
            if len(exact) == 1:
                chosen = exact[0]
        if chosen is None and bound_node:
            exact = [item for item in candidates if item["source_node_id"] == bound_node]
            if len(exact) == 1:
                chosen = exact[0]
        if chosen is None and len(candidates) > 1:
            market_candidates = [
                item
                for item in candidates
                if item["source_process_uuid"].startswith("market_")
                or (
                    item["source_node_id"] in node_by_id
                    and str(node_by_id[item["source_node_id"]].mode or "") == "normalized"
                )
            ]
            if len(market_candidates) == 1:
                chosen = market_candidates[0]
        if chosen is None and len(candidates) == 1:
            chosen = candidates[0]

        if chosen is None:
            ambiguities.append(
                {
                    "port_id": str(port.id or ""),
                    "port_name": str(port.name or ""),
                    "flow_uuid": flow_uuid,
                    "candidate_source_process_uuids": sorted({item["source_process_uuid"] for item in candidates}),
                }
            )
            continue

        uniq_key = (flow_uuid, chosen["source_process_uuid"])
        if uniq_key in seen:
            continue
        seen.add(uniq_key)
        selected.append(
            {
                "id": f"{pts_node.id}::{str(port.id or chosen['port_id'])}",
                "flowUuid": flow_uuid,
                "name": str(port.name or chosen["name"]),
                "unit": str(port.unit or chosen["unit"]),
                "unitGroup": port.unitGroup or chosen["unitGroup"],
                "amount": float(chosen["amount"] or 0.0),
                "externalSaleAmount": float(port.externalSaleAmount or 0.0),
                "type": "technosphere",
                "direction": "output",
                "showOnNode": True,
                "internalExposed": True,
                "isProduct": True,
                "sourceProcessUuid": chosen["source_process_uuid"],
                "sourceNodeId": chosen["source_node_id"],
                "sourcePortId": chosen["port_id"],
            }
        )

    if ambiguities:
        raise ValueError(
            "AMBIGUOUS_PTS_OUTPUT_PRODUCER|"
            "Multiple producers for same flow; provide sourceProcessUuid/sourceNodeId or ensure unique market candidate|"
            + json.dumps({"evidence": ambiguities}, ensure_ascii=False)
        )
    return selected


def _aggregate_exchange_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        flow_uuid = str(row.get("flowUuid") or "")
        unit = str(row.get("unit") or "")
        unit_group = str(row.get("unitGroup") or "")
        direction = str(row.get("direction") or "")
        name = str(row.get("name") or "")
        source_process_uuid = str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or "")
        source_process_name = str(row.get("sourceProcessName") or row.get("source_process_name") or "")
        source_node_id = str(row.get("sourceNodeId") or row.get("source_node_id") or "")
        key = (flow_uuid, unit, unit_group, direction, name, source_process_uuid, source_node_id)
        amount = float(row.get("amount") or 0.0)
        if key not in grouped:
            grouped[key] = {
                "flowUuid": flow_uuid,
                "name": name,
                "unit": unit,
                "unitGroup": unit_group,
                "sourceProcessUuid": source_process_uuid,
                "sourceProcessName": source_process_name,
                "sourceNodeId": source_node_id,
                "direction": direction,
                "amount": 0.0,
            }
        grouped[key]["amount"] = float(grouped[key]["amount"] or 0.0) + amount
    return list(grouped.values())


def _allocation_fraction_by_port_id(node: HybridNode) -> dict[str, float]:
    allocation_fraction_by_port_id: dict[str, float] = {}
    explicit_product_outputs = [
        port
        for port in node.outputs
        if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)) and bool(port.isProduct)
    ]
    if not explicit_product_outputs:
        return allocation_fraction_by_port_id

    unit_groups = {str(port.unitGroup or "").strip() for port in explicit_product_outputs if str(port.unitGroup or "").strip()}
    if len(unit_groups) > 1:
        return allocation_fraction_by_port_id

    custom_factors = [port.allocationFactor for port in explicit_product_outputs]
    if any(f is not None for f in custom_factors):
        positive_factor_sum = sum(float(f or 0.0) for f in custom_factors if float(f or 0.0) > 0)
        if positive_factor_sum > 0:
            for port in explicit_product_outputs:
                factor = float(port.allocationFactor or 0.0)
                if factor > 0:
                    allocation_fraction_by_port_id[str(port.id or "")] = factor / positive_factor_sum
            return allocation_fraction_by_port_id

    total_amount = sum(float(port.amount or 0.0) for port in explicit_product_outputs if float(port.amount or 0.0) > 0)
    if total_amount > 0:
        for port in explicit_product_outputs:
            amount = float(port.amount or 0.0)
            if amount > 0:
                allocation_fraction_by_port_id[str(port.id or "")] = amount / total_amount
    return allocation_fraction_by_port_id


def _build_virtual_processes_from_internal_graph(
    *,
    pts_node: HybridNode,
    solver_outputs: list[dict[str, Any]],
    internal_graph: HybridGraph,
    process_name_by_uuid: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    node_by_id = {str(node.id or ""): node for node in internal_graph.nodes}
    virtual_processes: list[dict[str, Any]] = []
    published_boundary_elementary: list[dict[str, Any]] = []
    used_process_uuids: set[str] = set()
    used_product_keys: set[str] = set()

    for idx, out in enumerate(solver_outputs, start=1):
        source_node_id = str(out.get("sourceNodeId") or out.get("source_node_id") or "").strip()
        source_process_uuid = str(out.get("sourceProcessUuid") or out.get("source_process_uuid") or "").strip()
        source_process_name = (
            str(out.get("sourceProcessName") or out.get("source_process_name") or "").strip()
            or process_name_by_uuid.get(source_process_uuid, "")
        )
        flow_uuid = str(out.get("flowUuid") or "").strip()
        flow_name = str(out.get("name") or "").strip()
        ref_amount = float(out.get("amount") or 0.0)
        source_node = node_by_id.get(source_node_id)
        if source_node is None or not flow_uuid or ref_amount <= 0:
            continue

        reference_port = next(
            (
                port
                for port in (source_node.outputs + source_node.inputs)
                if str(port.flowUuid or "").strip() == flow_uuid
                and (
                    str(port.id or "").strip() == str(out.get("sourcePortId") or out.get("source_port_id") or "").strip()
                    or bool(port.isProduct)
                )
            ),
            None,
        )
        if reference_port is None:
            reference_port = next(
                (
                    port
                    for port in _reference_product_ports(source_node)
                    if str(port.flowUuid or "").strip() == flow_uuid
                ),
                None,
            )
        if reference_port is None:
            reference_port = next(
                (port for port in (source_node.outputs + source_node.inputs) if str(port.flowUuid or "").strip() == flow_uuid),
                None,
            )

        reference_port_id = str(reference_port.id or "") if reference_port is not None else ""
        allocation_fraction_by_port_id = _allocation_fraction_by_port_id(source_node)
        allocation_fraction = allocation_fraction_by_port_id.get(reference_port_id, 1.0)
        scale = allocation_fraction / ref_amount if ref_amount > 0 else 0.0

        technosphere_inputs: list[dict[str, Any]] = []
        for port in source_node.inputs:
            if is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)):
                continue
            if reference_port is not None and str(port.id or "") == reference_port_id and str(reference_port.direction or "") == "input":
                continue
            matched_edges = []
            target_port_id = str(port.id or "").strip()
            for edge in internal_graph.exchanges:
                if str(edge.toNode or "").strip() != source_node_id:
                    continue
                if str(edge.flowUuid or "").strip() != str(port.flowUuid or "").strip():
                    continue
                edge_target_port_id = _port_id_from_handle(
                    str(getattr(edge, "target_port_id", "") or getattr(edge, "targetHandle", "") or ""),
                    "in",
                )
                if edge_target_port_id and target_port_id and edge_target_port_id != target_port_id:
                    continue
                matched_edges.append(edge)

            if matched_edges:
                for edge in matched_edges:
                    provider_node = node_by_id.get(str(edge.fromNode or "").strip())
                    amount = float(edge.consumerAmount or edge.amount or port.amount or 0.0) * scale
                    if amount <= 0:
                        continue
                    technosphere_inputs.append(
                        {
                            "flowUuid": str(port.flowUuid or ""),
                            "name": str(port.name or ""),
                            "unit": str(port.unit or ""),
                            "unitGroup": port.unitGroup,
                            "direction": "input",
                            "amount": amount,
                            "type": "technosphere",
                            "sourceProcessUuid": str(provider_node.process_uuid or "") if provider_node is not None else "",
                            "sourceProcessName": str(provider_node.name or "") if provider_node is not None else "",
                            "sourceNodeId": str(provider_node.id or "") if provider_node is not None else str(edge.fromNode or ""),
                        }
                    )
                continue

            amount = float(port.amount or 0.0) * scale
            if amount <= 0:
                continue
            technosphere_inputs.append(
                {
                    "flowUuid": str(port.flowUuid or ""),
                    "name": str(port.name or ""),
                    "unit": str(port.unit or ""),
                    "unitGroup": port.unitGroup,
                    "direction": "input",
                    "amount": amount,
                    "type": "technosphere",
                }
            )

        elementary_flows: list[dict[str, Any]] = []
        for port in list(source_node.inputs) + list(source_node.outputs):
            if not is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.type)):
                continue
            amount = float(port.amount or 0.0) * scale
            if amount == 0:
                continue
            elementary_flows.append(
                {
                    "flowUuid": str(port.flowUuid or ""),
                    "name": str(port.name or ""),
                    "unit": str(port.unit or ""),
                    "unitGroup": port.unitGroup,
                    "sourceProcessUuid": source_process_uuid,
                    "sourceProcessName": source_process_name,
                    "sourceNodeId": source_node_id,
                    "direction": str(port.direction or "output"),
                    "amount": amount,
                    "type": "biosphere",
                    "showOnNode": bool(port.showOnNode),
                    "internalExposed": port.internalExposed,
                }
            )

        technosphere_inputs = _aggregate_exchange_rows(technosphere_inputs)
        elementary_flows = _aggregate_exchange_rows(elementary_flows)
        published_boundary_elementary.extend([dict(item) for item in elementary_flows])

        process_uuid = (
            f"{pts_node.pts_uuid or pts_node.process_uuid}::product::"
            f"{_stable_id_token(source_process_uuid)}::{_stable_id_token(flow_uuid)}"
        )
        if process_uuid in used_process_uuids:
            suffix = 2
            candidate = f"{process_uuid}::dup{suffix}"
            while candidate in used_process_uuids:
                suffix += 1
                candidate = f"{process_uuid}::dup{suffix}"
            process_uuid = candidate
        used_process_uuids.add(process_uuid)

        product_key = f"{_stable_id_token(source_process_uuid)}::{_stable_id_token(flow_uuid)}"
        if product_key in used_product_keys:
            suffix = 2
            candidate = f"{product_key}::dup{suffix}"
            while candidate in used_product_keys:
                suffix += 1
                candidate = f"{product_key}::dup{suffix}"
            product_key = candidate
        used_product_keys.add(product_key)

        virtual_processes.append(
            {
                "process_uuid": process_uuid,
                "process_name": (
                    f"{flow_name or 'product'} @ {source_process_name or source_process_uuid}"
                    if (source_process_name or source_process_uuid)
                    else str(out.get("process_name") or f"VP-{idx}")
                ),
                "reference_product": {
                    "flowUuid": flow_uuid,
                    "name": flow_name,
                    "unit": str(out.get("unit") or ""),
                    "amount": 1.0,
                },
                "reference_product_flow_uuid": flow_uuid,
                "product_name": flow_name,
                "is_product": True,
                "technosphere_inputs": technosphere_inputs,
                "elementary_flows": elementary_flows,
                "source_process_uuid": source_process_uuid,
                "source_process_name": source_process_name,
                "source_node_id": source_node_id,
                "source_port_id": str(out.get("sourcePortId") or out.get("source_port_id") or out.get("id") or "").strip(),
                "source_port_name": str(out.get("sourcePortName") or out.get("source_port_name") or flow_name),
                "virtual_process_key": process_uuid,
                "product_key": product_key,
                "normalization_reference_amount": ref_amount,
                "allocation_fraction": allocation_fraction,
            }
        )

    return virtual_processes, _aggregate_exchange_rows(published_boundary_elementary)


def _build_internal_elementary_rows_by_binding(
    internal_graph: HybridGraph,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    by_process_uuid: dict[str, list[dict[str, Any]]] = {}
    by_node_id: dict[str, list[dict[str, Any]]] = {}
    all_rows: list[dict[str, Any]] = []

    for node in internal_graph.nodes:
        node_rows: list[dict[str, Any]] = []
        for port in node.outputs:
            if port.type != "biosphere" or not str(port.flowUuid or "").strip():
                continue
            row = {
                "flowUuid": str(port.flowUuid or ""),
                "name": str(port.name or ""),
                "unit": str(port.unit or ""),
                "unitGroup": port.unitGroup,
                "sourceProcessUuid": str(node.process_uuid or ""),
                "sourceProcessName": str(node.name or ""),
                "sourceNodeId": str(node.id or ""),
                "direction": "output",
                "amount": float(port.amount or 0.0),
                "type": "biosphere",
                "showOnNode": bool(port.showOnNode),
                "internalExposed": port.internalExposed,
            }
            node_rows.append(row)
            all_rows.append(row)
        aggregated = _aggregate_exchange_rows(node_rows)
        by_node_id[str(node.id or "")] = aggregated
        by_process_uuid.setdefault(str(node.process_uuid or ""), [])
        by_process_uuid[str(node.process_uuid or "")].extend(aggregated)

    for process_uuid, rows in list(by_process_uuid.items()):
        by_process_uuid[process_uuid] = _aggregate_exchange_rows(rows)

    return by_process_uuid, by_node_id, _aggregate_exchange_rows(all_rows)


def _reordered_internal_nodes_for_target(
    *,
    internal_nodes: list[HybridNode],
    target_source_process_uuid: str,
) -> list[dict[str, Any]]:
    target = str(target_source_process_uuid or "").strip()
    if not target:
        return [node.model_dump(mode="python") for node in internal_nodes]
    rows = [node.model_dump(mode="python") for node in internal_nodes]
    rows.sort(key=lambda item: 0 if str(item.get("process_uuid") or "") == target else 1)
    return rows


def _reordered_internal_solver_nodes_for_target(
    *,
    internal_nodes: list[HybridNode],
    target_source_process_uuid: str,
) -> list[dict[str, Any]]:
    target = str(target_source_process_uuid or "").strip()
    rows = [_solver_internal_node_payload(node) for node in internal_nodes]
    if target:
        rows.sort(key=lambda item: 0 if str(item.get("process_uuid") or "") == target else 1)
    return rows


def _solver_internal_node_payload(node: HybridNode) -> dict[str, Any]:
    row = node.model_dump(mode="python")
    raw_outputs = row.get("outputs") if isinstance(row.get("outputs"), list) else []
    technosphere_outputs: list[dict[str, Any]] = []
    biosphere_outputs: list[dict[str, Any]] = []
    for port in raw_outputs:
        if not isinstance(port, dict):
            continue
        semantic = graph_exchange_type_to_flow_semantic(port.get("type"))
        if is_elementary_flow_semantic(semantic):
            biosphere_outputs.append(dict(port))
        else:
            technosphere_outputs.append(dict(port))
    row["outputs"] = technosphere_outputs
    row["emissions"] = biosphere_outputs
    return row


def _stable_id_token(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return "unknown"
    return token.replace(" ", "_")


def _port_id_from_handle(handle_id: str | None, prefix: str) -> str:
    if not handle_id:
        return ""
    token = f"{prefix}:"
    if handle_id.startswith(token):
        return handle_id[len(token) :]
    if ":" in handle_id:
        return handle_id.split(":", 1)[1]
    return handle_id


def _target_input_amount_for_edge(*, internal_graph: HybridGraph, edge: object) -> float:
    target_node_id = str(getattr(edge, "toNode", "") or "")
    flow_uuid = str(getattr(edge, "flowUuid", "") or "")
    if not target_node_id:
        return 0.0
    node = next((n for n in internal_graph.nodes if n.id == target_node_id), None)
    if node is None:
        return 0.0

    target_handle_or_port = str(getattr(edge, "target_port_id", "") or getattr(edge, "targetHandle", "") or "")
    target_port_id = _port_id_from_handle(target_handle_or_port, "in")
    if target_port_id:
        port = next((p for p in node.inputs if str(p.id or "") == target_port_id), None)
        if port is not None:
            return float(port.amount or 0.0)

    if not flow_uuid:
        return 0.0
    matched = [
        float(port.amount or 0.0)
        for port in node.inputs
        if port.type != "biosphere" and str(port.flowUuid or "") == flow_uuid
    ]
    if not matched:
        return 0.0
    return sum(value for value in matched if value > 0)


def _normalize_and_validate_internal_edge_amounts(internal_graph: HybridGraph) -> None:
    bad_edges: list[dict[str, object]] = []
    for edge in internal_graph.exchanges:
        target_input_amount = _target_input_amount_for_edge(internal_graph=internal_graph, edge=edge)

        amount = float(edge.amount or 0.0)
        consumer_amount = float(edge.consumerAmount or 0.0)
        provider_amount = float(edge.providerAmount or 0.0)

        effective_amount = 0.0
        if edge.quantityMode == "dual":
            if consumer_amount > 0:
                effective_amount = consumer_amount
            elif target_input_amount > 0:
                effective_amount = target_input_amount
            elif amount > 0:
                effective_amount = amount

            if effective_amount > 0:
                edge.consumerAmount = effective_amount
                edge.amount = effective_amount
                if provider_amount <= 0:
                    edge.providerAmount = effective_amount
            else:
                bad_edges.append(
                    {
                        "edge_id": edge.id,
                        "from_node": edge.fromNode,
                        "to_node": edge.toNode,
                        "flow_uuid": edge.flowUuid,
                        "quantity_mode": edge.quantityMode,
                        "amount": amount,
                        "consumer_amount": consumer_amount,
                        "provider_amount": provider_amount,
                        "target_input_amount": target_input_amount,
                    }
                )
            continue

        # single mode
        if amount > 0:
            effective_amount = amount
        elif target_input_amount > 0:
            effective_amount = target_input_amount
        elif consumer_amount > 0:
            effective_amount = consumer_amount

        if effective_amount > 0:
            edge.amount = effective_amount
            edge.consumerAmount = effective_amount
            edge.providerAmount = effective_amount
            continue

        bad_edges.append(
            {
                "edge_id": edge.id,
                "from_node": edge.fromNode,
                "to_node": edge.toNode,
                "flow_uuid": edge.flowUuid,
                "quantity_mode": edge.quantityMode,
                "amount": amount,
                "consumer_amount": consumer_amount,
                "provider_amount": provider_amount,
                "target_input_amount": target_input_amount,
            }
        )

    if bad_edges:
        raise ValueError(
            "INVALID_INTERNAL_EDGE_AMOUNT|"
            "PTS internal edges have zero/non-positive effective amounts; set >0 in target input inventory or edge quantity, "
            "then resync before compile|"
            + json.dumps({"evidence": bad_edges}, ensure_ascii=False)
        )


def _pick_exportable_outputs(
    *,
    pts_node: HybridNode | None = None,
    outputs: list | None = None,
    total_inputs_raw: dict[str, float],
) -> list:
    # Exposure policy:
    # 1) all exported outputs must be explicitly exposed
    # 2) product outputs can be exported even when internally consumed
    # 3) non-product intermediate outputs are exported only when unlinked inside this PTS
    selected = []
    for port in (outputs if outputs is not None else (pts_node.outputs if pts_node is not None else [])):
        if port.type == "biosphere":
            continue
        if not _is_exposed(port):
            continue
        flow_uuid = str(port.flowUuid or "")
        if not flow_uuid:
            continue
        is_product = bool(port.isProduct)
        is_unlinked_intermediate = total_inputs_raw.get(flow_uuid, 0.0) <= 1e-12
        if is_product:
            selected.append(port)
            continue
        if is_unlinked_intermediate:
            selected.append(port)
    return selected


def _policy_ports_to_runtime_ports(rows: list[dict], *, direction: str) -> list[SimpleNamespace]:
    selected: list[SimpleNamespace] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        flow_uuid = str(row.get("flow_uuid") or row.get("flowUuid") or "").strip()
        if not flow_uuid:
            continue
        selected.append(
            SimpleNamespace(
                id=str(row.get("port_key") or row.get("product_key") or f"{direction}:{flow_uuid}"),
                flowUuid=flow_uuid,
                name=str(row.get("name") or row.get("product_name") or flow_uuid),
                type="technosphere",
                direction=direction,
                isProduct=bool(row.get("is_product") or row.get("isProduct")),
                internalExposed=row.get("internal_exposed", row.get("internalExposed")),
                externalVisible=row.get("external_visible", row.get("externalVisible")),
                showOnNode=bool(row.get("show_on_node", row.get("showOnNode", row.get("external_visible", row.get("externalVisible", False))))),
                unit=str(row.get("unit") or ""),
                unitGroup=row.get("unit_group", row.get("unitGroup")),
                sourceProcessUuid=str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or ""),
                sourceProcessName=str(row.get("source_process_name") or row.get("sourceProcessName") or ""),
                sourceNodeId=str(row.get("source_node_id") or row.get("sourceNodeId") or ""),
            )
        )
    return selected


def _expand_publication_outputs_with_source_bindings(
    *,
    publication_outputs: list,
    internal_solver_outputs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Expand exposed product ports to source-bound rows when one flow has multiple producers.

    Compute set is driven by internal graph products; publication set is a projection. For exposed
    output ports without explicit source binding, we materialize one row per internal producer so
    downstream mapping can stay deterministic.
    """
    candidates_by_flow: dict[str, list[dict[str, Any]]] = {}
    for row in internal_solver_outputs:
        flow_uuid = str(row.get("flowUuid") or "").strip()
        if not flow_uuid:
            continue
        source_process_uuid = str(row.get("sourceProcessUuid") or "").strip()
        source_node_id = str(row.get("sourceNodeId") or "").strip()
        if not source_process_uuid and not source_node_id:
            continue
        candidates_by_flow.setdefault(flow_uuid, []).append(
            {
                "sourceProcessUuid": source_process_uuid,
                "sourceProcessName": str(row.get("sourceProcessName") or "").strip(),
                "sourceNodeId": source_node_id,
            }
        )

    expanded: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for port in publication_outputs:
        if hasattr(port, "model_dump"):
            base = port.model_dump(mode="python", by_alias=True)
        elif isinstance(port, dict):
            base = dict(port)
        else:
            base = dict(getattr(port, "__dict__", {}) or {})
        flow_uuid = str(base.get("flowUuid") or "").strip()
        is_product = bool(base.get("isProduct"))
        bound_process = str(base.get("sourceProcessUuid") or "").strip()
        bound_node = str(base.get("sourceNodeId") or "").strip()
        if not flow_uuid:
            continue

        if is_product and not bound_process and not bound_node:
            candidates = candidates_by_flow.get(flow_uuid, [])
            if candidates:
                for candidate in candidates:
                    row = dict(base)
                    row["sourceProcessUuid"] = candidate["sourceProcessUuid"]
                    row["sourceProcessName"] = candidate["sourceProcessName"]
                    row["sourceNodeId"] = candidate["sourceNodeId"]
                    key = (
                        str(row.get("id") or ""),
                        flow_uuid,
                        str(row.get("sourceProcessUuid") or ""),
                        str(row.get("sourceNodeId") or ""),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    expanded.append(row)
                continue

        key = (
            str(base.get("id") or ""),
            flow_uuid,
            bound_process,
            bound_node,
        )
        if key in seen:
            continue
        seen.add(key)
        expanded.append(base)
    return expanded


def _validate_pts_internal_node_kinds(internal_graph: HybridGraph) -> None:
    allowed_kinds = {"unit_process", "market_process", "lci_dataset"}
    invalid_nodes: list[dict[str, str]] = []
    for node in internal_graph.nodes:
        node_kind = str(node.node_kind or "").strip()
        if node_kind in allowed_kinds:
            continue
        invalid_nodes.append(
            {
                "node_id": str(node.id or ""),
                "node_name": str(node.name or ""),
                "node_kind": node_kind,
            }
        )

    if invalid_nodes:
        has_nested_pts = any(item.get("node_kind") in {"pts", "pts_module"} for item in invalid_nodes)
        code = "INVALID_PTS_NESTED_PTS" if has_nested_pts else "INVALID_PTS_INTERNAL_NODE_KIND"
        message = (
            "PTS internal graph contains nested PTS nodes, which are not supported"
            if has_nested_pts
            else "PTS internal graph contains unsupported node kinds; allowed: unit_process, market_process, lci_dataset"
        )
        raise ValueError(
            f"{code}|{message}|" + json.dumps({"evidence": invalid_nodes}, ensure_ascii=False)
        )


def compile_pts(graph: HybridGraph, pts_node_id: str, ports_policy: dict | None = None) -> dict[str, Any]:
    pts_node = next((node for node in graph.nodes if node.id == pts_node_id and node.node_kind == "pts_module"), None)
    if pts_node is None:
        raise ValueError(f"PTS node not found: {pts_node_id}")

    internal_canvas = _pick_internal_canvas(graph, pts_node_id)
    if internal_canvas is None:
        raise ValueError(f"PTS internal canvas not found for node: {pts_node_id}")

    internal_nodes = internal_canvas.get("nodes")
    internal_edges = internal_canvas.get("edges")
    if not isinstance(internal_nodes, list) or not isinstance(internal_edges, list):
        raise ValueError(f"PTS internal canvas format invalid: {pts_node_id}")

    internal_graph = HybridGraph.model_validate(
        {
            "functionalUnit": graph.functionalUnit,
            "nodes": internal_nodes,
            "exchanges": internal_edges,
            "metadata": {},
        }
    )
    process_name_by_uuid = {str(node.process_uuid or "").strip(): str(node.name or "").strip() for node in internal_graph.nodes}
    _validate_pts_internal_node_kinds(internal_graph)
    _normalize_and_validate_internal_edge_amounts(internal_graph)

    product_node_ids = _extract_product_node_ids(internal_graph)
    validation: PtsValidationResult = validate_pts_compile(
        graph=internal_graph,
        internal_node_ids=[node.id for node in internal_graph.nodes],
        product_node_ids=product_node_ids,
    )

    incoming_flow_by_node: dict[str, set[str]] = {}
    for edge in internal_graph.exchanges:
        if not edge.toNode or not edge.flowUuid:
            continue
        incoming_flow_by_node.setdefault(edge.toNode, set()).add(edge.flowUuid)

    unresolved_input_flow_uuids: set[str] = set()
    for inode in internal_graph.nodes:
        incoming_flows = incoming_flow_by_node.get(inode.id, set())
        for port in inode.inputs:
            if port.type == "biosphere" or not port.flowUuid:
                continue
            if port.flowUuid in incoming_flows:
                continue
            unresolved_input_flow_uuids.add(port.flowUuid)

    policy_inputs = ports_policy.get("inputs") if isinstance(ports_policy, dict) and isinstance(ports_policy.get("inputs"), list) else None
    policy_outputs = ports_policy.get("outputs") if isinstance(ports_policy, dict) and isinstance(ports_policy.get("outputs"), list) else None
    runtime_inputs = _policy_ports_to_runtime_ports(policy_inputs or [], direction="input") if policy_inputs is not None else list(pts_node.inputs)
    runtime_outputs = _policy_ports_to_runtime_ports(policy_outputs or [], direction="output") if policy_outputs is not None else list(pts_node.outputs)

    exposed_input_flow_uuids = {
        port.flowUuid
        for port in runtime_inputs
        if port.type != "biosphere" and _is_exposed(port) and bool(port.flowUuid)
    }
    pts_output_flow_uuids = {
        port.flowUuid
        for port in runtime_outputs
        if port.type != "biosphere" and bool(port.flowUuid)
    }
    allowed_inventory_input_flow_uuids = {
        flow_uuid
        for flow_uuid in unresolved_input_flow_uuids
        if flow_uuid in exposed_input_flow_uuids and flow_uuid not in pts_output_flow_uuids
    }

    total_inputs_raw, total_outputs_raw = _build_internal_flow_totals(internal_graph)
    # Internal solve set must be derived from PTS internal products only. It must not depend on
    # external exposure ports, so compute and publish stay decoupled.
    internal_solver_outputs = _build_solver_pts_outputs_from_internal_graph(internal_graph)
    # External publication set: governed by exposure policy.
    publication_outputs = _pick_exportable_outputs(
        pts_node=pts_node,
        outputs=runtime_outputs,
        total_inputs_raw=total_inputs_raw,
    )
    solver_outputs = internal_solver_outputs
    if not solver_outputs:
        raise ValueError(
            "PTS compile found no solvable outputs. "
            "Require non-biosphere internal reference products with isProduct=true on outputs or inputs."
        )
    internally_satisfied_flow_uuids = {
        flow_uuid
        for flow_uuid, in_total in total_inputs_raw.items()
        if in_total > 0
        and total_outputs_raw.get(flow_uuid, 0.0) > 0
        and total_outputs_raw.get(flow_uuid, 0.0) >= in_total - 1e-12
    }
    allowed_inventory_input_flow_uuids = {
        flow_uuid
        for flow_uuid in allowed_inventory_input_flow_uuids
        if flow_uuid not in internally_satisfied_flow_uuids
    }

    solver_payload = {
        "pts_uuid": pts_node.pts_uuid or pts_node.process_uuid,
        "pts_node_id": pts_node_id,
        "pts_outputs": solver_outputs,
        "internal_nodes": [node.model_dump(mode="python") for node in internal_graph.nodes],
        "internal_edges": [edge.model_dump(mode="python") for edge in internal_graph.exchanges],
        "allowed_inventory_input_flow_uuids": sorted(allowed_inventory_input_flow_uuids),
    }

    merged_virtual_processes: list[dict[str, Any]] = []
    merged_warnings: list[str] = []
    solver_payloads: list[dict[str, Any]] = []
    for out in solver_outputs:
        target_source_process_uuid = str(out.get("sourceProcessUuid") or out.get("source_process_uuid") or "").strip()
        single_payload = {
            "pts_uuid": pts_node.pts_uuid or pts_node.process_uuid,
            "pts_node_id": pts_node_id,
            "pts_outputs": [out],
            "internal_nodes": _reordered_internal_solver_nodes_for_target(
                internal_nodes=internal_graph.nodes,
                target_source_process_uuid=target_source_process_uuid,
            ),
            "internal_edges": [edge.model_dump(mode="python") for edge in internal_graph.exchanges],
            "allowed_inventory_input_flow_uuids": sorted(allowed_inventory_input_flow_uuids),
        }
        solver_payloads.append(single_payload)
        single_result = run_tiangong_pts_compile(single_payload)
        merged_warnings.extend([str(w) for w in (single_result.get("warnings") or [])])
        vps = single_result.get("virtual_processes")
        if isinstance(vps, list):
            for local_idx, vp in enumerate(vps, start=1):
                if not isinstance(vp, dict):
                    continue
                enriched = dict(vp)
                source_process_uuid = str(out.get("sourceProcessUuid") or out.get("source_process_uuid") or "").strip()
                source_process_name = str(out.get("sourceProcessName") or out.get("source_process_name") or "").strip()
                source_node_id = str(out.get("sourceNodeId") or out.get("source_node_id") or "").strip()
                source_port_id = str(out.get("sourcePortId") or out.get("source_port_id") or out.get("id") or "").strip()
                flow_uuid = str(out.get("flowUuid") or "").strip()
                flow_name = str(out.get("name") or "").strip()
                source_node = next(
                    (node for node in internal_graph.nodes if str(node.id or "").strip() == source_node_id),
                    None,
                )
                reference_port = _find_reference_port_for_solver_output(
                    source_node=source_node,
                    out=out,
                    flow_uuid=flow_uuid,
                )
                reference_unit = str(
                    enriched.get("reference_unit")
                    or ((enriched.get("reference_product") or {}).get("unit") if isinstance(enriched.get("reference_product"), dict) else "")
                    or getattr(reference_port, "unit", "")
                    or out.get("unit")
                    or ""
                )
                reference_unit_group = (
                    enriched.get("reference_unit_group")
                    or ((enriched.get("reference_product") or {}).get("unitGroup") if isinstance(enriched.get("reference_product"), dict) else None)
                    or getattr(reference_port, "unitGroup", None)
                    or out.get("unitGroup")
                    or out.get("unit_group")
                    or None
                )
                product_key = f"{_stable_id_token(source_process_uuid)}::{_stable_id_token(flow_uuid)}" if source_process_uuid and flow_uuid else flow_uuid
                enriched["source_process_uuid"] = source_process_uuid
                enriched["source_process_name"] = source_process_name
                enriched["source_node_id"] = source_node_id
                enriched["source_port_id"] = source_port_id
                enriched["source_port_name"] = str(out.get("sourcePortName") or out.get("source_port_name") or flow_name)
                enriched["product_key"] = product_key
                enriched["product_name"] = str(enriched.get("product_name") or flow_name)
                enriched["is_product"] = bool(enriched.get("is_product", True))
                enriched["reference_product_flow_uuid"] = str(
                    enriched.get("reference_product_flow_uuid")
                    or ((enriched.get("reference_product") or {}).get("flowUuid") if isinstance(enriched.get("reference_product"), dict) else "")
                    or flow_uuid
                )
                reference_product = dict(enriched.get("reference_product") or {}) if isinstance(enriched.get("reference_product"), dict) else {}
                reference_product.setdefault("flowUuid", flow_uuid)
                reference_product.setdefault("name", str(enriched.get("product_name") or flow_name))
                reference_product["unit"] = reference_unit
                if reference_unit_group:
                    reference_product["unitGroup"] = reference_unit_group
                reference_product.setdefault("amount", 1.0)
                enriched["reference_product"] = reference_product
                enriched["reference_unit"] = reference_unit
                enriched["reference_unit_group"] = reference_unit_group
                if not isinstance(enriched.get("outputs"), list) or not enriched.get("outputs"):
                    output_row: dict[str, Any] = {
                        "id": source_port_id or f"out_{local_idx}",
                        "flowUuid": flow_uuid,
                        "name": str(enriched.get("product_name") or flow_name),
                        "unit": reference_unit,
                        "amount": 1.0,
                        "direction": "output",
                        "type": "technosphere",
                        "isProduct": True,
                    }
                    if reference_unit_group:
                        output_row["unitGroup"] = reference_unit_group
                    enriched["outputs"] = [output_row]
                merged_virtual_processes.append(enriched)

    solver_result = {
        "warnings": merged_warnings,
        "virtual_processes": merged_virtual_processes,
        "matrix_size": len(internal_graph.nodes),
        "invertible": validation.invertible,
    }
    validation.warnings.extend(merged_warnings)

    graph_hash = _compute_graph_hash(pts_node_id=pts_node_id, pts_node=pts_node, internal_canvas=internal_canvas)
    _, _, all_internal_elementary_rows = _build_internal_elementary_rows_by_binding(internal_graph)
    virtual_processes = merged_virtual_processes
    normalized_boundary_elementary = _aggregate_exchange_rows(
        [
            dict(item)
            for vp in virtual_processes
            for item in ((vp.get("elementary_flows") if isinstance(vp.get("elementary_flows"), list) else []))
            if isinstance(item, dict)
        ]
    )

    publication_rows = _expand_publication_outputs_with_source_bindings(
        publication_outputs=publication_outputs,
        internal_solver_outputs=internal_solver_outputs,
    )

    artifact = {
        "compile_schema_version": PTS_COMPILE_SCHEMA_VERSION,
        "pts_node_id": pts_node_id,
        "pts_uuid": pts_node.pts_uuid or pts_node.process_uuid,
        "internal_node_count": len(internal_graph.nodes),
        "internal_edge_count": len(internal_graph.exchanges),
        "product_node_ids": product_node_ids,
        "boundary_inputs": [
            {
                "flowUuid": str(port.flowUuid or ""),
                "name": str(port.name or ""),
                "unit": str(getattr(port, "unit", "") or ""),
                "unitGroup": getattr(port, "unitGroup", None),
                "direction": "input",
                "type": "technosphere",
                "internalExposed": getattr(port, "internalExposed", None),
                "showOnNode": bool(getattr(port, "showOnNode", False)),
                "sourceProcessUuid": str(getattr(port, "sourceProcessUuid", "") or ""),
                "sourceProcessName": str(getattr(port, "sourceProcessName", "") or ""),
                "sourceNodeId": str(getattr(port, "sourceNodeId", "") or ""),
            }
            for port in runtime_inputs
            if port.type != "biosphere"
        ],
        "boundary_outputs": publication_rows,
        "boundary_emissions": normalized_boundary_elementary or all_internal_elementary_rows,
        "virtual_processes": virtual_processes,
    }

    return {
        "compile_schema_version": PTS_COMPILE_SCHEMA_VERSION,
        "pts_uuid": pts_node.pts_uuid or pts_node.process_uuid,
        "graph_hash": graph_hash,
        "validation": validation,
        "artifact": artifact,
        "matrix_size": int(solver_result.get("matrix_size", len(internal_graph.nodes))),
        "invertible": bool(solver_result.get("invertible", True)),
        "solver_payload": solver_payload,
        "solver_payloads": solver_payloads,
        "allowed_inventory_input_flow_uuids": sorted(allowed_inventory_input_flow_uuids),
        "solver_result": solver_result,
    }


def compute_pts_graph_hash(graph: HybridGraph, pts_node_id: str) -> str:
    pts_node = next((node for node in graph.nodes if node.id == pts_node_id and node.node_kind == "pts_module"), None)
    if pts_node is None:
        raise ValueError(f"PTS node not found: {pts_node_id}")
    internal_canvas = _pick_internal_canvas(graph, pts_node_id)
    if internal_canvas is None:
        raise ValueError(f"PTS internal canvas not found for node: {pts_node_id}")
    return _compute_graph_hash(pts_node_id=pts_node_id, pts_node=pts_node, internal_canvas=internal_canvas)

