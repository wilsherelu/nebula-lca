from __future__ import annotations

from typing import Any

import numpy as np


def _to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_process_denominator(node: dict[str, Any]) -> float:
    outputs = node.get("outputs", []) or []
    product_outputs = [
        port
        for port in outputs
        if port.get("type") != "biosphere" and bool(port.get("isProduct"))
    ]
    denom = sum(max(_to_float(port.get("amount")), 0.0) for port in product_outputs)
    if denom > 0:
        return denom
    tech_outputs = [port for port in outputs if port.get("type") != "biosphere"]
    fallback = sum(max(_to_float(port.get("amount")), 0.0) for port in tech_outputs)
    return fallback if fallback > 0 else 1.0


def _port_amount_map(node: dict[str, Any], direction: str) -> dict[str, float]:
    ports = node.get("inputs", []) if direction == "input" else node.get("outputs", [])
    result: dict[str, float] = {}
    for port in ports:
        if port.get("type") == "biosphere":
            continue
        flow_uuid = str(port.get("flowUuid") or "")
        if not flow_uuid:
            continue
        result[flow_uuid] = result.get(flow_uuid, 0.0) + _to_float(port.get("amount"))
    return result


def _build_internal_matrix(
    internal_nodes: list[dict[str, Any]],
    internal_edges: list[dict[str, Any]],
) -> tuple[np.ndarray, list[str], dict[str, int], dict[str, float]]:
    node_ids = [str(node.get("id") or "") for node in internal_nodes if str(node.get("id") or "")]
    node_idx = {node_id: idx for idx, node_id in enumerate(node_ids)}
    n = len(node_ids)
    a_pts = np.zeros((n, n), dtype=float)
    denom_by_node: dict[str, float] = {}
    by_id = {str(node.get("id") or ""): node for node in internal_nodes}
    for node_id in node_ids:
        denom_by_node[node_id] = _build_process_denominator(by_id[node_id])

    for edge in internal_edges:
        from_node = str(edge.get("fromNode") or "")
        to_node = str(edge.get("toNode") or "")
        if from_node not in node_idx or to_node not in node_idx:
            continue
        i = node_idx[from_node]
        j = node_idx[to_node]
        quantity_mode = str(edge.get("quantityMode") or "single")
        consumer_amount = _to_float(edge.get("consumerAmount") if quantity_mode == "dual" else edge.get("amount"))
        denom = denom_by_node.get(to_node, 1.0) or 1.0
        a_pts[i, j] += consumer_amount / denom

    return a_pts, node_ids, node_idx, denom_by_node


def _build_boundary_matrices(
    internal_nodes: list[dict[str, Any]],
    node_ids: list[str],
    node_idx: dict[str, int],
    denom_by_node: dict[str, float],
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    g_pts: dict[str, np.ndarray] = {}
    b_pts: dict[str, np.ndarray] = {}
    by_id = {str(node.get("id") or ""): node for node in internal_nodes}
    for node_id in node_ids:
        node = by_id[node_id]
        j = node_idx[node_id]
        denom = denom_by_node.get(node_id, 1.0) or 1.0

        input_map = _port_amount_map(node, "input")
        output_map = _port_amount_map(node, "output")
        for flow_uuid in set(input_map.keys()) | set(output_map.keys()):
            net = output_map.get(flow_uuid, 0.0) - input_map.get(flow_uuid, 0.0)
            if abs(net) < 1e-12:
                continue
            row = g_pts.get(flow_uuid)
            if row is None:
                row = np.zeros((len(node_ids),), dtype=float)
                g_pts[flow_uuid] = row
            row[j] += net / denom

        for port in node.get("emissions", []) or []:
            if port.get("type") != "biosphere":
                continue
            flow_uuid = str(port.get("flowUuid") or "")
            if not flow_uuid:
                continue
            amount = _to_float(port.get("amount"))
            if abs(amount) < 1e-12:
                continue
            row = b_pts.get(flow_uuid)
            if row is None:
                row = np.zeros((len(node_ids),), dtype=float)
                b_pts[flow_uuid] = row
            row[j] += amount / denom

    return g_pts, b_pts


def _find_flow_meta(internal_nodes: list[dict[str, Any]], flow_uuid: str) -> tuple[str, str, str]:
    for node in internal_nodes:
        for key in ("inputs", "outputs", "emissions"):
            for port in node.get(key, []) or []:
                if str(port.get("flowUuid") or "") == flow_uuid:
                    return (
                        str(port.get("name") or flow_uuid),
                        str(port.get("unit") or ""),
                        str(port.get("unitGroup") or ""),
                    )
    return (flow_uuid, "", "")


def compile_pts_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    pts_uuid = str(payload.get("pts_uuid") or "")
    pts_outputs = payload.get("pts_outputs", []) or []
    internal_nodes = payload.get("internal_nodes", []) or []
    internal_edges = payload.get("internal_edges", []) or []
    allowed_input_uuids = set(payload.get("allowed_inventory_input_flow_uuids", []) or [])

    a_pts, node_ids, node_idx, denom_by_node = _build_internal_matrix(internal_nodes, internal_edges)
    n = len(node_ids)
    m = np.eye(n, dtype=float) - a_pts
    g_pts, b_pts = _build_boundary_matrices(internal_nodes, node_ids, node_idx, denom_by_node)

    virtual_processes: list[dict[str, Any]] = []
    warnings: list[str] = []

    for out_port in pts_outputs:
        flow_uuid = str(out_port.get("flowUuid") or "")
        if not flow_uuid:
            continue
        ref_coeff = g_pts.get(flow_uuid)
        if ref_coeff is None:
            warnings.append(f"missing boundary output in G_pts: {flow_uuid}")
            continue

        producer_node_id = None
        for node in internal_nodes:
            node_id = str(node.get("id") or "")
            for port in node.get("outputs", []) or []:
                if str(port.get("flowUuid") or "") == flow_uuid and _to_float(port.get("amount")) > 0:
                    producer_node_id = node_id
                    break
            if producer_node_id:
                break
        if producer_node_id is None or producer_node_id not in node_idx:
            warnings.append(f"cannot find producer node for flow: {flow_uuid}")
            continue

        d = np.zeros((n,), dtype=float)
        d[node_idx[producer_node_id]] = 1.0
        try:
            x = np.linalg.solve(m, d)
        except np.linalg.LinAlgError:
            warnings.append(f"A_pts is singular for output flow: {flow_uuid}")
            continue

        ref_output = float(np.dot(ref_coeff, x))
        if abs(ref_output) < 1e-12:
            warnings.append(f"near-zero reference output for flow: {flow_uuid}")
            continue

        tech_inputs: list[dict[str, Any]] = []
        for f_uuid, coeff in g_pts.items():
            amount = float(np.dot(coeff, x))
            if abs(amount) < 1e-12:
                continue
            if f_uuid == flow_uuid and amount > 0:
                continue
            if amount < 0 and f_uuid in allowed_input_uuids:
                name, unit, unit_group = _find_flow_meta(internal_nodes, f_uuid)
                tech_inputs.append(
                    {
                        "flowUuid": f_uuid,
                        "name": name,
                        "unit": unit,
                        "unitGroup": unit_group,
                        "direction": "input",
                        "amount": abs(amount),
                    }
                )

        elementary_flows: list[dict[str, Any]] = []
        for f_uuid, coeff in b_pts.items():
            amount = float(np.dot(coeff, x))
            if abs(amount) < 1e-12:
                continue
            name, unit, unit_group = _find_flow_meta(internal_nodes, f_uuid)
            elementary_flows.append(
                {
                    "flowUuid": f_uuid,
                    "name": name,
                    "unit": unit,
                    "unitGroup": unit_group,
                    "direction": "output" if amount >= 0 else "input",
                    "amount": abs(amount),
                }
            )

        virtual_processes.append(
            {
                "process_uuid": f"{pts_uuid}::product::{flow_uuid}",
                "process_name": f"{str(out_port.get('name') or flow_uuid)} @ {pts_uuid}",
                "reference_product": {
                    "flowUuid": flow_uuid,
                    "name": str(out_port.get("name") or flow_uuid),
                    "unit": str(out_port.get("unit") or ""),
                },
                "technosphere_inputs": tech_inputs,
                "elementary_flows": elementary_flows,
            }
        )

    return {
        "matrix_size": n,
        "invertible": True,
        "warnings": warnings,
        "virtual_processes": virtual_processes,
    }
