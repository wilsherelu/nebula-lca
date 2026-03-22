from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .schemas import HybridEdge, HybridGraph, HybridNode


@dataclass
class PtsValidationResult:
    ok: bool
    errors: list[str]
    warnings: list[str]
    matrix_size: int
    invertible: bool


def _is_intermediate_port_type(port_type: str) -> bool:
    return port_type != "biosphere"


def _reference_ports(node: HybridNode) -> list:
    explicit_product_outputs = [
        port
        for port in node.outputs
        if _is_intermediate_port_type(port.type) and bool(port.isProduct)
    ]
    if explicit_product_outputs:
        return explicit_product_outputs
    return [
        port
        for port in node.inputs
        if _is_intermediate_port_type(port.type) and bool(port.isProduct)
    ]


def _normalization_denom(node: HybridNode) -> float:
    """Use explicit product outputs, or product inputs for waste-treatment style nodes."""
    values = [
        float(port.amount or 0.0)
        for port in _reference_ports(node)
        if float(port.amount or 0.0) > 0
    ]
    return sum(values)


def _build_internal_matrix_a(
    *,
    internal_nodes: list[HybridNode],
    internal_edges: list[HybridEdge],
) -> tuple[list[list[float]], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    node_idx = {node.id: idx for idx, node in enumerate(internal_nodes)}
    n = len(internal_nodes)
    a = [[0.0 for _ in range(n)] for _ in range(n)]

    denoms: dict[str, float] = {}
    for node in internal_nodes:
        denom = _normalization_denom(node)
        if denom <= 1e-12:
            errors.append(f"{node.name}: missing positive reference products (isProduct=true on outputs or inputs)")
            continue
        denoms[node.id] = denom

    for edge in internal_edges:
        if edge.fromNode not in node_idx or edge.toNode not in node_idx:
            continue
        src_i = node_idx[edge.fromNode]
        dst_j = node_idx[edge.toNode]
        denom_j = denoms.get(edge.toNode)
        if denom_j is None:
            continue
        amount = float(edge.consumerAmount if edge.quantityMode == "dual" else edge.amount)
        a[src_i][dst_j] += amount / denom_j

    return a, errors, warnings


def _is_invertible_i_minus_a(a: list[list[float]], tol: float = 1e-12) -> bool:
    n = len(a)
    if n == 0:
        return False
    m = [[(1.0 if i == j else 0.0) - a[i][j] for j in range(n)] for i in range(n)]
    rank = 0
    col = 0
    while rank < n and col < n:
        pivot = max(range(rank, n), key=lambda r: abs(m[r][col]))
        if abs(m[pivot][col]) <= tol:
            col += 1
            continue
        if pivot != rank:
            m[rank], m[pivot] = m[pivot], m[rank]
        pivot_val = m[rank][col]
        for c in range(col, n):
            m[rank][c] /= pivot_val
        for r in range(n):
            if r == rank:
                continue
            factor = m[r][col]
            if abs(factor) <= tol:
                continue
            for c in range(col, n):
                m[r][c] -= factor * m[rank][c]
        rank += 1
        col += 1
    return rank == n


def validate_pts_compile(
    *,
    graph: HybridGraph,
    internal_node_ids: Iterable[str],
    product_node_ids: Iterable[str] | None = None,
) -> PtsValidationResult:
    del product_node_ids
    errors: list[str] = []
    warnings: list[str] = []

    internal_set = {node_id for node_id in internal_node_ids if node_id}
    if not internal_set:
        return PtsValidationResult(
            ok=False,
            errors=["internal_node_ids cannot be empty"],
            warnings=[],
            matrix_size=0,
            invertible=False,
        )

    node_by_id = {node.id: node for node in graph.nodes}
    missing_ids = sorted([node_id for node_id in internal_set if node_id not in node_by_id])
    if missing_ids:
        errors.append(f"internal_node_ids not found in graph: {', '.join(missing_ids)}")

    internal_nodes = [node_by_id[node_id] for node_id in internal_set if node_id in node_by_id]
    internal_edges = [edge for edge in graph.exchanges if edge.fromNode in internal_set and edge.toNode in internal_set]

    for node in internal_nodes:
        for port in [*node.inputs, *node.outputs]:
            if not _is_intermediate_port_type(port.type):
                continue
            if not port.flowUuid or not port.flowUuid.strip():
                errors.append(f"{node.name}: intermediate flow '{port.name}' missing flowUuid")

    a, matrix_errors, matrix_warnings = _build_internal_matrix_a(
        internal_nodes=internal_nodes,
        internal_edges=internal_edges,
    )
    errors.extend(matrix_errors)
    warnings.extend(matrix_warnings)

    invertible = False
    if not matrix_errors and len(internal_nodes) > 0:
        invertible = _is_invertible_i_minus_a(a)
        if not invertible:
            errors.append("PTS internal matrix (I - A_pts) is not invertible")

    return PtsValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        matrix_size=len(internal_nodes),
        invertible=invertible,
    )
