from __future__ import annotations

from typing import Any

from ..schemas import HybridGraph


def _target_port_id(edge: Any) -> str:
    explicit = str(getattr(edge, "target_port_id", None) or "").strip()
    if explicit:
        return explicit
    handle = str(getattr(edge, "targetHandle", None) or "").strip()
    for prefix in ("in:", "inl:", "inr:"):
        if handle.startswith(prefix):
            return handle[len(prefix):]
    return ""


def collect_unlinked_positive_technosphere_inputs(graph: HybridGraph) -> list[dict[str, Any]]:
    """Report foreground demands omitted from the process/provider graph."""
    linked_inputs = {
        (str(edge.toNode), _target_port_id(edge))
        for edge in graph.exchanges
        if _target_port_id(edge)
    }
    issues: list[dict[str, Any]] = []
    for node in graph.nodes:
        if node.hidden or node.node_kind == "lci_dataset":
            continue
        for port in node.inputs:
            if port.type == "biosphere" or float(port.amount or 0) <= 0:
                continue
            if (str(node.id), str(port.id)) in linked_inputs:
                continue
            issues.append({
                "code": "UNLINKED_POSITIVE_TECHNOSPHERE_INPUT",
                "message": "Positive technosphere input has no foreground or background provider and was omitted from upstream inventory.",
                "node_id": node.id,
                "node_name": node.name,
                "port_id": port.id,
                "flow_uuid": port.flowUuid,
                "flow_name": port.name,
                "amount": float(port.amount),
                "unit": port.unit,
            })
    return issues


def merge_calculation_issues(
    summary: object,
    solver_issues: object,
    integrity_issues: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[Any]]:
    merged_summary = dict(summary) if isinstance(summary, dict) else {}
    merged_issues = list(solver_issues) if isinstance(solver_issues, list) else []
    merged_issues.extend(integrity_issues)
    merged_summary["issue_count"] = len(merged_issues)
    merged_summary["unlinked_technosphere_input_count"] = len(integrity_issues)
    return merged_summary, merged_issues
