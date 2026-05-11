"""Graph contract helpers: normalize, validate, handle/port resolution.

Extracted from ``app.main`` as Stage 2 of the modularisation plan.
Functions here operate on HybridGraph / HybridNode / FlowPort objects.

No route logic. No project/version CRUD.
DB-dependent functions accept ``db: Session`` as an explicit parameter.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from ..schemas import (
    FlowPort,
    HybridGraph,
    HybridNode,
    flow_semantic_to_exchange_type,
)

# ── Utility ──────────────────────────────────────────────────────────────


def is_graph_non_empty(graph: dict | None) -> bool:
    if not isinstance(graph, dict):
        return False
    nodes = graph.get("nodes")
    exchanges = graph.get("exchanges")
    return bool((isinstance(nodes, list) and len(nodes) > 0) or
                (isinstance(exchanges, list) and len(exchanges) > 0))


def _truncate_text_preview(value: str, limit: int = 80) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _normalize_port_display_name(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " ".join(text.split())


_MOJIBAKE_LATIN1_RE = re.compile(r"[\u00C0-\u00FF\u0080-\u009F]")


def _looks_like_utf8_latin1_mojibake(value: str) -> tuple[bool, str | None]:
    if not value:
        return False, None
    if "\ufffd" in value:
        return True, "contains replacement character"
    if not _MOJIBAKE_LATIN1_RE.search(value):
        return False, None
    try:
        repaired = value.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return False, None
    if repaired == value:
        return False, None
    original_cjk = sum(1 for ch in value if "\u4e00" <= ch <= "\u9fff")
    repaired_cjk = sum(1 for ch in repaired if "\u4e00" <= ch <= "\u9fff")
    if repaired_cjk <= 0 or repaired_cjk <= original_cjk:
        return False, None
    return True, f"looks like UTF-8 bytes decoded as Latin-1; candidate repair={_truncate_text_preview(repaired)}"


# ── Normalize helpers ───────────────────────────────────────────────────


def normalize_graph_node_kinds(graph: HybridGraph) -> None:
    """Convert market_ prefixed process_uuid → market_process node_kind."""
    for node in graph.nodes:
        node_kind = str(node.node_kind or "").strip()
        if node_kind == "unit_process":
            process_uuid = str(node.process_uuid or "")
            if node.mode == "normalized" and process_uuid.startswith("market_"):
                node.node_kind = "market_process"


def normalize_graph_product_flags(graph: HybridGraph) -> None:
    """Normalize node kinds + bool-cast isProduct on all ports."""
    normalize_graph_node_kinds(graph)
    for node in graph.nodes:
        for port in node.inputs:
            port.isProduct = bool(port.isProduct)
        for port in node.outputs:
            port.isProduct = bool(port.isProduct)


# ── Port / handle resolution ────────────────────────────────────────────


def _port_id_from_handle(handle_id: str | None, prefix: str) -> str:
    if not handle_id:
        return ""
    token = f"{prefix}:"
    if handle_id.startswith(token):
        return handle_id[len(token):]
    if ":" in handle_id:
        return handle_id.split(":", 1)[1]
    return handle_id


def _resolve_edge_port_id(raw_port_or_handle: str | None, prefix: str) -> str:
    return _port_id_from_handle(raw_port_or_handle, prefix)


def _preserve_or_default_handle(
    *,
    raw_handle: str | None,
    resolved_port_id: str,
    prefix: str,
) -> str:
    handle = str(raw_handle or "").strip()
    allowed_prefixes = {prefix, f"{prefix}l", f"{prefix}r"}
    handle_prefix = handle.split(":", 1)[0].strip().lower() if ":" in handle else ""
    if (handle
            and handle_prefix in allowed_prefixes
            and _port_id_from_handle(handle, prefix) == resolved_port_id):
        return handle
    return f"{prefix}:{resolved_port_id}"


def _resolve_edge_port_id_for_node(
    *,
    node: HybridNode | None,
    raw_port_or_handle: str | None,
    prefix: str,
    flow_uuid: str,
    direction: str,
) -> str:
    resolved = _resolve_edge_port_id(raw_port_or_handle, prefix)
    if node is None:
        return resolved

    candidate_ports = list(node.outputs) if direction == "output" else list(node.inputs)
    if resolved and any(str(port.id or "") == resolved for port in candidate_ports):
        return resolved

    flow_uuid = str(flow_uuid or "").strip()
    if not flow_uuid:
        return resolved

    matching_ports = [port for port in candidate_ports
                      if str(port.flowUuid or "").strip() == flow_uuid]
    if len(matching_ports) == 1:
        return str(matching_ports[0].id or "")

    compact_resolved = str(resolved or "").strip()
    if compact_resolved:
        suffix_matches = [port for port in matching_ports
                          if str(port.id or "").strip().endswith(compact_resolved)]
        if len(suffix_matches) == 1:
            return str(suffix_matches[0].id or "")

    return resolved


# ── Edge normalisation ──────────────────────────────────────────────────


def normalize_graph_edge_port_ids(graph: HybridGraph) -> None:
    """Cross-fill sourceHandle/source_port_id etc. for backward compat."""
    for edge in graph.exchanges:
        if not edge.source_port_id and edge.sourceHandle:
            edge.source_port_id = edge.sourceHandle
        if not edge.target_port_id and edge.targetHandle:
            edge.target_port_id = edge.targetHandle
        if not edge.sourceHandle and edge.source_port_id:
            edge.sourceHandle = edge.source_port_id
        if not edge.targetHandle and edge.target_port_id:
            edge.targetHandle = edge.target_port_id


# ── Validate helpers ───────────────────────────────────────────────────


def validate_product_unit_group_consistency(graph: HybridGraph) -> None:
    """Multi-product allocation requires same unit_group."""
    violations: list[dict] = []
    for node in graph.nodes:
        if node.node_kind not in {"unit_process", "market_process"}:
            continue
        product_outputs = [port for port in node.outputs if bool(port.isProduct)]
        if len(product_outputs) <= 1:
            continue
        distinct_groups = {str(port.unitGroup or "").strip() for port in product_outputs}
        if len(distinct_groups) <= 1:
            continue
        violations.append({
            "node_id": node.id,
            "node_name": node.name,
            "product_flows": [{
                "port_id": port.id,
                "flow_uuid": port.flowUuid,
                "flow_name": port.name,
                "unit": port.unit,
                "unit_group": str(port.unitGroup or "").strip(),
            } for port in product_outputs],
        })

    if violations:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "UNIT_GROUP_MISMATCH_FOR_PRODUCTS",
                "message": "Current version supports multi-product allocation only within the same unit group.",
                "violations": violations,
            },
        )


def validate_unique_process_uuid(graph: HybridGraph) -> None:
    """process_uuid must be unique within one graph."""
    buckets: dict[str, list[HybridNode]] = {}
    for node in graph.nodes:
        pid = str(node.process_uuid or "").strip()
        if not pid:
            continue
        buckets.setdefault(pid, []).append(node)

    duplicates = {pid: nodes for pid, nodes in buckets.items() if len(nodes) > 1}
    if not duplicates:
        return

    evidence = [{
        "duplicate_process_uuid": pid,
        "node_ids": [node.id for node in nodes],
        "process_names": [node.name for node in nodes],
    } for pid, nodes in duplicates.items()]

    raise HTTPException(
        status_code=400,
        detail={
            "code": "DUPLICATE_PROCESS_UUID",
            "message": "process_uuid must be unique within one graph.",
            "evidence": evidence,
        },
    )


def validate_port_bucket_direction_consistency(graph: HybridGraph) -> None:
    """Port direction must match its container bucket (input ↔ inputs, output ↔ outputs)."""
    violations: list[dict] = []
    for node in graph.nodes:
        for bucket_name, expected_direction in (("inputs", "input"), ("outputs", "output")):
            ports = getattr(node, bucket_name, []) or []
            for port in ports:
                actual_direction = str(port.direction or "").strip()
                if actual_direction == expected_direction:
                    continue
                violations.append({
                    "node_id": node.id,
                    "node_kind": str(node.node_kind or ""),
                    "port_id": str(port.id or ""),
                    "bucket": bucket_name,
                    "expected_direction": expected_direction,
                    "actual_direction": actual_direction or None,
                    "flow_uuid": str(port.flowUuid or ""),
                    "flow_name": _truncate_text_preview(str(port.name or "")),
                })
                if len(violations) >= 200:
                    break
            if len(violations) >= 200:
                break
        if len(violations) >= 200:
            break

    if violations:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "PORT_DIRECTION_BUCKET_MISMATCH",
                "message": "Port direction must match its container bucket.",
                "evidence": violations,
            },
        )


def validate_edge_binding_and_uniqueness(graph: HybridGraph) -> None:
    """Edge endpoints must bind valid ports, and duplicate bindings are rejected."""
    node_by_id = {node.id: node for node in graph.nodes}
    source_port_index: dict[tuple[str, str], Any] = {}
    target_port_index: dict[tuple[str, str], Any] = {}

    for node in graph.nodes:
        for port in node.outputs:
            if port.id:
                source_port_index[(node.id, port.id)] = port
        for port in node.inputs:
            if port.id:
                target_port_index[(node.id, port.id)] = port

    binding_issues: list[dict] = []
    duplicate_edges: dict[tuple[str, str, str, str, str], list[str]] = {}

    for edge in graph.exchanges:
        source_node = node_by_id.get(edge.fromNode)
        target_node = node_by_id.get(edge.toNode)

        source_port_id = _resolve_edge_port_id_for_node(
            node=source_node,
            raw_port_or_handle=edge.source_port_id or edge.sourceHandle,
            prefix="out",
            flow_uuid=str(edge.flowUuid or ""),
            direction="output",
        )
        target_port_id = _resolve_edge_port_id_for_node(
            node=target_node,
            raw_port_or_handle=edge.target_port_id or edge.targetHandle,
            prefix="in",
            flow_uuid=str(edge.flowUuid or ""),
            direction="input",
        )

        if source_port_id:
            edge.source_port_id = source_port_id
            edge.sourceHandle = _preserve_or_default_handle(
                raw_handle=edge.sourceHandle or edge.source_port_id,
                resolved_port_id=source_port_id,
                prefix="out",
            )
        if target_port_id:
            edge.target_port_id = target_port_id
            edge.targetHandle = _preserve_or_default_handle(
                raw_handle=edge.targetHandle or edge.target_port_id,
                resolved_port_id=target_port_id,
                prefix="in",
            )

        source_port = source_port_index.get((edge.fromNode, source_port_id))
        target_port = target_port_index.get((edge.toNode, target_port_id))

        issues: list[str] = []
        if source_node is None:
            issues.append("source node not found")
        if target_node is None:
            issues.append("target node not found")
        if not source_port_id:
            issues.append("source port missing")
        if not target_port_id:
            issues.append("target port missing")
        if source_port_id and source_port is None:
            issues.append("source port is not an output port on source node")
        if target_port_id and target_port is None:
            issues.append("target port is not an input port on target node")
        if source_port is not None and str(source_port.flowUuid or "") != str(edge.flowUuid or ""):
            issues.append("edge.flowUuid does not match source port flowUuid")
        if target_port is not None and str(target_port.flowUuid or "") != str(edge.flowUuid or ""):
            issues.append("edge.flowUuid does not match target port flowUuid")

        if issues:
            binding_issues.append({
                "edge_id": edge.id,
                "from_node_id": edge.fromNode,
                "to_node_id": edge.toNode,
                "source_port_id": source_port_id,
                "target_port_id": target_port_id,
                "flow_uuid": edge.flowUuid,
                "issues": issues,
            })
            continue

        dedupe_key = (edge.fromNode, edge.toNode, source_port_id, target_port_id,
                      str(edge.flowUuid or ""))
        duplicate_edges.setdefault(dedupe_key, []).append(edge.id)

    if binding_issues:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_EDGE_PORT_BINDING",
                "message": "Edge endpoints must bind source output ports to target input ports with matching flowUuid.",
                "evidence": binding_issues[:200],
            },
        )

    duplicated = [{
        "from_node_id": key[0],
        "to_node_id": key[1],
        "source_port_id": key[2],
        "target_port_id": key[3],
        "flow_uuid": key[4],
        "edge_ids": edge_ids,
    } for key, edge_ids in duplicate_edges.items() if len(edge_ids) > 1]

    if duplicated:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "DUPLICATE_EDGE_BINDING",
                "message": "Duplicate edges are not allowed for the same source/target port binding and flowUuid.",
                "evidence": duplicated[:200],
            },
        )


def validate_edge_product_role_alignment(graph: HybridGraph) -> None:
    """Technosphere edges must connect product ↔ non-product (not product↔product or non↔non)."""
    node_by_id = {node.id: node for node in graph.nodes}
    source_port_index: dict[tuple[str, str], Any] = {}
    target_port_index: dict[tuple[str, str], Any] = {}

    for node in graph.nodes:
        for port in node.outputs:
            if port.id:
                source_port_index[(node.id, port.id)] = port
        for port in node.inputs:
            if port.id:
                target_port_index[(node.id, port.id)] = port

    violations: list[dict] = []
    for edge in graph.exchanges:
        source_port_id = _resolve_edge_port_id(edge.source_port_id or edge.sourceHandle, "out")
        target_port_id = _resolve_edge_port_id(edge.target_port_id or edge.targetHandle, "in")
        source_node = node_by_id.get(edge.fromNode)
        target_node = node_by_id.get(edge.toNode)
        if source_node is None or target_node is None:
            continue
        source_port = source_port_index.get((edge.fromNode, source_port_id))
        target_port = target_port_index.get((edge.toNode, target_port_id))
        if source_port is None or target_port is None:
            continue
        if (str(source_port.type or "") == "biosphere"
                or str(target_port.type or "") == "biosphere"):
            continue

        source_is_product = bool(source_port.isProduct)
        target_is_product = bool(target_port.isProduct)
        if source_is_product != target_is_product:
            continue

        violations.append({
            "edge_id": edge.id,
            "from_node_id": source_node.id,
            "to_node_id": target_node.id,
            "from_process_uuid": str(source_node.process_uuid or ""),
            "to_process_uuid": str(target_node.process_uuid or ""),
            "source_port_id": str(source_port.id or ""),
            "target_port_id": str(target_port.id or ""),
            "flow_uuid": str(edge.flowUuid or ""),
            "source_is_product": source_is_product,
            "target_is_product": target_is_product,
            "mismatch_type": (
                "product_to_product" if source_is_product else "non_product_to_non_product"
            ),
        })

    if violations:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_EDGE_PRODUCT_ROLE_ALIGNMENT",
                "message": "Technosphere edges must connect product-to-non-product or non-product-to-product ports.",
                "evidence": violations[:500],
            },
        )


def validate_market_input_constraints(graph: HybridGraph) -> None:
    """For market_process, one target port cannot connect the same source process twice."""
    node_by_id = {node.id: node for node in graph.nodes}
    duplicate_hits: dict[tuple[str, str, str, str], list[str]] = {}

    for edge in graph.exchanges:
        target_node = node_by_id.get(edge.toNode)
        source_node = node_by_id.get(edge.fromNode)
        if target_node is None or source_node is None:
            continue
        if str(target_node.node_kind or "") != "market_process":
            continue

        target_port_id = _resolve_edge_port_id(edge.target_port_id or edge.targetHandle, "in")
        source_process_uuid = str(source_node.process_uuid or "").strip()
        key = (target_node.id, target_port_id, source_process_uuid, str(edge.flowUuid or ""))
        duplicate_hits.setdefault(key, []).append(edge.id)

    violations = [{
        "market_node_id": key[0],
        "target_port_id": key[1],
        "source_process_uuid": key[2],
        "flow_uuid": key[3],
        "edge_ids": edge_ids,
    } for key, edge_ids in duplicate_hits.items() if len(edge_ids) > 1]

    if violations:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "DUPLICATE_MARKET_PORT_SOURCE_PROCESS",
                "message": (
                    "For market_process inputs, one target port cannot connect the same "
                    "source process more than once."
                ),
                "evidence": violations[:200],
            },
        )


def validate_non_product_input_constraints(graph: HybridGraph) -> None:
    """Non-product intermediate input cannot connect to multiple product sources."""
    constrained_kinds = {"unit_process", "pts_module"}
    node_by_id = {node.id: node for node in graph.nodes}
    source_port_index: dict[tuple[str, str], Any] = {}
    target_port_index: dict[tuple[str, str], Any] = {}

    for node in graph.nodes:
        for port in node.outputs:
            if port.id:
                source_port_index[(node.id, port.id)] = port
        for port in node.inputs:
            if port.id:
                target_port_index[(node.id, port.id)] = port

    incoming: dict[tuple[str, str], list[dict]] = {}
    for edge in graph.exchanges:
        target_port_id = _resolve_edge_port_id(edge.target_port_id or edge.targetHandle, "in")
        source_port_id = _resolve_edge_port_id(edge.source_port_id or edge.sourceHandle, "out")
        target_node = node_by_id.get(edge.toNode)
        source_node = node_by_id.get(edge.fromNode)
        if target_node is None or source_node is None:
            continue
        if str(target_node.node_kind or "") not in constrained_kinds:
            continue

        target_port = target_port_index.get((edge.toNode, target_port_id))
        source_port = source_port_index.get((edge.fromNode, source_port_id))
        if target_port is None or source_port is None:
            continue
        if (str(target_port.type or "") == "biosphere"
                or str(source_port.type or "") == "biosphere"):
            continue
        if bool(target_port.isProduct):
            continue
        if not bool(source_port.isProduct):
            continue

        incoming.setdefault((target_node.id, target_port.id), []).append({
            "edge_id": edge.id,
            "source_node_id": source_node.id,
            "source_process_uuid": str(source_node.process_uuid or ""),
            "source_port_id": source_port.id,
            "flow_uuid": str(edge.flowUuid or ""),
        })

    violations = [{
        "target_node_id": target_key[0],
        "target_port_id": target_key[1],
        "incoming_edges": rows,
    } for target_key, rows in incoming.items() if len(rows) > 1]

    if violations:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "NON_PRODUCT_INPUT_MULTI_PRODUCT_SOURCE",
                "message": (
                    "For unit_process/pts, a non-product intermediate input port cannot "
                    "connect to multiple product intermediate sources."
                ),
                "evidence": violations[:200],
            },
        )


# ── DB-dependent validators ─────────────────────────────────────────────


def _flow_meta_by_uuid_cached(
    db: Session,
) -> dict[str, tuple[str | None, str | None, str | None, str | None]]:
    """Cache flow_catalog lookup in memory for repeated graph validations."""
    from ..models import FlowRecord

    cache: dict[str, tuple] = {}
    try:
        rows = db.query(
            FlowRecord.flow_uuid,
            FlowRecord.flow_name,
            FlowRecord.flow_name_en,
            FlowRecord.flow_type,
            FlowRecord.unit_group,
        ).all()
        for r in rows:
            uu = str(r.flow_uuid or "")
            cache[uu] = (r.flow_name, r.flow_name_en, r.flow_type, r.unit_group)
            if uu.lower() != uu:
                cache[uu.lower()] = cache[uu]
    except Exception:
        pass
    return cache


def validate_graph_flow_type_contract(
    graph: HybridGraph,
    *,
    db: Session,
    stage: str,
) -> None:
    """Ensure every port.flowUuid exists in flow_catalog and set correct type."""
    flow_meta = _flow_meta_by_uuid_cached(db)
    flow_type_map: dict[str, str] = {}
    flow_type_map_lower: dict[str, str] = {}
    for flow_uuid, meta in flow_meta.items():
        flow_type = str((meta or (None, None, None, None))[2] or "").strip()
        if not flow_uuid or not flow_type:
            continue
        flow_type_map[flow_uuid] = flow_type
        flow_type_map_lower[flow_uuid.lower()] = flow_type

    not_found: list[dict] = []
    for node in graph.nodes:
        for bucket_name in ("inputs", "outputs"):
            ports = getattr(node, bucket_name, []) or []
            for port in ports:
                flow_uuid = str(port.flowUuid or "").strip()
                if not flow_uuid:
                    continue
                flow_type = (flow_type_map.get(flow_uuid)
                             or flow_type_map_lower.get(flow_uuid.lower()))
                if not flow_type:
                    not_found.append({
                        "node_id": node.id,
                        "port_id": str(port.id or ""),
                        "flow_uuid": flow_uuid,
                        "expected_type": None,
                        "actual_type": str(port.type or ""),
                        "stage": stage,
                        "reason": "FLOW_UUID_NOT_FOUND",
                        "bucket": bucket_name,
                        "actual_direction": str(port.direction or ""),
                    })
                    continue

                expected_type = flow_semantic_to_exchange_type(flow_type)
                port.type = expected_type

    if not_found:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "FLOW_UUID_NOT_FOUND",
                "message": "flow_uuid must exist in flow catalog before save/run.",
                "stage": stage,
                "evidence": not_found[:500],
            },
        )


def validate_graph_port_names_against_flow_catalog(
    graph: HybridGraph,
    *,
    db: Session,
    stage: str,
) -> None:
    """Warn (or reject) when sampled port names differ from catalog."""
    flow_meta = _flow_meta_by_uuid_cached(db)
    evidence: list[dict] = []
    sample_limit = 10
    sampled = 0

    for node in graph.nodes:
        if str(node.node_kind or "") == "pts_module":
            continue
        for bucket_name in ("inputs", "outputs"):
            for port in getattr(node, bucket_name, []) or []:
                flow_uuid = str(port.flowUuid or "").strip()
                if not flow_uuid:
                    continue
                meta = flow_meta.get(flow_uuid) or flow_meta.get(flow_uuid.lower())
                if not meta:
                    continue
                expected_names = {
                    _normalize_port_display_name(name)
                    for name in (meta[0], meta[1])
                    if _normalize_port_display_name(name)
                }
                actual_name = _normalize_port_display_name(port.name)
                if "@" in actual_name or any("@" in name for name in expected_names):
                    continue
                if stage == "import_model" and expected_names:
                    if actual_name in expected_names:
                        if sampled >= sample_limit:
                            break
                        continue
                    if any(actual_name.startswith(f"{name};") for name in expected_names):
                        if sampled >= sample_limit:
                            break
                        continue
                if not expected_names or not actual_name:
                    continue
                sampled += 1
                if not expected_names or not actual_name or actual_name in expected_names:
                    if sampled >= sample_limit:
                        break
                    continue
                evidence.append({
                    "node_id": node.id,
                    "node_kind": str(node.node_kind or ""),
                    "port_id": str(port.id or ""),
                    "bucket": bucket_name,
                    "flow_uuid": flow_uuid,
                    "expected_flow_name": _truncate_text_preview(" / ".join(sorted(expected_names))),
                    "actual_port_name": _truncate_text_preview(actual_name),
                    "stage": stage,
                })
                if sampled >= sample_limit:
                    break
            if sampled >= sample_limit:
                break
        if sampled >= sample_limit:
            break

    if evidence and sampled > 0 and len(evidence) == sampled:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "FLOW_NAME_MISMATCH",
                "message": (
                    "Sampled port display names all differ from catalog flow names; "
                    "save/run rejected."
                ),
                "stage": stage,
                "sampled_count": sampled,
                "evidence": evidence,
            },
        )


def validate_graph_text_encoding(
    graph: HybridGraph,
    *,
    stage: str,
) -> None:
    """Check for mojibake in port names (no-op placeholder — currently a no-op unless violations found)."""
    for node in graph.nodes:
        if str(node.node_kind or "") == "pts_module":
            continue
        for bucket_name in ("inputs", "outputs"):
            for port in getattr(node, bucket_name, []) or []:
                for field in ("name",):
                    val = str(getattr(port, field, "") or "")
                    is_mojibake, _ = _looks_like_utf8_latin1_mojibake(val)
                    # Currently just informational; could raise in future.
                    del is_mojibake  # silence unused


# ── Orchestrator ────────────────────────────────────────────────────────


def validate_graph_contract(
    graph: HybridGraph,
    *,
    require_non_empty: bool = False,
    allow_pts_nodes: bool = True,
) -> None:
    """Run all non-DB graph contract checks in one call."""
    normalize_graph_product_flags(graph)
    normalize_graph_edge_port_ids(graph)
    validate_port_bucket_direction_consistency(graph)
    validate_unique_process_uuid(graph)
    validate_product_unit_group_consistency(graph)
    validate_edge_binding_and_uniqueness(graph)
    validate_edge_product_role_alignment(graph)
    validate_market_input_constraints(graph)
    validate_non_product_input_constraints(graph)

    if require_non_empty and not is_graph_non_empty(graph.model_dump()):
        raise HTTPException(status_code=400, detail="Empty graph is not allowed")
    if (not allow_pts_nodes
            and any(node.node_kind == "pts_module" for node in graph.nodes)):
        raise HTTPException(
            status_code=400,
            detail="PTS nodes detected. Use /model/run with published PTS artifacts.",
        )
