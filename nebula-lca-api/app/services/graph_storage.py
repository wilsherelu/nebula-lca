"""Graph storage helpers: slim, hydrate, hash, canonicalise.

Extracted from ``app.main`` as Stage 1 of the modularisation plan.
Only pure (or lightly DB-dependent) functions live here — no route logic,
no project/version CRUD, no normalisation contract.

Public API
----------
# canonicalisation / hashing
_canonical_json
_canonicalize_value_for_hash
_compute_graph_hash_from_graph_json
_compute_graph_hash_from_slim_graph
_compute_graph_hash_from_graph  # needs HybridGraph from schemas

# slim
_STORAGE_SLIM_VERSION
slim_graph_for_storage  # renamed from _slim_graph_for_storage

# hydrate
hydrate_graph_for_api  # renamed from _hydrate_graph_for_api

Callers in *main.py* should import from this module and keep their old
wrappers/thunks if any (none expected for the functions below).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from ..schemas import HybridGraph

# ── Slim version marker ──────────────────────────────────────────────────

_STORAGE_SLIM_VERSION = "graph_slim_v1"

# Fields that can be recomputed from flow_catalog / source_process at read time.
_FLOWPORT_DERIVED_KEYS: set[str] = {
    # Pure display fields - recomputed from flow_catalog at read time.
    "flow_name_en",
    "display_name_en",
}

# Keys whose presence in a dict signals a PTS module node.
_PTS_NODE_KIND = "pts_module"

# Shell fields kept for a root canvas.
_ROOT_CANVAS_SHELL_KEYS = {"id", "name", "kind"}


# ── Slim helpers ─────────────────────────────────────────────────────────


def slim_flowport_for_storage(port: dict) -> dict:
    """Strip derived FlowPort fields, keep only core modelling data."""
    return {k: v for k, v in port.items() if k not in _FLOWPORT_DERIVED_KEYS}


def slim_node_for_storage(node: dict) -> dict:
    """Strip derived FlowPort fields from nested port lists."""
    slim: dict[str, Any] = {}
    for key, val in node.items():
        if key in ("inputs", "outputs", "emissions") and isinstance(val, list):
            slim[key] = [slim_flowport_for_storage(p) for p in val]
        else:
            slim[key] = val
    return slim


def slim_pts_node_for_storage(node: dict) -> dict:
    """PTS nodes: save only the shell - never ship compile artifacts."""
    allowed_pts_keys: set[str] = {
        "id", "node_kind", "mode", "lci_role", "pts_uuid",
        "pts_published_version", "pts_published_artifact_id",
        "process_uuid", "name", "location", "reference_product",
        "allocation_method", "inputs", "outputs", "emissions",
        "metadata",
    }
    slim: dict[str, Any] = {}
    for key, val in node.items():
        if key not in allowed_pts_keys:
            continue
        if key in ("inputs", "outputs", "emissions") and isinstance(val, list):
            slim[key] = [slim_flowport_for_storage(p) for p in val]
        else:
            slim[key] = val
    return slim


def slim_graph_for_storage(graph_json: dict) -> dict:
    """Return a slim copy of the graph ready for persistent storage.

    - FlowPort: drops pure display fields (flow_name_en, display_name_en).
    - PTS nodes: keep only shell fields; no compile artifacts.
    - Root canvas: drops full nodes/edges snapshot (top-level nodes/exchanges are
      the source of truth; frontend rebuilds root from them on import).
    - Node positions: drops metadata.node_positions when all nodes have inline
      position, avoiding redundant storage.
    - Writes storage_schema_version into metadata.
    """
    slim = {
        "functionalUnit": graph_json.get("functionalUnit"),
        "nodes": [],
        "exchanges": graph_json.get("exchanges", []),
        "metadata": dict(graph_json.get("metadata") or {}),
    }
    nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_kind = str(node.get("node_kind", "unit_process"))
        if node_kind == _PTS_NODE_KIND:
            slim["nodes"].append(slim_pts_node_for_storage(node))
        else:
            slim["nodes"].append(slim_node_for_storage(node))

    md = slim["metadata"]

    # ── Root canvas slim ────────────────────────────────────────────────
    canvases = md.get("canvases")
    if isinstance(canvases, list):
        slim_canvases: list[dict] = []
        for canvas in canvases:
            if not isinstance(canvas, dict):
                slim_canvases.append(canvas)
                continue
            canvas_kind = str(canvas.get("kind", "") or "")
            canvas_id = str(canvas.get("id", "") or "")
            if canvas_kind == "root" or canvas_id == "root":
                # Only shell the root canvas when top-level exchanges are the
                # source of truth. Older graphs can still keep root edges only
                # under metadata.canvases[root], so dropping them would erase
                # the model topology on the next save.
                root_edges = canvas.get("edges")
                top_level_edges = graph_json.get("exchanges")
                can_shell_root = (
                    isinstance(top_level_edges, list)
                    and len(top_level_edges) > 0
                ) or not (isinstance(root_edges, list) and len(root_edges) > 0)
                if can_shell_root:
                    slim_canvases.append({
                        "id": canvas_id,
                        "name": canvas.get("name", "Product System"),
                        "kind": "root",
                    })
                else:
                    slim_canvas = dict(canvas)
                    raw_cn = slim_canvas.get("nodes")
                    if isinstance(raw_cn, list):
                        slim_canvas["nodes"] = [
                            slim_node_for_storage(n) if isinstance(n, dict) else n
                            for n in raw_cn
                        ]
                    slim_canvases.append(slim_canvas)
            else:
                # Non-root canvas (e.g. pts_internal): keep nodes/edges but
                # slim the port buckets inside each node.
                slim_canvas = dict(canvas)
                raw_cn = slim_canvas.get("nodes")
                if isinstance(raw_cn, list):
                    slim_canvas["nodes"] = [
                        slim_node_for_storage(n) if isinstance(n, dict) else n
                        for n in raw_cn
                    ]
                slim_canvases.append(slim_canvas)
        md["canvases"] = slim_canvases

    # ── Node positions slim ─────────────────────────────────────────────
    node_positions_key = "node_positions"
    if node_positions_key in md:
        all_have_position = (
            isinstance(nodes, list)
            and len(nodes) > 0
            and all(
                isinstance(n, dict) and isinstance(n.get("position"), dict)
                for n in nodes
            )
        )
        if all_have_position:
            # Every node has inline position -> drop redundant dict.
            md = {**md, node_positions_key: None}

    md["storage_schema_version"] = _STORAGE_SLIM_VERSION
    slim["metadata"] = md
    return slim


# ── Hydrate helpers ──────────────────────────────────────────────────────


def repair_impossible_flow_units(graph: Any, db: Any) -> list[dict[str, str]]:
    """Repair units that cannot belong to their saved unit group.

    The Flow catalog unit group is authoritative. A repair is made only when
    there is no explicit unit-group switch, the saved port group conflicts
    with the Flow group or its unit is not a member of that group, and the
    Flow default unit is a valid member.
    """
    if db is None:
        return []
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else getattr(graph, "nodes", [])
    ports: list[Any] = []
    flow_uuids: set[str] = set()
    for node in nodes or []:
        for bucket in ("inputs", "outputs", "emissions"):
            bucket_ports = node.get(bucket, []) if isinstance(node, dict) else getattr(node, bucket, [])
            for port in bucket_ports or []:
                ports.append(port)
                flow_uuid = str(
                    (port.get("flowUuid") or port.get("flow_uuid") or "")
                    if isinstance(port, dict)
                    else (getattr(port, "flowUuid", None) or getattr(port, "flow_uuid", None) or "")
                ).strip()
                if flow_uuid:
                    flow_uuids.add(flow_uuid)
    if not flow_uuids:
        return []

    from ..models import FlowRecord, UnitDefinition

    flow_meta: dict[str, tuple[str, str]] = {}
    for start in range(0, len(flow_uuids), 500):
        batch = list(flow_uuids)[start:start + 500]
        for row in (
            db.query(FlowRecord.flow_uuid, FlowRecord.unit_group, FlowRecord.default_unit)
            .filter(FlowRecord.flow_uuid.in_(batch))
            .all()
        ):
            flow_meta[str(row.flow_uuid)] = (str(row.unit_group or "").strip(), str(row.default_unit or "").strip())
    units_by_group: dict[str, set[str]] = {}
    for row in db.query(UnitDefinition.unit_group, UnitDefinition.unit_name).all():
        group = str(row.unit_group or "").strip().casefold()
        unit = str(row.unit_name or "").strip().casefold()
        if group and unit:
            units_by_group.setdefault(group, set()).add(unit)

    repairs: list[dict[str, str]] = []
    for port in ports:
        get_value = port.get if isinstance(port, dict) else lambda key, default=None: getattr(port, key, default)
        flow_uuid = str(get_value("flowUuid") or get_value("flow_uuid") or "").strip()
        flow_group, default_unit = flow_meta.get(flow_uuid, ("", ""))
        current_group = str(get_value("unitGroup") or get_value("unit_group") or "").strip()
        current_unit = str(get_value("unit") or "").strip()
        switch = get_value("unitGroupSwitch") or get_value("unit_group_switch")
        flow_group_units = units_by_group.get(flow_group.casefold(), set())
        group_conflicts = bool(current_group) and current_group.casefold() != flow_group.casefold()
        unit_conflicts = bool(current_unit) and current_unit.casefold() not in flow_group_units
        if (
            not switch
            and flow_group
            and current_unit
            and default_unit
            and flow_group_units
            and (group_conflicts or unit_conflicts)
            and default_unit.casefold() in flow_group_units
        ):
            if isinstance(port, dict):
                port["unit"] = default_unit
                port["unitGroup"] = flow_group
            else:
                setattr(port, "unit", default_unit)
                setattr(port, "unitGroup", flow_group)
            repairs.append({
                "port_id": str(get_value("id") or ""),
                "flow_uuid": flow_uuid,
                "from_unit": current_unit,
                "to_unit": default_unit,
                "unit_group": flow_group,
            })
    return repairs


def repair_tidas_product_flags(graph: Any) -> list[dict[str, str]]:
    """Map TIDAS quantitative references to Nebula product flags.

    ILCD/TIDAS output markers are not Nebula product definitions. A port is a
    product only when it is the exact quantitative-reference output, carries
    an explicit allocation factor, or was identified by the TIDAS import policy
    for quantity allocation.
    """
    nodes = graph.get("nodes", []) if isinstance(graph, dict) else getattr(graph, "nodes", [])
    repairs: list[dict[str, str]] = []
    for node in nodes or []:
        get_node = node.get if isinstance(node, dict) else lambda key, default=None: getattr(node, key, default)
        outputs = get_node("outputs", []) or []
        if not outputs:
            continue
        reference_uuid = str(
            get_node("reference_product_flow_uuid")
            or get_node("referenceProductFlowUuid")
            or ""
        ).strip()
        def port_value(port: Any, key: str, fallback: str | None = None) -> Any:
            if isinstance(port, dict):
                return port.get(key, port.get(fallback)) if fallback else port.get(key)
            return getattr(port, key, getattr(port, fallback, None) if fallback else None)

        candidates = [
            port for port in outputs
            if reference_uuid and str(port_value(port, "flowUuid", "flow_uuid") or "").strip() == reference_uuid
        ]
        if len(candidates) != 1:
            continue
        reference_port = candidates[0]
        reference_flow_uuid = str(port_value(reference_port, "flowUuid", "flow_uuid") or "").strip()
        changed = False
        for port in outputs:
            allocation_factor = port_value(port, "allocationFactor", "allocation_factor")
            allocation_basis = port_value(port, "allocationBasis", "allocation_basis")
            allocation_method = str(
                allocation_basis.get("method") if isinstance(allocation_basis, dict) else ""
            ).strip()
            should_be_product = (
                port is reference_port
                or allocation_factor is not None
                or allocation_method in {"quantity", "manual_factor"}
            )
            current = bool(port_value(port, "isProduct", "is_product"))
            if current == should_be_product:
                continue
            if isinstance(port, dict):
                port["isProduct"] = should_be_product
            else:
                setattr(port, "isProduct", should_be_product)
            changed = True
        if isinstance(node, dict):
            node["reference_product_flow_uuid"] = reference_flow_uuid
            node["reference_product_direction"] = "output"
        else:
            setattr(node, "reference_product_flow_uuid", reference_flow_uuid)
            setattr(node, "reference_product_direction", "output")
        if changed:
            repairs.append({
                "node_id": str(get_node("id") or ""),
                "reference_product_flow_uuid": reference_flow_uuid,
            })
    return repairs


def hydrate_graph_for_api(graph_json: dict, db: Any) -> dict:
    """Restore display fields into a slim-stored graph for API responses.

    Accepts either the raw SQLAlchemy row object (which has a
    ``.hybrid_graph_json`` attribute) or the already-extracted dict.
    """
    if not isinstance(graph_json, dict):
        return graph_json

    # Legacy compatibility: some older saves kept the root topology only under
    # metadata.canvases[root]. When top-level exchanges are empty, promote that
    # root canvas back to the API graph so run/save paths see the real edges.
    top_level_edges = graph_json.get("exchanges")
    if not (isinstance(top_level_edges, list) and len(top_level_edges) > 0):
        md_for_root = graph_json.get("metadata") or {}
        canvases = md_for_root.get("canvases")
        if isinstance(canvases, list):
            root_canvas = next(
                (
                    c for c in canvases
                    if isinstance(c, dict)
                    and (str(c.get("kind") or "") == "root" or str(c.get("id") or "") == "root")
                ),
                None,
            )
            if isinstance(root_canvas, dict):
                root_edges = root_canvas.get("edges")
                root_nodes = root_canvas.get("nodes")
                if isinstance(root_edges, list) and len(root_edges) > 0:
                    promoted = dict(graph_json)
                    promoted["exchanges"] = root_edges
                    if isinstance(root_nodes, list) and len(root_nodes) > 0:
                        promoted["nodes"] = root_nodes
                    graph_json = promoted

    md = graph_json.get("metadata") or {}
    if md.get("storage_schema_version") == _STORAGE_SLIM_VERSION:
        nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
        flow_uuids: list[str] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            for bucket in ("inputs", "outputs", "emissions"):
                ports = node.get(bucket)
                if not isinstance(ports, list):
                    continue
                for port in ports:
                    fu = str(port.get("flowUuid", "") or "").strip()
                    if fu and fu not in flow_uuids:
                        flow_uuids.append(fu)

        flow_meta: dict[str, tuple[str, str]] = {}
        if flow_uuids:
            from ..models import FlowRecord

            batch_size = 500
            for i in range(0, len(flow_uuids), batch_size):
                batch = flow_uuids[i:i + batch_size]
                try:
                    rows = (
                        db.query(
                            FlowRecord.flow_uuid,
                            FlowRecord.flow_name_en,
                            FlowRecord.unit_group,
                        )
                        .filter(FlowRecord.flow_uuid.in_(batch))
                        .all()
                    )
                    for r in rows:
                        flow_meta[str(r.flow_uuid)] = (r.flow_name_en, r.unit_group)
                except Exception:
                    pass

        def _hydrate_flowport(port: dict) -> dict:
            if not isinstance(port, dict):
                return port
            result = dict(port)
            switch = result.get("unitGroupSwitch") if isinstance(result.get("unitGroupSwitch"), dict) else {}
            switch_target_group = str(
                switch.get("targetUnitGroup")
                or switch.get("target_unit_group")
                or ""
            ).strip()
            if switch_target_group and not result.get("unitGroup"):
                result["unitGroup"] = switch_target_group
            fu = str(port.get("flowUuid", "") or "").strip()
            if fu and fu in flow_meta:
                fn_en, ug = flow_meta[fu]
                if not result.get("flow_name_en"):
                    result["flow_name_en"] = fn_en
                if not result.get("unitGroup"):
                    result["unitGroup"] = ug
            return result

        def _hydrate_node(node: dict) -> dict:
            if not isinstance(node, dict):
                return node
            result = dict(node)
            for bucket in ("inputs", "outputs", "emissions"):
                ports = result.get(bucket)
                if isinstance(ports, list):
                    result[bucket] = [_hydrate_flowport(p) for p in ports]
            return result

        slim_nodes = graph_json.get("nodes")
        if isinstance(slim_nodes, list):
            slim_nodes = [_hydrate_node(n) for n in slim_nodes]
            result = dict(graph_json)
            result["nodes"] = slim_nodes
            return result

    return graph_json


# ── Canonicalisation / hashing ──────────────────────────────────────────

_UI_HASH_NOISE_KEYS: set[str] = {
    "selected",
    "dragging",
    "resizing",
    "positionAbsolute",
    "width",
    "height",
    "__rf",
    "updatedAt",
    "createdAt",
    "lastSavedAt",
    "lastModifiedAt",
    "uiTimestamp",
    "clientTimestamp",
    "tempId",
    "ephemeral",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert *value* to float."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _round_float_for_hash(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 12)
    return value


def canonical_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonicalize_value_for_hash(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key in sorted(value.keys()):
            if key in _UI_HASH_NOISE_KEYS:
                continue
            sanitized[key] = canonicalize_value_for_hash(value[key])
        return sanitized
    if isinstance(value, list):
        items = [canonicalize_value_for_hash(item) for item in value]
        if items and all(isinstance(item, dict) for item in items):
            def _item_key(item: dict) -> tuple:
                return (
                    str(item.get("id") or ""),
                    str(item.get("nodeId") or ""),
                    str(item.get("kind") or ""),
                    str(item.get("parentPtsNodeId") or ""),
                    str(item.get("parentNodeId") or ""),
                    str(item.get("fromNode") or ""),
                    str(item.get("toNode") or ""),
                    str(item.get("flowUuid") or ""),
                    str(item.get("process_uuid") or item.get("processUuid") or ""),
                )

            items = sorted(items, key=lambda item: _item_key(item))
        return items
    return _round_float_for_hash(value)


def compute_graph_hash_from_graph_json(graph_json: dict) -> str:
    # Import here to avoid circular import — _normalize_graph_json_for_storage
    # now lives in graph_contract.
    from ..services.graph_contract import _normalize_graph_json_for_storage

    normalized = _normalize_graph_json_for_storage(graph_json)
    canonical_value = canonicalize_value_for_hash(normalized)
    canonical_json_str = canonical_json(canonical_value)
    return hashlib.sha256(canonical_json_str.encode("utf-8")).hexdigest()


def compute_graph_hash_from_slim_graph(slim_graph_json: dict) -> str:
    """Compute hash directly on already-slimmed graph (bypass normalize again)."""
    canonical_value = canonicalize_value_for_hash(slim_graph_json)
    canonical_json_str = canonical_json(canonical_value)
    return hashlib.sha256(canonical_json_str.encode("utf-8")).hexdigest()


def compute_graph_hash_from_graph(graph: HybridGraph) -> str:
    return compute_graph_hash_from_graph_json(graph.model_dump(mode="python"))
