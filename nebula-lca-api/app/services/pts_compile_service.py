"""PTS compile pipeline helpers.

Extracted from ``app.main`` for Stage 6C.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from ..models import PtsCompileArtifact
from ..schemas import HybridGraph
from .graph_contract import normalize_graph_product_flags
from ..pts_compile import PTS_COMPILE_SCHEMA_VERSION, compile_pts, compute_pts_graph_hash
from .pts_resources import (
    _apply_pts_resource_policy_override,
    _get_pts_resource_ports_policy,
    extract_pts_definition,
    upsert_pts_compile_artifact,
    upsert_pts_definition,
)

def _raise_pts_compile_value_error_http(exc: ValueError, pts_node_id: str) -> None:
    raw = str(exc)
    code = "INVALID_PTS_EXPORT_PORTS"
    message = raw
    evidence: list[dict] = [{"pts_node_id": pts_node_id, "error": raw}]
    if "|" in raw:
        parts = raw.split("|", 2)
        if len(parts) >= 2:
            code = parts[0] or code
            message = parts[1] or message
        if len(parts) == 3:
            try:
                parsed = json.loads(parts[2])
                parsed_evidence = parsed.get("evidence")
                if isinstance(parsed_evidence, list):
                    evidence = parsed_evidence
            except Exception:
                pass
    raise HTTPException(status_code=400, detail={"code": code, "message": message, "evidence": evidence}) from exc

def _build_process_unit_map_from_snapshot(snapshot: dict) -> dict[str, dict]:
    processes = snapshot.get("processes") if isinstance(snapshot, dict) else []
    exchanges = snapshot.get("exchanges") if isinstance(snapshot, dict) else []
    flows = snapshot.get("flows") if isinstance(snapshot, dict) else []
    process_rows = [p for p in processes if isinstance(p, dict)]
    exchange_rows = [e for e in exchanges if isinstance(e, dict)]
    flow_rows = [f for f in flows if isinstance(f, dict)]

    exchange_by_id = {str(row.get("exchange_id") or ""): row for row in exchange_rows}
    flow_by_uuid = {str(row.get("flow_uuid") or ""): row for row in flow_rows}
    process_unit_map: dict[str, dict] = {}

    for process in process_rows:
        process_uuid = str(process.get("process_uuid") or "")
        ref_exchange_id = str(process.get("reference_product_flow_uuid") or "")
        ref_exchange = exchange_by_id.get(ref_exchange_id, {})
        flow_uuid = str(ref_exchange.get("flow_uuid") or "")
        flow_row = flow_by_uuid.get(flow_uuid, {})
        process_unit_map[process_uuid] = {
            "reference_flow_uuid": flow_uuid,
            "reference_unit": str(flow_row.get("default_unit_uuid") or ""),
            "reference_unit_group": str(flow_row.get("unit_group_uuid") or ""),
        }
    return process_unit_map

def _build_process_unit_map_from_graph(graph: HybridGraph) -> dict[str, dict]:
    process_unit_map: dict[str, dict] = {}
    for node in graph.nodes:
        process_uuid = str(node.process_uuid or "")
        if not process_uuid:
            continue

        ref_port = next((port for port in node.outputs if bool(port.isProduct) and port.type != "biosphere"), None)
        if ref_port is None:
            ref_port = next((port for port in node.outputs if port.type != "biosphere"), None)
        if ref_port is None:
            ref_port = next((port for port in node.inputs if bool(port.isProduct) and port.type != "biosphere"), None)
        if ref_port is None:
            ref_port = next((port for port in node.inputs if port.type != "biosphere"), None)
        if ref_port is None:
            continue

        process_unit_map[process_uuid] = {
            "reference_flow_uuid": str(ref_port.flowUuid or ""),
            "reference_unit": str(ref_port.unit or ""),
            "reference_unit_group": str(ref_port.unitGroup or ""),
        }
    return process_unit_map

def _build_product_result_view_from_graph(
    *,
    graph: HybridGraph,
    process_index: object,
    values: object,
) -> tuple[list[dict], dict[str, dict], object]:
    if not isinstance(process_index, list) or not process_index:
        return [], {}, values

    products_by_process: dict[str, list[dict]] = {}
    seen_keys: set[str] = set()
    for node in graph.nodes:
        process_uuid = str(node.process_uuid or "")
        process_name = str(node.name or process_uuid or node.id)
        if not process_uuid:
            continue
        reference_port = next((port for port in node.outputs if port.type != "biosphere" and bool(port.isProduct)), None)
        for port in node.outputs:
            if port.type == "biosphere" or not bool(port.isProduct):
                continue
            product_port_id = str(port.id or "")
            product_flow_uuid = str(port.flowUuid or "")
            product_key = f"{process_uuid}::{product_port_id or product_flow_uuid}"
            if not product_flow_uuid or product_key in seen_keys:
                continue
            seen_keys.add(product_key)
            products_by_process.setdefault(process_uuid, []).append(
                {
                    "product_key": product_key,
                    "process_uuid": process_uuid,
                    "process_name": process_name,
                    "product_port_id": product_port_id,
                    "product_flow_uuid": product_flow_uuid,
                    "product_name": str(port.name or product_flow_uuid),
                    "is_reference_product": bool(reference_port is not None and str(reference_port.id or "") == product_port_id),
                    "unit": str(port.unit or ""),
                    "unit_group": str(port.unitGroup or ""),
                    "flow_uuid": product_flow_uuid,
                }
            )

    product_result_index: list[dict] = []
    product_unit_map: dict[str, dict] = {}
    process_positions: list[list[int]] = []
    for pid in process_index:
        process_uuid = str(pid)
        product_rows = products_by_process.get(process_uuid) or []
        positions_for_process: list[int] = []
        for item in product_rows:
            pos = len(product_result_index)
            positions_for_process.append(pos)
            product_result_index.append(
                {
                    "product_key": item["product_key"],
                    "process_uuid": process_uuid,
                    "process_name": item["process_name"],
                    "product_port_id": item["product_port_id"],
                    "product_flow_uuid": item["product_flow_uuid"],
                    "product_name": item["product_name"],
                    "is_reference_product": bool(item["is_reference_product"]),
                }
            )
            product_unit_map[item["product_key"]] = {
                "unit": item["unit"],
                "unit_group": item["unit_group"],
                "flow_uuid": item["flow_uuid"],
            }
        process_positions.append(positions_for_process)

    if not product_result_index:
        return [], {}, values

    product_values = values
    if isinstance(values, list) and values:
        if all(isinstance(row, list) for row in values):
            expanded_rows: list[list[object]] = []
            for row in values:
                expanded_row: list[object] = []
                for idx, positions in enumerate(process_positions):
                    source_value = row[idx] if idx < len(row) else None
                    for _ in positions:
                        expanded_row.append(source_value)
                expanded_rows.append(expanded_row)
            product_values = expanded_rows
        elif len(values) == len(process_index):
            expanded_values: list[object] = []
            for idx, positions in enumerate(process_positions):
                source_value = values[idx] if idx < len(values) else None
                for _ in positions:
                    expanded_values.append(source_value)
            product_values = expanded_values

    return product_result_index, product_unit_map, product_values

def _rescale_lci_values_to_inventory_units(
    *,
    values: object,
    process_index: object,
    process_unit_map: dict[str, dict],
    unit_factor_by_group_and_name: dict[tuple[str, str], float],
) -> object:
    if not isinstance(process_index, list) or not process_index:
        return values
    if not isinstance(values, list) or not values:
        return values

    scale_by_position: list[float] = []
    for pid in process_index:
        meta = process_unit_map.get(str(pid), {}) if isinstance(process_unit_map, dict) else {}
        unit_group = str(meta.get("reference_unit_group") or "")
        unit_name = str(meta.get("reference_unit") or "")
        factor = unit_factor_by_group_and_name.get((unit_group, unit_name))
        scale_by_position.append(float(factor) if factor is not None else 1.0)

    if all(isinstance(row, list) for row in values):
        scaled_rows: list[list] = []
        for row in values:
            scaled_row: list = []
            for idx, val in enumerate(row):
                if idx >= len(scale_by_position):
                    scaled_row.append(val)
                    continue
                try:
                    scaled_row.append(float(val) * scale_by_position[idx])
                except (TypeError, ValueError):
                    scaled_row.append(val)
            scaled_rows.append(scaled_row)
        return scaled_rows

    if len(values) == len(process_index):
        scaled: list = []
        for idx, val in enumerate(values):
            try:
                scaled.append(float(val) * scale_by_position[idx])
            except (TypeError, ValueError):
                scaled.append(val)
        return scaled

    return values

def _compile_and_persist_pts_for_node(
    *,
    db: Session,
    project_id: str,
    graph: HybridGraph,
    node_id: str,
    force_recompile: bool,
) -> PtsCompileArtifact:
    try:
        graph_hash = compute_pts_graph_hash(graph, node_id)
        definition = extract_pts_definition(
            graph=graph,
            pts_node_id=node_id,
            graph_hash=graph_hash,
        )
        definition = _apply_pts_resource_policy_override(
            db=db,
            project_id=project_id,
            definition=definition,
        )
    except ValueError as exc:
        _raise_pts_compile_value_error_http(exc, node_id)
    definition_row = upsert_pts_definition(
        db=db,
        project_id=project_id,
        definition=definition,
    )
    pts_node = next((node for node in graph.nodes if node.id == node_id and node.node_kind == "pts_module"), None)
    pts_uuid = str(pts_node.pts_uuid or pts_node.process_uuid or pts_node.id).strip() if pts_node is not None else ""
    ports_policy = _get_pts_resource_ports_policy(db=db, project_id=project_id, pts_uuid=pts_uuid) if pts_uuid else None

    cached_row = (
        db.query(PtsCompileArtifact)
        .filter(
            PtsCompileArtifact.project_id == project_id,
            PtsCompileArtifact.pts_node_id == node_id,
            PtsCompileArtifact.graph_hash == graph_hash,
        )
        .first()
    )
    if cached_row is not None and not force_recompile:
        cached_artifact = cached_row.artifact_json if isinstance(cached_row.artifact_json, dict) else {}
        cached_version = str(cached_artifact.get("compile_schema_version") or "")
        if cached_version == PTS_COMPILE_SCHEMA_VERSION:
            compile_row = cached_row
        else:
            try:
                compile_result = compile_pts(graph, node_id, ports_policy=ports_policy)
            except ValueError as exc:
                _raise_pts_compile_value_error_http(exc, node_id)
            compile_row, _ = upsert_pts_compile_artifact(
                db=db,
                project_id=project_id,
                pts_node_id=node_id,
                force_recompile=True,
                compile_result=compile_result,
            )
    else:
        try:
            compile_result = compile_pts(graph, node_id, ports_policy=ports_policy)
        except ValueError as exc:
            _raise_pts_compile_value_error_http(exc, node_id)
        compile_row, _ = upsert_pts_compile_artifact(
            db=db,
            project_id=project_id,
            pts_node_id=node_id,
            force_recompile=force_recompile,
            compile_result=compile_result,
        )

    return compile_row

def _compile_pts_on_save_if_needed(
    *,
    db: Session,
    project_id: str,
    graph: HybridGraph,
    compile_on_save: bool,
) -> dict:
    # Main-graph save no longer performs implicit PTS compile/publish.
    # PTS lifecycle is handled by explicit /api/pts/compile and /api/pts/{pts_uuid}/publish.
    if not compile_on_save:
        return {
            "pts_compile_count": 0,
            "pts_compiled_uuids": [],
            "pts_failed_count": 0,
            "pts_failed_items": [],
        }
    return {
        "pts_compile_count": 0,
        "pts_compiled_uuids": [],
        "pts_failed_count": 0,
        "pts_failed_items": [],
    }
