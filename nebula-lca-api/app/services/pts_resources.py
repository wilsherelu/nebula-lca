"""PTS resource management and API helpers.

Extracted from ``app.main`` for Stage 6C.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..schemas import (
    FlowPort,
    HybridEdge,
    HybridGraph,
    HybridNode,
    PtsBoundaryPortHint,
    PtsPackFinalizeRequest,
    PtsModelWarning,
    PtsPublishRequest,
    PtsResourceOut,
    PtsUnpackPortBinding,
    graph_exchange_type_to_flow_semantic,
    is_elementary_flow_semantic,
    normalize_same_flow_uuid_opposite_direction_ports,
)
from ..models import (
    PtsCompileArtifact,
    PtsDefinition,
    PtsExternalArtifact,
    PtsResource,
)
from ..pts_compile import compile_pts, compute_pts_graph_hash
from .graph_contract import _port_id_from_handle, is_graph_non_empty
from .project_versions import _get_flow_name_en_by_uuid_cached


def _flow_name_en_by_uuid_cached(db: Session) -> dict[str, str]:
    return _get_flow_name_en_by_uuid_cached(db)


def _safe_str(value: object) -> str:
    return "" if value is None else str(value)


def _strip_source_suffix_from_pts_port_name(port: dict) -> dict:
    if not isinstance(port, dict):
        return port
    source_name = str(port.get("sourceProcessName") or port.get("source_process_name") or "").strip()
    raw_name = str(port.get("name") or "").strip()
    if not source_name or "@" not in raw_name:
        return port
    suffix = raw_name.rsplit("@", 1)[1].strip()
    if suffix != source_name:
        return port
    normalized = dict(port)
    normalized.setdefault("display_name", raw_name)
    normalized["name"] = raw_name.rsplit("@", 1)[0].strip()
    return normalized


def _normalize_pts_shell_port_names(shell_node: dict) -> dict:
    if not isinstance(shell_node, dict):
        return shell_node
    updated = dict(shell_node)
    for bucket in ("inputs", "outputs", "emissions"):
        rows = updated.get(bucket)
        if isinstance(rows, list):
            updated[bucket] = [
                _strip_source_suffix_from_pts_port_name(row) if isinstance(row, dict) else row
                for row in rows
            ]
    return updated

def _get_pts_resource_ports_policy(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
) -> dict | None:
    row = (
        db.query(PtsResource)
        .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == pts_uuid)
        .first()
    )
    if row is None:
        return None
    return _sanitize_pts_ports_policy(
        ports_policy=dict(row.ports_policy_json or {}),
        pts_graph=dict(row.pts_graph_json or {}),
    )

def _sanitize_pts_ports_policy(*, ports_policy: dict | None, pts_graph: dict | None = None) -> dict:
    policy = dict(ports_policy or {})
    inputs = policy.get("inputs") if isinstance(policy.get("inputs"), list) else []
    outputs = policy.get("outputs") if isinstance(policy.get("outputs"), list) else []

    allowed_inputs: set[tuple[str, str, str, str]] | None = None
    allowed_outputs: set[tuple[str, str, str, str]] | None = None
    if isinstance(pts_graph, dict) and pts_graph:
        normalized = _normalize_pts_ports_policy_from_graph(pts_graph=pts_graph, fallback_policy=None)
        allowed_inputs = {
            (
                str(row.get("flow_uuid") or "").strip(),
                str(row.get("source_process_uuid") or "").strip(),
                str(row.get("source_node_id") or "").strip(),
                str(row.get("name") or "").strip(),
            )
            for row in (normalized.get("inputs") or [])
            if isinstance(row, dict)
        }
        allowed_outputs = {
            (
                str(row.get("flow_uuid") or "").strip(),
                str(row.get("source_process_uuid") or "").strip(),
                str(row.get("source_node_id") or "").strip(),
                str(row.get("name") or "").strip(),
            )
            for row in (normalized.get("outputs") or [])
            if isinstance(row, dict)
        }

    def _norm_key(row: dict) -> tuple[str, str, str, str]:
        return (
            str(row.get("flow_uuid") or row.get("flowUuid") or "").strip(),
            str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
            str(row.get("name") or "").strip(),
        )

    sanitized_inputs: list[dict] = []
    for row in inputs:
        if not isinstance(row, dict):
            continue
        norm = {
            "flow_uuid": str(row.get("flow_uuid") or row.get("flowUuid") or "").strip(),
            "name": str(row.get("name") or "").strip(),
            "source_process_uuid": str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            "source_process_name": str(row.get("source_process_name") or row.get("sourceProcessName") or "").strip(),
            "source_node_id": str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
            "internal_exposed": bool(row.get("internal_exposed") if "internal_exposed" in row else row.get("internalExposed")),
            "external_visible": bool(
                row.get("external_visible")
                if "external_visible" in row
                else row.get("externalVisible", row.get("showOnNode"))
            ),
        }
        if not norm["flow_uuid"]:
            continue
        if allowed_inputs is not None and _norm_key(norm) not in allowed_inputs:
            continue
        sanitized_inputs.append(norm)

    sanitized_outputs: list[dict] = []
    for row in outputs:
        if not isinstance(row, dict):
            continue
        norm = {
            "flow_uuid": str(row.get("flow_uuid") or row.get("flowUuid") or "").strip(),
            "name": str(row.get("name") or "").strip(),
            "source_process_uuid": str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            "source_process_name": str(row.get("source_process_name") or row.get("sourceProcessName") or "").strip(),
            "source_node_id": str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
            "internal_exposed": bool(row.get("internal_exposed") if "internal_exposed" in row else row.get("internalExposed")),
            "external_visible": bool(
                row.get("external_visible")
                if "external_visible" in row
                else row.get("externalVisible", row.get("showOnNode"))
            ),
            "is_product": bool(row.get("is_product") if "is_product" in row else row.get("isProduct")),
        }
        if not norm["flow_uuid"]:
            continue
        if allowed_outputs is not None and _norm_key(norm) not in allowed_outputs:
            continue
        sanitized_outputs.append(norm)

    return {"inputs": sanitized_inputs, "outputs": sanitized_outputs}

def _apply_pts_resource_policy_override(
    *,
    db: Session,
    project_id: str,
    definition: dict,
) -> dict:
    pts_uuid = str(definition.get("pts_uuid") or "").strip()
    if not pts_uuid:
        return definition
    resource_policy = _get_pts_resource_ports_policy(db=db, project_id=project_id, pts_uuid=pts_uuid)
    if resource_policy:
        definition = dict(definition)
        definition["ports_policy"] = resource_policy
    return definition


def _is_pts_port_exposed_like(port: object) -> bool:
    internal_exposed = getattr(port, "internalExposed", None)
    if internal_exposed is not None:
        return bool(internal_exposed)
    external_visible = getattr(port, "externalVisible", None)
    if external_visible is not None:
        return bool(external_visible)
    show_on_node = getattr(port, "showOnNode", None)
    if show_on_node is not None:
        return bool(show_on_node)
    return False


def _build_pts_shell_node_snapshot(pts_node: HybridNode) -> dict:
    return {
        "id": pts_node.id,
        "node_kind": pts_node.node_kind,
        "mode": pts_node.mode,
        "lci_role": pts_node.lci_role,
        "pts_uuid": pts_node.pts_uuid,
        "pts_published_version": pts_node.pts_published_version,
        "pts_published_artifact_id": pts_node.pts_published_artifact_id,
        "process_uuid": pts_node.process_uuid,
        "name": pts_node.name,
        "location": pts_node.location,
        "reference_product": pts_node.reference_product,
        "allocation_method": pts_node.allocation_method,
        "inputs": [port.model_dump(mode="python", by_alias=True) for port in pts_node.inputs],
        "outputs": [port.model_dump(mode="python", by_alias=True) for port in pts_node.outputs],
        "emissions": [port.model_dump(mode="python", by_alias=True) for port in pts_node.emissions],
    }

def _find_pts_internal_canvas(graph: HybridGraph, pts_node_id: str) -> dict | None:
    metadata = graph.metadata if isinstance(graph.metadata, dict) else {}
    canvases = metadata.get("canvases")
    if not isinstance(canvases, list):
        return None
    candidates: list[dict] = []
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

def _normalize_pts_ports_policy_from_graph(*, pts_graph: dict, fallback_policy: dict | None = None) -> dict:
    if not isinstance(pts_graph, dict):
        return dict(fallback_policy or {})
    nodes = pts_graph.get("nodes")
    exchanges = pts_graph.get("exchanges")
    if not isinstance(nodes, list) or not isinstance(exchanges, list):
        return dict(fallback_policy or {})

    graph = HybridGraph.model_validate(
        {
            "functionalUnit": str(pts_graph.get("functionalUnit") or "PTS"),
            "nodes": nodes,
            "exchanges": exchanges,
            "metadata": pts_graph.get("metadata") or {},
        }
    )

    def _policy_rows(policy: dict | None, key: str) -> list[dict]:
        if not isinstance(policy, dict):
            return []
        rows = policy.get(key)
        if rows is None or not isinstance(rows, list):
            return []
        normalized_rows: list[dict] = []
        for row in rows:
            if isinstance(row, dict):
                normalized_rows.append(dict(row))
        return normalized_rows

    def _policy_bool(row: dict | None, *keys: str, default: bool = False) -> bool:
        if not isinstance(row, dict):
            return default
        for key in keys:
            if key in row and row.get(key) is not None:
                return bool(row.get(key))
        return default

    def _policy_row_keys(row: dict) -> list[tuple[str, str, str, str]]:
        flow_uuid = str(row.get("flow_uuid") or row.get("flowUuid") or "").strip()
        source_process_uuid = str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip()
        source_node_id = str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip()
        name = str(row.get("name") or "").strip()
        keys: list[tuple[str, str, str, str]] = []
        if flow_uuid:
            keys.append((flow_uuid, source_process_uuid, source_node_id, name))
            keys.append((flow_uuid, source_process_uuid, "", name))
            keys.append((flow_uuid, "", source_node_id, name))
            keys.append((flow_uuid, "", "", name))
            keys.append((flow_uuid, source_process_uuid, source_node_id, ""))
            keys.append((flow_uuid, "", "", ""))
        return keys

    def _derived_row_key(row: dict) -> tuple[str, str, str, str]:
        return (
            str(row.get("flow_uuid") or "").strip(),
            str(row.get("source_process_uuid") or "").strip(),
            str(row.get("source_node_id") or "").strip(),
            str(row.get("name") or "").strip(),
        )

    fallback_inputs = _policy_rows(fallback_policy or {}, "inputs")
    fallback_outputs = _policy_rows(fallback_policy or {}, "outputs")

    fallback_input_map: dict[tuple[str, str, str, str], dict] = {}
    fallback_output_map: dict[tuple[str, str, str, str], dict] = {}
    for row in fallback_inputs:
        for key in _policy_row_keys(row):
            fallback_input_map.setdefault(key, row)
    for row in fallback_outputs:
        for key in _policy_row_keys(row):
            fallback_output_map.setdefault(key, row)

    inbound_pairs: set[tuple[str, str]] = set()
    outbound_pairs: set[tuple[str, str]] = set()
    for edge in graph.exchanges:
        to_node = str(edge.toNode or "").strip()
        from_node = str(edge.fromNode or "").strip()
        flow_uuid = str(edge.flowUuid or "").strip()
        if not to_node or not flow_uuid:
            if not from_node or not flow_uuid:
                continue
        target_port_id = _port_id_from_handle(
            str(getattr(edge, "target_port_id", "") or getattr(edge, "targetHandle", "") or ""),
            "in",
        )
        source_port_id = _port_id_from_handle(
            str(getattr(edge, "source_port_id", "") or getattr(edge, "sourceHandle", "") or ""),
            "out",
        )
        if to_node and flow_uuid:
            inbound_pairs.add((to_node, flow_uuid))
            if target_port_id:
                inbound_pairs.add((f"{to_node}::{target_port_id}", flow_uuid))
        if from_node and flow_uuid:
            outbound_pairs.add((from_node, flow_uuid))
            if source_port_id:
                outbound_pairs.add((f"{from_node}::{source_port_id}", flow_uuid))

    inputs: list[dict] = []
    outputs: list[dict] = []

    for node in graph.nodes:
        node_id = str(node.id or "")
        process_uuid = str(node.process_uuid or "")
        process_name = str(node.name or "")
        for port in node.inputs:
            flow_uuid = str(port.flowUuid or "")
            if not flow_uuid or port.type == "biosphere":
                continue
            specific_key = (f"{node_id}::{str(port.id or '')}", flow_uuid)
            broad_key = (node_id, flow_uuid)
            if specific_key in inbound_pairs or broad_key in inbound_pairs:
                continue
            derived = {
                "flow_uuid": flow_uuid,
                "name": str(port.name or ""),
                "source_process_uuid": process_uuid,
                "source_process_name": process_name,
                "source_node_id": node_id,
            }
            fallback_row = fallback_input_map.get(_derived_row_key(derived))
            derived["internal_exposed"] = _policy_bool(
                fallback_row,
                "internal_exposed",
                "internalExposed",
                default=_is_pts_port_exposed_like(port),
            )
            derived["external_visible"] = _policy_bool(
                fallback_row,
                "external_visible",
                "externalVisible",
                "showOnNode",
                default=bool(getattr(port, "showOnNode", False)),
            )
            inputs.append(derived)
        for port in node.outputs:
            flow_uuid = str(port.flowUuid or "")
            if not flow_uuid or port.type == "biosphere":
                continue
            specific_key = (f"{node_id}::{str(port.id or '')}", flow_uuid)
            broad_key = (node_id, flow_uuid)
            is_product = bool(port.isProduct)
            if not is_product and (specific_key in outbound_pairs or broad_key in outbound_pairs):
                continue
            derived = {
                "flow_uuid": flow_uuid,
                "name": str(port.name or ""),
                "source_process_uuid": process_uuid,
                "source_process_name": process_name,
                "source_node_id": node_id,
                "is_product": is_product,
            }
            fallback_row = fallback_output_map.get(_derived_row_key(derived))
            derived["internal_exposed"] = _policy_bool(
                fallback_row,
                "internal_exposed",
                "internalExposed",
                default=_is_pts_port_exposed_like(port),
            )
            derived["external_visible"] = _policy_bool(
                fallback_row,
                "external_visible",
                "externalVisible",
                "showOnNode",
                default=bool(getattr(port, "showOnNode", False)),
            )
            if fallback_row and "is_product" in fallback_row and fallback_row.get("is_product") is not None:
                derived["is_product"] = bool(fallback_row.get("is_product"))
            outputs.append(derived)

    if inputs or outputs:
        return {"inputs": inputs, "outputs": outputs}
    return dict(fallback_policy or {})

def _upsert_pts_resource_from_graph(*, db: Session, project_id: str, graph: HybridGraph, pts_node: HybridNode) -> PtsResource | None:
    internal_canvas = _find_pts_internal_canvas(graph, pts_node.id)
    if internal_canvas is None:
        return None
    pts_uuid = str(pts_node.pts_uuid or pts_node.process_uuid or pts_node.id).strip()
    pts_graph = {
        "functionalUnit": graph.functionalUnit,
        "nodes": internal_canvas.get("nodes") if isinstance(internal_canvas.get("nodes"), list) else [],
        "exchanges": internal_canvas.get("edges") if isinstance(internal_canvas.get("edges"), list) else [],
        "metadata": {
            "kind": "pts_internal",
            "canvas_id": str(internal_canvas.get("id") or ""),
            "parentPtsNodeId": str(internal_canvas.get("parentPtsNodeId") or pts_node.id),
            "name": str(internal_canvas.get("name") or ""),
        },
    }
    normalized_ports_policy = _normalize_pts_ports_policy_from_graph(
        pts_graph=pts_graph,
        fallback_policy=None,
    )
    resource = (
        db.query(PtsResource)
        .filter(PtsResource.pts_uuid == pts_uuid)
        .first()
    )
    if resource is None:
        resource = PtsResource(
            project_id=project_id,
            pts_uuid=pts_uuid,
            name=str(pts_node.name or ""),
            pts_node_id=str(pts_node.id or ""),
            latest_graph_hash=str(compute_pts_graph_hash(graph, pts_node.id)),
            pts_graph_json=pts_graph,
            ports_policy_json=normalized_ports_policy,
            shell_node_json=_build_pts_shell_node_snapshot(pts_node),
        )
        db.add(resource)
    else:
        resource.project_id = project_id
        resource.name = str(pts_node.name or "")
        resource.pts_node_id = str(pts_node.id or "")
        resource.latest_graph_hash = str(compute_pts_graph_hash(graph, pts_node.id))
        resource.pts_graph_json = pts_graph
        resource.ports_policy_json = normalized_ports_policy
        resource.shell_node_json = _build_pts_shell_node_snapshot(pts_node)
    latest_external = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .first()
    )
    latest_compile = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    if latest_compile is not None:
        resource.compiled_graph_hash = str(latest_compile.graph_hash or "")
        resource.latest_compile_version = int(latest_compile.compile_version or 0) or resource.latest_compile_version
    if latest_external is not None:
        resource.latest_published_version = int(latest_external.published_version or 0) or resource.latest_published_version
        if resource.active_published_version is None:
            resource.active_published_version = resource.latest_published_version
        resource.published_at = latest_external.updated_at or latest_external.created_at
    return resource

def _get_or_materialize_pts_resource_row(*, db: Session, pts_uuid: str) -> PtsResource:
    row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    if row is not None:
        return row
    definition_row = db.query(PtsDefinition).filter(PtsDefinition.pts_uuid == pts_uuid).first()
    if definition_row is None:
        raise HTTPException(status_code=404, detail="PTS resource not found")
    latest_compile = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == definition_row.project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    latest_external = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == definition_row.project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.published_version.desc(), PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .first()
    )
    row = _upsert_pts_resource_from_definition(
        db=db,
        definition_row=definition_row,
        compile_row=latest_compile,
        external_row=latest_external,
    )
    db.commit()
    db.refresh(row)
    return row


def _load_pts_external_artifact(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
    published_version: int | None = None,
) -> PtsExternalArtifact | None:
    query = db.query(PtsExternalArtifact).filter(
        PtsExternalArtifact.project_id == project_id,
        PtsExternalArtifact.pts_uuid == pts_uuid,
    )
    if published_version is not None:
        query = query.filter(PtsExternalArtifact.published_version == published_version)
    return query.order_by(
        PtsExternalArtifact.published_version.desc(),
        PtsExternalArtifact.updated_at.desc(),
        PtsExternalArtifact.created_at.desc(),
    ).first()


def _load_pts_active_external_artifact(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
) -> PtsExternalArtifact | None:
    resource = (
        db.query(PtsResource)
        .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == pts_uuid)
        .first()
    )
    active_version = int(resource.active_published_version) if resource and resource.active_published_version is not None else None
    return _load_pts_external_artifact(
        db=db,
        project_id=project_id,
        pts_uuid=pts_uuid,
        published_version=active_version,
    )


def _build_pts_resource_out(*, row: PtsResource, db: Session) -> PtsResourceOut:
    pts_graph = dict(row.pts_graph_json or {})
    if pts_graph:
        _enrich_graph_flow_name_en(pts_graph, db=db)
    ports_policy = _sanitize_pts_ports_policy(
        ports_policy=dict(row.ports_policy_json or {}),
        pts_graph=pts_graph,
    )
    shell_node = dict(row.shell_node_json or {})
    if shell_node:
        shell_node = _normalize_pts_shell_port_names(shell_node)
        _enrich_node_ports_flow_name_en(shell_node, db=db)
    return PtsResourceOut(
        project_id=str(row.project_id),
        pts_uuid=str(row.pts_uuid),
        name=str(row.name or "") or None,
        pts_node_id=str(row.pts_node_id or "") or None,
        latest_graph_hash=str(row.latest_graph_hash or "") or None,
        compiled_graph_hash=str(row.compiled_graph_hash or "") or None,
        latest_compile_version=(int(row.latest_compile_version) if row.latest_compile_version is not None else None),
        latest_published_version=(int(row.latest_published_version) if row.latest_published_version is not None else None),
        active_published_version=(int(row.active_published_version) if row.active_published_version is not None else None),
        published_at=row.published_at,
        ports_policy=ports_policy,
        shell_node=shell_node,
        pts_graph=pts_graph,
    )

def _build_pts_shell_snapshot_from_external(*, row: PtsResource, external: PtsExternalArtifact) -> dict:
    shell_node = _normalize_pts_shell_port_names(dict(row.shell_node_json or {}))
    projected_inputs, projected_outputs = _build_projected_pts_ports_from_external(external)
    return {
        "id": str(shell_node.get("id") or row.pts_node_id or external.pts_node_id or ""),
        "node_kind": _normalize_pts_shell_node_kind(shell_node.get("node_kind")),
        "mode": str(shell_node.get("mode") or "normalized"),
        "lci_role": shell_node.get("lci_role"),
        "pts_uuid": str(shell_node.get("pts_uuid") or row.pts_uuid or external.pts_uuid),
        "pts_published_version": (int(external.published_version) if external.published_version is not None else None),
        "pts_published_artifact_id": str(external.id),
        "process_uuid": str(shell_node.get("process_uuid") or row.pts_uuid or external.pts_uuid),
        "name": str(shell_node.get("name") or row.name or "PTS"),
        "location": str(shell_node.get("location") or "Plant Internal"),
        "reference_product": str(shell_node.get("reference_product") or "PTS模块输出"),
        "allocation_method": shell_node.get("allocation_method"),
        "inputs": [port.model_dump(mode="python", by_alias=True) for port in projected_inputs],
        "outputs": [port.model_dump(mode="python", by_alias=True) for port in projected_outputs],
        "emissions": [],
    }

def _resolve_pts_shell_snapshot_for_resource(*, db: Session, row: PtsResource) -> dict:
    project_id = str(row.project_id or "").strip()
    pts_uuid = str(row.pts_uuid or "").strip()
    if project_id and pts_uuid:
        external = _load_pts_active_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
        )
        if external is not None:
            return _build_pts_shell_snapshot_from_external(row=row, external=external)
    return dict(row.shell_node_json or {})

def _build_pts_unpack_port_bindings(*, pts_graph: dict, ports_policy: dict, shell_node: dict) -> list[PtsUnpackPortBinding]:
    if not isinstance(pts_graph, dict) or not pts_graph:
        return []
    try:
        graph = HybridGraph.model_validate(
            {
                "functionalUnit": str(pts_graph.get("functionalUnit") or "PTS"),
                "nodes": pts_graph.get("nodes") or [],
                "exchanges": pts_graph.get("exchanges") or [],
                "metadata": pts_graph.get("metadata") or {},
            }
        )
    except Exception:
        return []

    input_ports_by_node_flow: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    output_ports_by_node_flow: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    product_output_ports_by_node_flow: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    for node in graph.nodes:
        node_id = str(node.id or "")
        for port in node.inputs:
            flow_uuid = str(port.flowUuid or "")
            if not node_id or not flow_uuid:
                continue
            input_ports_by_node_flow[(node_id, flow_uuid)].append(port)
        for port in node.outputs:
            flow_uuid = str(port.flowUuid or "")
            if not node_id or not flow_uuid:
                continue
            output_ports_by_node_flow[(node_id, flow_uuid)].append(port)
            if bool(port.isProduct):
                product_output_ports_by_node_flow[(node_id, flow_uuid)].append(port)

    def _unique_ports(ports: list[FlowPort]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for port in ports:
            port_id = str(port.id or "").strip()
            if not port_id or port_id in seen:
                continue
            seen.add(port_id)
            result.append(port_id)
        return result

    def _match_input_candidates(shell_port: dict) -> list[str]:
        flow_uuid = str(shell_port.get("flowUuid") or "").strip()
        source_node_id = str(shell_port.get("sourceNodeId") or "").strip()
        candidates: list[FlowPort] = []
        if source_node_id:
            candidates.extend(input_ports_by_node_flow.get((source_node_id, flow_uuid), []))
        else:
            for row in (ports_policy.get("inputs") or []):
                if not isinstance(row, dict):
                    continue
                if str(row.get("flow_uuid") or row.get("flowUuid") or "").strip() != flow_uuid:
                    continue
                row_node_id = str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip()
                if not row_node_id:
                    continue
                candidates.extend(input_ports_by_node_flow.get((row_node_id, flow_uuid), []))
        return _unique_ports(candidates)

    def _match_output_candidates(shell_port: dict) -> list[str]:
        flow_uuid = str(shell_port.get("flowUuid") or "").strip()
        source_node_id = str(shell_port.get("sourceNodeId") or "").strip()
        prefer_product = bool(shell_port.get("isProduct"))
        candidates: list[FlowPort] = []
        if source_node_id:
            indexed = product_output_ports_by_node_flow if prefer_product else output_ports_by_node_flow
            candidates.extend(indexed.get((source_node_id, flow_uuid), []))
            if not candidates and prefer_product:
                candidates.extend(output_ports_by_node_flow.get((source_node_id, flow_uuid), []))
        else:
            for row in (ports_policy.get("outputs") or []):
                if not isinstance(row, dict):
                    continue
                if str(row.get("flow_uuid") or row.get("flowUuid") or "").strip() != flow_uuid:
                    continue
                row_node_id = str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip()
                if not row_node_id:
                    continue
                indexed = product_output_ports_by_node_flow if prefer_product else output_ports_by_node_flow
                matches = indexed.get((row_node_id, flow_uuid), [])
                if not matches and prefer_product:
                    matches = output_ports_by_node_flow.get((row_node_id, flow_uuid), [])
                candidates.extend(matches)
        return _unique_ports(candidates)

    bindings: list[PtsUnpackPortBinding] = []
    for direction, shell_ports in (
        ("input", list(shell_node.get("inputs") or [])),
        ("output", list(shell_node.get("outputs") or [])),
    ):
        for shell_port in shell_ports:
            if not isinstance(shell_port, dict):
                continue
            shell_port_id = str(shell_port.get("id") or shell_port.get("port_key") or shell_port.get("product_key") or "").strip()
            flow_uuid = str(shell_port.get("flowUuid") or "").strip()
            if not shell_port_id or not flow_uuid:
                continue
            internal_port_ids = _match_input_candidates(shell_port) if direction == "input" else _match_output_candidates(shell_port)
            bindings.append(
                PtsUnpackPortBinding(
                    shell_port_id=shell_port_id,
                    flow_uuid=flow_uuid,
                    direction=direction,
                    source_node_id=str(shell_port.get("sourceNodeId") or "").strip() or None,
                    source_process_uuid=str(shell_port.get("sourceProcessUuid") or "").strip() or None,
                    internal_port_id=(internal_port_ids[0] if len(internal_port_ids) == 1 else None),
                    internal_port_ids=internal_port_ids,
                )
            )
    return bindings

def _pts_ports_policy_has_rows(policy: dict | None) -> bool:
    if not isinstance(policy, dict):
        return False
    inputs = policy.get("inputs")
    outputs = policy.get("outputs")
    return bool((isinstance(inputs, list) and len(inputs) > 0) or (isinstance(outputs, list) and len(outputs) > 0))

def _count_pts_policy_rows(policy: dict | None) -> tuple[int, int]:
    if not isinstance(policy, dict):
        return 0, 0
    inputs = policy.get("inputs")
    outputs = policy.get("outputs")
    return (
        len(inputs) if isinstance(inputs, list) else 0,
        len(outputs) if isinstance(outputs, list) else 0,
    )


def _count_shell_ports(shell_node: dict | None) -> tuple[int, int]:
    if not isinstance(shell_node, dict):
        return 0, 0
    inputs = shell_node.get("inputs")
    outputs = shell_node.get("outputs")
    return (
        len(inputs) if isinstance(inputs, list) else 0,
        len(outputs) if isinstance(outputs, list) else 0,
    )


def _raise_if_pack_finalize_obviously_reentered(
    *,
    pts_uuid: str,
    payload: PtsPackFinalizeRequest,
    derived_ports_policy: dict,
    existing_row: PtsResource | None,
) -> None:
    if existing_row is None:
        return
    existing_published_version = (
        int(existing_row.active_published_version)
        if existing_row.active_published_version is not None
        else (
            int(existing_row.latest_published_version)
            if existing_row.latest_published_version is not None
            else None
        )
    )
    if existing_published_version is None:
        return
    existing_shell = dict(existing_row.shell_node_json or {})
    existing_shell_inputs, existing_shell_outputs = _count_shell_ports(existing_shell)
    if existing_shell_inputs == 0 and existing_shell_outputs == 0:
        return

    incoming_policy_inputs, incoming_policy_outputs = _count_pts_policy_rows(derived_ports_policy)
    same_graph_hash = bool(
        str(payload.latest_graph_hash or "").strip()
        and str(existing_row.latest_graph_hash or "").strip()
        and str(payload.latest_graph_hash or "").strip() == str(existing_row.latest_graph_hash or "").strip()
    )
    if not same_graph_hash:
        return

    if incoming_policy_inputs <= existing_shell_inputs and incoming_policy_outputs <= existing_shell_outputs:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "PTS_PACK_FINALIZE_REENTRY_FORBIDDEN",
                "message": (
                    f"pack-finalize for pts_uuid={pts_uuid} looks like a second pass built from the current "
                    "published shell instead of the original wrap source. Do not re-run pack-finalize for an "
                    "already packed node on ordinary root refresh/click flows."
                ),
                "evidence": [
                    {
                        "pts_uuid": pts_uuid,
                        "latest_graph_hash": str(existing_row.latest_graph_hash or "") or None,
                        "existing_published_version": existing_published_version,
                        "existing_shell_input_count": existing_shell_inputs,
                        "existing_shell_output_count": existing_shell_outputs,
                        "incoming_policy_input_count": incoming_policy_inputs,
                        "incoming_policy_output_count": incoming_policy_outputs,
                    }
                ],
            },
        )


def _upsert_pts_resource_from_definition(
    *,
    db: Session,
    definition_row: PtsDefinition,
    compile_row: PtsCompileArtifact | None = None,
    external_row: PtsExternalArtifact | None = None,
) -> PtsResource:
    row = db.query(PtsResource).filter(PtsResource.pts_uuid == definition_row.pts_uuid).first()
    definition = dict(definition_row.definition_json or {})
    pts_graph = dict(definition.get("pts_graph") or {})
    ports_policy = _sanitize_pts_ports_policy(
        ports_policy=dict(definition_row.ports_policy_json or {}),
        pts_graph=pts_graph,
    )
    shell_node = dict(definition.get("shell_node") or {})
    if row is None:
        row = PtsResource(
            project_id=str(definition_row.project_id),
            pts_uuid=str(definition_row.pts_uuid),
            name=str(definition.get("name") or shell_node.get("name") or "") or None,
            pts_node_id=str(definition_row.pts_node_id or "") or None,
            latest_graph_hash=str(definition_row.latest_graph_hash or "") or None,
            pts_graph_json=pts_graph,
            ports_policy_json=ports_policy,
            shell_node_json=shell_node,
        )
        db.add(row)
    else:
        row.project_id = str(definition_row.project_id)
        row.name = str(definition.get("name") or shell_node.get("name") or row.name or "") or None
        row.pts_node_id = str(definition_row.pts_node_id or "") or None
        row.latest_graph_hash = str(definition_row.latest_graph_hash or "") or None
        row.pts_graph_json = pts_graph
        row.ports_policy_json = ports_policy
        row.shell_node_json = shell_node

    if compile_row is not None:
        row.compiled_graph_hash = str(compile_row.graph_hash or "") or None
        row.latest_compile_version = int(compile_row.compile_version or 0) or row.latest_compile_version
    if external_row is not None:
        row.latest_published_version = int(external_row.published_version or 0) or row.latest_published_version
        row.published_at = external_row.updated_at or external_row.created_at
        if row.active_published_version is None:
            row.active_published_version = row.latest_published_version
    return row

def _build_compile_graph_from_pts_resource(row: PtsResource) -> HybridGraph | None:
    pts_graph = dict(row.pts_graph_json or {})
    shell_node = dict(row.shell_node_json or {})
    if not pts_graph or not shell_node:
        return None
    shell_payload = {
        "id": str(shell_node.get("id") or row.pts_node_id or ""),
        "node_kind": _normalize_pts_shell_node_kind(shell_node.get("node_kind")),
        "mode": str(shell_node.get("mode") or "normalized"),
        "lci_role": shell_node.get("lci_role"),
        "pts_uuid": str(shell_node.get("pts_uuid") or row.pts_uuid),
        "process_uuid": str(shell_node.get("process_uuid") or row.pts_uuid),
        "name": str(shell_node.get("name") or row.name or "PTS"),
        "location": str(shell_node.get("location") or "Plant Internal"),
        "reference_product": str(shell_node.get("reference_product") or "PTS模块输出"),
        "allocation_method": shell_node.get("allocation_method"),
        "inputs": shell_node.get("inputs") if isinstance(shell_node.get("inputs"), list) else [],
        "outputs": shell_node.get("outputs") if isinstance(shell_node.get("outputs"), list) else [],
        "emissions": shell_node.get("emissions") if isinstance(shell_node.get("emissions"), list) else [],
    }
    internal_nodes = pts_graph.get("nodes") if isinstance(pts_graph.get("nodes"), list) else []
    internal_edges = pts_graph.get("exchanges") if isinstance(pts_graph.get("exchanges"), list) else []
    canvas_name = str((pts_graph.get("metadata") or {}).get("name") or shell_node.get("name") or "PTS")
    graph_payload = {
        "functionalUnit": str(pts_graph.get("functionalUnit") or "PTS"),
        "nodes": [shell_payload],
        "exchanges": [],
        "metadata": {
            "canvases": [
                {
                    "id": str((pts_graph.get("metadata") or {}).get("canvas_id") or f"canvas::{row.pts_uuid}"),
                    "kind": "pts_internal",
                    "parentPtsNodeId": str((pts_graph.get("metadata") or {}).get("parentPtsNodeId") or row.pts_node_id or shell_node.get("id") or ""),
                    "name": canvas_name,
                    "nodes": internal_nodes,
                    "edges": internal_edges,
                }
            ]
        },
    }
    try:
        return HybridGraph.model_validate(graph_payload)
    except Exception:
        return None

def _resolve_compile_row_for_publish(*, db: Session, pts_uuid: str, payload: PtsPublishRequest) -> PtsCompileArtifact:
    query = db.query(PtsCompileArtifact).filter(
        PtsCompileArtifact.project_id == payload.project_id,
        PtsCompileArtifact.pts_uuid == pts_uuid,
    )
    if payload.compile_id:
        row = query.filter(PtsCompileArtifact.id == payload.compile_id).first()
    elif payload.compile_version is not None:
        row = query.filter(PtsCompileArtifact.compile_version == payload.compile_version).first()
    elif payload.graph_hash:
        row = query.filter(PtsCompileArtifact.graph_hash == payload.graph_hash).first()
    else:
        row = query.order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc()).first()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "PTS_COMPILE_ARTIFACT_NOT_FOUND",
                "message": f"No compile artifact found for pts_uuid={pts_uuid}",
            },
        )
    return row

def _bind_pts_published_versions_for_graph(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        external = _load_pts_active_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
        )
        if external is None:
            node.pts_published_version = None
            node.pts_published_artifact_id = None
            continue
        node.pts_published_version = int(external.published_version) if external.published_version is not None else None
        node.pts_published_artifact_id = str(external.id)


def _is_unpublished_empty_pts_draft(*, graph: HybridGraph, pts_node: HybridNode) -> bool:
    internal_canvas = _find_pts_internal_canvas(graph, pts_node.id)
    if internal_canvas is None:
        return False
    internal_nodes = internal_canvas.get("nodes") if isinstance(internal_canvas.get("nodes"), list) else []
    internal_edges = internal_canvas.get("edges") if isinstance(internal_canvas.get("edges"), list) else []
    return len(internal_nodes) == 0 and len(internal_edges) == 0


def _load_published_compile_rows_for_graph(
    *,
    db: Session,
    project_id: str,
    graph: HybridGraph,
    graph_hash: str | None = None,
) -> list[PtsCompileArtifact]:
    rows: list[PtsCompileArtifact] = []
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id)
        expected_pts_graph_hash = compute_pts_graph_hash(graph, node.id)
        if graph_hash and graph_hash != expected_pts_graph_hash:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PTS_PUBLISHED_ARTIFACT_HASH_MISMATCH",
                    "message": (
                        f"Provided artifact_graph_hash does not match current PTS graph hash for pts_uuid={pts_uuid}. "
                        "Please republish current graph or omit artifact_graph_hash."
                    ),
                    "evidence": [
                        {
                            "project_id": project_id,
                            "pts_uuid": pts_uuid,
                            "provided_graph_hash": graph_hash,
                            "expected_pts_graph_hash": expected_pts_graph_hash,
                        }
                    ],
                },
            )
        external = _load_pts_active_external_artifact(
            db=db,
            project_id=project_id,
            pts_uuid=pts_uuid,
        )
        effective_hash = graph_hash or expected_pts_graph_hash
        if external is None:
            if _is_unpublished_empty_pts_draft(graph=graph, pts_node=node):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "PTS_DRAFT_EMPTY_NOT_RUNNABLE",
                        "message": (
                            f"PTS {pts_uuid} is still an empty draft. Open the PTS, add internal nodes/edges, publish it, "
                            "then run the main graph."
                        ),
                        "evidence": [
                            {
                                "project_id": project_id,
                                "pts_uuid": pts_uuid,
                                "pts_node_id": node.id,
                                "draft_state": "empty_unpublished_pts",
                                "expected_pts_graph_hash": expected_pts_graph_hash,
                            }
                        ],
                    },
                )
            latest = (
                db.query(PtsExternalArtifact)
                .filter(
                    PtsExternalArtifact.project_id == project_id,
                    PtsExternalArtifact.pts_uuid == pts_uuid,
                )
                .order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
                .first()
            )
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "PTS_PUBLISHED_ARTIFACT_NOT_FOUND",
                    "message": (
                        f"Published external artifact not found for pts_uuid={pts_uuid} and graph_hash={graph_hash}. "
                        "Please publish current graph first."
                        if effective_hash
                        else f"Published external artifact not found for pts_uuid={pts_uuid}"
                    ),
                    "evidence": [
                        {
                            "project_id": project_id,
                            "pts_uuid": pts_uuid,
                            "graph_hash": effective_hash,
                            "expected_pts_graph_hash": expected_pts_graph_hash,
                            "provided_graph_hash": graph_hash,
                            "latest_published_graph_hash": (latest.graph_hash if latest is not None else None),
                        }
                    ],
                },
            )
        payload = external.artifact_json if isinstance(external.artifact_json, dict) else {}
        rows.append(
            PtsCompileArtifact(
                project_id=project_id,
                pts_node_id=str(payload.get("pts_node_id") or node.id),
                pts_uuid=pts_uuid,
                graph_hash=str(payload.get("graph_hash") or external.graph_hash),
                compile_version=(
                    int(external.source_compile_version)
                    if external.source_compile_version is not None else None
                ),
                ok=bool(payload.get("ok", True)),
                matrix_size=int(payload.get("matrix_size") or 0),
                invertible=bool(payload.get("invertible", True)),
                errors_json=list(payload.get("errors") or []),
                warnings_json=list(payload.get("warnings") or []),
                artifact_json=dict(payload),
            )
        )
    return rows


def build_flattened_graph_for_run_pts(*, graph: HybridGraph, compile_rows: list[PtsCompileArtifact]) -> HybridGraph:
    """Replace published PTS shells with their virtual unit processes for run."""
    if not compile_rows:
        return graph

    node_by_id = {node.id: node for node in graph.nodes}
    pts_node_ids = {str(row.pts_node_id) for row in compile_rows}
    new_nodes = [node for node in graph.nodes if node.id not in pts_node_ids]
    new_edges = [edge for edge in graph.exchanges if edge.fromNode not in pts_node_ids and edge.toNode not in pts_node_ids]

    virtuals_by_pts: dict[str, list[dict]] = {}
    virtual_node_by_key: dict[tuple[str, str], HybridNode] = {}

    def _ref(vp: dict) -> dict:
        return vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else {}

    def _as_port(item: dict, *, idx: int, fallback_direction: str, fallback_type: str) -> dict:
        return {
            "id": str(item.get("id") or f"{fallback_type}_{idx}"),
            "flowUuid": str(item.get("flowUuid") or ""),
            "name": str(item.get("name") or ""),
            "unit": str(item.get("unit") or ""),
            "unitGroup": item.get("unitGroup"),
            "amount": float(item.get("amount") or 0.0),
            "externalSaleAmount": float(item.get("externalSaleAmount") or 0.0),
            "type": str(item.get("type") or fallback_type),
            "direction": str(item.get("direction") or fallback_direction),
            "showOnNode": bool(item.get("showOnNode", True)),
            "internalExposed": item.get("internalExposed"),
            "dbMapping": item.get("dbMapping"),
            "isProduct": bool(item.get("isProduct", False)),
            "sourceProcessUuid": item.get("sourceProcessUuid") or item.get("source_process_uuid"),
            "sourceNodeId": item.get("sourceNodeId") or item.get("source_node_id"),
        }

    def _vp_inputs(vp: dict) -> list[dict]:
        raw = vp.get("technosphere_inputs")
        if not isinstance(raw, list):
            return []
        return [
            _as_port(item, idx=idx, fallback_direction="input", fallback_type="technosphere")
            for idx, item in enumerate(raw, start=1)
            if isinstance(item, dict)
        ]

    def _vp_emissions(vp: dict) -> list[dict]:
        raw = vp.get("elementary_flows")
        if not isinstance(raw, list):
            return []
        return [
            _as_port(item, idx=idx, fallback_direction="output", fallback_type="biosphere")
            for idx, item in enumerate(raw, start=1)
            if isinstance(item, dict)
        ]

    for row in compile_rows:
        pts_node = node_by_id.get(str(row.pts_node_id))
        artifact = row.artifact_json if isinstance(row.artifact_json, dict) else {}
        virtuals = artifact.get("virtual_processes") if isinstance(artifact, dict) else []
        if pts_node is None or not isinstance(virtuals, list):
            continue
        virtuals_by_pts[str(row.pts_node_id)] = [vp for vp in virtuals if isinstance(vp, dict)]
        for idx, vp in enumerate(virtuals_by_pts[str(row.pts_node_id)], start=1):
            ref = _ref(vp)
            ref_uuid = str(ref.get("flowUuid") or vp.get("reference_product_flow_uuid") or "")
            ref_name = str(ref.get("name") or vp.get("product_name") or f"{pts_node.name}-product-{idx}")
            ref_unit = str(ref.get("unit") or vp.get("reference_unit") or "kg")
            ref_unit_group = str(ref.get("unitGroup") or vp.get("reference_unit_group") or "").strip()
            process_uuid = str(vp.get("process_uuid") or f"{pts_node.process_uuid}::vp::{idx}")
            virtual_node = HybridNode.model_validate(
                {
                    "id": f"{pts_node.id}::vp::{idx}",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": process_uuid,
                    "name": str(vp.get("process_name") or f"{pts_node.name}::{idx}"),
                    "location": pts_node.location,
                    "reference_product": ref_name,
                    "inputs": _vp_inputs(vp),
                    "outputs": [
                        {
                            "id": f"out_{idx}",
                            "flowUuid": ref_uuid,
                            "name": ref_name,
                            "unit": ref_unit,
                            "unitGroup": ref_unit_group or None,
                            "amount": 1.0,
                            "isProduct": True,
                            "type": "technosphere",
                            "direction": "output",
                            "showOnNode": True,
                        }
                    ],
                    "emissions": _vp_emissions(vp),
                }
            )
            new_nodes.append(virtual_node)
            virtual_node_by_key[(str(row.pts_node_id), process_uuid)] = virtual_node

    def _find_output_vp(pts_node_id: str, flow_uuid: str, source_port_id: str | None = None) -> HybridNode | None:
        candidates: list[tuple[dict, HybridNode]] = []
        for vp in virtuals_by_pts.get(pts_node_id, []):
            ref_uuid = str(_ref(vp).get("flowUuid") or vp.get("reference_product_flow_uuid") or "")
            if ref_uuid == flow_uuid:
                node = virtual_node_by_key.get((pts_node_id, str(vp.get("process_uuid") or "")))
                if node is not None:
                    candidates.append((vp, node))
        if not candidates:
            return None
        if source_port_id:
            pts_node = node_by_id.get(pts_node_id)
            shell_port = None
            if pts_node is not None:
                for port in pts_node.outputs:
                    port_ids = {
                        str(port.id or "").strip(),
                        str(port.legacy_port_id or "").strip(),
                        str(port.port_key or "").strip(),
                        str(port.product_key or "").strip(),
                    }
                    if source_port_id in port_ids:
                        shell_port = port
                        break
            if shell_port is not None:
                shell_product_key = str(shell_port.product_key or shell_port.port_key or "").strip()
                shell_source_process_uuid = str(shell_port.source_process_uuid or "").strip()
                shell_source_node_id = str(shell_port.source_node_id or "").strip()
                exact_by_key = [
                    node
                    for vp, node in candidates
                    if shell_product_key and str(vp.get("product_key") or vp.get("virtual_process_key") or "").strip() == shell_product_key
                ]
                if len(exact_by_key) == 1:
                    return exact_by_key[0]
                exact_by_source = [
                    node
                    for vp, node in candidates
                    if (
                        shell_source_process_uuid
                        and str(vp.get("source_process_uuid") or vp.get("sourceProcessUuid") or "").strip() == shell_source_process_uuid
                    )
                    or (
                        shell_source_node_id
                        and str(vp.get("source_node_id") or vp.get("sourceNodeId") or "").strip() == shell_source_node_id
                    )
                ]
                if len(exact_by_source) == 1:
                    return exact_by_source[0]
            exact = [
                node for vp, node in candidates
                if str(vp.get("source_port_id") or vp.get("sourcePortId") or "").endswith(source_port_id)
            ]
            if len(exact) == 1:
                return exact[0]
        if len(candidates) == 1:
            return candidates[0][1]
        raise HTTPException(
            status_code=422,
            detail={
                "code": "PTS_OUTPUT_PROVIDER_AMBIGUOUS",
                "message": (
                    "PTS output edge is ambiguous: multiple virtual product processes "
                    "share the same flow UUID, but the edge does not identify a concrete "
                    "PTS output port/provider."
                ),
                "pts_node_id": pts_node_id,
                "flow_uuid": flow_uuid,
                "source_port_id": source_port_id,
                "candidate_process_uuids": [
                    str(vp.get("process_uuid") or "")
                    for vp, _node in candidates[:20]
                ],
            },
        )

    def _find_input_targets(pts_node_id: str, flow_uuid: str) -> list[tuple[HybridNode, float, str | None]]:
        targets: list[tuple[HybridNode, float, str | None]] = []
        for vp in virtuals_by_pts.get(pts_node_id, []):
            node = virtual_node_by_key.get((pts_node_id, str(vp.get("process_uuid") or "")))
            if node is None:
                continue
            amount = 0.0
            port_id: str | None = None
            for item in _vp_inputs(vp):
                if str(item.get("flowUuid") or "") != flow_uuid:
                    continue
                if port_id is None:
                    raw_id = str(item.get("id") or "").strip()
                    port_id = raw_id or None
                amount += float(item.get("amount") or 0.0)
            if amount > 0:
                targets.append((node, amount, port_id))
        return targets

    for edge in graph.exchanges:
        from_pts = edge.fromNode in pts_node_ids
        to_pts = edge.toNode in pts_node_ids
        if not from_pts and not to_pts:
            continue

        if from_pts:
            source_port_id = _port_id_from_handle(edge.sourceHandle or edge.source_port_id, "out")
            source_node = _find_output_vp(edge.fromNode, edge.flowUuid, source_port_id)
            if source_node is None:
                continue
        else:
            source_node = node_by_id.get(edge.fromNode)

        if to_pts:
            targets = _find_input_targets(edge.toNode, edge.flowUuid)
            if not targets:
                continue
            for idx, (target_node, amount, input_port_id) in enumerate(targets, start=1):
                target_handle = f"in:{input_port_id}" if input_port_id else edge.targetHandle
                new_edges.append(
                    edge.model_copy(
                        update={
                            "id": f"{edge.id}::pts::{idx}",
                            "fromNode": source_node.id if source_node is not None else edge.fromNode,
                            "toNode": target_node.id,
                            "targetHandle": target_handle,
                            "target_port_id": target_handle,
                            "quantityMode": "dual",
                            "amount": amount,
                            "providerAmount": amount,
                            "consumerAmount": amount,
                        }
                    )
                )
            continue

        if source_node is not None:
            new_edges.append(edge.model_copy(update={"fromNode": source_node.id}))

    return graph.model_copy(update={"nodes": new_nodes, "exchanges": new_edges})

# PTS persistence/publish helpers restored from the pre-Stage-6C main module.
def _next_pts_compile_version(*, db: Session, project_id: str, pts_uuid: str) -> int:
    current_max = (
        db.query(func.max(PtsCompileArtifact.compile_version))
        .filter(PtsCompileArtifact.project_id == project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .scalar()
    )
    return int(current_max or 0) + 1

def _next_pts_published_version(*, db: Session, project_id: str, pts_uuid: str) -> int:
    current_max = (
        db.query(func.max(PtsExternalArtifact.published_version))
        .filter(PtsExternalArtifact.project_id == project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .scalar()
    )
    return int(current_max or 0) + 1

def upsert_pts_compile_artifact(
    *,
    db: Session,
    project_id: str,
    pts_node_id: str,
    force_recompile: bool,
    compile_result: dict | None = None,
    graph: HybridGraph | None = None,
) -> tuple[PtsCompileArtifact, bool]:
    if compile_result is None:
        if graph is None:
            raise ValueError("graph is required when compile_result is not provided")
        pts_node = next((node for node in graph.nodes if node.id == pts_node_id and node.node_kind == "pts_module"), None)
        pts_uuid = str(pts_node.pts_uuid or pts_node.process_uuid or pts_node.id).strip() if pts_node is not None else ""
        ports_policy = _get_pts_resource_ports_policy(db=db, project_id=project_id, pts_uuid=pts_uuid) if pts_uuid else None
        compile_result = compile_pts(graph, pts_node_id, ports_policy=ports_policy, db=db)
    graph_hash = compile_result["graph_hash"]
    cached = (
        db.query(PtsCompileArtifact)
        .filter(
            PtsCompileArtifact.project_id == project_id,
            PtsCompileArtifact.pts_node_id == pts_node_id,
            PtsCompileArtifact.graph_hash == graph_hash,
        )
        .first()
    )
    validation = compile_result["validation"]

    if cached is not None and not force_recompile:
        return cached, True

    if cached is None:
        compile_version = _next_pts_compile_version(
            db=db,
            project_id=project_id,
            pts_uuid=str(compile_result["pts_uuid"]),
        )
        cached = PtsCompileArtifact(
            project_id=project_id,
            pts_node_id=pts_node_id,
            pts_uuid=compile_result["pts_uuid"],
            graph_hash=graph_hash,
            compile_version=compile_version,
            ok=validation.ok,
            matrix_size=validation.matrix_size,
            invertible=validation.invertible,
            errors_json=validation.errors,
            warnings_json=validation.warnings,
            artifact_json=compile_result["artifact"],
        )
        db.add(cached)
    else:
        cached.pts_uuid = compile_result["pts_uuid"]
        cached.graph_hash = graph_hash
        if cached.compile_version is None:
            cached.compile_version = _next_pts_compile_version(
                db=db,
                project_id=project_id,
                pts_uuid=str(compile_result["pts_uuid"]),
            )
        cached.ok = validation.ok
        cached.matrix_size = validation.matrix_size
        cached.invertible = validation.invertible
        cached.errors_json = validation.errors
        cached.warnings_json = validation.warnings
        cached.artifact_json = compile_result["artifact"]

    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == str(compile_result["pts_uuid"])).first()
    if resource is not None:
        resource.project_id = project_id
        resource.pts_node_id = pts_node_id
        resource.compiled_graph_hash = graph_hash
        resource.latest_compile_version = int(cached.compile_version or 0) or resource.latest_compile_version

    db.commit()
    db.refresh(cached)
    return cached, False

def extract_pts_definition(*, graph: HybridGraph, pts_node_id: str, graph_hash: str) -> dict:
    pts_node = next((node for node in graph.nodes if node.id == pts_node_id and node.node_kind == "pts_module"), None)
    if pts_node is None:
        raise ValueError(f"PTS node not found: {pts_node_id}")

    internal_node_ids: list[str] = []
    internal_canvas = _find_pts_internal_canvas(graph, pts_node_id)
    pts_graph: dict = {}
    if isinstance(internal_canvas, dict):
        nodes = internal_canvas.get("nodes")
        if isinstance(nodes, list):
            internal_node_ids = [str(node.get("id")) for node in nodes if isinstance(node, dict) and node.get("id")]
        pts_graph = {
            "functionalUnit": graph.functionalUnit,
            "nodes": nodes if isinstance(nodes, list) else [],
            "exchanges": internal_canvas.get("edges") if isinstance(internal_canvas.get("edges"), list) else [],
            "metadata": {
                "kind": "pts_internal",
                "canvas_id": str(internal_canvas.get("id") or ""),
                "parentPtsNodeId": str(internal_canvas.get("parentPtsNodeId") or pts_node.id),
                "name": str(internal_canvas.get("name") or ""),
            },
        }

    product_refs: list[dict] = []
    for port in pts_node.outputs:
        if bool(port.isProduct):
            product_refs.append(
                {
                    "flow_uuid": port.flowUuid,
                    "flow_name": port.name,
                    "unit": port.unit,
                    "direction": "output",
                }
            )

    ports_policy = _normalize_pts_ports_policy_from_graph(
        pts_graph=pts_graph,
        fallback_policy=None,
    )

    return {
        "pts_uuid": pts_node.pts_uuid or pts_node.process_uuid or pts_node_id,
        "pts_node_id": pts_node_id,
        "internal_node_ids": internal_node_ids,
        "product_refs": product_refs,
        "ports_policy": ports_policy,
        "latest_graph_hash": graph_hash,
        "name": str(pts_node.name or ""),
        "shell_node": _build_pts_shell_node_snapshot(pts_node),
        "pts_graph": pts_graph,
    }

def upsert_pts_definition(*, db: Session, project_id: str, definition: dict) -> PtsDefinition:
    pts_uuid = str(definition["pts_uuid"])
    row = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == project_id, PtsDefinition.pts_uuid == pts_uuid)
        .first()
    )
    if row is None:
        row = PtsDefinition(
            project_id=project_id,
            pts_id=pts_uuid,
            pts_uuid=pts_uuid,
            pts_node_id=str(definition["pts_node_id"]),
            internal_node_ids_json=list(definition.get("internal_node_ids", [])),
            product_refs_json=list(definition.get("product_refs", [])),
            ports_policy_json=dict(definition.get("ports_policy", {})),
            latest_graph_hash=str(definition.get("latest_graph_hash") or ""),
            definition_json=dict(definition),
        )
        db.add(row)
    else:
        row.pts_id = pts_uuid
        row.pts_uuid = pts_uuid
        row.pts_node_id = str(definition["pts_node_id"])
        row.internal_node_ids_json = list(definition.get("internal_node_ids", []))
        row.product_refs_json = list(definition.get("product_refs", []))
        row.ports_policy_json = dict(definition.get("ports_policy", {}))
        row.latest_graph_hash = str(definition.get("latest_graph_hash") or "")
        row.definition_json = dict(definition)
    db.commit()
    db.refresh(row)
    return row

def _slim_exchange_row(item: dict | None, fallback_direction: str, include_amount: bool) -> dict | None:
    if not isinstance(item, dict):
        return None
    raw_direction = str(item.get("direction") or "").lower()
    direction = raw_direction if raw_direction in {"input", "output"} else fallback_direction
    result = {
        "flowUuid": str(item.get("flowUuid") or ""),
        "name": str(item.get("name") or ""),
        "flow_name_en": str(item.get("flow_name_en") or item.get("flowNameEn") or ""),
        "unit": str(item.get("unit") or ""),
        "unitGroup": str(item.get("unitGroup") or ""),
        "sourceProcessUuid": str(item.get("source_process_uuid") or item.get("sourceProcessUuid") or ""),
        "sourceProcessName": str(item.get("source_process_name") or item.get("sourceProcessName") or ""),
        "sourceNodeId": str(item.get("source_node_id") or item.get("sourceNodeId") or ""),
        "direction": direction,
        "isProduct": bool(item.get("isProduct") if "isProduct" in item else item.get("is_product")),
        "internalExposed": item.get("internalExposed", item.get("internal_exposed")),
        "showOnNode": bool(item.get("showOnNode", item.get("show_on_node", False))),
        "product_key": str(item.get("product_key") or ""),
        "port_key": str(item.get("port_key") or ""),
        "display_name": str(item.get("display_name") or ""),
        "reference_product_flow_uuid": str(item.get("reference_product_flow_uuid") or ""),
        "product_name": str(item.get("product_name") or ""),
    }
    if include_amount:
        result["amount"] = float(item.get("amount") or 0.0)
    return result

def _enrich_frontend_ports_flow_name_en(frontend_ports: dict, *, db: Session) -> None:
    if not isinstance(frontend_ports, dict):
        return
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return
    for bucket in ("inputs", "outputs"):
        rows = frontend_ports.get(bucket) if isinstance(frontend_ports.get(bucket), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("flow_name_en") or row.get("flowNameEn") or "").strip():
                continue
            flow_uuid = str(row.get("flowUuid") or "").strip()
            if not flow_uuid:
                continue
            flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
            if not flow_name_en:
                continue
            row["flow_name_en"] = flow_name_en

def _enrich_pts_external_payload_flow_name_en(payload: dict, *, db: Session) -> None:
    if not isinstance(payload, dict):
        return
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return

    def _lookup(flow_uuid: object) -> str:
        token = str(flow_uuid or "").strip()
        if not token:
            return ""
        return flow_name_en_by_uuid.get(token) or flow_name_en_by_uuid.get(token.lower()) or ""

    def _enrich_exchange_rows(rows: object) -> None:
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            flow_name_en = str(row.get("flow_name_en") or row.get("flowNameEn") or "").strip() or _lookup(row.get("flowUuid"))
            if flow_name_en:
                row["flow_name_en"] = flow_name_en
                row.setdefault("flowNameEn", flow_name_en)
            product_name_en = str(row.get("product_name_en") or row.get("productNameEn") or "").strip()
            if not product_name_en and flow_name_en:
                row["product_name_en"] = flow_name_en

    external_boundary = payload.get("external_boundary") if isinstance(payload.get("external_boundary"), dict) else {}
    _enrich_exchange_rows(external_boundary.get("inputs"))
    _enrich_exchange_rows(external_boundary.get("outputs"))
    _enrich_exchange_rows(external_boundary.get("elementary"))

    virtual_processes = payload.get("virtual_processes")
    if isinstance(virtual_processes, list):
        for vp in virtual_processes:
            if not isinstance(vp, dict):
                continue
            ref = vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else None
            ref_flow_uuid = ""
            ref_flow_name_en = ""
            if ref is not None:
                ref_flow_uuid = str(ref.get("flowUuid") or "").strip()
                ref_flow_name_en = str(ref.get("flow_name_en") or ref.get("flowNameEn") or "").strip() or _lookup(ref_flow_uuid)
                if ref_flow_name_en:
                    ref["flow_name_en"] = ref_flow_name_en
                    ref.setdefault("flowNameEn", ref_flow_name_en)
                    ref.setdefault("name_en", ref_flow_name_en)
            if ref_flow_name_en:
                vp.setdefault("product_name_en", ref_flow_name_en)
            _enrich_exchange_rows(vp.get("outputs"))
            _enrich_exchange_rows(vp.get("technosphere_inputs"))
            _enrich_exchange_rows(vp.get("elementary_flows"))

    frontend_ports = payload.get("frontend_ports")
    if isinstance(frontend_ports, dict):
        _enrich_frontend_ports_flow_name_en(frontend_ports, db=db)
        for bucket in ("inputs", "outputs"):
            rows = frontend_ports.get(bucket) if isinstance(frontend_ports.get(bucket), list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                flow_name_en = str(row.get("flow_name_en") or row.get("flowNameEn") or "").strip()
                if not flow_name_en:
                    continue
                base_name_en = str(row.get("product_name_en") or row.get("productNameEn") or "").strip() or flow_name_en
                row.setdefault("product_name_en", base_name_en)
                source_name = str(row.get("sourceProcessName") or row.get("source_process_name") or "").strip()
                if source_name:
                    row.setdefault("display_name_en", f"{base_name_en} @ {source_name}")
                else:
                    row.setdefault("display_name_en", base_name_en)

def _build_frontend_ports_from_external_payload(payload: dict) -> dict:
    pts_uuid = str(payload.get("pts_uuid") or payload.get("ptsUuid") or "").strip() if isinstance(payload, dict) else ""
    external_boundary = payload.get("external_boundary") if isinstance(payload, dict) else {}
    boundary_inputs_raw = external_boundary.get("inputs") if isinstance(external_boundary, dict) else []
    boundary_outputs_raw = external_boundary.get("outputs") if isinstance(external_boundary, dict) else []
    virtuals_raw = payload.get("virtual_processes") if isinstance(payload, dict) else []

    boundary_inputs = [row for row in (boundary_inputs_raw if isinstance(boundary_inputs_raw, list) else []) if isinstance(row, dict)]
    boundary_outputs = [row for row in (boundary_outputs_raw if isinstance(boundary_outputs_raw, list) else []) if isinstance(row, dict)]
    virtuals = [row for row in (virtuals_raw if isinstance(virtuals_raw, list) else []) if isinstance(row, dict)]

    vp_by_flow: dict[str, list[dict]] = {}
    for vp in virtuals:
        ref = vp.get("reference_product") if isinstance(vp.get("reference_product"), dict) else {}
        flow_uuid = str(ref.get("flowUuid") or "")
        if not flow_uuid:
            continue
        source_process_uuid = str(vp.get("source_process_uuid") or vp.get("sourceProcessUuid") or "")
        source_process_name = str(vp.get("source_process_name") or vp.get("sourceProcessName") or "")
        source_node_id = str(vp.get("source_node_id") or vp.get("sourceNodeId") or "")
        process_name = str(vp.get("process_name") or "")
        if not source_process_name and "@" in process_name:
            source_process_name = process_name.split("@", 1)[1].strip()
        vp_by_flow.setdefault(flow_uuid, []).append(
            {
                "sourceProcessUuid": source_process_uuid,
                "sourceProcessName": source_process_name,
                "sourceNodeId": source_node_id,
            }
        )

    outputs: list[dict] = []
    for row in boundary_outputs:
        flow_uuid = str(row.get("flowUuid") or "")
        base_name = str(row.get("name") or flow_uuid or "")
        source_process_uuid = str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or "")
        source_process_name = str(row.get("sourceProcessName") or row.get("source_process_name") or "")
        source_node_id = str(row.get("sourceNodeId") or row.get("source_node_id") or "")

        if source_process_uuid or source_node_id:
            if not source_process_name and flow_uuid:
                for candidate in vp_by_flow.get(flow_uuid, []):
                    if str(candidate.get("sourceProcessUuid") or "") == source_process_uuid:
                        source_process_name = str(candidate.get("sourceProcessName") or "")
                        break
            display_source = source_process_name or source_process_uuid
            display_name = f"{base_name} @ {display_source}" if display_source else base_name
            normalized = dict(row)
            normalized["sourceProcessUuid"] = source_process_uuid
            normalized["sourceProcessName"] = source_process_name
            normalized["sourceNodeId"] = source_node_id
            normalized["product_key"] = str(row.get("product_key") or "") or (
                f"{source_process_uuid}::{flow_uuid}" if source_process_uuid else flow_uuid
            )
            normalized["port_key"] = str(row.get("port_key") or "") or str(normalized["product_key"] or "")
            normalized["display_name"] = str(row.get("display_name") or "") or display_name
            outputs.append(normalized)
            continue

        candidates = vp_by_flow.get(flow_uuid, [])
        if len(candidates) <= 1:
            candidate = candidates[0] if candidates else {}
            candidate_source = str(candidate.get("sourceProcessUuid") or "")
            candidate_source_name = str(candidate.get("sourceProcessName") or "")
            candidate_node = str(candidate.get("sourceNodeId") or "")
            display_source = candidate_source_name or candidate_source
            normalized = dict(row)
            normalized["sourceProcessUuid"] = candidate_source
            normalized["sourceProcessName"] = candidate_source_name
            normalized["sourceNodeId"] = candidate_node
            normalized["product_key"] = str(row.get("product_key") or "") or (
                f"{candidate_source}::{flow_uuid}" if candidate_source else flow_uuid
            )
            normalized["port_key"] = str(row.get("port_key") or "") or str(normalized["product_key"] or "")
            normalized["display_name"] = (
                str(row.get("display_name") or "") or (f"{base_name} @ {display_source}" if display_source else base_name)
            )
            outputs.append(normalized)
            continue

        for candidate in candidates:
            candidate_source = str(candidate.get("sourceProcessUuid") or "")
            candidate_source_name = str(candidate.get("sourceProcessName") or "")
            candidate_node = str(candidate.get("sourceNodeId") or "")
            display_source = candidate_source_name or candidate_source
            normalized = dict(row)
            normalized["sourceProcessUuid"] = candidate_source
            normalized["sourceProcessName"] = candidate_source_name
            normalized["sourceNodeId"] = candidate_node
            normalized["product_key"] = f"{candidate_source}::{flow_uuid}" if candidate_source else flow_uuid
            normalized["port_key"] = str(normalized["product_key"] or "")
            normalized["display_name"] = f"{base_name} @ {display_source}" if display_source else base_name
            outputs.append(normalized)

    deduped_outputs: list[dict] = []
    seen_output_keys: set[tuple[str, str, str, str]] = set()
    for row in outputs:
        dedupe_key = (
            str(row.get("flowUuid") or ""),
            str(row.get("sourceProcessUuid") or ""),
            str(row.get("sourceNodeId") or ""),
            str(row.get("port_key") or row.get("product_key") or ""),
        )
        if dedupe_key in seen_output_keys:
            continue
        seen_output_keys.add(dedupe_key)
        deduped_outputs.append(row)

    outputs_by_flow: dict[str, list[dict]] = {}
    for row in deduped_outputs:
        flow_uuid = str(row.get("flowUuid") or "")
        if not flow_uuid:
            continue
        outputs_by_flow.setdefault(flow_uuid, []).append(row)
    for flow_uuid, rows in outputs_by_flow.items():
        if len(rows) <= 1:
            continue
        missing_binding = [
            row
            for row in rows
            if not str(row.get("sourceProcessUuid") or "").strip() and not str(row.get("sourceNodeId") or "").strip()
        ]
        if missing_binding:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "INVALID_PORTS_OUTPUT_SOURCE_BINDING",
                    "message": f"ports.outputs contains duplicated flowUuid without source binding: flowUuid={flow_uuid}",
                    "evidence": missing_binding,
                },
            )

    return {
        "inputs": [
            {
                **row,
                "pts_uuid": pts_uuid,
                "id": str(row.get("id") or _stable_shell_port_id(row={**row, "pts_uuid": pts_uuid}, idx=idx, direction="input", flow_type="technosphere")),
                "port_key": str(row.get("port_key") or f"in::{str(row.get('flowUuid') or '')}::{idx}"),
                "display_name": str(row.get("display_name") or row.get("name") or row.get("flowUuid") or ""),
            }
            for idx, row in enumerate(boundary_inputs, start=1)
        ],
        "outputs": [
            {
                **row,
                "pts_uuid": pts_uuid,
                "id": str(
                    row.get("id")
                    or _stable_shell_port_id(
                        row={**row, "pts_uuid": pts_uuid},
                        idx=idx,
                        direction="output",
                        flow_type="technosphere",
                    )
                ),
            }
            for idx, row in enumerate(deduped_outputs, start=1)
        ],
    }

def _external_row_to_flow_port(row: dict, *, idx: int, direction: str, flow_type: str) -> dict:
    port_id = _stable_shell_port_id(row=row, idx=idx, direction=direction, flow_type=flow_type)
    legacy_port_id = _legacy_shell_port_id(row=row, idx=idx, direction=direction, flow_type=flow_type)
    raw_name = str(row.get("name") or row.get("flowUuid") or "")
    source_name = str(row.get("sourceProcessName") or row.get("source_process_name") or "").strip()
    catalog_name = raw_name
    if source_name and "@" in raw_name:
        suffix = raw_name.rsplit("@", 1)[1].strip()
        if suffix == source_name:
            catalog_name = raw_name.rsplit("@", 1)[0].strip()
    return {
        "id": port_id,
        "legacyPortId": legacy_port_id,
        "flowUuid": str(row.get("flowUuid") or ""),
        "name": catalog_name,
        "flow_name_en": str(row.get("flow_name_en") or row.get("flowNameEn") or ""),
        "display_name_en": str(row.get("display_name_en") or row.get("displayNameEn") or ""),
        "unit": str(row.get("unit") or ""),
        "unitGroup": row.get("unitGroup"),
        "amount": float(row.get("amount") or 0.0),
        "externalSaleAmount": float(row.get("externalSaleAmount") or 0.0),
        "type": flow_type,
        "direction": direction,
        "showOnNode": bool(row.get("showOnNode", True)),
        "internalExposed": row.get("internalExposed"),
        "dbMapping": row.get("dbMapping"),
        "sourceProcessUuid": str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or ""),
        "sourceProcessName": str(row.get("sourceProcessName") or row.get("source_process_name") or ""),
        "sourceNodeId": str(row.get("sourceNodeId") or row.get("source_node_id") or ""),
        "isProduct": bool(row.get("isProduct", False)),
        "allocationFactor": row.get("allocationFactor"),
        "product_key": str(row.get("product_key") or ""),
        "port_key": str(row.get("port_key") or row.get("product_key") or ""),
        "reference_product_flow_uuid": str(row.get("reference_product_flow_uuid") or ""),
        "product_name": str(row.get("product_name") or ""),
        "product_name_en": str(row.get("product_name_en") or row.get("productNameEn") or ""),
    }

def _legacy_shell_port_id(*, row: dict, idx: int, direction: str, flow_type: str) -> str:
    if direction == "input":
        return str(row.get("port_key") or row.get("id") or f"in::{str(row.get('flowUuid') or '')}::{idx}")
    return str(row.get("port_key") or row.get("product_key") or row.get("id") or f"{flow_type}_{idx}")

def _stable_shell_port_id(*, row: dict, idx: int, direction: str, flow_type: str) -> str:
    legacy_port_id = _legacy_shell_port_id(row=row, idx=idx, direction=direction, flow_type=flow_type)
    pts_uuid = str(row.get("pts_uuid") or row.get("ptsUuid") or "").strip()
    digest = hashlib.sha256(f"{pts_uuid}|{direction}|{legacy_port_id}".encode("utf-8")).hexdigest()[:16]
    prefix = "ptsin" if direction == "input" else "ptsout"
    return f"{prefix}_{digest}"

def _build_pts_port_id_map(*, shell_node: dict) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for port in list(shell_node.get("inputs") or []) + list(shell_node.get("outputs") or []):
        if not isinstance(port, dict):
            continue
        legacy_port_id = str(port.get("legacyPortId") or port.get("legacy_port_id") or "").strip()
        port_id = str(port.get("id") or "").strip()
        if legacy_port_id and port_id:
            mapping[legacy_port_id] = port_id
    return mapping

def _resolve_default_visible_port_ids(
    *,
    shell_node: dict,
    hints: list[PtsBoundaryPortHint] | None,
) -> list[str]:
    if not isinstance(shell_node, dict) or not hints:
        return []

    buckets = {
        "input": list(shell_node.get("inputs") or []),
        "output": list(shell_node.get("outputs") or []),
    }
    matched_ids: list[str] = []
    seen_ids: set[str] = set()

    for hint in hints:
        if not isinstance(hint, PtsBoundaryPortHint):
            continue
        direction = str(hint.direction or "").strip()
        flow_uuid = str(hint.flow_uuid or "").strip()
        if direction not in buckets or not flow_uuid:
            continue

        candidates = [
            port
            for port in buckets[direction]
            if isinstance(port, dict) and str(port.get("flowUuid") or "").strip() == flow_uuid
        ]
        if not candidates:
            continue

        source_process_uuid = str(hint.source_process_uuid or "").strip()
        source_node_id = str(hint.source_node_id or "").strip()
        source_process_name = str(hint.source_process_name or "").strip()
        hint_name = str(hint.name or "").strip()

        def _unique_match(rows: list[dict], predicate) -> dict | None:
            matched = [row for row in rows if predicate(row)]
            return matched[0] if len(matched) == 1 else None

        chosen: dict | None = None
        if source_process_uuid:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or "").strip() == source_process_uuid,
            )
        if chosen is None and source_node_id:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("sourceNodeId") or row.get("source_node_id") or "").strip() == source_node_id,
            )
        if chosen is None and source_process_name:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("sourceProcessName") or row.get("source_process_name") or "").strip() == source_process_name,
            )
        if chosen is None and hint_name and "@" in hint_name:
            chosen = _unique_match(
                candidates,
                lambda row: str(row.get("name") or "").strip() == hint_name,
            )
        if chosen is None and len(candidates) == 1:
            chosen = candidates[0]

        if chosen is None:
            continue

        port_id = str(chosen.get("id") or "").strip()
        if port_id and port_id not in seen_ids:
            seen_ids.add(port_id)
            matched_ids.append(port_id)

    return matched_ids

def _apply_default_visible_port_ids_to_shell_node(*, shell_node: dict, default_visible_port_ids: list[str]) -> dict:
    if not isinstance(shell_node, dict):
        return shell_node
    selected_ids = {str(item).strip() for item in default_visible_port_ids if str(item).strip()}
    if not selected_ids:
        return shell_node

    updated = dict(shell_node)
    outputs = []
    for port in list(shell_node.get("outputs") or []):
        if not isinstance(port, dict):
            outputs.append(port)
            continue
        normalized = dict(port)
        port_id = str(normalized.get("id") or "").strip()
        normalized["showOnNode"] = port_id in selected_ids
        outputs.append(normalized)
    updated["outputs"] = outputs
    return updated

def _pts_port_identity_key(row: dict) -> tuple[str, str, str, str, str, str]:
    if not isinstance(row, dict):
        return ("", "", "", "", "", "")
    return (
        str(row.get("flowUuid") or row.get("flow_uuid") or "").strip(),
        str(row.get("sourceProcessUuid") or row.get("source_process_uuid") or "").strip(),
        str(row.get("sourceNodeId") or row.get("source_node_id") or "").strip(),
        str(row.get("sourceProcessName") or row.get("source_process_name") or "").strip(),
        str(row.get("direction") or "").strip(),
        str(row.get("port_key") or row.get("product_key") or "").strip(),
    )

def _apply_default_visible_port_ids_to_external_payload(*, payload: dict, shell_node: dict, default_visible_port_ids: list[str]) -> dict:
    if not isinstance(payload, dict):
        return payload
    selected_ids = {str(item).strip() for item in default_visible_port_ids if str(item).strip()}
    if not selected_ids:
        return payload

    selected_identity_keys = {
        _pts_port_identity_key(port)
        for port in list(shell_node.get("outputs") or [])
        if isinstance(port, dict) and str(port.get("id") or "").strip() in selected_ids
    }

    updated = dict(payload)
    frontend_ports = updated.get("frontend_ports") if isinstance(updated.get("frontend_ports"), dict) else {}
    outputs = frontend_ports.get("outputs") if isinstance(frontend_ports.get("outputs"), list) else []
    normalized_outputs: list[dict] = []
    for row in outputs:
        if not isinstance(row, dict):
            continue
        normalized = dict(row)
        port_id = str(normalized.get("id") or "").strip()
        normalized["showOnNode"] = (
            _pts_port_identity_key(normalized) in selected_identity_keys
            if selected_identity_keys
            else port_id in selected_ids
        )
        normalized_outputs.append(normalized)
    updated["frontend_ports"] = {
        **dict(frontend_ports),
        "outputs": normalized_outputs,
    }
    return updated

def _is_effectively_zero(value: float, *, tolerance: float = 1e-9) -> bool:
    return abs(float(value)) <= tolerance

def _is_total_significantly_below_target(
    actual_total: float,
    expected_total: float,
    *,
    tolerance: float = 1e-6,
) -> bool:
    if expected_total <= 0:
        return False
    return actual_total < (expected_total - tolerance)

def _build_pts_publish_warnings(
    *,
    project_id: str,
    pts_uuid: str,
    pts_node_id: str | None,
    pts_graph: dict | None,
    external_payload: dict | None,
) -> list[PtsModelWarning]:
    _ = project_id
    graph_nodes = list((pts_graph or {}).get("nodes") or []) if isinstance(pts_graph, dict) else []
    if not graph_nodes:
        return []

    warnings: list[PtsModelWarning] = []
    for node in graph_nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("node_kind") or "").strip() != "market_process":
            continue

        source_node_id = str(node.get("id") or "").strip()
        if not source_node_id:
            continue

        raw_inputs = node.get("inputs") if isinstance(node.get("inputs"), list) else []
        actual_total = 0.0
        expected_total = 1.0 if str(node.get("mode") or "").strip() == "normalized" else 0.0
        actual_rows = 0
        evidence: list[dict] = []
        for port in raw_inputs:
            if not isinstance(port, dict):
                continue
            if is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.get("type"))):
                continue
            try:
                amount = float(port.get("amount") or 0.0)
            except (TypeError, ValueError):
                amount = 0.0
            actual_total += amount
            actual_rows += 1
            evidence.append(
                {
                    "flow_uuid": str(port.get("flowUuid") or ""),
                    "flow_name": str(port.get("name") or ""),
                    "unit": str(port.get("unit") or ""),
                    "amount": amount,
                    "source_process_uuid": str(port.get("sourceProcessUuid") or port.get("source_process_uuid") or ""),
                    "source_process_name": str(port.get("sourceProcessName") or port.get("source_process_name") or ""),
                    "source_node_id": str(port.get("sourceNodeId") or port.get("source_node_id") or ""),
                }
            )

        if actual_rows <= 0 and _is_effectively_zero(actual_total):
            warning_code = "PTS_INPUT_SHARE_ZERO"
        elif _is_effectively_zero(actual_total):
            warning_code = "PTS_INPUT_SHARE_ZERO"
        elif expected_total > 0 and _is_total_significantly_below_target(actual_total, expected_total):
            warning_code = "PTS_INPUT_SHARE_BELOW_TARGET"
        else:
            continue

        warnings.append(
            PtsModelWarning(
                code=warning_code,
                severity="warning",
                message=f"Input share total is {actual_total:.6f} (target {expected_total:g})",
                pts_uuid=pts_uuid,
                pts_node_id=pts_node_id or source_node_id or None,
                node_name=str(node.get("name") or "").strip() or None,
                expected_total=expected_total,
                actual_total=actual_total,
                evidence=evidence[:50],
            )
        )

    return warnings


def _raise_if_pts_market_has_no_internal_share(
    *,
    pts_uuid: str,
    pts_node_id: str | None,
    pts_graph: dict | None,
) -> None:
    zero_share_warnings = [
        warning
        for warning in _build_pts_publish_warnings(
            project_id="",
            pts_uuid=pts_uuid,
            pts_node_id=pts_node_id,
            pts_graph=pts_graph,
            external_payload=None,
        )
        if warning.code == "PTS_INPUT_SHARE_ZERO"
    ]
    if not zero_share_warnings:
        return
    raise HTTPException(
        status_code=409,
        detail={
            "code": "PTS_MARKET_PROCESS_REQUIRES_INTERNAL_SUPPLIERS",
            "message": "Market processes inside PTS must include at least one internal upstream supplier.",
            "evidence": [warning.model_dump() for warning in zero_share_warnings[:20]],
        },
    )


def _market_input_port_id(port: dict) -> str:
    return str(port.get("id") or port.get("port_id") or port.get("portId") or "").strip()


def _edge_target_port_id(edge: dict) -> str:
    raw = str(
        edge.get("target_port_id")
        or edge.get("targetPortId")
        or edge.get("targetHandle")
        or ""
    )
    return _port_id_from_handle(raw, "in") or raw.strip()


def _validate_pts_market_supplier_coverage(
    *,
    pts_uuid: str,
    pts_node_id: str | None,
    pts_graph: dict | None,
) -> list[PtsModelWarning]:
    """Market processes inside PTS must keep supplier semantics internal.

    A market with all suppliers inside the PTS is valid. A market with some
    internal suppliers is allowed but warned as a truncated market. A market
    with no internal suppliers cannot be packaged because PTS boundary inputs
    cannot represent provider shares.
    """
    if not isinstance(pts_graph, dict):
        return []

    raw_nodes = [node for node in list(pts_graph.get("nodes") or []) if isinstance(node, dict)]
    raw_edges = [edge for edge in list(pts_graph.get("exchanges") or []) if isinstance(edge, dict)]
    if not raw_nodes:
        return []

    nodes_by_id = {str(node.get("id") or "").strip(): node for node in raw_nodes if str(node.get("id") or "").strip()}
    internal_sources_by_market_port: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    internal_sources_by_market_flow: dict[tuple[str, str], set[str]] = defaultdict(set)
    for edge in raw_edges:
        to_node = str(edge.get("toNode") or edge.get("target") or "").strip()
        from_node = str(edge.get("fromNode") or edge.get("source") or "").strip()
        flow_uuid = str(edge.get("flowUuid") or edge.get("flow_uuid") or "").strip()
        if not to_node or not from_node or not flow_uuid:
            continue
        if to_node not in nodes_by_id or from_node not in nodes_by_id:
            continue
        target_port_id = _edge_target_port_id(edge)
        internal_sources_by_market_flow[(to_node, flow_uuid)].add(from_node)
        if target_port_id:
            internal_sources_by_market_port[(to_node, target_port_id, flow_uuid)].add(from_node)

    warnings: list[PtsModelWarning] = []
    blocking_errors: list[dict] = []
    for node in raw_nodes:
        if str(node.get("node_kind") or "").strip() != "market_process":
            continue
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            continue

        provider_inputs: list[tuple[dict, str, str]] = []
        covered_inputs: list[dict] = []
        missing_inputs: list[dict] = []
        for port in list(node.get("inputs") or []):
            if not isinstance(port, dict):
                continue
            if is_elementary_flow_semantic(graph_exchange_type_to_flow_semantic(port.get("type"))):
                continue
            flow_uuid = str(port.get("flowUuid") or port.get("flow_uuid") or "").strip()
            if not flow_uuid:
                continue
            port_id = _market_input_port_id(port)
            provider_inputs.append((port, port_id, flow_uuid))

        if not provider_inputs:
            continue

        flow_input_counts: dict[str, int] = defaultdict(int)
        for _port, _port_id, flow_uuid in provider_inputs:
            flow_input_counts[flow_uuid] += 1

        for port, port_id, flow_uuid in provider_inputs:
            sources = set()
            if port_id:
                sources.update(internal_sources_by_market_port.get((node_id, port_id, flow_uuid), set()))
            if not sources and flow_input_counts.get(flow_uuid, 0) == 1:
                sources.update(internal_sources_by_market_flow.get((node_id, flow_uuid), set()))
            evidence = {
                "node_id": node_id,
                "node_name": str(node.get("name") or ""),
                "port_id": port_id,
                "flow_uuid": flow_uuid,
                "flow_name": str(port.get("name") or ""),
                "unit": str(port.get("unit") or ""),
                "amount": port.get("amount"),
                "internal_source_node_ids": sorted(sources),
            }
            if sources:
                covered_inputs.append(evidence)
            else:
                missing_inputs.append(evidence)

        node_name = str(node.get("name") or "").strip() or node_id
        if not covered_inputs:
            blocking_errors.append(
                {
                    "node_id": node_id,
                    "node_name": node_name,
                    "message": (
                        f"Market process {node_name} is packed without internal suppliers. "
                        "Package the market with at least one upstream supplier, preferably all suppliers."
                    ),
                    "missing_inputs": missing_inputs[:50],
                }
            )
            continue

        if missing_inputs:
            warnings.append(
                PtsModelWarning(
                    code="PTS_MARKET_PROCESS_PARTIAL_SUPPLIERS",
                    severity="warning",
                    message=(
                        f"Market process {node_name} includes only part of its suppliers. "
                        "Missing suppliers are not exposed as PTS inputs and their market shares are dropped."
                    ),
                    pts_uuid=pts_uuid,
                    pts_node_id=pts_node_id or node_id,
                    node_name=node_name,
                    expected_total=float(len(provider_inputs)),
                    actual_total=float(len(covered_inputs)),
                    evidence=[
                        {
                            "covered_inputs": covered_inputs[:50],
                            "missing_inputs": missing_inputs[:50],
                        }
                    ],
                )
            )

    if blocking_errors:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "PTS_MARKET_PROCESS_REQUIRES_INTERNAL_SUPPLIERS",
                "message": "Market processes inside PTS must include upstream suppliers; isolated market processes cannot be packaged.",
                "evidence": blocking_errors[:20],
            },
        )

    return warnings


def build_pts_external_payload(*, project_id: str, pts_uuid: str, definition: dict, compile_row: PtsCompileArtifact) -> dict:
    artifact = compile_row.artifact_json or {}
    virtual_processes_raw = artifact.get("virtual_processes") if isinstance(artifact, dict) else []
    ports_policy = definition.get("ports_policy") if isinstance(definition.get("ports_policy"), dict) else {}
    policy_inputs = ports_policy.get("inputs") if isinstance(ports_policy.get("inputs"), list) else []
    policy_outputs = ports_policy.get("outputs") if isinstance(ports_policy.get("outputs"), list) else []

    def _policy_flow_uuid(row: dict) -> str:
        return str(row.get("flow_uuid") or row.get("flowUuid") or "").strip()

    def _policy_is_exposed(row: dict) -> bool:
        return bool(
            row.get("internal_exposed")
            or row.get("internalExposed")
            or row.get("show_on_node")
            or row.get("showOnNode")
        )

    def _policy_key(row: dict) -> tuple[str, str, str]:
        return (
            _policy_flow_uuid(row),
            str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip(),
            str(row.get("source_node_id") or row.get("sourceNodeId") or "").strip(),
        )

    def _policy_allowed(rows: list[dict]) -> set[tuple[str, str, str]]:
        allowed: set[tuple[str, str, str]] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            if not _policy_is_exposed(row):
                continue
            allowed.add(_policy_key(row))
        return allowed

    exposed_input_policies = [
        row
        for row in policy_inputs
        if isinstance(row, dict)
        and not bool(row.get("is_product") or row.get("isProduct"))
        and _policy_is_exposed(row)
    ]
    allowed_input_keys = _policy_allowed(exposed_input_policies)
    allowed_input_flow_uuids = {_policy_flow_uuid(row) for row in exposed_input_policies if _policy_flow_uuid(row)}
    preferred_input_policy_by_flow: dict[str, dict] = {}
    for row in exposed_input_policies:
        flow_uuid = _policy_flow_uuid(row)
        if flow_uuid and flow_uuid not in preferred_input_policy_by_flow:
            preferred_input_policy_by_flow[flow_uuid] = row
    exposed_output_policies = [
        row
        for row in policy_outputs
        if isinstance(row, dict)
        and bool(row.get("is_product") or row.get("isProduct"))
        and _policy_is_exposed(row)
    ]

    def _boundary_rows(rows: object, fallback_direction: str) -> list[dict]:
        source = rows if isinstance(rows, list) else []
        return [
            row
            for row in (
                _slim_exchange_row(item if isinstance(item, dict) else None, fallback_direction, False)
                for item in source
            )
            if row is not None
        ]

    all_virtual_processes: list[dict] = []
    for idx, vp in enumerate((virtual_processes_raw if isinstance(virtual_processes_raw, list) else []), start=1):
        vp_obj = vp if isinstance(vp, dict) else {}
        ref_obj = vp_obj.get("reference_product") if isinstance(vp_obj.get("reference_product"), dict) else {}
        inputs_raw = vp_obj.get("technosphere_inputs") if isinstance(vp_obj, dict) else None
        emissions_raw = vp_obj.get("elementary_flows") if isinstance(vp_obj, dict) else None
        process_uuid = str(vp_obj.get("process_uuid") or "")
        flow_uuid = str(ref_obj.get("flowUuid") or "")
        technosphere_inputs = [
            row
            for row in (
                _slim_exchange_row(item if isinstance(item, dict) else None, "input", True)
                for item in (inputs_raw if isinstance(inputs_raw, list) else [])
            )
            if row is not None
        ]
        elementary_flows = [
            row
            for row in (
                _slim_exchange_row(item if isinstance(item, dict) else None, "output", True)
                for item in (emissions_raw if isinstance(emissions_raw, list) else [])
            )
            if row is not None
        ]
        all_virtual_processes.append(
            {
                "process_uuid": process_uuid,
                "process_name": str(vp_obj.get("process_name") or ""),
                "product_key": str(vp_obj.get("product_key") or ""),
                "product_name": str(vp_obj.get("product_name") or ref_obj.get("name") or ""),
                "is_product": bool(vp_obj.get("is_product", True)),
                "reference_unit": str(vp_obj.get("reference_unit") or ref_obj.get("unit") or ""),
                "reference_unit_group": (
                    vp_obj.get("reference_unit_group")
                    or ref_obj.get("unitGroup")
                    or None
                ),
                "reference_product_flow_uuid": str(
                    vp_obj.get("reference_product_flow_uuid") or ref_obj.get("flowUuid") or ""
                ),
                "virtual_process_key": f"{process_uuid or 'vp'}::{idx}",
                "source_process_uuid": str(vp_obj.get("source_process_uuid") or vp_obj.get("sourceProcessUuid") or ""),
                "source_process_name": str(vp_obj.get("source_process_name") or vp_obj.get("sourceProcessName") or ""),
                "source_node_id": str(vp_obj.get("source_node_id") or vp_obj.get("sourceNodeId") or ""),
                "source_port_id": str(vp_obj.get("source_port_id") or vp_obj.get("sourcePortId") or ""),
                "source_port_name": str(vp_obj.get("source_port_name") or vp_obj.get("sourcePortName") or ""),
                "allocation_fraction": vp_obj.get("allocation_fraction"),
                "normalization_reference_amount": vp_obj.get("normalization_reference_amount"),
                "reference_product": {
                    "flowUuid": flow_uuid,
                    "name": str(ref_obj.get("name") or ""),
                    "unit": str(ref_obj.get("unit") or ""),
                    "unitGroup": ref_obj.get("unitGroup") if isinstance(ref_obj, dict) else None,
                },
                "outputs": [
                    dict(item)
                    for item in (vp_obj.get("outputs") if isinstance(vp_obj.get("outputs"), list) else [])
                    if isinstance(item, dict)
                ],
                "technosphere_inputs": technosphere_inputs,
                "elementary_flows": elementary_flows,
            }
        )

    def _policy_product_key(row: dict) -> str:
        flow_uuid = str(row.get("flow_uuid") or row.get("flowUuid") or "").strip()
        source_process_uuid = str(row.get("source_process_uuid") or row.get("sourceProcessUuid") or "").strip()
        return f"{source_process_uuid}::{flow_uuid}" if source_process_uuid and flow_uuid else flow_uuid

    def _match_virtual_process(policy_row: dict) -> dict | None:
        policy_product_key = _policy_product_key(policy_row)
        if policy_product_key:
            exact = [vp for vp in all_virtual_processes if str(vp.get("product_key") or "") == policy_product_key]
            if len(exact) == 1:
                return exact[0]
        flow_uuid = str(policy_row.get("flow_uuid") or policy_row.get("flowUuid") or "").strip()
        source_process_uuid = str(policy_row.get("source_process_uuid") or policy_row.get("sourceProcessUuid") or "").strip()
        source_node_id = str(policy_row.get("source_node_id") or policy_row.get("sourceNodeId") or "").strip()
        candidates = [
            vp for vp in all_virtual_processes
            if str(vp.get("reference_product_flow_uuid") or "") == flow_uuid
        ]
        if source_process_uuid:
            exact = [
                vp for vp in candidates
                if str(vp.get("source_process_uuid") or vp.get("sourceProcessUuid") or "") == source_process_uuid
            ]
            if len(exact) == 1:
                return exact[0]
        if source_node_id:
            exact = [
                vp for vp in candidates
                if str(vp.get("source_node_id") or vp.get("sourceNodeId") or "") == source_node_id
            ]
            if len(exact) == 1:
                return exact[0]
        if len(candidates) == 1:
            return candidates[0]
        return None

    selected_virtual_processes: list[dict] = []
    boundary_outputs: list[dict] = []
    output_virtual_process_bindings: list[dict] = []
    matched_virtual_process_keys: set[str] = set()

    for policy_row in exposed_output_policies:
        vp = _match_virtual_process(policy_row)
        flow_uuid = str(policy_row.get("flow_uuid") or policy_row.get("flowUuid") or "").strip()
        source_process_uuid = str(policy_row.get("source_process_uuid") or policy_row.get("sourceProcessUuid") or "").strip()
        source_process_name = str(policy_row.get("source_process_name") or policy_row.get("sourceProcessName") or "").strip()
        source_node_id = str(policy_row.get("source_node_id") or policy_row.get("sourceNodeId") or "").strip()
        base_name = str(policy_row.get("name") or flow_uuid or "")
        product_key = _policy_product_key(policy_row)
        if vp is None:
            output_virtual_process_bindings.append(
                {
                    "product_key": product_key,
                    "flowUuid": flow_uuid,
                    "virtual_process_key": None,
                    "process_uuid": None,
                }
            )
            continue

        if not source_process_name:
            source_process_name = str(vp.get("source_process_name") or vp.get("sourceProcessName") or "").strip()
        if not source_node_id:
            source_node_id = str(vp.get("source_node_id") or vp.get("sourceNodeId") or "").strip()
        display_source = source_process_name or source_process_uuid
        display_name = f"{base_name} @ {display_source}" if display_source else base_name
        if "@" in base_name and display_source:
            existing_source = base_name.split("@", 1)[1].strip()
            display_name = base_name if existing_source == display_source else f"{base_name} @ {display_source}"

        boundary_outputs.append(
            {
                "flowUuid": flow_uuid,
                "name": base_name,
                "unit": str(
                    vp.get("reference_unit")
                    or ((vp.get("reference_product") or {}).get("unit") if isinstance(vp.get("reference_product"), dict) else "")
                    or ""
                ),
                "unitGroup": (
                    vp.get("reference_unit_group")
                    or ((vp.get("reference_product") or {}).get("unitGroup") if isinstance(vp.get("reference_product"), dict) else None)
                    or ""
                ),
                "sourceProcessUuid": source_process_uuid or str(vp.get("source_process_uuid") or ""),
                "sourceProcessName": source_process_name,
                "sourceNodeId": source_node_id,
                "direction": "output",
                "isProduct": True,
                "internalExposed": True,
                "showOnNode": True,
                "product_key": str(vp.get("product_key") or product_key),
                "port_key": str(vp.get("product_key") or product_key),
                "display_name": display_name,
                "reference_product_flow_uuid": str(vp.get("reference_product_flow_uuid") or flow_uuid),
                "product_name": str(vp.get("product_name") or base_name or flow_uuid),
            }
        )

        process_uuid = str(vp.get("process_uuid") or "")
        virtual_process_key = str(vp.get("virtual_process_key") or str(vp.get("product_key") or product_key))
        if virtual_process_key and virtual_process_key not in matched_virtual_process_keys:
            matched_virtual_process_keys.add(virtual_process_key)
            selected_virtual_processes.append(vp)

        output_virtual_process_bindings.append(
            {
                "product_key": str(vp.get("product_key") or product_key),
                "flowUuid": flow_uuid,
                "virtual_process_key": vp.get("virtual_process_key"),
                "process_uuid": process_uuid,
            }
        )

    def _aggregate_rows(rows: list[dict], *, collapse_source: bool) -> list[dict]:
        grouped: dict[tuple[str, str, str, str, str, str, str], dict] = {}
        for row in rows:
            key = (
                str(row.get("flowUuid") or ""),
                str(row.get("unit") or ""),
                str(row.get("unitGroup") or ""),
                str(row.get("direction") or ""),
                str(row.get("name") or ""),
                "" if collapse_source else str(row.get("sourceProcessUuid") or ""),
                "" if collapse_source else str(row.get("sourceNodeId") or ""),
            )
            amount = float(row.get("amount") or 0.0)
            if key not in grouped:
                grouped[key] = dict(row)
                if collapse_source:
                    grouped[key]["sourceProcessUuid"] = ""
                    grouped[key]["sourceProcessName"] = ""
                    grouped[key]["sourceNodeId"] = ""
                grouped[key]["amount"] = 0.0
            grouped[key]["amount"] = float(grouped[key]["amount"] or 0.0) + amount
        return list(grouped.values())

    boundary_inputs: list[dict] = []
    boundary_elementary: list[dict] = []
    for vp in selected_virtual_processes:
        for row in vp.get("technosphere_inputs") or []:
            if not isinstance(row, dict):
                continue
            slim = _slim_exchange_row(row, "input", True)
            if slim is None:
                continue
            if allowed_input_flow_uuids and str(slim.get("flowUuid") or "") not in allowed_input_flow_uuids:
                continue
            if allowed_input_keys:
                row_key = _policy_key(slim)
                if row_key not in allowed_input_keys and not any(
                    _policy_flow_uuid(item) == str(slim.get("flowUuid") or "") for item in policy_inputs if isinstance(item, dict) and _policy_is_exposed(item)
                ):
                    continue
            preferred_policy = preferred_input_policy_by_flow.get(str(slim.get("flowUuid") or ""))
            if preferred_policy is not None:
                slim["name"] = str(preferred_policy.get("name") or slim.get("name") or "")
                slim["unit"] = str(preferred_policy.get("unit") or slim.get("unit") or "")
                slim["unitGroup"] = str(preferred_policy.get("unit_group") or preferred_policy.get("unitGroup") or slim.get("unitGroup") or "")
                slim["sourceProcessUuid"] = ""
                slim["sourceProcessName"] = ""
                slim["sourceNodeId"] = ""
                slim["internalExposed"] = True
                slim["showOnNode"] = True
            boundary_inputs.append(slim)
        for row in vp.get("elementary_flows") or []:
            if not isinstance(row, dict):
                continue
            slim = _slim_exchange_row(row, "output", True)
            if slim is None:
                continue
            boundary_elementary.append(slim)

    boundary_inputs = _aggregate_rows(boundary_inputs, collapse_source=True)
    boundary_outputs = _aggregate_rows(boundary_outputs, collapse_source=False)
    boundary_elementary = _aggregate_rows(boundary_elementary, collapse_source=True)
    virtual_processes = selected_virtual_processes

    return {
        "project_id": project_id,
        "pts_uuid": pts_uuid,
        "pts_node_id": str(compile_row.pts_node_id),
        "graph_hash": str(compile_row.graph_hash),
        "ok": bool(compile_row.ok),
        "errors": list(compile_row.errors_json or []),
        "warnings": list(compile_row.warnings_json or []),
        "matrix_size": int(compile_row.matrix_size or 0),
        "invertible": bool(compile_row.invertible),
        "external_boundary": {
            "inputs": boundary_inputs,
            "outputs": boundary_outputs,
            "elementary": boundary_elementary,
        },
        "virtual_processes": virtual_processes,
        "frontend_ports": _build_frontend_ports_from_external_payload(
            {
                "external_boundary": {
                    "inputs": boundary_inputs,
                    "outputs": boundary_outputs,
                    "elementary": boundary_elementary,
                },
                "virtual_processes": virtual_processes,
            }
        ),
        "output_virtual_process_bindings": output_virtual_process_bindings,
        "definition_summary": {
            "pts_uuid": pts_uuid,
            "pts_node_id": str(definition.get("pts_node_id") or ""),
            "internal_node_count": len(definition.get("internal_node_ids") or []),
            "product_ref_count": len(definition.get("product_refs") or []),
        },
    }

def upsert_pts_external_artifact(
    *,
    db: Session,
    project_id: str,
    pts_uuid: str,
    pts_node_id: str,
    graph_hash: str,
    payload: dict,
    source_compile_id: str | None = None,
    source_compile_version: int | None = None,
    set_active: bool = True,
) -> PtsExternalArtifact:
    row = (
        db.query(PtsExternalArtifact)
        .filter(
            PtsExternalArtifact.project_id == project_id,
            PtsExternalArtifact.pts_uuid == pts_uuid,
            PtsExternalArtifact.graph_hash == graph_hash,
        )
        .first()
    )
    if row is None:
        published_version = _next_pts_published_version(db=db, project_id=project_id, pts_uuid=pts_uuid)
        row = PtsExternalArtifact(
            project_id=project_id,
            pts_id=pts_uuid,
            pts_uuid=pts_uuid,
            pts_node_id=pts_node_id,
            graph_hash=graph_hash,
            published_version=published_version,
            source_compile_id=source_compile_id,
            source_compile_version=source_compile_version,
            artifact_json=payload,
        )
        db.add(row)
    else:
        row.pts_id = pts_uuid
        row.pts_uuid = pts_uuid
        row.pts_node_id = pts_node_id
        if row.published_version is None:
            row.published_version = _next_pts_published_version(db=db, project_id=project_id, pts_uuid=pts_uuid)
        row.source_compile_id = source_compile_id or row.source_compile_id
        row.source_compile_version = source_compile_version if source_compile_version is not None else row.source_compile_version
        row.artifact_json = payload
    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    if resource is not None:
        resource.project_id = project_id
        resource.pts_node_id = pts_node_id
        resource.compiled_graph_hash = graph_hash
        resource.latest_published_version = int(row.published_version or 0) or resource.latest_published_version
        if set_active:
            resource.active_published_version = int(row.published_version or 0) or resource.active_published_version
        resource.published_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return row


def upsert_pts_resource_from_update(
    *,
    db: Session,
    pts_uuid: str,
    payload: Any,
) -> PtsResource:
    pts_graph = dict(getattr(payload, "pts_graph", None) or {})
    shell_node = dict(getattr(payload, "shell_node", None) or {})
    if shell_node:
        shell_node["node_kind"] = _normalize_pts_shell_node_kind(shell_node.get("node_kind"))

    explicit_ports_policy = getattr(payload, "ports_policy", None)
    normalized_ports_policy = (
        dict(explicit_ports_policy or {})
        if explicit_ports_policy is not None
        else _normalize_pts_ports_policy_from_graph(pts_graph=pts_graph, fallback_policy=None)
    )
    normalized_ports_policy = _sanitize_pts_ports_policy(
        ports_policy=normalized_ports_policy,
        pts_graph=pts_graph,
    )

    row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    if row is None:
        row = PtsResource(
            project_id=payload.project_id,
            pts_uuid=pts_uuid,
            name=payload.name,
            pts_node_id=payload.pts_node_id,
            latest_graph_hash=payload.latest_graph_hash,
            active_published_version=payload.active_published_version,
            pts_graph_json=pts_graph,
            ports_policy_json=normalized_ports_policy,
            shell_node_json=shell_node,
        )
        db.add(row)
    else:
        row.project_id = payload.project_id
        row.name = payload.name
        row.pts_node_id = payload.pts_node_id
        row.latest_graph_hash = payload.latest_graph_hash
        row.active_published_version = payload.active_published_version
        row.pts_graph_json = pts_graph
        row.ports_policy_json = normalized_ports_policy
        row.shell_node_json = shell_node

    latest_external = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == payload.project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .first()
    )
    latest_compile = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == payload.project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    if latest_compile is not None:
        row.compiled_graph_hash = str(latest_compile.graph_hash or "")
        row.latest_compile_version = int(latest_compile.compile_version or 0) or row.latest_compile_version
    if latest_external is not None:
        row.published_at = latest_external.updated_at or latest_external.created_at
        row.latest_published_version = int(latest_external.published_version or 0) or row.latest_published_version
        if row.active_published_version is None:
            row.active_published_version = row.latest_published_version
    db.commit()
    db.refresh(row)
    return row


# PTS helpers used by resource reads and main-graph projection.
def _normalize_pts_shell_node_kind(value: object) -> str:
    raw = str(value or "").strip()
    if raw == "pts":
        return "pts_module"
    return raw or "pts_module"

def _enrich_graph_flow_name_en(graph_json: dict, *, db: Session) -> None:
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return

    nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
    exchanges = graph_json.get("exchanges") if isinstance(graph_json.get("exchanges"), list) else []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        for bucket_name in ("inputs", "outputs"):
            ports = node.get(bucket_name) if isinstance(node.get(bucket_name), list) else []
            for port in ports:
                if not isinstance(port, dict):
                    continue
                flow_uuid = _safe_str(port.get("flowUuid"))
                if not flow_uuid:
                    continue
                flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
                if flow_name_en:
                    port["flow_name_en"] = flow_name_en

    for edge in exchanges:
        if not isinstance(edge, dict):
            continue
        flow_uuid = _safe_str(edge.get("flowUuid"))
        if not flow_uuid:
            continue
        flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
        if flow_name_en:
            edge["flow_name_en"] = flow_name_en

def _enrich_node_ports_flow_name_en(node_json: dict, *, db: Session) -> None:
    if not isinstance(node_json, dict):
        return
    flow_name_en_by_uuid = _flow_name_en_by_uuid_cached(db)
    if not flow_name_en_by_uuid:
        return
    for bucket_name in ("inputs", "outputs"):
        ports = node_json.get(bucket_name) if isinstance(node_json.get(bucket_name), list) else []
        for port in ports:
            if not isinstance(port, dict):
                continue
            flow_uuid = _safe_str(port.get("flowUuid"))
            if not flow_uuid:
                continue
            flow_name_en = flow_name_en_by_uuid.get(flow_uuid) or flow_name_en_by_uuid.get(flow_uuid.lower())
            if flow_name_en:
                port["flow_name_en"] = flow_name_en

def _build_projected_pts_ports_from_external(external: PtsExternalArtifact) -> tuple[list[FlowPort], list[FlowPort]]:
    payload = external.artifact_json if isinstance(external.artifact_json, dict) else {}
    frontend_ports = payload.get("frontend_ports") if isinstance(payload.get("frontend_ports"), dict) else None
    if not isinstance(frontend_ports, dict):
        frontend_ports = _build_frontend_ports_from_external_payload(payload)
    inputs_rows = frontend_ports.get("inputs") if isinstance(frontend_ports.get("inputs"), list) else []
    outputs_rows = frontend_ports.get("outputs") if isinstance(frontend_ports.get("outputs"), list) else []
    projected_inputs = [
        FlowPort.model_validate(
            _external_row_to_flow_port(
                {**item, "pts_uuid": str(external.pts_uuid or "")},
                idx=idx,
                direction="input",
                flow_type="technosphere",
            )
        )
        for idx, item in enumerate(inputs_rows, start=1)
        if isinstance(item, dict)
    ]
    projected_outputs = [
        FlowPort.model_validate(
            _external_row_to_flow_port(
                {**item, "pts_uuid": str(external.pts_uuid or "")},
                idx=idx,
                direction="output",
                flow_type="technosphere",
            )
        )
        for idx, item in enumerate(outputs_rows, start=1)
        if isinstance(item, dict)
    ]
    return projected_inputs, projected_outputs

# PTS main-graph projection helpers.
def _flow_port_identity_key(port: FlowPort | dict) -> tuple[str, str, str, str]:
    data = port.model_dump(mode="python", by_alias=True) if isinstance(port, FlowPort) else dict(port)
    return (
        str(data.get("flowUuid") or data.get("flow_uuid") or "").strip(),
        str(data.get("direction") or "").strip(),
        str(data.get("sourceProcessUuid") or data.get("source_process_uuid") or "").strip(),
        str(data.get("sourceNodeId") or data.get("source_node_id") or "").strip(),
    )

def _collect_connected_port_ids_for_pts_node(*, graph: HybridGraph, node_id: str) -> set[str]:
    connected: set[str] = set()
    for edge in graph.exchanges:
        if str(edge.fromNode or "") == node_id:
            port_id = _port_id_from_handle(edge.source_port_id or edge.sourceHandle, "out")
            if port_id:
                connected.add(port_id)
        if str(edge.toNode or "") == node_id:
            port_id = _port_id_from_handle(edge.target_port_id or edge.targetHandle, "in")
            if port_id:
                connected.add(port_id)
    return connected

def _overlay_pts_port_visibility(
    *,
    submitted_ports: list[FlowPort],
    projected_ports: list[FlowPort],
    connected_port_ids: set[str],
) -> list[FlowPort]:
    submitted_by_id: dict[str, FlowPort] = {}
    submitted_by_key: dict[tuple[str, str, str, str], FlowPort] = {}
    submitted_by_flow_direction: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    for port in submitted_ports:
        if str(port.id or "").strip():
            submitted_by_id[str(port.id)] = port
        submitted_by_key[_flow_port_identity_key(port)] = port
        flow_uuid = str(port.flowUuid or "").strip()
        direction = str(port.direction or "").strip()
        if flow_uuid and direction:
            submitted_by_flow_direction[(flow_uuid, direction)].append(port)

    projected_by_flow_direction: dict[tuple[str, str], list[FlowPort]] = defaultdict(list)
    for port in projected_ports:
        flow_uuid = str(port.flowUuid or "").strip()
        direction = str(port.direction or "").strip()
        if flow_uuid and direction:
            projected_by_flow_direction[(flow_uuid, direction)].append(port)

    merged: list[FlowPort] = []
    for port in projected_ports:
        submitted = submitted_by_id.get(str(port.id)) or submitted_by_key.get(_flow_port_identity_key(port))
        if submitted is None:
            flow_uuid = str(port.flowUuid or "").strip()
            direction = str(port.direction or "").strip()
            candidates = submitted_by_flow_direction.get((flow_uuid, direction), [])
            projected_candidates = projected_by_flow_direction.get((flow_uuid, direction), [])
            if len(candidates) == 1 and len(projected_candidates) == 1:
                submitted = candidates[0]
        show_on_node = bool(port.showOnNode)
        if submitted is not None:
            show_on_node = bool(submitted.showOnNode)
        connected_ids = {str(port.id or "").strip(), str(port.legacy_port_id or "").strip()}
        if submitted is not None:
            connected_ids.update({
                str(submitted.id or "").strip(),
                str(submitted.legacy_port_id or "").strip(),
            })
        if connected_ids.intersection(connected_port_ids):
            show_on_node = True
        # Use submitted port when matched to preserve original port IDs.
        # This prevents edge binding from breaking when projected ports have
        # auto-generated IDs (e.g. ptsout_<sha256>) that differ from the
        # IDs the graph edges reference.
        if submitted is not None:
            merged.append(submitted.model_copy(update={"showOnNode": show_on_node}))
        else:
            merged.append(port.model_copy(update={"showOnNode": show_on_node}))
    return merged
