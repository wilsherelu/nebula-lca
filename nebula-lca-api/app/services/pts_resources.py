"""PTS resource management and API helpers.

Extracted from ``app.main`` for Stage 6C.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..schemas import (
    FlowPort,
    HybridGraph,
    HybridNode,
    PtsPackFinalizeRequest,
    PtsPublishRequest,
    PtsResourceOut,
    PtsUnpackPortBinding,
    normalize_same_flow_uuid_opposite_direction_ports,
)
from ..models import (
    PtsCompileArtifact,
    PtsDefinition,
    PtsExternalArtifact,
    PtsResource,
)
from ..pts_compile import compute_pts_graph_hash
from .graph_contract import _port_id_from_handle, is_graph_non_empty

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
    shell_node = dict(row.shell_node_json or {})
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
