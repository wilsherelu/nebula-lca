"""PTS (Product Tree Structure) operations: compile, sync, bind, repair, validation.

Extracted from ``app.main`` for the modularisation cleanup.  These functions
depend on ``pts_resources`` (compiled PTS logic) but NOT on ``main.py``.

Public API
----------
* ``_compile_pts_on_save_if_needed``
* ``_sync_pts_resources_from_graph``
* ``_bind_pts_published_versions_for_graph``
* ``_enrich_graph_flow_name_en``
* ``_build_pts_validation_summary``
* ``_project_pts_external_ports_into_graph``
* ``_canonicalize_pts_nodes_for_main_graph_save``
* ``_repair_pts_publication_from_resource``
* ``_is_pts_publication_auto_repairable``
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from ..models import PtsCompileArtifact, PtsDefinition, PtsExternalArtifact, PtsResource
from ..schemas import HybridGraph, PtsValidationItem, PtsValidationSummary
from . import pts_resources as _pr


# ── Utility ──────────────────────────────────────────────────────────────

def _safe_str(value: object) -> str | None:
    """Return stripped string or None."""
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


# ── PTS compile on save ─────────────────────────────────────────────────

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


# ── PTS resource sync ───────────────────────────────────────────────────

def _sync_pts_resources_from_graph(*, db: Session, project_id: str, graph: HybridGraph) -> list[str]:
    """Sync PTS resources from the graph.

    Iterates all nodes, finds ``pts_module`` nodes, and calls
    ``_upsert_pts_resource_from_graph`` for each.  Returns list of synced PTS UUIDs.
    """
    synced: list[str] = []
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        resource = _pr._upsert_pts_resource_from_graph(db=db, project_id=project_id, graph=graph, pts_node=node)
        if resource is not None:
            synced.append(str(resource.pts_uuid))
    return synced


# ── PTS bind published versions ─────────────────────────────────────────

def _bind_pts_published_versions_for_graph(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    """Bind published version info back onto graph nodes.

    For each ``pts_module``, loads the active external artifact and sets
    ``pts_published_version`` and ``pts_published_artifact_id`` (or clears
    them if no artifact exists).
    """
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        external = _pr._load_pts_active_external_artifact(
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


# ── Flow name enrichment ────────────────────────────────────────────────

def _enrich_graph_flow_name_en(graph_json: dict, *, db: Session) -> None:
    """Enrich graph JSON nodes and edges with ``flow_name_en``.

    Looks up flow UUIDs in a cached catalog. Modifies the graph JSON in place.
    """
    flow_name_en_by_uuid = _pr._flow_name_en_by_uuid_cached(db)
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


# ── PTS validation summary ──────────────────────────────────────────────

def _build_pts_validation_summary(*, db: Session, project_id: str, graph: HybridGraph) -> PtsValidationSummary:
    """Build a ``PtsValidationSummary`` by iterating all ``pts_module`` nodes.

    Reports reasons like ``pts_resource_missing``,
    ``published_resource_missing_artifact``, or ``pts_resource_unpublished``.
    """
    items: list[PtsValidationItem] = []
    for node in graph.nodes:
        if node.node_kind != "pts_module":
            continue
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        resource = (
            db.query(PtsResource)
            .filter(PtsResource.project_id == project_id, PtsResource.pts_uuid == pts_uuid)
            .first()
        )
        external = _pr._load_pts_active_external_artifact(db=db, project_id=project_id, pts_uuid=pts_uuid)
        if external is not None:
            continue
        reason = "pts_resource_missing"
        if resource is not None and resource.active_published_version is not None:
            reason = "published_resource_missing_artifact"
        elif resource is not None:
            reason = "pts_resource_unpublished"
        auto_repairable = _is_pts_publication_auto_repairable(resource=resource, external=external)
        items.append(
            PtsValidationItem(
                node_id=str(node.id or ""),
                node_name=str(node.name or "") or None,
                pts_uuid=pts_uuid,
                reason=reason,
                has_resource=resource is not None,
                active_published_version=(
                    int(resource.active_published_version)
                    if resource is not None and resource.active_published_version is not None
                    else None
                ),
                latest_published_version=(
                    int(resource.latest_published_version)
                    if resource is not None and resource.latest_published_version is not None
                    else None
                ),
                has_active_artifact=False,
                auto_repairable=auto_repairable,
            )
        )
    return PtsValidationSummary(
        ok=not items,
        invalid_count=len(items),
        auto_repairable=any(item.auto_repairable for item in items),
        items=items,
    )


# ── Project external ports onto graph ───────────────────────────────────

def _project_pts_external_ports_into_graph(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    """Project external PTS artifacts onto graph nodes.

    Sets published version/artifact ID, overlays port visibility using
    submitted vs. projected ports from the external artifact, and clears
    emissions. Called before main-graph save.
    """
    pts_nodes = [node for node in graph.nodes if node.node_kind == "pts_module"]
    if not pts_nodes:
        return
    for node in pts_nodes:
        pts_uuid = str(node.pts_uuid or node.process_uuid or node.id).strip()
        if not pts_uuid:
            continue
        external = _pr._load_pts_active_external_artifact(db=db, project_id=project_id, pts_uuid=pts_uuid)
        if external is None:
            continue
        node.pts_published_version = (
            int(external.published_version)
            if external.published_version is not None
            else node.pts_published_version
        )
        node.pts_published_artifact_id = str(external.id)
        projected_inputs, projected_outputs = _pr._build_projected_pts_ports_from_external(external)
        connected_port_ids = _pr._collect_connected_port_ids_for_pts_node(graph=graph, node_id=str(node.id))
        node.inputs = _pr._overlay_pts_port_visibility(
            submitted_ports=list(node.inputs),
            projected_ports=projected_inputs,
            connected_port_ids=connected_port_ids,
        )
        node.outputs = _pr._overlay_pts_port_visibility(
            submitted_ports=list(node.outputs),
            projected_ports=projected_outputs,
            connected_port_ids=connected_port_ids,
        )
        node.emissions = []


# ── Canonicalize PTS nodes ──────────────────────────────────────────────

def _canonicalize_pts_nodes_for_main_graph_save(*, db: Session, project_id: str, graph: HybridGraph) -> None:
    """Thin wrapper: canonicalize PTS nodes before main-graph save.

    If any ``pts_module`` nodes exist, delegates to
    ``_project_pts_external_ports_into_graph``.
    """
    if any(node.node_kind == "pts_module" for node in graph.nodes):
        _project_pts_external_ports_into_graph(db=db, project_id=project_id, graph=graph)


# ── Auto-repairable check ───────────────────────────────────────────────

def _is_pts_publication_auto_repairable(*, resource: PtsResource | None, external: PtsExternalArtifact | None) -> bool:
    """Return whether a missing PTS publication can be auto-repaired."""
    if external is not None or resource is None:
        return False
    if not isinstance(resource.pts_graph_json, dict) or not resource.pts_graph_json.get("nodes"):
        return False
    if not isinstance(resource.shell_node_json, dict) or not resource.shell_node_json:
        return False
    return True


# ── Repair PTS publication from resource ─────────────────────────────────

def _repair_pts_publication_from_resource(*, db: Session, resource: PtsResource) -> tuple[bool, str]:
    """Re-publish a PTS resource that is missing its published artifact.

    Re-parses the resource's compile graph, upserts compile artifact and
    definition, builds external payload (with flow name enrichment), and
    upserts the external artifact.

    Returns ``(success: bool, detail: str)``.
    """
    graph = _pr._build_compile_graph_from_pts_resource(resource)
    if graph is None:
        return False, "pts_resource_graph_invalid"

    shell_node = dict(resource.shell_node_json or {})
    pts_node_id = str(resource.pts_node_id or shell_node.get("id") or "").strip()
    if not pts_node_id:
        return False, "pts_node_id_missing"

    try:
        compile_row, _cached = _pr.upsert_pts_compile_artifact(
            db=db,
            project_id=str(resource.project_id),
            pts_node_id=pts_node_id,
            force_recompile=True,
            graph=graph,
        )
        definition = _pr.extract_pts_definition(
            graph=graph,
            pts_node_id=pts_node_id,
            graph_hash=str(compile_row.graph_hash or ""),
        )
        definition_row = _pr.upsert_pts_definition(db=db, project_id=str(resource.project_id), definition=definition)
        external_payload = _pr.build_pts_external_payload(
            project_id=str(resource.project_id),
            pts_uuid=str(resource.pts_uuid),
            definition=definition_row.definition_json or definition,
            compile_row=compile_row,
        )
        _pr._enrich_pts_external_payload_flow_name_en(external_payload, db=db)
        external = _pr.upsert_pts_external_artifact(
            db=db,
            project_id=str(resource.project_id),
            pts_uuid=str(resource.pts_uuid),
            pts_node_id=pts_node_id,
            graph_hash=str(compile_row.graph_hash or ""),
            payload=external_payload,
            source_compile_id=str(compile_row.id),
            source_compile_version=(
                int(compile_row.compile_version)
                if compile_row.compile_version is not None
                else None
            ),
            set_active=True,
        )
        repaired_resource = _pr._upsert_pts_resource_from_definition(
            db=db,
            definition_row=definition_row,
            compile_row=compile_row,
            external_row=external,
        )
        repaired_resource.shell_node_json = _pr._build_pts_shell_snapshot_from_external(
            row=repaired_resource,
            external=external,
        )
        repaired_resource.active_published_version = int(external.published_version or 0) or repaired_resource.active_published_version
        db.commit()
        return True, "pts_publication_repaired"
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        return False, f"pts_publication_repair_failed: {exc}"
