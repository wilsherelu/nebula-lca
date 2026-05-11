"""Project/Version core helpers: build output, version creation, pruning.

Extracted from ``app.main`` for Stage 4A. No route logic; pure helpers for
project CRUD, version creation, and version pruning.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import func as sqla_func
from sqlalchemy.orm import Session

from ..config import settings
from ..models import FlowRecord, Model, ModelVersion, PtsCompileArtifact, PtsDefinition, PtsExternalArtifact
from ..schemas import ProjectOut

# ── graph_storage import (already exists, no circular) ────────────────────
from ..services.graph_storage import (
    compute_graph_hash_from_slim_graph,
    hydrate_graph_for_api,
    slim_graph_for_storage,
)

# ── graph_contract import (already exists, no circular) ───────────────────
from ..services.graph_contract import (
    _normalize_port_display_name,
    _truncate_text_preview,
    normalize_graph_product_flags,
    validate_graph_contract,
    validate_graph_flow_type_contract,
    validate_graph_port_names_against_flow_catalog,
)

# ── HybridGraph from schemas ─────────────────────────────────────────────
from ..schemas import HybridGraph

# ── Back-refs to main.py helpers ─────────────────────────────────────────
# Import these at module level inside a function to break circular dependency.
# Functions that need them will call _ensure_main_helpers() first.

_main_helpers_loaded = False


def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _ensure_main_helpers():
    """Ensure lazy main.py helpers are loaded into module globals."""
    global _main_helpers_loaded
    if _main_helpers_loaded:
        return
    from ..main import (
        _enrich_graph_flow_name_en,
        _normalize_graph_json_for_storage,
        _project_pts_external_ports_into_graph,
        _build_pts_validation_summary,
        _bind_pts_published_versions_for_graph,
        _sync_pts_resources_from_graph,
        _safe_str,
    )
    # fmt: on
    globals().update({
        "_enrich_graph_flow_name_en": _enrich_graph_flow_name_en,
        "_normalize_graph_json_for_storage": _normalize_graph_json_for_storage,
        "_project_pts_external_ports_into_graph": _project_pts_external_ports_into_graph,
        "_build_pts_validation_summary": _build_pts_validation_summary,
        "_bind_pts_published_versions_for_graph": _bind_pts_published_versions_for_graph,
        "_sync_pts_resources_from_graph": _sync_pts_resources_from_graph,
        "_safe_str": _safe_str,
    })
    _main_helpers_loaded = True


def _get_session_local():
    from ..database import SessionLocal as _sl
    return _sl


def _latest_version_by_project_id(db: Session) -> dict[str, ModelVersion]:
    """Return ``{model_id: latest_version_row}`` for all models.

    Uses ``ORDER BY model_id, version DESC`` and picks the first row per
    model_id (which is the latest version because of the sort).
    """
    rows = db.query(ModelVersion).order_by(
        ModelVersion.model_id.asc(),
        ModelVersion.version.desc(),
        ModelVersion.created_at.desc(),
    ).all()
    latest_by_project: dict[str, ModelVersion] = {}
    for row in rows:
        pid = str(row.model_id)
        if pid not in latest_by_project:
            latest_by_project[pid] = row
    return latest_by_project


def _build_project_out(model: Model, latest: ModelVersion | None = None) -> ProjectOut:
    """Build a ``ProjectOut`` response for a single project.

    Enriches with process/flow counts and flow-name sync state when a
    latest version is available.
    """
    _ensure_main_helpers()
    SessionLocal = _get_session_local()

    process_count = 0
    flow_count = 0
    flow_name_sync_needed = False
    outdated_flow_refs_count = 0
    outdated_flow_ref_examples: list[dict] = []

    if latest is not None:
        graph_json = latest.hybrid_graph_json if isinstance(latest.hybrid_graph_json, dict) else {}
        process_count, flow_count = _graph_process_and_flow_counts(graph_json)
        tmp_db = SessionLocal()
        try:
            outdated_flow_refs_count, outdated_flow_ref_examples = _detect_project_flow_name_outdated_refs(
                db=tmp_db,
                graph_json=graph_json,
            )
        finally:
            tmp_db.close()
        flow_name_sync_needed = outdated_flow_refs_count > 0

    return ProjectOut(
        project_id=model.id,
        name=model.name,
        reference_product=_safe_str(model.reference_product),
        functional_unit=_safe_str(model.functional_unit),
        system_boundary=_safe_str(model.system_boundary),
        time_representativeness=_safe_str(model.time_representativeness),
        geography=_safe_str(model.geography),
        description=_safe_str(model.description),
        status=_safe_str(model.status) or "active",
        process_count=process_count,
        flow_count=flow_count,
        created_at=model.created_at,
        updated_at=model.updated_at,
        latest_version=latest.version if latest else None,
        latest_version_created_at=latest.created_at if latest else None,
        flow_name_sync_needed=flow_name_sync_needed,
        outdated_flow_refs_count=outdated_flow_refs_count,
        outdated_flow_ref_examples=outdated_flow_ref_examples[:5],
    )


def _resolve_model_version_graph_hash(row: ModelVersion, *, assign_if_missing: bool = False) -> str:
    """Return the graph hash for a model version row.

    If the hash is missing and ``assign_if_missing`` is ``True``, computes it
    from the slimmed hybrid graph and writes it back to the row.
    """
    _ensure_main_helpers()
    resolved = str(row.graph_hash or "").strip()
    if resolved:
        return resolved
    source = row.hybrid_graph_json if isinstance(row.hybrid_graph_json, dict) else {}
    normalized = _normalize_graph_json_for_storage(source)
    slim = slim_graph_for_storage(normalized)
    resolved = compute_graph_hash_from_slim_graph(slim)
    if assign_if_missing:
        row.graph_hash = resolved
    return resolved


def _create_project_version_from_graph_json(*, db: Session, project_id: str, graph_json: dict) -> ModelVersion:
    """Create a new model version from a graph JSON payload.

    Runs graph validation, normalisation, slimming and hash computation
    before persisting. Also handles PTS publication binding and resource sync.

    Returns the newly created ``ModelVersion`` instance (not committed yet).
    """
    _ensure_main_helpers()

    graph = HybridGraph.model_validate(graph_json)
    normalize_graph_product_flags(graph)
    validate_graph_contract(graph, require_non_empty=True, allow_pts_nodes=True)
    validate_graph_flow_type_contract(graph, db=db, stage="import_model")
    validate_graph_port_names_against_flow_catalog(graph, db=db, stage="import_model")
    _bind_pts_published_versions_for_graph(db=db, project_id=project_id, graph=graph)

    latest_version = db.query(sqla_func.max(ModelVersion.version)).filter(
        ModelVersion.model_id == project_id
    ).scalar()
    next_version = (latest_version or 0) + 1

    persisted_graph_json = graph.model_dump(mode="python")
    _enrich_graph_flow_name_en(persisted_graph_json, db=db)
    normalized_graph_json = _normalize_graph_json_for_storage(persisted_graph_json)
    slim_graph_json = slim_graph_for_storage(normalized_graph_json)

    version = ModelVersion(
        model_id=project_id,
        version=next_version,
        graph_hash=compute_graph_hash_from_slim_graph(slim_graph_json),
        hybrid_graph_json=slim_graph_json,
    )
    db.add(version)
    _sync_pts_resources_from_graph(db=db, project_id=project_id, graph=graph)
    return version


def _auto_prune_versions(*, db: Session, model_id: str) -> None:
    """Auto-prune model versions after a new one is saved.

    Keeps at most ``KEEP_LATEST_VERSIONS_PER_PROJECT`` (default 20) versions
    per model. Always preserves versions referenced by RunJobs, the current
    latest, or draft pointer.
    """
    keep = max(1, int(settings.keep_latest_versions_per_project))
    _prune_model_versions_retention(
        db=db,
        keep_latest=keep,
        dry_run=False,
        project_id=model_id,
        vacuum_after_cleanup=False,
    )


def _prune_model_versions_retention(
    *,
    db: Session,
    keep_latest: int,
    dry_run: bool,
    project_id: str | None = None,
    vacuum_after_cleanup: bool = False,
) -> dict:
    """Prune model versions to keep at most ``keep_latest`` per model.

    Returns a dict with pruning stats. Uses the same logic as the admin
    route but can be filtered to a single project.

    **Why:** Without pruning, old versions accumulate and bloat the database.
    **How to apply:** Call after saving a new version (via ``_auto_prune_versions``)
    or from the admin maintenance route with explicit ``keep_latest``.
    """
    if keep_latest < 1:
        raise ValueError("keep_latest must be >= 1")

    query = db.query(ModelVersion)
    if project_id:
        query = query.filter(ModelVersion.model_id == project_id)
    rows = query.order_by(ModelVersion.model_id.asc(), ModelVersion.version.desc(), ModelVersion.created_at.desc()).all()

    scanned_versions = 0
    scanned_projects = 0
    redundant_version_ids: list[str] = []
    redundant_examples: list[dict] = []
    current_project: str | None = None
    seen_per_project = 0

    for row in rows:
        scanned_versions += 1
        row_project_id = str(row.model_id)
        if row_project_id != current_project:
            current_project = row_project_id
            scanned_projects += 1
            seen_per_project = 0
        seen_per_project += 1
        if seen_per_project <= keep_latest:
            continue
        redundant_version_ids.append(str(row.id))
        if len(redundant_examples) < 200:
            redundant_examples.append(
                {
                    "model_version_id": str(row.id),
                    "project_id": row_project_id,
                    "version": int(row.version),
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                }
            )

    cleared_run_job_refs = 0
    deleted_versions = 0
    if not dry_run and redundant_version_ids:
        from ..models import RunJob as _RunJob

        for start in range(0, len(redundant_version_ids), 500):
            batch = redundant_version_ids[start : start + 500]
            cleared_run_job_refs += (
                db.query(_RunJob)
                .filter(_RunJob.model_version_id.in_(batch))
                .update({_RunJob.model_version_id: None}, synchronize_session=False)
            )
            deleted_versions += db.query(ModelVersion).filter(ModelVersion.id.in_(batch)).delete(synchronize_session=False)
        db.commit()
    elif not dry_run:
        db.commit()

    vacuum_result = {"executed": False, "reason": "disabled"}
    if not dry_run and vacuum_after_cleanup:
        from ..main import _run_sqlite_vacuum

        vacuum_result = _run_sqlite_vacuum()

    return {
        "keep_latest": keep_latest,
        "project_id": project_id,
        "dry_run": dry_run,
        "scanned_projects": scanned_projects,
        "scanned_versions": scanned_versions,
        "redundant_versions": len(redundant_version_ids),
        "deleted_versions": deleted_versions,
        "cleared_run_job_refs": cleared_run_job_refs,
        "redundant_model_version_ids": redundant_version_ids[:500],
        "redundant_examples": redundant_examples,
        "vacuum": vacuum_result,
    }


# ── Project sync / integrity helpers (Stage 4B) ──────────────────────────
# These helpers are project-scoped and belong in this module.
# Some depend on cached flow metadata and graph-mutation helpers that live
# in main.py (catalog_cache / flow sync logic). They are imported lazily
# via _ensure_main_helpers() to avoid circular imports.


def _replace_flow_display_name(current_name: str | None, standard_name: str | None) -> str:
    """Return ``standard_name`` (preserving ``@suffix`` from current)."""
    standard = _safe_str(standard_name) or ""
    current = _safe_str(current_name) or ""
    if not standard:
        return current
    if not current:
        return standard
    if "@" in current:
        _, suffix = current.split("@", 1)
        suffix = suffix.strip()
        return f"{standard}@{suffix}" if suffix else standard
    return standard


def _looks_like_uuid_text(value: str | None) -> bool:
    text = _safe_str(value).lower()
    if not text:
        return False
    try:
        uuid.UUID(text)
        return True
    except ValueError:
        return False


def _valid_catalog_flow_names(meta: tuple | None) -> set[str]:
    if not meta:
        return set()
    names: set[str] = set()
    for value in (meta[0] if len(meta) > 0 else None, meta[1] if len(meta) > 1 else None):
        name = _normalize_port_display_name(value)
        if name and not _looks_like_uuid_text(name):
            names.add(name)
    return names


def _sync_graph_flow_names(graph_json: dict, *, updated_flow_names: dict[str, str]) -> tuple[int, int]:
    """Update flow display names in *graph_json* in-place.

    Returns ``(changed_port_count, changed_edge_count)``.
    """
    changed_port_count = 0
    changed_edge_count = 0
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
                standard_name = updated_flow_names.get(flow_uuid) or updated_flow_names.get(flow_uuid.lower())
                if not standard_name or _looks_like_uuid_text(standard_name):
                    continue
                new_name = _replace_flow_display_name(port.get("name"), standard_name)
                if _safe_str(port.get("name")) != new_name:
                    port["name"] = new_name
                    changed_port_count += 1

    for edge in exchanges:
        if not isinstance(edge, dict):
            continue
        flow_uuid = _safe_str(edge.get("flowUuid"))
        standard_name = updated_flow_names.get(flow_uuid) or updated_flow_names.get(flow_uuid.lower())
        if not standard_name or _looks_like_uuid_text(standard_name):
            continue
        new_name = _replace_flow_display_name(edge.get("flowName"), standard_name)
        if _safe_str(edge.get("flowName")) != new_name:
            edge["flowName"] = new_name
            changed_edge_count += 1

    return changed_port_count, changed_edge_count


def _graph_process_and_flow_counts(graph_json: dict | None) -> tuple[int, int]:
    """Count processes (nodes) and distinct flows in a graph JSON payload."""
    if not isinstance(graph_json, dict):
        return 0, 0
    nodes = graph_json.get("nodes")
    process_count = len(nodes) if isinstance(nodes, list) else 0
    flow_keys: set[str] = set()
    for node in nodes if isinstance(nodes, list) else []:
        if not isinstance(node, dict):
            continue
        for bucket in ("inputs", "outputs"):
            ports = node.get(bucket)
            if not isinstance(ports, list):
                continue
            for port in ports:
                if not isinstance(port, dict):
                    continue
                flow_uuid = _safe_str(port.get("flowUuid") or port.get("flow_uuid"))
                if flow_uuid:
                    flow_keys.add(flow_uuid)
    return process_count, len(flow_keys)


def _detect_project_flow_name_outdated_refs(
    *,
    db: Session,
    graph_json: dict | None,
) -> tuple[int, list[dict]]:
    """Detect node/port flow-name references that no longer match the catalog.

    Returns ``(count, evidence_list)``. Evidence items contain node/port
    identifiers plus the expected vs actual flow names.
    """
    _ensure_main_helpers()
    if not isinstance(graph_json, dict):
        return 0, []
    flow_meta = _get_flow_meta_by_uuid_cached(db)
    evidence: list[dict] = []
    nodes = graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []
    for node in nodes:
        if not isinstance(node, dict) or str(node.get("node_kind") or "") == "pts_module":
            continue
        for bucket_name in ("inputs", "outputs"):
            ports = node.get(bucket_name) if isinstance(node.get(bucket_name), list) else []
            for port in ports:
                if not isinstance(port, dict):
                    continue
                actual_name = _normalize_port_display_name(port.get("name"))
                if "@" in actual_name:
                    continue
                flow_uuid = _safe_str(port.get("flowUuid"))
                if not flow_uuid:
                    continue
                meta = flow_meta.get(flow_uuid) or flow_meta.get(flow_uuid.lower())
                if not meta:
                    continue
                expected_names = _valid_catalog_flow_names(meta)
                if not expected_names or not actual_name or actual_name in expected_names:
                    continue
                evidence.append(
                    {
                        "node_id": _safe_str(node.get("id")),
                        "node_name": _safe_str(node.get("name")),
                        "port_id": _safe_str(port.get("id")),
                        "bucket": bucket_name,
                        "flow_uuid": flow_uuid,
                        "expected_flow_name": _truncate_text_preview(" / ".join(sorted(expected_names))),
                        "actual_port_name": _truncate_text_preview(actual_name),
                    }
                )
                if len(evidence) >= 20:
                    return len(evidence), evidence
    return len(evidence), evidence


def _build_flow_sync_state_for_graph_json(*, db: Session, graph_json: dict) -> tuple[bool, int, list[dict]]:
    """Build flow-name sync state from a graph JSON payload.

    Returns ``(sync_needed, outdated_count, examples)``.
    """
    outdated_flow_refs_count, outdated_flow_ref_examples = _detect_project_flow_name_outdated_refs(
        db=db,
        graph_json=graph_json,
    )
    return outdated_flow_refs_count > 0, outdated_flow_refs_count, outdated_flow_ref_examples[:5]


def _sync_project_latest_version_flow_names(
    *,
    db: Session,
    project_id: str,
) -> tuple[int | None, int, int, int, int, int]:
    """Sync flow names in the latest version of a project.

    Updates the latest ``ModelVersion.hybrid_graph_json`` in-place and
    clears stale PTS compile/external/definition artifacts.

    Returns ``(latest_version, synced_port_count, synced_edge_count,
    cleared_pts_compile_count, cleared_pts_external_count, cleared_pts_definition_count)``.
    """
    _ensure_main_helpers()
    latest_row = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == project_id)
        .order_by(ModelVersion.version.desc(), ModelVersion.created_at.desc())
        .first()
    )
    if latest_row is None or not isinstance(latest_row.hybrid_graph_json, dict):
        return None, 0, 0, 0, 0, 0

    flow_meta = _get_flow_meta_by_uuid_cached(db)
    updated_flow_names = {
        flow_uuid: str(meta[0] or "")
        for flow_uuid, meta in flow_meta.items()
        if flow_uuid and meta and str(meta[0] or "") and not _looks_like_uuid_text(str(meta[0] or ""))
    }
    cloned_graph = json.loads(json.dumps(latest_row.hybrid_graph_json, ensure_ascii=False))
    synced_port_count, synced_edge_count = _sync_graph_flow_names(cloned_graph, updated_flow_names=updated_flow_names)
    if synced_port_count == 0 and synced_edge_count == 0:
        return latest_row.version, 0, 0, 0, 0, 0

    latest_row.hybrid_graph_json = cloned_graph
    latest_row.graph_hash = _compute_graph_hash_from_graph_json(cloned_graph)
    db.query(Model).filter(Model.id == project_id).update({Model.updated_at: datetime.utcnow()}, synchronize_session=False)

    cleared_pts_compile_count = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == project_id)
        .delete(synchronize_session=False)
    )
    cleared_pts_external_count = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == project_id)
        .delete(synchronize_session=False)
    )
    cleared_pts_definition_count = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == project_id)
        .delete(synchronize_session=False)
    )
    return (
        latest_row.version,
        synced_port_count,
        synced_edge_count,
        cleared_pts_compile_count,
        cleared_pts_external_count,
        cleared_pts_definition_count,
    )


def _build_project_integrity_summary(
    *,
    pts_validation: Any,
    flow_name_sync_needed: bool,
    outdated_flow_refs_count: int,
    outdated_flow_ref_examples: list[dict],
) -> Any:
    """Build a ``ProjectIntegritySummary`` from validation and sync state.

    **Why:** Both ``ModelVersionOut`` and ``RepairProjectIntegrityResponse``
    need the same integrity summary. Keeping it in the service layer avoids
    code duplication across routes.
    **How to apply:** Call from routes that need a project integrity summary.
    """
    _ensure_main_helpers()
    from ..schemas import ProjectIntegrityIssue, ProjectIntegritySummary

    issues: list[ProjectIntegrityIssue] = []  # type: ignore[assignment]
    for item in pts_validation.items:
        issues.append(
            ProjectIntegrityIssue(
                kind="pts_publication",
                severity="error",
                code="PTS_PUBLICATION_INVALID",
                message=f"PTS {item.pts_uuid} published state is invalid for save/run.",
                auto_repairable=item.auto_repairable,
                details=item.model_dump(mode="python"),
            )
        )
    if flow_name_sync_needed:
        issues.append(
            ProjectIntegrityIssue(
                kind="flow_name_sync",
                severity="warning",
                code="FLOW_NAME_SYNC_NEEDED",
                message="Some node/port flow names are outdated compared with the flow catalog.",
                auto_repairable=outdated_flow_refs_count > 0,
                details={
                    "outdated_count": int(outdated_flow_refs_count or 0),
                    "examples": list(outdated_flow_ref_examples or []),
                },
            )
        )
    return ProjectIntegritySummary(
        ok=not issues,
        issue_count=len(issues),
        auto_repairable=any(issue.auto_repairable for issue in issues),
        issues=issues,
    )


# ── Lazy helpers for cached flow metadata ─────────────────────────────────
# These mirror the cached helpers from main.py but are scoped to project
# version operations. Loaded lazily to avoid circular imports.

_flow_meta_cache: dict[str, tuple] = {}
_flow_meta_cache_key: str | None = None
_flow_name_en_cache: dict[str, str] = {}
_flow_name_en_cache_key: str | None = None
_FLOW_META_TTL: float = 30.0


def _get_flow_meta_by_uuid_cached(db: Session) -> dict[str, tuple[str | None, str | None, str | None, str | None]]:
    """Flow metadata by UUID, cached per session (mirrors main.py logic)."""
    from .catalog_cache import cache_get, cache_revision, cache_set

    cache_key = f"flow_meta_by_uuid:v1:rev={cache_revision('flow_meta')}"
    cached = cache_get(cache_key, ttl_seconds=_FLOW_META_TTL)
    if isinstance(cached, dict):
        return cached
    value = {
        str(row.flow_uuid).strip(): (
            _safe_str(row.flow_name),
            _safe_str(row.flow_name_en),
            _safe_str(row.flow_type),
            _safe_str(row.unit_group),
        )
        for row in db.query(
            FlowRecord.flow_uuid,
            FlowRecord.flow_name,
            FlowRecord.flow_name_en,
            FlowRecord.flow_type,
            FlowRecord.unit_group,
        ).all()
    }
    cache_set(cache_key, value)
    return value


def _get_flow_name_en_by_uuid_cached(db: Session) -> dict[str, str]:
    """Flow name-en by UUID, cached per session."""
    from .catalog_cache import cache_get, cache_revision, cache_set

    cache_key = f"flow_name_en_by_uuid:v1:rev={cache_revision('flow_meta')}"
    cached = cache_get(cache_key, ttl_seconds=_FLOW_META_TTL)
    if isinstance(cached, dict):
        return cached
    value = {
        str(row.flow_uuid).strip(): _safe_str(row.flow_name_en)
        for row in db.query(FlowRecord.flow_uuid, FlowRecord.flow_name_en).all()
        if str(row.flow_uuid or "").strip() and _safe_str(row.flow_name_en)
    }
    cache_set(cache_key, value)
    return value
