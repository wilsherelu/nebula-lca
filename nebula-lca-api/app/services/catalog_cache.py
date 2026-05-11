"""Catalog helpers: FTS5 search, used_in map, category segments, caching.

Extracted from ``app.main`` for Stage 3. No route logic; pure helpers for
flow / process catalog queries and management page features.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from hashlib import sha256
from typing import Any

from sqlalchemy import func as sqla_func
from sqlalchemy.orm import Session

from ..models import FlowRecord, Model, ModelVersion, ReferenceProcess


# ── Inline helper (was in main.py) ──────────────────────────────────────


def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


# ── FTS5 search ─────────────────────────────────────────────────────────


def fts5_flow_search_query(db: Session, normalized_search: str):
    """Return (query, used_fts) tuple for flow_catalog search.

    When FTS5 is available and search is non-empty, returns an FTS5-filtered
    query (filtered by flow_uuid from FTS results) with ``used_fts=True``.
    Otherwise returns unfiltered query with ``used_fts=False`` so the caller
    falls back to LIKE.
    """
    if not normalized_search:
        return (db.query(FlowRecord), False)

    fts_available = False
    try:
        conn = db.connection().connection
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='flow_catalog_fts' AND sql LIKE '%fts5%'"
        )
        fts_available = cursor.fetchone() is not None
    except Exception:
        pass

    if not fts_available:
        return (db.query(FlowRecord), False)

    safe_query = normalized_search.replace("'", "''")
    try:
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        cursor.execute(
            "SELECT flow_uuid FROM flow_catalog_fts WHERE flow_catalog_fts MATCH ?",
            (safe_query,),
        )
        matched_uuids = {row[0] for row in cursor.fetchall()}
        if matched_uuids:
            return (
                db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(matched_uuids)),
                True,
            )
        else:
            return (db.query(FlowRecord).filter(FlowRecord.flow_uuid == ""), True)
    except Exception:
        return (db.query(FlowRecord), False)


# ── Latest graphs helper ────────────────────────────────────────────────


def _latest_graphs_with_project_meta(db: Session) -> list[tuple[Model, ModelVersion]]:
    """Return (project, latest_version) for each project."""
    latest_subq = (
        db.query(
            ModelVersion.model_id,
            sqla_func.max(ModelVersion.version).label("max_version"),
        )
        .group_by(ModelVersion.model_id)
        .subquery()
    )
    rows = (
        db.query(Model, ModelVersion)
        .select_from(Model)
        .join(
            latest_subq,
            Model.id == latest_subq.c.model_id,
        )
        .join(
            ModelVersion,
            (ModelVersion.model_id == Model.id)
            & (ModelVersion.version == latest_subq.c.max_version),
        )
        .all()
    )
    return rows


# ── Flow used_in_processes map ──────────────────────────────────────────


def build_flow_used_in_processes_map(db: Session) -> dict[str, int]:
    """Build flow_uuid → count of distinct process_ids map."""
    latest_rows = _latest_graphs_with_project_meta(db)
    used_by_flow: dict[str, set[str]] = defaultdict(set)
    for _, version in latest_rows:
        graph_json = version.hybrid_graph_json if isinstance(version.hybrid_graph_json, dict) else {}
        nodes = graph_json.get("nodes")
        if not isinstance(nodes, list):
            continue
        for node in nodes:
            if not isinstance(node, dict):
                continue
            process_key = _safe_str(node.get("process_uuid")) or _safe_str(node.get("id"))
            if not process_key:
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
                        used_by_flow[flow_uuid].add(process_key)
    return {flow_uuid: len(processes) for flow_uuid, processes in used_by_flow.items()}


def build_flow_used_in_processes_map_cached(db: Session) -> dict[str, int]:
    """Build flow->process usage map, cached on the session via a static attr."""
    cache_key = "_flow_used_in_processes_map"
    result = getattr(db, cache_key, None)
    if result is not None:
        return result
    result = build_flow_used_in_processes_map(db)
    setattr(db, cache_key, result)
    return result


# ── Category helpers ────────────────────────────────────────────────────


def first_category_segment(value: str | None) -> str | None:
    raw = _safe_str(value)
    if not raw:
        return None
    return _safe_str(raw.split(";", 1)[0])


# ── API-level cache (in-memory LRU dict) ────────────────────────────────

_api_cache: dict[str, tuple[float, object]] = {}
_api_cache_revisions: dict[str, int] = {}
_CACHE_TTL_SECONDS: float = 30.0


def cache_get(key: str, ttl_seconds: float = _CACHE_TTL_SECONDS) -> object | None:
    entry = _api_cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    if (datetime.now().timestamp() - ts) > ttl_seconds:
        _api_cache.pop(key, None)
        return None
    return value


def cache_set(key: str, value: object) -> None:
    _api_cache[key] = (datetime.now().timestamp(), value)


def cache_invalidate_prefix(prefix: str) -> int:
    removed = 0
    for key in list(_api_cache.keys()):
        if key.startswith(prefix):
            _api_cache.pop(key, None)
            removed += 1
    return removed


def cache_revision(domain: str) -> int:
    return int(_api_cache_revisions.get(domain, 0))


def cache_bump_revision(domain: str) -> int:
    next_value = cache_revision(domain) + 1
    _api_cache_revisions[domain] = next_value
    return next_value


# ── ETag helpers ────────────────────────────────────────────────────────


def build_etag_for_payload(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = sha256(canonical.encode("utf-8")).hexdigest()
    return f'"{digest}"'


def is_if_none_match_hit(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match:
        return False
    if if_none_match.strip() == "*":
        return True
    return etag in if_none_match


# ── Invalidation helpers ────────────────────────────────────────────────


def invalidate_management_caches(
    *,
    projects: bool = False,
    flows: bool = False,
    stats: bool = False,
    reference_processes: bool = False,
) -> None:
    if projects:
        cache_bump_revision("projects")
        cache_invalidate_prefix("projects:v1:")
    if flows:
        cache_bump_revision("flows")
        cache_bump_revision("flow_categories")
        cache_bump_revision("flow_meta")
        cache_invalidate_prefix("flows:v1:")
        cache_invalidate_prefix("flows:v2:")
        cache_invalidate_prefix("flow_categories:v1:")
        cache_invalidate_prefix("flow_uuid_set:v1")
        cache_invalidate_prefix("flow_meta_by_uuid:v1")
        cache_invalidate_prefix("flow_name_en_by_uuid:v1")
    if stats:
        cache_bump_revision("stats")
        cache_invalidate_prefix("stats:v2")
    if reference_processes:
        cache_bump_revision("processes")
        cache_bump_revision("reference_processes_catalog")
        cache_bump_revision("reference_process_report")
        cache_invalidate_prefix("processes:v1:")
        cache_invalidate_prefix("reference_processes_catalog:v1:")
        cache_invalidate_prefix("reference_process_report:v1:")
