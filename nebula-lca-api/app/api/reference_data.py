"""Stats API route extracted from ``app.main`` (Stage 6B-lite).

Units, unit groups, and flow import/delete routes remain in ``app.main``
and will be extracted in a future Stage 6B+ migration.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas import StatsResponse

_api_router = APIRouter(tags=["api-reference-data"])


@_api_router.get("/api/stats", response_model=StatsResponse)
def get_stats_api(db: Session = Depends(get_db)) -> StatsResponse:
    from ..models import FlowRecord, Model, ReferenceProcess
    from ..services import catalog_cache as _cc
    from ..services import project_versions as _pv

    cache_key = f"stats:v2:rev={_cc.cache_revision('stats')}"
    cached = _cc.cache_get(cache_key, ttl_seconds=3600.0)
    if isinstance(cached, StatsResponse):
        return cached

    projects = db.query(Model).count()
    flows_library_total = db.query(FlowRecord).count()
    process_library_total = db.query(ReferenceProcess).count()
    latest_rows = _cc._latest_graphs_with_project_meta(db)
    graph_process_total = 0
    graph_flow_total = 0
    for _, latest in latest_rows:
        process_count, flow_count = _pv._graph_process_and_flow_counts(
            latest.hybrid_graph_json if isinstance(latest.hybrid_graph_json, dict) else {}
        )
        graph_process_total += process_count
        graph_flow_total += flow_count

    result = StatsResponse(
        projects=projects,
        processes=process_library_total,
        flows=flows_library_total,
        flows_latest_graph=graph_flow_total,
        flows_library_total=flows_library_total,
        flow_library_total=flows_library_total,
        graph_processes=graph_process_total,
        graph_flows=graph_flow_total,
    )
    _cc.cache_set(cache_key, result)
    return result
