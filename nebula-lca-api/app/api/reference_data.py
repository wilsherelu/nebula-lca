"""Reference data (units, unit conversion) and stats API routes.

Extracted from ``app.main`` for Stage 6B-lite (stats) and Stage 6B+ (units).
Uses ``APIRouter`` pattern; the router is included in main.py via
``app.include_router()``.

URL paths preserved to match the original ``@app.xxx`` registrations.

Unit groups and elementary/intermediate flow import/delete routes remain
in ``app.main`` — they had no ``@app.xxx`` decorators and were never
registered routes. They will be handled in a future migration if ever needed.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..config import settings
from ..database import get_db
from ..models import UnitDefinition
from ..services.ef31_runtime_csv import ACTIVE_MANIFEST_NAME, DEFAULT_EF31_RUNTIME_ROOT
from ..schemas import (
    StatsResponse,
    UnitConvertRequest,
    UnitConvertResponse,
    UnitDefinitionOut,
)
from ..ingest import convert_unit_value

# -- Router ----------------------------------------------------------------

_base_router = APIRouter(tags=["reference-data"])
_api_router = APIRouter(tags=["api-reference-data"])


# Stats ----------------------------------------------------------------

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


# Units ----------------------------------------------------------------

@_api_router.get("/api/reference/units", response_model=list[UnitDefinitionOut])
@_base_router.get("/reference/units", response_model=list[UnitDefinitionOut])
def list_units(unit_group: str | None = None, db: Session = Depends(get_db)) -> list[UnitDefinition]:
    query = db.query(UnitDefinition)
    if unit_group:
        query = query.filter(UnitDefinition.unit_group == unit_group)
    return query.order_by(UnitDefinition.unit_group.asc(), UnitDefinition.factor_to_reference.asc()).all()


@_api_router.get("/api/reference/lcia-methods")
@_base_router.get("/reference/lcia-methods")
def list_lcia_methods() -> dict:
    def _read_runtime_summary(csv_path: Path) -> dict:
        manifest_path = csv_path.parent / "manifest.json"
        summary_path = csv_path.parent / "runtime_summary.json"
        path = manifest_path if manifest_path.exists() else summary_path
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def _read_methods(csv_path: Path) -> set[str]:
        rows: set[str] = set()
        if csv_path.exists():
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                sample = handle.read(2048)
                handle.seek(0)
                delimiter = ";" if sample.count(";") >= sample.count(",") else ","
                reader = csv.DictReader(handle, delimiter=delimiter)
                for row in reader:
                    method = str(row.get("method_en") or row.get("method") or "").strip()
                    if method:
                        rows.add(method)
        return rows

    runtime_root = DEFAULT_EF31_RUNTIME_ROOT
    runtime_candidates = []
    active_manifest = runtime_root / ACTIVE_MANIFEST_NAME
    if active_manifest.exists():
        try:
            active = json.loads(active_manifest.read_text(encoding="utf-8"))
            active_dir = Path(str(active.get("artifact_dir") or active.get("output_dir") or ""))
            active_indicator = active_dir / "indicator_index.csv"
            if active_indicator.exists():
                runtime_candidates.append(active_indicator)
        except (OSError, json.JSONDecodeError):
            pass
    if runtime_root.exists():
        runtime_candidates.extend([
            candidate / "indicator_index.csv"
            for candidate in runtime_root.iterdir()
            if (candidate / "indicator_index.csv").exists()
        ])
    if runtime_candidates:
        scored = []
        for path in runtime_candidates:
            methods_for_path = _read_methods(path)
            summary = _read_runtime_summary(path)
            indicators_count = int(summary.get("indicators_count") or len(methods_for_path))
            flows_count = int(summary.get("flows_count") or 0)
            factors_count = int(summary.get("factors_count") or 0)
            if indicators_count < 10 or flows_count < 100:
                continue
            scored.append((path, methods_for_path, indicators_count, flows_count, factors_count))
        if scored:
            csv_path, methods, _, _, _ = max(
                scored,
                key=lambda item: (item[2], item[3], item[4], item[0].stat().st_mtime),
            )
        else:
            base = Path(settings.nebula_lca_ef31_dir)
            csv_path = base if base.is_file() else base / "indicator_index.csv"
            methods = _read_methods(csv_path)
    else:
        base = Path(settings.nebula_lca_ef31_dir)
        csv_path = base if base.is_file() else base / "indicator_index.csv"
        methods = _read_methods(csv_path)
    if not methods:
        methods.update(["EF v3.1", "EF v3.1 no LT"])
    return {
        "default_method": "EF v3.1",
        "methods": sorted(methods),
        "source": str(csv_path) if csv_path.exists() else "",
    }


# Unit Conversion ---------------------------------------------------------

@_api_router.post("/api/units/convert", response_model=UnitConvertResponse)
@_base_router.post("/units/convert", response_model=UnitConvertResponse)
def convert_units(payload: UnitConvertRequest, db: Session = Depends(get_db)) -> UnitConvertResponse:
    try:
        result = convert_unit_value(
            db,
            value=payload.value,
            from_unit=payload.from_unit,
            to_unit=payload.to_unit,
            unit_group=payload.unit_group,
        )
    except ValueError as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return UnitConvertResponse(**result)
