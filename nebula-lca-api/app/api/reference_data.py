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
from ..models import UnitDefinition, UnitGroup
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
def list_units(unit_group: str | None = None, db: Session = Depends(get_db)) -> list[dict]:
    query = db.query(UnitDefinition)
    if unit_group:
        query = query.filter(UnitDefinition.unit_group == unit_group)
    rows = query.order_by(UnitDefinition.unit_group.asc(), UnitDefinition.factor_to_reference.asc()).all()
    groups = {
        row.name: row
        for row in db.query(UnitGroup).filter(UnitGroup.name.in_({r.unit_group for r in rows})).all()
    } if rows else {}
    return [
        {
            "unit_group": row.unit_group,
            "unit_name": row.unit_name,
            "factor_to_reference": row.factor_to_reference,
            "is_reference": row.is_reference,
            "source_uuid": getattr(groups.get(row.unit_group), "source_uuid", None),
            "source_version": getattr(groups.get(row.unit_group), "source_version", None),
            "source_package_version": getattr(groups.get(row.unit_group), "source_package_version", None),
            "source_file": getattr(groups.get(row.unit_group), "source_file", None),
        }
        for row in rows
    ]


@_api_router.get("/api/reference/tidas-policy")
@_base_router.get("/reference/tidas-policy")
def get_tidas_policy_reference() -> dict:
    from ..source_policy import get_tidas_allowed_unit_groups
    from ..tidas_reference import load_tidas_reference_seed

    seed = load_tidas_reference_seed()

    return {
        "source_package_version": str(seed.get("source_package_version") or ""),
        "allowed_unit_groups": get_tidas_allowed_unit_groups(),
    }


@_api_router.get("/api/reference/lcia-methods")
@_base_router.get("/reference/lcia-methods")
def list_lcia_methods(db: Session = Depends(get_db)) -> dict:
    from ..models import FlowRecord, LciProcessVector

    EF31_CANONICAL_METHODS = {"EF v3.1"}
    EF31_RUNTIME_METHODS = {"EF v3.1", "EF v3.1 no LT"}
    LEGACY_INDICATORS_AS_METHODS = {"Acidification", "Climate change"}

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

    def _read_method_counts(csv_path: Path) -> dict[str, int]:
        rows: dict[str, int] = {}
        if csv_path.exists():
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                sample = handle.read(2048)
                handle.seek(0)
                delimiter = ";" if sample.count(";") >= sample.count(",") else ","
                reader = csv.DictReader(handle, delimiter=delimiter)
                for row in reader:
                    method = str(row.get("method_en") or row.get("method") or "").strip()
                    if method:
                        rows[method] = rows.get(method, 0) + 1
        return rows

    def _active_runtime_available() -> bool:
        active_path = DEFAULT_EF31_RUNTIME_ROOT / ACTIVE_MANIFEST_NAME
        if not active_path.exists():
            return False
        try:
            active = json.loads(active_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        artifact_dir = Path(str(active.get("artifact_dir") or active.get("output_dir") or ""))
        if not artifact_dir.is_absolute():
            artifact_dir = DEFAULT_EF31_RUNTIME_ROOT / artifact_dir
        return (
            (artifact_dir / "flow_index.csv").exists()
            and (artifact_dir / "indicator_index.csv").exists()
            and (artifact_dir / "lcia_factors.csv").exists()
            and int(active.get("flows_count") or 0) > 0
            and int(active.get("factors_count") or 0) > 0
        )

    def _is_legacy_ef31_indicator_set(methods: set[str]) -> bool:
        """Detect if method names look like legacy EF3.1 indicator names rather than real method names.

        Legacy EF3.1 indicator_index.csv has each row's method_en = indicator name
        (e.g. 'Climate change', 'Acidification') instead of a consistent method name
        (e.g. 'EF v3.1').  We detect this by checking if top-level names overlap with
        known EF3.1 indicator categories.
        """
        if methods & EF31_RUNTIME_METHODS:
            return False
        return bool(LEGACY_INDICATORS_AS_METHODS & methods)

    def _collapse_to_canonical(methods: set[str]) -> tuple[set[str], str]:
        """Collapse legacy indicator-based methods to canonical EF v3.1.

        Returns (methods_set, source_label).
        """
        if _is_legacy_ef31_indicator_set(methods):
            return set(EF31_CANONICAL_METHODS), "legacy_ef3.1_indicator_index"
        return methods, "runtime_indicator_index"

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
            counts_for_path = _read_method_counts(path)
            methods_for_path = set(counts_for_path)
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
            method_counts = _read_method_counts(csv_path)
        else:
            base = Path(settings.nebula_lca_ef31_dir)
            csv_path = base if base.is_file() else base / "indicator_index.csv"
            method_counts = _read_method_counts(csv_path)
            methods = set(method_counts)
    else:
        base = Path(settings.nebula_lca_ef31_dir)
        csv_path = base if base.is_file() else base / "indicator_index.csv"
        method_counts = _read_method_counts(csv_path)
        methods = set(method_counts)

    methods, source_label = _collapse_to_canonical(methods)
    if source_label == "legacy_ef3.1_indicator_index":
        method_counts = {"EF v3.1": sum(method_counts.values())}
    if not methods:
        methods = set(EF31_CANONICAL_METHODS)
        method_counts = {"EF v3.1": 0}

    if hasattr(db, "query"):
        ecoinvent_elementary_flow_count = db.query(FlowRecord).filter(FlowRecord.source.like("ecoinvent%")).count()
        ecoinvent_lci_vector_count = db.query(LciProcessVector).count()
    else:
        ecoinvent_elementary_flow_count = 0
        ecoinvent_lci_vector_count = 0

    return {
        "default_method": "EF v3.1",
        "methods": sorted(methods),
        "method_indicator_counts": {method: int(method_counts.get(method, 0)) for method in sorted(methods)},
        "total_indicators": int(sum(method_counts.get(method, 0) for method in methods)),
        "source": str(csv_path) if csv_path.exists() else "",
        "source_label": source_label,
        "ecoinvent_lcia_runtime_available": _active_runtime_available(),
        "ecoinvent_elementary_flow_count": ecoinvent_elementary_flow_count,
        "ecoinvent_lci_vector_count": ecoinvent_lci_vector_count,
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
