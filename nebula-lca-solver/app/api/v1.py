from fastapi import APIRouter, HTTPException

import json
import os
from pathlib import Path
import time

from app.core.schema import (
    ComputeRequest,
    ComputeResponse,
    LciaPayload,
    LciaResponse,
    PtsCompilePayload,
    PtsCompileResponse,
)
from app.core.ef31_runtime_cache import GLOBAL_EF31_RUNTIME_CACHE
from app.core.lcia import compute_lcia
from app.core.matrix_builder import (
    _build_matrix,
    _canonical_ef31_indicator_info,
    _canonical_ef31_indicator_key,
    _to_float,
    build_matrices_from_snapshot,
)
from app.core.pts_compile import compile_pts_from_payload
from app.core.solver import solve_compute

router = APIRouter()


def _resolve_runtime_csv_dir(path: Path) -> Path:
    """Return the actual CSV directory, resolving active_manifest.json roots."""
    if path.is_file() and path.name == "active_manifest.json":
        manifest_path = path
    else:
        manifest_path = path / "active_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            artifact_dir = str(manifest.get("artifact_dir") or "").strip()
            if artifact_dir:
                resolved = Path(artifact_dir)
                if resolved.exists():
                    return resolved
        except Exception:
            pass
    return path


def _default_api_ef31_runtime_root() -> Path:
    workspace_root = Path(__file__).resolve().parents[3]
    return workspace_root / "nebula-lca-api" / "runtime" / "ef31"


def _resolve_ef31_runtime_dir() -> Path:
    configured = os.environ.get("NEBULA_LCA_EF31_DIR")
    candidates = [Path(configured)] if configured else []
    candidates.extend([
        _default_api_ef31_runtime_root(),
        Path(__file__).resolve().parents[2] / "data" / "EF3.1",
    ])
    for candidate in candidates:
        resolved = _resolve_runtime_csv_dir(candidate)
        if (resolved / "flow_index.csv").exists() and (resolved / "lcia_factors.csv").exists():
            return resolved
    return _resolve_runtime_csv_dir(candidates[0])


def _is_ecoinvent_source(value: object) -> bool:
    return str(value or "").strip().lower().startswith("ecoinvent")


def _flow_source_by_uuid(snapshot: dict) -> dict[str, str]:
    return {
        str(flow.get("flow_uuid") or "").strip(): str(flow.get("source_system") or "").strip()
        for flow in (snapshot.get("flows") or [])
        if str(flow.get("flow_uuid") or "").strip()
    }


def _filter_b_matrix_rows(b_matrix: dict, selected_rows: set[str]) -> dict:
    rows = [row for row in (b_matrix.get("rows") or []) if row in selected_rows]
    row_set = set(rows)
    return {
        "rows": rows,
        "cols": list(b_matrix.get("cols") or []),
        "data": [
            entry
            for entry in (b_matrix.get("data") or [])
            if str(entry.get("row") or "") in row_set
        ],
    }


def _merge_c_packs(packs: list[dict], b_matrix: dict) -> dict:
    canonical_order: list[str] = []
    canonical_lookup: dict[str, dict] = {}
    canonical_entries: dict[tuple[str, str], float] = {}
    matched_flow_uuids: set[str] = set()
    runtime_flow_uuids: set[str] = set()

    for pack in packs:
        matched_flow_uuids.update(pack.get("matched_flow_uuids", set()))
        runtime_flow_uuids.update(pack.get("runtime_flow_uuids", set()))
        lookup = pack.get("indicator_lookup", {})
        c_matrix = pack.get("C", {})
        for row_id in c_matrix.get("rows", []) or []:
            info = lookup.get(row_id, {})
            canonical_key = _canonical_ef31_indicator_key(info)
            if not canonical_key:
                continue
            if canonical_key not in canonical_lookup:
                canonical_order.append(canonical_key)
                canonical_lookup[canonical_key] = _canonical_ef31_indicator_info(info)
        for entry in c_matrix.get("data", []) or []:
            row_id = entry.get("row")
            flow_uuid = str(entry.get("col") or "")
            info = lookup.get(row_id, {})
            canonical_key = _canonical_ef31_indicator_key(info)
            if not canonical_key or not flow_uuid:
                continue
            value = _to_float(entry.get("value"))
            if value is None or value == 0:
                continue
            canonical_entries.setdefault((canonical_key, flow_uuid), value)

    canonical_index = {key: idx for idx, key in enumerate(canonical_order)}
    c_entries_by_index: dict[tuple[int, str], float] = {}
    for (canonical_key, flow_uuid), value in canonical_entries.items():
        row_idx = canonical_index.get(canonical_key)
        if row_idx is not None:
            c_entries_by_index[(row_idx, flow_uuid)] = value

    indicator_lookup = {
        idx: {
            **canonical_lookup[canonical_key],
            "indicator_index": idx,
            "canonical_indicator_key": canonical_key,
        }
        for canonical_key, idx in canonical_index.items()
    }
    return {
        "C": _build_matrix(list(range(len(canonical_order))), b_matrix.get("rows", []) or [], c_entries_by_index),
        "indicator_lookup": indicator_lookup,
        "matched_flow_uuids": matched_flow_uuids,
        "runtime_flow_uuids": runtime_flow_uuids,
        "cache_hit": all(bool(pack.get("cache_hit", False)) for pack in packs) if packs else False,
        "runtime_source_count": len(packs),
    }


def _build_source_partitioned_ef31_c_matrix(
    *,
    snapshot: dict,
    b_matrix: dict,
    legacy_ef31_dir: str,
    ecoinvent_ef31_dir: str,
    lcia_methods: list[str],
    issues: list[str],
) -> dict:
    source_by_uuid = _flow_source_by_uuid(snapshot)
    eco_rows = {
        flow_uuid
        for flow_uuid in (b_matrix.get("rows") or [])
        if _is_ecoinvent_source(source_by_uuid.get(flow_uuid))
    }
    legacy_rows = set(b_matrix.get("rows") or []) - eco_rows
    packs: list[dict] = []
    if legacy_rows:
        packs.append(
            GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_dir(
                legacy_ef31_dir,
                _filter_b_matrix_rows(b_matrix, legacy_rows),
                lcia_methods=lcia_methods,
                issues=issues,
            )
        )
    if eco_rows:
        packs.append(
            GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_dir(
                ecoinvent_ef31_dir,
                _filter_b_matrix_rows(b_matrix, eco_rows),
                lcia_methods=lcia_methods,
                issues=issues,
            )
        )
    return _merge_c_packs(packs, b_matrix)


@router.post("/compute", response_model=ComputeResponse)
def compute(req: ComputeRequest) -> ComputeResponse:
    return solve_compute(req)


@router.post("/lcia", response_model=LciaResponse)
def lcia(payload: LciaPayload) -> LciaResponse:
    total_started_at = time.perf_counter()
    snapshot = payload.snapshot or {}
    if not snapshot:
        raise HTTPException(status_code=400, detail="snapshot payload is required")
    build_ab_started_at = time.perf_counter()
    base = build_matrices_from_snapshot(snapshot)
    build_ab_seconds = time.perf_counter() - build_ab_started_at
    issues = base.setdefault("issues", [])
    b_matrix = base["B"]
    ef31_dir = str(_resolve_ef31_runtime_dir())
    if not Path(ef31_dir).exists():
        raise HTTPException(
            status_code=400,
            detail=f"EF3.1 dir not found: {ef31_dir}; set NEBULA_LCA_EF31_DIR to a runtime CSV directory",
        )
    selected_methods = {str(method).strip() for method in payload.lcia_methods if str(method).strip()}
    legacy_ef31_dir = Path(os.environ.get("NEBULA_LCA_LEGACY_EF31_DIR", "data/EF3.1"))
    if not legacy_ef31_dir.is_absolute():
        legacy_ef31_dir = Path(__file__).resolve().parents[2] / legacy_ef31_dir
    build_c_started_at = time.perf_counter()
    if selected_methods and selected_methods.issubset({"EF v3.1"}):
        c_pack = _build_source_partitioned_ef31_c_matrix(
            snapshot=snapshot,
            b_matrix=b_matrix,
            legacy_ef31_dir=str(legacy_ef31_dir),
            ecoinvent_ef31_dir=ef31_dir,
            lcia_methods=payload.lcia_methods,
            issues=base.get("issues"),
        )
    else:
        c_pack = GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_dir(
            ef31_dir,
            b_matrix,
            lcia_methods=payload.lcia_methods,
            issues=base.get("issues"),
        )
    build_c_seconds = time.perf_counter() - build_c_started_at
    c_matrix = c_pack["C"]

    try:
        solve_started_at = time.perf_counter()
        lcia_matrix = compute_lcia(base["A"], b_matrix, c_matrix)
        solve_seconds = time.perf_counter() - solve_started_at
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    indicator_lookup = c_pack.get("indicator_lookup", {})
    b_flow_rows = b_matrix.get("rows", []) or []
    ef_flow_uuid_set: set[str] = set(c_pack.get("runtime_flow_uuids", set()))

    flows = snapshot.get("flows", []) or []
    flow_name_map = {
        item.get("flow_uuid", ""): item.get("flow_name", "")
        for item in flows
        if item.get("flow_uuid")
    }
    missing_ef31_flow_uuids = [uuid for uuid in b_flow_rows if uuid not in ef_flow_uuid_set]
    missing_ef31_flows = [
        {"flow_uuid": uuid, "flow_name": flow_name_map.get(uuid, "")}
        for uuid in missing_ef31_flow_uuids
    ]

    mmr_path: str | None = None
    mmr_started_at = time.perf_counter()
    try:
        # Lazy import to avoid hard-failing service startup when local protobuf
        # runtime version is temporarily mismatched.
        from app.core.mmr import build_mmr, default_mmr_path, write_mmr

        output_dir = os.environ.get("NEBULA_LCA_OUTPUT_DIR", "exports")
        mmr = build_mmr(snapshot)
        mmr_file = default_mmr_path(output_dir)
        write_mmr(mmr, str(mmr_file))
        mmr_path = str(mmr_file)
    except Exception as exc:
        issues.append(f"MMR export skipped: {exc}")
    mmr_seconds = time.perf_counter() - mmr_started_at
    total_seconds = time.perf_counter() - total_started_at

    return LciaResponse(
        summary={
            "process_count": len(base["A"]["rows"]),
            "elementary_flow_count": len(b_matrix["rows"]),
            "indicator_count": len(c_matrix["rows"]),
            "issue_count": len(issues),
            "missing_ef31_flow_count": len(missing_ef31_flow_uuids),
            "timing_build_ab_seconds": build_ab_seconds,
            "timing_build_c_seconds": build_c_seconds,
            "timing_solve_seconds": solve_seconds,
            "timing_mmr_seconds": mmr_seconds,
            "timing_total_seconds": total_seconds,
            "ef31_runtime_cache_hit": bool(c_pack.get("cache_hit", False)),
            "ef31_runtime_source_count": int(c_pack.get("runtime_source_count", 0)),
        },
        missing_ef31_flow_uuids=missing_ef31_flow_uuids,
        missing_ef31_flows=missing_ef31_flows,
        indicator_index=[
            {
                "indicator_index": idx,
                **(indicator_lookup.get(idx, {})),
            }
            for idx in c_matrix["rows"]
        ],
        process_index=base["A"]["rows"],
        values=lcia_matrix.tolist(),
        mmr_path=mmr_path,
        issues=issues,
    )


@router.post("/pts/compile", response_model=PtsCompileResponse)
def pts_compile(payload: PtsCompilePayload) -> PtsCompileResponse:
    raw = payload.payload or {}
    try:
        result = compile_pts_from_payload(raw)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PtsCompileResponse(
        matrix_size=int(result.get("matrix_size", 0)),
        invertible=bool(result.get("invertible", True)),
        warnings=result.get("warnings", []),
        virtual_processes=result.get("virtual_processes", []),
    )
