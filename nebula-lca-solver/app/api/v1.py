from fastapi import APIRouter, HTTPException

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
    build_matrices_from_snapshot,
)
from app.core.pts_compile import compile_pts_from_payload
from app.core.solver import solve_compute

router = APIRouter()


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
    ef31_dir = os.environ.get("NEBULA_LCA_EF31_DIR", "data/EF3.1")
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
        c_pack = GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_sources(
            [str(legacy_ef31_dir), ef31_dir],
            b_matrix,
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
