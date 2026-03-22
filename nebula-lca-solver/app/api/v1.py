from fastapi import APIRouter, HTTPException

import os
from pathlib import Path
import csv

from app.core.schema import (
    ComputeRequest,
    ComputeResponse,
    LciaPayload,
    LciaResponse,
    PtsCompilePayload,
    PtsCompileResponse,
)
from app.core.lcia import compute_lcia
from app.core.matrix_builder import build_c_matrix_from_ef31, build_matrices_from_snapshot
from app.core.pts_compile import compile_pts_from_payload
from app.core.solver import solve_compute

router = APIRouter()


@router.post("/compute", response_model=ComputeResponse)
def compute(req: ComputeRequest) -> ComputeResponse:
    return solve_compute(req)


@router.post("/lcia", response_model=LciaResponse)
def lcia(payload: LciaPayload) -> LciaResponse:
    snapshot = payload.snapshot or {}
    if not snapshot:
        raise HTTPException(status_code=400, detail="snapshot payload is required")
    base = build_matrices_from_snapshot(snapshot)
    issues = base.setdefault("issues", [])
    b_matrix = base["B"]
    ef31_dir = os.environ.get("NEBULA_LCA_EF31_DIR", "data/EF3.1")
    if not Path(ef31_dir).exists():
        raise HTTPException(status_code=400, detail=f"EF3.1 dir not found: {ef31_dir}")
    c_pack = build_c_matrix_from_ef31(ef31_dir, b_matrix, issues=base.get("issues"))
    c_matrix = c_pack["C"]

    try:
        lcia_matrix = compute_lcia(base["A"], b_matrix, c_matrix)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    indicator_lookup = c_pack.get("indicator_lookup", {})
    b_flow_rows = b_matrix.get("rows", []) or []
    ef_flow_uuid_set: set[str] = set()
    flow_index_path = Path(ef31_dir) / "flow_index.csv"
    with flow_index_path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, [])
        if header:
            header[0] = header[0].lstrip("\ufeff")
        col_map = {name: idx for idx, name in enumerate(header)}
        uuid_idx = col_map.get("FlowUUID")
        if uuid_idx is not None:
            for row in reader:
                if uuid_idx < len(row):
                    flow_uuid = row[uuid_idx].strip()
                    if flow_uuid:
                        ef_flow_uuid_set.add(flow_uuid)

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

    return LciaResponse(
        summary={
            "process_count": len(base["A"]["rows"]),
            "elementary_flow_count": len(b_matrix["rows"]),
            "indicator_count": len(c_matrix["rows"]),
            "issue_count": len(issues),
            "missing_ef31_flow_count": len(missing_ef31_flow_uuids),
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
