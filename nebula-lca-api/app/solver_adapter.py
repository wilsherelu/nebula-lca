from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path
from urllib import request, error

from .config import WORKSPACE_ROOT, settings
from .schemas import HybridGraph
from .solver import to_tiangong_like


def _ensure_embedded_solver_core() -> None:
    if "app.core.matrix_builder" in sys.modules:
        return
    pyinstaller_root = Path(getattr(sys, "_MEIPASS", ""))
    bundled_core = pyinstaller_root / "solver-core"
    solver_core = bundled_core if bundled_core.exists() else WORKSPACE_ROOT / "nebula-lca-solver" / "app" / "core"
    if not solver_core.exists():
        alt = WORKSPACE_ROOT / "ref_code" / "nebula-lca-solver" / "app" / "core"
        solver_core = alt if alt.exists() else solver_core
    if not solver_core.exists():
        raise RuntimeError(f"embedded solver core not found: {solver_core}")
    core_package = types.ModuleType("app.core")
    core_package.__path__ = [str(solver_core)]  # type: ignore[attr-defined]
    sys.modules.setdefault("app.core", core_package)


def _run_embedded_lcia(snapshot: dict, lcia_methods: list[str]) -> dict:
    _ensure_embedded_solver_core()
    lcia_module = importlib.import_module("app.core.lcia")
    matrix_builder = importlib.import_module("app.core.matrix_builder")
    runtime_cache = importlib.import_module("app.core.ef31_runtime_cache")

    base = matrix_builder.build_matrices_from_snapshot(snapshot)
    issues = base.setdefault("issues", [])
    b_matrix = base["B"]
    ef31_dir = Path(settings.nebula_lca_ef31_dir)
    if ef31_dir.is_file() and ef31_dir.name == "active_manifest.json":
        try:
            manifest = json.loads(ef31_dir.read_text(encoding="utf-8"))
            artifact_dir = manifest.get("artifact_dir")
            if artifact_dir:
                ef31_dir = Path(artifact_dir)
        except Exception:
            pass
    elif (ef31_dir / "active_manifest.json").exists():
        try:
            manifest = json.loads((ef31_dir / "active_manifest.json").read_text(encoding="utf-8"))
            artifact_dir = manifest.get("artifact_dir")
            if artifact_dir:
                ef31_dir = Path(artifact_dir)
        except Exception:
            pass
    c_pack = runtime_cache.GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_dir(
        str(ef31_dir),
        b_matrix,
        lcia_methods=lcia_methods,
        issues=issues,
    )
    c_matrix = c_pack["C"]
    values = lcia_module.compute_lcia(base["A"], b_matrix, c_matrix)
    indicator_lookup = c_pack.get("indicator_lookup", {})
    flow_name_map = {
        item.get("flow_uuid", ""): item.get("flow_name", "")
        for item in (snapshot.get("flows") or [])
        if item.get("flow_uuid")
    }
    runtime_flow_uuids = set(c_pack.get("runtime_flow_uuids", set()))
    missing_ef31_flow_uuids = [uuid for uuid in (b_matrix.get("rows") or []) if uuid not in runtime_flow_uuids]
    return {
        "summary": {
            "process_count": len(base["A"]["rows"]),
            "elementary_flow_count": len(b_matrix["rows"]),
            "indicator_count": len(c_matrix["rows"]),
            "issue_count": len(issues),
            "missing_ef31_flow_count": len(missing_ef31_flow_uuids),
            "ef31_runtime_cache_hit": bool(c_pack.get("cache_hit", False)),
            "ef31_runtime_source_count": int(c_pack.get("runtime_source_count", 0)),
        },
        "missing_ef31_flow_uuids": missing_ef31_flow_uuids,
        "missing_ef31_flows": [
            {"flow_uuid": uuid, "flow_name": flow_name_map.get(uuid, "")}
            for uuid in missing_ef31_flow_uuids
        ],
        "indicator_index": [
            {"indicator_index": idx, **(indicator_lookup.get(idx, {}))}
            for idx in c_matrix["rows"]
        ],
        "process_index": base["A"]["rows"],
        "values": values.tolist(),
        "mmr_path": None,
        "issues": issues,
    }


def _run_embedded_pts_compile(payload: dict) -> dict:
    _ensure_embedded_solver_core()
    pts_compile = importlib.import_module("app.core.pts_compile")
    result = pts_compile.compile_pts_from_payload(payload)
    return {
        "matrix_size": int(result.get("matrix_size", 0)),
        "invertible": bool(result.get("invertible", True)),
        "warnings": result.get("warnings", []),
        "virtual_processes": result.get("virtual_processes", []),
    }


def run_tiangong_lcia(
    graph: HybridGraph,
    *,
    flow_type_by_uuid: dict[str, str] | None = None,
    flow_source_by_uuid: dict[str, str] | None = None,
    lcia_methods: list[str] | None = None,
) -> dict:
    snapshot = to_tiangong_like(
        graph,
        flow_type_by_uuid=flow_type_by_uuid,
        flow_source_by_uuid=flow_source_by_uuid,
    )
    payload = json.dumps(
        {"snapshot": snapshot, "lcia_methods": lcia_methods or ["EF v3.1"]},
        ensure_ascii=False,
    ).encode("utf-8")
    if settings.desktop_mode:
        return {
            "tiangong_like_input": snapshot,
            "solver_output": _run_embedded_lcia(snapshot, lcia_methods or ["EF v3.1"]),
        }
    api_url = settings.nebula_lca_solver_api_url.rstrip("/") + "/v1/lcia"
    req = request.Request(
        api_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"tiangong solver api failed (status={exc.code}): {detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"tiangong solver api request failed: {exc}") from exc

    return {
        "tiangong_like_input": snapshot,
        "solver_output": data,
    }


def run_tiangong_pts_compile(payload: dict) -> dict:
    if settings.desktop_mode:
        return _run_embedded_pts_compile(payload)
    body = json.dumps({"payload": payload}, ensure_ascii=False).encode("utf-8")
    api_url = settings.nebula_lca_solver_api_url.rstrip("/") + "/v1/pts/compile"
    req = request.Request(
        api_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"tiangong pts api failed (status={exc.code}): {detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"tiangong pts api request failed: {exc}") from exc
