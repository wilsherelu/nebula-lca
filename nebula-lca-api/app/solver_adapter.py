from __future__ import annotations

import json
from urllib import request, error

from .config import settings
from .schemas import HybridGraph
from .solver import to_tiangong_like


def run_tiangong_lcia(graph: HybridGraph, *, flow_type_by_uuid: dict[str, str] | None = None) -> dict:
    snapshot = to_tiangong_like(graph, flow_type_by_uuid=flow_type_by_uuid)
    payload = json.dumps({"snapshot": snapshot}, ensure_ascii=False).encode("utf-8")
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
