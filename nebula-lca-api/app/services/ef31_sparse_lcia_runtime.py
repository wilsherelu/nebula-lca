"""Direct sparse EF 3.1 LCIA runtime for compressed ecoinvent LCI vectors."""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from ..config import PROJECT_ROOT, WORKSPACE_ROOT, settings
from .ef31_runtime_csv import ACTIVE_MANIFEST_NAME, DEFAULT_EF31_RUNTIME_ROOT
from .lci_runtime import expand_graph_lci_inventory, load_process_vectors
from ..models import FlowRecord, LciBiosphereFlowKey
from ..schemas import HybridGraph
from .public_flow_mapping_service import apply_elementary_mappings_to_snapshot


@dataclass(frozen=True)
class Ef31SparseRuntime:
    runtime_dir: Path
    indicator_index: list[dict[str, Any]]
    cf_by_flow_uuid: dict[str, list[tuple[int, float]]]
    flow_name_by_uuid: dict[str, str]
    source_dirs: tuple[str, ...] = ()

    @property
    def source_count(self) -> int:
        return len(self.source_dirs) or 1


@dataclass(frozen=True)
class DirectSparseLciaResult:
    solver_output: dict[str, Any]
    tiangong_like_input: dict[str, Any]


def try_run_hybrid_sparse_lcia(
    *,
    db: Session,
    graph: HybridGraph,
    lcia_methods: list[str] | None,
    flow_type_by_uuid: dict[str, str] | None = None,
    flow_source_by_uuid: dict[str, str] | None = None,
    runtime_root: Path | None = None,
) -> DirectSparseLciaResult | None:
    """Solve foreground technology while keeping terminal LCI vectors compressed."""
    lci_nodes = [node for node in graph.nodes if node.node_kind == "lci_dataset"]
    if not lci_nodes or len(lci_nodes) == len(graph.nodes):
        return None
    process_uuids = [str(node.process_uuid or "").strip() for node in lci_nodes]
    if any(not process_uuid or process_uuid.startswith("lci_") for process_uuid in process_uuids):
        return None
    if any(
        str(port.type or "") == "biosphere"
        for node in lci_nodes
        for port in [*(node.inputs or []), *(node.outputs or [])]
    ):
        return None

    from ..solver import to_tiangong_like
    from ..solver_adapter import _ensure_embedded_solver_core, _resolve_embedded_ef31_dirs

    snapshot = to_tiangong_like(
        graph,
        flow_type_by_uuid=flow_type_by_uuid,
        flow_source_by_uuid=flow_source_by_uuid,
    )
    elementary_mapping_trace = apply_elementary_mappings_to_snapshot(snapshot)
    lci_process_set = set(process_uuids)
    if any(str(link.get("consumer_process_uuid") or "") in lci_process_set for link in snapshot.get("links", [])):
        return None

    vectors = load_process_vectors(db, sorted(lci_process_set))
    missing_vectors = sorted(lci_process_set.difference(vectors))
    if missing_vectors:
        return None

    _ensure_embedded_solver_core()
    import importlib
    import numpy as np

    matrix_builder = importlib.import_module("app.core.matrix_builder")
    runtime_cache = importlib.import_module("app.core.ef31_runtime_cache")
    base = matrix_builder.build_matrices_from_snapshot(snapshot)
    process_index = list(base["A"]["rows"])
    process_pos = {process_uuid: index for index, process_uuid in enumerate(process_index)}
    process_count = len(process_index)

    needed_flow_key_ids = sorted({flow_key_id for vector in vectors.values() for flow_key_id in vector})
    flow_key_rows = _load_flow_keys(db, needed_flow_key_ids)
    b_matrix = base["B"]
    observed_flow_uuids = {
        *[str(flow_uuid) for flow_uuid in b_matrix.get("rows", [])],
        *[str(row.flow_uuid) for row in flow_key_rows.values()],
    }
    runtime_dirs = [runtime_root] if runtime_root is not None else _resolve_embedded_ef31_dirs()
    issues = base.setdefault("issues", [])
    c_pack = runtime_cache.GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_sources(
        [str(path) for path in runtime_dirs],
        {
            "rows": sorted(observed_flow_uuids),
            "cols": process_index,
            "shape": [len(observed_flow_uuids), process_count],
            "data": [],
        },
        lcia_methods=lcia_methods or ["EF v3.1"],
        issues=issues,
    )
    c_matrix = c_pack["C"]
    indicator_count = len(c_matrix["rows"])
    if indicator_count == 0:
        return None
    factor_by_flow_uuid: dict[str, list[tuple[int, float]]] = {}
    for entry in c_matrix.get("data", []):
        factor_by_flow_uuid.setdefault(str(entry["col"]), []).append(
            (int(entry["row_index"]), float(entry["value"]))
        )

    a_matrix = np.zeros((process_count, process_count), dtype=float)
    for entry in base["A"]["data"]:
        a_matrix[int(entry["row_index"]), int(entry["col_index"])] = float(entry["value"])

    characterized = np.zeros((indicator_count, process_count), dtype=float)
    flow_name_map = {
        str(item.get("flow_uuid") or ""): str(item.get("flow_name") or "")
        for item in snapshot.get("flows", [])
        if item.get("flow_uuid")
    }
    flow_rows = {
        row.flow_uuid: row
        for row in db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(sorted(observed_flow_uuids))).all()
    } if observed_flow_uuids else {}
    for flow_uuid, row in flow_rows.items():
        flow_name_map.setdefault(flow_uuid, str(row.flow_name or ""))
    for entry in b_matrix.get("data", []):
        flow_uuid = str(b_matrix["rows"][int(entry["row_index"])])
        amount = float(entry["value"])
        factors = factor_by_flow_uuid.get(flow_uuid, [])
        column = int(entry["col_index"])
        for indicator_pos, coefficient in factors:
            characterized[indicator_pos, column] += amount * float(coefficient)

    expanded_port_count = 0
    provenance: list[dict[str, Any]] = []
    allocation_total = base.get("allocation_total", {})
    for process_uuid, vector in vectors.items():
        column = process_pos.get(process_uuid)
        if column is None:
            return None
        denominator = float(allocation_total.get(process_uuid) or 1.0)
        if denominator <= 0:
            return None
        expanded_port_count += len(vector)
        for flow_key_id, amount in vector.items():
            flow_key = flow_key_rows.get(int(flow_key_id))
            if flow_key is None:
                continue
            flow_uuid = str(flow_key.flow_uuid)
            factors = factor_by_flow_uuid.get(flow_uuid, [])
            normalized_amount = float(amount) / denominator
            for indicator_pos, coefficient in factors:
                characterized[indicator_pos, column] += normalized_amount * float(coefficient)
        provenance.append({"process_uuid": process_uuid, "nnz": len(vector), "denominator": denominator})

    values = characterized @ np.linalg.inv(a_matrix)
    runtime_flow_uuids = set(c_pack.get("runtime_flow_uuids", set()))
    missing_flow_uuids = sorted(observed_flow_uuids.difference(runtime_flow_uuids))
    flow_key_by_uuid = {str(row.flow_uuid): row for row in flow_key_rows.values()}
    missing_flows = []
    for flow_uuid in missing_flow_uuids:
        flow_key = flow_key_by_uuid.get(flow_uuid)
        missing_flows.append({
            "flow_uuid": flow_uuid,
            "flow_name": flow_name_map.get(flow_uuid, ""),
            "unit": flow_key.canonical_unit if flow_key is not None else "",
            "direction": flow_key.direction if flow_key is not None else "",
            "compartment": flow_key.compartment if flow_key is not None else None,
            "subcompartment": flow_key.subcompartment if flow_key is not None else None,
            "covered_by_runtime_sources": 0,
        })
    indicator_lookup = c_pack.get("indicator_lookup", {})
    solver_output = {
        "summary": {
            "process_count": process_count,
            "elementary_flow_count": len(observed_flow_uuids),
            "indicator_count": indicator_count,
            "issue_count": len(issues),
            "missing_ef31_flow_count": len(missing_flow_uuids),
            "ef31_runtime_cache_hit": bool(c_pack.get("cache_hit", False)),
            "ef31_runtime_source_count": int(c_pack.get("runtime_source_count", 0)),
            "ef31_runtime_sources": [str(path) for path in runtime_dirs],
        },
        "issues": issues,
        "missing_ef31_flow_uuids": missing_flow_uuids,
        "missing_ef31_flows": missing_flows,
        "elementary_flow_mappings": elementary_mapping_trace,
        "indicator_index": [
            {"indicator_index": index, **indicator_lookup.get(index, {})}
            for index in c_matrix["rows"]
        ],
        "process_index": process_index,
        "values": values.tolist(),
        "lci_vector_runtime": {
            "mode": "hybrid_sparse_ef31_v1",
            "cf_match_scope": "flow_uuid",
            "expanded_process_count": len(vectors),
            "expanded_port_count": expanded_port_count,
            "materialized_port_count": 0,
            "missing_vectors": [],
            "provenance": provenance,
            "runtime_dir": str(runtime_dirs[0]) if runtime_dirs else "",
            "runtime_source_count": int(c_pack.get("runtime_source_count", 0)),
            "runtime_sources": [str(path) for path in runtime_dirs],
        },
    }
    return DirectSparseLciaResult(solver_output=solver_output, tiangong_like_input=snapshot)


def try_run_direct_sparse_lcia(
    *,
    db: Session,
    graph: HybridGraph,
    lcia_methods: list[str] | None,
    runtime_root: Path | None = None,
) -> DirectSparseLciaResult | None:
    """Run direct LCIA for pure compressed LCI graphs, otherwise return None."""
    if not _is_direct_sparse_candidate(graph, lcia_methods):
        return None
    runtime = load_ef31_sparse_runtime_bundle(runtime_root=runtime_root)
    if runtime is None:
        return None
    runtime = _filter_runtime_by_methods(runtime, lcia_methods)

    expanded = expand_graph_lci_inventory(db, graph)
    if expanded.missing_vectors:
        return None

    process_index = [
        str(node.process_uuid)
        for node in graph.nodes
        if node.node_kind == "lci_dataset" and node.process_uuid
    ]
    process_values = _characterize_by_process(db, graph, runtime)
    aggregate_result = characterize_inventory(db, expanded.inventory, runtime)

    solver_output = {
        "summary": {
            "process_count": len(process_index),
            "elementary_flow_count": len(expanded.inventory),
            "indicator_count": len(runtime.indicator_index),
            "issue_count": 0,
            "missing_ef31_flow_count": len(aggregate_result["missing_ef31_flow_uuids"]),
            "ef31_runtime_source_count": runtime.source_count,
            "ef31_runtime_sources": runtime.source_dirs,
        },
        "issues": [],
        "missing_ef31_flow_uuids": aggregate_result["missing_ef31_flow_uuids"],
        "missing_ef31_flows": aggregate_result["missing_ef31_flows"],
        "indicator_index": runtime.indicator_index,
        "process_index": process_index,
        "values": process_values,
        "lci_vector_runtime": {
            "mode": "direct_sparse_ef31_v1",
            "cf_match_scope": "flow_uuid",
            "expanded_process_count": len(expanded.provenance),
            "expanded_port_count": int(sum(item.get("nnz", 0) for item in expanded.provenance)),
            "missing_vectors": expanded.missing_vectors,
            "provenance": expanded.provenance,
            "runtime_dir": str(runtime.runtime_dir),
            "runtime_source_count": runtime.source_count,
            "runtime_sources": runtime.source_dirs,
        },
    }
    return DirectSparseLciaResult(
        solver_output=solver_output,
        tiangong_like_input=_minimal_tiangong_like_snapshot(graph),
    )


def characterize_inventory(
    db: Session,
    inventory: dict[int, float],
    runtime: Ef31SparseRuntime,
) -> dict[str, Any]:
    """Characterize a sparse flow_key_id inventory with EF 3.1 factors."""
    flow_key_rows = _load_flow_keys(db, sorted(inventory))
    missing_by_uuid: dict[str, dict[str, Any]] = {}
    values = [0.0 for _ in runtime.indicator_index]

    for flow_key_id, amount in inventory.items():
        if amount == 0:
            continue
        flow_key = flow_key_rows.get(int(flow_key_id))
        if flow_key is None:
            continue
        factors = runtime.cf_by_flow_uuid.get(flow_key.flow_uuid)
        if not factors:
            item = missing_by_uuid.setdefault(
                flow_key.flow_uuid,
                {
                    "flow_uuid": flow_key.flow_uuid,
                    "flow_name": runtime.flow_name_by_uuid.get(flow_key.flow_uuid, ""),
                    "amount": 0.0,
                    "unit": flow_key.canonical_unit,
                    "direction": flow_key.direction,
                    "compartment": flow_key.compartment,
                    "subcompartment": flow_key.subcompartment,
                    "covered_by_runtime_sources": 0,
                },
            )
            item["amount"] = float(item["amount"]) + float(amount)
            continue
        for indicator_pos, coefficient in factors:
            values[indicator_pos] += float(amount) * coefficient

    return {
        "values": values,
        "missing_ef31_flow_uuids": sorted(missing_by_uuid),
        "missing_ef31_flows": [missing_by_uuid[key] for key in sorted(missing_by_uuid)],
    }


def load_active_ef31_sparse_runtime(runtime_root: Path | None = None) -> Ef31SparseRuntime | None:
    root = runtime_root or DEFAULT_EF31_RUNTIME_ROOT
    runtime_dir = _resolve_runtime_dir(root)
    if runtime_dir is None:
        return None
    runtime = _load_runtime_cached(str(runtime_dir.resolve()))
    return _with_runtime_sources(runtime, [runtime_dir])


def load_ef31_sparse_runtime_bundle(runtime_root: Path | None = None) -> Ef31SparseRuntime | None:
    runtime_dirs: list[Path] = []
    seen: set[str] = set()
    for candidate in _ef31_runtime_candidates(runtime_root=runtime_root):
        runtime_dir = _resolve_runtime_dir(candidate)
        if runtime_dir is None:
            continue
        key = str(runtime_dir.resolve())
        if key in seen:
            continue
        seen.add(key)
        runtime_dirs.append(runtime_dir)
    if not runtime_dirs:
        return None
    runtimes = [_with_runtime_sources(_load_runtime_cached(str(path.resolve())), [path]) for path in runtime_dirs]
    return _merge_sparse_runtimes(runtimes)


def _ef31_runtime_candidates(runtime_root: Path | None = None) -> list[Path]:
    candidates = [runtime_root or DEFAULT_EF31_RUNTIME_ROOT]
    if settings.desktop_mode:
        bundle_root_raw = str(getattr(sys, "_MEIPASS", "") or "").strip()
        if bundle_root_raw:
            bundle_root = Path(bundle_root_raw)
            candidates.extend([
                bundle_root / "runtime" / "ef31",
                bundle_root / "solver-data" / "EF3.1",
            ])
    if runtime_root is None:
        candidates.extend([
            Path(settings.nebula_lca_ef31_dir),
            PROJECT_ROOT / "data" / "EF3.1",
            WORKSPACE_ROOT / "nebula-lca-solver" / "data" / "EF3.1",
        ])
    return candidates


def _with_runtime_sources(runtime: Ef31SparseRuntime, paths: list[Path]) -> Ef31SparseRuntime:
    return Ef31SparseRuntime(
        runtime_dir=runtime.runtime_dir,
        indicator_index=runtime.indicator_index,
        cf_by_flow_uuid=runtime.cf_by_flow_uuid,
        flow_name_by_uuid=runtime.flow_name_by_uuid,
        source_dirs=tuple(str(path) for path in paths),
    )


def _indicator_key(row: dict[str, Any]) -> str:
    canonical = str(row.get("canonical_indicator_key") or "").strip().lower()
    if canonical:
        return canonical
    method = str(row.get("method_en") or row.get("method_zh") or row.get("method") or "").strip().lower()
    indicator = str(row.get("indicator_en") or row.get("indicator_zh") or row.get("indicator") or "").strip().lower()
    category = str(row.get("ecoinvent_category") or "").strip().lower()
    return "||".join(part for part in (method, indicator, category) if part)


def _merge_sparse_runtimes(runtimes: list[Ef31SparseRuntime]) -> Ef31SparseRuntime:
    if len(runtimes) == 1:
        return runtimes[0]
    indicator_rows: list[dict[str, Any]] = []
    key_to_pos: dict[str, int] = {}
    row_pos_maps: list[dict[int, int]] = []
    cf_by_flow_uuid: dict[str, list[tuple[int, float]]] = {}
    flow_name_by_uuid: dict[str, str] = {}
    source_dirs: list[str] = []

    for runtime in runtimes:
        source_dirs.extend(runtime.source_dirs or (str(runtime.runtime_dir),))
        pos_map: dict[int, int] = {}
        for old_pos, row in enumerate(runtime.indicator_index):
            key = _indicator_key(row) or f"{runtime.runtime_dir}:{old_pos}"
            new_pos = key_to_pos.get(key)
            if new_pos is None:
                new_pos = len(indicator_rows)
                key_to_pos[key] = new_pos
                item = dict(row)
                item["indicator_index"] = new_pos
                indicator_rows.append(item)
            pos_map[old_pos] = new_pos
        row_pos_maps.append(pos_map)
        for flow_uuid, flow_name in runtime.flow_name_by_uuid.items():
            flow_name_by_uuid.setdefault(flow_uuid, flow_name)

    for runtime, pos_map in zip(runtimes, row_pos_maps):
        for flow_uuid, factors in runtime.cf_by_flow_uuid.items():
            rows = cf_by_flow_uuid.setdefault(flow_uuid, [])
            existing_positions = {pos for pos, _ in rows}
            for old_pos, coefficient in factors:
                new_pos = pos_map.get(old_pos)
                if new_pos is None or new_pos in existing_positions:
                    continue
                rows.append((new_pos, coefficient))
                existing_positions.add(new_pos)

    return Ef31SparseRuntime(
        runtime_dir=runtimes[0].runtime_dir,
        indicator_index=indicator_rows,
        cf_by_flow_uuid=cf_by_flow_uuid,
        flow_name_by_uuid=flow_name_by_uuid,
        source_dirs=tuple(source_dirs),
    )


@lru_cache(maxsize=4)
def _load_runtime_cached(runtime_dir_raw: str) -> Ef31SparseRuntime:
    runtime_dir = Path(runtime_dir_raw)
    flow_uuid_by_column: dict[int, str] = {}
    flow_name_by_uuid: dict[str, str] = {}
    with (runtime_dir / "flow_index.csv").open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            flow_uuid = str(row.get("FlowUUID") or "").strip()
            if not flow_uuid:
                continue
            column = int(row.get("flow_index") or 0)
            flow_uuid_by_column[column] = flow_uuid
            flow_name_by_uuid[flow_uuid] = str(row.get("FlowName") or "").strip()

    indicator_rows: list[dict[str, Any]] = []
    with (runtime_dir / "indicator_index.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            indicator_index = int(row.get("indicator_index") or 0)
            item = dict(row)
            item["indicator_index"] = indicator_index
            indicator_rows.append(item)
    indicator_rows.sort(key=lambda item: int(item["indicator_index"]))
    indicator_position_by_index = {
        int(item["indicator_index"]): pos for pos, item in enumerate(indicator_rows)
    }

    cf_by_flow_uuid: dict[str, list[tuple[int, float]]] = {}
    with (runtime_dir / "lcia_factors.csv").open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            flow_uuid = flow_uuid_by_column.get(int(row.get("column") or 0))
            indicator_pos = indicator_position_by_index.get(int(row.get("row") or 0))
            if flow_uuid is None or indicator_pos is None:
                continue
            coefficient = float(row.get("coefficient") or 0.0)
            if coefficient == 0:
                continue
            cf_by_flow_uuid.setdefault(flow_uuid, []).append((indicator_pos, coefficient))

    return Ef31SparseRuntime(
        runtime_dir=runtime_dir,
        indicator_index=indicator_rows,
        cf_by_flow_uuid=cf_by_flow_uuid,
        flow_name_by_uuid=flow_name_by_uuid,
    )


def _characterize_by_process(
    db: Session,
    graph: HybridGraph,
    runtime: Ef31SparseRuntime,
) -> list[list[float]]:
    process_columns: list[list[float]] = []
    for node in graph.nodes:
        if node.node_kind != "lci_dataset":
            continue
        single_graph = graph.model_copy(deep=True)
        single_graph.nodes = [item for item in single_graph.nodes if item.id == node.id]
        single_graph.exchanges = []
        expanded = expand_graph_lci_inventory(db, single_graph)
        result = characterize_inventory(db, expanded.inventory, runtime)
        process_columns.append(result["values"])

    values: list[list[float]] = []
    for indicator_pos in range(len(runtime.indicator_index)):
        values.append([
            column[indicator_pos] if indicator_pos < len(column) else 0.0
            for column in process_columns
        ])
    return values


def _is_direct_sparse_candidate(graph: HybridGraph, lcia_methods: list[str] | None) -> bool:
    methods = lcia_methods or ["EF v3.1"]
    if not all(str(method).strip() for method in methods):
        return False
    if not graph.nodes:
        return False
    if graph.exchanges:
        return False
    return all(
        node.node_kind == "lci_dataset"
        and str(node.process_uuid or "").strip()
        and not str(node.process_uuid or "").strip().startswith("lci_")
        for node in graph.nodes
    )


def _filter_runtime_by_methods(runtime: Ef31SparseRuntime, lcia_methods: list[str] | None) -> Ef31SparseRuntime:
    selected = {str(method).strip() for method in (lcia_methods or ["EF v3.1"]) if str(method).strip()}
    selected_rows = [
        (old_pos, row)
        for old_pos, row in enumerate(runtime.indicator_index)
        if str(row.get("method_en") or row.get("method_zh") or "").strip() in selected
    ]
    indicator_rows = [row for _, row in selected_rows]
    if not indicator_rows:
        return Ef31SparseRuntime(
            runtime_dir=runtime.runtime_dir,
            indicator_index=[],
            cf_by_flow_uuid={},
            flow_name_by_uuid=runtime.flow_name_by_uuid,
            source_dirs=runtime.source_dirs,
        )
    old_to_new = {old_pos: new_pos for new_pos, (old_pos, _) in enumerate(selected_rows)}
    remapped_rows = []
    for pos, row in enumerate(indicator_rows):
        item = dict(row)
        item["indicator_index"] = pos
        remapped_rows.append(item)
    cf_by_flow_uuid: dict[str, list[tuple[int, float]]] = {}
    for flow_uuid, factors in runtime.cf_by_flow_uuid.items():
        remapped = [
            (old_to_new[indicator_pos], coefficient)
            for indicator_pos, coefficient in factors
            if indicator_pos in old_to_new
        ]
        if remapped:
            cf_by_flow_uuid[flow_uuid] = remapped
    return Ef31SparseRuntime(
        runtime_dir=runtime.runtime_dir,
        indicator_index=remapped_rows,
        cf_by_flow_uuid=cf_by_flow_uuid,
        flow_name_by_uuid=runtime.flow_name_by_uuid,
        source_dirs=runtime.source_dirs,
    )


def _resolve_runtime_dir(root: Path) -> Path | None:
    if (root / "flow_index.csv").exists() and (root / "lcia_factors.csv").exists():
        return root
    manifest_path = root / ACTIVE_MANIFEST_NAME
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    artifact_dir = Path(str(manifest.get("artifact_dir") or manifest.get("output_dir") or ""))
    if not artifact_dir.is_absolute():
        artifact_dir = root / artifact_dir
    if not (artifact_dir / "flow_index.csv").exists():
        return None
    if not (artifact_dir / "indicator_index.csv").exists():
        return None
    if not (artifact_dir / "lcia_factors.csv").exists():
        return None
    return artifact_dir


def _load_flow_keys(db: Session, flow_key_ids: list[int]) -> dict[int, LciBiosphereFlowKey]:
    if not flow_key_ids:
        return {}
    return {
        int(row.flow_key_id): row
        for row in db.query(LciBiosphereFlowKey)
        .filter(LciBiosphereFlowKey.flow_key_id.in_(flow_key_ids))
        .all()
    }


def _minimal_tiangong_like_snapshot(graph: HybridGraph) -> dict[str, Any]:
    return {
        "processes": [
            {
                "process_uuid": node.process_uuid,
                "name": node.name,
                "node_kind": node.node_kind,
            }
            for node in graph.nodes
        ],
        "flows": [],
        "exchanges": [],
        "links": [],
        "runtime": "direct_sparse_ef31_v1",
    }
