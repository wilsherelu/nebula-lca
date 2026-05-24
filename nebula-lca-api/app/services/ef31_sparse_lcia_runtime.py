"""Direct sparse EF 3.1 LCIA runtime for compressed ecoinvent LCI vectors."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from .ef31_runtime_csv import ACTIVE_MANIFEST_NAME, DEFAULT_EF31_RUNTIME_ROOT
from .lci_runtime import expand_graph_lci_inventory
from ..models import LciBiosphereFlowKey
from ..schemas import HybridGraph


@dataclass(frozen=True)
class Ef31SparseRuntime:
    runtime_dir: Path
    indicator_index: list[dict[str, Any]]
    cf_by_flow_uuid: dict[str, list[tuple[int, float]]]
    flow_name_by_uuid: dict[str, str]


@dataclass(frozen=True)
class DirectSparseLciaResult:
    solver_output: dict[str, Any]
    tiangong_like_input: dict[str, Any]


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
    runtime = load_active_ef31_sparse_runtime(runtime_root=runtime_root)
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
    return _load_runtime_cached(str(runtime_dir.resolve()))


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
    return all(node.node_kind == "lci_dataset" for node in graph.nodes)


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
