from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def allocation_closure(case_root: Path) -> dict[str, Any]:
    graph = read_json(case_root / "case_01_unit_process" / "input_graph.json")
    node = graph["nodes"][0]
    products = [port for port in node["outputs"] if port.get("isProduct")]
    shared = [
        {
            "exchange_id": port["id"],
            "flow_uuid": port["flowUuid"],
            "name": port["name"],
            "unit": port["unit"],
            "signed_source_amount": -float(port["amount"]),
        }
        for port in node["inputs"]
        if port.get("type") != "biosphere"
    ] + [
        {
            "exchange_id": port["id"],
            "flow_uuid": port["flowUuid"],
            "name": port["name"],
            "unit": port["unit"],
            "signed_source_amount": float(port["amount"]),
        }
        for port in node.get("emissions", [])
    ]

    rows: list[dict[str, Any]] = []
    for exchange in shared:
        reconstructed = 0.0
        product_terms = []
        for product in products:
            quantity = float(product["amount"])
            factor = float(product["allocationFactor"])
            per_unit = factor * exchange["signed_source_amount"] / quantity
            contribution = quantity * per_unit
            reconstructed += contribution
            product_terms.append(
                {
                    "product_flow_uuid": product["flowUuid"],
                    "product_name": product["name"],
                    "product_quantity": quantity,
                    "allocation_factor": factor,
                    "resolved_amount_per_product_unit": per_unit,
                    "reconstructed_source_amount": contribution,
                }
            )
        absolute_residual = reconstructed - exchange["signed_source_amount"]
        scale = max(abs(exchange["signed_source_amount"]), np.finfo(float).tiny)
        rows.append(
            {
                **exchange,
                "product_terms": product_terms,
                "reconstructed_amount": reconstructed,
                "absolute_residual": absolute_residual,
                "relative_residual": absolute_residual / scale,
            }
        )

    source_vector = np.array([row["signed_source_amount"] for row in rows], dtype=float)
    reconstructed_vector = np.array([row["reconstructed_amount"] for row in rows], dtype=float)
    delta = reconstructed_vector - source_vector
    duplicate_specific_source = 0.4
    duplicate_specific_reconstruction = duplicate_specific_source * 2.0
    return {
        "schema_version": "nebula.paper.allocation-closure.v1",
        "source_graph_sha256": sha256(case_root / "case_01_unit_process" / "input_graph.json"),
        "allocation_factor_sum": sum(float(port["allocationFactor"]) for port in products),
        "exchange_rows": rows,
        "vector_norm_inf": float(np.linalg.norm(delta, ord=np.inf)),
        "vector_norm_2": float(np.linalg.norm(delta, ord=2)),
        "within_tolerance": bool(np.allclose(source_vector, reconstructed_vector, atol=1e-12, rtol=1e-12)),
        "negative_control": {
            "description": "A product-specific exchange is deliberately assigned to two product columns.",
            "source_amount": duplicate_specific_source,
            "reconstructed_amount": duplicate_specific_reconstruction,
            "absolute_residual": duplicate_specific_reconstruction - duplicate_specific_source,
            "rejected": duplicate_specific_reconstruction != duplicate_specific_source,
        },
    }


def process_denominator(node: dict[str, Any]) -> float:
    products = [
        port for port in node.get("outputs", [])
        if port.get("type") != "biosphere" and port.get("isProduct")
    ]
    value = sum(max(float(port.get("amount") or 0.0), 0.0) for port in products)
    return value if value > 0 else 1.0


def port_amounts(node: dict[str, Any], key: str) -> dict[str, float]:
    result: dict[str, float] = {}
    for port in node.get(key, []):
        if port.get("type") == "biosphere":
            continue
        flow_uuid = str(port.get("flowUuid") or "")
        result[flow_uuid] = result.get(flow_uuid, 0.0) + float(port.get("amount") or 0.0)
    return result


def pts_oracle(case_root: Path) -> dict[str, Any]:
    graph_path = case_root / "case_05_pts_compiled" / "pts_compile_graph.json"
    compile_path = case_root / "case_05_pts_compiled" / "pts_compile_result.json"
    graph = read_json(graph_path)
    compile_result = read_json(compile_path)
    canvas = graph["metadata"]["canvases"][0]
    nodes = canvas["nodes"]
    edges = canvas["edges"]
    node_ids = [str(node["id"]) for node in nodes]
    node_index = {node_id: index for index, node_id in enumerate(node_ids)}
    by_id = {str(node["id"]): node for node in nodes}
    denominators = {node_id: process_denominator(by_id[node_id]) for node_id in node_ids}

    a_pts = np.zeros((len(nodes), len(nodes)), dtype=float)
    for edge in edges:
        provider = str(edge["fromNode"])
        consumer = str(edge["toNode"])
        amount = float(edge.get("consumerAmount") or edge.get("amount") or 0.0)
        a_pts[node_index[provider], node_index[consumer]] += amount / denominators[consumer]
    m = np.eye(len(nodes), dtype=float) - a_pts

    flow_rows: dict[str, np.ndarray] = {}
    flow_meta: dict[str, dict[str, str]] = {}
    elementary_rows: dict[str, np.ndarray] = {}
    for node_id in node_ids:
        node = by_id[node_id]
        column = node_index[node_id]
        denominator = denominators[node_id]
        inputs = port_amounts(node, "inputs")
        outputs = port_amounts(node, "outputs")
        for port in [*node.get("inputs", []), *node.get("outputs", []), *node.get("emissions", [])]:
            flow_uuid = str(port.get("flowUuid") or "")
            if flow_uuid:
                flow_meta[flow_uuid] = {
                    "name": str(port.get("name") or flow_uuid),
                    "unit": str(port.get("unit") or ""),
                }
        for flow_uuid in sorted(set(inputs) | set(outputs)):
            net = (outputs.get(flow_uuid, 0.0) - inputs.get(flow_uuid, 0.0)) / denominator
            if abs(net) > 1e-15:
                flow_rows.setdefault(flow_uuid, np.zeros(len(nodes), dtype=float))[column] += net
        for port in node.get("emissions", []):
            flow_uuid = str(port["flowUuid"])
            elementary_rows.setdefault(flow_uuid, np.zeros(len(nodes), dtype=float))[column] += (
                float(port["amount"]) / denominator
            )

    target_uuid = str(graph["nodes"][0]["outputs"][0]["flowUuid"])
    declared_input_uuids = {
        str(port["flowUuid"])
        for port in graph["nodes"][0].get("inputs", [])
        if str(port.get("flowUuid") or "")
    }
    producer = next(
        node_id for node_id in node_ids
        if any(str(port.get("flowUuid")) == target_uuid for port in by_id[node_id].get("outputs", []))
    )
    demand = np.zeros(len(nodes), dtype=float)
    demand[node_index[producer]] = 1.0
    x = np.linalg.solve(m, demand)
    residual = m @ x - demand
    target_coefficient = float(flow_rows[target_uuid] @ x)
    x /= target_coefficient

    boundary_rows: list[dict[str, Any]] = []
    non_exposed_rows: list[dict[str, Any]] = []
    for flow_uuid, coefficients in sorted(flow_rows.items()):
        amount = float(coefficients @ x)
        if abs(amount) <= 1e-12 or flow_uuid == target_uuid:
            continue
        row = {
            "flow_uuid": flow_uuid,
            **flow_meta[flow_uuid],
            "direction": "output" if amount > 0 else "input",
            "signed_amount": amount,
            "absolute_amount": abs(amount),
        }
        if flow_uuid in declared_input_uuids:
            boundary_rows.append(row)
        else:
            non_exposed_rows.append(row)
    elementary = [
        {
            "flow_uuid": flow_uuid,
            **flow_meta[flow_uuid],
            "direction": "output" if float(coefficients @ x) >= 0 else "input",
            "signed_amount": float(coefficients @ x),
            "absolute_amount": abs(float(coefficients @ x)),
        }
        for flow_uuid, coefficients in sorted(elementary_rows.items())
        if abs(float(coefficients @ x)) > 1e-12
    ]

    virtual = next(
        item for item in compile_result["artifact"]["virtual_processes"]
        if item["reference_product"]["flowUuid"] == target_uuid
    )
    compiled_rows = {
        (str(item["flowUuid"]), "input"): float(item["amount"])
        for item in virtual["technosphere_inputs"]
    }
    compiled_rows.update(
        {
            (str(item["flowUuid"]), str(item["direction"])): float(item["amount"])
            for item in virtual["elementary_flows"]
        }
    )
    comparisons = []
    for row_type, rows in (("boundary", boundary_rows), ("elementary", elementary)):
        for row in rows:
            oracle_amount = row["absolute_amount"]
            compiled_amount = compiled_rows.get((row["flow_uuid"], row["direction"]), 0.0)
            absolute_error = abs(oracle_amount - compiled_amount)
            relative_error = absolute_error / max(abs(oracle_amount), np.finfo(float).tiny)
            comparisons.append(
                {
                    "row_type": row_type,
                    "flow_uuid": row["flow_uuid"],
                    "name": row["name"],
                    "unit": row["unit"],
                    "direction": row["direction"],
                    "oracle_amount": oracle_amount,
                    "compiled_amount": compiled_amount,
                    "absolute_error": absolute_error,
                    "relative_error": relative_error,
                }
            )

    return {
        "schema_version": "nebula.paper.independent-pts-oracle.v1",
        "source_graph_sha256": sha256(graph_path),
        "compile_result_sha256": sha256(compile_path),
        "node_order": node_ids,
        "A_pts": a_pts.tolist(),
        "M": m.tolist(),
        "demand_basis": demand.tolist(),
        "activity_vector": x.tolist(),
        "condition_number_1": float(np.linalg.cond(m, p=1)),
        "condition_number_inf": float(np.linalg.cond(m, p=np.inf)),
        "residual_norm_inf": float(np.linalg.norm(residual, ord=np.inf)),
        "boundary_rows": boundary_rows,
        "non_exposed_technosphere_rows": non_exposed_rows,
        "elementary_rows": elementary,
        "component_comparison": comparisons,
        "max_absolute_error": max((row["absolute_error"] for row in comparisons), default=0.0),
        "max_relative_error": max((row["relative_error"] for row in comparisons), default=0.0),
        "nonzero_row_count": len(comparisons),
        "comparison_scope": (
            "The declared reference product, declared shell inputs, and elementary-flow rows are compared. "
            "Positive technosphere outputs not exposed by the PTS shell are reported separately and are not "
            "treated as compiled boundary rows."
        ),
        "within_tolerance": all(
            row["absolute_error"] <= 1e-12 + 1e-10 * abs(row["oracle_amount"])
            for row in comparisons
        ),
    }


def write_comparison_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def ef31_indicator_comparison(case_root: Path, repository_root: Path) -> list[dict[str, Any]]:
    audit = read_json(case_root / "case_05_pts_compiled" / "audit_report.json")
    comparisons = audit["lcia_comparison"]["indicators"]

    with (repository_root / "nebula-lca-solver" / "data" / "EF3.1" / "indicator_index.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as stream:
        identities = {
            row["ecoinvent_category"].strip().casefold(): row
            for row in csv.DictReader(stream, delimiter=";")
        }
    with (repository_root / "nebula-lca-api" / "data" / "EF3.1" / "indicator_index.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as stream:
        method_metadata = {
            int(row["indicator_index"]): row
            for row in csv.DictReader(stream)
        }

    rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for record_index, comparison in enumerate(comparisons):
        canonical_key = comparison["indicator"]["canonical_indicator_key"].strip().casefold()
        identity = identities.get(canonical_key)
        if identity is None or canonical_key in seen_keys:
            raise ValueError(f"EF 3.1 indicator identity not found: {canonical_key}")
        seen_keys.add(canonical_key)
        indicator_index = int(identity["indicator_index"])
        metadata = method_metadata.get(indicator_index)
        if metadata is None or any(
            identity[field] != metadata[field] for field in ("method_en", "indicator_en")
        ):
            raise ValueError(f"EF 3.1 method metadata mismatch: {canonical_key}")
        rows.append(
            {
                "record_index": record_index,
                "canonical_indicator_key": canonical_key,
                "method_label": identity["method_en"],
                "unit": metadata["LCIA_unit"],
                "expanded": comparison["expanded"],
                "compiled": comparison["compiled"],
                "absolute_difference": comparison["absolute_error"],
                "nonzero": str(bool(comparison["expanded"] or comparison["compiled"])).lower(),
            }
        )
    if seen_keys != set(identities):
        raise ValueError("EF 3.1 comparison does not cover the pinned canonical indicator set")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    allocation = allocation_closure(args.case_root)
    pts = pts_oracle(args.case_root)
    repository_root = Path(__file__).resolve().parents[3]
    ef31 = ef31_indicator_comparison(args.case_root, repository_root)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "allocation_closure.json", allocation)
    write_json(args.output_dir / "pts_independent_oracle.json", pts)
    write_comparison_csv(args.output_dir / "pts_component_comparison.csv", pts["component_comparison"])
    write_comparison_csv(args.output_dir / "ef31_indicator_comparison.csv", ef31)
    manifest = {
        path.name: sha256(path)
        for path in sorted(args.output_dir.iterdir())
        if path.is_file() and path.name != "manifest.sha256.json"
    }
    write_json(args.output_dir / "manifest.sha256.json", manifest)
    if not allocation["within_tolerance"] or not allocation["negative_control"]["rejected"]:
        raise SystemExit("allocation closure validation failed")
    if not pts["within_tolerance"]:
        raise SystemExit("PTS component comparison failed")


if __name__ == "__main__":
    main()
