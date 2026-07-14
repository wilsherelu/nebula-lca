"""Verify the reviewed ecoinvent <-> EF GHG mapping package.

This is a read-only acceptance check. It loads a mapping row from data, checks
all flow records in the current SQLite catalog, compares the four EF 3.1
climate-change characterization factors, and verifies numeric round trips.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable


API_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = API_ROOT / "lca_demo.db"
DEFAULT_MAPPING = API_ROOT / "data" / "flow_mappings" / "ghg_ef31_v1.json"
DEFAULT_ECO_RUNTIME = API_ROOT / "runtime" / "ef31" / "lcia-official-3.11"
DEFAULT_EF_RUNTIME = API_ROOT / "data" / "EF3.1"
DEFAULT_ECO_MASTERDATA = Path(r"D:\ecoinvent\ecoinvent 3.11_cutoff_lci_ecoSpold02\MasterData")

CLIMATE_KEYS = ("total", "biogenic", "fossil", "land_use_change")


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        return list(csv.DictReader(handle, dialect=dialect))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _masterdata_contexts(
    masterdata_dir: Path, flow_uuids: set[str]
) -> dict[str, dict[str, str]]:
    path = masterdata_dir / "ElementaryExchanges.xml"
    contexts: dict[str, dict[str, str]] = {}
    for _, element in ET.iterparse(path, events=("end",)):
        tag = element.tag.split("}")[-1]
        if tag not in {"elementaryExchange", "ElementaryFlow"}:
            continue
        candidate_uuid = element.get("id", "")
        if not candidate_uuid:
            for child in element:
                if child.tag.split("}")[-1] == "uuid":
                    candidate_uuid = (child.text or "").strip()
                    break
        if candidate_uuid not in flow_uuids:
            element.clear()
            continue
        compartment = ""
        subcompartment = ""
        for child in element:
            if child.tag.split("}")[-1] != "compartment":
                continue
            if list(child):
                for nested in child:
                    nested_tag = nested.tag.split("}")[-1]
                    if nested_tag == "compartment":
                        compartment = (nested.text or "").strip()
                    elif nested_tag == "subcompartment":
                        subcompartment = (nested.text or "").strip()
            else:
                compartment = (child.text or "").strip()
        contexts[candidate_uuid] = {
            "compartment": compartment,
            "subcompartment": subcompartment,
        }
        element.clear()
    missing = flow_uuids - contexts.keys()
    if missing:
        raise ValueError(f"flows are absent from MasterData: {sorted(missing)}")
    return contexts


def _evidence_checks(
    package: dict[str, object],
    masterdata_dir: Path,
    eco_runtime: Path,
    ef_runtime: Path,
) -> tuple[bool, dict[str, bool]]:
    evidence_rows = {
        row["evidence_id"]: row
        for row in package.get("evidence", [])
    }
    mappings = [
        *package.get("mappings", []),
        *package.get("one_way_mappings", []),
    ]
    evidence_ids = sorted(
        {
            str(evidence_id)
            for mapping in mappings
            for evidence_id in mapping.get("evidence_ids", [])
        }
    )
    checks: dict[str, bool] = {
        "evidence_ids_resolved": bool(evidence_ids) and all(item in evidence_rows for item in evidence_ids),
    }
    for evidence_id in evidence_ids:
        evidence = evidence_rows.get(evidence_id)
        if evidence is None:
            continue
        if evidence.get("kind") == "masterdata_flow":
            artifact = masterdata_dir / str(evidence["artifact"])
            checks[f"artifact_hash:{evidence_id}"] = (
                artifact.is_file() and _sha256(artifact) == str(evidence["sha256"]).casefold()
            )
        elif evidence.get("kind") == "cf_runtime":
            root = eco_runtime if evidence.get("namespace") == "ecoinvent" else ef_runtime
            checks[f"artifact_hash:{evidence_id}"] = all(
                (root / filename).is_file()
                and _sha256(root / filename) == str(expected_hash).casefold()
                for filename, expected_hash in evidence.get("files", {}).items()
            )
    return all(checks.values()), checks


def _load_sparse_cf_vectors(
    runtime_dir: Path,
    flow_uuids: set[str],
    indicator_selector: Callable[[dict[str, str]], str | None],
) -> dict[str, dict[str, float]]:
    flow_rows = _csv_rows(runtime_dir / "flow_index.csv")
    indicator_rows = _csv_rows(runtime_dir / "indicator_index.csv")
    flow_columns = {
        str(row.get("FlowUUID") or "").strip(): int(row.get("flow_index") or 0)
        for row in flow_rows
    }
    missing = flow_uuids - flow_columns.keys()
    if missing:
        raise ValueError(f"flows are absent from runtime: {sorted(missing)}")

    selected_rows: dict[int, str] = {}
    for row in indicator_rows:
        key = indicator_selector(row)
        if key is not None:
            selected_rows[int(row.get("indicator_index") or 0)] = key
    if set(selected_rows.values()) != set(CLIMATE_KEYS):
        raise ValueError(f"runtime does not expose the four climate indicators: {runtime_dir}")

    columns_to_uuid = {flow_columns[flow_uuid]: flow_uuid for flow_uuid in flow_uuids}
    vectors = {
        flow_uuid: {key: 0.0 for key in CLIMATE_KEYS}
        for flow_uuid in flow_uuids
    }
    for row in _csv_rows(runtime_dir / "lcia_factors.csv"):
        flow_uuid = columns_to_uuid.get(int(row.get("column") or 0))
        if flow_uuid is None:
            continue
        indicator_key = selected_rows.get(int(row.get("row") or 0))
        if indicator_key is not None:
            vectors[flow_uuid][indicator_key] += float(row.get("coefficient") or 0.0)
    return vectors


def _ecoinvent_indicator(row: dict[str, str]) -> str | None:
    if str(row.get("method_en") or "").strip() != "EF v3.1":
        return None
    category = str(row.get("ecoinvent_category") or "").strip().lower()
    return {
        "climate change": "total",
        "climate change: biogenic": "biogenic",
        "climate change: fossil": "fossil",
        "climate change: land use and land use change": "land_use_change",
    }.get(category)


def _ef_indicator(row: dict[str, str]) -> str | None:
    method = str(row.get("method_en") or "").strip().lower()
    return {
        "climate change": "total",
        "climate change-biogenic": "biogenic",
        "climate change-fossil": "fossil",
        "climate change-land use and land use change": "land_use_change",
    }.get(method)


def _flow_record(connection: sqlite3.Connection, flow_uuid: str) -> dict[str, object]:
    row = connection.execute(
        """
        SELECT flow_uuid, flow_name, flow_name_en, flow_type, default_unit,
               unit_group, compartment, subcompartment, source
        FROM flow_catalog
        WHERE flow_uuid = ?
        """,
        (flow_uuid,),
    ).fetchone()
    if row is None:
        raise ValueError(f"flow is absent from database: {flow_uuid}")
    return dict(row)


def verify(
    database: Path,
    mapping_path: Path,
    eco_runtime: Path,
    ef_runtime: Path,
    eco_masterdata: Path,
    amount: float,
) -> dict[str, object]:
    package = json.loads(mapping_path.read_text(encoding="utf-8"))
    mappings = package["mappings"]
    one_way_mappings = package.get("one_way_mappings", [])
    all_mappings = [*mappings, *one_way_mappings]

    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        flow_records = {
            flow_uuid: _flow_record(connection, flow_uuid)
            for mapping in all_mappings
            for flow_uuid in (
                mapping["ecoinvent_flow_uuid"],
                mapping["ef_flow_uuid"],
            )
        }
    finally:
        connection.close()

    masterdata_contexts = _masterdata_contexts(
        eco_masterdata,
        {str(mapping["ecoinvent_flow_uuid"]) for mapping in all_mappings},
    )
    review_ok = (
        package.get("review_status") == "approved"
        and package.get("mapping_count") == len(mappings)
        and package.get("one_way_mapping_count", 0) == len(one_way_mappings)
        and all(
            mapping.get("review_status") == "approved"
            and mapping.get("semantic_grade") == "S1"
            for mapping in all_mappings
        )
    )
    evidence_ok, evidence_checks = _evidence_checks(
        package,
        eco_masterdata,
        eco_runtime,
        ef_runtime,
    )
    eco_vectors = _load_sparse_cf_vectors(
        eco_runtime,
        {str(mapping["ecoinvent_flow_uuid"]) for mapping in all_mappings},
        _ecoinvent_indicator,
    )
    ef_vectors = _load_sparse_cf_vectors(
        ef_runtime,
        {str(mapping["ef_flow_uuid"]) for mapping in all_mappings},
        _ef_indicator,
    )
    results: list[dict[str, object]] = []
    for mapping in all_mappings:
        eco_flow = flow_records[mapping["ecoinvent_flow_uuid"]]
        ef_flow = flow_records[mapping["ef_flow_uuid"]]
        eco_context = mapping["ecoinvent_context"]
        ef_context = mapping["ef_context"]
        metadata_ok = all(
            (
                str(eco_flow["flow_name_en"] or eco_flow["flow_name"]).casefold()
                == str(mapping["ecoinvent_flow_name"]).casefold(),
                str(ef_flow["flow_name_en"] or ef_flow["flow_name"]).casefold()
                == str(mapping["ef_flow_name"]).casefold(),
                str(eco_flow["default_unit"]) == mapping["unit"],
                str(ef_flow["default_unit"]) == mapping["unit"],
                "elementary" in str(eco_flow["flow_type"]).casefold(),
                "elementary" in str(ef_flow["flow_type"]).casefold(),
                "ecoinvent" in str(eco_flow["source"]).casefold(),
                str(ef_flow["source"]).casefold() == "ef3.1",
                str(eco_flow["compartment"] or "") == eco_context["compartment"],
                str(eco_flow["subcompartment"] or "") == eco_context["subcompartment"],
                str(ef_flow["compartment"] or "") == ef_context["catalog_compartment"],
                (ef_flow["subcompartment"] or None) == ef_context["catalog_subcompartment"],
            )
        )
        context_evidence_ok = masterdata_contexts[mapping["ecoinvent_flow_uuid"]] == {
            "compartment": eco_context["compartment"],
            "subcompartment": eco_context["subcompartment"],
        }
        eco_vector = eco_vectors[mapping["ecoinvent_flow_uuid"]]
        ef_vector = ef_vectors[mapping["ef_flow_uuid"]]
        expected_vector = {
            key: float(mapping["cf_vector"][f"climate_{key}"])
            for key in CLIMATE_KEYS
        }
        is_one_way = mapping in one_way_mappings
        if is_one_way:
            amount_factor = float(mapping["amount_factor"])
            cf_equal = all(
                math.isclose(ef_vector[key], amount_factor * eco_vector[key], rel_tol=1e-12, abs_tol=1e-15)
                and math.isclose(eco_vector[key], expected_vector[key], rel_tol=1e-12, abs_tol=1e-15)
                for key in CLIMATE_KEYS
            )
            amount_checks = {"amount_conversion": math.isfinite(amount * amount_factor)}
            amounts = {
                "ef_alias_input": amount,
                "ecoinvent_output": amount * amount_factor,
            }
        else:
            forward_factor = float(mapping["eco_to_ef_factor"])
            reverse_factor = float(mapping["ef_to_eco_factor"])
            cf_equal = all(
                math.isclose(eco_vector[key], forward_factor * ef_vector[key], rel_tol=1e-12, abs_tol=1e-15)
                and math.isclose(eco_vector[key], expected_vector[key], rel_tol=1e-12, abs_tol=1e-15)
                for key in CLIMATE_KEYS
            )
            ef_amount = amount * forward_factor
            roundtrip_amount = ef_amount * reverse_factor
            amount_checks = {
                "factors_reciprocal": math.isclose(forward_factor * reverse_factor, 1.0, rel_tol=1e-12),
                "amount_roundtrip": math.isclose(amount, roundtrip_amount, rel_tol=1e-12, abs_tol=1e-15),
            }
            amounts = {
                "ecoinvent_input": amount,
                "ef_output": ef_amount,
                "ecoinvent_roundtrip": roundtrip_amount,
            }
        results.append(
            {
                "mapping_mode": "one_way_alias" if is_one_way else "bidirectional",
                "ecoinvent_flow_uuid": mapping["ecoinvent_flow_uuid"],
                "ef_flow_uuid": mapping["ef_flow_uuid"],
                "checks": {
                    "metadata": metadata_ok,
                    "context_evidence": context_evidence_ok,
                    "cf_vector_equal": cf_equal,
                    **amount_checks,
                },
                "cf_vectors": {"ecoinvent": eco_vector, "ef": ef_vector},
                "amounts": amounts,
            }
        )
    mapping_checks_ok = all(all(row["checks"].values()) for row in results)
    passed = review_ok and evidence_ok and mapping_checks_ok
    return {
        "status": "passed" if passed else "failed",
        "mapping_package": f"{package['package_id']}:{package['package_version']}",
        "mapping_count": len(mappings),
        "one_way_mapping_count": len(one_way_mappings),
        "database": str(database),
        "checks": {
            "review_approved": review_ok,
            "evidence_artifacts": evidence_ok,
            "all_mappings": mapping_checks_ok,
        },
        "evidence_checks": evidence_checks,
        "mappings": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING)
    parser.add_argument("--ecoinvent-runtime", type=Path, default=DEFAULT_ECO_RUNTIME)
    parser.add_argument("--ef-runtime", type=Path, default=DEFAULT_EF_RUNTIME)
    parser.add_argument("--ecoinvent-masterdata", type=Path, default=DEFAULT_ECO_MASTERDATA)
    parser.add_argument("--amount", type=float, default=1.0)
    args = parser.parse_args()
    result = verify(
        args.database,
        args.mapping,
        args.ecoinvent_runtime,
        args.ef_runtime,
        args.ecoinvent_masterdata,
        args.amount,
    )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
