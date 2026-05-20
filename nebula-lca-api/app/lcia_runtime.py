"""LCIA runtime artifact generator.

Wraps the existing ef31_runtime_csv service to generate runtime artifacts
specifically from LCIA factor uploads.

This enables:
- LCIA factor .xlsx upload → runtime manifest with flow/indicator/factor counts
- All-method CF matching results (cf_matches_all.json) used by solver
"""

from __future__ import annotations

import json
import logging
import csv
import uuid
from pathlib import Path
from typing import Optional

from app.config import PROJECT_ROOT
from app.ecoinvent_ef31_loader import (
    parse_lcia_excel,
    filter_cf_ef31,
    match_cf_to_flows as _match_cf_to_flows,
)

logger = logging.getLogger(__name__)

_RUNTIME_LCIA_ROOT = PROJECT_ROOT / "runtime" / "lcia"


def generate_lcia_runtime_artifact(
    lcia_excel_path: str | Path,
    elementary_flows: list[dict],
    output_root: Optional[Path] = None,
) -> dict:
    """Generate LCIA runtime artifacts from a raw LCIA Excel file.

    Steps:
    1. Parse LCIA Excel → indicators + CFs
    2. Match CFs to elementary flows
    3. Write flow_index.csv, indicator_index.csv, lcia_factors.csv
    4. Write active_manifest.json

    Returns:
        Manifest dict with flows_count, indicators_count, factors_count, etc.
    """
    lcia_path = Path(lcia_excel_path)
    if not lcia_path.exists():
        raise FileNotFoundError(f"LCIA Excel not found: {lcia_path}")

    # Parse LCIA Excel
    indicators, all_cfs = parse_lcia_excel(lcia_path)
    ef31_cfs = filter_cf_ef31(all_cfs)

    # Build flow lookup
    flow_lookup = {
        f.get("flow_uuid"): f
        for f in elementary_flows
        if f.get("flow_uuid")
    }

    # Match CFs to flows
    # Convert CFs to format expected by match_cf_to_flows
    cf_objects = []
    for cf in all_cfs:
        class _Cf:
            pass
        obj = _Cf()
        obj.method = cf.get("method", "")
        obj.category = cf.get("category", "")
        obj.indicator = cf.get("indicator", "")
        obj.flow_name = cf.get("flow_name", "")
        obj.compartment = cf.get("compartment", "")
        obj.subcompartment = cf.get("subcompartment", "")
        obj.cf_value = cf.get("cf_value", 0.0)
        cf_objects.append(obj)

    elem_flow_objects = []
    for ef in elementary_flows:
        class _Ef:
            pass
        obj = _Ef()
        obj.flow_uuid = ef.get("flow_uuid", "")
        obj.flow_name = ef.get("flow_name", "")
        obj.compartment = ef.get("compartment", "")
        obj.subcompartment = ef.get("subcompartment", "")
        elem_flow_objects.append(obj)

    matched, unmatched, ambiguous = _match_cf_to_flows(cf_objects, elem_flow_objects)

    # Build matched list with flow UUID lookup
    matched_with_uuid = []
    for m in matched:
        flow_key = m.get("flow_name", "")
        flow_uuid = None
        for ef in elementary_flows:
            if ef.get("flow_name") == flow_key or ef.get("flow_uuid") == flow_key:
                flow_uuid = ef.get("flow_uuid")
                break
        matched_with_uuid.append({
            "method": m.get("method", ""),
            "indicator": m.get("indicator", ""),
            "flow_name": flow_key,
            "flow_uuid": flow_uuid,
            "cf_value": m.get("cf_value", 0.0),
        })

    # Generate output
    output_dir = output_root or (_RUNTIME_LCIA_ROOT / str(uuid.uuid4())[:12])
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write flow_index.csv
    flow_index_path = output_dir / "flow_index.csv"
    with open(flow_index_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        seen_uuids = set()
        idx = 0
        for ef in elementary_flows:
            fuuid = ef.get("flow_uuid", "")
            if fuuid and fuuid not in seen_uuids:
                seen_uuids.add(fuuid)
                writer.writerow([idx, fuuid, ef.get("flow_name", "")])
                idx += 1

    # Write indicator_index.csv
    indicator_index_path = output_dir / "indicator_index.csv"
    with open(indicator_index_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "indicator_index", "method_en", "method_zh",
            "indicator_en", "indicator_zh", "ecoinvent_category",
        ])
        idx = 0
        seen_methods = set()
        for ind in indicators:
            method_key = (ind.get("method"), ind.get("category"), ind.get("indicator"))
            if method_key in seen_methods:
                continue
            seen_methods.add(method_key)
            method = ind.get("method", "")
            category = ind.get("category", "")
            indicator = ind.get("indicator", "")
            # Simple split for method en/zh
            parts = method.split(" ", 1)
            method_en = parts[0] if parts else method
            method_zh = parts[1] if len(parts) > 1 else ""
            ind_en = indicator
            ind_zh = ""
            writer.writerow([idx, method_en, method_zh, ind_en, ind_zh, category])
            idx += 1

    # Write lcia_factors.csv
    lcia_factors_path = output_dir / "lcia_factors.csv"
    with open(lcia_factors_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["row", "column", "coefficient"])
        row_idx = 0
        # Build indicator index lookup
        indicator_index = {}
        for ind in indicators:
            method_key = (ind.get("method"), ind.get("category"), ind.get("indicator"))
            if method_key not in indicator_index:
                indicator_index[method_key] = len(indicator_index)

        for m in matched_with_uuid:
            method = m.get("method", "")
            indicator = m.get("indicator", "")
            category = ""
            for ind in indicators:
                if ind.get("method") == method and ind.get("indicator") == indicator:
                    category = ind.get("category", "")
                    break
            key = (method, category, indicator)
            col_idx = indicator_index.get(key, 0)
            writer.writerow([row_idx, col_idx, m.get("cf_value", 0.0)])
            row_idx += 1

    # Build manifest
    flows_count = len(seen_uuids)
    indicators_count = len(indicator_index)
    factors_count = row_idx
    unmatched_count = len(unmatched)
    ambiguous_count = len(ambiguous)

    manifest = {
        "runtime_schema_version": "lcia-runtime-artifact-v1",
        "runtime_id": str(uuid.uuid4()),
        "output_dir": str(output_dir),
        "active": True,
        "files": {
            "flow_index": "flow_index.csv",
            "indicator_index": "indicator_index.csv",
            "lcia_factors": "lcia_factors.csv",
        },
        "flows_count": flows_count,
        "indicators_count": indicators_count,
        "factors_count": factors_count,
        "cf_matched": len(matched_with_uuid),
        "cf_unmatched": unmatched_count,
        "cf_ambiguous": ambiguous_count,
        "generated_at": str(uuid.uuid1()),
    }

    manifest_path = output_dir / "active_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    logger.info(
        f"LCIA runtime generated: {flows_count} flows, "
        f"{indicators_count} indicators, {factors_count} factors "
        f"in {output_dir}"
    )

    return manifest


def generate_lcia_runtime_from_job(
    job_id: str,
    _job_dir_override: Optional[Path] = None,
) -> dict:
    """Generate LCIA runtime CSVs from an existing EF3.1 import job."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))
    from app.ef31_import_job_service import _IMPORT_CACHE_ROOT
    from app.services.ef31_runtime_csv import generate_ef31_runtime_csvs

    job_dir = _job_dir_override or (_IMPORT_CACHE_ROOT / job_id)
    if not job_dir.exists():
        raise FileNotFoundError(f"Job directory not found: {job_dir}")

    if not (job_dir / "cf_matches_all.json").exists() or not (job_dir / "indicators.json").exists():
        raise FileNotFoundError(
            f"LCIA artifacts not found in job: {job_id}. "
            "Ensure LCIA Excel was parsed during preview."
        )
    return generate_ef31_runtime_csvs(job_id, overwrite=True, _job_dir_override=job_dir)


def generate_lcia_runtime_artifact_from_artifacts(
    indicators: list[dict],
    matched: list[dict],
    unmatched: list[dict],
    ambiguous: list[dict],
    elementary_flows: list[dict],
    output_root: Path | None = None,
) -> dict:
    """Generate runtime CSVs from already-matched LCIA artifacts.

    This is a lighter path that skips CF matching (already done).
    """
    output_dir = output_root or (_RUNTIME_LCIA_ROOT / str(uuid.uuid4())[:12])
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write flow_index.csv
    flow_index_path = output_dir / "flow_index.csv"
    with open(flow_index_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        idx = 0
        seen = set()
        for ef in elementary_flows:
            fuuid = ef.get("flow_uuid", "")
            if fuuid and fuuid not in seen:
                seen.add(fuuid)
                writer.writerow([idx, fuuid, ef.get("flow_name", "")])
                idx += 1

    # Write indicator_index.csv
    indicator_index_path = output_dir / "indicator_index.csv"
    indicator_index: dict[tuple, int] = {}
    with open(indicator_index_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "indicator_index", "method_en", "method_zh",
            "indicator_en", "indicator_zh", "ecoinvent_category",
        ])
        for ind in indicators:
            key = (ind.get("method"), ind.get("category"), ind.get("indicator"))
            if key in indicator_index:
                continue
            indicator_index[key] = len(indicator_index)
            method = ind.get("method", "")
            parts = method.split(" ", 1)
            writer.writerow([
                indicator_index[key],
                parts[0] if parts else method,
                parts[1] if len(parts) > 1 else "",
                ind.get("indicator", ""),
                "",
                ind.get("category", ""),
            ])

    # Write lcia_factors.csv
    lcia_factors_path = output_dir / "lcia_factors.csv"
    with open(lcia_factors_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["row", "column", "coefficient"])
        row_idx = 0
        for m in matched:
            method = m.get("method", "")
            indicator = m.get("indicator", "")
            category = ""
            for ind in indicators:
                if ind.get("method") == method and ind.get("indicator") == indicator:
                    category = ind.get("category", "")
                    break
            col_idx = indicator_index.get((method, category, indicator), 0)
            writer.writerow([row_idx, col_idx, m.get("cf_value", 0.0)])
            row_idx += 1

    # Manifest
    manifest = {
        "runtime_schema_version": "lcia-runtime-artifact-v1",
        "runtime_id": str(uuid.uuid4()),
        "output_dir": str(output_dir),
        "active": True,
        "files": {
            "flow_index": "flow_index.csv",
            "indicator_index": "indicator_index.csv",
            "lcia_factors": "lcia_factors.csv",
        },
        "flows_count": len(seen),
        "indicators_count": len(indicator_index),
        "factors_count": row_idx,
        "cf_matched": len(matched),
        "cf_unmatched": len(unmatched),
        "cf_ambiguous": len(ambiguous),
        "generated_at": str(uuid.uuid1()),
    }

    (output_dir / "active_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    return manifest
