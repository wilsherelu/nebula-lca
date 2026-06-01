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

from app.config import settings
from app.ecoinvent_ef31_loader import (
    parse_lcia_excel,
    match_cf_to_flows as _match_cf_to_flows,
)
from app.services.ef31_runtime_csv import (
    ACTIVE_MANIFEST_NAME,
    DEFAULT_EF31_RUNTIME_ROOT,
    should_update_active_manifest,
)

logger = logging.getLogger(__name__)

_RUNTIME_LCIA_ROOT = Path(settings.nebula_lca_runtime_root) / "lcia"


def _field(obj, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def generate_lcia_runtime_artifact(
    lcia_excel_path: str | Path,
    elementary_flows: list[dict],
    output_root: Optional[Path] = None,
    activate: bool = False,
    force_activate: bool = False,
) -> dict:
    """Generate LCIA runtime artifacts from a raw LCIA Excel file.

    Steps:
    1. Parse LCIA Excel → indicators + CFs
    2. Match CFs to elementary flows
    3. Write flow_index.csv, indicator_index.csv, lcia_factors.csv
    4. Optionally write active_manifest.json when activate=True

    Returns:
        Manifest dict with flows_count, indicators_count, factors_count, etc.
    """
    lcia_path = Path(lcia_excel_path)
    if not lcia_path.exists():
        raise FileNotFoundError(f"LCIA Excel not found: {lcia_path}")

    # Parse LCIA Excel. Keep all ecoinvent LCIA method families in the runtime;
    # EF-only compatibility is enforced at request time for TIDAS/ILCD flow spaces.
    indicators, all_cfs = parse_lcia_excel(lcia_path)

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
        obj.method = _field(cf, "method", "")
        obj.category = _field(cf, "category", "")
        obj.indicator = _field(cf, "indicator", "")
        obj.flow_name = _field(cf, "flow_name", "")
        obj.compartment = _field(cf, "compartment", "")
        obj.subcompartment = _field(cf, "subcompartment", "")
        obj.cf_value = _field(cf, "cf_value", 0.0)
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
        flow_key = m.get("cf_flow_name", "")
        flow_uuid = m.get("matched_flow_uuid")
        matched_with_uuid.append({
            "method": m.get("cf_method", ""),
            "category": m.get("cf_category", ""),
            "indicator": m.get("cf_indicator", ""),
            "flow_name": flow_key,
            "flow_uuid": flow_uuid,
            "cf_value": m.get("cf_value", 0.0),
        })

    # Generate output
    output_dir = output_root or (DEFAULT_EF31_RUNTIME_ROOT / f"lcia-{str(uuid.uuid4())[:12]}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write flow_index.csv
    flow_index_path = output_dir / "flow_index.csv"
    flow_uuid_to_index: dict[str, int] = {}
    with open(flow_index_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        seen_uuids = set()
        idx = 0
        for ef in elementary_flows:
            fuuid = ef.get("flow_uuid", "")
            if fuuid and fuuid not in seen_uuids:
                seen_uuids.add(fuuid)
                flow_uuid_to_index[fuuid] = idx
                writer.writerow([idx, fuuid, ef.get("flow_name", "")])
                idx += 1

    # Write indicator_index.csv
    indicator_index_path = output_dir / "indicator_index.csv"
    indicator_by_key = {
        (_field(ind, "method", ""), _field(ind, "category", ""), _field(ind, "indicator", "")): ind
        for ind in indicators
    }
    matched_indicator_keys = []
    seen_indicator_keys = set()
    for m in matched_with_uuid:
        key = (m.get("method", ""), m.get("category", ""), m.get("indicator", ""))
        if key in indicator_by_key and key not in seen_indicator_keys:
            matched_indicator_keys.append(key)
            seen_indicator_keys.add(key)

    with open(indicator_index_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "indicator_index", "method_en", "method_zh",
            "indicator_en", "indicator_zh", "ecoinvent_category",
        ])
        for idx, key in enumerate(matched_indicator_keys):
            method, category, indicator = key
            writer.writerow([idx, method, method, indicator, indicator, category])

    # Write lcia_factors.csv
    lcia_factors_path = output_dir / "lcia_factors.csv"
    with open(lcia_factors_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        # Build indicator index lookup
        indicator_index = {key: idx for idx, key in enumerate(matched_indicator_keys)}

        factors_count = 0
        for m in matched_with_uuid:
            flow_idx = flow_uuid_to_index.get(str(m.get("flow_uuid") or ""))
            if flow_idx is None:
                continue
            method = m.get("method", "")
            indicator = m.get("indicator", "")
            category = m.get("category", "")
            key = (method, category, indicator)
            row_idx = indicator_index.get(key)
            if row_idx is None:
                continue
            writer.writerow([row_idx, flow_idx, m.get("cf_value", 0.0)])
            factors_count += 1

    # Build manifest
    flows_count = len(seen_uuids)
    indicators_count = len(indicator_index)
    unmatched_count = len(unmatched)
    ambiguous_count = len(ambiguous)

    manifest = {
        "runtime_schema_version": "lcia-runtime-artifact-v1",
        "runtime_id": str(uuid.uuid4()),
        "job_id": output_dir.name,
        "output_dir": str(output_dir),
        "artifact_dir": str(output_dir),
        "active": False,
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

    should_activate = bool(activate) and (force_activate or should_update_active_manifest(output_dir.parent, manifest))
    manifest["active"] = should_activate
    manifest_text = json.dumps(manifest, ensure_ascii=False, default=str)
    (output_dir / "manifest.json").write_text(manifest_text, encoding="utf-8")
    (output_dir / "runtime_summary.json").write_text(manifest_text, encoding="utf-8")
    (output_dir / "active_manifest.json").write_text(manifest_text, encoding="utf-8")
    if should_activate:
        (output_dir.parent / ACTIVE_MANIFEST_NAME).write_text(manifest_text, encoding="utf-8")

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
        writer = csv.writer(f, delimiter=";")
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
        writer = csv.writer(f, delimiter=";")
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
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        factors_count = 0
        for m in matched:
            flow_uuid = m.get("matched_flow_uuid") or m.get("flow_uuid") or ""
            if not flow_uuid:
                continue
            flow_idx = None
            for idx, ef in enumerate(elementary_flows):
                if ef.get("flow_uuid") == flow_uuid:
                    flow_idx = idx
                    break
            if flow_idx is None:
                continue
            method = m.get("method", "")
            indicator = m.get("indicator", "")
            category = m.get("category", "")
            row_idx = indicator_index.get((method, category, indicator))
            if row_idx is None:
                continue
            writer.writerow([row_idx, flow_idx, m.get("cf_value", 0.0)])
            factors_count += 1

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
        "factors_count": factors_count,
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
