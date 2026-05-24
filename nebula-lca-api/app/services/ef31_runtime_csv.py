"""EF 3.1 runtime CSV generator.

Transforms parsed LCIA artifacts (indicators, CFs, match results) from an
ef31 import job into the three CSV files that the solver expects:

    flow_index.csv      — flow_index, FlowUUID, FlowName
    indicator_index.csv  — indicator_index, method_en, method_zh, indicator_en, indicator_zh, ecoinvent_category
    lcia_factors.csv     — row, column, coefficient

Output directory is placed under ``nebula-lca-api/runtime/ef31/{job_id}/`` by
default.  This directory is a managed local runtime artifact, not source data.
"""
from __future__ import annotations

import csv
import json
import logging
import uuid
from pathlib import Path
from typing import Optional

from app.config import PROJECT_ROOT

logger = logging.getLogger(__name__)

DEFAULT_EF31_RUNTIME_ROOT = PROJECT_ROOT / "runtime" / "ef31"
ACTIVE_MANIFEST_NAME = "active_manifest.json"


def should_update_active_manifest(output_root: Path, summary: dict) -> bool:
    """Avoid replacing a broad default EF3.1 runtime with a tiny preview runtime."""
    try:
        if output_root.resolve() != DEFAULT_EF31_RUNTIME_ROOT.resolve():
            return True
        active_path = output_root / ACTIVE_MANIFEST_NAME
        if not active_path.exists():
            return True
        active = json.loads(active_path.read_text(encoding="utf-8"))
        active_flows = int(active.get("flows_count") or 0)
        active_indicators = int(active.get("indicators_count") or 0)
        active_factors = int(active.get("factors_count") or 0)
        new_flows = int(summary.get("flows_count") or 0)
        new_indicators = int(summary.get("indicators_count") or 0)
        new_factors = int(summary.get("factors_count") or 0)
        if active_flows >= 1000 and active_indicators >= 10 and active_factors >= 1000:
            return (
                new_flows >= active_flows
                and new_indicators >= active_indicators
                and new_factors >= active_factors
            )
    except Exception:
        return True
    return True


def generate_ef31_runtime_csvs(
    job_id: str,
    output_root: Optional[Path] = None,
    overwrite: bool = False,
    activate: bool = False,
    _job_dir_override: Optional[Path] = None,
) -> dict:
    """Generate solver-compatible EF 3.1 runtime CSV files from a preview job.

    Args:
        job_id: The ef31 import job id (used to find artifacts).
        output_root: Where to write the CSVs.  Defaults to
            ``nebula-lca-api/runtime/ef31/{job_id}/``.
        overwrite: If False, raises ValueError when output dir already exists.
        activate: If True, update ``active_manifest.json`` after generation.
            Defaults to False so preview/test artifacts cannot silently replace
            the active production runtime.
        _job_dir_override: Internal test-only parameter to override where
            job artifacts are read from (bypasses _IMPORT_CACHE_ROOT).

    Returns:
        Summary dict with counts of flows, indicators, factors written.
    """
    # Resolve input path (job directory)
    if _job_dir_override is not None:
        job_dir = _job_dir_override
    else:
        from app.ef31_import_job_service import _IMPORT_CACHE_ROOT
        job_dir = _IMPORT_CACHE_ROOT / job_id
    if not job_dir.exists():
        raise FileNotFoundError(f"Job directory not found: {job_dir}")

    indicators_path = job_dir / "indicators.json"
    cfs_path = job_dir / "all_cfs.json"
    matches_path = job_dir / "cf_matches_all.json"
    if not cfs_path.exists():
        cfs_path = job_dir / "ef31_cfs.json"
    if not matches_path.exists():
        matches_path = job_dir / "cf_matches.json"
    elementary_flows_path = job_dir / "elementary_flows.json"

    if not indicators_path.exists():
        raise FileNotFoundError(f"indicators.json not found in job dir: {job_dir}")
    if not cfs_path.exists():
        raise FileNotFoundError(f"all_cfs.json or ef31_cfs.json not found in job dir: {job_dir}")
    if not matches_path.exists():
        raise FileNotFoundError(f"cf_matches.json not found in job dir: {job_dir}")

    # Resolve output path
    if output_root is None:
        output_root = DEFAULT_EF31_RUNTIME_ROOT
    output_dir = output_root / job_id
    if output_dir.exists() and not overwrite:
        raise FileExistsError(
            f"Output directory already exists: {output_dir}. "
            "Set overwrite=True to regenerate."
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load artifacts
    indicators = json.loads(indicators_path.read_text(encoding="utf-8"))
    ef31_cfs = json.loads(cfs_path.read_text(encoding="utf-8"))
    matches = json.loads(matches_path.read_text(encoding="utf-8"))

    matched = matches.get("matched", [])
    unmatched = matches.get("unmatched", [])
    ambiguous = matches.get("ambiguous", [])

    # --- Build flow index from matched CFs ---
    # We need flow_uuid -> flow_name mapping.  The matched CFs contain
    # matched_flow_uuid and matched_flow_name.
    flow_uuid_to_name: dict[str, str] = {}
    for m in matched:
        uuid_val = m.get("matched_flow_uuid", "")
        name = m.get("matched_flow_name", "")
        if uuid_val and name:
            flow_uuid_to_name[uuid_val] = name

    # Also load elementary_flows.json if available (has full flow list)
    if elementary_flows_path.exists():
        elem_flows = json.loads(elementary_flows_path.read_text(encoding="utf-8"))
        for ef in elem_flows:
            uuid_val = ef.get("flow_uuid", "")
            name = ef.get("flow_name", "")
            if uuid_val and name:
                flow_uuid_to_name.setdefault(uuid_val, name)

    # Write flow_index.csv
    flow_uuids = sorted(flow_uuid_to_name.keys())
    flow_index_path = output_dir / "flow_index.csv"
    with open(flow_index_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        for idx, uuid_val in enumerate(flow_uuids):
            writer.writerow([idx, uuid_val, flow_uuid_to_name.get(uuid_val, "")])

    # Build flow_uuid_to_index map
    flow_uuid_to_index = {uuid: idx for idx, uuid in enumerate(flow_uuids)}

    # --- Build indicator index ---
    # indicators list has: method, category, indicator, indicator_unit.
    # Category is part of the identity: EF3.1 reuses some indicator names
    # across different impact categories.  Older test/job artifacts may lack
    # cf_category; only resolve those when method+indicator is unique.
    indicator_by_key: dict[tuple, dict] = {}
    indicator_candidates: dict[tuple, list[tuple]] = {}
    for ind in indicators:
        key = (ind.get("method", ""), ind.get("category", ""), ind.get("indicator", ""))
        indicator_by_key[key] = ind
        indicator_candidates.setdefault((key[0], key[2]), []).append(key)

    def _resolve_indicator_key(row: dict) -> tuple | None:
        method = row.get("cf_method", "")
        category = row.get("cf_category", "")
        indicator = row.get("cf_indicator", "")
        key = (method, category, indicator)
        if category or key in indicator_by_key:
            return key if key in indicator_by_key else None
        candidates = indicator_candidates.get((method, indicator), [])
        return candidates[0] if len(candidates) == 1 else None

    indicator_map: dict[tuple, dict] = {}
    matched_indicator_keys = {
        key for key in (_resolve_indicator_key(m) for m in matched) if key is not None
    }
    for key in matched_indicator_keys:
        ind = indicator_by_key[key]
        if key not in indicator_map:
            indicator_map[key] = {
                "method_en": ind.get("method", ""),
                "method_zh": ind.get("method", ""),
                "indicator_en": ind.get("indicator", ""),
                "indicator_zh": ind.get("indicator", ""),
                "ecoinvent_category": ind.get("category", ""),
                "indicator_unit": ind.get("indicator_unit", ""),
            }

    indicator_keys = sorted(indicator_map.keys())
    indicator_index_path = output_dir / "indicator_index.csv"
    with open(indicator_index_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "indicator_index", "method_en", "method_zh",
            "indicator_en", "indicator_zh", "ecoinvent_category",
        ])
        for idx, key in enumerate(indicator_keys):
            info = indicator_map[key]
            writer.writerow([
                idx,
                info["method_en"],
                info["method_zh"],
                info["indicator_en"],
                info["indicator_zh"],
                info["ecoinvent_category"],
            ])

    indicator_key_to_index = {key: idx for idx, key in enumerate(indicator_keys)}

    # --- Build lcia_factors.csv ---
    # Only include matched CFs (unmatched and ambiguous are skipped).
    lcia_factors_path = output_dir / "lcia_factors.csv"
    factor_count = 0
    with open(lcia_factors_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        for m in matched:
            method = m.get("cf_method", "")
            indicator = m.get("cf_indicator", "")
            cf_value = m.get("cf_value", 0)
            if cf_value is None or cf_value == 0:
                continue
            flow_uuid = m.get("matched_flow_uuid", "")
            flow_idx = flow_uuid_to_index.get(flow_uuid)
            if flow_idx is None:
                continue

            ind_key = _resolve_indicator_key(m)
            row_idx = indicator_key_to_index.get(ind_key)
            if row_idx is None:
                continue

            writer.writerow([row_idx, flow_idx, cf_value])
            factor_count += 1

    # Write manifest / summary.  runtime_summary.json is kept for backward
    # compatibility with existing UI/tests; manifest.json is the managed
    # artifact contract going forward.
    summary = {
        "runtime_schema_version": "ef31-runtime-artifact-v1",
        "runtime_id": job_id,
        "job_id": job_id,
        "output_dir": str(output_dir),
        "artifact_dir": str(output_dir),
        "active": False,
        "files": {
            "flow_index": "flow_index.csv",
            "indicator_index": "indicator_index.csv",
            "lcia_factors": "lcia_factors.csv",
        },
        "flows_count": len(flow_uuids),
        "indicators_count": len(indicator_keys),
        "factors_count": factor_count,
        "cf_matched": len(matched),
        "cf_unmatched": len(unmatched),
        "cf_ambiguous": len(ambiguous),
    }
    should_activate = bool(activate) and should_update_active_manifest(output_root, summary)
    summary["active"] = should_activate
    with open(output_dir / "runtime_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    with open(output_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    if should_activate:
        with open(output_root / ACTIVE_MANIFEST_NAME, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

    logger.info(
        "Generated EF3.1 runtime CSVs for job %s: %d flows, %d indicators, %d factors",
        job_id,
        len(flow_uuids),
        len(indicator_keys),
        factor_count,
    )
    return summary


# CLI entry point
def cmd_generate_runtime_csv(args):
    """CLI command to generate EF 3.1 runtime CSVs from a preview job."""
    job_id = args.job_id
    output_dir = getattr(args, "output_dir", None)
    overwrite = getattr(args, "overwrite", False)

    output_root = Path(output_dir) if output_dir else None
    try:
        result = generate_ef31_runtime_csvs(
            job_id=job_id,
            output_root=output_root,
            overwrite=overwrite,
        )
        print(f"\n=== EF 3.1 Runtime CSV Generation ===")
        print(f"Job ID: {job_id}")
        print(f"Output: {result['output_dir']}")
        print(f"Flows: {result['flows_count']}")
        print(f"Indicators: {result['indicators_count']}")
        print(f"Factors: {result['factors_count']}")
        print(f"CF matched: {result['cf_matched']}, unmatched: {result['cf_unmatched']}, ambiguous: {result['cf_ambiguous']}")
    except (FileNotFoundError, FileExistsError) as e:
        print(f"ERROR: {e}", file=__import__("sys").stderr)
        __import__("sys").exit(1)
