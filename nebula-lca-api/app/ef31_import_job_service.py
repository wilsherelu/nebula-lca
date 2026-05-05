"""EF 3.1 LCI Import Job Service.

Handles the upload → selective extract → parse → preview → commit pipeline
for ecoinvent EF 3.1 LCI data.

Job artifacts are stored in `import-cache/ef31_jobs/{job_id}/` and persist
for 24 hours.  Reports are also persisted via DebugDiagnostic for audit.
"""

import json
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from app.config import settings
from app.ecoinvent_ef31_loader import (
    selective_extract_7z,
    parse_spold_file,
    parse_spold_exchanges,
    parse_units,
    parse_unit_conversions,
    parse_elementary_exchanges,
    parse_intermediate_exchanges,
    parse_lcia_excel,
    filter_cf_ef31,
    FoundationReport,
    LCIDataset,
    LCIElementaryExchange,
    ElementaryFlow,
    IntermediateFlow,
    UnitRecord,
    Indicator,
    CharacterizationFactor,
)
from app.ef31_db_service import (
    dry_run_lci_import,
    commit_lci_import,
    DbDryRunResult,
    DbCommitResult,
)

_IMPORT_CACHE_ROOT = Path("import-cache") / "ef31_jobs"
_JOB_EXPIRY_HOURS = 24


def _ensure_job_dir(job_id: str) -> Path:
    """Create (or return) the job directory, cleaning up expired jobs."""
    job_dir = _IMPORT_CACHE_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    # Clean up any expired jobs in the parent
    _cleanup_expired_jobs(_IMPORT_CACHE_ROOT)
    return job_dir


def _cleanup_expired_jobs(root: Path) -> None:
    """Delete job dirs older than JOB_EXPIRY_HOURS."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_JOB_EXPIRY_HOURS)
    if not root.exists():
        return
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        try:
            mtime = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                import shutil
                shutil.rmtree(entry, ignore_errors=True)
        except OSError:
            pass


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, default=str), encoding="utf-8")


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")


def _find_first_dir(root: Path, name: str) -> Optional[Path]:
    """Find a named directory below root, including one archive root folder."""
    direct = root / name
    if direct.is_dir():
        return direct
    for item in root.rglob(name):
        if item.is_dir():
            return item
    return None


def _dataset_to_dict(ds: LCIDataset) -> dict:
    return {
        "filename": ds.filename,
        "activity_id": ds.activity_id,
        "activity_name": ds.activity_name,
        "location": ds.location,
        "reference_product_name": ds.reference_product_name,
        "reference_product_unit": ds.reference_product_unit,
        "reference_product_amount": ds.reference_product_amount,
        "exchange_count": ds.exchange_count,
        "reference_product_id": ds.reference_product_id,
    }


def _exchange_to_dict(exc: LCIElementaryExchange) -> dict:
    return {
        "dataset_filename": exc.dataset_filename,
        "exchange_id": exc.exchange_id,
        "exchange_name": exc.exchange_name,
        "unit": exc.unit,
        "direction": exc.direction,
        "amount": exc.amount,
        "compartment": getattr(exc, "compartment", ""),
        "subcompartment": getattr(exc, "subcompartment", ""),
        "CAS": getattr(exc, "CAS", ""),
        "formula": getattr(exc, "formula", ""),
    }


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def preview_ef31_import(
    lci_archive_path: Path,
    lcia_archive_path: Optional[Path] = None,
    limit: int = 100,
) -> tuple[str, dict]:
    """Execute a preview of EF 3.1 LCI import.

    Steps:
    1. Create job dir, save uploaded archives.
    2. Selective extract: MasterData XML, limited SPOLDs, LCIA Excel.
    3. Parse foundation, units, MasterData flows, LCI datasets & exchanges.
    4. Save job artifacts (manifest, foundation, datasets, exchanges).
    5. Run DB dry-run.
    6. Return (job_id, response_dict).

    The response dict matches Ef31ImportPreviewResponse shape.
    """
    job_id = str(uuid.uuid4())
    job_dir = _ensure_job_dir(job_id)
    uploads_dir = job_dir / "uploads"
    uploads_dir.mkdir(exist_ok=True)
    extract_dir = job_dir / "extracted"

    # Save uploaded archives
    lci_dest = uploads_dir / lci_archive_path.name
    shutil.copy2(lci_archive_path, lci_dest)

    lcia_dest = None
    if lcia_archive_path and lcia_archive_path.exists():
        lcia_dest = uploads_dir / lcia_archive_path.name
        shutil.copy2(lcia_archive_path, lcia_dest)

    # ---- Selective extract LCI archive ----
    extract_result = selective_extract_7z(
        lci_dest, extract_dir, spold_limit=limit
    )

    master_dir = extract_result.get("master_dir")
    datasets_dir = extract_result.get("datasets_dir")
    lcia_excel = extract_result.get("lcia_excel")
    spold_count = extract_result.get("spold_count", 0)
    spold_count_total = extract_result.get("spold_count_total", 0)

    # ---- Handle optional LCIA archive/workbook ----
    # lcia_excel from LCI archive scanning is already set above.
    # If user separately uploaded LCIA archive, process it now.
    if lcia_archive_path and lcia_archive_path.exists() and not lcia_excel:
        ext = lcia_archive_path.name.lower()
        if ext.endswith('.xlsx'):
            # Direct xlsx — use it directly
            lcia_excel = lcia_archive_path
        elif ext.endswith('.7z'):
            # Selective extract LCIA archive (only LCIA Excel, no spold limit)
            lcia_extract_dir = job_dir / "lcia_extracted"
            lcia_extract_result = selective_extract_7z(
                lcia_archive_path, lcia_extract_dir, spold_limit=0
            )
            lcia_excel = lcia_extract_result.get("lcia_excel")

    warnings: list[str] = []
    errors: list[str] = []

    # ---- Build archive_file_discovery ----
    archive_file_discovery = {
        "master_data_files": extract_result.get("master_data_count", 0),
        "datasets_spold_files_selected": spold_count,
        "datasets_spold_files_total": spold_count_total,
        "lcia_excel_found": 1 if lcia_excel else 0,
    }

    # Parse units
    units_map: dict[str, UnitRecord] = {}
    if master_dir:
        try:
            units_map = parse_units(master_dir)
        except Exception as e:
            errors.append(f"Failed to parse units: {e}")
            master_dir = None

    # Parse MasterData flows
    elementary_flows: list[ElementaryFlow] = []
    intermediate_flows: list[IntermediateFlow] = []
    if master_dir:
        try:
            elementary_flows = parse_elementary_exchanges(master_dir, units_map)
        except Exception as e:
            errors.append(f"Failed to parse elementary exchanges: {e}")
        try:
            intermediate_flows = parse_intermediate_exchanges(master_dir, units_map)
        except Exception as e:
            errors.append(f"Failed to parse intermediate exchanges: {e}")

    # Parse unit conversions
    unit_conversions: list = []
    if master_dir:
        try:
            unit_conversions = parse_unit_conversions(master_dir)
        except Exception as e:
            warnings.append(f"Failed to parse unit conversions: {e}")

    # Parse LCIA Excel (Indicators + CFs)
    indicators: list[Indicator] = []
    all_cfs: list[CharacterizationFactor] = []
    ef31_cfs: list[CharacterizationFactor] = []
    if lcia_excel and lcia_excel.exists():
        try:
            indicators, all_cfs = parse_lcia_excel(lcia_excel)
            ef31_cfs = filter_cf_ef31(all_cfs)
        except Exception as e:
            warnings.append(f"Failed to parse LCIA Excel: {e}")

    # Build foundation report
    foundation_report: dict | None = None
    if master_dir or lcia_excel:
        foundation_report = {
            "units": len(units_map),
            "elementary_flows": len(elementary_flows),
            "intermediate_flows": len(intermediate_flows),
            "unit_conversions": len(unit_conversions),
            "indicators_total": len(indicators),
            "indicators_ef31": len([i for i in indicators if i.method in ('EF v3.1', 'EF v3.1 no LT')]),
            "cf_rows_total": len(all_cfs),
            "cf_rows_ef31": len(ef31_cfs),
        }

    # Parse LCI datasets
    datasets: list[LCIDataset] = []
    exchanges_map: dict[str, list[LCIElementaryExchange]] = {}
    if datasets_dir:
        spold_files = sorted(datasets_dir.glob("*.spold"))
        for spold_path in spold_files:
            try:
                ds = parse_spold_file(spold_path)
                if ds:
                    datasets.append(ds)
                    exs = parse_spold_exchanges(spold_path)
                    if exs:
                        exchanges_map[spold_path.name] = exs
            except Exception as e:
                errors.append(f"Failed to parse {spold_path.name}: {e}")

    # Save job artifacts
    manifest = {
        "job_id": job_id,
        "lci_archive": str(lci_dest.name),
        "lcia_archive": str(lcia_dest.name) if lcia_dest else None,
        "spold_count": spold_count,
        "parsed_datasets": len(datasets),
        "master_data": {
            "elementary_flows": len(elementary_flows),
            "intermediate_flows": len(intermediate_flows),
            "units": len(units_map),
        },
        "lcia_excel": str(lcia_excel.name) if lcia_excel else None,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(job_dir / "manifest.json", manifest)
    _write_json(job_dir / "foundation.json", foundation_report or {})
    _write_jsonl(job_dir / "datasets.jsonl", [_dataset_to_dict(ds) for ds in datasets])
    _write_jsonl(
        job_dir / "exchanges.jsonl",
        [_exchange_to_dict(exc) for excs in exchanges_map.values() for exc in excs],
    )

    # DB dry-run
    dry_run = dry_run_lci_import(
        datasets=datasets,
        exchanges_map=exchanges_map,
        elementary_flows=elementary_flows,
        intermediate_flows=intermediate_flows,
        units=units_map,
    )
    dry_run_summary = dry_run.summary()
    combined_errors = [*errors, *dry_run.errors]
    combined_warnings = [*warnings, *dry_run.warnings]

    missing_refs_count = dry_run.flows_error
    can_commit = len(combined_errors) == 0 and missing_refs_count == 0

    parsed_counts = {
        "datasets": len(datasets),
        "flows": len(elementary_flows),
        "intermediate_flows": len(intermediate_flows),
        "exchanges": sum(len(v) for v in exchanges_map.values()),
        "missing_refs": missing_refs_count,
        "units": len(units_map),
        "indicators_total": len(indicators),
        "indicators_ef31": len([i for i in indicators if i.method in ('EF v3.1', 'EF v3.1 no LT')]),
        "cf_rows_total": len(all_cfs),
        "cf_rows_ef31": len(ef31_cfs),
    }

    expires_at = (
        datetime.now(timezone.utc) + timedelta(hours=_JOB_EXPIRY_HOURS)
    ).isoformat()

    response = {
        "job_id": job_id,
        "can_commit": can_commit,
        "limit": limit,
        "counts": parsed_counts,
        "dry_run_summary": dry_run_summary,
        "warnings": combined_warnings,
        "errors": combined_errors,
        "expires_at": expires_at,
        "archive_name": lci_dest.name,
        "archive_file_discovery": archive_file_discovery,
        "foundation": foundation_report,
        "preview_counts": {
            "spold_count": spold_count,
            "parsed_datasets": len(datasets),
            "master_data": {
                "elementary_flows": len(elementary_flows),
                "intermediate_flows": len(intermediate_flows),
                "units": len(units_map),
            },
        },
    }

    # Persist report via DebugDiagnostic-compatible payload
    report_payload = {
        "job_id": job_id,
        "status": "preview",
        "diagnostic_type": "ef31.import.report.v1",
        **response,
    }
    _write_json(job_dir / "preview_report.json", report_payload)

    return job_id, response


# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------

def commit_ef31_import(
    job_id: str,
    db,
) -> DbCommitResult:
    """Commit a previously previewed EF 3.1 LCI import job.

    Loads job artifacts from disk (never trusts frontend counts),
    re-parses datasets if needed, and calls commit_lci_import.

    Raises ValueError if job not found, expired, or never previewed.
    """
    job_dir = _IMPORT_CACHE_ROOT / job_id
    if not job_dir.exists():
        raise ValueError(f"Job not found: {job_id}")

    manifest_path = job_dir / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"Job manifest missing: {job_id}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    # Check expiry
    expires_at_str = manifest.get("extracted_at", "")
    if expires_at_str:
        try:
            extracted = datetime.fromisoformat(expires_at_str)
            if extracted.tzinfo is None:
                extracted = extracted.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > extracted + timedelta(hours=_JOB_EXPIRY_HOURS):
                raise ValueError(f"Job expired: {job_id}")
        except (ValueError, TypeError):
            pass  # If we can't parse, proceed anyway

    lci_archive = job_dir / "uploads" / manifest.get("lci_archive", "")
    if not lci_archive.exists():
        raise ValueError(f"LCI archive missing: {lci_archive}")

    extract_dir = job_dir / "extracted"
    datasets_dir = _find_first_dir(extract_dir, "datasets")
    master_dir = _find_first_dir(extract_dir, "MasterData")

    # Reload units from extracted MasterData
    units_map: dict[str, UnitRecord] = {}
    elementary_flows: list[ElementaryFlow] = []
    intermediate_flows: list[IntermediateFlow] = []

    if master_dir and master_dir.exists():
        try:
            units_map = parse_units(master_dir)
        except Exception:
            pass
        try:
            elementary_flows = parse_elementary_exchanges(master_dir, units_map)
        except Exception:
            pass
        try:
            intermediate_flows = parse_intermediate_exchanges(master_dir, units_map)
        except Exception:
            pass

    # Reload LCI datasets from disk (not JSONL — re-parse for consistency)
    datasets: list[LCIDataset] = []
    exchanges_map: dict[str, list[LCIElementaryExchange]] = {}

    # Load from JSONL first (faster), but also verify SPOLDs exist
    datasets_jsonl = job_dir / "datasets.jsonl"
    if datasets_jsonl.exists():
        for line in datasets_jsonl.read_text(encoding="utf-8").strip().splitlines():
            if line:
                raw = json.loads(line)
                datasets.append(LCIDataset(**raw))

    if datasets_dir and datasets_dir.exists():
        for spold_file in sorted(datasets_dir.glob("*.spold")):
            try:
                exs = parse_spold_exchanges(spold_file)
                if exs:
                    exchanges_map[spold_file.name] = exs
            except Exception:
                pass

    # Execute commit
    commit_result = commit_lci_import(
        datasets=datasets,
        exchanges_map=exchanges_map,
        elementary_flows=elementary_flows,
        intermediate_flows=intermediate_flows,
        units=units_map,
        db=db,
    )
    commit_result.job_id = job_id

    # Persist commit report
    commit_report = {
        "job_id": job_id,
        "status": "committed",
        "diagnostic_type": "ef31.import.report.v1",
        **commit_result.summary(),
        "committed_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(job_dir / "commit_report.json", commit_report)

    return commit_result
