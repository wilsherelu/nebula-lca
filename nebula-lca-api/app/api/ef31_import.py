"""EF 3.1 LCI Import API routes (preview/commit/report).

Extracted from ``app.main`` for Stage 5B. Uses ``APIRouter`` pattern;
the router is included in main.py via ``app.include_router(_base_router)``.

URL paths preserved to match the original ``@app.xxx`` registrations.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from .. import database as _database
from ..database import get_db
from ..models import DebugDiagnostic
from ..schemas import (
    Ef31ImportCommitRequest,
    Ef31ImportCommitResponse,
    Ef31ImportReportResponse,
    Ef31ImportPreviewResponse,
    Ef31RuntimeCsvResponse,
)

_EF31_DIAGNOSTIC_TYPE = "ef31.import.report.v1"

# ── Routers ────────────────────────────────────────────────────────────────
_base_router = APIRouter(prefix="/import")
_api_router = APIRouter(prefix="/api/import")


# ── Helpers ────────────────────────────────────────────────────────────────

def _persist_ef31_report(db: Session, report_payload: dict) -> dict:
    """Persist an EF 3.1 import report into DebugDiagnostic."""
    diagnostic = db.get(DebugDiagnostic, report_payload["job_id"])
    if diagnostic is None:
        diagnostic = DebugDiagnostic(
            id=report_payload["job_id"],
            diagnostic_type=_EF31_DIAGNOSTIC_TYPE,
            payload_json=report_payload,
            result_json=report_payload,
        )
        db.add(diagnostic)
    else:
        diagnostic.diagnostic_type = _EF31_DIAGNOSTIC_TYPE
        diagnostic.payload_json = report_payload
        diagnostic.result_json = report_payload
    db.commit()
    db.refresh(diagnostic)
    return report_payload


# ── Preview route ──────────────────────────────────────────────────────────

@_api_router.post("/ef31/preview", response_model=Ef31ImportPreviewResponse)
@_base_router.post("/ef31/preview", response_model=Ef31ImportPreviewResponse)
def preview_ef31_lci_import(
    lci_archive: UploadFile = File(..., description="ecoinvent LCIA EF3.1 LCI .7z archive"),
    lcia_archive: UploadFile | None = File(None, description="Optional LCIA implementation .7z archive"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max datasets to parse"),
) -> Ef31ImportPreviewResponse:
    """Preview an EF 3.1 LCI import from a .7z archive.

    Selectively extracts MasterData XML, limited SPOLD files, and optional
    LCIA Excel.  Parses foundation data, LCI datasets, and runs DB dry-run.

    Returns a job_id that can be used for:
    - POST /import/ef31/commit (with confirm=true)
    - GET /import/ef31/reports/{job_id}
    """
    tmp_root = Path(__file__).resolve().parent.parent / "tmp" / "import_jobs"
    tmp_root.mkdir(parents=True, exist_ok=True)

    lci_path = tmp_root / f"{uuid.uuid4().hex}_{lci_archive.filename}"
    lci_path.write_bytes(lci_archive.file.read())

    lcia_path = None
    if lcia_archive and lcia_archive.filename:
        lcia_path = tmp_root / f"{uuid.uuid4().hex}_{lcia_archive.filename}"
        lcia_path.write_bytes(lcia_archive.file.read())

    try:
        from app.ef31_import_job_service import preview_ef31_import as _preview
        job_id, response = _preview(
            lci_archive_path=lci_path,
            lcia_archive_path=lcia_path,
            limit=limit,
        )
        # Persist to DebugDiagnostic.
        report_db = _database.SessionLocal()
        try:
            _persist_ef31_report(report_db, {
                "job_id": job_id,
                "status": "preview",
                "diagnostic_type": _EF31_DIAGNOSTIC_TYPE,
                **response,
            })
        finally:
            report_db.close()
        return Ef31ImportPreviewResponse(**response)
    except Exception as e:
        raise HTTPException(status_code=400, detail={"code": "PREVIEW_FAILED", "message": str(e)})


# ── Commit route ───────────────────────────────────────────────────────────

@_api_router.post("/ef31/commit", response_model=Ef31ImportCommitResponse)
@_base_router.post("/ef31/commit", response_model=Ef31ImportCommitResponse)
def commit_ef31_lci_import(
    request: Ef31ImportCommitRequest,
    db: Session = Depends(get_db),
) -> Ef31ImportCommitResponse:
    """Commit a previously previewed EF 3.1 LCI import job.

    Must provide confirm=true. Loads job artifacts from disk, re-parses,
    and executes DB writes with the provided session.
    """
    job_id = request.job_id

    try:
        from app.ef31_import_job_service import commit_ef31_import as _commit
        commit_result = _commit(job_id, db=db)
        summary = commit_result.summary()
        summary["committed"] = True
        summary["catalog_target_kind"] = "lci_dataset"

        # Persist commit report
        _persist_ef31_report(db, {
            "job_id": job_id,
            "status": "committed",
            "diagnostic_type": _EF31_DIAGNOSTIC_TYPE,
            **summary,
        })

        return Ef31ImportCommitResponse(**summary)
    except ValueError as e:
        raise HTTPException(status_code=404, detail={"code": "JOB_NOT_FOUND", "message": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"code": "COMMIT_FAILED", "message": str(e)})


# ── Report route ───────────────────────────────────────────────────────────

@_api_router.post("/ef31/runtime-csv/{job_id}", response_model=Ef31RuntimeCsvResponse)
@_base_router.post("/ef31/runtime-csv/{job_id}", response_model=Ef31RuntimeCsvResponse)
def generate_ef31_runtime_csv(job_id: str, db: Session = Depends(get_db)) -> Ef31RuntimeCsvResponse:
    """Generate solver runtime CSVs for a previewed EF 3.1 import job."""
    try:
        from app.services.ef31_runtime_csv import generate_ef31_runtime_csvs

        summary = generate_ef31_runtime_csvs(job_id, overwrite=True)
        summary["env_var"] = "NEBULA_LCA_EF31_DIR"

        row = db.get(DebugDiagnostic, job_id)
        if row is not None and row.diagnostic_type == _EF31_DIAGNOSTIC_TYPE:
            payload = dict(row.result_json) if isinstance(row.result_json, dict) else {}
            payload["runtime_csv"] = summary
            row.result_json = payload
            row.payload_json = payload
            db.commit()

        return Ef31RuntimeCsvResponse(**summary)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail={"code": "JOB_ARTIFACT_NOT_FOUND", "message": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"code": "RUNTIME_CSV_FAILED", "message": str(e)})


@_api_router.get("/ef31/reports/{job_id}", response_model=Ef31ImportReportResponse)
@_base_router.get("/ef31/reports/{job_id}", response_model=Ef31ImportReportResponse)
def get_ef31_import_report(job_id: str, db: Session = Depends(get_db)) -> Ef31ImportReportResponse:
    """Retrieve the latest report for an EF 3.1 import job.

    Returns the most recent preview or commit report.
    """
    row = db.get(DebugDiagnostic, job_id)
    if row is None or row.diagnostic_type != _EF31_DIAGNOSTIC_TYPE:
        raise HTTPException(
            status_code=404,
            detail={"code": "IMPORT_REPORT_NOT_FOUND", "message": f"report not found: {job_id}"},
        )
    result_json = row.result_json if isinstance(row.result_json, dict) else {}
    return Ef31ImportReportResponse.model_validate({
        **result_json,
        "job_id": result_json.get("job_id", job_id),
        "payload": result_json,
    })
