"""EF 3.1 chunked upload and import job API routes.

Replaces the old preview/commit flow with:
- Chunked upload sessions (resume-friendly)
- Persistent import jobs with checkpoint-based resume
- Pause/resume/retry-failed support

Old preview/commit routes remain in ef31_import.py for compatibility.
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from ..database import SessionLocal, get_db
from ..models import ImportJob, DatasetCheckpoint
from ..schemas import (
    ImportJobCreateRequest,
    ImportJobListResponse,
    ImportJobStartRequest,
    ImportJobStatusResponse,
    UploadChunkCompleteResponse,
    UploadSessionCreateResponse,
)

logger = logging.getLogger(__name__)

_router = APIRouter(prefix="/api/import/ef31", tags=["EF3.1 Import"])


def _job_response(job: ImportJob, failed_datasets: list[str] | None = None) -> ImportJobStatusResponse:
    return ImportJobStatusResponse(
        job_id=job.job_id,
        file_path=job.file_path,
        file_type=job.file_type,
        phase=job.phase,
        progress_pct=job.progress_pct,
        workers=job.workers,
        limit=job.limit,
        status=job.status,
        error_summary=job.error_summary,
        stats=job.stats_json,
        failed_datasets=failed_datasets or [],
        created_at=str(job.created_at),
        updated_at=str(job.updated_at),
    )


def _run_job_background(job_id: str, resume_from_failed: bool) -> None:
    db = SessionLocal()
    try:
        job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
        if job is None:
            return

        file_path = Path(job.file_path)
        if not file_path.exists():
            job.status = "failed"
            job.error_summary = f"File not found: {job.file_path}"
            db.commit()
            return

        if file_path.suffix.lower() == ".7z":
            extract_dir = Path("import-cache") / "job_extract" / job_id
            extract_dir.mkdir(parents=True, exist_ok=True)
            from ..ecoinvent_ef31_loader import selective_extract_7z

            extract_result = selective_extract_7z(file_path, extract_dir, spold_limit=job.limit or 0)
            spold_dir = str(extract_result.get("datasets_dir", extract_dir))
            master_data_dir = str(extract_result.get("master_dir", ""))
        else:
            spold_dir = str(file_path)
            master_data_dir = ""

        from ..lci_import_executor import LciImportJobExecutor

        executor = LciImportJobExecutor(
            job_id=job_id,
            db=db,
            spold_dir=spold_dir,
            master_data_dir=master_data_dir if master_data_dir else None,
            workers=job.workers,
            limit=job.limit,
            resume_from_failed=resume_from_failed,
        )
        result = executor.run()

        job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
        if job is None:
            return
        if job.status not in {"paused", "cancelled"}:
            job.status = "completed" if not result.error_summary else "failed"
            job.phase = "done" if job.status == "completed" else "failed"
        job.error_summary = result.error_summary
        job.stats_json = {
            "processes_inserted": result.processes_inserted,
            "processes_skipped": result.processes_skipped,
            "processes_failed": result.processes_failed,
            "vectors_written": result.vectors_written,
            "vector_nnz_total": result.vector_nnz_total,
            "failed_datasets": result.failed_datasets,
            "duration_seconds": result.duration_seconds,
        }
        job.updated_at = datetime.utcnow()
        db.commit()
    except Exception as exc:
        logger.exception("Import job %s failed", job_id)
        job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
        if job is not None:
            job.status = "failed"
            job.phase = "failed"
            job.error_summary = str(exc)[:1024]
            db.commit()
    finally:
        db.close()


# ── Upload Session ───────────────────────────────────────────────────────


@_router.post("/upload-session", response_model=UploadSessionCreateResponse)
async def create_upload_session(
    file_name: str = Query(..., description="Original file name"),
    file_type: str = Query("lci", description="lci or lcia"),
):
    """Create a chunked upload session."""
    from ..chunked_upload import create_upload_session as _create

    upload_id, response = _create(file_name=file_name, file_type=file_type)
    return UploadSessionCreateResponse(**response)


@_router.put("/upload-session/{upload_id}/chunks/{chunk_index}")
async def upload_chunk(
    upload_id: str,
    chunk_index: int,
    file: UploadFile,
):
    """Upload a single chunk (idempotent by index)."""
    from ..chunked_upload import upload_chunk as _upload

    data = await file.read()
    result = _upload(upload_id, chunk_index, data)
    return {
        "chunk_index": chunk_index,
        "uploaded_count": result["uploaded_count"],
        "uploaded_chunks": result["uploaded_chunks"],
    }


@_router.get("/upload-session/{upload_id}", response_model=UploadSessionCreateResponse)
async def get_upload_session(upload_id: str):
    """Get upload session status (uploaded chunks)."""
    from ..chunked_upload import get_upload_session as _get

    result = _get(upload_id)
    if result is None:
        raise HTTPException(status_code=404, detail={"code": "UPLOAD_SESSION_NOT_FOUND", "message": f"No session: {upload_id}"})
    return UploadSessionCreateResponse(**result)


@_router.post("/upload-session/{upload_id}/complete", response_model=UploadChunkCompleteResponse)
async def complete_upload_session(
    upload_id: str,
    total_chunks: int = Query(..., ge=1, description="Total number of chunks"),
    file_type: str = Query("lci", description="lci or lcia"),
    expected_hash: str | None = Query(None, description="SHA-256 of complete file"),
    expected_size: int | None = Query(None, description="Expected file size in bytes"),
):
    """Validate and merge all chunks into the final file."""
    from ..chunked_upload import complete_upload_session as _complete

    try:
        result = _complete(
            upload_id=upload_id,
            total_chunks=total_chunks,
            file_type=file_type,
            expected_hash=expected_hash,
            expected_size=expected_size,
        )
        return UploadChunkCompleteResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "UPLOAD_COMPLETE_FAILED", "message": str(e)})


# ── Import Jobs ──────────────────────────────────────────────────────────


@_router.post("/jobs", response_model=ImportJobStatusResponse)
def create_import_job(
    request: ImportJobCreateRequest,
    db: Session = Depends(get_db),
):
    """Create an import job from an uploaded file or local path."""
    from ..chunked_upload import register_import_source

    try:
        file_path = str(register_import_source(request.file_path, request.file_type))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail={"code": "FILE_NOT_FOUND", "message": str(e)})

    # Check for existing active job on same file
    existing = (
        db.query(ImportJob)
        .filter(
            ImportJob.file_path == file_path,
            ImportJob.status.in_(["pending", "running", "paused"]),
        )
        .first()
    )
    if existing:
        return _job_response(existing)

    job_id = str(uuid.uuid4())
    job = ImportJob(
        job_id=job_id,
        file_path=file_path,
        file_type=request.file_type,
        phase="created",
        progress_pct=0.0,
        workers=request.workers,
        limit=request.limit,
        status="pending",
        created_at=datetime.utcnow(),
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    return _job_response(job)


@_router.post("/jobs/{job_id}/start", response_model=ImportJobStatusResponse)
def start_import_job(
    job_id: str,
    request: ImportJobStartRequest = ImportJobStartRequest(),
    db: Session = Depends(get_db),
):
    """Start or resume an import job in a background worker."""
    job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "JOB_NOT_FOUND", "message": f"No job: {job_id}"})

    if job.status in {"completed", "running"}:
        return _job_response(job)

    job.status = "running"
    job.phase = "parsing"
    job.error_summary = None
    job.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(job)

    thread = threading.Thread(
        target=_run_job_background,
        args=(job_id, request.resume_from_failed),
        daemon=True,
    )
    thread.start()
    return _job_response(job)


@_router.post("/jobs/{job_id}/pause")
def pause_import_job(job_id: str, db: Session = Depends(get_db)):
    """Pause an import job (best-effort, pauses at next checkpoint)."""
    job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "JOB_NOT_FOUND", "message": f"No job: {job_id}"})
    if job.status != "running":
        raise HTTPException(status_code=400, detail={"code": "NOT_RUNNING", "message": f"Job is {job.status}, not running"})
    job.status = "paused"
    job.updated_at = datetime.utcnow()
    db.commit()
    return {"job_id": job_id, "status": "paused", "message": "Pause requested, will stop at next dataset boundary"}


@_router.post("/jobs/{job_id}/cancel")
def cancel_import_job(job_id: str, db: Session = Depends(get_db)):
    """Cancel an import job."""
    job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "JOB_NOT_FOUND", "message": f"No job: {job_id}"})
    job.status = "cancelled"
    job.phase = "cancelled"
    job.updated_at = datetime.utcnow()
    db.commit()
    return {"job_id": job_id, "status": "cancelled"}


@_router.get("/jobs/{job_id}", response_model=ImportJobStatusResponse)
def get_import_job(job_id: str, db: Session = Depends(get_db)):
    """Get import job status."""
    job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "JOB_NOT_FOUND", "message": f"No job: {job_id}"})

    # Get failed datasets from checkpoints
    failed_checkpoints = (
        db.query(DatasetCheckpoint)
        .filter(DatasetCheckpoint.job_id == job_id, DatasetCheckpoint.status == "failed")
        .all()
    )
    failed_keys = [cp.dataset_key for cp in failed_checkpoints]

    return _job_response(job, failed_keys)


@_router.get("/jobs", response_model=ImportJobListResponse)
def list_import_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """List import jobs."""
    query = db.query(ImportJob).order_by(ImportJob.created_at.desc())
    if status:
        query = query.filter(ImportJob.status == status)
    jobs = query.limit(limit).all()
    return ImportJobListResponse(
        jobs=[_job_response(j) for j in jobs],
        total=len(jobs),
    )


@_router.post("/jobs/{job_id}/retry-failed", response_model=ImportJobStatusResponse)
def retry_failed_datasets(
    job_id: str,
    request: ImportJobStartRequest = ImportJobStartRequest(resume_from_failed=True),
    db: Session = Depends(get_db),
):
    """Retry only failed datasets for a job."""
    # Reuse start logic with resume_from_failed
    return start_import_job(job_id, request, db)


@_router.post("/jobs/{job_id}/lcia-runtime")
def generate_lcia_runtime_for_job(
    job_id: str,
    db: Session = Depends(get_db),
):
    """Generate LCIA runtime from a completed LCIA upload session."""
    from ..lcia_runtime import generate_lcia_runtime_from_job as _gen

    try:
        manifest = _gen(job_id)
        return manifest
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail={"code": "LCIA_RUNTIME_FAILED", "message": str(e)})
