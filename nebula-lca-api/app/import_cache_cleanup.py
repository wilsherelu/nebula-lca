"""Disk cleanup helpers for resumable EF 3.1 imports.

Upload sessions and archive extraction are intentionally persistent while a
job can be resumed. Terminal jobs should not keep multi-GB archives forever.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import PROJECT_ROOT, settings

logger = logging.getLogger(__name__)

IMPORT_CACHE_ROOT = Path(settings.import_cache_root)
UPLOAD_SESSIONS_ROOT = IMPORT_CACHE_ROOT / "upload_sessions"
JOB_EXTRACT_ROOT = IMPORT_CACHE_ROOT / "job_extract"
EF31_JOBS_ROOT = IMPORT_CACHE_ROOT / "ef31_jobs"


def _resolve_existing(path: Path) -> Path | None:
    try:
        return path.resolve() if path.exists() else None
    except OSError:
        return None


def _is_relative_to(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except (OSError, ValueError):
        return False


def _safe_rmtree(path: Path, allowed_root: Path) -> bool:
    resolved = _resolve_existing(path)
    if resolved is None:
        return False
    if not _is_relative_to(resolved, allowed_root):
        logger.warning("Refusing to remove import cache path outside %s: %s", allowed_root, resolved)
        return False
    shutil.rmtree(resolved, ignore_errors=True)
    return True


def cleanup_upload_session_by_file(file_path: str | Path | None) -> bool:
    """Remove the upload session that owns a completed job file, if any."""
    if not file_path:
        return False
    path = Path(file_path)
    if not path.is_absolute():
        cwd_path = Path.cwd() / path
        path = cwd_path if cwd_path.exists() else PROJECT_ROOT / path
    try:
        relative = path.resolve().relative_to(UPLOAD_SESSIONS_ROOT.resolve())
    except (OSError, ValueError):
        return False
    parts = relative.parts
    if not parts:
        return False
    return _safe_rmtree(UPLOAD_SESSIONS_ROOT / parts[0], UPLOAD_SESSIONS_ROOT)


def cleanup_job_extract(job_id: str) -> bool:
    """Remove extracted archive contents for one import job."""
    return _safe_rmtree(JOB_EXTRACT_ROOT / job_id, JOB_EXTRACT_ROOT)


def cleanup_terminal_import_artifacts(job_id: str, file_path: str | Path | None) -> dict:
    """Remove large temporary files once a job no longer needs resume inputs."""
    if not settings.import_cache_cleanup_on_terminal:
        return {"enabled": False, "upload_session_removed": False, "job_extract_removed": False}
    return {
        "enabled": True,
        "upload_session_removed": cleanup_upload_session_by_file(file_path),
        "job_extract_removed": cleanup_job_extract(job_id),
    }


def cleanup_stale_import_cache(retention_hours: int | None = None) -> dict:
    """Delete stale resumable-upload and extract directories."""
    hours = retention_hours if retention_hours is not None else settings.import_cache_retention_hours
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, hours))
    result = {
        "upload_sessions_removed": 0,
        "job_extracts_removed": 0,
        "ef31_jobs_removed": 0,
    }
    for root, key in (
        (UPLOAD_SESSIONS_ROOT, "upload_sessions_removed"),
        (JOB_EXTRACT_ROOT, "job_extracts_removed"),
        (EF31_JOBS_ROOT, "ef31_jobs_removed"),
    ):
        if not root.exists():
            continue
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            try:
                mtime = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
            except OSError:
                continue
            if mtime >= cutoff:
                continue
            if _safe_rmtree(entry, root):
                result[key] += 1
    return result
