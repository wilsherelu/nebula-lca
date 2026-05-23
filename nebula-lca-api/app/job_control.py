"""Job control signal management for import executor.

Provides file-based control signals so pause/cancel can be recorded even
when the SQLite database is locked.  The executor periodically checks these
signals and reacts at safe checkpoints (between batches / before each flush).

Signal file format: ``import-cache/job_control/{job_id}.json``

Contents:
    {
        "pause_requested": true,
        "cancel_requested": false,
        "updated_at": "2026-05-21T12:00:00.000000"
    }
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SIGNAL_DIR = Path("import-cache") / "job_control"


def _ensure_signal_dir() -> None:
    """Ensure the job control signal directory exists."""
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)


def _signal_path(job_id: str) -> Path:
    return SIGNAL_DIR / f"{job_id}.json"


def write_job_control_signal(
    job_id: str,
    *,
    pause_requested: bool = False,
    cancel_requested: bool = False,
) -> dict:
    """Write (or update) a control signal file for the given job.

    This is a file-write operation and does NOT depend on the database
    session, so it remains safe even when the DB is locked.

    Returns the written signal dict.
    """
    _ensure_signal_dir()
    path = _signal_path(job_id)
    signal = {
        "job_id": job_id,
        "pause_requested": pause_requested,
        "cancel_requested": cancel_requested,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    # Use a lock so concurrent API calls don't race on the same file
    lock = _signal_locks.get(job_id)
    if lock is None:
        lock = threading.Lock()
        _signal_locks[job_id] = lock
    with lock:
        path.write_text(json.dumps(signal), encoding="utf-8")
    logger.debug("[%s] Wrote control signal: pause=%s cancel=%s", job_id, pause_requested, cancel_requested)
    return signal


def read_job_control_signal(job_id: str) -> Optional[dict]:
    """Read the latest control signal file for a job.  Returns None if not found."""
    path = _signal_path(job_id)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, OSError):
        return None


def clear_job_control_signal(job_id: str) -> None:
    """Remove the control signal file for a completed / cleaned-up job."""
    path = _signal_path(job_id)
    if path.exists():
        try:
            path.unlink()
        except OSError:
            pass


def job_needs_action(job_id: str) -> tuple[bool, bool]:
    """Check if the job needs pause or cancel from signal files.

    Returns ``(pause_requested, cancel_requested)``.
    Falls back to the in-memory state passed by the caller (see executor).
    """
    signal = read_job_control_signal(job_id)
    if signal:
        return bool(signal.get("pause_requested", False)), bool(signal.get("cancel_requested", False))
    return False, False


def recover_stale_jobs(
    db,
    *,
    max_seconds: int = 600,
    statuses: Optional[list[str]] = None,
) -> dict:
    """Mark stale jobs (running/pending/paused) as cancelled and reset checkpoints.

    A job is considered stale when:
    - Its status is one of the target ``statuses`` AND
    - Its ``updated_at`` is older than ``max_seconds`` seconds.

    This is a maintenance helper; it does NOT touch running executors.
    It only updates DB state so the job can be retried.

    Args:
        db: SQLAlchemy session.
        max_seconds: Max seconds a job is allowed without DB updates
            before being considered stale (default: 600 = 10 min).
        statuses: ImportJob statuses to check.  Defaults to
            ``["running", "pending", "paused"]``.

    Returns:
        Dict with counts of recovered jobs and updated checkpoints.
    """
    from .models import DatasetCheckpoint, ImportJob

    if statuses is None:
        statuses = ["running", "pending", "paused"]

    recovered: dict = {
        "jobs_cancelled": 0,
        "checkpoints_reset": 0,
        "signals_cleared": 0,
    }

    if SIGNAL_DIR.exists():
        for signal_file in SIGNAL_DIR.glob("*.json"):
            job = db.query(ImportJob).filter(
                ImportJob.job_id == signal_file.stem,
                ImportJob.status.in_(["completed", "failed", "cancelled"]),
            ).first()
            if job is not None:
                clear_job_control_signal(job.job_id)
                recovered["signals_cleared"] += 1

    cutoff = datetime.utcnow() - timedelta(seconds=max_seconds)
    jobs = db.query(ImportJob).filter(
        ImportJob.status.in_(statuses),
        ImportJob.updated_at < cutoff,
    ).all()
    for job in jobs:
        reset_count = db.query(DatasetCheckpoint).filter(
            DatasetCheckpoint.job_id == job.job_id,
            DatasetCheckpoint.status == "running",
        ).update(
            {
                "status": "pending",
                "error_message": "Reset from stale import job",
                "updated_at": datetime.utcnow(),
            },
            synchronize_session=False,
        )
        job.status = "cancelled"
        job.phase = "cancelled"
        job.error_summary = "Recovered stale import job"
        job.updated_at = datetime.utcnow()
        clear_job_control_signal(job.job_id)
        recovered["jobs_cancelled"] += 1
        recovered["checkpoints_reset"] += int(reset_count or 0)
    db.commit()
    return recovered

# Per-job lock for signal file writes
_signal_locks: dict[str, threading.Lock] = {}
