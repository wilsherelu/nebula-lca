import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


def test_cleanup_upload_session_by_file_removes_owning_session(tmp_path, monkeypatch):
    import app.import_cache_cleanup as cleanup

    cache_root = tmp_path / "import-cache"
    upload_root = cache_root / "upload_sessions"
    session = upload_root / "upload-1"
    session.mkdir(parents=True)
    uploaded_file = session / "source.7z"
    uploaded_file.write_bytes(b"archive")

    monkeypatch.setattr(cleanup, "IMPORT_CACHE_ROOT", cache_root)
    monkeypatch.setattr(cleanup, "UPLOAD_SESSIONS_ROOT", upload_root)

    assert cleanup.cleanup_upload_session_by_file(uploaded_file) is True
    assert not session.exists()


def test_cleanup_terminal_import_artifacts_removes_upload_and_extract(tmp_path, monkeypatch):
    import app.import_cache_cleanup as cleanup

    cache_root = tmp_path / "import-cache"
    upload_root = cache_root / "upload_sessions"
    extract_root = cache_root / "job_extract"
    session = upload_root / "upload-2"
    extract = extract_root / "job-2"
    session.mkdir(parents=True)
    extract.mkdir(parents=True)
    uploaded_file = session / "source.7z"
    uploaded_file.write_bytes(b"archive")
    (extract / "dataset.spold").write_bytes(b"spold")

    monkeypatch.setattr(cleanup, "IMPORT_CACHE_ROOT", cache_root)
    monkeypatch.setattr(cleanup, "UPLOAD_SESSIONS_ROOT", upload_root)
    monkeypatch.setattr(cleanup, "JOB_EXTRACT_ROOT", extract_root)
    monkeypatch.setattr(cleanup.settings, "import_cache_cleanup_on_terminal", True)

    result = cleanup.cleanup_terminal_import_artifacts("job-2", uploaded_file)

    assert result["enabled"] is True
    assert result["upload_session_removed"] is True
    assert result["job_extract_removed"] is True
    assert not session.exists()
    assert not extract.exists()


def test_cleanup_stale_import_cache_keeps_recent_sessions(tmp_path, monkeypatch):
    import app.import_cache_cleanup as cleanup

    cache_root = tmp_path / "import-cache"
    upload_root = cache_root / "upload_sessions"
    extract_root = cache_root / "job_extract"
    jobs_root = cache_root / "ef31_jobs"
    old_session = upload_root / "old"
    recent_session = upload_root / "recent"
    old_extract = extract_root / "old-job"
    old_preview = jobs_root / "old-preview"
    for path in (old_session, recent_session, old_extract, old_preview):
        path.mkdir(parents=True)

    old_time = (datetime.now(timezone.utc) - timedelta(hours=48)).timestamp()
    for path in (old_session, old_extract, old_preview):
        Path(path).touch()
        os.utime(path, (old_time, old_time))

    monkeypatch.setattr(cleanup, "IMPORT_CACHE_ROOT", cache_root)
    monkeypatch.setattr(cleanup, "UPLOAD_SESSIONS_ROOT", upload_root)
    monkeypatch.setattr(cleanup, "JOB_EXTRACT_ROOT", extract_root)
    monkeypatch.setattr(cleanup, "EF31_JOBS_ROOT", jobs_root)

    result = cleanup.cleanup_stale_import_cache(retention_hours=24)

    assert result == {
        "upload_sessions_removed": 1,
        "job_extracts_removed": 1,
        "ef31_jobs_removed": 1,
    }
    assert not old_session.exists()
    assert recent_session.exists()
    assert not old_extract.exists()
    assert not old_preview.exists()
