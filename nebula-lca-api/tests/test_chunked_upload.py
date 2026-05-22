"""Tests for chunked upload session service.

Covers:
- Session creation and listing uploaded chunks
- Missing chunks prevent completion
- Duplicate chunk upload is idempotent
- Session survives "refresh" (list uploaded chunks)
"""

import shutil
import tempfile
from pathlib import Path

import pytest

# Ensure app can be imported
TEST_DIR = Path(__file__).resolve().parent
API_DIR = TEST_DIR.parent


@pytest.fixture
def chunked_upload_dir():
    """Create a temp directory for upload sessions."""
    d = Path(tempfile.mkdtemp()) / "import-cache" / "upload_sessions"
    d.mkdir(parents=True, exist_ok=True)
    yield d
    shutil.rmtree(d.parent.parent, ignore_errors=True)


@pytest.fixture(autouse=True)
def _patch_upload_dir(chunked_upload_dir, monkeypatch):
    """Patch the upload sessions directory to use our temp dir."""
    import app.chunked_upload as mod
    monkeypatch.setattr(mod, "_UPLOAD_SESSIONS_DIR", chunked_upload_dir, raising=False)


def test_create_upload_session():
    from app.chunked_upload import create_upload_session

    upload_id, response = create_upload_session(
        file_name="ecoinvent_3.11_lci.7z",
        file_type="lci",
    )
    assert upload_id
    assert response["upload_id"] == upload_id
    assert response["chunk_size"] == 5 * 1024 * 1024
    assert response["uploaded_chunks"] == []


def test_upload_chunk_idempotent():
    from app.chunked_upload import create_upload_session, upload_chunk, get_upload_session

    upload_id, _ = create_upload_session("test.7z", "lci")

    # Upload chunk 0
    result1 = upload_chunk(upload_id, 0, b"chunk0")
    assert result1["uploaded_count"] == 1
    assert 0 in result1["uploaded_chunks"]

    # Upload chunk 0 again (idempotent)
    result2 = upload_chunk(upload_id, 0, b"chunk0_modified")
    assert result2["uploaded_count"] == 1  # Still 1, not 2
    # Data should be the modified version
    from app.chunked_upload import _chunks_dir
    chunks = _chunks_dir(upload_id)
    assert (chunks / "0").read_bytes() == b"chunk0_modified"


def test_upload_chunk_resume_list():
    """Simulate: upload chunks 0,1,2 then refresh browser, check list."""
    from app.chunked_upload import (
        create_upload_session,
        upload_chunk,
        get_upload_session,
    )

    upload_id, _ = create_upload_session("resume.7z", "lci")

    upload_chunk(upload_id, 0, b"data0")
    upload_chunk(upload_id, 1, b"data1")

    # "Refresh browser" — list session
    session = get_upload_session(upload_id)
    assert session is not None
    assert session["uploaded_chunks"] == [0, 1]

    # Continue uploading remaining
    upload_chunk(upload_id, 2, b"data2")

    session2 = get_upload_session(upload_id)
    assert session2["uploaded_chunks"] == [0, 1, 2]


def test_create_upload_session_reuses_unfinished_same_file():
    from app.chunked_upload import create_upload_session, upload_chunk

    upload_id, _ = create_upload_session("resume-same.7z", "lci", expected_size=123)
    upload_chunk(upload_id, 0, b"data0")

    reused_id, response = create_upload_session("resume-same.7z", "lci", expected_size=123)

    assert reused_id == upload_id
    assert response["upload_id"] == upload_id
    assert response["uploaded_chunks"] == [0]


def test_complete_missing_chunks():
    from app.chunked_upload import (
        create_upload_session,
        upload_chunk,
        complete_upload_session,
    )

    upload_id, _ = create_upload_session("missing.7z", "lci")

    # Only upload chunk 0, skip chunk 1
    upload_chunk(upload_id, 0, b"data0")
    upload_chunk(upload_id, 2, b"data2")

    with pytest.raises(ValueError, match="Missing chunks"):
        complete_upload_session(upload_id, total_chunks=3, file_type="lci")


def test_complete_success():
    from app.chunked_upload import (
        create_upload_session,
        upload_chunk,
        complete_upload_session,
    )

    upload_id, _ = create_upload_session("complete.7z", "lci")

    total_chunks = 3
    for i in range(total_chunks):
        upload_chunk(upload_id, i, f"chunk{i}".encode())

    result = complete_upload_session(
        upload_id,
        total_chunks=total_chunks,
        file_type="lci",
    )
    assert result["file_name"] == "complete.7z"
    assert result["file_size"] == sum(len(f"chunk{i}".encode()) for i in range(total_chunks))
    assert Path(result["file_path"]).exists()


def test_complete_hash_mismatch():
    from app.chunked_upload import (
        create_upload_session,
        upload_chunk,
        complete_upload_session,
    )

    upload_id, _ = create_upload_session("hash.7z", "lci")
    upload_chunk(upload_id, 0, b"data0")

    with pytest.raises(ValueError, match="Hash mismatch"):
        complete_upload_session(
            upload_id,
            total_chunks=1,
            file_type="lci",
            expected_hash="bad_hash",
        )


def test_get_upload_session_not_found():
    from app.chunked_upload import get_upload_session

    result = get_upload_session("nonexistent-id")
    assert result is None


def test_cleanup(chunked_upload_dir, monkeypatch):
    """Test cleanup removes session from disk."""
    from app.chunked_upload import (
        create_upload_session,
        upload_chunk,
        cleanup_upload_session,
    )

    import app.chunked_upload as mod

    # Apply the patch again since autouse may not have run in this test
    monkeypatch.setattr(mod, "_UPLOAD_SESSIONS_DIR", chunked_upload_dir, raising=False)

    upload_id, _ = create_upload_session("cleanup.7z", "lci")
    upload_chunk(upload_id, 0, b"test")

    cleanup_upload_session(upload_id)

    # Session dir should be gone
    session_dir = chunked_upload_dir / upload_id
    assert not session_dir.exists()
