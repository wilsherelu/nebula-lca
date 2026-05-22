"""Chunked upload session service for EF 3.1 LCI/LCIA imports.

Manages:
- upload session lifecycle (create → chunk → complete)
- on-disk chunk storage and merge
- supports resume after page refresh
"""

from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from pathlib import Path
from typing import Optional

_DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB
_UPLOAD_SESSIONS_DIR = Path("import-cache") / "upload_sessions"


def _session_dir(upload_id: str) -> Path:
    d = _UPLOAD_SESSIONS_DIR / upload_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _chunks_dir(upload_id: str) -> Path:
    chunks = _session_dir(upload_id) / "chunks"
    chunks.mkdir(parents=True, exist_ok=True)
    return chunks


def create_upload_session(
    file_name: str,
    file_type: str = "lci",
    expected_size: int | None = None,
) -> tuple[str, dict]:
    """Create a new upload session.

    Returns:
        (upload_id, response_dict)
    """
    existing = find_upload_session(file_name=file_name, file_type=file_type, expected_size=expected_size)
    if existing is not None:
        return existing["upload_id"], {
            "upload_id": existing["upload_id"],
            "chunk_size": existing["chunk_size"],
            "uploaded_chunks": existing["uploaded_chunks"],
        }

    upload_id = str(uuid.uuid4())
    session = _session_dir(upload_id)

    metadata = {
        "upload_id": upload_id,
        "file_name": file_name,
        "file_type": file_type,
        "chunk_size": _DEFAULT_CHUNK_SIZE,
        "expected_size": expected_size,
        "created_at": str(uuid.uuid1()),  # simple timestamp placeholder
    }
    (session / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    _chunks_dir(upload_id)  # ensure chunks dir exists

    return upload_id, {
        "upload_id": upload_id,
        "chunk_size": _DEFAULT_CHUNK_SIZE,
        "uploaded_chunks": [],
    }


def upload_chunk(
    upload_id: str,
    chunk_index: int,
    data: bytes,
) -> dict:
    """Upload a single chunk (idempotent by chunk_index).

    Returns:
        {uploaded_count, total_expected_hint}
    """
    chunks = _chunks_dir(upload_id)
    chunk_path = chunks / str(chunk_index)
    chunk_path.write_bytes(data)

    # Count uploaded chunks
    uploaded = sorted(int(p.stem) for p in chunks.iterdir() if p.suffix == "")
    return {
        "chunk_index": chunk_index,
        "uploaded_count": len(uploaded),
        "uploaded_chunks": uploaded,
    }


def get_upload_session(upload_id: str) -> dict | None:
    """Get session info including already uploaded chunks.

    Returns None if session not found.
    """
    session = _session_dir(upload_id)
    meta_path = session / "metadata.json"
    if not meta_path.exists():
        return None

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    chunks = _chunks_dir(upload_id)
    uploaded = sorted(int(p.stem) for p in chunks.iterdir() if p.suffix == "")

    return {
        "upload_id": upload_id,
        "file_name": metadata.get("file_name", ""),
        "file_type": metadata.get("file_type", "lci"),
        "chunk_size": metadata.get("chunk_size", _DEFAULT_CHUNK_SIZE),
        "expected_size": metadata.get("expected_size"),
        "uploaded_chunks": uploaded,
    }


def find_upload_session(
    *,
    file_name: str,
    file_type: str = "lci",
    expected_size: int | None = None,
) -> dict | None:
    """Find an unfinished upload session for the same file."""
    if not _UPLOAD_SESSIONS_DIR.exists():
        return None
    sessions = sorted(_UPLOAD_SESSIONS_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    for session_dir in sessions:
        if not session_dir.is_dir():
            continue
        meta_path = session_dir / "metadata.json"
        if not meta_path.exists():
            continue
        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if metadata.get("file_name") != file_name or metadata.get("file_type", "lci") != file_type:
            continue
        stored_size = metadata.get("expected_size")
        if expected_size is not None and stored_size not in (None, expected_size):
            continue
        dest_name = file_name if file_name.endswith((".7z", ".xlsx")) else f"{file_name}.7z"
        if (session_dir / dest_name).exists():
            continue
        return get_upload_session(session_dir.name)
    return None


def complete_upload_session(
    upload_id: str,
    total_chunks: int,
    file_type: str = "lci",
    expected_hash: str | None = None,
    expected_size: int | None = None,
) -> dict:
    """Validate and merge all chunks into the final file.

    Returns:
        {file_path, file_name, file_size, file_type}

    Raises:
        ValueError if chunks are missing or hash mismatch.
    """
    chunks = _chunks_dir(upload_id)
    metadata_path = _session_dir(upload_id) / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    # Validate received chunks
    received = sorted(int(p.stem) for p in chunks.iterdir() if p.suffix == "")
    expected_set = set(range(total_chunks))
    missing = expected_set - set(received)
    if missing:
        raise ValueError(f"Missing chunks: {sorted(missing)}")

    original_name = metadata.get("file_name", f"uploaded_{upload_id}")
    dest_name = original_name if original_name.endswith((".7z", ".xlsx")) else f"{original_name}.7z"
    dest_path = _session_dir(upload_id) / dest_name

    # Merge chunks sequentially
    total_size = 0
    sha256 = hashlib.sha256()
    with open(dest_path, "wb") as f:
        for idx in range(total_chunks):
            chunk_data = (chunks / str(idx)).read_bytes()
            f.write(chunk_data)
            sha256.update(chunk_data)
            total_size += len(chunk_data)

    # Verify hash
    if expected_hash and sha256.hexdigest() != expected_hash:
        dest_path.unlink(missing_ok=True)
        raise ValueError(f"Hash mismatch: expected {expected_hash}, got {sha256.hexdigest()}")
    if expected_size and total_size != expected_size:
        dest_path.unlink(missing_ok=True)
        raise ValueError(f"Size mismatch: expected {expected_size}, got {total_size}")

    return {
        "file_path": str(dest_path),
        "file_name": dest_name,
        "file_size": total_size,
        "file_type": file_type,
    }


def cleanup_upload_session(upload_id: str) -> None:
    """Remove an upload session and its chunks from disk."""
    import shutil
    session = _session_dir(upload_id)
    shutil.rmtree(session, ignore_errors=True)


def register_import_source(
    file_path: str,
    file_type: str = "lci",
) -> Path:
    """Validate file exists and return absolute path for import consumption."""
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Import source not found: {file_path}")
    return p
