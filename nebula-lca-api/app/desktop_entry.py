from __future__ import annotations

import os
import json
import shutil
import sys
from pathlib import Path

import uvicorn


def _bundled_runtime_root() -> Path | None:
    bundle_root = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[1]))
    candidate = bundle_root / "runtime"
    return candidate if candidate.exists() else None


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _runtime_manifest_ready(runtime_root: Path) -> bool:
    manifest_path = runtime_root / "ef31" / "active_manifest.json"
    if not manifest_path.exists():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    artifact_dir = Path(str(manifest.get("artifact_dir") or manifest.get("output_dir") or ""))
    if not artifact_dir.is_absolute():
        artifact_dir = manifest_path.parent / artifact_dir
    if not _is_under(artifact_dir, runtime_root):
        return False
    return all(
        (artifact_dir / name).exists()
        for name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv")
    )


def _normalize_ef31_active_manifest(runtime_root: Path) -> None:
    manifest_path = runtime_root / "ef31" / "active_manifest.json"
    if not manifest_path.exists():
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return
    job_id = str(manifest.get("job_id") or "")
    artifact_dir = runtime_root / "ef31" / job_id if job_id else None
    if artifact_dir is None or not artifact_dir.exists():
        raw_artifact_dir = Path(str(manifest.get("artifact_dir") or manifest.get("output_dir") or ""))
        artifact_dir = raw_artifact_dir if raw_artifact_dir.is_absolute() else manifest_path.parent / raw_artifact_dir
    if not artifact_dir.exists():
        return
    manifest["artifact_dir"] = str(artifact_dir)
    manifest["output_dir"] = str(artifact_dir)
    text = json.dumps(manifest, ensure_ascii=False, default=str)
    manifest_path.write_text(text, encoding="utf-8")
    artifact_manifest = artifact_dir / "active_manifest.json"
    if artifact_manifest.exists():
        artifact_manifest.write_text(text, encoding="utf-8")


def _seed_runtime_if_needed(runtime_root: Path) -> None:
    if _runtime_manifest_ready(runtime_root):
        return
    bundled = _bundled_runtime_root()
    if bundled is None:
        return
    shutil.copytree(bundled, runtime_root, dirs_exist_ok=True)
    _normalize_ef31_active_manifest(runtime_root)


def _ensure_desktop_dirs() -> None:
    data_dir = Path(os.environ["NEBULA_DATA_DIR"])
    runtime_root = Path(os.environ.get("NEBULA_LCA_RUNTIME_ROOT") or data_dir / "runtime")
    import_cache = Path(os.environ.get("NEBULA_IMPORT_CACHE_DIR") or data_dir / "import-cache")
    credential_file = Path(
        os.environ.get("DATA_PLATFORM_CREDENTIAL_KEY_FILE")
        or runtime_root / "secrets" / "data_platform_credential.key"
    )
    for path in (
        data_dir,
        runtime_root,
        import_cache,
        data_dir / "logs",
        data_dir / "backups",
        credential_file.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)
    _seed_runtime_if_needed(runtime_root)


def main() -> None:
    data_dir = Path(os.environ.get("NEBULA_DATA_DIR") or Path.home() / "AppData" / "Roaming" / "Nebula LCA")
    os.environ.setdefault("NEBULA_DESKTOP", "1")
    os.environ.setdefault("NEBULA_DATA_DIR", str(data_dir))
    os.environ.setdefault("NEBULA_DB_PATH", str(data_dir / "nebula-lca.db"))
    os.environ.setdefault("DATABASE_URL", f"sqlite:///{(data_dir / 'nebula-lca.db').as_posix()}")
    os.environ.setdefault("NEBULA_LCA_RUNTIME_ROOT", str(data_dir / "runtime"))
    os.environ.setdefault("NEBULA_IMPORT_CACHE_DIR", str(data_dir / "import-cache"))
    os.environ.setdefault(
        "DATA_PLATFORM_CREDENTIAL_KEY_FILE",
        str(data_dir / "runtime" / "secrets" / "data_platform_credential.key"),
    )
    os.environ.setdefault("AUTO_BOOTSTRAP_REFERENCE_DATA_ON_STARTUP", "false")
    os.environ.setdefault("AUTO_STARTUP_MAINTENANCE_ON_STARTUP", "false")
    _ensure_desktop_dirs()

    port = int(os.environ.get("NEBULA_API_PORT", "0") or "0")
    if port <= 0:
        port = 8765
    uvicorn.run("app.main:app", host="127.0.0.1", port=port, log_level="info")


if __name__ == "__main__":
    main()
