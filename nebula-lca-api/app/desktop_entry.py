from __future__ import annotations

import os
from pathlib import Path

import uvicorn


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
