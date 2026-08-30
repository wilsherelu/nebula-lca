from pydantic import BaseModel, Field
import os
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent


def _env_path(name: str) -> Path | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    return Path(raw.strip())


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None:
        return default
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or default


def _default_data_dir() -> Path:
    configured = _env_path("NEBULA_DATA_DIR")
    if configured is not None:
        return configured
    appdata = os.getenv("APPDATA")
    if _env_bool("NEBULA_DESKTOP", False) and appdata:
        return Path(appdata) / "Nebula LCA"
    return PROJECT_ROOT


def _default_database_url() -> str:
    configured = os.getenv("DATABASE_URL")
    if configured:
        return configured
    db_path = _env_path("NEBULA_DB_PATH")
    if db_path is None and _env_bool("NEBULA_DESKTOP", False):
        db_path = _default_data_dir() / "nebula-lca.db"
    if db_path is not None:
        return f"sqlite:///{db_path.as_posix()}"
    return "sqlite:///./lca_demo.db"


def _default_runtime_root() -> Path:
    return (
        _env_path("NEBULA_LCA_RUNTIME_ROOT")
        or _env_path("NEBULA_RUNTIME_DIR")
        or (_default_data_dir() / "runtime" if _env_bool("NEBULA_DESKTOP", False) else PROJECT_ROOT / "runtime")
    )


def _default_import_cache_root() -> Path:
    return _env_path("NEBULA_IMPORT_CACHE_DIR") or _default_data_dir() / "import-cache"


def _default_ef31_dir() -> str:
    candidates = [
        WORKSPACE_ROOT / "ref_code" / "nebula-lca-solver" / "data" / "EF3.1",
        WORKSPACE_ROOT / "nebula-lca-solver" / "data" / "EF3.1",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(candidates[0])


class Settings(BaseModel):
    app_name: str = "LCA Backend Demo"
    desktop_mode: bool = Field(default_factory=lambda: _env_bool("NEBULA_DESKTOP", False))
    data_dir: str = Field(default_factory=lambda: str(_default_data_dir()))
    database_url: str = Field(default_factory=_default_database_url)
    cors_origins: list[str] = Field(default_factory=lambda: _env_list("NEBULA_CORS_ORIGINS", ["http://localhost:5173", "http://127.0.0.1:5173"]))
    nebula_lca_ef31_dir: str = Field(default_factory=lambda: os.getenv("NEBULA_LCA_EF31_DIR", _default_ef31_dir()))
    provider_tidas_flow_snapshot_path: str = Field(
        default_factory=lambda: os.getenv("NEBULA_PROVIDER_TIDAS_FLOW_SNAPSHOT_PATH", "").strip()
    )
    provider_tidas_process_snapshot_path: str = Field(
        default_factory=lambda: os.getenv("NEBULA_PROVIDER_TIDAS_PROCESS_SNAPSHOT_PATH", "").strip()
    )
    nebula_lca_runtime_root: str = Field(default_factory=lambda: str(_default_runtime_root()))
    import_cache_root: str = Field(default_factory=lambda: str(_default_import_cache_root()))
    nebula_lca_solver_api_url: str = Field(default_factory=lambda: os.getenv("NEBULA_LCA_SOLVER_API_URL", "http://127.0.0.1:8000"))
    debug: bool = Field(default_factory=lambda: _env_bool("DEBUG", False))
    admin_token: str = Field(default_factory=lambda: os.getenv("ADMIN_TOKEN", ""))
    keep_latest_versions_per_project: int = Field(default_factory=lambda: int(os.getenv("KEEP_LATEST_VERSIONS_PER_PROJECT", "20")))
    auto_prune_on_startup: bool = Field(default_factory=lambda: _env_bool("AUTO_PRUNE_ON_STARTUP", False))
    auto_vacuum_after_prune_on_startup: bool = Field(default_factory=lambda: _env_bool("AUTO_VACUUM_AFTER_PRUNE_ON_STARTUP", False))
    auto_bootstrap_reference_data_on_startup: bool = Field(default_factory=lambda: _env_bool("AUTO_BOOTSTRAP_REFERENCE_DATA_ON_STARTUP", True))
    auto_startup_maintenance_on_startup: bool = Field(default_factory=lambda: _env_bool("AUTO_STARTUP_MAINTENANCE_ON_STARTUP", True))
    import_cache_retention_hours: int = Field(default_factory=lambda: int(os.getenv("IMPORT_CACHE_RETENTION_HOURS", "24")))
    import_cache_cleanup_on_terminal: bool = Field(default_factory=lambda: _env_bool("IMPORT_CACHE_CLEANUP_ON_TERMINAL", True))
    data_platform_credential_key: str = Field(default_factory=lambda: os.getenv("DATA_PLATFORM_CREDENTIAL_KEY", ""))
    data_platform_credential_key_file: str = Field(
        default_factory=lambda: os.getenv(
            "DATA_PLATFORM_CREDENTIAL_KEY_FILE",
            str(_default_runtime_root() / "secrets" / "data_platform_credential.key"),
        )
    )


settings = Settings()
