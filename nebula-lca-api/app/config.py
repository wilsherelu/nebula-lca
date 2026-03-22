from pydantic import BaseModel
import os
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent


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
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./lca_demo.db")
    cors_origins: list[str] = ["http://localhost:5173", "http://127.0.0.1:5173"]
    nebula_lca_ef31_dir: str = os.getenv(
        "NEBULA_LCA_EF31_DIR",
        _default_ef31_dir(),
    )
    nebula_lca_solver_api_url: str = os.getenv("NEBULA_LCA_SOLVER_API_URL", "http://127.0.0.1:8000")
    debug: bool = _env_bool("DEBUG", False)
    admin_token: str = os.getenv("ADMIN_TOKEN", "")
    keep_latest_versions_per_project: int = int(os.getenv("KEEP_LATEST_VERSIONS_PER_PROJECT", "1000"))
    auto_prune_on_startup: bool = _env_bool("AUTO_PRUNE_ON_STARTUP", True)
    auto_vacuum_after_prune_on_startup: bool = _env_bool("AUTO_VACUUM_AFTER_PRUNE_ON_STARTUP", False)
    auto_bootstrap_reference_data_on_startup: bool = _env_bool("AUTO_BOOTSTRAP_REFERENCE_DATA_ON_STARTUP", True)


settings = Settings()
