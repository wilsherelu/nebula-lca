from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import app


def test_desktop_settings_keep_writable_paths_in_data_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("NEBULA_DESKTOP", "1")
    monkeypatch.setenv("NEBULA_DATA_DIR", str(tmp_path))
    settings = Settings()

    assert settings.desktop_mode is True
    assert settings.data_dir == str(tmp_path)
    assert settings.database_url == f"sqlite:///{(tmp_path / 'nebula-lca.db').as_posix()}"
    assert settings.nebula_lca_runtime_root == str(tmp_path / "runtime")
    assert settings.import_cache_root == str(tmp_path / "import-cache")
    assert settings.data_platform_credential_key_file == str(
        tmp_path / "runtime" / "secrets" / "data_platform_credential.key"
    )


def test_health_and_desktop_status_routes() -> None:
    with TestClient(app) as client:
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        status = client.get("/api/desktop/status")
        assert status.status_code == 200
        payload = status.json()
        assert payload["status"] == "ok"
        assert "paths" in payload
        assert "catalog" in payload
        assert "runtime" in payload
