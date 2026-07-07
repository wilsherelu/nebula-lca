from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import app
from app import solver_adapter


def _write_runtime_csvs(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "flow_index.csv").write_text("flow_index;FlowUUID;FlowName\n0;flow-a;Flow A\n", encoding="utf-8")
    (root / "indicator_index.csv").write_text(
        "indicator_index;method_en;method_zh;indicator_en;indicator_zh\n0;EF v3.1;EF v3.1;Climate change;Climate change\n",
        encoding="utf-8",
    )
    (root / "lcia_factors.csv").write_text("row;column;coefficient\n0;0;1.0\n", encoding="utf-8")


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


def test_desktop_embedded_solver_uses_appdata_runtime_before_workspace_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime_dir = tmp_path / "runtime" / "ef31"
    artifact_dir = runtime_dir / "lcia-official"
    artifact_dir.mkdir(parents=True)
    for name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv"):
        (artifact_dir / name).write_text("", encoding="utf-8")
    (runtime_dir / "active_manifest.json").write_text(
        '{"artifact_dir":"' + artifact_dir.as_posix() + '"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(solver_adapter.settings, "desktop_mode", True)
    monkeypatch.setattr(solver_adapter.settings, "nebula_lca_runtime_root", str(tmp_path / "runtime"))
    monkeypatch.setattr(solver_adapter.settings, "nebula_lca_ef31_dir", str(tmp_path / "missing-ref-code"))

    assert solver_adapter._resolve_embedded_ef31_dir() == artifact_dir


def test_desktop_embedded_solver_resolves_appdata_and_bundled_baseline(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime_dir = tmp_path / "runtime" / "ef31"
    active_dir = runtime_dir / "active"
    _write_runtime_csvs(active_dir)
    (runtime_dir / "active_manifest.json").write_text(
        '{"artifact_dir":"' + active_dir.as_posix() + '"}',
        encoding="utf-8",
    )
    bundle_root = tmp_path / "bundle"
    baseline_dir = bundle_root / "solver-data" / "EF3.1"
    _write_runtime_csvs(baseline_dir)

    monkeypatch.setattr(sys, "_MEIPASS", str(bundle_root), raising=False)
    monkeypatch.setattr(solver_adapter.settings, "desktop_mode", True)
    monkeypatch.setattr(solver_adapter.settings, "nebula_lca_runtime_root", str(tmp_path / "runtime"))
    monkeypatch.setattr(solver_adapter.settings, "nebula_lca_ef31_dir", str(tmp_path / "missing-ref-code"))

    assert solver_adapter._resolve_embedded_ef31_dirs() == [active_dir, baseline_dir]
