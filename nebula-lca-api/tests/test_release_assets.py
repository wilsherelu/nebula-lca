from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.release_assets import select_latest_public_mapping_root, select_official_ef31_runtime


def _write_mapping_release(root: Path, version: str, *, status: str = "passed", l3: bool = False) -> None:
    root.mkdir(parents=True)
    (root / "MANIFEST.json").write_text(json.dumps({
        "dataset_version": version,
        "schema_version": "nebula-flow-mapping-release.v1",
        "mapping_direction": "TIANGONG_TO_ECOINVENT",
        "forbidden_content_included": False,
        "l3_included": l3,
    }), encoding="utf-8")
    (root / "ACCEPTANCE.json").write_text(json.dumps({
        "dataset_version": version,
        "status": status,
        "l3_included": l3,
        "forbidden_public_fields": 0,
    }), encoding="utf-8")


def test_latest_accepted_mapping_release_wins_and_smoke_is_ignored(tmp_path: Path) -> None:
    _write_mapping_release(tmp_path / "nebula-flow-mapping-v1", "1.0.0")
    _write_mapping_release(tmp_path / "nebula-flow-mapping-v2", "2.0.0")
    _write_mapping_release(tmp_path / "nebula-flow-mapping-smoke", "99.0.0", status="smoke")
    _write_mapping_release(tmp_path / "nebula-flow-mapping-l3", "98.0.0", l3=True)

    assert select_latest_public_mapping_root(tmp_path).name == "nebula-flow-mapping-v2"


def test_mapping_release_selection_fails_closed(tmp_path: Path) -> None:
    _write_mapping_release(tmp_path / "nebula-flow-mapping-demo", "1.0.0-dev")

    with pytest.raises(FileNotFoundError):
        select_latest_public_mapping_root(tmp_path)


def test_release_runtime_must_be_active_official_artifact(tmp_path: Path) -> None:
    ef31_root = tmp_path / "ef31"
    artifact = ef31_root / "lcia-official-3.11"
    artifact.mkdir(parents=True)
    for name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv"):
        (artifact / name).write_text("ok", encoding="utf-8")
    (ef31_root / "active_manifest.json").write_text(
        json.dumps({"job_id": artifact.name, "artifact_dir": "ignored-local-path"}),
        encoding="utf-8",
    )

    manifest, selected = select_official_ef31_runtime(tmp_path)

    assert manifest["job_id"] == artifact.name
    assert selected == artifact.resolve()


def test_release_runtime_rejects_smoke_artifact(tmp_path: Path) -> None:
    ef31_root = tmp_path / "ef31"
    ef31_root.mkdir(parents=True)
    (ef31_root / "active_manifest.json").write_text(
        json.dumps({"job_id": "smoke-runtime"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="lcia-official"):
        select_official_ef31_runtime(tmp_path)
