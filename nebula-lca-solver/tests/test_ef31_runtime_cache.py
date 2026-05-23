import csv
import json

from fastapi.testclient import TestClient

from app.api.v1 import _build_source_partitioned_ef31_c_matrix, _resolve_runtime_csv_dir
from app.core.ef31_runtime_cache import Ef31RuntimeCache, GLOBAL_EF31_RUNTIME_CACHE
from app.main import app


def _write_legacy_runtime(root):
    with (root / "flow_index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        writer.writerow([0, "flow-co2", "Carbon dioxide"])

    with (root / "indicator_index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["indicator_index", "method_en", "method_zh", "indicator_en", "indicator_zh", "ecoinvent_category"])
        writer.writerow([0, "Climate change", "Climate change", "Climate change", "Climate change", "climate change"])
        writer.writerow([1, "Acidification", "Acidification", "Acidification", "Acidification", "acidification"])

    with (root / "lcia_factors.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        writer.writerow([0, 0, 1.0])
        writer.writerow([1, 0, 2.0])


def _write_ecoinvent_runtime(root):
    with (root / "flow_index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        writer.writerow([0, "flow-ch4", "Methane"])

    with (root / "indicator_index.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["indicator_index", "method_en", "method_zh", "indicator_en", "indicator_zh", "ecoinvent_category"])
        writer.writerow([0, "EF v3.1", "EF v3.1", "Global warming", "Global warming", "climate change"])
        writer.writerow([1, "EF v3.1 no LT", "EF v3.1 no LT", "Global warming no LT", "Global warming no LT", "climate change no LT"])

    with (root / "lcia_factors.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        writer.writerow([0, 0, 27.0])
        writer.writerow([1, 0, 25.0])


def _snapshot():
    return {
        "processes": [
            {
                "process_uuid": "p1",
                "process_name": "process",
                "reference_product_flow_uuid": "ref",
            }
        ],
        "flows": [
            {
                "flow_uuid": "product",
                "flow_name": "product",
                "flow_type": "Product flow",
                "unit": "kg",
                "unit_group_uuid": "mass",
            },
            {
                "flow_uuid": "flow-co2",
                "flow_name": "Carbon dioxide",
                "flow_type": "Elementary flow",
                "unit": "kg",
                "unit_group_uuid": "mass",
                "source_system": "ef3.1",
            },
            {
                "flow_uuid": "flow-ch4",
                "flow_name": "Methane",
                "flow_type": "Elementary flow",
                "unit": "kg",
                "unit_group_uuid": "mass",
                "source_system": "ecoinvent",
            },
        ],
        "exchanges": [
            {
                "exchange_id": "ref",
                "process_uuid": "p1",
                "flow_uuid": "product",
                "direction": "output",
                "amount": 1.0,
            },
            {
                "exchange_id": "co2",
                "process_uuid": "p1",
                "flow_uuid": "flow-co2",
                "direction": "output",
                "amount": 1.0,
            },
            {
                "exchange_id": "ch4",
                "process_uuid": "p1",
                "flow_uuid": "flow-ch4",
                "direction": "output",
                "amount": 1.0,
            },
        ],
        "links": [],
    }


def test_ef31_runtime_cache_reuses_loaded_sources(tmp_path):
    legacy_dir = tmp_path / "legacy"
    eco_dir = tmp_path / "eco"
    legacy_dir.mkdir()
    eco_dir.mkdir()
    _write_legacy_runtime(legacy_dir)
    _write_ecoinvent_runtime(eco_dir)

    cache = Ef31RuntimeCache()
    b_matrix = {"rows": ["flow-co2", "flow-ch4"], "cols": ["p1"], "data": []}

    first = cache.build_c_matrix_from_sources(
        [str(legacy_dir), str(eco_dir)],
        b_matrix,
        lcia_methods=["EF v3.1"],
        issues=[],
    )
    second = cache.build_c_matrix_from_sources(
        [str(legacy_dir), str(eco_dir)],
        b_matrix,
        lcia_methods=["EF v3.1"],
        issues=[],
    )

    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert first["C"] == second["C"]
    assert first["C"]["rows"] == [0, 1]
    assert [item["value"] for item in first["C"]["data"]] == [1.0, 27.0, 2.0]


def test_active_manifest_runtime_root_resolves_to_artifact_dir(tmp_path):
    runtime_root = tmp_path / "runtime" / "ef31"
    artifact_dir = runtime_root / "official"
    artifact_dir.mkdir(parents=True)
    _write_ecoinvent_runtime(artifact_dir)
    (runtime_root / "active_manifest.json").write_text(
        json.dumps({"artifact_dir": str(artifact_dir)}),
        encoding="utf-8",
    )

    assert _resolve_runtime_csv_dir(runtime_root) == artifact_dir


def test_source_partitioning_does_not_cross_match_ecoinvent_flows(tmp_path):
    legacy_dir = tmp_path / "legacy"
    eco_dir = tmp_path / "eco"
    legacy_dir.mkdir()
    eco_dir.mkdir()
    _write_legacy_runtime(legacy_dir)
    _write_ecoinvent_runtime(eco_dir)
    GLOBAL_EF31_RUNTIME_CACHE.clear()

    snapshot = {
        "flows": [
            {
                "flow_uuid": "flow-co2",
                "flow_name": "Carbon dioxide",
                "source_system": "ecoinvent",
            }
        ]
    }
    b_matrix = {"rows": ["flow-co2"], "cols": ["p1"], "data": []}
    issues = []

    pack = _build_source_partitioned_ef31_c_matrix(
        snapshot=snapshot,
        b_matrix=b_matrix,
        legacy_ef31_dir=str(legacy_dir),
        ecoinvent_ef31_dir=str(eco_dir),
        lcia_methods=["EF v3.1"],
        issues=issues,
    )

    assert pack["C"]["data"] == []
    assert any("missing 1 flow_uuids" in issue for issue in issues)


def test_lcia_response_includes_runtime_cache_timing(tmp_path, monkeypatch):
    legacy_dir = tmp_path / "legacy"
    eco_dir = tmp_path / "eco"
    legacy_dir.mkdir()
    eco_dir.mkdir()
    _write_legacy_runtime(legacy_dir)
    _write_ecoinvent_runtime(eco_dir)
    GLOBAL_EF31_RUNTIME_CACHE.clear()

    monkeypatch.setenv("NEBULA_LCA_LEGACY_EF31_DIR", str(legacy_dir))
    monkeypatch.setenv("NEBULA_LCA_EF31_DIR", str(eco_dir))
    monkeypatch.setenv("NEBULA_LCA_OUTPUT_DIR", str(tmp_path / "exports"))

    client = TestClient(app)
    payload = {"snapshot": _snapshot(), "lcia_methods": ["EF v3.1"]}

    first = client.post("/v1/lcia", json=payload)
    second = client.post("/v1/lcia", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    first_summary = first.json()["summary"]
    second_summary = second.json()["summary"]
    assert first_summary["indicator_count"] == 2
    assert first_summary["missing_ef31_flow_count"] == 0
    assert first_summary["ef31_runtime_cache_hit"] is False
    assert second_summary["ef31_runtime_cache_hit"] is True
    assert second_summary["ef31_runtime_source_count"] == 2
    assert "timing_build_c_seconds" in second_summary
    assert "timing_solve_seconds" in second_summary
