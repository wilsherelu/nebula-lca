import csv

from app.core.matrix_builder import build_c_matrix_from_ef31, build_c_matrix_from_ef31_sources


def _write_runtime_csvs(root):
    with (root / "flow_index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        writer.writerow([0, "flow-co2", "Carbon dioxide"])
        writer.writerow([1, "flow-ch4", "Methane"])

    with (root / "indicator_index.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["indicator_index", "method_en", "method_zh", "indicator_en", "indicator_zh", "ecoinvent_category"])
        writer.writerow([0, "EF v3.1", "EF v3.1", "Global warming", "Global warming", "climate change"])
        writer.writerow([1, "EF v3.1 no LT", "EF v3.1 no LT", "Global warming no LT", "Global warming no LT", "climate change no LT"])

    with (root / "lcia_factors.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        writer.writerow([0, 0, 1.0])
        writer.writerow([0, 1, 27.0])
        writer.writerow([1, 0, 0.9])
        writer.writerow([1, 1, 25.0])


def _write_legacy_tiangong_csvs(root):
    with (root / "flow_index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        writer.writerow([0, "flow-co2", "Carbon dioxide"])

    with (root / "indicator_index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["indicator_index", "method_en", "method_zh", "indicator_en", "indicator_zh", "ecoinvent_category"])
        writer.writerow([0, "Climate change", "气候变化", "Climate change", "气候变化", "climate change"])
        writer.writerow([1, "Acidification", "酸化", "Acidification", "酸化", "acidification"])

    with (root / "lcia_factors.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        writer.writerow([0, 0, 1.0])
        writer.writerow([1, 0, 2.0])


def test_build_c_matrix_reads_generated_ef31_runtime_csvs(tmp_path):
    _write_runtime_csvs(tmp_path)
    issues = []

    result = build_c_matrix_from_ef31(
        str(tmp_path),
        {"rows": ["flow-co2", "flow-ch4"], "cols": [], "data": []},
        issues=issues,
    )

    assert issues == []
    assert result["C"]["rows"] == [0, 1]
    assert result["C"]["cols"] == ["flow-co2", "flow-ch4"]
    assert result["C"]["shape"] == [2, 2]
    assert [item["value"] for item in result["C"]["data"]] == [1.0, 27.0, 0.9, 25.0]
    assert result["indicator_lookup"][0]["ecoinvent_category"] == "climate change"


def test_build_c_matrix_filters_lcia_methods(tmp_path):
    _write_runtime_csvs(tmp_path)
    issues = []

    result = build_c_matrix_from_ef31(
        str(tmp_path),
        {"rows": ["flow-co2", "flow-ch4"], "cols": [], "data": []},
        lcia_methods=["EF v3.1"],
        issues=issues,
    )

    assert issues == []
    assert result["C"]["rows"] == [0]
    assert result["C"]["shape"] == [1, 2]
    assert [item["value"] for item in result["C"]["data"]] == [1.0, 27.0]


def test_build_c_matrix_keeps_legacy_tiangong_ef31_family(tmp_path):
    _write_legacy_tiangong_csvs(tmp_path)
    issues = []

    result = build_c_matrix_from_ef31(
        str(tmp_path),
        {"rows": ["flow-co2"], "cols": [], "data": []},
        lcia_methods=["EF v3.1"],
        issues=issues,
    )

    assert issues == []
    assert result["C"]["rows"] == [0, 1]
    assert result["C"]["shape"] == [2, 1]
    assert [item["value"] for item in result["C"]["data"]] == [1.0, 2.0]


def test_build_c_matrix_reports_missing_runtime_flow(tmp_path):
    _write_runtime_csvs(tmp_path)
    issues = []

    result = build_c_matrix_from_ef31(
        str(tmp_path),
        {"rows": ["flow-co2", "flow-not-in-runtime"], "cols": [], "data": []},
        issues=issues,
    )

    assert result["C"]["cols"] == ["flow-co2", "flow-not-in-runtime"]
    assert any("missing 1 flow_uuids" in issue for issue in issues)


def test_build_c_matrix_merges_tiangong_and_ecoinvent_ef31_sources(tmp_path):
    legacy_dir = tmp_path / "legacy"
    eco_dir = tmp_path / "eco"
    legacy_dir.mkdir()
    eco_dir.mkdir()
    _write_legacy_tiangong_csvs(legacy_dir)
    _write_runtime_csvs(eco_dir)
    issues = []

    result = build_c_matrix_from_ef31_sources(
        [str(legacy_dir), str(eco_dir)],
        {"rows": ["flow-co2", "flow-ch4"], "cols": [], "data": []},
        lcia_methods=["EF v3.1"],
        issues=issues,
    )

    assert issues == []
    assert result["C"]["rows"] == [0, 1]
    assert result["C"]["cols"] == ["flow-co2", "flow-ch4"]
    assert result["C"]["shape"] == [2, 2]
    assert result["indicator_lookup"][0]["method_en"] == "EF v3.1"
    assert result["indicator_lookup"][0]["canonical_indicator_key"] == "climate change"
    assert [item["value"] for item in result["C"]["data"]] == [1.0, 27.0, 2.0]


def test_build_c_matrix_does_not_treat_no_lt_as_legacy_family(tmp_path):
    _write_legacy_tiangong_csvs(tmp_path)
    issues = []

    result = build_c_matrix_from_ef31(
        str(tmp_path),
        {"rows": ["flow-co2"], "cols": [], "data": []},
        lcia_methods=["EF v3.1 no LT"],
        issues=issues,
    )

    assert issues == []
    assert result["C"]["rows"] == []
    assert result["C"]["shape"] == [0, 1]
