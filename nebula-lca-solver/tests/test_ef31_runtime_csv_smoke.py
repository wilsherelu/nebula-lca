import csv

from app.core.matrix_builder import build_c_matrix_from_ef31


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

    with (root / "lcia_factors.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        writer.writerow([0, 0, 1.0])
        writer.writerow([0, 1, 27.0])


def test_build_c_matrix_reads_generated_ef31_runtime_csvs(tmp_path):
    _write_runtime_csvs(tmp_path)
    issues = []

    result = build_c_matrix_from_ef31(
        str(tmp_path),
        {"rows": ["flow-co2", "flow-ch4"], "cols": [], "data": []},
        issues=issues,
    )

    assert issues == []
    assert result["C"]["rows"] == [0]
    assert result["C"]["cols"] == ["flow-co2", "flow-ch4"]
    assert result["C"]["shape"] == [1, 2]
    assert [item["value"] for item in result["C"]["data"]] == [1.0, 27.0]
    assert result["indicator_lookup"][0]["ecoinvent_category"] == "climate change"


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
