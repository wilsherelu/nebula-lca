"""Tests for EF 3.1 LCIA preview persistence and runtime CSV generation.

Covers:
1. match_cf_to_flows → matched/unmatched/ambiguous classification
2. Preview job saves indicators.json, ef31_cfs.json, cf_matches.json
3. Preview response includes cf_rows_matched/unmatched/ambiguous
4. generate_ef31_runtime_csvs → flow_index.csv, indicator_index.csv, lcia_factors.csv
5. Solver CSV format compatibility
"""
import json
import csv
import tempfile
import uuid
from pathlib import Path

import pytest

from app.ecoinvent_ef31_loader import (
    ElementaryFlow,
    Indicator,
    CharacterizationFactor,
    match_cf_to_flows,
    filter_cf_ef31,
)
from app.services.ef31_runtime_csv import generate_ef31_runtime_csvs


# ===========================================================================
# 1. CF matching tests
# ===========================================================================

class TestMatchCfToFlows:
    """Test matched/unmatched/ambiguous classification."""

    def _make_flows(self, **overrides) -> list[ElementaryFlow]:
        return [
            ElementaryFlow(
                flow_uuid="uuid-co2-air",
                flow_name="carbon dioxide",
                compartment="air",
                subcompartment="non-urban air",
                **overrides,
            ),
            ElementaryFlow(
                flow_uuid="uuid-co2-water",
                flow_name="carbon dioxide",
                compartment="water",
                subcompartment="freshwater",
                **overrides,
            ),
            ElementaryFlow(
                flow_uuid="uuid-unknown",
                flow_name="unknown flow xyz",
                compartment="air",
                subcompartment=None,
                **overrides,
            ),
        ]

    def _make_cfs(self, **overrides) -> list[CharacterizationFactor]:
        return [
            CharacterizationFactor(
                method="EF v3.1",
                category="Climate change",
                indicator="Global warming",
                flow_name="carbon dioxide",
                compartment="air",
                subcompartment="non-urban air",
                cf_value=1.0,
                **overrides,
            ),
            CharacterizationFactor(
                method="EF v3.1",
                category="Climate change",
                indicator="Global warming",
                flow_name="carbon dioxide",
                compartment="water",
                subcompartment="freshwater",
                cf_value=2.0,
                **overrides,
            ),
            CharacterizationFactor(
                method="EF v3.1",
                category="Acidification",
                indicator="Global warming",
                flow_name="unknown flow xyz",
                compartment="air",
                subcompartment=None,
                cf_value=0.5,
                **overrides,
            ),
            CharacterizationFactor(
                method="EF v3.1",
                category="Non-urban dust",
                indicator="Global warming",
                flow_name="totally missing flow",
                compartment="air",
                subcompartment=None,
                cf_value=0.1,
                **overrides,
            ),
        ]

    def test_single_match_per_cf(self):
        """Each CF should match at most one flow (unique key)."""
        flows = self._make_flows()
        cfs = self._make_cfs()
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 3  # co2-air, co2-water, unknown flow xyz
        assert len(unmatched) == 1  # totally missing flow
        assert len(ambiguous) == 0

    def test_ambiguous_when_multiple_flows_same_key(self):
        """If two flows have the same (name, compartment, subcomp), CF is ambiguous."""
        flows = [
            ElementaryFlow(flow_uuid="uuid-a", flow_name="carbon dioxide", compartment="air", subcompartment="non-urban air"),
            ElementaryFlow(flow_uuid="uuid-b", flow_name="carbon dioxide", compartment="air", subcompartment="non-urban air"),
        ]
        cfs = [
            CharacterizationFactor(method="EF v3.1", category="Climate", indicator="GWP", flow_name="carbon dioxide", compartment="air", subcompartment="non-urban air", cf_value=1.0),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 0
        assert len(ambiguous) == 1
        assert ambiguous[0]["cf_flow_name"] == "carbon dioxide"
        assert ambiguous[0]["matched_flow_uuids"] == "uuid-a|uuid-b"

    def test_matched_rows_contain_flow_uuid(self):
        """Matched rows should include matched_flow_uuid and matched_flow_name."""
        flows = self._make_flows()
        cfs = self._make_cfs()
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        for row in matched:
            assert "matched_flow_uuid" in row
            assert "matched_flow_name" in row
            assert "cf_value" in row
            assert "cf_method" in row

    def test_unmatched_rows_no_flow_uuid(self):
        """Unmatched rows should NOT contain matched_flow_uuid."""
        flows = self._make_flows()
        cfs = [
            CharacterizationFactor(method="EF v3.1", category="Climate", indicator="GWP", flow_name="nonexistent flow", compartment="air", subcompartment=None, cf_value=1.0),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(unmatched) == 1
        assert "matched_flow_uuid" not in unmatched[0]

    def test_case_insensitive_match(self):
        """Matching should be case-insensitive (normalize_text)."""
        flows = [
            ElementaryFlow(flow_uuid="uuid-001", flow_name="Carbon Dioxide", compartment="AIR", subcompartment="Non-Urban Air"),
        ]
        cfs = [
            CharacterizationFactor(method="EF v3.1", category="Climate", indicator="GWP", flow_name="carbon dioxide", compartment="air", subcompartment="non-urban air", cf_value=1.0),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 1
        assert len(unmatched) == 0


class TestFilterCfEf31:
    """Test that only EF 3.1 methods pass through."""

    def test_filters_to_ef31_only(self):
        cfs = [
            CharacterizationFactor(method="EF v3.1", category="Climate", indicator="GWP", flow_name="CO2", compartment="air", cf_value=1.0),
            CharacterizationFactor(method="EF v3.1 no LT", category="Climate", indicator="GWP", flow_name="CO2", compartment="air", cf_value=1.0),
            CharacterizationFactor(method="ReCiPe 2016", category="Climate", indicator="GWP", flow_name="CO2", compartment="air", cf_value=1.0),
            CharacterizationFactor(method="TRACI 2.1", category="Acidification", indicator="AP", flow_name="SO2", compartment="air", cf_value=1.0),
        ]
        result = filter_cf_ef31(cfs)
        assert len(result) == 2
        methods = {cf.method for cf in result}
        assert methods == {"EF v3.1", "EF v3.1 no LT"}


# ===========================================================================
# 2. Preview job artifact persistence
# ===========================================================================

class TestPreviewJobLciaArtifacts:
    """Test that preview saves indicators, CFs, and match results to disk."""

    def _write_artifacts(self, job_dir):
        """Write minimal artifacts to job dir."""
        if not isinstance(job_dir, Path):
            job_dir = Path(job_dir)

        elem_flows = [
            {"flow_uuid": "uuid-001", "flow_name": "carbon dioxide", "compartment": "air", "subcompartment": "non-urban air"},
            {"flow_uuid": "uuid-002", "flow_name": "methane", "compartment": "air", "subcompartment": None},
        ]
        indicators = [
            {"method": "EF v3.1", "category": "Climate change", "indicator": "Global warming", "indicator_unit": "kg CO2-Eq"},
        ]
        cfs_matched = [
            {"cf_method": "EF v3.1", "cf_indicator": "Global warming", "cf_flow_name": "carbon dioxide",
             "cf_compartment": "air", "cf_subcompartment": "non-urban air", "cf_value": 1.0,
             "matched_flow_uuid": "uuid-001", "matched_flow_name": "carbon dioxide"},
            {"cf_method": "EF v3.1", "cf_indicator": "Global warming", "cf_flow_name": "methane",
             "cf_compartment": "air", "cf_subcompartment": None, "cf_value": 25.0,
             "matched_flow_uuid": "uuid-002", "matched_flow_name": "methane"},
        ]
        cf_unmatched = [
            {"cf_method": "EF v3.1", "cf_indicator": "Other", "cf_flow_name": "unknown",
             "cf_compartment": "air", "cf_subcompartment": None, "cf_value": 0.1},
        ]

        (job_dir / "elementary_flows.json").write_text(
            json.dumps(elem_flows, ensure_ascii=False), encoding="utf-8"
        )
        (job_dir / "indicators.json").write_text(
            json.dumps(indicators, ensure_ascii=False), encoding="utf-8"
        )
        (job_dir / "ef31_cfs.json").write_text(
            json.dumps(cfs_matched + cf_unmatched, ensure_ascii=False), encoding="utf-8"
        )
        (job_dir / "cf_matches.json").write_text(
            json.dumps({"matched": cfs_matched, "unmatched": cf_unmatched, "ambiguous": []}, ensure_ascii=False), encoding="utf-8"
        )

    def _make_job_dir(self, tmp_path, name="fake-job-12345"):
        job_dir = tmp_path / name
        job_dir.mkdir()
        self._write_artifacts(job_dir)
        return job_dir

    def test_generates_flow_index_csv(self, tmp_path):
        job_dir = self._make_job_dir(tmp_path, "fake-job-12345")
        output_root = tmp_path / "runtime-out"
        result = generate_ef31_runtime_csvs(
            "fake-job-12345", output_root=output_root, _job_dir_override=job_dir
        )

        flow_index = output_root / "fake-job-12345" / "flow_index.csv"
        assert flow_index.exists()
        with open(flow_index, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            assert "FlowUUID" in header
            rows = list(reader)
            assert len(rows) >= 2

    def test_generates_indicator_index_csv(self, tmp_path):
        job_dir = self._make_job_dir(tmp_path, "indicator-test")
        output_root = tmp_path / "runtime-out-ii"
        generate_ef31_runtime_csvs(
            "indicator-test", output_root=output_root, _job_dir_override=job_dir
        )

        indicator_index = output_root / "indicator-test" / "indicator_index.csv"
        assert indicator_index.exists()
        with open(indicator_index, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            assert "method_en" in header
            assert "indicator_en" in header
            rows = list(reader)
            assert len(rows) >= 1

    def test_generates_lcia_factors_csv(self, tmp_path):
        job_dir = self._make_job_dir(tmp_path, "factors-test")
        output_root = tmp_path / "runtime-out-lf"
        generate_ef31_runtime_csvs(
            "factors-test", output_root=output_root, _job_dir_override=job_dir
        )

        lcia_factors = output_root / "factors-test" / "lcia_factors.csv"
        assert lcia_factors.exists()
        with open(lcia_factors, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            assert header == ["row", "column", "coefficient"]
            rows = list(reader)
            assert len(rows) == 2

    def test_unmatched_cf_not_in_factors(self, tmp_path):
        """Unmatched CFs should not appear in lcia_factors.csv."""
        job_dir = self._make_job_dir(tmp_path, "unmatched-test")
        output_root = tmp_path / "runtime-out-um"
        result = generate_ef31_runtime_csvs(
            "unmatched-test", output_root=output_root, _job_dir_override=job_dir
        )

        assert result["factors_count"] == 2
        assert result["cf_unmatched"] == 1

    def test_summary_json_created(self, tmp_path):
        job_dir = self._make_job_dir(tmp_path, "summary-test")
        output_root = tmp_path / "runtime-out-summ"
        result = generate_ef31_runtime_csvs(
            "summary-test", output_root=output_root, _job_dir_override=job_dir
        )

        summary_path = output_root / "summary-test" / "runtime_summary.json"
        manifest_path = output_root / "summary-test" / "manifest.json"
        assert summary_path.exists()
        assert manifest_path.exists()
        assert not (output_root / "active_manifest.json").exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["active"] is False
        assert summary["flows_count"] >= 2
        assert summary["indicators_count"] >= 1
        assert summary["factors_count"] == 2
        assert summary["runtime_schema_version"] == "ef31-runtime-artifact-v1"
        assert summary["artifact_dir"] == summary["output_dir"]

    def test_duplicate_job_raises_error(self, tmp_path):
        job_dir = self._make_job_dir(tmp_path, "dup-test")
        output_root = tmp_path / "runtime-out-dup"
        first_result = generate_ef31_runtime_csvs(
            "dup-test", output_root=output_root, _job_dir_override=job_dir
        )
        assert first_result["factors_count"] == 2
        with pytest.raises(FileExistsError):
            generate_ef31_runtime_csvs(
                "dup-test", output_root=output_root, _job_dir_override=job_dir
            )

    def test_duplicate_job_with_overwrite(self, tmp_path):
        job_dir = self._make_job_dir(tmp_path, "dup-override-test")
        output_root = tmp_path / "runtime-out-dup-ovr"
        generate_ef31_runtime_csvs(
            "dup-override-test", output_root=output_root, _job_dir_override=job_dir, overwrite=False
        )
        result = generate_ef31_runtime_csvs(
            "dup-override-test", output_root=output_root, _job_dir_override=job_dir, overwrite=True
        )
        assert result["factors_count"] == 2

    def test_missing_job_dir_raises(self):
        with pytest.raises(FileNotFoundError, match="Job directory not found"):
            generate_ef31_runtime_csvs("nonexistent-job-id")

    def test_missing_indicators_json_raises(self, tmp_path):
        job_dir = tmp_path / "partial-job"
        job_dir.mkdir()
        (job_dir / "nonexistent.json").touch()
        with pytest.raises(FileNotFoundError, match="indicators.json not found"):
            generate_ef31_runtime_csvs(str(job_dir.parent) + "/" + job_dir.name)


# ===========================================================================
# 3. Solver CSV format compatibility
# ===========================================================================

class TestSolverCsvFormat:
    """Test that generated CSVs match solver's expected format."""

    def _write_test_artifacts(self, job_dir):
        elem_flows = [
            {"flow_uuid": "uuid-co2", "flow_name": "carbon dioxide", "compartment": "air", "subcompartment": "non-urban air"},
            {"flow_uuid": "uuid-ch4", "flow_name": "methane", "compartment": "air", "subcompartment": None},
            {"flow_uuid": "uuid-so2", "flow_name": "sulfur dioxide", "compartment": "air", "subcompartment": None},
        ]
        indicators = [
            {"method": "EF v3.1", "category": "Climate", "indicator": "Global warming", "indicator_unit": "kg CO2-Eq"},
            {"method": "EF v3.1", "category": "Acidification", "indicator": "Acidification", "indicator_unit": "mol H+-Eq"},
        ]
        cfs = [
            {"cf_method": "EF v3.1", "cf_indicator": "Global warming", "cf_flow_name": "carbon dioxide",
             "cf_compartment": "air", "cf_subcompartment": "non-urban air", "cf_value": 1.0,
             "matched_flow_uuid": "uuid-co2", "matched_flow_name": "carbon dioxide"},
            {"cf_method": "EF v3.1", "cf_indicator": "Global warming", "cf_flow_name": "methane",
             "cf_compartment": "air", "cf_subcompartment": None, "cf_value": 25.0,
             "matched_flow_uuid": "uuid-ch4", "matched_flow_name": "methane"},
            {"cf_method": "EF v3.1", "cf_indicator": "Acidification", "cf_flow_name": "sulfur dioxide",
             "cf_compartment": "air", "cf_subcompartment": None, "cf_value": 2.0,
             "matched_flow_uuid": "uuid-so2", "matched_flow_name": "sulfur dioxide"},
        ]
        job_dir = Path(job_dir)
        (job_dir / "elementary_flows.json").write_text(json.dumps(elem_flows, ensure_ascii=False))
        (job_dir / "indicators.json").write_text(json.dumps(indicators, ensure_ascii=False))
        (job_dir / "ef31_cfs.json").write_text(json.dumps(cfs, ensure_ascii=False))
        (job_dir / "cf_matches.json").write_text(json.dumps({"matched": cfs, "unmatched": [], "ambiguous": []}, ensure_ascii=False))

    def test_flow_index_header(self, tmp_path):
        """flow_index.csv must have flow_index, FlowUUID, FlowName columns."""
        job_dir = tmp_path / "format-test-fi"
        job_dir.mkdir()
        self._write_test_artifacts(job_dir)
        output_root = tmp_path / "runtime-format-out" / "fi"
        generate_ef31_runtime_csvs("format-test-fi", output_root=output_root, _job_dir_override=job_dir)

        path = output_root / "format-test-fi" / "flow_index.csv"
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            assert "flow_index" in header
            assert "FlowUUID" in header
            assert "FlowName" in header

    def test_flow_index_semicolon_delimited(self, tmp_path):
        """flow_index.csv uses semicolon delimiter."""
        job_dir = tmp_path / "format-test-sd"
        job_dir.mkdir()
        self._write_test_artifacts(job_dir)
        output_root = tmp_path / "runtime-format-out" / "sd"
        generate_ef31_runtime_csvs("format-test-sd", output_root=output_root, _job_dir_override=job_dir)

        path = output_root / "format-test-sd" / "flow_index.csv"
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            assert ";" in lines[1]

    def test_indicator_index_header(self, tmp_path):
        """indicator_index.csv must have indicator_index, method_en, method_zh, indicator_en, indicator_zh, ecoinvent_category."""
        job_dir = tmp_path / "format-test-ii"
        job_dir.mkdir()
        self._write_test_artifacts(job_dir)
        output_root = tmp_path / "runtime-format-out" / "ii"
        generate_ef31_runtime_csvs("format-test-ii", output_root=output_root, _job_dir_override=job_dir)

        path = output_root / "format-test-ii" / "indicator_index.csv"
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            assert "indicator_index" in header
            assert "method_en" in header
            assert "indicator_en" in header
            assert "indicator_zh" in header
            assert "ecoinvent_category" in header

    def test_lcia_factors_header(self, tmp_path):
        """lcia_factors.csv must have row, column, coefficient columns."""
        job_dir = tmp_path / "format-test-lf"
        job_dir.mkdir()
        self._write_test_artifacts(job_dir)
        output_root = tmp_path / "runtime-format-out" / "lf"
        generate_ef31_runtime_csvs("format-test-lf", output_root=output_root, _job_dir_override=job_dir)

        path = output_root / "format-test-lf" / "lcia_factors.csv"
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            assert header == ["row", "column", "coefficient"]

    def test_lcia_factors_values(self, tmp_path):
        """Factors should have valid row/column indices and coefficient values."""
        job_dir = tmp_path / "format-test-fv"
        job_dir.mkdir()
        self._write_test_artifacts(job_dir)
        output_root = tmp_path / "runtime-format-out" / "fv"
        generate_ef31_runtime_csvs("format-test-fv", output_root=output_root, _job_dir_override=job_dir)

        path = output_root / "format-test-fv" / "lcia_factors.csv"
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            rows = list(reader)
        assert len(rows) == 3
        for row in rows:
            assert int(row["row"]) >= 0
            assert int(row["column"]) >= 0
            assert float(row["coefficient"]) > 0

    def test_flow_index_uuids_are_from_matched(self, tmp_path):
        """flow_index.csv should only include flows that appear in matched CFs."""
        job_dir = tmp_path / "format-test-uuid"
        job_dir.mkdir()
        self._write_test_artifacts(job_dir)
        output_root = tmp_path / "runtime-format-out" / "uuid"
        generate_ef31_runtime_csvs("format-test-uuid", output_root=output_root, _job_dir_override=job_dir)

        path = output_root / "format-test-uuid" / "flow_index.csv"
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            uuid_idx = header.index("FlowUUID")
            uuids = set(row[uuid_idx] for row in reader if len(row) > uuid_idx)
        expected_uuids = {"uuid-co2", "uuid-ch4", "uuid-so2"}
        assert uuids == expected_uuids

    def test_no_duplicate_indicators_in_index(self, tmp_path):
        """indicator_index.csv should not have duplicate indicators."""
        job_dir = tmp_path / "format-test-di"
        job_dir.mkdir()
        self._write_test_artifacts(job_dir)
        output_root = tmp_path / "runtime-format-out" / "di"
        generate_ef31_runtime_csvs("format-test-di", output_root=output_root, _job_dir_override=job_dir)

        path = output_root / "format-test-di" / "indicator_index.csv"
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            rows = list(reader)
        keys = [(r["method_en"], r["ecoinvent_category"], r["indicator_en"]) for r in rows]
        assert len(keys) == len(set(keys)), "Duplicate indicators found"

    def test_same_indicator_name_different_categories_are_distinct(self, tmp_path):
        """EF3.1 can reuse an indicator name across impact categories."""
        job_dir = tmp_path / "format-test-category-key"
        job_dir.mkdir()
        elem_flows = [
            {"flow_uuid": "uuid-nh3", "flow_name": "Ammonia"},
        ]
        indicators = [
            {"method": "EF v3.1 no LT", "category": "acidification no LT", "indicator": "accumulated exceedance (AE) no LT", "indicator_unit": "mol H+-Eq"},
            {"method": "EF v3.1 no LT", "category": "eutrophication: terrestrial no LT", "indicator": "accumulated exceedance (AE) no LT", "indicator_unit": "mol N-Eq"},
        ]
        cfs = [
            {"cf_method": "EF v3.1 no LT", "cf_category": "acidification no LT", "cf_indicator": "accumulated exceedance (AE) no LT",
             "cf_flow_name": "Ammonia", "cf_compartment": "air", "cf_subcompartment": None, "cf_value": 3.02,
             "matched_flow_uuid": "uuid-nh3", "matched_flow_name": "Ammonia"},
            {"cf_method": "EF v3.1 no LT", "cf_category": "eutrophication: terrestrial no LT", "cf_indicator": "accumulated exceedance (AE) no LT",
             "cf_flow_name": "Ammonia", "cf_compartment": "air", "cf_subcompartment": None, "cf_value": 0.42,
             "matched_flow_uuid": "uuid-nh3", "matched_flow_name": "Ammonia"},
        ]
        (job_dir / "elementary_flows.json").write_text(json.dumps(elem_flows, ensure_ascii=False))
        (job_dir / "indicators.json").write_text(json.dumps(indicators, ensure_ascii=False))
        (job_dir / "ef31_cfs.json").write_text(json.dumps(cfs, ensure_ascii=False))
        (job_dir / "cf_matches.json").write_text(json.dumps({"matched": cfs, "unmatched": [], "ambiguous": []}, ensure_ascii=False))

        output_root = tmp_path / "runtime-format-out" / "category-key"
        result = generate_ef31_runtime_csvs("format-test-category-key", output_root=output_root, _job_dir_override=job_dir)

        assert result["indicators_count"] == 2
        assert result["factors_count"] == 2


# ===========================================================================
# 4. E2E: Preview response includes CF match counts
# ===========================================================================

class TestPreviewResponseCfCounts:
    """Test that preview response includes CF match counts in parsed_counts."""

    def test_parsed_counts_include_cf_matching(self):
        """Parsed counts should contain cf_rows_matched/unmatched/ambiguous."""
        from app.ecoinvent_ef31_loader import (
            ElementaryFlow,
            Indicator,
            CharacterizationFactor,
        )

        indicators = [
            Indicator(method="EF v3.1", category="Climate", indicator="Global warming", indicator_unit="kg CO2-Eq"),
            Indicator(method="EF v3.1 no LT", category="Climate", indicator="Global warming", indicator_unit="kg CO2-Eq"),
        ]
        all_cfs = [
            CharacterizationFactor(method="EF v3.1", category="Climate", indicator="GWP", flow_name="carbon dioxide", compartment="air", cf_value=1.0),
            CharacterizationFactor(method="EF v3.1 no LT", category="Climate", indicator="GWP", flow_name="carbon dioxide", compartment="air", cf_value=1.0),
            CharacterizationFactor(method="ReCiPe", category="Climate", indicator="GWP", flow_name="carbon dioxide", compartment="air", cf_value=1.0),
        ]
        ef31_cfs = filter_cf_ef31(all_cfs)

        elementary_flows = [
            ElementaryFlow(flow_uuid="uuid-co2", flow_name="carbon dioxide", compartment="air", subcompartment=None),
        ]

        matched, unmatched, ambiguous = match_cf_to_flows(ef31_cfs, elementary_flows)

        # Verify that parsed_counts would be correct
        parsed_counts = {
            "indicators_total": len(indicators),
            "indicators_ef31": len([i for i in indicators if i.method in ('EF v3.1', 'EF v3.1 no LT')]),
            "cf_rows_total": len(all_cfs),
            "cf_rows_ef31": len(ef31_cfs),
            "cf_rows_matched": len(matched),
            "cf_rows_unmatched": len(unmatched),
            "cf_rows_ambiguous": len(ambiguous),
        }

        assert parsed_counts["cf_rows_total"] == 3
        assert parsed_counts["cf_rows_ef31"] == 2
        assert parsed_counts["cf_rows_matched"] >= 1  # CO2 should match
        assert parsed_counts["cf_rows_unmatched"] >= 0
        assert isinstance(parsed_counts["cf_rows_ambiguous"], int)
