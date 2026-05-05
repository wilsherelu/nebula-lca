"""Tests for EF 3.1 DB dry-run/commit service."""

import pytest
from app.ecoinvent_ef31_loader import (
    LCIDataset,
    LCIElementaryExchange,
    ElementaryFlow,
    IntermediateFlow,
    UnitRecord,
)
from app.ef31_db_service import (
    DbDryRunResult,
    dry_run_lci_import,
    _generate_lci_process_uuid,
    _build_elementary_exchanges_json,
)


class TestGenerateLciProcessUuid:
    """Test stable UUID generation for LCI datasets."""

    def test_from_activity_id_and_rp_id(self):
        """Use activity_id:rp_id composite when both available."""
        dataset = LCIDataset(
            filename="test.spold",
            activity_id="activity_uuid_0001",
            activity_name="Test",
            location="CH",
            reference_product_name="market for product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="rp_uuid_0001",
        )
        assert _generate_lci_process_uuid(dataset) == "activity_uuid_0001:rp_uuid_0001"

    def test_fallback_to_activity_id_only(self):
        """Use activity_id alone when rp_id is empty."""
        dataset = LCIDataset(
            filename="test.spold",
            activity_id="activity_uuid_0001",
            activity_name="Test",
            location="CH",
            reference_product_name="market for product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="",
        )
        assert _generate_lci_process_uuid(dataset) == "activity_uuid_0001"

    def test_fallback_to_filename_stem(self):
        """Use filename stem when activity_id is empty."""
        dataset = LCIDataset(
            filename="electricity_ch.spold",
            activity_id="",
            activity_name="Test",
            location="CH",
            reference_product_name="market for product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
        )
        assert _generate_lci_process_uuid(dataset) == "electricity_ch"


class TestBuildElementaryExchangesJson:
    """Test process_json.exchanges enrichment."""

    def test_enriches_with_flow_ref(self):
        """Each exchange entry should include compartment from flow ref."""
        exchanges = [
            LCIElementaryExchange(
                dataset_filename="test.spold",
                exchange_id="elem-001",
                exchange_name="carbon dioxide",
                unit="kilogram",
                direction="output",
                amount=0.5,
            )
        ]
        elem_flows = [
            ElementaryFlow(
                flow_uuid="elem-001",
                flow_name="carbon dioxide",
                flow_type="Elementary flow",
                default_unit="kilogram",
                compartment="air",
                subcompartment="non-urban air",
                source="ef3.1",
            )
        ]
        result = _build_elementary_exchanges_json(exchanges, elem_flows)
        assert len(result) == 1
        assert result[0]["flow_uuid"] == "elem-001"
        assert result[0]["name"] == "carbon dioxide"
        assert result[0]["compartment"] == "air"
        assert result[0]["subcompartment"] == "non-urban air"
        assert result[0]["unit"] == "kilogram"
        assert result[0]["amount"] == 0.5

    def test_missing_flow_ref_graceful(self):
        """When flow ref missing, compartment should be None."""
        exchanges = [
            LCIElementaryExchange(
                dataset_filename="test.spold",
                exchange_id="unknown-001",
                exchange_name="Unknown flow",
                unit="kilogram",
                direction="output",
                amount=0.1,
            )
        ]
        elem_flows = []
        result = _build_elementary_exchanges_json(exchanges, elem_flows)
        assert len(result) == 1
        assert result[0]["compartment"] is None
        assert result[0]["name"] == "Unknown flow"

    def test_multiple_exchanges(self):
        """Multiple exchanges should be enriched correctly."""
        exchanges = [
            LCIElementaryExchange(
                dataset_filename="test.spold",
                exchange_id="elem-001",
                exchange_name="CO2",
                unit="kg",
                direction="output",
                amount=0.5,
            ),
            LCIElementaryExchange(
                dataset_filename="test.spold",
                exchange_id="elem-002",
                exchange_name="CH4",
                unit="kg",
                direction="output",
                amount=0.001,
            ),
        ]
        elem_flows = [
            ElementaryFlow(
                flow_uuid="elem-001",
                flow_name="CO2",
                flow_type="Elementary flow",
                default_unit="kg",
                compartment="air",
                source="ef3.1",
            ),
            ElementaryFlow(
                flow_uuid="elem-002",
                flow_name="CH4",
                flow_type="Elementary flow",
                default_unit="kg",
                compartment="air",
                source="ef3.1",
            ),
        ]
        result = _build_elementary_exchanges_json(exchanges, elem_flows)
        assert len(result) == 2


class TestDbDryRunResult:
    """Test dry-run result summary."""

    def test_summary_structure(self):
        """Summary should have flows/units/processes counts."""
        result = DbDryRunResult(
            flows_new=10,
            flows_skipped=2,
            flows_error=0,
            units_new=5,
            units_skipped=0,
            units_error=0,
            processes_new=3,
            processes_skipped=1,
            processes_error=0,
            warnings=["w1"],
            errors=["e1"],
        )
        summary = result.summary()
        assert summary["flows"]["new"] == 10
        assert summary["flows"]["skipped"] == 2
        assert summary["units"]["new"] == 5
        assert summary["processes"]["new"] == 3
        assert summary["warnings"] == ["w1"]
        assert summary["errors"] == ["e1"]

    def test_empty_summary(self):
        """Empty result should have all zeros."""
        result = DbDryRunResult()
        summary = result.summary()
        assert summary["flows"]["new"] == 0
        assert summary["processes"]["error"] == 0


class TestDryRunLciImport:
    """Test dry-run LCI import counting."""

    def test_dry_run_counts_new(self):
        """Should count new datasets and flows correctly."""
        datasets = [
            LCIDataset(
                filename="a.spold",
                activity_id="act-001",
                activity_name="Dataset A",
                location="CH",
                reference_product_name="market for A",
                reference_product_unit="kg",
                reference_product_amount=1.0,
            ),
            LCIDataset(
                filename="b.spold",
                activity_id="act-002",
                activity_name="Dataset B",
                location="DE",
                reference_product_name="market for B",
                reference_product_unit="kg",
                reference_product_amount=1.0,
            ),
        ]
        exchanges_map = {
            "a.spold": [
                LCIElementaryExchange(
                    dataset_filename="a.spold",
                    exchange_id="elem-001",
                    exchange_name="CO2",
                    unit="kg",
                    direction="output",
                    amount=0.5,
                )
            ],
            "b.spold": [
                LCIElementaryExchange(
                    dataset_filename="b.spold",
                    exchange_id="elem-002",
                    exchange_name="CH4",
                    unit="kg",
                    direction="output",
                    amount=0.001,
                )
            ],
        }
        elem_flows = [
            ElementaryFlow(
                flow_uuid="elem-001",
                flow_name="CO2",
                flow_type="Elementary flow",
                default_unit="kg",
                compartment="air",
                source="ef3.1",
            ),
            ElementaryFlow(
                flow_uuid="elem-002",
                flow_name="CH4",
                flow_type="Elementary flow",
                default_unit="kg",
                compartment="air",
                source="ef3.1",
            ),
        ]

        result = dry_run_lci_import(
            datasets=datasets,
            exchanges_map=exchanges_map,
            elementary_flows=elem_flows,
        )
        # Both datasets are new processes (DB is empty)
        assert result.processes_new == 2
        # All exchange_ids exist in MasterData, so no missing refs
        assert result.flows_error == 0
        assert result.errors == []

    def test_dry_run_detects_existing_process(self, monkeypatch):
        """Should skip datasets whose process already exists in DB."""
        datasets = [
            LCIDataset(
                filename="a.spold",
                activity_id="act-001",
                activity_name="Dataset A",
                location="CH",
                reference_product_name="market for A",
                reference_product_unit="kg",
                reference_product_amount=1.0,
            ),
        ]
        exchanges_map = {}
        elem_flows = []

        # Mock DB: flow query returns empty, process query returns existing row
        class FakeFlowRow:
            pass

        class FakeProcRow:
            pass

        class FakeDb:
            def execute(self, sql):
                class FakeResult:
                    @staticmethod
                    def all():
                        return []
                    def first(self):
                        return FakeProcRow()
                return FakeResult()

        result = dry_run_lci_import(
            datasets=datasets,
            exchanges_map=exchanges_map,
            elementary_flows=elem_flows,
            db=FakeDb(),
        )
        assert result.processes_skipped == 1
        assert result.processes_new == 0

    def test_dry_run_detects_missing_flow_refs(self):
        """Should flag missing elementary flow refs in errors list."""
        datasets = [
            LCIDataset(
                filename="bad.spold",
                activity_id="act-001",
                activity_name="Bad Dataset",
                location="CH",
                reference_product_name="market for product",
                reference_product_unit="kg",
                reference_product_amount=1.0,
            ),
        ]
        exchanges_map = {
            "bad.spold": [
                LCIElementaryExchange(
                    dataset_filename="bad.spold",
                    exchange_id="unknown_flow_id",
                    exchange_name="Unknown",
                    unit="kg",
                    direction="output",
                    amount=0.1,
                )
            ],
        }
        elem_flows = []  # No MasterData flows

        result = dry_run_lci_import(
            datasets=datasets,
            exchanges_map=exchanges_map,
            elementary_flows=elem_flows,
        )
        # Flow is missing from MasterData
        assert result.flows_error >= 1
        assert any("unknown_flow_id" in e for e in result.errors)
        assert any("Missing elementary flow ref" in e for e in result.errors)
