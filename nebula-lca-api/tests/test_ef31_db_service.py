"""Tests for EF 3.1 DB dry-run/commit service."""

import os
import tempfile
import uuid
from pathlib import Path

import pytest
from app.ecoinvent_ef31_loader import (
    LCIDataset,
    LCIElementaryExchange,
    ElementaryFlow,
    IntermediateFlow,
    UnitRecord,
    UnitConversion,
)

# Keep service tests isolated when a helper uses SessionLocal internally.
_TEST_DB = Path(tempfile.gettempdir()) / f"nebula_ef31_db_service_{uuid.uuid4().hex}.db"
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_TEST_DB.as_posix()}")

from app.ef31_db_service import (
    DbDryRunResult,
    commit_lci_import,
    dry_run_lci_import,
    _generate_lci_process_uuid,
    _build_elementary_exchanges_json,
)


class EmptyDb:
    """Tiny read-only DB stub for dry-run tests."""

    def execute(self, sql):
        class EmptyResult:
            @staticmethod
            def all():
                return []

            @staticmethod
            def first():
                return None

        return EmptyResult()


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
            db=EmptyDb(),
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
            db=EmptyDb(),
        )
        # Flow is missing from MasterData
        assert result.flows_error >= 1
        assert any("unknown_flow_id" in e for e in result.errors)
        assert any("Missing elementary flow ref" in e for e in result.errors)


class TestCommitWithDbSession:
    """Test commit_lci_import with actual DB session (SQLite)."""

    def _make_dataset(self, **kwargs) -> LCIDataset:
        defaults = {
            "filename": "test.spold",
            "activity_id": "act-001",
            "activity_name": "Test Activity",
            "location": "CH",
            "reference_product_name": "market for test",
            "reference_product_unit": "kg",
            "reference_product_amount": 1.0,
            "reference_product_id": "rp-001",
        }
        defaults.update(kwargs)
        return LCIDataset(**defaults)

    def _make_elem_exchange(self, **kwargs) -> LCIElementaryExchange:
        defaults = {
            "dataset_filename": "test.spold",
            "exchange_id": "flow-001",
            "exchange_name": "CO2",
            "unit": "kg",
            "direction": "output",
            "amount": 0.5,
        }
        defaults.update(kwargs)
        return LCIElementaryExchange(**defaults)

    def _make_elem_flow(self, **kwargs) -> ElementaryFlow:
        defaults = {
            "flow_uuid": "flow-001",
            "flow_name": "CO2",
            "flow_name_en": "Carbon dioxide",
            "flow_type": "Elementary flow",
            "default_unit": "kg",
            "unit_group": "default",
            "compartment": "air",
            "subcompartment": None,
            "cas_number": None,
            "formula": "CO2",
            "source": "ecoinvent",
        }
        defaults.update(kwargs)
        return ElementaryFlow(**defaults)

    def test_commit_creates_reference_process(self):
        """Commit should create ReferenceProcess with correct fields."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import FlowRecord, UnitDefinition, ReferenceProcess

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            dataset = self._make_dataset()
            exchange = self._make_elem_exchange()
            elem_flow = self._make_elem_flow()

            commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                db=db,
            )
            db.commit()

            # Check ReferenceProcess was created
            proc = db.execute(
                ReferenceProcess.__table__.select()
            ).first()
            assert proc is not None
            assert proc.process_type == "lci_dataset"
            assert proc.import_mode == "ecoinvent_ef31_lci"
            assert proc.reference_flow_uuid == "rp-001"
            assert proc.process_uuid == "act-001:rp-001"

            # Check process_json
            pj = proc.process_json
            assert isinstance(pj, dict)
            assert "exchanges" in pj
            assert "elementary_exchanges" in pj
            assert len(pj["exchanges"]) == 2
            assert pj["reference_flow_uuid"] == "rp-001"
            assert pj["reference_flow_internal_id"] == "rp-001"
            assert pj["exchanges"][0]["flow_uuid"] == "rp-001"
            assert pj["exchanges"][0]["isProduct"] is True
            assert pj["exchanges"][1]["flow_uuid"] == "flow-001"
        finally:
            db.close()

    def test_commit_imports_unit_conversions_and_flow_unit_groups(self):
        """UnitConversions.xml data should become the authoritative eco unit catalog."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import FlowRecord, UnitDefinition, UnitGroup

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            dataset = self._make_dataset(reference_product_unit="g")
            exchange = self._make_elem_exchange(unit="g")
            elem_flow = self._make_elem_flow(default_unit="g", unit_group="")
            conversions = [
                UnitConversion(
                    conversion_id="conv-g-kg",
                    unit_from_name="g",
                    unit_to_name="kg",
                    unit_type="mass",
                    factor=0.001,
                ),
                UnitConversion(
                    conversion_id="conv-mg-kg",
                    unit_from_name="mg",
                    unit_to_name="kg",
                    unit_type="mass",
                    factor=0.000001,
                ),
            ]

            result = commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                unit_conversions=conversions,
                db=db,
            )
            db.commit()

            assert result.units_new >= 3
            mass_group = db.get(UnitGroup, "mass")
            assert mass_group is not None
            assert mass_group.reference_unit == "kg"

            unit_defs = {
                row.unit_name: row
                for row in db.query(UnitDefinition).filter(UnitDefinition.unit_group == "mass").all()
            }
            assert unit_defs["kg"].is_reference is True
            assert unit_defs["kg"].factor_to_reference == 1.0
            assert unit_defs["g"].factor_to_reference == 0.001
            assert unit_defs["mg"].factor_to_reference == 0.000001

            flow = db.get(FlowRecord, "flow-001")
            assert flow is not None
            assert flow.unit_group == "mass"
        finally:
            db.close()

    def test_commit_derives_multi_step_unit_conversion(self):
        """Indirect conversion paths should still resolve to the group reference unit."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import UnitDefinition

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            dataset = self._make_dataset(reference_product_unit="Wh")
            exchange = self._make_elem_exchange(unit="Wh")
            elem_flow = self._make_elem_flow(default_unit="Wh", unit_group="")
            conversions = [
                UnitConversion("conv-wh-kwh", "Wh", "kWh", "energy", 0.001),
                UnitConversion("conv-kwh-mj", "kWh", "MJ", "energy", 3.6),
            ]

            commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                unit_conversions=conversions,
                db=db,
            )
            db.commit()

            unit_defs = {
                row.unit_name: row.factor_to_reference
                for row in db.query(UnitDefinition).filter(UnitDefinition.unit_group == "energy").all()
            }
            assert unit_defs["MJ"] == 1.0
            assert abs(unit_defs["kWh"] - 3.6) < 1e-12
            assert abs(unit_defs["Wh"] - 0.0036) < 1e-12
        finally:
            db.close()

    def test_commit_backfills_existing_flow_unit_group(self):
        """Re-running an import should repair old FlowRecord rows with missing unit groups."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import FlowRecord

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            db.add(FlowRecord(
                flow_uuid="flow-001",
                flow_name="CO2",
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="",
                source="ef3.1",
            ))
            db.commit()

            dataset = self._make_dataset()
            exchange = self._make_elem_exchange()
            elem_flow = self._make_elem_flow(default_unit="kg", unit_group="")
            conversions = [
                UnitConversion("conv-g-kg", "g", "kg", "mass", 0.001),
            ]

            commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                unit_conversions=conversions,
                db=db,
            )
            db.commit()

            flow = db.get(FlowRecord, "flow-001")
            assert flow.unit_group == "mass"
        finally:
            db.close()

    def test_commit_skips_existing_process(self):
        """Second commit of same dataset should skip existing process."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import FlowRecord, ReferenceProcess

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            dataset = self._make_dataset()
            exchange = self._make_elem_exchange()
            elem_flow = self._make_elem_flow()

            # First commit
            r1 = commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                db=db,
            )
            db.commit()
            assert r1.processes_new == 1

            # Second commit — same dataset
            r2 = commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                db=db,
            )
            db.commit()
            assert r2.processes_skipped == 1
            assert r2.processes_new == 0

            # Only one ReferenceProcess in DB
            count = db.execute(ReferenceProcess.__table__.select()).first()
            procs = db.execute(ReferenceProcess.__table__.select()).all()
            assert len(procs) == 1
        finally:
            db.close()

    def test_commit_multiple_different_rps(self):
        """Same activity, different RP ids should create separate processes."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import ReferenceProcess

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            dataset1 = self._make_dataset(
                reference_product_id="rp-A",
                reference_product_name="market for A",
            )
            dataset2 = self._make_dataset(
                reference_product_id="rp-B",
                reference_product_name="market for B",
            )
            exchange = self._make_elem_exchange()
            elem_flow = self._make_elem_flow()

            r = commit_lci_import(
                datasets=[dataset1, dataset2],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                db=db,
            )
            db.commit()

            # Both should be created (different rp_id → different composite key)
            assert r.processes_new == 2

            procs = db.execute(ReferenceProcess.__table__.select()).all()
            assert len(procs) == 2
        finally:
            db.close()

    def test_commit_block_on_missing_ref(self):
        """Dataset with missing MasterData ref should be skipped."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import ReferenceProcess

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            dataset = self._make_dataset()
            # Exchange references a flow that does NOT exist in MasterData
            exchange = self._make_elem_exchange(exchange_id="nonexistent-flow-001")
            elem_flows = []  # Empty — no MasterData flows

            r = commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=elem_flows,
                db=db,
            )
            db.commit()

            # Dataset should be skipped, not created
            assert r.processes_skipped == 1
            assert r.processes_new == 0
            assert any("Missing elementary flow ref" in e for e in r.errors)

            # No ReferenceProcess created
            procs = db.execute(ReferenceProcess.__table__.select()).all()
            assert len(procs) == 0
        finally:
            db.close()

    def test_commit_uses_masterdata_flow_fields(self):
        """FlowRecord should use MasterData fields (compartment, etc.)."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from app.database import Base
        from app.models import FlowRecord

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        db = Session()

        try:
            dataset = self._make_dataset()
            exchange = self._make_elem_exchange()
            elem_flow = self._make_elem_flow(compartment="air", formula="CO2")

            commit_lci_import(
                datasets=[dataset],
                exchanges_map={"test.spold": [exchange]},
                elementary_flows=[elem_flow],
                db=db,
            )
            db.commit()

            # FlowRecord should have compartment from MasterData
            flows = db.execute(FlowRecord.__table__.select()).all()
            assert len(flows) >= 1
            flow = flows[0]
            assert flow.compartment == "air"
            assert flow.source == "ecoinvent"
        finally:
            db.close()
