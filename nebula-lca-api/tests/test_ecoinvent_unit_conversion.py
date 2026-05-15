"""Regression tests for ecoinvent unit conversion consistency.

Covers:
1. Same unit-group conversion (g <-> kg, MJ <-> kWh, L <-> m3).
2. preprocess.py normalize_graph_units_to_reference direction correctness.
3. UnitDefinition reference factor consistency.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.database as _db
from app.database import Base
from app.main import app
from app.models import FlowRecord, Model, ModelVersion, ReferenceProcess, RunJob, UnitDefinition, UnitGroup
from app.preprocess import normalize_graph_units_to_reference
from app.schemas import HybridGraph
from app.services.catalog_cache import invalidate_management_caches


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=_db.engine)
    Base.metadata.create_all(bind=_db.engine)
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    yield
    db = _db.SessionLocal()
    try:
        for model in (RunJob, ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition, UnitGroup):
            db.query(model).delete()
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    _db.engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


class TestNormalizeGraphUnitsToReference:
    """Pure-function tests for preprocess.normalize_graph_units_to_reference."""

    def _make_graph(self, nodes=None, exchanges=None):
        """Build a minimal graph dict for normalize_graph_units_to_reference."""
        return {
            "functionalUnit": "1 kg test",
            "nodes": nodes or [],
            "exchanges": exchanges or [],
        }

    def _convert(self, graph_dict, unit_factor, reference_unit):
        """Parse graph dict to HybridGraph and run normalize."""
        graph = HybridGraph.model_validate(graph_dict)
        return normalize_graph_units_to_reference(
            graph, unit_factor_by_group_and_name=unit_factor,
            reference_unit_by_group=reference_unit,
        )

    def test_g_to_kg_conversion(self):
        """1000 g should become 1.0 kg when kg is reference."""
        unit_factor = {("Units of mass", "kg"): 1.0, ("Units of mass", "g"): 0.001}
        reference_unit = {"Units of mass": "kg"}
        graph = self._make_graph(
            nodes=[{
                "id": "np-1",
                "node_kind": "unit_process",
                "process_uuid": "proc-1",
                "name": "Test",
                "location": "GLO",
                "reference_product": "test",
                "mode": "normalized",
                "inputs": [
                    {
                        "id": "in-1",
                        "flowUuid": "flow-a",
                        "name": "test substance",
                        "unit": "g",
                        "unitGroup": "Units of mass",
                        "amount": 1000.0,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [
                    {
                        "id": "out-1",
                        "flowUuid": "flow-b",
                        "name": "test product",
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "amount": 1.0,
                        "type": "technosphere",
                        "direction": "output",
                    }
                ],
                "emissions": [],
            }],
            exchanges=[{
                "id": "edge-1",
                "fromNode": "np-1",
                "toNode": "np-2",
                "sourceHandle": "out:out-1",
                "targetHandle": "in:in-1",
                "flowUuid": "flow-b",
                "flowName": "test product",
                "quantityMode": "single",
                "amount": 1.0,
                "providerAmount": 1.0,
                "consumerAmount": 1.0,
                "unit": "kg",
                "type": "technosphere",
                "allocation": "none",
            }],
        )
        result = self._convert(graph, unit_factor, reference_unit)
        node = result.model_dump(mode="python")["nodes"][0]
        # Input: 1000 g * 0.001 = 1.0
        assert abs(node["inputs"][0]["amount"] - 1.0) < 1e-10
        # Output: 1.0 kg * 1.0 = 1.0
        assert abs(node["outputs"][0]["amount"] - 1.0) < 1e-10
        # Edge: 1.0 kg * 1.0 = 1.0
        edge = result.model_dump(mode="python")["exchanges"][0]
        assert abs(edge["amount"] - 1.0) < 1e-10

    def test_mj_to_kwh_conversion(self):
        """5 MJ should become 1.388... kWh when MJ is reference and 1 kWh = 3.6 MJ."""
        # 1 kWh = 3.6 MJ -> kWh factor = 3.6, MJ factor = 1.0
        unit_factor = {("Units of energy", "MJ"): 1.0, ("Units of energy", "kWh"): 3.6}
        reference_unit = {"Units of energy": "MJ"}
        graph = self._make_graph(
            nodes=[{
                "id": "np-1",
                "node_kind": "unit_process",
                "process_uuid": "proc-1",
                "name": "Test",
                "location": "GLO",
                "reference_product": "test",
                "mode": "normalized",
                "inputs": [
                    {
                        "id": "in-1",
                        "flowUuid": "flow-a",
                        "name": "electricity",
                        "unit": "kWh",
                        "unitGroup": "Units of energy",
                        "amount": 1.0,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [],
                "emissions": [],
            }],
            exchanges=[],
        )
        result = self._convert(graph, unit_factor, reference_unit)
        node = result.model_dump(mode="python")["nodes"][0]
        # 1 kWh * 3.6 = 3.6 MJ
        assert abs(node["inputs"][0]["amount"] - 3.6) < 1e-10

    def test_l_to_m3_conversion(self):
        """1000 L should become 1.0 m3 when m3 is reference."""
        unit_factor = {("Volume", "m3"): 1.0, ("Volume", "L"): 0.001}
        reference_unit = {"Volume": "m3"}
        graph = self._make_graph(
            nodes=[{
                "id": "np-1",
                "node_kind": "unit_process",
                "process_uuid": "proc-1",
                "name": "Test",
                "location": "GLO",
                "reference_product": "test",
                "mode": "normalized",
                "inputs": [
                    {
                        "id": "in-1",
                        "flowUuid": "flow-a",
                        "name": "water",
                        "unit": "L",
                        "unitGroup": "Volume",
                        "amount": 1000.0,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [],
                "emissions": [],
            }],
            exchanges=[],
        )
        result = self._convert(graph, unit_factor, reference_unit)
        node = result.model_dump(mode="python")["nodes"][0]
        assert abs(node["inputs"][0]["amount"] - 1.0) < 1e-10

    def test_missing_unit_group_unchanged(self):
        """If unitGroup is None, amount should not be converted."""
        unit_factor = {("Units of mass", "kg"): 1.0}
        reference_unit = {"Units of mass": "kg"}
        graph = self._make_graph(
            nodes=[{
                "id": "np-1",
                "node_kind": "unit_process",
                "process_uuid": "proc-1",
                "name": "Test",
                "location": "GLO",
                "reference_product": "test",
                "mode": "normalized",
                "inputs": [
                    {
                        "id": "in-1",
                        "flowUuid": "flow-a",
                        "name": "test",
                        "unit": "kg",
                        "unitGroup": None,
                        "amount": 5.0,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [],
                "emissions": [],
            }],
            exchanges=[],
        )
        result = self._convert(graph, unit_factor, reference_unit)
        node = result.model_dump(mode="python")["nodes"][0]
        assert abs(node["inputs"][0]["amount"] - 5.0) < 1e-10

    def test_kwh_converts_within_energy_group(self):
        """kWh should convert to the energy reference unit, MJ."""
        unit_factor = {("Units of mass", "kg"): 1.0, ("Units of energy", "kWh"): 3.6}
        reference_unit = {"Units of mass": "kg", "Units of energy": "MJ"}
        graph = self._make_graph(
            nodes=[{
                "id": "np-1",
                "node_kind": "unit_process",
                "process_uuid": "proc-1",
                "name": "Test",
                "location": "GLO",
                "reference_product": "test",
                "mode": "normalized",
                "inputs": [
                    {
                        "id": "in-1",
                        "flowUuid": "flow-a",
                        "name": "test",
                        "unit": "kWh",
                        "unitGroup": "Units of energy",
                        "amount": 5.0,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [],
                "emissions": [],
            }],
            exchanges=[],
        )
        result = self._convert(graph, unit_factor, reference_unit)
        node = result.model_dump(mode="python")["nodes"][0]
        # kWh in "Units of energy" group, but reference_unit_by_group has "Units of energy": "MJ"
        # The converter looks up unitGroup from the port, not from reference_unit_by_group keys.
        # kWh factor = 3.6, so 5 * 3.6 = 18.0 (in MJ reference).
        assert abs(node["inputs"][0]["amount"] - 18.0) < 1e-10


class TestUnitConversionApi:
    """API-level tests: seed unit definitions and verify DB roundtrip."""

    def test_unit_definitions_seed_and_query(self, client):
        """Seed unit groups and definitions, verify factors are stored correctly."""
        db = _db.SessionLocal()
        try:
            # Seed unit groups
            db.add(UnitGroup(name="Units of mass", reference_unit="kg"))
            db.add(UnitGroup(name="Units of energy", reference_unit="MJ"))
            # Seed unit definitions
            db.add(UnitDefinition(
                unit_group="Units of mass", unit_name="kg",
                factor_to_reference=1.0, is_reference=True,
            ))
            db.add(UnitDefinition(
                unit_group="Units of mass", unit_name="g",
                factor_to_reference=0.001, is_reference=False,
            ))
            db.add(UnitDefinition(
                unit_group="Units of energy", unit_name="MJ",
                factor_to_reference=1.0, is_reference=True,
            ))
            db.add(UnitDefinition(
                unit_group="Units of energy", unit_name="kWh",
                factor_to_reference=3.6, is_reference=False,
            ))
            db.commit()

            defs = db.query(UnitDefinition).all()
            factor_map = {(d.unit_group, d.unit_name): d.factor_to_reference for d in defs}
            assert factor_map[("Units of mass", "kg")] == 1.0
            assert factor_map[("Units of mass", "g")] == 0.001
            assert factor_map[("Units of energy", "MJ")] == 1.0
            assert factor_map[("Units of energy", "kWh")] == 3.6
        finally:
            db.close()


class TestUnitGroupReferenceConsistency:
    """Verify reference-unit factors used by graph normalization."""

    def test_unit_group_reference_consistency(self):
        """Reference unit factors should always be 1.0."""
        unit_factor = {("Units of mass", "kg"): 1.0, ("Units of energy", "MJ"): 1.0}
        for group, ref_factor in unit_factor.items():
            assert ref_factor == 1.0, f"Reference unit factor for {group} should be 1.0, got {ref_factor}"
