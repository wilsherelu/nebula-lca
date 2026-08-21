from types import SimpleNamespace

import pytest

from app.flow_unit_semantics import (
    build_unit_reference_maps,
    build_unit_group_identity_map,
    collect_flow_default_unit_conversion_violations,
    normalize_graph_flow_unit_switches,
    resolve_flow_port_unit_semantics,
)
from app.main import _build_product_result_view_from_graph
from app.ef31_db_service import _ensure_unit_group
from app.models import FlowRecord, ReferenceProcess, UnitDefinition, UnitGroup
from app.schemas import HybridGraph
from app.tidas_export import ExportReport, _build_process_data


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows

    def filter(self, *args):
        return self

    def one_or_none(self):
        return self._rows[0] if self._rows else None


class _FakeDb:
    def __init__(self):
        self.flow_by_uuid = {
            "oil-flow": SimpleNamespace(
                flow_uuid="oil-flow",
                default_unit="m3",
                unit_group="Units of volume",
                allocation_properties=[],
            ),
        }
        self.unit_defs = [
            SimpleNamespace(unit_group="Units of volume", unit_name="m3", factor_to_reference=1.0, is_reference=True),
            SimpleNamespace(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True),
            SimpleNamespace(unit_group="Units of mass", unit_name="g", factor_to_reference=0.001, is_reference=False),
        ]
        self.unit_groups = [
            SimpleNamespace(name="Units of volume", reference_unit="m3"),
            SimpleNamespace(name="Units of mass", reference_unit="kg"),
        ]
        self.flow_versions = []

    def get(self, model, key):
        if model is FlowRecord:
            return self.flow_by_uuid.get(key)
        if model is ReferenceProcess:
            return None
        return None

    def query(self, model):
        from app.models import FlowVersionRecord

        if model is FlowVersionRecord:
            return _FakeQuery(self.flow_versions)
        if model is UnitDefinition:
            return _FakeQuery(self.unit_defs)
        if model is UnitGroup:
            return _FakeQuery(self.unit_groups)
        return _FakeQuery([])


def _switched_oil_port(amount=800, unit="kg"):
    return {
        "id": "out_oil",
        "flowUuid": "oil-flow",
        "name": "oil",
        "type": "technosphere",
        "direction": "output",
        "isProduct": True,
        "amount": amount,
        "unit": unit,
        "unitGroup": "Units of mass",
        "unitGroupSwitch": {
            "sourceFlowUuid": "oil-flow",
            "sourceUnitGroup": "Units of volume",
            "sourceUnit": "m3",
            "sourceReferenceUnit": "m3",
            "sourceAmount": 1,
            "targetUnitGroup": "Units of mass",
            "targetUnit": "kg",
            "targetReferenceUnit": "kg",
            "factor": 800,
        },
    }


def test_flow_unit_semantics_converts_current_amount_back_to_flow_default_unit():
    sem = resolve_flow_port_unit_semantics(_FakeDb(), _switched_oil_port())

    assert sem.ok is True
    assert sem.current_unit_group == "Units of mass"
    assert sem.flow_default_unit_group == "Units of volume"
    assert sem.flow_default_unit == "m3"
    assert sem.amount_in_flow_default_unit == 1
    assert sem.result_factor_to_flow_default_unit == 800


def test_flow_unit_semantics_treats_ecoinvent_mass_alias_as_same_group():
    db = _FakeDb()
    db.flow_by_uuid["fiber-flow"] = SimpleNamespace(
        flow_uuid="fiber-flow",
        default_unit="kg",
        unit_group="mass",
        allocation_properties=[],
    )
    db.unit_defs.append(SimpleNamespace(unit_group="mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
    db.unit_groups.append(SimpleNamespace(name="mass", reference_unit="kg"))
    port = {
        "id": "out_fiber",
        "flowUuid": "fiber-flow",
        "name": "fibre, polyester",
        "type": "technosphere",
        "direction": "output",
        "isProduct": True,
        "amount": 1,
        "unit": "kg",
        "unitGroup": "Units of mass",
    }

    sem = resolve_flow_port_unit_semantics(db, port)
    violations = collect_flow_default_unit_conversion_violations(
        {
            "nodes": [
                {
                    "id": "node-1",
                    "process_uuid": "process-1",
                    "outputs": [port],
                    "inputs": [],
                    "emissions": [],
                }
            ]
        },
        db,
    )

    assert sem.ok is True
    assert sem.amount_in_flow_default_unit == 1
    assert sem.result_factor_to_flow_default_unit == 1
    assert violations == []


def test_flow_unit_semantics_treats_flow_name_saved_as_unit_as_default_unit():
    db = _FakeDb()
    db.flow_by_uuid["limestone-flow"] = SimpleNamespace(
        flow_uuid="limestone-flow",
        flow_name="石灰石，粉碎，磨机用",
        flow_name_en="Limestone, crushed, for mill",
        default_unit="kg",
        unit_group="Units of mass",
        allocation_properties=[],
    )
    port = {
        "id": "input_limestone",
        "flowUuid": "limestone-flow",
        "name": "石灰石，粉碎，磨机用",
        "type": "technosphere",
        "direction": "input",
        "isProduct": False,
        "amount": 588.5,
        "unit": "Limestone, crushed, for mill",
        "unitGroup": "Units of mass",
        "unitGroupSwitch": None,
    }

    sem = resolve_flow_port_unit_semantics(db, port)
    violations = collect_flow_default_unit_conversion_violations(
        {
            "nodes": [
                {
                    "id": "node-1",
                    "process_uuid": "process-1",
                    "inputs": [port],
                    "outputs": [],
                    "emissions": [],
                }
            ]
        },
        db,
    )

    assert sem.ok is True
    assert sem.amount_in_flow_default_unit == 588.5
    assert sem.result_factor_to_flow_default_unit == 1
    assert violations == []


def test_unit_group_identity_uses_physical_id_for_basic_groups():
    db = _FakeDb()
    db.unit_groups.append(
        SimpleNamespace(
            name="mass",
            reference_unit="kg",
            source_uuid="ecoinvent:unit-type:mass",
        )
    )
    db.unit_groups.append(
        SimpleNamespace(
            name="Units of mass",
            reference_unit="kg",
            source_uuid="ecoinvent:unit-type:mass",
        )
    )

    identity_by_name = build_unit_group_identity_map(db)

    assert identity_by_name["mass"] == "physical:mass"
    assert identity_by_name["Units of mass"] == "physical:mass"


def test_ensure_ecoinvent_unit_group_writes_stable_source_identity():
    class _FakeDbForUnitGroups:
        def __init__(self):
            self.rows = {}
            self.added = []

        def get(self, model, key):
            assert model is UnitGroup
            return self.rows.get(key)

        def add(self, row):
            self.rows[row.name] = row
            self.added.append(row)

        def flush(self):
            pass

    db = _FakeDbForUnitGroups()

    row, was_new = _ensure_unit_group(db, "mass", "kg")

    assert was_new is True
    assert row.source_uuid == "ecoinvent:unit-type:mass"
    assert row.source_version == "ecoinvent"


def test_flow_default_unit_conversion_requires_switch_for_cross_group_current_unit():
    db = _FakeDb()
    db.flow_versions = [db.flow_by_uuid["oil-flow"]]
    port = _switched_oil_port()
    port.pop("unitGroupSwitch")
    port["flowSourceNamespace"] = "tiangong_open_data"
    port["flowVersion"] = "01.01.000"

    violations = collect_flow_default_unit_conversion_violations(
        {
            "nodes": [
                {
                    "id": "node-1",
                    "process_uuid": "process-1",
                    "name": "process",
                    "inputs": [],
                    "outputs": [port],
                    "emissions": [],
                }
            ]
        },
        db,
    )

    assert len(violations) == 1
    assert violations[0]["flow_default_unit_group"] == "Units of volume"
    assert violations[0]["current_unit_group"] == "Units of mass"


def test_flow_default_unit_conversion_uses_flow_level_rule_when_port_switch_missing():
    db = _FakeDb()
    db.flow_by_uuid["oil-flow"].allocation_properties = [
        {
            "propertyType": "custom_conversion",
            "value": 800,
            "basisUnit": "m3",
            "targetUnitGroup": "Units of mass",
            "targetUnit": "kg",
            "source": "user_declared",
        }
    ]
    port = _switched_oil_port()
    port.pop("unitGroupSwitch")

    sem = resolve_flow_port_unit_semantics(db, port)

    assert sem.ok is True
    assert sem.amount_in_flow_default_unit == 800
    assert sem.unit_group_switch == {}


def test_normalize_graph_flow_unit_switches_does_not_infer_from_mutable_catalog_for_legacy_port():
    db = _FakeDb()
    db.flow_by_uuid["oil-flow"].allocation_properties = [
        {
            "propertyType": "custom_conversion",
            "value": 800,
            "basisUnit": "m3",
            "targetUnitGroup": "Units of mass",
            "targetUnit": "kg",
        }
    ]
    port = _switched_oil_port()
    port.pop("unitGroupSwitch")
    graph = {
        "nodes": [
            {
                "id": "node-1",
                "process_uuid": "process-1",
                "name": "process",
                "inputs": [],
                "outputs": [port],
                "emissions": [],
            }
        ]
    }

    normalize_graph_flow_unit_switches(graph, db)

    assert "unitGroupSwitch" not in graph["nodes"][0]["outputs"][0]


def test_legacy_unversioned_flow_uses_saved_switch_source_over_mutable_catalog():
    port = _switched_oil_port()
    port["unitGroupSwitch"]["sourceUnitGroup"] = "Units of mass"
    port["unitGroupSwitch"]["sourceUnit"] = "kg"
    port["unitGroupSwitch"]["sourceReferenceUnit"] = "kg"

    sem = resolve_flow_port_unit_semantics(_FakeDb(), port)

    assert sem.ok is True
    assert sem.flow_default_unit_group == "Units of mass"
    assert sem.amount_in_flow_default_unit == 800


def test_legacy_unversioned_flow_keeps_saved_unit_when_catalog_unit_group_changed():
    db = _FakeDb()
    db.flow_by_uuid["diesel-flow"] = SimpleNamespace(
        flow_uuid="diesel-flow",
        default_unit="MJ",
        unit_group="Units of energy",
        allocation_properties=[],
    )
    port = {
        "id": "out_diesel",
        "flowUuid": "diesel-flow",
        "name": "diesel",
        "amount": 1,
        "unit": "kg",
        "unitGroup": "Units of mass",
        "type": "technosphere",
        "direction": "output",
    }

    sem = resolve_flow_port_unit_semantics(db, port)

    assert sem.ok is True
    assert sem.flow_default_unit_group == "Units of mass"
    assert sem.flow_default_unit == "kg"
    assert sem.amount_in_flow_default_unit == 1


def test_versioned_flow_fails_closed_when_exact_snapshot_is_missing():
    port = {
        "id": "out_diesel",
        "flowUuid": "diesel-flow",
        "flowSourceNamespace": "tiangong_open_data",
        "flowVersion": "02.00.000",
        "name": "diesel",
        "amount": 1,
        "unit": "kg",
        "unitGroup": "Units of mass",
        "type": "technosphere",
        "direction": "output",
    }

    sem = resolve_flow_port_unit_semantics(_FakeDb(), port)

    assert sem.ok is False
    assert sem.reason == "missing_flow_version_snapshot"


def test_flow_default_unit_conversion_ignores_legacy_switch_source_amount_mismatch():
    port = _switched_oil_port(amount=800, unit="kg")
    port["unitGroupSwitch"]["sourceAmount"] = 2

    sem = resolve_flow_port_unit_semantics(_FakeDb(), port)

    assert sem.ok is True
    assert sem.snapshot_source_amount == 2
    assert sem.inferred_source_amount == 1
    assert sem.amount_in_flow_default_unit == 1


def test_tidas_process_exchange_uses_flow_default_amount():
    graph = {
        "functionalUnit": "1 m3 oil",
        "nodes": [
            {
                "id": "node-1",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "process-1",
                "name": "process",
                "location": "CN",
                "reference_product": "oil-flow",
                "inputs": [],
                "outputs": [_switched_oil_port()],
                "emissions": [],
            }
        ],
        "exchanges": [],
    }
    report = ExportReport()

    process_data = _build_process_data(
        _FakeDb(),
        "process-1",
        graph,
        report,
        None,
        {"oil-flow": "product_flow"},
    )

    exchange = process_data["processDataSet"]["exchanges"]["exchange"][0]
    assert exchange["meanAmount"] == "1.0"
    assert report.errors == []


def test_product_result_unit_map_exposes_flow_default_unit_factor():
    graph = HybridGraph.model_validate({
        "functionalUnit": "1 m3 oil",
        "nodes": [
            {
                "id": "node-1",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "process-1",
                "name": "process",
                "location": "CN",
                "reference_product": "oil-flow",
                "inputs": [],
                "outputs": [_switched_oil_port()],
                "emissions": [],
            }
        ],
        "exchanges": [],
    })
    db = _FakeDb()
    unit_factor_by_group_and_name, reference_unit_by_group = build_unit_reference_maps(db)

    _, product_unit_map, product_values = _build_product_result_view_from_graph(
        db=db,
        graph=graph,
        process_index=["process-1"],
        values=[[0.001]],
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
        reference_unit_by_group=reference_unit_by_group,
    )

    row = product_unit_map["process-1::out_oil"]
    assert row["current_unit"] == "kg"
    assert row["flow_default_unit"] == "m3"
    assert row["result_factor_to_flow_default_unit"] == 800
    assert product_values == [[0.001]]


def test_product_result_view_uses_exact_flow_version_localized_names():
    db = _FakeDb()
    db.flow_versions = [
        SimpleNamespace(
            source_namespace="tiangong_open_data",
            flow_uuid="oil-flow",
            source_version="2.0",
            flow_name="版本化原油",
            flow_name_en="Versioned crude oil",
            default_unit="m3",
            unit_group="Units of volume",
        )
    ]
    port = _switched_oil_port()
    port.update({
        "name": "Crude oil",
        "flowNameEn": "Crude oil",
        "flowSourceNamespace": "tiangong_open_data",
        "flowVersion": "2.0",
    })
    graph = HybridGraph.model_validate({
        "functionalUnit": "1 m3 oil",
        "nodes": [{
            "id": "node-1",
            "node_kind": "unit_process",
            "mode": "normalized",
            "process_uuid": "process-1",
            "name": "process",
            "location": "CN",
            "reference_product": "oil-flow",
            "inputs": [],
            "outputs": [port],
            "emissions": [],
        }],
        "exchanges": [],
    })
    unit_factor_by_group_and_name, reference_unit_by_group = build_unit_reference_maps(db)

    product_index, _, _ = _build_product_result_view_from_graph(
        db=db,
        graph=graph,
        process_index=["process-1"],
        values=[[1.0]],
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
        reference_unit_by_group=reference_unit_by_group,
    )

    assert product_index[0]["product_name_zh"] == "版本化原油"
    assert product_index[0]["product_name_en"] == "Versioned crude oil"


def test_product_result_view_excludes_hidden_lci_dataset_products():
    graph = HybridGraph.model_validate({
        "functionalUnit": "1 kg foreground product",
        "nodes": [
            {
                "id": "foreground-node",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "foreground-process",
                "name": "foreground",
                "location": "CN",
                "reference_product": "foreground-flow",
                "inputs": [],
                "outputs": [{
                    "id": "foreground-output",
                    "flowUuid": "foreground-flow",
                    "name": "foreground product",
                    "type": "technosphere",
                    "direction": "output",
                    "isProduct": True,
                    "amount": 1,
                    "unit": "kg",
                }],
                "emissions": [],
            },
            {
                "id": "background-node",
                "node_kind": "lci_dataset",
                "lci_role": "provider",
                "hidden": True,
                "mode": "normalized",
                "process_uuid": "background-process",
                "name": "background",
                "location": "GLO",
                "reference_product": "background-flow",
                "inputs": [],
                "outputs": [{
                    "id": "background-output",
                    "flowUuid": "background-flow",
                    "name": "background product",
                    "type": "technosphere",
                    "direction": "output",
                    "isProduct": True,
                    "amount": 1,
                    "unit": "kg",
                }],
                "emissions": [],
            },
        ],
        "exchanges": [],
    })

    product_index, _, product_values = _build_product_result_view_from_graph(
        db=_FakeDb(),
        graph=graph,
        process_index=["foreground-process", "background-process"],
        values=[[10.0, 2.0]],
        unit_factor_by_group_and_name={("Units of mass", "kg"): 1.0},
        reference_unit_by_group={"Units of mass": "kg"},
    )

    assert [row["process_uuid"] for row in product_index] == ["foreground-process"]
    assert product_values == [[10.0]]


def test_product_result_view_uses_current_unit_group_for_allocation_display():
    db = _FakeDb()
    db.flow_by_uuid.update({
        "gas-flow": SimpleNamespace(
            flow_uuid="gas-flow",
            default_unit="kg",
            unit_group="Units of mass",
            allocation_properties=[],
        ),
        "transport-flow": SimpleNamespace(
            flow_uuid="transport-flow",
            default_unit="kg",
            unit_group="Units of mass",
            allocation_properties=[],
        ),
    })
    db.unit_defs.extend([
        SimpleNamespace(unit_group="Units of energy", unit_name="MJ", factor_to_reference=1.0, is_reference=True),
        SimpleNamespace(unit_group="Units of energy", unit_name="GJ", factor_to_reference=1000.0, is_reference=False),
    ])
    db.unit_groups.append(SimpleNamespace(name="Units of energy", reference_unit="MJ"))
    graph = HybridGraph.model_validate({
        "functionalUnit": "1 kg product",
        "nodes": [
            {
                "id": "node-gas",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "process-gas",
                "name": "gas process",
                "location": "CN",
                "reference_product": "gas-flow",
                "inputs": [],
                "outputs": [
                    {
                        "id": "out_gas",
                        "flowUuid": "gas-flow",
                        "name": "gas",
                        "type": "technosphere",
                        "direction": "output",
                        "isProduct": True,
                        "amount": 1000,
                        "unit": "MJ",
                        "unitGroupSwitch": {
                            "sourceFlowUuid": "gas-flow",
                            "sourceUnitGroup": "Units of mass",
                            "sourceUnit": "kg",
                            "sourceReferenceUnit": "kg",
                            "sourceAmount": 1,
                            "targetUnitGroup": "Units of energy",
                            "targetUnit": "MJ",
                            "targetReferenceUnit": "MJ",
                            "factor": 1000,
                        },
                    },
                    {
                        "id": "out_transport",
                        "flowUuid": "transport-flow",
                        "name": "transport",
                        "type": "technosphere",
                        "direction": "output",
                        "isProduct": True,
                        "amount": 0.8,
                        "unit": "GJ",
                        "unitGroupSwitch": {
                            "sourceFlowUuid": "transport-flow",
                            "sourceUnitGroup": "Units of mass",
                            "sourceUnit": "kg",
                            "sourceReferenceUnit": "kg",
                            "sourceAmount": 0,
                            "targetUnitGroup": "Units of energy",
                            "targetUnit": "MJ",
                            "targetReferenceUnit": "MJ",
                            "factor": 600,
                        },
                    },
                ],
                "emissions": [],
            }
        ],
        "exchanges": [],
    })
    unit_factor_by_group_and_name, reference_unit_by_group = build_unit_reference_maps(db)

    product_index, product_unit_map, product_values = _build_product_result_view_from_graph(
        db=db,
        graph=graph,
        process_index=["process-gas"],
        values=[[42.85714285714286]],
        unit_factor_by_group_and_name=unit_factor_by_group_and_name,
        reference_unit_by_group=reference_unit_by_group,
    )

    conversion_by_port = {row["product_port_id"]: row["product_conversion_factor"] for row in product_index}
    assert conversion_by_port["out_gas"] == pytest.approx(0.7714285714285715)
    assert conversion_by_port["out_transport"] == pytest.approx(1.285714285714286)
    assert product_values[0][0] == pytest.approx(55.55555555555556)
    assert product_values[0][1] == pytest.approx(33.33333333333333)
    transport_units = product_unit_map["process-gas::out_transport"]
    assert transport_units["unit_group"] == "Units of energy"
    assert transport_units["current_unit"] == "GJ"
    assert transport_units["result_factor_to_flow_default_unit"] == pytest.approx(0.6)
