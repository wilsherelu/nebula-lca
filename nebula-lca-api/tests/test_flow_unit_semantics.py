from types import SimpleNamespace

from app.flow_unit_semantics import (
    build_unit_reference_maps,
    collect_flow_default_unit_conversion_violations,
    resolve_flow_port_unit_semantics,
)
from app.main import _build_product_result_view_from_graph
from app.models import FlowRecord, ReferenceProcess, UnitDefinition, UnitGroup
from app.schemas import HybridGraph
from app.tidas_export import ExportReport, _build_process_data


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeDb:
    def __init__(self):
        self.flow_by_uuid = {
            "oil-flow": SimpleNamespace(
                flow_uuid="oil-flow",
                default_unit="m3",
                unit_group="Units of volume",
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

    def get(self, model, key):
        if model is FlowRecord:
            return self.flow_by_uuid.get(key)
        if model is ReferenceProcess:
            return None
        return None

    def query(self, model):
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


def test_flow_default_unit_conversion_requires_switch_for_cross_group_current_unit():
    port = _switched_oil_port()
    port.pop("unitGroupSwitch")

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
        _FakeDb(),
    )

    assert len(violations) == 1
    assert violations[0]["flow_default_unit_group"] == "Units of volume"
    assert violations[0]["current_unit_group"] == "Units of mass"


def test_flow_default_unit_conversion_uses_catalog_default_over_stale_switch_source_group():
    port = _switched_oil_port()
    port["unitGroupSwitch"]["sourceUnitGroup"] = "Units of mass"

    sem = resolve_flow_port_unit_semantics(_FakeDb(), port)

    assert sem.ok is True
    assert sem.flow_default_unit_group == "Units of volume"
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
    assert exchange["meanAmount"] == 1
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
