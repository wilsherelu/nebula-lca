import pytest

import app.database as _db_module
from app.main import _build_product_result_view_from_graph, _graph_with_solver_unit_defaults
from app.models import FlowRecord, UnitDefinition, UnitGroup
from app.database import Base
from app.allocation import calculate_product_allocation, collect_multi_product_unit_group_violations
from app.schemas import HybridGraph
from app.solver import to_tiangong_like


def _product(port_id: str, flow_uuid: str, amount: float, allocation_factor: float | None = None) -> dict:
    port = {
        "id": port_id,
        "flowUuid": flow_uuid,
        "name": flow_uuid,
        "unit": "kg",
        "unitGroup": "Units of mass",
        "amount": amount,
        "type": "technosphere",
        "direction": "output",
        "isProduct": True,
    }
    if allocation_factor is not None:
        port["allocationFactor"] = allocation_factor
        port["allocationBasis"] = {"method": "manual_factor"}
    return port


def _graph_with_manual_products() -> dict:
    return {
        "functionalUnit": "1 kg A",
        "nodes": [
            {
                "id": "provider-node",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "provider",
                "name": "provider",
                "location": "GLO",
                "reference_product": "A",
                "inputs": [],
                "outputs": [
                    _product("out-a", "flow-a", 1.0, 0.4),
                    _product("out-b", "flow-b", 1.0, 0.6),
                    {
                        "id": "em-co2",
                        "flowUuid": "flow-co2",
                        "name": "CO2",
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "amount": 2.0,
                        "type": "biosphere",
                        "direction": "output",
                    },
                ],
                "emissions": [],
            },
            {
                "id": "consumer-node",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "consumer",
                "name": "consumer",
                "location": "GLO",
                "reference_product": "final",
                "inputs": [
                    {
                        "id": "in-a",
                        "flowUuid": "flow-a",
                        "name": "A",
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "amount": 1.0,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [_product("out-final", "flow-final", 1.0)],
                "emissions": [],
            },
        ],
        "exchanges": [
            {
                "id": "edge-a",
                "fromNode": "provider-node",
                "toNode": "consumer-node",
                "sourceHandle": "out:out-a",
                "targetHandle": "in:in-a",
                "flowUuid": "flow-a",
                "flowName": "A",
                "quantityMode": "single",
                "amount": 1.0,
                "providerAmount": 1.0,
                "consumerAmount": 1.0,
                "unit": "kg",
                "type": "technosphere",
            }
        ],
        "metadata": {},
    }


def test_snapshot_writes_product_conversion_factors_and_link_provider_product():
    snapshot = to_tiangong_like(HybridGraph.model_validate(_graph_with_manual_products()))
    exchange_by_id = {item["exchange_id"]: item for item in snapshot["exchanges"]}
    link = snapshot["links"][0]

    assert exchange_by_id["provider-node::out-a"]["allocation_fraction"] == 0.4
    assert exchange_by_id["provider-node::out-a"]["baseline_quantity_fraction"] == 0.5
    assert exchange_by_id["provider-node::out-a"]["product_conversion_factor"] == pytest.approx(1.25)
    assert exchange_by_id["provider-node::out-b"]["product_conversion_factor"] == pytest.approx(5 / 6)
    assert exchange_by_id["provider-node::out-a"]["allocation_scale"] == pytest.approx(0.8)
    assert exchange_by_id["provider-node::out-b"]["allocation_scale"] == pytest.approx(1.2)
    assert link["provider_product_port_id"] == "out-a"
    assert link["provider_product_exchange_id"] == "provider-node::out-a"
    assert link["provider_product_conversion_factor"] == pytest.approx(1.25)
    assert link["provider_allocation_scale"] == pytest.approx(0.8)


def test_manual_factors_allow_different_product_unit_groups():
    graph = _graph_with_manual_products()
    graph["nodes"][0]["outputs"][1]["unit"] = "m3"
    graph["nodes"][0]["outputs"][1]["unitGroup"] = "Units of volume"

    assert collect_multi_product_unit_group_violations(graph) == []


def test_quantity_allocation_ignores_cached_factors_and_converts_units():
    ports = [
        {
            "id": "gas",
            "flowUuid": "gas-flow",
            "unit": "MJ",
            "unitGroup": "Units of energy",
            "amount": 1000,
            "type": "technosphere",
            "direction": "output",
            "isProduct": True,
            "allocationFactor": 0.999201,
            "allocationBasis": {"method": "quantity"},
        },
        {
            "id": "transport",
            "flowUuid": "transport-flow",
            "unit": "GJ",
            "unitGroup": "Units of energy",
            "amount": 0.8,
            "type": "technosphere",
            "direction": "output",
            "isProduct": True,
            "allocationFactor": 0.000799,
            "allocationBasis": {"method": "quantity"},
        },
    ]

    result = calculate_product_allocation(
        ports,
        process_uuid="energy-process",
        unit_factor_by_group_and_name={
            ("Units of energy", "MJ"): 1.0,
            ("Units of energy", "GJ"): 1000.0,
        },
    )

    assert result.method == "quantity"
    assert result.weights == {"gas": pytest.approx(1000), "transport": pytest.approx(800)}
    assert result.factors == {"gas": pytest.approx(1000 / 1800), "transport": pytest.approx(800 / 1800)}


def test_quantity_allocation_ignores_legacy_cached_factors_without_basis():
    ports = [
        {
            "id": "gas",
            "flowUuid": "gas-flow",
            "unit": "MJ",
            "unitGroup": "Units of energy",
            "amount": 1000,
            "type": "technosphere",
            "direction": "output",
            "isProduct": True,
            "allocationFactor": 0.999201,
        },
        {
            "id": "transport",
            "flowUuid": "transport-flow",
            "unit": "GJ",
            "unitGroup": "Units of energy",
            "amount": 0.8,
            "type": "technosphere",
            "direction": "output",
            "isProduct": True,
            "allocationFactor": 0.000799,
        },
    ]

    result = calculate_product_allocation(
        ports,
        process_uuid="energy-process",
        unit_factor_by_group_and_name={
            ("Units of energy", "MJ"): 1.0,
            ("Units of energy", "GJ"): 1000.0,
        },
    )

    assert result.method == "quantity"
    assert result.factors == {"gas": pytest.approx(1000 / 1800), "transport": pytest.approx(800 / 1800)}


def test_quantity_allocation_prefers_actual_unit_over_stale_switch_target_unit():
    ports = [
        {
            "id": "gas",
            "flowUuid": "gas-flow",
            "unit": "MJ",
            "unitGroup": None,
            "amount": 1000,
            "type": "technosphere",
            "direction": "output",
            "isProduct": True,
            "unitGroupSwitch": {
                "sourceUnitGroup": "Units of mass",
                "sourceUnit": "kg",
                "sourceReferenceUnit": "kg",
                "targetUnitGroup": "Units of energy",
                "targetUnit": "MJ",
                "factor": 1000,
            },
        },
        {
            "id": "transport",
            "flowUuid": "transport-flow",
            "unit": "GJ",
            "unitGroup": None,
            "amount": 0.8,
            "type": "technosphere",
            "direction": "output",
            "isProduct": True,
            "unitGroupSwitch": {
                "sourceUnitGroup": "Units of mass",
                "sourceUnit": "kg",
                "sourceReferenceUnit": "kg",
                "targetUnitGroup": "Units of energy",
                "targetUnit": "MJ",
                "factor": 600,
            },
        },
    ]

    result = calculate_product_allocation(
        ports,
        process_uuid="energy-process",
        unit_factor_by_group_and_name={
            ("Units of energy", "MJ"): 1.0,
            ("Units of energy", "GJ"): 1000.0,
        },
    )

    assert result.method == "quantity"
    assert result.weights == {"gas": pytest.approx(1000), "transport": pytest.approx(800)}
    assert result.factors == {"gas": pytest.approx(1000 / 1800), "transport": pytest.approx(800 / 1800)}


def test_product_result_view_applies_allocation_scale():
    Base.metadata.create_all(bind=_db_module.engine)
    db = _db_module.SessionLocal()
    try:
        db.merge(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        db.commit()

        product_index, _, product_values = _build_product_result_view_from_graph(
            db=db,
            graph=HybridGraph.model_validate({
                **_graph_with_manual_products(),
                "nodes": [_graph_with_manual_products()["nodes"][0]],
                "exchanges": [],
            }),
            process_index=["provider"],
            values=[[1.0]],
            unit_factor_by_group_and_name={("Units of mass", "kg"): 1.0},
            reference_unit_by_group={"Units of mass": "kg"},
        )
    finally:
        db.close()

    assert [item["product_port_id"] for item in product_index] == ["out-a", "out-b"]
    assert product_values == [[pytest.approx(0.8), pytest.approx(1.2)]]


def test_solver_unit_defaults_convert_switched_flow_to_default_unit_group():
    Base.metadata.create_all(bind=_db_module.engine)
    db = _db_module.SessionLocal()
    try:
        db.query(UnitDefinition).delete()
        db.merge(UnitGroup(name="Units of volume", reference_unit="m3"))
        db.merge(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.add(UnitDefinition(unit_group="Units of volume", unit_name="m3", factor_to_reference=1.0, is_reference=True))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        db.merge(
            FlowRecord(
                flow_uuid="oil-flow",
                flow_name="oil",
                flow_type="Product flow",
                default_unit="m3",
                unit_group="Units of volume",
                source="test",
                is_custom=False,
            )
        )
        db.commit()

        graph = HybridGraph.model_validate({
            "functionalUnit": "1 m3 oil",
            "nodes": [
                {
                    "id": "node-1",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "proc-1",
                    "name": "process",
                    "location": "GLO",
                    "reference_product": "oil",
                    "inputs": [],
                    "outputs": [
                        {
                            "id": "out-oil",
                            "flowUuid": "oil-flow",
                            "name": "oil",
                            "unit": "kg",
                            "unitGroup": "Units of mass",
                            "amount": 800,
                            "type": "technosphere",
                            "direction": "output",
                            "isProduct": True,
                            "unitGroupSwitch": {
                                "sourceFlowUuid": "oil-flow",
                                "sourceUnitGroup": "Units of volume",
                                "sourceUnit": "m3",
                                "sourceReferenceUnit": "m3",
                                "targetUnitGroup": "Units of mass",
                                "targetUnit": "kg",
                                "targetReferenceUnit": "kg",
                                "factor": 800,
                            },
                        }
                    ],
                    "emissions": [],
                }
            ],
            "exchanges": [],
            "metadata": {},
        })

        normalized = _graph_with_solver_unit_defaults(
            db=db,
            graph=graph,
            unit_factor_by_group_and_name={
                ("Units of volume", "m3"): 1.0,
                ("Units of mass", "kg"): 1.0,
            },
            reference_unit_by_group={"Units of volume": "m3", "Units of mass": "kg"},
        )
    finally:
        db.close()

    port = normalized.nodes[0].outputs[0]
    assert port.amount == pytest.approx(1.0)
    assert port.unit == "m3"
    assert port.unitGroup == "Units of volume"


def test_allocation_weight_survives_flow_default_unit_standardization():
    Base.metadata.create_all(bind=_db_module.engine)
    db = _db_module.SessionLocal()
    try:
        db.query(UnitDefinition).delete()
        db.merge(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.merge(UnitGroup(name="Units of volume", reference_unit="m3"))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        db.add(UnitDefinition(unit_group="Units of volume", unit_name="m3", factor_to_reference=1.0, is_reference=True))
        db.merge(
            FlowRecord(
                flow_uuid="flow-solvent",
                flow_name="solvent",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="test",
                is_custom=False,
            )
        )
        db.merge(
            FlowRecord(
                flow_uuid="flow-gas",
                flow_name="natural gas",
                flow_type="Product flow",
                default_unit="m3",
                unit_group="Units of volume",
                source="test",
                is_custom=False,
            )
        )
        db.commit()

        graph = HybridGraph.model_validate({
            "functionalUnit": "1 kg solvent",
            "nodes": [
                {
                    "id": "node-1",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "proc-1",
                    "name": "process",
                    "location": "GLO",
                    "reference_product": "solvent",
                    "inputs": [],
                    "outputs": [
                        _product("out-solvent", "flow-solvent", 5.0),
                        {
                            **_product("out-gas", "flow-gas", 3.0),
                            "unitGroupSwitch": {
                                "sourceFlowUuid": "flow-gas",
                                "sourceUnitGroup": "Units of volume",
                                "sourceUnit": "m3",
                                "sourceReferenceUnit": "m3",
                                "targetUnitGroup": "Units of mass",
                                "targetUnit": "kg",
                                "targetReferenceUnit": "kg",
                                "factor": 3,
                            },
                        },
                        {
                            "id": "em-co2",
                            "flowUuid": "flow-co2",
                            "name": "CO2",
                            "unit": "kg",
                            "unitGroup": "Units of mass",
                            "amount": 2.0,
                            "type": "biosphere",
                            "direction": "output",
                        },
                    ],
                    "emissions": [],
                }
            ],
            "exchanges": [],
            "metadata": {},
        })

        normalized = _graph_with_solver_unit_defaults(
            db=db,
            graph=graph,
            unit_factor_by_group_and_name={
                ("Units of mass", "kg"): 1.0,
                ("Units of volume", "m3"): 1.0,
            },
            reference_unit_by_group={"Units of mass": "kg", "Units of volume": "m3"},
        )
    finally:
        db.close()

    snapshot = to_tiangong_like(normalized)
    exchange_by_id = {item["exchange_id"]: item for item in snapshot["exchanges"]}

    assert exchange_by_id["node-1::out-solvent"]["allocation_weight"] == pytest.approx(5.0)
    assert exchange_by_id["node-1::out-gas"]["allocation_weight"] == pytest.approx(3.0)
    assert exchange_by_id["node-1::out-solvent"]["product_conversion_factor"] == pytest.approx(4 / 3)
    assert exchange_by_id["node-1::out-gas"]["product_conversion_factor"] == pytest.approx(4 / 9)
