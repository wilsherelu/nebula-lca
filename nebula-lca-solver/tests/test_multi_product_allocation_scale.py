import pytest

from app.core.matrix_builder import build_matrices_from_snapshot


def test_provider_product_conversion_factor_adjusts_technosphere_link():
    snapshot = {
        "processes": [
            {
                "process_uuid": "provider",
                "process_name": "multi product provider",
                "reference_product_flow_uuid": "provider::a",
            },
            {
                "process_uuid": "consumer",
                "process_name": "consumer",
                "reference_product_flow_uuid": "consumer::out",
            },
        ],
        "flows": [
            {
                "flow_uuid": "flow-a",
                "flow_name": "A",
                "flow_type": "Product flow",
                "unit_group_uuid": "Units of mass",
            },
            {
                "flow_uuid": "flow-b",
                "flow_name": "B",
                "flow_type": "Product flow",
                "unit_group_uuid": "Units of mass",
            },
            {
                "flow_uuid": "flow-final",
                "flow_name": "final",
                "flow_type": "Product flow",
                "unit_group_uuid": "Units of mass",
            },
            {
                "flow_uuid": "flow-co2",
                "flow_name": "CO2",
                "flow_type": "Elementary flow",
                "unit_group_uuid": "Units of mass",
            },
        ],
        "exchanges": [
            {
                "exchange_id": "provider::a",
                "process_uuid": "provider",
                "flow_uuid": "flow-a",
                "direction": "output",
                "amount": 1.0,
                "allocation_fraction": 0.4,
                "product_conversion_factor": 1.25,
            },
            {
                "exchange_id": "provider::b",
                "process_uuid": "provider",
                "flow_uuid": "flow-b",
                "direction": "output",
                "amount": 1.0,
                "allocation_fraction": 0.6,
                "product_conversion_factor": 5 / 6,
            },
            {
                "exchange_id": "provider::co2",
                "process_uuid": "provider",
                "flow_uuid": "flow-co2",
                "direction": "output",
                "amount": 2.0,
            },
            {
                "exchange_id": "consumer::in",
                "process_uuid": "consumer",
                "flow_uuid": "flow-a",
                "direction": "input",
                "amount": 1.0,
            },
            {
                "exchange_id": "consumer::out",
                "process_uuid": "consumer",
                "flow_uuid": "flow-final",
                "direction": "output",
                "amount": 1.0,
            },
        ],
        "links": [
            {
                "consumer_process_uuid": "consumer",
                "provider_process_uuid": "provider",
                "provider_product_exchange_id": "provider::a",
                "provider_product_conversion_factor": 1.25,
                "flow_uuid": "flow-a",
                "quantity_mode": "single",
                "amount": 1.0,
            }
        ],
    }

    result = build_matrices_from_snapshot(snapshot)
    a_entry = next(
        item
        for item in result["A"]["data"]
        if item["row"] == "provider" and item["col"] == "consumer"
    )

    assert a_entry["value"] == pytest.approx(-0.8)


def test_allocation_weight_sets_multi_product_process_denominator():
    snapshot = {
        "processes": [
            {
                "process_uuid": "provider",
                "process_name": "multi product provider",
                "reference_product_flow_uuid": "provider::solvent",
            },
        ],
        "flows": [
            {
                "flow_uuid": "flow-solvent",
                "flow_name": "solvent",
                "flow_type": "Product flow",
                "unit_group_uuid": "unit_group::Units of mass",
            },
            {
                "flow_uuid": "flow-gas",
                "flow_name": "gas",
                "flow_type": "Product flow",
                "unit_group_uuid": "unit_group::Units of volume",
            },
            {
                "flow_uuid": "flow-co2",
                "flow_name": "CO2",
                "flow_type": "Elementary flow",
                "unit_group_uuid": "unit_group::Units of mass",
            },
        ],
        "exchanges": [
            {
                "exchange_id": "provider::solvent",
                "process_uuid": "provider",
                "flow_uuid": "flow-solvent",
                "direction": "output",
                "amount": 5.0,
                "allocation_fraction": 0.625,
                "allocation_weight": 5.0,
                "allocation_weight_unit_group": "Units of mass",
            },
            {
                "exchange_id": "provider::gas",
                "process_uuid": "provider",
                "flow_uuid": "flow-gas",
                "direction": "output",
                "amount": 1.0,
                "allocation_fraction": 0.375,
                "allocation_weight": 3.0,
                "allocation_weight_unit_group": "Units of mass",
            },
            {
                "exchange_id": "provider::co2",
                "process_uuid": "provider",
                "flow_uuid": "flow-co2",
                "direction": "output",
                "amount": 2.0,
            },
        ],
        "links": [],
    }

    result = build_matrices_from_snapshot(snapshot)
    b_entry = next(
        item
        for item in result["B"]["data"]
        if item["row"] == "flow-co2" and item["col"] == "provider"
    )

    assert b_entry["value"] == pytest.approx(0.25)
    assert result["issues"] == []
