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


def test_standardized_product_amount_sets_multi_product_process_denominator():
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

    assert b_entry["value"] == pytest.approx(2.0 / 6.0)
    assert result["issues"] == []


def test_matrix_denominator_uses_standardized_default_product_amounts_not_allocation_weights():
    snapshot = {
        "processes": [
            {
                "process_uuid": "provider",
                "process_name": "gas transport provider",
                "reference_product_flow_uuid": "provider::transport",
            },
            {
                "process_uuid": "consumer",
                "process_name": "consumer",
                "reference_product_flow_uuid": "consumer::product",
            },
        ],
        "flows": [
            {
                "flow_uuid": "flow-gas",
                "flow_name": "natural gas",
                "flow_type": "Product flow",
                "unit_group_uuid": "unit_group::Units of mass",
            },
            {
                "flow_uuid": "flow-transport",
                "flow_name": "transport, pipeline, long distance, natural gas",
                "flow_type": "Product flow",
                "unit_group_uuid": "unit_group::Units of mass",
            },
            {
                "flow_uuid": "flow-final",
                "flow_name": "1,4-butanediol",
                "flow_type": "Product flow",
                "unit_group_uuid": "unit_group::Units of mass",
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
                "exchange_id": "provider::gas",
                "process_uuid": "provider",
                "flow_uuid": "flow-gas",
                "direction": "output",
                "amount": 1000.0 / 600.0,
                "allocation_fraction": 1000.0 / 1800.0,
                "allocation_weight": 1000.0,
                "allocation_weight_unit_group": "Units of energy",
            },
            {
                "exchange_id": "provider::transport",
                "process_uuid": "provider",
                "flow_uuid": "flow-transport",
                "direction": "output",
                "amount": 800.0 / 600.0,
                "allocation_fraction": 800.0 / 1800.0,
                "allocation_weight": 800.0,
                "allocation_weight_unit_group": "Units of energy",
            },
            {
                "exchange_id": "provider::co2",
                "process_uuid": "provider",
                "flow_uuid": "flow-co2",
                "direction": "output",
                "amount": 100.0,
            },
            {
                "exchange_id": "consumer::transport",
                "process_uuid": "consumer",
                "flow_uuid": "flow-transport",
                "direction": "input",
                "amount": 1.0,
            },
            {
                "exchange_id": "consumer::co2",
                "process_uuid": "consumer",
                "flow_uuid": "flow-co2",
                "direction": "output",
                "amount": 0.6,
            },
            {
                "exchange_id": "consumer::product",
                "process_uuid": "consumer",
                "flow_uuid": "flow-final",
                "direction": "output",
                "amount": 5.0,
                "allocation_fraction": 1.0,
            },
        ],
        "links": [
            {
                "consumer_process_uuid": "consumer",
                "provider_process_uuid": "provider",
                "provider_product_exchange_id": "provider::transport",
                "provider_product_conversion_factor": 1.0,
                "flow_uuid": "flow-transport",
                "quantity_mode": "single",
                "amount": 1.0,
            }
        ],
    }

    result = build_matrices_from_snapshot(snapshot)
    provider_b = next(
        item
        for item in result["B"]["data"]
        if item["row"] == "flow-co2" and item["col"] == "provider"
    )
    link_a = next(
        item
        for item in result["A"]["data"]
        if item["row"] == "provider" and item["col"] == "consumer"
    )
    consumer_b = next(
        item
        for item in result["B"]["data"]
        if item["row"] == "flow-co2" and item["col"] == "consumer"
    )

    assert provider_b["value"] == pytest.approx(100.0 / 3.0)
    assert link_a["value"] == pytest.approx(-1.0 / 5.0)
    assert consumer_b["value"] == pytest.approx(0.6 / 5.0)


def test_complete_manual_fractions_allow_mismatched_product_unit_groups_without_warning():
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
                "unit_group_uuid": "Units of mass",
            },
            {
                "flow_uuid": "flow-oil",
                "flow_name": "oil",
                "flow_type": "Product flow",
                "unit_group_uuid": "Units of volume",
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
                "exchange_id": "provider::solvent",
                "process_uuid": "provider",
                "flow_uuid": "flow-solvent",
                "direction": "output",
                "amount": 500.0,
                "allocation_fraction": 0.4,
                "product_conversion_factor": 1.0,
            },
            {
                "exchange_id": "provider::oil",
                "process_uuid": "provider",
                "flow_uuid": "flow-oil",
                "direction": "output",
                "amount": 1.0,
                "allocation_fraction": 0.6,
                "product_conversion_factor": 1.0,
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

    assert not any("allocation outputs have inconsistent unit groups" in issue for issue in result["issues"])


def test_manual_cross_default_unit_groups_use_product_port_scale_as_dimensionless_demand():
    snapshot = {
        "processes": [
            {
                "process_uuid": "provider",
                "process_name": "manual cross unit provider",
                "reference_product_flow_uuid": "provider::solvent",
            },
            {
                "process_uuid": "consumer",
                "process_name": "consumer",
                "reference_product_flow_uuid": "consumer::out",
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
                "flow_uuid": "flow-oil",
                "flow_name": "oil",
                "flow_type": "Product flow",
                "unit_group_uuid": "unit_group::Units of volume",
            },
            {
                "flow_uuid": "flow-final",
                "flow_name": "final",
                "flow_type": "Product flow",
                "unit_group_uuid": "unit_group::Units of mass",
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
                "amount": 500.0,
                "allocation_fraction": 0.4,
                "baseline_quantity_fraction": 500.0 / 501.0,
                "product_conversion_factor": (500.0 / 501.0) / 0.4,
                "allocation_scale": 0.4 / (500.0 / 501.0),
            },
            {
                "exchange_id": "provider::oil",
                "process_uuid": "provider",
                "flow_uuid": "flow-oil",
                "direction": "output",
                "amount": 1.0,
                "allocation_fraction": 0.6,
                "baseline_quantity_fraction": 1.0 / 501.0,
                "product_conversion_factor": (1.0 / 501.0) / 0.6,
                "allocation_scale": 0.6 / (1.0 / 501.0),
            },
            {
                "exchange_id": "provider::co2",
                "process_uuid": "provider",
                "flow_uuid": "flow-co2",
                "direction": "output",
                "amount": 2.0,
            },
            {
                "exchange_id": "consumer::oil",
                "process_uuid": "consumer",
                "flow_uuid": "flow-oil",
                "direction": "input",
                "amount": 1.0,
            },
            {
                "exchange_id": "consumer::out",
                "process_uuid": "consumer",
                "flow_uuid": "flow-final",
                "direction": "output",
                "amount": 1.0,
                "allocation_fraction": 1.0,
            },
        ],
        "links": [
            {
                "consumer_process_uuid": "consumer",
                "provider_process_uuid": "provider",
                "provider_product_exchange_id": "provider::oil",
                "provider_product_conversion_factor": (1.0 / 501.0) / 0.6,
                "flow_uuid": "flow-oil",
                "quantity_mode": "single",
                "amount": 1.0,
            }
        ],
    }

    result = build_matrices_from_snapshot(snapshot)
    provider_b = next(
        item
        for item in result["B"]["data"]
        if item["row"] == "flow-co2" and item["col"] == "provider"
    )
    link_a = next(
        item
        for item in result["A"]["data"]
        if item["row"] == "provider" and item["col"] == "consumer"
    )

    assert provider_b["value"] == pytest.approx(2.0 / 501.0)
    assert link_a["value"] == pytest.approx(-0.6 / (1.0 / 501.0))
    assert abs(provider_b["value"] * abs(link_a["value"]) - 1.2) < 1e-12
    assert not any("allocation outputs have inconsistent unit groups" in issue for issue in result["issues"])
