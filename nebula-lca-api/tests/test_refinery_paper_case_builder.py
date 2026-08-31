from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_refinery_paper_cases.py"
SPEC = importlib.util.spec_from_file_location("build_refinery_paper_cases", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _flows() -> dict[str, str]:
    return {key: f"flow-{key}" for key in MODULE.FLOW_SPECS}


def test_progressive_graphs_add_processes_without_changing_existing_identity():
    graphs = MODULE.build_progressive_graphs(_flows())

    first = graphs["case_01_unit_process"]
    second = graphs["case_02_two_process"]
    third = graphs["case_03_four_process"]

    assert [len(graph["nodes"]) for graph in (first, second, third)] == [1, 2, 4]
    assert [len(graph["exchanges"]) for graph in (first, second, third)] == [0, 1, 3]
    assert first["nodes"][0] == second["nodes"][0] == third["nodes"][0]
    assert second["nodes"][1] == third["nodes"][1]
    assert third["metadata"]["data_status"].startswith("hypothetical")


def test_distillation_is_an_explicit_unallocated_multi_product_source():
    graph = MODULE.build_progressive_graphs(_flows())["case_01_unit_process"]
    process = graph["nodes"][0]
    products = [port for port in process["outputs"] if port.get("isProduct")]

    assert process["allocation_method"] == "unit_group_physical_v1"
    assert len(products) == 3
    assert sum(port["amount"] for port in products) == 1.0
    assert sum(port["allocationFactor"] for port in products) == 1.0
    assert all(port["allocationBasis"] == {"method": "quantity"} for port in products)


def test_pts_compiles_exactly_the_two_upgrading_processes():
    graph = MODULE.build_pts_compile_graph(_flows())
    canvas = graph["metadata"]["canvases"][0]

    assert len(graph["nodes"]) == 1
    assert graph["nodes"][0]["node_kind"] == "pts_module"
    assert [node["process_uuid"] for node in canvas["nodes"]] == [
        "paper-refinery-hydrotreating",
        "paper-refinery-reforming",
    ]
    assert len(canvas["edges"]) == 1
    assert canvas["edges"][0]["flowUuid"] == "flow-hydrotreated_naphtha"


def test_provider_elementary_refs_are_exact_and_unique():
    graph = MODULE.build_progressive_graphs(_flows())["case_03_four_process"]
    refs = MODULE._elementary_refs(graph)

    assert len(refs) == 4
    assert len({row["exchange_id"] for row in refs}) == 4
    assert {row["flow_uuid"] for row in refs} == {MODULE.CO2_UUID}
    assert {row["version"] for row in refs} == {MODULE.CO2_VERSION}


def test_multi_product_connection_residual_uses_normalized_provider_supply():
    graph = MODULE.build_progressive_graphs(_flows())["case_02_two_process"]
    result = {
        "activity_vector": [
            {"process_uuid": "paper-refinery-distillation", "activity_amount": 1.05},
            {"process_uuid": "paper-refinery-hydrotreating", "activity_amount": 1.0},
        ],
        "scaled_exchanges": [
            {
                "exchange_id": "node-distillation::out-naphtha",
                "scaled_amount": 0.2625,
            },
            {
                "exchange_id": "node-hydrotreating::in-naphtha",
                "scaled_amount": 1.05,
            },
        ],
    }

    residual = MODULE._connection_residuals(graph, result)[0]

    assert residual["source_raw_exchange_scaled_amount"] == 0.2625
    assert residual["source_normalized_supply_amount"] == 1.05
    assert residual["target_scaled_amount"] == 1.05
    assert residual["absolute_residual"] == 0.0


def test_lcia_comparison_is_component_wise_for_target_process():
    def result(values):
        return {
            "lci_result": {
                "process_index": ["target", "other"],
                "indicator_index": [{"canonical_indicator_key": "climate change"}],
                "values": [values],
            }
        }

    comparison = MODULE._compare_process_lcia(
        result([0.7, 9.0]),
        result([0.7 + 1e-12, 3.0]),
        "target",
    )

    assert comparison["comparable"] is True
    assert comparison["indicator_count"] == 1
    assert comparison["within_tolerance"] is True
