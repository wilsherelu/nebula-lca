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
    second = graphs["case_02_balanced_chain"]
    third = graphs["case_03_mixed_normalized_supply"]
    fourth = graphs["case_04_recycle_loop"]

    assert [len(graph["nodes"]) for graph in (first, second, third, fourth)] == [1, 4, 5, 5]
    assert [len(graph["exchanges"]) for graph in (first, second, third, fourth)] == [0, 3, 7, 8]
    assert {
        first["nodes"][0]["process_uuid"],
        second["nodes"][0]["process_uuid"],
        next(node for node in third["nodes"] if node["id"] == "node-distillation")["process_uuid"],
    } == {"paper-refinery-distillation"}
    assert all(node["mode"] == "balanced" for node in second["nodes"])
    assert [node["mode"] for node in third["nodes"]].count("normalized") == 1
    assert next(node for node in third["nodes"] if node["mode"] == "normalized")["node_kind"] == "market_process"
    assert third["metadata"]["data_status"].startswith("hypothetical")


def test_distillation_is_an_explicit_unallocated_multi_product_source():
    graph = MODULE.build_progressive_graphs(_flows())["case_01_unit_process"]
    process = graph["nodes"][0]
    products = [port for port in process["outputs"] if port.get("isProduct")]

    assert process["allocation_method"] == "unit_group_physical_v1"
    assert process["mode"] == "balanced"
    assert len(products) == 3
    assert sum(port["amount"] for port in products) == 4.2
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
    assert len(canvas["edges"]) == 2
    assert {edge["flowUuid"] for edge in canvas["edges"]} == {
        "flow-hydrotreated_naphtha",
        "flow-recycle_naphtha",
    }
    assert all(node["mode"] == "balanced" for node in canvas["nodes"])


def test_provider_elementary_refs_are_exact_and_unique():
    graph = MODULE.build_progressive_graphs(_flows())["case_04_recycle_loop"]
    refs = MODULE._elementary_refs(graph)

    assert len(refs) == 5
    assert len({row["exchange_id"] for row in refs}) == 5
    assert {row["flow_uuid"] for row in refs} == {MODULE.CO2_UUID}
    assert {row["version"] for row in refs} == {MODULE.CO2_VERSION}


def test_balanced_connection_residual_aggregates_provider_requirements():
    graph = MODULE.build_progressive_graphs(_flows())["case_02_balanced_chain"]
    result = {
        "activity_vector": [
            {"process_uuid": "paper-refinery-distillation", "activity_amount": 1.05},
            {"process_uuid": "paper-refinery-hydrotreating", "activity_amount": 1.0},
            {"process_uuid": "paper-refinery-reforming", "activity_amount": 0.8},
            {"process_uuid": "paper-refinery-blending", "activity_amount": 1.0},
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
            {"exchange_id": "node-hydrotreating::out-hydrotreated", "scaled_amount": 1.0},
            {"exchange_id": "node-reforming::in-hydrotreated", "scaled_amount": 1.0},
            {"exchange_id": "node-reforming::out-reformate", "scaled_amount": 0.8},
            {"exchange_id": "node-blending::in-reformate", "scaled_amount": 0.8},
        ],
    }

    residual = MODULE._connection_residuals(graph, result)[0]

    assert residual["source_raw_exchange_scaled_amount"] == 0.2625
    assert residual["provider_requirement_contribution"] == 1.05
    assert residual["target_scaled_amount"] == 1.05
    assert residual["provider_aggregate_expected_activity"] == 1.05
    assert residual["absolute_residual"] == 0.0


def test_mixed_case_normalized_electricity_feeds_multiple_balanced_processes():
    graph = MODULE.build_progressive_graphs(_flows())["case_03_mixed_normalized_supply"]
    supply = next(node for node in graph["nodes"] if node["process_uuid"] == "market_refinery_electricity_supply")
    electricity_edges = [edge for edge in graph["exchanges"] if edge["flowUuid"] == "flow-electricity"]

    assert supply["mode"] == "normalized"
    assert len(electricity_edges) == 4
    assert all(edge["quantityMode"] == "dual" for edge in electricity_edges)
    assert all(
        next(node for node in graph["nodes"] if node["id"] == edge["toNode"])["mode"] == "balanced"
        for edge in electricity_edges
    )


def test_connection_residual_aggregates_multiple_edges_from_one_supplier():
    graph = {
        "nodes": [
            {
                "id": "supplier-node",
                "process_uuid": "supplier",
                "mode": "normalized",
                "outputs": [{"id": "out", "amount": 1.0, "isProduct": True}],
            },
            {"id": "consumer-a", "process_uuid": "a", "outputs": []},
            {"id": "consumer-b", "process_uuid": "b", "outputs": []},
        ],
        "exchanges": [
            MODULE._edge("edge-a", "supplier-node", "out", "consumer-a", "in", "flow", "flow", 1.0),
            MODULE._edge("edge-b", "supplier-node", "out", "consumer-b", "in", "flow", "flow", 2.0),
        ],
    }
    result = {
        "activity_vector": [
            {"process_uuid": "supplier", "activity_amount": 3.0},
            {"process_uuid": "a", "activity_amount": 1.0},
            {"process_uuid": "b", "activity_amount": 1.0},
        ],
        "scaled_exchanges": [
            {"exchange_id": "supplier-node::out", "scaled_amount": 3.0},
            {"exchange_id": "consumer-a::in", "scaled_amount": 1.0},
            {"exchange_id": "consumer-b::in", "scaled_amount": 2.0},
        ],
    }

    residuals = MODULE._connection_residuals(graph, result)

    assert [row["provider_requirement_contribution"] for row in residuals] == [1.0, 2.0]
    assert all(row["provider_aggregate_expected_activity"] == 3.0 for row in residuals)
    assert all(row["absolute_residual"] == 0.0 for row in residuals)


def test_recycle_case_adds_a_balanced_back_edge_and_changes_process_coefficients():
    graphs = MODULE.build_progressive_graphs(_flows())
    open_loop = graphs["case_03_mixed_normalized_supply"]
    recycle = graphs["case_04_recycle_loop"]
    recycle_edge = next(edge for edge in recycle["exchanges"] if edge["id"] == "edge-recycle-naphtha")
    open_reforming = next(node for node in open_loop["nodes"] if node["id"] == "node-reforming")
    recycle_reforming = next(node for node in recycle["nodes"] if node["id"] == "node-reforming")

    assert recycle_edge["fromNode"] == "node-reforming"
    assert recycle_edge["toNode"] == "node-hydrotreating"
    assert recycle_edge["quantityMode"] == "single"
    assert next(port for port in open_reforming["outputs"] if port["id"] == "out-reformate")["amount"] == 1.0
    assert next(port for port in recycle_reforming["outputs"] if port["id"] == "out-reformate")["amount"] == 0.8
    recycle_port = next(port for port in recycle_reforming["outputs"] if port["id"] == "out-recycle-naphtha")
    assert recycle_port["amount"] == 0.2
    assert recycle_port["isProduct"] is True
    assert sum(
        port["allocationFactor"]
        for port in recycle_reforming["outputs"]
        if port.get("isProduct")
    ) == 1.0


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
