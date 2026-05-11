from fastapi import HTTPException

from app.models import PtsCompileArtifact
from app.schemas import HybridGraph
from app.services.pts_resources import (
    _validate_pts_market_supplier_coverage,
    build_flattened_graph_for_run_pts,
)


def _market_graph(edges):
    return {
        "nodes": [
            {
                "id": "supplier-pv",
                "node_kind": "unit_process",
                "name": "PV",
                "inputs": [],
                "outputs": [
                    {
                        "id": "out-electricity",
                        "flowUuid": "flow-electricity",
                        "name": "electricity",
                        "type": "technosphere",
                    }
                ],
            },
            {
                "id": "supplier-coal",
                "node_kind": "unit_process",
                "name": "Coal",
                "inputs": [],
                "outputs": [
                    {
                        "id": "out-electricity",
                        "flowUuid": "flow-electricity",
                        "name": "electricity",
                        "type": "technosphere",
                    }
                ],
            },
            {
                "id": "market",
                "node_kind": "market_process",
                "name": "Electricity market",
                "inputs": [
                    {
                        "id": "in-pv",
                        "flowUuid": "flow-electricity",
                        "name": "electricity @ PV",
                        "type": "technosphere",
                        "amount": 0.2,
                    },
                    {
                        "id": "in-coal",
                        "flowUuid": "flow-electricity",
                        "name": "electricity @ Coal",
                        "type": "technosphere",
                        "amount": 0.8,
                    },
                ],
                "outputs": [
                    {
                        "id": "out-electricity",
                        "flowUuid": "flow-electricity",
                        "name": "electricity",
                        "type": "technosphere",
                    }
                ],
            },
        ],
        "exchanges": edges,
    }


def test_pts_market_with_no_internal_supplier_is_blocked():
    try:
        _validate_pts_market_supplier_coverage(
            pts_uuid="pts-1",
            pts_node_id="pts-node",
            pts_graph=_market_graph([]),
        )
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail["code"] == "PTS_MARKET_PROCESS_REQUIRES_INTERNAL_SUPPLIERS"
    else:
        raise AssertionError("Expected isolated market process to be blocked")


def test_pts_market_with_partial_suppliers_warns():
    warnings = _validate_pts_market_supplier_coverage(
        pts_uuid="pts-1",
        pts_node_id="pts-node",
        pts_graph=_market_graph(
            [
                {
                    "fromNode": "supplier-pv",
                    "toNode": "market",
                    "flowUuid": "flow-electricity",
                    "targetHandle": "in:in-pv",
                }
            ]
        ),
    )

    assert [warning.code for warning in warnings] == ["PTS_MARKET_PROCESS_PARTIAL_SUPPLIERS"]
    assert warnings[0].actual_total == 1.0
    assert warnings[0].expected_total == 2.0


def test_pts_market_with_all_suppliers_has_no_warning():
    warnings = _validate_pts_market_supplier_coverage(
        pts_uuid="pts-1",
        pts_node_id="pts-node",
        pts_graph=_market_graph(
            [
                {
                    "fromNode": "supplier-pv",
                    "toNode": "market",
                    "flowUuid": "flow-electricity",
                    "targetHandle": "in:in-pv",
                },
                {
                    "fromNode": "supplier-coal",
                    "toNode": "market",
                    "flowUuid": "flow-electricity",
                    "targetHandle": "in:in-coal",
                },
            ]
        ),
    )

    assert warnings == []


def _pts_shell_graph(source_handle: str | None) -> HybridGraph:
    edge_payload = {
        "id": "edge-pts-market",
        "fromNode": "pts-node",
        "toNode": "market",
        "targetHandle": "in:market-diesel-a",
        "targetPortId": "in:market-diesel-a",
        "flowUuid": "flow-diesel",
        "flowName": "diesel",
        "quantityMode": "dual",
        "amount": 0.5,
        "providerAmount": 0.5,
        "consumerAmount": 0.5,
        "unit": "kg",
        "type": "technosphere",
    }
    if source_handle is not None:
        edge_payload["sourceHandle"] = source_handle
        edge_payload["sourcePortId"] = source_handle
    return HybridGraph.model_validate(
        {
            "schema_version": "hybrid-0.2",
            "functionalUnit": "1 kg diesel",
            "nodes": [
                {
                    "id": "pts-node",
                    "node_kind": "pts_module",
                    "mode": "normalized",
                    "pts_uuid": "pts-1",
                    "process_uuid": "pts-1",
                    "name": "Packed PTS",
                    "location": "GLO",
                    "reference_product": "diesel",
                    "inputs": [],
                    "outputs": [
                        {
                            "id": "ptsout-a",
                            "flowUuid": "flow-diesel",
                            "name": "diesel",
                            "unit": "kg",
                            "amount": 1.0,
                            "type": "technosphere",
                            "direction": "output",
                            "product_key": "proc-a::flow-diesel",
                            "sourceProcessUuid": "proc-a",
                            "sourceNodeId": "node-a",
                            "isProduct": True,
                        },
                        {
                            "id": "ptsout-b",
                            "flowUuid": "flow-diesel",
                            "name": "diesel",
                            "unit": "kg",
                            "amount": 1.0,
                            "type": "technosphere",
                            "direction": "output",
                            "product_key": "proc-b::flow-diesel",
                            "sourceProcessUuid": "proc-b",
                            "sourceNodeId": "node-b",
                            "isProduct": True,
                        },
                    ],
                    "emissions": [],
                },
                {
                    "id": "market",
                    "node_kind": "market_process",
                    "mode": "normalized",
                    "process_uuid": "market-1",
                    "name": "Market",
                    "location": "GLO",
                    "reference_product": "diesel",
                    "inputs": [
                        {
                            "id": "market-diesel-a",
                            "flowUuid": "flow-diesel",
                            "name": "diesel @ a",
                            "unit": "kg",
                            "amount": 0.5,
                            "type": "technosphere",
                            "direction": "input",
                        }
                    ],
                    "outputs": [
                        {
                            "id": "market-out",
                            "flowUuid": "flow-diesel",
                            "name": "diesel",
                            "unit": "kg",
                            "amount": 1.0,
                            "type": "technosphere",
                            "direction": "output",
                            "isProduct": True,
                        }
                    ],
                    "emissions": [],
                },
            ],
            "exchanges": [edge_payload],
        }
    )


def _compile_row_with_two_same_flow_vps() -> PtsCompileArtifact:
    return PtsCompileArtifact(
        project_id="project-1",
        pts_node_id="pts-node",
        pts_uuid="pts-1",
        graph_hash="hash-1",
        ok=True,
        artifact_json={
            "virtual_processes": [
                {
                    "process_uuid": "pts-1::product::proc-a::ptsout-a::flow-diesel",
                    "process_name": "diesel A",
                    "product_key": "proc-a::flow-diesel",
                    "source_process_uuid": "proc-a",
                    "source_node_id": "node-a",
                    "source_port_id": "internal-out-a",
                    "reference_product": {
                        "id": "out-a",
                        "flowUuid": "flow-diesel",
                        "name": "diesel",
                        "unit": "kg",
                        "amount": 1.0,
                    },
                    "elementary_flows": [],
                    "technosphere_inputs": [],
                },
                {
                    "process_uuid": "pts-1::product::proc-b::ptsout-b::flow-diesel",
                    "process_name": "diesel B",
                    "product_key": "proc-b::flow-diesel",
                    "source_process_uuid": "proc-b",
                    "source_node_id": "node-b",
                    "source_port_id": "internal-out-b",
                    "reference_product": {
                        "id": "out-b",
                        "flowUuid": "flow-diesel",
                        "name": "diesel",
                        "unit": "kg",
                        "amount": 1.0,
                    },
                    "elementary_flows": [],
                    "technosphere_inputs": [],
                },
            ]
        },
    )


def test_pts_flatten_uses_shell_output_port_to_select_provider_vp():
    flattened = build_flattened_graph_for_run_pts(
        graph=_pts_shell_graph("out:ptsout-b"),
        compile_rows=[_compile_row_with_two_same_flow_vps()],
    )

    assert len(flattened.exchanges) == 1
    source_node = next(node for node in flattened.nodes if node.id == flattened.exchanges[0].fromNode)
    assert source_node.process_uuid == "pts-1::product::proc-b::ptsout-b::flow-diesel"


def test_pts_flatten_rejects_ambiguous_same_flow_without_output_port():
    try:
        build_flattened_graph_for_run_pts(
            graph=_pts_shell_graph(None),
            compile_rows=[_compile_row_with_two_same_flow_vps()],
        )
    except HTTPException as exc:
        assert exc.status_code == 422
        assert exc.detail["code"] == "PTS_OUTPUT_PROVIDER_AMBIGUOUS"
    else:
        raise AssertionError("Expected ambiguous PTS output provider to be rejected")


def _market_provider_links_by_process(graph: HybridGraph) -> list[tuple[str, float]]:
    links: list[tuple[str, float]] = []
    nodes_by_id = {node.id: node for node in graph.nodes}
    for edge in graph.exchanges:
        if edge.toNode != "market":
            continue
        source_node = nodes_by_id[edge.fromNode]
        links.append((source_node.process_uuid, float(edge.consumerAmount or edge.amount or 0.0)))
    return sorted(links)


def _weighted_market_score(links: list[tuple[str, float]], scores: dict[str, float]) -> float:
    return sum(scores[process_uuid] * amount for process_uuid, amount in links)


def test_pts_packed_market_result_matches_unpacked_baseline_for_same_flow_providers():
    """Regression baseline for PTS合并测试.

    Market-process inputs may repeat the same flow UUID for different provider
    processes. PTS packaging must preserve the provider process identity through
    the shell output port, otherwise both market links collapse to the first VP
    and the weighted market result changes.
    """
    unpacked_links = sorted(
        [
            ("pts-1::product::proc-a::ptsout-a::flow-diesel", 0.5),
            ("pts-1::product::proc-b::ptsout-b::flow-diesel", 0.5),
        ]
    )
    packed_a = build_flattened_graph_for_run_pts(
        graph=_pts_shell_graph("out:ptsout-a"),
        compile_rows=[_compile_row_with_two_same_flow_vps()],
    )
    packed_b = build_flattened_graph_for_run_pts(
        graph=_pts_shell_graph("out:ptsout-b"),
        compile_rows=[_compile_row_with_two_same_flow_vps()],
    )
    packed_links = sorted(_market_provider_links_by_process(packed_a) + _market_provider_links_by_process(packed_b))

    provider_scores = {
        "pts-1::product::proc-a::ptsout-a::flow-diesel": 2.0,
        "pts-1::product::proc-b::ptsout-b::flow-diesel": 2.35,
    }
    assert packed_links == unpacked_links
    assert _weighted_market_score(packed_links, provider_scores) == _weighted_market_score(unpacked_links, provider_scores)
