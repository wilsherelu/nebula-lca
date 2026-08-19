import pytest
from fastapi import HTTPException

from app.schemas import HybridGraph
from app.services.graph_contract import validate_graph_contract


def _balanced_graph(*, provider_amount=3, consumer_amount=3, provider_switch=None):
    provider_port = {
        "id": "out-gas",
        "flowUuid": "gas-flow",
        "name": "gas",
        "unit": "kg",
        "unitGroup": "mass",
        "amount": provider_amount,
        "type": "technosphere",
        "direction": "output",
        "isProduct": True,
    }
    if provider_switch:
        provider_port["unitGroupSwitch"] = provider_switch
    return {
        "functionalUnit": "1 unit",
        "nodes": [
            {
                "id": "provider",
                "node_kind": "unit_process",
                "mode": "balanced",
                "process_uuid": "provider",
                "name": "provider",
                "location": "GLO",
                "reference_product": "gas",
                "inputs": [],
                "outputs": [provider_port],
            },
            {
                "id": "consumer",
                "node_kind": "unit_process",
                "mode": "balanced",
                "process_uuid": "consumer",
                "name": "consumer",
                "location": "GLO",
                "reference_product": "product",
                "inputs": [
                    {
                        "id": "in-gas",
                        "flowUuid": "gas-flow",
                        "name": "gas",
                        "unit": "m3",
                        "unitGroup": "volume",
                        "amount": consumer_amount,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [],
            },
        ],
        "exchanges": [
            {
                "id": "edge-gas",
                "fromNode": "provider",
                "toNode": "consumer",
                "sourceHandle": "out:out-gas",
                "targetHandle": "in:in-gas",
                "flowUuid": "gas-flow",
                "flowName": "gas",
                "quantityMode": "single",
                "amount": 3,
                "providerAmount": 3,
                "consumerAmount": 3,
                "unit": "kg",
                "type": "technosphere",
            }
        ],
        "metadata": {},
    }


def test_graph_contract_rejects_edge_between_different_flow_versions():
    graph = _balanced_graph()
    graph["nodes"][0]["outputs"][0].update({
        "flowSourceNamespace": "tiangong_open_data",
        "flowVersion": "01.01.002",
    })
    graph["nodes"][1]["inputs"][0].update({
        "flowSourceNamespace": "tiangong_open_data",
        "flowVersion": "01.01.003",
    })

    with pytest.raises(HTTPException) as error:
        validate_graph_contract(HybridGraph.model_validate(graph))

    assert error.value.detail["code"] == "INVALID_EDGE_PORT_BINDING"
    assert "incompatible Flow versions" in error.value.detail["evidence"][0]["issues"][0]


def _same_name_graph(node_kind: str):
    return HybridGraph.model_validate({
        "functionalUnit": "1 unit",
        "nodes": [
            {
                "id": "first",
                "node_kind": node_kind,
                "mode": "balanced",
                "process_uuid": "process-first",
                "name": "market for diesel",
                "location": "GLO",
                "reference_product": "diesel",
                "inputs": [],
                "outputs": [],
            },
            {
                "id": "second",
                "node_kind": node_kind,
                "mode": "balanced",
                "process_uuid": "process-second",
                "name": "market for diesel",
                "location": "RER",
                "reference_product": "diesel",
                "inputs": [],
                "outputs": [],
            },
        ],
        "exchanges": [],
    })


def test_lci_datasets_may_share_display_name_when_process_uuids_differ():
    validate_graph_contract(_same_name_graph("lci_dataset"))


def test_run_contract_rejects_duplicate_foreground_process_names():
    with pytest.raises(HTTPException) as exc:
        validate_graph_contract(_same_name_graph("unit_process"))

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "DUPLICATE_PROCESS_NAME"


def test_balanced_single_edge_rejects_non_conserved_flow_basis_amounts():
    graph = HybridGraph.model_validate(_balanced_graph(provider_switch={"targetUnitGroup": "mass", "targetUnit": "kg", "factor": 0.75}))

    with pytest.raises(HTTPException) as exc_info:
        validate_graph_contract(graph, require_balanced_conservation=True)

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "BALANCED_FLOW_CONSERVATION_MISMATCH"


def test_balanced_single_edge_accepts_unit_switch_when_flow_basis_conserves():
    graph = HybridGraph.model_validate(
        _balanced_graph(provider_amount=2.25, consumer_amount=3, provider_switch={"targetUnitGroup": "mass", "targetUnit": "kg", "factor": 0.75})
    )

    validate_graph_contract(graph, require_balanced_conservation=True)
