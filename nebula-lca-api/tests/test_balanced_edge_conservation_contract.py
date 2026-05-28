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
