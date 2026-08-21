from app.schemas import HybridGraph
from app.services.calculation_integrity import collect_unlinked_positive_technosphere_inputs


def _graph() -> HybridGraph:
    return HybridGraph.model_validate({
        "functionalUnit": "1 kg",
        "nodes": [
            {
                "id": "provider",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "provider-process",
                "name": "Provider",
                "location": "CN",
                "reference_product": "linked",
                "inputs": [],
                "outputs": [{
                    "id": "linked-out", "flowUuid": "linked-flow", "name": "Linked",
                    "unit": "kg", "amount": 1, "type": "technosphere", "direction": "output",
                    "isProduct": True,
                }],
            },
            {
                "id": "consumer",
                "node_kind": "unit_process",
                "mode": "balanced",
                "process_uuid": "consumer-process",
                "name": "Consumer",
                "location": "CN",
                "reference_product": "result",
                "inputs": [
                    {"id": "linked-in", "flowUuid": "linked-flow", "name": "Linked", "unit": "kg", "amount": 2, "type": "technosphere", "direction": "input"},
                    {"id": "missing-in", "flowUuid": "missing-flow", "name": "Missing", "unit": "kg", "amount": 3, "type": "technosphere", "direction": "input"},
                    {"id": "zero-in", "flowUuid": "zero-flow", "name": "Zero", "unit": "kg", "amount": 0, "type": "technosphere", "direction": "input"},
                    {"id": "bio-in", "flowUuid": "bio-flow", "name": "Resource", "unit": "kg", "amount": 4, "type": "biosphere", "direction": "input"},
                ],
                "outputs": [{
                    "id": "result-out", "flowUuid": "result-flow", "name": "Result",
                    "unit": "kg", "amount": 1, "type": "technosphere", "direction": "output",
                    "isProduct": True,
                }],
            },
        ],
        "exchanges": [{
            "id": "edge", "fromNode": "provider", "toNode": "consumer",
            "sourceHandle": "out:linked-out", "targetHandle": "in:linked-in",
            "flowUuid": "linked-flow", "flowName": "Linked", "amount": 2,
            "unit": "kg", "type": "technosphere", "allocation": "none", "quantityMode": "single",
        }],
    })


def test_only_positive_unlinked_technosphere_inputs_are_reported():
    issues = collect_unlinked_positive_technosphere_inputs(_graph())

    assert len(issues) == 1
    assert issues[0]["port_id"] == "missing-in"
    assert issues[0]["amount"] == 3
