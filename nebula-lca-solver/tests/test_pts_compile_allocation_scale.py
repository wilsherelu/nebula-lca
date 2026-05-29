import pytest

from app.core.pts_compile import compile_pts_from_payload


def test_pts_internal_matrix_scales_multi_product_provider_allocation():
    payload = {
        "pts_uuid": "pts-test",
        "pts_outputs": [
            {
                "flowUuid": "flow-final",
                "name": "final",
                "sourceProcessUuid": "consumer",
                "sourceNodeId": "consumer-node",
                "sourcePortId": "final-out",
            }
        ],
        "internal_nodes": [
            {
                "id": "provider-node",
                "process_uuid": "provider",
                "outputs": [
                    {
                        "id": "co-product-out",
                        "flowUuid": "flow-co-product",
                        "name": "co-product",
                        "type": "technosphere",
                        "isProduct": True,
                        "amount": 500.0,
                    },
                    {
                        "id": "oil-out",
                        "flowUuid": "flow-oil",
                        "name": "oil",
                        "type": "technosphere",
                        "isProduct": True,
                        "amount": 1.0,
                        "allocationFactor": 0.6,
                    },
                ],
                "emissions": [
                    {
                        "id": "co2",
                        "flowUuid": "flow-co2",
                        "name": "CO2",
                        "type": "biosphere",
                        "amount": 1.0,
                        "unit": "kg",
                    }
                ],
            },
            {
                "id": "consumer-node",
                "process_uuid": "consumer",
                "inputs": [
                    {
                        "id": "oil-in",
                        "flowUuid": "flow-oil",
                        "name": "oil",
                        "type": "technosphere",
                        "amount": 1.0,
                    }
                ],
                "outputs": [
                    {
                        "id": "final-out",
                        "flowUuid": "flow-final",
                        "name": "final",
                        "type": "technosphere",
                        "isProduct": True,
                        "amount": 1.0,
                    }
                ],
                "emissions": [],
            },
        ],
        "internal_edges": [
            {
                "fromNode": "provider-node",
                "toNode": "consumer-node",
                "sourceHandle": "out:oil-out",
                "targetHandle": "in:oil-in",
                "flowUuid": "flow-oil",
                "quantityMode": "single",
                "amount": 1.0,
                "consumerAmount": 1.0,
                "providerAmount": 1.0,
            }
        ],
    }

    result = compile_pts_from_payload(payload)
    final_vp = result["virtual_processes"][0]

    assert final_vp["reference_product"]["flowUuid"] == "flow-final"
    assert final_vp["elementary_flows"][0]["flowUuid"] == "flow-co2"
    assert final_vp["elementary_flows"][0]["amount"] == pytest.approx(0.6)
