from app.services.tidas_import_core import (
    _extract_tidas_flow_record,
    _extract_tidas_model_record,
    _infer_unit_defaults_from_flow_dataset,
    _misclassified_elementary_port_uuids,
    _normalize_exchange,
)


def _flow_row(flow_type: str, classification: dict) -> dict:
    return {
        "flowDataSet": {
            "flowInformation": {
                "dataSetInformation": {
                    "common:UUID": f"flow-{flow_type}",
                    "name": {"baseName": {"#text": flow_type, "@xml:lang": "en"}},
                },
                "classificationInformation": classification,
            },
            "modellingAndValidation": {"LCIMethod": {"typeOfDataSet": flow_type}},
        }
    }


def test_extract_flow_record_uses_authoritative_ilcd_dataset_type() -> None:
    elementary, error = _extract_tidas_flow_record(_flow_row(
        "Elementary flow",
        {
            "common:elementaryFlowCategorization": {
                "common:category": [{"#text": "Emissions"}, {"#text": "Emissions to air"}]
            }
        },
    ))
    product, product_error = _extract_tidas_flow_record(_flow_row(
        "Product flow",
        {"common:classification": {"common:class": {"#text": "Energy carriers"}}},
    ))

    assert error is None
    assert elementary is not None
    assert elementary["flow_type"] == "Elementary flow"
    assert elementary["compartment"] == "Emissions;Emissions to air"
    assert product_error is None
    assert product is not None
    assert product["flow_type"] == "Product flow"


def test_import_contract_detects_elementary_flow_emitted_as_technosphere() -> None:
    graph = {
        "nodes": [{
            "inputs": [{"flowUuid": "elementary-1", "type": "technosphere"}],
            "outputs": [{"flowUuid": "product-1", "type": "technosphere"}],
        }]
    }

    assert _misclassified_elementary_port_uuids(graph, {"elementary-1"}) == ["elementary-1"]


def test_extract_model_record_reads_standard_dataset_and_xflow() -> None:
    row = {
        "lifeCycleModelDataSet": {
            "lifeCycleModelInformation": {
                "dataSetInformation": {
                    "common:UUID": "model-1",
                    "name": {"baseName": {"#text": "模型", "@xml:lang": "zh"}},
                },
                "technology": {
                    "processes": {
                        "processInstance": {
                            "referenceToProcess": {"@refObjectId": "process-1"}
                        }
                    }
                },
            }
        },
        "json_tg": {
            "xflow": {
                "nodes": [{"id": "node-1", "data": {"id": "process-1"}}],
                "edges": [{"id": "edge-1"}],
            }
        },
    }

    record, error = _extract_tidas_model_record(row)

    assert error is None
    assert record is not None
    assert record["model_uuid"] == "model-1"
    assert record["model_name"] == "模型"
    assert record["process_refs"] == ["process-1"]
    assert record["topology_empty"] is False
    assert len(record["xflow_nodes"]) == 1


def test_extract_model_record_preserves_standard_ilcd_connections() -> None:
    row = {
        "lifeCycleModelDataSet": {
            "lifeCycleModelInformation": {
                "dataSetInformation": {
                    "common:UUID": "model-standard",
                    "name": {"baseName": {"#text": "Standard model", "@xml:lang": "en"}},
                },
                "technology": {
                    "processes": {
                        "processInstance": [
                            {
                                "@dataSetInternalID": "0",
                                "referenceToProcess": {"@refObjectId": "process-a"},
                                "connections": {
                                    "outputExchange": {
                                        "@flowUUID": "flow-a",
                                        "downstreamProcess": {"@id": "1", "@flowUUID": "flow-a"},
                                    }
                                },
                            },
                            {
                                "@dataSetInternalID": "1",
                                "referenceToProcess": {"@refObjectId": "process-b"},
                            },
                        ]
                    }
                },
            }
        }
    }

    record, error = _extract_tidas_model_record(row)

    assert error is None
    assert record is not None
    assert record["topology_empty"] is False
    assert record["model_instances"][0]["output_connections"] == [{
        "flow_uuid": "flow-a",
        "downstream_instance_id": "1",
        "downstream_flow_uuid": "flow-a",
    }]


def test_calorific_flow_property_resolves_to_energy_default() -> None:
    flow_dataset = {
        "flowProperties": {
            "flowProperty": {
                "referenceToFlowPropertyDataSet": {
                    "common:shortDescription": {
                        "#text": "Net calorific value",
                        "@xml:lang": "en",
                    }
                }
            }
        }
    }

    assert _infer_unit_defaults_from_flow_dataset(flow_dataset) == ("MJ", "Units of energy")


def test_normalize_exchange_uses_only_explicit_allocation_factor() -> None:
    reference = _normalize_exchange({
        "flow_uuid": "flow-1",
        "direction": "output",
        "is_reference_flow": True,
    })
    allocated = _normalize_exchange({
        "flow_uuid": "flow-2",
        "direction": "output",
        "allocationFactor": 0.25,
    })
    marker_only = _normalize_exchange({
        "flow_uuid": "flow-3",
        "direction": "output",
        "productOutput": True,
    })

    assert reference["isProduct"] is True
    assert allocated["isProduct"] is True
    assert allocated["allocationFactor"] == 0.25
    assert marker_only["isProduct"] is False
