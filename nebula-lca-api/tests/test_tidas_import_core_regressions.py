from app.services.tidas_import_core import (
    _extract_tidas_model_record,
    _infer_unit_defaults_from_flow_dataset,
    _normalize_exchange,
)


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
