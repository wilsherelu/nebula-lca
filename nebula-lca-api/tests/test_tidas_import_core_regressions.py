from app.services.tidas_import_core import (
    _build_tidas_graph_from_xflow_record,
    _extract_tidas_flow_record,
    _extract_tidas_model_record,
    _infer_unit_defaults_from_flow_dataset,
    _misclassified_elementary_port_uuids,
    _normalize_exchange,
)
from app.database import Base, SessionLocal, engine
from app.models import ReferenceProcess
from app.services.reference_catalog import (
    TidasAllocationImportError,
    _mark_reference_product_exchange,
    _materialize_process_exchanges_for_graph,
)
import pytest


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
        "json": {"lifeCycleModelDataSet": {
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
        }},
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


def test_xflow_reused_process_is_materialized_as_unique_node_instance() -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        source_uuid = "process-reused"
        source_json = {
            "process_uuid": source_uuid,
            "process_name": "Reused process",
            "exchanges": [],
        }
        db.add(ReferenceProcess(
            process_uuid=source_uuid,
            process_name="Reused process",
            process_type="unit_process",
            process_json=source_json,
            import_mode="locked",
        ))
        db.flush()

        graph, unresolved = _build_tidas_graph_from_xflow_record(
            db=db,
            model_record={
                "model_uuid": "model-reused",
                "model_name": "Repeated process model",
                "xflow_nodes": [
                    {"id": "0", "data": {"id": source_uuid}},
                    {"id": "2", "data": {"id": source_uuid}},
                ],
                "xflow_edges": [],
            },
            process_json_by_uuid={source_uuid: source_json},
            display_lang="en",
            allocation_policy="quantity",
        )

        assert graph is not None
        assert unresolved == []
        process_uuids = [node["process_uuid"] for node in graph["nodes"]]
        assert process_uuids[0] == source_uuid
        assert process_uuids[1] != source_uuid
        assert len(set(process_uuids)) == 2
        db.flush()
        alias = db.get(ReferenceProcess, process_uuids[1])
        assert alias is not None
        assert alias.source_process_uuid == source_uuid
        assert alias.process_json["source_process_uuid"] == source_uuid
    finally:
        db.rollback()
        db.close()


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


def _allocated_output(internal_id: str, flow_uuid: str, fraction: float, unit_group: str) -> dict:
    return {
        "exchange_internal_id": internal_id,
        "flow_uuid": flow_uuid,
        "direction": "output",
        "flow_type": "Product flow",
        "unit_group": unit_group,
        "amount": 10.0,
        "tidasAllocationPresent": True,
        "tidasAllocatedFraction": fraction,
    }


def test_normalize_exchange_preserves_standard_tidas_allocated_fraction() -> None:
    exchange = _normalize_exchange({
        "flow_uuid": "flow-1",
        "exchangeDirection": "Output",
        "allocations": {"allocation": {"@allocatedFraction": "25"}},
    })

    assert exchange["tidasAllocatedFraction"] == 25.0
    assert exchange["tidasAllocationPresent"] is True
    assert exchange["allocationFactor"] is None


def test_same_group_import_policy_selects_quantity_or_tidas_factors() -> None:
    for policy, expected_method in (("quantity", "quantity"), ("tidas", "manual_factor")):
        exchanges = [
            _allocated_output("1", "flow-a", 1, "Units of mass"),
            _allocated_output("2", "flow-b", 3, "Units of mass"),
        ]

        _mark_reference_product_exchange(
            process_uuid="process-1",
            process_json={"reference_flow_internal_id": "1"},
            exchanges=exchanges,
            allocation_policy=policy,
        )

        assert [row["isProduct"] for row in exchanges] == [True, True]
        assert {row["allocationBasis"]["method"] for row in exchanges} == {expected_method}
        if policy == "tidas":
            assert [row["allocationFactor"] for row in exchanges] == [0.25, 0.75]
        else:
            assert [row["allocationFactor"] for row in exchanges] == [None, None]


def test_graph_materialization_does_not_mutate_raw_process_exchanges() -> None:
    raw_exchanges = [
        _allocated_output("1", "flow-a", 1, "Units of mass"),
        _allocated_output("2", "flow-b", 3, "Units of mass"),
    ]

    graph_exchanges, reference_flow_uuid, _warnings = _materialize_process_exchanges_for_graph(
        process_uuid="process-1",
        process_json={"reference_flow_internal_id": "1"},
        exchanges=raw_exchanges,
        allocation_policy="tidas",
    )

    assert reference_flow_uuid == "flow-a"
    assert all("isProduct" not in row for row in raw_exchanges)
    assert all("allocationBasis" not in row for row in raw_exchanges)
    assert [row["isProduct"] for row in graph_exchanges] == [True, True]
    assert [row["allocationFactor"] for row in graph_exchanges] == [0.25, 0.75]


def test_cross_group_import_requires_complete_tidas_factors() -> None:
    exchanges = [
        _allocated_output("1", "flow-a", 2, "Units of mass"),
        _allocated_output("2", "flow-b", 3, "Units of energy"),
    ]

    _, warnings = _mark_reference_product_exchange(
        process_uuid="process-1",
        process_json={"reference_flow_internal_id": "1"},
        exchanges=exchanges,
        allocation_policy="quantity",
    )

    assert [row["allocationFactor"] for row in exchanges] == [0.4, 0.6]
    assert any("required automatically" in warning for warning in warnings)

    exchanges[1]["tidasAllocatedFraction"] = None
    with pytest.raises(TidasAllocationImportError):
        _mark_reference_product_exchange(
            process_uuid="process-1",
            process_json={"reference_flow_internal_id": "1"},
            exchanges=exchanges,
            allocation_policy="quantity",
        )


def test_allocated_non_product_exchange_is_not_defined_as_product() -> None:
    reference = _allocated_output("1", "flow-a", 1, "Units of mass")
    elementary = {
        "exchange_internal_id": "2",
        "flow_uuid": "flow-emission",
        "direction": "output",
        "flow_type": "Elementary flow",
        "unit_group": "Units of mass",
        "tidasAllocatedFraction": 1,
    }

    _mark_reference_product_exchange(
        process_uuid="process-1",
        process_json={"reference_flow_internal_id": "1"},
        exchanges=[reference, elementary],
        allocation_policy="tidas",
    )

    assert reference["isProduct"] is True
    assert elementary["isProduct"] is False
