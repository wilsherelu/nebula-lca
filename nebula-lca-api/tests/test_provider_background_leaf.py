from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
import app.services.provider_tidas_process_snapshot as process_snapshot_service
import app.services.provider_tidas_reference_snapshot as reference_snapshot_service
import app.services.provider_tidas_snapshot as flow_snapshot_service
from app.database import Base
from app.main import app
from app.lci_vector_codec import pack_lci_vector
from app.models import (
    FlowRecord,
    FlowVersionRecord,
    LciBiosphereFlowKey,
    LciProcessVector,
    Model,
    ReferenceProcess,
    UnitDefinition,
    UnitGroup,
)
from app.schemas import HybridEdge, HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph
from app.services.elementary_flow_conversion_service import get_elementary_flow_mapping_registry
from app.services.provider_mapped_background import (
    _inverse_elementary_mapping,
    _standardized_elementary,
    mapped_process_content_hash,
)
from app.services.public_flow_mapping_service import get_public_flow_mapping_registry


PROCESS_UUID = "22222222-2222-4222-8222-222222222222"
PROCESS_VERSION = "01.00.000"
QREF_UUID = "33333333-3333-4333-8333-333333333333"
QREF_VERSION = "01.00.000"
CO2_UUID = "08a91e70-3ddc-11dd-923d-0050c2490048"
NOX_UUID = "f79d0f8f-2b0e-49cb-bed0-b1ea0fbd8625"
SO2_UUID = "fe0acd60-3ddc-11dd-ac48-0050c2490048"
MASS_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
MASS_UNIT_GROUP_UUID = "93a60a57-a4c8-11da-a746-0800200c9a66"
ENERGY_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200c9a66"
ENERGY_UNIT_GROUP_UUID = "93a60a57-a3c8-11da-a746-0800200c9a66"
REFERENCE_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "provider_cases"
    / "tidas_reference_dependency"
    / "minimal_energy_dependency.json"
)


@pytest.fixture(autouse=True)
def isolated_database():
    previous = (
        flow_snapshot_service.settings.provider_tidas_flow_snapshot_path,
        process_snapshot_service.settings.provider_tidas_process_snapshot_path,
        reference_snapshot_service.settings.provider_tidas_reference_dependency_snapshot_path,
    )
    flow_snapshot_service.settings.provider_tidas_flow_snapshot_path = ""
    process_snapshot_service.settings.provider_tidas_process_snapshot_path = ""
    reference_snapshot_service.settings.provider_tidas_reference_dependency_snapshot_path = ""
    Base.metadata.drop_all(bind=db_module.engine)
    Base.metadata.create_all(bind=db_module.engine)
    yield
    db_module.engine.dispose()
    (
        flow_snapshot_service.settings.provider_tidas_flow_snapshot_path,
        process_snapshot_service.settings.provider_tidas_process_snapshot_path,
        reference_snapshot_service.settings.provider_tidas_reference_dependency_snapshot_path,
    ) = previous


@pytest.fixture()
def client():
    return TestClient(app)


def test_inverse_elementary_mapping_prefers_unique_strict_identity():
    resolution = _inverse_elementary_mapping("16fcd4f9-0bb8-4b84-8334-3ffb1a17aa74")
    assert resolution is not None
    assert resolution.mapping_level == "L1"
    assert resolution.tiangong_flow_uuid == "fe0acd60-3ddc-11dd-a6d5-0050c2490048"


def test_inverse_elementary_mapping_uses_certified_canonical_identity_for_l2_aliases():
    resolution = _inverse_elementary_mapping("99585564-bfce-4845-9aaa-2f24b8f26a41")
    assert resolution is not None
    assert resolution.mapping_level == "L2"
    assert resolution.tiangong_flow_uuid == "9ae45122-e251-11e6-bf01-fe55135034f3"
    assert resolution.amount_factor == 1.0


def test_inverse_elementary_mapping_does_not_guess_unreviewed_identity():
    assert _inverse_elementary_mapping("b32061b1-8d24-4663-b10d-0a1c705f2937") is None


def test_standardized_elementary_reports_certified_adjudication_dependency():
    key = LciBiosphereFlowKey(
        flow_uuid="99585564-bfce-4845-9aaa-2f24b8f26a41",
        compartment="air",
        subcompartment="non-urban air or from high stacks",
        direction="output",
        canonical_unit="kg",
    )
    _, amount, dependencies = _standardized_elementary(key, 1.0)
    certified = get_elementary_flow_mapping_registry()
    assert amount == pytest.approx(1.0)
    assert [item.role for item in dependencies] == [
        "public_interoperability_projection",
        "certified_elementary_identity_adjudication",
    ]
    assert dependencies[1].package_id == certified.package_id
    assert dependencies[1].package_version == certified.package_version
    assert dependencies[1].package_hash == certified.package_hash


def _hash(value) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _snapshot(kind: str, records: list[dict]) -> dict:
    records = sorted(records, key=lambda row: (row["source_object_id"], row["source_version"]))
    document = {
        "schema_version": "tiangong-open-dataset-snapshot.v1",
        "source_namespace": "tiangong_open_data",
        "dataset_kind": kind,
        "state_scope": "open",
        "filters": {
            "exact_refs": [
                f"{row['source_object_id']}@{row['source_version']}" for row in records
            ]
        },
        "declared_total": len(records),
        "records": records,
        "counts": {kind: len(records)},
    }
    for record in records:
        record["content_hash"] = _hash(record["payload"])
    body = {
        key: document[key]
        for key in (
            "schema_version",
            "source_namespace",
            "dataset_kind",
            "state_scope",
            "filters",
            "declared_total",
            "records",
        )
    }
    document["snapshot_hash"] = _hash(body)
    return document


def _flow_record(
    flow_uuid: str,
    version: str,
    flow_type: str,
    property_uuid: str,
    name: str,
    classification: list[str] | None = None,
) -> dict:
    data_information: dict = {
        "common:UUID": flow_uuid,
        "name": {"baseName": {"#text": name, "@xml:lang": "en"}},
    }
    if classification:
        data_information["classificationInformation"] = {
            "common:elementaryFlowCategorization": {
                "common:category": [
                    {"#text": item, "@level": str(index)}
                    for index, item in enumerate(classification)
                ]
            }
        }
    payload = {
        "flowDataSet": {
            "flowInformation": {
                "dataSetInformation": data_information,
                "quantitativeReference": {"referenceToReferenceFlowProperty": "0"},
            },
            "flowProperties": {
                "flowProperty": {
                    "@dataSetInternalID": "0",
                    "meanValue": "1",
                    "referenceToFlowPropertyDataSet": {
                        "@refObjectId": property_uuid,
                        "@version": "03.00.003",
                    },
                }
            },
            "modellingAndValidation": {"LCIMethod": {"typeOfDataSet": flow_type}},
        }
    }
    return {
        "dataset_kind": "flow",
        "source_namespace": "tiangong_open_data",
        "source_object_id": flow_uuid,
        "source_version": version,
        "source_modified_at": "2026-08-31T00:00:00+00:00",
        "type_of_data_set": flow_type,
        "payload": payload,
    }


def _process_record(*, technosphere_input: bool = False) -> dict:
    exchanges = [
        {
            "@dataSetInternalID": "1",
            "exchangeDirection": "Output",
            "meanAmount": "1.25",
            "referenceToFlowDataSet": {"@refObjectId": CO2_UUID, "@version": "03.00.004"},
        },
        {
            "@dataSetInternalID": "2",
            "exchangeDirection": "Output",
            "meanAmount": "3.6",
            "referenceToFlowDataSet": {"@refObjectId": QREF_UUID, "@version": QREF_VERSION},
        },
        {
            "@dataSetInternalID": "3",
            "exchangeDirection": "Output",
            "meanAmount": "0.5",
            "referenceToFlowDataSet": {"@refObjectId": NOX_UUID, "@version": "01.00.004"},
        },
        {
            "@dataSetInternalID": "4",
            "exchangeDirection": "Output",
            "meanAmount": "0.25",
            "referenceToFlowDataSet": {"@refObjectId": SO2_UUID, "@version": "03.00.004"},
        },
    ]
    if technosphere_input:
        exchanges.append(
            {
                "@dataSetInternalID": "5",
                "exchangeDirection": "Input",
                "meanAmount": "1",
                "referenceToFlowDataSet": {"@refObjectId": QREF_UUID, "@version": QREF_VERSION},
            }
        )
    payload = {
        "processDataSet": {
            "processInformation": {
                "dataSetInformation": {
                    "common:UUID": PROCESS_UUID,
                    "name": {"baseName": {"#text": "Closed background leaf", "@xml:lang": "en"}},
                },
                "quantitativeReference": {"referenceToReferenceFlow": "2"},
            },
            "exchanges": {"exchange": exchanges},
            "modellingAndValidation": {
                "LCIMethodAndAllocation": {"typeOfDataSet": "Partly terminated system"}
            },
        }
    }
    return {
        "dataset_kind": "process",
        "source_namespace": "tiangong_open_data",
        "source_object_id": PROCESS_UUID,
        "source_version": PROCESS_VERSION,
        "source_modified_at": "2026-08-31T00:00:00+00:00",
        "type_of_data_set": "Partly terminated system",
        "payload": payload,
    }


def _write(path: Path, value: dict) -> Path:
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return path


def _configure(monkeypatch, tmp_path: Path, *, technosphere_input: bool = False) -> tuple[dict, dict]:
    air = ["Emissions", "Emissions to air", "Emissions to air, unspecified"]
    flow_document = _snapshot(
        "flow",
        [
            _flow_record(CO2_UUID, "03.00.004", "Elementary flow", MASS_PROPERTY_UUID, "carbon dioxide (fossil)", air),
            _flow_record(QREF_UUID, QREF_VERSION, "Product flow", ENERGY_PROPERTY_UUID, "electricity"),
            _flow_record(NOX_UUID, "01.00.004", "Elementary flow", MASS_PROPERTY_UUID, "Nitrogen oxides", air),
            _flow_record(SO2_UUID, "03.00.004", "Elementary flow", MASS_PROPERTY_UUID, "sulfur dioxide", air),
        ],
    )
    process_document = _snapshot("process", [_process_record(technosphere_input=technosphere_input)])
    flow_path = _write(tmp_path / "flow-snapshot.json", flow_document)
    process_path = _write(tmp_path / "process-snapshot.json", process_document)
    monkeypatch.setattr(flow_snapshot_service.settings, "provider_tidas_flow_snapshot_path", str(flow_path))
    monkeypatch.setattr(process_snapshot_service.settings, "provider_tidas_process_snapshot_path", str(process_path))
    monkeypatch.setattr(
        reference_snapshot_service.settings,
        "provider_tidas_reference_dependency_snapshot_path",
        str(REFERENCE_FIXTURE),
    )
    return flow_document, process_document


def _port(port_id: str, flow_uuid: str, version: str, amount: float, direction: str, unit: str, *, product: bool = False) -> dict:
    energy = unit == "MJ"
    return {
        "id": port_id,
        "flowUuid": flow_uuid,
        "flowSourceNamespace": "tiangong_open_data" if energy else "casepack",
        "flowVersion": version,
        "flowPropertyUuid": ENERGY_PROPERTY_UUID if energy else MASS_PROPERTY_UUID,
        "flowPropertyVersion": "03.00.003",
        "unitGroupUuid": ENERGY_UNIT_GROUP_UUID if energy else MASS_UNIT_GROUP_UUID,
        "unitGroupVersion": "03.00.003",
        "name": port_id,
        "unit": unit,
        "unitGroup": "Units of energy" if energy else "Units of mass",
        "amount": amount,
        "type": "technosphere",
        "direction": direction,
        "isProduct": product,
    }


def _graph(input_amount: float) -> HybridGraph:
    return HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg product",
            "nodes": [
                {
                    "id": "consumer-node",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "consumer-process",
                    "name": "Consumer",
                    "location": "CN",
                    "reference_product": "product",
                    "inputs": [_port("electricity-in", QREF_UUID, QREF_VERSION, input_amount, "input", "MJ")],
                    "outputs": [_port("product-out", "case-product", "case-1", 1.0, "output", "kg", product=True)],
                }
            ],
            "exchanges": [],
            "metadata": {
                "functional_unit": {
                    "display_text": "1 kg product",
                    "amount": 1.0,
                    "flow_uuid": "case-product",
                    "flow_source_namespace": "casepack",
                    "flow_version": "case-1",
                    "unit": "kg",
                    "unit_group_uuid": MASS_UNIT_GROUP_UUID,
                    "unit_group_version": "03.00.003",
                }
            },
        }
    )


def _request(graph: HybridGraph, process_document: dict) -> dict:
    process_record = process_document["records"][0]
    return {
        "inline_snapshot": {
            "schema_version": "provider.snapshot.v1",
            "graph_hash": compute_graph_hash_from_graph(graph),
            "functional_unit": graph.metadata["functional_unit"],
            "graph": graph.model_dump(mode="json", by_alias=True),
            "source_policy": "open_mixed",
            "allowed_lcia_scope": "ef31_only",
        },
        "demand": [{"process_uuid": "consumer-process", "amount": 1.0, "unit": "kg"}],
        "lcia_methods": ["EF v3.1"],
        "background_process_pins": [
            {
                "consumer_exchange_id": "consumer-node::electricity-in",
                "source_namespace": "tiangong_open_data",
                "process_uuid": PROCESS_UUID,
                "version": PROCESS_VERSION,
                "expected_process_content_hash": process_record["content_hash"],
                "expected_process_snapshot_hash": process_document["snapshot_hash"],
                "quantitative_reference_exchange_internal_id": "2",
                "claim_role": "partial_background_leaf",
            }
        ],
    }


@pytest.mark.parametrize(("input_amount", "expected_scale"), [(3.6, 1.0), (7.2, 2.0)])
def test_closed_leaf_scales_exact_process_inventory_and_lcia(
    client,
    monkeypatch,
    tmp_path,
    input_amount,
    expected_scale,
):
    _, process_document = _configure(monkeypatch, tmp_path)
    graph = _graph(input_amount)
    response = client.post("/api/provider/v1/solve", json=_request(graph, process_document))
    assert response.status_code == 200, response.text
    body = response.json()
    receipt = body["background_process_receipts"][0]
    assert receipt["process_name"] == "Closed background leaf"
    assert receipt["claim_role"] == "partial_background_leaf"
    assert receipt["claim_limit"] == "partial_background_leaf_only_not_complete_cradle_to_gate"
    assert receipt["quantitative_reference_amount"] == pytest.approx(3.6)
    assert receipt["activity_amount"] == pytest.approx(input_amount)
    assert receipt["process_scale"] == pytest.approx(expected_scale)
    scaled_by_internal_id = {
        item["exchange_internal_id"]: item["scaled_amount"] for item in receipt["exchanges"]
    }
    assert scaled_by_internal_id == pytest.approx(
        {"1": 1.25 * expected_scale, "2": 3.6 * expected_scale, "3": 0.5 * expected_scale, "4": 0.25 * expected_scale}
    )
    names_by_internal_id = {
        item["exchange_internal_id"]: item["flow_name"] for item in receipt["exchanges"]
    }
    assert names_by_internal_id == {
        "1": "carbon dioxide (fossil)",
        "2": "electricity",
        "3": "Nitrogen oxides",
        "4": "sulfur dioxide",
    }
    assert body["lcia"]["method"] == "EF v3.1"
    assert body["lcia"]["indicator_results"]
    assert len(body["elementary_flow_receipts"]) == 3
    assert body["provenance"]["background_process_pins_hash"]
    assert body["provenance"]["background_claim_scope"] == receipt["claim_limit"]
    with db_module.SessionLocal() as db:
        counts = [
            db.query(model).count()
            for model in (ReferenceProcess, FlowRecord, FlowVersionRecord, UnitGroup, UnitDefinition, Model)
        ]
    assert counts == [0, 0, 0, 0, 0, 0]


def test_closed_leaf_rejects_process_hash_drift(client, monkeypatch, tmp_path):
    _, process_document = _configure(monkeypatch, tmp_path)
    graph = _graph(3.6)
    request = _request(graph, process_document)
    request["background_process_pins"][0]["expected_process_content_hash"] = "0" * 64
    response = client.post("/api/provider/v1/solve", json=request)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "BACKGROUND_PROCESS_CONTENT_HASH_MISMATCH"


def test_missing_display_names_are_null_without_changing_solve(client, monkeypatch, tmp_path):
    flow_document, process_document = _configure(monkeypatch, tmp_path)
    qref_record = next(
        row for row in flow_document["records"] if row["source_object_id"] == QREF_UUID
    )
    qref_record["payload"]["flowDataSet"]["flowInformation"]["dataSetInformation"].pop("name")
    flow_document = _snapshot("flow", flow_document["records"])
    flow_path = _write(tmp_path / "flow-without-name.json", flow_document)
    monkeypatch.setattr(
        flow_snapshot_service.settings,
        "provider_tidas_flow_snapshot_path",
        str(flow_path),
    )
    process_record = process_document["records"][0]
    process_record["payload"]["processDataSet"]["processInformation"]["dataSetInformation"].pop("name")
    process_document = _snapshot("process", process_document["records"])
    process_path = _write(tmp_path / "process-without-name.json", process_document)
    monkeypatch.setattr(
        process_snapshot_service.settings,
        "provider_tidas_process_snapshot_path",
        str(process_path),
    )
    graph = _graph(3.6)
    response = client.post("/api/provider/v1/solve", json=_request(graph, process_document))
    assert response.status_code == 200, response.text
    receipt = response.json()["background_process_receipts"][0]
    assert receipt["process_name"] is None
    qref = next(item for item in receipt["exchanges"] if item["role"] == "quantitative_reference")
    assert qref["flow_name"] is None
    assert receipt["activity_amount"] == pytest.approx(3.6)
    assert receipt["process_scale"] == pytest.approx(1.0)


def test_closed_leaf_rejects_qref_unit_drift(client, monkeypatch, tmp_path):
    _, process_document = _configure(monkeypatch, tmp_path)
    graph = _graph(3.6)
    graph.nodes[0].inputs[0].unit = "kWh"
    request = _request(graph, process_document)
    response = client.post("/api/provider/v1/solve", json=request)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_GRAPH_IDENTITY_MISMATCH"


def test_closed_leaf_rejects_technosphere_dependency(client, monkeypatch, tmp_path):
    _, process_document = _configure(monkeypatch, tmp_path, technosphere_input=True)
    graph = _graph(3.6)
    response = client.post("/api/provider/v1/solve", json=_request(graph, process_document))
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "BACKGROUND_PROCESS_NOT_CLOSED_LEAF"


def test_closed_leaf_rejects_duplicate_pin(client, monkeypatch, tmp_path):
    _, process_document = _configure(monkeypatch, tmp_path)
    graph = _graph(3.6)
    request = _request(graph, process_document)
    request["background_process_pins"].append(copy.deepcopy(request["background_process_pins"][0]))
    response = client.post("/api/provider/v1/solve", json=request)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "BACKGROUND_PROCESS_PIN_DUPLICATE"


@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    [
        ("expected_process_snapshot_hash", "0" * 64, "BACKGROUND_PROCESS_SNAPSHOT_HASH_MISMATCH"),
        ("quantitative_reference_exchange_internal_id", "9", "BACKGROUND_PROCESS_QREF_MISMATCH"),
    ],
)
def test_closed_leaf_rejects_snapshot_or_qref_pin_drift(
    client,
    monkeypatch,
    tmp_path,
    field,
    value,
    expected_code,
):
    _, process_document = _configure(monkeypatch, tmp_path)
    graph = _graph(3.6)
    request = _request(graph, process_document)
    request["background_process_pins"][0][field] = value
    response = client.post("/api/provider/v1/solve", json=request)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == expected_code


def test_closed_leaf_rejects_already_connected_consumer(client, monkeypatch, tmp_path):
    _, process_document = _configure(monkeypatch, tmp_path)
    graph = _graph(3.6)
    graph.nodes.append(
        graph.nodes[0].model_copy(
            deep=True,
            update={
                "id": "existing-provider-node",
                "process_uuid": "existing-provider-process",
                "name": "Existing provider",
                "inputs": [],
                "outputs": [
                    graph.nodes[0].inputs[0].model_copy(
                        deep=True,
                        update={"id": "electricity-out", "direction": "output", "isProduct": True},
                    )
                ],
            },
        )
    )
    graph.exchanges.append(
        HybridEdge.model_validate(
            {
            "id": "existing-edge",
            "fromNode": "existing-provider-node",
            "toNode": "consumer-node",
            "sourcePortId": "electricity-out",
            "targetPortId": "electricity-in",
            "sourceHandle": "out:electricity-out",
            "targetHandle": "in:electricity-in",
            "flowUuid": QREF_UUID,
            "flowName": "electricity",
            "quantityMode": "single",
            "amount": 3.6,
            "providerAmount": 3.6,
            "consumerAmount": 3.6,
            "unit": "MJ",
            "providerUnit": "MJ",
            "consumerUnit": "MJ",
            "type": "technosphere",
            }
        )
    )
    graph = HybridGraph.model_validate(graph.model_dump(mode="json", by_alias=True))
    response = client.post("/api/provider/v1/solve", json=_request(graph, process_document))
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "BACKGROUND_CONSUMER_ALREADY_CONNECTED"


def _second_process_record(*, includes_first: bool) -> dict:
    record = copy.deepcopy(_process_record())
    second_uuid = "44444444-4444-4444-8444-444444444444"
    record["source_object_id"] = second_uuid
    root = record["payload"]["processDataSet"]
    root["processInformation"]["dataSetInformation"]["common:UUID"] = second_uuid
    root["processInformation"]["dataSetInformation"]["name"]["baseName"]["#text"] = "Second leaf"
    if includes_first:
        root["processInformation"]["technology"] = {
            "referenceToIncludedProcesses": {
                "@refObjectId": PROCESS_UUID,
                "@version": PROCESS_VERSION,
            }
        }
    return record


def _two_pin_request(graph: HybridGraph, process_document: dict) -> dict:
    request = _request(graph, process_document)
    second = process_document["records"][1]
    second_pin = copy.deepcopy(request["background_process_pins"][0])
    second_pin.update(
        {
            "consumer_exchange_id": "consumer-node::electricity-in-2",
            "process_uuid": second["source_object_id"],
            "expected_process_content_hash": second["content_hash"],
        }
    )
    request["background_process_pins"].append(second_pin)
    return request


@pytest.mark.parametrize(
    ("includes_first", "expected_code"),
    [
        (False, "BACKGROUND_FLOW_MULTIPLE_PROVIDERS"),
        (True, "BACKGROUND_PROCESS_WRAPPER_DUPLICATE"),
    ],
)
def test_closed_leaf_rejects_multiple_provider_or_wrapper_duplicate(
    client,
    monkeypatch,
    tmp_path,
    includes_first,
    expected_code,
):
    _, process_document = _configure(monkeypatch, tmp_path)
    process_document = _snapshot(
        "process",
        [_process_record(), _second_process_record(includes_first=includes_first)],
    )
    process_path = _write(tmp_path / "two-processes.json", process_document)
    monkeypatch.setattr(
        process_snapshot_service.settings,
        "provider_tidas_process_snapshot_path",
        str(process_path),
    )
    graph = _graph(3.6)
    second_input = graph.nodes[0].inputs[0].model_copy(
        deep=True,
        update={"id": "electricity-in-2"},
    )
    graph.nodes[0].inputs.append(second_input)
    response = client.post(
        "/api/provider/v1/solve",
        json=_two_pin_request(graph, process_document),
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == expected_code


def test_solve_without_background_pin_remains_numerically_compatible(client, monkeypatch, tmp_path):
    _, process_document = _configure(monkeypatch, tmp_path)
    graph = _graph(3.6)
    request = _request(graph, process_document)
    request.pop("background_process_pins")
    request.pop("lcia_methods")
    response = client.post("/api/provider/v1/solve", json=request)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["background_process_receipts"] == []
    assert body["provenance"]["background_process_pins_hash"] is None
    assert body["activity_vector"][0]["process_uuid"] == "consumer-process"
    assert body["activity_vector"][0]["activity_amount"] == pytest.approx(1.0)


def test_mapped_background_pin_executes_local_vector_and_shared_consumers_with_frozen_mapping_evidence(
    client,
    monkeypatch,
    tmp_path,
):
    source_flow_uuid = "3d76981f-964a-4865-b588-0e067a2a1163"
    source_version = "01.01.001"
    target_flow_uuid = "759b89bd-3aa6-42ad-b767-5bb9ef5d331d"
    licensed_process_uuid = "44444444-4444-4444-8444-444444444444"
    licensed_co2_uuid = "349b29d1-3e58-4c66-98b9-9d1a076efd2e"
    flow_document = _snapshot(
        "flow",
        [
            _flow_record(
                source_flow_uuid,
                source_version,
                "Product flow",
                ENERGY_PROPERTY_UUID,
                "electricity, medium voltage",
            ),
            _flow_record(
                CO2_UUID,
                "03.00.004",
                "Elementary flow",
                MASS_PROPERTY_UUID,
                "carbon dioxide (fossil)",
                ["Emissions", "Emissions to air", "Emissions to air, unspecified"],
            ),
        ],
    )
    flow_path = _write(tmp_path / "mapped-flow-snapshot.json", flow_document)
    monkeypatch.setattr(flow_snapshot_service.settings, "provider_tidas_flow_snapshot_path", str(flow_path))
    monkeypatch.setattr(
        reference_snapshot_service.settings,
        "provider_tidas_reference_dependency_snapshot_path",
        str(REFERENCE_FIXTURE),
    )

    packed = pack_lci_vector([1], [0.5])
    with db_module.SessionLocal() as db:
        db.add(
            FlowRecord(
                flow_uuid=target_flow_uuid,
                flow_name="electricity, medium voltage",
                flow_type="product",
                default_unit="kWh",
                unit_group="Units of energy",
                source="ecoinvent",
                source_namespace="ecoinvent",
                source_version="3.11",
                is_custom=False,
            )
        )
        process = ReferenceProcess(
            process_uuid=licensed_process_uuid,
            process_name="synthetic licensed-vector fixture",
            process_type="lci_dataset",
            reference_flow_uuid=target_flow_uuid,
            process_json={
                "reference_product_amount": 1.0,
                "reference_product_unit": "kWh",
                "location": "CN",
            },
            source_file="synthetic-test://no-commercial-data",
        )
        vector = LciProcessVector(
            process_uuid=licensed_process_uuid,
            dataset_level="linked_lci",
            system_model="synthetic",
            nnz=packed.nnz,
            flow_key_ids_blob=packed.flow_key_ids_blob,
            amounts_blob=packed.amounts_blob,
            index_dtype=packed.index_dtype,
            amount_dtype=packed.amount_dtype,
            compression=packed.compression,
            canonicalized=True,
            checksum=packed.checksum,
            source="synthetic-test",
            source_package_version="3.11",
        )
        db.add(process)
        db.add(vector)
        db.add(
            LciBiosphereFlowKey(
                flow_key_id=1,
                flow_uuid=licensed_co2_uuid,
                compartment="Emissions to air, unspecified",
                subcompartment=None,
                direction="output",
                canonical_unit="kg",
                source="synthetic-test",
                source_package_version="3.11",
            )
        )
        db.commit()
        content_hash = mapped_process_content_hash(process, vector)

    graph = HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg product",
            "nodes": [
                {
                    "id": "consumer-node",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "consumer-process",
                    "name": "Consumer",
                    "location": "CN",
                    "reference_product": "product",
                    "inputs": [
                        _port(
                            "electricity-in",
                            source_flow_uuid,
                            source_version,
                            3.6,
                            "input",
                            "MJ",
                        )
                    ],
                    "outputs": [
                        _port("product-out", "case-product", "case-1", 1.0, "output", "kg", product=True)
                    ],
                }
            ],
            "exchanges": [],
            "metadata": {"functional_unit": {"display_text": "1 kg product", "amount": 1.0, "unit": "kg"}},
        }
    )
    mapping_registry = get_public_flow_mapping_registry()
    mapping = mapping_registry.resolve_intermediate(source_flow_uuid)
    assert mapping is not None
    request = {
        "inline_snapshot": {
            "schema_version": "provider.snapshot.v1",
            "graph_hash": compute_graph_hash_from_graph(graph),
            "functional_unit": graph.metadata["functional_unit"],
            "graph": graph.model_dump(mode="json", by_alias=True),
            "source_policy": "open_mixed",
            "allowed_lcia_scope": "ef31_only",
        },
        "demand": [{"process_uuid": "consumer-process", "amount": 1.0, "unit": "kg"}],
        "lcia_methods": ["EF v3.1"],
        "mapped_background_process_pins": [
            {
                "consumer_exchange_id": "consumer-node::electricity-in",
                "source_flow_namespace": "tiangong_open_data",
                "source_flow_uuid": source_flow_uuid,
                "source_flow_version": source_version,
                "target_flow_uuid": target_flow_uuid,
                "source_unit": "MJ",
                "target_unit": "kWh",
                "amount_factor": mapping.amount_factor,
                "mapping_level": mapping.mapping_level,
                "mapping_status": "user_confirmed",
                "mapping_rule_id": f"nebula-flow-mapping-v1:intermediate:{source_flow_uuid}",
                "mapping_package_id": mapping_registry.package_id,
                "mapping_package_version": mapping_registry.package_version,
                "mapping_package_hash": mapping_registry.package_hash,
                "database_release": "3.11",
                "process_uuid": licensed_process_uuid,
                "expected_process_content_hash": content_hash,
                "expected_vector_checksum": packed.checksum,
            }
        ],
    }
    response = client.post("/api/provider/v1/solve", json=request)
    assert response.status_code == 200, response.text
    body = response.json()
    receipt = body["mapped_background_process_receipts"][0]
    assert receipt["database_release"] == "3.11"
    assert receipt["mapping_package_hash"] == mapping_registry.package_hash
    assert receipt["elementary_mapping_dependencies"] == [
        {
            "package_id": mapping_registry.package_id,
            "package_version": mapping_registry.package_version,
            "package_hash": mapping_registry.package_hash,
            "role": "public_interoperability_projection",
        }
    ]
    assert receipt["vector_checksum"] == packed.checksum
    assert receipt["process_content_hash"] == content_hash
    assert receipt["activity_amount"] == pytest.approx(1.0)
    assert receipt["consumer_attributed_activity_amount"] == pytest.approx(1.0)
    assert receipt["shared_process_total_activity_amount"] == pytest.approx(1.0)
    assert receipt["consumer_process_uuid"] == "consumer-process"
    assert receipt["provider_instance_id"]
    assert receipt["shared_activity_schema_version"] == "provider.mapped-background-shared-activity.v1"
    assert body["provenance"]["database_release"] == "3.11"
    assert body["provenance"]["mapped_background_process_pins_hash"]
    assert body["provenance"]["elementary_mapping_dependencies"] == receipt[
        "elementary_mapping_dependencies"
    ]
    assert body["lcia"]["indicator_results"]
    co2 = next(item for item in body["inventory_totals"] if item["flow_uuid"] == CO2_UUID)
    assert co2["amount"] == pytest.approx(0.5)

    shared_graph = HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg product A + 1 kg product B",
            "nodes": [
                {
                    "id": "consumer-a",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "consumer-process-a",
                    "name": "Consumer A",
                    "location": "CN",
                    "reference_product": "product A",
                    "inputs": [
                        _port("electricity-in", source_flow_uuid, source_version, 3.6, "input", "MJ")
                    ],
                    "outputs": [
                        _port("product-out", "case-product-a", "case-1", 1.0, "output", "kg", product=True)
                    ],
                },
                {
                    "id": "consumer-b",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "consumer-process-b",
                    "name": "Consumer B",
                    "location": "CN",
                    "reference_product": "product B",
                    "inputs": [
                        _port("electricity-in", source_flow_uuid, source_version, 7.2, "input", "MJ")
                    ],
                    "outputs": [
                        _port("product-out", "case-product-b", "case-1", 1.0, "output", "kg", product=True)
                    ],
                },
            ],
            "exchanges": [],
            "metadata": {
                "functional_unit": {
                    "display_text": "1 kg product A + 1 kg product B",
                    "amount": 2.0,
                    "unit": "kg",
                }
            },
        }
    )
    shared_pin = dict(request["mapped_background_process_pins"][0])
    shared_request = {
        "inline_snapshot": {
            "schema_version": "provider.snapshot.v1",
            "graph_hash": compute_graph_hash_from_graph(shared_graph),
            "functional_unit": shared_graph.metadata["functional_unit"],
            "graph": shared_graph.model_dump(mode="json", by_alias=True),
            "source_policy": "open_mixed",
            "allowed_lcia_scope": "ef31_only",
        },
        "demand": [
            {"process_uuid": "consumer-process-a", "amount": 1.0, "unit": "kg"},
            {"process_uuid": "consumer-process-b", "amount": 1.0, "unit": "kg"},
        ],
        "lcia_methods": ["EF v3.1"],
        "mapped_background_process_pins": [
            {**shared_pin, "consumer_exchange_id": "consumer-a::electricity-in"},
            {**shared_pin, "consumer_exchange_id": "consumer-b::electricity-in"},
        ],
    }
    shared_response = client.post("/api/provider/v1/solve", json=shared_request)
    assert shared_response.status_code == 200, shared_response.text
    shared_body = shared_response.json()
    provider_activities = [
        item for item in shared_body["activity_vector"] if item["process_uuid"] == licensed_process_uuid
    ]
    assert len(provider_activities) == 1
    assert provider_activities[0]["activity_amount"] == pytest.approx(3.0)
    shared_receipts = shared_body["mapped_background_process_receipts"]
    assert len(shared_receipts) == 2
    assert all(
        item["shared_activity_schema_version"] == "provider.mapped-background-shared-activity.v1"
        for item in shared_receipts
    )
    assert len({item["provider_instance_id"] for item in shared_receipts}) == 1
    assert {item["consumer_process_uuid"] for item in shared_receipts} == {
        "consumer-process-a",
        "consumer-process-b",
    }
    assert sorted(item["consumer_attributed_activity_amount"] for item in shared_receipts) == pytest.approx([1.0, 2.0])
    assert all(item["shared_process_total_activity_amount"] == pytest.approx(3.0) for item in shared_receipts)
    assert sum(item["consumer_attributed_activity_amount"] for item in shared_receipts) == pytest.approx(3.0)
    shared_co2 = next(item for item in shared_body["inventory_totals"] if item["flow_uuid"] == CO2_UUID)
    assert shared_co2["amount"] == pytest.approx(1.5)

    # R3 path-weighted contribution view (extends the versioned public contract): per-characterized-
    # term split by consumption path, reconciled to the source totals and never double-counting
    # foreground with background. This is HTTP-level acceptance, not a mock-only check.
    path_receipts = shared_body["lcia"]["path_weighted_contribution_receipts"]
    indicator_receipts = shared_body["lcia"]["indicator_contribution_receipts"]
    assert len(path_receipts) == len(indicator_receipts)
    for indicator_receipt, path_receipt in zip(indicator_receipts, path_receipts, strict=True):
        # Per-indicator reconciliation: Σ path == Σ term (no double count, no dropped contribution).
        assert path_receipt["reconciliation_delta"] == pytest.approx(0.0, abs=1e-9)
        assert path_receipt["path_weighted_total"] == pytest.approx(
            indicator_receipt["contribution_total"], rel=1e-9, abs=1e-9
        )
        per_process_term: dict[str, float] = {}
        for term in indicator_receipt["terms"]:
            per_process_term[term["process_uuid"]] = (
                per_process_term.get(term["process_uuid"], 0.0) + float(term["contribution_value"])
            )
        per_process_path: dict[str, float] = {}
        for row in path_receipt["terms"]:
            per_process_path[row["process_uuid"]] = (
                per_process_path.get(row["process_uuid"], 0.0) + float(row["path_contribution_value"])
            )
        assert set(per_process_path) == set(per_process_term)
        for process_uuid, expected in per_process_term.items():
            assert per_process_path[process_uuid] == pytest.approx(expected, rel=1e-9, abs=1e-9)
    # The shared background's split across the two consumers uses the exact dimensionless shares
    # (1/3 and 2/3), carries the consumer process instance per row, and preserves source evidence.
    primary_path = next(item for item in path_receipts if item["canonical_indicator_key"] == "climate change")
    bg_rows = [
        row for row in primary_path["terms"] if row["process_uuid"] == licensed_process_uuid
    ]
    assert bg_rows, "the shared background process must appear in the path-weighted receipt"
    assert {row["consumer_exchange_id"] for row in bg_rows} == {
        "consumer-a::electricity-in",
        "consumer-b::electricity-in",
    }
    assert sorted(row["path_share"] for row in bg_rows) == pytest.approx([1.0 / 3.0, 2.0 / 3.0])
    assert {row["consumer_process_uuid"] for row in bg_rows} == {
        "consumer-process-a",
        "consumer-process-b",
    }
    assert all(row["attribution_basis"] == "background_consumer_path" for row in bg_rows)
    assert all(row["solve_hash"] == shared_body["lcia"]["solve_hash"] for row in bg_rows)
    # Attribution for the shared background is complete → no unattributed remainder rows.
    assert primary_path["attribution_coverage"] == "complete"
    assert primary_path["unattributed_contribution_value"] == pytest.approx(0.0, abs=1e-9)

    # A 1 kg product demand uses 1/1000 of each foreground process. The mapped
    # supply is measured in kWh, while both consumer ports remain measured in MJ.
    chain_graph = HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg product B",
            "nodes": [
                {
                    "id": "consumer-a",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "consumer-process-a",
                    "name": "Consumer A",
                    "location": "CN",
                    "reference_product": "intermediate",
                    "inputs": [_port("electricity-in", source_flow_uuid, source_version, 600.0, "input", "MJ")],
                    "outputs": [_port("intermediate-out", "case-intermediate", "case-1", 1000.0, "output", "kg", product=True)],
                },
                {
                    "id": "consumer-b",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "consumer-process-b",
                    "name": "Consumer B",
                    "location": "CN",
                    "reference_product": "product B",
                    "inputs": [
                        _port("intermediate-in", "case-intermediate", "case-1", 1000.0, "input", "kg"),
                        _port("electricity-in", source_flow_uuid, source_version, 25.0, "input", "MJ"),
                    ],
                    "outputs": [_port("product-out", "case-product-b", "case-1", 1000.0, "output", "kg", product=True)],
                },
            ],
            "exchanges": [
                {
                    "id": "foreground-dual-link",
                    "fromNode": "consumer-a",
                    "toNode": "consumer-b",
                    "sourcePortId": "intermediate-out",
                    "targetPortId": "intermediate-in",
                    "flowUuid": "case-intermediate",
                    "flowName": "intermediate",
                    "quantityMode": "dual",
                    "amount": 1000.0,
                    "providerAmount": 1000.0,
                    "consumerAmount": 1000.0,
                    "unit": "kg",
                    "type": "technosphere",
                }
            ],
            "metadata": {"functional_unit": {"display_text": "1 kg product B", "amount": 1.0, "unit": "kg"}},
        }
    )
    chain_request = {
        **shared_request,
        "inline_snapshot": {
            **shared_request["inline_snapshot"],
            "graph_hash": compute_graph_hash_from_graph(chain_graph),
            "functional_unit": chain_graph.metadata["functional_unit"],
            "graph": chain_graph.model_dump(mode="json", by_alias=True),
        },
        "demand": [{"process_uuid": "consumer-process-b", "amount": 1.0, "unit": "kg"}],
    }
    chain_response = client.post("/api/provider/v1/solve", json=chain_request)
    assert chain_response.status_code == 200, chain_response.text
    chain = chain_response.json()
    scaled = {item["exchange_id"]: item for item in chain["scaled_exchanges"]}
    assert scaled["consumer-a::electricity-in"]["raw_amount"] == pytest.approx(600.0)
    assert scaled["consumer-a::electricity-in"]["scaled_amount"] == pytest.approx(0.6)
    assert scaled["consumer-a::electricity-in"]["unit"] == "MJ"
    assert scaled["consumer-b::electricity-in"]["raw_amount"] == pytest.approx(25.0)
    assert scaled["consumer-b::electricity-in"]["scaled_amount"] == pytest.approx(0.025)
    assert scaled["consumer-b::electricity-in"]["unit"] == "MJ"
    assert scaled["consumer-b::intermediate-in"]["raw_amount"] == pytest.approx(1000.0)
    assert scaled["consumer-b::intermediate-in"]["scaled_amount"] == pytest.approx(1.0)
    assert scaled["consumer-b::intermediate-in"]["unit"] == "kg"
    provider_activity = next(item for item in chain["activity_vector"] if item["process_uuid"] == licensed_process_uuid)
    assert provider_activity["activity_amount"] == pytest.approx(0.1736111111111111)
    assert provider_activity["unit"] == "kWh"
    receipts = {item["consumer_exchange_id"]: item for item in chain["mapped_background_process_receipts"]}
    assert receipts["consumer-a::electricity-in"]["consumer_converted_amount"] == pytest.approx(166.66666666666666)
    assert receipts["consumer-b::electricity-in"]["consumer_converted_amount"] == pytest.approx(6.944444444444445)
    assert receipts["consumer-a::electricity-in"]["consumer_attributed_activity_amount"] == pytest.approx(0.16666666666666666)
    assert receipts["consumer-b::electricity-in"]["consumer_attributed_activity_amount"] == pytest.approx(0.006944444444444445)


def test_mapped_background_pin_rejects_mapping_hash_drift(client, monkeypatch, tmp_path):
    # Request validation reaches the mapping evidence check before any licensed Process lookup.
    source_flow_uuid = "3d76981f-964a-4865-b588-0e067a2a1163"
    source_version = "01.01.001"
    target_flow_uuid = "759b89bd-3aa6-42ad-b767-5bb9ef5d331d"
    flow_document = _snapshot(
        "flow",
        [_flow_record(source_flow_uuid, source_version, "Product flow", ENERGY_PROPERTY_UUID, "electricity")],
    )
    flow_path = _write(tmp_path / "mapped-flow-snapshot.json", flow_document)
    monkeypatch.setattr(flow_snapshot_service.settings, "provider_tidas_flow_snapshot_path", str(flow_path))
    monkeypatch.setattr(
        reference_snapshot_service.settings,
        "provider_tidas_reference_dependency_snapshot_path",
        str(REFERENCE_FIXTURE),
    )
    graph = _graph(3.6)
    graph.nodes[0].inputs[0].flowUuid = source_flow_uuid
    graph.nodes[0].inputs[0].flow_version = source_version
    registry = get_public_flow_mapping_registry()
    mapping = registry.resolve_intermediate(source_flow_uuid)
    request = {
        "inline_snapshot": {
            "graph_hash": compute_graph_hash_from_graph(graph),
            "functional_unit": graph.metadata["functional_unit"],
            "graph": graph.model_dump(mode="json", by_alias=True),
        },
        "demand": [{"process_uuid": "consumer-process", "amount": 1.0, "unit": "kg"}],
        "mapped_background_process_pins": [
            {
                "consumer_exchange_id": "consumer-node::electricity-in",
                "source_flow_namespace": "tiangong_open_data",
                "source_flow_uuid": source_flow_uuid,
                "source_flow_version": source_version,
                "target_flow_uuid": target_flow_uuid,
                "source_unit": "MJ",
                "target_unit": "kWh",
                "amount_factor": mapping.amount_factor,
                "mapping_level": mapping.mapping_level,
                "mapping_status": "user_confirmed",
                "mapping_rule_id": f"nebula-flow-mapping-v1:intermediate:{source_flow_uuid}",
                "mapping_package_id": registry.package_id,
                "mapping_package_version": registry.package_version,
                "mapping_package_hash": "0" * 64,
                "database_release": "3.11",
                "process_uuid": "44444444-4444-4444-8444-444444444444",
                "expected_process_content_hash": "1" * 64,
                "expected_vector_checksum": "2" * 64,
            }
        ],
    }
    response = client.post("/api/provider/v1/solve", json=request)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "MAPPED_BACKGROUND_MAPPING_EVIDENCE_MISMATCH"
