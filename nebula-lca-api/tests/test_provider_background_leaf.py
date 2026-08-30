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
from app.models import FlowRecord, FlowVersionRecord, Model, ReferenceProcess, UnitDefinition, UnitGroup
from app.schemas import HybridEdge, HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph


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
