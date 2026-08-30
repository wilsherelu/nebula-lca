from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
import app.services.provider_tidas_process_snapshot as process_snapshot_service
from app.database import Base
from app.main import app
from app.models import Model, ModelVersion
from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph


PROCESS_UUID = "f784e009-504d-4422-9e7d-915394e5cdbd"
PROCESS_VERSION = "01.01.000"
PROCESS_CONTENT_HASH = "f74371b38d8ac86ecb67b2c98046bde0e4427494cc941003a0c78cfc938b4a04"
PROCESS_SNAPSHOT_HASH = "02db4261119b36bfec3f94e1de47d8593a9bf5f10b2767a7621ce4978199d18b"
INPUT_FLOW_UUID = "e8186909-07ca-41ef-9f5f-0ba05982720a"
OUTPUT_FLOW_UUID = "b7ce9d42-8843-4752-b408-040a32e6aa4e"
PROCESS_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "provider_cases"
    / "tidas_process_snapshot"
    / "minimal_process_snapshot.json"
)


@pytest.fixture(autouse=True)
def isolated_database():
    previous = process_snapshot_service.settings.provider_tidas_process_snapshot_path
    process_snapshot_service.settings.provider_tidas_process_snapshot_path = ""
    Base.metadata.drop_all(bind=db_module.engine)
    Base.metadata.create_all(bind=db_module.engine)
    yield
    db_module.engine.dispose()
    process_snapshot_service.settings.provider_tidas_process_snapshot_path = previous


@pytest.fixture()
def client():
    return TestClient(app)


def _port(port_id: str, flow_uuid: str, version: str, direction: str, *, product: bool = False) -> dict:
    return {
        "id": port_id,
        "flowUuid": flow_uuid,
        "flowSourceNamespace": "tiangong_open_data",
        "flowVersion": version,
        "flowPropertyUuid": "93a60a56-a3c8-11da-a746-0800200b9a66",
        "flowPropertyVersion": "03.00.003",
        "unitGroupUuid": "93a60a57-a4c8-11da-a746-0800200c9a66",
        "unitGroupVersion": "03.00.003",
        "name": port_id,
        "unit": "kg",
        "unitGroup": "Units of mass",
        "amount": 1.0,
        "type": "technosphere",
        "direction": direction,
        "isProduct": product,
    }


def _exact_process_graph() -> HybridGraph:
    return HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg product",
            "nodes": [
                {
                    "id": "exact-node",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": PROCESS_UUID,
                    "name": "Exact process",
                    "location": "CN",
                    "reference_product": "product",
                    "inputs": [_port("input", INPUT_FLOW_UUID, "01.01.001", "input")],
                    "outputs": [_port("output", OUTPUT_FLOW_UUID, "01.01.000", "output", product=True)],
                }
            ],
            "exchanges": [],
            "metadata": {
                "functional_unit": {
                    "display_text": "1 kg product",
                    "amount": 1.0,
                    "flow_uuid": OUTPUT_FLOW_UUID,
                    "flow_source_namespace": "tiangong_open_data",
                    "flow_version": "01.01.000",
                    "unit": "kg",
                    "unit_group_uuid": "93a60a57-a4c8-11da-a746-0800200c9a66",
                    "unit_group_version": "03.00.003",
                }
            },
        }
    )


def _custom_graph() -> HybridGraph:
    graph = _exact_process_graph()
    graph.nodes[0].process_uuid = "custom-process"
    for port in graph.nodes[0].inputs + graph.nodes[0].outputs:
        port.flow_source_namespace = "commercial-import"
        port.flow_version = "release-2026"
    graph.metadata["functional_unit"]["flow_source_namespace"] = "commercial-import"
    graph.metadata["functional_unit"]["flow_version"] = "release-2026"
    return graph


def _inline(graph: HybridGraph) -> dict:
    return {
        "schema_version": "provider.snapshot.v1",
        "graph_hash": compute_graph_hash_from_graph(graph),
        "functional_unit": graph.metadata["functional_unit"],
        "graph": graph.model_dump(mode="json", by_alias=True),
        "source_policy": "open_mixed",
        "allowed_lcia_scope": "none",
    }


def _stored_snapshot(graph: HybridGraph) -> dict:
    graph_hash = compute_graph_hash_from_graph(graph)
    with db_module.SessionLocal() as db:
        db.add(Model(id="project-identity", name="Identity project", functional_unit="1 kg product"))
        db.add(
            ModelVersion(
                id="model-version-identity",
                model_id="project-identity",
                version=1,
                graph_hash=graph_hash,
                hybrid_graph_json=graph.model_dump(mode="json", by_alias=True),
            )
        )
        db.commit()
    return {"project_id": "project-identity", "version": 1, "graph_hash": graph_hash}


def _tidas_identity(**updates) -> dict:
    value = {
        "process_uuid": PROCESS_UUID,
        "source_namespace": "tiangong_open_data",
        "version": PROCESS_VERSION,
        "content_hash": PROCESS_CONTENT_HASH,
        "snapshot_hash": PROCESS_SNAPSHOT_HASH,
    }
    value.update(updates)
    return value


def test_custom_inline_process_identity_is_explicitly_consumer_asserted(client):
    graph = _custom_graph()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline(graph),
            "demand": [{"process_uuid": "custom-process", "amount": 1.0, "unit": "kg"}],
            "process_identities": [
                {
                    "process_uuid": "custom-process",
                    "source_namespace": "commercial-import",
                    "version": "release-2026",
                    "content_hash": "1" * 64,
                }
            ],
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["process_identity_receipts"] == [
        {
            "process_uuid": "custom-process",
            "source_namespace": "commercial-import",
            "version": "release-2026",
            "content_hash": "1" * 64,
            "snapshot_hash": None,
            "resolution": "inline_custom",
            "verification_scope": "consumer_asserted_hash",
            "process_name": None,
            "process_type": None,
        }
    ]
    codes = {issue["code"] for issue in body["issues"]}
    assert "INLINE_CUSTOM_PROCESS_IDENTITY" in codes
    assert "PROCESS_EXACT_VERSION_UNAVAILABLE" not in codes
    assert body["provenance"]["process_identities_hash"]


def test_missing_process_identity_keeps_legacy_warning_and_response(client):
    graph = _custom_graph()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline(graph),
            "demand": [{"process_uuid": "custom-process", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["process_identity_receipts"] == []
    assert body["provenance"]["process_identities_hash"] is None
    assert "PROCESS_EXACT_VERSION_UNAVAILABLE" in {issue["code"] for issue in body["issues"]}


def test_exact_tidas_process_identity_is_snapshot_verified_without_solve_writes(client, monkeypatch):
    monkeypatch.setattr(
        process_snapshot_service.settings,
        "provider_tidas_process_snapshot_path",
        str(PROCESS_FIXTURE),
    )
    graph = _exact_process_graph()
    snapshot_ref = _stored_snapshot(graph)
    with db_module.SessionLocal() as db:
        before = (db.query(Model).count(), db.query(ModelVersion).count())
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "snapshot_ref": snapshot_ref,
            "demand": [{"process_uuid": PROCESS_UUID, "amount": 1.0, "unit": "kg"}],
            "process_identities": [_tidas_identity()],
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    receipt = body["process_identity_receipts"][0]
    assert receipt["resolution"] == "tidas_exact_snapshot"
    assert receipt["verification_scope"] == "provider_exact_snapshot"
    assert receipt["content_hash"] == PROCESS_CONTENT_HASH
    assert receipt["snapshot_hash"] == PROCESS_SNAPSHOT_HASH
    assert receipt["process_name"] == "Crude petroleum delivery to plant gate"
    assert "PROCESS_EXACT_VERSION_UNAVAILABLE" not in {issue["code"] for issue in body["issues"]}
    with db_module.SessionLocal() as db:
        after = (db.query(Model).count(), db.query(ModelVersion).count())
    assert after == before == (1, 1)


@pytest.mark.parametrize(
    ("update", "expected_code"),
    [
        ({"content_hash": "0" * 64}, "TIDAS_PROCESS_IDENTITY_HASH_MISMATCH"),
        ({"snapshot_hash": None}, "TIDAS_PROCESS_IDENTITY_SNAPSHOT_HASH_REQUIRED"),
        ({"process_uuid": "11111111-1111-4111-8111-111111111111"}, "PROCESS_IDENTITY_GRAPH_MISMATCH"),
    ],
)
def test_process_identity_mismatch_fails_closed(client, monkeypatch, update, expected_code):
    monkeypatch.setattr(
        process_snapshot_service.settings,
        "provider_tidas_process_snapshot_path",
        str(PROCESS_FIXTURE),
    )
    graph = _exact_process_graph()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "snapshot_ref": _stored_snapshot(graph),
            "demand": [{"process_uuid": PROCESS_UUID, "amount": 1.0, "unit": "kg"}],
            "process_identities": [_tidas_identity(**update)],
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == expected_code


def test_exact_process_identity_rejects_graph_exchange_drift(client, monkeypatch):
    monkeypatch.setattr(
        process_snapshot_service.settings,
        "provider_tidas_process_snapshot_path",
        str(PROCESS_FIXTURE),
    )
    graph = _exact_process_graph()
    graph.nodes[0].inputs[0].amount = 2.0
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "snapshot_ref": _stored_snapshot(graph),
            "demand": [{"process_uuid": PROCESS_UUID, "amount": 1.0, "unit": "kg"}],
            "process_identities": [_tidas_identity()],
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIDAS_PROCESS_GRAPH_SIGNATURE_MISMATCH"
