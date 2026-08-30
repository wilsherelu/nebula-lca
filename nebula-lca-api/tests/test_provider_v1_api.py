from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
from app.database import Base
from app.main import app
from app.models import FlowVersionRecord, Model, ModelVersion, UnitDefinition, UnitGroup
from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph


@pytest.fixture(autouse=True)
def isolated_database():
    Base.metadata.drop_all(bind=db_module.engine)
    Base.metadata.create_all(bind=db_module.engine)
    yield
    db_module.engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


def _port(port_id: str, flow_uuid: str, name: str, amount: float, direction: str, exchange_type: str, *, product: bool = False):
    return {
        "id": port_id,
        "flowUuid": flow_uuid,
        "flowSourceNamespace": "test-catalog",
        "flowVersion": "1.0",
        "flowPropertyUuid": "fp-mass",
        "flowPropertyVersion": "1.0",
        "unitGroupUuid": "ug-mass",
        "unitGroupVersion": "1.0",
        "name": name,
        "unit": "kg",
        "unitGroup": "Mass",
        "amount": amount,
        "type": exchange_type,
        "direction": direction,
        "isProduct": product,
    }


def _graph() -> HybridGraph:
    return HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg final product",
            "nodes": [
                {
                    "id": "node-p1",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "p1",
                    "name": "Provider",
                    "location": "CN",
                    "reference_product": "Intermediate",
                    "inputs": [],
                    "outputs": [
                        _port("out-mid", "flow-mid", "Intermediate", 1.0, "output", "technosphere", product=True),
                        _port("out-co2", "flow-co2", "Carbon dioxide", 3.0, "output", "biosphere"),
                    ],
                },
                {
                    "id": "node-p2",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "p2",
                    "name": "Consumer",
                    "location": "CN",
                    "reference_product": "Final product",
                    "inputs": [
                        _port("in-mid", "flow-mid", "Intermediate", 2.0, "input", "technosphere"),
                    ],
                    "outputs": [
                        _port("out-final", "flow-final", "Final product", 1.0, "output", "technosphere", product=True),
                    ],
                },
            ],
            "exchanges": [
                {
                    "id": "edge-mid",
                    "fromNode": "node-p1",
                    "toNode": "node-p2",
                    "sourceHandle": "out:out-mid",
                    "targetHandle": "in:in-mid",
                    "flowUuid": "flow-mid",
                    "flowName": "Intermediate",
                    "quantityMode": "single",
                    "amount": 2.0,
                    "unit": "kg",
                    "type": "technosphere",
                }
            ],
            "metadata": {
                "functional_unit": {
                    "display_text": "1 kg final product",
                    "amount": 1.0,
                    "flow_uuid": "flow-final",
                    "flow_source_namespace": "test-catalog",
                    "flow_version": "1.0",
                    "unit": "kg",
                    "unit_group_uuid": "ug-mass",
                    "unit_group_version": "1.0",
                }
            },
        }
    )


def _inline_snapshot(graph: HybridGraph, base_ref: dict | None = None) -> dict:
    return {
        "schema_version": "provider.snapshot.v1",
        "base_snapshot_ref": base_ref,
        "graph_hash": compute_graph_hash_from_graph(graph),
        "functional_unit": {
            "display_text": "1 kg final product",
            "amount": 1.0,
            "flow_uuid": "flow-final",
            "flow_source_namespace": "test-catalog",
            "flow_version": "1.0",
            "unit": "kg",
            "unit_group_uuid": "ug-mass",
            "unit_group_version": "1.0",
        },
        "graph": graph.model_dump(mode="json", by_alias=True),
        "source_policy": "open_mixed",
        "allowed_lcia_scope": "ef31_only",
    }


def test_snapshot_returns_full_graph_engine_and_exact_identity_issues(client):
    graph = _graph()
    graph.nodes[0].outputs[0].flow_version = None
    graph_hash = compute_graph_hash_from_graph(graph)
    db = db_module.SessionLocal()
    try:
        project = Model(
            id="project-1",
            name="Provider project",
            functional_unit="1 kg final product",
            source_policy="open_mixed",
            allowed_lcia_scope="ef31_only",
        )
        db.add(project)
        db.add(
            ModelVersion(
                id="model-version-1",
                model_id=project.id,
                version=7,
                graph_hash=graph_hash,
                hybrid_graph_json=graph.model_dump(mode="json", by_alias=True),
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.get("/api/provider/v1/models/project-1/versions/7/snapshot")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["engine"]["commit"]
    assert body["project_id"] == "project-1"
    assert body["version"] == 7
    assert body["graph_hash"] == graph_hash
    assert len(body["graph"]["nodes"]) == 2
    assert body["functional_unit"]["flow_uuid"] == "flow-final"
    assert body["source_policy"] == "open_mixed"
    assert body["allowed_lcia_scope"] == "ef31_only"
    assert any(issue["code"] == "EXCHANGE_EXACT_IDENTITY_INCOMPLETE" for issue in body["issues"])

    solve_response = client.post(
        "/api/provider/v1/solve",
        json={
            "snapshot_ref": {"project_id": "project-1", "version": 7, "graph_hash": graph_hash},
            "demand": [{"process_uuid": "p2", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert solve_response.status_code == 200, solve_response.text
    assert solve_response.json()["snapshot_ref"] == {
        "project_id": "project-1",
        "version": 7,
        "graph_hash": graph_hash,
    }


def test_inline_solve_returns_true_activity_vector_and_explicit_boundaries(client):
    graph = _graph()
    provider_graph_hash = compute_graph_hash_from_graph(graph)
    consumer_graph_hash = "consumer-exact-json-hash"
    inline_snapshot = _inline_snapshot(
        graph,
        {"project_id": "draft-project", "version": 3, "graph_hash": "published-base-hash"},
    )
    inline_snapshot["graph_hash"] = consumer_graph_hash
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": inline_snapshot,
            "demand": [{"process_uuid": "p2", "amount": 1.0, "unit": "kg"}],
            "scenario_id": "scenario-a",
            "operation_hash": "operation-hash-a",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "completed"
    assert uuid.UUID(body["run_id"])
    assert body["snapshot_ref"] == {
        "project_id": "draft-project",
        "version": 3,
        "graph_hash": consumer_graph_hash,
    }
    assert body["demand"] == [
        {
            "process_uuid": "p2",
            "reference_exchange_id": None,
            "reference_flow_uuid": None,
            "amount": 1.0,
            "unit": "kg",
            "unit_group_uuid": None,
            "unit_group_version": None,
        }
    ]
    activities = {item["process_uuid"]: item["activity_amount"] for item in body["activity_vector"]}
    assert activities == pytest.approx({"p1": 2.0, "p2": 1.0})
    scaled = {item["exchange_id"]: item for item in body["scaled_exchanges"]}
    assert scaled["node-p1::out-mid"]["exchange_type"] == "technosphere"
    assert scaled["node-p1::out-mid"]["boundary_role"] == "internal"
    assert scaled["node-p1::out-co2"]["exchange_type"] == "elementary"
    assert scaled["node-p1::out-co2"]["boundary_role"] == "boundary"
    assert scaled["node-p1::out-co2"]["scaled_amount"] == pytest.approx(6.0)
    assert body["inventory_totals"] == [
        {
            "flow_uuid": "flow-co2",
            "flow_source_namespace": "test-catalog",
            "flow_version": "1.0",
            "flow_property_uuid": "fp-mass",
            "flow_property_version": "1.0",
            "unit_group_uuid": "ug-mass",
            "unit_group_version": "1.0",
            "unit": "kg",
            "direction": "output",
            "amount": pytest.approx(6.0),
        }
    ]
    assert body["process_residuals"] == []
    assert body["contribution_graph"] == {}
    assert {issue["code"] for issue in body["issues"]} >= {
        "PROCESS_RESIDUALS_UNSUPPORTED",
        "CONTRIBUTION_GRAPH_UNSUPPORTED",
    }
    assert body["provenance"]["activity_vector_semantics"] == "x in A*x=f"
    assert body["provenance"]["system_revision_hash"] == consumer_graph_hash
    assert body["provenance"]["consumer_graph_hash"] == consumer_graph_hash
    assert body["provenance"]["provider_graph_hash"] == provider_graph_hash
    assert "GRAPH_HASH_DOMAINS_DISTINCT" in {issue["code"] for issue in body["issues"]}


def test_catalog_resolve_is_exact_and_never_falls_back_to_current_or_name(client):
    db = db_module.SessionLocal()
    try:
        db.add(
            FlowVersionRecord(
                source_namespace="test-catalog",
                flow_uuid="flow-a",
                source_version="1.0",
                version_label="1.0",
                flow_name="Flow A",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Mass",
                flow_property_uuid="fp-mass",
                flow_property_version="1.0",
                unit_group_uuid="ug-mass",
                unit_group_version="1.0",
            )
        )
        db.add(UnitGroup(name="Mass", reference_unit="kg", source_uuid="ug-mass", source_version="1.0"))
        db.add(UnitDefinition(unit_group="Mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        db.commit()
    finally:
        db.close()

    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flows": [
                {"source_namespace": "test-catalog", "flow_uuid": "flow-a", "version": "1.0"},
                {"source_namespace": "test-catalog", "flow_uuid": "flow-a", "version": "2.0"},
            ],
            "flow_properties": [{"flow_property_uuid": "fp-mass", "version": "1.0"}],
            "unit_groups": [{"unit_group_uuid": "ug-mass", "version": "1.0"}],
            "units": [{"unit_group_uuid": "ug-mass", "version": "1.0", "unit": "kg"}],
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["issues"] == []
    assert [item["status"] for item in body["items"]] == [
        "resolved",
        "not_found",
        "unsupported",
        "resolved",
        "resolved",
    ]
    assert body["items"][1]["code"] == "EXACT_FLOW_VERSION_NOT_FOUND"
    assert body["items"][2]["code"] == "FLOW_PROPERTY_RESOURCE_UNAVAILABLE"


def test_existing_model_run_openapi_contract_is_unchanged(client):
    schema = client.get("/openapi.json").json()
    assert schema["paths"]["/api/model/run"]["post"]["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/RunResponse"
    }
