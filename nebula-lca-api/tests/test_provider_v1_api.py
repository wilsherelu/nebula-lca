from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
import app.services.provider_ef31 as provider_ef31
from app.database import Base
from app.main import app
from app.models import FlowRecord, FlowVersionRecord, Model, ModelVersion, UnitDefinition, UnitGroup
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


CO2_UUID = "08a91e70-3ddc-11dd-923d-0050c2490048"
MASS_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
MASS_UNIT_GROUP_UUID = "93a60a57-a4c8-11da-a746-0800200c9a66"


def _petrochemical_graph() -> HybridGraph:
    custom_namespace = "nebula-one.casepack.petroleum"
    custom_version = "casepack-1"

    def custom_port(port_id, flow_uuid, name, amount, direction, *, product=False):
        value = _port(port_id, flow_uuid, name, amount, direction, "technosphere", product=product)
        value.update(
            {
                "flowSourceNamespace": custom_namespace,
                "flowVersion": custom_version,
                "flowPropertyUuid": MASS_PROPERTY_UUID,
                "flowPropertyVersion": "03.00.003",
                "unitGroupUuid": MASS_UNIT_GROUP_UUID,
                "unitGroupVersion": "03.00.003",
            }
        )
        return value

    def co2_port(port_id, amount):
        value = _port(port_id, CO2_UUID, "carbon dioxide (fossil)", amount, "output", "biosphere")
        value.update(
            {
                "flowSourceNamespace": "tiangong_open_data",
                "flowVersion": "03.00.004",
                "flowPropertyUuid": MASS_PROPERTY_UUID,
                "flowPropertyVersion": "03.00.003",
                "unitGroupUuid": MASS_UNIT_GROUP_UUID,
                "unitGroupVersion": "03.00.003",
            }
        )
        return value

    return HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg low-sulfur diesel",
            "nodes": [
                {
                    "id": "node-cracking",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "process-cracking",
                    "name": "Cracking",
                    "location": "CN",
                    "reference_product": "Cracked diesel",
                    "inputs": [],
                    "outputs": [
                        custom_port("out-cracked", "custom-cracked-diesel", "Cracked diesel", 1.0, "output", product=True),
                        co2_port("out-co2-cracking", 0.5),
                    ],
                },
                {
                    "id": "node-desulfurization",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "process-desulfurization",
                    "name": "Desulfurization",
                    "location": "CN",
                    "reference_product": "Low-sulfur diesel",
                    "inputs": [
                        custom_port("in-cracked", "custom-cracked-diesel", "Cracked diesel", 1.2, "input"),
                    ],
                    "outputs": [
                        custom_port("out-final", "custom-low-sulfur-diesel", "Low-sulfur diesel", 1.0, "output", product=True),
                        co2_port("out-co2-desulfurization", 0.1),
                    ],
                },
            ],
            "exchanges": [
                {
                    "id": "edge-cracked-diesel",
                    "fromNode": "node-cracking",
                    "toNode": "node-desulfurization",
                    "sourceHandle": "out:out-cracked",
                    "targetHandle": "in:in-cracked",
                    "flowUuid": "custom-cracked-diesel",
                    "flowName": "Cracked diesel",
                    "quantityMode": "single",
                    "amount": 1.2,
                    "unit": "kg",
                    "type": "technosphere",
                }
            ],
            "metadata": {
                "database_release": "EF3.1",
                "functional_unit": {
                    "display_text": "1 kg low-sulfur diesel",
                    "amount": 1.0,
                    "flow_uuid": "custom-low-sulfur-diesel",
                    "flow_source_namespace": custom_namespace,
                    "flow_version": custom_version,
                    "unit": "kg",
                    "unit_group_uuid": MASS_UNIT_GROUP_UUID,
                    "unit_group_version": "03.00.003",
                },
            },
        }
    )


def _co2_refs(graph: HybridGraph) -> list[dict]:
    refs = []
    for node in graph.nodes:
        for port in node.outputs:
            if port.flowUuid != CO2_UUID:
                continue
            refs.append(
                {
                    "exchange_id": f"{node.id}::{port.id}",
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": CO2_UUID,
                    "version": "03.00.004",
                    "flow_property_uuid": MASS_PROPERTY_UUID,
                    "flow_property_version": "03.00.003",
                    "unit_group_uuid": MASS_UNIT_GROUP_UUID,
                    "unit_group_version": "03.00.003",
                    "unit": "kg",
                    "direction": "output",
                    "compartment": "Emissions to air, unspecified",
                }
            )
    return refs


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
                content_hash="flow-a-content-hash",
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
                {
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": CO2_UUID,
                    "version": "03.00.004",
                },
                {"source_namespace": "test-catalog", "flow_uuid": "flow-a", "version": "1.0"},
                {"source_namespace": "test-catalog", "flow_uuid": "flow-a", "version": "2.0"},
            ],
            "flow_properties": [{"flow_property_uuid": "fp-mass", "version": "1.0"}],
            "unit_groups": [
                {"unit_group_uuid": MASS_UNIT_GROUP_UUID, "version": "03.00.003"},
                {"unit_group_uuid": "ug-mass", "version": "1.0"},
            ],
            "units": [
                {"unit_group_uuid": MASS_UNIT_GROUP_UUID, "version": "03.00.003", "unit": "kg"},
                {"unit_group_uuid": "ug-mass", "version": "1.0", "unit": "kg"},
            ],
            "flow_candidates": [{"query": "Flow A", "flow_type": "Product flow", "unit": "kg"}],
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["issues"] == []
    assert [item["status"] for item in body["items"]] == [
        "resolved",
        "resolved",
        "not_found",
        "unsupported",
        "resolved",
        "resolved",
        "resolved",
        "resolved",
    ]
    exact_co2 = body["items"][0]["value"]
    assert exact_co2["runtime_flow_index"] == 86033
    assert exact_co2["flow_property_uuid"] == MASS_PROPERTY_UUID
    assert exact_co2["unit_group_uuid"] == MASS_UNIT_GROUP_UUID
    assert exact_co2["compartment"] == "Emissions to air, unspecified"
    assert exact_co2["content_hash"]
    assert body["items"][2]["code"] == "EXACT_FLOW_VERSION_NOT_FOUND"
    assert body["items"][3]["code"] == "FLOW_PROPERTY_RESOURCE_UNAVAILABLE"
    assert body["candidate_sets"][0]["candidates"][0]["flow_uuid"] == "flow-a"


def test_inline_custom_technosphere_with_exact_ef31_lcia_does_not_write_catalog_or_projects(client):
    graph = _petrochemical_graph()
    db = db_module.SessionLocal()
    try:
        before = {
            "projects": db.query(Model).count(),
            "flows": db.query(FlowRecord).count(),
            "flow_versions": db.query(FlowVersionRecord).count(),
        }
    finally:
        db.close()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
            "lcia_methods": ["EF v3.1"],
            "elementary_flows": _co2_refs(graph),
            "scenario_id": "petroleum-baseline",
            "operation_hash": "petroleum-operation-hash",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    activities = {item["process_uuid"]: item["activity_amount"] for item in body["activity_vector"]}
    assert activities == pytest.approx({"process-cracking": 1.2, "process-desulfurization": 1.0})
    inventory = {item["flow_uuid"]: item["amount"] for item in body["inventory_totals"]}
    assert inventory[CO2_UUID] == pytest.approx(0.7)
    assert body["lcia"]["method"] == "EF v3.1"
    assert body["lcia"]["database_release"] == "EF3.1"
    assert body["lcia"]["indicator_results"]
    indicator_metadata = body["lcia"]["indicator_metadata"]
    assert {
        key: indicator_metadata[key]
        for key in (
            "schema_version",
            "method",
            "database_release",
            "asset",
            "sha256",
            "indicator_count",
        )
    } == {
        "schema_version": "provider.lcia.indicator_metadata.v1",
        "method": "EF v3.1",
        "database_release": "EF3.1",
        "asset": "data/EF3.1/indicator_index.csv",
        "sha256": "d41e72b826f39c93958cab0ed1eec5b0ef6153c502dddcd98e54bd7e4daf99fe",
        "indicator_count": 25,
    }
    assert indicator_metadata["content_hash"]
    assert indicator_metadata["identity_runtime_indicator_sha256"]
    assert indicator_metadata["validated_runtime_indicator_sha256s"]
    climate = {
        item["canonical_indicator_key"]: item
        for item in body["lcia"]["indicator_results"]
        if item.get("canonical_indicator_key") in {"climate change", "climate change: fossil"}
    }
    assert {key: item["value"] for key, item in climate.items()} == pytest.approx(
        {"climate change": 0.7, "climate change: fossil": 0.7}
    )
    assert {item["unit"] for item in climate.values()} == {"kg CO2-Eq"}
    assert {item["indicator_unit"] for item in climate.values()} == {"kg CO2-Eq"}
    assert all(item["indicator_metadata_hash"] for item in climate.values())
    assert len(body["elementary_flow_receipts"]) == 2
    assert all(item["factor_count"] == 2 for item in body["elementary_flow_receipts"])
    assert all(item["factor_hash"] for item in body["elementary_flow_receipts"])
    db = db_module.SessionLocal()
    try:
        after = {
            "projects": db.query(Model).count(),
            "flows": db.query(FlowRecord).count(),
            "flow_versions": db.query(FlowVersionRecord).count(),
        }
    finally:
        db.close()
    assert after == before


def test_inline_custom_technosphere_mfa_without_lcia_remains_available(client):
    graph = _petrochemical_graph()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["lcia"] is None
    assert body["elementary_flow_receipts"] == []
    assert body["activity_vector"]
    assert body["scaled_exchanges"]


def test_inline_custom_technosphere_requires_complete_identity(client):
    graph = _petrochemical_graph()
    graph.nodes[0].outputs[0].flow_property_version = None
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "CUSTOM_TECHNOSPHERE_IDENTITY_INCOMPLETE"


def test_inline_ef31_lcia_fails_closed_when_indicator_unit_is_unresolved(client, monkeypatch):
    monkeypatch.setattr(provider_ef31, "_indicator_unit_metadata", lambda _runtime_dirs: ({}, {}))
    graph = _petrochemical_graph()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
            "lcia_methods": ["EF v3.1"],
            "elementary_flows": _co2_refs(graph),
        },
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "EF31_INDICATOR_UNIT_NOT_FOUND"


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing_reference", "ELEMENTARY_FLOW_REFERENCE_REQUIRED"),
        ("unknown_uuid", "ELEMENTARY_FLOW_NOT_IN_EF31_RUNTIME"),
        ("wrong_unit", "ELEMENTARY_FLOW_IDENTITY_MISMATCH"),
        ("wrong_direction", "ELEMENTARY_FLOW_IDENTITY_MISMATCH"),
        ("wrong_compartment", "ELEMENTARY_FLOW_IDENTITY_MISMATCH"),
        ("wrong_flow_property", "ELEMENTARY_FLOW_IDENTITY_MISMATCH"),
        ("wrong_unit_group_version", "ELEMENTARY_FLOW_IDENTITY_MISMATCH"),
    ],
)
def test_inline_ef31_lcia_fails_closed_for_unknown_or_mismatched_elementary_identity(client, mutation, expected_code):
    graph = _petrochemical_graph()
    refs = _co2_refs(graph)
    if mutation == "missing_reference":
        refs.pop()
    elif mutation == "unknown_uuid":
        for node in graph.nodes:
            for port in node.outputs:
                if port.flowUuid == CO2_UUID:
                    port.flowUuid = "00000000-0000-0000-0000-000000000000"
        for ref in refs:
            ref["flow_uuid"] = "00000000-0000-0000-0000-000000000000"
    elif mutation == "wrong_unit":
        refs[0]["unit"] = "g"
    elif mutation == "wrong_direction":
        refs[0]["direction"] = "input"
    elif mutation == "wrong_compartment":
        refs[0]["compartment"] = "Emissions to water, unspecified"
    elif mutation == "wrong_flow_property":
        refs[0]["flow_property_uuid"] = "wrong-flow-property"
    elif mutation == "wrong_unit_group_version":
        refs[0]["unit_group_version"] = "99.00.000"
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
            "lcia_methods": ["EF v3.1"],
            "elementary_flows": refs,
        },
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == expected_code


def test_existing_model_run_openapi_contract_is_unchanged(client):
    schema = client.get("/openapi.json").json()
    assert schema["paths"]["/api/model/run"]["post"]["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/RunResponse"
    }
