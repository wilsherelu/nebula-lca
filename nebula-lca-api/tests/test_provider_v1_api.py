from __future__ import annotations

import copy
import hashlib
import json
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
import app.services.provider_ef31 as provider_ef31
import app.services.provider_tidas_reference_snapshot as provider_tidas_reference_snapshot
import app.services.provider_tidas_snapshot as provider_tidas_snapshot
from app.database import Base
from app.main import app
from app.models import FlowRecord, FlowVersionRecord, Model, ModelVersion, UnitDefinition, UnitGroup
from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph


@pytest.fixture(autouse=True)
def isolated_database():
    previous_snapshot_path = provider_tidas_snapshot.settings.provider_tidas_flow_snapshot_path
    previous_reference_snapshot_path = (
        provider_tidas_reference_snapshot.settings.provider_tidas_reference_dependency_snapshot_path
    )
    provider_tidas_snapshot.settings.provider_tidas_flow_snapshot_path = ""
    provider_tidas_reference_snapshot.settings.provider_tidas_reference_dependency_snapshot_path = ""
    Base.metadata.drop_all(bind=db_module.engine)
    Base.metadata.create_all(bind=db_module.engine)
    yield
    db_module.engine.dispose()
    provider_tidas_snapshot.settings.provider_tidas_flow_snapshot_path = previous_snapshot_path
    provider_tidas_reference_snapshot.settings.provider_tidas_reference_dependency_snapshot_path = (
        previous_reference_snapshot_path
    )


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
WATER_VAPOUR_UUID = "fe0acd60-3ddc-11dd-ac04-0050c2490048"
MASS_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
MASS_UNIT_GROUP_UUID = "93a60a57-a4c8-11da-a746-0800200c9a66"
CRUDE_OIL_UUID = "95151b26-d16b-4669-b433-fc0bd633f564"
CRUDE_OIL_VERSION = "01.01.002"
TIDAS_SNAPSHOT_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "provider_cases"
    / "tidas_flow_snapshot"
    / "minimal_crude_oil_snapshot.json"
)


def _canonical_hash(value) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _recompute_snapshot_hashes(document: dict) -> None:
    for record in document["records"]:
        record["content_hash"] = _canonical_hash(record["payload"])
    document["counts"] = {"flow": len(document["records"])}
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
    document["snapshot_hash"] = _canonical_hash(body)


def _snapshot_copy() -> dict:
    return copy.deepcopy(json.loads(TIDAS_SNAPSHOT_FIXTURE.read_text(encoding="utf-8")))


def _write_snapshot(path: Path, document: dict) -> Path:
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _configure_tidas_snapshot(monkeypatch, path: Path = TIDAS_SNAPSHOT_FIXTURE) -> None:
    monkeypatch.setattr(
        provider_tidas_snapshot.settings,
        "provider_tidas_flow_snapshot_path",
        str(path),
    )


def _water_vapour_snapshot(path: Path) -> Path:
    document = {
        "schema_version": "tiangong-open-dataset-snapshot.v1",
        "source_namespace": "tiangong_open_data",
        "dataset_kind": "flow",
        "state_scope": "open",
        "filters": {"exact_refs": [f"{WATER_VAPOUR_UUID}@03.00.004"]},
        "declared_total": 1,
        "records": [
            {
                "dataset_kind": "flow",
                "source_namespace": "tiangong_open_data",
                "source_object_id": WATER_VAPOUR_UUID,
                "source_version": "03.00.004",
                "source_modified_at": "2012-01-12T14:51:49Z",
                "type_of_data_set": "Elementary flow",
                "payload": {
                    "flowDataSet": {
                        "flowProperties": {
                            "flowProperty": {
                                "meanValue": "1.0",
                                "@dataSetInternalID": "0",
                                "referenceToFlowPropertyDataSet": {
                                    "@version": "03.00.003",
                                    "@refObjectId": MASS_PROPERTY_UUID,
                                },
                            }
                        },
                        "flowInformation": {
                            "dataSetInformation": {
                                "name": {"baseName": {"#text": "water vapour", "@xml:lang": "en"}},
                                "common:UUID": WATER_VAPOUR_UUID,
                                "classificationInformation": {
                                    "common:elementaryFlowCategorization": {
                                        "common:category": [
                                            {"#text": "Emissions", "@level": "0"},
                                            {"#text": "Emissions to air", "@level": "1"},
                                            {"#text": "Emissions to air, unspecified", "@level": "2"},
                                        ]
                                    }
                                },
                            },
                            "quantitativeReference": {"referenceToReferenceFlowProperty": "0"},
                        },
                        "modellingAndValidation": {"LCIMethod": {"typeOfDataSet": "Elementary flow"}},
                        "administrativeInformation": {
                            "publicationAndOwnership": {"common:dataSetVersion": "03.00.004"}
                        },
                    }
                },
            }
        ],
    }
    _recompute_snapshot_hashes(document)
    return _write_snapshot(path, document)


def _water_vapour_graph() -> HybridGraph:
    product = _port(
        "water-service-out",
        "flow-final",
        "Water-use service",
        1.0,
        "output",
        "technosphere",
        product=True,
    )
    vapour = _port(
        "water-vapour-out",
        WATER_VAPOUR_UUID,
        "water vapour",
        0.9,
        "output",
        "biosphere",
    )
    vapour.update(
        {
            "flowSourceNamespace": "tiangong_open_data",
            "flowVersion": "03.00.004",
            "flowPropertyUuid": MASS_PROPERTY_UUID,
            "flowPropertyVersion": "03.00.003",
            "unitGroupUuid": MASS_UNIT_GROUP_UUID,
            "unitGroupVersion": "03.00.003",
        }
    )
    return HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg water-use service",
            "nodes": [
                {
                    "id": "reclaimed-water-utilization",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "water-process",
                    "name": "Reclaimed water utilization",
                    "location": "CN",
                    "reference_product": "Water-use service",
                    "inputs": [],
                    "outputs": [product, vapour],
                }
            ],
            "exchanges": [],
            "metadata": {
                "functional_unit": {
                    "display_text": "1 kg water-use service",
                    "amount": 1.0,
                    "flow_uuid": "flow-final",
                    "flow_source_namespace": "test-catalog",
                    "flow_version": "1.0",
                    "unit": "kg",
                    "unit_group_uuid": "ug-mass",
                    "unit_group_version": "1.0",
                },
                "elementary_flow_refs": [
                    {
                        "exchange_id": "reclaimed-water-utilization::water-vapour-out",
                        "source_namespace": "tiangong_open_data",
                        "flow_uuid": WATER_VAPOUR_UUID,
                        "version": "03.00.004",
                        "flow_property_uuid": MASS_PROPERTY_UUID,
                        "flow_property_version": "03.00.003",
                        "unit_group_uuid": MASS_UNIT_GROUP_UUID,
                        "unit_group_version": "03.00.003",
                        "unit": "kg",
                        "direction": "output",
                        "compartment": "Emissions to air, unspecified",
                    }
                ],
            },
        }
    )


def _petrochemical_graph(*, include_tidas_crude: bool = False) -> HybridGraph:
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

    def crude_oil_port():
        value = _port("in-crude-oil", CRUDE_OIL_UUID, "Crude Oil", 1.5, "input", "technosphere")
        value.update(
            {
                "flowSourceNamespace": "tiangong_open_data",
                "flowVersion": CRUDE_OIL_VERSION,
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
                    "inputs": [crude_oil_port()] if include_tidas_crude else [],
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


def test_tidas_snapshot_exact_catalog_and_solve_receipts_do_not_write_database(client, monkeypatch):
    _configure_tidas_snapshot(monkeypatch)
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flows": [
                {
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": CRUDE_OIL_UUID,
                    "version": CRUDE_OIL_VERSION,
                }
            ]
        },
    )
    assert response.status_code == 200, response.text
    item = response.json()["items"][0]
    assert item["status"] == "resolved"
    assert item["value"] == {
        "resolution_source": "tidas_exact_snapshot",
        "source_namespace": "tiangong_open_data",
        "flow_uuid": CRUDE_OIL_UUID,
        "version": CRUDE_OIL_VERSION,
        "flow_type": "Product flow",
        "flow_property_uuid": MASS_PROPERTY_UUID,
        "flow_property_version": "03.00.003",
        "flow_property_mean_value": 1.0,
        "flow_property_content_hash": item["value"]["flow_property_content_hash"],
        "unit_group_uuid": MASS_UNIT_GROUP_UUID,
        "unit_group_version": "03.00.003",
        "unit_group": "Units of mass",
        "unit_group_content_hash": item["value"]["unit_group_content_hash"],
        "units": item["value"]["units"],
        "default_unit": "kg",
        "unit_content_hash": item["value"]["unit_content_hash"],
        "reference_dependency_resolution_source": "provider_reference_seed",
        "reference_dependency_snapshot_hash": None,
        "content_hash": "73197a675ed5596ec8fe9f8b532250daf63e47e28d5c1df1595d4f553df55566",
        "snapshot_hash": "e6d37ac32d1e661d30ebcd04f6160617657ab4c8e47bccd897e455cf0b97f90e",
        "snapshot_schema_version": "tiangong-open-dataset-snapshot.v1",
        "snapshot_state_scope": "open",
        "source_modified_at": "2026-08-31T00:00:00+00:00",
    }
    assert item["value"]["flow_property_content_hash"]
    assert item["value"]["unit_group_content_hash"]
    assert item["value"]["unit_content_hash"]

    db = db_module.SessionLocal()
    try:
        before = {
            "projects": db.query(Model).count(),
            "flows": db.query(FlowRecord).count(),
            "flow_versions": db.query(FlowVersionRecord).count(),
        }
    finally:
        db.close()
    graph = _petrochemical_graph(include_tidas_crude=True)
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
            "lcia_methods": ["EF v3.1"],
            "elementary_flows": _co2_refs(graph),
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    receipt = next(
        row
        for row in body["technosphere_flow_receipts"]
        if row["flow_uuid"] == CRUDE_OIL_UUID
    )
    assert receipt["resolution"] == "tidas_exact_snapshot"
    assert receipt["content_hash"] == item["value"]["content_hash"]
    assert receipt["snapshot_hash"] == item["value"]["snapshot_hash"]
    assert receipt["flow_property_uuid"] == MASS_PROPERTY_UUID
    assert receipt["unit_group_uuid"] == MASS_UNIT_GROUP_UUID
    assert receipt["unit"] == "kg"
    assert receipt["reference_dependency_resolution_source"] == "provider_reference_seed"
    assert receipt["reference_dependency_snapshot_hash"] is None
    custom_receipts = [
        row for row in body["technosphere_flow_receipts"] if row["resolution"] == "inline_custom"
    ]
    assert custom_receipts
    assert "INLINE_CUSTOM_TECHNOSPHERE_FLOW" in {issue["code"] for issue in body["issues"]}
    db = db_module.SessionLocal()
    try:
        after = {
            "projects": db.query(Model).count(),
            "flows": db.query(FlowRecord).count(),
            "flow_versions": db.query(FlowVersionRecord).count(),
        }
    finally:
        db.close()
    assert after == before == {"projects": 0, "flows": 0, "flow_versions": 0}


def test_tidas_snapshot_unconfigured_fails_closed(client):
    graph = _petrochemical_graph(include_tidas_crude=True)
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_SNAPSHOT_REQUIRED"

    db = db_module.SessionLocal()
    try:
        db.add(
            FlowVersionRecord(
                source_namespace="tiangong_open_data",
                flow_uuid=CRUDE_OIL_UUID,
                source_version=CRUDE_OIL_VERSION,
                version_label=CRUDE_OIL_VERSION,
                flow_name="Crude Oil",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                flow_property_uuid=MASS_PROPERTY_UUID,
                flow_property_version="03.00.003",
                unit_group_uuid=MASS_UNIT_GROUP_UUID,
                unit_group_version="03.00.003",
                content_hash="73197a675ed5596ec8fe9f8b532250daf63e47e28d5c1df1595d4f553df55566",
            )
        )
        db.commit()
    finally:
        db.close()
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flows": [
                {
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": CRUDE_OIL_UUID,
                    "version": CRUDE_OIL_VERSION,
                }
            ]
        },
    )
    assert response.status_code == 200, response.text
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "TIDAS_FLOW_SNAPSHOT_REQUIRED"


def test_tidas_snapshot_invalid_hash_fails_closed(client, monkeypatch, tmp_path):
    document = _snapshot_copy()
    document["snapshot_hash"] = "0" * 64
    path = _write_snapshot(tmp_path / "bad-hash.json", document)
    _configure_tidas_snapshot(monkeypatch, path)
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": []})
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_SNAPSHOT_HASH_MISMATCH"


def test_tidas_snapshot_record_content_hash_fails_closed(client, monkeypatch, tmp_path):
    document = _snapshot_copy()
    document["records"][0]["content_hash"] = "0" * 64
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
    document["snapshot_hash"] = _canonical_hash(body)
    path = _write_snapshot(tmp_path / "bad-record-hash.json", document)
    _configure_tidas_snapshot(monkeypatch, path)
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": []})
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_RECORD_HASH_MISMATCH"


def test_tidas_snapshot_duplicate_identity_fails_closed(client, monkeypatch, tmp_path):
    document = _snapshot_copy()
    document["records"].append(copy.deepcopy(document["records"][0]))
    _recompute_snapshot_hashes(document)
    path = _write_snapshot(tmp_path / "duplicate.json", document)
    _configure_tidas_snapshot(monkeypatch, path)
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": []})
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_SNAPSHOT_IDENTITY_DUPLICATE"


def test_tidas_snapshot_exact_version_does_not_fall_back(client, monkeypatch):
    _configure_tidas_snapshot(monkeypatch)
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flows": [
                {
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": CRUDE_OIL_UUID,
                    "version": "01.01.001",
                }
            ]
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["items"][0]["status"] == "not_found"
    assert response.json()["items"][0]["code"] == "EXACT_FLOW_VERSION_NOT_FOUND"

    graph = _petrochemical_graph(include_tidas_crude=True)
    graph.nodes[0].inputs[0].flow_version = "01.01.001"
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_EXACT_VERSION_NOT_IN_SNAPSHOT"


def test_tidas_snapshot_graph_identity_mismatch_fails_closed(client, monkeypatch):
    _configure_tidas_snapshot(monkeypatch)
    graph = _petrochemical_graph(include_tidas_crude=True)
    graph.nodes[0].inputs[0].unit_group_version = "99.00.000"
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_GRAPH_IDENTITY_MISMATCH"


def test_tidas_snapshot_missing_dependency_is_unsupported(client, monkeypatch, tmp_path):
    document = _snapshot_copy()
    reference = document["records"][0]["payload"]["flowDataSet"]["flowProperties"]["flowProperty"][
        "referenceToFlowPropertyDataSet"
    ]
    reference["@refObjectId"] = "11111111-1111-4111-8111-111111111111"
    _recompute_snapshot_hashes(document)
    path = _write_snapshot(tmp_path / "missing-dependency.json", document)
    _configure_tidas_snapshot(monkeypatch, path)
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flows": [
                {
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": CRUDE_OIL_UUID,
                    "version": CRUDE_OIL_VERSION,
                }
            ]
        },
    )
    assert response.status_code == 200, response.text
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "TIDAS_REFERENCE_DEPENDENCY_SNAPSHOT_REQUIRED"


def test_tidas_snapshot_database_conflict_fails_closed(client, monkeypatch):
    _configure_tidas_snapshot(monkeypatch)
    db = db_module.SessionLocal()
    try:
        db.add(
            FlowVersionRecord(
                source_namespace="tiangong_open_data",
                flow_uuid=CRUDE_OIL_UUID,
                source_version=CRUDE_OIL_VERSION,
                version_label=CRUDE_OIL_VERSION,
                flow_name="Crude Oil",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                flow_property_uuid=MASS_PROPERTY_UUID,
                flow_property_version="03.00.003",
                unit_group_uuid=MASS_UNIT_GROUP_UUID,
                unit_group_version="03.00.003",
                content_hash="conflicting-content-hash",
            )
        )
        db.commit()
    finally:
        db.close()
    request = {
        "flows": [
            {
                "source_namespace": "tiangong_open_data",
                "flow_uuid": CRUDE_OIL_UUID,
                "version": CRUDE_OIL_VERSION,
            }
        ]
    }
    response = client.post("/api/provider/v1/catalog/resolve", json=request)
    assert response.status_code == 200, response.text
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "TIDAS_FLOW_DATABASE_CONTENT_CONFLICT"

    graph = _petrochemical_graph(include_tidas_crude=True)
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "process-desulfurization", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_DATABASE_CONTENT_CONFLICT"


def test_inline_custom_technosphere_with_exact_ef31_lcia_does_not_write_catalog_or_projects(client):
    graph = _petrochemical_graph()
    co2_refs = _co2_refs(graph)
    graph.metadata["elementary_flow_refs"] = co2_refs
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
            "elementary_flows": co2_refs,
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
    contribution_receipts = {
        item["canonical_indicator_key"]: item
        for item in body["lcia"]["indicator_contribution_receipts"]
    }
    assert set(contribution_receipts) == {
        item["canonical_indicator_key"] for item in body["lcia"]["indicator_results"]
    }
    for receipt in contribution_receipts.values():
        assert len(receipt["terms"]) == 2
        assert receipt["contribution_total"] == pytest.approx(receipt["indicator_value"])
        assert receipt["reconciliation_delta"] == pytest.approx(0.0, abs=1e-12)
        assert receipt["solve_hash"] == body["lcia"]["solve_hash"]
        assert receipt["provider_commit"] == body["provenance"]["engine"]["commit"]
        assert receipt["content_hash"]
    climate_terms = contribution_receipts["climate change"]["terms"]
    assert {item["exchange_id"]: item["contribution_value"] for item in climate_terms} == pytest.approx(
        {
            "node-cracking::out-co2-cracking": 0.6,
            "node-desulfurization::out-co2-desulfurization": 0.1,
        }
    )
    assert all(item["process_uuid"] for item in climate_terms)
    assert all(item["flow_uuid"] == CO2_UUID for item in climate_terms)
    assert all(item["flow_version"] == "03.00.004" for item in climate_terms)
    assert all(item["amount_unit"] == "kg" for item in climate_terms)
    assert all(item["cf_value"] == pytest.approx(1.0) for item in climate_terms)
    assert all(item["cf_presence"] == "explicit_nonzero" for item in climate_terms)
    assert all(item["cf_hash"] and item["cf_runtime_id"] for item in climate_terms)
    assert all(item["snapshot_hash"] == body["provenance"]["consumer_graph_hash"] for item in climate_terms)
    assert body["lcia"]["contribution_receipts_hash"]
    assert body["lcia"]["runtime_assets_hash"]
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


def test_inline_metadata_elementary_ref_returns_exact_receipt_without_lcia_or_background_pins(
    client,
    monkeypatch,
    tmp_path,
):
    snapshot_path = _water_vapour_snapshot(tmp_path / "water-vapour-flow-snapshot.json")
    _configure_tidas_snapshot(monkeypatch, snapshot_path)
    graph = _water_vapour_graph()

    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "water-process", "amount": 1.0, "unit": "kg"}],
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["lcia"] is None
    assert body["background_process_receipts"] == []
    scaled = {
        item["exchange_id"]: item
        for item in body["scaled_exchanges"]
        if item["exchange_type"] == "elementary"
    }
    assert scaled["reclaimed-water-utilization::water-vapour-out"]["scaled_amount"] == pytest.approx(0.9)
    assert len(body["elementary_flow_receipts"]) == 1
    receipt = body["elementary_flow_receipts"][0]
    assert receipt == {
        **receipt,
        "exchange_id": "reclaimed-water-utilization::water-vapour-out",
        "source_namespace": "tiangong_open_data",
        "flow_uuid": WATER_VAPOUR_UUID,
        "version": "03.00.004",
        "flow_property_uuid": MASS_PROPERTY_UUID,
        "flow_property_version": "03.00.003",
        "unit_group_uuid": MASS_UNIT_GROUP_UUID,
        "unit_group_version": "03.00.003",
        "unit": "kg",
        "direction": "output",
        "compartment": "Emissions to air, unspecified",
        "factor_count": 0,
        "runtime_flow_index": None,
    }
    assert receipt["content_hash"]
    assert receipt["snapshot_hash"]


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


def test_inline_ef31_lcia_fails_closed_when_runtime_factor_sources_disagree(client, monkeypatch):
    runtime_cache = provider_ef31.importlib.import_module("app.core.ef31_runtime_cache")
    original = runtime_cache.GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_sources

    def conflicting_sources(*args, **kwargs):
        pack = original(*args, **kwargs)
        factor_sources = pack["factor_sources"]
        key = next(iter(factor_sources))
        factor_sources[key] = [
            *factor_sources[key],
            {
                "source_index": factor_sources[key][0]["source_index"],
                "coefficient": float(factor_sources[key][0]["coefficient"]) + 1.0,
            },
        ]
        return pack

    monkeypatch.setattr(
        runtime_cache.GLOBAL_EF31_RUNTIME_CACHE,
        "build_c_matrix_from_sources",
        conflicting_sources,
    )
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
    assert response.json()["detail"]["code"] == "EF31_CF_SOURCE_AMBIGUOUS"


def test_inline_ef31_lcia_fails_closed_when_factor_provenance_is_missing(client, monkeypatch):
    runtime_cache = provider_ef31.importlib.import_module("app.core.ef31_runtime_cache")
    original = runtime_cache.GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_sources

    def missing_sources(*args, **kwargs):
        pack = original(*args, **kwargs)
        pack["factor_sources"] = {}
        return pack

    monkeypatch.setattr(
        runtime_cache.GLOBAL_EF31_RUNTIME_CACHE,
        "build_c_matrix_from_sources",
        missing_sources,
    )
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
    assert response.json()["detail"]["code"] == "EF31_CF_PROVENANCE_NOT_FOUND"


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
