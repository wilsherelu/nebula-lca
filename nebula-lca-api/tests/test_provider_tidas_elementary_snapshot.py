from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
import app.services.provider_ef31 as provider_ef31
import app.services.provider_v1 as provider_v1
import app.services.provider_tidas_reference_snapshot as reference_snapshot_service
import app.services.provider_tidas_snapshot as flow_snapshot_service
from app.database import Base
from app.main import app
from app.models import FlowRecord, FlowVersionRecord, Model, ReferenceProcess, UnitDefinition, UnitGroup
from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph


NOX_UUID = "f79d0f8f-2b0e-49cb-bed0-b1ea0fbd8625"
NOX_VERSION = "01.00.004"
RESOURCE_UUID = "fe0acd60-3ddc-11dd-a6f8-0050c2490048"
RESOURCE_VERSION = "04.00.004"
CO2_UUID = "08a91e70-3ddc-11dd-923d-0050c2490048"
MASS_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
MASS_UNIT_GROUP_UUID = "93a60a57-a4c8-11da-a746-0800200c9a66"
ENERGY_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200c9a66"
ENERGY_UNIT_GROUP_UUID = "93a60a57-a3c8-11da-a746-0800200c9a66"
ELEMENTARY_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "provider_cases"
    / "tidas_elementary_flow_snapshot"
    / "minimal_versioned_elementary_snapshot.json"
)
REFERENCE_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "provider_cases"
    / "tidas_reference_dependency"
    / "minimal_energy_dependency.json"
)


@pytest.fixture(autouse=True)
def isolated_database():
    previous_flow = flow_snapshot_service.settings.provider_tidas_flow_snapshot_path
    previous_reference = (
        reference_snapshot_service.settings.provider_tidas_reference_dependency_snapshot_path
    )
    flow_snapshot_service.settings.provider_tidas_flow_snapshot_path = ""
    reference_snapshot_service.settings.provider_tidas_reference_dependency_snapshot_path = ""
    Base.metadata.drop_all(bind=db_module.engine)
    Base.metadata.create_all(bind=db_module.engine)
    yield
    db_module.engine.dispose()
    flow_snapshot_service.settings.provider_tidas_flow_snapshot_path = previous_flow
    reference_snapshot_service.settings.provider_tidas_reference_dependency_snapshot_path = previous_reference


@pytest.fixture()
def client():
    return TestClient(app)


def _hash(value) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _snapshot_copy() -> dict:
    return copy.deepcopy(json.loads(ELEMENTARY_FIXTURE.read_text(encoding="utf-8")))


def _recompute_hashes(document: dict) -> None:
    for record in document["records"]:
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


def _write(path: Path, document: dict) -> Path:
    path.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return path


def _configure(monkeypatch, flow_path: Path | None = ELEMENTARY_FIXTURE) -> None:
    monkeypatch.setattr(
        flow_snapshot_service.settings,
        "provider_tidas_flow_snapshot_path",
        str(flow_path) if flow_path is not None else "",
    )
    monkeypatch.setattr(
        reference_snapshot_service.settings,
        "provider_tidas_reference_dependency_snapshot_path",
        str(REFERENCE_FIXTURE),
    )


def _flow_refs() -> list[dict]:
    return [
        {
            "source_namespace": "tiangong_open_data",
            "flow_uuid": NOX_UUID,
            "version": NOX_VERSION,
        },
        {
            "source_namespace": "tiangong_open_data",
            "flow_uuid": RESOURCE_UUID,
            "version": RESOURCE_VERSION,
        },
    ]


def _port(
    port_id: str,
    flow_uuid: str,
    version: str,
    amount: float,
    direction: str,
    exchange_type: str,
    *,
    flow_property_uuid: str,
    unit_group_uuid: str,
    unit: str,
    product: bool = False,
) -> dict:
    return {
        "id": port_id,
        "flowUuid": flow_uuid,
        "flowSourceNamespace": "tiangong_open_data" if flow_uuid == NOX_UUID else "test-custom",
        "flowVersion": version,
        "flowPropertyUuid": flow_property_uuid,
        "flowPropertyVersion": "03.00.003",
        "unitGroupUuid": unit_group_uuid,
        "unitGroupVersion": "03.00.003",
        "name": port_id,
        "unit": unit,
        "unitGroup": "Units of mass",
        "amount": amount,
        "type": exchange_type,
        "direction": direction,
        "isProduct": product,
    }


def _single_process_graph(*, nox_type: str = "biosphere") -> HybridGraph:
    return HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg product",
            "nodes": [
                {
                    "id": "node-p1",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "p1",
                    "name": "Single process",
                    "location": "CN",
                    "reference_product": "Product",
                    "inputs": [],
                    "outputs": [
                        _port(
                            "out-product",
                            "custom-product",
                            "case-1",
                            1.0,
                            "output",
                            "technosphere",
                            flow_property_uuid=MASS_PROPERTY_UUID,
                            unit_group_uuid=MASS_UNIT_GROUP_UUID,
                            unit="kg",
                            product=True,
                        ),
                        _port(
                            "out-nox",
                            NOX_UUID,
                            NOX_VERSION,
                            0.1,
                            "output",
                            nox_type,
                            flow_property_uuid=MASS_PROPERTY_UUID,
                            unit_group_uuid=MASS_UNIT_GROUP_UUID,
                            unit="kg",
                        ),
                    ],
                }
            ],
            "exchanges": [],
            "metadata": {
                "functional_unit": {
                    "display_text": "1 kg product",
                    "amount": 1.0,
                    "flow_uuid": "custom-product",
                    "flow_source_namespace": "test-custom",
                    "flow_version": "case-1",
                    "unit": "kg",
                    "unit_group_uuid": MASS_UNIT_GROUP_UUID,
                    "unit_group_version": "03.00.003",
                }
            },
        }
    )


def _inline_snapshot(graph: HybridGraph) -> dict:
    return {
        "schema_version": "provider.snapshot.v1",
        "graph_hash": compute_graph_hash_from_graph(graph),
        "functional_unit": graph.metadata["functional_unit"],
        "graph": graph.model_dump(mode="json", by_alias=True),
        "source_policy": "open_mixed",
        "allowed_lcia_scope": "ef31_only",
    }


def test_versioned_elementary_snapshot_resolves_exact_runtime_semantics_without_database_writes(
    client,
    monkeypatch,
):
    _configure(monkeypatch)
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": _flow_refs()})
    assert response.status_code == 200, response.text
    items = response.json()["items"]
    assert [item["status"] for item in items] == ["resolved", "resolved"]
    nox, resource = [item["value"] for item in items]
    assert (nox["version"], nox["unit"], nox["direction"], nox["compartment"]) == (
        NOX_VERSION,
        "kg",
        "output",
        "Emissions to air, unspecified",
    )
    assert nox["factor_count"] == 185
    assert (resource["version"], resource["unit"], resource["direction"], resource["compartment"]) == (
        RESOURCE_VERSION,
        "MJ",
        "input",
        "Non-renewable energy resources from ground",
    )
    assert resource["flow_property_uuid"] == ENERGY_PROPERTY_UUID
    assert resource["unit_group_uuid"] == ENERGY_UNIT_GROUP_UUID
    assert resource["factor_count"] == 1
    with db_module.SessionLocal() as db:
        counts = [
            db.query(model).count()
            for model in (
                ReferenceProcess,
                FlowRecord,
                FlowVersionRecord,
                UnitGroup,
                UnitDefinition,
                Model,
            )
        ]
    assert counts == [0, 0, 0, 0, 0, 0]


def test_non_builtin_elementary_version_requires_exact_snapshot(client, monkeypatch):
    _configure(monkeypatch, None)
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": [_flow_refs()[0]]})
    assert response.status_code == 200
    assert response.json()["items"][0]["code"] == "TIDAS_FLOW_SNAPSHOT_REQUIRED"


def test_wrong_elementary_version_does_not_fall_back(client, monkeypatch):
    _configure(monkeypatch)
    ref = _flow_refs()[0]
    ref["version"] = "03.00.004"
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": [ref]})
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "not_found"
    assert item["code"] == "EXACT_FLOW_VERSION_NOT_FOUND"


@pytest.mark.parametrize("mutation", ["classification", "unit_property"])
def test_snapshot_runtime_classification_or_unit_conflict_is_unsupported(
    client,
    monkeypatch,
    tmp_path,
    mutation,
):
    document = _snapshot_copy()
    record = document["records"][1 if mutation == "unit_property" else 0]
    dataset = record["payload"]["flowDataSet"]
    if mutation == "classification":
        categories = dataset["flowInformation"]["dataSetInformation"]["classificationInformation"][
            "common:elementaryFlowCategorization"
        ]["common:category"]
        categories[-1]["#text"] = "Emissions to water, unspecified"
    else:
        reference = dataset["flowProperties"]["flowProperty"]["referenceToFlowPropertyDataSet"]
        reference["@refObjectId"] = MASS_PROPERTY_UUID
    _recompute_hashes(document)
    path = _write(tmp_path / f"{mutation}.json", document)
    _configure(monkeypatch, path)
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={"flows": [_flow_refs()[1 if mutation == "unit_property" else 0]]},
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "ELEMENTARY_FLOW_SNAPSHOT_RUNTIME_CONFLICT"


def test_snapshot_hash_mismatch_fails_closed(client, monkeypatch, tmp_path):
    document = _snapshot_copy()
    document["snapshot_hash"] = "0" * 64
    path = _write(tmp_path / "bad-hash.json", document)
    _configure(monkeypatch, path)
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": [_flow_refs()[0]]})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIDAS_FLOW_SNAPSHOT_HASH_MISMATCH"


def test_elementary_flow_without_cf_coverage_is_unsupported(client, monkeypatch):
    _configure(monkeypatch)
    monkeypatch.setattr(provider_ef31, "_runtime_factor_coverage", lambda runtime_dir: {})
    response = client.post("/api/provider/v1/catalog/resolve", json={"flows": [_flow_refs()[0]]})
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "ELEMENTARY_FLOW_CF_NOT_FOUND"


def test_elementary_flow_absent_from_ef31_runtime_is_unsupported(client, monkeypatch, tmp_path):
    unknown_uuid = "11111111-1111-4111-8111-111111111111"
    document = _snapshot_copy()
    record = document["records"][0]
    record["source_object_id"] = unknown_uuid
    record["payload"]["flowDataSet"]["flowInformation"]["dataSetInformation"][
        "common:UUID"
    ] = unknown_uuid
    document["records"] = [record]
    document["filters"] = {"exact_refs": [f"{unknown_uuid}@{NOX_VERSION}"]}
    document["declared_total"] = 1
    document["counts"] = {"flow": 1}
    _recompute_hashes(document)
    path = _write(tmp_path / "unknown-runtime-flow.json", document)
    _configure(monkeypatch, path)
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flows": [
                {
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": unknown_uuid,
                    "version": NOX_VERSION,
                }
            ]
        },
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "ELEMENTARY_FLOW_NOT_IN_EF31_RUNTIME_CATALOG"


def test_elementary_snapshot_cannot_be_used_as_technosphere_port(client, monkeypatch):
    _configure(monkeypatch)
    graph = _single_process_graph(nox_type="technosphere")
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "p1", "amount": 1.0, "unit": "kg"}],
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIDAS_TECHNOSPHERE_FLOW_TYPE_MISMATCH"


def test_exact_elementary_version_is_used_by_lcia_receipt(client, monkeypatch):
    _configure(monkeypatch)
    graph = _single_process_graph()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "p1", "amount": 1.0, "unit": "kg"}],
            "lcia_methods": ["EF v3.1"],
            "elementary_flows": [
                {
                    "exchange_id": "node-p1::out-nox",
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": NOX_UUID,
                    "version": NOX_VERSION,
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
    )
    assert response.status_code == 200, response.text
    receipt = response.json()["elementary_flow_receipts"][0]
    assert receipt["version"] == NOX_VERSION
    assert receipt["snapshot_hash"] == "c7d94df703c41ee19197e3b4d86500a6a698cc49266df86d38ebf4e1816ee6c6"
    assert receipt["factor_count"] > 0


def test_exact_elementary_receipt_is_available_without_lcia_from_one_flow_snapshot(
    client,
    monkeypatch,
):
    _configure(monkeypatch)
    configured_snapshot = provider_v1._configured_tidas_snapshot
    calls = 0

    def counted_snapshot():
        nonlocal calls
        calls += 1
        return configured_snapshot()

    monkeypatch.setattr(provider_v1, "_configured_tidas_snapshot", counted_snapshot)
    graph = _single_process_graph()
    response = client.post(
        "/api/provider/v1/solve",
        json={
            "inline_snapshot": _inline_snapshot(graph),
            "demand": [{"process_uuid": "p1", "amount": 1.0, "unit": "kg"}],
            "elementary_flows": [
                {
                    "exchange_id": "node-p1::out-nox",
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": NOX_UUID,
                    "version": NOX_VERSION,
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
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["lcia"] is None
    assert calls == 1
    assert len(body["elementary_flow_receipts"]) == 1
    receipt = body["elementary_flow_receipts"][0]
    scaled = next(
        item for item in body["scaled_exchanges"]
        if item["exchange_id"] == receipt["exchange_id"]
    )
    assert scaled["process_uuid"] == "p1"
    assert scaled["flow_uuid"] == receipt["flow_uuid"]
    assert receipt["flow_property_content_hash"]
    assert receipt["unit_group_content_hash"]
    assert receipt["unit_content_hash"]
    assert body["provenance"]["flow_snapshot_hash"] == receipt["snapshot_hash"]
    assert len(body["provenance"]["reference_dependency_snapshot_hash"]) == 64
    assert receipt["reference_dependency_snapshot_hash"] is None


def test_existing_0300004_elementary_catalog_remains_compatible(client, monkeypatch):
    _configure(monkeypatch)
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flows": [
                {
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": CO2_UUID,
                    "version": "03.00.004",
                }
            ]
        },
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "resolved"
    assert item["value"]["version"] == "03.00.004"
