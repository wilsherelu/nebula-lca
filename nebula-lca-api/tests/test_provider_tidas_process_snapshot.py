from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
import app.services.provider_tidas_process_snapshot as process_snapshot_service
from app.database import Base
from app.main import app
from app.models import FlowRecord, Model, ReferenceProcess


PROCESS_UUID = "f784e009-504d-4422-9e7d-915394e5cdbd"
PROCESS_VERSION = "01.01.000"
QREF_FLOW_UUID = "b7ce9d42-8843-4752-b408-040a32e6aa4e"
CASEPACK_CRUDE_UUID = "95151b26-d16b-4669-b433-fc0bd633f564"
FIXTURE = (
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


def _hash(value) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _snapshot_copy() -> dict:
    return copy.deepcopy(json.loads(FIXTURE.read_text(encoding="utf-8")))


def _recompute_hashes(document: dict) -> None:
    for record in document["records"]:
        record["content_hash"] = _hash(record["payload"])
    document["counts"] = {"process": len(document["records"])}
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


def _write_snapshot(path: Path, document: dict) -> Path:
    path.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return path


def _configure(monkeypatch, path: Path = FIXTURE) -> None:
    monkeypatch.setattr(
        process_snapshot_service.settings,
        "provider_tidas_process_snapshot_path",
        str(path),
    )


def _request(version: str = PROCESS_VERSION) -> dict:
    return {
        "schema_version": "provider.catalog.resolve.request.v1",
        "processes": [
            {
                "source_namespace": "tiangong_open_data",
                "process_uuid": PROCESS_UUID,
                "version": version,
                "correlation_id": "casepack-crude-candidate",
            }
        ],
    }


def test_exact_process_catalog_receipt_is_read_only_and_does_not_substitute_casepack_flow(client, monkeypatch):
    _configure(monkeypatch)
    with db_module.SessionLocal() as db:
        before = {
            "processes": db.query(ReferenceProcess).count(),
            "flows": db.query(FlowRecord).count(),
            "models": db.query(Model).count(),
        }

    response = client.post("/api/provider/v1/catalog/resolve", json=_request())

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["kind"] == "process"
    assert item["status"] == "resolved"
    value = item["value"]
    assert value["process_uuid"] == PROCESS_UUID
    assert value["version"] == PROCESS_VERSION
    assert value["quantitative_reference"]["exchange_internal_id"] == "4"
    assert value["quantitative_reference"]["direction"] == "output"
    assert value["quantitative_reference"]["flow"]["flow_uuid"] == QREF_FLOW_UUID
    assert value["quantitative_reference"]["flow"]["flow_uuid"] != CASEPACK_CRUDE_UUID
    assert value["dependency_closure_status"] == "unresolved"
    assert value["background_solve_supported"] is False
    assert value["calculation_role"] == "catalog_only"
    assert len(value["exchanges"]) == 2
    assert len(value["input_dependencies"]) == 1
    assert value["unresolved_exact_flow_refs"] == [value["input_dependencies"][0]["flow"]]
    assert value["input_dependencies"][0]["flow_catalog_binding_status"] == "requires_exact_flow_resolution"
    assert value["input_dependencies"][0]["unit_identity_status"] == "requires_exact_flow_resolution"
    assert value["other_outputs"] == []
    with db_module.SessionLocal() as db:
        after = {
            "processes": db.query(ReferenceProcess).count(),
            "flows": db.query(FlowRecord).count(),
            "models": db.query(Model).count(),
        }
    assert after == before


def test_exact_process_catalog_requires_configured_snapshot(client):
    response = client.post("/api/provider/v1/catalog/resolve", json=_request())
    assert response.status_code == 200
    assert response.json()["items"][0]["code"] == "TIDAS_PROCESS_SNAPSHOT_REQUIRED"


def test_exact_process_catalog_does_not_fall_back_to_latest(client, monkeypatch):
    _configure(monkeypatch)
    response = client.post("/api/provider/v1/catalog/resolve", json=_request("01.01.001"))
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "not_found"
    assert item["code"] == "EXACT_PROCESS_VERSION_NOT_FOUND"


@pytest.mark.parametrize(
    ("process_uuid", "version"),
    [
        ("not-a-uuid", PROCESS_VERSION),
        (PROCESS_UUID.upper(), PROCESS_VERSION),
        (PROCESS_UUID, "latest"),
        (PROCESS_UUID, "1.1"),
    ],
)
def test_process_request_requires_canonical_uuid_and_exact_version(client, process_uuid, version):
    payload = _request()
    payload["processes"][0]["process_uuid"] = process_uuid
    payload["processes"][0]["version"] = version
    response = client.post("/api/provider/v1/catalog/resolve", json=payload)
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing_qref", "TIDAS_PROCESS_QUANTITATIVE_REFERENCE_AMBIGUOUS"),
        ("multiple_qref", "TIDAS_PROCESS_QUANTITATIVE_REFERENCE_AMBIGUOUS"),
        ("input_qref", "TIDAS_PROCESS_QUANTITATIVE_REFERENCE_NOT_OUTPUT"),
        ("missing_flow_version", "TIDAS_PROCESS_SNAPSHOT_VERSION_INVALID"),
        ("duplicate_exchange_id", "TIDAS_PROCESS_EXCHANGE_IDENTITY_DUPLICATE"),
    ],
)
def test_invalid_process_contract_fails_closed(client, monkeypatch, tmp_path, mutation, expected_code):
    document = _snapshot_copy()
    root = document["records"][0]["payload"]["processDataSet"]
    qref = root["processInformation"]["quantitativeReference"]
    exchanges = root["exchanges"]["exchange"]
    if mutation == "missing_qref":
        qref.pop("referenceToReferenceFlow")
    elif mutation == "multiple_qref":
        qref["referenceToReferenceFlow"] = ["1", "4"]
    elif mutation == "input_qref":
        exchanges[1]["exchangeDirection"] = "Input"
    elif mutation == "missing_flow_version":
        exchanges[0]["referenceToFlowDataSet"].pop("@version")
    elif mutation == "duplicate_exchange_id":
        exchanges[0]["@dataSetInternalID"] = "4"
    _recompute_hashes(document)
    _configure(monkeypatch, _write_snapshot(tmp_path / f"{mutation}.json", document))

    response = client.post("/api/provider/v1/catalog/resolve", json=_request())

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == expected_code


def test_snapshot_and_record_hashes_are_verified(client, monkeypatch, tmp_path):
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
    document["snapshot_hash"] = _hash(body)
    _configure(monkeypatch, _write_snapshot(tmp_path / "bad-record-hash.json", document))
    response = client.post("/api/provider/v1/catalog/resolve", json=_request())
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIDAS_PROCESS_RECORD_HASH_MISMATCH"

    document = _snapshot_copy()
    document["snapshot_hash"] = "0" * 64
    _configure(monkeypatch, _write_snapshot(tmp_path / "bad-snapshot-hash.json", document))
    response = client.post("/api/provider/v1/catalog/resolve", json=_request())
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIDAS_PROCESS_SNAPSHOT_HASH_MISMATCH"


def test_duplicate_exact_process_identity_fails_closed(client, monkeypatch, tmp_path):
    document = _snapshot_copy()
    document["records"].append(copy.deepcopy(document["records"][0]))
    _recompute_hashes(document)
    _configure(monkeypatch, _write_snapshot(tmp_path / "duplicate.json", document))
    response = client.post("/api/provider/v1/catalog/resolve", json=_request())
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIDAS_PROCESS_SNAPSHOT_IDENTITY_DUPLICATE"


def test_same_exact_database_identity_conflict_fails_closed(client, monkeypatch):
    _configure(monkeypatch)
    with db_module.SessionLocal() as db:
        db.add(
            ReferenceProcess(
                process_uuid=PROCESS_UUID,
                process_name="Conflicting local row",
                process_type="unit_process",
                reference_flow_uuid=CASEPACK_CRUDE_UUID,
                reference_flow_internal_id="9",
                process_json={},
                import_report_json={
                    "source_namespace": "tiangong_open_data",
                    "source_version": PROCESS_VERSION,
                    "content_hash": "0" * 64,
                    "type_of_data_set": "Market process",
                },
            )
        )
        db.commit()

    response = client.post("/api/provider/v1/catalog/resolve", json=_request())

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "TIDAS_PROCESS_DATABASE_CONTENT_CONFLICT"
    assert set(item["value"]["database_conflicts"]) == {
        "content_hash",
        "process_type",
        "reference_flow_uuid",
        "reference_flow_internal_id",
    }


def test_same_exact_database_identity_without_hash_is_unsupported(client, monkeypatch):
    _configure(monkeypatch)
    with db_module.SessionLocal() as db:
        db.add(
            ReferenceProcess(
                process_uuid=PROCESS_UUID,
                process_name="Unpinned local row",
                process_type="unit_process",
                process_json={},
                import_report_json={
                    "source_namespace": "tiangong_open_data",
                    "source_version": PROCESS_VERSION,
                },
            )
        )
        db.commit()
    response = client.post("/api/provider/v1/catalog/resolve", json=_request())
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "TIDAS_PROCESS_DATABASE_IDENTITY_INCOMPLETE"
