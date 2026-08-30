from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
import app.services.provider_tidas_reference_snapshot as reference_snapshot_service
import app.services.provider_tidas_snapshot as flow_snapshot_service
from app.database import Base
from app.main import app
from app.models import (
    FlowRecord,
    FlowVersionRecord,
    Model,
    ReferenceProcess,
    UnitDefinition,
    UnitGroup,
)


FLOW_UUID = "3d76981f-964a-4865-b588-0e067a2a1163"
FLOW_VERSION = "01.01.003"
FLOW_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200c9a66"
FLOW_PROPERTY_VERSION = "03.00.003"
UNIT_GROUP_UUID = "93a60a57-a3c8-11da-a746-0800200c9a66"
UNIT_GROUP_VERSION = "03.00.003"
REFERENCE_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "provider_cases"
    / "tidas_reference_dependency"
    / "minimal_energy_dependency.json"
)
FLOW_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "provider_cases"
    / "tidas_flow_snapshot"
    / "minimal_crude_oil_snapshot.json"
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


def _write(path: Path, document: dict) -> Path:
    path.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return path


def _reference_copy() -> dict:
    return copy.deepcopy(json.loads(REFERENCE_FIXTURE.read_text(encoding="utf-8")))


def _flow_copy() -> dict:
    return copy.deepcopy(json.loads(FLOW_FIXTURE.read_text(encoding="utf-8")))


def _recompute_reference_hashes(document: dict) -> None:
    for record in document["records"]:
        record["content_hash"] = _hash(record["payload"])
    document["snapshot_hash"] = _hash(
        {key: value for key, value in document.items() if key != "snapshot_hash"}
    )


def _recompute_flow_hashes(document: dict) -> None:
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


def _electricity_flow_snapshot(tmp_path: Path) -> Path:
    document = _flow_copy()
    record = document["records"][0]
    record["source_object_id"] = FLOW_UUID
    record["source_version"] = FLOW_VERSION
    dataset = record["payload"]["flowDataSet"]
    dataset["flowInformation"]["dataSetInformation"]["common:UUID"] = FLOW_UUID
    reference = dataset["flowProperties"]["flowProperty"]["referenceToFlowPropertyDataSet"]
    reference["@refObjectId"] = FLOW_PROPERTY_UUID
    reference["@version"] = FLOW_PROPERTY_VERSION
    document["filters"] = {"exact_refs": [f"{FLOW_UUID}@{FLOW_VERSION}"]}
    _recompute_flow_hashes(document)
    return _write(tmp_path / "electricity-flow.json", document)


def _configure(monkeypatch, *, flow_path: Path, reference_path: Path | None) -> None:
    monkeypatch.setattr(
        flow_snapshot_service.settings,
        "provider_tidas_flow_snapshot_path",
        str(flow_path),
    )
    monkeypatch.setattr(
        reference_snapshot_service.settings,
        "provider_tidas_reference_dependency_snapshot_path",
        str(reference_path) if reference_path is not None else "",
    )


def _flow_request() -> dict:
    return {
        "flows": [
            {
                "source_namespace": "tiangong_open_data",
                "flow_uuid": FLOW_UUID,
                "version": FLOW_VERSION,
            }
        ]
    }


def test_exact_flow_uses_reference_dependency_snapshot_and_does_not_write_database(
    client,
    monkeypatch,
    tmp_path,
):
    flow_path = _electricity_flow_snapshot(tmp_path)
    _configure(monkeypatch, flow_path=flow_path, reference_path=REFERENCE_FIXTURE)
    with db_module.SessionLocal() as db:
        before = [
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

    response = client.post("/api/provider/v1/catalog/resolve", json=_flow_request())

    assert response.status_code == 200, response.text
    item = response.json()["items"][0]
    assert item["status"] == "resolved"
    value = item["value"]
    assert value["flow_property_uuid"] == FLOW_PROPERTY_UUID
    assert value["flow_property_version"] == FLOW_PROPERTY_VERSION
    assert value["unit_group_uuid"] == UNIT_GROUP_UUID
    assert value["unit_group_version"] == UNIT_GROUP_VERSION
    assert value["default_unit"] == "MJ"
    assert value["reference_dependency_resolution_source"] == "tidas_exact_reference_snapshot"
    assert value["reference_dependency_snapshot_hash"] == (
        "a2dd2e73af7f461213b626ab1d940eebe360edb06479c49d92a8411dedba3f16"
    )
    assert {row["unit"]: row["factor_to_reference"] for row in value["units"]} == {
        "MJ": 1.0,
        "kWh": 3.6,
    }
    with db_module.SessionLocal() as db:
        after = [
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
    assert before == after == [0, 0, 0, 0, 0, 0]


def test_exact_flow_without_reference_snapshot_fails_closed(client, monkeypatch, tmp_path):
    _configure(
        monkeypatch,
        flow_path=_electricity_flow_snapshot(tmp_path),
        reference_path=None,
    )
    response = client.post("/api/provider/v1/catalog/resolve", json=_flow_request())
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "unsupported"
    assert item["code"] == "TIDAS_REFERENCE_DEPENDENCY_SNAPSHOT_REQUIRED"


def test_reference_resources_resolve_exactly_through_catalog(client, monkeypatch, tmp_path):
    _configure(
        monkeypatch,
        flow_path=_electricity_flow_snapshot(tmp_path),
        reference_path=REFERENCE_FIXTURE,
    )
    response = client.post(
        "/api/provider/v1/catalog/resolve",
        json={
            "flow_properties": [
                {"flow_property_uuid": FLOW_PROPERTY_UUID, "version": FLOW_PROPERTY_VERSION},
                {"flow_property_uuid": FLOW_PROPERTY_UUID, "version": "03.00.004"},
            ],
            "unit_groups": [
                {"unit_group_uuid": UNIT_GROUP_UUID, "version": UNIT_GROUP_VERSION},
                {"unit_group_uuid": UNIT_GROUP_UUID, "version": "03.00.004"},
            ],
            "units": [
                {"unit_group_uuid": UNIT_GROUP_UUID, "version": UNIT_GROUP_VERSION, "unit": "kWh"},
                {"unit_group_uuid": UNIT_GROUP_UUID, "version": UNIT_GROUP_VERSION, "unit": "Wh"},
            ],
        },
    )
    assert response.status_code == 200, response.text
    items = response.json()["items"]
    assert [item["status"] for item in items] == [
        "resolved",
        "unsupported",
        "resolved",
        "not_found",
        "resolved",
        "not_found",
    ]
    assert items[0]["value"]["reference_unit_group_uuid"] == UNIT_GROUP_UUID
    assert items[2]["value"]["reference_unit"] == "MJ"
    assert items[4]["value"]["factor_to_reference"] == 3.6


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("snapshot_hash", "TIDAS_REFERENCE_SNAPSHOT_HASH_MISMATCH"),
        ("record_hash", "TIDAS_REFERENCE_RECORD_HASH_MISMATCH"),
        ("unit_group_version", "TIDAS_REFERENCE_UNIT_GROUP_NOT_IN_SNAPSHOT"),
        ("multiple_reference_units", "TIDAS_REFERENCE_UNIT_GROUP_REFERENCE_UNIT_AMBIGUOUS"),
        ("missing_reference_unit", "TIDAS_REFERENCE_UNIT_GROUP_REFERENCE_UNIT_AMBIGUOUS"),
        ("duplicate_unit_id", "TIDAS_REFERENCE_UNIT_DUPLICATE"),
        ("duplicate_unit_name", "TIDAS_REFERENCE_UNIT_DUPLICATE"),
    ],
)
def test_invalid_reference_dependency_snapshot_fails_closed(
    client,
    monkeypatch,
    tmp_path,
    mutation,
    expected_code,
):
    document = _reference_copy()
    flow_property = document["records"][0]["payload"]["flowPropertyDataSet"]
    unit_group = document["records"][1]["payload"]["unitGroupDataSet"]
    if mutation == "snapshot_hash":
        document["snapshot_hash"] = "0" * 64
    elif mutation == "record_hash":
        document["records"][0]["content_hash"] = "0" * 64
        document["snapshot_hash"] = _hash(
            {key: value for key, value in document.items() if key != "snapshot_hash"}
        )
    elif mutation == "unit_group_version":
        reference = flow_property["flowPropertiesInformation"]["quantitativeReference"][
            "referenceToReferenceUnitGroup"
        ]
        reference["@version"] = "03.00.004"
        _recompute_reference_hashes(document)
    elif mutation == "multiple_reference_units":
        unit_group["unitGroupInformation"]["quantitativeReference"][
            "referenceToReferenceUnit"
        ] = ["0", "1"]
        _recompute_reference_hashes(document)
    elif mutation == "missing_reference_unit":
        unit_group["unitGroupInformation"]["quantitativeReference"][
            "referenceToReferenceUnit"
        ] = "9"
        _recompute_reference_hashes(document)
    elif mutation == "duplicate_unit_id":
        unit_group["units"]["unit"][1]["@dataSetInternalID"] = "0"
        _recompute_reference_hashes(document)
    elif mutation == "duplicate_unit_name":
        unit_group["units"]["unit"][1]["name"] = "mj"
        _recompute_reference_hashes(document)
    reference_path = _write(tmp_path / f"{mutation}.json", document)
    _configure(
        monkeypatch,
        flow_path=_electricity_flow_snapshot(tmp_path),
        reference_path=reference_path,
    )
    response = client.post("/api/provider/v1/catalog/resolve", json=_flow_request())
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == expected_code
