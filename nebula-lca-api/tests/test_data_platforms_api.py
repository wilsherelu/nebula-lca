"""API tests for external LCA data platform integration."""

import base64
import json
from pathlib import Path
import tempfile
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import app.database as _db_module
from app.config import settings
from app.database import Base
from app.main import app
from app.models import (
    DataPlatformAccount,
    DataPlatformAccountSession,
    DataPlatformRemoteCache,
    DataPlatformSyncJob,
    ExternalDataSyncRecord,
    FlowRecord,
    LciBiosphereFlowKey,
    LciProcessVector,
    Model,
    ModelVersion,
    ReferenceProcess,
    UnitDefinition,
    UnitGroup,
)
from app.services.data_platform_connectors import encrypt_credential


@pytest.fixture(autouse=True)
def setup_db(monkeypatch):
    monkeypatch.setenv("DATA_PLATFORM_CREDENTIAL_KEY", "test-platform-key")
    settings.data_platform_credential_key = "test-platform-key"
    Base.metadata.create_all(bind=_db_module.engine)
    yield
    engine_db_path = Path(str(_db_module.engine.url.database or "")).resolve()
    temp_root = Path(tempfile.gettempdir()).resolve()
    if temp_root != engine_db_path and temp_root not in engine_db_path.parents:
        raise RuntimeError(f"Refusing to clean data platform API test tables outside temp DB: {engine_db_path}")
    db = _db_module.SessionLocal()
    try:
        db.execute(ExternalDataSyncRecord.__table__.delete())
        db.execute(DataPlatformSyncJob.__table__.delete())
        db.execute(DataPlatformRemoteCache.__table__.delete())
        db.execute(DataPlatformAccountSession.__table__.delete())
        db.execute(DataPlatformAccount.__table__.delete())
        db.execute(LciProcessVector.__table__.delete())
        db.execute(LciBiosphereFlowKey.__table__.delete())
        db.execute(ReferenceProcess.__table__.delete())
        db.execute(FlowRecord.__table__.delete())
        db.execute(UnitDefinition.__table__.delete())
        db.execute(UnitGroup.__table__.delete())
        db.execute(ModelVersion.__table__.delete())
        db.execute(Model.__table__.delete())
        db.commit()
    finally:
        db.close()
    _db_module.engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


def _create_mock_account(client: TestClient, metadata: dict | None = None) -> str:
    response = client.post(
        "/api/data-platforms/accounts",
        json={
            "platform": "mock",
            "alias": "Mock Platform",
            "auth_type": "api_key",
            "credential": {"api_key": "secret-api-key"},
            "status": "active",
            "metadata": metadata or {},
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["has_credential"] is True
    assert "secret-api-key" not in str(data)
    return data["id"]


def _tg_api_key(email: str = "user@example.com", password: str = "password") -> str:
    return base64.b64encode(json.dumps({"email": email, "password": password}).encode("utf-8")).decode("ascii")


def _create_tiangong_account(client: TestClient, api_key: str | None = None) -> str:
    response = client.post(
        "/api/data-platforms/accounts",
        json={
            "platform": "tiangong",
            "alias": "TianGong",
            "base_url": "https://tg.example",
            "auth_type": "api_key",
            "credential": {"api_key": api_key or _tg_api_key()},
            "status": "active",
            "metadata": {"publishable_key": "pub-key", "environment_label": "test"},
        },
    )
    assert response.status_code == 201
    return response.json()["id"]


class _FakeSupabaseResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def _graph_payload():
    return {
        "functionalUnit": "1 kg remote product",
        "nodes": [
            {
                "id": "node-remote-process",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "remote-process",
                "name": "Remote process",
                "location": "CN",
                "reference_product": "remote product",
                "inputs": [],
                "outputs": [
                    {
                        "id": "out-1",
                        "flowUuid": "flow-model",
                        "name": "Remote flow",
                        "unit": "kg",
                        "amount": 1,
                        "type": "technosphere",
                        "direction": "output",
                    }
                ],
            }
        ],
        "exchanges": [],
        "metadata": {"source": "tiangong"},
    }


def _flow_row(flow_id: str = "flow-1"):
    return {
        "id": flow_id,
        "name": "Remote flow",
        "version": "1",
        "json": {
            "flowDataSet": {
                "flowProperties": {
                    "flowProperty": {
                        "referenceToFlowPropertyDataSet": {
                            "@refObjectId": "flowproperty-1",
                            "@version": "1",
                        }
                    }
                }
            }
        },
    }


def _flowproperty_row():
    return {
        "id": "flowproperty-1",
        "version": "1",
        "json": {
            "flowPropertyDataSet": {
                "flowPropertiesInformation": {
                    "quantitativeReference": {
                        "referenceToReferenceUnitGroup": {
                            "@refObjectId": "unitgroup-1",
                            "@version": "1",
                            "common:shortDescription": "Units of mass",
                        }
                    }
                }
            }
        },
    }


def _unitgroup_row():
    return {
        "id": "unitgroup-1",
        "version": "1",
        "json": {
            "unitGroupDataSet": {
                "unitGroupInformation": {
                    "dataSetInformation": {"common:name": "Units of mass"},
                    "quantitativeReference": {"referenceToReferenceUnit": "0"},
                },
                "units": {
                    "unit": [
                        {"@dataSetInternalID": "0", "name": "kg", "meanValue": 1},
                        {"@dataSetInternalID": "1", "name": "g", "meanValue": 0.001},
                    ]
                },
            }
        },
    }


def _install_fake_tiangong_http(monkeypatch, *, invalid_model: bool = False, refresh_fails: bool = False):
    calls = []

    def fake_urlopen(req, timeout=20):  # noqa: ARG001
        url = req.full_url
        calls.append({"url": url, "method": req.get_method(), "body": req.data.decode("utf-8") if req.data else ""})
        if "/auth/v1/token?grant_type=refresh_token" in url:
            if refresh_fails:
                return _FakeSupabaseResponse({})
            return _FakeSupabaseResponse({"access_token": "refreshed-token", "refresh_token": "refresh-token-2", "expires_in": 3600, "token_type": "bearer"})
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": "session-token", "refresh_token": "refresh-token-1", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/rpc/search_flows_latest" in url:
            return _FakeSupabaseResponse({"items": [{"id": "flow-1", "name": "Remote flow", "version": "1"}], "total": 1})
        if "/rest/v1/rpc/search_processes_latest" in url:
            return _FakeSupabaseResponse({"items": [{"id": "process-1", "name": "Remote process", "version": "1"}], "total": 1})
        if "/rest/v1/rpc/search_lifecyclemodels_latest" in url:
            return _FakeSupabaseResponse({"items": [{"id": "model-1", "name": "Remote model", "version": "1"}], "total": 1})
        if "/rest/v1/flows" in url:
            flow_id = "flow-model" if "flow-model" in url else "flow-1"
            return _FakeSupabaseResponse([_flow_row(flow_id)])
        if "/rest/v1/flowproperties" in url:
            return _FakeSupabaseResponse([_flowproperty_row()])
        if "/rest/v1/unitgroups" in url:
            return _FakeSupabaseResponse([_unitgroup_row()])
        if "/rest/v1/processes" in url:
            return _FakeSupabaseResponse([
                {
                    "id": "process-1",
                    "name": "Remote process",
                    "version": "1",
                    "json": {
                        "process_uuid": "process-1",
                        "process_name": "Remote process",
                        "reference_flow_uuid": "flow-1",
                        "exchanges": [{"flow_uuid": "flow-1", "flow_name": "Remote flow", "direction": "output", "amount": 1, "unit": "kg"}],
                    },
                }
            ])
        if "/rest/v1/lifecyclemodels" in url:
            model_json = {"json_tg": {"not_graph": True}} if invalid_model else {"json_tg": {"hybrid_graph": _graph_payload()}}
            return _FakeSupabaseResponse([{"id": "model-1", "name": "Remote model", "version": "1", **model_json}])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)
    return calls


def test_data_platform_account_crud_keeps_credentials_private(client):
    account_id = _create_mock_account(client)

    db = _db_module.SessionLocal()
    try:
        row = db.get(DataPlatformAccount, account_id)
        assert row is not None
        assert row.credential_ciphertext
        assert "secret-api-key" not in row.credential_ciphertext
    finally:
        db.close()

    listed = client.get("/api/data-platforms/accounts")
    assert listed.status_code == 200
    assert listed.json()[0]["id"] == account_id
    assert "secret-api-key" not in str(listed.json())

    patched = client.patch(f"/api/data-platforms/accounts/{account_id}", json={"status": "disabled", "alias": "Renamed"})
    assert patched.status_code == 200
    assert patched.json()["status"] == "disabled"
    assert patched.json()["alias"] == "Renamed"

    deleted = client.delete(f"/api/data-platforms/accounts/{account_id}")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True


def test_mock_connector_connection_success_and_failure(client):
    ok_id = _create_mock_account(client)
    failed_id = _create_mock_account(client, metadata={"fail_connection": True})

    ok = client.post(f"/api/data-platforms/accounts/{ok_id}/test")
    failed = client.post(f"/api/data-platforms/accounts/{failed_id}/test")

    assert ok.status_code == 200
    assert ok.json()["ok"] is True
    assert failed.status_code == 200
    assert failed.json()["ok"] is False


def test_tiangong_api_key_account_can_be_bound_without_exposing_secret(client, monkeypatch):
    _install_fake_tiangong_http(monkeypatch)
    api_key = _tg_api_key()
    response = client.post(
        "/api/data-platforms/accounts",
        json={
            "platform": "tiangong",
            "alias": "TianGong",
            "base_url": "https://tg.example",
            "auth_type": "api_key",
            "credential": {"api_key": api_key},
            "status": "active",
            "metadata": {"publishable_key": "pub-key"},
        },
    )

    assert response.status_code == 201
    data = response.json()
    assert data["platform"] == "tiangong"
    assert data["has_credential"] is True
    assert api_key not in str(data)

    checked = client.post(f"/api/data-platforms/accounts/{data['id']}/test")
    assert checked.status_code == 200
    assert checked.json()["ok"] is True
    assert "Auth ok" in checked.json()["message"]
    assert api_key not in str(checked.json())
    db = _db_module.SessionLocal()
    try:
        session_row = db.query(DataPlatformAccountSession).filter(DataPlatformAccountSession.account_id == data["id"]).first()
        assert session_row is not None
        assert "session-token" not in session_row.session_ciphertext
    finally:
        db.close()


def test_tiangong_session_cache_reuses_unexpired_token(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    first = client.post(f"/api/data-platforms/accounts/{account_id}/test")
    second = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote")

    assert first.status_code == 200
    assert second.status_code == 200
    password_grants = [call for call in calls if "grant_type=password" in call["url"]]
    assert len(password_grants) == 1


def test_tiangong_expired_session_refreshes_then_falls_back_to_password(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch, refresh_fails=False)
    account_id = _create_tiangong_account(client)
    db = _db_module.SessionLocal()
    try:
        db.add(
            DataPlatformAccountSession(
                account_id=account_id,
                session_ciphertext=encrypt_credential({"access_token": "old-token", "refresh_token": "refresh-token-1", "token_type": "bearer"}),
                expires_at=datetime.utcnow() - timedelta(minutes=1),
            )
        )
        db.commit()
    finally:
        db.close()

    refreshed = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote")

    assert refreshed.status_code == 200
    assert any("grant_type=refresh_token" in call["url"] for call in calls)
    assert not any("grant_type=password" in call["url"] for call in calls)

    calls_fallback = _install_fake_tiangong_http(monkeypatch, refresh_fails=True)
    db = _db_module.SessionLocal()
    try:
        row = db.query(DataPlatformAccountSession).filter(DataPlatformAccountSession.account_id == account_id).first()
        assert row is not None
        row.session_ciphertext = encrypt_credential({"access_token": "old-token", "refresh_token": "refresh-token-bad", "token_type": "bearer"})
        row.expires_at = datetime.utcnow() - timedelta(minutes=1)
        db.commit()
    finally:
        db.close()

    fallback = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote")

    assert fallback.status_code == 200
    assert any("grant_type=refresh_token" in call["url"] for call in calls_fallback)
    assert any("grant_type=password" in call["url"] for call in calls_fallback)


def test_tiangong_connection_test_requires_api_key_or_token(client):
    response = client.post(
        "/api/data-platforms/accounts",
        json={
            "platform": "tiangong",
            "alias": "TianGong Empty",
            "auth_type": "api_key",
            "credential": {},
            "status": "active",
        },
    )

    assert response.status_code == 201
    checked = client.post(f"/api/data-platforms/accounts/{response.json()['id']}/test")
    assert checked.status_code == 200
    assert checked.json()["ok"] is False
    assert "required" in checked.json()["message"]


def test_tiangong_api_key_decode_errors_do_not_leak_secret(client):
    bad_key = "not-base64-json-secret"
    account_id = _create_tiangong_account(client, api_key=bad_key)

    checked = client.post(f"/api/data-platforms/accounts/{account_id}/test")

    assert checked.status_code == 200
    assert checked.json()["ok"] is False
    assert "Base64 JSON" in checked.json()["message"]
    assert bad_key not in str(checked.json())


def test_tiangong_api_key_requires_email_and_password(client):
    missing_password = base64.b64encode(json.dumps({"email": "user@example.com"}).encode("utf-8")).decode("ascii")
    account_id = _create_tiangong_account(client, api_key=missing_password)

    checked = client.post(f"/api/data-platforms/accounts/{account_id}/test")

    assert checked.status_code == 200
    assert checked.json()["ok"] is False
    assert "email and password" in checked.json()["message"]


def test_remote_search_caches_but_does_not_write_catalog(client):
    account_id = _create_mock_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?q=ethylene&page_size=2")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert data["page"] == 1
    assert data["page_size"] == 2
    db = _db_module.SessionLocal()
    try:
        assert db.query(DataPlatformRemoteCache).count() == 2
        assert db.query(ReferenceProcess).count() == 0
        assert db.query(FlowRecord).count() == 0
    finally:
        db.close()


def test_sync_process_writes_catalog_and_is_idempotent(client):
    account_id = _create_mock_account(client)

    first = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )
    second = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["process_uuid"] == "mock-process-sync"
    db = _db_module.SessionLocal()
    try:
        assert db.query(ReferenceProcess).count() == 1
        assert db.query(FlowRecord).count() == 2
        assert db.query(DataPlatformSyncJob).count() == 2
        assert db.query(ExternalDataSyncRecord).count() == 3
    finally:
        db.close()


def test_tiangong_supabase_search_and_selected_sync(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    flow_search = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=1&page_size=10")
    process_search = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?q=remote&page=1&page_size=10")
    model_search = client.get(f"/api/data-platforms/accounts/{account_id}/models/search?q=remote&page=1&page_size=10")

    assert flow_search.status_code == 200
    assert process_search.status_code == 200
    assert model_search.status_code == 200
    assert model_search.json()["items"][0]["model_uuid"] == "model-1"
    db = _db_module.SessionLocal()
    try:
        assert db.query(DataPlatformRemoteCache).count() == 3
        assert db.query(FlowRecord).count() == 0
        assert db.query(ReferenceProcess).count() == 0
        assert db.query(Model).count() == 0
    finally:
        db.close()

    flow_sync = client.post(f"/api/data-platforms/accounts/{account_id}/flows/sync", json={"remote_flow_id": "flow-1", "remote_version": "1"})
    process_sync = client.post(f"/api/data-platforms/accounts/{account_id}/processes/sync", json={"remote_process_id": "process-1", "remote_version": "1"})
    model_sync = client.post(f"/api/data-platforms/accounts/{account_id}/models/sync", json={"remote_model_id": "model-1", "remote_version": "1"})

    assert flow_sync.status_code == 200, flow_sync.text
    assert process_sync.status_code == 200, process_sync.text
    assert model_sync.status_code == 200, model_sync.text
    assert any("/auth/v1/token" in call["url"] for call in calls)
    assert any("/rest/v1/rpc/search_flows_latest" in call["url"] for call in calls)
    db = _db_module.SessionLocal()
    try:
        assert db.query(FlowRecord).count() == 2
        flow = db.get(FlowRecord, "flow-1")
        assert flow is not None
        assert flow.unit_group == "Units of mass"
        assert flow.default_unit == "kg"
        assert flow.tidas_flow_property_uuid == "flowproperty-1"
        assert db.query(UnitGroup).count() == 1
        assert db.query(UnitDefinition).count() == 2
        assert db.query(ReferenceProcess).count() == 1
        assert db.query(Model).count() == 1
        assert db.query(ModelVersion).count() == 1
        assert db.query(ExternalDataSyncRecord).count() >= 4
    finally:
        db.close()


def test_tiangong_model_sync_invalid_graph_fails_without_partial_project(client, monkeypatch):
    _install_fake_tiangong_http(monkeypatch, invalid_model=True)
    account_id = _create_tiangong_account(client)

    response = client.post(f"/api/data-platforms/accounts/{account_id}/models/sync", json={"remote_model_id": "model-1", "remote_version": "1"})

    assert response.status_code == 400
    assert "HybridGraph" in response.text
    db = _db_module.SessionLocal()
    try:
        assert db.query(Model).count() == 0
        assert db.query(ModelVersion).count() == 0
        assert db.query(DataPlatformSyncJob).count() == 1
        assert db.query(DataPlatformSyncJob).first().status == "failed"
    finally:
        db.close()
