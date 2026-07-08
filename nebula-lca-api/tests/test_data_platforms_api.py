"""API tests for external LCA data platform integration."""

import base64
import io
import json
from pathlib import Path
import tempfile
from datetime import datetime, timedelta
from urllib.error import HTTPError

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
    DebugDiagnostic,
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
from app.services.data_platform_connectors import ConnectorError, PlatformAccountContext, TianGongSupabaseConnector, encrypt_credential


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
        db.execute(DebugDiagnostic.__table__.delete())
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


def _jwt(sub: str = "user-1") -> str:
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode("utf-8")).decode("ascii").rstrip("=")
    payload = base64.urlsafe_b64encode(json.dumps({"sub": sub}).encode("utf-8")).decode("ascii").rstrip("=")
    return f"{header}.{payload}.sig"


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


def _create_tiangong_login_account(client: TestClient) -> str:
    response = client.post(
        "/api/data-platforms/accounts",
        json={
            "platform": "tiangong",
            "alias": "TianGong Login",
            "base_url": "https://tg.example",
            "auth_type": "basic",
            "credential": {"username": "user@example.com", "password": "password"},
            "status": "active",
            "metadata": {"publishable_key": "pub-key", "environment_label": "test"},
        },
    )
    assert response.status_code == 201
    assert "password" not in str(response.json())
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


def _install_fake_tiangong_http(monkeypatch, *, invalid_model: bool = False, refresh_fails: bool = False, new_search_404: bool = False, legacy_search_404: bool = False):
    calls = []

    def fake_urlopen(req, timeout=20):  # noqa: ARG001
        url = req.full_url
        calls.append({"url": url, "method": req.get_method(), "body": req.data.decode("utf-8") if req.data else ""})
        if "/auth/v1/token?grant_type=refresh_token" in url:
            if refresh_fails:
                return _FakeSupabaseResponse({})
            return _FakeSupabaseResponse({"access_token": _jwt("user-2"), "refresh_token": "refresh-token-2", "expires_in": 3600, "token_type": "bearer"})
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "refresh-token-1", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/rpc/get_latest_flow_versions" in url:
            return _FakeSupabaseResponse([{"id": "flow-1", "name": "Remote flow", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/get_latest_process_versions" in url:
            return _FakeSupabaseResponse([{"id": "process-1", "name": "Remote process", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/get_latest_lifecyclemodel_versions" in url:
            return _FakeSupabaseResponse([{"id": "model-1", "name": "Remote model", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_flows_v1" in url:
            if new_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "flow-1", "name": "Remote flow", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_processes_v1" in url:
            if new_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "process-1", "name": "Remote process", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_lifecyclemodels_v1" in url:
            if new_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "model-1", "name": "Remote model", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/search_flows_latest" in url:
            if legacy_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse({"items": [{"id": "flow-1", "name": "Remote flow", "version": "1"}], "total": 1})
        if "/rest/v1/rpc/search_processes_latest" in url:
            if legacy_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse({"items": [{"id": "process-1", "name": "Remote process", "version": "1"}], "total": 1})
        if "/rest/v1/rpc/search_lifecyclemodels_latest" in url:
            if legacy_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
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


def test_local_sqlite_generates_persistent_credential_key(monkeypatch, tmp_path):
    key_file = tmp_path / "data_platform_credential.key"
    monkeypatch.setattr(settings, "data_platform_credential_key", "")
    monkeypatch.setattr(settings, "admin_token", "")
    monkeypatch.setattr(settings, "debug", False)
    monkeypatch.setattr(settings, "database_url", "sqlite:///local-test.db")
    monkeypatch.setattr(settings, "data_platform_credential_key_file", str(key_file))

    ciphertext = encrypt_credential({"api_key": "secret-api-key"})

    assert key_file.exists()
    assert key_file.read_text(encoding="utf-8").strip()
    assert "secret-api-key" not in ciphertext


def test_account_create_returns_safe_error_when_credential_key_missing(client, monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "data_platform_credential_key", "")
    monkeypatch.setattr(settings, "admin_token", "")
    monkeypatch.setattr(settings, "debug", False)
    monkeypatch.setattr(settings, "database_url", "postgresql://example/db")
    monkeypatch.setattr(settings, "data_platform_credential_key_file", str(tmp_path / "missing.key"))

    response = client.post(
        "/api/data-platforms/accounts",
        json={
            "platform": "tiangong",
            "alias": "TianGong",
            "auth_type": "basic",
            "credential": {"username": "user@example.com", "password": "secret-password"},
            "status": "active",
        },
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATA_PLATFORM_CREDENTIAL_KEY_REQUIRED"
    assert "secret-password" not in str(response.json())


def test_tiangong_auth_http_400_surfaces_login_failure(monkeypatch):
    def fake_urlopen(req, timeout):
        raise HTTPError(
            req.full_url,
            400,
            "Bad Request",
            hdrs=None,
            fp=io.BytesIO(b'{"error_description":"Invalid login credentials"}'),
        )

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)
    connector = TianGongSupabaseConnector(
        PlatformAccountContext(
            account_id="tg-1",
            platform="tiangong",
            alias="TianGong",
            base_url="https://tg.example",
            auth_type="basic",
            credential={"username": "user@example.com", "password": "wrong-password"},
            metadata={"publishable_key": "pub-key"},
        )
    )

    with pytest.raises(ConnectorError) as exc_info:
        connector.test_connection()

    assert exc_info.value.status_code == 400
    assert "username or password" in str(exc_info.value)
    assert "wrong-password" not in str(exc_info.value)


def test_account_update_with_new_credential_clears_old_validation(client):
    account_id = _create_tiangong_login_account(client)
    db = _db_module.SessionLocal()
    try:
        row = db.get(DataPlatformAccount, account_id)
        assert row is not None
        row.last_validated_at = datetime.utcnow()
        row.last_validation_status = "failed"
        row.last_validation_message = "Credential ciphertext failed integrity check."
        db.add(
            DataPlatformAccountSession(
                account_id=account_id,
                session_ciphertext=encrypt_credential({"access_token": "old-token"}),
                expires_at=datetime.utcnow() + timedelta(hours=1),
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.patch(
        f"/api/data-platforms/accounts/{account_id}",
        json={"credential": {"username": "new-user@example.com", "password": "new-password"}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["last_validated_at"] is None
    assert payload["last_validation_status"] is None
    assert payload["last_validation_message"] is None
    db = _db_module.SessionLocal()
    try:
        assert db.query(DataPlatformAccountSession).filter(DataPlatformAccountSession.account_id == account_id).count() == 0
    finally:
        db.close()


def test_mock_connector_connection_success_and_failure(client):
    ok_id = _create_mock_account(client)
    failed_id = _create_mock_account(client, metadata={"fail_connection": True})

    ok = client.post(f"/api/data-platforms/accounts/{ok_id}/test")
    failed = client.post(f"/api/data-platforms/accounts/{failed_id}/test")

    assert ok.status_code == 200
    assert ok.json()["ok"] is True
    assert failed.status_code == 200
    assert failed.json()["ok"] is False


def test_search_connector_error_returns_safe_502(client):
    account_id = _create_mock_account(client, metadata={"raise_search_error": True})

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=wind")

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "DATA_PLATFORM_CONNECTOR_ERROR"


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


def test_tiangong_account_login_gets_cached_session_without_exposing_password(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_login_account(client)

    checked = client.post(f"/api/data-platforms/accounts/{account_id}/test")
    searched = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote")

    assert checked.status_code == 200
    assert checked.json()["ok"] is True
    assert searched.status_code == 200
    password_grants = [call for call in calls if "grant_type=password" in call["url"]]
    assert len(password_grants) == 1
    assert "password" in password_grants[0]["body"]
    assert "password" not in str(checked.json())
    db = _db_module.SessionLocal()
    try:
        row = db.query(DataPlatformAccountSession).filter(DataPlatformAccountSession.account_id == account_id).first()
        assert row is not None
        assert "session-token" not in row.session_ciphertext
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


def test_tiangong_search_uses_latest_search_rpc_payload(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?q=remote&page=2&page_size=5&state_code=100")

    assert response.status_code == 200
    assert response.json()["total"] == 1
    search_call = next(call for call in calls if "/rest/v1/rpc/search_processes_latest" in call["url"])
    body = json.loads(search_call["body"])
    assert body["query_text"] == "remote"
    assert body["page_current"] == 2
    assert body["page_size"] == 5
    assert body["data_source"] == "tg"
    assert body["state_code_filter"] == 100
    assert body["type_of_data_set_filter"] == "all"
    assert body["this_user_id"] == "user-1"
    assert body["team_id_filter"] is None
    assert "sort_by" not in body
    assert "sort_direction" not in body


def test_tiangong_empty_query_uses_latest_rpc_with_open_state_code(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?page=1&page_size=10")

    assert response.status_code == 200
    assert response.json()["items"][0]["process_uuid"] == "process-1"
    search_call = next(call for call in calls if "/rest/v1/rpc/get_latest_process_versions" in call["url"])
    body = json.loads(search_call["body"])
    assert body["page_current"] == 1
    assert body["page_size"] == 10
    assert body["data_source"] == "tg"
    assert body["state_code_filter"] == 100
    assert body["type_of_data_set_filter"] == "all"


def test_tiangong_search_can_request_all_states(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?q=remote&page=1&page_size=10&state_scope=all")

    assert response.status_code == 200
    search_call = next(call for call in calls if "/rest/v1/rpc/search_processes_latest" in call["url"])
    body = json.loads(search_call["body"])
    assert body["state_code_filter"] is None


def test_tiangong_search_sends_flow_type_filter(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=1&page_size=10&state_code=100&flow_type=Product+flow")

    assert response.status_code == 200
    search_call = next(call for call in calls if "/rest/v1/rpc/search_flows_latest" in call["url"])
    body = json.loads(search_call["body"])
    assert body["state_code_filter"] == 100
    assert body["filter_condition"] == {"flowType": "Product flow"}


def test_tiangong_search_sends_process_type_filter(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?q=remote&page=1&page_size=10&state_code=100&process_type=LCI+result")

    assert response.status_code == 200
    search_call = next(call for call in calls if "/rest/v1/rpc/search_processes_latest" in call["url"])
    body = json.loads(search_call["body"])
    assert body["state_code_filter"] == 100
    assert body["type_of_data_set_filter"] == "LCI result"


def test_tiangong_preview_reads_detail_without_importing(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/process-1/preview?remote_version=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["remote_kind"] == "process"
    assert payload["title"] == "Remote process"
    assert payload["summary"]["exchange_count"] == 1
    assert payload["related"][0]["flow_id"] == "flow-1"
    assert any("/rest/v1/processes" in call["url"] for call in calls)
    db = _db_module.SessionLocal()
    try:
        assert db.query(FlowRecord).count() == 0
        assert db.query(ReferenceProcess).count() == 0
        assert db.query(Model).count() == 0
    finally:
        db.close()


def test_tiangong_search_falls_back_to_pgroonga_rpc_on_latest_search_rpc_404(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch, legacy_search_404=True)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=1&page_size=10")

    assert response.status_code == 200
    assert response.json()["items"][0]["flow_uuid"] == "flow-1"
    assert any("/rest/v1/rpc/search_flows_latest" in call["url"] for call in calls)
    assert any("/rest/v1/rpc/pgroonga_search_flows_v1" in call["url"] for call in calls)


def test_tiangong_search_reports_attempted_rpcs_when_all_search_rpcs_fail(client, monkeypatch):
    _install_fake_tiangong_http(monkeypatch, new_search_404=True, legacy_search_404=True)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=1&page_size=10")

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "DATA_PLATFORM_CONNECTOR_ERROR"
    assert "search_flows_latest" in response.json()["detail"]["message"]
    assert "pgroonga_search_flows_v1" in response.json()["detail"]["message"]


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
    assert flow_sync.json()["tidas_import_job_id"]
    assert process_sync.json()["tidas_import_report"]["import_type"] == "processes"
    assert model_sync.json()["tidas_import_report"]["created_projects"]
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
        assert db.query(DebugDiagnostic).count() >= 3
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
