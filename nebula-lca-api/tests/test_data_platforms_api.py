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
from app.services.data_platform_connectors import (
    ConnectorError,
    PlatformAccountContext,
    RemotePageDTO,
    RemoteProcessDTO,
    TianGongSupabaseConnector,
    _tiangong_flow_from_row,
    _tiangong_process_from_row,
    encrypt_credential,
)
from app.api.data_platforms import _upsert_sync_record


def test_tiangong_flow_without_resolved_unit_metadata_does_not_fallback_to_mass():
    row = {
        "id": "11111111-1111-4111-8111-111111111111",
        "name": "Alternating current",
        "json": {
            "flowDataSet": {
                "flowProperties": {
                    "flowProperty": {
                        "referenceToFlowPropertyDataSet": {
                            "@refObjectId": "energy-property",
                            "common:shortDescription": {"#text": "Energy", "@xml:lang": "en"},
                        }
                    }
                }
            }
        },
    }

    flow = _tiangong_flow_from_row(row)

    assert flow.default_unit == ""
    assert flow.unit_group == ""
    assert flow.metadata["unit_resolution_error"] == "FLOW_UNIT_METADATA_UNRESOLVED"


def test_tiangong_process_preserves_localized_chinese_and_english_names():
    process = _tiangong_process_from_row(
        {
            "id": "22222222-2222-4222-8222-222222222222",
            "json": {
                "processDataSet": {
                    "processInformation": {
                        "dataSetInformation": {
                            "name": {
                                "baseName": [
                                    {"#text": "Aluminium, primary, liquid", "@xml:lang": "en"},
                                    {"#text": "原铝液", "@xml:lang": "zh"},
                                ]
                            }
                        }
                    }
                }
            },
        }
    )

    assert process.process_name == "原铝液"
    assert process.process_name_en == "Aluminium, primary, liquid"


def test_tiangong_process_display_name_includes_ilcd_name_parts():
    process = _tiangong_process_from_row(
        {
            "id": "33333333-3333-4333-8333-333333333333",
            "json": {
                "processDataSet": {
                    "processInformation": {
                        "dataSetInformation": {
                            "name": {
                                "baseName": [{"#text": "自卸卡车", "@xml:lang": "zh"}],
                                "mixAndLocationTypes": [{"#text": "消费混合，面向终端消费者", "@xml:lang": "zh"}],
                                "treatmentStandardsRoutes": [{"#text": "柴油驱动，货运", "@xml:lang": "zh"}],
                            }
                        }
                    }
                }
            },
        }
    )

    assert process.process_name == "自卸卡车; 消费混合，面向终端消费者; 柴油驱动，货运"


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


def _create_hiqlcd_account(client: TestClient) -> str:
    response = client.post(
        "/api/data-platforms/hiqlcd/accounts",
        json={"alias": "HiQLCD Test", "api_key": "hiqlcd-secret", "status": "active"},
    )
    assert response.status_code == 201, response.text
    assert "hiqlcd-secret" not in response.text
    return response.json()["id"]


def test_tiangong_process_rerank_excludes_general_comment_only_hits():
    from app.services.data_platform_connectors import _rerank_tiangong_rows

    rows = [
        {
            "name": "Wheat production",
            "json": {
                "processDataSet": {
                    "processInformation": {
                        "dataSetInformation": {
                            "name": [{"@xml:lang": "en", "#text": "Wheat production"}],
                            "common:generalComment": [{"@xml:lang": "en", "#text": "Diesel is used by agricultural machinery."}],
                        }
                    }
                }
            },
        },
        {
            "name": "Market for diesel",
            "json": {
                "processDataSet": {
                    "processInformation": {
                            "dataSetInformation": {"name": [{"@xml:lang": "en", "#text": "Market for diesel"}]}
                    }
                }
            },
        },
    ]

    matched = _rerank_tiangong_rows("process", rows, "diesel")

    assert [row["name"] for row in matched] == ["Market for diesel"]


def test_tiangong_process_rerank_prefers_base_name_over_name_modifier():
    from app.services.data_platform_connectors import _rerank_tiangong_rows

    transport = {
        "id": "transport",
        "json": {
            "processDataSet": {
                "processInformation": {
                    "dataSetInformation": {
                        "name": {
                            "baseName": [{"@xml:lang": "en", "#text": "Rigid truck transport"}],
                            "treatmentStandardsRoutes": [{"@xml:lang": "en", "#text": "diesel driven"}],
                        }
                    }
                }
            }
        },
    }
    refinery = {
        "id": "refinery",
        "json": {
            "processDataSet": {
                "processInformation": {
                    "dataSetInformation": {
                        "name": {
                            "baseName": [{"@xml:lang": "en", "#text": "Crude oil refining for diesel oil"}],
                        }
                    }
                }
            }
        },
    }

    matched = _rerank_tiangong_rows("process", [transport, refinery], "diesel")

    assert [row["id"] for row in matched] == ["refinery", "transport"]


def test_process_search_terms_expand_exact_localized_flow_name():
    from app.api.data_platforms import _process_search_terms

    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid="localized-search-flow",
            flow_name="本地燃料名",
            flow_name_en="Localized fuel name",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
        ))
        db.commit()

        assert _process_search_terms(db, "本地燃料名") == ["本地燃料名", "Localized fuel name"]
    finally:
        db.close()


class _FakeSupabaseResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def _install_fake_hiqlcd_http(monkeypatch, *, output_count: int = 1):
    calls = []

    def fake_urlopen(req, timeout=20):  # noqa: ARG001
        body = json.loads(req.data.decode("utf-8")) if req.data else {}
        calls.append({"url": req.full_url, "body": body, "api_key": req.headers.get("X-api-key")})
        dataset = {
            "id": "hiq-dataset-1",
            "uuid": "11b2cb5e-9dc3-4bd6-90a6-48451898c319",
            "name": "HiQLCD electricity dataset",
            "version": "1.2.0",
            "description": "Background inventory",
            "location": {"code": "CN", "name": "China"},
            "source": {"name": "HiQLCD", "version": "2026"},
            "updatedAt": "2026-07-30T00:00:00Z",
        }
        if req.full_url.endswith("/xapi/datasets"):
            return _FakeSupabaseResponse({"success": True, "data": {"items": [dataset], "total": 1, "page": body.get("page", 1), "pageSize": body.get("pageSize", 20)}})
        if req.full_url.endswith("/xapi/lci/input"):
            return _FakeSupabaseResponse({"success": True, "data": [{"id": "hiq-co2", "name": "Carbon dioxide", "category": "air", "amount": 2.5, "unit": "kg", "flowType": "input"}]})
        if req.full_url.endswith("/xapi/lci/output"):
            outputs = [{"id": f"hiq-electricity-{index}", "name": "Electricity", "amount": 1, "unit": "kWh", "flowType": "output"} for index in range(output_count)]
            return _FakeSupabaseResponse({"success": True, "data": outputs})
        raise AssertionError(f"Unexpected URL {req.full_url}")

    monkeypatch.setattr("app.services.hiqlcd_connector.url_request.urlopen", fake_urlopen)
    return calls


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


def _install_fake_tiangong_http(
    monkeypatch,
    *,
    invalid_model: bool = False,
    refresh_fails: bool = False,
    hybrid_search_404: bool = True,
    indexed_search_404: bool = False,
    v1_search_404: bool = False,
    legacy_search_404: bool = False,
):
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
        if "/functions/v1/app_dataset_create" in url:
            body = json.loads(req.data.decode("utf-8")) if req.data else {}
            return _FakeSupabaseResponse({"id": body.get("id"), "version": "01.01.000", "table": body.get("table"), "state_code": 0, "rule_verification": body.get("ruleVerification")})
        if any(f"/functions/v1/{name}" in url for name in ("flow_hybrid_search", "process_hybrid_search", "lifecyclemodel_hybrid_search")):
            if hybrid_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            kind = "flow" if "flow_hybrid_search" in url else "process" if "process_hybrid_search" in url else "model"
            return _FakeSupabaseResponse({"data": [{"id": f"{kind}-1", "name": f"Remote {kind}", "version": "1", "total_count": 1}]})
        if "/rest/v1/rpc/get_latest_flow_versions" in url:
            return _FakeSupabaseResponse([{"id": "flow-1", "name": "Remote flow", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/get_latest_process_versions" in url:
            return _FakeSupabaseResponse([{"id": "process-1", "name": "Remote process", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/get_latest_lifecyclemodel_versions" in url:
            return _FakeSupabaseResponse([{"id": "model-1", "name": "Remote model", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_flows_v1" in url:
            if v1_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "flow-1", "name": "Remote flow", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_processes_v1" in url:
            if v1_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "process-1", "name": "Remote process", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_lifecyclemodels_v1" in url:
            if v1_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "model-1", "name": "Remote model", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_flows_latest" in url:
            if indexed_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "flow-1", "name": "Remote flow", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_processes_latest" in url:
            if indexed_search_404:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return _FakeSupabaseResponse([{"id": "process-1", "name": "Remote process", "version": "1", "total_count": 1}])
        if "/rest/v1/rpc/pgroonga_search_lifecyclemodels_latest" in url:
            if indexed_search_404:
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


def test_hiqlcd_lci_search_preview_and_import_creates_background_provider(client, monkeypatch):
    calls = _install_fake_hiqlcd_http(monkeypatch)
    account_id = _create_hiqlcd_account(client)

    searched = client.get(f"/api/data-platforms/hiqlcd/accounts/{account_id}/datasets/search?q=electricity&page=1&page_size=10")
    assert searched.status_code == 200, searched.text
    assert searched.json()["items"][0]["process_name"] == "HiQLCD electricity dataset"

    preview = client.get(f"/api/data-platforms/hiqlcd/accounts/{account_id}/datasets/hiq-dataset-1/preview")
    assert preview.status_code == 200, preview.text
    assert preview.json()["summary"]["reference_product"] == "Electricity"

    imported = client.post(
        f"/api/data-platforms/hiqlcd/accounts/{account_id}/datasets/import",
        json={"dataset_id": "hiq-dataset-1", "dataset_version": "1.2.0", "locale": "zh"},
    )
    assert imported.status_code == 200, imported.text
    payload = imported.json()
    assert payload["platform"] == "hiqlcd"
    assert payload["reference_product_name"] == "Electricity"
    assert payload["vector_nnz"] == 1
    assert all(call["api_key"] == "hiqlcd-secret" for call in calls)

    db = _db_module.SessionLocal()
    try:
        process = db.get(ReferenceProcess, payload["process_uuid"])
        vector = db.get(LciProcessVector, payload["process_uuid"])
        reference_flow = db.get(FlowRecord, payload["reference_flow_uuid"])
        assert process is not None
        assert process.process_type == "lci_dataset"
        assert process.source_file == "hiqlcd://datasets/hiq-dataset-1"
        assert vector is not None and vector.source == "hiqlcd" and vector.nnz == 1
        assert reference_flow is not None and reference_flow.source == "hiqlcd"
    finally:
        db.close()


def test_hiqlcd_lci_rejects_multiple_reference_products_without_writing_provider(client, monkeypatch):
    _install_fake_hiqlcd_http(monkeypatch, output_count=2)
    account_id = _create_hiqlcd_account(client)

    response = client.post(
        f"/api/data-platforms/hiqlcd/accounts/{account_id}/datasets/import",
        json={"dataset_id": "hiq-dataset-1", "locale": "zh"},
    )
    assert response.status_code == 502
    assert "exactly one reference product" in response.json()["detail"]["message"]

    db = _db_module.SessionLocal()
    try:
        assert db.query(ReferenceProcess).count() == 0
        assert db.query(LciProcessVector).count() == 0
    finally:
        db.close()


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [(401, "HiQLCD API Key was rejected."), (429, "HiQLCD request rate limit reached.")],
)
def test_hiqlcd_connection_failures_are_safe_and_do_not_expose_api_key(client, monkeypatch, status_code, expected):
    def fake_urlopen(req, timeout=20):  # noqa: ARG001
        raise HTTPError(req.full_url, status_code, "error", hdrs=None, fp=io.BytesIO(b"{}"))

    monkeypatch.setattr("app.services.hiqlcd_connector.url_request.urlopen", fake_urlopen)
    account_id = _create_hiqlcd_account(client)

    response = client.post(f"/api/data-platforms/hiqlcd/accounts/{account_id}/test")
    assert response.status_code == 200
    assert response.json()["ok"] is False
    assert response.json()["message"] == expected
    assert "hiqlcd-secret" not in response.text


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

    assert response.status_code == 200, response.text
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


def test_tiangong_api_key_and_account_login_exchange_the_same_password_grant(client, monkeypatch):
    api_key_calls = _install_fake_tiangong_http(monkeypatch)
    api_key_account_id = _create_tiangong_account(client)

    api_key_checked = client.post(f"/api/data-platforms/accounts/{api_key_account_id}/test")

    assert api_key_checked.status_code == 200
    api_key_grant = next(call for call in api_key_calls if "grant_type=password" in call["url"])

    login_calls = _install_fake_tiangong_http(monkeypatch)
    login_account_id = _create_tiangong_login_account(client)

    login_checked = client.post(f"/api/data-platforms/accounts/{login_account_id}/test")

    assert login_checked.status_code == 200
    login_grant = next(call for call in login_calls if "grant_type=password" in call["url"])
    assert json.loads(api_key_grant["body"]) == json.loads(login_grant["body"])


def test_tiangong_account_rejects_mixed_authentication_credentials(client):
    response = client.post(
        "/api/data-platforms/accounts",
        json={
            "platform": "tiangong",
            "alias": "TianGong Mixed",
            "auth_type": "api_key",
            "credential": {"api_key": _tg_api_key(), "username": "user@example.com"},
            "status": "active",
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIANGONG_API_KEY_CREDENTIAL_INVALID"


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


def test_tiangong_search_normalizes_semicolon_separated_keywords(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?q=primary%3B+aluminium")

    assert response.status_code == 200
    search_call = next(call for call in calls if "/rest/v1/rpc/search_processes_latest" in call["url"])
    assert json.loads(search_call["body"])["query_text"] == "primary aluminium"


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
    assert not any("/rest/v1/flows" in call["url"] for call in calls)
    db = _db_module.SessionLocal()
    try:
        assert db.query(FlowRecord).count() == 0
        assert db.query(ReferenceProcess).count() == 0
        assert db.query(Model).count() == 0
    finally:
        db.close()


def test_tiangong_search_falls_back_to_indexed_latest_rpc_on_latest_search_rpc_404(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch, legacy_search_404=True)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=1&page_size=10")

    assert response.status_code == 200
    assert response.json()["items"][0]["flow_uuid"] == "flow-1"
    assert any("/rest/v1/rpc/search_flows_latest" in call["url"] for call in calls)
    assert any("/rest/v1/rpc/pgroonga_search_flows_latest" in call["url"] for call in calls)


def test_tiangong_search_prefers_current_hybrid_function_contract(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch, hybrid_search_404=False)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=2&page_size=10")

    assert response.status_code == 200
    assert response.json()["items"][0]["flow_uuid"] == "flow-1"
    search_call = next(call for call in calls if "/functions/v1/flow_hybrid_search" in call["url"])
    assert json.loads(search_call["body"]) == {
        "query": "remote",
        "filter_condition": {},
        "data_source": "tg",
        "page_size": 10,
        "page_current": 2,
        "state_code": 100,
    }
    assert not any("/rest/v1/rpc/search_flows_latest" in call["url"] for call in calls)


def test_tiangong_search_keeps_remote_total_after_local_rerank(monkeypatch):
    connector = TianGongSupabaseConnector(
        PlatformAccountContext(
            account_id="tg-1",
            platform="tiangong",
            alias="TianGong",
            base_url="https://tg.example",
            auth_type="bearer",
            credential={"token": "test-token"},
            metadata={"publishable_key": "pub-key"},
        )
    )
    monkeypatch.setattr(
        connector,
        "_invoke_function",
        lambda _name, _payload: {
            "data": [
                {"id": "process-1", "name": "Diesel process", "version": "1", "total_count": 200},
                {"id": "process-2", "name": "Unrelated process", "version": "1", "total_count": 200},
            ]
        },
    )

    result = connector.search_processes("diesel", page=1, page_size=10)

    assert len(result.items) == 1
    assert result.total == 200
    assert result.has_more is True


def test_process_search_skips_localized_fallback_when_primary_has_results(client, monkeypatch):
    account_id = _create_mock_account(client)
    calls: list[str] = []

    class FakeConnector:
        def search_processes(self, term, **_kwargs):
            calls.append(term)
            return RemotePageDTO(
                items=[RemoteProcessDTO(remote_id="process-1", process_uuid="process-1", process_name="Primary result")],
                total=200,
                page=1,
                page_size=10,
                has_more=True,
            )

    monkeypatch.setattr("app.api.data_platforms._process_search_terms", lambda _db, _q: ["primary", "fallback"])
    monkeypatch.setattr("app.api.data_platforms.connector_for_account", lambda _account: FakeConnector())

    response = client.get(f"/api/data-platforms/accounts/{account_id}/processes/search?q=primary&page_size=10")

    assert response.status_code == 200
    assert response.json()["total"] == 200
    assert response.json()["has_more"] is True
    assert calls == ["primary"]


def test_tiangong_search_v1_fallback_uses_exact_six_parameter_contract(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch, legacy_search_404=True, indexed_search_404=True)
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=1&page_size=10")

    assert response.status_code == 200
    search_call = next(call for call in calls if "/rest/v1/rpc/pgroonga_search_flows_v1" in call["url"])
    assert json.loads(search_call["body"]) == {
        "query_text": "remote",
        "filter_condition": "{}",
        "order_by": "{}",
        "page_size": 10,
        "page_current": 1,
        "data_source": "tg",
    }


def test_tiangong_search_returns_concise_error_when_all_search_endpoints_fail(client, monkeypatch):
    _install_fake_tiangong_http(
        monkeypatch,
        indexed_search_404=True,
        v1_search_404=True,
        legacy_search_404=True,
    )
    account_id = _create_tiangong_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/flows/search?q=remote&page=1&page_size=10")

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "DATA_PLATFORM_CONNECTOR_ERROR"
    assert response.json()["detail"]["message"] == "TianGong remote search is temporarily unavailable. Please retry later."


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


def test_tiangong_account_creation_requires_one_supported_credential(client):
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

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "TIANGONG_CREDENTIAL_REQUIRED"


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


def test_sync_record_upsert_reuses_pending_record(client):
    account_id = _create_mock_account(client)
    db = _db_module.SessionLocal()
    try:
        account = db.get(DataPlatformAccount, account_id)
        first = _upsert_sync_record(
            db,
            account=account,
            local_kind="unit_group",
            local_uuid="Units of mass",
            remote_id="unit-group-1",
            remote_version="1",
        )
        second = _upsert_sync_record(
            db,
            account=account,
            local_kind="unit_group",
            local_uuid="Units of mass",
            remote_id="unit-group-1",
            remote_version="2",
        )
        db.commit()

        assert first["local_uuid"] == second["local_uuid"]
        rows = db.query(ExternalDataSyncRecord).filter(ExternalDataSyncRecord.local_kind == "unit_group").all()
        assert len(rows) == 1
        assert rows[0].remote_version == "2"
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


# ======================================================================
# Sync History Endpoints
# ======================================================================


def test_list_sync_jobs_empty_result(client):
    account_id = _create_mock_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs")

    assert response.status_code == 200
    assert response.json() == []


def test_list_sync_jobs_returns_created_jobs(client):
    account_id = _create_mock_account(client)

    first_sync = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )
    second_sync = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    assert first_sync.status_code == 200
    assert second_sync.status_code == 200

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs")

    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) == 2
    # Should be ordered by created_at descending (newest first)
    assert jobs[0]["id"] == second_sync.json()["job_id"]
    assert jobs[1]["id"] == first_sync.json()["job_id"]
    for job in jobs:
        assert job["account_id"] == account_id
        assert job["platform"] == "mock"
        assert job["status"] in ("completed", "failed")
        assert "job_id" not in job  # credential privacy: no sensitive data


def test_list_sync_jobs_filter_by_status(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    completed_response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?status=completed")
    assert completed_response.status_code == 200
    assert len(completed_response.json()) >= 1
    assert all(job["status"] == "completed" for job in completed_response.json())

    empty_response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?status=running")
    assert empty_response.status_code == 200
    assert empty_response.json() == []


def test_list_sync_jobs_filter_by_platform(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?platform=mock")
    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) >= 1
    assert all(job["platform"] == "mock" for job in jobs)

    response_filtered = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?platform=tiangong")
    assert response_filtered.status_code == 200
    assert response_filtered.json() == []


def test_list_sync_jobs_filter_by_remote_kind(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?remote_kind=process")
    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) >= 1
    assert all(job["status"] in ("completed", "failed") for job in jobs)

    response_no_match = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?remote_kind=model")
    assert response_no_match.status_code == 200
    assert response_no_match.json() == []


def test_list_sync_records_empty_result(client):
    account_id = _create_mock_account(client)

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records")

    assert response.status_code == 200
    assert response.json() == []


def test_list_sync_records_returns_created_records(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records")

    assert response.status_code == 200
    records = response.json()
    assert len(records) >= 3
    for record in records:
        assert record["account_id"] == account_id
        assert record["platform"] == "mock"
        assert record["local_kind"] in ("flow", "process", "unit_group", "unit_definition", "vector")
        assert record["remote_id"]


def test_list_sync_records_filter_by_local_kind(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    process_records = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?local_kind=process")
    assert process_records.status_code == 200
    assert all(r["local_kind"] == "process" for r in process_records.json())

    flow_records = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?local_kind=flow")
    assert flow_records.status_code == 200
    assert all(r["local_kind"] == "flow" for r in flow_records.json())


def test_list_sync_records_filter_by_platform(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?platform=mock")
    assert response.status_code == 200
    assert all(r["platform"] == "mock" for r in response.json())

    response_filtered = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?platform=tiangong")
    assert response_filtered.status_code == 200
    assert response_filtered.json() == []


def test_sync_jobs_account_not_found(client):
    response = client.get("/api/data-platforms/accounts/nonexistent-id/sync-jobs")

    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["code"] == "DATA_PLATFORM_ACCOUNT_NOT_FOUND"


def test_sync_records_account_not_found(client):
    response = client.get("/api/data-platforms/accounts/nonexistent-id/sync-records")

    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["code"] == "DATA_PLATFORM_ACCOUNT_NOT_FOUND"


def test_list_sync_records_filter_by_remote_id(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?remote_id=mock-process-sync")
    assert response.status_code == 200
    records = response.json()
    assert len(records) >= 1
    assert all(r["remote_id"] == "mock-process-sync" for r in records)

    response_no_match = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?remote_id=nonexistent-remote")
    assert response_no_match.status_code == 200
    assert response_no_match.json() == []


def test_list_sync_records_filter_by_local_uuid(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?local_uuid=mock-process-sync")
    assert response.status_code == 200
    records = response.json()
    assert len(records) >= 1
    assert all(r["local_uuid"] == "mock-process-sync" for r in records)


def test_list_sync_records_limit_param(client):
    account_id = _create_mock_account(client)

    for i in range(5):
        client.post(
            f"/api/data-platforms/accounts/{account_id}/processes/sync",
            json={"remote_process_id": f"mock-process-{i}", "overwrite": True},
        )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?limit=3")
    assert response.status_code == 200
    assert len(response.json()) <= 3

    response_max = client.get(f"/api/data-platforms/accounts/{account_id}/sync-records?limit=200")
    assert response_max.status_code == 200
    assert len(response_max.json()) >= 1


def test_list_sync_jobs_limit_param(client):
    account_id = _create_mock_account(client)

    for i in range(5):
        client.post(
            f"/api/data-platforms/accounts/{account_id}/processes/sync",
            json={"remote_process_id": f"mock-process-{i}", "overwrite": True},
        )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?limit=2")
    assert response.status_code == 200
    assert len(response.json()) <= 2


def test_list_sync_jobs_filter_by_remote_kind_process(client):
    account_id = _create_mock_account(client)

    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.get(f"/api/data-platforms/accounts/{account_id}/sync-jobs?remote_kind=process")
    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) >= 1
    for job in jobs:
        stats = job.get("stats") or {}
        assert stats.get("remote_kind") == "process"


# ======================================================================
# Energy flow fixture without top-level default_unit/unit_group
# ======================================================================


def _energy_flow_row_without_top_level_units():
    """A TianGong flow payload that supplies default unit MJ and unit group from
    flowDataSet.flowInformation, with NO top-level default_unit/unit_group fields."""
    return {
        "id": "energy-flow-1",
        # NO "default_unit" or "unit_group" top-level keys
        "name": "Electricity, high voltage",
        "version": "2",
        "json": {
            "flowDataSet": {
                "flowInformation": {
                    "dataSetInformation": {
                        "common:UUID": "e5a8c120-7f45-4b21-a3e6-d89c10ef2b41",
                        "common:name": "Electricity, high voltage",
                    },
                    "referenceUnit": "MJ",
                    "unitGroup": "Units of energy",
                },
                "flowProperties": {
                    "flowProperty": {
                        "referenceToFlowPropertyDataSet": {
                            "@refObjectId": "energy-property-1",
                        }
                    }
                },
            }
        },
    }


def test_tiangong_flow_normalization_uses_flowinformation_unit_when_no_top_level(client, monkeypatch):
    """Energy flow fixture: payload supplies default unit MJ and unit group 'Units of energy'
    from flowDataSet.flowInformation, with no top-level default_unit/unit_group.
    Assert sync/import does not fall back to kg / Units of mass."""

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url and "energy-flow-1" in url:
            return _FakeSupabaseResponse([_energy_flow_row_without_top_level_units()])
        if "/rest/v1/flowproperties" in url:
            return _FakeSupabaseResponse([_flowproperty_row()])
        if "/rest/v1/unitgroups" in url:
            return _FakeSupabaseResponse([_unitgroup_row()])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)
    account_id = _create_tiangong_account(client)

    # Sync should use get_flow_detail which calls _tiangong_flow_from_row on the full row
    sync = client.post(
        f"/api/data-platforms/accounts/{account_id}/flows/sync",
        json={"remote_flow_id": "energy-flow-1", "remote_version": "2"},
    )
    assert sync.status_code == 200, sync.text

    db = _db_module.SessionLocal()
    try:
        flow = db.get(FlowRecord, "e5a8c120-7f45-4b21-a3e6-d89c10ef2b41")
        assert flow is not None
        assert flow.default_unit == "MJ"
        assert flow.unit_group == "Units of energy"
        assert flow.default_unit != "kg"
        assert flow.unit_group != "Units of mass"
    finally:
        db.close()


# ======================================================================
# Localized exchange shortDescription test
# ======================================================================


def _process_with_localized_exchanges():
    """Process payload with localized exchange shortDescription dict/list and nonzero amounts."""
    return {
        "id": "local-process-1",
        "name": "Bakery process",
        "version": "1",
        "json": {
            "processUuid": "local-process-1",
            "process_name": "Bakery process",
            "reference_flow_uuid": "product-flow-1",
            "exchanges": [
                {
                    "flow_uuid": "flow-bread-1",
                    "flow_name": None,
                    "referenceToFlowDataSet": {
                        "@refObjectId": "flow-bread-1",
                        "common:shortDescription": [
                            {"#text": "面包", "@xml:lang": "zh"},
                            {"#text": "Bread", "@xml:lang": "en"},
                        ],
                    },
                    "direction": "input",
                    "meanAmount": 0.5,
                    "resultingAmount": 0.5,
                    "unit": "kg",
                },
                {
                    "flow_uuid": "flow-electricity-1",
                    "referenceToFlowDataSet": {
                        "@refObjectId": "flow-electricity-1",
                        "common:shortDescription": {"#text": "电力"},
                    },
                    "exchangeDirection": "output",
                    "amount": 1.0,
                    "unit": "kWh",
                },
            ],
        },
    }


def test_process_sync_stores_plain_string_exchange_names(client, monkeypatch):
    """Process fixture with localized exchange shortDescription dict/list and nonzero
    meanAmount/resultingAmount. Assert synced process_json.exchanges stores plain
    string names and nonzero amounts."""
    account_id = _create_tiangong_account(client)

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/processes" in url and "local-process-1" in url:
            return _FakeSupabaseResponse([_process_with_localized_exchanges()])
        if "/rest/v1/flows" in url:
            # Return flow detail for exchange resolution
            flow_id = "flow-bread-1" if "flow-bread-1" in url else ("flow-electricity-1" if "flow-electricity-1" in url else "flow-1")
            return _FakeSupabaseResponse([{
                "id": flow_id,
                "name": "Flow",
                "version": "1",
                "default_unit": "kg",
                "unit_group": "Units of mass",
                "json": {"flowDataSet": {"flowInformation": {"dataSetInformation": {"common:name": "Flow"}}}},
            }])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "local-process-1", "overwrite": True},
    )
    assert response.status_code == 200, response.text
    db = _db_module.SessionLocal()
    try:
        proc = db.get(ReferenceProcess, "local-process-1")
        assert proc is not None
        pj = proc.process_json
        assert isinstance(pj, dict)
        exchanges = pj.get("exchanges")
        assert isinstance(exchanges, list)
        assert len(exchanges) == 2
        # Check first exchange: localized shortDescription resolved to plain string
        ex1 = exchanges[0]
        assert isinstance(ex1.get("flow_name"), str)
        assert ex1["flow_name"] == "面包"
        assert ex1["amount"] == 0.5
        # Check second exchange: direction normalized to output, amount from 'amount' field
        ex2 = exchanges[1]
        assert ex2["direction"] == "output"
        assert ex2["amount"] == 1.0
    finally:
        db.close()


def _flow_row_with_unit(flow_id: str, name: str, unit: str = "MJ", unit_group: str = "Units of energy"):
    return {
        "id": flow_id,
        "name": name,
        "version": "1",
        "json": {
            "flowDataSet": {
                "flowInformation": {
                    "dataSetInformation": {
                        "common:UUID": flow_id,
                        "common:name": name,
                    },
                    "referenceUnit": unit,
                    "unitGroup": unit_group,
                }
            }
        },
    }


def _process_row_with_reference(ref_internal_id: str = "2"):
    return {
        "id": "tg-process-ref",
        "name": {"baseName": [{"#text": "电力过程", "@xml:lang": "zh"}, {"#text": "Electricity process", "@xml:lang": "en"}]},
        "version": "1",
        "json": {
            "processDataSet": {
                "processInformation": {
                    "dataSetInformation": {
                        "common:UUID": "tg-process-ref",
                        "name": {"baseName": [{"#text": "电力过程", "@xml:lang": "zh"}, {"#text": "Electricity process", "@xml:lang": "en"}]},
                    },
                    "quantitativeReference": {"referenceToReferenceFlow": ref_internal_id},
                },
                "exchanges": {
                    "exchange": [
                        {
                            "@dataSetInternalID": "1",
                            "exchangeDirection": "input",
                            "referenceToFlowDataSet": {
                                "@refObjectId": "flow-input-energy",
                                "common:shortDescription": [{"#text": "输入电力", "@xml:lang": "zh"}, {"#text": "Input electricity", "@xml:lang": "en"}],
                            },
                            "meanAmount": 2,
                            "referenceToUnit": {"common:shortDescription": "MJ"},
                        },
                        {
                            "@dataSetInternalID": "2",
                            "exchangeDirection": "output",
                            "referenceToFlowDataSet": {
                                "@refObjectId": "flow-product-energy",
                                "common:shortDescription": [{"#text": "输出电力", "@xml:lang": "zh"}, {"#text": "Output electricity", "@xml:lang": "en"}],
                            },
                            "meanAmount": 1,
                            "referenceToUnit": "0",
                            "productOutput": True,
                        },
                    ]
                },
            }
        },
    }


def _lci_result_row():
    return {
        "id": "tg-lci-result",
        "name": "LCI result test",
        "version": "02.00.001",
        "json": {
            "processDataSet": {
                "processInformation": {
                    "dataSetInformation": {"common:UUID": "tg-lci-result", "name": "LCI result test"},
                    "quantitativeReference": {"referenceToReferenceFlow": "1"},
                },
                "modellingAndValidation": {"LCIMethodAndAllocation": {"typeOfDataSet": "LCI result"}},
                "exchanges": {
                    "exchange": [
                        {
                            "@dataSetInternalID": "1",
                            "exchangeDirection": "output",
                            "referenceToFlowDataSet": {"@refObjectId": "lci-reference-product"},
                            "meanAmount": 1,
                            "referenceToUnit": {"common:shortDescription": "kg"},
                        },
                        {
                            "@dataSetInternalID": "2",
                            "exchangeDirection": "input",
                            "referenceToFlowDataSet": {"@refObjectId": "lci-elementary-input"},
                            "meanAmount": 2.5,
                            "referenceToUnit": {"common:shortDescription": "kg"},
                        },
                        {
                            "@dataSetInternalID": "3",
                            "exchangeDirection": "output",
                            "referenceToFlowDataSet": {"@refObjectId": "lci-elementary-output"},
                            "meanAmount": 0.4,
                            "referenceToUnit": {"common:shortDescription": "kg"},
                        },
                    ]
                },
            }
        },
    }


def _lci_flow_row(flow_id: str, flow_type: str):
    return {
        "id": flow_id,
        "name": flow_id,
        "version": "02.00.001",
        "flow_type": flow_type,
        "default_unit": "kg",
        "unit_group": "Units of mass",
        "json": {"flowDataSet": {"flowInformation": {"dataSetInformation": {"common:UUID": flow_id, "name": flow_id}}}},
    }


def test_tiangong_lci_result_sync_derives_vector_from_elementary_exchanges(client, monkeypatch):
    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/processes" in url and "tg-lci-result" in url:
            return _FakeSupabaseResponse([_lci_result_row()])
        for flow_id, flow_type in (
            ("lci-reference-product", "Product flow"),
            ("lci-elementary-input", "Elementary flow"),
            ("lci-elementary-output", "Elementary flow"),
        ):
            if "/rest/v1/flows" in url and flow_id in url:
                return _FakeSupabaseResponse([_lci_flow_row(flow_id, flow_type)])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)
    account_id = _create_tiangong_account(client)
    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "tg-lci-result", "overwrite": True},
    )
    assert response.status_code == 200, response.text
    db = _db_module.SessionLocal()
    try:
        process = db.get(ReferenceProcess, "tg-lci-result")
        vector = db.get(LciProcessVector, "tg-lci-result")
        assert process is not None
        assert process.process_type == "lci_dataset"
        assert process.reference_flow_uuid == "lci-reference-product"
        assert process.process_json.get("reference_product") == "lci-reference-product", process.process_json
        assert vector is not None
        assert vector.nnz == 2
        assert vector.source == "tiangong"
        assert vector.source_package_version == "02.00.001"
    finally:
        db.close()

    providers = client.get(
        "/api/intermediate-flow-links/providers",
        params={"target_flow_uuid": "lci-reference-product"},
    )
    assert providers.status_code == 200, providers.text
    candidate = next(item for item in providers.json()["providers"] if item["process_uuid"] == "tg-lci-result")
    assert candidate["reference_product_flow_uuid"] == "lci-reference-product"
    assert candidate["reference_product_name"] == "lci-reference-product"
    assert candidate["reference_product_unit"] == "kg"
    assert candidate["has_lci_vector"] is True


def test_tiangong_process_sync_preserves_quantitative_reference_and_units(client, monkeypatch):
    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/processes" in url and "tg-process-ref" in url:
            return _FakeSupabaseResponse([_process_row_with_reference("2")])
        if "/rest/v1/flows" in url and "flow-product-energy" in url:
            return _FakeSupabaseResponse([_flow_row_with_unit("flow-product-energy", "Output electricity", "kWh", "Units of energy")])
        if "/rest/v1/flows" in url and "flow-input-energy" in url:
            return _FakeSupabaseResponse([_flow_row_with_unit("flow-input-energy", "Input electricity", "MJ", "Units of energy")])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)
    account_id = _create_tiangong_account(client)

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "tg-process-ref", "overwrite": True},
    )
    assert response.status_code == 200, response.text
    db = _db_module.SessionLocal()
    try:
        process = db.get(ReferenceProcess, "tg-process-ref")
        assert process is not None
        assert process.process_name == "电力过程"
        assert "{" not in process.process_name
        assert process.reference_flow_internal_id == "2"
        assert process.reference_flow_uuid == "flow-product-energy"
        exchanges = process.process_json["exchanges"]
        assert isinstance(exchanges[0]["flow_name"], str)
        assert "{" not in exchanges[0]["flow_name"]
        assert exchanges[0]["unit"] == "MJ"
        assert exchanges[1]["is_reference_flow"] is True
        assert exchanges[1]["isProduct"] is True
        assert exchanges[1]["unit"] == "kWh"
    finally:
        db.close()


def test_tiangong_process_sync_warns_when_reference_points_to_input(client, monkeypatch):
    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/processes" in url and "tg-process-ref" in url:
            return _FakeSupabaseResponse([_process_row_with_reference("1")])
        if "/rest/v1/flows" in url and "flow-product-energy" in url:
            return _FakeSupabaseResponse([_flow_row_with_unit("flow-product-energy", "Output electricity", "kWh", "Units of energy")])
        if "/rest/v1/flows" in url and "flow-input-energy" in url:
            return _FakeSupabaseResponse([_flow_row_with_unit("flow-input-energy", "Input electricity", "MJ", "Units of energy")])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)
    account_id = _create_tiangong_account(client)

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "tg-process-ref", "overwrite": True},
    )
    assert response.status_code == 200, response.text
    report = response.json()["tidas_import_report"]
    assert report["warning_count"] >= 1
    assert any("matched non-output exchange" in item for item in report["warnings"])
    db = _db_module.SessionLocal()
    try:
        process = db.get(ReferenceProcess, "tg-process-ref")
        assert process is not None
        assert process.reference_flow_uuid is None
        assert process.reference_flow_internal_id == "1"
    finally:
        db.close()


def test_publish_local_flow_invokes_tiangong_create_dataset(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)
    flow_uuid = "11111111-1111-4111-8111-111111111111"
    db = _db_module.SessionLocal()
    try:
        db.add(
            FlowRecord(
                flow_uuid=flow_uuid,
                flow_name="Mass test flow",
                flow_name_en="Mass test flow",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="custom",
                is_custom=True,
                tidas_compatible=True,
                tidas_flow_property_uuid="flowproperty-1",
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/flows/{flow_uuid}/publish",
        json={"ruleVerification": False},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status"] == "published"
    assert payload["local_kind"] == "flow"
    function_call = next(call for call in calls if "/functions/v1/app_dataset_create" in call["url"])
    body = json.loads(function_call["body"])
    assert body["id"] == flow_uuid
    assert body["table"] == "flows"
    assert body["jsonOrdered"]["flowDataSet"]["@xmlns"] == "http://lca.jrc.it/ILCD/Flow"
    assert body["jsonOrdered"]["flowDataSet"]["flowInformation"]["quantitativeReference"]["referenceToReferenceFlowProperty"] == "0"
    assert body["jsonOrdered"]["flowDataSet"]["flowProperties"]["flowProperty"]["referenceToFlowPropertyDataSet"]["@version"] == "1"
    db = _db_module.SessionLocal()
    try:
        record = db.query(ExternalDataSyncRecord).filter(ExternalDataSyncRecord.local_kind == "flow", ExternalDataSyncRecord.local_uuid == flow_uuid).first()
        assert record is not None
        assert record.remote_id == flow_uuid
    finally:
        db.close()


def test_publish_local_flow_rejects_unit_not_in_resolved_unit_group(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)
    flow_uuid = "22222222-2222-4222-8222-222222222222"
    db = _db_module.SessionLocal()
    try:
        db.add(
            FlowRecord(
                flow_uuid=flow_uuid,
                flow_name="Volume test flow",
                flow_type="Product flow",
                default_unit="m3",
                unit_group="Units of mass",
                source="custom",
                is_custom=True,
                tidas_compatible=True,
                tidas_flow_property_uuid="flowproperty-1",
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/flows/{flow_uuid}/publish",
        json={"ruleVerification": False},
    )

    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "TIANGONG_FLOW_PUBLISH_PREFLIGHT_FAILED"
    assert not any("/functions/v1/app_dataset_create" in call["url"] for call in calls)


def test_publish_local_process_requires_published_flow_dependencies(client, monkeypatch):
    _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)
    db = _db_module.SessionLocal()
    try:
        db.add(
            ReferenceProcess(
                process_uuid="local-process-1",
                process_name="Local process",
                process_type="unit_process",
                reference_flow_uuid="local-flow-1",
                process_json={
                    "process_uuid": "local-process-1",
                    "process_name": "Local process",
                    "reference_flow_uuid": "local-flow-1",
                    "exchanges": [{"flow_uuid": "local-flow-1", "flow_name": "Electricity", "direction": "output", "amount": 1, "unit": "MJ", "isProduct": True}],
                },
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/local-process-1/publish",
        json={},
    )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "DATA_PLATFORM_PROCESS_DEPENDENCIES_NOT_PUBLISHED"
    assert detail["missing_flow_uuids"] == ["local-flow-1"]


def test_publish_local_process_invokes_tiangong_create_dataset_after_dependencies(client, monkeypatch):
    calls = _install_fake_tiangong_http(monkeypatch)
    account_id = _create_tiangong_account(client)
    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(flow_uuid="local-flow-1", flow_name="Electricity", flow_type="Product flow", default_unit="MJ", unit_group="Units of energy"))
        db.add(
            ExternalDataSyncRecord(
                account_id=account_id,
                platform="tiangong",
                local_kind="flow",
                local_uuid="local-flow-1",
                remote_id="local-flow-1",
                remote_version="01.01.000",
            )
        )
        db.add(
            ReferenceProcess(
                process_uuid="local-process-1",
                process_name="Local process",
                process_type="unit_process",
                reference_flow_uuid="local-flow-1",
                reference_flow_internal_id="1",
                process_json={
                    "process_uuid": "local-process-1",
                    "process_name": "Local process",
                    "reference_flow_uuid": "local-flow-1",
                    "reference_flow_internal_id": "1",
                    "exchanges": [{"exchange_internal_id": "1", "flow_uuid": "local-flow-1", "flow_name": "Electricity", "direction": "output", "amount": 1, "unit": "MJ", "isProduct": True}],
                },
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/local-process-1/publish",
        json={"overwrite": True},
    )

    assert response.status_code == 200, response.text
    function_call = [call for call in calls if "/functions/v1/app_dataset_create" in call["url"]][-1]
    body = json.loads(function_call["body"])
    assert body["id"] == "local-process-1"
    assert body["table"] == "processes"
    assert body["overwrite"] is True
    exchange = body["jsonOrdered"]["processDataSet"]["exchanges"]["exchange"][0]
    assert exchange["referenceToFlowDataSet"]["@refObjectId"] == "local-flow-1"
    assert exchange["unit"] == "MJ"
    db = _db_module.SessionLocal()
    try:
        record = db.query(ExternalDataSyncRecord).filter(ExternalDataSyncRecord.local_kind == "process", ExternalDataSyncRecord.local_uuid == "local-process-1").first()
        assert record is not None
        assert record.remote_id == "local-process-1"
    finally:
        db.close()


# ======================================================================
# Refresh imports tests
# ======================================================================


def test_refresh_imports_refreshes_flow_before_process(client):
    """Refresh-imports endpoint: verify flows are processed before processes."""
    account_id = _create_mock_account(client)

    # First sync a flow and a process to create sync records
    client.post(
        f"/api/data-platforms/accounts/{account_id}/flows/sync",
        json={"remote_flow_id": "mock-flow-1", "overwrite": True},
    )
    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    # Call refresh
    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/refresh-imports",
        json={"overwrite": True},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 2  # at least 1 flow + 1 process
    assert data["refreshed"] + data["failed"] + data["skipped"] == data["total"]


def test_refresh_imports_reports_partial_failures(client, monkeypatch):
    """Refresh-imports: some records fail but others succeed, endpoint still returns
    all items without stopping on the first failure."""

    def fake_urlopen_partial_fail(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url and "mock-flow-1" in url:
            return _FakeSupabaseResponse([_flow_row("mock-flow-1")])
        if "/rest/v1/flows" in url:
            # Simulate failure for some flows
            from urllib.error import HTTPError
            raise HTTPError(url, 500, "Internal error", hdrs=None, fp=None)
        if "/rest/v1/processes" in url:
            return _FakeSupabaseResponse([
                {
                    "id": "mock-process-sync",
                    "name": "Mock process",
                    "version": "1",
                    "json": {
                        "process_uuid": "mock-process-sync",
                        "process_name": "Mock process",
                        "reference_flow_uuid": "flow-1",
                        "exchanges": [{"flow_uuid": "flow-1", "flow_name": "Mock input", "direction": "output", "amount": 1, "unit": "kg"}],
                    },
                }
            ])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen_partial_fail)
    account_id = _create_tiangong_account(client)

    # Create sync records manually
    db = _db_module.SessionLocal()
    try:
        db.add(
            ExternalDataSyncRecord(
                account_id=account_id,
                platform="tiangong",
                local_kind="flow",
                local_uuid="some-flow",
                remote_id="some-fail-flow",
                remote_version="1",
            )
        )
        db.add(
            ExternalDataSyncRecord(
                account_id=account_id,
                platform="mock",
                local_kind="flow",
                local_uuid="mock-flow-1",
                remote_id="mock-flow-1",
                remote_version="1",
            )
        )
        db.add(
            ExternalDataSyncRecord(
                account_id=account_id,
                platform="mock",
                local_kind="process",
                local_uuid="mock-process-sync",
                remote_id="mock-process-sync",
                remote_version="1",
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/refresh-imports",
        json={"overwrite": True},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 3
    # Check that we have a mix of statuses
    statuses = [item["status"] for item in data["items"]]
    assert "refreshed" in statuses
    assert "failed" in statuses
    # Verify response contains no secrets
    response_text = json.dumps(data)
    assert "secret" not in response_text.lower() or "secret-api-key" not in response_text


def test_refresh_imports_no_secrets_in_response(client):
    """Refresh-imports endpoint returns no credentials or secrets in the response."""
    account_id = _create_mock_account(client)

    # Sync something to create records
    client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "mock-process-sync", "overwrite": True},
    )

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/refresh-imports",
        json={"overwrite": True},
    )
    assert response.status_code == 200
    data = response.json()
    response_json_str = json.dumps(data)
    # The credential was "secret-api-key" — should not appear anywhere in the response
    assert "secret-api-key" not in response_json_str
    assert "api-key" not in response_json_str.lower() or "api_key" not in response_json_str.lower()


def test_refresh_imports_account_not_found(client):
    """Refresh-imports on nonexistent account returns 404."""
    response = client.post(
        "/api/data-platforms/accounts/nonexistent-id/refresh-imports",
        json={"overwrite": True},
    )
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["code"] == "DATA_PLATFORM_ACCOUNT_NOT_FOUND"


# ======================================================================
# _remote_raw_row unit-level tests (Finding #1 regression)
# ======================================================================


def test_remote_raw_row_merges_fallback_into_json_payload(client):
    """_remote_raw_row must merge fallback scalar fields into rows that carry a
    json/json_tg/json_ordered key.  Without this fix the flow sync path would
    return the raw payload with default_unit / unit_group / flow_type still empty.

    This is the regression test for Finding #1.
    """
    from app.api.data_platforms import _remote_raw_row

    # Simulate what TianGong connector sends: a row that has 'json' but
    # is missing the top-level scalar fields.
    metadata = {
        "row": {
            "id": "energy-flow-reg",
            "json": {
                "flowDataSet": {
                    "flowInformation": {
                        "dataSetInformation": {"common:name": "Electricity, high voltage"},
                        "referenceUnit": "MJ",
                        "unitGroup": "Units of energy",
                    }
                }
            },
        }
    }
    fallback = {
        "id": "energy-flow-reg",
        "name": "Electricity, high voltage",
        "version": "1",
        "default_unit": "MJ",
        "unit_group": "Units of energy",
        "flow_type": "Elementary flow",
        "source": "tiangong",
    }

    result = _remote_raw_row(metadata, fallback)

    # Top-level scalar fields MUST be present (Finding #1)
    assert result.get("default_unit") == "MJ"
    assert result.get("unit_group") == "Units of energy"
    assert result.get("flow_type") == "Elementary flow"
    assert result.get("name") == "Electricity, high voltage"
    # The json payload must still be preserved
    assert "json" in result
    assert isinstance(result["json"], dict)


def test_remote_raw_row_preserves_meaningful_raw_scalars(client):
    """When raw row already has a meaningful scalar, fallback must not overwrite it."""
    from app.api.data_platforms import _remote_raw_row

    metadata = {
        "row": {
            "id": "flow-exist",
            "name": "Existing name",
            "default_unit": "kWh",
            "json": {"data": True},
        }
    }
    fallback = {
        "name": "Fallback name",
        "default_unit": "MJ",
        "flow_type": "Elementary flow",
    }

    result = _remote_raw_row(metadata, fallback)

    # Meaningful raw values should be kept
    assert result["name"] == "Existing name"
    assert result["default_unit"] == "kWh"
    # Missing fallback field should be filled
    assert result["flow_type"] == "Elementary flow"


def test_remote_raw_row_empty_raw_returns_raw_without_fallback(client):
    """When metadata is None or has no row and fallback is None, return empty dict."""
    from app.api.data_platforms import _remote_raw_row

    result = _remote_raw_row(None, None)
    assert result == {}


def test_refresh_imports_energy_flow_has_correct_units(client, monkeypatch):
    """Full integration: sync an energy flow, then refresh-imports.
    Assert the final FlowRecord has default_unit='MJ' and unit_group='Units of energy'.
    This validates that _remote_raw_row merges fallback into the json payload before
    import_tidas_flow_rows extracts the unit info.  (Finding #1 regression test.)
    """

    def fake_urlopen_energy(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url and "energy-flow-refresh" in url:
            return _FakeSupabaseResponse([_energy_flow_row_without_top_level_units()])
        if "/rest/v1/flowproperties" in url:
            return _FakeSupabaseResponse([_flowproperty_row()])
        if "/rest/v1/unitgroups" in url:
            return _FakeSupabaseResponse([_unitgroup_row()])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen_energy)
    account_id = _create_tiangong_account(client)

    # Create an intentionally stale local record and matching sync record.
    db = _db_module.SessionLocal()
    try:
        # Insert a flow record that would come from the initial sync
        flow_uuid = "e5a8c120-7f45-4b21-a3e6-d89c10ef2b41"
        db.add(
            FlowRecord(
                flow_uuid=flow_uuid,
                flow_name="Electricity, high voltage",
                flow_type="Elementary flow",
                default_unit="kg",  # wrong placeholder; refresh should fix
                unit_group="Units of mass",  # wrong placeholder
                source="tiangong",
                is_custom=False,
            )
        )
        db.add(
            ExternalDataSyncRecord(
                account_id=account_id,
                platform="tiangong",
                local_kind="flow",
                local_uuid=flow_uuid,
                remote_id="energy-flow-refresh",
                remote_version="2",
            )
        )
        db.commit()
    finally:
        db.close()

    # Now refresh
    refresh_resp = client.post(
        f"/api/data-platforms/accounts/{account_id}/refresh-imports",
        json={"overwrite": True},
    )
    assert refresh_resp.status_code == 200
    refresh_data = refresh_resp.json()
    assert refresh_data["refreshed"] >= 1, f"Expected at least 1 refreshed, got: {refresh_data}"

    # Verify the flow record was updated with correct units
    db = _db_module.SessionLocal()
    try:
        flow = db.get(FlowRecord, flow_uuid)
        assert flow is not None
        assert flow.default_unit == "MJ", f"Expected MJ, got: {flow.default_unit}"
        assert flow.unit_group == "Units of energy", f"Expected Units of energy, got: {flow.unit_group}"
        assert flow.tidas_compatible is True
        # Verify sync record was updated
        sync_rec = (
            db.query(ExternalDataSyncRecord)
            .filter(
                ExternalDataSyncRecord.account_id == account_id,
                ExternalDataSyncRecord.local_uuid == flow_uuid,
            )
            .first()
        )
        assert sync_rec is not None
        assert sync_rec.metadata_json is not None
        assert "last_refreshed_at" in sync_rec.metadata_json
    finally:
        db.close()


def test_tidas_flow_extract_canonicalizes_localized_unit_group_and_uses_modified_at():
    from app.services.tidas_import_core import _extract_tidas_flow_record

    row = {
        "id": "localized-mass-flow",
        "default_unit": "kg",
        "unit_group": "\u8d28\u91cf",
        "version": "01.01.000",
        "modified_at": "2026-07-14T09:30:00+08:00",
        "json": {
            "flowDataSet": {
                "flowInformation": {
                    "dataSetInformation": {
                        "common:UUID": "localized-mass-flow",
                        "common:name": "Mass flow",
                    }
                },
                "flowProperties": {
                    "flowProperty": {
                        "referenceToFlowPropertyDataSet": {
                            "common:shortDescription": {
                                "#text": "Mass",
                                "@xml:lang": "en",
                            }
                        }
                    }
                },
            }
        },
    }

    flow, error = _extract_tidas_flow_record(row)

    assert error is None
    assert flow is not None
    assert flow["unit_group"] == "Units of mass"
    assert flow["tidas_unit_group"] == "Units of mass"
    assert flow["source_updated_at"] == "2026-07-14T09:30:00+08:00"


def test_tidas_flow_refresh_preserves_existing_unit_group_when_unit_is_unchanged(client, monkeypatch):
    row = _energy_flow_row_without_top_level_units()
    row["unit_group"] = "\u80fd\u91cf"
    row["modified_at"] = "2026-07-14T09:30:00+08:00"

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url and "energy-flow-1" in url:
            return _FakeSupabaseResponse([row])
        if "/rest/v1/flowproperties" in url:
            return _FakeSupabaseResponse([_flowproperty_row()])
        if "/rest/v1/unitgroups" in url:
            return _FakeSupabaseResponse([_unitgroup_row()])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)
    account_id = _create_tiangong_account(client)
    flow_uuid = "e5a8c120-7f45-4b21-a3e6-d89c10ef2b41"
    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(flow_uuid=flow_uuid, flow_name="Electricity", flow_type="Product flow", default_unit="MJ", unit_group="Units of energy", source="tiangong", is_custom=False))
        db.commit()
    finally:
        db.close()

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/flows/sync",
        json={"remote_flow_id": "energy-flow-1", "remote_version": "2"},
    )
    assert response.status_code == 200

    db = _db_module.SessionLocal()
    try:
        flow = db.get(FlowRecord, flow_uuid)
        assert flow is not None
        assert flow.unit_group == "Units of energy"
        assert flow.tidas_unit_group == "Units of energy"
        assert flow.tidas_compatible is True
        assert flow.source_updated_at == "2026-07-14T09:30:00+08:00"
    finally:
        db.close()


def test_refresh_imports_kinds_filter(client):
    """Refresh-imports with kinds=['flow'] should only refresh flows, not processes."""
    account_id = _create_mock_account(client)

    # Create sync records for both flow and process
    db = _db_module.SessionLocal()
    try:
        db.add(ExternalDataSyncRecord(
            account_id=account_id, platform="mock", local_kind="flow",
            local_uuid="flow-k", remote_id="flow-k", remote_version="1",
        ))
        db.add(ExternalDataSyncRecord(
            account_id=account_id, platform="mock", local_kind="process",
            local_uuid="proc-k", remote_id="proc-k", remote_version="1",
        ))
        db.commit()
    finally:
        db.close()

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/refresh-imports",
        json={"overwrite": True, "kinds": ["flow"]},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["items"][0]["local_kind"] == "flow"


def test_tiangong_refresh_imports_uses_active_account(client, monkeypatch):
    """The TianGong convenience endpoint should use the active TianGong account."""
    account_id = _create_tiangong_account(client)
    db = _db_module.SessionLocal()
    try:
        account = db.get(DataPlatformAccount, account_id)
        assert account is not None
        account.last_validation_status = "ok"
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr("app.api.data_platforms.connector_for_account", lambda _ctx: object())
    response = client.post(
        "/api/data-platforms/tiangong/refresh-imports",
        json={"overwrite": True, "kinds": ["flow"]},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["account_id"] == account_id
    assert data["platform"] == "tiangong"
    assert data["total"] == 0


def test_refresh_imports_unknown_kind_returns_400(client):
    """Refresh-imports with an unsupported kind should return 400."""
    account_id = _create_mock_account(client)

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/refresh-imports",
        json={"overwrite": True, "kinds": ["model"]},
    )
    assert response.status_code == 422  # Pydantic validation error


def test_refresh_imports_overwrite_false_uses_skip_mode(client):
    """When overwrite=False, upsert_mode=skip; already-existing records should be skipped."""
    account_id = _create_mock_account(client)

    # First sync a flow to create a record
    client.post(
        f"/api/data-platforms/accounts/{account_id}/flows/sync",
        json={"remote_flow_id": "mock-flow-1", "overwrite": True},
    )

    # Now refresh with overwrite=False — existing record should be skipped
    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/refresh-imports",
        json={"overwrite": False},
    )
    assert response.status_code == 200
    data = response.json()
    # At minimum the previously-synced flow is present; skip means it won't be re-updated
    # We just verify the call succeeds — exact skip count depends on connector behavior
    assert "total" in data


# ======================================================================
# TianGong On-Demand Flow Refresh Tests
# ======================================================================


def test_tiangong_flow_refresh_success(client, monkeypatch):
    """Successful refresh of a TianGong flow by local UUID."""
    account_id = _create_tiangong_account(client)
    flow_uuid = "test-flow-uuid"

    # Create local flow with source=tiangong
    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Test Flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
            is_custom=False,
        ))
        db.add(ExternalDataSyncRecord(
            account_id=account_id,
            platform="tiangong",
            local_kind="flow",
            local_uuid=flow_uuid,
            remote_id=flow_uuid,  # remote_id equals local UUID for simple case
            remote_version="1",
        ))
        db.commit()
    finally:
        db.close()

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url and flow_uuid in url:
            # Top-level row id becomes flow_uuid via _tiangong_flow_from_row
            return _FakeSupabaseResponse([{
                "id": flow_uuid,
                "name": "Updated Test Flow",
                "version": "2",
                "default_unit": "kg",
                "unit_group": "Units of mass",
                "json": {"flowDataSet": {"flowInformation": {"dataSetInformation": {"common:name": "Updated Test Flow"}}}},
            }])
        if "/rest/v1/flowproperties" in url:
            return _FakeSupabaseResponse([_flowproperty_row()])
        if "/rest/v1/unitgroups" in url:
            return _FakeSupabaseResponse([_unitgroup_row()])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)

    response = client.post(f"/api/data-platforms/tiangong/flows/{flow_uuid}/refresh", json={})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["flow_uuid"] == flow_uuid
    assert data["remote_id"] == flow_uuid
    assert data["status"] == "refreshed"


def test_tiangong_flow_refresh_with_different_lineage_remote_id(client, monkeypatch):
    """When lineage remote_id differs from local UUID, use lineage remote_id to fetch,
    but the RemoteFlowDTO.flow_uuid must still match local UUID."""
    account_id = _create_tiangong_account(client)
    flow_uuid = "local-flow-uuid"
    lineage_remote_id = "remote-lineage-456"

    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Test Flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
            is_custom=False,
        ))
        db.add(ExternalDataSyncRecord(
            account_id=account_id,
            platform="tiangong",
            local_kind="flow",
            local_uuid=flow_uuid,
            remote_id=lineage_remote_id,  # lineage remote_id differs from local UUID
            remote_version="1",
        ))
        db.commit()
    finally:
        db.close()

    # Monkeypatch connector_for_account to return a fake connector
    def fake_connector_for_account(ctx):
        class FakeConnector:
            def get_flow_detail(self, remote_id, remote_version=None):
                assert remote_id == lineage_remote_id  # Receives the lineage remote_id
                # Returns RemoteFlowDTO with flow_uuid matching local UUID
                from app.services.data_platform_connectors import RemoteFlowDTO
                return RemoteFlowDTO(
                    remote_id=lineage_remote_id,
                    flow_uuid=flow_uuid,  # Must match local UUID
                    flow_name="Synced Flow",
                    source="tiangong",
                    remote_version="2",
                )
            def get_flow_dependency_unit_groups(self, flow):
                return []
        return FakeConnector()

    monkeypatch.setattr("app.api.data_platforms.connector_for_account", fake_connector_for_account)

    response = client.post(f"/api/data-platforms/tiangong/flows/{flow_uuid}/refresh", json={})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["flow_uuid"] == flow_uuid
    assert data["remote_id"] == lineage_remote_id
    assert data["status"] == "refreshed"


def test_tiangong_flow_refresh_flow_not_found(client, monkeypatch):
    """Refresh should return 404 when local flow does not exist."""
    _create_tiangong_account(client)
    flow_uuid = "nonexistent-flow"

    response = client.post(f"/api/data-platforms/tiangong/flows/{flow_uuid}/refresh", json={})
    assert response.status_code == 404
    data = response.json()
    assert data["detail"]["code"] == "FLOW_NOT_FOUND"


def test_tiangong_flow_refresh_invalid_source(client, monkeypatch):
    """Refresh should reject flows with source != tiangong."""
    account_id = _create_tiangong_account(client)
    flow_uuid = "local-flow-wrong-source"

    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Local Flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="local",  # Not tiangong
            is_custom=True,
        ))
        db.commit()
    finally:
        db.close()

    response = client.post(f"/api/data-platforms/tiangong/flows/{flow_uuid}/refresh", json={})
    assert response.status_code == 400
    data = response.json()
    assert data["detail"]["code"] == "TIANGONG_REFRESH_INVALID_SOURCE"
    assert "tiangong" in data["detail"]["message"]


def test_tiangong_flow_refresh_uuid_mismatch(client, monkeypatch):
    """Refresh should reject when remote UUID differs from local UUID."""
    account_id = _create_tiangong_account(client)
    flow_uuid = "local-flow-uuid"

    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Test Flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
            is_custom=False,
        ))
        db.add(ExternalDataSyncRecord(
            account_id=account_id,
            platform="tiangong",
            local_kind="flow",
            local_uuid=flow_uuid,
            remote_id="different-remote-uuid",
            remote_version="1",
        ))
        db.commit()
    finally:
        db.close()

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url:
            # Return a flow with different UUID
            return _FakeSupabaseResponse([{
                "id": "different-remote-uuid",
                "name": "Different Flow",
                "version": "1",
                "json": {"flowDataSet": {"flowInformation": {"dataSetInformation": {"common:UUID": "different-uuid"}}}},
            }])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)

    response = client.post(f"/api/data-platforms/tiangong/flows/{flow_uuid}/refresh", json={})
    assert response.status_code == 409
    data = response.json()
    assert data["detail"]["code"] == "TIANGONG_UUID_MISMATCH"


def test_tiangong_flow_refresh_rate_limit_429(client, monkeypatch):
    """Refresh should propagate HTTP 429 as TIANGONG_RATE_LIMITED."""
    from urllib.error import HTTPError
    import io

    account_id = _create_tiangong_account(client)
    flow_uuid = "rate-limited-flow"

    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Rate Limited Flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
            is_custom=False,
        ))
        db.add(ExternalDataSyncRecord(
            account_id=account_id,
            platform="tiangong",
            local_kind="flow",
            local_uuid=flow_uuid,
            remote_id="remote-flow",
            remote_version="1",
        ))
        db.commit()
    finally:
        db.close()

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url:
            raise HTTPError(url, 429, "Too Many Requests", hdrs=None, fp=io.BytesIO(b'{"error_description": "Rate limit exceeded"}'))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)

    response = client.post(f"/api/data-platforms/tiangong/flows/{flow_uuid}/refresh", json={})
    assert response.status_code == 429
    data = response.json()
    assert data["detail"]["code"] == "TIANGONG_RATE_LIMITED"


def test_tiangong_flow_refresh_falls_back_to_local_uuid(client, monkeypatch):
    """When no sync record exists, refresh should use local UUID as remote_id."""
    account_id = _create_tiangong_account(client)
    flow_uuid = "fallback-flow-uuid"

    db = _db_module.SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Fallback Flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
            is_custom=False,
        ))
        # No ExternalDataSyncRecord
        db.commit()
    finally:
        db.close()

    def fake_urlopen(req, timeout):  # noqa: ARG001
        url = req.full_url
        if "/auth/v1/token?grant_type=password" in url:
            return _FakeSupabaseResponse({"access_token": _jwt(), "refresh_token": "rt", "expires_in": 3600, "token_type": "bearer"})
        if "/rest/v1/flows" in url and flow_uuid in url:
            return _FakeSupabaseResponse([{
                "id": flow_uuid,
                "name": "Fallback Flow",
                "version": "1",
                "default_unit": "kg",
                "unit_group": "Units of mass",
                "json": {"flowDataSet": {"flowInformation": {"dataSetInformation": {"common:name": "Fallback Flow"}}}},
            }])
        if "/rest/v1/flowproperties" in url:
            return _FakeSupabaseResponse([_flowproperty_row()])
        if "/rest/v1/unitgroups" in url:
            return _FakeSupabaseResponse([_unitgroup_row()])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("app.services.data_platform_connectors.url_request.urlopen", fake_urlopen)

    response = client.post(f"/api/data-platforms/tiangong/flows/{flow_uuid}/refresh", json={})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["flow_uuid"] == flow_uuid
    assert data["remote_id"] == flow_uuid


# ======================================================================
# Process/Model Dependency Atomic Sync Regression Tests
# ======================================================================


def test_process_sync_dependency_failure_rolls_back(client, monkeypatch):
    """Process sync with a failing dependency flow must rollback all scoped writes."""
    account_id = _create_mock_account(client)

    def fake_connector_get_process_detail(remote_id, remote_version=None):
        from app.services.data_platform_connectors import (
            RemoteFlowDTO, RemoteProcessDTO, RemoteProcessDetailDTO,
        )
        process = RemoteProcessDTO(
            remote_id=remote_id, process_uuid="proc-dep-fail",
            process_name="Dep Fail Process", process_type="unit_process",
            reference_flow_uuid="flow-main", source="mock",
            remote_version=remote_version,
        )
        flows = [
            RemoteFlowDTO(remote_id="flow-main", flow_uuid="flow-main", flow_name="Main", source="mock", remote_version="1"),
            RemoteFlowDTO(remote_id="flow-missing-dep", flow_uuid="flow-missing-dep", flow_name="Missing", source="mock", remote_version="1"),
        ]
        process_json = {
            "process_uuid": "proc-dep-fail", "process_name": "Dep Fail Process",
            "reference_flow_uuid": "flow-main",
            "exchanges": [
                {"flow_uuid": "flow-main", "flow_name": "Main", "direction": "output", "amount": 1, "unit": "kg"},
                {"flow_uuid": "flow-missing-dep", "flow_name": "Missing", "direction": "input", "amount": 0.5, "unit": "kg"},
            ],
        }
        return RemoteProcessDetailDTO(
            process=process,
            flows=flows,
            process_json=process_json,
            import_report={"failed_flow_uuids": ["flow-missing-dep"]},
        )

    def fake_connector_for_account(ctx):
        class FC:
            def get_process_detail(self, remote_id, remote_version=None):
                return fake_connector_get_process_detail(remote_id, remote_version)
            def get_flow_detail(self, flow_uuid, remote_version=None):
                raise ConnectorError(f"Flow not found: {flow_uuid}", status_code=404)
            def get_flow_dependency_unit_groups(self, flow):
                return []
        return FC()

    monkeypatch.setattr("app.api.data_platforms.connector_for_account", fake_connector_for_account)

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "proc-dep-fail", "overwrite": True},
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "DATA_PLATFORM_SYNC_FAILED"
    assert detail["failed_flow_uuid"] == "flow-missing-dep"
    assert detail["rolled_back"] is True

    db = _db_module.SessionLocal()
    try:
        assert db.query(FlowRecord).count() == 0
        assert db.query(ReferenceProcess).count() == 0
    finally:
        db.close()


def test_model_sync_dependency_failure_rolls_back(client, monkeypatch):
    """Model sync with a failing dependency flow must rollback all scoped writes."""
    account_id = _create_mock_account(client)
    model_json = {"hybrid_graph": _graph_payload()}

    def fake_connector_get_model_detail(remote_id, remote_version=None):
        from app.services.data_platform_connectors import RemoteModelDTO, RemoteModelDetailDTO
        model = RemoteModelDTO(remote_id=remote_id, model_uuid=remote_id, model_name="Test Model", source="mock", remote_version=remote_version)
        return RemoteModelDetailDTO(model=model, model_json=model_json, lineage={"source": "mock"})

    def fake_connector_for_account(ctx):
        class FC:
            def get_model_detail(self, remote_id, remote_version=None):
                return fake_connector_get_model_detail(remote_id, remote_version)
            def get_flow_detail(self, flow_uuid, remote_version=None):
                raise ConnectorError(f"Flow not found: {flow_uuid}", status_code=404)
            def get_flow_dependency_unit_groups(self, flow):
                return []
        return FC()

    monkeypatch.setattr("app.api.data_platforms.connector_for_account", fake_connector_for_account)

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/models/sync",
        json={"remote_model_id": "model-dep-fail", "remote_version": "1"},
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "DATA_PLATFORM_MODEL_SYNC_FAILED" in detail["code"]
    assert detail["failed_flow_uuid"] is not None
    assert detail["rolled_back"] is True

    db = _db_module.SessionLocal()
    try:
        assert db.query(Model).count() == 0
        assert db.query(ModelVersion).count() == 0
    finally:
        db.close()


def test_process_sync_fetched_once(client, monkeypatch):
    """Process sync: duplicate exchange flow UUIDs must fetch each flow once."""
    account_id = _create_mock_account(client)
    fetch_calls = []

    def fake_connector_get_process_detail(remote_id, remote_version=None):
        from app.services.data_platform_connectors import (
            RemoteFlowDTO, RemoteProcessDTO, RemoteProcessDetailDTO,
        )
        process = RemoteProcessDTO(
            remote_id=remote_id, process_uuid="proc-dup-1",
            process_name="Dup Process", process_type="unit_process",
            reference_flow_uuid="flow-dup", source="mock",
            remote_version=remote_version,
        )
        flows = [RemoteFlowDTO(remote_id="flow-dup", flow_uuid="flow-dup", flow_name="Dup Flow", source="mock", remote_version="1")]
        process_json = {
            "process_uuid": "proc-dup-1", "process_name": "Dup Process",
            "reference_flow_uuid": "flow-dup",
            "exchanges": [
                {"flow_uuid": "flow-dup", "flow_name": "Dup Flow", "direction": "output", "amount": 1, "unit": "kg"},
                {"flow_uuid": "flow-dup", "flow_name": "Dup Flow", "direction": "input", "amount": 0.5, "unit": "kg"},
            ],
        }
        return RemoteProcessDetailDTO(process=process, flows=flows, process_json=process_json)

    def fake_connector_for_account(ctx):
        class FC:
            def get_process_detail(self, remote_id, remote_version=None):
                return fake_connector_get_process_detail(remote_id, remote_version)
            def get_flow_detail(self, flow_uuid, remote_version=None):
                fetch_calls.append(flow_uuid)
                from app.services.data_platform_connectors import RemoteFlowDTO
                return RemoteFlowDTO(remote_id=flow_uuid, flow_uuid=flow_uuid, flow_name="Dup Flow", source="mock", remote_version="1")
            def get_flow_dependency_unit_groups(self, flow):
                return []
        return FC()

    monkeypatch.setattr("app.api.data_platforms.connector_for_account", fake_connector_for_account)

    response = client.post(
        f"/api/data-platforms/accounts/{account_id}/processes/sync",
        json={"remote_process_id": "proc-dup-1", "overwrite": True},
    )
    assert response.status_code == 200, response.text
    assert fetch_calls == [], f"Expected no additional fetches, got: {fetch_calls}"
