from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import app.api.provider_v1 as provider_api
import app.database as db_module
import app.services.provider_v1 as provider_service
from app.config import settings
from app.database import Base
from app.main import app
from app.models import ProviderSolveIdempotency
from app.provider_schemas import ProviderEngineIdentity, ProviderSolveRequest
from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph
from app.services.provider_contract import ProviderContractError
from app.services.provider_idempotency import canonical_solve_request_hash


@pytest.fixture(autouse=True)
def isolated_database():
    Base.metadata.drop_all(bind=db_module.engine)
    Base.metadata.create_all(bind=db_module.engine)
    previous_wait = settings.provider_idempotency_wait_seconds
    previous_lease = settings.provider_idempotency_lease_seconds
    settings.provider_idempotency_wait_seconds = 2.0
    settings.provider_idempotency_lease_seconds = 5.0
    yield
    settings.provider_idempotency_wait_seconds = previous_wait
    settings.provider_idempotency_lease_seconds = previous_lease
    db_module.engine.dispose()


def _graph() -> HybridGraph:
    return HybridGraph.model_validate(
        {
            "functionalUnit": "1 kg product",
            "nodes": [
                {
                    "id": "node-1",
                    "node_kind": "unit_process",
                    "mode": "normalized",
                    "process_uuid": "custom-process",
                    "name": "Custom process",
                    "location": "CN",
                    "reference_product": "product",
                    "inputs": [],
                    "outputs": [
                        {
                            "id": "product",
                            "flowUuid": "custom-product",
                            "flowSourceNamespace": "casepack",
                            "flowVersion": "v1",
                            "flowPropertyUuid": "mass-property",
                            "flowPropertyVersion": "v1",
                            "unitGroupUuid": "mass-group",
                            "unitGroupVersion": "v1",
                            "name": "product",
                            "unit": "kg",
                            "unitGroup": "Units of mass",
                            "amount": 1.0,
                            "type": "technosphere",
                            "direction": "output",
                            "isProduct": True,
                        }
                    ],
                }
            ],
            "exchanges": [],
            "metadata": {
                "functional_unit": {
                    "display_text": "1 kg product",
                    "amount": 1.0,
                    "flow_uuid": "custom-product",
                    "flow_source_namespace": "casepack",
                    "flow_version": "v1",
                    "unit": "kg",
                    "unit_group_uuid": "mass-group",
                    "unit_group_version": "v1",
                }
            },
        }
    )


def _payload(key: str | None = None, *, amount: float = 1.0, unit: str = "kg") -> dict:
    graph = _graph()
    value = {
        "inline_snapshot": {
            "schema_version": "provider.snapshot.v1",
            "graph_hash": compute_graph_hash_from_graph(graph),
            "functional_unit": graph.metadata["functional_unit"],
            "graph": graph.model_dump(mode="json", by_alias=True),
            "source_policy": "open_mixed",
            "allowed_lcia_scope": "none",
        },
        "demand": [{"process_uuid": "custom-process", "amount": amount, "unit": unit}],
    }
    if key is not None:
        value["idempotency_key"] = key
    return value


def _count_solves(monkeypatch, *, delay: float = 0.0, error: ProviderContractError | None = None):
    original = provider_api.solve_provider
    state = {"count": 0}
    lock = threading.Lock()

    def counted(*args, **kwargs):
        with lock:
            state["count"] += 1
        if delay:
            time.sleep(delay)
        if error is not None:
            raise error
        return original(*args, **kwargs)

    monkeypatch.setattr(provider_api, "solve_provider", counted)
    return state


def test_without_key_preserves_non_idempotent_behavior(monkeypatch):
    calls = _count_solves(monkeypatch)
    client = TestClient(app)
    first = client.post("/api/provider/v1/solve", json=_payload())
    second = client.post("/api/provider/v1/solve", json=_payload())
    assert first.status_code == second.status_code == 200
    assert calls["count"] == 2
    assert first.json()["run_id"] != second.json()["run_id"]
    assert first.json()["idempotency_key"] is None


def test_same_key_replays_frozen_json_bytes_across_client_restart(monkeypatch):
    calls = _count_solves(monkeypatch)
    first = TestClient(app).post("/api/provider/v1/solve", json=_payload("case-run-1"))
    monkeypatch.setattr(
        provider_api,
        "provider_solve_runtime_fingerprint",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("replay loaded runtime")),
    )
    replay = TestClient(app).post("/api/provider/v1/solve", json=_payload("case-run-1"))
    assert first.status_code == replay.status_code == 200
    assert calls["count"] == 1
    assert first.content == replay.content
    body = first.json()
    assert body["idempotency_key"] == "case-run-1"
    assert body["request_hash"] == body["provenance"]["request_hash"]
    assert body["provenance"]["idempotency_key"] == "case-run-1"


def test_same_key_with_different_request_is_conflict(monkeypatch):
    calls = _count_solves(monkeypatch)
    client = TestClient(app)
    first = client.post("/api/provider/v1/solve", json=_payload("case-run-2"))
    conflict = client.post(
        "/api/provider/v1/solve",
        json=_payload("case-run-2", amount=2.0),
    )
    assert first.status_code == 200
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "IDEMPOTENCY_KEY_REQUEST_MISMATCH"
    assert calls["count"] == 1


def test_concurrent_same_request_has_one_solver_owner(monkeypatch):
    calls = _count_solves(monkeypatch, delay=0.3)
    barrier = threading.Barrier(2)
    responses = []

    def invoke() -> None:
        client = TestClient(app)
        barrier.wait()
        responses.append(client.post("/api/provider/v1/solve", json=_payload("case-run-3")))

    threads = [threading.Thread(target=invoke) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert len(responses) == 2
    assert {response.status_code for response in responses} == {200}
    assert calls["count"] == 1
    assert responses[0].content == responses[1].content


def test_deterministic_provider_error_is_frozen_and_replayed(monkeypatch):
    calls = _count_solves(
        monkeypatch,
        error=ProviderContractError(422, "DETERMINISTIC_TEST_ERROR", "deterministic"),
    )
    client = TestClient(app)
    first = client.post("/api/provider/v1/solve", json=_payload("case-run-4"))
    replay = client.post("/api/provider/v1/solve", json=_payload("case-run-4"))
    assert first.status_code == replay.status_code == 422
    assert first.json() == replay.json()
    assert calls["count"] == 1


def test_transient_provider_error_is_not_frozen(monkeypatch):
    calls = _count_solves(
        monkeypatch,
        error=ProviderContractError(503, "TRANSIENT_TEST_ERROR", "transient"),
    )
    first = TestClient(app).post("/api/provider/v1/solve", json=_payload("case-run-5"))
    assert first.status_code == 503
    with db_module.SessionLocal() as db:
        row = db.get(ProviderSolveIdempotency, "case-run-5")
        assert row is not None
        assert row.status == "pending"
        row.lease_expires_at = datetime.utcnow() - timedelta(seconds=1)
        db.commit()
    retried = TestClient(app).post("/api/provider/v1/solve", json=_payload("case-run-5"))
    assert retried.status_code == 503
    assert calls["count"] == 2


def test_expired_claim_reuses_run_id_and_runtime_drift_fails_closed(monkeypatch):
    request = ProviderSolveRequest.model_validate(_payload("case-run-6"))
    request_hash = canonical_solve_request_hash(request)
    with db_module.SessionLocal() as db:
        runtime_fingerprint = provider_service.provider_solve_runtime_fingerprint(db, request)
        db.add(
            ProviderSolveIdempotency(
                idempotency_key="case-run-6",
                request_hash=request_hash,
                run_id="11111111-1111-4111-8111-111111111111",
                status="pending",
                runtime_fingerprint=runtime_fingerprint,
                claim_token="22222222-2222-4222-8222-222222222222",
                lease_expires_at=datetime.utcnow() - timedelta(seconds=1),
            )
        )
        db.add(
            ProviderSolveIdempotency(
                idempotency_key="case-run-drift",
                request_hash=canonical_solve_request_hash(
                    ProviderSolveRequest.model_validate(_payload("case-run-drift"))
                ),
                run_id="33333333-3333-4333-8333-333333333333",
                status="pending",
                runtime_fingerprint="0" * 64,
                claim_token="44444444-4444-4444-8444-444444444444",
                lease_expires_at=datetime.utcnow() - timedelta(seconds=1),
            )
        )
        db.commit()
    calls = _count_solves(monkeypatch)
    client = TestClient(app)
    recovered = client.post("/api/provider/v1/solve", json=_payload("case-run-6"))
    drift = client.post("/api/provider/v1/solve", json=_payload("case-run-drift"))
    assert recovered.status_code == 200
    assert recovered.json()["run_id"] == "11111111-1111-4111-8111-111111111111"
    assert drift.status_code == 409
    assert drift.json()["detail"]["code"] == "IDEMPOTENCY_RUNTIME_DRIFT"
    assert calls["count"] == 1


def test_unexpired_claim_returns_retryable_in_progress_without_runtime_or_solve(monkeypatch):
    request = ProviderSolveRequest.model_validate(_payload("case-run-pending"))
    with db_module.SessionLocal() as db:
        db.add(
            ProviderSolveIdempotency(
                idempotency_key="case-run-pending",
                request_hash=canonical_solve_request_hash(request),
                run_id="55555555-5555-4555-8555-555555555555",
                status="pending",
                runtime_fingerprint="5" * 64,
                claim_token="66666666-6666-4666-8666-666666666666",
                lease_expires_at=datetime.utcnow() + timedelta(seconds=60),
            )
        )
        db.commit()
    settings.provider_idempotency_wait_seconds = 0
    monkeypatch.setattr(
        provider_api,
        "provider_solve_runtime_fingerprint",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pending loaded runtime")),
    )
    calls = _count_solves(monkeypatch)
    response = TestClient(app).post(
        "/api/provider/v1/solve",
        json=_payload("case-run-pending"),
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "IDEMPOTENCY_REQUEST_IN_PROGRESS"
    assert response.headers["Retry-After"]
    assert calls["count"] == 0


def test_missing_engine_build_fails_before_claim(monkeypatch):
    monkeypatch.setattr(
        provider_service,
        "engine_identity",
        lambda: ProviderEngineIdentity(version="0.1.0", commit=None),
    )
    response = TestClient(app).post("/api/provider/v1/solve", json=_payload("case-run-7"))
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "IDEMPOTENCY_RUNTIME_IDENTITY_UNAVAILABLE"
    with db_module.SessionLocal() as db:
        assert db.get(ProviderSolveIdempotency, "case-run-7") is None


def test_incomplete_runtime_fingerprint_fails_before_claim(monkeypatch):
    monkeypatch.setattr(
        provider_api,
        "provider_solve_runtime_fingerprint",
        lambda *args, **kwargs: "incomplete",
    )
    response = TestClient(app).post(
        "/api/provider/v1/solve",
        json=_payload("case-run-8"),
    )
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "IDEMPOTENCY_RUNTIME_IDENTITY_UNAVAILABLE"
    with db_module.SessionLocal() as db:
        assert db.get(ProviderSolveIdempotency, "case-run-8") is None
