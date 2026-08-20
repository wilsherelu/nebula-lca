"""Regression tests for project integrity auto-repair routes."""

import uuid

from fastapi.testclient import TestClient

from app.database import Base
import app.database as _db_module
from app.main import app
from app.models import FlowRecord, Model, ModelVersion


def _stale_flow_graph(flow_uuid: str) -> dict:
    return {
        "functionalUnit": "1 kg product",
        "nodes": [
            {
                "id": "node-1",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "proc-1",
                "name": "Process 1",
                "location": "CN",
                "reference_product": "Product",
                "inputs": [
                    {
                        "id": "in-1",
                        "flowUuid": flow_uuid,
                        "name": "硅石",
                        "unit": "kg",
                        "amount": 1.0,
                        "type": "technosphere",
                        "direction": "input",
                    }
                ],
                "outputs": [
                    {
                        "id": "out-1",
                        "flowUuid": "flow-product",
                        "name": "Product",
                        "unit": "kg",
                        "amount": 1.0,
                        "type": "technosphere",
                        "direction": "output",
                        "isProduct": True,
                    }
                ],
                "emissions": [],
            }
        ],
        "exchanges": [],
        "metadata": {},
    }


def test_stale_flow_names_are_advisory_and_optional_sync_allows_save():
    Base.metadata.create_all(bind=_db_module.engine)
    client = TestClient(app)
    project_id = str(uuid.uuid4())
    flow_uuid = "flow-silica"

    db = _db_module.SessionLocal()
    try:
        db.add(
            FlowRecord(
                flow_uuid=flow_uuid,
                flow_name="silica stone / 二氧化硅石",
                flow_name_en="silica stone",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="test",
                is_custom=False,
            )
        )
        db.add(
            FlowRecord(
                flow_uuid="flow-product",
                flow_name="Product",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="test",
                is_custom=False,
            )
        )
        db.add(Model(id=project_id, name="Flow Sync Repair Test"))
        db.add(ModelVersion(model_id=project_id, version=1, hybrid_graph_json=_stale_flow_graph(flow_uuid)))
        db.commit()
    finally:
        db.close()

    before = client.get(f"/api/projects/{project_id}/latest")
    assert before.status_code == 200, before.text
    assert before.json()["flow_name_sync_needed"] is True
    assert before.json()["project_integrity"]["ok"] is True

    saved_before_sync = client.post(
        f"/api/projects/{project_id}/versions",
        json={"graph": before.json()["graph"]},
    )
    assert saved_before_sync.status_code == 200, saved_before_sync.text

    repaired = client.post(f"/api/projects/{project_id}/repair-integrity")
    assert repaired.status_code == 200, repaired.text
    repaired_payload = repaired.json()
    assert repaired_payload["failed_count"] == 0
    assert repaired_payload["repaired_count"] == 1
    assert repaired_payload["items"][0]["kind"] == "flow_name_sync"

    after = client.get(f"/api/projects/{project_id}/latest")
    assert after.status_code == 200, after.text
    after_payload = after.json()
    assert after_payload["flow_name_sync_needed"] is False
    assert after_payload["outdated_flow_refs_count"] == 0
    graph = after_payload["graph"]
    assert graph["nodes"][0]["inputs"][0]["name"] == "silica stone / 二氧化硅石"

    saved = client.post(f"/api/projects/{project_id}/versions", json={"graph": graph})
    assert saved.status_code == 200, saved.text
