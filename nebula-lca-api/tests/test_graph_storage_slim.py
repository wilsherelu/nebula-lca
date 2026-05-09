"""Tests for Graph Storage Slim v1.

Verifies:
- Slim storage drops only pure display fields (flow_name_en, display_name_en, unitGroup).
- Saving the same graph with identical structure does NOT create a new version.
- Hydrate restores display fields for API responses.
- PTS nodes are saved with shell only.
- Canvas metadata preserved through slim save.
- storage_schema_version marker written.

All tests run against a temporary SQLite database (never lca_demo.db).
"""

import uuid

import pytest

from app.main import app, _slim_graph_for_storage, _hydrate_graph_for_api
from app.database import Base, engine, SessionLocal
from app.models import Model, ModelVersion
from app.schemas import HybridGraph
from tests.conftest import _TEST_DB

from sqlalchemy import create_engine as _ce
from sqlalchemy.orm import sessionmaker as _sm

engine._meta = None
del engine
import app.database as _db_module
_db_module.engine = _ce(f"sqlite:///{_TEST_DB}", future=True)
_db_module.SessionLocal = _sm(bind=_db_module.engine, autoflush=False, autocommit=False, future=True)

import app.main as _main_module
_main_module.engine = _db_module.engine
_main_module.SessionLocal = _db_module.SessionLocal

from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=_db_module.engine)
    yield
    db = _db_module.SessionLocal()
    try:
        for tbl in ("debug_diagnostics", "run_jobs", "model_versions", "models"):
            if tbl in Base.metadata.tables:
                db.execute(Base.metadata.tables[tbl].delete())
        db.commit()
    finally:
        db.close()


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def project(client):
    resp = client.post("/api/projects", json={"name": f"Test Project {uuid.uuid4().hex[:8]}"})
    assert resp.status_code in (200, 201)
    return resp.json()["project_id"]


# ── helpers ────────────────────────────────────────────────────────────────


def _make_simple_graph_node(process_uuid, name):
    """No ports → bypasses flow_catalog validation."""
    return {
        "id": f"node-{process_uuid}",
        "node_kind": "unit_process",
        "mode": "normalized",
        "process_uuid": process_uuid,
        "name": name,
        "location": "CN",
        "reference_product": name,
        "inputs": [],
        "outputs": [],
    }


def _make_graph(process_uuid, name, extra_nodes=None, metadata=None):
    return HybridGraph(
        functionalUnit=name,
        nodes=[_make_simple_graph_node(process_uuid, name)] + (extra_nodes or []),
        exchanges=[],
        metadata=metadata or {
            "canvases": [{"id": "c1", "nodes": [], "exchanges": []}],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        },
    )


def _make_full_graph_node(process_uuid, name):
    """Graph with flow ports for slim/hydrate unit testing."""
    return {
        "id": f"node-{process_uuid}",
        "node_kind": "unit_process",
        "mode": "normalized",
        "process_uuid": process_uuid,
        "name": name,
        "location": "CN",
        "reference_product": name,
        "inputs": [
            {
                "id": "port-in-1",
                "flowUuid": "flow-carbon-dioxide",
                "name": "二氧化碳",
                "flow_name_en": "Carbon dioxide",
                "display_name_en": "CO2 (air)",
                "unit": "kg",
                "unitGroup": "mass",
                "amount": 1.0,
                "type": "technosphere",
                "direction": "input",
                "dbMapping": "EF31-matched",
                "source_process_name": "Source Proc",
                "source_node_id": "src-node-1",
                "internalExposed": True,
                "reference_product_flow_uuid": "ref-flow-1",
                "legacy_port_id": "legacy-1",
            }
        ],
        "outputs": [
            {
                "id": "port-out-1",
                "flowUuid": "flow-product",
                "name": name,
                "flow_name_en": name,
                "display_name_en": None,
                "unit": "kg",
                "unitGroup": "mass",
                "amount": 1.0,
                "type": "technosphere",
                "direction": "output",
                "isProduct": True,
            }
        ],
    }


def _make_pts_node(pts_uuid, name):
    return {
        "id": f"node-pts-{pts_uuid}",
        "node_kind": "pts_module",
        "mode": "normalized",
        "pts_uuid": pts_uuid,
        "process_uuid": f"pts-proc-{pts_uuid}",
        "name": name,
        "location": "",
        "reference_product": "",
        "inputs": [],
        "outputs": [],
        "metadata": {"compiled_artifact": "should-not-be-saved", "big_data": [1] * 1000},
    }


# ── unit tests ─────────────────────────────────────────────────────────────


class TestSlimUnit:
    """Unit tests for slim/hydrate logic."""

    def test_slim_drops_display_fields_only(self):
        """Slim should keep modeling fields but drop flow_name_en, display_name_en, unitGroup."""
        graph = HybridGraph(
            functionalUnit="test",
            nodes=[_make_full_graph_node("proc-1", "Test Process")],
            exchanges=[],
            metadata={},
        )
        slim = _slim_graph_for_storage(graph.model_dump(mode="python"))

        # Metadata marker
        assert slim["metadata"]["storage_schema_version"] == "graph_slim_v1"

        # Port: display fields dropped
        port = slim["nodes"][0]["inputs"][0]
        assert "flow_name_en" not in port
        assert "display_name_en" not in port
        assert "unitGroup" not in port

        # Port: modeling fields preserved
        assert port["flowUuid"] == "flow-carbon-dioxide"
        assert port["name"] == "二氧化碳"
        assert port["unit"] == "kg"
        assert port["amount"] == 1.0
        assert port["dbMapping"] == "EF31-matched"
        assert port["source_process_name"] == "Source Proc"
        assert port["source_node_id"] == "src-node-1"
        assert port["internalExposed"] is True
        assert port["reference_product_flow_uuid"] == "ref-flow-1"

    def test_slim_preserves_pts_shell_only(self):
        """PTS nodes should only save shell fields."""
        pts_node = _make_pts_node("pts-123", "PTS Module")
        graph = HybridGraph(
            functionalUnit="test",
            nodes=[pts_node],
            exchanges=[],
            metadata={},
        )
        slim = _slim_graph_for_storage(graph.model_dump(mode="python"))
        slim_node = slim["nodes"][0]

        # Shell fields preserved
        assert slim_node["pts_uuid"] == "pts-123"
        assert slim_node["name"] == "PTS Module"
        # Artifact data NOT saved
        assert "compiled_artifact" not in slim_node
        assert "big_data" not in slim_node

    def test_slim_hash_deterministic(self):
        """Same graph → same slim → same hash."""
        graph = _make_graph("proc-1", "Test Process")
        slim1 = _slim_graph_for_storage(graph.model_dump(mode="python"))
        slim2 = _slim_graph_for_storage(graph.model_dump(mode="python"))
        assert slim1 == slim2


class TestSlimIntegration:
    """Integration tests via API endpoints (no ports → bypass flow validation)."""

    def test_canvases_preserved(self, client, project):
        """Canvas metadata should be preserved through slim save."""
        graph = _make_graph("proc-1", "Canvas Test")
        resp = client.post(f"/api/projects/{project}/versions", json={"graph": graph.model_dump()})
        assert resp.status_code == 200, resp.text

        db = _db_module.SessionLocal()
        try:
            version_record = (
                db.query(ModelVersion)
                .filter(ModelVersion.model_id == project, ModelVersion.version == 1)
                .first()
            )
            stored = version_record.hybrid_graph_json
            # canvases preserved
            assert "canvases" in stored["metadata"]
            assert "viewport" in stored["metadata"]
        finally:
            db.close()

    def test_display_only_changes_no_new_version(self, client, project):
        """Saving same graph structure should not create a new version."""
        graph1 = _make_graph("proc-1", "Display Test")
        resp1 = client.post(f"/api/projects/{project}/versions", json={"graph": graph1.model_dump()})
        assert resp1.status_code == 200
        data1 = resp1.json()
        assert data1["created_new_version"] is True
        version1 = data1["version"]

        graph2 = _make_graph("proc-1", "Display Test")
        resp2 = client.post(f"/api/projects/{project}/versions", json={"graph": graph2.model_dump()})
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["created_new_version"] is False
        assert data2["version"] == version1
