"""Tests for model version auto-prune and run job request_json lightweight storage.

Each test runs against a temporary SQLite database (never lca_demo.db).
The DATABASE_URL env var is set by conftest.py *before* app.main is imported.
"""

import os
import uuid
from pathlib import Path

import pytest

# conftest.py already sets DATABASE_URL / KEEP_LATEST_VERSIONS_PER_PROJECT / AUTO_PRUNE_ON_STARTUP
# before this module's imports run.
from app.main import app, _build_run_job_request_json
from app.database import Base, engine, SessionLocal
from app.models import Model, ModelVersion, RunJob
from app.schemas import HybridGraph
from tests.conftest import _TEST_DB  # same temp DB path

# Point SQLAlchemy engine at the temp DB (re-create engine if settings didn't pick up env)
from sqlalchemy import create_engine as _ce
from sqlalchemy.orm import sessionmaker as _sm

engine._meta = None  # invalidate cached bind
del engine
# Force fresh engine pointing at temp DB
import app.database as _db_module
_db_module.engine = _ce(f"sqlite:///{_TEST_DB}", future=True)
_db_module.SessionLocal = _sm(bind=_db_module.engine, autoflush=False, autocommit=False, future=True)

# Rebind the module-level imports to use the new engine
import app.main as _main_module
_main_module.engine = _db_module.engine
_main_module.SessionLocal = _db_module.SessionLocal

from fastapi.testclient import TestClient


# ---------- fixtures -------------------------------------------------------- #

@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=_db_module.engine)
    yield
    db = _db_module.SessionLocal()
    try:
        # Delete all test data in reverse FK order
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
    assert resp.status_code == 200 or resp.status_code == 201
    return resp.json()["project_id"]


# ---------- helpers --------------------------------------------------------- #

def _make_graph_node(process_uuid, name):
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


def _make_graph(process_uuid, name):
    return HybridGraph(
        functionalUnit=name,
        nodes=[_make_graph_node(process_uuid, name)],
        exchanges=[],
        metadata={},
    )


# ---------- tests ----------------------------------------------------------- #

class TestVersionAutoPrune:
    """Test that auto-prune keeps only keep_latest versions."""

    def test_same_graph_no_new_version(self, client, project):
        """Saving the same graph twice should not create a new version."""
        graph = _make_graph("proc-1", "Test Process 1")
        resp1 = client.post(f"/api/projects/{project}/versions", json={"graph": graph.model_dump()})
        assert resp1.status_code == 200, resp1.text
        data1 = resp1.json()
        assert data1["created_new_version"] is True
        version1 = data1["version"]

        resp2 = client.post(f"/api/projects/{project}/versions", json={"graph": graph.model_dump()})
        assert resp2.status_code == 200, resp2.text
        data2 = resp2.json()
        assert data2["created_new_version"] is False
        assert data2["version"] == version1

        db = _db_module.SessionLocal()
        try:
            count = db.query(ModelVersion).filter(ModelVersion.model_id == project).count()
            assert count == 1
        finally:
            db.close()

    def test_prune_keeps_latest_n(self, client, project):
        """After saving 25 versions, only the latest 20 should remain."""
        for i in range(25):
            graph = _make_graph(f"proc-{i}", f"Test Process {i}")
            resp = client.post(f"/api/projects/{project}/versions", json={"graph": graph.model_dump()})
            assert resp.status_code == 200, f"Failed at version {i}: {resp.text}"
            data = resp.json()
            assert data["created_new_version"] is True, f"Version {i} should be new"

        db = _db_module.SessionLocal()
        try:
            count = db.query(ModelVersion).filter(ModelVersion.model_id == project).count()
            assert count == 20, f"Expected 20 versions, got {count}"
            remaining = (
                db.query(ModelVersion)
                .filter(ModelVersion.model_id == project)
                .order_by(ModelVersion.version.desc())
                .all()
            )
            versions_set = {v.version for v in remaining}
            expected = set(range(6, 26))  # versions 6-25
            assert versions_set == expected, f"Expected {expected}, got {versions_set}"
        finally:
            db.close()

    def test_prune_clears_run_job_fk_on_delete(self, client, project):
        """When a version is pruned, the FK on RunJob is safely cleared (set to NULL)."""
        for i in range(3):
            graph = _make_graph(f"proc-{i}", f"Test Process {i}")
            resp = client.post(f"/api/projects/{project}/versions", json={"graph": graph.model_dump()})
            assert resp.status_code == 200

        db = _db_module.SessionLocal()
        try:
            v1 = (
                db.query(ModelVersion)
                .filter(ModelVersion.model_id == project, ModelVersion.version == 1)
                .first()
            )
            v1_id = str(v1.id)
        finally:
            db.close()

        run_db = _db_module.SessionLocal()
        try:
            run_job = RunJob(
                model_version_id=v1_id,
                status="completed",
                request_json={"test": True},
                result_json={"test": True},
                message="test run",
            )
            run_db.add(run_job)
            run_db.commit()
            run_job_id = str(run_job.id)
        finally:
            run_db.close()

        for i in range(20):
            graph = _make_graph(f"proc-extra-{i}", f"Extra Process {i}")
            resp = client.post(f"/api/projects/{project}/versions", json={"graph": graph.model_dump()})
            assert resp.status_code == 200, resp.text

        final_db = _db_module.SessionLocal()
        try:
            v1_exists = (
                final_db.query(ModelVersion)
                .filter(ModelVersion.model_id == project, ModelVersion.version == 1)
                .first()
            )
            assert v1_exists is None, "Version 1 should be pruned"
            run_job_record = final_db.query(RunJob).filter(RunJob.id == run_job_id).first()
            assert run_job_record is not None
            assert run_job_record.model_version_id is None, "FK should be NULL after prune"
            count = final_db.query(ModelVersion).filter(ModelVersion.model_id == project).count()
            assert count == 20, f"Expected 20 versions, got {count}"
        finally:
            final_db.close()


class TestRunJobLightweightRequestJson:
    """Test that run jobs store lightweight request_json by default."""

    def test_default_stores_metadata_only(self):
        """By default, request_json should not contain full nodes/edges."""

        class FakePayload:
            def __init__(self, graph):
                self.graph = graph
                self.model_version_id = "test-id"
                self.project_id = "test-project"
                self.force_recompile = False

        fake_graph = _make_graph("proc-1", "Test Process 1")
        fake_payload = FakePayload(fake_graph)
        result = _build_run_job_request_json(fake_payload)

        assert "nodes" not in result or not isinstance(result.get("nodes"), list)
        assert result["node_count"] == 1
        assert result["edge_count"] == 0
        assert result["model_version_id"] == "test-id"

    def test_debug_env_restores_full_storage(self):
        """With NEBULA_DEBUG_STORE_RUN_GRAPH=1, full graph should be stored."""

        class FakePayload:
            def __init__(self, graph):
                self.graph = graph
                self.model_version_id = "test-id"
                self.project_id = "test-project"
                self.force_recompile = False

        os.environ["NEBULA_DEBUG_STORE_RUN_GRAPH"] = "1"
        try:
            fake_graph = _make_graph("proc-1", "Test Process 1")
            fake_payload = FakePayload(fake_graph)
            result = _build_run_job_request_json(fake_payload)
            assert "nodes" in result
            assert isinstance(result["nodes"], list)
            assert len(result["nodes"]) == 1
        finally:
            del os.environ["NEBULA_DEBUG_STORE_RUN_GRAPH"]
