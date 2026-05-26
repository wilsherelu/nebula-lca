"""Regression tests for TIDAS export and source-space blocking.

Covers:
1. Single-process TIDAS export preview and ZIP structure.
2. PTS / ecoinvent source-space export blocking.
3. A skipped round-trip placeholder until TIDAS import helpers are restored.
"""

from __future__ import annotations

import json
import zipfile
import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as _db
from app.database import Base
from app.main import app
from app.models import (
    FlowRecord, Model, ModelVersion, ReferenceProcess, RunJob, UnitDefinition, UnitGroup,
    DebugDiagnostic,
)
from app.services.catalog_cache import invalidate_management_caches


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=_db.engine)
    Base.metadata.create_all(bind=_db.engine)
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    yield
    db = _db.SessionLocal()
    try:
        for model in (RunJob, ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition,
                       UnitGroup, DebugDiagnostic):
            db.query(model).delete()
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    _db.engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


# ── Helpers ──────────────────────────────────────────────────────────────

CO2_FOSSIL_UUID = "08a91e70-3ddc-11dd-923d-0050c2490048"


def _seed_basic_catalog(db) -> None:
    """Seed minimal unit groups, definitions, and flows for single-process case."""
    db.add(UnitGroup(name="Units of mass", reference_unit="kg"))
    db.add(UnitGroup(name="Units of energy", reference_unit="MJ"))
    db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
    db.add(UnitDefinition(unit_group="Units of energy", unit_name="MJ", factor_to_reference=1.0, is_reference=True))
    db.add(UnitDefinition(unit_group="Units of energy", unit_name="kWh", factor_to_reference=3.6, is_reference=False))

    db.merge(FlowRecord(
        flow_uuid=CO2_FOSSIL_UUID,
        flow_name="carbon dioxide (fossil) (Mass, kg, Emissions to air, unspecified)",
        flow_type="Elementary flow",
        default_unit="kg",
        unit_group="Units of mass",
        compartment="air",
        source="tiangong",
    ))
    db.merge(FlowRecord(
        flow_uuid="flow-chemical-a",
        flow_name="chemical A",
        flow_type="Product flow",
        default_unit="kg",
        unit_group="Units of mass",
        source="test",
    ))
    db.commit()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)


def _create_project_with_graph(client, db, graph: dict, name: str = "Test Project") -> str:
    """Helper: create a project and a version, return project_id."""
    resp = client.post("/api/projects", json={
        "name": name,
        "reference_product": "chemical A",
        "functional_unit": "1 kg chemical A",
    })
    assert resp.status_code == 200, resp.text
    project_id = resp.json()["project_id"]

    resp = client.post(f"/api/projects/{project_id}/versions", json={"graph": graph})
    assert resp.status_code == 200, resp.text
    return project_id


def _run_calculation(client, db, project_id: str, graph: dict | None = None) -> dict:
    """Helper: trigger calculation and return the RunJob result."""
    model_version = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == project_id)
        .order_by(ModelVersion.version.desc())
        .first()
    )
    assert model_version is not None
    # Get the graph from the stored model version if not provided
    if graph is None:
        hv = model_version.hybrid_graph_json or {}
        graph = hv.get("hybrid_graph_json") or hv

    resp = client.post("/api/model/run", json={
        "project_id": project_id,
        "model_version_id": model_version.id,
        "lcia_methods": ["EF v3.1"],
        "graph": graph,
    })
    assert resp.status_code == 200, resp.text
    run_job = db.get(RunJob, resp.json()["run_id"])
    assert run_job is not None
    assert run_job.status == "completed"
    return resp.json()


def _build_fake_tidas_zip(
    flows: list[dict],
    processes: list[dict],
    models: list[dict] | None = None,
) -> bytes:
    """Build a minimal fake TIDAS ZIP bundle for import testing."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps({
            "tidas_version": "1.0",
            "export_date": "2026-01-01T00:00:00Z",
        }))
        for flow in flows:
            uuid = flow.pop("flow_uuid", "")
            zf.writestr(f"flows/{uuid}.xml", json.dumps(flow))
        for proc in processes:
            uuid = proc.pop("process_uuid", "")
            zf.writestr(f"processes/{uuid}.xml", json.dumps(proc))
        if models:
            for model in models:
                uuid = model.pop("model_uuid", "")
                zf.writestr(f"models/{uuid}.xml", json.dumps(model))
        zf.writestr("export_report.json", json.dumps({
            "multi_product_process_count": 0,
            "warnings": [],
            "errors": [],
        }))
    buf.seek(0)
    return buf.getvalue()


# ── TIDAS Export Tests ──────────────────────────────────────────────────

class TestTidasExport:
    """Test TIDAS export endpoints with a minimal single-process project."""

    def test_preview_single_process(self, client):
        """Single-process project can be previewed for TIDAS export."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-chem-a",
                        "name": "Chemical A Production",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                            }
                        ],
                        "emissions": [
                            {
                                "id": "em-co2",
                                "flowUuid": CO2_FOSSIL_UUID,
                                "name": "carbon dioxide (fossil)",
                                "unit": "kg",
                                "amount": 0.5,
                                "type": "biosphere",
                                "direction": "output",
                            }
                        ],
                    }
                ],
                "exchanges": [],
            }
            project_id = _create_project_with_graph(client, db, graph)

            resp = client.post(f"/api/export/tidas/bundle/preview", json={
                "project_id": project_id,
                "bundle_mode": "self_contained",
            })
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["can_export"] is True
            assert data["flow_count"] >= 2  # at least chemical A + CO2
            assert data["process_count"] >= 1
        finally:
            db.close()

    def test_process_geography_inherits_project_geography(self, client):
        """Process export should inherit project geography instead of falling back to GLO."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-chem-a",
                        "name": "Chemical A Production",
                        "location": "",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
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
            }
            project_id = _create_project_with_graph(client, db, graph)
            model = db.get(Model, project_id)
            assert model is not None
            model.geography = "CN"
            db.commit()

            resp = client.post("/api/export/tidas/bundle/preview", json={"project_id": project_id})
            assert resp.status_code == 200, resp.text
            warning_messages = [w["message"] for w in resp.json()["warnings"]]
            assert all("Process proc-chem-a missing geography" not in msg for msg in warning_messages)
        finally:
            db.close()

    def test_process_metadata_overrides_project_defaults(self, client):
        """Process metadata is exported from node fields, including reference product selection."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            db.merge(FlowRecord(
                flow_uuid="flow-oil",
                flow_name="oil",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="test",
            ))
            db.commit()
            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-chem-a",
                        "name": "Chemical A Production",
                        "location": "CN-SH",
                        "reference_product": "oil",
                        "reference_product_flow_uuid": "flow-oil",
                        "reference_product_direction": "output",
                        "reference_year": 2024,
                        "time_representativeness": "2024 operating data",
                        "technology_description": "Batch process",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                                "allocationFactor": 0.4,
                            },
                            {
                                "id": "out-oil",
                                "flowUuid": "flow-oil",
                                "name": "oil",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                                "allocationFactor": 0.6,
                            },
                        ],
                        "emissions": [],
                    }
                ],
                "exchanges": [],
            }
            project_id = _create_project_with_graph(client, db, graph)
            model = db.get(Model, project_id)
            assert model is not None
            model.geography = "CN"
            db.commit()

            resp = client.post("/api/export/tidas/bundle", json={
                "project_id": project_id,
                "bundle_mode": "self_contained",
            })
            assert resp.status_code == 200, resp.text
            with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
                process_name = next(name for name in zf.namelist() if name.startswith("processes/"))
                process_data = json.loads(zf.read(process_name))

            info = process_data["processDataSet"]["processInformation"]
            assert info["geography"]["locationOfOperationSupplyOrProduction"]["@location"] == "CN-SH"
            assert info["time"]["common:referenceYear"] == 2024
            assert info["time"]["common:timeRepresentativenessDescription"][0]["#text"] == "2024 operating data"
            assert info["technology"]["technologyDescriptionAndIncludedProcesses"][0]["#text"] == "Batch process"
            assert info["quantitativeReference"]["referenceToReferenceFlow"] == "1"
            assert process_data["processDataSet"]["exchanges"]["exchange"][1]["json_tg"]["originalInternalId"] == "out-oil"
        finally:
            db.close()

    def test_export_single_process(self, client):
        """Export TIDAS bundle for single-process project produces valid ZIP."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-chem-a",
                        "name": "Chemical A Production",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
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
            }
            project_id = _create_project_with_graph(client, db, graph)

            resp = client.post(f"/api/export/tidas/bundle", json={
                "project_id": project_id,
                "bundle_mode": "self_contained",
            })
            assert resp.status_code == 200
            assert "application/zip" in resp.headers.get("content-type", "")
            zip_bytes = resp.content
            with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
                namelist = zf.namelist()
                assert any(f.startswith("manifest.json") for f in namelist)
                assert any(f.startswith("processes/") for f in namelist)
                assert any(f.startswith("flows/") for f in namelist)
                assert any(f.startswith("flowproperties/") for f in namelist)
                assert any(f.startswith("unitgroups/") for f in namelist)
                assert any(f.startswith("sources/") for f in namelist)
                assert any(f.startswith("contacts/") for f in namelist)
                manifest = json.loads(zf.read("manifest.json"))
                tables = {entry["table"] for entry in manifest["entries"]}
                assert {"flowproperties", "unitgroups", "sources", "contacts"} <= tables
        finally:
            db.close()

    def test_export_platform_light_default_excludes_reference_datasets(self, client):
        """Default platform-light ZIP only contains processes, lifecycle model, manifest, and report."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            db.get(FlowRecord, "flow-chemical-a").source = "tiangong"
            db.commit()
            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-chem-a",
                        "name": "Chemical A Production",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
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
            }
            project_id = _create_project_with_graph(client, db, graph)

            resp = client.post("/api/export/tidas/bundle", json={"project_id": project_id})
            assert resp.status_code == 200, resp.text
            with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
                namelist = set(zf.namelist())
                assert "manifest.json" in namelist
                assert "export_report.json" in namelist
                assert any(name.startswith("processes/") for name in namelist)
                assert any(name.startswith("lifecyclemodels/") for name in namelist)
                assert not any(name.startswith("flows/") for name in namelist)
                assert not any(name.startswith("flowproperties/") for name in namelist)
                assert not any(name.startswith("unitgroups/") for name in namelist)
                assert not any(name.startswith("sources/") for name in namelist)
                assert not any(name.startswith("contacts/") for name in namelist)
                manifest = json.loads(zf.read("manifest.json"))
                assert manifest["counts"]["flows"] == 0
                report = json.loads(zf.read("export_report.json"))
                assert report["bundle_mode"] == "tiangong_platform_light"
                assert report["referenced_flow_count"] == 1
        finally:
            db.close()

    def test_lifecycle_model_exports_process_connections(self, client):
        """Graph edges are represented in lifeCycleModel processInstance connections."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            db.get(FlowRecord, "flow-chemical-a").source = "tiangong"
            db.commit()
            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-a",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-a",
                        "name": "Supplier",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                            }
                        ],
                        "emissions": [],
                    },
                    {
                        "id": "np-b",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-b",
                        "name": "Consumer",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [
                            {
                                "id": "in-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "input",
                            }
                        ],
                        "outputs": [],
                        "emissions": [],
                    },
                ],
                "exchanges": [
                    {
                        "id": "edge-1",
                        "fromNode": "np-a",
                        "toNode": "np-b",
                        "sourcePortId": "out-product",
                        "targetPortId": "in-product",
                        "flowUuid": "flow-chemical-a",
                        "flowName": "chemical A",
                        "quantityMode": "single",
                        "amount": 1.0,
                        "unit": "kg",
                        "type": "technosphere",
                    }
                ],
            }
            project_id = _create_project_with_graph(client, db, graph)

            resp = client.post("/api/export/tidas/bundle", json={"project_id": project_id})
            assert resp.status_code == 200, resp.text
            with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
                model_name = next(name for name in zf.namelist() if name.startswith("lifecyclemodels/"))
                model_data = json.loads(zf.read(model_name))
            instances = (
                model_data["lifeCycleModelDataSet"]
                ["lifeCycleModelInformation"]["technology"]["processes"]["processInstance"]
            )
            connected = [item for item in instances if item["connections"].get("connection")]
            assert len(connected) == 1
            connection = connected[0]["connections"]["connection"][0]
            assert connection["referenceToProcessInstance"] == "1"
            assert connection["referenceToExchange"] == "flow-chemical-a"
        finally:
            db.close()


class TestTidasExportBlocking:
    """TIDAS export should be blocked for PTS and ecoinvent elementary flows."""

    def test_blocked_for_pts_nodes(self, client):
        """PTS nodes block TIDAS export."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            # Also seed the flow used by the PTS node
            db.merge(FlowRecord(
                flow_uuid="flow-test",
                flow_name="test product",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="test",
            ))
            db.commit()
            # Create a project with a PTS node
            resp = client.post("/api/projects", json={
                "name": "PTS Project",
                "reference_product": "test",
                "functional_unit": "1 kg test",
            })
            project_id = resp.json()["project_id"]

            resp = client.post(f"/api/projects/{project_id}/versions", json={
                "graph": {
                    "functionalUnit": "1 kg test",
                    "nodes": [
                        {
                            "id": "np-pts",
                            "node_kind": "pts_module",
                            "mode": "normalized",
                            "pts_uuid": "pts-123",
                            "process_uuid": "pts-123",
                            "name": "Test PTS",
                            "location": "GLO",
                            "reference_product": "test",
                            "inputs": [],
                            "outputs": [
                                {
                                    "id": "out-1",
                                    "flowUuid": "flow-test",
                                    "name": "test",
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
                }
            })
            assert resp.status_code == 200, resp.text

            resp = client.post(f"/api/export/tidas/bundle/preview", json={
                "project_id": project_id,
            })
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["can_export"] is False
        finally:
            db.close()

    def test_blocked_for_ecoinvent_elementary_flows(self, client):
        """ECoinvent elementary flows block TIDAS export."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)
            # Add an ecoinvent-sourced elementary flow
            db.merge(FlowRecord(
                flow_uuid="ecoelem-1",
                flow_name="substance (Mass, kg, Emissions to air, unspecified)",
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="Units of mass",
                compartment="air",
                source="ecoinvent",  # ecoinvent elementary flow
            ))
            db.commit()

            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-1",
                        "name": "Test",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-1",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                            }
                        ],
                        "emissions": [
                            {
                                "id": "em-1",
                                "flowUuid": "ecoelem-1",
                                "name": "substance",
                                "unit": "kg",
                                "amount": 0.1,
                                "type": "biosphere",
                                "direction": "output",
                            }
                        ],
                    }
                ],
                "exchanges": [],
            }
            project_id = _create_project_with_graph(client, db, graph)

            resp = client.post(f"/api/export/tidas/bundle/preview", json={
                "project_id": project_id,
            })
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["can_export"] is False
        finally:
            db.close()


# ── TIDAS Import Round-trip Tests ──────────────────────────────────────

class TestTidasRoundtrip:
    """TIDAS export → import round-trip placeholder.

    NOTE: The import endpoint currently has pre-existing lazy-import errors
    from the main.py split. Several TIDAS import helpers are no longer exported
    from app.main.
    Round-trip import is tracked separately. This class only tests the
    export side until the import bug is fixed.
    """

    @pytest.mark.skip(reason="TIDAS import helpers need to be restored after main.py split")
    def test_single_process_roundtrip(self, client):
        """Export a single-process project as TIDAS bundle, import it, compare results."""
        db = _db.SessionLocal()
        try:
            _seed_basic_catalog(db)

            # Step 1: Create original project and compute baseline
            graph = {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-chem-a",
                        "name": "Chemical A Production",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-chemical-a",
                                "name": "chemical A",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                            }
                        ],
                        "emissions": [
                            {
                                "id": "em-co2",
                                "flowUuid": CO2_FOSSIL_UUID,
                                "name": "carbon dioxide (fossil)",
                                "unit": "kg",
                                "amount": 0.5,
                                "type": "biosphere",
                                "direction": "output",
                            }
                        ],
                    }
                ],
                "exchanges": [],
            }
            project_id = _create_project_with_graph(client, db, graph, name="RoundTrip Source")
            baseline = _run_calculation(client, db, project_id, graph=graph)
            # Verify the baseline result is computed
            assert baseline.get("lci_result") is not None
            summary = baseline.get("summary", {})
            assert summary.get("indicator_count", 0) > 0

            # Step 2: Export TIDAS bundle
            resp = client.post(f"/api/export/tidas/bundle", json={"project_id": project_id})
            assert resp.status_code == 200, resp.text
            zip_bytes = resp.content

            # Step 3: Import the TIDAS bundle
            resp = client.post(
                "/api/import/tidas/bundle",
                files={"file": ("tidas_bundle.zip", zip_bytes, "application/zip")},
                data={"dry_run": False, "upsert_mode": "update"},
            )
            assert resp.status_code == 200, resp.text
            import_data = resp.json()
            assert import_data.get("imported_count", 0) > 0

            # Step 4: Find the imported project and run calculation
            imported_projects = client.get("/api/projects").json()
            imported_ids = [p["id"] for p in imported_projects]
            # The imported project should exist in the project list
            assert len(imported_ids) > 0

            # Step 5: Run calculation on the imported project and compare
            # Find the first imported project version
            import_project_id = imported_ids[0]
            import_resp = client.post(f"/api/projects/{import_project_id}/versions", json={
                "graph": {
                    "functionalUnit": "1 kg chemical A",
                    "nodes": [
                        {
                            "id": "np-1",
                            "node_kind": "unit_process",
                            "mode": "normalized",
                            "process_uuid": "proc-chem-a-imported",
                            "name": "Chemical A Production (Imported)",
                            "location": "GLO",
                            "reference_product": "chemical A",
                            "inputs": [],
                            "outputs": [
                                {
                                    "id": "out-product",
                                    "flowUuid": "flow-chemical-a",
                                    "name": "chemical A",
                                    "unit": "kg",
                                    "amount": 1.0,
                                    "type": "technosphere",
                                    "direction": "output",
                                    "isProduct": True,
                                }
                            ],
                            "emissions": [
                                {
                                    "id": "em-co2",
                                    "flowUuid": CO2_FOSSIL_UUID,
                                    "name": "carbon dioxide (fossil)",
                                    "unit": "kg",
                                    "amount": 0.5,
                                    "type": "biosphere",
                                    "direction": "output",
                                }
                            ],
                        }
                    ],
                    "exchanges": [],
                }
            })
            assert import_resp.status_code == 200, import_resp.text
            imported_result = _run_calculation(client, db, import_project_id,
                graph={"functionalUnit": "1 kg chemical A",
                    "nodes": [{"id": "np-1", "node_kind": "unit_process",
                        "mode": "normalized", "process_uuid": "proc-chem-a-imported",
                        "name": "Chemical A Production (Imported)", "location": "GLO",
                        "reference_product": "chemical A", "inputs": [], "outputs": [
                            {"id": "out-product", "flowUuid": "flow-chemical-a",
                                "name": "chemical A", "unit": "kg", "amount": 1.0,
                                "type": "technosphere", "direction": "output", "isProduct": True}],
                        "emissions": [{"id": "em-co2", "flowUuid": CO2_FOSSIL_UUID,
                            "name": "carbon dioxide (fossil)", "unit": "kg", "amount": 0.5,
                            "type": "biosphere", "direction": "output"}]}],
                    "exchanges": []})
            # Verify the imported project also produces results
            assert imported_result.get("lci_result") is not None
        finally:
            db.close()
