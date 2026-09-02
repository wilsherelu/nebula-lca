from io import BytesIO
from zipfile import ZipFile

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
from app.database import Base
from app.main import app
from app.models import (
    FlowRecord, Model, ModelVersion, PtsCompileArtifact, PtsDefinition,
    PtsExternalArtifact, PtsResource, RunJob, UnitDefinition, UnitGroup,
)


@pytest.fixture(autouse=True)
def isolated_schema():
    Base.metadata.drop_all(bind=db_module.engine)
    Base.metadata.create_all(bind=db_module.engine)
    yield
    Base.metadata.drop_all(bind=db_module.engine)


def seed_project() -> str:
    db = db_module.SessionLocal()
    try:
        project = Model(id="project-native-1", name="Native bundle case", functional_unit="1 kg product")
        graph = {
            "functionalUnit": "1 kg product",
            "nodes": [{
                "id": "supplier", "node_kind": "market_process", "mode": "normalized",
                "process_uuid": "supplier-process", "name": "Supplier", "location": "GLO",
                "reference_product": "electricity", "reference_product_flow_uuid": "flow-custom-electricity",
                "inputs": [],
                "outputs": [
                    {"id": "product", "flowUuid": "flow-custom-electricity", "name": "electricity", "amount": 1, "unit": "MJ", "unitGroup": "Energy", "type": "technosphere", "direction": "output", "isProduct": True},
                    {"id": "co2", "flowUuid": "08a91e70-3ddc-11dd-923d-0050c2490048", "flowSourceNamespace": "ecoinvent", "flowVersion": "3.11", "name": "carbon dioxide, fossil", "amount": 0.004, "unit": "kg", "unitGroup": "Mass", "type": "biosphere", "direction": "output"},
                ],
                "position": {"x": 20, "y": 40},
            }, {
                "id": "pts-shell", "node_kind": "pts_module", "mode": "normalized",
                "pts_uuid": "pts-native-1", "pts_published_artifact_id": "pts-external-1",
                "process_uuid": "pts-native-1", "name": "Native PTS", "location": "GLO",
                "reference_product": "electricity", "inputs": [], "outputs": [],
            }],
            "exchanges": [],
            "metadata": {"viewport": {"x": 1, "y": 2, "zoom": 0.9}},
        }
        version = ModelVersion(id="version-native-1", model_id=project.id, version=1, graph_hash="source-hash", hybrid_graph_json=graph)
        db.add_all([
            UnitGroup(name="Energy", reference_unit="MJ"),
            UnitDefinition(unit_group="Energy", unit_name="MJ", factor_to_reference=1, is_reference=True),
            FlowRecord(flow_uuid="flow-custom-electricity", flow_name="electricity", flow_type="product_flow", default_unit="MJ", unit_group="Energy", source="custom", is_custom=True),
            FlowRecord(flow_uuid="flow-pts-internal", flow_name="PTS internal flow", flow_type="product_flow", default_unit="MJ", unit_group="Energy", source="custom", is_custom=True),
            FlowRecord(flow_uuid="08a91e70-3ddc-11dd-923d-0050c2490048", flow_name="carbon dioxide, fossil", flow_type="elementary_flow", default_unit="kg", unit_group="Mass", source="ecoinvent", source_namespace="ecoinvent", source_version="3.11"),
            project,
            version,
            RunJob(id="run-native-1", model_version_id=version.id, status="completed", request_json={"demand": 1}, result_json={"inventory": {"co2": 0.004}}),
            PtsResource(id="pts-resource-1", project_id=project.id, pts_uuid="pts-native-1", name="Native PTS", pts_graph_json={"nodes": [{"outputs": [{"flowUuid": "flow-pts-internal"}]}]}),
            PtsDefinition(id="pts-definition-1", project_id=project.id, pts_uuid="pts-native-1", pts_node_id="pts-shell"),
            PtsCompileArtifact(id="pts-compile-1", project_id=project.id, pts_node_id="pts-shell", pts_uuid="pts-native-1", graph_hash="pts-hash"),
            PtsExternalArtifact(id="pts-external-1", project_id=project.id, pts_uuid="pts-native-1", pts_node_id="pts-shell", graph_hash="pts-hash", source_compile_id="pts-compile-1"),
        ])
        db.commit()
        return project.id
    finally:
        db.close()


def test_native_bundle_exports_checksums_and_restores_a_full_project_copy():
    project_id = seed_project()
    with TestClient(app) as client:
        exported = client.get(f"/api/native-project-bundles/projects/{project_id}")
        assert exported.status_code == 200
        assert exported.headers["x-nebula-bundle-schema"] == "nebula.project.bundle.v1"
        with ZipFile(BytesIO(exported.content)) as archive:
            assert "manifest.json" in archive.namelist()
            assert "pts/resources.json" in archive.namelist()
            dependencies = archive.read("dependencies.json").decode("utf-8")
            assert "08a91e70-3ddc-11dd-923d-0050c2490048" in dependencies
            assert "flow-custom-electricity" in archive.read("catalog/flows.json").decode("utf-8")
            assert "flow-pts-internal" in archive.read("catalog/flows.json").decode("utf-8")

        restored = client.post(
            "/api/native-project-bundles/import?conflict_policy=rename",
            files={"file": ("case.nebula.zip", exported.content, "application/zip")},
        )
        assert restored.status_code == 200, restored.text
        receipt = restored.json()
        assert receipt["status"] == "restored_with_issues"
        assert receipt["identity_remapped"] is True
        assert receipt["counts"]["versions"] == 1
        assert receipt["counts"]["runs"] == 1

    db = db_module.SessionLocal()
    try:
        restored_project = db.query(Model).filter(Model.id == receipt["project_id"]).one()
        restored_version = db.query(ModelVersion).filter(ModelVersion.model_id == restored_project.id).one()
        assert restored_project.name == "Native bundle case (restored)"
        assert restored_version.hybrid_graph_json["nodes"][0]["position"] == {"x": 20, "y": 40}
        assert db.query(RunJob).filter(RunJob.model_version_id == restored_version.id).count() == 1
        assert db.query(PtsResource).filter(PtsResource.project_id == restored_project.id).count() == 1
        restored_external = db.query(PtsExternalArtifact).filter(PtsExternalArtifact.project_id == restored_project.id).one()
        assert restored_external.id != "pts-external-1"
        assert restored_external.source_compile_id != "pts-compile-1"
        pts_node = next(node for node in restored_version.hybrid_graph_json["nodes"] if node["id"] == "pts-shell")
        assert pts_node["pts_published_artifact_id"] == restored_external.id
    finally:
        db.close()


def test_native_bundle_conflict_policy_fail_is_non_mutating():
    project_id = seed_project()
    with TestClient(app) as client:
        exported = client.get(f"/api/native-project-bundles/projects/{project_id}")
        restored = client.post(
            "/api/native-project-bundles/import?conflict_policy=fail",
            files={"file": ("case.nebula.zip", exported.content, "application/zip")},
        )
        assert restored.status_code == 422
    db = db_module.SessionLocal()
    try:
        assert db.query(Model).count() == 1
    finally:
        db.close()
