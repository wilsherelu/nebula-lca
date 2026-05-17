from fastapi.testclient import TestClient

import app.database as _db_module
from app.database import Base
from app.main import app
from app.models import FlowRecord, Model, ModelVersion, ReferenceProcess, RunJob, UnitDefinition, UnitGroup
from app.services.catalog_cache import invalidate_management_caches


def setup_function():
    Base.metadata.drop_all(bind=_db_module.engine)
    Base.metadata.create_all(bind=_db_module.engine)
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)


def teardown_function():
    db = _db_module.SessionLocal()
    try:
        for model in (RunJob, ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition, UnitGroup):
            db.query(model).delete()
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    _db_module.engine.dispose()


def _seed_flows() -> None:
    db = _db_module.SessionLocal()
    try:
        db.merge(UnitGroup(name="Units of volume", reference_unit="m3"))
        db.add(UnitDefinition(unit_group="Units of volume", unit_name="m3", factor_to_reference=1.0, is_reference=True))
        db.add(FlowRecord(
            flow_uuid="diesel-flow",
            flow_name="diesel",
            flow_type="Product flow",
            default_unit="m3",
            unit_group="Units of volume",
            source="Tiangong 1.0",
            is_custom=False,
        ))
        db.add(FlowRecord(
            flow_uuid="co2-flow",
            flow_name="carbon dioxide",
            flow_type="Elementary flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="Tiangong 1.0",
            is_custom=False,
        ))
        db.commit()
    finally:
        db.close()


def test_patch_flow_allocation_properties_success():
    _seed_flows()
    client = TestClient(app)

    resp = client.patch("/api/flows/diesel-flow/allocation-properties", json={
        "properties": [
            {
                "propertyType": "density",
                "value": 830,
                "basisUnit": "kg/m3",
                "targetUnitGroup": "Units of mass",
                "targetUnit": "kg",
                "source": "user_declared",
                "note": "site value",
            }
        ]
    })

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["flow_uuid"] == "diesel-flow"
    assert payload["properties"][0]["propertyType"] == "density"
    assert payload["properties"][0]["value"] == 830
    assert payload["properties"][0]["targetUnitGroup"] == "Units of mass"

    fetched = client.get("/api/flows/diesel-flow/allocation-properties")
    assert fetched.status_code == 200
    assert fetched.json()["properties"][0]["basisUnit"] == "kg/m3"


def test_reference_flow_returns_empty_allocation_properties_for_legacy_null():
    _seed_flows()
    client = TestClient(app)

    resp = client.get("/api/reference/flows/diesel-flow")

    assert resp.status_code == 200
    assert resp.json()["allocation_properties"] == []


def test_patch_flow_allocation_properties_rejects_elementary_flow():
    _seed_flows()
    client = TestClient(app)

    resp = client.patch("/api/flows/co2-flow/allocation-properties", json={
        "properties": [
            {
                "propertyType": "density",
                "value": 1,
                "targetUnitGroup": "Units of mass",
            }
        ]
    })

    assert resp.status_code == 422
    assert resp.json()["detail"]["code"] == "ALLOCATION_PROPERTIES_NOT_EDITABLE"


def test_patch_flow_allocation_properties_rejects_invalid_property():
    _seed_flows()
    client = TestClient(app)

    resp = client.patch("/api/flows/diesel-flow/allocation-properties", json={
        "properties": [
            {
                "propertyType": "density",
                "value": -1,
                "targetUnitGroup": "Units of mass",
            }
        ]
    })

    assert resp.status_code == 422
