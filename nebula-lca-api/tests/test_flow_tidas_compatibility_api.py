from fastapi.testclient import TestClient

import app.database as _db_module
from app.database import Base
from app.main import app
from app.models import FlowRecord, Model, ModelVersion, ReferenceProcess, RunJob, UnitDefinition, UnitGroup
from app.services.catalog_cache import invalidate_management_caches
from app.source_policy import set_tidas_allowed_unit_groups


def setup_function():
    Base.metadata.drop_all(bind=_db_module.engine)
    Base.metadata.create_all(bind=_db_module.engine)
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    set_tidas_allowed_unit_groups(["Units of mass"])


def teardown_function():
    db = _db_module.SessionLocal()
    try:
        for model in (RunJob, ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition, UnitGroup):
            db.query(model).delete()
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    set_tidas_allowed_unit_groups(None)
    _db_module.engine.dispose()


def _seed_flows() -> None:
    db = _db_module.SessionLocal()
    try:
        db.merge(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.merge(UnitGroup(name="Unsupported group", reference_unit="u"))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        db.add(FlowRecord(
            flow_uuid="custom-product",
            flow_name="custom product",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="user_custom",
            is_custom=True,
        ))
        db.add(FlowRecord(
            flow_uuid="builtin-product",
            flow_name="builtin product",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="Tiangong 1.0",
            is_custom=False,
        ))
        db.add(FlowRecord(
            flow_uuid="custom-elementary",
            flow_name="custom elementary",
            flow_type="Elementary flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="user_custom",
            is_custom=True,
        ))
        db.commit()
    finally:
        db.close()


def test_patch_custom_flow_tidas_compatibility_success():
    _seed_flows()
    client = TestClient(app)

    resp = client.patch("/api/flows/custom-product/tidas-compatibility", json={
        "tidasCompatible": True,
        "tidasUnitGroup": "Units of mass",
        "tidasFlowPropertyUuid": "fp-test",
        "tidasReferenceSource": "user_declared",
    })

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["tidas_compatible"] is True
    assert payload["tidas_unit_group"] == "Units of mass"
    assert payload["tidas_flow_property_uuid"] == "fp-test"
    assert payload["tidas_reference_source"] == "user_declared"


def test_patch_tidas_compatibility_rejects_builtin_or_elementary_flow():
    _seed_flows()
    client = TestClient(app)

    builtin = client.patch("/api/flows/builtin-product/tidas-compatibility", json={
        "tidasCompatible": True,
        "tidasUnitGroup": "Units of mass",
    })
    elementary = client.patch("/api/flows/custom-elementary/tidas-compatibility", json={
        "tidasCompatible": True,
        "tidasUnitGroup": "Units of mass",
    })

    assert builtin.status_code == 422
    assert elementary.status_code == 422
    assert builtin.json()["detail"]["code"] == "TIDAS_COMPATIBILITY_NOT_EDITABLE"
    assert elementary.json()["detail"]["code"] == "TIDAS_COMPATIBILITY_NOT_EDITABLE"


def test_patch_tidas_compatibility_rejects_unsupported_unit_group():
    _seed_flows()
    client = TestClient(app)

    resp = client.patch("/api/flows/custom-product/tidas-compatibility", json={
        "tidasCompatible": True,
        "tidasUnitGroup": "Unsupported group",
    })

    assert resp.status_code == 422
    assert resp.json()["detail"]["code"] == "TIDAS_UNIT_GROUP_NOT_ALLOWED"
