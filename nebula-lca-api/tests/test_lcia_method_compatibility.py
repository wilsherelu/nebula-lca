from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.api.reference_data as reference_data
import app.database as _db_module
from app.database import Base
from app.main import app
from app.models import FlowRecord, Model, ModelVersion, ReferenceProcess, RunJob, UnitDefinition, UnitGroup
from app.services.catalog_cache import invalidate_management_caches


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=_db_module.engine)
    Base.metadata.create_all(bind=_db_module.engine)
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    yield
    db = _db_module.SessionLocal()
    try:
        for model in (RunJob, ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition, UnitGroup):
            db.query(model).delete()
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    _db_module.engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


def _seed_unit_and_flow(*, source: str = "ef3.1") -> None:
    db = _db_module.SessionLocal()
    try:
        db.merge(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        db.merge(
            FlowRecord(
                flow_uuid="flow-co2-legacy",
                flow_name="carbon dioxide legacy",
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="Units of mass",
                source=source,
                is_custom=False,
            )
        )
        db.merge(
            FlowRecord(
                flow_uuid="flow-product",
                flow_name="product",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="test",
                is_custom=False,
            )
        )
        db.commit()
    finally:
        db.close()


def test_lcia_methods_collapse_legacy_indicator_names_to_ef31_only(tmp_path, monkeypatch):
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    ef_dir = tmp_path / "legacy"
    ef_dir.mkdir()
    (ef_dir / "indicator_index.csv").write_text(
        "\n".join(
            [
                "indicator_index;method_en;method_zh;indicator_en;indicator_zh;ecoinvent_category",
                "0;Acidification;酸雨;Acidification - Accumulated Exceedance (AE);;acidification",
                "1;Climate change;气候变化;Climate change - GWP100;;climate change",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(reference_data, "DEFAULT_EF31_RUNTIME_ROOT", runtime_root)
    monkeypatch.setattr(reference_data.settings, "nebula_lca_ef31_dir", str(ef_dir))

    payload = reference_data.list_lcia_methods()

    assert payload["default_method"] == "EF v3.1"
    assert payload["methods"] == ["EF v3.1"]
    assert payload["source_label"] == "legacy_ef3.1_indicator_index"


def test_flows_api_exposes_source_and_custom_flags(client):
    _seed_unit_and_flow(source="ecoinvent")

    response = client.get("/api/flows?type=elementary_flow&page_size=10")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["flow_id"] == "flow-co2-legacy"
    assert item["source"] == "ecoinvent"
    assert item["is_custom"] is False


def test_run_model_blocks_non_ef31_for_non_ecoinvent_elementary_flow(client):
    _seed_unit_and_flow(source="ef3.1")
    graph = {
        "functionalUnit": "1 kg product",
        "nodes": [
            {
                "id": "node-1",
                "node_kind": "unit_process",
                "mode": "normalized",
                "process_uuid": "proc-1",
                "name": "process",
                "location": "GLO",
                "reference_product": "product",
                "inputs": [],
                "outputs": [
                    {
                        "id": "out-product",
                        "flowUuid": "flow-product",
                        "name": "product",
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "amount": 1.0,
                        "type": "technosphere",
                        "direction": "output",
                        "isProduct": True,
                    }
                ],
                "emissions": [
                    {
                        "id": "em-co2",
                        "flowUuid": "flow-co2-legacy",
                        "name": "carbon dioxide legacy",
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "amount": 1.0,
                        "type": "biosphere",
                        "direction": "output",
                    }
                ],
            }
        ],
        "exchanges": [],
        "metadata": {},
    }

    response = client.post("/api/model/run", json={"graph": graph, "lcia_methods": ["Climate change"]})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "LCIA_METHOD_INCOMPATIBLE_WITH_ELEMENTARY_FLOWS"
    assert detail["evidence"]["allowed_methods"] == ["EF v3.1"]
