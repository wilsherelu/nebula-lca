import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

import app.api.reference_data as reference_data
import app.database as _db_module
import app.main as main_module
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


def _seed_volume_product() -> None:
    db = _db_module.SessionLocal()
    try:
        db.merge(UnitGroup(name="Units of volume", reference_unit="m3"))
        db.add(UnitDefinition(unit_group="Units of volume", unit_name="m3", factor_to_reference=1.0, is_reference=True))
        db.merge(
            FlowRecord(
                flow_uuid="flow-volume-product",
                flow_name="volume product",
                flow_type="Product flow",
                default_unit="m3",
                unit_group="Units of volume",
                source="test",
                is_custom=False,
            )
        )
        db.commit()
    finally:
        db.close()


def _multi_product_mismatched_graph() -> dict:
    return {
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
                    },
                    {
                        "id": "out-volume-product",
                        "flowUuid": "flow-volume-product",
                        "name": "volume product",
                        "unit": "m3",
                        "unitGroup": "Units of volume",
                        "amount": 1.0,
                        "type": "technosphere",
                        "direction": "output",
                        "isProduct": True,
                    },
                ],
                "emissions": [],
            }
        ],
        "exchanges": [],
        "metadata": {},
    }


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
    assert payload["method_indicator_counts"] == {"EF v3.1": 2}
    assert payload["total_indicators"] == 2
    assert payload["source_label"] == "legacy_ef3.1_indicator_index"


def test_lcia_methods_reports_runtime_method_indicator_counts(tmp_path, monkeypatch):
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    runtime_dir = runtime_root / "official"
    runtime_dir.mkdir()
    (runtime_dir / "indicator_index.csv").write_text(
        "\n".join(
            [
                "indicator_index;method_en;method_zh;indicator_en;indicator_zh;ecoinvent_category",
                "0;EF v3.1;EF v3.1;acidification;;acidification",
                "1;EF v3.1;EF v3.1;climate change;;climate change",
                "2;EF v3.1 no LT;EF v3.1 no LT;acidification no LT;;acidification no LT",
            ]
        ),
        encoding="utf-8",
    )
    (runtime_dir / "manifest.json").write_text(
        '{"flows_count": 1000, "indicators_count": 10, "factors_count": 1000}',
        encoding="utf-8",
    )
    (runtime_root / "active_manifest.json").write_text(
        f'{{"artifact_dir": "{runtime_dir.as_posix()}"}}',
        encoding="utf-8",
    )
    monkeypatch.setattr(reference_data, "DEFAULT_EF31_RUNTIME_ROOT", runtime_root)

    payload = reference_data.list_lcia_methods()

    assert payload["methods"] == ["EF v3.1", "EF v3.1 no LT"]
    assert payload["method_indicator_counts"] == {"EF v3.1": 2, "EF v3.1 no LT": 1}
    assert payload["total_indicators"] == 3


def test_flows_api_exposes_source_and_custom_flags(client):
    _seed_unit_and_flow(source="ecoinvent")

    response = client.get("/api/flows?type=elementary_flow&page_size=10")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["flow_id"] == "flow-co2-legacy"
    assert item["source"] == "ecoinvent"
    assert item["is_custom"] is False


def test_flows_api_ranks_exact_before_prefix_and_contains_when_fts_is_available(client):
    db = _db_module.SessionLocal()
    try:
        for flow_uuid, flow_name in (
            ("resin", "树脂"),
            ("resin-material", "树脂材料"),
            ("composite-resin-material", "复合树脂材料"),
        ):
            db.add(
                FlowRecord(
                    flow_uuid=flow_uuid,
                    flow_name=flow_name,
                    flow_type="Product flow",
                    default_unit="kg",
                    unit_group="Units of mass",
                    source="tiangong",
                    is_custom=False,
                )
            )
        db.commit()
        db.execute(text("CREATE VIRTUAL TABLE flow_catalog_fts USING fts5(flow_uuid, flow_name, flow_name_en)"))
        db.execute(
            text(
                "INSERT INTO flow_catalog_fts(flow_uuid, flow_name, flow_name_en) "
                "SELECT flow_uuid, flow_name, coalesce(flow_name_en, '') FROM flow_catalog"
            )
        )
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(flows=True)

    response = client.get("/api/flows?search=树脂&type=intermediate_flow&page_size=10")

    assert response.status_code == 200
    assert [item["flow_name"] for item in response.json()["items"]] == [
        "树脂",
        "树脂材料",
        "复合树脂材料",
    ]


def test_flows_api_filters_and_annotates_ef_tidas_conversion_rows(client):
    package_path = Path(__file__).resolve().parents[1] / "data" / "flow_mappings" / "ghg_ef31_v1.json"
    package = json.loads(package_path.read_text(encoding="utf-8"))
    mapped = package["mappings"][0]
    db = _db_module.SessionLocal()
    try:
        db.merge(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        for flow_uuid, name, source, context in (
            (mapped["ecoinvent_flow_uuid"], mapped["ecoinvent_flow_name"], "ecoinvent_3.11", mapped["ecoinvent_context"]),
            (mapped["ef_flow_uuid"], mapped["ef_flow_name"], "EF3.1", mapped["ef_context"]),
            ("unmapped-ecoinvent", "Unmapped GHG", "ecoinvent_3.11", {"compartment": "air", "subcompartment": "unspecified"}),
        ):
            db.add(FlowRecord(
                flow_uuid=flow_uuid,
                flow_name=name,
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="Units of mass",
                compartment=context.get("catalog_compartment", context.get("compartment")),
                subcompartment=context.get("catalog_subcompartment", context.get("subcompartment")),
                source=source,
                is_custom=False,
            ))
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(flows=True)

    unfiltered = client.get("/api/flows?type=elementary_flow&page_size=10")
    filtered = client.get("/api/flows?type=elementary_flow&conversion_target=ef_tidas&page_size=10")
    categories = client.get("/api/flows/categories?type=elementary_flow&conversion_target=ef_tidas")

    assert unfiltered.status_code == 200
    assert unfiltered.json()["total"] == 3
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 2
    items = {item["flow_id"]: item for item in filtered.json()["items"]}
    assert items[mapped["ecoinvent_flow_uuid"]]["conversion_mode"] == "bidirectional"
    assert items[mapped["ef_flow_uuid"]]["conversion_mode"] == "canonical"
    assert all(item["conversion_target_flow_uuid"] == mapped["ef_flow_uuid"] for item in items.values())
    assert categories.status_code == 200
    assert sum(item["count"] for item in categories.json()["items"]) == 2


def test_flows_api_filters_source_space_before_pagination(client):
    db = _db_module.SessionLocal()
    try:
        db.merge(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        for idx in range(5):
            db.merge(
                FlowRecord(
                    flow_uuid=f"ecoinvent-flow-{idx}",
                    flow_name=f"ecoinvent flow {idx}",
                    flow_type="Elementary flow",
                    default_unit="kg",
                    unit_group="Units of mass",
                    source="ecoinvent_3.11",
                    is_custom=False,
                )
            )
        for idx in range(3):
            db.merge(
                FlowRecord(
                    flow_uuid=f"ef-flow-{idx}",
                    flow_name=f"EF flow {idx}",
                    flow_type="Elementary flow",
                    default_unit="kg",
                    unit_group="Units of mass",
                    source="ef3.1",
                    is_custom=False,
                )
            )
        db.commit()
    finally:
        db.close()

    response = client.get("/api/flows?type=elementary_flow&source_space=tiangong&page=1&page_size=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert [item["flow_id"] for item in payload["items"]] == ["ef-flow-0", "ef-flow-1", "ef-flow-2"]


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


def test_run_model_blocks_ecoinvent_flow_without_lcia_runtime(client, tmp_path, monkeypatch):
    _seed_unit_and_flow(source="ecoinvent_3.11")
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    monkeypatch.setattr(main_module, "DEFAULT_EF31_RUNTIME_ROOT", runtime_root)

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
                    },
                    {
                        "id": "out-co2",
                        "flowUuid": "flow-co2-legacy",
                        "name": "carbon dioxide legacy",
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "amount": 1.0,
                        "type": "biosphere",
                        "direction": "output",
                    },
                ],
                "emissions": [],
            }
        ],
        "exchanges": [],
        "metadata": {},
    }

    response = client.post("/api/model/run", json={"graph": graph, "lcia_methods": ["EF v3.1"]})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "ECOINVENT_LCIA_RUNTIME_NOT_IMPORTED"
    assert detail["evidence"]["required_archive"] == "LCIA_implementation.7z"


def test_save_version_allows_multi_product_unit_group_mismatch(client):
    _seed_unit_and_flow(source="ef3.1")
    _seed_volume_product()
    project_id = "project-mismatch-save"
    db = _db_module.SessionLocal()
    try:
        db.add(Model(id=project_id, name="mismatch save", source_policy="open_mixed"))
        db.commit()
    finally:
        db.close()

    response = client.post(f"/api/projects/{project_id}/versions", json={"graph": _multi_product_mismatched_graph()})

    assert response.status_code == 200
    assert response.json()["project_id"] == project_id


def test_run_model_blocks_multi_product_unit_group_mismatch(client):
    _seed_unit_and_flow(source="ef3.1")
    _seed_volume_product()

    response = client.post("/api/model/run", json={"graph": _multi_product_mismatched_graph(), "lcia_methods": ["EF v3.1"]})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "MULTI_PRODUCT_UNIT_GROUP_MISMATCH"
    assert detail["violations"][0]["base_flow"]["unit_group"] == "Units of mass"
    assert detail["violations"][0]["mismatched_flows"][0]["unit_group"] == "Units of volume"
