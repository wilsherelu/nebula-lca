from __future__ import annotations

import copy
import io
import json
import zipfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import FlowRecord, Model, ModelVersion, UnitDefinition, UnitGroup
from app.services.elementary_flow_conversion_service import (
    MAPPING_CONTEXT_MISMATCH,
    MAPPING_DIRECTION_MISMATCH,
    MAPPING_MISSING,
    ElementaryFlowMappingRegistry,
    build_ef_shadow_export_view,
)
from app.tidas_export import build_tidas_readiness, export_bundle


PACKAGE_PATH = Path(__file__).resolve().parents[1] / "data" / "flow_mappings" / "ghg_ef31_v1.json"


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add(UnitGroup(name="Units of mass", reference_unit="kg"))
    session.add(UnitDefinition(
        unit_group="Units of mass",
        unit_name="kg",
        factor_to_reference=1.0,
        is_reference=True,
    ))
    session.add(UnitDefinition(
        unit_group="Units of mass",
        unit_name="g",
        factor_to_reference=0.001,
        is_reference=False,
    ))
    session.commit()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _package() -> dict:
    return json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))


def _add_flow(db, uuid: str, name: str, source: str, context: dict) -> None:
    compartment = context.get("catalog_compartment", context.get("compartment"))
    subcompartment = context.get("catalog_subcompartment", context.get("subcompartment"))
    db.add(FlowRecord(
        flow_uuid=uuid,
        flow_name=name,
        flow_name_en=name,
        flow_type="Elementary flow",
        default_unit="kg",
        unit_group="Units of mass",
        compartment=compartment,
        subcompartment=subcompartment,
        source=source,
        is_custom=False,
    ))
    db.commit()


def _graph(flow_uuid: str, *, direction: str = "output", amount: float = 2.0) -> dict:
    bucket = "outputs" if direction == "output" else "inputs"
    return {
        "nodes": [{
            "id": "process-1",
            "process_uuid": "process-1",
            "node_kind": "unit_process",
            "name": "Process",
            "inputs": [],
            "outputs": [],
            bucket: [{
                "id": "exchange-1",
                "flowUuid": flow_uuid,
                "name": "Source flow",
                "amount": amount,
                "unit": "kg",
                "unitGroup": "Units of mass",
                "type": "biosphere",
                "direction": direction,
            }],
        }],
        "exchanges": [],
    }


def test_registry_exposes_reviewed_v12_package():
    registry = ElementaryFlowMappingRegistry(PACKAGE_PATH)

    assert registry.package_version == "1.2.0"
    assert len(registry.compatible_flow_uuids) == 376


def test_ecoinvent_exchange_converts_without_mutating_source_graph(db):
    row = _package()["mappings"][0]
    _add_flow(db, row["ecoinvent_flow_uuid"], row["ecoinvent_flow_name"], "ecoinvent_3.11", row["ecoinvent_context"])
    _add_flow(db, row["ef_flow_uuid"], row["ef_flow_name"], "EF3.1", row["ef_context"])
    graph = _graph(row["ecoinvent_flow_uuid"])
    original = copy.deepcopy(graph)

    view = build_ef_shadow_export_view(db, graph, ElementaryFlowMappingRegistry(PACKAGE_PATH))

    assert view.blocking == []
    converted = view.graph["nodes"][0]["outputs"][0]
    assert converted["flowUuid"] == row["ef_flow_uuid"]
    assert converted["amount"] == pytest.approx(2.0 * row["eco_to_ef_factor"])
    assert converted["unit"] == "kg"
    assert view.summary()["ecoinvent_to_ef_count"] == 1
    assert graph == original


def test_one_way_ef_alias_resolves_through_ecoinvent_to_canonical_ef(db):
    package = _package()
    alias = package["one_way_mappings"][0]
    canonical = next(row for row in package["mappings"] if row["ecoinvent_flow_uuid"] == alias["ecoinvent_flow_uuid"])
    _add_flow(db, alias["ef_flow_uuid"], alias["ef_flow_name"], "EF3.1", alias["ef_context"])
    _add_flow(db, canonical["ef_flow_uuid"], canonical["ef_flow_name"], "EF3.1", canonical["ef_context"])

    view = build_ef_shadow_export_view(db, _graph(alias["ef_flow_uuid"]), ElementaryFlowMappingRegistry(PACKAGE_PATH))

    assert view.blocking == []
    assert view.graph["nodes"][0]["outputs"][0]["flowUuid"] == canonical["ef_flow_uuid"]
    assert view.traces[0]["mode"] == "one_way_canonicalization"
    assert view.traces[0]["warning_codes"] == ["ONE_WAY_CANONICALIZATION"]


def test_exchange_amount_is_normalized_to_source_default_unit_before_mapping(db):
    row = _package()["mappings"][0]
    _add_flow(db, row["ecoinvent_flow_uuid"], row["ecoinvent_flow_name"], "ecoinvent_3.11", row["ecoinvent_context"])
    _add_flow(db, row["ef_flow_uuid"], row["ef_flow_name"], "EF3.1", row["ef_context"])
    graph = _graph(row["ecoinvent_flow_uuid"], amount=2000.0)
    graph["nodes"][0]["outputs"][0]["unit"] = "g"

    view = build_ef_shadow_export_view(db, graph, ElementaryFlowMappingRegistry(PACKAGE_PATH))

    assert view.blocking == []
    assert view.graph["nodes"][0]["outputs"][0]["amount"] == pytest.approx(2.0)
    assert view.graph["nodes"][0]["outputs"][0]["unit"] == "kg"


def test_unmapped_ecoinvent_flow_is_blocked(db):
    _add_flow(db, "unmapped-eco", "Unmapped", "ecoinvent_3.11", {"compartment": "air", "subcompartment": "unspecified"})

    view = build_ef_shadow_export_view(db, _graph("unmapped-eco"), ElementaryFlowMappingRegistry(PACKAGE_PATH))

    assert [issue["code"] for issue in view.blocking] == [MAPPING_MISSING]


def test_context_drift_is_blocked(db):
    row = _package()["mappings"][0]
    wrong_context = {**row["ecoinvent_context"], "subcompartment": "unspecified"}
    _add_flow(db, row["ecoinvent_flow_uuid"], row["ecoinvent_flow_name"], "ecoinvent_3.11", wrong_context)
    _add_flow(db, row["ef_flow_uuid"], row["ef_flow_name"], "EF3.1", row["ef_context"])

    view = build_ef_shadow_export_view(db, _graph(row["ecoinvent_flow_uuid"]), ElementaryFlowMappingRegistry(PACKAGE_PATH))

    assert [issue["code"] for issue in view.blocking] == [MAPPING_CONTEXT_MISMATCH]


def test_mapping_direction_mismatch_is_blocked(db):
    row = _package()["mappings"][0]
    _add_flow(db, row["ecoinvent_flow_uuid"], row["ecoinvent_flow_name"], "ecoinvent_3.11", row["ecoinvent_context"])
    _add_flow(db, row["ef_flow_uuid"], row["ef_flow_name"], "EF3.1", row["ef_context"])

    view = build_ef_shadow_export_view(
        db,
        _graph(row["ecoinvent_flow_uuid"], direction="input"),
        ElementaryFlowMappingRegistry(PACKAGE_PATH),
    )

    assert [issue["code"] for issue in view.blocking] == [MAPPING_DIRECTION_MISMATCH]


def test_tidas_export_uses_shadow_graph_and_preserves_model_version(db):
    row = _package()["mappings"][0]
    _add_flow(db, row["ecoinvent_flow_uuid"], row["ecoinvent_flow_name"], "ecoinvent_3.11", row["ecoinvent_context"])
    _add_flow(db, row["ef_flow_uuid"], row["ef_flow_name"], "EF3.1", row["ef_context"])
    db.add(FlowRecord(
        flow_uuid="product-flow",
        flow_name="Product",
        flow_type="Product flow",
        default_unit="kg",
        unit_group="Units of mass",
        source="Tiangong 1.0",
        is_custom=False,
    ))
    graph = {
        "functionalUnit": "1 kg Product",
        "nodes": [{
            "id": "process-1",
            "process_uuid": "process-1",
            "node_kind": "unit_process",
            "mode": "balanced",
            "name": "Process",
            "location": "CN",
            "reference_product": "Product",
            "inputs": [],
            "outputs": [
                {
                    "id": "product-output",
                    "flowUuid": "product-flow",
                    "name": "Product",
                    "amount": 1.0,
                    "unit": "kg",
                    "unitGroup": "Units of mass",
                    "type": "technosphere",
                    "direction": "output",
                    "isProduct": True,
                },
                {
                    "id": "ghg-output",
                    "flowUuid": row["ecoinvent_flow_uuid"],
                    "name": row["ecoinvent_flow_name"],
                    "amount": 1.0,
                    "unit": "kg",
                    "unitGroup": "Units of mass",
                    "type": "biosphere",
                    "direction": "output",
                },
            ],
        }],
        "exchanges": [],
        "metadata": {},
    }
    db.add(Model(
        id="conversion-project",
        name="Conversion project",
        reference_product="Product",
        functional_unit="1 kg Product",
        source_policy="open_mixed",
        allowed_lcia_scope="ef31_only",
    ))
    db.add(ModelVersion(
        id="conversion-version",
        model_id="conversion-project",
        version=1,
        hybrid_graph_json=copy.deepcopy(graph),
    ))
    db.commit()

    readiness = build_tidas_readiness(db, "conversion-project", bundle_mode="self_contained")
    archive_bytes, report = export_bundle(db, "conversion-project", bundle_mode="self_contained")

    assert readiness["can_export"] is True
    assert readiness["elementary_flow_conversion"]["ecoinvent_to_ef_count"] == 1
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        names = archive.namelist()
        assert "elementary_flow_conversion.json" in names
        business_dataset_json = "\n".join(
            archive.read(name).decode("utf-8")
            for name in names
            if name.endswith(".json")
            and name not in {"elementary_flow_conversion.json", "export_report.json"}
        )
        conversion_trace = json.loads(archive.read("elementary_flow_conversion.json"))
    assert row["ef_flow_uuid"] in business_dataset_json
    assert row["ecoinvent_flow_uuid"] not in business_dataset_json
    assert conversion_trace["traces"][0]["source_flow_uuid"] == row["ecoinvent_flow_uuid"]
    assert report.elementary_flow_conversion["package_version"] == "1.2.0"
    db.expire_all()
    persisted = db.get(ModelVersion, "conversion-version")
    assert persisted.hybrid_graph_json == graph
