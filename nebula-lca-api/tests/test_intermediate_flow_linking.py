from __future__ import annotations

import json
from dataclasses import replace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.database import SessionLocal, engine as app_engine
from app.api.intermediate_flow_links import (
    ConfirmL2Request,
    ResolveBatchRequest,
    ResolvePortRequest,
    confirm_l2,
    resolve_batch,
)
from app.main import app
from app.models import FlowRecord, LciProcessVector, Model, ModelVersion, ReferenceProcess, UnitDefinition
from app.schemas import HybridGraph, IntermediateFlowLink
from app.services.graph_contract import analyze_handle_consistency, validate_graph_contract
from app.services.intermediate_flow_linking_service import (
    DEFAULT_PACKAGE_PATH,
    IntermediateFlowLinkRegistry,
    _validate_resolution_records,
    _resolution_with_catalog_units,
    backfill_ecoinvent_reference_flow_uuids,
    get_intermediate_flow_link_registry,
    list_provider_candidates,
    resolve_intermediate_flow,
    validate_graph_intermediate_flow_links,
    validate_intermediate_flow_link,
)
from app.tidas_export import build_tidas_readiness


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()


def _seed_first_l1_pair(db):
    package = json.loads(DEFAULT_PACKAGE_PATH.read_text(encoding="utf-8"))
    rows = package.get("rules") or package.get("mappings") or []
    row = dict(next(item for item in rows if item["mapping_level"] == "L1"))
    dimension = row.get("unit_dimension") or "mass"
    source_groups = {"mass": "Units of mass", "energy": "Units of energy", "volume": "Units of volume"}
    target_groups = {"mass": "mass", "energy": "energy", "volume": "volume"}
    row.setdefault("source_name", f"source-{row['source_flow_uuid']}")
    row.setdefault("target_name", f"target-{row['target_flow_uuid']}")
    row.setdefault("source_unit_group", source_groups.get(dimension, dimension))
    row.setdefault("target_unit_group", target_groups.get(dimension, dimension))
    db.add_all([
        FlowRecord(
            flow_uuid=row["source_flow_uuid"],
            flow_name=row["source_name"],
            flow_name_en=row["source_name"],
            flow_type=row["source_flow_type"],
            default_unit=row["source_unit"],
            unit_group=row["source_unit_group"],
            source="Tiangong",
        ),
        FlowRecord(
            flow_uuid=row["target_flow_uuid"],
            flow_name=row["target_name"],
            flow_name_en=row["target_name"],
            flow_type=row["target_flow_type"],
            default_unit=row["target_unit"],
            unit_group=row["target_unit_group"],
            source="ecoinvent_3.11",
        ),
    ])
    db.commit()
    return row


def _seed_first_compatible_pair(db):
    package = json.loads(DEFAULT_PACKAGE_PATH.read_text(encoding="utf-8"))
    row = dict(next(item for item in package["mappings"] if item["application_mode"] == "auto_compatible"))
    dimension = row["unit_dimension"]
    source_groups = {"mass": "Units of mass", "energy": "Units of energy", "volume": "Units of volume"}
    target_groups = {"mass": "mass", "energy": "energy", "volume": "volume"}
    row.update({
        "source_name": f"source-{row['source_flow_uuid']}",
        "target_name": f"target-{row['target_flow_uuid']}",
        "source_unit_group": source_groups.get(dimension, dimension),
        "target_unit_group": target_groups.get(dimension, dimension),
    })
    db.add_all([
        FlowRecord(
            flow_uuid=row["source_flow_uuid"], flow_name=row["source_name"],
            flow_type=row["source_flow_type"], default_unit=row["source_unit"],
            unit_group=row["source_unit_group"], source="Tiangong",
        ),
        FlowRecord(
            flow_uuid=row["target_flow_uuid"], flow_name=row["target_name"],
            flow_type=row["target_flow_type"], default_unit=row["target_unit"],
            unit_group=row["target_unit_group"], source="ecoinvent_3.11",
        ),
    ])
    db.commit()
    return row


def test_incremental_reviewed_medium_voltage_rule_is_available():
    resolution = get_intermediate_flow_link_registry().resolve(
        "128818e9-cefb-4c94-8b9b-fe8883223f3b"
    )

    assert resolution is not None
    assert resolution.target_flow_uuid == "759b89bd-3aa6-42ad-b767-5bb9ef5d331d"
    assert resolution.mapping_level == "L2"
    assert resolution.amount_factor == pytest.approx(1 / 3.6)


def test_rework_acceptance_package_keeps_only_globally_unique_l1():
    registry = get_intermediate_flow_link_registry()
    copper_scrap = registry.resolve("dd15940e-a6be-4335-9a0b-d754746a4713")
    electricity_alias = registry.resolve("c0e1aaac-9086-46ad-9d83-878f9fb97da4")

    assert registry.package_version == "2.14.0"
    assert len(registry.rules) == 1389
    assert copper_scrap is not None
    assert copper_scrap.mapping_level == "L1"
    assert copper_scrap.target_flow_uuid == "cc0d4252-6207-41d6-8567-bcbad58a7bef"
    assert electricity_alias is not None
    assert electricity_alias.mapping_level == "L2"
    assert "L1_GLOBAL_UNIQUENESS_NOT_MET" in electricity_alias.warnings


def test_l3_shortlist_is_not_exposed_as_an_executable_builtin_rule():
    registry = get_intermediate_flow_link_registry()

    assert registry.resolve("0fab8c14-7641-454b-9f45-9201ddfe567a") is None


def test_voltage_band_corrections_keep_only_safe_executable_rules():
    registry = get_intermediate_flow_link_registry()

    low_voltage_sources = {
        "1d628fbe-aeb6-5714-9402-020bdbe70cb6",
        "50657322-939c-4829-a87b-47c093bfa6a7",
    }
    for source_uuid in low_voltage_sources:
        resolution = registry.resolve(source_uuid)
        assert resolution is not None
        assert resolution.mapping_level == "L2"
        assert resolution.target_flow_uuid == "d69294d7-8d64-4915-a896-9996a014c410"
        assert resolution.amount_factor == pytest.approx(1 / 3.6)

    ambiguous_voltage_sources = {
        "09635ee8-8993-4f3e-96fb-54917a2127c5",
        "890a70b7-b677-4e2a-8a1b-7d017e0a10ae",
        "d90ae09f-433d-46f0-a4f2-79c9e74a7c07",
    }
    assert all(
        registry.resolve(source_uuid) is None
        for source_uuid in ambiguous_voltage_sources
    )


def test_catalog_role_drift_rules_are_withdrawn_from_execution():
    registry = get_intermediate_flow_link_registry()
    withdrawn_sources = {
        "1eb68227-7d39-4354-956b-2acb63931247",
        "37a44719-0587-415a-a222-a2e48dfe8e80",
        "7e0caeab-a6f8-4706-b349-7ed22e0832bb",
        "e1a44d20-d968-4d64-bd7c-253a1441ab35",
        "e7f0099e-ecb6-48b5-a683-c0da022022b4",
    }

    assert all(
        registry.resolve(source_uuid) is None
        for source_uuid in withdrawn_sources
    )


def test_orphan_sources_are_not_exposed_as_executable_builtin_rules():
    registry = get_intermediate_flow_link_registry()
    orphan_sources = {
        "72c8ae93-36ff-4153-a0e2-84fef01462e7",
        "5fd237b0-2305-4216-8c40-eb1524a8c177",
        "695bb4aa-fca2-44a8-8656-3aa7316e854d",
        "b59452f3-c6c2-4d36-a17a-ca4f106b82a6",
        "dce4b62f-0ba7-460a-b99a-7d62e6b55932",
        "3fbc2a8c-5368-436b-b0f7-0b6bdb88e99a",
        "78db838b-1b97-48ac-b90d-39a678be7a4a",
        "e49e116f-436d-4aa7-9994-d8c221c76fe0",
    }

    assert all(registry.resolve(source_uuid) is None for source_uuid in orphan_sources)


def test_reviewed_v4_l2_increment_is_available_without_reprocessing():
    resolution = get_intermediate_flow_link_registry().resolve(
        "009a4421-0580-4496-9a93-688f7686b995"
    )

    assert resolution is not None
    assert resolution.target_flow_uuid == "759b89bd-3aa6-42ad-b767-5bb9ef5d331d"
    assert resolution.mapping_level == "L2"
    assert resolution.application_mode == "auto_compatible"


def test_pragmatic_l3_review_removes_public_rules_and_retargets_animal_water():
    registry = get_intermediate_flow_link_registry()
    removed_sources = {
        "85c3d99f-341d-42c8-8fb5-8c7c8ceab69d",
        "c05f08b8-e3a2-4f7c-8534-e04c89be8a9f",
        "d6a6b877-75b8-48c5-b219-857c70de3e3d",
        "e4b98afa-eb1f-4372-a463-1a440ce31f7f",
        "fa402946-6236-41bb-8a2d-f7890b2e75cf",
    }

    assert all(registry.resolve(source_uuid) is None for source_uuid in removed_sources)
    animal_water = registry.resolve("cf8836e6-a586-452d-9c53-b6487e93d07d")
    assert animal_water is not None
    assert animal_water.mapping_level == "L2"
    assert animal_water.target_flow_uuid == "c5adb1fb-872e-4446-a3bb-c4b61aa4bd45"
    assert animal_water.amount_factor == 1


def test_dry_basis_sodium_hydroxide_rule_keeps_factor_one():
    resolution = get_intermediate_flow_link_registry().resolve(
        "2e7fda39-6310-42b0-ab45-b4eb571dd825"
    )

    assert resolution is not None
    assert resolution.target_flow_uuid == "61396bcb-bf35-411a-a6a6-8543ccef83e8"
    assert resolution.mapping_level == "L2"
    assert resolution.amount_factor == 1
    assert "DRY_SUBSTANCE_QUANTITY_BASIS" in resolution.warnings


def test_reviewed_l2_flow_subtype_override_is_explicit_and_validated(tmp_path, db):
    payload = {
        "package_id": "override-test",
        "version": "1.0.0",
        "direction": "tiangong_to_ecoinvent",
        "unit_group_contracts": {
            "mass": {
                "source_unit_group": "Units of mass",
                "target_unit_group": "mass",
            }
        },
        "mappings": [{
            "rule_id": "override-rule",
            "source_flow_uuid": "source-product-metadata",
            "target_flow_uuid": "target-waste",
            "source_flow_type": "Product flow",
            "target_flow_type": "Waste flow",
            "source_unit": "kg",
            "target_unit": "kg",
            "unit_dimension": "mass",
            "amount_factor": 1,
            "mapping_level": "L2",
            "review_status": "approved_with_warning",
            "application_mode": "auto_compatible",
            "flow_subtype_override": True,
            "warnings": ["FLOW_SUBTYPE_OVERRIDE"],
            "evidence_sha256": "a" * 64,
        }],
    }
    missing_evidence = json.loads(json.dumps(payload))
    missing_evidence["mappings"][0].pop("evidence_sha256")
    missing_path = tmp_path / "override-missing-evidence.json"
    missing_path.write_text(json.dumps(missing_evidence), encoding="utf-8")
    with pytest.raises(ValueError, match="flow type mismatch"):
        IntermediateFlowLinkRegistry(missing_path)

    package_path = tmp_path / "override.json"
    package_path.write_text(json.dumps(payload), encoding="utf-8")
    registry = IntermediateFlowLinkRegistry(package_path)
    resolution = registry.resolve("source-product-metadata")

    assert resolution is not None
    assert resolution.flow_subtype_override is True
    assert resolution.source_flow_type == "Product flow"
    assert resolution.target_flow_type == "Waste flow"
    link = IntermediateFlowLink.model_validate({
        **resolution.to_dict(),
        "status": "user_confirmed",
    })
    assert link.flow_subtype_override is True
    assert link.source_flow_type == "Product flow"
    assert link.target_flow_type == "Waste flow"

    source = FlowRecord(
        flow_uuid="source-product-metadata",
        flow_name="waste oil",
        flow_type="Product flow",
        default_unit="kg",
        unit_group="Units of mass",
        source="Tiangong",
    )
    target = FlowRecord(
        flow_uuid="target-waste",
        flow_name="waste mineral oil",
        flow_type="Waste flow",
        default_unit="kg",
        unit_group="mass",
        source="ecoinvent_3.11",
    )
    db.add_all([source, target])
    db.commit()
    assert _validate_resolution_records(source, target, resolution) is None

    payload["mappings"][0]["warnings"] = ["SEMANTIC_GENERALIZATION"]
    package_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="flow type mismatch"):
        IntermediateFlowLinkRegistry(package_path)


def test_tidas_import_source_is_eligible_for_reviewed_forward_mapping(db):
    row = _seed_first_l1_pair(db)
    source = db.get(FlowRecord, row["source_flow_uuid"])
    source.source = "tidas_bundle_import"
    db.commit()

    resolution, issue = resolve_intermediate_flow(db, row["source_flow_uuid"])

    assert issue is None
    assert resolution is not None
    assert resolution.target_flow_uuid == row["target_flow_uuid"]


def _graph(row, *, package_hash: str | None = None) -> HybridGraph:
    registry = get_intermediate_flow_link_registry()
    mapping_level = row.get("mapping_level", "L1")
    return HybridGraph.model_validate({
        "functionalUnit": "1 kg",
        "nodes": [
            {
                "id": "provider",
                "node_kind": "lci_dataset",
                "mode": "normalized",
                "process_uuid": "eco-provider",
                "name": "Provider",
                "location": "RER",
                "reference_product": row["target_name"],
                "inputs": [],
                "outputs": [{
                    "id": "eco-out",
                    "flowUuid": row["target_flow_uuid"],
                    "name": row["target_name"],
                    "unit": row["target_unit"],
                    "unitGroup": row["target_unit_group"],
                    "amount": 1.0,
                    "isProduct": True,
                    "type": "technosphere",
                    "direction": "output",
                }],
            },
            {
                "id": "consumer",
                "node_kind": "unit_process",
                "mode": "balanced",
                "process_uuid": "tg-consumer",
                "name": "Consumer",
                "location": "CN",
                "reference_product": "result",
                "inputs": [{
                    "id": "tg-in",
                    "flowUuid": row["source_flow_uuid"],
                    "name": row["source_name"],
                    "unit": row["source_unit"],
                    "unitGroup": row["source_unit_group"],
                    "amount": 2.0,
                    "type": "technosphere",
                    "direction": "input",
                    "intermediateFlowLink": {
                        "sourceFlowUuid": row["source_flow_uuid"],
                        "targetFlowUuid": row["target_flow_uuid"],
                        "amountFactor": row["amount_factor"],
                        "sourceUnit": row["source_unit"],
                        "targetUnit": row["target_unit"],
                        "mappingLevel": mapping_level,
                        "mappingReason": (
                            "approved_one_way_reference_product_link"
                            if mapping_level == "L1"
                            else "approved_one_way_compatible_reference_product_link"
                        ),
                        "ruleId": row["rule_id"],
                        "ruleOrigin": "builtin",
                        "status": "auto",
                        "packageId": registry.package_id,
                        "packageVersion": registry.package_version,
                        "packageHash": package_hash or registry.package_hash,
                        "applicationMode": row.get("application_mode", "strict_identity"),
                        "warnings": row.get("warnings", []),
                    },
                }],
                "outputs": [{
                    "id": "result-out",
                    "flowUuid": "result-flow",
                    "name": "result",
                    "unit": "kg",
                    "amount": 1.0,
                    "isProduct": True,
                    "type": "technosphere",
                    "direction": "output",
                }],
            },
        ],
        "exchanges": [{
            "id": "linked-edge",
            "fromNode": "provider",
            "toNode": "consumer",
            "sourceHandle": "out:eco-out",
            "targetHandle": "in:tg-in",
            "flowUuid": row["target_flow_uuid"],
            "consumerFlowUuid": row["source_flow_uuid"],
            "flowName": row["target_name"],
            "quantityMode": "dual",
            "amount": 2.0,
            "providerAmount": 1.0,
            "consumerAmount": 2.0,
            "unit": row["target_unit"],
            "type": "technosphere",
            "allocation": "none",
            "intermediateFlowLinkRuleId": row["rule_id"],
            "intermediateFlowLinkFactor": row["amount_factor"],
        }],
    })


def test_l1_resolution_is_one_way_and_validated_against_catalog(db):
    row = _seed_first_l1_pair(db)
    resolution, issue = resolve_intermediate_flow(db, row["source_flow_uuid"])
    assert issue is None
    assert resolution is not None
    assert resolution.target_flow_uuid == row["target_flow_uuid"]
    assert resolution.to_dict()["link_direction"] == "tiangong_to_ecoinvent"


def test_hidden_background_provider_is_preserved_in_graph_contract(db):
    row = _seed_first_l1_pair(db)
    payload = _graph(row).model_dump(mode="json", by_alias=True)
    payload["nodes"][0]["hidden"] = True

    graph = HybridGraph.model_validate(payload)

    assert graph.nodes[0].hidden is True
    assert graph.model_dump(mode="json", by_alias=True)["nodes"][0]["hidden"] is True
    validate_graph_contract(graph)
    validate_graph_intermediate_flow_links(db, graph)


def test_warned_l2_resolution_requires_confirmation_and_evidence_is_checked(db):
    row = _seed_first_compatible_pair(db)
    resolution, issue = resolve_intermediate_flow(db, row["source_flow_uuid"])
    assert issue is None
    assert resolution is not None
    assert resolution.mapping_level == "L2"
    assert resolution.application_mode == "auto_compatible"
    assert resolution.warnings

    payload = {
        **resolution.to_dict(),
        "rule_origin": "builtin",
        "status": "auto",
    }
    link = IntermediateFlowLink.model_validate(payload)
    assert validate_intermediate_flow_link(db, row["source_flow_uuid"], link) is None
    stale = link.model_copy(update={"warnings": ["stale"]})
    assert validate_intermediate_flow_link(db, row["source_flow_uuid"], stale) == "L2_EVIDENCE_MISMATCH"

    confirmed_payload = confirm_l2(ConfirmL2Request(
        source_flow_uuid=row["source_flow_uuid"],
        rule_id=row["rule_id"],
    ), db)
    assert confirmed_payload["status"] == "user_confirmed"
    confirmed_link = IntermediateFlowLink.model_validate(confirmed_payload)
    assert validate_intermediate_flow_link(db, row["source_flow_uuid"], confirmed_link) is None

    graph = _graph(row)
    validate_graph_contract(graph)
    validate_graph_intermediate_flow_links(db, graph)


def test_reviewed_factor_uses_current_catalog_unit_spelling(db):
    row = _seed_first_compatible_pair(db)
    source = db.get(FlowRecord, row["source_flow_uuid"])
    target = db.get(FlowRecord, row["target_flow_uuid"])
    source.default_unit = "MJ"
    source.unit_group = "Units of energy"
    target.default_unit = "kWh"
    target.unit_group = "energy"
    db.add_all([
        UnitDefinition(unit_group="Units of energy", unit_name="MJ", factor_to_reference=1.0),
        UnitDefinition(unit_group="energy", unit_name="kWh", factor_to_reference=3.6),
    ])
    row["amount_factor"] = 1 / 3.6
    registry_resolution = get_intermediate_flow_link_registry().resolve(row["source_flow_uuid"])
    assert registry_resolution is not None
    registry_resolution = replace(
        registry_resolution,
        source_unit="MJ",
        target_unit="kwh",
        amount_factor=1 / 3.6,
    )
    db.commit()

    resolution = _resolution_with_catalog_units(db, source, target, registry_resolution)

    assert resolution.source_unit == "MJ"
    assert resolution.target_unit == "kWh"
    assert resolution.amount_factor == pytest.approx(1 / 3.6)
    assert _validate_resolution_records(source, target, resolution) is None


def test_alias_edge_preserves_consumer_uuid_and_requires_current_l1_evidence(db):
    row = _seed_first_l1_pair(db)
    graph = _graph(row)
    validate_graph_contract(graph)
    validate_graph_intermediate_flow_links(db, graph)
    assert graph.nodes[1].inputs[0].flowUuid == row["source_flow_uuid"]
    assert graph.exchanges[0].flowUuid == row["target_flow_uuid"]
    assert graph.exchanges[0].consumer_flow_uuid == row["source_flow_uuid"]
    assert analyze_handle_consistency(graph)["ok"] is True

    stale = _graph(row, package_hash="stale")
    with pytest.raises(HTTPException) as exc:
        validate_graph_intermediate_flow_links(db, stale)
    assert exc.value.detail["code"] == "INVALID_INTERMEDIATE_FLOW_LINK"
    assert exc.value.detail["evidence"][0]["reason"] == "L1_EVIDENCE_MISMATCH"


def test_provider_candidates_are_all_returned_and_never_collapsed(db):
    row = _seed_first_l1_pair(db)
    for index, location in enumerate(("RER", "CH", "GLO"), start=1):
        process_uuid = f"provider-{index}"
        db.add(ReferenceProcess(
            process_uuid=process_uuid,
            process_name=f"Provider {index}",
            process_type="lci_dataset",
            reference_flow_uuid=row["target_flow_uuid"],
            process_json={"location": location, "reference_product_id": row["target_flow_uuid"]},
        ))
        if index != 3:
            db.add(LciProcessVector(process_uuid=process_uuid, nnz=index, amounts_blob=b"x"))
    db.commit()

    providers = list_provider_candidates(db, row["target_flow_uuid"])
    assert len(providers) == 3
    assert {item["location"] for item in providers} == {"RER", "CH", "GLO"}
    assert sum(bool(item["has_lci_vector"]) for item in providers) == 2


def test_provider_candidates_require_exact_reference_flow_identity(db):
    row = _seed_first_l1_pair(db)
    db.add(ReferenceProcess(
        process_uuid="hiqlcd-provider",
        process_name="HiQLCD provider",
        process_type="lci_dataset",
        reference_flow_uuid="hiqlcd-product-flow",
        process_json={"reference_product_id": "hiqlcd-product-flow"},
        source_file="hiqlcd://datasets/dataset-1",
    ))
    db.add(LciProcessVector(process_uuid="hiqlcd-provider", nnz=1, amounts_blob=b"x"))
    db.commit()

    providers = list_provider_candidates(db, row["target_flow_uuid"])

    assert all(item["process_uuid"] != "hiqlcd-provider" for item in providers)


def test_unreviewed_name_candidates_are_opt_in_only(db):
    db.add_all([
        FlowRecord(
            flow_uuid="unreviewed-source",
            flow_name="custom widget",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="Tiangong",
        ),
        FlowRecord(
            flow_uuid="unreviewed-target",
            flow_name="custom widget",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="mass",
            source="ecoinvent_3.11",
        ),
    ])
    db.commit()
    item = ResolvePortRequest(flow_uuid="unreviewed-source")

    default_result = resolve_batch(ResolveBatchRequest(items=[item]), db)
    assert default_result["items"][0]["status"] == "unmatched"
    assert default_result["items"][0]["l2_candidates"] == []

    audit_result = resolve_batch(ResolveBatchRequest(
        items=[item],
        include_unreviewed_candidates=True,
    ), db)
    assert audit_result["items"][0]["status"] == "L2"
    assert audit_result["items"][0]["l2_candidates"][0]["target_flow_uuid"] == "unreviewed-target"


def test_reference_flow_backfill_is_uuid_only_dry_run_commit_and_idempotent(db):
    row = _seed_first_l1_pair(db)
    db.add_all([
        ReferenceProcess(
            process_uuid="valid",
            process_name="Valid",
            process_type="lci_dataset",
            process_json={"reference_product_id": row["target_flow_uuid"]},
        ),
        ReferenceProcess(
            process_uuid="missing",
            process_name="Missing",
            process_type="lci_dataset",
            process_json={"reference_product_id": "unknown-uuid"},
        ),
    ])
    db.commit()

    dry_run = backfill_ecoinvent_reference_flow_uuids(db, commit=False)
    assert dry_run["updated"] == 1
    assert db.get(ReferenceProcess, "valid").reference_flow_uuid is None

    committed = backfill_ecoinvent_reference_flow_uuids(db, commit=True)
    assert committed["updated"] == 1
    assert db.get(ReferenceProcess, "valid").reference_flow_uuid == row["target_flow_uuid"]
    repeated = backfill_ecoinvent_reference_flow_uuids(db, commit=True)
    assert repeated["updated"] == 0
    assert repeated["unchanged"] == 1


def test_tidas_readiness_blocks_calculation_only_eco_provider_links(db):
    row = _seed_first_l1_pair(db)
    graph = _graph(row).model_dump(mode="json", by_alias=True)
    model = Model(id="linked-project", name="Linked project", source_policy="open_mixed")
    db.add(model)
    db.add(ModelVersion(model_id=model.id, version=1, hybrid_graph_json=graph))
    db.commit()

    readiness = build_tidas_readiness(db, model.id)
    assert readiness["can_export"] is False
    issue = next(item for item in readiness["blocking"] if item["code"] == "INTERMEDIATE_FLOW_ECO_PROVIDER_LINK_PRESENT")
    assert issue["details"]["link_count"] == 1


def test_resolve_and_provider_apis_keep_provider_choice_explicit():
    Base.metadata.create_all(app_engine)
    session = SessionLocal()
    try:
        row = _seed_first_l1_pair(session)
        for index in range(2):
            process_uuid = f"api-provider-{index}"
            session.merge(ReferenceProcess(
                process_uuid=process_uuid,
                process_name=f"API Provider {index}",
                process_type="lci_dataset",
                reference_flow_uuid=row["target_flow_uuid"],
                process_json={"location": f"L{index}", "reference_product_id": row["target_flow_uuid"]},
            ))
            session.merge(LciProcessVector(process_uuid=process_uuid, nnz=1, amounts_blob=b"x"))
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        resolved = client.post("/api/intermediate-flow-links/resolve-batch", json={
            "items": [{
                "port_id": "input-1",
                "flow_uuid": row["source_flow_uuid"],
                "direction": "input",
                "exchange_type": "technosphere",
            }],
        })
        assert resolved.status_code == 200
        assert resolved.json()["items"][0]["status"] == "L1"

        providers = client.get(
            "/api/intermediate-flow-links/providers",
            params={"target_flow_uuid": row["target_flow_uuid"]},
        )
        assert providers.status_code == 200
        payload = providers.json()
        assert payload["auto_selected"] is False
        assert {item["process_uuid"] for item in payload["providers"]}.issuperset(
            {"api-provider-0", "api-provider-1"}
        )

        created = client.post("/api/intermediate-flow-links/user-rules", json={
            "source_flow_uuid": row["source_flow_uuid"],
            "target_flow_uuid": row["target_flow_uuid"],
        })
        assert created.status_code == 201
        assert created.json()["mapping_level"] == "L3"
        assert created.json()["amount_factor"] == 1.0
        assert created.json()["mapping_reason"] == ""
        assert created.json()["source_unit_group"] == row["source_unit_group"]
        assert created.json()["target_unit_group"] == row["target_unit_group"]
        assert created.json()["status"] == "user_confirmed"
        deactivated = client.delete(f"/api/intermediate-flow-links/user-rules/{created.json()['id']}")
        assert deactivated.status_code == 200
        assert deactivated.json()["status"] == "inactive"


def test_user_rule_accepts_explicit_cross_group_factor():
    Base.metadata.create_all(app_engine)
    source_uuid = "test-cross-group-source"
    target_uuid = "test-cross-group-target"
    session = SessionLocal()
    try:
        session.merge(FlowRecord(
            flow_uuid=source_uuid,
            flow_name="Raw fuel",
            flow_type="Product flow",
            default_unit="MJ",
            unit_group="Units of energy",
            source="tidas_bundle_import",
        ))
        session.merge(FlowRecord(
            flow_uuid=target_uuid,
            flow_name="Fuel mass",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="mass",
            source="ecoinvent_3.11",
        ))
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        missing_factor = client.post("/api/intermediate-flow-links/user-rules", json={
            "source_flow_uuid": source_uuid,
            "target_flow_uuid": target_uuid,
        })
        assert missing_factor.status_code == 422
        assert missing_factor.json()["detail"]["code"] == "CROSS_GROUP_FACTOR_REQUIRED"

        created = client.post("/api/intermediate-flow-links/user-rules", json={
            "source_flow_uuid": source_uuid,
            "target_flow_uuid": target_uuid,
            "amount_factor": 0.04,
            "mapping_reason": "Declared heating-value conversion",
        })
        assert created.status_code == 201
        payload = created.json()
        assert payload["amount_factor"] == pytest.approx(0.04)
        assert payload["source_unit_group"] == "Units of energy"
        assert payload["target_unit_group"] == "mass"

        resolved = client.post("/api/intermediate-flow-links/resolve-batch", json={
            "items": [{
                "port_id": "input-cross-group",
                "flow_uuid": source_uuid,
                "direction": "input",
                "exchange_type": "technosphere",
            }],
        })
        assert resolved.status_code == 200
        assert resolved.json()["items"][0]["status"] == "L3"
