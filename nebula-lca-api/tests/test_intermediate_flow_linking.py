from __future__ import annotations

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
    UserRuleCreateRequest,
    confirm_l2,
    create_user_rule,
    resolve_batch,
)
from app.main import app
from app.models import FlowRecord, FlowVersionRecord, IntermediateFlowLinkRule, IntermediateFlowLinkRuleVersion, LciProcessVector, Model, ModelVersion, ReferenceProcess, UnitDefinition
from app.schemas import FlowPort, HybridGraph, IntermediateFlowLink
from app.services.graph_contract import analyze_handle_consistency, validate_graph_contract
from app.services.intermediate_flow_linking_service import (
    _validate_resolution_records,
    _resolution_with_catalog_units,
    backfill_ecoinvent_reference_flow_uuids,
    get_intermediate_flow_link_registry,
    list_provider_candidates,
    repair_legacy_intermediate_flow_links,
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
    registry = get_intermediate_flow_link_registry()
    public_row = next(item for item in registry.rules.values() if item.mapping_level == "L1")
    resolution = registry.resolve(public_row.tiangong_flow_uuid)
    assert resolution is not None
    source_unit = resolution.source_unit or "kg"
    target_unit = resolution.target_unit or source_unit
    dimension = "energy" if source_unit in {"MJ", "kWh"} else "mass"
    row = {
        **resolution.to_dict(),
        "source_name": f"source-{resolution.source_flow_uuid}",
        "target_name": f"target-{resolution.target_flow_uuid}",
        "source_flow_type": "Product flow",
        "target_flow_type": "Product flow",
        "source_unit": source_unit,
        "target_unit": target_unit,
        "source_unit_group": "Units of energy" if dimension == "energy" else "Units of mass",
        "target_unit_group": "energy" if dimension == "energy" else "mass",
    }
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
    registry = get_intermediate_flow_link_registry()
    public_row = next(item for item in registry.rules.values() if item.mapping_level == "L2")
    resolution = registry.resolve(public_row.tiangong_flow_uuid)
    assert resolution is not None
    source_unit = resolution.source_unit or "kg"
    target_unit = resolution.target_unit or source_unit
    dimension = "energy" if source_unit in {"MJ", "kWh"} else "mass"
    row = {
        **resolution.to_dict(),
        "source_name": f"source-{resolution.source_flow_uuid}",
        "target_name": f"target-{resolution.target_flow_uuid}",
        "source_flow_type": "Product flow",
        "target_flow_type": "Product flow",
        "source_unit": source_unit,
        "target_unit": target_unit,
        "source_unit_group": "Units of energy" if dimension == "energy" else "Units of mass",
        "target_unit_group": "energy" if dimension == "energy" else "mass",
    }
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
                        "flowSubtypeOverride": row.get("flow_subtype_override", False),
                        "sourceFlowType": row.get("source_flow_type"),
                        "targetFlowType": row.get("target_flow_type"),
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


def test_alias_edge_preserves_consumer_uuid_and_refreshes_metadata_only_package_hash(db):
    row = _seed_first_l1_pair(db)
    graph = _graph(row)
    validate_graph_contract(graph)
    validate_graph_intermediate_flow_links(db, graph)
    assert graph.nodes[1].inputs[0].flowUuid == row["source_flow_uuid"]
    assert graph.exchanges[0].flowUuid == row["target_flow_uuid"]
    assert graph.exchanges[0].consumer_flow_uuid == row["source_flow_uuid"]
    assert analyze_handle_consistency(graph)["ok"] is True

    stale = _graph(row, package_hash="stale")
    repairs = repair_legacy_intermediate_flow_links(db, stale)

    assert len(repairs) == 1
    assert stale.nodes[1].inputs[0].intermediate_flow_link is not None
    assert (
        stale.nodes[1].inputs[0].intermediate_flow_link.package_hash
        == get_intermediate_flow_link_registry().package_hash
    )
    validate_graph_intermediate_flow_links(db, stale)


def test_package_hash_refresh_does_not_accept_changed_mapping_evidence(db):
    row = _seed_first_l1_pair(db)
    stale = _graph(row, package_hash="stale")
    link = stale.nodes[1].inputs[0].intermediate_flow_link
    assert link is not None
    stale.nodes[1].inputs[0].intermediate_flow_link = link.model_copy(update={
        "target_flow_uuid": "different-target",
    })

    repair_legacy_intermediate_flow_links(db, stale)

    with pytest.raises(HTTPException) as exc:
        validate_graph_intermediate_flow_links(db, stale)
    assert exc.value.detail["code"] == "INVALID_INTERMEDIATE_FLOW_LINK"
    assert exc.value.detail["evidence"][0]["reason"] == "TARGET_FLOW_NOT_FOUND"


def test_legacy_builtin_link_inherits_explicit_port_flow_version(db):
    row = _seed_first_compatible_pair(db)
    db.add(FlowVersionRecord(
        source_namespace="tiangong_open_data",
        flow_uuid=row["source_flow_uuid"],
        source_version="01.01.001",
        version_label="TIDAS 01.01.001",
        flow_name=row["source_name"],
        flow_type=row["source_flow_type"],
        default_unit=row["source_unit"],
        unit_group=row["source_unit_group"],
    ))
    db.commit()
    graph = _graph(row)
    port = graph.nodes[1].inputs[0]
    port.flow_source_namespace = "tiangong_open_data"
    port.flow_version = "01.01.001"

    repairs = repair_legacy_intermediate_flow_links(db, graph)

    assert len(repairs) == 1
    assert port.intermediate_flow_link is not None
    assert port.intermediate_flow_link.source_flow_namespace == "tiangong_open_data"
    assert port.intermediate_flow_link.source_flow_version == "01.01.001"
    assert graph.exchanges[0].intermediate_flow_link_factor == row["amount_factor"]
    validate_graph_intermediate_flow_links(db, graph)


def test_legacy_l3_link_is_pinned_to_unique_matching_flow_version(db):
    source_uuid = "versioned-diesel"
    target_uuid = "ecoinvent-diesel"
    db.add_all([
        FlowRecord(
            flow_uuid=source_uuid,
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="MJ",
            unit_group="Units of energy",
            source="Tiangong",
        ),
        FlowRecord(
            flow_uuid=target_uuid,
            flow_name="Diesel market",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="ecoinvent_3.11",
        ),
        FlowVersionRecord(
            source_namespace="tiangong_open_source",
            flow_uuid=source_uuid,
            source_version="TG-1.0",
            version_label="TG 1.0",
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="MJ",
            unit_group="Units of energy",
        ),
        FlowVersionRecord(
            source_namespace="tiangong_open_data",
            flow_uuid=source_uuid,
            source_version="2.0.0",
            version_label="TIDAS 2.0.0",
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            flow_property_uuid="mass-property",
            unit_group_uuid="mass-group",
        ),
        IntermediateFlowLinkRule(
            id="legacy-diesel-rule",
            source_flow_uuid=source_uuid,
            target_flow_uuid=target_uuid,
            amount_factor=0.0234,
            source_unit="MJ",
            target_unit="kg",
            mapping_level="L3",
            mapping_reason="reviewed diesel proxy",
            rule_origin="user",
            status="active",
        ),
    ])
    db.commit()
    graph = HybridGraph.model_validate({
        "functionalUnit": "1 kg",
        "nodes": [
            {
                "id": "provider",
                "node_kind": "lci_dataset",
                "mode": "normalized",
                "process_uuid": "provider-process",
                "name": "Provider",
                "location": "GLO",
                "reference_product": "Diesel market",
                "inputs": [],
                "outputs": [{
                    "id": "provider-out",
                    "flowUuid": target_uuid,
                    "name": "Diesel market",
                    "unit": "kg",
                    "unitGroup": "Units of mass",
                    "amount": 1,
                    "type": "technosphere",
                    "direction": "output",
                }],
            },
            {
                "id": "consumer",
                "node_kind": "unit_process",
                "mode": "balanced",
                "process_uuid": "consumer-process",
                "name": "Consumer",
                "location": "CN",
                "reference_product": "result",
                "inputs": [{
                    "id": "diesel-in",
                    "flowUuid": source_uuid,
                    "name": "Diesel",
                    "unit": "kg",
                    "unitGroup": "Units of mass",
                    "amount": 2,
                    "type": "technosphere",
                    "direction": "input",
                    "unitGroupSwitch": {
                        "sourceUnit": "MJ",
                        "sourceUnitGroup": "Units of energy",
                        "targetUnitGroup": "Units of mass",
                        "factor": 0.0234,
                    },
                    "intermediateFlowLink": {
                        "sourceFlowUuid": source_uuid,
                        "targetFlowUuid": target_uuid,
                        "amountFactor": 0.0234,
                        "sourceUnit": "MJ",
                        "targetUnit": "kg",
                        "sourceUnitGroup": "Units of energy",
                        "targetUnitGroup": "Units of mass",
                        "mappingLevel": "L3",
                        "mappingReason": "reviewed diesel proxy",
                        "ruleId": "legacy-diesel-rule",
                        "ruleOrigin": "user",
                        "status": "user_confirmed",
                    },
                }],
                "outputs": [{
                    "id": "result-out",
                    "flowUuid": "result-flow",
                    "name": "result",
                    "unit": "kg",
                    "amount": 1,
                    "type": "technosphere",
                    "direction": "output",
                }],
            },
        ],
        "exchanges": [{
            "id": "diesel-edge",
            "fromNode": "provider",
            "toNode": "consumer",
            "sourceHandle": "out:provider-out",
            "targetHandle": "in:diesel-in",
            "flowUuid": target_uuid,
            "consumerFlowUuid": source_uuid,
            "flowName": "Diesel market",
            "quantityMode": "dual",
            "amount": 0.0468,
            "providerAmount": 1,
            "consumerAmount": 0.0468,
            "unit": "kg",
            "providerUnit": "kg",
            "consumerUnit": "MJ",
            "type": "technosphere",
            "intermediateFlowLinkRuleId": "legacy-diesel-rule",
            "intermediateFlowLinkFactor": 0.0234,
        }],
    })

    repairs = repair_legacy_intermediate_flow_links(db, graph)

    assert len(repairs) == 1
    port = graph.nodes[1].inputs[0]
    assert port.flow_source_namespace == "tiangong_open_data"
    assert port.flow_version == "2.0.0"
    assert port.unitGroupSwitch is None
    assert port.intermediate_flow_link is not None
    assert port.intermediate_flow_link.amount_factor == 1
    assert port.intermediate_flow_link.source_unit == "kg"
    assert db.get(IntermediateFlowLinkRuleVersion, port.intermediate_flow_link.rule_id) is not None
    edge = graph.exchanges[0]
    assert edge.amount == 2
    assert edge.consumerAmount == 2
    assert edge.consumer_unit == "kg"
    assert edge.intermediate_flow_link_factor == 1
    validate_graph_intermediate_flow_links(db, graph)


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


def test_builtin_cross_group_mapping_is_reviewable_with_manual_factor(db):
    row = _seed_first_compatible_pair(db)
    source = db.get(FlowRecord, row["source_flow_uuid"])
    source.unit_group = (
        "Units of mass"
        if "energy" in row["target_unit_group"].casefold()
        else "Units of energy"
    )
    db.commit()

    result = resolve_batch(ResolveBatchRequest(items=[ResolvePortRequest(
        port_id="cross-group-input",
        flow_uuid=row["source_flow_uuid"],
        unit=row["source_unit"],
        unit_group=source.unit_group,
    )]), db)

    item = result["items"][0]
    assert item["status"] == "L2"
    assert item["resolution"]["requires_manual_factor"] is True
    assert "CROSS_GROUP_FACTOR_REQUIRED" in item["resolution"]["warnings"]
    assert item["resolution"]["target_flow_uuid"] == row["target_flow_uuid"]


def test_ecoinvent_reference_product_wins_over_tidas_catalog_uuid_collision(db):
    row = _seed_first_compatible_pair(db)
    target = db.get(FlowRecord, row["target_flow_uuid"])
    target.source = "tidas_bundle_import"
    target.flow_name = "Conflicting Tiangong Flow"
    target.default_unit = "m3"
    target.unit_group = "Units of volume"
    db.add(ReferenceProcess(
        process_uuid="ecoinvent-collision-provider",
        process_name="Authoritative ecoinvent provider",
        process_type="lci_dataset",
        reference_flow_uuid=row["target_flow_uuid"],
        process_json={
            "reference_product_id": row["target_flow_uuid"],
            "reference_product": row["target_name"],
            "reference_product_unit": row["target_unit"],
        },
    ))
    db.commit()

    result = resolve_batch(ResolveBatchRequest(items=[ResolvePortRequest(
        port_id="collision-input",
        flow_uuid=row["source_flow_uuid"],
        unit=row["source_unit"],
        unit_group=row["source_unit_group"],
    )]), db)

    item = result["items"][0]
    assert item["status"] == "L2"
    assert item["resolution"]["target_flow_name"] == row["target_name"]
    assert item["resolution"]["target_unit"] == row["target_unit"]


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
        explicit = dict(resolved.json()["items"][0]["resolution"])
        explicit.update({"status": "auto", "package_hash": "legacy-manifest-hash"})
        metadata_drift = client.post("/api/intermediate-flow-links/resolve-batch", json={
            "items": [{
                "port_id": "input-1",
                "flow_uuid": row["source_flow_uuid"],
                "direction": "input",
                "exchange_type": "technosphere",
                "unit": row["source_unit"],
                "unit_group": row["source_unit_group"],
                "intermediate_flow_link": explicit,
            }],
        })
        assert metadata_drift.status_code == 200
        assert metadata_drift.json()["items"][0]["status"] == "explicit"

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

        missing = client.get(
            "/api/intermediate-flow-links/providers",
            params={"target_flow_uuid": "not-an-ecoinvent-target"},
        )
        assert missing.status_code == 200
        assert missing.json() == {
            "target_flow_uuid": "not-an-ecoinvent-target",
            "target_flow_name": "",
            "target_unit": "",
            "providers": [],
            "total": 0,
            "auto_selected": False,
        }

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


def test_versioned_tidas_flow_does_not_reuse_legacy_cross_group_rule(db):
    source_uuid = "versioned-diesel"
    target_uuid = "eco-diesel"
    db.add_all([
        FlowRecord(
            flow_uuid=source_uuid,
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tidas_bundle_import",
        ),
        FlowRecord(
            flow_uuid=target_uuid,
            flow_name="Diesel market",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="mass",
            source="ecoinvent_3.11",
        ),
        FlowVersionRecord(
            source_namespace="tiangong_open_source",
            flow_uuid=source_uuid,
            source_version="TG-1.0",
            version_label="TG 1.0",
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="MJ",
            unit_group="Units of energy",
        ),
        FlowVersionRecord(
            source_namespace="tiangong_open_data",
            flow_uuid=source_uuid,
            source_version="01.01.003",
            version_label="TIDAS 01.01.003",
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
        ),
        IntermediateFlowLinkRule(
            id="legacy-diesel-rule",
            source_flow_uuid=source_uuid,
            target_flow_uuid=target_uuid,
            amount_factor=0.0234,
            source_unit="MJ",
            target_unit="kg",
            mapping_level="L3",
            mapping_reason="legacy heating value",
            rule_origin="user",
            status="active",
        ),
    ])
    db.commit()

    resolution, issue = resolve_intermediate_flow(
        db,
        source_uuid,
        source_namespace="tiangong_open_data",
        source_version="01.01.003",
        source_unit="kg",
        source_unit_group="Units of mass",
    )

    assert issue is None
    assert resolution is None

    created = create_user_rule(UserRuleCreateRequest(
        source_flow_uuid=source_uuid,
        target_flow_uuid=target_uuid,
        source_flow_namespace="tiangong_open_data",
        source_flow_version="01.01.003",
        source_unit="kg",
        source_unit_group="Units of mass",
    ), db)
    assert created["amount_factor"] == 1.0
    assert created["source_flow_version"] == "01.01.003"

    resolved, issue = resolve_intermediate_flow(
        db,
        source_uuid,
        source_namespace="tiangong_open_data",
        source_version="01.01.003",
        source_unit="kg",
        source_unit_group="Units of mass",
    )
    assert issue is None
    assert resolved is not None
    assert resolved.rule_id == created["id"]
    assert resolved.amount_factor == 1.0


def test_unversioned_mass_port_rejects_legacy_energy_link(db):
    source_uuid = "legacy-diesel-port"
    target_uuid = "eco-diesel-port"
    db.add_all([
        FlowRecord(
            flow_uuid=source_uuid,
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tidas_bundle_import",
        ),
        FlowRecord(
            flow_uuid=target_uuid,
            flow_name="Diesel market",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="mass",
            source="ecoinvent_3.11",
        ),
        FlowVersionRecord(
            source_namespace="tiangong_open_source",
            flow_uuid=source_uuid,
            source_version="TG-1.0",
            version_label="TG 1.0",
            flow_name="Diesel",
            flow_type="Product flow",
            default_unit="MJ",
            unit_group="Units of energy",
        ),
        IntermediateFlowLinkRule(
            id="legacy-port-rule",
            source_flow_uuid=source_uuid,
            target_flow_uuid=target_uuid,
            amount_factor=0.0234,
            source_unit="MJ",
            target_unit="kg",
            mapping_level="L3",
            mapping_reason="legacy heating value",
            rule_origin="user",
            status="active",
        ),
    ])
    db.commit()
    link = IntermediateFlowLink.model_validate({
        "sourceFlowUuid": source_uuid,
        "targetFlowUuid": target_uuid,
        "amountFactor": 0.0234,
        "sourceUnit": "MJ",
        "targetUnit": "kg",
        "mappingLevel": "L3",
        "mappingReason": "legacy heating value",
        "ruleId": "legacy-port-rule",
        "ruleOrigin": "user",
        "status": "user_confirmed",
    })
    port = FlowPort(
        id="diesel-input",
        flowUuid=source_uuid,
        name="Diesel",
        unit="kg",
        unitGroup="Units of mass",
        amount=1,
        type="technosphere",
        direction="input",
        intermediateFlowLink=link,
    )

    assert validate_intermediate_flow_link(db, source_uuid, link, port=port) == "SOURCE_UNIT_DRIFT"
