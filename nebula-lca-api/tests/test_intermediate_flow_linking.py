from __future__ import annotations

import json

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.database import SessionLocal, engine as app_engine
from app.main import app
from app.models import FlowRecord, LciProcessVector, Model, ModelVersion, ReferenceProcess
from app.schemas import HybridGraph
from app.services.graph_contract import analyze_handle_consistency, validate_graph_contract
from app.services.intermediate_flow_linking_service import (
    DEFAULT_PACKAGE_PATH,
    backfill_ecoinvent_reference_flow_uuids,
    get_intermediate_flow_link_registry,
    list_provider_candidates,
    resolve_intermediate_flow,
    validate_graph_intermediate_flow_links,
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
    row = package["rules"][0]
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


def _graph(row, *, package_hash: str | None = None) -> HybridGraph:
    registry = get_intermediate_flow_link_registry()
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
                        "mappingLevel": "L1",
                        "mappingReason": "approved_one_way_reference_product_link",
                        "ruleId": row["rule_id"],
                        "ruleOrigin": "builtin",
                        "status": "auto",
                        "packageId": registry.package_id,
                        "packageVersion": registry.package_version,
                        "packageHash": package_hash or registry.package_hash,
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
            "mapping_reason": "User-selected calculation proxy",
        })
        assert created.status_code == 201
        assert created.json()["mapping_level"] == "L3"
        assert created.json()["amount_factor"] == 1.0
        deactivated = client.delete(f"/api/intermediate-flow-links/user-rules/{created.json()['id']}")
        assert deactivated.status_code == 200
        assert deactivated.json()["status"] == "inactive"
