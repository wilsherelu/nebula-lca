from __future__ import annotations

from app.services.public_flow_mapping_service import (
    PublicFlowMappingRegistry,
    apply_elementary_mappings_to_snapshot,
)


def test_public_mapping_package_is_complete_and_verified():
    registry = PublicFlowMappingRegistry()

    assert registry.package_version == "1.0.0"
    assert len(registry.intermediate) == 1379
    assert len(registry.elementary) == 9764
    assert registry.manifest["intermediate"]["mapping_levels"] == {"L1": 147, "L2": 1232}
    assert registry.manifest["elementary"]["mapping_levels"] == {"L1": 6354, "L2": 3410}


def test_public_intermediate_mapping_preserves_energy_conversion():
    registry = PublicFlowMappingRegistry()
    resolution = registry.resolve_intermediate("3d76981f-964a-4865-b588-0e067a2a1163")

    assert resolution is not None
    assert resolution.ecoinvent_flow_uuid == "759b89bd-3aa6-42ad-b767-5bb9ef5d331d"
    assert resolution.mapping_level == "L2"
    assert resolution.source_unit == "MJ"
    assert resolution.target_unit == "kWh"
    assert resolution.amount_factor == 1 / 3.6


def test_elementary_mapping_rewrites_solver_snapshot_before_ef31():
    snapshot = {
        "flows": [{
            "flow_uuid": "0009a787-4fe4-48ad-ad29-cc9735fa5a3d",
            "flow_name": "source elementary flow",
            "flow_type": "Elementary flow",
        }],
        "exchanges": [{
            "exchange_id": "exchange-1",
            "flow_uuid": "0009a787-4fe4-48ad-ad29-cc9735fa5a3d",
            "amount": 2.5,
        }],
    }

    trace = apply_elementary_mappings_to_snapshot(snapshot)

    assert snapshot["flows"][0]["flow_uuid"] == "cbbbf558-24bb-5fc2-b289-b1a630a86ab2"
    assert snapshot["exchanges"][0]["flow_uuid"] == "cbbbf558-24bb-5fc2-b289-b1a630a86ab2"
    assert snapshot["exchanges"][0]["amount"] == 2.5
    assert trace == [{
        "source_flow_uuid": "0009a787-4fe4-48ad-ad29-cc9735fa5a3d",
        "target_flow_uuid": "cbbbf558-24bb-5fc2-b289-b1a630a86ab2",
        "amount_factor": 1.0,
        "mapping_level": "L1",
        "package_id": "nebula-flow-mapping",
        "package_version": "1.0.0",
    }]
