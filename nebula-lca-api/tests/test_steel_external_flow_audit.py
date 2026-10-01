from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from app.services.public_flow_mapping_service import get_public_flow_mapping_registry


AUDIT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "provider_profiles"
    / "steel_external_flows.v1.json"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@pytest.fixture(scope="module")
def audit() -> dict:
    return json.loads(AUDIT_PATH.read_text(encoding="utf-8"))


def test_steel_audit_has_one_ready_replacement_and_explicit_blockers(audit):
    assert audit["schema_version"] == "provider.steel-external-flow-audit.v1"
    assert [item["role"] for item in audit["ready_replacements"]] == ["grid_electricity"]

    unresolved = audit["unresolved_roles"]
    assert len(unresolved) == 8
    assert len({item["role"] for item in unresolved}) == len(unresolved)
    assert all(item["blocker"].strip() for item in unresolved)


def test_ready_electricity_identity_matches_active_mapping_package(audit):
    registry = get_public_flow_mapping_registry()
    package = audit["mapping_package"]
    assert package["package_id"] == registry.package_id
    assert package["package_version"] == registry.package_version
    assert package["package_hash"] == registry.package_hash

    ready = audit["ready_replacements"][0]
    source = ready["source_flow"]
    mapping = ready["mapping"]
    active = registry.resolve_intermediate(source["flow_uuid"])
    assert active is not None
    assert source["source_namespace"] == "tiangong_open_data"
    assert source["version"] == "01.01.003"
    assert source["unit"] == "MJ"
    assert SHA256_PATTERN.fullmatch(source["content_hash"])
    assert SHA256_PATTERN.fullmatch(source["exact_snapshot_hash"])
    assert mapping["mapping_level"] == active.mapping_level
    assert mapping["mapping_status_required"] == "user_confirmed"
    assert mapping["target_flow_uuid"] == active.ecoinvent_flow_uuid
    assert mapping["amount_factor"] == pytest.approx(active.amount_factor)
    assert mapping["source_unit"] == active.source_unit
    assert mapping["target_unit"] == active.target_unit
    assert mapping["mapping_rule_id"].endswith(source["flow_uuid"])


def test_audit_contains_no_commercial_provider_or_inventory(audit):
    def keys(value):
        if isinstance(value, dict):
            for key, child in value.items():
                yield key
                yield from keys(child)
        elif isinstance(value, list):
            for child in value:
                yield from keys(child)

    assert not {"process_uuid", "vector_checksum", "scaled_exchanges", "inventory"}.intersection(keys(audit))
