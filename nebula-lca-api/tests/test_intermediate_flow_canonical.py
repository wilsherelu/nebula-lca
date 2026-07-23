from __future__ import annotations

import hashlib
import json

import pytest
from fastapi import HTTPException

from app.api.intermediate_flow_links import get_canonical_tidas_flow
from app.services.intermediate_flow_canonical_service import (
    DEFAULT_CANONICAL_PACKAGE_PATH,
    IntermediateFlowCanonicalRegistry,
    get_intermediate_flow_canonical_registry,
)
from app.services.intermediate_flow_linking_service import DEFAULT_PACKAGE_PATH


def test_canonical_package_is_explicit_and_bound_to_forward_release():
    registry = get_intermediate_flow_canonical_registry()
    payload = json.loads(DEFAULT_CANONICAL_PACKAGE_PATH.read_text(encoding="utf-8"))

    assert registry.package_id == "intermediate_ecoinvent_to_tidas_canonical_v1"
    assert registry.package_version == "1.0.5"
    assert len(registry.rules) == 314
    assert registry.unresolved_source_count == 160
    assert payload["source_package_version"] == "2.8.0"
    assert payload["source_package_sha256"] == hashlib.sha256(DEFAULT_PACKAGE_PATH.read_bytes()).hexdigest()


def test_canonical_package_excludes_forward_only_flow_subtype_overrides():
    forward = json.loads(DEFAULT_PACKAGE_PATH.read_text(encoding="utf-8"))
    canonical = json.loads(DEFAULT_CANONICAL_PACKAGE_PATH.read_text(encoding="utf-8"))
    override_targets = {
        mapping["target_flow_uuid"]
        for mapping in forward["mappings"]
        if mapping.get("flow_subtype_override")
    }
    canonical_sources = {
        mapping["source_flow_uuid"]
        for mapping in canonical["mappings"]
    }

    assert len(override_targets) == 1
    assert override_targets.isdisjoint(canonical_sources)


def test_canonical_resolution_prefers_reviewed_active_tiangong_alias():
    resolution = get_intermediate_flow_canonical_registry().resolve(
        "ec23da1b-e5fa-4f01-bd2c-3233fece2175"
    )

    assert resolution is not None
    assert resolution.target_flow_uuid == "da3e6005-a9a3-4048-bff9-8da8801d6802"
    assert resolution.amount_factor == 1
    assert resolution.selection_mode == "active_alias_override"
    assert resolution.origin_mapping_level == "L1"


def test_canonical_endpoint_reports_unmapped_flow():
    with pytest.raises(HTTPException) as exc_info:
        get_canonical_tidas_flow("00000000-0000-4000-8000-000000000000")

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["code"] == "INTERMEDIATE_FLOW_CANONICAL_MAPPING_MISSING"


def test_canonical_loader_rejects_duplicate_target_choice(tmp_path):
    payload = json.loads(DEFAULT_CANONICAL_PACKAGE_PATH.read_text(encoding="utf-8"))
    duplicate = dict(payload["mappings"][1])
    duplicate["source_flow_uuid"] = "00000000-0000-4000-8000-000000000001"
    payload["mappings"].append(duplicate)
    path = tmp_path / "duplicate.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate UUID choices"):
        IntermediateFlowCanonicalRegistry(path)
