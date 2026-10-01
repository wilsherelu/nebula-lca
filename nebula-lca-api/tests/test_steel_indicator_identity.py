"""Lock the steel foreground biosphere indicator identity against production data files.

Verifies that the official steel seed's elementary flows satisfy the certified-bijective-core
and method-package preconditions for native-background-climate merge_status='merged'.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[1]
METHOD_PACKAGE_PATH = API_ROOT / "data" / "methods" / "ecoinvent_ef31_climate_total_311.v1.json"
CERTIFIED_CORE_PATH = API_ROOT / "data" / "flow_mappings" / "ghg_ef31_v1.json"

STEEL_SEED_BIOSPHERE_UUID = "08a91e70-3ddc-11dd-923d-0050c2490048"
STEEL_SEED_BIOSPHERE_VERSION = "03.00.004"
STEEL_SEED_BIOSPHERE_UNIT = "kg"
STEEL_SEED_BIOSPHERE_DIRECTION = "output"
STEEL_SEED_BIOSPHERE_COMPARTMENT = "Emissions to air, unspecified"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _certified_entry(flow_uuid: str) -> dict | None:
    core = _load_json(CERTIFIED_CORE_PATH)
    for m in core.get("mappings") or []:
        if m.get("ef_flow_uuid") == flow_uuid:
            return m
    return None


def test_method_package_identity_is_stable():
    pkg = _load_json(METHOD_PACKAGE_PATH)
    assert pkg["package_id"] == "ecoinvent-ef31-climate-total-3.11"
    assert pkg["package_version"] == "1.0.0"
    assert pkg["method"] == "EF v3.1"
    assert pkg["category"] == "climate change"
    assert pkg["indicator"] == "global warming potential (GWP100)"
    assert pkg["indicator_unit"] == "kg CO2-Eq"
    assert pkg["database_release"] == "ecoinvent_3.11"
    assert pkg["native_flow_namespace"] == "ecoinvent"
    assert pkg["foreground_flow_namespace"] == "tiangong_open_data"
    assert pkg["no_cf_policy"] == "uncharacterized_no_numeric_contribution"
    assert pkg["merge_policy"] == "require_certified_identity_and_equal_coefficient_for_each_contributing_foreground_flow"


def test_steel_seed_biosphere_flow_exists_in_certified_core():
    entry = _certified_entry(STEEL_SEED_BIOSPHERE_UUID)
    assert entry is not None, (
        f"Steel seed foreground flow {STEEL_SEED_BIOSPHERE_UUID} missing from certified bijective core"
    )
    assert entry["direction"] == STEEL_SEED_BIOSPHERE_DIRECTION
    assert entry["unit"] == STEEL_SEED_BIOSPHERE_UNIT


def test_steel_seed_biosphere_compartment_matches_characterizer():
    entry = _certified_entry(STEEL_SEED_BIOSPHERE_UUID)
    assert entry is not None
    catalog_compartment = str(entry["ef_context"].get("catalog_compartment") or "")
    last_segment = catalog_compartment.split(";")[-1]
    assert last_segment == STEEL_SEED_BIOSPHERE_COMPARTMENT, (
        f"compartment drift: expected {STEEL_SEED_BIOSPHERE_COMPARTMENT!r} got {last_segment!r}"
    )
    assert entry["ef_context"].get("catalog_subcompartment") in (None, ""), (
        "Steel CO2 must not acquire a non-empty catalog_subcompartment"
    )


def test_steel_seed_biosphere_cf_equivalence_for_merge():
    entry = _certified_entry(STEEL_SEED_BIOSPHERE_UUID)
    assert entry is not None
    cf = entry["cf_vector"]["climate_total"]
    factor = float(entry["eco_to_ef_factor"])
    assert factor == 1.0, "eco_to_ef_factor must remain 1.0 for mass-basis CO2"
    assert cf == 1.0, "GWP100 for CO2 fossil must be 1.0 (definition of the metric)"
    assert entry["ecoinvent_flow_name"] == "Carbon dioxide, fossil"
    assert entry["ef_flow_name"] == "carbon dioxide (fossil)"


def test_certified_core_identity_of_steel_flow_is_unique():
    core = _load_json(CERTIFIED_CORE_PATH)
    matches = [m for m in (core.get("mappings") or []) if m["ef_flow_uuid"] == STEEL_SEED_BIOSPHERE_UUID]
    assert len(matches) == 1, f"Expected exactly 1 certified entry for steel CO2, got {len(matches)}"


def test_certified_core_package_metadata_is_valid():
    core = _load_json(CERTIFIED_CORE_PATH)
    assert core["package_id"] == "ghg-ef31-certified-bijective-core"
    assert core["method_scope"]["method_id"] == "EF climate change"
    assert core["method_scope"]["method_version"] == "3.1"
    assert core["review_status"] == "approved"
    assert core["mapping_count"] == len(core["mappings"])


def test_method_package_certified_core_sha_matches_committed_file():
    pkg = _load_json(METHOD_PACKAGE_PATH)
    expected_sha = pkg["certified_core_sha256"]
    import hashlib
    actual_sha = hashlib.sha256(CERTIFIED_CORE_PATH.read_bytes()).hexdigest()
    assert actual_sha == expected_sha, (
        f"Certified core file hash {actual_sha} does not match method package pin {expected_sha}"
    )


def test_method_package_self_hash_matches_stable_identity():
    import hashlib
    sha = hashlib.sha256(METHOD_PACKAGE_PATH.read_bytes()).hexdigest()
    assert sha == "b3cb07ccb69f9e6efa6669c6dcc68912de82ae54ea6f1fa5974dbce1d1797633", (
        f"Method package file hash drifted: {sha}"
    )
