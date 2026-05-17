from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.main import app
from app.tidas_export import ExportReport, _flow_property
from app.tidas_reference import (
    get_tidas_allowed_unit_groups,
    get_tidas_flow_property_reference,
    load_tidas_reference_seed,
    normalize_tidas_unit_group,
)


def _write_seed(path):
    path.write_text(
        json.dumps(
            {
                "source_package_version": "tiangong-1.0-test",
                "flow_properties": [
                    {
                        "flow_property_uuid": "11111111-2222-3333-4444-555555555555",
                        "name_en": "Mass",
                        "name_zh": "质量",
                        "ref_uri": "../flowproperties/11111111-2222-3333-4444-555555555555.xml",
                        "version": "01.00.000",
                    }
                ],
                "unit_group_mappings": [
                    {
                        "source_unit_group": "Units of mass",
                        "tidas_unit_group": "Units of mass",
                        "aliases": ["Units of mass_time"],
                        "flow_property_uuid": "11111111-2222-3333-4444-555555555555",
                        "mapping_status": "allowed",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_tidas_reference_seed_maps_unit_group(monkeypatch, tmp_path):
    seed_path = tmp_path / "tidas_reference_seed.json"
    _write_seed(seed_path)
    monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_SEED", str(seed_path))
    load_tidas_reference_seed.cache_clear()

    reference = get_tidas_flow_property_reference("Units of mass")

    assert reference is not None
    assert reference["@refObjectId"] == "11111111-2222-3333-4444-555555555555"
    assert reference["@uri"].endswith("11111111-2222-3333-4444-555555555555.xml")


def test_tidas_reference_seed_maps_unit_group_alias(monkeypatch, tmp_path):
    seed_path = tmp_path / "tidas_reference_seed.json"
    _write_seed(seed_path)
    monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_SEED", str(seed_path))
    load_tidas_reference_seed.cache_clear()

    reference = get_tidas_flow_property_reference("Units of mass/time")

    assert reference is not None
    assert reference["@refObjectId"] == "11111111-2222-3333-4444-555555555555"


def test_tidas_allowed_unit_groups_include_aliases(monkeypatch, tmp_path):
    seed_path = tmp_path / "tidas_reference_seed.json"
    _write_seed(seed_path)
    monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_SEED", str(seed_path))
    load_tidas_reference_seed.cache_clear()

    allowed = get_tidas_allowed_unit_groups()

    assert normalize_tidas_unit_group("Units of mass") in allowed
    assert normalize_tidas_unit_group("Units of mass_time") in allowed


def test_tidas_allowed_unit_groups_exclude_missing_mapping_when_mappings_exist(monkeypatch, tmp_path):
    seed_path = tmp_path / "tidas_reference_seed.json"
    seed_path.write_text(
        json.dumps(
            {
                "unit_groups": [
                    {"name": "Units of mass"},
                    {"name": "Unit of kg*km"},
                ],
                "flow_properties": [{"flow_property_uuid": "fp-mass"}],
                "unit_group_mappings": [
                    {
                        "source_unit_group": "Units of mass",
                        "tidas_unit_group": "Units of mass",
                        "flow_property_uuid": "fp-mass",
                        "mapping_status": "allowed",
                    }
                ],
                "missing_flow_property_mappings": [
                    {"source_unit_group": "Unit of kg*km"},
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_SEED", str(seed_path))
    load_tidas_reference_seed.cache_clear()

    allowed = get_tidas_allowed_unit_groups()

    assert normalize_tidas_unit_group("Units of mass") in allowed
    assert normalize_tidas_unit_group("Unit of kg*km") not in allowed


def test_flow_property_uses_seed_without_placeholder_warning(monkeypatch, tmp_path):
    seed_path = tmp_path / "tidas_reference_seed.json"
    _write_seed(seed_path)
    monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_SEED", str(seed_path))
    load_tidas_reference_seed.cache_clear()
    report = ExportReport()

    result = _flow_property("flow-1", "Units of mass", report)
    reference = result["flowProperty"]["referenceToFlowPropertyDataSet"]

    assert reference["@refObjectId"] == "11111111-2222-3333-4444-555555555555"
    assert not [w for w in report.warnings if w.category == "tidas_placeholder"]


def test_flow_property_falls_back_when_seed_mapping_missing(monkeypatch, tmp_path):
    seed_path = tmp_path / "empty_seed.json"
    seed_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_SEED", str(seed_path))
    load_tidas_reference_seed.cache_clear()
    report = ExportReport()

    result = _flow_property("flow-2", "Units of energy", report)
    reference = result["flowProperty"]["referenceToFlowPropertyDataSet"]

    assert reference["@refObjectId"] == "93a60a56-a3c8-11da-a746-0800200b9a66"
    assert [w for w in report.warnings if w.category == "tidas_placeholder"]


def test_default_tidas_seed_maps_core_unit_groups(monkeypatch):
    monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_SEED", raising=False)
    load_tidas_reference_seed.cache_clear()

    try:
        allowed = set(get_tidas_allowed_unit_groups())
        mass = get_tidas_flow_property_reference("Units of mass")
        energy = get_tidas_flow_property_reference("Units of energy")
        volume = get_tidas_flow_property_reference("Units of volume")

        assert mass is not None
        assert mass["@refObjectId"] == "93a60a56-a3c8-11da-a746-0800200b9a66"
        assert mass["@uri"] == "../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66.xml"
        assert energy is not None
        assert energy["@refObjectId"] == "f6811440-ee37-11de-8a39-0800200c9a66"
        assert volume is not None
        assert volume["@refObjectId"] == "93a60a56-a3c8-22da-a746-0800200c9a66"
        assert normalize_tidas_unit_group("Units of mass_time") in allowed
        kg_km = get_tidas_flow_property_reference("Unit of kg*km")
        sej = get_tidas_flow_property_reference("sej")
        assert kg_km is not None
        assert kg_km["@refObjectId"] == "751ca877-3326-59a1-82b8-7ef88d9c2dd4"
        assert sej is not None
        assert sej["@refObjectId"] == "bf1b9b1d-ff62-5399-a09f-5d8eba6d5cd7"
        assert normalize_tidas_unit_group("Unit of kg*km") in allowed
        assert normalize_tidas_unit_group("sej") in allowed
    finally:
        load_tidas_reference_seed.cache_clear()


def test_tidas_policy_reference_endpoint_exposes_allowed_unit_groups(monkeypatch):
    monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_SEED", raising=False)
    load_tidas_reference_seed.cache_clear()

    try:
        resp = TestClient(app).get("/api/reference/tidas-policy")

        assert resp.status_code == 200
        payload = resp.json()
        assert payload["source_package_version"]
        assert normalize_tidas_unit_group("Units of mass") in payload["allowed_unit_groups"]
        assert normalize_tidas_unit_group("Unit of kg*km") in payload["allowed_unit_groups"]
        assert normalize_tidas_unit_group("sej") in payload["allowed_unit_groups"]
    finally:
        load_tidas_reference_seed.cache_clear()
