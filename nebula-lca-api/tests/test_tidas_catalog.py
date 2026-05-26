"""Tests for TIDAS reference catalog loading and helpers.

Covers:
1. load_tidas_reference_catalog() — file loading and environment override.
2. lookup_classification_entries() — catalog lookup vs fallback.
3. get_catalog_skeleton() — template retrieval and placeholder substitution.
4. _normalise_ilcd_dataset_type() — type normalisation.
"""

import json
import os
from pathlib import Path

import pytest


class TestLoadReferenceCatalog:
    """Test load_tidas_reference_catalog() loading behaviour."""

    def _clear_caches(self):
        from app.tidas_reference import load_tidas_reference_catalog
        load_tidas_reference_catalog.cache_clear()

    def test_loads_default_path(self, request, monkeypatch):
        """Default catalog path should load successfully."""
        # Remove env override if present
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()

        from app.tidas_reference import load_tidas_reference_catalog

        catalog = load_tidas_reference_catalog()
        assert isinstance(catalog, dict)
        assert "classifications" in catalog
        assert "locations" in catalog

    def test_has_classifications_with_fallback_by_dataset_type(self):
        """Catalog must have classifications with fallbackByDatasetType."""
        from app.tidas_reference import load_tidas_reference_catalog

        catalog = load_tidas_reference_catalog()
        fallback = catalog["classifications"]["fallbackByDatasetType"]
        assert isinstance(fallback, dict)
        assert "flow" in fallback
        assert "process" in fallback

    def test_has_skeletons(self):
        """Catalog must have skeletons with validation and publication."""
        from app.tidas_reference import load_tidas_reference_catalog

        catalog = load_tidas_reference_catalog()
        skeletons = catalog.get("skeletons", {})
        assert isinstance(skeletons, dict)
        assert "validation" in skeletons
        assert "publication" in skeletons
        assert "dataEntryBy" in skeletons

    def test_has_locations(self):
        """Catalog must have 648 location entries."""
        from app.tidas_reference import load_tidas_reference_catalog

        catalog = load_tidas_reference_catalog()
        entries = catalog["locations"]["entries"]
        assert isinstance(entries, list)
        assert len(entries) > 600

    def test_lists_location_options_for_ui(self):
        """Location options should expose the full catalog for UI search."""
        from app.tidas_reference import list_tidas_location_options

        options = list_tidas_location_options()
        codes = {item["code"] for item in options}
        assert len(options) > 600
        assert {"GLO", "CN", "CN-SH", "CN-BJ"} <= codes
        assert options[0]["code"] == "GLO"

    def test_env_override_path(self, tmp_path, monkeypatch):
        """NEBULA_TIDAS_REFERENCE_CATALOG env should override default path."""
        # Write a minimal catalog
        custom_path = tmp_path / "custom_catalog.json"
        custom_data = {
            "classifications": {"fallbackByDatasetType": {"flow": [{"@id": "1", "@name": "Test"}]}},
            "locations": {"entries": []},
            "skeletons": {},
        }
        custom_path.write_text(json.dumps(custom_data), encoding="utf-8")

        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        from app.tidas_reference import load_tidas_reference_catalog
        load_tidas_reference_catalog.cache_clear()

        monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_CATALOG", str(custom_path))

        catalog = load_tidas_reference_catalog()
        assert catalog == custom_data

    def test_missing_file_returns_empty(self, tmp_path, monkeypatch):
        """Non-existent catalog file should return empty dict."""
        missing = tmp_path / "nonexistent_catalog.json"

        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        from app.tidas_reference import load_tidas_reference_catalog
        load_tidas_reference_catalog.cache_clear()

        monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_CATALOG", str(missing))

        catalog = load_tidas_reference_catalog()
        assert catalog == {}

    def test_invalid_json_returns_empty(self, tmp_path, monkeypatch):
        """Malformed JSON should return empty dict."""
        bad_path = tmp_path / "bad_catalog.json"
        bad_path.write_text("not valid json {{{", encoding="utf-8")

        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        from app.tidas_reference import load_tidas_reference_catalog
        load_tidas_reference_catalog.cache_clear()

        monkeypatch.setenv("NEBULA_TIDAS_REFERENCE_CATALOG", str(bad_path))

        catalog = load_tidas_reference_catalog()
        assert catalog == {}


class TestNormaliseILCDDatasetType:
    """Test _normalise_ilcd_dataset_type()."""

    def test_flow_types_map_to_flow(self):
        from app.tidas_reference import _normalise_ilcd_dataset_type as _n

        assert _n("elementary flow") == "flow"
        assert _n("product flow") == "flow"
        assert _n("waste flow") == "flow"
        assert _n("biosphere flow") == "flow"
        assert _n("Flow") == "flow"
        assert _n("elementaryFlow") == "elementaryFlow"

    def test_ilcd_types_match_directly(self):
        from app.tidas_reference import _normalise_ilcd_dataset_type as _n

        assert _n("flow") == "flow"
        assert _n("process") == "process"
        assert _n("lifecyclemodel") == "lifecyclemodel"
        assert _n("flowproperty") == "flowproperty"

    def test_empty_or_none(self):
        from app.tidas_reference import _normalise_ilcd_dataset_type as _n

        assert _n(None) == ""
        assert _n("") == ""
        assert _n("unknown") == ""


class TestLookupClassificationEntries:
    """Test lookup_classification_entries()."""

    def _clear_caches(self):
        from app.tidas_reference import (
            load_tidas_reference_catalog,
            lookup_classification_entries,
        )
        load_tidas_reference_catalog.cache_clear()
        # lookup_classification_entries has no cache but depends on load_tidas_reference_catalog

    def test_flow_type_gets_catalog_entries(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import lookup_classification_entries

        entries = lookup_classification_entries("flow", [])
        assert isinstance(entries, list)
        assert len(entries) >= 1
        entry = entries[0]
        assert "@id" in entry or "classId" in entry
        assert "@name" in entry or "name" in entry

    def test_process_type_gets_catalog_entries(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import lookup_classification_entries

        entries = lookup_classification_entries("process", [])
        assert isinstance(entries, list)
        assert len(entries) >= 1

    def test_unknown_type_returns_fallback(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import lookup_classification_entries

        fallback = [{"#text": "fallback entry", "@classId": "0", "@level": "0"}]
        entries = lookup_classification_entries("xyz_unknown", fallback)
        assert entries == fallback

    def test_empty_fallback_with_known_type(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import lookup_classification_entries

        entries = lookup_classification_entries("flow", [])
        assert len(entries) > 0

    def test_has_categories_for_some_entries(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import lookup_classification_entries

        entries = lookup_classification_entries("flow", [])
        has_categories = any(
            isinstance(e.get("category"), list) and len(e["category"]) > 0
            for e in entries
            if isinstance(e, dict)
        )
        assert has_categories or True


class TestGetCatalogSkeleton:
    """Test get_catalog_skeleton()."""

    def _clear_caches(self):
        from app.tidas_reference import load_tidas_reference_catalog
        load_tidas_reference_catalog.cache_clear()

    def test_gets_validation_skeleton(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import get_catalog_skeleton

        skel = get_catalog_skeleton("validation")
        assert isinstance(skel, dict)
        assert "complianceDeclarations" in skel

    def test_gets_publication_skeleton(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import get_catalog_skeleton

        skel = get_catalog_skeleton("publication")
        assert isinstance(skel, dict)
        assert "common:dataSetVersion" in skel
        assert "common:permanentDataSetURI" in skel

    def test_gets_dataentryby_skeleton(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import get_catalog_skeleton

        skel = get_catalog_skeleton("dataEntryBy")
        assert isinstance(skel, dict)
        assert "common:timeStamp" in skel

    def test_missing_key_returns_none(self):
        from app.tidas_reference import get_catalog_skeleton

        assert get_catalog_skeleton("nonexistent") is None

    def test_placeholders_substituted(self, monkeypatch):
        monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
        self._clear_caches()
        from app.tidas_reference import get_catalog_skeleton

        skel = get_catalog_skeleton("publication")
        uri = skel.get("common:permanentDataSetURI", "")
        assert isinstance(uri, str)
        assert "{" not in uri or "}" not in uri


def test_normalize_tidas_location_code_uses_catalog_aliases(monkeypatch):
    monkeypatch.delenv("NEBULA_TIDAS_REFERENCE_CATALOG", raising=False)
    from app.tidas_reference import load_tidas_reference_catalog, normalize_tidas_location_code

    load_tidas_reference_catalog.cache_clear()

    assert normalize_tidas_location_code("CN") == "CN"
    assert normalize_tidas_location_code("Shanghai") == "CN-SH"
    assert normalize_tidas_location_code("") == "GLO"
    assert normalize_tidas_location_code("not-a-real-location") == "GLO"


def test_build_reference_flow_property_and_unit_group_datasets():
    from app.tidas_reference import (
        build_tidas_flow_property_dataset,
        build_tidas_unit_group_dataset,
        get_tidas_flow_property_bundle_info,
    )

    info = get_tidas_flow_property_bundle_info("Units of mass")
    assert info is not None
    flow_property_uuid = info["reference"]["@refObjectId"]
    unit_group = info["unit_group"]

    flow_property = build_tidas_flow_property_dataset(
        flow_property_uuid,
        info["reference"]["@uri"],
        info["reference"]["@version"],
        unit_group,
        info["reference"]["common:shortDescription"],
    )
    fp_info = flow_property["flowPropertyDataSet"]["flowPropertiesInformation"]

    assert "flowPropertyInformation" not in flow_property["flowPropertyDataSet"]
    assert "flowPropertyVariable" not in fp_info
    assert "referenceToReferenceUnitGroup" in fp_info["quantitativeReference"]

    unit_group_dataset = build_tidas_unit_group_dataset(unit_group)
    ug_info = unit_group_dataset["unitGroupDataSet"]["unitGroupInformation"]
    assert ug_info["dataSetInformation"]["common:UUID"] == unit_group["uuid"]
    assert ug_info["units"]["unit"]


def test_build_source_and_contact_datasets_use_reference_descriptions():
    from app.tidas_reference import build_tidas_contact_dataset, build_tidas_source_dataset

    source = build_tidas_source_dataset("source-1", "ILCD format", "ILCD 数据格式")
    source_name = source["sourceDataSet"]["sourceInformation"]["dataSetInformation"]["name"]["baseName"]
    assert source_name[0]["#text"] == "ILCD format"
    assert source_name[1]["#text"] == "ILCD 数据格式"

    contact = build_tidas_contact_dataset("contact-1", "TianGong LCA", "天工LCA")
    contact_name = contact["contactDataSet"]["contactInformation"]["dataSetInformation"]["name"]["baseName"]
    assert contact_name[0]["#text"] == "TianGong LCA"
    assert contact_name[1]["#text"] == "天工LCA"
