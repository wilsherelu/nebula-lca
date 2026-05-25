"""TIDAS reference seed loading helpers.

The seed is optional at runtime. When it is absent or incomplete, callers should
fall back to the legacy export behavior instead of fabricating reference IDs.
"""

from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_TIDAS_REFERENCE_SEED_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "Tiangong"
    / "tidas_reference_seed.json"
)

DEFAULT_TIDAS_REFERENCE_CATALOG_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "Tiangong"
    / "tidas_reference_catalog.json"
)


def _seed_path() -> Path:
    override = os.environ.get("NEBULA_TIDAS_REFERENCE_SEED")
    if override:
        return Path(override)
    return DEFAULT_TIDAS_REFERENCE_SEED_PATH


def _catalog_path() -> Path:
    override = os.environ.get("NEBULA_TIDAS_REFERENCE_CATALOG")
    if override:
        return Path(override)
    return DEFAULT_TIDAS_REFERENCE_CATALOG_PATH


@lru_cache(maxsize=1)
def load_tidas_reference_catalog() -> dict[str, Any]:
    """Load the optional TIDAS reference catalog JSON.

    The catalog is a large static reference file containing locations,
    classifications, reference objects, and skeleton templates used by
    the TIDAS exporter to populate classificationInformation,
    publicationAndOwnership, and modellingAndValidation sections.

    When absent, callers fall back to legacy hardcoded values.
    """
    return _load_seed_payload(_catalog_path())


# ---------------------------------------------------------------------------
# Classification helpers (catalog-driven)
# ---------------------------------------------------------------------------


def _normalise_ilcd_dataset_type(raw: str | None) -> str:
    """Map a Nebula flow_type or ILCD dataset type to a canonical key.

    The ILCD catalog ``fallbackByDatasetType`` uses keys like:
    ``flow``, ``elementaryFlow``, ``process``, ``lifecyclemodel``,
    ``flowproperty``, ``unitgroup``, ``contact``, ``source``.
    """
    text = str(raw or "").strip().lower()
    if not text:
        return ""
    # Direct match common ILCD types
    direct = {
        "flow": "flow",
        "elementaryflow": "elementaryFlow",
        "process": "process",
        "lifecyclemodel": "lifecyclemodel",
        "flowproperty": "flowproperty",
        "unitgroup": "unitgroup",
        "contact": "contact",
        "source": "source",
    }
    if text in direct:
        return direct[text]
    # Map Nebula-style flow_type values
    if text in {"elementary flow", "product flow", "waste flow", "biosphere flow"}:
        return "flow"
    return ""


def lookup_classification_entries(dataset_type: str | None, fallback: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return classification entries from catalog for a given dataset type.

    Falls back to ``fallback`` list when catalog is absent or type is unknown.

    Each entry is a dict with keys ``classId`` (or ``@id``), ``name`` (or
    ``@name``), ``level`` (optional, defaults to 0), and optional ``category``
    (for hierarchical nesting).

    Catalog entries may have ``classId`` / ``level`` / ``name`` keys (the ILCD
    catalog serialisation format) or legacy ``@id`` / ``@name`` keys.
    """
    catalog = load_tidas_reference_catalog()
    if not catalog:
        return fallback

    classifications = catalog.get("classifications", {})
    fallback_by_type = classifications.get("fallbackByDatasetType", {})

    type_key = _normalise_ilcd_dataset_type(dataset_type)
    if type_key and type_key in fallback_by_type:
        return fallback_by_type[type_key]

    # Try to look up in systems (ilcd, isic, etc.)
    systems = classifications.get("systems", {})
    ilcd = systems.get("ilcd", [])
    if isinstance(ilcd, list):
        for entry in ilcd:
            if not isinstance(entry, dict):
                continue
            data_type = str(entry.get("dataType", "")).lower()
            if data_type == (str(dataset_type or "").strip().lower().replace(" ", "") if dataset_type else ""):
                hierarchy = entry.get("hierarchy", [])
                if hierarchy:
                    return [{"@id": str(h.get("@id", "")), "@name": str(h.get("@name", ""))} for h in hierarchy]

    return fallback


def get_catalog_skeleton(skeleton_key: str) -> dict[str, Any] | str | None:
    """Return a skeleton template from the catalog, with placeholders replaced.

    Valid skeleton keys correspond to top-level keys in the catalog ``skeletons``
    object (e.g. ``validation``, ``publication``, ``dataEntryBy``).

    Placeholders like ``{datasetPath}``, ``{datasetId}``, ``{version}`` are
    replaced with ``None`` so the exporter can inject real values later.
    """
    catalog = load_tidas_reference_catalog()
    if not catalog:
        return None

    skeletons = catalog.get("skeletons", {})
    template = skeletons.get(skeleton_key)
    if template is None:
        return None

    if isinstance(template, dict):
        return _substitute_placeholders(template)
    return template


def _substitute_placeholders(obj: Any) -> Any:
    """Recursively replace ``{...}`` placeholders with empty string in a dict/list."""
    PLACEHOLDER_RE = re.compile(r"\{[^}]+\}")

    def _replace(text: str) -> str:
        return PLACEHOLDER_RE.sub("", text)

    def _walk(item: Any) -> Any:
        if isinstance(item, dict):
            return {k: _walk(v) for k, v in item.items()}
        if isinstance(item, list):
            return [_walk(i) for i in item]
        if isinstance(item, str):
            return _replace(item)
        return item

    return _walk(obj)


def normalize_tidas_unit_group(value: str | None) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s*/\\]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def _load_seed_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _flow_property_by_uuid(seed: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in seed.get("flow_properties") or []:
        if not isinstance(item, dict):
            continue
        uuid = str(item.get("flow_property_uuid") or item.get("uuid") or "").strip()
        if uuid:
            result[uuid] = item
    return result


# Known English → Chinese translations for common flow property unit groups.
# When the seed has no name_zh, these provide safe fallbacks.
_FLOW_PROPERTY_ZH_TRANSLATIONS: dict[str, str] = {
    "mass": "质量",
    "mass*distance": "质量距离",
    "mass distance": "质量距离",
    "volume": "体积",
    "energy": "能量",
    "number of items": "数量",
    "item(s)": "项",
    "unit(s)": "单位",
    "currency": "货币",
    "length": "长度",
    "time": "时间",
    "area": "面积",
    "temperature": "温度",
    "pressure": "压力",
    "amount of substance": "物质的量",
    "mol": "摩尔",
    "electric current": "电流",
    "kilogram": "千克",
    "gram": "克",
    "joule": "焦耳",
    "watt": "瓦特",
    "litre": "升",
    "liters": "升",
    "meter": "米",
    "meters": "米",
    "kilometer": "千米",
    "square meter": "平方米",
    "cubic meter": "立方米",
    "decibel": "分贝",
    "sej": "社会环境焦耳",
    "pct": "百分比",
    "percent": "百分比",
    "h": "小时",
    "day": "天",
    "year": "年",
}


def _safe_zh_translation(name_en: str) -> str | None:
    """Return a safe Chinese translation for a flow property name, or None.

    Returns None when we cannot produce a reliable Chinese name, so the
    caller can omit the ``zh`` entry entirely (ILCD validator accepts
    single-language items).
    """
    key = str(name_en or "").strip().lower()
    if not key:
        return None
    # Exact match first
    if key in _FLOW_PROPERTY_ZH_TRANSLATIONS:
        return _FLOW_PROPERTY_ZH_TRANSLATIONS[key]
    # Substring match for compound units like "Units of mass", "Mass*time"
    for pattern, zh in _FLOW_PROPERTY_ZH_TRANSLATIONS.items():
        if pattern in key or key in pattern:
            return zh
    # No reliable translation → caller should omit zh
    return None


def _short_description(mapping: dict[str, Any], flow_property: dict[str, Any]) -> list[dict[str, str]]:
    descriptions = mapping.get("short_description") or flow_property.get("short_description")
    if isinstance(descriptions, list) and descriptions:
        return descriptions

    name_en = str(
        mapping.get("name_en")
        or flow_property.get("name_en")
        or flow_property.get("name")
        or "Unspecified"
    ).strip()
    name_zh = str(mapping.get("name_zh") or flow_property.get("name_zh") or "").strip()

    # If seed provides name_zh, use it directly
    if name_zh:
        return [
            {"#text": name_en, "@xml:lang": "en"},
            {"#text": name_zh, "@xml:lang": "zh"},
        ]

    # No name_zh in seed — use known translation table; omit zh if unknown
    zh = _safe_zh_translation(name_en)
    items = [{"#text": name_en, "@xml:lang": "en"}]
    if zh:
        items.append({"#text": zh, "@xml:lang": "zh"})
    return items


@lru_cache(maxsize=1)
def load_tidas_reference_seed() -> dict[str, Any]:
    """Load the optional TIDAS reference seed JSON."""

    return _load_seed_payload(_seed_path())


def get_tidas_flow_property_reference(unit_group: str | None) -> dict[str, Any] | None:
    """Return a TIDAS flow-property reference for a source unit group.

    Expected seed shape:
    {
      "unit_group_mappings": [
        {
          "source_unit_group": "Units of mass",
          "tidas_unit_group": "Units of mass",
          "flow_property_uuid": "...",
          "ref_uri": "../flowproperties/....xml",
          "version": "01.00.000",
          "mapping_status": "allowed"
        }
      ],
      "flow_properties": [...]
    }
    """

    normalized = normalize_tidas_unit_group(unit_group)
    if not normalized:
        return None

    seed = load_tidas_reference_seed()
    flow_properties = _flow_property_by_uuid(seed)
    for mapping in seed.get("unit_group_mappings") or []:
        if not isinstance(mapping, dict):
            continue
        candidates = {
            normalize_tidas_unit_group(mapping.get("source_unit_group")),
            normalize_tidas_unit_group(mapping.get("tidas_unit_group")),
        }
        aliases = mapping.get("aliases")
        if isinstance(aliases, list):
            candidates.update(normalize_tidas_unit_group(alias) for alias in aliases)
        if normalized not in candidates:
            continue

        status = str(mapping.get("mapping_status") or "allowed").strip().lower()
        if status not in {"allowed", "exact", "approved"}:
            return None

        flow_property_uuid = str(mapping.get("flow_property_uuid") or "").strip()
        if not flow_property_uuid:
            return None
        flow_property = flow_properties.get(flow_property_uuid, {})
        ref_uri = str(
            mapping.get("ref_uri")
            or flow_property.get("ref_uri")
            or f"../flowproperties/{flow_property_uuid}.xml"
        ).strip()
        version = str(mapping.get("version") or flow_property.get("version") or "01.00.000").strip()
        return {
            "@refObjectId": flow_property_uuid,
            "@type": "flow property data set",
            "@uri": ref_uri,
            "@version": version,
            "common:shortDescription": _short_description(mapping, flow_property),
        }

    return None


def get_tidas_allowed_unit_groups() -> list[str]:
    """Return normalized unit group names allowed by the active TIDAS seed."""

    seed = load_tidas_reference_seed()
    allowed_from_mappings: list[str] = []
    seen_from_mappings: set[str] = set()
    allowed_from_unit_groups: list[str] = []
    seen_from_unit_groups: set[str] = set()

    def add(value: str | None, target: list[str], seen: set[str]) -> None:
        normalized = normalize_tidas_unit_group(value)
        if normalized and normalized not in seen:
            seen.add(normalized)
            target.append(normalized)

    for unit_group in seed.get("unit_groups") or []:
        if not isinstance(unit_group, dict):
            continue
        add(unit_group.get("name"), allowed_from_unit_groups, seen_from_unit_groups)
        add(unit_group.get("source_unit_group"), allowed_from_unit_groups, seen_from_unit_groups)
        aliases = unit_group.get("aliases")
        if isinstance(aliases, list):
            for alias in aliases:
                add(alias, allowed_from_unit_groups, seen_from_unit_groups)

    for mapping in seed.get("unit_group_mappings") or []:
        if not isinstance(mapping, dict):
            continue
        status = str(mapping.get("mapping_status") or "allowed").strip().lower()
        if status not in {"allowed", "exact", "approved"}:
            continue
        if not str(mapping.get("flow_property_uuid") or "").strip():
            continue
        add(mapping.get("source_unit_group"), allowed_from_mappings, seen_from_mappings)
        add(mapping.get("tidas_unit_group"), allowed_from_mappings, seen_from_mappings)
        aliases = mapping.get("aliases")
        if isinstance(aliases, list):
            for alias in aliases:
                add(alias, allowed_from_mappings, seen_from_mappings)

    return allowed_from_mappings or allowed_from_unit_groups


# ---------------------------------------------------------------------------
# Bundled reference dataset generation
# ---------------------------------------------------------------------------


def build_tidas_source_dataset(
    ref_object_id: str,
    short_description_en: str,
    short_description_zh: str | None = None,
) -> dict[str, Any]:
    """Build a minimal valid ILCD sourceDataSet.

    The dataset is self-contained and can be written directly into a ZIP.
    """
    return {
        "sourceDataSet": {
            "@locations": "../ILCDLocations.xml",
            "@version": "1.1",
            "@xmlns": "http://lca.jrc.it/ILCD/Source",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Source ../../schemas/ILCD_SourceDataSet.xsd",
            "administrativeInformation": {
                "common:commissionerAndGoal": {
                    "common:intendedApplications": [
                        {"#text": "Generated by Nebula LCA", "@xml:lang": "en"},
                    ],
                    "common:referenceToCommissioner": {
                        "@refObjectId": "11111111-1111-4111-8111-111111111111",
                        "@type": "contact data set",
                        "@uri": "../contacts/11111111-1111-4111-8111-111111111111.xml",
                        "common:shortDescription": {"#text": "TianGong LCA", "@xml:lang": "en"},
                    },
                },
                "dataEntryBy": {
                    "common:timeStamp": "2026-01-01T00:00:00Z",
                    "common:referenceToDataSetFormat": {
                        "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
                        "@type": "source data set",
                        "@version": "03.00.003",
                    },
                    "common:referenceToPersonOrEntityEnteringTheData": {
                        "@refObjectId": "11111111-1111-4111-8111-111111111111",
                        "@type": "contact data set",
                        "@uri": "../contacts/11111111-1111-4111-8111-111111111111.xml",
                    },
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": "03.00.003",
                    "common:referenceToOwnershipOfDataSet": {
                        "@refObjectId": "11111111-1111-4111-8111-111111111111",
                        "@type": "contact data set",
                        "@uri": "../contacts/11111111-1111-4111-8111-111111111111.xml",
                    },
                },
            },
            "sourceInformation": {
                "dataSetInformation": {
                    "common:UUID": ref_object_id,
                    "common:generalComment": [
                        {"#text": "Generated by Nebula LCA", "@xml:lang": "en"},
                    ],
                    "name": {
                        "baseName": [
                            {"#text": "Reference Source", "@xml:lang": "en"},
                        ],
                        "mixAndLocationTypes": [
                            {"#text": "Reference", "@xml:lang": "en"},
                        ],
                        "treatmentStandardsRoutes": [
                            {"#text": "Unspecified", "@xml:lang": "en"},
                        ],
                    },
                },
            },
        }
    }


def build_tidas_contact_dataset(
    ref_object_id: str,
    short_description_en: str,
    short_description_zh: str | None = None,
) -> dict[str, Any]:
    """Build a minimal valid ILCD contactDataSet."""
    return {
        "contactDataSet": {
            "@locations": "../ILCDLocations.xml",
            "@version": "1.1",
            "@xmlns": "http://lca.jrc.it/ILCD/Contact",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Contact ../../schemas/ILCD_ContactDataSet.xsd",
            "administrativeInformation": {
                "common:commissionerAndGoal": {
                    "common:intendedApplications": [
                        {"#text": "Generated by Nebula LCA", "@xml:lang": "en"},
                    ],
                },
                "dataEntryBy": {
                    "common:timeStamp": "2026-01-01T00:00:00Z",
                    "common:referenceToDataSetFormat": {
                        "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
                        "@type": "source data set",
                        "@version": "03.00.003",
                    },
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": "01.00.000",
                },
            },
            "contactInformation": {
                "dataSetInformation": {
                    "common:UUID": ref_object_id,
                    "common:generalComment": [
                        {"#text": "Generated by Nebula LCA", "@xml:lang": "en"},
                    ],
                    "name": {
                        "baseName": [
                            {"#text": short_description_en, "@xml:lang": "en"},
                        ],
                        "mixAndLocationTypes": [
                            {"#text": "Contact", "@xml:lang": "en"},
                        ],
                        "treatmentStandardsRoutes": [
                            {"#text": "Unspecified", "@xml:lang": "en"},
                        ],
                    },
                },
            },
        }
    }


def build_tidas_flow_property_dataset(
    ref_object_id: str,
    ref_uri: str,
    version: str,
    unit_groups: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a minimal valid ILCD flowPropertyDataSet.

    Args:
        ref_object_id: UUID of the flow property
        ref_uri: URI of the dataset (without path prefix)
        version: dataset version string
        unit_groups: list of ILCD unit group dicts to include in flowProperty
    """
    unit_groups_block = {"unitGroup": unit_groups} if unit_groups else {}

    return {
        "flowPropertyDataSet": {
            "@locations": "../ILCDLocations.xml",
            "@version": "1.1",
            "@xmlns": "http://lca.jrc.it/ILCD/FlowProperty",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/FlowProperty ../../schemas/ILCD_FlowPropertyDataSet.xsd",
            "administrativeInformation": {
                "common:commissionerAndGoal": {
                    "common:intendedApplications": [
                        {"#text": "Generated by Nebula LCA", "@xml:lang": "en"},
                    ],
                },
                "dataEntryBy": {
                    "common:timeStamp": "2026-01-01T00:00:00Z",
                    "common:referenceToDataSetFormat": {
                        "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
                        "@type": "source data set",
                        "@version": "03.00.003",
                    },
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": version,
                },
            },
            "flowPropertyInformation": {
                "dataSetInformation": {
                    "common:UUID": ref_object_id,
                    "common:generalComment": [
                        {"#text": "Generated by Nebula LCA", "@xml:lang": "en"},
                    ],
                    "name": {
                        "baseName": [
                            {"#text": "Flow Property", "@xml:lang": "en"},
                        ],
                        "mixAndLocationTypes": [
                            {"#text": "Flow Property", "@xml:lang": "en"},
                        ],
                        "treatmentStandardsRoutes": [
                            {"#text": "Unspecified", "@xml:lang": "en"},
                        ],
                    },
                },
                "flowPropertyVariable": unit_groups_block,
            },
        }
    }
