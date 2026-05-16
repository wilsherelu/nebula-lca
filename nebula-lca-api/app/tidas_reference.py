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


def _seed_path() -> Path:
    override = os.environ.get("NEBULA_TIDAS_REFERENCE_SEED")
    if override:
        return Path(override)
    return DEFAULT_TIDAS_REFERENCE_SEED_PATH


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
    name_zh = str(mapping.get("name_zh") or flow_property.get("name_zh") or name_en).strip()
    return [
        {"#text": name_en, "@xml:lang": "en"},
        {"#text": name_zh, "@xml:lang": "zh"},
    ]


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
