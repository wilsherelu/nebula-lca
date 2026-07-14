"""Validated context crosswalk used before flow-level candidate certification."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class ContextMappingMode(str, Enum):
    BIJECTIVE = "bijective"
    EF_TO_ECOINVENT_CANONICALIZATION = "ef_to_ecoinvent_canonicalization"
    REJECTED = "rejected"


@dataclass(frozen=True)
class ContextMapping:
    ef_context: str
    ecoinvent_compartment: str | None
    ecoinvent_subcompartment: str | None
    mode: ContextMappingMode
    review_status: str
    canonical_ef_context: str | None = None
    reason: str | None = None


def load_context_crosswalk(path: Path) -> tuple[ContextMapping, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows: list[ContextMapping] = []
    seen_ef: set[str] = set()
    bijective_eco: set[tuple[str, str]] = set()
    for raw in payload["mappings"]:
        ef_context = str(raw["ef_context"])
        if ef_context in seen_ef:
            raise ValueError(f"duplicate EF context: {ef_context}")
        seen_ef.add(ef_context)
        mode = ContextMappingMode(str(raw["mode"]))
        eco = raw.get("ecoinvent_context")
        compartment = str(eco["compartment"]) if eco is not None else None
        subcompartment = str(eco["subcompartment"]) if eco is not None else None
        canonical = raw.get("canonical_ef_context")
        if mode == ContextMappingMode.BIJECTIVE:
            if eco is None or raw.get("review_status") != "approved":
                raise ValueError(f"bijective context is not approved: {ef_context}")
            eco_key = (compartment, subcompartment)
            if eco_key in bijective_eco:
                raise ValueError(f"duplicate bijective ecoinvent context: {eco_key}")
            bijective_eco.add(eco_key)
        elif mode == ContextMappingMode.EF_TO_ECOINVENT_CANONICALIZATION:
            if eco is None or not canonical:
                raise ValueError(f"canonicalization lacks a canonical target: {ef_context}")
        elif eco is not None:
            raise ValueError(f"rejected context must not expose a target: {ef_context}")
        rows.append(
            ContextMapping(
                ef_context=ef_context,
                ecoinvent_compartment=compartment,
                ecoinvent_subcompartment=subcompartment,
                mode=mode,
                review_status=str(raw["review_status"]),
                canonical_ef_context=str(canonical) if canonical else None,
                reason=str(raw["reason"]) if raw.get("reason") else None,
            )
        )
    canonical_targets = {
        row.ef_context for row in rows if row.mode == ContextMappingMode.BIJECTIVE
    }
    missing_targets = {
        row.canonical_ef_context
        for row in rows
        if row.mode == ContextMappingMode.EF_TO_ECOINVENT_CANONICALIZATION
        and row.canonical_ef_context not in canonical_targets
    }
    if missing_targets:
        raise ValueError(f"canonical EF contexts are not bijective: {sorted(missing_targets)}")
    return tuple(rows)
