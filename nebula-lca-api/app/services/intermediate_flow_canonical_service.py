"""Resolve reviewed ecoinvent reference products to canonical TIDAS flow UUIDs."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_CANONICAL_PACKAGE_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "flow_mappings"
    / "intermediate_ecoinvent_to_tidas_canonical_v1.json"
)
_UUID_RE = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$", re.IGNORECASE)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)


@dataclass(frozen=True)
class CanonicalTidasFlowResolution:
    source_flow_uuid: str
    target_flow_uuid: str
    amount_factor: float
    source_unit: str
    target_unit: str
    unit_dimension: str
    origin_mapping_level: str
    origin_rule_id: str
    rule_id: str
    selection_mode: str
    tiangong_source_version: str
    warnings: tuple[str, ...]
    package_id: str
    package_version: str
    package_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_flow_uuid": self.source_flow_uuid,
            "target_flow_uuid": self.target_flow_uuid,
            "amount_factor": self.amount_factor,
            "source_unit": self.source_unit,
            "target_unit": self.target_unit,
            "unit_dimension": self.unit_dimension,
            "origin_mapping_level": self.origin_mapping_level,
            "origin_rule_id": self.origin_rule_id,
            "rule_id": self.rule_id,
            "selection_mode": self.selection_mode,
            "tiangong_source_version": self.tiangong_source_version,
            "warnings": list(self.warnings),
            "package_id": self.package_id,
            "package_version": self.package_version,
            "package_hash": self.package_hash,
            "direction": "ecoinvent_to_tidas_canonical",
        }


class IntermediateFlowCanonicalRegistry:
    def __init__(self, path: Path = DEFAULT_CANONICAL_PACKAGE_PATH):
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("direction") != "ecoinvent_to_tidas_canonical":
            raise ValueError("intermediate-flow canonical package has an unsupported direction")
        if not str(payload.get("package_id") or "").strip() or not str(payload.get("version") or "").strip():
            raise ValueError("intermediate-flow canonical package metadata is incomplete")
        rows = payload.get("mappings")
        if not isinstance(rows, list) or not rows:
            raise ValueError("intermediate-flow canonical package contains no mappings")
        indexed: dict[str, dict[str, Any]] = {}
        target_ids: set[str] = set()
        for row in rows:
            if not isinstance(row, dict) or row.get("canonical_status") != "approved":
                raise ValueError("intermediate-flow canonical package contains an unapproved rule")
            source_uuid = str(row.get("source_flow_uuid") or "").strip()
            target_uuid = str(row.get("target_flow_uuid") or "").strip()
            if not _UUID_RE.fullmatch(source_uuid) or not _UUID_RE.fullmatch(target_uuid):
                raise ValueError("intermediate-flow canonical package requires exact UUIDs")
            if source_uuid in indexed or target_uuid in target_ids:
                raise ValueError("intermediate-flow canonical package contains duplicate UUID choices")
            if row.get("source_flow_type") != row.get("target_flow_type"):
                raise ValueError(f"flow type mismatch in canonical rule {row.get('rule_id')}")
            factor = float(row.get("amount_factor") or 0)
            if factor <= 0 or not math.isfinite(factor):
                raise ValueError(f"invalid amount factor in canonical rule {row.get('rule_id')}")
            for field in ("origin_evidence_sha256", "selection_evidence_sha256"):
                if not _SHA256_RE.fullmatch(str(row.get(field) or "")):
                    raise ValueError(f"invalid {field} in canonical rule {row.get('rule_id')}")
            indexed[source_uuid] = row
            target_ids.add(target_uuid)
        self.path = path
        self.package_id = str(payload["package_id"])
        self.package_version = str(payload["version"])
        self.package_hash = hashlib.sha256(raw).hexdigest()
        self.source_package_id = str(payload.get("source_package_id") or "")
        self.source_package_version = str(payload.get("source_package_version") or "")
        self.unresolved_source_count = int(payload.get("unresolved_source_count") or 0)
        self.rules = indexed

    def resolve(self, flow_uuid: str) -> CanonicalTidasFlowResolution | None:
        row = self.rules.get(str(flow_uuid or "").strip())
        if row is None:
            return None
        return CanonicalTidasFlowResolution(
            source_flow_uuid=str(row["source_flow_uuid"]),
            target_flow_uuid=str(row["target_flow_uuid"]),
            amount_factor=float(row["amount_factor"]),
            source_unit=str(row["source_unit"]),
            target_unit=str(row["target_unit"]),
            unit_dimension=str(row["unit_dimension"]),
            origin_mapping_level=str(row["origin_mapping_level"]),
            origin_rule_id=str(row["origin_rule_id"]),
            rule_id=str(row["rule_id"]),
            selection_mode=str(row["selection_mode"]),
            tiangong_source_version=str(row.get("tiangong_source_version") or ""),
            warnings=tuple(str(item) for item in row.get("warnings") or []),
            package_id=self.package_id,
            package_version=self.package_version,
            package_hash=self.package_hash,
        )


@lru_cache(maxsize=1)
def get_intermediate_flow_canonical_registry() -> IntermediateFlowCanonicalRegistry:
    return IntermediateFlowCanonicalRegistry()


def clear_intermediate_flow_canonical_registry_cache() -> None:
    get_intermediate_flow_canonical_registry.cache_clear()
