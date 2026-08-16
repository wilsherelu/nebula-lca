"""Validated TianGong-to-ecoinvent mappings from nebula-flow-mapping."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_PUBLIC_MAPPING_ROOT = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "flow_mappings"
    / "nebula-flow-mapping-v1"
)


@dataclass(frozen=True)
class PublicFlowMapping:
    tiangong_flow_uuid: str
    ecoinvent_flow_uuid: str
    flow_scope: str
    mapping_level: str
    relationship: str
    manual_confirmation_recommended: bool
    conversion_rule_id: str | None
    amount_factor: float
    source_unit: str
    target_unit: str


class PublicFlowMappingRegistry:
    """Load the public data-only package and fail closed on manifest drift."""

    def __init__(self, root: Path = DEFAULT_PUBLIC_MAPPING_ROOT):
        manifest_path = root / "MANIFEST.json"
        manifest_raw = manifest_path.read_bytes()
        manifest = json.loads(manifest_raw.decode("utf-8"))
        if manifest.get("dataset_version") != "1.0.0":
            raise ValueError("unsupported public flow-mapping dataset version")
        if manifest.get("mapping_direction") != "TIANGONG_TO_ECOINVENT":
            raise ValueError("unsupported public flow-mapping direction")
        if manifest.get("forbidden_content_included") is not False:
            raise ValueError("public flow-mapping package contains forbidden content")

        conversions = self._load_conversions(root / "data" / "unit-conversions.v1.json")
        self.intermediate = self._load_scope(root, manifest, "intermediate", conversions)
        self.elementary = self._load_scope(root, manifest, "elementary", conversions)
        self.root = root
        self.package_id = "nebula-flow-mapping"
        self.package_version = str(manifest["dataset_version"])
        self.package_hash = hashlib.sha256(manifest_raw).hexdigest()
        self.manifest = manifest

    @staticmethod
    def _load_conversions(path: Path) -> dict[str, dict[str, Any]]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("conversions")
        if not isinstance(rows, list):
            raise ValueError("public flow-mapping conversions are missing")
        indexed: dict[str, dict[str, Any]] = {}
        for row in rows:
            rule_id = str(row.get("conversion_rule_id") or "").strip()
            multiplier = float(row.get("multiplier") or 0)
            if not rule_id or rule_id in indexed or multiplier <= 0:
                raise ValueError("public flow-mapping conversion is invalid")
            indexed[rule_id] = row
        return indexed

    @staticmethod
    def _load_scope(
        root: Path,
        manifest: dict[str, Any],
        scope: str,
        conversions: dict[str, dict[str, Any]],
    ) -> dict[str, PublicFlowMapping]:
        contract = manifest.get(scope)
        if not isinstance(contract, dict):
            raise ValueError(f"public flow-mapping manifest has no {scope} contract")
        path = root / str(contract.get("file") or "")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != str(contract.get("sha256") or ""):
            raise ValueError(f"public flow-mapping {scope} hash mismatch")

        indexed: dict[str, PublicFlowMapping] = {}
        levels: dict[str, int] = {"L1": 0, "L2": 0}
        expected_scope = scope.upper()
        for line_number, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
            row = json.loads(line)
            source_uuid = str(row.get("tiangong_flow_uuid") or "").strip()
            target_uuid = str(row.get("ecoinvent_flow_uuid") or "").strip()
            mapping_level = str(row.get("mapping_level") or "")
            flow_scope = str(row.get("flow_scope") or "")
            if not source_uuid or not target_uuid or source_uuid in indexed:
                raise ValueError(f"invalid public {scope} mapping at line {line_number}")
            if mapping_level not in levels or flow_scope != expected_scope:
                raise ValueError(f"invalid public {scope} contract at line {line_number}")
            if row.get("mapping_direction") != "TIANGONG_TO_ECOINVENT":
                raise ValueError(f"invalid public {scope} direction at line {line_number}")

            conversion_rule_id = str(row.get("conversion_rule_id") or "").strip() or None
            conversion = conversions.get(conversion_rule_id) if conversion_rule_id else None
            if bool(row.get("conversion_required")) != bool(conversion):
                raise ValueError(f"invalid public {scope} conversion at line {line_number}")
            indexed[source_uuid] = PublicFlowMapping(
                tiangong_flow_uuid=source_uuid,
                ecoinvent_flow_uuid=target_uuid,
                flow_scope=flow_scope,
                mapping_level=mapping_level,
                relationship=str(row.get("relationship") or ""),
                manual_confirmation_recommended=bool(row.get("manual_confirmation_recommended")),
                conversion_rule_id=conversion_rule_id,
                amount_factor=float(conversion.get("multiplier", 1.0)) if conversion else 1.0,
                source_unit=str(conversion.get("source_unit") or "") if conversion else "",
                target_unit=str(conversion.get("target_unit") or "") if conversion else "",
            )
            levels[mapping_level] += 1

        if len(indexed) != int(contract.get("mapping_count") or -1):
            raise ValueError(f"public flow-mapping {scope} count mismatch")
        if levels != contract.get("mapping_levels"):
            raise ValueError(f"public flow-mapping {scope} level count mismatch")
        return indexed

    def resolve_intermediate(self, flow_uuid: str) -> PublicFlowMapping | None:
        return self.intermediate.get(str(flow_uuid or "").strip())

    def resolve_elementary(self, flow_uuid: str) -> PublicFlowMapping | None:
        return self.elementary.get(str(flow_uuid or "").strip())


@lru_cache(maxsize=1)
def get_public_flow_mapping_registry() -> PublicFlowMappingRegistry:
    return PublicFlowMappingRegistry()


def clear_public_flow_mapping_registry_cache() -> None:
    get_public_flow_mapping_registry.cache_clear()


def apply_elementary_mappings_to_snapshot(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Map TianGong elementary UUIDs before EF characterization."""
    registry = get_public_flow_mapping_registry()
    mapped: dict[str, PublicFlowMapping] = {}
    for flow in snapshot.get("flows") or []:
        if str(flow.get("flow_type") or "") != "Elementary flow":
            continue
        resolution = registry.resolve_elementary(str(flow.get("flow_uuid") or ""))
        if resolution is not None:
            mapped[resolution.tiangong_flow_uuid] = resolution
    if not mapped:
        return []

    trace: list[dict[str, Any]] = []
    for exchange in snapshot.get("exchanges") or []:
        resolution = mapped.get(str(exchange.get("flow_uuid") or ""))
        if resolution is None:
            continue
        exchange["flow_uuid"] = resolution.ecoinvent_flow_uuid
        exchange["amount"] = float(exchange.get("amount") or 0) * resolution.amount_factor

    flows_by_uuid: dict[str, dict[str, Any]] = {}
    for flow in snapshot.get("flows") or []:
        source_uuid = str(flow.get("flow_uuid") or "")
        resolution = mapped.get(source_uuid)
        if resolution is not None:
            flow = {
                **flow,
                "flow_uuid": resolution.ecoinvent_flow_uuid,
                "source_system": "ecoinvent_3.11",
            }
            trace.append({
                "source_flow_uuid": source_uuid,
                "target_flow_uuid": resolution.ecoinvent_flow_uuid,
                "amount_factor": resolution.amount_factor,
                "mapping_level": resolution.mapping_level,
                "package_id": registry.package_id,
                "package_version": registry.package_version,
            })
        flows_by_uuid.setdefault(str(flow.get("flow_uuid") or ""), flow)
    snapshot["flows"] = list(flows_by_uuid.values())
    return trace
