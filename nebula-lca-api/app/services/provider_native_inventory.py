"""Method-independent, pinned native elementary inventory for a mapped background leaf."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..ecoinvent_ef31_loader import parse_elementary_exchanges, parse_units
from ..provider_schemas import ProviderNativeBackgroundInventoryRequest
from .provider_contract import ProviderContractError
from .provider_native_climate import (
    REFERENCE_SEED_PATH,
    _check_hash,
    _native_unit_bindings,
    _read_json,
    build_native_source_flow_index,
)
from .provider_tidas_snapshot import TidasFlowSnapshot


MANIFEST_ENV = "NEBULA_PROVIDER_NATIVE_INVENTORY_MANIFEST_PATH"


@dataclass(frozen=True)
class NativeInventoryRuntime:
    manifest_sha256: str
    source_asset_sha256: dict[str, str]
    native_source_flows: dict[str, dict[str, str]]
    unit_bindings: dict[str, dict[str, str]]
    unit_binding_sha256: str
    allowed_flow_sources: tuple[str, ...]


def load_native_inventory_runtime(
    pin: ProviderNativeBackgroundInventoryRequest,
    *,
    flow_snapshot: TidasFlowSnapshot | None,
) -> NativeInventoryRuntime:
    configured = str(os.getenv(MANIFEST_ENV) or "").strip()
    if not configured:
        raise ProviderContractError(422, "NATIVE_INVENTORY_RUNTIME_UNAVAILABLE", "The pinned native inventory manifest is not configured.")
    manifest_path = Path(configured)
    _check_hash(manifest_path, pin.inventory_runtime_manifest_sha256)
    manifest: dict[str, Any] = _read_json(manifest_path)
    asset_hashes = manifest.get("source_asset_sha256")
    labels = manifest.get("vector_flow_source_identities")
    if (
        manifest.get("schema_version") != "provider.native-inventory-runtime-manifest.v1"
        or manifest.get("database_release") != pin.database_release
        or not isinstance(asset_hashes, dict)
        or set(asset_hashes) != {"ElementaryExchanges.xml", "Units.xml"}
        or not all(isinstance(value, str) and len(value) == 64 for value in asset_hashes.values())
        or not isinstance(labels, list)
        or not labels
        or not all(isinstance(value, str) and value for value in labels)
    ):
        raise ProviderContractError(422, "NATIVE_INVENTORY_MANIFEST_INVALID", "The native inventory manifest does not pin the original source assets and release labels.")
    if (
        flow_snapshot is None
        or flow_snapshot.snapshot_hash != pin.flow_snapshot_hash
        or manifest.get("flow_snapshot_hash") != pin.flow_snapshot_hash
        or flow_snapshot.reference_dependencies is None
        or flow_snapshot.reference_dependencies.snapshot_hash != pin.reference_dependency_snapshot_hash
        or manifest.get("reference_dependency_snapshot_hash") != pin.reference_dependency_snapshot_hash
    ):
        raise ProviderContractError(422, "NATIVE_INVENTORY_SOURCE_MISMATCH", "The exact TianGong Flow/reference snapshots differ from the inventory request.")
    electricity = flow_snapshot.resolve(str(manifest.get("electricity_flow_uuid")), str(manifest.get("electricity_flow_version")))
    if electricity is None or electricity.get("content_hash") != manifest.get("electricity_flow_payload_content_hash"):
        raise ProviderContractError(422, "NATIVE_INVENTORY_SOURCE_MISMATCH", "The exact electricity Flow payload differs from the inventory manifest.")
    source_dir = Path(str(manifest.get("source_asset_dir") or ""))
    for filename, expected in asset_hashes.items():
        _check_hash(source_dir / filename, expected)
    _check_hash(REFERENCE_SEED_PATH, str(manifest.get("reference_seed_sha256") or ""))
    source_units = parse_units(source_dir)
    unit_bindings = _native_unit_bindings(
        Path(str(manifest.get("unit_binding_path") or "")),
        expected_hash=str(manifest.get("unit_binding_sha256") or ""),
        source_units={"sha256": asset_hashes["Units.xml"], "records": source_units},
        database_release=pin.database_release,
    )
    parsed_flows = parse_elementary_exchanges(source_dir, source_units)
    source_flows = build_native_source_flow_index(
        parsed_flows, {item.flow_uuid: index for index, item in enumerate(parsed_flows)}
    )
    return NativeInventoryRuntime(
        manifest_sha256=pin.inventory_runtime_manifest_sha256,
        source_asset_sha256=asset_hashes,
        native_source_flows=source_flows,
        unit_bindings=unit_bindings,
        unit_binding_sha256=str(manifest["unit_binding_sha256"]),
        allowed_flow_sources=tuple(labels),
    )
