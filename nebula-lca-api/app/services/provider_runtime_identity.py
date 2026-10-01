from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session

from ..provider_schemas import ProviderEngineIdentity, ProviderSolveRequest
from ..schemas import HybridGraph
from .provider_contract import ProviderContractError
from .elementary_flow_conversion_service import get_elementary_flow_mapping_registry
from .provider_ef31 import ProviderEf31Error, provider_runtime_identity
from .public_flow_mapping_service import get_public_flow_mapping_registry
from .provider_tidas_process_snapshot import TidasProcessSnapshot, canonical_hash
from .provider_tidas_snapshot import SOURCE_NAMESPACE as TIDAS_SOURCE_NAMESPACE, TidasFlowSnapshot


def build_solve_runtime_fingerprint(
    db: Session,
    request: ProviderSolveRequest,
    *,
    engine: ProviderEngineIdentity,
    resolve_snapshot: Callable[
        [Session, ProviderSolveRequest],
        tuple[HybridGraph, str, str, Any, list[Any]],
    ],
    load_flow_snapshot: Callable[[], TidasFlowSnapshot | None],
    load_process_snapshot: Callable[[], TidasProcessSnapshot | None],
) -> str:
    if not engine.version or not engine.commit:
        raise ProviderContractError(
            503,
            "IDEMPOTENCY_RUNTIME_IDENTITY_UNAVAILABLE",
            "Idempotent solve requires an exact engine version and build commit.",
        )
    graph, consumer_hash, provider_hash, _, _ = resolve_snapshot(db, request)
    needs_flow_snapshot = bool(
        request.inline_snapshot is not None
        or request.background_process_pins
        or request.mapped_background_process_pins
        or request.elementary_flows
        or request.lcia_methods
    )
    flow_snapshot = load_flow_snapshot() if needs_flow_snapshot else None
    needs_process_snapshot = bool(
        request.background_process_pins
        or any(
            item.source_namespace == TIDAS_SOURCE_NAMESPACE
            for item in request.process_identities
        )
    )
    process_snapshot = load_process_snapshot() if needs_process_snapshot else None
    if needs_process_snapshot and process_snapshot is None:
        raise ProviderContractError(
            503,
            "IDEMPOTENCY_RUNTIME_IDENTITY_UNAVAILABLE",
            "Idempotent solve cannot bind the required exact Process snapshot.",
        )
    needs_ef31 = bool(
        request.native_background_climate is None
        and request.native_background_inventory is None
        and (
            request.elementary_flows
            or request.background_process_pins
            or request.mapped_background_process_pins
            or request.lcia_methods
        )
    )
    ef31_identity = None
    if needs_ef31:
        try:
            ef31_identity = provider_runtime_identity()
        except ProviderEf31Error as exc:
            raise ProviderContractError(503, exc.code, exc.message, **exc.details) from exc
    elementary_mapping_dependencies = []
    if request.mapped_background_process_pins and request.native_background_climate is None and request.native_background_inventory is None:
        public_mapping = get_public_flow_mapping_registry()
        certified_mapping = get_elementary_flow_mapping_registry()
        elementary_mapping_dependencies = [
            {
                "package_id": public_mapping.package_id,
                "package_version": public_mapping.package_version,
                "package_hash": public_mapping.package_hash,
                "role": "public_interoperability_projection",
            },
            {
                "package_id": certified_mapping.package_id,
                "package_version": certified_mapping.package_version,
                "package_hash": certified_mapping.package_hash,
                "role": "certified_elementary_identity_adjudication",
            },
        ]
    components = {
        "schema_version": "provider.solve-runtime-fingerprint.v1",
        "engine": engine.model_dump(mode="json"),
        "consumer_graph_hash": consumer_hash,
        "provider_graph_hash": provider_hash,
        "flow_snapshot_hash": flow_snapshot.snapshot_hash if flow_snapshot is not None else None,
        "reference_dependency_snapshot_hash": (
            flow_snapshot.reference_dependencies.snapshot_hash
            if flow_snapshot is not None and flow_snapshot.reference_dependencies is not None
            else None
        ),
        "process_snapshot_hash": (
            process_snapshot.snapshot_hash if process_snapshot is not None else None
        ),
        "ef31": ef31_identity,
        "graph_database_release": str((graph.metadata or {}).get("database_release") or "unpinned"),
        "mapped_background_pins": [
            {
                "process_uuid": item.process_uuid,
                "database_release": item.database_release,
                "process_content_hash": item.expected_process_content_hash,
                "vector_checksum": item.expected_vector_checksum,
                "mapping_package_hash": item.mapping_package_hash,
            }
            for item in request.mapped_background_process_pins
        ],
        "elementary_mapping_dependencies": elementary_mapping_dependencies,
    }
    if request.native_background_climate is not None:
        from .provider_native_climate import load_native_climate_runtime

        native = load_native_climate_runtime(
            request.native_background_climate,
            flow_snapshot=flow_snapshot,
        )
        components["native_climate"] = {
            "method_package_sha256": native.package_sha256,
            "runtime_manifest_sha256": native.manifest_sha256,
            "source_flow_snapshot_hash": request.native_background_climate.flow_snapshot_hash,
            "reference_dependency_snapshot_hash": request.native_background_climate.reference_dependency_snapshot_hash,
            "database_release": request.native_background_climate.database_release,
        }
    if request.native_background_inventory is not None:
        from .provider_native_inventory import load_native_inventory_runtime

        native = load_native_inventory_runtime(
            request.native_background_inventory,
            flow_snapshot=flow_snapshot,
        )
        components["native_inventory"] = {
            "runtime_manifest_sha256": native.manifest_sha256,
            "source_asset_sha256": native.source_asset_sha256,
            "unit_binding_sha256": native.unit_binding_sha256,
            "flow_snapshot_hash": request.native_background_inventory.flow_snapshot_hash,
            "reference_dependency_snapshot_hash": request.native_background_inventory.reference_dependency_snapshot_hash,
            "database_release": request.native_background_inventory.database_release,
        }
    return canonical_hash(components)
