from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session

from ..provider_schemas import ProviderEngineIdentity, ProviderSolveRequest
from ..schemas import HybridGraph
from .provider_contract import ProviderContractError
from .provider_ef31 import ProviderEf31Error, provider_runtime_identity
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
        request.elementary_flows
        or request.background_process_pins
        or request.lcia_methods
    )
    ef31_identity = None
    if needs_ef31:
        try:
            ef31_identity = provider_runtime_identity()
        except ProviderEf31Error as exc:
            raise ProviderContractError(503, exc.code, exc.message, **exc.details) from exc
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
    }
    return canonical_hash(components)
