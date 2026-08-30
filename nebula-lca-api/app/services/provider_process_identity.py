from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session

from ..models import ReferenceProcess
from ..provider_schemas import (
    ExactProcessRef,
    ProviderIssue,
    ProviderProcessIdentityReceipt,
    ProviderProcessIdentityRef,
)
from ..schemas import HybridGraph
from .provider_contract import ProviderContractError
from .provider_tidas_process_snapshot import (
    TidasProcessSnapshot,
    canonical_hash as canonical_process_hash,
)
from .provider_tidas_snapshot import SOURCE_NAMESPACE as TIDAS_SOURCE_NAMESPACE


def _validate_graph_node_against_exact_process(
    graph: HybridGraph,
    *,
    process_uuid: str,
    process: dict[str, Any],
) -> None:
    node = next(node for node in graph.nodes if node.process_uuid == process_uuid)
    graph_ports = list(node.inputs) + list(node.outputs)
    process_exchanges = list(process.get("exchanges") or [])
    if len(graph_ports) != len(process_exchanges):
        raise ProviderContractError(
            422,
            "TIDAS_PROCESS_GRAPH_SIGNATURE_MISMATCH",
            "The graph node exchange count does not match the exact Process snapshot.",
            process_uuid=process_uuid,
            graph_exchange_count=len(graph_ports),
            snapshot_exchange_count=len(process_exchanges),
        )
    unmatched = list(process_exchanges)
    for port in graph_ports:
        matches = [
            exchange
            for exchange in unmatched
            if exchange["direction"] == port.direction
            and exchange["flow"]["source_namespace"] == str(port.flow_source_namespace or "")
            and exchange["flow"]["flow_uuid"] == port.flowUuid
            and exchange["flow"]["version"] == str(port.flow_version or "")
            and abs(float(exchange["amount"]) - float(port.amount)) <= 1e-12
        ]
        if len(matches) != 1:
            raise ProviderContractError(
                422,
                "TIDAS_PROCESS_GRAPH_SIGNATURE_MISMATCH",
                "A graph exchange does not uniquely match the exact Process snapshot identity and amount.",
                process_uuid=process_uuid,
                graph_exchange={
                    "direction": port.direction,
                    "source_namespace": port.flow_source_namespace,
                    "flow_uuid": port.flowUuid,
                    "version": port.flow_version,
                    "amount": port.amount,
                },
                match_count=len(matches),
            )
        unmatched.remove(matches[0])
    qref = process["quantitative_reference"]
    qref_flow = qref["flow"]
    qref_ports = [
        port
        for port in node.outputs
        if bool(port.isProduct)
        and port.flowUuid == qref_flow["flow_uuid"]
        and str(port.flow_version or "") == qref_flow["version"]
        and abs(float(port.amount) - float(qref["amount"])) <= 1e-12
    ]
    if len(qref_ports) != 1:
        raise ProviderContractError(
            422,
            "TIDAS_PROCESS_GRAPH_QREF_MISMATCH",
            "The graph node does not preserve the exact Process quantitative-reference output.",
            process_uuid=process_uuid,
            match_count=len(qref_ports),
        )


def build_process_identity_receipts(
    db: Session,
    graph: HybridGraph,
    identities: list[ProviderProcessIdentityRef],
    issues: list[ProviderIssue],
    *,
    process_snapshot: TidasProcessSnapshot | None,
    database_content_hash: Callable[[ReferenceProcess], str],
    database_conflicts: Callable[[ReferenceProcess | None, dict[str, Any]], dict[str, Any]],
) -> tuple[list[ProviderProcessIdentityReceipt], str | None]:
    if not identities:
        return [], None
    graph_process_counts: dict[str, int] = defaultdict(int)
    for node in graph.nodes:
        graph_process_counts[node.process_uuid] += 1
    requested = [item.process_uuid for item in identities]
    if len(requested) != len(set(requested)):
        raise ProviderContractError(
            422,
            "PROCESS_IDENTITY_DUPLICATE",
            "Each graph Process UUID may have at most one identity sidecar.",
        )
    receipts: list[ProviderProcessIdentityReceipt] = []
    ordered = sorted(identities, key=lambda item: item.process_uuid)
    for identity in ordered:
        match_count = graph_process_counts.get(identity.process_uuid, 0)
        if match_count != 1:
            raise ProviderContractError(
                422,
                "PROCESS_IDENTITY_GRAPH_MISMATCH",
                "A Process identity sidecar must select exactly one graph node.",
                process_uuid=identity.process_uuid,
                match_count=match_count,
            )
        if identity.source_namespace != TIDAS_SOURCE_NAMESPACE:
            receipts.append(
                ProviderProcessIdentityReceipt(
                    process_uuid=identity.process_uuid,
                    source_namespace=identity.source_namespace,
                    version=identity.version,
                    content_hash=identity.content_hash,
                    snapshot_hash=identity.snapshot_hash,
                    resolution="inline_custom",
                    verification_scope="consumer_asserted_hash",
                )
            )
            issues.append(
                ProviderIssue(
                    code="INLINE_CUSTOM_PROCESS_IDENTITY",
                    message=(
                        "The Process version and content hash are consumer assertions bound into provenance; "
                        "the provider has no authoritative catalog snapshot for this namespace."
                    ),
                    severity="info",
                    path=f"process_identity_receipts.{identity.process_uuid}",
                )
            )
            continue
        try:
            ExactProcessRef(
                source_namespace=identity.source_namespace,
                process_uuid=identity.process_uuid,
                version=identity.version,
            )
        except ValueError as exc:
            raise ProviderContractError(
                422,
                "PROCESS_IDENTITY_INVALID",
                "A TianGong Process identity must use a canonical UUID and exact fixed-width version.",
                process_uuid=identity.process_uuid,
                version=identity.version,
            ) from exc
        if identity.snapshot_hash is None:
            raise ProviderContractError(
                422,
                "TIDAS_PROCESS_IDENTITY_SNAPSHOT_HASH_REQUIRED",
                "A provider-verified TianGong Process identity requires its exact snapshot hash.",
                process_uuid=identity.process_uuid,
                version=identity.version,
            )
        if process_snapshot is None:
            raise ProviderContractError(
                422,
                "TIDAS_PROCESS_SNAPSHOT_REQUIRED",
                "Provider-verified TianGong Process identities require a configured exact read-only snapshot.",
            )
        value = process_snapshot.resolve(identity.process_uuid, identity.version)
        if value is None:
            raise ProviderContractError(
                422,
                "EXACT_PROCESS_VERSION_NOT_FOUND",
                "The exact Process UUID and version are absent from the configured snapshot.",
                process_uuid=identity.process_uuid,
                version=identity.version,
                snapshot_hash=process_snapshot.snapshot_hash,
            )
        mismatches = {
            field: {"requested": requested_value, "snapshot": snapshot_value}
            for field, requested_value, snapshot_value in (
                ("content_hash", identity.content_hash, value["content_hash"]),
                ("snapshot_hash", identity.snapshot_hash, value["snapshot_hash"]),
            )
            if requested_value != snapshot_value
        }
        if mismatches:
            raise ProviderContractError(
                422,
                "TIDAS_PROCESS_IDENTITY_HASH_MISMATCH",
                "The Process identity sidecar hashes do not match the exact snapshot.",
                process_uuid=identity.process_uuid,
                version=identity.version,
                mismatches=mismatches,
            )
        _validate_graph_node_against_exact_process(
            graph,
            process_uuid=identity.process_uuid,
            process=value,
        )
        row = db.get(ReferenceProcess, identity.process_uuid)
        same_identity = (
            row is not None
            and isinstance(row.import_report_json, dict)
            and row.import_report_json.get("source_namespace") == identity.source_namespace
            and row.import_report_json.get("source_version") == identity.version
        )
        if same_identity and not database_content_hash(row):
            raise ProviderContractError(
                422,
                "TIDAS_PROCESS_DATABASE_IDENTITY_INCOMPLETE",
                "The database claims the same Process identity but cannot prove its content hash.",
                process_uuid=identity.process_uuid,
                version=identity.version,
            )
        conflicts = database_conflicts(row, value)
        if conflicts:
            raise ProviderContractError(
                422,
                "TIDAS_PROCESS_DATABASE_CONTENT_CONFLICT",
                "The exact Process snapshot conflicts with the database row for the same identity.",
                process_uuid=identity.process_uuid,
                version=identity.version,
                conflicts=conflicts,
            )
        receipts.append(
            ProviderProcessIdentityReceipt(
                process_uuid=identity.process_uuid,
                source_namespace=identity.source_namespace,
                version=identity.version,
                content_hash=value["content_hash"],
                snapshot_hash=value["snapshot_hash"],
                resolution="tidas_exact_snapshot",
                verification_scope="provider_exact_snapshot",
                process_name=value.get("name"),
                process_type=value.get("process_type"),
            )
        )
    return receipts, canonical_process_hash(
        [item.model_dump(mode="json") for item in ordered]
    )
