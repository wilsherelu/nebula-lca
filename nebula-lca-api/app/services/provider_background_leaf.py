from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from ..provider_schemas import (
    ExactFlowRef,
    ProviderBackgroundExchangeReceipt,
    ProviderBackgroundProcessPin,
    ProviderBackgroundProcessReceipt,
    ProviderElementaryFlowRef,
    ProviderScaledExchange,
)
from ..schemas import FlowPort, HybridEdge, HybridGraph, HybridNode
from .provider_ef31 import ProviderEf31Error, resolve_standard_flow
from .provider_tidas_process_snapshot import TidasProcessSnapshot
from .provider_tidas_snapshot import ProviderTidasSnapshotError, TidasFlowSnapshot


SOURCE_NAMESPACE = "tiangong_open_data"
CLAIM_LIMIT = "partial_background_leaf_only_not_complete_cradle_to_gate"


class ProviderBackgroundLeafError(RuntimeError):
    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class BackgroundReceiptDraft:
    pin: ProviderBackgroundProcessPin
    process: dict[str, Any]
    exchange_sources: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class BackgroundExpansion:
    graph: HybridGraph
    elementary_refs: tuple[ProviderElementaryFlowRef, ...]
    receipt_drafts: tuple[BackgroundReceiptDraft, ...]
    pins_hash: str | None


def _port_lookup(graph: HybridGraph) -> dict[str, tuple[FlowPort, str, str]]:
    result: dict[str, tuple[FlowPort, str, str]] = {}
    for node in graph.nodes:
        for port in node.inputs + node.outputs:
            exchange_id = f"{node.id}::{port.id}"
            if exchange_id in result:
                raise ProviderBackgroundLeafError(
                    "BACKGROUND_CONSUMER_EXCHANGE_AMBIGUOUS",
                    "A graph exchange ID is not unique.",
                    consumer_exchange_id=exchange_id,
                )
            result[exchange_id] = (port, node.id, node.process_uuid)
    return result


def _connected_exchange_ids(graph: HybridGraph) -> set[str]:
    result: set[str] = set()
    for edge in graph.exchanges:
        source_port = str(edge.source_port_id or edge.sourceHandle or "").split(":", 1)[-1]
        target_port = str(edge.target_port_id or edge.targetHandle or "").split(":", 1)[-1]
        if source_port:
            result.add(f"{edge.fromNode}::{source_port}")
        if target_port:
            result.add(f"{edge.toNode}::{target_port}")
    return result


def _resolve_exact_snapshot_flow(
    snapshot: TidasFlowSnapshot,
    *,
    flow_uuid: str,
    version: str,
) -> dict[str, Any] | None:
    try:
        value = snapshot.resolve(flow_uuid, version)
    except ProviderTidasSnapshotError as exc:
        raise ProviderBackgroundLeafError(exc.code, exc.message, **exc.details) from exc
    if value is None and snapshot.contains_uuid(flow_uuid):
        raise ProviderBackgroundLeafError(
            "BACKGROUND_FLOW_EXACT_VERSION_NOT_IN_SNAPSHOT",
            "The configured Flow snapshot contains this UUID, but not the exact Process exchange version.",
            flow_uuid=flow_uuid,
            version=version,
            snapshot_hash=snapshot.snapshot_hash,
        )
    return value


def _resolve_elementary_flow(
    snapshot: TidasFlowSnapshot,
    *,
    flow_uuid: str,
    version: str,
) -> dict[str, Any]:
    value = _resolve_exact_snapshot_flow(snapshot, flow_uuid=flow_uuid, version=version)
    if value is None:
        try:
            value = resolve_standard_flow(
                ExactFlowRef(
                    source_namespace=SOURCE_NAMESPACE,
                    flow_uuid=flow_uuid,
                    version=version,
                )
            )
        except ProviderEf31Error as exc:
            raise ProviderBackgroundLeafError(exc.code, exc.message, **exc.details) from exc
    if value is None:
        raise ProviderBackgroundLeafError(
            "BACKGROUND_ELEMENTARY_FLOW_NOT_RESOLVED",
            "A non-reference Process exchange is not an exact EF3.1 elementary Flow.",
            flow_uuid=flow_uuid,
            version=version,
        )
    if value.get("flow_type") != "Elementary flow":
        raise ProviderBackgroundLeafError(
            "BACKGROUND_PROCESS_NOT_CLOSED_LEAF",
            "A closed-leaf background Process may not contain technosphere dependencies or non-reference product outputs.",
            flow_uuid=flow_uuid,
            version=version,
            flow_type=value.get("flow_type"),
        )
    return value


def _identity_mismatches(port: FlowPort, value: dict[str, Any]) -> dict[str, dict[str, Any]]:
    comparisons = {
        "source_namespace": (port.flow_source_namespace, value.get("source_namespace")),
        "flow_uuid": (port.flowUuid, value.get("flow_uuid")),
        "version": (port.flow_version, value.get("version")),
        "flow_property_uuid": (port.flow_property_uuid, value.get("flow_property_uuid")),
        "flow_property_version": (port.flow_property_version, value.get("flow_property_version")),
        "unit_group_uuid": (port.unit_group_uuid, value.get("unit_group_uuid")),
        "unit_group_version": (port.unit_group_version, value.get("unit_group_version")),
        "unit": (port.unit, value.get("unit") or value.get("default_unit")),
    }
    return {
        field: {"consumer": consumer, "provider": provider}
        for field, (consumer, provider) in comparisons.items()
        if str(consumer or "").strip() != str(provider or "").strip()
    }


def expand_background_process_pins(
    *,
    graph: HybridGraph,
    pins: list[ProviderBackgroundProcessPin],
    process_snapshot: TidasProcessSnapshot | None,
    flow_snapshot: TidasFlowSnapshot | None,
) -> BackgroundExpansion:
    if not pins:
        return BackgroundExpansion(
            graph=graph,
            elementary_refs=(),
            receipt_drafts=(),
            pins_hash=None,
        )
    if process_snapshot is None:
        raise ProviderBackgroundLeafError(
            "BACKGROUND_PROCESS_SNAPSHOT_REQUIRED",
            "Background Process pins require a configured exact read-only Process snapshot.",
        )
    if flow_snapshot is None:
        raise ProviderBackgroundLeafError(
            "BACKGROUND_FLOW_SNAPSHOT_REQUIRED",
            "Background Process pins require a configured exact read-only Flow snapshot.",
        )

    ordered = sorted(pins, key=lambda item: item.consumer_exchange_id)
    consumer_ids = [pin.consumer_exchange_id for pin in ordered]
    if len(consumer_ids) != len(set(consumer_ids)):
        raise ProviderBackgroundLeafError(
            "BACKGROUND_PROCESS_PIN_DUPLICATE",
            "A consumer boundary exchange may have only one background Process pin.",
        )
    original_ports = _port_lookup(graph)
    connected = _connected_exchange_ids(graph)
    existing_process_uuids = {node.process_uuid for node in graph.nodes}
    selected_processes = {(pin.process_uuid, pin.version) for pin in ordered}
    resolved_qref_providers: dict[tuple[str, str, str], tuple[str, str]] = {}
    expanded = graph.model_copy(deep=True)
    elementary_refs: list[ProviderElementaryFlowRef] = []
    drafts: list[BackgroundReceiptDraft] = []

    for pin in ordered:
        if pin.source_namespace != SOURCE_NAMESPACE:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_NAMESPACE_UNSUPPORTED",
                "Provider v1 background leaves currently require exact TianGong open Process identities.",
                source_namespace=pin.source_namespace,
            )
        consumer_entry = original_ports.get(pin.consumer_exchange_id)
        if consumer_entry is None:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_CONSUMER_EXCHANGE_NOT_FOUND",
                "The pinned consumer exchange is not present in the consumer graph.",
                consumer_exchange_id=pin.consumer_exchange_id,
            )
        consumer_port, consumer_node_id, _ = consumer_entry
        if (
            consumer_port.type != "technosphere"
            or consumer_port.direction != "input"
            or float(consumer_port.amount) <= 0
        ):
            raise ProviderBackgroundLeafError(
                "BACKGROUND_CONSUMER_EXCHANGE_INVALID",
                "A background Process pin must target a positive boundary technosphere input.",
                consumer_exchange_id=pin.consumer_exchange_id,
            )
        if pin.consumer_exchange_id in connected:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_CONSUMER_ALREADY_CONNECTED",
                "A background Process pin cannot target an already connected consumer port.",
                consumer_exchange_id=pin.consumer_exchange_id,
            )
        process = process_snapshot.resolve(pin.process_uuid, pin.version)
        if process is None:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_EXACT_VERSION_NOT_FOUND",
                "The exact pinned Process is absent from the configured Process snapshot.",
                process_uuid=pin.process_uuid,
                version=pin.version,
                snapshot_hash=process_snapshot.snapshot_hash,
            )
        if process["content_hash"] != pin.expected_process_content_hash:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_CONTENT_HASH_MISMATCH",
                "The exact Process payload hash does not match the pin.",
                expected=pin.expected_process_content_hash,
                actual=process["content_hash"],
            )
        if process["snapshot_hash"] != pin.expected_process_snapshot_hash:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_SNAPSHOT_HASH_MISMATCH",
                "The Process snapshot hash does not match the pin.",
                expected=pin.expected_process_snapshot_hash,
                actual=process["snapshot_hash"],
            )
        qref = process["quantitative_reference"]
        if qref["exchange_internal_id"] != pin.quantitative_reference_exchange_internal_id:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_QREF_MISMATCH",
                "The Process quantitative-reference exchange does not match the pin.",
                expected=pin.quantitative_reference_exchange_internal_id,
                actual=qref["exchange_internal_id"],
            )
        qref_flow = qref["flow"]
        qref_value = _resolve_exact_snapshot_flow(
            flow_snapshot,
            flow_uuid=qref_flow["flow_uuid"],
            version=qref_flow["version"],
        )
        if qref_value is None or qref_value.get("flow_type") not in {"Product flow", "Waste flow"}:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_QREF_FLOW_NOT_RESOLVED",
                "The Process quantitative reference must resolve to an exact Product or Waste Flow snapshot.",
                flow_uuid=qref_flow["flow_uuid"],
                version=qref_flow["version"],
            )
        mismatches = _identity_mismatches(consumer_port, qref_value)
        if mismatches:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_QREF_CONSUMER_MISMATCH",
                "The Process quantitative reference does not exactly match the consumer boundary Flow identity and unit.",
                consumer_exchange_id=pin.consumer_exchange_id,
                mismatches=mismatches,
            )
        provider_key = (
            qref_value["source_namespace"],
            qref_value["flow_uuid"],
            qref_value["version"],
        )
        process_key = (pin.process_uuid, pin.version)
        for included in process.get("included_process_refs") or []:
            included_key = (included["process_uuid"], included["version"])
            if included_key in selected_processes:
                raise ProviderBackgroundLeafError(
                    "BACKGROUND_PROCESS_WRAPPER_DUPLICATE",
                    "A selected background Process includes another selected Process and would duplicate its projected inventory.",
                    wrapper=process_key,
                    included=included_key,
                )
        previous_provider = resolved_qref_providers.get(provider_key)
        if previous_provider is not None and previous_provider != process_key:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_FLOW_MULTIPLE_PROVIDERS",
                "One exact boundary Flow may not be pinned to multiple background Process providers.",
                flow_identity=provider_key,
                providers=[previous_provider, process_key],
            )
        resolved_qref_providers[provider_key] = process_key
        if pin.process_uuid in existing_process_uuids:
            raise ProviderBackgroundLeafError(
                "BACKGROUND_PROCESS_UUID_COLLISION",
                "The pinned background Process UUID already exists in the consumer graph.",
                process_uuid=pin.process_uuid,
            )

        node_token = _canonical_hash(pin.model_dump(mode="json"))[:16]
        node_id = f"provider-background-{node_token}"
        ports_by_direction: dict[str, list[FlowPort]] = {"input": [], "output": []}
        exchange_sources: list[dict[str, Any]] = []
        for exchange in process["exchanges"]:
            flow_ref = exchange["flow"]
            is_qref = exchange["exchange_internal_id"] == qref["exchange_internal_id"]
            flow_value = qref_value if is_qref else _resolve_elementary_flow(
                flow_snapshot,
                flow_uuid=flow_ref["flow_uuid"],
                version=flow_ref["version"],
            )
            port_id = f"exchange-{exchange['exchange_internal_id']}"
            expanded_exchange_id = f"{node_id}::{port_id}"
            flow_unit = str(flow_value.get("unit") or flow_value.get("default_unit") or "")
            display_flow_name = (
                flow_snapshot.display_name(flow_ref["flow_uuid"], flow_ref["version"])
                or flow_ref.get("name")
            )
            port = FlowPort(
                id=port_id,
                flowUuid=flow_value["flow_uuid"],
                flowSourceNamespace=flow_value["source_namespace"],
                flowVersion=flow_value["version"],
                flowPropertyUuid=flow_value["flow_property_uuid"],
                flowPropertyVersion=flow_value["flow_property_version"],
                unitGroupUuid=flow_value["unit_group_uuid"],
                unitGroupVersion=flow_value["unit_group_version"],
                name=str(flow_value.get("name") or flow_ref.get("name") or flow_value["flow_uuid"]),
                unit=flow_unit,
                unitGroup=flow_value.get("unit_group"),
                amount=float(exchange["amount"]),
                type="technosphere" if is_qref else "biosphere",
                direction=exchange["direction"],
                isProduct=is_qref,
                sourceSystem=SOURCE_NAMESPACE,
            )
            ports_by_direction[exchange["direction"]].append(port)
            source = {
                "exchange_internal_id": exchange["exchange_internal_id"],
                "expanded_exchange_id": expanded_exchange_id,
                "role": "quantitative_reference" if is_qref else "elementary",
                "source_namespace": flow_value["source_namespace"],
                "flow_uuid": flow_value["flow_uuid"],
                "version": flow_value["version"],
                "flow_name": display_flow_name,
                "flow_content_hash": flow_value["content_hash"],
                "flow_snapshot_hash": flow_value.get("snapshot_hash"),
                "flow_property_uuid": flow_value["flow_property_uuid"],
                "flow_property_version": flow_value["flow_property_version"],
                "unit_group_uuid": flow_value["unit_group_uuid"],
                "unit_group_version": flow_value["unit_group_version"],
                "unit": flow_unit,
                "direction": exchange["direction"],
                "raw_amount": float(exchange["amount"]),
            }
            exchange_sources.append(source)
            if not is_qref:
                elementary_refs.append(
                    ProviderElementaryFlowRef(
                        exchange_id=expanded_exchange_id,
                        source_namespace=flow_value["source_namespace"],
                        flow_uuid=flow_value["flow_uuid"],
                        version=flow_value["version"],
                        flow_property_uuid=flow_value["flow_property_uuid"],
                        flow_property_version=flow_value["flow_property_version"],
                        unit_group_uuid=flow_value["unit_group_uuid"],
                        unit_group_version=flow_value["unit_group_version"],
                        unit=flow_unit,
                        direction=exchange["direction"],
                        compartment=flow_value["compartment"],
                    )
                )
        qref_port = next(
            port for port in ports_by_direction["output"] if port.id == f"exchange-{qref['exchange_internal_id']}"
        )
        expanded.nodes.append(
            HybridNode(
                id=node_id,
                node_kind="lci_dataset",
                mode="normalized",
                process_uuid=pin.process_uuid,
                name=process.get("name") or pin.process_uuid,
                location="unspecified",
                source_system=SOURCE_NAMESPACE,
                reference_product=qref_port.name,
                reference_product_flow_uuid=qref_port.flowUuid,
                reference_product_direction="output",
                inputs=ports_by_direction["input"],
                outputs=ports_by_direction["output"],
            )
        )
        expanded.exchanges.append(
            HybridEdge(
                id=f"background-pin-{node_token}",
                fromNode=node_id,
                toNode=consumer_node_id,
                sourcePortId=qref_port.id,
                targetPortId=consumer_port.id,
                sourceHandle=f"out:{qref_port.id}",
                targetHandle=f"in:{consumer_port.id}",
                flowUuid=qref_port.flowUuid,
                flowName=qref_port.name,
                quantityMode="single",
                amount=float(consumer_port.amount),
                providerAmount=float(consumer_port.amount),
                consumerAmount=float(consumer_port.amount),
                unit=consumer_port.unit,
                providerUnit=qref_port.unit,
                consumerUnit=consumer_port.unit,
                type="technosphere",
            )
        )
        drafts.append(
            BackgroundReceiptDraft(
                pin=pin,
                process=process,
                exchange_sources=tuple(exchange_sources),
            )
        )
        existing_process_uuids.add(pin.process_uuid)

    pins_hash = _canonical_hash([pin.model_dump(mode="json") for pin in ordered])
    return BackgroundExpansion(
        graph=expanded,
        elementary_refs=tuple(elementary_refs),
        receipt_drafts=tuple(drafts),
        pins_hash=pins_hash,
    )


def build_background_process_receipts(
    *,
    drafts: tuple[BackgroundReceiptDraft, ...],
    activity_by_process: dict[str, float],
    scaled_exchanges: list[ProviderScaledExchange],
) -> list[ProviderBackgroundProcessReceipt]:
    scaled_by_id = {item.exchange_id: item for item in scaled_exchanges}
    receipts: list[ProviderBackgroundProcessReceipt] = []
    for draft in drafts:
        process = draft.process
        qref = process["quantitative_reference"]
        activity = activity_by_process[draft.pin.process_uuid]
        exchanges = [
            ProviderBackgroundExchangeReceipt(
                **source,
                scaled_amount=scaled_by_id[source["expanded_exchange_id"]].scaled_amount,
            )
            for source in draft.exchange_sources
        ]
        receipts.append(
            ProviderBackgroundProcessReceipt(
                consumer_exchange_id=draft.pin.consumer_exchange_id,
                source_namespace=draft.pin.source_namespace,
                process_uuid=draft.pin.process_uuid,
                version=draft.pin.version,
                process_name=process.get("name"),
                process_type=process["process_type"],
                process_content_hash=process["content_hash"],
                process_snapshot_hash=process["snapshot_hash"],
                quantitative_reference_exchange_internal_id=qref["exchange_internal_id"],
                quantitative_reference_flow_uuid=qref["flow"]["flow_uuid"],
                quantitative_reference_flow_version=qref["flow"]["version"],
                quantitative_reference_amount=float(qref["amount"]),
                activity_amount=activity,
                process_scale=activity / float(qref["amount"]),
                exchanges=exchanges,
            )
        )
    return receipts
