from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from ..models import FlowRecord, LciBiosphereFlowKey, LciProcessVector, ReferenceProcess
from ..provider_schemas import (
    ExactFlowRef,
    ProviderElementaryFlowRef,
    ProviderMappingDependencyReceipt,
    ProviderMappedBackgroundProcessPin,
    ProviderMappedBackgroundProcessReceipt,
)
from ..schemas import FlowPort, HybridEdge, HybridGraph, HybridNode
from .elementary_flow_conversion_service import get_elementary_flow_mapping_registry
from .lci_runtime import load_process_vectors
from .provider_background_leaf import CLAIM_LIMIT, ProviderBackgroundLeafError
from .provider_ef31 import (
    TIANGONG_ELEMENTARY_FLOW_VERSION,
    TIANGONG_OPEN_NAMESPACE,
    resolve_standard_flow,
)
from .provider_tidas_snapshot import TidasFlowSnapshot
from .public_flow_mapping_service import PublicFlowMapping, get_public_flow_mapping_registry


LICENSED_NAMESPACE = "ecoinvent"


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def mapped_process_content_hash(process: ReferenceProcess, vector: LciProcessVector) -> str:
    data = process.process_json if isinstance(process.process_json, dict) else {}
    return _canonical_hash(
        {
            "schema_version": "provider.mapped-background-process-content.v1",
            "process_uuid": process.process_uuid,
            "process_type": process.process_type,
            "reference_flow_uuid": process.reference_flow_uuid,
            "reference_product_amount": float(data.get("reference_product_amount") or 1.0),
            "reference_product_unit": str(data.get("reference_product_unit") or ""),
            "dataset_level": vector.dataset_level,
            "system_model": vector.system_model,
            "database_release": vector.source_package_version,
            "vector_checksum": vector.checksum,
        }
    )


@dataclass(frozen=True)
class MappedBackgroundReceiptDraft:
    pin: ProviderMappedBackgroundProcessPin
    process_content_hash: str
    vector_nnz: int
    quantitative_reference_amount: float
    provider_instance_id: str
    consumer_process_uuid: str
    consumer_converted_amount: float
    elementary_mapping_dependencies: tuple[ProviderMappingDependencyReceipt, ...]


@dataclass(frozen=True)
class MappedBackgroundExpansion:
    graph: HybridGraph
    elementary_refs: tuple[ProviderElementaryFlowRef, ...]
    receipt_drafts: tuple[MappedBackgroundReceiptDraft, ...]
    pins_hash: str | None
    database_releases: tuple[str, ...]
    elementary_mapping_dependencies: tuple[ProviderMappingDependencyReceipt, ...]


def _port_lookup(graph: HybridGraph) -> dict[str, tuple[FlowPort, str]]:
    result: dict[str, tuple[FlowPort, str]] = {}
    for node in graph.nodes:
        for port in [*node.inputs, *node.outputs]:
            exchange_id = f"{node.id}::{port.id}"
            if exchange_id in result:
                raise ProviderBackgroundLeafError(
                    "MAPPED_BACKGROUND_CONSUMER_EXCHANGE_AMBIGUOUS",
                    "A graph exchange ID is not unique.",
                    consumer_exchange_id=exchange_id,
                )
            result[exchange_id] = (port, node.id)
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


def _exact_source_flow(snapshot: TidasFlowSnapshot | None, pin: ProviderMappedBackgroundProcessPin) -> dict[str, Any]:
    if snapshot is None:
        raise ProviderBackgroundLeafError(
            "MAPPED_BACKGROUND_FLOW_SNAPSHOT_REQUIRED",
            "Mapped background pins require a configured exact read-only TianGong Flow snapshot.",
        )
    value = snapshot.resolve(pin.source_flow_uuid, pin.source_flow_version)
    if value is None:
        code = (
            "MAPPED_BACKGROUND_SOURCE_FLOW_EXACT_VERSION_NOT_FOUND"
            if snapshot.contains_uuid(pin.source_flow_uuid)
            else "MAPPED_BACKGROUND_SOURCE_FLOW_NOT_FOUND"
        )
        raise ProviderBackgroundLeafError(
            code,
            "The exact mapped source Flow is absent from the configured snapshot.",
            source_flow_uuid=pin.source_flow_uuid,
            source_flow_version=pin.source_flow_version,
            snapshot_hash=snapshot.snapshot_hash,
        )
    return value


def _inverse_elementary_mapping(flow_uuid: str) -> PublicFlowMapping | None:
    mapping, _ = _inverse_elementary_mapping_with_evidence(flow_uuid)
    return mapping


def _inverse_elementary_mapping_with_evidence(
    flow_uuid: str,
) -> tuple[PublicFlowMapping | None, bool]:
    registry = get_public_flow_mapping_registry()
    matches = [row for row in registry.elementary.values() if row.ecoinvent_flow_uuid == flow_uuid]
    if not matches:
        return None, False
    strict_matches = [row for row in matches if row.mapping_level == "L1"]
    if len(strict_matches) == 1:
        # The reverse lookup may include reviewed compatibility aliases for
        # the same ecoinvent Flow. A unique strict identity is authoritative;
        # compatibility rows must not make that exact identity ambiguous.
        return strict_matches[0], False
    if strict_matches:
        matches = strict_matches
    if len(matches) > 1:
        certified = get_elementary_flow_mapping_registry().resolve_mapped(flow_uuid)
        if certified is not None and certified.mode == "bidirectional":
            certified_matches = [
                row
                for row in matches
                if row.tiangong_flow_uuid == certified.target_flow_uuid
                and row.amount_factor == certified.amount_factor
            ]
            if len(certified_matches) == 1:
                # The public interoperability projection intentionally allows
                # many-to-one L2 rows and cannot express canonical EF aliases.
                # Its independently certified exact GHG core can disambiguate
                # only when identity, context, unit, and amount factor agree.
                return certified_matches[0], True
    if len(matches) != 1:
        raise ProviderBackgroundLeafError(
            "MAPPED_BACKGROUND_ELEMENTARY_MAPPING_AMBIGUOUS",
            "A licensed elementary Flow maps to more than one TianGong identity.",
            ecoinvent_flow_uuid=flow_uuid,
            match_count=len(matches),
        )
    return matches[0], False


def _standardized_elementary(
    flow_key: LciBiosphereFlowKey,
    amount: float,
) -> tuple[dict[str, Any], float, tuple[ProviderMappingDependencyReceipt, ...]]:
    direct = resolve_standard_flow(
        ExactFlowRef(
            source_namespace=TIANGONG_OPEN_NAMESPACE,
            flow_uuid=flow_key.flow_uuid,
            version=TIANGONG_ELEMENTARY_FLOW_VERSION,
        )
    )
    if direct is not None:
        if direct["unit"] != flow_key.canonical_unit:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_ELEMENTARY_UNIT_MISMATCH",
                "A licensed elementary inventory row does not use the exact standard Flow unit.",
                flow_uuid=flow_key.flow_uuid,
                vector_unit=flow_key.canonical_unit,
                standard_unit=direct["unit"],
            )
        return direct, amount, ()

    mapping, certified_adjudication = _inverse_elementary_mapping_with_evidence(flow_key.flow_uuid)
    if mapping is None:
        raise ProviderBackgroundLeafError(
            "MAPPED_BACKGROUND_ELEMENTARY_MAPPING_NOT_FOUND",
            "A licensed elementary inventory row has no accepted TianGong interoperability mapping.",
            ecoinvent_flow_uuid=flow_key.flow_uuid,
        )
    if mapping.target_unit and mapping.target_unit != flow_key.canonical_unit:
        raise ProviderBackgroundLeafError(
            "MAPPED_BACKGROUND_ELEMENTARY_UNIT_MISMATCH",
            "The licensed elementary inventory unit does not match the mapping target unit.",
            ecoinvent_flow_uuid=flow_key.flow_uuid,
            vector_unit=flow_key.canonical_unit,
            mapping_target_unit=mapping.target_unit,
        )
    standard = resolve_standard_flow(
        ExactFlowRef(
            source_namespace=TIANGONG_OPEN_NAMESPACE,
            flow_uuid=mapping.tiangong_flow_uuid,
            version=TIANGONG_ELEMENTARY_FLOW_VERSION,
        )
    )
    if standard is None:
        raise ProviderBackgroundLeafError(
            "MAPPED_BACKGROUND_ELEMENTARY_STANDARD_NOT_FOUND",
            "The mapped TianGong elementary Flow is absent from the exact EF3.1 catalog.",
            tiangong_flow_uuid=mapping.tiangong_flow_uuid,
        )
    if mapping.source_unit and mapping.source_unit != standard["unit"]:
        raise ProviderBackgroundLeafError(
            "MAPPED_BACKGROUND_ELEMENTARY_UNIT_MISMATCH",
            "The mapping source unit does not match the exact TianGong elementary Flow.",
            tiangong_flow_uuid=mapping.tiangong_flow_uuid,
            mapping_source_unit=mapping.source_unit,
            standard_unit=standard["unit"],
        )
    public = get_public_flow_mapping_registry()
    dependencies = [
        ProviderMappingDependencyReceipt(
            package_id=public.package_id,
            package_version=public.package_version,
            package_hash=public.package_hash,
            role="public_interoperability_projection",
        )
    ]
    if certified_adjudication:
        certified = get_elementary_flow_mapping_registry()
        dependencies.append(
            ProviderMappingDependencyReceipt(
                package_id=certified.package_id,
                package_version=certified.package_version,
                package_hash=certified.package_hash,
                role="certified_elementary_identity_adjudication",
            )
        )
    return standard, amount / mapping.amount_factor, tuple(dependencies)


def _native_elementary(
    flow_key: LciBiosphereFlowKey,
    *,
    database_release: str,
    allowed_flow_sources: tuple[str, ...],
    source_flows: dict[str, dict[str, str]],
    unit_bindings: dict[str, dict[str, str]],
) -> dict[str, Any]:
    original = source_flows.get(flow_key.flow_uuid)
    if original is None:
        raise ProviderBackgroundLeafError(
            "NATIVE_CLIMATE_FLOW_IDENTITY_MISSING",
            "A licensed vector Flow is absent from the pinned original ecoinvent MasterData.",
            flow_uuid=flow_key.flow_uuid,
        )
    if (
        original["unit"] != flow_key.canonical_unit
        or original["compartment"] != str(flow_key.compartment or "")
        or flow_key.source not in allowed_flow_sources
        or flow_key.source_package_version not in allowed_flow_sources
        or flow_key.direction not in {"input", "output"}
    ):
        raise ProviderBackgroundLeafError(
            "NATIVE_CLIMATE_VECTOR_IDENTITY_MISMATCH",
            "A licensed vector row does not match its pinned original ecoinvent Flow identity, release, direction, and unit.",
            flow_uuid=flow_key.flow_uuid,
            database_release=database_release,
        )
    binding = unit_bindings.get(original["unit_id"])
    if binding is None:
        raise ProviderBackgroundLeafError(
            "NATIVE_CLIMATE_UNIT_BINDING_MISSING",
            "The pinned native unit has no reviewed reference FlowProperty and UnitGroup binding.",
            flow_uuid=flow_key.flow_uuid,
            source_unit_id=original["unit_id"],
        )
    return {
        "source_namespace": LICENSED_NAMESPACE,
        "flow_uuid": flow_key.flow_uuid,
        "version": database_release,
        "flow_property_uuid": binding["flow_property_uuid"],
        "flow_property_version": binding["flow_property_version"],
        "unit_group_uuid": binding["unit_group_uuid"],
        "unit_group_version": binding["unit_group_version"],
        "name": original["name"],
        "unit": original["unit"],
        "unit_group": binding["unit_group"],
        "direction": flow_key.direction,
        "compartment": original["compartment"],
        "subcompartment": original["subcompartment"],
    }


def expand_mapped_background_process_pins(
    db: Session,
    *,
    graph: HybridGraph,
    pins: list[ProviderMappedBackgroundProcessPin],
    flow_snapshot: TidasFlowSnapshot | None,
    native_elementary: bool = False,
    native_allowed_flow_sources: tuple[str, ...] = (),
    native_source_flows: dict[str, dict[str, str]] | None = None,
    native_unit_bindings: dict[str, dict[str, str]] | None = None,
) -> MappedBackgroundExpansion:
    if not pins:
        return MappedBackgroundExpansion(graph, (), (), None, (), ())

    registry = get_public_flow_mapping_registry()
    expanded = graph.model_copy(deep=True)
    ports = _port_lookup(graph)
    process_uuid_by_node_id = {node.id: node.process_uuid for node in graph.nodes}
    connected = _connected_exchange_ids(graph)
    existing_processes = {node.process_uuid for node in graph.nodes}
    provider_instances: dict[str, tuple[str, FlowPort]] = {}
    provider_mapping_dependencies: dict[str, tuple[ProviderMappingDependencyReceipt, ...]] = {}
    elementary_refs: list[ProviderElementaryFlowRef] = []
    drafts: list[MappedBackgroundReceiptDraft] = []
    releases: set[str] = set()
    ordered = sorted(pins, key=lambda item: item.consumer_exchange_id)
    consumer_ids = [item.consumer_exchange_id for item in ordered]
    if len(consumer_ids) != len(set(consumer_ids)):
        raise ProviderBackgroundLeafError(
            "MAPPED_BACKGROUND_PROCESS_PIN_DUPLICATE",
            "A consumer boundary exchange may have only one mapped background Process pin.",
        )

    for pin in ordered:
        entry = ports.get(pin.consumer_exchange_id)
        if entry is None:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_CONSUMER_EXCHANGE_NOT_FOUND",
                "The mapped consumer exchange is not present in the graph.",
                consumer_exchange_id=pin.consumer_exchange_id,
            )
        consumer, consumer_node_id = entry
        if consumer.type != "technosphere" or consumer.direction != "input" or float(consumer.amount) <= 0:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_CONSUMER_EXCHANGE_INVALID",
                "A mapped background pin must target a positive boundary technosphere input.",
                consumer_exchange_id=pin.consumer_exchange_id,
            )
        if pin.consumer_exchange_id in connected:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_CONSUMER_ALREADY_CONNECTED",
                "A mapped background pin cannot target an already connected consumer port.",
                consumer_exchange_id=pin.consumer_exchange_id,
            )

        source = _exact_source_flow(flow_snapshot, pin)
        expected_source = {
            "source_namespace": pin.source_flow_namespace,
            "flow_uuid": pin.source_flow_uuid,
            "version": pin.source_flow_version,
            "unit": pin.source_unit,
        }
        graph_source = {
            "source_namespace": consumer.flow_source_namespace,
            "flow_uuid": consumer.flowUuid,
            "version": consumer.flow_version,
            "unit": consumer.unit,
        }
        snapshot_source = {
            "source_namespace": source.get("source_namespace"),
            "flow_uuid": source.get("flow_uuid"),
            "version": source.get("version"),
            "unit": source.get("unit") or source.get("default_unit"),
        }
        if graph_source != expected_source or snapshot_source != expected_source:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_SOURCE_IDENTITY_MISMATCH",
                "The consumer port, pin, and exact source Flow snapshot do not agree.",
                consumer=graph_source,
                pin=expected_source,
                snapshot=snapshot_source,
            )

        mapping = registry.resolve_intermediate(pin.source_flow_uuid)
        expected_rule_id = f"nebula-flow-mapping-v1:intermediate:{pin.source_flow_uuid}"
        if mapping is None:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_MAPPING_NOT_FOUND",
                "The exact TianGong source Flow has no accepted intermediate mapping.",
                source_flow_uuid=pin.source_flow_uuid,
            )
        mapping_evidence = (
            pin.mapping_package_id == registry.package_id
            and pin.mapping_package_version == registry.package_version
            and pin.mapping_package_hash == registry.package_hash
            and pin.mapping_rule_id == expected_rule_id
            and pin.target_flow_uuid == mapping.ecoinvent_flow_uuid
            and pin.mapping_level == mapping.mapping_level
            and abs(pin.amount_factor - mapping.amount_factor) <= 1e-12
        )
        if not mapping_evidence:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_MAPPING_EVIDENCE_MISMATCH",
                "The mapped background pin does not match the accepted mapping package.",
                source_flow_uuid=pin.source_flow_uuid,
                active_package_version=registry.package_version,
                active_package_hash=registry.package_hash,
            )
        if pin.mapping_level == "L2" and pin.mapping_status != "user_confirmed":
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_L2_CONFIRMATION_REQUIRED",
                "An L2 compatibility mapping must be explicitly confirmed.",
                mapping_rule_id=pin.mapping_rule_id,
            )
        expected_source_unit = mapping.source_unit or pin.source_unit
        if expected_source_unit != pin.source_unit:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_SOURCE_UNIT_MISMATCH",
                "The pin source unit does not match the mapping conversion rule.",
                expected=expected_source_unit,
                actual=pin.source_unit,
            )

        target = db.get(FlowRecord, pin.target_flow_uuid)
        if target is None or "ecoinvent" not in str(target.source or "").casefold():
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_TARGET_FLOW_NOT_FOUND",
                "The mapped ecoinvent target Flow is absent from the licensed local catalog.",
                target_flow_uuid=pin.target_flow_uuid,
            )
        expected_target_unit = mapping.target_unit or str(target.default_unit or "")
        if pin.target_unit != expected_target_unit or pin.target_unit != target.default_unit:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_TARGET_UNIT_MISMATCH",
                "The pin, mapping, and licensed target Flow unit do not agree.",
                pin_unit=pin.target_unit,
                mapping_unit=expected_target_unit,
                catalog_unit=target.default_unit,
            )

        process = db.get(ReferenceProcess, pin.process_uuid)
        vector_row = db.get(LciProcessVector, pin.process_uuid)
        if process is None or process.process_type != "lci_dataset" or vector_row is None or vector_row.nnz <= 0:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_PROCESS_NOT_EXECUTABLE",
                "The selected licensed Process has no executable local LCI vector.",
                process_uuid=pin.process_uuid,
            )
        if process.reference_flow_uuid != pin.target_flow_uuid:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_PROCESS_REFERENCE_FLOW_MISMATCH",
                "The selected Process quantitative-reference Flow does not match the mapped target Flow.",
                expected=pin.target_flow_uuid,
                actual=process.reference_flow_uuid,
            )
        if vector_row.source_package_version != pin.database_release:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_DATABASE_RELEASE_MISMATCH",
                "The selected Process vector does not belong to the pinned database release.",
                expected=pin.database_release,
                actual=vector_row.source_package_version,
            )
        if not vector_row.checksum or vector_row.checksum != pin.expected_vector_checksum:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_VECTOR_CHECKSUM_MISMATCH",
                "The selected Process vector checksum does not match the pin.",
                expected=pin.expected_vector_checksum,
                actual=vector_row.checksum,
            )
        content_hash = mapped_process_content_hash(process, vector_row)
        if content_hash != pin.expected_process_content_hash:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_PROCESS_CONTENT_HASH_MISMATCH",
                "The selected licensed Process content does not match the pin.",
                expected=pin.expected_process_content_hash,
                actual=content_hash,
            )
        provider_instance = provider_instances.get(pin.process_uuid)
        if pin.process_uuid in existing_processes and provider_instance is None:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_PROCESS_UUID_COLLISION",
                "The selected mapped background Process already exists in the graph.",
                process_uuid=pin.process_uuid,
            )

        process_json = process.process_json if isinstance(process.process_json, dict) else {}
        qref_amount = float(process_json.get("reference_product_amount") or 1.0)
        qref_unit = str(process_json.get("reference_product_unit") or target.default_unit or "")
        if qref_amount <= 0 or qref_unit != pin.target_unit:
            raise ProviderBackgroundLeafError(
                "MAPPED_BACKGROUND_PROCESS_REFERENCE_UNIT_MISMATCH",
                "The selected Process reference amount/unit does not match the mapped target unit.",
                reference_amount=qref_amount,
                reference_unit=qref_unit,
                target_unit=pin.target_unit,
            )

        token = _canonical_hash(pin.model_dump(mode="json"))[:16]
        if provider_instance is None:
            vector = load_process_vectors(db, [pin.process_uuid]).get(pin.process_uuid)
            if vector is None or len(vector) != vector_row.nnz:
                raise ProviderBackgroundLeafError(
                    "MAPPED_BACKGROUND_VECTOR_UNREADABLE",
                    "The selected licensed Process vector cannot be decoded exactly.",
                    process_uuid=pin.process_uuid,
                )
            key_rows = {
                int(row.flow_key_id): row
                for row in db.query(LciBiosphereFlowKey)
                .filter(LciBiosphereFlowKey.flow_key_id.in_(sorted(vector)))
                .all()
            }
            if len(key_rows) != len(vector):
                raise ProviderBackgroundLeafError(
                    "MAPPED_BACKGROUND_VECTOR_FLOW_KEY_MISSING",
                    "The selected licensed Process vector references missing local Flow keys.",
                    process_uuid=pin.process_uuid,
                )

            instance_token = _canonical_hash(
                {
                    "process_uuid": pin.process_uuid,
                    "database_release": pin.database_release,
                    "process_content_hash": content_hash,
                    "vector_checksum": pin.expected_vector_checksum,
                }
            )[:16]
            node_id = f"provider-mapped-background-{instance_token}"
            if native_elementary and pin.target_unit not in {
                item.get("unit") for item in source.get("units") or []
            }:
                raise ProviderBackgroundLeafError(
                    "NATIVE_CLIMATE_REFERENCE_UNIT_BINDING_MISSING",
                    "The exact source Flow reference UnitGroup does not contain the mapped provider reference unit.",
                    source_flow_uuid=pin.source_flow_uuid,
                    target_unit=pin.target_unit,
                )
            qref_port = FlowPort(
                id="reference-product",
                flowUuid=pin.target_flow_uuid,
                flowSourceNamespace=LICENSED_NAMESPACE,
                flowVersion=pin.database_release,
                flowPropertyUuid=(source["flow_property_uuid"] if native_elementary else None),
                flowPropertyVersion=(source["flow_property_version"] if native_elementary else None),
                unitGroupUuid=(source["unit_group_uuid"] if native_elementary else None),
                unitGroupVersion=(source["unit_group_version"] if native_elementary else None),
                name=target.flow_name,
                unit=pin.target_unit,
                unitGroup=(source["unit_group"] if native_elementary else target.unit_group),
                amount=qref_amount,
                type="technosphere",
                direction="output",
                isProduct=True,
                sourceSystem=f"ecoinvent_{pin.database_release}",
            )
            inputs: list[FlowPort] = []
            outputs: list[FlowPort] = [qref_port]
            dependency_index: dict[
                tuple[str, str, str, str],
                ProviderMappingDependencyReceipt,
            ] = {}
            for flow_key_id, raw_amount in sorted(vector.items()):
                flow_key = key_rows[flow_key_id]
                if native_elementary:
                    standard = _native_elementary(
                        flow_key,
                        database_release=pin.database_release,
                        allowed_flow_sources=native_allowed_flow_sources,
                        source_flows=native_source_flows or {},
                        unit_bindings=native_unit_bindings or {},
                    )
                    standard_amount = float(raw_amount)
                    mapping_dependencies = ()
                else:
                    standard, standard_amount, mapping_dependencies = _standardized_elementary(
                        flow_key,
                        float(raw_amount),
                    )
                for dependency in mapping_dependencies:
                    dependency_index[
                        (
                            dependency.package_id,
                            dependency.package_version,
                            dependency.package_hash,
                            dependency.role,
                        )
                    ] = dependency
                if standard["direction"] != ("input" if flow_key.direction == "input" else "output"):
                    raise ProviderBackgroundLeafError(
                        "MAPPED_BACKGROUND_ELEMENTARY_DIRECTION_MISMATCH",
                        "The licensed vector direction does not match the exact standard elementary Flow.",
                        flow_uuid=flow_key.flow_uuid,
                        vector_direction=flow_key.direction,
                        standard_direction=standard["direction"],
                    )
                port_id = f"lci-vector-{flow_key_id}"
                exchange_id = f"{node_id}::{port_id}"
                port = FlowPort(
                    id=port_id,
                    flowUuid=standard["flow_uuid"],
                    flowSourceNamespace=standard["source_namespace"],
                    flowVersion=standard["version"],
                    flowPropertyUuid=standard["flow_property_uuid"],
                    flowPropertyVersion=standard["flow_property_version"],
                    unitGroupUuid=standard["unit_group_uuid"],
                    unitGroupVersion=standard["unit_group_version"],
                    name=standard.get("name") or standard.get("name_en") or standard["flow_uuid"],
                    unit=standard["unit"],
                    unitGroup=standard.get("unit_group"),
                    amount=standard_amount,
                    type="biosphere",
                    direction=standard["direction"],
                    showOnNode=False,
                    sourceSystem=standard["source_namespace"],
                )
                (inputs if port.direction == "input" else outputs).append(port)
                if not native_elementary:
                    elementary_refs.append(
                        ProviderElementaryFlowRef(
                            exchange_id=exchange_id,
                            source_namespace=standard["source_namespace"],
                            flow_uuid=standard["flow_uuid"],
                            version=standard["version"],
                            flow_property_uuid=standard["flow_property_uuid"],
                            flow_property_version=standard["flow_property_version"],
                            unit_group_uuid=standard["unit_group_uuid"],
                            unit_group_version=standard["unit_group_version"],
                            unit=standard["unit"],
                            direction=standard["direction"],
                            compartment=standard["compartment"],
                        )
                    )

            expanded.nodes.append(
                HybridNode(
                    id=node_id,
                    hidden=True,
                    node_kind="lci_dataset",
                    mode="normalized",
                    lci_role="provider",
                    process_uuid=pin.process_uuid,
                    name=process.process_name,
                    location=str(process_json.get("location") or ""),
                    source_system=f"ecoinvent_{pin.database_release}",
                    reference_product=target.flow_name,
                    reference_product_flow_uuid=pin.target_flow_uuid,
                    reference_product_direction="output",
                    inputs=inputs,
                    outputs=outputs,
                )
            )
            provider_instances[pin.process_uuid] = (node_id, qref_port)
            provider_mapping_dependencies[pin.process_uuid] = tuple(
                dependency_index[key] for key in sorted(dependency_index)
            )
            existing_processes.add(pin.process_uuid)
        else:
            node_id, qref_port = provider_instance

        converted_amount = float(consumer.amount) * pin.amount_factor
        expanded.exchanges.append(
            HybridEdge(
                id=f"mapped-background-pin-{token}",
                fromNode=node_id,
                toNode=consumer_node_id,
                sourcePortId=qref_port.id,
                targetPortId=consumer.id,
                sourceHandle=f"out:{qref_port.id}",
                targetHandle=f"in:{consumer.id}",
                flowUuid=pin.target_flow_uuid,
                flowName=target.flow_name,
                quantityMode="dual",
                amount=converted_amount,
                providerAmount=converted_amount,
                consumerAmount=converted_amount,
                unit=pin.target_unit,
                providerUnit=pin.target_unit,
                consumerUnit=pin.source_unit,
                consumerFlowUuid=pin.source_flow_uuid,
                intermediateFlowLinkRuleId=pin.mapping_rule_id,
                intermediateFlowLinkFactor=pin.amount_factor,
                type="technosphere",
            )
        )
        drafts.append(
            MappedBackgroundReceiptDraft(
                pin=pin,
                process_content_hash=content_hash,
                vector_nnz=vector_row.nnz,
                quantitative_reference_amount=qref_amount,
                provider_instance_id=node_id,
                consumer_process_uuid=process_uuid_by_node_id[consumer_node_id],
                consumer_converted_amount=converted_amount,
                elementary_mapping_dependencies=provider_mapping_dependencies.get(pin.process_uuid, ()),
            )
        )
        releases.add(pin.database_release)

    return MappedBackgroundExpansion(
        graph=expanded,
        elementary_refs=tuple(elementary_refs),
        receipt_drafts=tuple(drafts),
        pins_hash=_canonical_hash([pin.model_dump(mode="json") for pin in ordered]),
        database_releases=tuple(sorted(releases)),
        elementary_mapping_dependencies=tuple(
            dependency
            for key, dependency in sorted(
                {
                    (
                        item.package_id,
                        item.package_version,
                        item.package_hash,
                        item.role,
                    ): item
                    for dependencies in provider_mapping_dependencies.values()
                    for item in dependencies
                }.items()
            )
        ),
    )


def build_mapped_background_process_receipts(
    *,
    drafts: tuple[MappedBackgroundReceiptDraft, ...],
    activity_by_process: dict[str, float],
    allocation_total_by_process: dict[str, float],
) -> list[ProviderMappedBackgroundProcessReceipt]:
    receipts: list[ProviderMappedBackgroundProcessReceipt] = []
    for draft in drafts:
        pin = draft.pin
        shared_activity = activity_by_process[pin.process_uuid]
        consumer_allocation_total = allocation_total_by_process[draft.consumer_process_uuid]
        attributed_activity = (
            activity_by_process[draft.consumer_process_uuid]
            * draft.consumer_converted_amount
            / consumer_allocation_total
        )
        receipts.append(
            ProviderMappedBackgroundProcessReceipt(
                **pin.model_dump(mode="json", exclude={"schema_version", "expected_process_content_hash", "expected_vector_checksum"}),
                process_content_hash=draft.process_content_hash,
                vector_checksum=pin.expected_vector_checksum,
                vector_nnz=draft.vector_nnz,
                elementary_mapping_dependencies=list(draft.elementary_mapping_dependencies),
                quantitative_reference_amount=draft.quantitative_reference_amount,
                activity_amount=shared_activity,
                process_scale=shared_activity / draft.quantitative_reference_amount,
                shared_activity_schema_version="provider.mapped-background-shared-activity.v1",
                provider_instance_id=draft.provider_instance_id,
                consumer_process_uuid=draft.consumer_process_uuid,
                consumer_converted_amount=draft.consumer_converted_amount,
                consumer_attributed_activity_amount=attributed_activity,
                shared_process_total_activity_amount=shared_activity,
                attributed_process_scale=attributed_activity / draft.quantitative_reference_amount,
                claim_limit=CLAIM_LIMIT,
            )
        )
    return receipts
