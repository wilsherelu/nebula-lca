"""Pinned, opt-in native ecoinvent climate-total characterization."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..ecoinvent_ef31_loader import parse_elementary_exchanges, parse_units
from ..provider_schemas import (
    ProviderElementaryFlowRef,
    ProviderNativeBackgroundClimateRequest,
    ProviderScaledExchange,
)
from .provider_contract import ProviderContractError
from .provider_tidas_snapshot import TidasFlowSnapshot


API_ROOT = Path(__file__).resolve().parents[2]
METHOD_PACKAGE_PATH = API_ROOT / "data/methods/ecoinvent_ef31_climate_total_311.v1.json"
REFERENCE_SEED_PATH = API_ROOT / "data/Tiangong/tidas_reference_seed.json"
MANIFEST_ENV = "NEBULA_PROVIDER_NATIVE_CLIMATE_MANIFEST_PATH"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProviderContractError(422, "NATIVE_CLIMATE_RUNTIME_UNAVAILABLE", "A pinned native climate asset cannot be read.", asset=str(path)) from exc
    if not isinstance(value, dict):
        raise ProviderContractError(422, "NATIVE_CLIMATE_RUNTIME_UNAVAILABLE", "A pinned native climate asset is not a JSON object.", asset=str(path))
    return value


def _check_hash(path: Path, expected: str) -> None:
    try:
        actual = _sha256(path)
    except OSError as exc:
        raise ProviderContractError(422, "NATIVE_CLIMATE_RUNTIME_UNAVAILABLE", "A pinned native climate asset is missing.", asset=str(path)) from exc
    if actual != expected:
        raise ProviderContractError(
            422, "NATIVE_CLIMATE_RUNTIME_HASH_MISMATCH", "A pinned native climate asset changed.",
            asset=str(path), expected_sha256=expected, actual_sha256=actual,
        )


def _csv_rows(path: Path, delimiter: str) -> list[dict[str, str]]:
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            return list(csv.DictReader(stream, delimiter=delimiter))
    except (OSError, csv.Error) as exc:
        raise ProviderContractError(422, "NATIVE_CLIMATE_RUNTIME_UNAVAILABLE", "A pinned climate CSV cannot be read.", asset=str(path)) from exc


def _flow_columns(path: Path) -> dict[str, int]:
    rows = _csv_rows(path, ";")
    uuids = [row["FlowUUID"] for row in rows]
    columns = [int(row["flow_index"]) for row in rows]
    if len(set(uuids)) != len(uuids) or len(set(columns)) != len(columns):
        raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE", "A pinned climate Flow index contains duplicate UUIDs or columns.", asset=str(path))
    return dict(zip(uuids, columns, strict=True))


def _indicator(path: Path, *, delimiter: str, expected_index: int, method: str, category: str | None) -> None:
    matches = [row for row in _csv_rows(path, delimiter) if int(row["indicator_index"]) == expected_index]
    if len(matches) != 1 or matches[0].get("method_en") != method or (category is not None and matches[0].get("ecoinvent_category") != category):
        raise ProviderContractError(422, "NATIVE_CLIMATE_UNSUPPORTED_SCOPE", "The pinned climate indicator identity does not match the method package.", asset=str(path), indicator_index=expected_index)


def _factor_columns(path: Path, indicator_index: int) -> dict[int, float]:
    values: dict[int, list[float]] = defaultdict(list)
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream, delimiter=";"):
            if int(row["row"]) == indicator_index:
                value = float(row["coefficient"])
                if not math.isfinite(value):
                    raise ProviderContractError(422, "NATIVE_CLIMATE_CF_DUPLICATE", "A climate CF is non-finite.", asset=str(path))
                values[int(row["column"])].append(value)
    duplicate = [column for column, factors in values.items() if len(factors) != 1]
    if duplicate:
        raise ProviderContractError(422, "NATIVE_CLIMATE_CF_DUPLICATE", "A climate Flow column has multiple CF rows.", asset=str(path), column_count=len(duplicate))
    return {column: factors[0] for column, factors in values.items()}


def build_native_source_flow_index(parsed_flows: list[Any], runtime_columns: dict[str, int]) -> dict[str, dict[str, str]]:
    source_flows = {
        item.flow_uuid: {
            "name": item.flow_name,
            "unit": item.default_unit,
            "unit_id": item.unit_id,
            "compartment": str(item.compartment or ""),
            "subcompartment": str(item.subcompartment or ""),
        }
        for item in parsed_flows
    }
    if len(source_flows) != len(parsed_flows):
        raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE", "The pinned original elementary Flow identities contain duplicate UUIDs.")
    if set(source_flows) != set(runtime_columns):
        raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_MISSING", "The pinned original elementary identities do not match the generated runtime Flow index.")
    return source_flows


def _native_unit_bindings(
    path: Path, *, expected_hash: str, source_units: dict[str, Any], database_release: str,
) -> dict[str, dict[str, str]]:
    _check_hash(path, expected_hash)
    package = _read_json(path)
    _check_hash(REFERENCE_SEED_PATH, str(package.get("reference_seed_sha256") or ""))
    reference = _read_json(REFERENCE_SEED_PATH)
    groups = {row["name"]: row for row in reference["unit_groups"]}
    properties = {row["source_unit_group"]: row for row in reference["unit_group_mappings"]}
    if (
        package.get("schema_version") != "provider.native-unit-reference-bindings.v1"
        or package.get("database_release") != database_release
        or package.get("source_units_sha256") != source_units["sha256"]
        or package.get("claim") != "dimension_reference_only_no_unit_conversion_or_flow_identity_mapping"
        or not isinstance(package.get("bindings"), list)
    ):
        raise ProviderContractError(422, "NATIVE_CLIMATE_UNIT_BINDING_INVALID", "The pinned native unit binding package has an unsupported identity or claim.")
    result: dict[str, dict[str, str]] = {}
    for row in package["bindings"]:
        if not isinstance(row, dict):
            raise ProviderContractError(422, "NATIVE_CLIMATE_UNIT_BINDING_INVALID", "A native unit binding row is invalid.")
        unit_id = str(row.get("source_unit_id") or "")
        unit = source_units["records"].get(unit_id)
        group = groups.get(row.get("reference_unit_group"))
        property_mapping = properties.get(row.get("reference_unit_group"))
        level = row.get("binding_level")
        if (
            not unit_id or unit_id in result or unit is None
            or unit.name != row.get("source_unit")
            or group is None or property_mapping is None
            or level not in {"exact_unit_member", "dimension_reference_only"}
        ):
            raise ProviderContractError(422, "NATIVE_CLIMATE_UNIT_BINDING_INVALID", "A native unit binding does not match the pinned source unit and reference group.", source_unit_id=unit_id)
        members = {item.get("name") for item in group.get("units") or []}
        if (unit.name in members) != (level == "exact_unit_member"):
            raise ProviderContractError(422, "NATIVE_CLIMATE_UNIT_BINDING_INVALID", "The native unit binding level disagrees with the reference unit group.", source_unit_id=unit_id)
        flow_property = next((item for item in reference["flow_properties"] if item.get("flow_property_uuid") == property_mapping["flow_property_uuid"] and item.get("version") == property_mapping["version"]), None)
        if flow_property is None:
            raise ProviderContractError(422, "NATIVE_CLIMATE_UNIT_BINDING_INVALID", "The reference FlowProperty is absent from the pinned reference catalog.", source_unit_id=unit_id)
        result[unit_id] = {
            "flow_property_uuid": property_mapping["flow_property_uuid"],
            "flow_property_version": property_mapping["version"],
            "unit_group_uuid": group["uuid"],
            "unit_group_version": group["version"],
            "unit_group": group["name"],
            "binding_level": level,
        }
    return result


@dataclass(frozen=True)
class NativeClimateRuntime:
    package: dict[str, Any]
    package_sha256: str
    manifest_sha256: str
    native_columns: dict[str, int]
    native_source_flows: dict[str, dict[str, str]]
    unit_bindings: dict[str, dict[str, str]]
    unit_binding_sha256: str
    native_factors: dict[int, float]
    foreground_columns: dict[str, int]
    foreground_factors: dict[int, float]
    certified_by_foreground: dict[str, dict[str, Any]]

    @property
    def allowed_flow_sources(self) -> tuple[str, ...]:
        return tuple(self.package["vector_flow_source_identities"])


def load_native_climate_runtime(
    pin: ProviderNativeBackgroundClimateRequest,
    *,
    flow_snapshot: TidasFlowSnapshot | None,
) -> NativeClimateRuntime:
    configured = str(os.getenv(MANIFEST_ENV) or "").strip()
    if not configured:
        raise ProviderContractError(422, "NATIVE_CLIMATE_RUNTIME_UNAVAILABLE", "The exact local native climate runtime manifest is not configured.")
    manifest_path = Path(configured)
    _check_hash(manifest_path, pin.runtime_manifest_sha256)
    manifest = _read_json(manifest_path)
    _check_hash(METHOD_PACKAGE_PATH, pin.method_package_sha256)
    package = _read_json(METHOD_PACKAGE_PATH)
    if (
        manifest.get("schema_version") != "provider.native-climate-runtime-manifest.v1"
        or Path(str(manifest.get("method_package_path") or "")).resolve() != METHOD_PACKAGE_PATH.resolve()
        or manifest.get("method_package_sha256") != pin.method_package_sha256
        or package.get("schema_version") != "provider.native-climate-method-package.v1"
        or package.get("package_id") != pin.method_package_id
        or package.get("package_version") != pin.method_package_version
        or package.get("database_release") != pin.database_release
        or package.get("method") != "EF v3.1"
        or package.get("category") != "climate change"
        or package.get("indicator") != "global warming potential (GWP100)"
        or package.get("indicator_unit") != "kg CO2-Eq"
    ):
        raise ProviderContractError(422, "NATIVE_CLIMATE_METHOD_PIN_MISMATCH", "The method package, local manifest, and request do not identify one exact climate-total implementation.")
    if (
        flow_snapshot is None
        or flow_snapshot.snapshot_hash != pin.flow_snapshot_hash
        or manifest.get("flow_snapshot_hash") != pin.flow_snapshot_hash
        or flow_snapshot.reference_dependencies is None
        or flow_snapshot.reference_dependencies.snapshot_hash != pin.reference_dependency_snapshot_hash
        or manifest.get("reference_dependency_snapshot_hash") != pin.reference_dependency_snapshot_hash
    ):
        raise ProviderContractError(422, "NATIVE_CLIMATE_METHOD_PIN_MISMATCH", "The exact TianGong Flow/reference snapshots differ from the native request.")
    electricity = flow_snapshot.resolve(str(manifest.get("electricity_flow_uuid")), str(manifest.get("electricity_flow_version")))
    if electricity is None or electricity.get("content_hash") != manifest.get("electricity_flow_payload_content_hash"):
        raise ProviderContractError(422, "NATIVE_CLIMATE_METHOD_PIN_MISMATCH", "The exact electricity Flow payload differs from the local method manifest.")

    native_dir = Path(manifest["native_runtime_dir"])
    foreground_dir = Path(manifest["foreground_runtime_dir"])
    source_dir = Path(manifest["source_asset_dir"])
    for filename, expected in package["source_asset_sha256"].items():
        _check_hash(source_dir / filename, expected)
    for filename, expected in package["native_runtime_sha256"].items():
        _check_hash(native_dir / filename, expected)
    for filename, expected in package["foreground_runtime_sha256"].items():
        _check_hash(foreground_dir / filename, expected)
    _check_hash(Path(manifest["audit_v2_script_path"]), package["audit_v2_script_sha256"])
    _check_hash(Path(manifest["audit_v2_evidence_path"]), package["audit_v2_evidence_sha256"])
    audit = _read_json(Path(manifest["audit_v2_evidence_path"]))
    if audit.get("schema_version") != "local.native-climate-total-audit.v2" or audit.get("passed") is not True:
        raise ProviderContractError(422, "NATIVE_CLIMATE_RUNTIME_UNAVAILABLE", "The pinned native climate audit did not pass.")

    native_index = int(package["native_indicator_index"])
    foreground_index = int(package["foreground_indicator_index"])
    _indicator(native_dir / "indicator_index.csv", delimiter=";", expected_index=native_index, method="EF v3.1", category="climate change")
    _indicator(foreground_dir / "indicator_index.csv", delimiter=",", expected_index=foreground_index, method="Climate change", category=None)
    native_columns = _flow_columns(native_dir / "flow_index.csv")
    source_units = parse_units(source_dir)
    unit_bindings = _native_unit_bindings(
        Path(str(manifest.get("unit_binding_path") or "")),
        expected_hash=str(manifest.get("unit_binding_sha256") or ""),
        source_units={"sha256": package["source_asset_sha256"]["Units.xml"], "records": source_units},
        database_release=pin.database_release,
    )
    parsed_flows = parse_elementary_exchanges(source_dir, source_units)
    native_source_flows = build_native_source_flow_index(parsed_flows, native_columns)
    # The package may cover only the selected Process vector. Every used row is
    # checked again during expansion so a new Process cannot silently bypass it.
    foreground_columns = _flow_columns(foreground_dir / "flow_index.csv")
    native_factors = _factor_columns(native_dir / "lcia_factors.csv", native_index)
    foreground_factors = _factor_columns(foreground_dir / "lcia_factors.csv", foreground_index)
    mapping_path = API_ROOT / "data/flow_mappings/ghg_ef31_v1.json"
    _check_hash(mapping_path, package["certified_core_sha256"])
    mappings = _read_json(mapping_path)["mappings"]
    certified = {row["ef_flow_uuid"]: row for row in mappings}
    if len(certified) != len(mappings):
        raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE", "The certified foreground identity set is not unique.")
    return NativeClimateRuntime(
        package=package, package_sha256=pin.method_package_sha256,
        manifest_sha256=pin.runtime_manifest_sha256,
        native_columns=native_columns, native_source_flows=native_source_flows,
        unit_bindings=unit_bindings, unit_binding_sha256=str(manifest["unit_binding_sha256"]),
        native_factors=native_factors,
        foreground_columns=foreground_columns, foreground_factors=foreground_factors,
        certified_by_foreground=certified,
    )


def _factor(flow_uuid: str, columns: dict[str, int], factors: dict[int, float]) -> tuple[str, float | None]:
    column = columns.get(flow_uuid)
    if column is None:
        raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_MISSING", "A solved elementary Flow is absent from the pinned method Flow index.", flow_uuid=flow_uuid)
    if column not in factors:
        return "no_cf", None
    value = factors[column]
    return ("explicit_zero" if value == 0.0 else "explicit_nonzero"), value


def characterize_native_climate(
    *,
    runtime: NativeClimateRuntime,
    scaled_exchanges: list[ProviderScaledExchange],
    foreground_refs: list[ProviderElementaryFlowRef],
    flow_snapshot: TidasFlowSnapshot,
    native_process_uuids: set[str],
) -> dict[str, Any]:
    refs = {ref.exchange_id: ref for ref in foreground_refs}
    if len(refs) != len(foreground_refs):
        raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE", "A foreground exchange has duplicate exact Flow references.")
    terms: dict[str, list[dict[str, Any]]] = {"foreground": [], "native_background": []}
    coverage = {domain: Counter() for domain in terms}
    negative_cf_count = Counter()
    totals = {domain: 0.0 for domain in terms}
    blockers: list[dict[str, Any]] = []
    seen_exchanges: set[str] = set()
    for exchange in scaled_exchanges:
        if exchange.exchange_type != "elementary" or exchange.boundary_role != "boundary":
            continue
        if exchange.exchange_id in seen_exchanges:
            raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE", "A solved elementary exchange appears more than once.", exchange_id=exchange.exchange_id)
        seen_exchanges.add(exchange.exchange_id)
        native = exchange.process_uuid in native_process_uuids
        domain = "native_background" if native else "foreground"
        if native:
            original_flow = runtime.native_source_flows.get(exchange.flow_uuid)
            if original_flow is None:
                raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_MISSING", "A solved native exchange is absent from the pinned original MasterData.", exchange_id=exchange.exchange_id)
            if (
                exchange.flow_source_namespace != "ecoinvent"
                or exchange.flow_version != runtime.package["database_release"]
                or exchange.unit != original_flow["unit"]
            ):
                raise ProviderContractError(422, "NATIVE_CLIMATE_VECTOR_IDENTITY_MISMATCH", "A solved native exchange lost its exact source/release/unit.", exchange_id=exchange.exchange_id)
            status, coefficient = _factor(exchange.flow_uuid, runtime.native_columns, runtime.native_factors)
            source = "ecoinvent_3.11_native_EF_v3.1_climate_total"
        else:
            ref = refs.get(exchange.exchange_id)
            if ref is None or ref.source_namespace != "tiangong_open_data" or ref.flow_uuid != exchange.flow_uuid or ref.unit != exchange.unit or ref.direction != exchange.direction:
                raise ProviderContractError(422, "NATIVE_CLIMATE_FLOW_IDENTITY_MISSING", "A solved foreground exchange lacks a matching exact TianGong Flow reference.", exchange_id=exchange.exchange_id)
            resolved = flow_snapshot.resolve(ref.flow_uuid, ref.version, require_elementary_factor_coverage=False)
            if resolved is None or resolved.get("flow_type") != "Elementary flow" or resolved.get("unit") != ref.unit or resolved.get("direction") != ref.direction or resolved.get("compartment") != ref.compartment:
                raise ProviderContractError(422, "NATIVE_CLIMATE_VECTOR_IDENTITY_MISMATCH", "The foreground reference differs from its exact source Flow snapshot.", exchange_id=exchange.exchange_id)
            status, coefficient = _factor(exchange.flow_uuid, runtime.foreground_columns, runtime.foreground_factors)
            source = "tiangong_open_data_EF_v3.1_climate_total"
            if status == "explicit_nonzero" and exchange.scaled_amount != 0:
                mapping = runtime.certified_by_foreground.get(exchange.flow_uuid)
                reason = None
                if mapping is None:
                    reason = "foreground_contributing_flow_not_certified"
                elif mapping["unit"] != exchange.unit or mapping["direction"] != exchange.direction:
                    reason = "foreground_unit_or_direction_differs_from_certified_identity"
                else:
                    catalog_context = str(mapping["ef_context"].get("catalog_compartment") or "").split(";")[-1]
                    if catalog_context != ref.compartment or mapping["ef_context"].get("catalog_subcompartment") not in (None, ""):
                        reason = "foreground_compartment_differs_from_certified_identity"
                    else:
                        native_status, native_coefficient = _factor(
                            mapping["ecoinvent_flow_uuid"], runtime.native_columns, runtime.native_factors
                        )
                        expected_native = coefficient * float(mapping["eco_to_ef_factor"])
                        if native_status != "explicit_nonzero" or native_coefficient != expected_native:
                            reason = "foreground_native_method_coefficient_differs"
                if reason:
                    blockers.append({"exchange_id": exchange.exchange_id, "flow_uuid": exchange.flow_uuid, "reason": reason})
        coverage[domain][status] += 1
        if coefficient is not None and coefficient < 0:
            negative_cf_count[domain] += 1
        contribution = exchange.scaled_amount * coefficient if coefficient is not None else None
        if contribution is not None:
            totals[domain] += contribution
        terms[domain].append({
            "exchange_id": exchange.exchange_id,
            "process_uuid": exchange.process_uuid,
            "source_namespace": "ecoinvent" if native else "tiangong_open_data",
            "flow_uuid": exchange.flow_uuid,
            "flow_version": exchange.flow_version if native else refs[exchange.exchange_id].version,
            "direction": exchange.direction,
            "compartment": (
                original_flow["compartment"] if native else refs[exchange.exchange_id].compartment
            ),
            "subcompartment": original_flow["subcompartment"] if native else None,
            "amount": exchange.scaled_amount,
            "amount_unit": exchange.unit,
            "cf_presence": status,
            "cf_value": coefficient,
            "cf_source": source,
            "cf_method": runtime.package["method"],
            "cf_database_release": runtime.package["database_release"] if native else "EF v3.1",
            "method_package_sha256": runtime.package_sha256,
            "contribution_value": contribution,
        })
    if not terms["native_background"]:
        raise ProviderContractError(422, "NATIVE_CLIMATE_VECTOR_IDENTITY_MISMATCH", "The opt-in solve produced no native background elementary exchanges.")
    merged = not blockers
    return {
        "schema_version": "provider.native-background-climate-total.response.v1",
        "method_package_id": runtime.package["package_id"],
        "method_package_version": runtime.package["package_version"],
        "method_package_sha256": runtime.package_sha256,
        "runtime_manifest_sha256": runtime.manifest_sha256,
        "indicator": {"method": "EF v3.1", "category": "climate change", "name": "GWP100", "unit": "kg CO2-Eq"},
        "merge_status": "merged" if merged else "split_diagnostic",
        "merge_blockers": blockers,
        "unified_climate_total": sum(totals.values()) if merged else None,
        "domains": {
            domain: {
                "source_namespace": "ecoinvent" if domain == "native_background" else "tiangong_open_data",
                "contribution_value": totals[domain],
                "indicator_unit": "kg CO2-Eq",
                "coverage": dict(coverage[domain]),
                "negative_cf_exchange_count": negative_cf_count[domain],
                "terms": terms[domain],
            }
            for domain in terms
        },
        "provider_system_model_evidence": runtime.package["provider_system_model_evidence"],
        "carbon_storage_permanence": "not_assessed",
        "claim_limit": "partial_background_leaf_only_not_complete_product_carbon_footprint",
    }
