from __future__ import annotations

import csv
import hashlib
import importlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..config import PROJECT_ROOT
from ..provider_schemas import (
    ExactFlowPropertyRef,
    ExactFlowRef,
    ExactUnitGroupRef,
    ExactUnitRef,
    ProviderElementaryFlowReceipt,
    ProviderElementaryFlowRef,
    ProviderInventoryTotal,
    ProviderScaledExchange,
)
from ..solver_adapter import _ensure_embedded_solver_core, _resolve_embedded_ef31_dirs


EF31_METHOD = "EF v3.1"
EF31_DATABASE_RELEASE = "EF3.1"
TIANGONG_OPEN_NAMESPACE = "tiangong_open_data"
TIANGONG_ELEMENTARY_FLOW_VERSION = "03.00.004"

_REFERENCE_SEED = PROJECT_ROOT / "data" / "Tiangong" / "tidas_reference_seed.json"
_ELEMENTARY_CATALOG = PROJECT_ROOT / "data" / "Tiangong" / "elementary_flows_sample.csv"
_INDICATOR_METHOD_METADATA = PROJECT_ROOT / "data" / "EF3.1" / "indicator_index.csv"


class ProviderEf31Error(RuntimeError):
    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@lru_cache(maxsize=1)
def _reference_catalog() -> dict[str, Any]:
    seed = json.loads(_REFERENCE_SEED.read_text(encoding="utf-8"))
    unit_groups = {
        (str(item.get("uuid") or ""), str(item.get("version") or "")): item
        for item in seed.get("unit_groups") or []
        if item.get("uuid") and item.get("version")
    }
    unit_group_by_name = {
        str(item.get("name") or ""): item
        for item in unit_groups.values()
        if item.get("name")
    }
    flow_properties = {
        (str(item.get("flow_property_uuid") or ""), str(item.get("version") or "")): item
        for item in seed.get("flow_properties") or []
        if item.get("flow_property_uuid") and item.get("version")
    }
    flow_property_by_unit_group_name: dict[str, dict[str, Any]] = {}
    for mapping in seed.get("unit_group_mappings") or []:
        group_name = str(mapping.get("source_unit_group") or "")
        property_uuid = str(mapping.get("flow_property_uuid") or "")
        property_version = str(mapping.get("version") or "")
        property_row = flow_properties.get((property_uuid, property_version))
        if group_name and property_row:
            flow_property_by_unit_group_name[group_name] = property_row
    return {
        "unit_groups": unit_groups,
        "unit_group_by_name": unit_group_by_name,
        "flow_properties": flow_properties,
        "flow_property_by_unit_group_name": flow_property_by_unit_group_name,
    }


@lru_cache(maxsize=1)
def _elementary_catalog() -> dict[str, dict[str, Any]]:
    reference = _reference_catalog()
    result: dict[str, dict[str, Any]] = {}
    with _ELEMENTARY_CATALOG.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if len(row) < 7:
                continue
            flow_uuid, name_zh, name_en, unit, unit_group_name, classification, updated_at = row[:7]
            flow_uuid = flow_uuid.strip()
            if not flow_uuid:
                continue
            classification_path = [item.strip() for item in classification.split(";") if item.strip()]
            if not classification_path:
                continue
            root = classification_path[0].casefold()
            if root == "emissions":
                direction = "output"
            elif root == "resources":
                direction = "input"
            else:
                continue
            unit_group = reference["unit_group_by_name"].get(unit_group_name.strip())
            flow_property = reference["flow_property_by_unit_group_name"].get(unit_group_name.strip())
            if unit_group is None or flow_property is None:
                continue
            record = {
                "source_namespace": TIANGONG_OPEN_NAMESPACE,
                "flow_uuid": flow_uuid,
                "version": TIANGONG_ELEMENTARY_FLOW_VERSION,
                "name": name_zh.strip(),
                "name_en": name_en.strip(),
                "flow_type": "Elementary flow",
                "unit": unit.strip(),
                "unit_group": unit_group_name.strip(),
                "unit_group_uuid": str(unit_group["uuid"]),
                "unit_group_version": str(unit_group["version"]),
                "flow_property_uuid": str(flow_property["flow_property_uuid"]),
                "flow_property_version": str(flow_property["version"]),
                "direction": direction,
                "compartment": classification_path[-1],
                "classification_path": classification_path,
                "source_updated_at": updated_at.strip(),
            }
            record["content_hash"] = _canonical_hash(record)
            result[flow_uuid] = record
    return result


@lru_cache(maxsize=4)
def _runtime_flow_index(runtime_dir_raw: str) -> dict[str, int]:
    result: dict[str, int] = {}
    with (Path(runtime_dir_raw) / "flow_index.csv").open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            flow_uuid = str(row.get("FlowUUID") or "").strip()
            if flow_uuid:
                result[flow_uuid] = int(row.get("flow_index") or 0)
    return result


@lru_cache(maxsize=12)
def _file_sha256(path_raw: str) -> str:
    digest = hashlib.sha256()
    with Path(path_raw).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _csv_rows_by_indicator_index(path: Path) -> dict[int, dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(2048)
        handle.seek(0)
        delimiter = ";" if sample.count(";") >= sample.count(",") else ","
        rows: dict[int, dict[str, str]] = {}
        for row in csv.DictReader(handle, delimiter=delimiter):
            raw_index = str(row.get("indicator_index") or "").strip()
            if not raw_index:
                continue
            try:
                indicator_index = int(raw_index)
            except ValueError as exc:
                raise ProviderEf31Error(
                    "EF31_INDICATOR_METADATA_INVALID",
                    "EF3.1 indicator metadata contains a non-integer indicator index.",
                    asset=path.name,
                    indicator_index=raw_index,
                ) from exc
            if indicator_index in rows:
                raise ProviderEf31Error(
                    "EF31_INDICATOR_METADATA_DUPLICATE",
                    "EF3.1 indicator metadata contains a duplicate indicator index.",
                    asset=path.name,
                    indicator_index=indicator_index,
                )
            rows[indicator_index] = {
                key: str(value or "").strip()
                for key, value in row.items()
                if key is not None
            }
    return rows


def _indicator_unit_metadata(runtime_dirs: list[Path]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if not _INDICATOR_METHOD_METADATA.exists():
        raise ProviderEf31Error(
            "EF31_INDICATOR_UNIT_METADATA_UNAVAILABLE",
            "The authoritative EF3.1 indicator-unit metadata asset is unavailable.",
            asset="data/EF3.1/indicator_index.csv",
        )
    method_rows = _csv_rows_by_indicator_index(_INDICATOR_METHOD_METADATA)
    unit_by_index: dict[int, str] = {}
    for indicator_index, row in sorted(method_rows.items()):
        method_en = row.get("method_en", "")
        indicator_en = row.get("indicator_en", "")
        unit = row.get("LCIA_unit", "") or row.get("lcia_unit", "")
        if not method_en or not indicator_en or not unit:
            raise ProviderEf31Error(
                "EF31_INDICATOR_UNIT_NOT_FOUND",
                "An EF3.1 indicator lacks an authoritative result unit.",
                indicator_index=indicator_index,
                method_en=method_en,
                indicator_en=indicator_en,
            )
        unit_by_index[indicator_index] = unit

    runtime_sources = [
        (runtime_dir, _csv_rows_by_indicator_index(runtime_dir / "indicator_index.csv"))
        for runtime_dir in runtime_dirs
    ]
    reference_runtime: tuple[Path, dict[int, dict[str, str]]] | None = None
    for runtime_dir, runtime_rows in runtime_sources:
        if set(runtime_rows) != set(method_rows):
            continue
        if all(
            runtime_rows[indicator_index].get(field, "") == method_rows[indicator_index].get(field, "")
            for indicator_index in method_rows
            for field in ("method_en", "indicator_en")
        ):
            reference_runtime = (runtime_dir, runtime_rows)
            break
    if reference_runtime is None:
        raise ProviderEf31Error(
            "EF31_INDICATOR_METADATA_MISMATCH",
            "No configured EF3.1 runtime exactly binds the authoritative indicator-unit metadata to canonical identities.",
            unit_metadata_sha256=_file_sha256(str(_INDICATOR_METHOD_METADATA.resolve())),
        )

    unit_rows: dict[str, dict[str, Any]] = {}
    canonical_rows = []
    reference_dir, reference_rows = reference_runtime
    for indicator_index, method_row in sorted(method_rows.items()):
        runtime_row = reference_rows[indicator_index]
        canonical_key = str(runtime_row.get("ecoinvent_category") or "").strip().casefold()
        if not canonical_key:
            raise ProviderEf31Error(
                "EF31_INDICATOR_IDENTITY_UNAVAILABLE",
                "The EF3.1 runtime indicator lacks a canonical category identity.",
                indicator_index=indicator_index,
            )
        if canonical_key in unit_rows:
            raise ProviderEf31Error(
                "EF31_INDICATOR_IDENTITY_AMBIGUOUS",
                "The EF3.1 runtime contains a duplicate canonical indicator identity.",
                canonical_indicator_key=canonical_key,
            )
        canonical = {
            "indicator_index": indicator_index,
            "canonical_indicator_key": canonical_key,
            "method_en": method_row["method_en"],
            "indicator_en": method_row["indicator_en"],
            "unit": unit_by_index[indicator_index],
        }
        canonical["content_hash"] = _canonical_hash(canonical)
        canonical_rows.append(canonical)
        unit_rows[canonical_key] = canonical

    expected_keys = set(unit_rows)
    validated_runtime_hashes = []
    for runtime_dir, runtime_rows in runtime_sources:
        runtime_hash = _file_sha256(str((runtime_dir / "indicator_index.csv").resolve()))
        explicit_ef31_rows = [
            row
            for row in runtime_rows.values()
            if str(row.get("method_en") or "").strip() == EF31_METHOD
        ]
        if explicit_ef31_rows:
            runtime_keys = {
                str(row.get("ecoinvent_category") or "").strip().casefold()
                for row in explicit_ef31_rows
                if str(row.get("ecoinvent_category") or "").strip()
            }
            if runtime_keys != expected_keys:
                raise ProviderEf31Error(
                    "EF31_INDICATOR_METADATA_MISMATCH",
                    "An EF3.1 runtime source does not expose the authoritative canonical indicator set.",
                    runtime_indicator_sha256=runtime_hash,
                    missing_canonical_indicators=sorted(expected_keys.difference(runtime_keys)),
                    unexpected_canonical_indicators=sorted(runtime_keys.difference(expected_keys)),
                )
        else:
            is_legacy_exact = set(runtime_rows) == set(method_rows) and all(
                runtime_rows[indicator_index].get(field, "") == method_rows[indicator_index].get(field, "")
                for indicator_index in method_rows
                for field in ("method_en", "indicator_en")
            ) and all(
                runtime_rows[indicator_index].get("ecoinvent_category", "")
                == reference_rows[indicator_index].get("ecoinvent_category", "")
                for indicator_index in method_rows
            )
            if not is_legacy_exact:
                raise ProviderEf31Error(
                    "EF31_INDICATOR_METADATA_MISMATCH",
                    "A legacy EF3.1 runtime source cannot be tied exactly to the indicator-unit metadata.",
                    runtime_indicator_sha256=runtime_hash,
                )
        validated_runtime_hashes.append(runtime_hash)

    metadata_path = _INDICATOR_METHOD_METADATA.resolve()
    receipt = {
        "schema_version": "provider.lcia.indicator_metadata.v1",
        "method": EF31_METHOD,
        "database_release": EF31_DATABASE_RELEASE,
        "asset": "data/EF3.1/indicator_index.csv",
        "sha256": _file_sha256(str(metadata_path)),
        "content_hash": _canonical_hash(canonical_rows),
        "indicator_count": len(canonical_rows),
        "identity_runtime_indicator_sha256": _file_sha256(
            str((reference_dir / "indicator_index.csv").resolve())
        ),
        "validated_runtime_indicator_sha256s": sorted(set(validated_runtime_hashes)),
    }
    return unit_rows, receipt


def _runtime_dirs() -> list[Path]:
    dirs = _resolve_embedded_ef31_dirs()
    if not dirs:
        raise ProviderEf31Error("EF31_RUNTIME_UNAVAILABLE", "No usable EF3.1 runtime is configured.")
    return dirs


def resolve_standard_flow(ref: ExactFlowRef) -> dict[str, Any] | None:
    if ref.source_namespace != TIANGONG_OPEN_NAMESPACE:
        return None
    if ref.version != TIANGONG_ELEMENTARY_FLOW_VERSION:
        return None
    record = _elementary_catalog().get(ref.flow_uuid)
    if record is None:
        return None
    runtime_dirs = _runtime_dirs()
    runtime_index = next(
        (
            index[ref.flow_uuid]
            for runtime_dir in runtime_dirs
            if ref.flow_uuid in (index := _runtime_flow_index(str(runtime_dir.resolve())))
        ),
        None,
    )
    if runtime_index is None:
        return None
    return {
        **record,
        "default_unit": record["unit"],
        "runtime_flow_index": runtime_index,
        "database_release": EF31_DATABASE_RELEASE,
        "content_hash_scope": "provider_canonical_runtime_record",
    }


def resolve_standard_flow_property(ref: ExactFlowPropertyRef) -> dict[str, Any] | None:
    item = _reference_catalog()["flow_properties"].get((ref.flow_property_uuid, ref.version))
    if item is None:
        return None
    value = {
        "flow_property_uuid": ref.flow_property_uuid,
        "version": ref.version,
        "name_en": str(item.get("name_en") or ""),
        "ref_uri": str(item.get("ref_uri") or ""),
    }
    value["content_hash"] = _canonical_hash(value)
    return value


def resolve_standard_unit_group(ref: ExactUnitGroupRef) -> dict[str, Any] | None:
    item = _reference_catalog()["unit_groups"].get((ref.unit_group_uuid, ref.version))
    if item is None:
        return None
    value = {
        "unit_group_uuid": ref.unit_group_uuid,
        "version": ref.version,
        "name": str(item.get("name") or ""),
        "reference_unit": str(item.get("reference_unit") or item.get("default_unit") or ""),
        "units": [
            {
                "unit": str(unit.get("name") or ""),
                "factor_to_reference": float(unit.get("meanValue") or 0.0),
                "is_reference": str(unit.get("name") or "")
                == str(item.get("reference_unit") or item.get("default_unit") or ""),
            }
            for unit in item.get("units") or []
        ],
    }
    value["content_hash"] = _canonical_hash(value)
    return value


def resolve_standard_unit(ref: ExactUnitRef) -> dict[str, Any] | None:
    group = resolve_standard_unit_group(
        ExactUnitGroupRef(
            unit_group_uuid=ref.unit_group_uuid,
            version=ref.version,
            correlation_id=ref.correlation_id,
        )
    )
    if group is None:
        return None
    unit = next((item for item in group["units"] if item["unit"] == ref.unit), None)
    if unit is None:
        return None
    return {
        "unit_group_uuid": ref.unit_group_uuid,
        "unit_group_version": ref.version,
        "unit_group": group["name"],
        **unit,
        "content_hash": _canonical_hash(
            {
                "unit_group_uuid": ref.unit_group_uuid,
                "unit_group_version": ref.version,
                **unit,
            }
        ),
    }


def _raise_mismatch(field: str, exchange_id: str, expected: Any, actual: Any) -> None:
    raise ProviderEf31Error(
        "ELEMENTARY_FLOW_IDENTITY_MISMATCH",
        f"Elementary Flow {field} does not match the exact EF3.1 catalog record.",
        exchange_id=exchange_id,
        field=field,
        expected=expected,
        actual=actual,
    )


def characterize_scaled_inventory(
    *,
    methods: list[str],
    elementary_refs: list[ProviderElementaryFlowRef],
    scaled_exchanges: list[ProviderScaledExchange],
    inventory_totals: list[ProviderInventoryTotal],
) -> tuple[dict[str, Any], list[ProviderElementaryFlowReceipt]]:
    if not methods or any(method != EF31_METHOD for method in methods):
        raise ProviderEf31Error(
            "LCIA_METHOD_UNSUPPORTED",
            "Provider inline LCIA currently supports only EF v3.1.",
            requested_methods=methods,
            supported_methods=[EF31_METHOD],
        )
    scaled_elementary = [item for item in scaled_exchanges if item.exchange_type == "elementary"]
    ref_by_exchange: dict[str, ProviderElementaryFlowRef] = {}
    for ref in elementary_refs:
        if ref.exchange_id in ref_by_exchange:
            raise ProviderEf31Error(
                "ELEMENTARY_FLOW_REFERENCE_DUPLICATE",
                "Each elementary exchange must have exactly one exact reference.",
                exchange_id=ref.exchange_id,
            )
        ref_by_exchange[ref.exchange_id] = ref
    scaled_ids = {item.exchange_id for item in scaled_elementary}
    unknown_ref_ids = sorted(set(ref_by_exchange).difference(scaled_ids))
    if unknown_ref_ids:
        raise ProviderEf31Error(
            "ELEMENTARY_FLOW_REFERENCE_UNKNOWN_EXCHANGE",
            "An elementary reference points to an exchange not present in the solved inventory.",
            exchange_ids=unknown_ref_ids,
        )
    missing_ref_ids = sorted(scaled_ids.difference(ref_by_exchange))
    if missing_ref_ids:
        raise ProviderEf31Error(
            "ELEMENTARY_FLOW_REFERENCE_REQUIRED",
            "LCIA requires an exact EF3.1 reference for every elementary exchange.",
            exchange_ids=missing_ref_ids,
        )

    runtime_dirs = _runtime_dirs()
    standard_by_exchange: dict[str, dict[str, Any]] = {}
    for exchange in scaled_elementary:
        ref = ref_by_exchange[exchange.exchange_id]
        standard = resolve_standard_flow(
            ExactFlowRef(
                source_namespace=ref.source_namespace,
                flow_uuid=ref.flow_uuid,
                version=ref.version,
            )
        )
        if standard is None:
            raise ProviderEf31Error(
                "ELEMENTARY_FLOW_NOT_IN_EF31_RUNTIME",
                "The exact elementary Flow is not present in the configured EF3.1 runtime catalog.",
                exchange_id=exchange.exchange_id,
                source_namespace=ref.source_namespace,
                flow_uuid=ref.flow_uuid,
                version=ref.version,
            )
        comparisons = {
            "flow_uuid": (standard["flow_uuid"], exchange.flow_uuid, ref.flow_uuid),
            "source_namespace": (standard["source_namespace"], exchange.flow_source_namespace, ref.source_namespace),
            "version": (standard["version"], exchange.flow_version, ref.version),
            "flow_property_uuid": (standard["flow_property_uuid"], exchange.flow_property_uuid, ref.flow_property_uuid),
            "flow_property_version": (
                standard["flow_property_version"],
                exchange.flow_property_version,
                ref.flow_property_version,
            ),
            "unit_group_uuid": (standard["unit_group_uuid"], exchange.unit_group_uuid, ref.unit_group_uuid),
            "unit_group_version": (
                standard["unit_group_version"],
                exchange.unit_group_version,
                ref.unit_group_version,
            ),
            "unit": (standard["unit"], exchange.unit, ref.unit),
            "direction": (standard["direction"], exchange.direction, ref.direction),
        }
        for field, (expected, graph_actual, ref_actual) in comparisons.items():
            if graph_actual != expected:
                _raise_mismatch(field, exchange.exchange_id, expected, graph_actual)
            if ref_actual != expected:
                _raise_mismatch(field, exchange.exchange_id, expected, ref_actual)
        if ref.compartment != standard["compartment"]:
            _raise_mismatch("compartment", exchange.exchange_id, standard["compartment"], ref.compartment)
        standard_by_exchange[exchange.exchange_id] = standard

    flow_uuids = sorted({item.flow_uuid for item in inventory_totals})
    if not flow_uuids:
        raise ProviderEf31Error("ELEMENTARY_INVENTORY_EMPTY", "LCIA was requested but the scaled elementary inventory is empty.")
    _ensure_embedded_solver_core()
    runtime_cache = importlib.import_module("app.core.ef31_runtime_cache")
    issues: list[str] = []
    c_pack = runtime_cache.GLOBAL_EF31_RUNTIME_CACHE.build_c_matrix_from_sources(
        [str(path) for path in runtime_dirs],
        {
            "rows": flow_uuids,
            "cols": ["inventory"],
            "shape": [len(flow_uuids), 1],
            "data": [],
        },
        lcia_methods=methods,
        issues=issues,
    )
    runtime_flow_uuids = set(c_pack.get("runtime_flow_uuids") or set())
    missing = sorted(set(flow_uuids).difference(runtime_flow_uuids))
    if missing:
        raise ProviderEf31Error(
            "ELEMENTARY_FLOW_CF_NOT_FOUND",
            "At least one exact elementary Flow has no EF3.1 runtime coverage.",
            flow_uuids=missing,
        )
    c_matrix = c_pack["C"]
    if not c_matrix.get("rows"):
        raise ProviderEf31Error("EF31_INDICATOR_SET_EMPTY", "The EF3.1 runtime returned no indicators for the requested method.")

    inventory_by_flow: dict[str, float] = {}
    for item in inventory_totals:
        inventory_by_flow[item.flow_uuid] = inventory_by_flow.get(item.flow_uuid, 0.0) + float(item.amount)
    values = [0.0 for _ in c_matrix["rows"]]
    factors_by_flow: dict[str, list[dict[str, Any]]] = {}
    indicator_lookup = c_pack.get("indicator_lookup") or {}
    for entry in c_matrix.get("data") or []:
        row_pos = int(entry["row_index"])
        flow_uuid = str(entry["col"])
        coefficient = float(entry["value"])
        values[row_pos] += inventory_by_flow.get(flow_uuid, 0.0) * coefficient
        info = indicator_lookup.get(entry["row"], {})
        factors_by_flow.setdefault(flow_uuid, []).append(
            {
                "canonical_indicator_key": str(info.get("canonical_indicator_key") or ""),
                "coefficient": coefficient,
            }
        )

    factor_receipt_by_flow = {
        flow_uuid: {
            "factor_count": len(factors),
            "factor_hash": _canonical_hash(sorted(factors, key=lambda item: item["canonical_indicator_key"])),
        }
        for flow_uuid, factors in factors_by_flow.items()
    }
    uncovered = sorted(set(flow_uuids).difference(factor_receipt_by_flow))
    if uncovered:
        raise ProviderEf31Error(
            "ELEMENTARY_FLOW_CF_NOT_FOUND",
            "At least one exact elementary Flow has no non-zero EF3.1 characterization factor.",
            flow_uuids=uncovered,
        )

    receipts = []
    for exchange in scaled_elementary:
        standard = standard_by_exchange[exchange.exchange_id]
        factor_receipt = factor_receipt_by_flow[exchange.flow_uuid]
        receipts.append(
            ProviderElementaryFlowReceipt(
                exchange_id=exchange.exchange_id,
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
                content_hash=standard["content_hash"],
                runtime_flow_index=int(standard["runtime_flow_index"]),
                method=EF31_METHOD,
                factor_count=int(factor_receipt["factor_count"]),
                factor_hash=str(factor_receipt["factor_hash"]),
            )
        )

    indicator_units, indicator_metadata = _indicator_unit_metadata(runtime_dirs)
    indicators = []
    for row_pos, row_id in enumerate(c_matrix["rows"]):
        info = dict(indicator_lookup.get(row_id, {}))
        canonical_key = str(info.get("canonical_indicator_key") or "").strip().casefold()
        unit_record = indicator_units.get(canonical_key)
        if unit_record is None:
            raise ProviderEf31Error(
                "EF31_INDICATOR_UNIT_NOT_FOUND",
                "The solved EF3.1 indicator has no exact result-unit metadata match.",
                method_en=str(info.get("method_en") or ""),
                indicator_en=str(info.get("indicator_en") or ""),
                canonical_indicator_key=canonical_key,
            )
        indicators.append(
            {
                **info,
                "indicator_unit": unit_record["unit"],
                "unit": unit_record["unit"],
                "indicator_metadata_hash": unit_record["content_hash"],
                "value": values[row_pos],
            }
        )
    runtime_assets = []
    for runtime_dir in runtime_dirs:
        runtime_assets.append(
            {
                "runtime_id": _canonical_hash(
                    {
                        name: _file_sha256(str((runtime_dir / name).resolve()))
                        for name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv")
                    }
                ),
                "flow_index_sha256": _file_sha256(str((runtime_dir / "flow_index.csv").resolve())),
                "indicator_index_sha256": _file_sha256(str((runtime_dir / "indicator_index.csv").resolve())),
                "lcia_factors_sha256": _file_sha256(str((runtime_dir / "lcia_factors.csv").resolve())),
            }
        )
    lcia = {
        "schema_version": "provider.lcia.response.v1",
        "method": EF31_METHOD,
        "database_release": EF31_DATABASE_RELEASE,
        "inventory_hash": _canonical_hash([item.model_dump(mode="json") for item in inventory_totals]),
        "indicator_results": indicators,
        "indicator_metadata": indicator_metadata,
        "runtime_assets": runtime_assets,
        "runtime_source_count": len(runtime_dirs),
    }
    return lcia, receipts
