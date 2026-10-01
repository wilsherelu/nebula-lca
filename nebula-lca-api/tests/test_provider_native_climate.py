from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

from app.services.provider_background_leaf import ProviderBackgroundLeafError
from app.services.provider_contract import ProviderContractError
from app.services.provider_mapped_background import _native_elementary
from app.services.provider_native_climate import (
    NativeClimateRuntime,
    _check_hash,
    _factor_columns,
    _flow_columns,
    _native_unit_bindings,
    REFERENCE_SEED_PATH,
    build_native_source_flow_index,
    characterize_native_climate,
)


def _exchange(exchange_id, process_uuid, flow_uuid, amount, *, namespace, version="1", unit="kg"):
    return SimpleNamespace(
        exchange_id=exchange_id,
        process_uuid=process_uuid,
        flow_uuid=flow_uuid,
        flow_source_namespace=namespace,
        flow_version=version,
        scaled_amount=amount,
        unit=unit,
        direction="output",
        exchange_type="elementary",
        boundary_role="boundary",
    )


def _runtime() -> NativeClimateRuntime:
    return NativeClimateRuntime(
        package={
            "package_id": "synthetic-climate",
            "package_version": "1",
            "database_release": "synthetic-release",
            "method": "EF v3.1",
            "provider_system_model_evidence": "not_asserted_by_local_vector_metadata",
        },
        package_sha256="a" * 64,
            manifest_sha256="b" * 64,
            unit_bindings={},
            unit_binding_sha256="c" * 64,
        native_columns={"native-carbon": 1, "native-storage": 2, "native-no-cf": 3},
        native_source_flows={
            key: {"name": key, "unit": "kg", "compartment": "air" if key != "native-storage" else "soil", "subcompartment": "unspecified"}
            for key in ("native-carbon", "native-storage", "native-no-cf")
        },
        native_factors={1: 2.0, 2: -1.0},
        foreground_columns={"ef-carbon": 1, "ef-explicit-zero": 2, "ef-uncertified": 3},
        foreground_factors={1: 2.0, 2: 0.0, 3: 5.0},
        certified_by_foreground={
            "ef-carbon": {
                "ecoinvent_flow_uuid": "native-carbon",
                "eco_to_ef_factor": 1.0,
                "unit": "kg",
                "direction": "output",
                "ef_context": {"catalog_compartment": "Emissions;air", "catalog_subcompartment": None},
            }
        },
    )


def _ref(exchange_id, flow_uuid):
    return SimpleNamespace(
        exchange_id=exchange_id, source_namespace="tiangong_open_data",
        flow_uuid=flow_uuid, version="1", unit="kg", direction="output", compartment="air",
    )


class _Snapshot:
    def resolve(self, flow_uuid, version, *, require_elementary_factor_coverage):
        assert require_elementary_factor_coverage is False
        return {"flow_type": "Elementary flow", "unit": "kg", "direction": "output", "compartment": "air"}


def _base_exchanges():
    return [
        _exchange("bg::carbon", "provider", "native-carbon", 3.0, namespace="ecoinvent", version="synthetic-release"),
        _exchange("bg::storage", "provider", "native-storage", 2.0, namespace="ecoinvent", version="synthetic-release"),
        _exchange("bg::no-cf", "provider", "native-no-cf", 5.0, namespace="ecoinvent", version="synthetic-release"),
        _exchange("fg::carbon", "foreground", "ef-carbon", 4.0, namespace="tiangong_open_data"),
        _exchange("fg::zero", "foreground", "ef-explicit-zero", 1.0, namespace="tiangong_open_data"),
    ]


def test_native_and_foreground_characterize_once_with_distinct_no_cf_and_zero():
    result = characterize_native_climate(
        runtime=_runtime(), scaled_exchanges=_base_exchanges(),
        foreground_refs=[_ref("fg::carbon", "ef-carbon"), _ref("fg::zero", "ef-explicit-zero")],
        flow_snapshot=_Snapshot(), native_process_uuids={"provider"},
    )
    assert result["merge_status"] == "merged"
    assert result["unified_climate_total"] == 12.0  # 3*2 + 2*(-1) + 4*2
    assert result["domains"]["native_background"]["coverage"] == {
        "explicit_nonzero": 2, "no_cf": 1,
    }
    assert result["domains"]["foreground"]["coverage"] == {
        "explicit_nonzero": 1, "explicit_zero": 1,
    }
    native_terms = result["domains"]["native_background"]["terms"]
    assert next(term for term in native_terms if term["flow_uuid"] == "native-storage")["cf_value"] == -1.0
    assert next(term for term in native_terms if term["flow_uuid"] == "native-storage")["contribution_value"] == -2.0
    assert next(term for term in native_terms if term["flow_uuid"] == "native-no-cf")["contribution_value"] is None
    assert result["domains"]["native_background"]["negative_cf_exchange_count"] == 1


def test_two_native_providers_contribute_once_each():
    exchanges = _base_exchanges() + [
        _exchange("bg-2::carbon", "provider-2", "native-carbon", 1.5,
                  namespace="ecoinvent", version="synthetic-release"),
    ]
    result = characterize_native_climate(
        runtime=_runtime(), scaled_exchanges=exchanges,
        foreground_refs=[_ref("fg::carbon", "ef-carbon"), _ref("fg::zero", "ef-explicit-zero")],
        flow_snapshot=_Snapshot(), native_process_uuids={"provider", "provider-2"},
    )
    assert result["merge_status"] == "merged"
    assert result["unified_climate_total"] == 15.0
    terms = result["domains"]["native_background"]["terms"]
    assert {term["process_uuid"] for term in terms} == {"provider", "provider-2"}
    assert sum(term["contribution_value"] or 0 for term in terms) == 7.0


def test_uncertified_contributing_foreground_returns_split_diagnostic():
    exchanges = _base_exchanges() + [
        _exchange("fg::unreviewed", "foreground", "ef-uncertified", 1.0, namespace="tiangong_open_data")
    ]
    result = characterize_native_climate(
        runtime=_runtime(), scaled_exchanges=exchanges,
        foreground_refs=[_ref("fg::carbon", "ef-carbon"), _ref("fg::zero", "ef-explicit-zero"), _ref("fg::unreviewed", "ef-uncertified")],
        flow_snapshot=_Snapshot(), native_process_uuids={"provider"},
    )
    assert result["merge_status"] == "split_diagnostic"
    assert result["unified_climate_total"] is None
    assert result["merge_blockers"][0]["reason"] == "foreground_contributing_flow_not_certified"


def test_missing_native_identity_and_duplicate_exchange_fail_closed():
    exchanges = _base_exchanges()
    exchanges[0] = _exchange("bg::carbon", "provider", "missing-native", 3.0, namespace="ecoinvent", version="synthetic-release")
    with pytest.raises(ProviderContractError, match="absent from the pinned original MasterData") as error:
        characterize_native_climate(runtime=_runtime(), scaled_exchanges=exchanges,
            foreground_refs=[_ref("fg::carbon", "ef-carbon"), _ref("fg::zero", "ef-explicit-zero")],
            flow_snapshot=_Snapshot(), native_process_uuids={"provider"})
    assert error.value.code == "NATIVE_CLIMATE_FLOW_IDENTITY_MISSING"
    exchanges = _base_exchanges()
    exchanges.append(exchanges[0])
    with pytest.raises(ProviderContractError) as error:
        characterize_native_climate(runtime=_runtime(), scaled_exchanges=exchanges,
            foreground_refs=[_ref("fg::carbon", "ef-carbon"), _ref("fg::zero", "ef-explicit-zero")],
            flow_snapshot=_Snapshot(), native_process_uuids={"provider"})
    assert error.value.code == "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE"


def test_original_masterdata_identity_overrides_colliding_catalog_name_and_type():
    source = {"same-uuid": {"name": "Original elementary", "unit": "kg", "unit_id": "kg-unit", "compartment": "natural resource", "subcompartment": "unspecified"}}
    key = SimpleNamespace(flow_uuid="same-uuid", canonical_unit="kg", compartment="natural resource",
        direction="input", source="synthetic-release", source_package_version="synthetic-release")
    # A separate catalog's Product flow with this UUID is deliberately irrelevant.
    result = _native_elementary(key, database_release="synthetic-release",
        allowed_flow_sources=("synthetic-release",), source_flows=source,
        unit_bindings={"kg-unit": {"flow_property_uuid": "mass-property", "flow_property_version": "1", "unit_group_uuid": "mass-group", "unit_group_version": "1", "unit_group": "mass"}})
    assert result["name"] == "Original elementary"
    assert result["source_namespace"] == "ecoinvent"
    assert result["direction"] == "input"
    assert result["unit_group_uuid"] == "mass-group"
    assert result["flow_property_uuid"] == "mass-property"
    with pytest.raises(ProviderBackgroundLeafError) as error:
        _native_elementary(key, database_release="synthetic-release",
            allowed_flow_sources=("synthetic-release",), source_flows=source,
            unit_bindings={})
    assert error.value.code == "NATIVE_CLIMATE_UNIT_BINDING_MISSING"
    key.source_package_version = "wrong-release"
    with pytest.raises(ProviderBackgroundLeafError) as error:
        _native_elementary(key, database_release="synthetic-release",
            allowed_flow_sources=("synthetic-release",), source_flows=source,
            unit_bindings={})
    assert error.value.code == "NATIVE_CLIMATE_VECTOR_IDENTITY_MISMATCH"


def test_original_masterdata_duplicate_and_missing_uuid_fail():
    row = SimpleNamespace(flow_uuid="a", flow_name="A", default_unit="kg", unit_id="kg-unit", compartment="air", subcompartment="")
    with pytest.raises(ProviderContractError) as error:
        build_native_source_flow_index([row, row], {"a": 1})
    assert error.value.code == "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE"
    with pytest.raises(ProviderContractError) as error:
        build_native_source_flow_index([row], {"a": 1, "missing": 2})
    assert error.value.code == "NATIVE_CLIMATE_FLOW_IDENTITY_MISSING"


def test_runtime_hash_and_duplicate_columns_or_cf_fail(tmp_path):
    asset = tmp_path / "asset.txt"
    asset.write_text("original", encoding="utf-8")
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    _check_hash(asset, digest)
    asset.write_text("tampered", encoding="utf-8")
    with pytest.raises(ProviderContractError) as error:
        _check_hash(asset, digest)
    assert error.value.code == "NATIVE_CLIMATE_RUNTIME_HASH_MISMATCH"

    flow_index = tmp_path / "flow_index.csv"
    flow_index.write_text("flow_index;FlowUUID;FlowName\n1;a;A\n1;b;B\n", encoding="utf-8")
    with pytest.raises(ProviderContractError) as error:
        _flow_columns(flow_index)
    assert error.value.code == "NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE"
    factors = tmp_path / "lcia_factors.csv"
    factors.write_text("row;column;coefficient\n1;1;2\n1;1;3\n", encoding="utf-8")
    with pytest.raises(ProviderContractError) as error:
        _factor_columns(factors, 1)
    assert error.value.code == "NATIVE_CLIMATE_CF_DUPLICATE"


def test_native_unit_binding_is_pinned_and_rejects_unlisted_unit(tmp_path):
    source_id = "225da0bd-ba54-48bc-9a0e-5fa27d722185"
    package = {
        "schema_version": "provider.native-unit-reference-bindings.v1",
        "database_release": "ecoinvent_3.11",
        "source_units_sha256": "a" * 64,
        "reference_seed_sha256": hashlib.sha256(REFERENCE_SEED_PATH.read_bytes()).hexdigest(),
        "claim": "dimension_reference_only_no_unit_conversion_or_flow_identity_mapping",
        "bindings": [{
            "source_unit_id": source_id,
            "source_unit": "Sm3",
            "reference_unit_group": "Units of volume",
            "binding_level": "dimension_reference_only",
        }],
    }
    path = tmp_path / "bindings.json"
    path.write_text(json.dumps(package), encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    result = _native_unit_bindings(
        path, expected_hash=digest,
        source_units={"sha256": "a" * 64, "records": {source_id: SimpleNamespace(name="Sm3")}},
        database_release="ecoinvent_3.11",
    )
    assert result[source_id]["unit_group_uuid"] == "93a60a57-a3c8-12da-a746-0800200c9a66"
    assert result[source_id]["flow_property_uuid"] == "93a60a56-a3c8-22da-a746-0800200c9a66"
    package["bindings"][0]["binding_level"] = "exact_unit_member"
    path.write_text(json.dumps(package), encoding="utf-8")
    with pytest.raises(ProviderContractError) as error:
        _native_unit_bindings(
            path, expected_hash=hashlib.sha256(path.read_bytes()).hexdigest(),
            source_units={"sha256": "a" * 64, "records": {source_id: SimpleNamespace(name="Sm3")}},
            database_release="ecoinvent_3.11",
        )
    assert error.value.code == "NATIVE_CLIMATE_UNIT_BINDING_INVALID"
