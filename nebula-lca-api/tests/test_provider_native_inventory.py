from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

from app.provider_schemas import ProviderNativeBackgroundInventoryRequest, ProviderSolveRequest
from app.services.provider_contract import ProviderContractError
from app.services import provider_native_inventory as inventory


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_inventory_runtime_needs_original_source_and_bindings_but_no_cf(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    files = {"ElementaryExchanges.xml": b"original-flow", "Units.xml": b"original-unit"}
    for name, data in files.items():
        (source / name).write_bytes(data)
    binding = tmp_path / "bindings.json"
    binding.write_text("{}", encoding="utf-8")
    reference = tmp_path / "reference.json"
    reference.write_text("{}", encoding="utf-8")
    manifest = {
        "schema_version": "provider.native-inventory-runtime-manifest.v1",
        "database_release": "ecoinvent_3.11",
        "source_asset_dir": str(source),
        "source_asset_sha256": {name: _sha(data) for name, data in files.items()},
        "unit_binding_path": str(binding),
        "unit_binding_sha256": _sha(binding.read_bytes()),
        "reference_seed_sha256": _sha(reference.read_bytes()),
        "vector_flow_source_identities": ["ecoinvent_3.11_cutoff_lci"],
        "flow_snapshot_hash": "a" * 64,
        "reference_dependency_snapshot_hash": "b" * 64,
        "electricity_flow_uuid": "electricity",
        "electricity_flow_version": "01.01.003",
        "electricity_flow_payload_content_hash": "c" * 64,
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv(inventory.MANIFEST_ENV, str(path))
    monkeypatch.setattr(inventory, "REFERENCE_SEED_PATH", reference)
    monkeypatch.setattr(inventory, "parse_units", lambda _path: {"unit": SimpleNamespace(name="kg")})
    monkeypatch.setattr(inventory, "parse_elementary_exchanges", lambda _path, _units: [
        SimpleNamespace(flow_uuid="flow", flow_name="flow", default_unit="kg", unit_id="unit", compartment="air", subcompartment="unspecified")
    ])
    monkeypatch.setattr(inventory, "_native_unit_bindings", lambda *args, **kwargs: {"unit": {"unit_group_uuid": "group"}})
    snapshot = SimpleNamespace(snapshot_hash="a" * 64, reference_dependencies=SimpleNamespace(snapshot_hash="b" * 64),
                               resolve=lambda *_args: {"content_hash": "c" * 64})
    pin = ProviderNativeBackgroundInventoryRequest(
        database_release="ecoinvent_3.11", inventory_runtime_manifest_sha256=_sha(path.read_bytes()),
        flow_snapshot_hash="a" * 64, reference_dependency_snapshot_hash="b" * 64,
    )
    result = inventory.load_native_inventory_runtime(pin, flow_snapshot=snapshot)
    assert result.native_source_flows["flow"]["unit"] == "kg"
    assert result.unit_binding_sha256 == manifest["unit_binding_sha256"]
    assert not (tmp_path / "lcia_factors.csv").exists()
    (source / "ElementaryExchanges.xml").write_bytes(b"changed")
    with pytest.raises(ProviderContractError) as error:
        inventory.load_native_inventory_runtime(pin, flow_snapshot=snapshot)
    assert error.value.code == "NATIVE_CLIMATE_RUNTIME_HASH_MISMATCH"


def test_inventory_request_rejects_climate_method_selection():
    with pytest.raises(ValueError, match="native inventory cannot select"):
        ProviderSolveRequest.model_validate({
            "snapshot_ref": {"project_id": "project", "version": 1},
            "demand": [{"reference_flow_uuid": "product", "amount": 1, "unit": "kg"}],
            "native_background_inventory": {
                "database_release": "ecoinvent_3.11",
                "inventory_runtime_manifest_sha256": "a" * 64,
                "flow_snapshot_hash": "b" * 64,
                "reference_dependency_snapshot_hash": "c" * 64,
            },
            "lcia_methods": ["EF v3.1"],
        })
