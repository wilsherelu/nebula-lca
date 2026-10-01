import json
from pathlib import Path

from app.services.provider_tidas_reference_snapshot import _load_snapshot as load_references
from app.services.provider_tidas_snapshot import _load_snapshot as load_flows


DATA = Path(__file__).resolve().parents[1] / "data" / "provider_profiles"


def test_steel_open_tiangong_assets_match_published_identity() -> None:
    metadata = json.loads((DATA / "steel_electricity_tiangong_metadata.v1.json").read_text(encoding="utf-8"))
    references = load_references(DATA / "steel_tiangong_open_references.v1.json")
    flows = load_flows(DATA / "steel_tiangong_open_flows.v1.json", references)

    assert flows.snapshot_hash == metadata["flow_snapshot_hash"]
    assert references.snapshot_hash == metadata["reference_dependency_snapshot_hash"]
    assert set(flows.records) == {
        ("08a91e70-3ddc-11dd-923d-0050c2490048", "03.00.004"),
        (metadata["flow_uuid"], metadata["flow_version"]),
    }
    electricity = flows.resolve(metadata["flow_uuid"], metadata["flow_version"])
    assert electricity["flow_property_uuid"] == metadata["flow_property_uuid"]
    assert electricity["unit_group_uuid"] == metadata["unit_group_uuid"]
