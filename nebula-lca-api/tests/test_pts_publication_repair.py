from app.models import PtsResource
from app.services import graph_storage, pts_operations


def test_pts_publication_repair_normalizes_legacy_units_before_compile(monkeypatch):
    resource = PtsResource(
        project_id="project-1",
        pts_uuid="pts-1",
        pts_node_id="pts-node",
        pts_graph_json={
            "nodes": [{"id": "internal", "inputs": [{"id": "input", "unit": "kg"}]}],
            "exchanges": [],
        },
        ports_policy_json={},
        shell_node_json={"id": "pts-node", "node_kind": "pts_module"},
    )
    observed = {}

    def fake_repair(graph, _db, **_kwargs):
        graph["nodes"][0]["inputs"][0]["unit"] = "MJ"
        return [{"port_id": "input"}]

    def fake_build(row):
        observed["unit"] = row.pts_graph_json["nodes"][0]["inputs"][0]["unit"]
        return None

    monkeypatch.setattr(graph_storage, "repair_impossible_flow_units", fake_repair)
    monkeypatch.setattr(pts_operations._pr, "_build_compile_graph_from_pts_resource", fake_build)

    repaired, reason = pts_operations._repair_pts_publication_from_resource(db=object(), resource=resource)

    assert repaired is False
    assert reason == "pts_resource_graph_invalid"
    assert observed["unit"] == "MJ"
    assert resource.pts_graph_json["nodes"][0]["inputs"][0]["unit"] == "MJ"
