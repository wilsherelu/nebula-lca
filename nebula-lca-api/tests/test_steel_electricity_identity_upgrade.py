from __future__ import annotations

import json
from pathlib import Path

from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph
from scripts.build_steel_electricity_identity_upgrade import NEW_IDENTITY, TARGETS, _canonical_hash


ARTIFACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "provider_profiles"
    / "steel_electricity_identity_upgrade.v1.json"
)


def test_steel_electricity_upgrade_is_exactly_scoped_and_hash_valid():
    artifact = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
    assert artifact["schema_version"] == "nebula.steel-template-graph-upgrade.v1"
    assert artifact["template_version"] == "1.2.0"
    assert artifact["base_template_version"] == "1.1.0"
    assert artifact["change_scope"] == "exact_electricity_flow_identity_only"
    assert {item["exchange_id"] for item in artifact["replacements"]} == set(TARGETS)

    graph = artifact["graph"]
    ports = {
        f"{node['id']}::{port['id']}": port
        for node in graph["nodes"]
        for port in node.get("inputs", [])
    }
    for exchange_id, amount in TARGETS.items():
        port = ports[exchange_id]
        assert float(port["amount"]) == amount
        assert all(port[key] == value for key, value in NEW_IDENTITY.items())

    validated = HybridGraph.model_validate(graph)
    assert artifact["exact_graph_sha256"] == _canonical_hash(graph)
    assert artifact["provider_graph_hash"] == compute_graph_hash_from_graph(validated)


def test_steel_electricity_upgrade_retains_other_graph_content():
    artifact = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
    graph = artifact["graph"]
    assert len(graph["nodes"]) == 5
    assert len(graph["exchanges"]) == 4
    assert graph["functionalUnit"] == "1 kg crude steel"
    assert artifact["source_commit"] == "ad37d2107e7a7427cfc4808e9af6857abcf99a3f"
