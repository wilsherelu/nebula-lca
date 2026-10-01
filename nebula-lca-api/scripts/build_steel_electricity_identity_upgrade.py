from __future__ import annotations

import argparse
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph


SOURCE_TEMPLATE_ID = "nebula.iron-steel.bf-bof-crude-steel"
SOURCE_TEMPLATE_VERSION = "1.1.0"
TARGET_TEMPLATE_VERSION = "1.2.0"
OLD_FLOW_UUID = "222665f3-76a4-5894-a34a-27107e793f08"
OLD_FLOW_NAMESPACE = "nebula.standard-lca-seed"
OLD_FLOW_VERSION = "1.3.0"
NEW_IDENTITY = {
    "flowUuid": "3d76981f-964a-4865-b588-0e067a2a1163",
    "flowSourceNamespace": "tiangong_open_data",
    "flowVersion": "01.01.003",
    "flowPropertyUuid": "93a60a56-a3c8-11da-a746-0800200c9a66",
    "flowPropertyVersion": "03.00.003",
    "unitGroupUuid": "93a60a57-a3c8-11da-a746-0800200c9a66",
    "unitGroupVersion": "03.00.003",
    "unit": "MJ",
    "unitGroup": "Units of energy",
}
TARGETS = {
    "ore-sintering::grid-electricity-in": 600.0,
    "continuous-casting::grid-electricity-in": 25.0,
}


def _canonical_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_artifact(source: dict[str, Any], *, source_commit: str) -> dict[str, Any]:
    if source.get("seed_id") != SOURCE_TEMPLATE_ID or source.get("version") != SOURCE_TEMPLATE_VERSION:
        raise ValueError("The source is not the audited steel template 1.1.0.")
    source_graph = source.get("graph")
    if not isinstance(source_graph, dict):
        raise ValueError("The source template has no graph object.")

    graph = deepcopy(source_graph)
    replacements: list[dict[str, Any]] = []
    found: set[str] = set()
    for node in graph.get("nodes", []):
        for port in node.get("inputs", []):
            exchange_id = f"{node.get('id')}::{port.get('id')}"
            if exchange_id not in TARGETS:
                continue
            expected_amount = TARGETS[exchange_id]
            if (
                port.get("flowUuid") != OLD_FLOW_UUID
                or port.get("flowSourceNamespace") != OLD_FLOW_NAMESPACE
                or port.get("flowVersion") != OLD_FLOW_VERSION
                or float(port.get("amount")) != expected_amount
                or port.get("unit") != "MJ"
            ):
                raise ValueError(f"Audited source identity drifted at {exchange_id}.")
            before = {key: port.get(key) for key in NEW_IDENTITY}
            port.update(NEW_IDENTITY)
            after = {key: port.get(key) for key in NEW_IDENTITY}
            replacements.append(
                {
                    "exchange_id": exchange_id,
                    "amount_preserved": expected_amount,
                    "unit_preserved": "MJ",
                    "before": before,
                    "after": after,
                }
            )
            found.add(exchange_id)

    missing = sorted(set(TARGETS) - found)
    if missing:
        raise ValueError(f"Audited electricity ports are missing: {missing}")

    validated = HybridGraph.model_validate(graph)
    return {
        "schema_version": "nebula.steel-template-graph-upgrade.v1",
        "template_id": SOURCE_TEMPLATE_ID,
        "template_version": TARGET_TEMPLATE_VERSION,
        "base_template_version": SOURCE_TEMPLATE_VERSION,
        "source_commit": source_commit,
        "change_scope": "exact_electricity_flow_identity_only",
        "replacements": replacements,
        "exact_graph_sha256": _canonical_hash(graph),
        "provider_graph_hash": compute_graph_hash_from_graph(validated),
        "graph": graph,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    source = json.loads(args.source.read_text(encoding="utf-8"))
    artifact = build_artifact(source, source_commit=args.source_commit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({key: artifact[key] for key in ("template_version", "exact_graph_sha256", "provider_graph_hash")}))


if __name__ == "__main__":
    main()
