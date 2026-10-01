from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import requests


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _capture(response: requests.Response) -> dict[str, Any]:
    try:
        body: Any = response.json()
    except ValueError:
        body = response.text
    return {"status_code": response.status_code, "body": body}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18021")
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--tiangong-account-id", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    base_url = args.base_url.rstrip("/")
    artifact = json.loads(args.artifact.read_text(encoding="utf-8"))
    session = requests.Session()
    session.trust_env = False

    evidence: dict[str, dict[str, Any]] = {}
    health = session.get(f"{base_url}/health", timeout=30)
    evidence["health"] = _capture(health)
    assert health.status_code == 200, health.text

    sync_request = {
        "remote_flow_id": "3d76981f-964a-4865-b588-0e067a2a1163",
        "remote_version": "01.01.003",
        "overwrite": True,
    }
    synced = session.post(
        f"{base_url}/api/data-platforms/accounts/{args.tiangong_account_id}/flows/sync",
        json=sync_request,
        timeout=120,
    )
    evidence["exact_tiangong_flow_sync"] = {"request": sync_request, **_capture(synced)}
    assert synced.status_code == 200, synced.text
    assert synced.json()["status"] == "completed"
    assert synced.json()["flow_uuid"] == sync_request["remote_flow_id"]

    negative_request = {
        "items": [
            {
                "node_id": "ore-sintering",
                "port_id": "grid-electricity-in",
                "flow_uuid": "222665f3-76a4-5894-a34a-27107e793f08",
                "flow_source_namespace": "nebula.standard-lca-seed",
                "flow_version": "1.3.0",
                "direction": "input",
                "exchange_type": "technosphere",
                "unit": "MJ",
                "unit_group": "Units of energy",
            }
        ]
    }
    negative = session.post(
        f"{base_url}/api/intermediate-flow-links/resolve-batch",
        json=negative_request,
        timeout=30,
    )
    evidence["negative_custom_flow"] = {"request": negative_request, **_capture(negative)}
    assert negative.status_code == 200, negative.text
    assert negative.json()["items"][0]["status"] == "blocked"

    positive_request = {
        "items": [
            {
                "node_id": "ore-sintering",
                "port_id": "grid-electricity-in",
                "flow_uuid": "3d76981f-964a-4865-b588-0e067a2a1163",
                "flow_source_namespace": "tiangong_open_data",
                "flow_version": "01.01.003",
                "direction": "input",
                "exchange_type": "technosphere",
                "unit": "MJ",
                "unit_group": "Units of energy",
            }
        ]
    }
    positive = session.post(
        f"{base_url}/api/intermediate-flow-links/resolve-batch",
        json=positive_request,
        timeout=30,
    )
    evidence["standard_flow_mapping_gate"] = {"request": positive_request, **_capture(positive)}
    assert positive.status_code == 200, positive.text
    resolved = positive.json()["items"][0]
    assert resolved["status"] == "blocked"
    assert resolved["reason"] == "TARGET_FLOW_NOT_FOUND"

    providers = session.get(
        f"{base_url}/api/intermediate-flow-links/providers",
        params={"target_flow_uuid": "759b89bd-3aa6-42ad-b767-5bb9ef5d331d"},
        timeout=30,
    )
    evidence["local_provider_candidates"] = _capture(providers)
    assert providers.status_code == 200, providers.text
    assert providers.json()["auto_selected"] is False

    for name, payload in evidence.items():
        _write_json(output / f"{name}.json", payload)
    files = [path for path in sorted(output.glob("*.json")) if path.name != "manifest.json"]
    manifest = {
        "schema_version": "nebula.steel-electricity-http-smoke.v1",
        "base_url": base_url,
        "isolated": True,
        "exact_graph_sha256": artifact["exact_graph_sha256"],
        "provider_graph_hash": artifact["provider_graph_hash"],
        "legacy_source_status": negative.json()["items"][0]["status"],
        "legacy_source_reason": negative.json()["items"][0]["reason"],
        "standard_source_gate": resolved["reason"],
        "local_provider_count": providers.json()["total"],
        "files": [{"path": path.name, "sha256": _sha256(path)} for path in files],
    }
    _write_json(output / "manifest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
