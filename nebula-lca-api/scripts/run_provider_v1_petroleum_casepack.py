from __future__ import annotations

import argparse
import hashlib
import json
import math
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = (
    ROOT
    / "tests"
    / "fixtures"
    / "provider_cases"
    / "petroleum_inline_ef31"
    / "request.json"
)
DEFAULT_OUTPUT = ROOT.parent / "tmp" / "provider-v1-petroleum-casepack"


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_request(
    base_url: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    method: str = "GET",
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} returned {exc.code}: {body}") from exc


def _catalog_request(solve_request: dict[str, Any]) -> dict[str, Any]:
    elementary = solve_request["elementary_flows"][0]
    return {
        "schema_version": "provider.catalog.resolve.request.v1",
        "flows": [
            {
                "source_namespace": elementary["source_namespace"],
                "flow_uuid": elementary["flow_uuid"],
                "version": elementary["version"],
                "correlation_id": "casepack-co2",
            }
        ],
        "flow_properties": [
            {
                "flow_property_uuid": elementary["flow_property_uuid"],
                "version": elementary["flow_property_version"],
                "correlation_id": "casepack-mass-property",
            }
        ],
        "unit_groups": [
            {
                "unit_group_uuid": elementary["unit_group_uuid"],
                "version": elementary["unit_group_version"],
                "correlation_id": "casepack-mass-unit-group",
            }
        ],
        "units": [
            {
                "unit_group_uuid": elementary["unit_group_uuid"],
                "version": elementary["unit_group_version"],
                "unit": elementary["unit"],
                "correlation_id": "casepack-kg",
            }
        ],
        "flow_candidates": [
            {
                "query": "diesel",
                "flow_type": "Product flow",
                "unit": "kg",
                "limit": 20,
                "correlation_id": "casepack-diesel-candidates",
            }
        ],
    }


def _chart_data(result: dict[str, Any]) -> dict[str, Any]:
    activities = {
        row["process_uuid"]: row["activity_amount"]
        for row in result["activity_vector"]
    }
    scaled = {row["exchange_id"]: row for row in result["scaled_exchanges"]}
    links: list[dict[str, Any]] = []

    internal = scaled["node-cracking::out-cracked"]
    links.append(
        {
            "source": "process-cracking",
            "target": "process-desulfurization",
            "label": "Cracked diesel",
            "amount": abs(internal["scaled_amount"]),
            "unit": internal["unit"],
            "scaled_exchange_ids": [internal["exchange_id"]],
            "boundary_role": internal["boundary_role"],
        }
    )
    for exchange_id in (
        "node-cracking::out-co2-cracking",
        "node-desulfurization::out-co2-desulfurization",
    ):
        exchange = scaled[exchange_id]
        links.append(
            {
                "source": exchange["process_uuid"],
                "target": "environment-air",
                "label": "CO2 fossil",
                "amount": abs(exchange["scaled_amount"]),
                "unit": exchange["unit"],
                "scaled_exchange_ids": [exchange_id],
                "boundary_role": exchange["boundary_role"],
            }
        )
    return {
        "schema_version": "provider.casepack.sankey.v1",
        "source": "ProviderSolveResponse.scaled_exchanges",
        "nodes": [
            {"id": "process-cracking", "label": "Cracking", "activity": activities["process-cracking"]},
            {
                "id": "process-desulfurization",
                "label": "Desulfurization",
                "activity": activities["process-desulfurization"],
            },
            {"id": "environment-air", "label": "Air", "activity": None},
        ],
        "links": links,
    }


def _sankey_svg(chart: dict[str, Any]) -> str:
    positions = {
        "process-cracking": (80, 150),
        "process-desulfurization": (380, 150),
        "environment-air": (680, 150),
    }
    paths: list[str] = []
    labels: list[str] = []
    colors = ["#2589e8", "#6eafea", "#84c6ef"]
    for index, link in enumerate(chart["links"]):
        sx, sy = positions[link["source"]]
        tx, ty = positions[link["target"]]
        offset = 0 if index == 0 else (-42 if index == 1 else 42)
        start_x, end_x = sx + 150, tx
        start_y, end_y = sy + 36 + offset, ty + 36 + offset
        width = max(4.0, min(30.0, 5.0 + 13.0 * math.sqrt(link["amount"])))
        mid_x = (start_x + end_x) / 2
        path = (
            f"M {start_x} {start_y} C {mid_x} {start_y}, "
            f"{mid_x} {end_y}, {end_x} {end_y}"
        )
        paths.append(
            f'<path d="{path}" fill="none" stroke="{colors[index]}" '
            f'stroke-width="{width:.2f}" stroke-opacity="0.72"/>'
        )
        exchange_ids = ", ".join(link["scaled_exchange_ids"])
        label = escape(
            f'{link["label"]}: {link["amount"]:.6g} {link["unit"]} | {exchange_ids}'
        )
        labels.append(f'<text x="{mid_x:.0f}" y="{start_y - 16:.0f}" text-anchor="middle">{label}</text>')

    node_svg = []
    for node in chart["nodes"]:
        x, y = positions[node["id"]]
        node_svg.append(
            f'<rect x="{x}" y="{y}" width="150" height="72" rx="12" '
            'fill="#ffffff" stroke="#1778d4" stroke-width="2"/>'
        )
        node_svg.append(
            f'<text x="{x + 75}" y="{y + 42}" text-anchor="middle" '
            f'class="node">{escape(node["label"])}</text>'
        )
    return "\n".join(
        [
            '<svg xmlns="http://www.w3.org/2000/svg" width="920" height="390" viewBox="0 0 920 390">',
            '<rect width="920" height="390" fill="#f7fbff"/>',
            '<style>text{font:13px sans-serif;fill:#24445f}.node{font-size:16px;font-weight:600}</style>',
            '<text x="40" y="48" style="font-size:22px;font-weight:700">Petroleum provider v1 solved-flow Sankey</text>',
            '<text x="40" y="76">Every link cites the exact ProviderSolveResponse.scaled_exchanges ID.</text>',
            *paths,
            *node_svg,
            *labels,
            '</svg>',
        ]
    )


def _audit(
    health: dict[str, Any],
    openapi: dict[str, Any],
    catalog: dict[str, Any],
    result: dict[str, Any],
    chart: dict[str, Any],
) -> dict[str, Any]:
    exchange_ids = {row["exchange_id"] for row in result["scaled_exchanges"]}
    traced_ids = {
        exchange_id
        for link in chart["links"]
        for exchange_id in link["scaled_exchange_ids"]
    }
    inventory = {row["flow_uuid"]: row["amount"] for row in result["inventory_totals"]}
    indicators = {
        row["canonical_indicator_key"]: row
        for row in result["lcia"]["indicator_results"]
    }
    indicator_metadata = result["lcia"].get("indicator_metadata") or {}
    checks = {
        "health_ok": health.get("status") in {"ok", "healthy"},
        "openapi_has_provider_solve": "/api/provider/v1/solve" in openapi.get("paths", {}),
        "catalog_exact_items_resolved": all(item["status"] == "resolved" for item in catalog["items"]),
        "solve_completed": result["status"] == "completed",
        "activity_vector_is_a_x_equals_f": result["provenance"]["activity_vector_semantics"] == "x in A*x=f",
        "co2_inventory_is_0_7_kg": abs(inventory["08a91e70-3ddc-11dd-923d-0050c2490048"] - 0.7) < 1e-10,
        "climate_change_is_0_7": abs(indicators["climate change"]["value"] - 0.7) < 1e-10,
        "climate_change_fossil_is_0_7": abs(indicators["climate change: fossil"]["value"] - 0.7) < 1e-10,
        "indicator_units_are_complete": all(
            row.get("unit")
            and row.get("indicator_unit") == row.get("unit")
            and row.get("indicator_metadata_hash")
            for row in indicators.values()
        ),
        "climate_change_unit_is_kg_co2_eq": indicators["climate change"]["unit"] == "kg CO2-Eq",
        "indicator_metadata_is_hashed": bool(
            indicator_metadata.get("sha256")
            and indicator_metadata.get("content_hash")
            and indicator_metadata.get("indicator_count") == len(indicators)
        ),
        "elementary_receipts_present": len(result["elementary_flow_receipts"]) == 2,
        "sankey_links_trace_to_scaled_exchanges": traced_ids <= exchange_ids and bool(traced_ids),
    }
    return {
        "schema_version": "provider.casepack.audit.v1",
        "status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "run_id": result["run_id"],
        "consumer_graph_hash": result["provenance"]["consumer_graph_hash"],
        "provider_graph_hash": result["provenance"]["provider_graph_hash"],
        "database_release": result["provenance"]["database_release"],
    }


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the provider v1 petroleum EF3.1 case pack.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    solve_request = json.loads(args.input.read_text(encoding="utf-8"))
    graph_hash = _canonical_hash(solve_request["inline_snapshot"]["graph"])
    solve_request["inline_snapshot"]["graph_hash"] = graph_hash
    catalog_request = _catalog_request(solve_request)

    health = _json_request(args.base_url, "/health")
    openapi = _json_request(args.base_url, "/openapi.json")
    catalog = _json_request(
        args.base_url,
        "/api/provider/v1/catalog/resolve",
        payload=catalog_request,
        method="POST",
    )
    result = _json_request(
        args.base_url,
        "/api/provider/v1/solve",
        payload=solve_request,
        method="POST",
    )
    chart = _chart_data(result)
    audit = _audit(health, openapi, catalog, result, chart)

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "input_snapshot.json", solve_request["inline_snapshot"])
    _write_json(
        output_dir / "parameters.json",
        {
            "base_url": args.base_url,
            "demand": solve_request["demand"],
            "scenario_id": solve_request["scenario_id"],
            "operation_hash": solve_request["operation_hash"],
            "lcia_methods": solve_request["lcia_methods"],
            "elementary_flows": solve_request["elementary_flows"],
            "catalog_request": catalog_request,
        },
    )
    _write_json(output_dir / "catalog_result.json", catalog)
    _write_json(output_dir / "raw_result.json", result)
    _write_json(output_dir / "provenance.json", result["provenance"])
    _write_json(output_dir / "chart_data.json", chart)
    _write_json(output_dir / "audit_report.json", audit)
    (output_dir / "sankey.svg").write_text(_sankey_svg(chart), encoding="utf-8")

    manifest_lines = []
    for path in sorted(output_dir.iterdir(), key=lambda item: item.name):
        if path.is_file() and path.name != "manifest.sha256":
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest_lines.append(f"{digest}  {path.name}")
    (output_dir / "manifest.sha256").write_text("\n".join(manifest_lines) + "\n", encoding="utf-8")

    print(json.dumps({"status": audit["status"], "output_dir": str(output_dir)}, ensure_ascii=False))
    return 0 if audit["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
