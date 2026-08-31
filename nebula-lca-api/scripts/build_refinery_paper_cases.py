from __future__ import annotations

import argparse
import hashlib
import json
import math
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "tmp" / "refinery-paper-cases"

MASS_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
MASS_PROPERTY_VERSION = "03.00.003"
MASS_GROUP_UUID = "93a60a57-a4c8-11da-a746-0800200c9a66"
MASS_GROUP_VERSION = "03.00.003"
MASS_GROUP_NAME = "Units of mass"
ENERGY_PROPERTY_UUID = "b269a229-13ff-5919-b9ae-42167a693e16"
ENERGY_PROPERTY_VERSION = "benchmark-1"
ENERGY_GROUP_UUID = "25c4989f-fc43-526b-b8d3-4c7091d01e9b"
ENERGY_GROUP_VERSION = "benchmark-1"
ENERGY_GROUP_NAME = "Units of energy"
CO2_UUID = "08a91e70-3ddc-11dd-923d-0050c2490048"
CO2_VERSION = "03.00.004"
CUSTOM_NAMESPACE = "nebula.refinery-paper-benchmark"
CUSTOM_VERSION = "benchmark-1"


FLOW_SPECS = {
    "crude": "Crude oil feed",
    "naphtha": "Straight-run naphtha",
    "diesel_cut": "Atmospheric diesel fraction",
    "residue": "Atmospheric residue",
    "makeup_h2": "Make-up hydrogen",
    "hydrotreated_naphtha": "Hydrotreated naphtha",
    "reformate": "Reformate",
    "byproduct_h2": "Reformer by-product hydrogen",
    "blendstock": "External gasoline blendstock",
    "gasoline": "Finished gasoline",
    "electricity": "Refinery electricity supply",
    "recycle_naphtha": "Recycle naphtha",
}


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _request(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
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
        with opener.open(request, timeout=180) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} returned {exc.code}: {body}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _custom_port(
    flow_uuid: str,
    name: str,
    amount: float,
    *,
    port_id: str,
    direction: str,
    is_product: bool = False,
    allocation_factor: float | None = None,
    unit: str = "kg",
    unit_group: str = MASS_GROUP_NAME,
    flow_property_uuid: str = MASS_PROPERTY_UUID,
    flow_property_version: str = MASS_PROPERTY_VERSION,
    unit_group_uuid: str = MASS_GROUP_UUID,
    unit_group_version: str = MASS_GROUP_VERSION,
) -> dict[str, Any]:
    port = {
        "id": port_id,
        "flowUuid": flow_uuid,
        "flowSourceNamespace": CUSTOM_NAMESPACE,
        "flowVersion": CUSTOM_VERSION,
        "flowPropertyUuid": flow_property_uuid,
        "flowPropertyVersion": flow_property_version,
        "unitGroupUuid": unit_group_uuid,
        "unitGroupVersion": unit_group_version,
        "name": name,
        "unit": unit,
        "unitGroup": unit_group,
        "amount": amount,
        "type": "technosphere",
        "direction": direction,
    }
    if is_product:
        port["isProduct"] = True
        port["allocationBasis"] = {"method": "quantity"}
    if allocation_factor is not None:
        port["allocationFactor"] = allocation_factor
    return port


def _co2_port(node_key: str, amount: float) -> dict[str, Any]:
    return {
        "id": f"em-co2-{node_key}",
        "flowUuid": CO2_UUID,
        "flowSourceNamespace": "tiangong_open_data",
        "flowVersion": CO2_VERSION,
        "flowPropertyUuid": MASS_PROPERTY_UUID,
        "flowPropertyVersion": MASS_PROPERTY_VERSION,
        "unitGroupUuid": MASS_GROUP_UUID,
        "unitGroupVersion": MASS_GROUP_VERSION,
        "name": "carbon dioxide (fossil)",
        "unit": "kg",
        "unitGroup": MASS_GROUP_NAME,
        "amount": amount,
        "type": "biosphere",
        "direction": "output",
    }


def _edge(
    edge_id: str,
    source_node: str,
    source_port: str,
    target_node: str,
    target_port: str,
    flow_uuid: str,
    flow_name: str,
    amount: float,
    *,
    quantity_mode: str = "single",
    unit: str = "kg",
) -> dict[str, Any]:
    return {
        "id": edge_id,
        "fromNode": source_node,
        "toNode": target_node,
        "sourceHandle": f"out:{source_port}",
        "targetHandle": f"in:{target_port}",
        "flowUuid": flow_uuid,
        "flowName": flow_name,
        "quantityMode": quantity_mode,
        "amount": amount,
        "providerAmount": amount,
        "consumerAmount": amount,
        "unit": unit,
        "type": "technosphere",
        "allocation": "none",
    }


def build_processes(
    flows: dict[str, str],
    *,
    include_electricity: bool = False,
    include_recycle: bool = False,
) -> dict[str, dict[str, Any]]:
    electricity_supply = {
        "id": "node-electricity-supply",
        "node_kind": "market_process",
        "mode": "normalized",
        "process_uuid": "market_refinery_electricity_supply",
        "name": "Refinery electricity supply chain",
        "location": "CN",
        "reference_product": FLOW_SPECS["electricity"],
        "inputs": [],
        "outputs": [
            _custom_port(
                flows["electricity"], FLOW_SPECS["electricity"], 1.0,
                port_id="out-electricity", direction="output", is_product=True,
                unit="MJ", unit_group=ENERGY_GROUP_NAME,
                flow_property_uuid=ENERGY_PROPERTY_UUID,
                flow_property_version=ENERGY_PROPERTY_VERSION,
                unit_group_uuid=ENERGY_GROUP_UUID,
                unit_group_version=ENERGY_GROUP_VERSION,
            )
        ],
        "emissions": [_co2_port("electricity-supply", 0.004)],
    }
    distillation = {
        "id": "node-distillation",
        "node_kind": "unit_process",
        "mode": "balanced",
        "process_uuid": "paper-refinery-distillation",
        "name": "Atmospheric distillation",
        "location": "CN",
        "reference_product": FLOW_SPECS["naphtha"],
        "allocation_method": "unit_group_physical_v1",
        "inputs": [
            _custom_port(
                flows["crude"], FLOW_SPECS["crude"], 4.2,
                port_id="in-crude", direction="input",
            )
        ],
        "outputs": [
            _custom_port(
                flows["naphtha"], FLOW_SPECS["naphtha"], 1.05,
                port_id="out-naphtha", direction="output", is_product=True,
                allocation_factor=0.25,
            ),
            _custom_port(
                flows["diesel_cut"], FLOW_SPECS["diesel_cut"], 1.89,
                port_id="out-diesel-cut", direction="output", is_product=True,
                allocation_factor=0.45,
            ),
            _custom_port(
                flows["residue"], FLOW_SPECS["residue"], 1.26,
                port_id="out-residue", direction="output", is_product=True,
                allocation_factor=0.30,
            ),
        ],
        "emissions": [_co2_port("distillation", 0.08)],
    }
    hydrotreating = {
        "id": "node-hydrotreating",
        "node_kind": "unit_process",
        "mode": "balanced",
        "process_uuid": "paper-refinery-hydrotreating",
        "name": "Naphtha hydrotreating",
        "location": "CN",
        "reference_product": FLOW_SPECS["hydrotreated_naphtha"],
        "inputs": [
            _custom_port(
                flows["naphtha"], FLOW_SPECS["naphtha"], 1.05,
                port_id="in-naphtha", direction="input",
            ),
            _custom_port(
                flows["makeup_h2"], FLOW_SPECS["makeup_h2"], 0.02,
                port_id="in-makeup-h2", direction="input",
            ),
        ],
        "outputs": [
            _custom_port(
                flows["hydrotreated_naphtha"], FLOW_SPECS["hydrotreated_naphtha"], 1.0,
                port_id="out-hydrotreated", direction="output", is_product=True,
            )
        ],
        "emissions": [_co2_port("hydrotreating", 0.03)],
    }
    reforming = {
        "id": "node-reforming",
        "node_kind": "unit_process",
        "mode": "balanced",
        "process_uuid": "paper-refinery-reforming",
        "name": "Catalytic reforming",
        "location": "CN",
        "reference_product": FLOW_SPECS["reformate"],
        "inputs": [
            _custom_port(
                flows["hydrotreated_naphtha"], FLOW_SPECS["hydrotreated_naphtha"], 1.0,
                port_id="in-hydrotreated", direction="input",
            )
        ],
        "outputs": [
            _custom_port(
                flows["reformate"], FLOW_SPECS["reformate"], 1.0,
                port_id="out-reformate", direction="output", is_product=True,
            ),
            _custom_port(
                flows["byproduct_h2"], FLOW_SPECS["byproduct_h2"], 0.03,
                port_id="out-byproduct-h2", direction="output",
            ),
        ],
        "emissions": [_co2_port("reforming", 0.05)],
    }
    reforming["outputs"][0]["externalSaleAmount"] = 0.20
    blending = {
        "id": "node-blending",
        "node_kind": "unit_process",
        "mode": "balanced",
        "process_uuid": "paper-refinery-blending",
        "name": "Gasoline blending",
        "location": "CN",
        "reference_product": FLOW_SPECS["gasoline"],
        "inputs": [
            _custom_port(
                flows["reformate"], FLOW_SPECS["reformate"], 0.80,
                port_id="in-reformate", direction="input",
            ),
            _custom_port(
                flows["blendstock"], FLOW_SPECS["blendstock"], 0.20,
                port_id="in-blendstock", direction="input",
            ),
        ],
        "outputs": [
            _custom_port(
                flows["gasoline"], FLOW_SPECS["gasoline"], 1.0,
                port_id="out-gasoline", direction="output", is_product=True,
            )
        ],
        "emissions": [_co2_port("blending", 0.01)],
    }
    if include_electricity:
        electricity_amounts = {
            "distillation": 8.0,
            "hydrotreating": 3.0,
            "reforming": 2.0,
            "blending": 0.5,
        }
        for key, node in {
            "distillation": distillation,
            "hydrotreating": hydrotreating,
            "reforming": reforming,
            "blending": blending,
        }.items():
            node["inputs"].append(
                _custom_port(
                    flows["electricity"], FLOW_SPECS["electricity"], electricity_amounts[key],
                    port_id="in-electricity", direction="input",
                    unit="MJ", unit_group=ENERGY_GROUP_NAME,
                    flow_property_uuid=ENERGY_PROPERTY_UUID,
                    flow_property_version=ENERGY_PROPERTY_VERSION,
                    unit_group_uuid=ENERGY_GROUP_UUID,
                    unit_group_version=ENERGY_GROUP_VERSION,
                )
            )

    if include_recycle:
        naphtha_input = next(port for port in hydrotreating["inputs"] if port["id"] == "in-naphtha")
        naphtha_input["amount"] = 0.80
        hydrotreating["inputs"].append(
            _custom_port(
                flows["recycle_naphtha"], FLOW_SPECS["recycle_naphtha"], 0.20,
                port_id="in-recycle-naphtha", direction="input",
            )
        )
        distillation["outputs"][0]["externalSaleAmount"] = 0.25
        reforming["allocation_method"] = "unit_group_physical_v1"
        reformate_output = next(port for port in reforming["outputs"] if port["id"] == "out-reformate")
        reformate_output["amount"] = 0.80
        reformate_output["allocationFactor"] = 0.80
        reformate_output["allocationBasis"] = {"method": "quantity"}
        reformate_output.pop("externalSaleAmount", None)
        reforming["outputs"].append(
            _custom_port(
                flows["recycle_naphtha"], FLOW_SPECS["recycle_naphtha"], 0.20,
                port_id="out-recycle-naphtha", direction="output", is_product=True,
                allocation_factor=0.20,
            )
        )

    processes = {
        "distillation": distillation,
        "hydrotreating": hydrotreating,
        "reforming": reforming,
        "blending": blending,
    }
    if include_electricity:
        return {"electricity_supply": electricity_supply, **processes}
    return processes


def _metadata(target_key: str, flows: dict[str, str], stage: str) -> dict[str, Any]:
    return {
        "database_release": "EF3.1",
        "case_family": "refinery-inspired-hypothetical-benchmark-v1",
        "case_stage": stage,
        "data_status": "hypothetical quantities informed by refinery process structure",
        "functional_unit": {
            "display_text": f"1 kg {FLOW_SPECS[target_key]}",
            "amount": 1.0,
            "flow_uuid": flows[target_key],
            "flow_source_namespace": CUSTOM_NAMESPACE,
            "flow_version": CUSTOM_VERSION,
            "unit": "kg",
            "unit_group_uuid": MASS_GROUP_UUID,
            "unit_group_version": MASS_GROUP_VERSION,
        },
    }


def build_progressive_graphs(flows: dict[str, str]) -> dict[str, dict[str, Any]]:
    base = build_processes(flows)
    mixed = build_processes(flows, include_electricity=True)
    recycle = build_processes(flows, include_electricity=True, include_recycle=True)

    def foreground_edges(processes: dict[str, dict[str, Any]], *, recycle_enabled: bool) -> list[dict[str, Any]]:
        naphtha_amount = 0.80 if recycle_enabled else 1.05
        edges = [
            _edge(
                "edge-naphtha",
                "node-distillation", "out-naphtha",
                "node-hydrotreating", "in-naphtha",
                flows["naphtha"], FLOW_SPECS["naphtha"], naphtha_amount,
            ),
            _edge(
                "edge-hydrotreated",
                "node-hydrotreating", "out-hydrotreated",
                "node-reforming", "in-hydrotreated",
                flows["hydrotreated_naphtha"], FLOW_SPECS["hydrotreated_naphtha"], 1.0,
            ),
            _edge(
                "edge-reformate",
                "node-reforming", "out-reformate",
                "node-blending", "in-reformate",
                flows["reformate"], FLOW_SPECS["reformate"], 0.80,
            ),
        ]
        if "electricity_supply" in processes:
            for key in ("distillation", "hydrotreating", "reforming", "blending"):
                target = processes[key]
                port = next(row for row in target["inputs"] if row["id"] == "in-electricity")
                edges.append(
                    _edge(
                        f"edge-electricity-{key}",
                        "node-electricity-supply", "out-electricity",
                        target["id"], "in-electricity",
                        flows["electricity"], FLOW_SPECS["electricity"], float(port["amount"]),
                        quantity_mode="dual", unit="MJ",
                    )
                )
        if recycle_enabled:
            edges.append(
                _edge(
                    "edge-recycle-naphtha",
                    "node-reforming", "out-recycle-naphtha",
                    "node-hydrotreating", "in-recycle-naphtha",
                    flows["recycle_naphtha"], FLOW_SPECS["recycle_naphtha"], 0.20,
                )
            )
        return edges

    return {
        "case_01_unit_process": {
            "functionalUnit": f"1 kg {FLOW_SPECS['naphtha']}",
            "nodes": [base["distillation"]],
            "exchanges": [],
            "metadata": _metadata("naphtha", flows, "case_01_unit_process"),
        },
        "case_02_balanced_chain": {
            "functionalUnit": f"1 kg {FLOW_SPECS['gasoline']}",
            "nodes": list(base.values()),
            "exchanges": foreground_edges(base, recycle_enabled=False),
            "metadata": _metadata("gasoline", flows, "case_02_balanced_chain"),
        },
        "case_03_mixed_normalized_supply": {
            "functionalUnit": f"1 kg {FLOW_SPECS['gasoline']}",
            "nodes": list(mixed.values()),
            "exchanges": foreground_edges(mixed, recycle_enabled=False),
            "metadata": _metadata("gasoline", flows, "case_03_mixed_normalized_supply"),
        },
        "case_04_recycle_loop": {
            "functionalUnit": f"1 kg {FLOW_SPECS['gasoline']}",
            "nodes": list(recycle.values()),
            "exchanges": foreground_edges(recycle, recycle_enabled=True),
            "metadata": _metadata("gasoline", flows, "case_04_recycle_loop"),
        },
    }


def build_pts_compile_graph(flows: dict[str, str]) -> dict[str, Any]:
    processes = build_processes(flows, include_electricity=True, include_recycle=True)
    pts_uuid = "paper-refinery-upgrading-pts"
    shell = {
        "id": "node-upgrading-pts",
        "node_kind": "pts_module",
        "mode": "normalized",
        "pts_uuid": pts_uuid,
        "process_uuid": pts_uuid,
        "name": "Naphtha upgrading PTS",
        "location": "CN",
        "reference_product": FLOW_SPECS["reformate"],
        "inputs": [
            _custom_port(
                flows["naphtha"], FLOW_SPECS["naphtha"], 0.80,
                port_id="in-naphtha", direction="input",
            ),
            _custom_port(
                flows["makeup_h2"], FLOW_SPECS["makeup_h2"], 0.02,
                port_id="in-makeup-h2", direction="input",
            ),
            _custom_port(
                flows["electricity"], FLOW_SPECS["electricity"], 5.0,
                port_id="in-electricity", direction="input",
                unit="MJ", unit_group=ENERGY_GROUP_NAME,
                flow_property_uuid=ENERGY_PROPERTY_UUID,
                flow_property_version=ENERGY_PROPERTY_VERSION,
                unit_group_uuid=ENERGY_GROUP_UUID,
                unit_group_version=ENERGY_GROUP_VERSION,
            ),
        ],
        "outputs": [
            _custom_port(
                flows["reformate"], FLOW_SPECS["reformate"], 0.80,
                port_id="out-reformate", direction="output", is_product=True,
            ),
            _custom_port(
                flows["byproduct_h2"], FLOW_SPECS["byproduct_h2"], 0.03,
                port_id="out-byproduct-h2", direction="output", is_product=True,
            ),
        ],
        "emissions": [],
    }
    internal_edge = _edge(
        "edge-hydrotreated",
        "node-hydrotreating", "out-hydrotreated",
        "node-reforming", "in-hydrotreated",
        flows["hydrotreated_naphtha"], FLOW_SPECS["hydrotreated_naphtha"], 1.0,
    )
    recycle_edge = _edge(
        "edge-recycle-naphtha",
        "node-reforming", "out-recycle-naphtha",
        "node-hydrotreating", "in-recycle-naphtha",
        flows["recycle_naphtha"], FLOW_SPECS["recycle_naphtha"], 0.20,
    )
    graph = {
        "functionalUnit": "Naphtha upgrading PTS",
        "nodes": [shell],
        "exchanges": [],
        "metadata": {
            "database_release": "EF3.1",
            "canvases": [
                {
                    "id": "canvas-upgrading-pts",
                    "kind": "pts_internal",
                    "parentPtsNodeId": shell["id"],
                    "name": "Naphtha upgrading internal graph",
                    "nodes": [processes["hydrotreating"], processes["reforming"]],
                    "edges": [internal_edge, recycle_edge],
                }
            ],
        },
    }
    for node in [shell, processes["hydrotreating"], processes["reforming"]]:
        for port in [*node.get("inputs", []), *node.get("outputs", []), *node.get("emissions", [])]:
            # PTS compilation resolves current catalog rows. The custom-flow API
            # does not create exact FlowVersion snapshots, and the bundled EF
            # catalog is likewise current-row based in this isolated database.
            port.pop("flowSourceNamespace", None)
            port.pop("flowVersion", None)
    return graph


def build_pts_main_graph(
    flows: dict[str, str],
    shell_node: dict[str, Any],
    pts_compile_graph: dict[str, Any],
) -> dict[str, Any]:
    processes = build_processes(flows, include_electricity=True, include_recycle=True)
    shell = json.loads(json.dumps(shell_node))
    shell["id"] = "node-upgrading-pts"
    shell["pts_uuid"] = "paper-refinery-upgrading-pts"
    shell["process_uuid"] = "paper-refinery-upgrading-pts"

    outer_nodes = [processes["electricity_supply"], processes["distillation"], shell, processes["blending"]]
    for node in outer_nodes:
        for port in [*node.get("inputs", []), *node.get("outputs", []), *node.get("emissions", [])]:
            if port.get("flowUuid") != CO2_UUID:
                port.pop("flowSourceNamespace", None)
                port.pop("flowVersion", None)

    naphtha_input = next(
        port for port in shell.get("inputs", [])
        if port.get("flowUuid") == flows["naphtha"]
    )
    reformate_output = next(
        port for port in shell.get("outputs", [])
        if port.get("flowUuid") == flows["reformate"]
    )
    electricity_input = next(
        port for port in shell.get("inputs", [])
        if port.get("flowUuid") == flows["electricity"]
    )
    edges = [
        _edge(
            "edge-electricity-distillation",
            "node-electricity-supply", "out-electricity",
            "node-distillation", "in-electricity",
            flows["electricity"], FLOW_SPECS["electricity"], 8.0,
            quantity_mode="dual", unit="MJ",
        ),
        _edge(
            "edge-electricity-pts",
            "node-electricity-supply", "out-electricity",
            shell["id"], str(electricity_input["id"]),
            flows["electricity"], FLOW_SPECS["electricity"], 5.0,
            quantity_mode="dual", unit="MJ",
        ),
        _edge(
            "edge-electricity-blending",
            "node-electricity-supply", "out-electricity",
            "node-blending", "in-electricity",
            flows["electricity"], FLOW_SPECS["electricity"], 0.5,
            quantity_mode="dual", unit="MJ",
        ),
        _edge(
            "edge-naphtha-to-pts",
            "node-distillation", "out-naphtha",
            shell["id"], str(naphtha_input["id"]),
            flows["naphtha"], FLOW_SPECS["naphtha"], 0.80,
            quantity_mode="dual",
        ),
        _edge(
            "edge-pts-to-blending",
            shell["id"], str(reformate_output["id"]),
            "node-blending", "in-reformate",
            flows["reformate"], FLOW_SPECS["reformate"], 0.80,
            quantity_mode="dual",
        ),
    ]
    metadata = _metadata("gasoline", flows, "case_05_pts_compiled")
    metadata["canvases"] = pts_compile_graph["metadata"]["canvases"]
    return {
        "functionalUnit": f"1 kg {FLOW_SPECS['gasoline']}",
        "nodes": outer_nodes,
        "exchanges": edges,
        "metadata": metadata,
    }


def _elementary_refs(graph: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for node in graph.get("nodes", []):
        for port in [*node.get("inputs", []), *node.get("outputs", []), *node.get("emissions", [])]:
            if port.get("type") != "biosphere":
                continue
            refs.append(
                {
                    "exchange_id": f"{node['id']}::{port['id']}",
                    "source_namespace": "tiangong_open_data",
                    "flow_uuid": port["flowUuid"],
                    "version": port.get("flowVersion") or CO2_VERSION,
                    "flow_property_uuid": port.get("flowPropertyUuid") or MASS_PROPERTY_UUID,
                    "flow_property_version": port.get("flowPropertyVersion") or MASS_PROPERTY_VERSION,
                    "unit_group_uuid": port.get("unitGroupUuid") or MASS_GROUP_UUID,
                    "unit_group_version": port.get("unitGroupVersion") or MASS_GROUP_VERSION,
                    "unit": port["unit"],
                    "direction": port["direction"],
                    "compartment": "Emissions to air, unspecified",
                }
            )
    return refs


def _solve_project(
    base_url: str,
    *,
    project_id: str,
    version: int,
    graph: dict[str, Any],
    target_process_uuid: str,
    scenario_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    snapshot = _request(
        base_url,
        f"/api/provider/v1/models/{urllib.parse.quote(project_id)}/versions/{version}/snapshot",
    )
    request_payload = {
        "schema_version": "provider.solve.request.v1",
        "snapshot_ref": {
            "project_id": project_id,
            "version": version,
            "graph_hash": snapshot["graph_hash"],
        },
        "demand": [
            {
                "process_uuid": target_process_uuid,
                "amount": 1.0,
                "unit": "kg",
            }
        ],
        "scenario_id": scenario_id,
        "operation_hash": _canonical_hash(
            {"scenario_id": scenario_id, "graph_hash": snapshot["graph_hash"]}
        ),
        "lcia_methods": ["EF v3.1"],
        "elementary_flows": _elementary_refs(graph),
    }
    return snapshot, _request(
        base_url,
        "/api/provider/v1/solve",
        method="POST",
        payload=request_payload,
    )


def _connection_residuals(
    graph: dict[str, Any],
    solve_result: dict[str, Any],
) -> list[dict[str, Any]]:
    scaled = {row["exchange_id"]: row for row in solve_result["scaled_exchanges"]}
    activities = {
        row["process_uuid"]: float(row["activity_amount"])
        for row in solve_result["activity_vector"]
    }
    node_by_id = {node["id"]: node for node in graph.get("nodes", [])}
    rows: list[dict[str, Any]] = []
    expected_by_provider: dict[str, float] = {}
    for edge in graph.get("exchanges", []):
        source_port = str(edge["sourceHandle"]).split(":", 1)[-1]
        target_port = str(edge["targetHandle"]).split(":", 1)[-1]
        source_id = f"{edge['fromNode']}::{source_port}"
        target_id = f"{edge['toNode']}::{target_port}"
        source_node = node_by_id[edge["fromNode"]]
        source_process_uuid = source_node["process_uuid"]
        target_amount = abs(float(scaled[target_id]["scaled_amount"]))
        product_ports = [port for port in source_node.get("outputs", []) if port.get("isProduct")]
        selected_port = next((port for port in product_ports if port["id"] == source_port), None)
        allocation_scale = 1.0
        if selected_port is not None and product_ports:
            total_product_amount = sum(max(float(port.get("amount") or 0.0), 0.0) for port in product_ports)
            if total_product_amount > 0:
                baseline_fraction = float(selected_port.get("amount") or 0.0) / total_product_amount
                allocation_fraction = float(selected_port.get("allocationFactor") or baseline_fraction)
                if baseline_fraction > 0:
                    allocation_scale = allocation_fraction / baseline_fraction
        provider_requirement = target_amount * allocation_scale
        expected_by_provider[source_process_uuid] = (
            expected_by_provider.get(source_process_uuid, 0.0) + provider_requirement
        )
        rows.append(
            {
                "edge_id": edge["id"],
                "flow_uuid": edge["flowUuid"],
                "source_exchange_id": source_id,
                "target_exchange_id": target_id,
                "source_process_uuid": source_process_uuid,
                "source_activity_amount": activities[source_process_uuid],
                "provider_allocation_scale": allocation_scale,
                "provider_requirement_contribution": provider_requirement,
                "source_raw_exchange_scaled_amount": abs(float(scaled[source_id]["scaled_amount"])),
                "target_scaled_amount": target_amount,
                "unit": edge["unit"],
            }
        )
    for row in rows:
        process_uuid = row["source_process_uuid"]
        expected = expected_by_provider[process_uuid]
        actual = abs(activities[process_uuid])
        row["provider_aggregate_expected_activity"] = expected
        row["provider_aggregate_actual_activity"] = actual
        row["absolute_residual"] = abs(actual - expected)
    return rows


def _audit_case(
    graph: dict[str, Any],
    solve_result: dict[str, Any],
    *,
    expected_process_count: int,
) -> dict[str, Any]:
    residuals = _connection_residuals(graph, solve_result)
    finite = all(
        math.isfinite(float(row["activity_amount"]))
        for row in solve_result["activity_vector"]
    ) and all(
        math.isfinite(float(row["scaled_amount"]))
        for row in solve_result["scaled_exchanges"]
    )
    allocation_factors = []
    for node in graph.get("nodes", []):
        product_ports = [port for port in node.get("outputs", []) if port.get("isProduct")]
        factors = [port.get("allocationFactor") for port in product_ports]
        if product_ports and all(value is not None for value in factors):
            allocation_factors.append(
                {
                    "process_uuid": node["process_uuid"],
                    "sum": sum(float(value) for value in factors),
                }
            )
    checks = {
        "solve_completed": solve_result.get("status") == "completed",
        "activity_vector_semantics": (
            solve_result.get("provenance", {}).get("activity_vector_semantics") == "x in A*x=f"
        ),
        "expected_process_count": len(solve_result.get("activity_vector", [])) == expected_process_count,
        "finite_numeric_outputs": finite,
        "connection_residuals_within_tolerance": all(
            float(row["absolute_residual"]) <= 1e-10 for row in residuals
        ),
        "declared_allocation_factors_close": all(
            abs(float(row["sum"]) - 1.0) <= 1e-12 for row in allocation_factors
        ),
        "lcia_present": bool(solve_result.get("lcia", {}).get("indicator_results")),
    }
    return {
        "schema_version": "nebula.refinery-paper-case.audit.v1",
        "status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "connection_residuals": residuals,
        "allocation_factor_sums": allocation_factors,
        "run_id": solve_result.get("run_id"),
        "consumer_graph_hash": solve_result.get("provenance", {}).get("consumer_graph_hash"),
        "provider_graph_hash": solve_result.get("provenance", {}).get("provider_graph_hash"),
    }


def _compare_recycle_solve(
    open_loop: dict[str, Any],
    recycle_loop: dict[str, Any],
) -> dict[str, Any]:
    def activities(result: dict[str, Any]) -> dict[str, float]:
        return {
            str(row["process_uuid"]): float(row["activity_amount"])
            for row in result.get("activity_vector", [])
        }

    def boundary(result: dict[str, Any]) -> dict[str, float]:
        totals: dict[str, float] = {}
        for row in result.get("scaled_exchanges", []):
            if row.get("boundary_role") != "boundary":
                continue
            key = f"{row.get('exchange_type')}::{row.get('flow_uuid')}::{row.get('unit')}"
            totals[key] = totals.get(key, 0.0) + float(row.get("scaled_amount") or 0.0)
        return totals

    open_activities = activities(open_loop)
    recycle_activities = activities(recycle_loop)
    open_boundary = boundary(open_loop)
    recycle_boundary = boundary(recycle_loop)
    activity_keys = sorted(set(open_activities) | set(recycle_activities))
    boundary_keys = sorted(set(open_boundary) | set(recycle_boundary))
    activity_rows = [
        {
            "process_uuid": key,
            "open_loop": open_activities.get(key, 0.0),
            "recycle_loop": recycle_activities.get(key, 0.0),
            "difference": recycle_activities.get(key, 0.0) - open_activities.get(key, 0.0),
        }
        for key in activity_keys
    ]
    boundary_rows = [
        {
            "identity": key,
            "open_loop": open_boundary.get(key, 0.0),
            "recycle_loop": recycle_boundary.get(key, 0.0),
            "difference": recycle_boundary.get(key, 0.0) - open_boundary.get(key, 0.0),
        }
        for key in boundary_keys
    ]
    tolerance = 1e-12
    return {
        "schema_version": "nebula.refinery-paper-case.recycle-comparison.v1",
        "open_loop_run_id": open_loop.get("run_id"),
        "recycle_loop_run_id": recycle_loop.get("run_id"),
        "activity_vector": activity_rows,
        "boundary_exchanges": boundary_rows,
        "activity_vector_changed": any(abs(row["difference"]) > tolerance for row in activity_rows),
        "boundary_exchanges_changed": any(abs(row["difference"]) > tolerance for row in boundary_rows),
        "tolerance": tolerance,
    }


def _sankey_data(graph: dict[str, Any], solve_result: dict[str, Any]) -> dict[str, Any]:
    scaled = {row["exchange_id"]: row for row in solve_result["scaled_exchanges"]}
    activities = {
        row["process_uuid"]: float(row["activity_amount"])
        for row in solve_result["activity_vector"]
    }
    nodes = [
        {
            "id": node["process_uuid"],
            "label": node["name"],
            "activity": activities.get(node["process_uuid"]),
        }
        for node in graph.get("nodes", [])
    ]
    links = []
    process_uuid_by_node = {node["id"]: node["process_uuid"] for node in graph.get("nodes", [])}
    for edge in graph.get("exchanges", []):
        source_port = str(edge["sourceHandle"]).split(":", 1)[-1]
        target_port = str(edge["targetHandle"]).split(":", 1)[-1]
        source_exchange_id = f"{edge['fromNode']}::{source_port}"
        target_exchange_id = f"{edge['toNode']}::{target_port}"
        links.append(
            {
                "source": process_uuid_by_node[edge["fromNode"]],
                "target": process_uuid_by_node[edge["toNode"]],
                "label": edge["flowName"],
                "amount": abs(float(scaled[target_exchange_id]["scaled_amount"])),
                "unit": edge["unit"],
                "scaled_exchange_ids": [source_exchange_id, target_exchange_id],
                "boundary_role": scaled[target_exchange_id]["boundary_role"],
            }
        )
    environment_id = "environment-air"
    elementary = [
        row for row in solve_result["scaled_exchanges"]
        if row["exchange_type"] == "elementary" and row["boundary_role"] == "boundary"
    ]
    if elementary:
        nodes.append({"id": environment_id, "label": "Air", "activity": None})
    for row in elementary:
        links.append(
            {
                "source": row["process_uuid"],
                "target": environment_id,
                "label": "CO2 fossil",
                "amount": abs(float(row["scaled_amount"])),
                "unit": row["unit"],
                "scaled_exchange_ids": [row["exchange_id"]],
                "boundary_role": row["boundary_role"],
            }
        )
    return {
        "schema_version": "nebula.refinery-paper-case.sankey.v1",
        "source": "ProviderSolveResponse.scaled_exchanges",
        "nodes": nodes,
        "links": links,
    }


def _sankey_svg(chart: dict[str, Any], title: str) -> str:
    node_rows = chart["nodes"]
    positions = {
        row["id"]: (80 + index * 220, 150 if row["id"] != "environment-air" else 330)
        for index, row in enumerate(node_rows)
    }
    paths: list[str] = []
    labels: list[str] = []
    for index, link in enumerate(chart["links"]):
        sx, sy = positions[link["source"]]
        tx, ty = positions[link["target"]]
        start_x, end_x = sx + 150, tx
        start_y = sy + 32 + ((index % 3) - 1) * 18
        end_y = ty + 32
        width = max(3.0, min(24.0, 4.0 + 10.0 * math.sqrt(max(link["amount"], 0.0))))
        mid_x = (start_x + end_x) / 2
        paths.append(
            f'<path d="M {start_x} {start_y} C {mid_x} {start_y}, {mid_x} {end_y}, {end_x} {end_y}" '
            f'fill="none" stroke="#2386df" stroke-width="{width:.2f}" stroke-opacity="0.65"/>'
        )
        labels.append(
            f'<text x="{mid_x:.0f}" y="{min(start_y, end_y) - 10:.0f}" text-anchor="middle">'
            f'{escape(link["label"])}: {link["amount"]:.5g} {escape(link["unit"])}</text>'
        )
    node_svg: list[str] = []
    for row in node_rows:
        x, y = positions[row["id"]]
        node_svg.append(
            f'<rect x="{x}" y="{y}" width="150" height="64" rx="10" fill="#fff" stroke="#1778d4" stroke-width="2"/>'
        )
        node_svg.append(
            f'<text x="{x + 75}" y="{y + 38}" text-anchor="middle" class="node">{escape(row["label"])}</text>'
        )
    width = max(980, 260 + len(node_rows) * 220)
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="480" viewBox="0 0 {width} 480">',
            f'<rect width="{width}" height="480" fill="#f7fbff"/>',
            '<style>text{font:12px sans-serif;fill:#24445f}.node{font-size:14px;font-weight:600}</style>',
            f'<text x="40" y="42" style="font-size:21px;font-weight:700">{escape(title)}</text>',
            '<text x="40" y="68">All links cite ProviderSolveResponse.scaled_exchanges.</text>',
            *paths,
            *node_svg,
            *labels,
            '</svg>',
        ]
    )


def _write_case_pack(
    case_dir: Path,
    *,
    project: dict[str, Any],
    parent_project_id: str | None,
    version_response: dict[str, Any],
    graph: dict[str, Any],
    snapshot: dict[str, Any] | None = None,
    solve_result: dict[str, Any] | None = None,
    audit: dict[str, Any] | None = None,
) -> None:
    _write_json(case_dir / "project.json", {**project, "parent_project_id": parent_project_id})
    _write_json(case_dir / "version.json", version_response)
    _write_json(case_dir / "input_graph.json", graph)
    if snapshot is not None:
        _write_json(case_dir / "provider_snapshot.json", snapshot)
    if solve_result is not None:
        _write_json(case_dir / "raw_result.json", solve_result)
        _write_json(case_dir / "provenance.json", solve_result.get("provenance", {}))
        chart = _sankey_data(graph, solve_result)
        _write_json(case_dir / "chart_data.json", chart)
        (case_dir / "sankey.svg").write_text(
            _sankey_svg(chart, project["name"]),
            encoding="utf-8",
        )
    if audit is not None:
        _write_json(case_dir / "audit_report.json", audit)


def _create_custom_flows(base_url: str) -> tuple[dict[str, str], list[dict[str, Any]]]:
    ids: dict[str, str] = {}
    receipts: list[dict[str, Any]] = []
    for key, name in FLOW_SPECS.items():
        is_energy = key == "electricity"
        response = _request(
            base_url,
            "/api/flows",
            method="POST",
            payload={
                "flow_name": name,
                "flow_name_en": name,
                "flow_type": "product_flow",
                "unitGroupUuid": ENERGY_GROUP_NAME if is_energy else MASS_GROUP_NAME,
                "default_unit": "MJ" if is_energy else "kg",
                "confirmCreate": True,
                "sourcePolicy": "open_mixed",
            },
        )
        flow = response["flow"]
        ids[key] = flow["flow_uuid"]
        receipts.append(flow)
    return ids, receipts


def _create_project(base_url: str, name: str, graph: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    project = _request(
        base_url,
        "/api/projects",
        method="POST",
        payload={
            "name": name,
            "reference_product": graph["metadata"]["functional_unit"]["display_text"].removeprefix("1 kg "),
            "functional_unit": graph["functionalUnit"],
            "system_boundary": "Refinery-inspired foreground benchmark; upstream feed production excluded",
            "time_representativeness": "Illustrative steady-state basis",
            "geography": "CN illustrative",
            "description": "Hypothetical, analytically inspectable paper acceptance case; not a measured refinery inventory.",
            "source_policy": "open_mixed",
            "allowed_lcia_scope": "ef31_only",
        },
    )
    version = _request(
        base_url,
        f"/api/projects/{urllib.parse.quote(project['project_id'])}/versions?compile_pts_on_save=false",
        method="POST",
        payload={"graph": graph},
    )
    return project, version


def _duplicate_and_advance(
    base_url: str,
    *,
    parent_project_id: str,
    name: str,
    graph: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    project = _request(
        base_url,
        f"/api/projects/{urllib.parse.quote(parent_project_id)}/duplicate",
        method="POST",
        payload={"name": name},
    )
    _request(
        base_url,
        f"/api/projects/{urllib.parse.quote(project['project_id'])}",
        method="PATCH",
        payload={
            "reference_product": graph["metadata"]["functional_unit"]["display_text"].removeprefix("1 kg "),
            "functional_unit": graph["functionalUnit"],
            "description": "Hypothetical progressive refinery paper case; cloned from the preceding stage.",
        },
    )
    version = _request(
        base_url,
        f"/api/projects/{urllib.parse.quote(project['project_id'])}/versions?compile_pts_on_save=false",
        method="POST",
        payload={"graph": graph},
    )
    project = _request(base_url, f"/api/projects/{urllib.parse.quote(project['project_id'])}")
    return project, version


def _manifest(output_dir: Path) -> None:
    rows = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file() or path.name == "manifest.sha256":
            continue
        rows.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(output_dir).as_posix()}")
    (output_dir / "manifest.sha256").write_text("\n".join(rows) + "\n", encoding="utf-8")


def _compare_process_lcia(
    expanded: dict[str, Any],
    compiled: dict[str, Any],
    process_uuid: str,
) -> dict[str, Any]:
    expanded_lci = expanded.get("lci_result") or {}
    compiled_lci = compiled.get("lci_result") or {}
    expanded_processes = list(expanded_lci.get("process_index") or [])
    compiled_processes = list(compiled_lci.get("process_index") or [])
    if process_uuid not in expanded_processes or process_uuid not in compiled_processes:
        return {"comparable": False, "reason": "target process missing from one result"}
    expanded_values = list(expanded_lci.get("values") or [])
    compiled_values = list(compiled_lci.get("values") or [])
    if not expanded_values or len(expanded_values) != len(compiled_values):
        return {"comparable": False, "reason": "LCIA vectors are empty or have different dimensions"}
    expanded_index = expanded_processes.index(process_uuid)
    compiled_index = compiled_processes.index(process_uuid)
    indicators = list(expanded_lci.get("indicator_index") or [])
    rows = []
    for index, (expanded_row, compiled_row) in enumerate(zip(expanded_values, compiled_values, strict=True)):
        expanded_value = float(expanded_row[expanded_index])
        compiled_value = float(compiled_row[compiled_index])
        indicator = indicators[index] if index < len(indicators) else {"indicator_index": index}
        rows.append(
            {
                "indicator": indicator,
                "expanded": expanded_value,
                "compiled": compiled_value,
                "absolute_error": abs(expanded_value - compiled_value),
            }
        )
    return {
        "comparable": True,
        "process_uuid": process_uuid,
        "indicator_count": len(rows),
        "max_absolute_error": max(row["absolute_error"] for row in rows),
        "tolerance": 1e-10,
        "within_tolerance": all(row["absolute_error"] <= 1e-10 for row in rows),
        "indicators": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create five progressive refinery paper-case projects through the real Nebula LCA API."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--project-prefix", default="Paper Refinery Benchmark")
    parser.add_argument("--run-model", action="store_true", help="Also call /api/model/run; requires the solver service.")
    args = parser.parse_args()

    health = _request(args.base_url, "/health")
    openapi = _request(args.base_url, "/openapi.json")
    required_paths = {
        "/api/projects",
        "/api/projects/{project_id}/duplicate",
        "/api/projects/{project_id}/versions",
        "/api/provider/v1/solve",
        "/api/pts/compile",
        "/api/pts/{pts_uuid}/publish",
    }
    missing_paths = sorted(required_paths - set(openapi.get("paths", {})))
    if missing_paths:
        raise RuntimeError(f"Required API paths are missing: {missing_paths}")

    run_token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_root.resolve() / run_token
    output_dir.mkdir(parents=True, exist_ok=False)

    flows, flow_receipts = _create_custom_flows(args.base_url)
    graphs = build_progressive_graphs(flows)
    _write_json(output_dir / "flow_catalog_receipts.json", flow_receipts)

    case_defs = [
        ("case_01_unit_process", "01 Unit process - distillation", "paper-refinery-distillation", 1),
        ("case_02_balanced_chain", "02 Balanced refinery chain", "paper-refinery-blending", 4),
        ("case_03_mixed_normalized_supply", "03 Balanced chain with normalized electricity supply", "paper-refinery-blending", 5),
        ("case_04_recycle_loop", "04 Recycle-loop comparison", "paper-refinery-blending", 5),
    ]
    records: list[dict[str, Any]] = []
    parent_id: str | None = None
    expanded_case_result: dict[str, Any] | None = None
    expanded_case_graph: dict[str, Any] | None = None
    expanded_model_result: dict[str, Any] | None = None
    open_loop_result: dict[str, Any] | None = None
    for key, suffix, target_process, process_count in case_defs:
        graph = graphs[key]
        name = f"{args.project_prefix} - {suffix} - {run_token}"
        if parent_id is None:
            project, version = _create_project(args.base_url, name, graph)
        else:
            project, version = _duplicate_and_advance(
                args.base_url,
                parent_project_id=parent_id,
                name=name,
                graph=graph,
            )
        snapshot, solve_result = _solve_project(
            args.base_url,
            project_id=project["project_id"],
            version=int(version["version"]),
            graph=graph,
            target_process_uuid=target_process,
            scenario_id=key,
        )
        audit = _audit_case(graph, solve_result, expected_process_count=process_count)
        recycle_comparison = None
        if key == "case_04_recycle_loop":
            if open_loop_result is None:
                raise RuntimeError("Case 4 requires the solved Case 3 open-loop baseline")
            recycle_comparison = _compare_recycle_solve(open_loop_result, solve_result)
            audit["checks"]["recycle_activity_vector_changed"] = recycle_comparison["activity_vector_changed"]
            audit["checks"]["recycle_boundary_exchanges_changed"] = recycle_comparison["boundary_exchanges_changed"]
            audit["status"] = "passed" if all(audit["checks"].values()) else "failed"
        case_dir = output_dir / key
        _write_case_pack(
            case_dir,
            project=project,
            parent_project_id=parent_id,
            version_response=version,
            graph=graph,
            snapshot=snapshot,
            solve_result=solve_result,
            audit=audit,
        )
        if recycle_comparison is not None:
            _write_json(case_dir / "recycle_comparison.json", recycle_comparison)
        if args.run_model:
            model_result = _request(
                args.base_url,
                "/api/model/run",
                method="POST",
                payload={
                    "graph": graph,
                    "project_id": project["project_id"],
                    "lcia_methods": ["EF v3.1"],
                },
            )
            _write_json(case_dir / "model_run_result.json", model_result)
            if key == "case_04_recycle_loop":
                expanded_model_result = model_result
        records.append(
            {
                "case": key,
                "project_id": project["project_id"],
                "parent_project_id": parent_id,
                "version": int(version["version"]),
                "graph_hash": version.get("graph_hash"),
                "audit_status": audit["status"],
                "run_id": solve_result.get("run_id"),
            }
        )
        if key == "case_04_recycle_loop":
            expanded_case_result = solve_result
            expanded_case_graph = graph
        if key == "case_03_mixed_normalized_supply":
            open_loop_result = solve_result
        parent_id = project["project_id"]

    assert parent_id is not None
    pts_project_name = f"{args.project_prefix} - 05 PTS compiled recycle upgrading - {run_token}"
    pts_project = _request(
        args.base_url,
        f"/api/projects/{urllib.parse.quote(parent_id)}/duplicate",
        method="POST",
        payload={"name": pts_project_name},
    )
    pts_compile_graph = build_pts_compile_graph(flows)
    compile_result = _request(
        args.base_url,
        "/api/pts/compile",
        method="POST",
        payload={
            "graph": pts_compile_graph,
            "pts_uuid": "paper-refinery-upgrading-pts",
            "project_id": pts_project["project_id"],
            "force_recompile": True,
        },
    )
    publish_result = _request(
        args.base_url,
        "/api/pts/paper-refinery-upgrading-pts/publish",
        method="POST",
        payload={
            "project_id": pts_project["project_id"],
            "compile_id": compile_result["compile_id"],
            "set_active": True,
        },
    )
    pts_resource = _request(args.base_url, "/api/pts/paper-refinery-upgrading-pts")
    pts_graph = build_pts_main_graph(flows, pts_resource["shell_node"], pts_compile_graph)
    _request(
        args.base_url,
        f"/api/projects/{urllib.parse.quote(pts_project['project_id'])}",
        method="PATCH",
        payload={
            "reference_product": FLOW_SPECS["gasoline"],
            "functional_unit": pts_graph["functionalUnit"],
            "description": "PTS form of the mixed-mode refinery benchmark; the cyclic hydrotreating-reforming subsystem is compiled.",
        },
    )
    pts_version = _request(
        args.base_url,
        f"/api/projects/{urllib.parse.quote(pts_project['project_id'])}/versions?compile_pts_on_save=false",
        method="POST",
        payload={"graph": pts_graph},
    )
    pts_case_dir = output_dir / "case_05_pts_compiled"
    _write_case_pack(
        pts_case_dir,
        project=pts_project,
        parent_project_id=parent_id,
        version_response=pts_version,
        graph=pts_graph,
    )
    _write_json(pts_case_dir / "pts_compile_graph.json", pts_compile_graph)
    _write_json(pts_case_dir / "pts_compile_result.json", compile_result)
    _write_json(pts_case_dir / "pts_publish_result.json", publish_result)
    _write_json(pts_case_dir / "pts_resource.json", pts_resource)
    pts_model_result = None
    if args.run_model:
        pts_model_result = _request(
            args.base_url,
            "/api/model/run",
            method="POST",
            payload={
                "graph": pts_graph,
                "project_id": pts_project["project_id"],
                "lcia_methods": ["EF v3.1"],
            },
        )
        _write_json(pts_case_dir / "model_run_result.json", pts_model_result)

    lcia_comparison = (
        _compare_process_lcia(expanded_model_result, pts_model_result, "paper-refinery-blending")
        if expanded_model_result is not None and pts_model_result is not None
        else {"comparable": False, "reason": "model-run comparison was not requested"}
    )
    pts_checks = {
        "compile_ok": bool(compile_result.get("ok")),
        "compile_invertible": bool(compile_result.get("invertible")),
        "compile_matrix_size_is_two": int(compile_result.get("matrix_size") or 0) == 2,
        "published_version_created": int(publish_result.get("published_version") or 0) >= 1,
        "active_publication_bound": publish_result.get("active_published_version") == publish_result.get("published_version"),
        "main_graph_has_four_nodes": len(pts_graph["nodes"]) == 4,
        "internal_canvas_has_two_nodes": len(pts_compile_graph["metadata"]["canvases"][0]["nodes"]) == 2,
        "model_run_completed": (pts_model_result or {}).get("status") == "completed" if args.run_model else True,
        "model_run_has_lcia": (
            int((pts_model_result or {}).get("summary", {}).get("indicator_count") or 0) > 0
            if args.run_model else True
        ),
        "expanded_vs_pts_lcia_within_tolerance": (
            bool(lcia_comparison.get("within_tolerance")) if args.run_model else True
        ),
    }
    pts_audit = {
        "schema_version": "nebula.refinery-paper-case.pts-audit.v1",
        "status": "passed" if all(pts_checks.values()) else "failed",
        "checks": pts_checks,
        "compile_graph_hash": compile_result.get("graph_hash"),
        "published_artifact_id": publish_result.get("published_artifact_id"),
        "expanded_provider_run_id": (expanded_case_result or {}).get("run_id"),
        "expanded_graph_hash": _canonical_hash(expanded_case_graph) if expanded_case_graph else None,
        "lcia_comparison": lcia_comparison,
        "comparison_scope": (
            "PTS compile/publication, runnable main graph, and target-process LCIA comparison. "
            "Component-wise provider scaled-exchange equivalence remains a separate acceptance step "
            "because provider v1 does not flatten PTS shells."
        ),
    }
    _write_json(pts_case_dir / "audit_report.json", pts_audit)
    records.append(
        {
            "case": "case_05_pts_compiled",
            "project_id": pts_project["project_id"],
            "parent_project_id": parent_id,
            "version": int(pts_version["version"]),
            "graph_hash": pts_version.get("graph_hash"),
            "audit_status": pts_audit["status"],
            "published_pts_version": publish_result.get("published_version"),
        }
    )

    acceptance = {
        "schema_version": "nebula.refinery-paper-case.index.v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "base_url": args.base_url,
        "health": health,
        "data_status": "refinery-inspired hypothetical benchmark; not measured plant data",
        "flow_catalog": flows,
        "projects": records,
        "all_audits_passed": all(row["audit_status"] == "passed" for row in records),
        "project_copy_route_used": "/api/projects/{project_id}/duplicate",
        "limitations": [
            "Quantities are declared benchmark assumptions, not a representative refinery inventory.",
            "The present PTS audit proves compilation, publication, and runnable integration when --run-model is used.",
            "Provider v1 does not currently flatten PTS shells, so component-wise scaled-exchange equivalence is not claimed here.",
        ],
    }
    _write_json(output_dir / "acceptance_index.json", acceptance)
    _manifest(output_dir)
    print(
        json.dumps(
            {
                "status": "passed" if acceptance["all_audits_passed"] else "failed",
                "output_dir": str(output_dir),
                "projects": records,
            },
            ensure_ascii=False,
        )
    )
    return 0 if acceptance["all_audits_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
