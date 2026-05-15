from __future__ import annotations

import csv
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from urllib import request

from fastapi.testclient import TestClient

from app.config import settings
from app.models import FlowRecord, ModelVersion, ReferenceProcess, RunJob, UnitDefinition, UnitGroup
from app.services.catalog_cache import invalidate_management_caches


REPO_ROOT = Path(__file__).resolve().parents[2]
SOLVER_ROOT = REPO_ROOT / "nebula-lca-solver"
LEGACY_EF31_DIR = SOLVER_ROOT / "data" / "EF3.1"
REAL_CO2_FOSSIL_FLOW_UUID = "08a91e70-3ddc-11dd-923d-0050c2490048"
REAL_CO2_FOSSIL_FLOW_NAME = "carbon dioxide (fossil) (Mass, kg, Emissions to air, unspecified)"

EF31_CATEGORIES = [
    "climate change",
    "acidification",
    "climate change: biogenic",
    "climate change: fossil",
    "climate change: land use and land use change",
    "ecotoxicity: freshwater",
    "ecotoxicity: freshwater, inorganics",
    "ecotoxicity: freshwater, organics",
    "energy resources: non-renewable",
    "eutrophication: freshwater",
    "eutrophication: marine",
    "eutrophication: terrestrial",
    "human toxicity: carcinogenic",
    "human toxicity: carcinogenic, inorganics",
    "human toxicity: carcinogenic, organics",
    "human toxicity: non-carcinogenic",
    "human toxicity: non-carcinogenic, inorganics",
    "human toxicity: non-carcinogenic, organics",
    "ionising radiation: human health",
    "land use",
    "material resources: metals/minerals",
    "ozone depletion",
    "particulate matter formation",
    "photochemical oxidant formation: human health",
    "water use",
]


def load_case(case_dir: Path) -> dict:
    return {
        "case": _read_json(case_dir / "case.json"),
        "seed_catalog": _read_json(case_dir / "seed_catalog.json"),
        "expected": _read_json(case_dir / "expected.json"),
    }


def seed_catalog(db, seed_catalog: dict) -> None:
    for group in seed_catalog.get("unit_groups", []):
        db.merge(
            UnitGroup(
                name=group["name"],
                reference_unit=group.get("reference_unit"),
            )
        )
    for unit in seed_catalog.get("unit_definitions", []):
        db.add(
            UnitDefinition(
                unit_group=unit["unit_group"],
                unit_name=unit["unit_name"],
                factor_to_reference=float(unit["factor_to_reference"]),
                is_reference=bool(unit.get("is_reference", False)),
            )
        )
    for flow in seed_catalog.get("flows", []):
        db.merge(
            FlowRecord(
                flow_uuid=flow["flow_uuid"],
                flow_name=flow["flow_name"],
                flow_name_en=flow.get("flow_name_en"),
                flow_type=flow["flow_type"],
                default_unit=flow["default_unit"],
                unit_group=flow["unit_group"],
                compartment=flow.get("compartment"),
                source=flow.get("source", "test"),
                is_custom=bool(flow.get("is_custom", False)),
            )
        )
    for process in seed_catalog.get("processes", []):
        db.merge(
            ReferenceProcess(
                process_uuid=process["process_uuid"],
                process_name=process["process_name"],
                process_type=process.get("process_type", "unit_process"),
                reference_flow_uuid=process.get("reference_flow_uuid"),
                process_json=process.get("process_json"),
                import_mode=process.get("import_mode"),
                import_report_json=process.get("import_report_json"),
            )
        )
    db.commit()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)


def run_project_case(client: TestClient, db, case_dir: Path) -> dict:
    loaded = load_case(case_dir)
    seed_catalog(db, loaded["seed_catalog"])
    graph = loaded["case"]["graph"]
    expected = loaded["expected"]

    project_response = client.post(
        "/api/projects",
        json={
            "name": loaded["case"]["name"],
            "reference_product": expected.get("target_product"),
            "functional_unit": graph.get("functionalUnit"),
        },
    )
    assert project_response.status_code == 200, project_response.text
    project_id = project_response.json()["project_id"]

    version_response = client.post(
        f"/api/projects/{project_id}/versions",
        json={"graph": graph},
    )
    assert version_response.status_code == 200, version_response.text
    model_version = (
        db.query(ModelVersion)
        .filter(ModelVersion.model_id == project_id)
        .order_by(ModelVersion.version.desc())
        .first()
    )
    assert model_version is not None

    run_response = client.post(
        "/api/model/run",
        json={
            "graph": graph,
            "project_id": project_id,
            "model_version_id": model_version.id,
            "lcia_methods": ["EF v3.1"],
        },
    )
    assert run_response.status_code == 200, run_response.text
    payload = run_response.json()

    run_job = db.get(RunJob, payload["run_id"])
    assert run_job is not None
    assert run_job.status == "completed"
    assert run_job.model_version_id == model_version.id
    return {
        "project_id": project_id,
        "response": payload,
        "expected": expected,
        "run_job": run_job,
    }


@contextmanager
def solver_server(runtime_root: Path):
    legacy_dir = LEGACY_EF31_DIR
    eco_dir = runtime_root / "eco"
    eco_dir.mkdir(parents=True, exist_ok=True)
    _write_runtime(eco_dir, {"flow-eco-ch4": 27.0}, method_name="EF v3.1")

    port = _free_port()
    env = {
        **dict(os.environ),
        "NEBULA_LCA_LEGACY_EF31_DIR": str(legacy_dir),
        "NEBULA_LCA_EF31_DIR": str(eco_dir),
        "NEBULA_LCA_OUTPUT_DIR": str(runtime_root / "exports"),
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(SOLVER_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_solver(url, process)
        previous_url = settings.nebula_lca_solver_api_url
        settings.nebula_lca_solver_api_url = url
        try:
            yield url
        finally:
            settings.nebula_lca_solver_api_url = previous_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def assert_case_result(result: dict) -> None:
    response = result["response"]
    expected = result["expected"]
    summary = response["summary"]
    lci_result = response["lci_result"]
    snapshot = response["tiangong_like_input"]

    assert summary["indicator_count"] == expected["summary"]["indicator_count"]
    assert summary["missing_ef31_flow_count"] == expected["summary"]["missing_ef31_flow_count"]
    assert len(snapshot["processes"]) == expected["snapshot"]["process_count"]
    assert len(snapshot["links"]) == expected["snapshot"]["link_count"]
    for flow_uuid in expected.get("snapshot", {}).get("required_flow_uuids", []):
        assert any(flow.get("flow_uuid") == flow_uuid for flow in snapshot["flows"])

    climate_index = _indicator_index_by_category(lci_result["indicator_index"], "climate change")
    process_index = lci_result["process_index"].index(expected["target_process_uuid"])
    actual = float(lci_result["values"][climate_index][process_index])
    assert actual == expected["climate_change"]


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_runtime(root: Path, flow_factors: dict[str, float], *, method_name: str = "Climate change") -> None:
    with (root / "flow_index.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["flow_index", "FlowUUID", "FlowName"])
        for idx, flow_uuid in enumerate(flow_factors):
            writer.writerow([idx, flow_uuid, flow_uuid])

    with (root / "indicator_index.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["indicator_index", "method_en", "method_zh", "indicator_en", "indicator_zh", "ecoinvent_category"])
        for idx, category in enumerate(EF31_CATEGORIES):
            writer.writerow([idx, method_name if idx == 0 else category, "EF v3.1", category, category, category])

    with (root / "lcia_factors.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["row", "column", "coefficient"])
        for flow_idx, coefficient in enumerate(flow_factors.values()):
            writer.writerow([0, flow_idx, coefficient])


def _indicator_index_by_category(indicators: list[dict], category: str) -> int:
    for idx, item in enumerate(indicators):
        keys = [
            item.get("ecoinvent_category", ""),
            item.get("canonical_indicator_key", ""),
            item.get("indicator_en", ""),
            item.get("indicator_zh", ""),
        ]
        if any(str(key).strip().lower() == category for key in keys):
            return idx
    raise AssertionError(f"indicator category not found: {category}")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_solver(url: str, process: subprocess.Popen) -> None:
    deadline = time.time() + 20
    last_error: Exception | None = None
    while time.time() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(f"solver exited early\nstdout={stdout}\nstderr={stderr}")
        try:
            with request.urlopen(f"{url}/health", timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last_error = exc
            time.sleep(0.2)
    raise RuntimeError(f"solver did not start: {last_error}")
