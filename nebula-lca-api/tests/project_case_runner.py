from __future__ import annotations

import csv
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib import request

from fastapi.testclient import TestClient

from app.config import settings
from app.models import (
    FlowRecord,
    ModelVersion,
    PtsCompileArtifact,
    PtsDefinition,
    PtsExternalArtifact,
    PtsResource,
    ReferenceProcess,
    RunJob,
    UnitDefinition,
    UnitGroup,
)
from app.services.catalog_cache import invalidate_management_caches
from app.services.public_flow_mapping_service import get_public_flow_mapping_registry


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
    result = {
        "case": _read_json(case_dir / "case.json"),
        "seed_catalog": _read_json(case_dir / "seed_catalog.json"),
        "expected": _read_json(case_dir / "expected.json"),
    }
    pts_seed_path = case_dir / "pts_seed.json"
    if pts_seed_path.exists():
        result["pts_seed"] = _read_json(pts_seed_path)
    return result


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


def _parse_datetime(val) -> datetime:
    """Parse an ISO-format datetime string to a timezone-aware datetime, or return a default."""
    if isinstance(val, datetime):
        return val
    if isinstance(val, str) and val:
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
    return datetime.now(timezone.utc)


def seed_pts_tables(db, pts_seed: dict, project_id: str) -> None:
    """Seed PTS tables (resources, definitions, compile artifacts, external artifacts) for a project.

    This is used by PTS regression tests that need published PTS data pre-loaded
    before calling ``/api/model/run``.
    """
    # Replace PLACEHOLDER project_id with actual project_id
    _replace_project_id(pts_seed, "PLACEHOLDER", project_id)

    # 1. Seed pts_resources
    for row in pts_seed.get("resources", []):
        db.add(
            PtsResource(
                id=row["id"],
                project_id=project_id,
                pts_uuid=row["pts_uuid"],
                name=row.get("name"),
                pts_node_id=row.get("pts_node_id"),
                latest_graph_hash=row.get("latest_graph_hash"),
                compiled_graph_hash=row.get("compiled_graph_hash"),
                latest_compile_version=row.get("latest_compile_version"),
                latest_published_version=row.get("latest_published_version"),
                active_published_version=row.get("active_published_version"),
                pts_graph_json=row.get("pts_graph_json", {}),
                ports_policy_json=row.get("ports_policy_json", {}),
                shell_node_json=row.get("shell_node_json", {}),
                published_at=_parse_datetime(row.get("published_at")),
                created_at=_parse_datetime(row.get("created_at")),
                updated_at=_parse_datetime(row.get("updated_at")),
            )
        )

    # 2. Seed pts_definitions
    for row in pts_seed.get("definitions", []):
        db.add(
            PtsDefinition(
                id=row["id"],
                project_id=project_id,
                pts_id=row.get("pts_id"),
                pts_uuid=row["pts_uuid"],
                pts_node_id=row["pts_node_id"],
                internal_node_ids_json=row.get("internal_node_ids_json", []),
                product_refs_json=row.get("product_refs_json", []),
                ports_policy_json=row.get("ports_policy_json", {}),
                latest_graph_hash=row.get("latest_graph_hash"),
                definition_json=row.get("definition_json", {}),
                created_at=_parse_datetime(row.get("created_at")),
                updated_at=_parse_datetime(row.get("updated_at")),
            )
        )

    # 3. Seed pts_compile_artifacts
    for row in pts_seed.get("compile_artifacts", []):
        db.add(
            PtsCompileArtifact(
                id=row["id"],
                project_id=project_id,
                pts_node_id=row["pts_node_id"],
                pts_uuid=row["pts_uuid"],
                graph_hash=row["graph_hash"],
                compile_version=row.get("compile_version"),
                ok=row["ok"],
                matrix_size=row.get("matrix_size", 0),
                invertible=row.get("invertible", True),
                errors_json=row.get("errors_json", []),
                warnings_json=row.get("warnings_json", []),
                artifact_json=row.get("artifact_json", {}),
                created_at=_parse_datetime(row.get("created_at")),
                updated_at=_parse_datetime(row.get("updated_at")),
            )
        )

    # 4. Seed pts_external_artifacts
    for row in pts_seed.get("external_artifacts", []):
        db.add(
            PtsExternalArtifact(
                id=row["id"],
                project_id=project_id,
                pts_id=row.get("pts_id"),
                pts_uuid=row["pts_uuid"],
                pts_node_id=row["pts_node_id"],
                graph_hash=row["graph_hash"],
                published_version=row.get("published_version"),
                source_compile_id=row.get("source_compile_id"),
                source_compile_version=row.get("source_compile_version"),
                artifact_json=row.get("artifact_json", {}),
                created_at=_parse_datetime(row.get("created_at")),
                updated_at=_parse_datetime(row.get("updated_at")),
            )
        )

    try:
        db.commit()
    except Exception as exc:
        import sys
        sys.stderr.write(f"PTS seed commit ERROR: {exc}\n")
        sys.stderr.flush()
        db.rollback()
        raise
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)


def run_project_case(client: TestClient, db, case_dir: Path, use_pts: bool = False,
                     pts_graph_hashes: dict | None = None) -> dict:
    """Run a project case end-to-end: seed catalog → create project → seed PTS (optional) → create version → run → return result.

    Args:
        client: TestClient
        db: SQLAlchemy session
        case_dir: Path to the case fixture directory
        use_pts: If True, seed PTS tables from pts_seed.json (if present)
        pts_graph_hashes: Optional dict mapping pts_uuid → graph_hash, used to patch
            pts_seed before seeding. Required when PTS artifacts reference a graph
            whose hash is not static (most common case).
    """
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

    # Seed PTS data if the case is a PTS test case
    if use_pts and "pts_seed" in loaded:
        pts_seed = loaded["pts_seed"].get("pts_seed", loaded["pts_seed"])
        if pts_graph_hashes:
            # Patch graph hashes in resources, compile_artifacts, external_artifacts
            for resource in pts_seed.get("resources", []):
                puuid = resource.get("pts_uuid")
                if puuid in pts_graph_hashes:
                    h = pts_graph_hashes[puuid]
                    resource["latest_graph_hash"] = h
                    resource["compiled_graph_hash"] = h
            for ca in pts_seed.get("compile_artifacts", []):
                puuid = ca.get("pts_uuid")
                if puuid in pts_graph_hashes:
                    h = pts_graph_hashes[puuid]
                    ca["graph_hash"] = h
                    if "artifact_json" in ca:
                        ca["artifact_json"]["graph_hash"] = h
            for ea in pts_seed.get("external_artifacts", []):
                puuid = ea.get("pts_uuid")
                if puuid in pts_graph_hashes:
                    h = pts_graph_hashes[puuid]
                    ea["graph_hash"] = h
                    if "artifact_json" in ea:
                        ea["artifact_json"]["graph_hash"] = h
        seed_pts_tables(db, pts_seed, project_id)

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
    _write_runtime(
        eco_dir,
        {
            "349b29d1-3e58-4c66-98b9-9d1a076efd2e": 1.0,
            "flow-eco-ch4": 27.0,
        },
        method_name="EF v3.1",
    )

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

    snap_expected = expected.get("snapshot", {})
    snapshot_process_key = "snapshot_shell_process_count" if "snapshot_shell_process_count" in snap_expected else "process_count"
    assert len(snapshot["processes"]) == snap_expected.get(snapshot_process_key, snap_expected.get("process_count", 0))
    assert len(snapshot["links"]) == snap_expected.get("link_count", 0)
    for flow_uuid in snap_expected.get("required_flow_uuids", []):
        mapping = get_public_flow_mapping_registry().resolve_elementary(flow_uuid)
        expected_flow_uuid = mapping.ecoinvent_flow_uuid if mapping is not None else flow_uuid
        assert any(flow.get("flow_uuid") == expected_flow_uuid for flow in snapshot["flows"])

    climate_index = _indicator_index_by_category(lci_result["indicator_index"], "climate change")
    process_index = lci_result["process_index"]
    target_uuid = expected["target_process_uuid"]
    assert target_uuid in process_index, f"target_process_uuid {target_uuid} not in process_index {process_index}"
    process_idx = process_index.index(target_uuid)
    actual = float(lci_result["values"][climate_index][process_idx])
    assert abs(actual - expected["climate_change"]) < 1e-10, (
        f"Climate change mismatch: actual={actual}, expected={expected['climate_change']}"
    )

    # PTS-specific: verify expected process UUIDs exist in process_index
    for proc_uuid in expected.get("process_index_uuids", []):
        assert proc_uuid in process_index, f"Expected process UUID {proc_uuid} not in process_index"

    # Optional: product_results — dict of { target_process_uuid: climate_change_value }
    for product_uuid, expected_cc in expected.get("product_results", {}).items():
        assert product_uuid in process_index, f"Product UUID {product_uuid} not in process_index"
        pidx = process_index.index(product_uuid)
        actual_cc = float(lci_result["values"][climate_index][pidx])
        assert abs(actual_cc - expected_cc) < 1e-10, (
            f"Product {product_uuid} climate change mismatch: actual={actual_cc}, expected={expected_cc}"
        )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _replace_project_id(obj: dict, old_value: str, new_value: str) -> None:
    """Recursively replace all occurrences of old_value with new_value in dict/list.

    Only replaces string values that are exactly old_value (not substrings).
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and value == old_value:
                obj[key] = new_value
            elif isinstance(value, (dict, list)):
                _replace_project_id(value, old_value, new_value)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                _replace_project_id(item, old_value, new_value)
            elif isinstance(item, str) and item == old_value:
                idx = obj.index(item)
                obj[idx] = new_value


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
    opener = request.build_opener(request.ProxyHandler({}))
    while time.time() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(f"solver exited early\nstdout={stdout}\nstderr={stderr}")
        try:
            with opener.open(f"{url}/health", timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last_error = exc
            time.sleep(0.2)
    raise RuntimeError(f"solver did not start: {last_error}")
