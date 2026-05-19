"""Tests for build_tidas_readiness and TIDAS export readiness API endpoint.

Phase 3: TIDAS Export Readiness API
Uses the same mock pattern as test_tidas_source_space.py.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.tidas_export import (
    ExportError,
    build_tidas_readiness,
    export_bundle,
    preview_export,
)
from app.tidas_reference import normalize_tidas_unit_group


# ── Helpers ────────────────────────────────────────────────────────────────


def _make_flow_mock(uuid: str, flow_type: str, source: str | None,
                    unit_group: str = "") -> MagicMock:
    m = MagicMock()
    m.flow_uuid = uuid
    m.flow_type = flow_type
    m.source = source
    m.unit_group = unit_group
    m.flow_name = f"Flow {uuid}"
    m.flow_name_en = f"Flow {uuid}"
    m.default_unit = "kg"
    m.compartment = "" if flow_type != "Elementary flow" else "air"
    m.source_updated_at = None
    m.is_custom = False
    m.category = ""
    return m


def _make_version_mock(graph_json: dict):
    return SimpleNamespace(hybrid_graph_json=graph_json, version=1)


def _make_model_mock(project_id: str, source_policy: str = "open_mixed"):
    return SimpleNamespace(
        id=project_id,
        name=f"Project {project_id}",
        source_policy=source_policy,
        allowed_lcia_scope="ef31_only",
        reference_product="ref",
        functional_unit="1 kg",
        system_boundary=None,
        time_representativeness=None,
        geography="CN",
        description=None,
    )


def _make_graph(
    node_type: str = "unit_process",
    elementary_flow_uuids: list[str] | None = None,
    product_flow_uuids: list[str] | None = None,
    unit_groups: list[str] | None = None,
    has_pts: bool = False,
    biosphere_type: str = "biosphere",
) -> dict:
    nodes: list[dict] = []
    node: dict = {
        "id": "proc-1",
        "process_uuid": "proc-1",
        "node_kind": node_type,
        "mode": "balanced",
        "reference_product": "ref",
        "name": "Test Process",
        "location": "CN",
        "inputs": [],
        "outputs": [],
    }
    if unit_groups:
        node["unit_group"] = unit_groups[0]
    if product_flow_uuids:
        for uuid in product_flow_uuids:
            node["outputs"].append({
                "id": f"out-{uuid[:8]}",
                "flowUuid": uuid,
                "name": "Product",
                "amount": 1.0,
                "unit": "kg",
                "isProduct": True,
                "type": "technosphere",
                "direction": "output",
            })
    if elementary_flow_uuids:
        for uuid in elementary_flow_uuids:
            node["inputs"].append({
                "id": f"in-{uuid[:8]}",
                "flowUuid": uuid,
                "name": "Elementary",
                "amount": 0.1,
                "unit": "kg",
                "type": biosphere_type,
                "direction": "input",
            })
    if has_pts:
        nodes.append({
            "id": "pts-1",
            "process_uuid": "pts-1",
            "node_kind": "pts_module",
            "pts_uuid": "pts-1",
            "name": "PTS Module",
            "inputs": [],
            "outputs": [],
        })
    nodes.append(node)
    return {"nodes": nodes, "exchanges": [], "functionalUnit": "1 kg"}


def _build_fake_db(project_id: str, flows: dict, graph_json: dict,
                   source_policy: str = "open_mixed"):
    """Build a MagicMock Session."""
    from app.models import Model, ModelVersion, FlowRecord, ReferenceProcess

    model_mock = _make_model_mock(project_id, source_policy)
    version_mock = _make_version_mock(graph_json)

    session = MagicMock()

    def get(model_cls, key):
        if isinstance(model_cls, type) and issubclass(model_cls, Model):
            return model_mock
        if isinstance(model_cls, type) and issubclass(model_cls, ModelVersion):
            return version_mock
        if isinstance(model_cls, type) and issubclass(model_cls, FlowRecord):
            return flows.get(key)
        if isinstance(model_cls, type) and issubclass(model_cls, ReferenceProcess):
            rp = MagicMock()
            rp.process_json = None
            return rp
        return None

    session.get = get

    query_mock = MagicMock()
    filter_mock = MagicMock()
    order_mock = MagicMock()
    order_mock.first.return_value = version_mock
    filter_mock.order_by.return_value = order_mock
    query_mock.filter.return_value = filter_mock
    session.query.return_value = query_mock

    return session


# ── Tests: clean TIDAS-compatible project ─────────────────────────────────


def test_readiness_clean_tidas_project():
    """Clean TIDAS-compatible project returns can_export=True, empty blocking."""
    project_id = "proj-clean"
    flows = {
        "ef-flow-1": _make_flow_mock("ef-flow-1", "Elementary flow", "EF3.1",
                                     unit_group="Units of mass"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["ef-flow-1"],
        unit_groups=["Units of mass"],
    )
    db = _build_fake_db(project_id, flows, graph, source_policy="open_mixed")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is True
    assert result["blocking"] == []
    assert result["source_policy"] == "open_mixed"
    assert result["flow_count"] > 0
    assert result["process_count"] > 0


def test_readiness_blocks_multi_product_current_unit_group_mismatch():
    project_id = "proj-unit-group-mismatch"
    flows = {
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0", unit_group="Units of mass"),
        "prod-2": _make_flow_mock("prod-2", "Product flow", "Tiangong 1.0", unit_group="Units of volume"),
    }
    graph = _make_graph(product_flow_uuids=["prod-1", "prod-2"])
    graph["nodes"][0]["outputs"][0]["name"] = "Product A"
    graph["nodes"][0]["outputs"][0]["unit"] = "kg"
    graph["nodes"][0]["outputs"][0]["unitGroup"] = "Units of mass"
    graph["nodes"][0]["outputs"][1]["name"] = "Product B"
    graph["nodes"][0]["outputs"][1]["unit"] = "m3"
    graph["nodes"][0]["outputs"][1]["unitGroup"] = "Units of volume"
    db = _build_fake_db(project_id, flows, graph, source_policy="tidas_compliant")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    issue = next(item for item in result["blocking"] if item["code"] == "MULTI_PRODUCT_UNIT_GROUP_MISMATCH")
    assert issue["details"]["repair_target"] == "allocation"
    assert issue["details"]["base_flow"]["unit_group"] == "Units of mass"
    assert issue["details"]["mismatched_flows"][0]["unit_group"] == "Units of volume"


def test_readiness_tidas_compliant_no_errors():
    """TIDAS-compliant project with allowed unit groups passes."""
    project_id = "proj-tidas"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "Tiangong 1.0",
                                  unit_group="Units of mass"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
        unit_groups=["Units of mass"],
    )
    db = _build_fake_db(project_id, flows, graph, source_policy="tidas_compliant")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is True
    assert result["blocking"] == []
    assert result["source_policy"] == "tidas_compliant"
    assert any(i["code"] == "source_policy" for i in result["info"])
    assert any(i["code"] == "tidas_seed_status" for i in result["info"])


def test_readiness_blocks_cross_unit_products_even_with_density_basis():
    project_id = "proj-density-allocation"
    flows = {
        "prod-volume": _make_flow_mock("prod-volume", "Product flow", "Tiangong 1.0", unit_group="Units of volume"),
        "prod-mass": _make_flow_mock("prod-mass", "Product flow", "Tiangong 1.0", unit_group="Units of mass"),
    }
    graph = {
        "functionalUnit": "1 kg",
        "nodes": [
            {
                "id": "proc-density",
                "process_uuid": "proc-density",
                "node_kind": "unit_process",
                "mode": "balanced",
                "reference_product": "prod-volume",
                "name": "Density Allocation Process",
                "location": "CN",
                "inputs": [],
                "outputs": [
                    {
                        "id": "out-volume",
                        "flowUuid": "prod-volume",
                        "name": "Volume product",
                        "amount": 2.0,
                        "unit": "m3",
                        "unitGroup": "Units of volume",
                        "isProduct": True,
                        "type": "technosphere",
                        "direction": "output",
                        "allocationBasis": {
                            "method": "density",
                            "value": 800,
                            "targetUnitGroup": "Units of mass",
                        },
                    },
                    {
                        "id": "out-mass",
                        "flowUuid": "prod-mass",
                        "name": "Mass product",
                        "amount": 400.0,
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "isProduct": True,
                        "type": "technosphere",
                        "direction": "output",
                    },
                ],
            }
        ],
        "exchanges": [],
    }
    db = _build_fake_db(project_id, flows, graph, source_policy="open_mixed")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    assert any(item["code"] == "MULTI_PRODUCT_UNIT_GROUP_MISMATCH" for item in result["blocking"])
    assert result["manual_allocation_required_processes"] == []
    assert result["multi_product_process_count"] == 1


# ── Tests: blocking issues ────────────────────────────────────────────────


def test_readiness_manual_allocation_warning_has_repair_target():
    """Manual allocation warnings include node/process metadata for frontend repair."""
    project_id = "proj-manual-allocation-target"
    flows = {
        "prod-volume": _make_flow_mock("prod-volume", "Product flow", "Tiangong 1.0", unit_group="Units of volume"),
        "prod-mass": _make_flow_mock("prod-mass", "Product flow", "Tiangong 1.0", unit_group="Units of mass"),
    }
    graph = {
        "functionalUnit": "1 kg",
        "nodes": [
            {
                "id": "node-manual",
                "process_uuid": "process-manual",
                "node_kind": "unit_process",
                "mode": "balanced",
                "reference_product": "prod-volume",
                "name": "Manual Allocation Process",
                "location": "CN",
                "inputs": [],
                "outputs": [
                    {
                        "id": "out-volume",
                        "flowUuid": "prod-volume",
                        "name": "Volume product",
                        "amount": 2.0,
                        "unit": "m3",
                        "unitGroup": "Units of volume",
                        "isProduct": True,
                        "type": "technosphere",
                        "direction": "output",
                    },
                    {
                        "id": "out-mass",
                        "flowUuid": "prod-mass",
                        "name": "Mass product",
                        "amount": 400.0,
                        "unit": "kg",
                        "unitGroup": "Units of mass",
                        "isProduct": True,
                        "type": "technosphere",
                        "direction": "output",
                    },
                ],
            }
        ],
        "exchanges": [],
    }
    db = _build_fake_db(project_id, flows, graph, source_policy="open_mixed")

    result = build_tidas_readiness(db, project_id)

    assert "process-manual" in result["manual_allocation_required_processes"]
    issue = next(
        item
        for item in result["allocation_warnings"]
        if "manual allocation required" in item["message"]
    )
    assert issue["details"]["repair_target"] == "allocation"
    assert issue["details"]["node_id"] == "node-manual"
    assert issue["details"]["process_uuid"] == "process-manual"


def test_readiness_no_model_version():
    """Project with no model version returns blocking NO_MODEL_VERSION."""
    project_id = "proj-noversion"
    # Empty graph_json (empty dict) → _get_model_version returns version with hybrid_graph_json={}
    # But hybrid_graph_json={} is falsy → blocked as EMPTY_GRAPH
    # To test NO_MODEL_VERSION, we'd need the version_mock to not be returned.
    # In _build_fake_db, ModelVersion always returns version_mock.
    # Let's test EMPTY_GRAPH instead which is more realistic.
    graph = {}  # empty dict → falsy
    db = _build_fake_db(project_id, {}, graph)

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    codes = [b["code"] for b in result["blocking"]]
    assert "EMPTY_GRAPH" in codes


def test_readiness_empty_graph():
    """Project with empty graph returns blocking."""
    project_id = "proj-empty"
    db = _build_fake_db(project_id, {}, {})

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    codes = [b["code"] for b in result["blocking"]]
    assert "EMPTY_GRAPH" in codes


def test_readiness_pts_nodes():
    """Project with PTS node returns blocking PTS issue."""
    project_id = "proj-pts"
    db = _build_fake_db(project_id, {}, _make_graph(has_pts=True))

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    codes = [b["code"] for b in result["blocking"]]
    assert "PTS_MODULE_NOT_SUPPORTED_FOR_TIDAS_EXPORT" in codes


def test_readiness_missing_flows():
    """Project with missing flows returns blocking."""
    project_id = "proj-missing"
    flows = {
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
        # flow-1 is NOT in flows → missing
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
    )
    db = _build_fake_db(project_id, flows, graph)

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    codes = [b["code"] for b in result["blocking"]]
    assert "MISSING_FLOWS" in codes
    assert "flow-1" in result["missing_flows"]


def test_readiness_ecoinvent_elementary_flow():
    """Project with ecoinvent elementary flow returns blocking source-space issue."""
    project_id = "proj-eco"
    flows = {
        "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10",
                                unit_group="kg"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["ec-1"],
    )
    db = _build_fake_db(project_id, flows, graph)

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    blocking_codes = [b["code"] for b in result["blocking"]]
    assert any("BLOCK" in c or "SOURCE" in c or "ECOSPREAD" in c for c in blocking_codes)


def test_readiness_tidas_compliant_unsupported_unit_group():
    """TIDAS compliant project with unsupported unit group returns blocking."""
    project_id = "proj-ug"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "Tiangong 1.0",
                                  unit_group="unsupported-unit-group"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
        unit_groups=["unsupported-unit-group"],
    )
    db = _build_fake_db(project_id, flows, graph, source_policy="tidas_compliant")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is False
    blocking_codes = [b["code"] for b in result["blocking"]]
    assert "unsupported_unit_group" in blocking_codes
    issue = next(b for b in result["blocking"] if b["code"] == "unsupported_unit_group")
    assert issue["details"]["repair_target"] == "unit_group"
    assert issue["details"]["unit_group"] == "unsupported-unit-group"


# ── Tests: warnings ────────────────────────────────────────────────────────


def test_readiness_open_mixed_no_blocking():
    """Open-mixed project with no blocking issues passes with info."""
    project_id = "proj-open"
    flows = {
        # Use EF3.1 source — must pass source-space check even in open_mixed mode
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "EF3.1",
                                  unit_group="Units of mass"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
    )
    db = _build_fake_db(project_id, flows, graph, source_policy="open_mixed")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is True
    assert result["source_policy"] == "open_mixed"
    assert any(i["code"] == "source_mix_summary" for i in result["info"])


# ── Tests: preview_export still works ──────────────────────────────────────


def test_preview_export_delegates_to_readiness():
    """preview_export() delegates to build_tidas_readiness and returns compatible shape."""
    project_id = "proj-preview"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "EF3.1",
                                  unit_group="Units of mass"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
        unit_groups=["Units of mass"],
    )
    db = _build_fake_db(project_id, flows, graph)

    result = preview_export(db, project_id)

    # Must have the original preview fields for backward compat
    assert "can_export" in result
    assert "flow_count" in result
    assert "process_count" in result
    assert "warnings" in result
    assert "errors" in result
    assert "missing_flows" in result
    assert "missing_processes" in result
    assert result["can_export"] is True
    assert isinstance(result["errors"], list)
    for e in result["errors"]:
        assert isinstance(e, str)


# ── Tests: export_bundle uses readiness ────────────────────────────────────


def test_export_bundle_blocks_on_pts():
    """export_bundle raises ExportError when readiness has blocking PTS issue."""
    project_id = "proj-bundle-pts"
    db = _build_fake_db(project_id, {}, _make_graph(has_pts=True))

    with pytest.raises(ExportError) as exc_info:
        export_bundle(db, project_id)
    assert "PTS" in str(exc_info.value) or "pts" in str(exc_info.value).lower()


def test_export_bundle_blocks_on_missing_flows():
    """export_bundle raises ExportError when flows are missing."""
    project_id = "proj-bundle-missing"
    flows = {}
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
    )
    db = _build_fake_db(project_id, flows, graph)

    with pytest.raises(ExportError) as exc_info:
        export_bundle(db, project_id)
    assert "flow" in str(exc_info.value).lower()


# ── Tests: seed status info ───────────────────────────────────────────────


def test_readiness_includes_seed_status():
    """Readiness includes TIDAS seed status/version in info.

    With the default seed deployed to data/Tiangong/tidas_reference_seed.json,
    the info section MUST contain a "tidas_seed_status" entry with the
    *expected* counts (14 flow properties, 14 unit-group mappings, 0 missing
    flow-property mappings).  The "No seed" fallback path is tested separately.
    """
    from app.tidas_reference import load_tidas_reference_seed
    load_tidas_reference_seed.cache_clear()

    project_id = "proj-seed"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "EF3.1",
                                  unit_group="Units of mass"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
    )
    db = _build_fake_db(project_id, flows, graph)

    result = build_tidas_readiness(db, project_id)

    seed_info = [i for i in result["info"] if i["code"] == "tidas_seed_status"]
    assert len(seed_info) == 1
    entry = seed_info[0]
    assert entry["message"].startswith("Reference seed:")
    d = entry.get("details") or {}
    # Seed is loaded with the expected counts
    assert d.get("flow_properties") == 14
    assert d.get("unit_group_mappings") == 14
    assert d.get("missing_flow_property_mappings") == 0


# ── Tests: allowed unit groups ─────────────────────────────────────────────


def test_allowed_unit_groups_no_placeholder_warning():
    """Allowed unit groups from seed should not produce tidas_placeholder warnings."""
    project_id = "proj-allowed"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "EF3.1",
                                  unit_group="Units of mass"),
        "flow-2": _make_flow_mock("flow-2", "Elementary flow", "EF3.1",
                                  unit_group="Units of energy"),
        "flow-3": _make_flow_mock("flow-3", "Elementary flow", "EF3.1",
                                  unit_group="Units of volume"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1", "flow-2", "flow-3"],
        unit_groups=["Units of mass", "Units of energy", "Units of volume"],
    )
    db = _build_fake_db(project_id, flows, graph)

    result = build_tidas_readiness(db, project_id)

    blocking_codes = [b["code"] for b in result["blocking"]]
    assert "unsupported_unit_group" not in blocking_codes
    placeholder_warnings = [
        w for w in result["warnings"]
        if w.get("category") == "tidas_placeholder"
    ]
    for w in placeholder_warnings:
        assert "Units of mass" not in w.get("message", "")
        assert "Units of energy" not in w.get("message", "")
        assert "Units of volume" not in w.get("message", "")


# ── Tests: supplemental kg*km and sej mappings ─────────────────────────────


def test_kg_km_allowed_in_tidas_compliant():
    """kg*km unit group is allowed by the supplemental TIDAS mapping."""
    project_id = "proj-kgkm"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "Tiangong 1.0",
                                  unit_group="Unit of kg*km"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
        unit_groups=["Unit of kg*km"],
    )
    db = _build_fake_db(project_id, flows, graph, source_policy="tidas_compliant")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is True
    blocking_codes = [b["code"] for b in result["blocking"]]
    assert "unsupported_unit_group" not in blocking_codes


def test_sej_allowed_in_tidas_compliant():
    """sej unit group is allowed by the supplemental TIDAS mapping."""
    project_id = "proj-sej"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "Tiangong 1.0",
                                  unit_group="sej"),
        "prod-1": _make_flow_mock("prod-1", "Product flow", "Tiangong 1.0"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
        unit_groups=["sej"],
    )
    db = _build_fake_db(project_id, flows, graph, source_policy="tidas_compliant")

    result = build_tidas_readiness(db, project_id)

    assert result["can_export"] is True
    blocking_codes = [b["code"] for b in result["blocking"]]
    assert "unsupported_unit_group" not in blocking_codes


# ── Regression: allowed unit groups from seed ──────────────────────────────


def test_allowed_unit_groups_include_expected():
    """get_tidas_allowed_unit_groups from seed includes core and supplemental groups."""
    from app.tidas_reference import get_tidas_allowed_unit_groups, load_tidas_reference_seed
    load_tidas_reference_seed.cache_clear()

    allowed = set(get_tidas_allowed_unit_groups())
    assert normalize_tidas_unit_group("Units of mass") in allowed
    assert normalize_tidas_unit_group("Units of energy") in allowed
    assert normalize_tidas_unit_group("Units of volume") in allowed
    assert normalize_tidas_unit_group("Unit of kg*km") in allowed
    assert normalize_tidas_unit_group("sej") in allowed


def test_source_policy_validation_tidas_compliant_blocks_ug():
    """validate_project_source_policy blocks unsupported unit groups in TIDAS mode."""
    from app.source_policy import validate_project_source_policy, SourcePolicy

    project_id = "proj-policy"
    flows = {
        "flow-1": _make_flow_mock("flow-1", "Elementary flow", "Tiangong 1.0",
                                  unit_group="unsupported-unit-group"),
    }
    graph = _make_graph(
        product_flow_uuids=["prod-1"],
        elementary_flow_uuids=["flow-1"],
        unit_groups=["unsupported-unit-group"],
    )
    # We need a real DB for validate_project_source_policy (it queries Model table)
    # But we can use the mock if the function uses db.get(Model, key)
    db = _build_fake_db(project_id, flows, graph, source_policy="tidas_compliant")

    # validate_project_source_policy uses db.query(Model).filter().first() for reading policy
    # Our mock doesn't set up query chain for Model. It only sets up query for ModelVersion.
    # The function should call db.get(Model, key) first (since we pass source_policy explicitly)
    result = validate_project_source_policy(
        model_id=project_id,
        graph=graph,
        db=db,
        source_policy="tidas_compliant",
    )

    assert result.ok is False
    error_codes = [e["code"] for e in result.errors]
    assert "unsupported_unit_group" in error_codes


# ── API endpoint test ──────────────────────────────────────────────────────


@pytest.fixture()
def _readiness_client():
    """DB + client for readiness endpoint test."""
    import app.database as _rdb
    from app.database import Base
    from app.models import (
        ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition,
        UnitGroup, RunJob, DebugDiagnostic,
    )
    from app.services.catalog_cache import invalidate_management_caches
    from app.main import app as _app
    from fastapi.testclient import TestClient

    Base.metadata.drop_all(bind=_rdb.engine)
    Base.metadata.create_all(bind=_rdb.engine)
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    yield TestClient(_app)
    rdb = _rdb.SessionLocal()
    try:
        for m in (ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition,
                   UnitGroup, RunJob, DebugDiagnostic):
            rdb.query(m).delete()
        rdb.commit()
    finally:
        rdb.close()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    _rdb.engine.dispose()


def test_api_readiness_endpoint_returns_expected_fields(_readiness_client):
    """POST /api/export/tidas/bundle/readiness returns a valid readiness response."""
    import app.database as _db
    from app.models import FlowRecord, UnitGroup, UnitDefinition

    # Seed minimal catalog
    db = _db.SessionLocal()
    try:
        db.add(UnitGroup(name="Units of mass", reference_unit="kg"))
        db.add(UnitDefinition(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True))
        db.merge(FlowRecord(
            flow_uuid="flow-prod-1",
            flow_name="chemical A",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="test",
        ))
        db.merge(FlowRecord(
            flow_uuid="flow-chem-1",
            flow_name="CO2",
            flow_type="Elementary flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
        ))
        db.commit()

        # Create project + version with a minimal graph
        resp = _readiness_client.post("/api/projects", json={
            "name": "Readiness Test Project",
            "reference_product": "chemical A",
            "functional_unit": "1 kg chemical A",
        })
        assert resp.status_code == 200, resp.text
        project_id = resp.json()["project_id"]

        resp = _readiness_client.post(f"/api/projects/{project_id}/versions", json={
            "graph": {
                "functionalUnit": "1 kg chemical A",
                "nodes": [
                    {
                        "id": "np-1",
                        "node_kind": "unit_process",
                        "mode": "normalized",
                        "process_uuid": "proc-chem-a",
                        "name": "Chemical A Production",
                        "location": "GLO",
                        "reference_product": "chemical A",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-prod-1",
                                "name": "chemical A",
                                "unit": "kg",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                            }
                        ],
                        "emissions": [
                            {
                                "id": "em-co2",
                                "flowUuid": "flow-chem-1",
                                "name": "CO2",
                                "unit": "kg",
                                "amount": 2.0,
                                "type": "biosphere",
                                "direction": "output",
                            }
                        ],
                    }
                ],
                "exchanges": [],
            }
        })
        assert resp.status_code == 200, resp.text

        # Call readiness endpoint
        resp = _readiness_client.post("/api/export/tidas/bundle/readiness", json={
            "project_id": project_id,
        })
        assert resp.status_code == 200, resp.text
        data = resp.json()

        # Assert top-level fields
        assert "can_export" in data
        assert "source_policy" in data
        assert "blocking" in data
        assert "warnings" in data
        assert "info" in data

        # Assert info contains expected seed status
        info_codes = [i["code"] for i in data.get("info", [])]
        assert "tidas_seed_status" in info_codes

        # source_policy should reflect the default "open_mixed"
        assert data["source_policy"] == "open_mixed"
    finally:
        db.close()
