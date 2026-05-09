"""Tests for source-space hardening in tidas_export.

Scenarios covered:
1. EF/TianGong-only model → preview can_export=True
2. PTS module model → preview can_export=False, PTS error
3. ecoinvent elementary flow model → preview can_export=False, source-space error
4. Unknown-source elementary flow model → preview can_export=False, unknown-source warning
5. Blocked projects → export returns 400, no ZIP
"""

import ast
import io
import json
import zipfile
import pytest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------
from app.tidas_export import (
    _classify_source_space,
    _scan_source_space,
    preview_export,
    export_bundle,
    ExportReport,
    SOURCE_SPACE_EF_TIANGONG,
    SOURCE_SPACE_ECOSPREAD,
    SOURCE_SPACE_UNKNOWN,
    SOURCE_SPACE_TIDAS_BLOCKED_CODE,
    PTS_TIDAS_EXPORT_ERROR_CODE,
)


# ---------------------------------------------------------------------------
# Helpers to build minimal fake DB session
# ---------------------------------------------------------------------------

def _make_flow_mock(uuid: str, flow_type: str, source: str | None) -> MagicMock:
    m = MagicMock()
    m.flow_uuid = uuid
    m.flow_type = flow_type
    m.source = source
    m.flow_name = "dummy"
    m.flow_name_en = None
    m.default_unit = "kg"
    m.unit_group = "kg"
    m.compartment = None
    m.source_updated_at = None
    m.is_custom = False
    return m


def _make_flow_mock_with_meta(
    uuid: str,
    flow_type: str,
    source: str | None,
    *,
    compartment: str | None = None,
    is_custom: bool = False,
) -> MagicMock:
    m = _make_flow_mock(uuid, flow_type, source)
    m.compartment = compartment
    m.is_custom = is_custom
    return m


def _make_version_mock(graph_json: dict):
    from types import SimpleNamespace
    return SimpleNamespace(hybrid_graph_json=graph_json, version=1)


def _make_model_mock(project_id: str):
    """Return a simple object (not MagicMock) to avoid JSON serialization issues."""
    from types import SimpleNamespace
    return SimpleNamespace(
        id=project_id,
        name=f"Project {project_id}",
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
    has_pts: bool = False,
) -> dict:
    """Build a minimal hybrid graph for testing."""
    nodes: list[dict] = []
    # single unit process node
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
                "type": "biosphere",
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


def _build_fake_db(
    project_id: str,
    flows: dict[str, MagicMock],
    graph_json: dict,
    has_ref_process: bool = False,
):
    """Return a MagicMock Session that returns flows by db.get(FlowRecord, uuid).

    Also mocks db.query(ModelVersion) to return a chain ending with our version_mock.
    """
    from app.models import Model, ModelVersion, FlowRecord, ReferenceProcess

    model_mock = _make_model_mock(project_id)
    version_mock = _make_version_mock(graph_json)

    session = MagicMock()

    def get(model_cls, key):
        # Use isinstance against the actual SQLAlchemy model classes
        if isinstance(model_cls, type) and issubclass(model_cls, Model):
            return model_mock
        if isinstance(model_cls, type) and issubclass(model_cls, ModelVersion):
            return version_mock
        if isinstance(model_cls, type) and issubclass(model_cls, FlowRecord):
            return flows.get(key)
        if isinstance(model_cls, type) and issubclass(model_cls, ReferenceProcess):
            return MagicMock() if has_ref_process else None
        return None

    session.get = get

    # Mock db.query(ModelVersion).filter().order_by().first() chain
    query_mock = MagicMock()
    filter_mock = MagicMock()
    order_mock = MagicMock()
    order_mock.first.return_value = version_mock
    filter_mock.order_by.return_value = order_mock
    query_mock.filter.return_value = filter_mock
    session.query.return_value = query_mock

    return session


# ---------------------------------------------------------------------------
# Test 1: _classify_source_space
# ---------------------------------------------------------------------------

class TestClassifySourceSpace:
    def test_ef_keyword(self):
        assert _classify_source_space("EF3.1.104") == SOURCE_SPACE_EF_TIANGONG
        assert _classify_source_space("ecoinvent 3.11 cut-off EF") == SOURCE_SPACE_ECOSPREAD  # ecoinvent first
        assert _classify_source_space("TianGong 1.1") == SOURCE_SPACE_EF_TIANGONG
        assert _classify_source_space("official ILCD") == SOURCE_SPACE_EF_TIANGONG
        assert _classify_source_space("EF31") == SOURCE_SPACE_EF_TIANGONG

    def test_ecoinvent(self):
        assert _classify_source_space("ecoinvent 3.10") == SOURCE_SPACE_ECOSPREAD
        assert _classify_source_space("ecoinvent.org") == SOURCE_SPACE_ECOSPREAD

    def test_unknown(self):
        assert _classify_source_space(None) == SOURCE_SPACE_UNKNOWN
        assert _classify_source_space("") == SOURCE_SPACE_UNKNOWN
        assert _classify_source_space("BILBI") == SOURCE_SPACE_UNKNOWN
        assert _classify_source_space("openLCA") == SOURCE_SPACE_UNKNOWN

    def test_case_insensitive(self):
        assert _classify_source_space("ECOINVENT 3.10") == SOURCE_SPACE_ECOSPREAD
        assert _classify_source_space("TIANgong") == SOURCE_SPACE_EF_TIANGONG

    # P1 fix: "reference" / "undefined" must NOT match as EF/TianGong
    def test_reference_not_ef(self):
        assert _classify_source_space("reference") == SOURCE_SPACE_UNKNOWN
        assert _classify_source_space("openLCA reference") == SOURCE_SPACE_UNKNOWN
        assert _classify_source_space("undefined") == SOURCE_SPACE_UNKNOWN

    def test_ef_standalone_matches(self):
        assert _classify_source_space("ef") == SOURCE_SPACE_EF_TIANGONG
        assert _classify_source_space("my ef data") == SOURCE_SPACE_EF_TIANGONG


# ---------------------------------------------------------------------------
# Test 2: _scan_source_space — no blocking
# ---------------------------------------------------------------------------

class TestScanSourceSpaceNoBlock:
    def test_ef_tiangong_only(self):
        flows = {
            "ef-flow-1": _make_flow_mock("ef-flow-1", "Elementary flow", "EF3.1"),
            "ef-flow-2": _make_flow_mock("ef-flow-2", "Elementary flow", "TianGong 1.0"),
            "prod-1": _make_flow_mock("prod-1", "Product flow", "TianGong 1.0"),
        }
        db = _build_fake_db("proj-1", flows, _make_graph())
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is True
        assert report.has_errors() is False

    def test_product_flow_no_block(self):
        """Product flows with non-EF source must NOT block."""
        flows = {
            "prod-1": _make_flow_mock("prod-1", "Product flow", "ecoinvent 3.10"),
        }
        db = _build_fake_db("proj-1", flows, _make_graph(product_flow_uuids=["prod-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is True
        assert report.has_errors() is False


# ---------------------------------------------------------------------------
# Test 3: _scan_source_space — ecoinvent blocks
# ---------------------------------------------------------------------------

class TestScanSourceSpaceEcoinventBlock:
    def test_ecoinvent_elementary_blocks(self):
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
            "ec-2": _make_flow_mock("ec-2", "Elementary flow", "ecoinvent 3.11 cutoff"),
            "ef-1": _make_flow_mock("ef-1", "Elementary flow", "EF 3.1"),
        }
        db = _build_fake_db("proj-1", flows, _make_graph(elementary_flow_uuids=["ec-1", "ec-2", "ef-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True
        # Check error code
        found = any(SOURCE_SPACE_TIDAS_BLOCKED_CODE in e for e in report.errors)
        assert found, f"Expected error code in: {report.errors}"
        # Check warning context
        ec_warnings = [w for w in report.warnings if w.category == "unsupported_source_space"]
        assert len(ec_warnings) >= 1
        assert ec_warnings[0].context.get("source_space") == SOURCE_SPACE_ECOSPREAD


# ---------------------------------------------------------------------------
# Test 4: _scan_source_space — unknown source blocks
# ---------------------------------------------------------------------------

class TestScanSourceSpaceUnknownBlock:
    def test_unknown_source_blocks(self):
        flows = {
            "unk-1": _make_flow_mock("unk-1", "Elementary flow", "BILBI"),
            "unk-2": _make_flow_mock("unk-2", "Elementary flow", "openLCA"),
            "ef-1": _make_flow_mock("ef-1", "Elementary flow", "EF3.1"),
        }
        db = _build_fake_db("proj-1", flows, _make_graph(elementary_flow_uuids=["unk-1", "unk-2", "ef-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True
        found = any("unknown" in e.lower() for e in report.errors)
        assert found, f"Expected unknown-source error in: {report.errors}"


# P1 regression: "reference" / "undefined" must block, not pass
class TestScanSourceSpaceReferenceUndefined:
    def test_reference_source_blocks(self):
        """flows with source='reference' must be blocked as unknown source-space."""
        flows = {
            "ref-1": _make_flow_mock("ref-1", "Elementary flow", "reference"),
            "ref-2": _make_flow_mock("ref-2", "Elementary flow", "openLCA reference"),
        }
        db = _build_fake_db("proj-ref", flows, _make_graph(elementary_flow_uuids=["ref-1", "ref-2"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True


class TestScanSourceSpaceSourceLabels:
    def test_builtin_ef_source_passes(self):
        flows = {
            "legacy-1": _make_flow_mock_with_meta(
                "legacy-1",
                "Elementary flow",
                "ef3.1",
                compartment="Emissions;Emissions to air",
                is_custom=False,
            ),
            "legacy-2": _make_flow_mock_with_meta(
                "legacy-2",
                "Elementary flow",
                "ef3.1",
                compartment="Resources;Resources from ground",
                is_custom=False,
            ),
        }
        db = _build_fake_db("proj-legacy", flows, _make_graph(elementary_flow_uuids=["legacy-1", "legacy-2"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is True
        assert report.has_errors() is False

    def test_null_source_blocks_even_when_not_custom(self):
        flows = {
            "legacy-1": _make_flow_mock_with_meta(
                "legacy-1",
                "Elementary flow",
                None,
                compartment=None,
                is_custom=False,
            ),
        }
        db = _build_fake_db("proj-legacy", flows, _make_graph(elementary_flow_uuids=["legacy-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True

    def test_external_import_source_blocks(self):
        flows = {
            "external-1": _make_flow_mock_with_meta(
                "external-1",
                "Elementary flow",
                "external_import",
                compartment="Emissions;Emissions to air",
                is_custom=False,
            ),
        }
        db = _build_fake_db("proj-external", flows, _make_graph(elementary_flow_uuids=["external-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True

    def test_undefined_source_blocks(self):
        flows = {
            "undef-1": _make_flow_mock("undef-1", "Elementary flow", "undefined"),
        }
        db = _build_fake_db("proj-undef", flows, _make_graph(elementary_flow_uuids=["undef-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True


# P2 fix: flow_type variants like "elementary_flow" must also be caught
class TestScanSourceSpaceFlowTypeVariants:
    def test_underscore_variant_blocks(self):
        """flow_type='elementary_flow' with ecoinvent source must block."""
        flows = {
            "ef-1": _make_flow_mock("ef-1", "elementary_flow", "ecoinvent 3.10"),
        }
        db = _build_fake_db("proj-ft", flows, _make_graph(elementary_flow_uuids=["ef-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True

    def test_lowercase_variant_blocks(self):
        """flow_type='elementary flow' (lowercase) must also be caught."""
        flows = {
            "ef-1": _make_flow_mock("ef-1", "elementary flow", "ecoinvent 3.10"),
        }
        db = _build_fake_db("proj-ft", flows, _make_graph(elementary_flow_uuids=["ef-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is False
        assert report.has_errors() is True

    def test_non_elementary_flow_not_blocked_by_source(self):
        """Product flow with any flow_type variant must NOT be subject to source-space check."""
        flows = {
            "pf-1": _make_flow_mock("pf-1", "Product flow", "ecoinvent 3.10"),
        }
        db = _build_fake_db("proj-pf", flows, _make_graph(product_flow_uuids=["pf-1"]))
        report = ExportReport()
        passed = _scan_source_space(db, set(flows.keys()), report)
        assert passed is True


# ---------------------------------------------------------------------------
# Test 5: preview_export — end-to-end scenarios
# ---------------------------------------------------------------------------

class TestPreviewExport:
    def test_preview_ef_only_can_export(self):
        flows = {
            "ef-1": _make_flow_mock("ef-1", "Elementary flow", "EF3.1"),
            "prod-1": _make_flow_mock("prod-1", "Product flow", "TianGong 1.0"),
        }
        graph = _make_graph(elementary_flow_uuids=["ef-1"], product_flow_uuids=["prod-1"])
        db = _build_fake_db("proj-1", flows, graph)

        result = preview_export(db, "proj-1")
        assert result["can_export"] is True

    def test_preview_pts_block(self):
        """PTS model → can_export=False, PTS error present."""
        graph = _make_graph(has_pts=True)
        db = _build_fake_db("proj-pts", {}, graph)
        result = preview_export(db, "proj-pts")
        assert result["can_export"] is False
        found = any(PTS_TIDAS_EXPORT_ERROR_CODE in e for e in result["errors"])
        assert found, f"Expected PTS error in: {result['errors']}"

    def test_preview_ecoinvent_block(self):
        """ecoinvent elementary → can_export=False, source-space error."""
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
            "prod-1": _make_flow_mock("prod-1", "Product flow", "TianGong 1.0"),
        }
        graph = _make_graph(elementary_flow_uuids=["ec-1"], product_flow_uuids=["prod-1"])
        db = _build_fake_db("proj-ec", flows, graph)
        result = preview_export(db, "proj-ec")
        assert result["can_export"] is False
        found = any(SOURCE_SPACE_TIDAS_BLOCKED_CODE in e for e in result["errors"])
        assert found, f"Expected source-space error in: {result['errors']}"
        assert len(result["warnings"]) > 0
        assert any(w["category"] == "unsupported_source_space" for w in result["warnings"])

    def test_preview_unknown_source_block(self):
        """Unknown-source elementary → can_export=False."""
        flows = {
            "unk-1": _make_flow_mock("unk-1", "Elementary flow", "openLCA"),
            "prod-1": _make_flow_mock("prod-1", "Product flow", "TianGong 1.0"),
        }
        graph = _make_graph(elementary_flow_uuids=["unk-1"], product_flow_uuids=["prod-1"])
        db = _build_fake_db("proj-unk", flows, graph)
        result = preview_export(db, "proj-unk")
        assert result["can_export"] is False
        assert len(result["errors"]) > 0


# ---------------------------------------------------------------------------
# Test 6: export_bundle — blocked projects raise ExportError
# ---------------------------------------------------------------------------

class TestExportBundle:
    def test_export_ecoinvent_raises(self):
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
        }
        graph = _make_graph(elementary_flow_uuids=["ec-1"])
        db = _build_fake_db("proj-ec", flows, graph, has_ref_process=True)

        from app.tidas_export import ExportError
        with pytest.raises(ExportError, match=SOURCE_SPACE_TIDAS_BLOCKED_CODE):
            export_bundle(db, "proj-ec")

    def test_export_pts_raises(self):
        graph = _make_graph(has_pts=True)
        db = _build_fake_db("proj-pts", {}, graph)
        from app.tidas_export import ExportError
        with pytest.raises(ExportError):
            export_bundle(db, "proj-pts")


# ---------------------------------------------------------------------------
# Test 7: exported dataset skeleton
# ---------------------------------------------------------------------------

class TestExportBundleSchemaSkeleton:
    def test_export_bundle_contains_tidas_dataset_skeleton(self):
        """Exported datasets should include Tiangong/ILCD required skeleton fields."""
        flow_uuid = "08a91e70-3ddc-11dd-9c14-0050c2490048"
        flows = {
            flow_uuid: _make_flow_mock_with_meta(
                flow_uuid,
                "Elementary flow",
                "EF3.1",
                compartment="air",
            ),
        }
        graph = _make_graph(elementary_flow_uuids=[flow_uuid])
        db = _build_fake_db("proj-schema", flows, graph)

        zip_bytes, report = export_bundle(db, "proj-schema")
        assert report.has_errors() is False

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            flow_name = next(n for n in zf.namelist() if n.startswith("flows/"))
            process_name = next(n for n in zf.namelist() if n.startswith("processes/"))
            model_name = next(n for n in zf.namelist() if n.startswith("lifecyclemodels/"))

            flow = json.loads(zf.read(flow_name).decode("utf-8"))["flowDataSet"]
            process = json.loads(zf.read(process_name).decode("utf-8"))["processDataSet"]
            model = json.loads(zf.read(model_name).decode("utf-8"))["lifeCycleModelDataSet"]

        for key in ("@xmlns", "@xmlns:common", "@xmlns:xsi", "@version", "@locations", "@xsi:schemaLocation"):
            assert key in flow
            assert key in process
            assert key in model

        assert "@xmlns:ecn" in flow
        assert "classificationInformation" in flow["flowInformation"]["dataSetInformation"]
        assert "quantitativeReference" in flow["flowInformation"]
        assert "administrativeInformation" in flow
        assert "modellingAndValidation" in flow

        assert "classificationInformation" in process["processInformation"]["dataSetInformation"]
        assert "common:generalComment" in process["processInformation"]["dataSetInformation"]
        assert "time" in process["processInformation"]
        assert "geography" in process["processInformation"]
        assert process["processInformation"]["quantitativeReference"]["@type"] == "Reference flow(s)"
        assert isinstance(process["exchanges"]["exchange"], list)

        model_info = model["lifeCycleModelInformation"]
        assert "classificationInformation" in model_info["dataSetInformation"]
        assert "quantitativeReference" in model_info
        assert "technology" in model_info


# ---------------------------------------------------------------------------
# Test 8: syntax check
# ---------------------------------------------------------------------------

class TestSyntax:
    def test_ast_parse(self):
        """Verify tidas_export.py has no syntax errors."""
        import os
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent = os.path.dirname(script_dir)
        module_path = os.path.join(parent, "app", "tidas_export.py")
        with open(module_path, encoding="utf-8") as f:
            ast.parse(f.read())
