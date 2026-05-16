"""Tests for source policy Phase 1: classifier, validation, and LCIA scope.

Covered scenarios:
1. classify_flow_source — basic keyword classification.
2. classify_process_source — process source classification.
3. validate_project_source_policy — TIDAS compliant blocks ecoinvent flow.
4. validate_project_source_policy — TIDAS compliant blocks unsupported unit group.
5. validate_project_source_policy — ecoinvent strict blocks tiangong flow.
6. validate_project_source_policy — open mixed does not block but returns source mix.
7. validate_lcia_scope_compatibility — TIDAS compliant blocks non-EF31.
8. validate_lcia_scope_compatibility — ecoinvent strict allows non-EF31.
9. Open mixed default for old projects — no regression.
"""

import pytest
from unittest.mock import MagicMock, patch

from app.source_policy import (
    SourcePolicy,
    AllowedLciaScope,
    classify_flow_source,
    classify_process_source,
    validate_project_source_policy,
    validate_lcia_scope_compatibility,
    ValidationResult,
    get_tidas_allowed_unit_groups,
    set_tidas_allowed_unit_groups,
    _collect_biosphere_flow_uuids,
    _collect_unit_groups,
    SOURCE_SPACE_TIANGONG,
    SOURCE_SPACE_ECOSPREAD,
    SOURCE_SPACE_CUSTOM,
    SOURCE_SPACE_UNKNOWN,
)


# ---------------------------------------------------------------------------
# Helpers: build minimal fake data
# ---------------------------------------------------------------------------


def _make_flow_mock(
    uuid: str,
    flow_type: str,
    source: str | None,
    *,
    is_custom: bool = False,
) -> MagicMock:
    m = MagicMock()
    m.flow_uuid = uuid
    m.flow_type = flow_type
    m.source = source
    m.is_custom = is_custom
    m.flow_name = "dummy"
    m.flow_name_en = None
    m.default_unit = "kg"
    m.unit_group = "kg"
    m.compartment = None
    return m


def _make_model_mock(
    project_id: str,
    source_policy: str | None = None,
    allowed_lcia_scope: str | None = None,
):
    """Return a simple object (not MagicMock) to avoid JSON serialization issues."""
    from types import SimpleNamespace
    return SimpleNamespace(
        id=project_id,
        name=f"Project {project_id}",
        source_policy=source_policy,
        allowed_lcia_scope=allowed_lcia_scope,
    )


def _make_graph(
    elementary_flow_uuids: list[str] | None = None,
    product_flow_uuids: list[str] | None = None,
    unit_groups: list[str] | None = None,
) -> dict:
    """Build a minimal hybrid graph for testing."""
    nodes: list[dict] = []
    node: dict = {
        "id": "proc-1",
        "process_uuid": "proc-1",
        "node_kind": "unit_process",
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
    if unit_groups:
        for ug in unit_groups:
            node["outputs"].append({
                "id": f"out-ug-{ug[:8]}",
                "flowUuid": f"ug-{ug}",
                "name": ug,
                "amount": 1.0,
                "unitGroup": ug,
                "unit_group": ug,
                "isProduct": True,
                "type": "technosphere",
                "direction": "output",
            })
    nodes.append(node)
    return {"nodes": nodes, "exchanges": [], "functionalUnit": "1 kg"}


def _build_fake_db(
    project_id: str,
    flows: dict[str, MagicMock],
    graph_json: dict | None = None,
    source_policy: str | None = None,
    allowed_lcia_scope: str | None = None,
):
    """Return a MagicMock Session with query mock for Model + FlowRecord."""
    from app.models import Model, FlowRecord

    model_mock = _make_model_mock(
        project_id,
        source_policy=source_policy,
        allowed_lcia_scope=allowed_lcia_scope,
    )

    session = MagicMock()

    def get(model_cls, key):
        if hasattr(model_cls, "__tablename__") and model_cls.__tablename__ == "models":
            return model_mock
        if hasattr(model_cls, "__tablename__") and model_cls.__tablename__ == "flow_catalog":
            return flows.get(str(key))
        return None

    session.get = get

    # Mock db.query(Model).filter().first() for source_policy reads in projects.py
    model_query_mock = MagicMock()
    model_filter_mock = MagicMock()
    model_filter_mock.first.return_value = model_mock
    model_query_mock.filter.return_value = model_filter_mock

    # Mock db.query(Model).filter().all() for other queries
    model_query_mock.all.return_value = []

    def session_query(model_cls):
        if hasattr(model_cls, "__tablename__") and model_cls.__tablename__ == "models":
            return model_query_mock
        return MagicMock()

    session.query = session_query
    return session


# ---------------------------------------------------------------------------
# Test 1: classify_flow_source
# ---------------------------------------------------------------------------


class TestClassifyFlowSource:
    def test_ecoinvent_source(self):
        assert classify_flow_source("ecoinvent 3.10") == SOURCE_SPACE_ECOSPREAD
        assert classify_flow_source("ecoinvent 3.11 cut-off") == SOURCE_SPACE_ECOSPREAD
        assert classify_flow_source("ECOSPREAD 3.9") == SOURCE_SPACE_ECOSPREAD

    def test_tiangong_source(self):
        assert classify_flow_source("TianGong 1.0") == SOURCE_SPACE_TIANGONG
        assert classify_flow_source("EF3.1") == SOURCE_SPACE_TIANGONG
        assert classify_flow_source("官方 ilcd") == SOURCE_SPACE_TIANGONG

    def test_custom_is_custom(self):
        assert classify_flow_source("any", is_custom=True) == SOURCE_SPACE_CUSTOM
        assert classify_flow_source("ecoinvent", is_custom=True) == SOURCE_SPACE_CUSTOM

    def test_null_is_unknown(self):
        assert classify_flow_source(None) == SOURCE_SPACE_UNKNOWN
        assert classify_flow_source("") == SOURCE_SPACE_UNKNOWN
        assert classify_flow_source("reference") == SOURCE_SPACE_UNKNOWN
        assert classify_flow_source("undefined") == SOURCE_SPACE_UNKNOWN
        assert classify_flow_source("openLCA") == SOURCE_SPACE_UNKNOWN

    def test_case_insensitive(self):
        assert classify_flow_source("ECOINVENT 3.10") == SOURCE_SPACE_ECOSPREAD
        assert classify_flow_source("tiangong 1.0") == SOURCE_SPACE_TIANGONG


# ---------------------------------------------------------------------------
# Test 2: classify_process_source
# ---------------------------------------------------------------------------


class TestClassifyProcessSource:
    def test_test_import_mode(self):
        assert classify_process_source(import_mode="test") == "test"

    def test_falls_back_to_source_file(self):
        assert classify_process_source(source_file="ecoinvent 3.10") == SOURCE_SPACE_ECOSPREAD
        assert classify_process_source(source_file="TianGong 1.0") == SOURCE_SPACE_TIANGONG
        assert classify_process_source(source_file=None) == SOURCE_SPACE_UNKNOWN


# ---------------------------------------------------------------------------
# Test 3: validate_project_source_policy — TIDAS compliant
# ---------------------------------------------------------------------------


class TestValidateTidasCompliant:
    def test_tidas_compliant_blocks_ecoinvent_elementary_flow(self):
        """TIDAS compliant must block ecoinvent elementary flows."""
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
            "ef-1": _make_flow_mock("ef-1", "Elementary flow", "EF3.1"),
        }
        graph = _make_graph(elementary_flow_uuids=["ec-1", "ef-1"])
        db = _build_fake_db("proj-1", flows, source_policy="tidas_compliant")

        result = validate_project_source_policy("proj-1", graph, db)
        assert result.ok is False
        assert any(e["code"] == "ecoinvent_elementary_flow" for e in result.errors)

    def test_tidas_compliant_blocks_unsupported_unit_group(self):
        """TIDAS compliant must block unit groups not in the allowed list."""
        set_tidas_allowed_unit_groups(["kg", "liter"])
        try:
            flows = {}
            graph = _make_graph(
                elementary_flow_uuids=["ef-1"],
                unit_groups=["kg", "m3"],
            )
            db = _build_fake_db("proj-ug", flows, source_policy="tidas_compliant")

            result = validate_project_source_policy("proj-ug", graph, db)
            assert result.ok is False
            assert any(e["code"] == "unsupported_unit_group" for e in result.errors)
        finally:
            # Reset to sentinel
            set_tidas_allowed_unit_groups(None)

    def test_tidas_compliant_allows_ef_tiangong_only(self):
        """TIDAS compliant allows only tiangong/EF elementary flows."""
        flows = {
            "ef-1": _make_flow_mock("ef-1", "Elementary flow", "EF3.1"),
            "tg-1": _make_flow_mock("tg-1", "Elementary flow", "TianGong 1.0"),
        }
        graph = _make_graph(elementary_flow_uuids=["ef-1", "tg-1"])
        db = _build_fake_db("proj-ok", flows, source_policy="tidas_compliant")

        result = validate_project_source_policy("proj-ok", graph, db)
        assert result.ok is True

    def test_tidas_compliant_blocks_lci_dataset_node(self):
        """TIDAS compliant must block ecoinvent LCI dataset nodes."""
        graph = _make_graph(elementary_flow_uuids=["ef-1"])
        graph["nodes"].append({
            "id": "eco-ds-1",
            "process_uuid": "eco-ds-1",
            "node_kind": "lci_dataset",
            "mode": "normalized",
            "reference_product": "eco ref",
            "name": "Ecoinvent Dataset",
            "location": "GLO",
            "inputs": [],
            "outputs": [],
        })
        flows = {
            "ef-1": _make_flow_mock("ef-1", "Elementary flow", "EF3.1"),
        }
        db = _build_fake_db("proj-ld", flows, source_policy="tidas_compliant")

        result = validate_project_source_policy("proj-ld", graph, db)
        assert result.ok is False
        assert any(e["code"] == "ecoinvent_lci_dataset_node" for e in result.errors)


# ---------------------------------------------------------------------------
# Test 4: validate_project_source_policy — ecoinvent strict
# ---------------------------------------------------------------------------


class TestValidateEcoinventStrict:
    def test_ecoinvent_strict_blocks_tiangong_elementary_flow(self):
        """Ecoinvent strict must block tiangong elementary flows."""
        flows = {
            "tg-1": _make_flow_mock("tg-1", "Elementary flow", "TianGong 1.0"),
            "ef-1": _make_flow_mock("ef-1", "Elementary flow", "EF3.1"),
        }
        graph = _make_graph(elementary_flow_uuids=["tg-1", "ef-1"])
        db = _build_fake_db("proj-strict", flows, source_policy="ecoinvent_strict")

        result = validate_project_source_policy("proj-strict", graph, db)
        assert result.ok is False
        assert any(e["code"] == "tiangong_elementary_flow" for e in result.errors)

    def test_ecoinvent_strict_allows_ecoinvent_elementary(self):
        """Ecoinvent strict allows ecoinvent elementary flows."""
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
        }
        graph = _make_graph(elementary_flow_uuids=["ec-1"])
        db = _build_fake_db("proj-eco-ok", flows, source_policy="ecoinvent_strict")

        result = validate_project_source_policy("proj-eco-ok", graph, db)
        assert result.ok is True

    def test_ecoinvent_strict_warns_unknown_source(self):
        """Ecoinvent strict warns on unknown source elementary flows."""
        flows = {
            "unk-1": _make_flow_mock("unk-1", "Elementary flow", None),
        }
        graph = _make_graph(elementary_flow_uuids=["unk-1"])
        db = _build_fake_db("proj-unk", flows, source_policy="ecoinvent_strict")

        result = validate_project_source_policy("proj-unk", graph, db)
        assert result.ok is True  # warnings only, not errors
        assert any(w["code"] == "unknown_elementary_flow_source" for w in result.warnings)


# ---------------------------------------------------------------------------
# Test 5: validate_project_source_policy — open mixed
# ---------------------------------------------------------------------------


class TestValidateOpenMixed:
    def test_open_mixed_does_not_block(self):
        """Open mixed must not block, even with mixed sources."""
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
            "tg-1": _make_flow_mock("tg-1", "Elementary flow", "TianGong 1.0"),
            "unk-1": _make_flow_mock("unk-1", "Elementary flow", None),
        }
        graph = _make_graph(elementary_flow_uuids=["ec-1", "tg-1", "unk-1"])
        db = _build_fake_db("proj-mixed", flows, source_policy="open_mixed")

        result = validate_project_source_policy("proj-mixed", graph, db)
        assert result.ok is True

    def test_open_mixed_returns_source_mix_info(self):
        """Open mixed should record source mix summary in info."""
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
            "tg-1": _make_flow_mock("tg-1", "Elementary flow", "TianGong 1.0"),
        }
        graph = _make_graph(elementary_flow_uuids=["ec-1", "tg-1"])
        db = _build_fake_db("proj-mix-info", flows, source_policy="open_mixed")

        result = validate_project_source_policy("proj-mix-info", graph, db)
        assert result.ok is True
        assert any(i["code"] == "source_mix_summary" for i in result.info)

    def test_open_mixed_warns_on_unknown_source(self):
        """Open mixed warns but does not block unknown source flows."""
        flows = {
            "unk-1": _make_flow_mock("unk-1", "Elementary flow", "BILBI"),
        }
        graph = _make_graph(elementary_flow_uuids=["unk-1"])
        db = _build_fake_db("proj-unk-warn", flows, source_policy="open_mixed")

        result = validate_project_source_policy("proj-unk-warn", graph, db)
        assert result.ok is True
        assert any(w["code"] == "unknown_flow_source" for w in result.warnings)


# ---------------------------------------------------------------------------
# Test 6: validate_lcia_scope_compatibility
# ---------------------------------------------------------------------------


class TestValidateLciaScope:
    def test_tidas_compliant_blocks_non_ef31(self):
        """TIDAS compliant must reject non-EF v3.1 LCIA methods."""
        graph = _make_graph(elementary_flow_uuids=["ef-1"])
        db = _build_fake_db("proj-lcia", graph, source_policy="tidas_compliant")

        with pytest.raises(Exception) as exc_info:
            validate_lcia_scope_compatibility(
                "proj-lcia", graph, ["ReCIPe 2016 Midpoint (H)"], db,
            )
        assert "SOURCE_POLICY_VIOLATION" in str(exc_info.value)

    def test_tidas_compliant_allows_ef31(self):
        """TIDAS compliant allows EF v3.1."""
        graph = _make_graph(elementary_flow_uuids=["ef-1"])
        db = _build_fake_db("proj-ef31", graph, source_policy="tidas_compliant")

        # Should not raise
        result = validate_lcia_scope_compatibility("proj-ef31", graph, ["EF v3.1"], db)
        assert result.ok is True

    def test_ecoinvent_strict_allows_non_ef31(self):
        """Ecoinvent strict allows non-EF v3.1 methods."""
        graph = _make_graph(elementary_flow_uuids=["ec-1"])
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
        }
        db = _build_fake_db("proj-eco-methods", flows, source_policy="ecoinvent_strict")

        result = validate_lcia_scope_compatibility(
            "proj-eco-methods", graph, ["ReCIPe 2016 Midpoint (H)"], db,
        )
        assert result.ok is True

    def test_open_mixed_non_ef31_with_non_eco_flows_blocks(self):
        """Open mixed with non-ecoinvent elementary flows must block non-EF31 methods."""
        flows = {
            "tg-1": _make_flow_mock("tg-1", "Elementary flow", "TianGong 1.0"),
        }
        graph = _make_graph(elementary_flow_uuids=["tg-1"])
        db = _build_fake_db("proj-open-ec", flows, source_policy="open_mixed")

        with pytest.raises(Exception) as exc_info:
            validate_lcia_scope_compatibility(
                "proj-open-ec", graph, ["ReCIPe 2016 Midpoint (H)"], db,
            )
        assert "lcia_method_incompatible_with_elementary_sources" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test 7: backward compatibility — old projects default to open_mixed
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    def test_old_project_none_source_policy_defaults_to_open_mixed(self):
        """Projects with no source_policy should default to open_mixed (backward compat)."""
        flows = {
            "ec-1": _make_flow_mock("ec-1", "Elementary flow", "ecoinvent 3.10"),
            "tg-1": _make_flow_mock("tg-1", "Elementary flow", "TianGong 1.0"),
        }
        graph = _make_graph(elementary_flow_uuids=["ec-1", "tg-1"])
        # source_policy is None — simulates old project without the column
        db = _build_fake_db("proj-old", flows, source_policy=None)

        result = validate_project_source_policy("proj-old", graph, db)
        # Should NOT block
        assert result.ok is True
        # Should default to open_mixed
        assert result.source_policy == "open_mixed"

    def test_old_project_none_policy_allows_all_methods(self):
        """Old projects with no source_policy should allow EF v3.1 without restriction."""
        graph = _make_graph(elementary_flow_uuids=["ef-1"])
        db = _build_fake_db("proj-old2", graph, source_policy=None)

        result = validate_lcia_scope_compatibility("proj-old2", graph, ["EF v3.1"], db)
        assert result.ok is True


# ---------------------------------------------------------------------------
# Test 8: ValidationResult utility
# ---------------------------------------------------------------------------


class TestValidationResult:
    def test_ok_when_no_errors(self):
        r = ValidationResult()
        assert r.ok is True

    def test_not_ok_after_add_error(self):
        r = ValidationResult()
        r.add_error("test_code", "test message")
        assert r.ok is False
        assert len(r.errors) == 1
        assert r.errors[0]["code"] == "test_code"
        assert r.errors[0]["message"] == "test message"

    def test_add_warning_and_info(self):
        r = ValidationResult()
        r.add_warning("warn_code", "warn message")
        r.add_info("info_code", "info message")
        assert len(r.warnings) == 1
        assert len(r.info) == 1

    def test_add_error_with_details(self):
        r = ValidationResult()
        r.add_error("err", "msg", {"key": "value"})
        assert r.errors[0]["details"] == {"key": "value"}


# ---------------------------------------------------------------------------
# Test 9: Unit group helpers
# ---------------------------------------------------------------------------


class TestUnitGroupHelpers:
    def test_collect_biosphere_flow_uuids(self):
        graph = _make_graph(elementary_flow_uuids=["uuid-a", "uuid-b"])
        uuids = _collect_biosphere_flow_uuids(graph)
        assert uuids == {"uuid-a", "uuid-b"}

    def test_collect_biosphere_flow_uuids_no_emissions(self):
        graph = _make_graph()
        uuids = _collect_biosphere_flow_uuids(graph)
        assert uuids == set()

    def test_collect_unit_groups(self):
        graph = _make_graph(unit_groups=["kg", "liter"])
        groups = _collect_unit_groups(graph)
        assert groups == {"kg", "liter"}

    def test_get_tidas_allowed_unit_groups_fallback(self):
        # Reset sentinel
        set_tidas_allowed_unit_groups(None)
        assert get_tidas_allowed_unit_groups() == []

    def test_set_and_get_tidas_allowed_unit_groups(self):
        allowed = ["kg", "liter", "m3"]
        set_tidas_allowed_unit_groups(allowed)
        try:
            assert get_tidas_allowed_unit_groups() == [g.lower() for g in allowed]
        finally:
            set_tidas_allowed_unit_groups(None)
