from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.database as _db_module
from app.database import Base
from app.main import app
from app.models import FlowRecord, Model, ModelVersion, ReferenceProcess, RunJob, UnitDefinition, UnitGroup
from app.services.catalog_cache import invalidate_management_caches
from project_case_runner import assert_case_result, run_project_case, solver_server


CASES_ROOT = Path(__file__).resolve().parent / "fixtures" / "project_cases"


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.drop_all(bind=_db_module.engine)
    Base.metadata.create_all(bind=_db_module.engine)
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    yield
    db = _db_module.SessionLocal()
    try:
        for model in (RunJob, ModelVersion, Model, ReferenceProcess, FlowRecord, UnitDefinition, UnitGroup):
            db.query(model).delete()
        db.commit()
    finally:
        db.close()
    invalidate_management_caches(projects=True, flows=True, reference_processes=True, stats=True)
    _db_module.engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture(scope="module")
def solver_url(tmp_path_factory):
    with solver_server(tmp_path_factory.mktemp("project_case_solver_runtime")) as url:
        yield url


@pytest.mark.parametrize(
    "case_name",
    [
        "single_process",
        "chain_two_processes",
        "ecoinvent_lci_provider",
        "parallel_merge",
        "multi_product_same_unit_group",
        "mixed_tiangong_ecoinvent_ef31",
        "eco_lci_to_custom_process",
        # ── real-project regression fixtures ──
        "real_multi_process_multi_product_no_cycle",
    ],
)
def test_project_case_closed_loop_calculation(client, solver_url, case_name):
    db = _db_module.SessionLocal()
    try:
        result = run_project_case(client, db, CASES_ROOT / case_name)
        assert_case_result(result)
    finally:
        db.close()


# ── PTS single-module smoke test ──────────────────────────────────────────

def test_project_case_pts_single_module_smoke(client, solver_url):
    """PTS smoke: 1 pts_module, 1 product VP, CO2 emission, no secondary process.

    Verifies: PTS seed → version save → /api/model/run → RunJob completes.
    Expected: Climate change = 0.5 (direct CO2 fossil emission).
    """
    db = _db_module.SessionLocal()
    try:
        result = run_project_case(
            client, db, CASES_ROOT / "pts_single_module_smoke", use_pts=True,
        )
        assert_case_result(result)
    finally:
        db.close()


# ── PTS two-module coupled regression (xfail: edge binding unverified) ───

def test_project_case_real_pts_two_modules_coupled(client, solver_url):
    """PTS regression: two coupled PTS modules (diesel + electricity) with inter-supply edges.

    This test exercises the full PTS coupling pipeline:
      - 2 pts_module nodes with internal canvases
      - 2 inter-PTS technosphere edges (diesel→electricity, electricity→diesel)
      - Pre-seeded PTS resources / definitions / compile artifacts / external artifacts
      - Dynamic graph hash computation

    EXPECTED FAILURE: Edge binding validation in version creation fails for
    pts_module ↔ pts_module technosphere links. The normalized graph produces
    edge targets that don't match any known ports in the pts_module's internal
    scope, causing /api/projects/{id}/versions to return 400.

    When this regression is fixed (PTS normalization should preserve technosphere
    edges between pts_module nodes, and edge binding should accept them), the
    expected results are:
      - diesel PTS climate change: 0.9616985845129058
      - electricity PTS climate change: 0.9983347210657786
      - indicator_count: 25, missing_ef31_flow_count: 0
    """
    pytest.xfail(
        "PTS edge binding fails for pts_module↔pts_module technosphere edges "
        "(version creation API returns 400). Fix in PTS normalization/binding "
        "before enabling this test."
    )
