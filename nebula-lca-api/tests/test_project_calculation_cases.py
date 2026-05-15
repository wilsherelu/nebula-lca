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
    ],
)
def test_project_case_closed_loop_calculation(client, solver_url, case_name):
    db = _db_module.SessionLocal()
    try:
        result = run_project_case(client, db, CASES_ROOT / case_name)
        assert_case_result(result)
    finally:
        db.close()
