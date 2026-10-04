from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.api.paginated_projects import _router as projects_router
from app.api.reference_data import _api_router as reference_router
from app.database import Base, get_db
from app.models import Model
from app.services.catalog_cache import invalidate_management_caches


def test_stats_count_visible_projects_and_preserve_background_resources(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'stats.db'}")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add_all([
            Model(id="steel", name="Steel", status="active"),
            Model(id="electricity", name="Electricity", status="resource"),
            Model(id="grid", name="Grid", status="resource"),
        ])
        db.commit()

    def database():
        with Session(engine) as db:
            yield db

    app = FastAPI()
    app.include_router(reference_router)
    app.include_router(projects_router, prefix="/api/projects")
    app.dependency_overrides[get_db] = database
    invalidate_management_caches()
    try:
        with TestClient(app) as client:
            stats = client.get("/api/stats")
            visible = client.get("/api/projects")
            resources = client.get("/api/projects?include_resources=true")
            assert stats.status_code == visible.status_code == resources.status_code == 200
            assert stats.json()["projects"] == visible.json()["total"] == 1
            assert resources.json()["total"] == 3
            assert {item["project_id"] for item in resources.json()["items"]} == {
                "steel", "electricity", "grid",
            }
    finally:
        invalidate_management_caches()
        engine.dispose()
