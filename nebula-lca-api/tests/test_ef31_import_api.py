"""API-level tests for EF 3.1 LCI import endpoints.

Uses testclient to hit /import/ef31/preview, /import/ef31/commit,
and /import/ef31/reports/{job_id}.

Uses dynamic .7z generation from fixture SPOLD + MasterData XML.
"""

import json
import os
import tempfile
import uuid
from pathlib import Path

import pytest
import py7zr
from fastapi.testclient import TestClient

# Keep API tests isolated from the developer's real lca_demo.db.
_TEST_DB = Path(tempfile.gettempdir()) / f"nebula_ef31_import_api_{uuid.uuid4().hex}.db"
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_TEST_DB.as_posix()}")

# Ensure app imports work
from app.main import app
from app.database import Base, engine, SessionLocal
from app.models import DebugDiagnostic, ReferenceProcess


@pytest.fixture(autouse=True)
def setup_db():
    """Create tables for each test, clean up after."""
    Base.metadata.create_all(bind=engine)
    yield
    # Cleanup using Session
    db = SessionLocal()
    try:
        db.execute(ReferenceProcess.__table__.delete())
        db.execute(DebugDiagnostic.__table__.delete())
        db.commit()
    finally:
        db.close()
    engine.dispose()


@pytest.fixture()
def client():
    return TestClient(app)


def _make_spold_xml(activity_id: str, rp_id: str, activity_name: str,
                    location: str, ref_product_name: str,
                    exchanges: list[dict] | None = None) -> str:
    """Build a minimal ecoSpold02 LCI XML string."""
    exch_xml = ""
    for ex in (exchanges or []):
        exch_xml += f'''
            <es:elementaryExchange id="{ex['exchange_id']}" outputGroup="output">
                <es:name>{ex['name']}</es:name>
                <es:unitName>{ex['unit']}</es:unitName>
                <es:compartment>{ex.get('compartment', '')}</es:compartment>
            </es:elementaryExchange>'''

    return f'''<?xml version="1.0" encoding="UTF-8"?>
<es:generalData es:ecoSpoldVersion="0.2"
    xmlns:es="http://www.ecoinvent.org/1.0/spold">
  <es:activity id="{activity_id}" location="{location}">
    <es:activityName>{activity_name}</es:activityName>
    <es:intermediateExchange id="{rp_id}" variableName="RP" outputGroup="0" amount="1">
      <es:name>{ref_product_name}</es:name>
      <es:unitName>kg</es:unitName>
    </es:intermediateExchange>
    {exch_xml}
  </es:activity>
</es:generalData>'''


def _make_elementary_exchanges_xml(flows: list[dict]) -> str:
    """Build MasterData/ElementaryExchanges.xml."""
    flow_xml = ""
    for f in flows:
        flow_xml += f'''
        <ElementaryFlow>
          <uuid>{f['uuid']}</uuid>
          <shortNameEN>{f['name']}</shortNameEN>
          <nameEN>Flow for {f['name']}</nameEN>
          <flowType>Elementary flow</flowType>
          <unit>{f['unit']}</unit>
          <unitGroup>default</unitGroup>
          <compartment>{f.get('compartment', '')}</compartment>
          <CAS>{f.get('CAS', '')}</CAS>
          <formula>{f.get('formula', '')}</formula>
          <isReferenceFlow>{f.get('isReference', 'false')}</isReferenceFlow>
        </ElementaryFlow>'''
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<ElementaryExchanges xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  {flow_xml}
</ElementaryExchanges>'''


def _make_units_xml(units: list[str]) -> str:
    """Build MasterData/Units.xml."""
    unit_xml = "".join(
        f'    <Unit><name>{u}</name></Unit>' for u in units
    )
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<Units>
  {unit_xml}
</Units>'''


def _make_masterdata_xmls() -> tuple[str, str, str]:
    return (
        _make_elementary_exchanges_xml([
            {"uuid": "flow-co2-001", "name": "CO2", "unit": "kg",
             "compartment": "air", "formula": "CO2", "isReference": "true"},
            {"uuid": "flow-ch4-001", "name": "Methane", "unit": "kg",
             "compartment": "air", "formula": "CH4"},
        ]),
        _make_units_xml(["kg", "g"]),
        "",  # foundation placeholder not needed for LCI preview
    )


def _make_fake_7z(tmp_path: Path, spold_files: list[dict],
                  include_masterdata: bool = True) -> Path:
    """Create a minimal .7z archive with given spold + masterdata files."""
    archive_path = tmp_path / "fake_lci.7z"

    masterdata_xmls = []
    if include_masterdata:
        elem_xml, units_xml, _ = _make_masterdata_xmls()
        masterdata_xmls = [
            ("MasterData/ElementaryExchanges.xml", elem_xml),
            ("MasterData/Units.xml", units_xml),
        ]

    with py7zr.SevenZipFile(
        str(archive_path), mode='w'
    ) as archive:
        # MasterData
        for name, content in masterdata_xmls:
            archive.writestr(content, f"ecoinventLCI311/{name}")

        # FilenameToActivityLookup.csv
        csv_content = "activityId,datasetId,filename\n"
        for sf in spold_files:
            csv_content += f"{sf['activity_id']},{uuid.uuid4()},{sf['filename']}\n"
        archive.writestr(
            csv_content, "ecoinventLCI311/FilenameToActivityLookup.csv"
        )

        # SPOLD files
        for sf in spold_files:
            xml = _make_spold_xml(
                activity_id=sf["activity_id"],
                rp_id=sf["rp_id"],
                activity_name=sf["activity_name"],
                location=sf["location"],
                ref_product_name=sf["ref_product_name"],
                exchanges=sf.get("exchanges", []),
            )
            archive.writestr(
                xml, f"ecoinventLCI311/datasets/{sf['filename']}"
            )

    return archive_path


def _preview_and_get_job(client, tmp_path, spold_files: list[dict]) -> str:
    """Run preview and return job_id."""
    archive = _make_fake_7z(tmp_path, spold_files)
    with open(archive, "rb") as f:
        resp = client.post(
            "/import/ef31/preview",
            files={"lci_archive": ("lci.7z", f, "application/x-7z-compressed")},
            params={"limit": 10},
        )
    assert resp.status_code == 200, resp.json()
    return resp.json()["job_id"]


class TestEf31PreviewEndpoint:
    """Test POST /import/ef31/preview."""

    def test_preview_success(self, client, tmp_path):
        """Successful preview returns job_id, can_commit, counts."""
        spold_files = [
            {
                "activity_id": "act-001",
                "rp_id": "rp-001",
                "activity_name": "Electricity, medium voltage",
                "location": "CH",
                "ref_product_name": "market for electricity, medium voltage",
                "filename": "electricity_ch.spold",
                "exchanges": [
                    {"exchange_id": "flow-co2-001", "name": "CO2",
                     "unit": "kg", "compartment": "air"},
                ],
            },
        ]
        archive = _make_fake_7z(tmp_path, spold_files)

        with open(archive, "rb") as f:
            resp = client.post(
                "/import/ef31/preview",
                files={"lci_archive": ("lci.7z", f, "application/x-7z-compressed")},
                params={"limit": 10},
            )

        assert resp.status_code == 200, resp.json()
        data = resp.json()
        assert "job_id" in data
        assert data["can_commit"] is True
        assert data["counts"]["datasets"] == 1
        assert data["counts"]["exchanges"] == 1

    def test_preview_with_missing_refs(self, client, tmp_path):
        """Preview with missing MasterData refs should set can_commit=false."""
        spold_files = [
            {
                "activity_id": "act-bad",
                "rp_id": "rp-bad",
                "activity_name": "Bad Activity",
                "location": "DE",
                "ref_product_name": "market for bad",
                "filename": "bad.spold",
                "exchanges": [
                    {"exchange_id": "unknown-flow-999", "name": "Unknown",
                     "unit": "kg", "compartment": "air"},
                ],
            },
        ]
        archive = _make_fake_7z(tmp_path, spold_files)

        with open(archive, "rb") as f:
            resp = client.post(
                "/import/ef31/preview",
                files={"lci_archive": ("lci.7z", f, "application/x-7z-compressed")},
                params={"limit": 10},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["can_commit"] is False
        assert len(data["errors"]) > 0
        assert any("unknown-flow-999" in e for e in data["errors"])


class TestEf31CommitEndpoint:
    """Test POST /import/ef31/commit."""

    def test_commit_success(self, client, tmp_path):
        """Successful commit creates ReferenceProcess."""
        spold_files = [
            {
                "activity_id": "act-001",
                "rp_id": "rp-001",
                "activity_name": "Test Activity",
                "location": "CH",
                "ref_product_name": "market for test",
                "filename": "test.spold",
                "exchanges": [
                    {"exchange_id": "flow-co2-001", "name": "CO2",
                     "unit": "kg", "compartment": "air"},
                ],
            },
        ]
        job_id = _preview_and_get_job(client, tmp_path, spold_files)

        resp = client.post(
            "/import/ef31/commit",
            json={"job_id": job_id, "confirm": True},
        )
        assert resp.status_code == 200, resp.json()
        data = resp.json()
        assert data["committed"] is True
        assert data["catalog_target_kind"] == "lci_dataset"

        # Verify ReferenceProcess in DB
        db = SessionLocal()
        try:
            procs = db.execute(ReferenceProcess.__table__.select()).all()
            assert len(procs) >= 1
            proc = procs[0]
            assert proc.process_type == "lci_dataset"
            assert proc.import_mode == "ecoinvent_ef31_lci"
            assert proc.reference_flow_uuid == "rp-001"
            assert proc.process_json["exchange_count"] == 1
            assert len(proc.process_json["exchanges"]) == 1
        finally:
            db.close()

    def test_commit_without_confirm_rejected(self, client):
        """Commit with confirm=false should return 422 (validation error)."""
        resp = client.post(
            "/import/ef31/commit",
            json={"job_id": "fake-job", "confirm": False},
        )
        # Pydantic validation rejects confirm=Literal[True] with False
        assert resp.status_code == 422


class TestEf31ReportEndpoint:
    """Test GET /import/ef31/reports/{job_id}."""

    def test_report_after_preview(self, client, tmp_path):
        """Report should be available after preview."""
        spold_files = [
            {
                "activity_id": "act-001",
                "rp_id": "rp-001",
                "activity_name": "Test",
                "location": "CH",
                "ref_product_name": "market for test",
                "filename": "test.spold",
                "exchanges": [
                    {"exchange_id": "flow-co2-001", "name": "CO2",
                     "unit": "kg", "compartment": "air"},
                ],
            },
        ]
        job_id = _preview_and_get_job(client, tmp_path, spold_files)

        resp = client.get(f"/import/ef31/reports/{job_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == job_id
        assert data["counts"]["datasets"] == 1

    def test_report_not_found(self, client):
        """Report for non-existent job returns 404."""
        resp = client.get("/import/ef31/reports/nonexistent-job-id")
        assert resp.status_code == 404
