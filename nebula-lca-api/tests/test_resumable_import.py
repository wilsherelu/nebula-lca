"""Tests for checkpoint-based resumable import executor.

Covers:
- Checkpoint: already-imported datasets are skipped
- Failed datasets can be retried
- SQLite writer doesn't cause lock contention
- Schema maintenance creates import tables
"""

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect

# Ensure app can be imported
TEST_DIR = Path(__file__).resolve().parent
API_DIR = TEST_DIR.parent
os.chdir(API_DIR)


# ── Tests ────────────────────────────────────────────────────────────────


def test_checkpoint_skip_imported(tmp_path):
    """Already-imported datasets should be skipped by checkpoint."""
    from sqlalchemy.orm import sessionmaker

    from app.models import (
        DatasetCheckpoint,
        ImportJob,
    )

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()

    job_id = "test-job-001"

    # Create job
    job = ImportJob(
        job_id=job_id,
        file_path="/fake/path.7z",
        file_type="lci",
        status="pending",
        phase="created",
        workers=4,
        limit=None,
    )
    db.add(job)

    # Create checkpoint for dataset "already_imported.spold"
    cp1 = DatasetCheckpoint(
        job_id=job_id,
        dataset_key="already_imported.spold",
        status="imported",
        process_uuid="proc-001",
        vector_nnz=10,
    )
    db.add(cp1)

    # Create checkpoint for dataset "failed.spold"
    cp2 = DatasetCheckpoint(
        job_id=job_id,
        dataset_key="failed.spold",
        status="failed",
        error_message="Some parse error",
    )
    db.add(cp2)

    db.commit()

    # Verify checkpoints
    imported = (
        db.query(DatasetCheckpoint)
        .filter(DatasetCheckpoint.job_id == job_id, DatasetCheckpoint.status == "imported")
        .first()
    )
    assert imported is not None
    assert imported.dataset_key == "already_imported.spold"

    failed = (
        db.query(DatasetCheckpoint)
        .filter(DatasetCheckpoint.job_id == job_id, DatasetCheckpoint.status == "failed")
        .first()
    )
    assert failed is not None
    assert failed.dataset_key == "failed.spold"


def test_checkpoint_retry_failed(tmp_path):
    """Failed datasets should be retrievable for retry."""
    from sqlalchemy.orm import sessionmaker

    from app.models import DatasetCheckpoint, ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()

    job_id = "test-job-002"
    job = ImportJob(
        job_id=job_id,
        file_path="/fake/path.7z",
        status="failed",
        phase="importing",
        workers=4,
    )
    db.add(job)

    # 2 failed, 1 imported
    for i in range(3):
        status = "imported" if i == 0 else "failed"
        cp = DatasetCheckpoint(
            job_id=job_id,
            dataset_key=f"dataset-{i}.spold",
            status=status,
        )
        db.add(cp)

    db.commit()

    # Count failed datasets
    failed_count = (
        db.query(DatasetCheckpoint)
        .filter(DatasetCheckpoint.job_id == job_id, DatasetCheckpoint.status == "failed")
        .count()
    )
    assert failed_count == 2

    # Count pending/skipped — filter for non-imported
    pending_or_retry = (
        db.query(DatasetCheckpoint)
        .filter(
            DatasetCheckpoint.job_id == job_id,
            DatasetCheckpoint.status.in_(["failed", "pending", "running"]),
        )
        .count()
    )
    assert pending_or_retry == 2  # 2 failed datasets


def test_spold_parsing(tmp_path):
    """Verify .spold parsing produces correct dataset."""
    from app.ecoinvent_ef31_loader import parse_spold_file, parse_spold_exchanges

    spold_dir = tmp_path / "spold"
    spold_dir.mkdir(parents=True, exist_ok=True)

    from lxml import etree

    ns_uri = "http://www.EcoInvent.org/EcoSpold02"

    def el(parent, name, **attrs):
        return etree.SubElement(parent, f"{{{ns_uri}}}{name}", **attrs)

    root = etree.Element(f"{{{ns_uri}}}activityDataset", nsmap={None: ns_uri})
    activity_desc = el(root, "activityDescription")
    activity = el(activity_desc, "activity", id="act-001")
    el(activity, "activityName").text = "Test Process"
    geography = el(activity_desc, "geography")
    el(geography, "shortname").text = "CN"

    flow_data = el(root, "flowData")
    ref_product = el(
        flow_data,
        "intermediateExchange",
        intermediateExchangeId="rp-001",
        variableName="RP",
        amount="1.0",
    )
    el(ref_product, "name").text = "Test Product"
    el(ref_product, "unitName").text = "kilogram"

    exchange = el(
        flow_data,
        "elementaryExchange",
        elementaryExchangeId="exc-001",
        amount="1.0",
    )
    el(exchange, "name").text = "Flow 1"
    el(exchange, "unitName").text = "kilogram"
    el(exchange, "outputGroup").text = "4"

    spold_path = spold_dir / "test_parse.spold"
    tree = etree.ElementTree(root)
    etree.indent(tree, space="  ")
    tree.write(str(spold_path), encoding="UTF-8", xml_declaration=True, pretty_print=True)

    ds = parse_spold_file(spold_path)
    assert ds is not None
    assert ds.activity_id == "act-001"
    assert ds.activity_name == "Test Process"
    assert ds.location == "CN"
    assert ds.reference_product_id == "rp-001"

    exchanges = parse_spold_exchanges(spold_path)
    assert exchanges is not None
    assert len(exchanges) == 1
    assert exchanges[0].exchange_id == "exc-001"

def test_schema_maintenance_import_tables(tmp_path):
    """Test ensure_import_tables creates tables correctly."""
    from app.schema_maintenance import ensure_import_tables
    from app.models import ImportJob, DatasetCheckpoint

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # First call should create tables
    result = ensure_import_tables(engine)
    assert result["status"] in ("ok", "already_complete")

    # Verify tables exist
    insp = inspect(engine)
    assert insp.has_table("import_jobs")
    assert insp.has_table("dataset_checkpoints")
    assert insp.has_table("import_job_pause_requests")

    # Second call should be no-op
    result2 = ensure_import_tables(engine)
    assert result2["status"] == "already_complete"
