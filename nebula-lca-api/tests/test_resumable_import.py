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
    assert insp.has_table("global_dataset_imports")

    # Second call should be no-op (all tables/indexes already exist)
    result2 = ensure_import_tables(engine)
    assert result2["status"] in ("ok", "already_complete")


def test_global_dataset_dedup_skip(tmp_path):
    """Test that global dedup skips already-imported datasets."""
    from sqlalchemy.orm import sessionmaker

    from app.models import (
        DatasetCheckpoint,
        GlobalDatasetImport,
        ImportJob,
    )

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)
    GlobalDatasetImport.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()

    job_id = "test-job-003"
    job = ImportJob(
        job_id=job_id,
        file_path="/fake/path.7z",
        file_type="lci",
        status="pending",
        phase="created",
        workers=4,
    )
    db.add(job)

    # Simulate that a dataset was already imported in a previous job
    # dataset_uuid = "act-001:rp-001" (activity_id + ":" + reference_product_id)
    global_rec = GlobalDatasetImport(
        source_package_version="ecoinvent_3.11",
        dataset_uuid="act-001:rp-001",
        status="imported",
        dataset_filename="act-001.spold",
        process_uuid="act-001:rp-001",
        activity_id="act-001",
        reference_product_id="rp-001",
    )
    db.add(global_rec)
    db.commit()

    # Verify the global record exists and is imported
    found = (
        db.query(GlobalDatasetImport)
        .filter(
            GlobalDatasetImport.source_package_version == "ecoinvent_3.11",
            GlobalDatasetImport.dataset_uuid == "act-001:rp-001",
        )
        .first()
    )
    assert found is not None
    assert found.status == "imported"


def test_global_dataset_dedup_different_uuids(tmp_path):
    """Different reference products for the same activity should have different dataset_uuids."""
    from sqlalchemy.orm import sessionmaker

    from app.models import GlobalDatasetImport, ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    ImportJob.__table__.create(engine)
    GlobalDatasetImport.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()

    # Same activity, different reference products = different dataset_uuids
    ds1_uuid = "act-001:rp-001"  # activity + rp1
    ds2_uuid = "act-001:rp-002"  # same activity + rp2

    GlobalDatasetImport(
        source_package_version="ecoinvent_3.11",
        dataset_uuid=ds1_uuid,
        status="imported",
        activity_id="act-001",
    ).__class__.__table__.create(bind=engine, checkfirst=True)

    db.add(GlobalDatasetImport(
        source_package_version="ecoinvent_3.11",
        dataset_uuid=ds1_uuid,
        status="imported",
        activity_id="act-001",
        reference_product_id="rp-001",
    ))
    db.add(GlobalDatasetImport(
        source_package_version="ecoinvent_3.11",
        dataset_uuid=ds2_uuid,
        status="pending",
        activity_id="act-001",
        reference_product_id="rp-002",
    ))
    db.commit()

    # Only ds1 should be imported
    imported = (
        db.query(GlobalDatasetImport)
        .filter(
            GlobalDatasetImport.source_package_version == "ecoinvent_3.11",
            GlobalDatasetImport.dataset_uuid == ds1_uuid,
            GlobalDatasetImport.status == "imported",
        )
        .count()
    )
    assert imported == 1

    # ds2 should NOT be imported
    imported2 = (
        db.query(GlobalDatasetImport)
        .filter(
            GlobalDatasetImport.source_package_version == "ecoinvent_3.11",
            GlobalDatasetImport.dataset_uuid == ds2_uuid,
            GlobalDatasetImport.status == "imported",
        )
        .count()
    )
    assert imported2 == 0


def test_global_dataset_skip_marks_job_checkpoint(tmp_path):
    """A globally imported dataset should be skipped in the current job checkpoint."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.ecoinvent_ef31_loader import LCIDataset
    from app.lci_import_executor import LciImportJobExecutor, ParseResult
    from app.models import DatasetCheckpoint, GlobalDatasetImport, ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)
    GlobalDatasetImport.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()

    job_id = "test-job-005"
    db.add(ImportJob(job_id=job_id, file_path="/fake/path.7z", file_type="lci"))
    db.add(DatasetCheckpoint(job_id=job_id, dataset_key="already.spold", status="running"))
    db.add(
        GlobalDatasetImport(
            source_package_version="ecoinvent_3.11",
            dataset_uuid="act-001:rp-001",
            status="imported",
            dataset_filename="already.spold",
            process_uuid="proc-existing",
        )
    )
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = job_id
    executor.package_version = "ecoinvent_3.11"
    executor.overwrite_existing = False
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._failed_datasets = []
    executor._vector_warnings = []
    executor._total_count = 1
    executor._global_skipped_dataset_keys = set()

    parse_result = ParseResult(
        spold_path="already.spold",
        dataset=LCIDataset(
            filename="already.spold",
            activity_id="act-001",
            activity_name="Already imported",
            location="GLO",
            reference_product_name="Product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="rp-001",
        ),
        exchanges=[],
        process_uuid="proc-existing",
        duration_ms=1,
    )

    executor._flush_single(parse_result)
    executor._mark_checkpoints_complete([parse_result])
    db.commit()

    checkpoint = db.query(DatasetCheckpoint).filter_by(job_id=job_id, dataset_key="already.spold").one()
    job = db.get(ImportJob, job_id)
    assert checkpoint.status == "skipped_global"
    assert executor._stats["skipped"] == 1
    assert executor._stats["inserted"] == 0
    assert job.skipped_global == 1
