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
            vector_nnz=10,
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
    assert checkpoint.vector_status == "reused"
    assert checkpoint.vector_nnz == 10
    assert executor._stats["datasets_processed"] == 1
    assert executor._stats["datasets_skipped_global"] == 1
    assert executor._stats["vectors_reused"] == 1
    assert executor._stats["processes_inserted"] == 0


def test_parse_write_pipeline_flushes_full_batch_and_remainder(tmp_path):
    """Pipeline should write every parse result with one writer and bounded commits."""
    import threading
    from collections import defaultdict
    from pathlib import Path

    from app.lci_import_executor import LciImportJobExecutor, ParseResult

    class DummyDb:
        def __init__(self):
            self.commit_count = 0

        def commit(self):
            self.commit_count += 1

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = DummyDb()
    executor.job_id = "pipeline-test"
    executor.workers = 2
    executor._paused = False
    executor._cancel_requested = False
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._global_skipped_dataset_keys = set()
    executor._write_batch_size = 32
    executor._queue_maxsize = 8
    executor._write_flush_interval_seconds = 30.0
    executor._perf_stats = {
        "parse_wall_seconds": 0.0,
        "write_wall_seconds": 0.0,
        "commit_count": 0,
        "write_batch_size": executor._write_batch_size,
        "queue_max_observed": 0,
        "parse_duration_ms_total": 0,
        "parse_result_count": 0,
        "flush_duration_ms_total": 0.0,
        "flush_result_count": 0,
    }
    flushed: list[str] = []
    checkpoint_batches: list[int] = []

    def parse_one(path: Path) -> ParseResult:
        return ParseResult(
            spold_path=str(path),
            dataset=None,
            exchanges=[],
            process_uuid=path.stem,
            duration_ms=5,
            error="parse_spold_file returned None",
        )

    def flush_single(result: ParseResult) -> None:
        flushed.append(Path(result.spold_path).name)

    def mark_complete(results: list[ParseResult]) -> None:
        checkpoint_batches.append(len(results))

    executor._parse_one = parse_one
    executor._flush_single = flush_single
    executor._mark_checkpoints_complete = mark_complete
    executor._update_progress = lambda: None
    executor._check_control_signals = lambda: (False, False)

    files = [tmp_path / f"dataset-{idx}.spold" for idx in range(33)]
    LciImportJobExecutor._run_parse_write_pipeline(executor, files)

    assert sorted(flushed) == sorted(path.name for path in files)
    assert sorted(checkpoint_batches) == [1, 32]
    assert executor.db.commit_count == 2
    assert executor._perf_stats["commit_count"] == 2
    assert executor._build_performance_stats()["avg_parse_ms"] == 5.0


def test_parse_one_fast_skips_global_import_without_exchange_parse(tmp_path, monkeypatch):
    """Imported datasets should skip exchange parsing when global cache has a hit."""
    import threading
    from collections import defaultdict

    from app.ecoinvent_ef31_loader import LCIDataset
    import app.lci_import_executor as executor_module
    from app.lci_import_executor import LciImportJobExecutor

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.overwrite_existing = False
    executor._paused = False
    executor._cancel_requested = False
    executor._pause_event = threading.Event()
    executor._unit_conversion_cache = {"kg": (1.0, "kg")}
    executor._flow_metadata_cache = {"flow-001": ("air", "urban air")}
    executor._elem_flow_lookup = {}
    executor._perf_stats = {}
    executor._global_import_cache = {
        "act-001:rp-001": {
            "status": "imported",
            "process_uuid": "proc-existing",
            "vector_nnz": 10,
        }
    }

    monkeypatch.setattr(
        executor_module._loader,
        "parse_spold_file",
        lambda path: LCIDataset(
            filename=path.name,
            activity_id="act-001",
            activity_name="Already imported",
            location="GLO",
            reference_product_name="Product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="rp-001",
        ),
    )

    def fail_exchange_parse(path):
        raise AssertionError("exchange parser should not run for global fast skip")

    monkeypatch.setattr(executor_module._loader, "parse_spold_exchanges", fail_exchange_parse)

    result = LciImportJobExecutor._parse_one(executor, tmp_path / "already.spold")

    assert result.global_skip is True
    assert result.parse_stage == "metadata"
    assert result.write_plan is None
    assert result.process_uuid == "proc-existing"
    assert result.vector_status == "reused"
    assert result.vector_nnz == 10


def test_parse_one_filename_fast_skip_avoids_xml_parsers(tmp_path, monkeypatch):
    """UUID-based ecoinvent filenames should fast skip before XML parsing."""
    import threading

    import app.lci_import_executor as executor_module
    from app.lci_import_executor import LciImportJobExecutor

    activity_id = "00082bd6-67b0-509f-a229-7428fb2418ca"
    product_id = "ad5a20dd-4c4d-499d-8dc1-254ebab8f3bf"
    dataset_uuid = f"{activity_id}:{product_id}"
    spold_path = tmp_path / f"{activity_id}_{product_id}.spold"

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.overwrite_existing = False
    executor._paused = False
    executor._cancel_requested = False
    executor._pause_event = threading.Event()
    executor._global_import_cache = {
        dataset_uuid: {
            "status": "imported",
            "process_uuid": "proc-existing",
            "vector_nnz": 10,
        }
    }
    executor._perf_stats = {
        "filename_fast_skip_count": 0,
        "filename_metadata_hit_count": 0,
        "filename_metadata_miss_count": 0,
    }
    executor._lock = threading.Lock()

    monkeypatch.setattr(
        executor_module._loader,
        "parse_spold_metadata_early",
        lambda path: (_ for _ in ()).throw(AssertionError("metadata parser should not run")),
    )
    monkeypatch.setattr(
        executor_module._loader,
        "parse_spold_streaming_agg",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("streaming parser should not run")),
    )

    result = LciImportJobExecutor._parse_one(executor, spold_path)

    assert result.global_skip is True
    assert result.parse_stage == "filename"
    assert result.dataset_uuid == dataset_uuid
    assert result.process_uuid == "proc-existing"
    assert result.vector_status == "reused"
    assert result.vector_nnz == 10
    assert executor._perf_stats["filename_metadata_hit_count"] == 1
    assert executor._perf_stats["filename_fast_skip_count"] == 1


def test_parse_one_overwrite_bypasses_global_fast_skip(tmp_path, monkeypatch):
    """Overwrite imports must parse exchanges even if global cache has a hit."""
    import threading

    from app.ecoinvent_ef31_loader import LCIDataset, LCIElementaryExchange
    import app.lci_import_executor as executor_module
    from app.lci_import_executor import LciImportJobExecutor

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.overwrite_existing = True
    executor._paused = False
    executor._cancel_requested = False
    executor._pause_event = threading.Event()
    executor._unit_conversion_cache = {"kg": (1.0, "kg")}
    executor._flow_metadata_cache = {"flow-001": ("air", "urban air")}
    executor._elem_flow_lookup = {}
    executor._perf_stats = {}
    executor._global_import_cache = {
        "act-001:rp-001": {
            "status": "imported",
            "process_uuid": "proc-existing",
            "vector_nnz": 10,
        }
    }

    monkeypatch.setattr(
        executor_module._loader,
        "parse_spold_file",
        lambda path: LCIDataset(
            filename=path.name,
            activity_id="act-001",
            activity_name="Overwrite",
            location="GLO",
            reference_product_name="Product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="rp-001",
        ),
    )
    monkeypatch.setattr(
        executor_module._loader,
        "parse_spold_exchanges",
        lambda path: [
            LCIElementaryExchange(
                dataset_filename=path.name,
                exchange_id="flow-001",
                exchange_name="Flow",
                unit="kg",
                direction="output",
                amount=1.0,
            )
        ],
    )

    result = LciImportJobExecutor._parse_one(executor, tmp_path / "overwrite.spold")

    assert result.global_skip is False
    assert result.parse_stage == "aggregated"
    assert result.write_plan is not None
    assert result.write_plan.has_exchanges is True
    assert result.write_plan.flow_key_aggs
    assert result.write_plan.canonicalized is True


def test_streaming_write_plan_matches_single_pass_fixture():
    """Streaming aggregation should preserve the existing single-pass vector keys."""
    import threading

    from app.ecoinvent_ef31_loader import parse_spold_dataset_and_exchanges
    from app.ef31_db_service import _generate_lci_process_uuid
    from app.lci_import_executor import LciImportJobExecutor

    fixture = Path("tests/fixtures/ef31_lci/electricity_medium_voltage_ch.spold")
    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.package_version = "ecoinvent_3.11"
    executor._lock = threading.Lock()
    executor._unit_conversion_cache = {"kilogram": (1.0, "kg"), "joule": (1.0, "J")}
    executor._elem_flow_lookup = {}
    executor._flow_metadata_cache = {}
    executor._perf_stats = {
        "stream_parse_wall_seconds": 0.0,
        "exchange_object_count": 0,
        "aggregated_result_count": 0,
        "worker_aggregate_wall_seconds": 0.0,
    }

    streamed = LciImportJobExecutor._parse_streaming_write_plan(executor, fixture)
    assert streamed is not None
    _, _, _, streaming_plan = streamed

    single = parse_spold_dataset_and_exchanges(fixture, include_exchanges=True)
    assert single is not None
    procs = _generate_lci_process_uuid(single.dataset)
    legacy_plan = LciImportJobExecutor._build_write_plan(
        executor,
        spold_path=fixture,
        ds=single.dataset,
        exchanges=single.exchanges,
        procs=procs,
        dataset_uuid=f"{single.dataset.activity_id}:{single.dataset.reference_product_id}",
    )

    assert streaming_plan.flow_key_aggs == legacy_plan.flow_key_aggs
    assert executor._perf_stats["streaming_parser_enabled"] is True


def test_sqlite_core_upsert_path_is_active(tmp_path):
    """SQLite core upsert should really run instead of silently falling back to ORM merge."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.lci_import_executor import LciImportJobExecutor
    from app.models import GlobalDatasetImport, ReferenceProcess

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ReferenceProcess.__table__.create(engine)
    GlobalDatasetImport.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.package_version = "ecoinvent_3.11"
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._global_import_cache = {}
    executor._perf_stats = {"core_upsert_enabled": False}

    assert LciImportJobExecutor._is_sqlite(executor) is True

    LciImportJobExecutor._core_upsert_reference_processes(executor, [{
        "process_uuid": "proc-001",
        "process_name": "Process 1",
        "process_name_en": "Process 1",
        "process_type": "lci_dataset",
        "reference_flow_uuid": None,
        "process_json": {"v": 1},
        "source_file": "one.spold",
        "source_process_uuid": None,
        "import_mode": "ecoinvent_ef31_lci",
        "import_report_json": {"package_version": "ecoinvent_3.11"},
    }])
    LciImportJobExecutor._core_upsert_global_dataset_imports(executor, [{
        "source_package_version": "ecoinvent_3.11",
        "dataset_uuid": "dataset-001",
        "dataset_filename": "one.spold",
        "process_uuid": "proc-001",
        "activity_id": "act-001",
        "reference_product_id": "rp-001",
        "status": "imported",
        "vector_nnz": 3,
        "last_job_id": "job-001",
        "error_message": None,
    }])
    db.commit()

    assert executor._perf_stats["core_upsert_enabled"] is True
    assert db.query(ReferenceProcess).count() == 1
    assert db.query(GlobalDatasetImport).count() == 1
    assert db.query(GlobalDatasetImport).first().last_job_id == "job-001"

    LciImportJobExecutor._core_upsert_reference_processes(executor, [{
        "process_uuid": "proc-001",
        "process_name": "Process 1 updated",
        "process_name_en": "Process 1 updated",
        "process_type": "lci_dataset",
        "reference_flow_uuid": None,
        "process_json": {"v": 2},
        "source_file": "one.spold",
        "source_process_uuid": None,
        "import_mode": "ecoinvent_ef31_lci",
        "import_report_json": {"package_version": "ecoinvent_3.11"},
    }])
    db.commit()

    row = db.get(ReferenceProcess, "proc-001")
    assert row.process_name == "Process 1 updated"
    assert row.process_json == {"v": 2}
    assert db.query(ReferenceProcess).count() == 1


def test_sqlite_core_checkpoint_upsert_updates_without_duplicates(tmp_path):
    """Checkpoint core upsert should update by job_id + dataset_key."""
    import threading

    from sqlalchemy.orm import sessionmaker

    from app.lci_import_executor import LciImportJobExecutor
    from app.models import DatasetCheckpoint

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    DatasetCheckpoint.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = "job-001"
    executor._lock = threading.Lock()
    executor._perf_stats = {
        "checkpoint_core_upsert_enabled": False,
        "checkpoint_upsert_execute_seconds": 0.0,
    }

    row = {
        "job_id": "job-001",
        "dataset_key": "one.spold",
        "status": "running",
        "process_uuid": "proc-001",
        "vector_status": None,
        "vector_nnz": None,
        "duration_ms": 1,
        "error_message": None,
    }
    LciImportJobExecutor._core_upsert_dataset_checkpoints(executor, [row])
    db.commit()

    updated = {
        **row,
        "status": "imported",
        "vector_status": "written",
        "vector_nnz": 3,
        "duration_ms": 2,
    }
    LciImportJobExecutor._core_upsert_dataset_checkpoints(executor, [updated])
    db.commit()

    checkpoints = db.query(DatasetCheckpoint).all()
    assert len(checkpoints) == 1
    assert checkpoints[0].status == "imported"
    assert checkpoints[0].vector_status == "written"
    assert checkpoints[0].vector_nnz == 3
    assert checkpoints[0].duration_ms == 2
    assert executor._perf_stats["checkpoint_core_upsert_enabled"] is True


def test_debug_writer_replay_mode_caches_plans_without_db_writes():
    """writer_replay mode should cache write plans and skip normal DB work."""
    from collections import defaultdict

    from app.ecoinvent_ef31_loader import LCIDataset
    from app.lci_import_executor import LciImportJobExecutor, LciWritePlan, ParseResult

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.debug_parser_blackhole = False
    executor.debug_writer_replay = True
    executor._replay_write_plans = []
    executor._perf_stats = {"debug_writer_replay": False, "blackhole_drain_count": 0}
    executor._stats = defaultdict(int)

    write_plan = LciWritePlan(
        spold_path="one.spold",
        process_uuid="proc-001",
        dataset_uuid="dataset-001",
        process_json={"process_uuid": "proc-001", "process_name": "Process"},
        flow_key_aggs={("flow-001", "air", "", "output", "kg"): 1.0},
        has_exchanges=True,
    )
    result = ParseResult(
        spold_path="one.spold",
        dataset=LCIDataset(
            filename="one.spold",
            activity_id="act-001",
            activity_name="Process",
            location="GLO",
            reference_product_name="Product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="rp-001",
        ),
        exchanges=[],
        process_uuid="proc-001",
        duration_ms=1,
        dataset_uuid="dataset-001",
        write_plan=write_plan,
    )

    LciImportJobExecutor._flush_result_batch_bulk(executor, [result])

    assert executor._perf_stats["debug_writer_replay"] is True
    assert executor._perf_stats["blackhole_drain_count"] == 1
    assert executor._replay_write_plans == [write_plan]
    assert executor._stats["datasets_processed"] == 0


def test_driver_timing_unregisters_listener(tmp_path):
    """Driver-level SQL timing should not leave listeners attached after cleanup."""
    import threading

    from sqlalchemy import text
    from sqlalchemy.orm import sessionmaker

    from app.lci_import_executor import LciImportJobExecutor
    from app.models import ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor._lock = threading.Lock()
    executor._perf_stats = {"db_fetch_seconds": 0.0, "db_execute_seconds": 0.0}
    executor._driver_timing_listeners = None

    LciImportJobExecutor._register_driver_timing(executor)
    db.execute(text("SELECT 1")).all()
    before_unregister = executor._perf_stats["db_fetch_seconds"]
    assert before_unregister > 0

    LciImportJobExecutor._unregister_driver_timing(executor)
    db.execute(text("SELECT 1")).all()
    assert executor._perf_stats["db_fetch_seconds"] == before_unregister


def test_global_fast_skip_flush_marks_checkpoint_and_stats(tmp_path):
    """Fast skipped parse results should update stats and checkpoint without DB lookup."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.ecoinvent_ef31_loader import LCIDataset
    from app.lci_import_executor import LciImportJobExecutor, ParseResult
    from app.models import DatasetCheckpoint, ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    job_id = "fast-skip-job"
    db.add(ImportJob(job_id=job_id, file_path="/fake/path.7z", file_type="lci"))
    db.add(DatasetCheckpoint(job_id=job_id, dataset_key="already.spold", status="running"))
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = job_id
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._global_skipped_dataset_keys = set()
    executor._failed_datasets = []

    result = ParseResult(
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
        dataset_uuid="act-001:rp-001",
        global_skip=True,
        reused_process_uuid="proc-existing",
        reused_vector_nnz=10,
        parse_stage="metadata",
        vector_status="reused",
        vector_nnz=10,
    )

    executor._flush_single(result)
    executor._mark_checkpoints_complete([result])
    db.commit()

    checkpoint = db.query(DatasetCheckpoint).filter_by(job_id=job_id, dataset_key="already.spold").one()
    assert checkpoint.status == "skipped_global"
    assert checkpoint.process_uuid == "proc-existing"
    assert checkpoint.vector_status == "reused"
    assert checkpoint.vector_nnz == 10
    assert executor._stats["datasets_processed"] == 1
    assert executor._stats["datasets_skipped_global"] == 1
    assert executor._stats["vectors_reused"] == 1


def test_bulk_writer_records_global_fast_skip(tmp_path):
    """Bulk writer should count parser-level global skips and checkpoint them."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.ecoinvent_ef31_loader import LCIDataset
    from app.lci_import_executor import LciImportJobExecutor, ParseResult
    from app.models import DatasetCheckpoint, ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    job_id = "bulk-fast-skip-job"
    db.add(ImportJob(job_id=job_id, file_path="/fake/path.7z", file_type="lci", status="running"))
    db.add(DatasetCheckpoint(job_id=job_id, dataset_key="already.spold", status="running"))
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = job_id
    executor.package_version = "ecoinvent_3.11"
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._failed_datasets = []
    executor._vector_warnings = []
    executor._global_skipped_dataset_keys = set()
    executor._perf_stats = {
        "commit_count": 0,
        "flush_duration_ms_total": 0.0,
        "flush_result_count": 0,
        "batch_result_count": 0,
        "batch_prefetch_wall_seconds": 0.0,
        "batch_prefetch_global_wall_seconds": 0.0,
        "batch_prefetch_checkpoint_wall_seconds": 0.0,
        "batch_pack_wall_seconds": 0.0,
        "batch_db_upsert_wall_seconds": 0.0,
        "batch_checkpoint_wall_seconds": 0.0,
        "db_upsert_duration_ms_total": 0.0,
        "db_upsert_batch_count": 0,
        "commit_seconds": 0.0,
    }
    executor._update_global_skipped = lambda count: None
    executor._update_progress = lambda: None

    result = ParseResult(
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
        dataset_uuid="act-001:rp-001",
        global_skip=True,
        vector_status="reused",
        vector_nnz=10,
    )

    LciImportJobExecutor._flush_parse_result_batch(executor, [result])

    checkpoint = db.query(DatasetCheckpoint).filter_by(job_id=job_id, dataset_key="already.spold").one()
    assert checkpoint.status == "skipped_global"
    assert checkpoint.vector_status == "reused"
    assert checkpoint.vector_nnz == 10
    assert executor._stats["datasets_processed"] == 1
    assert executor._stats["datasets_skipped_global"] == 1
    assert executor._stats["vectors_reused"] == 1


def test_missing_elementary_flow_metadata_warns_without_failing_dataset(tmp_path):
    """Unresolved flow metadata should not discard an otherwise valid LCI vector."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.ecoinvent_ef31_loader import LCIDataset, LCIElementaryExchange
    from app.lci_import_executor import LciImportJobExecutor, ParseResult
    from app.models import DatasetCheckpoint, GlobalDatasetImport, ImportJob, ReferenceProcess

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)
    ReferenceProcess.__table__.create(engine)
    GlobalDatasetImport.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    job_id = "missing-flow-warning-job"
    db.add(ImportJob(job_id=job_id, file_path="/fake/path.7z", file_type="lci"))
    db.add(DatasetCheckpoint(job_id=job_id, dataset_key="gangue.spold", status="running"))
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = job_id
    executor.package_version = "ecoinvent_3.11"
    executor.overwrite_existing = True
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._failed_datasets = []
    executor._vector_warnings = []
    executor._global_import_cache = {}
    executor._global_skipped_dataset_keys = set()
    executor._elem_flow_lookup = {"known-flow": "known-flow"}
    executor._write_process_vector = lambda *args, **kwargs: {
        "nnz": 2,
        "vectors_written": 1,
        "warnings": [],
    }

    result = ParseResult(
        spold_path="gangue.spold",
        dataset=LCIDataset(
            filename="gangue.spold",
            activity_id="act-001",
            activity_name="Gangue process",
            location="GLO",
            reference_product_name="Product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="rp-001",
        ),
        exchanges=[
            LCIElementaryExchange(
                dataset_filename="gangue.spold",
                exchange_id="known-flow",
                exchange_name="Known",
                unit="kg",
                direction="output",
                amount=1.0,
            ),
            LCIElementaryExchange(
                dataset_filename="gangue.spold",
                exchange_id="missing-flow",
                exchange_name="Missing",
                unit="kg",
                direction="input",
                amount=2.0,
            ),
        ],
        process_uuid="act-001:rp-001",
        duration_ms=1,
    )

    executor._flush_single(result)
    executor._mark_checkpoints_complete([result])
    db.commit()

    checkpoint = db.query(DatasetCheckpoint).filter_by(job_id=job_id, dataset_key="gangue.spold").one()
    global_row = db.query(GlobalDatasetImport).filter_by(dataset_uuid="act-001:rp-001").one()

    assert checkpoint.status == "imported"
    assert checkpoint.vector_status == "written"
    assert checkpoint.vector_nnz == 2
    assert checkpoint.error_message == "missing elementary flow metadata refs: missing-flow"
    assert global_row.status == "imported"
    assert global_row.error_message == "missing elementary flow metadata refs: missing-flow"
    assert executor._stats["processes_failed"] == 0
    assert executor._stats["vectors_written"] == 1


def test_bulk_writer_flushes_empty_dataset_without_flush_single(tmp_path):
    """Batch writer should upsert process/global/checkpoint without per-dataset flush."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.ecoinvent_ef31_loader import LCIDataset
    from app.lci_import_executor import LciImportJobExecutor, ParseResult
    from app.models import (
        DatasetCheckpoint,
        GlobalDatasetImport,
        ImportJob,
        LciProcessVector,
        ReferenceProcess,
    )

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)
    ReferenceProcess.__table__.create(engine)
    LciProcessVector.__table__.create(engine)
    GlobalDatasetImport.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    job_id = "bulk-writer-job"
    db.add(ImportJob(job_id=job_id, file_path="/fake/path.7z", file_type="lci", status="running"))
    db.add(DatasetCheckpoint(job_id=job_id, dataset_key="empty.spold", status="running"))
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = job_id
    executor.package_version = "ecoinvent_3.11"
    executor.overwrite_existing = True
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._failed_datasets = []
    executor._vector_warnings = []
    executor._global_import_cache = {}
    executor._global_skipped_dataset_keys = set()
    executor._elem_flow_lookup = {}
    executor._unit_conversion_cache = {}
    executor._flow_key_cache = {}
    executor._flow_metadata_cache = {}
    executor._write_batch_size = 32
    executor._perf_stats = {
        "commit_count": 0,
        "flush_duration_ms_total": 0.0,
        "flush_result_count": 0,
        "batch_result_count": 0,
        "bulk_writer_enabled": True,
    }
    executor._flush_single = lambda result: (_ for _ in ()).throw(AssertionError("_flush_single should not be called"))

    result = ParseResult(
        spold_path="empty.spold",
        dataset=LCIDataset(
            filename="empty.spold",
            activity_id="act-empty",
            activity_name="Empty process",
            location="GLO",
            reference_product_name="Product",
            reference_product_unit="kg",
            reference_product_amount=1.0,
            reference_product_id="rp-empty",
        ),
        exchanges=[],
        process_uuid="act-empty:rp-empty",
        duration_ms=1,
    )

    executor._flush_parse_result_batch([result])

    checkpoint = db.query(DatasetCheckpoint).filter_by(job_id=job_id, dataset_key="empty.spold").one()
    process = db.get(ReferenceProcess, "act-empty:rp-empty")
    global_row = db.query(GlobalDatasetImport).filter_by(dataset_uuid="act-empty:rp-empty").one()

    assert checkpoint.status == "imported"
    assert checkpoint.vector_status == "empty"
    assert checkpoint.vector_nnz == 0
    assert process is not None
    assert global_row.status == "imported"
    assert global_row.vector_nnz == 0
    assert executor._stats["datasets_processed"] == 1
    assert executor._stats["empty_vectors"] == 1
    assert executor._perf_stats["commit_count"] == 1


def test_cache_built_once_per_job(tmp_path):
    """Unit conversion / flow key caches should be built once, not per-process."""
    import threading
    from collections import defaultdict
    from unittest.mock import MagicMock, patch

    from sqlalchemy.orm import sessionmaker

    from app.lci_import_executor import LciImportJobExecutor
    from app.models import ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    ImportJob.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    db.add(ImportJob(job_id="cache-test", file_path="/fake/path.7z", file_type="lci"))
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = "cache-test"
    executor.package_version = "ecoinvent_3.11"
    executor.overwrite_existing = False
    executor.spold_dir = tmp_path / "spold"
    executor.master_data_dir = None
    executor.workers = 2
    executor.limit = None
    executor.write_matrix_debug = False
    executor.resume_from_failed = False
    executor._pause_event = threading.Event()
    executor._paused = False
    executor._cancel_requested = False
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._failed_datasets = []
    executor._vector_warnings = []
    executor._total_count = 0
    executor._elementary_flows = []
    executor._elem_flow_lookup = {}
    executor._global_skipped_dataset_keys = set()
    executor._unit_conversion_cache = {}
    executor._flow_key_cache = {}
    executor._flow_metadata_cache = {}

    # Call _load_master_data when no master_data_dir → should do nothing
    executor._load_master_data()

    # Verify caches are still empty when no MasterData
    assert executor._unit_conversion_cache == {}
    assert executor._flow_key_cache == {}
    assert executor._flow_metadata_cache == {}


def test_masterdata_reuses_existing_db_catalog(tmp_path, monkeypatch):
    """Existing ecoinvent MasterData catalog should skip XML re-import on later jobs."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.lci_import_executor import LciImportJobExecutor
    from app.models import FlowRecord, ImportJob, UnitDefinition, UnitGroup
    from app import ingest_ecoinvent

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)
    UnitGroup.__table__.create(engine)
    UnitDefinition.__table__.create(engine)
    FlowRecord.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    db.add(ImportJob(job_id="masterdata-reuse", file_path="/fake/path.7z", file_type="lci"))
    db.add(UnitGroup(name="mass", reference_unit="kg"))
    for idx in range(100):
        db.add(UnitDefinition(unit_group="mass", unit_name=f"unit-{idx}", factor_to_reference=1.0, is_reference=idx == 0))
    for idx in range(9795):
        db.add(FlowRecord(
            flow_uuid=f"elementary-{idx}",
            flow_name=f"Elementary {idx}",
            flow_type="Elementary flow",
            default_unit="kg",
            unit_group="mass",
            source="ecoinvent_3.11",
        ))
    for idx in range(4000):
        db.add(FlowRecord(
            flow_uuid=f"product-{idx}",
            flow_name=f"Product {idx}",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="mass",
            source="ecoinvent_3.11",
        ))
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = "masterdata-reuse"
    executor.master_data_dir = tmp_path
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._perf_stats = {}
    executor._elementary_flows = []
    executor._elem_flow_lookup = {}
    executor._unit_conversion_cache = {}
    executor._flow_key_cache = {}
    executor._flow_metadata_cache = {}
    executor._update_job_phase = lambda phase: None

    def fail_import(*args, **kwargs):
        raise AssertionError("MasterData XML import should be skipped")

    monkeypatch.setattr(ingest_ecoinvent, "import_ecoinvent_units", fail_import)
    monkeypatch.setattr(ingest_ecoinvent, "import_ecoinvent_elementary_flows", fail_import)
    monkeypatch.setattr(ingest_ecoinvent, "import_ecoinvent_intermediate_flows", fail_import)

    LciImportJobExecutor._load_master_data(executor)

    assert executor._perf_stats["masterdata_reused"] is True
    assert len(executor._elementary_flows) == 9795
    assert "elementary-0" in executor._elem_flow_lookup


def test_control_signal_file_written(tmp_path):
    """Control signal API should write file even when DB is locked."""
    import json

    from app.job_control import read_job_control_signal, write_job_control_signal, clear_job_control_signal

    signal_dir = tmp_path / "job_control"
    # Monkey-patch the signal dir
    import app.job_control as jc
    original_dir = jc.SIGNAL_DIR
    jc.SIGNAL_DIR = signal_dir

    try:
        job_id = "signal-test-001"

        # Write signal
        result = write_job_control_signal(job_id, pause_requested=True, cancel_requested=False)
        assert result["job_id"] == job_id
        assert result["pause_requested"] is True
        assert result["cancel_requested"] is False

        # Read back
        signal = read_job_control_signal(job_id)
        assert signal is not None
        assert signal["pause_requested"] is True

        # Clear
        clear_job_control_signal(job_id)
        assert read_job_control_signal(job_id) is None

    finally:
        jc.SIGNAL_DIR = original_dir


def test_progress_stats_json_updates(tmp_path):
    """_update_progress should sync stats_json with current counts."""
    import threading
    from collections import defaultdict

    from sqlalchemy.orm import sessionmaker

    from app.lci_import_executor import LciImportJobExecutor
    from app.models import ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    ImportJob.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()

    job_id = "stats-test-001"
    db.add(ImportJob(
        job_id=job_id,
        file_path="/fake/path.7z",
        file_type="lci",
        progress_pct=0.0,
        stats_json=None,
    ))
    db.commit()

    executor = LciImportJobExecutor.__new__(LciImportJobExecutor)
    executor.db = db
    executor.job_id = job_id
    executor._total_count = 100
    executor._lock = threading.Lock()
    executor._stats = defaultdict(int)
    executor._stats["datasets_processed"] = 17
    executor._stats["processes_inserted"] = 10
    executor._stats["processes_updated"] = 3
    executor._stats["datasets_skipped_global"] = 2
    executor._stats["processes_failed"] = 2
    executor._stats["vectors_written"] = 10
    executor._stats["vectors_reused"] = 2
    executor._stats["empty_vectors"] = 1
    executor._stats["nnz_total"] = 500

    # Call _update_progress
    executor._update_progress()

    # Check DB state
    job = db.query(ImportJob).filter_by(job_id=job_id).one()
    assert job.progress_pct == 17.0  # (10 + 5 + 2) / 100 * 100 = 17.0
    assert job.stats_json is not None
    assert job.stats_json["datasets_processed"] == 17
    assert job.stats_json["processes_inserted"] == 10
    assert job.stats_json["processes_updated"] == 3
    assert job.stats_json["datasets_skipped_global"] == 2
    assert job.stats_json["processes_skipped"] == 2
    assert job.stats_json["processes_failed"] == 2
    assert job.stats_json["vectors_written"] == 10
    assert job.stats_json["vectors_reused"] == 2
    assert job.stats_json["empty_vectors"] == 1
    assert job.stats_json["vector_nnz_total"] == 500
    assert job.stats_json["commit_count"] == 0
    assert job.stats_json["write_batch_size"] == LciImportJobExecutor.DEFAULT_WRITE_BATCH_SIZE
    assert job.stats_json["avg_parse_ms"] == 0.0
    assert job.stats_json["global_skip_fast_count"] == 0
    assert job.stats_json["metadata_parse_count"] == 0
    assert job.stats_json["exchange_parse_count"] == 0
    assert job.stats_json["avg_metadata_parse_ms"] == 0.0
    assert job.stats_json["avg_exchange_parse_ms"] == 0.0
    assert job.stats_json["masterdata_reused"] is False
    assert job.stats_json["masterdata_wall_seconds"] == 0.0


def test_recover_stale_jobs_resets_running_checkpoints(tmp_path, monkeypatch):
    """Stale running jobs should be cancellable for retry maintenance."""
    from datetime import datetime, timedelta

    from sqlalchemy.orm import sessionmaker

    from app.job_control import read_job_control_signal, recover_stale_jobs, write_job_control_signal
    from app.models import DatasetCheckpoint, ImportJob

    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)
    DatasetCheckpoint.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    job_id = "stale-job-001"
    db.add(ImportJob(
        job_id=job_id,
        file_path="/fake/path.7z",
        file_type="lci",
        status="running",
        phase="importing",
        updated_at=datetime.utcnow() - timedelta(hours=2),
    ))
    db.add(DatasetCheckpoint(
        job_id=job_id,
        dataset_key="dataset.spold",
        status="running",
    ))
    terminal_job_id = "terminal-job-001"
    db.add(ImportJob(
        job_id=terminal_job_id,
        file_path="/fake/terminal.7z",
        file_type="lci",
        status="completed",
        phase="done",
        updated_at=datetime.utcnow(),
    ))
    db.commit()
    write_job_control_signal(terminal_job_id, pause_requested=True, cancel_requested=True)

    result = recover_stale_jobs(db, max_seconds=60)

    assert result["jobs_cancelled"] == 1
    assert result["checkpoints_reset"] == 1
    assert result["signals_cleared"] == 1
    job = db.query(ImportJob).filter_by(job_id=job_id).one()
    checkpoint = db.query(DatasetCheckpoint).filter_by(job_id=job_id).one()
    assert job.status == "cancelled"
    assert checkpoint.status == "pending"
    assert read_job_control_signal(terminal_job_id) is None


def test_lcia_runtime_rejects_lci_import_job(tmp_path):
    """LCI import jobs should not fall through to the LCIA runtime artifact path."""
    from fastapi import HTTPException
    from sqlalchemy.orm import sessionmaker

    from app.api.ef31_chunked_import import generate_lcia_runtime_for_job
    from app.models import ImportJob

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    ImportJob.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    db = Session()
    job_id = "completed-lci-job"
    db.add(ImportJob(
        job_id=job_id,
        file_path="/fake/path.7z",
        file_type="lci",
        status="completed",
        phase="done",
    ))
    db.commit()

    with pytest.raises(HTTPException) as exc_info:
        generate_lcia_runtime_for_job(job_id, db)

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "LCIA_RUNTIME_UNSUPPORTED_JOB_TYPE"
