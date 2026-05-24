"""Resumable EF 3.1 LCI import job executor.

Core execution engine:
- MasterData / unit / flow dictionary loaded once per job
- .spold files processed with dataset-level checkpoint (skipped/imported/failed)
- ``workers`` parser threads read + parse XML concurrently
- Single DB writer thread batches upserts to avoid SQLite lock contention
- Supports pause/resume/retry-failed

Usage:
    executor = LciImportJobExecutor(job_id, db_session, workers=4)
    result = executor.run()
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

from sqlalchemy import event
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from . import ecoinvent_ef31_loader as _loader
from .ef31_db_service import _generate_lci_process_uuid
from .models import (
    DatasetCheckpoint,
    GlobalDatasetImport,
    ImportJob,
    LciExchangeMatrix,
    LciProcessVector,
    ReferenceProcess,
)

logger = logging.getLogger(__name__)

# ── Event types passed through the parsing queue ────────────────────────────

@dataclass
class ParseResult:
    """Parsed dataset + exchanges from a single .spold file."""
    spold_path: str
    dataset: Optional[object]  # LCIDataset
    exchanges: list[object]    # list[LCIElementaryExchange] (kept for compat)
    process_uuid: str
    duration_ms: int
    error: str | None = None
    dataset_uuid: str | None = None
    global_skip: bool = False
    reused_process_uuid: str | None = None
    reused_vector_nnz: int | None = None
    parse_stage: str = "exchanges"
    vector_status: str | None = None  # written | reused | empty | failed
    vector_nnz: int | None = None
    warning: str | None = None
    # Worker-produced write plan (non-DB payload, set by _build_write_plan)
    write_plan: LciWritePlan | None = None


@dataclass
class _VectorExchange:
    flow_uuid: str
    amount: float
    unit: str
    direction: str


@dataclass
class _PackedVector:
    nnz: int
    flow_key_ids_blob: bytes
    amounts_blob: bytes
    index_dtype: str
    amount_dtype: str
    compression: str
    checksum: str
    canonicalized: bool
    compressed_bytes: int
    warnings: list[str] = field(default_factory=list)


@dataclass
class LciWritePlan:
    """Worker-produced write plan for the DB writer.

    Contains only in-memory data (no ORM objects) so it can be
    created by parser threads without touching the DB session.
    """
    spold_path: str
    process_uuid: str
    dataset_uuid: str
    process_json: dict
    # Aggregated flow key map: (flow_uuid, compartment, subcompartment, direction, canonical_unit) -> amount
    flow_key_aggs: dict[tuple[str, str, str, str, str], float] = field(default_factory=dict)
    # Missing flow uuids the writer needs to resolve
    missing_flow_uuids: list[str] = field(default_factory=list)
    # Vector-level warning (missing elementary refs, etc.)
    warning: str | None = None
    # Precomputed packed vector (from worker if cache hits, else writer packs)
    packed_blob: bytes | None = None
    packed_nnz: int = 0
    # Stats hints
    has_exchanges: bool = False
    canonicalized: bool = True
    pack_warnings: list[str] = field(default_factory=list)


@dataclass
class FlushBatch:
    """Batch to be written by the single DB writer thread."""
    # Process metadata upserts
    processes_to_upsert: list[dict] = field(default_factory=list)
    # Flow key upserts (from exchanges)
    flow_uuids_needed: set[str] = field(default_factory=set)
    # Compressed vectors
    vectors_to_upsert: list[dict] = field(default_factory=list)
    # Error info
    error_summary: str | None = None


@dataclass
class ImportResult:
    """Final result after executor completes."""
    job_id: str
    datasets_processed: int = 0
    processes_inserted: int = 0
    processes_updated: int = 0
    processes_skipped: int = 0
    processes_failed: int = 0
    datasets_skipped_global: int = 0
    skipped_global: int = 0
    flows_new: int = 0
    flows_error: int = 0
    vectors_written: int = 0
    vectors_reused: int = 0
    empty_vectors: int = 0
    vector_nnz_total: int = 0
    vector_warnings: list = field(default_factory=list)
    failed_datasets: list[str] = field(default_factory=list)
    performance_stats: dict = field(default_factory=dict)
    duration_seconds: float = 0.0
    error_summary: str | None = None


# ── Executor ──────────────────────────────────────────────────────────────


class LciImportJobExecutor:
    """Thread-safe resumable import executor.

    DB access: only the main thread (writer) touches the DB session.
    Parser threads produce ParseResult objects which are collected
    and flushed by the writer.
    """

    DEFAULT_WRITE_BATCH_SIZE = 32
    DEFAULT_QUEUE_MAXSIZE = 128
    DEFAULT_WRITE_FLUSH_INTERVAL_SECONDS = 2.0
    DEFAULT_VECTOR_COMPRESSION_LEVEL = 1

    @classmethod
    def _configured_vector_compression_level(cls) -> int:
        raw = os.getenv("LCI_VECTOR_COMPRESSION_LEVEL")
        if raw is None or not raw.strip():
            return cls.DEFAULT_VECTOR_COMPRESSION_LEVEL
        try:
            return max(0, min(9, int(raw.strip())))
        except ValueError:
            return cls.DEFAULT_VECTOR_COMPRESSION_LEVEL

    def __init__(
        self,
        job_id: str,
        db: Session,
        *,
        spold_dir: str,
        master_data_dir: str | None = None,
        workers: int = 4,
        limit: int | None = None,
        write_matrix_debug: bool = False,
        resume_from_failed: bool = False,
        overwrite_existing: bool = False,
        package_version: str = "ecoinvent_3.11",
    ):
        self.job_id = job_id
        self.db = db
        self.spold_dir = Path(spold_dir)
        self.master_data_dir = Path(master_data_dir) if master_data_dir else None
        self.workers = max(1, min(8, workers))
        self.limit = limit
        self.write_matrix_debug = write_matrix_debug
        self.resume_from_failed = resume_from_failed
        self.overwrite_existing = overwrite_existing
        self.package_version = package_version

        # Debug mode flags (default False — never active in production)
        self.debug_parser_blackhole = False
        self.debug_writer_replay = False
        self._replay_write_plans: list[LciWritePlan] = []
        self._driver_timing_listeners: tuple[object, object, object] | None = None

        # Pause control
        self._pause_event = threading.Event()
        self._paused = False
        self._cancel_requested = False

        # Performance caches (built once per job, shared across workers)
        self._unit_conversion_cache: dict[str, tuple[float, str]] = {}
        self._flow_key_cache: dict[tuple[str, str, str, str, str], int] = {}
        self._flow_metadata_cache: dict[str, tuple[str, str]] = {}

        # Statistics (protected by lock)
        self._lock = threading.Lock()
        self._stats = {
            "datasets_processed": 0,
            "processes_inserted": 0,
            "processes_updated": 0,
            "datasets_skipped_global": 0,
            "processes_failed": 0,
            "vectors_written": 0,
            "vectors_reused": 0,
            "empty_vectors": 0,
            "nnz_total": 0,
        }
        self._failed_datasets: list[str] = []
        self._vector_warnings: list[str] = []
        self._error_summary: str | None = None
        self._total_count = 0
        self._global_skipped_dataset_keys: set[str] = set()
        self._global_import_cache: dict[str, dict] = {}
        self._write_batch_size = self.DEFAULT_WRITE_BATCH_SIZE
        self._queue_maxsize = self.DEFAULT_QUEUE_MAXSIZE
        self._write_flush_interval_seconds = self.DEFAULT_WRITE_FLUSH_INTERVAL_SECONDS
        self._vector_compression_level = self._configured_vector_compression_level()
        self._vector_compression_mode = "none" if self._vector_compression_level == 0 else "zlib"
        self._perf_stats = {
            "parse_wall_seconds": 0.0,
            "write_wall_seconds": 0.0,
            "commit_count": 0,
            "write_batch_size": self._write_batch_size,
            "queue_max_observed": 0,
            "parse_duration_ms_total": 0,
            "parse_result_count": 0,
            "metadata_duration_ms_total": 0,
            "metadata_parse_count": 0,
            "exchange_duration_ms_total": 0,
            "exchange_parse_count": 0,
            "global_skip_fast_count": 0,
            "filename_fast_skip_count": 0,
            "filename_metadata_hit_count": 0,
            "filename_metadata_miss_count": 0,
            "flush_duration_ms_total": 0.0,
            "flush_result_count": 0,
            "batch_prefetch_wall_seconds": 0.0,
            "batch_prefetch_process_wall_seconds": 0.0,
            "batch_prefetch_vector_wall_seconds": 0.0,
            "batch_prefetch_global_wall_seconds": 0.0,
            "batch_prefetch_checkpoint_wall_seconds": 0.0,
            "batch_pack_wall_seconds": 0.0,
            "batch_db_upsert_wall_seconds": 0.0,
            "batch_checkpoint_wall_seconds": 0.0,
            "batch_result_count": 0,
            "pack_duration_ms_total": 0.0,
            "pack_result_count": 0,
            "db_upsert_duration_ms_total": 0.0,
            "db_upsert_batch_count": 0,
            "bulk_writer_enabled": True,
            "single_parse_enabled": True,
            "worker_aggregate_enabled": True,
            "worker_aggregate_wall_seconds": 0.0,
            "flow_key_resolve_wall_seconds": 0.0,
            "flow_key_missing_count": 0,
            "flow_key_cache_hits": 0,
            "flow_key_created": 0,
            "flow_key_resolve_query_seconds": 0.0,
            "avg_worker_aggregate_ms": 0.0,
            "avg_flow_key_resolve_ms": 0.0,
            "aggregated_result_count": 0,
            "exchange_object_count": 0,
            "core_upsert_enabled": False,
            "core_upsert_wall_seconds": 0.0,
            "streaming_parser_enabled": False,
            "stream_parse_wall_seconds": 0.0,
            # Writer-side exclusive timing
            "writer_queue_get_block_seconds": 0.0,
            "writer_queue_drain_nowait_seconds": 0.0,
            "db_execute_seconds": 0.0,
            "db_fetch_seconds": 0.0,
            "orm_materialize_seconds": 0.0,
            "flow_resolve_sql_seconds": 0.0,
            "flow_resolve_python_seconds": 0.0,
            "pack_sort_seconds": 0.0,
            "pack_compress_seconds": 0.0,
            "upsert_execute_seconds": 0.0,
            "checkpoint_core_upsert_enabled": False,
            "checkpoint_upsert_execute_seconds": 0.0,
            "vector_compression_level": self._vector_compression_level,
            "vector_compression_mode": self._vector_compression_mode,
            "commit_seconds": 0.0,
            "session_clear_gc_seconds": 0.0,
            # Worker-side exclusive timing
            "file_read_seconds": 0.0,
            "xml_parse_seconds": 0.0,
            "exchange_extract_seconds": 0.0,
            "unit_canonicalize_seconds": 0.0,
            "aggregate_seconds": 0.0,
            "result_serialize_seconds": 0.0,
            "queue_put_block_seconds": 0.0,
            # Queue occupancy sampling (for p50/p95/max)
            "_queue_occupancy_samples": [],
        }

        # Pre-load MasterData flows (shared across workers)
        self._elementary_flows = []
        self._elem_flow_lookup: dict[str, object] = {}

    # ── Public API ─────────────────────────────────────────────────────

    def run(self) -> ImportResult:
        """Execute the import job. Blocks until done, paused, or cancelled."""
        start_time = time.time()
        result = ImportResult(job_id=self.job_id)

        try:
            # Register SQLAlchemy driver-level timing for cursor executes
            self._register_driver_timing()

            self._load_master_data()

            spold_files = self._discover_spold_files()
            if not spold_files:
                self._error_summary = "No .spold files found"
                logger.warning("[%s] No .spold files found in %s", self.job_id, self.spold_dir)
                return self._finish_result(start_time, result)

            if self.limit and self.limit > 0:
                spold_files = spold_files[: self.limit]

            self._total_count = len(spold_files)
            checkpoints = self._load_checkpoints(spold_files)
            pending_files, skipped_count = self._filter_by_checkpoint(spold_files, checkpoints)
            self._load_global_import_cache()

            if not pending_files:
                logger.info("[%s] All datasets already imported or skipped", self.job_id)

            self._update_job_phase("importing")
            self._mark_datasets_running(pending_files, checkpoints)
            self.db.commit()

            if pending_files:
                sig_pause, sig_cancel = self._check_control_signals()
                if sig_cancel:
                    self._cancel_requested = True
                    self._update_job_status("cancelled")
                elif sig_pause:
                    self._update_job_status("paused")
                else:
                    self._run_parse_write_pipeline(pending_files)
                    self._update_progress()

            with self._lock:
                result.datasets_processed = self._stats["datasets_processed"]
                result.processes_inserted = self._stats["processes_inserted"]
                result.processes_updated = self._stats["processes_updated"]
                result.datasets_skipped_global = self._stats["datasets_skipped_global"] + skipped_count
                result.processes_skipped = result.datasets_skipped_global
                result.processes_failed = self._stats["processes_failed"]
                result.skipped_global = result.datasets_skipped_global
                result.vectors_written = self._stats["vectors_written"]
                result.vectors_reused = self._stats["vectors_reused"]
                result.empty_vectors = self._stats["empty_vectors"]
                result.vector_nnz_total = self._stats["nnz_total"]
                result.vector_warnings = list(self._vector_warnings)
                result.failed_datasets = list(self._failed_datasets)
                result.performance_stats = self._build_performance_stats()

            current_status = self._get_requested_status()
            if current_status == "paused" or self._paused:
                self._update_job_status("paused")
            elif current_status == "cancelled" or self._cancel_requested:
                self._update_job_status("cancelled")
            else:
                self._update_job_status("completed")
            result.duration_seconds = time.time() - start_time

        except Exception as exc:
            self._error_summary = str(exc)[:1024]
            logger.exception("[%s] Import job failed: %s", self.job_id, exc)
            self._update_job_status("failed")
        finally:
            self._unregister_driver_timing()

        return self._finish_result(start_time, result)

    def pause(self) -> None:
        """Request a pause at the next safe point."""
        self._paused = True
        self._pause_event.set()

    def resume(self) -> None:
        """Resume after pause."""
        self._paused = False
        self._pause_event.clear()

    def cancel(self) -> None:
        """Request cancellation."""
        self._cancel_requested = True
        self._paused = True
        self._pause_event.set()

    # ── MasterData loading ─────────────────────────────────────────────

    def _load_master_data(self) -> None:
        """Load elementary flows from MasterData (shared lookup for all workers).

        Also pre-builds performance caches (unit conversion, flow keys,
        flow metadata) so they are NOT rebuilt per-process-vector.
        """
        if not self.master_data_dir or not self.master_data_dir.exists():
            return
        started = time.perf_counter()
        try:
            self._update_job_phase("masterdata")
            self.db.commit()
            from .ingest_ecoinvent import (
                import_ecoinvent_elementary_flows,
                import_ecoinvent_intermediate_flows,
                import_ecoinvent_units,
                _build_unit_conversion_cache,
                _build_lci_flow_key_cache,
                _build_flow_metadata_cache,
            )

            if self._has_loaded_master_data():
                self._load_elementary_flow_lookup_from_db()
                self._perf_stats["masterdata_reused"] = True
            else:
                import_ecoinvent_units(self.db, data_dir=str(self.master_data_dir), package_version="ecoinvent_3.11")
                import_ecoinvent_elementary_flows(self.db, data_dir=str(self.master_data_dir), source="ecoinvent_3.11")
                import_ecoinvent_intermediate_flows(self.db, data_dir=str(self.master_data_dir), source="ecoinvent_3.11")

                units_map = _loader.parse_units(self.master_data_dir)
                self._elementary_flows = _loader.parse_elementary_exchanges(
                    self.master_data_dir, units_map
                )
                self._elem_flow_lookup = {f.flow_uuid: f for f in self._elementary_flows}
                self._perf_stats["masterdata_reused"] = False

            # Build job-level caches ONCE (not per-process)
            try:
                self._unit_conversion_cache = _build_unit_conversion_cache(self.db)
                self._flow_key_cache = _build_lci_flow_key_cache(self.db)
                self._flow_metadata_cache = _build_flow_metadata_cache(self.db)
            except Exception:
                # Non-critical: caches degrade gracefully
                pass

            logger.info(
                f"[{self.job_id}] Loaded {len(self._elementary_flows)} elementary flows"
            )
        except Exception as exc:
            logger.warning(f"[{self.job_id}] Failed to load MasterData: {exc}")
            self._elementary_flows = []
            self._elem_flow_lookup = {}
        finally:
            self._perf_stats["masterdata_wall_seconds"] = round(time.perf_counter() - started, 3)

    def _has_loaded_master_data(self) -> bool:
        from .models import FlowRecord, UnitDefinition

        units_count = self.db.query(UnitDefinition.id).count()
        elementary_count = (
            self.db.query(FlowRecord.flow_uuid)
            .filter(
                FlowRecord.flow_type == "Elementary flow",
                FlowRecord.source.in_(["ecoinvent", "ecoinvent_3.11"]),
            )
            .count()
        )
        intermediate_count = (
            self.db.query(FlowRecord.flow_uuid)
            .filter(
                FlowRecord.flow_type.in_(["Product flow", "Waste flow"]),
                FlowRecord.source.in_(["ecoinvent", "ecoinvent_3.11"]),
            )
            .count()
        )
        return units_count >= 100 and elementary_count >= 9795 and intermediate_count >= 4000

    def _load_elementary_flow_lookup_from_db(self) -> None:
        from .models import FlowRecord

        rows = (
            self.db.query(FlowRecord.flow_uuid)
            .filter(
                FlowRecord.flow_type == "Elementary flow",
                FlowRecord.source.in_(["ecoinvent", "ecoinvent_3.11"]),
            )
            .all()
        )
        flow_uuids = [row[0] for row in rows if row[0]]
        self._elementary_flows = flow_uuids
        self._elem_flow_lookup = {flow_uuid: flow_uuid for flow_uuid in flow_uuids}

    # ── File discovery ─────────────────────────────────────────────────

    def _discover_spold_files(self) -> list[Path]:
        files = sorted(p for p in self.spold_dir.rglob("*.spold") if p.is_file())
        if not files:
            files = sorted(p for p in self.spold_dir.rglob("*.xml") if p.is_file())
        return files

    # ── Checkpoint management ──────────────────────────────────────────

    def _load_checkpoints(self, spold_files: list[Path]) -> dict[str, DatasetCheckpoint]:
        """Load existing checkpoints, keyed by spold filename."""
        checkpoints: dict[str, DatasetCheckpoint] = {}
        existing = (
            self.db.query(DatasetCheckpoint)
            .filter(DatasetCheckpoint.job_id == self.job_id)
            .all()
        )
        for cp in existing:
            checkpoints[Path(cp.dataset_key).name] = cp
        return checkpoints

    def _filter_by_checkpoint(
        self,
        spold_files: list[Path],
        checkpoints: dict[str, DatasetCheckpoint],
    ) -> tuple[list[Path], int]:
        """Filter to pending/failed files, skip already-imported."""
        pending: list[Path] = []
        skipped = 0
        for sp in spold_files:
            key = sp.name
            cp = checkpoints.get(key)
            if cp is None:
                pending.append(sp)
            elif cp.status == "imported":
                skipped += 1
            elif cp.status == "failed" and not self.resume_from_failed:
                pending.append(sp)  # retry failed
            elif cp.status == "failed" and self.resume_from_failed:
                pending.append(sp)  # only retry failed
            elif cp.status in ("pending", "running"):
                pending.append(sp)
        return pending, skipped

    def _mark_datasets_running(
        self,
        pending_files: list[Path],
        checkpoints: dict[str, DatasetCheckpoint],
    ) -> None:
        for sp in pending_files:
            key = sp.name
            cp = checkpoints.get(key)
            if cp is None:
                cp = DatasetCheckpoint(
                    job_id=self.job_id,
                    dataset_key=key,
                    status="running",
                )
                self.db.add(cp)
            else:
                cp.status = "running"
        self.db.flush()

    def _mark_checkpoints_complete(self, results: list[ParseResult]) -> None:
        for pr in results:
            if pr.error:
                status = "failed"
            elif pr.global_skip or Path(pr.spold_path).name in self._global_skipped_dataset_keys:
                status = "skipped_global"
            elif pr.dataset:
                status = "imported"
            else:
                status = "skipped"
            key = Path(pr.spold_path).name
            cp = (
                self.db.query(DatasetCheckpoint)
                .filter(
                    DatasetCheckpoint.job_id == self.job_id,
                    DatasetCheckpoint.dataset_key == key,
                )
                .first()
            )
            if cp:
                cp.status = status
                cp.process_uuid = pr.process_uuid
                cp.vector_status = pr.vector_status
                cp.vector_nnz = pr.vector_nnz
                cp.duration_ms = pr.duration_ms
                cp.error_message = pr.error or pr.warning
                self.db.flush()

    # ── Concurrent parsing ─────────────────────────────────────────────

    def _parse_one(self, spold_path: Path) -> ParseResult:
        """Parse a single .spold file in a worker thread.

        Strategy:
        1. Parse metadata first when global fast-skip is possible.
        2. Global dedup check before exchange parsing.
        3. For normal datasets: parse exchanges and build a ``LciWritePlan`` in the worker
           (unit canonicalize + logical key aggregation), stored as
           ``ParseResult.write_plan``.  The writer only does DB upserts.
        """
        t0 = time.time()
        try:
            # Check pause
            while self._paused and not self._cancel_requested:
                self._pause_event.wait(timeout=0.5)
            if self._cancel_requested:
                return ParseResult(
                    spold_path=str(spold_path),
                    dataset=None,
                    exchanges=[],
                    process_uuid="",
                    duration_ms=0,
                    error="cancelled",
                )

            # ── Filename-based global fast skip ────────────────────────
            # ecoinvent LCI filenames are activity_uuid_reference_uuid.spold.
            # For no-overwrite jobs this lets already-imported datasets skip
            # XML metadata parsing entirely.
            filename_ids = _loader.parse_spold_filename_dataset_ids(spold_path)
            if not self.overwrite_existing and filename_ids is not None:
                activity_id, ref_product_id, dataset_uuid = filename_ids
                perf = getattr(self, "_perf_stats", None)
                if isinstance(perf, dict):
                    lock = getattr(self, "_lock", None)

                    def _record_filename_hit() -> None:
                        perf["filename_metadata_hit_count"] = (
                            int(perf.get("filename_metadata_hit_count", 0) or 0) + 1
                        )

                    if lock is None:
                        _record_filename_hit()
                    else:
                        with lock:
                            _record_filename_hit()
                cached_global = self._global_import_cache.get(dataset_uuid)
                if (
                    not self.overwrite_existing
                    and cached_global
                    and cached_global.get("status") == "imported"
                ):
                    if isinstance(perf, dict):
                        lock = getattr(self, "_lock", None)

                        def _record_filename_skip() -> None:
                            perf["filename_fast_skip_count"] = (
                                int(perf.get("filename_fast_skip_count", 0) or 0) + 1
                            )

                        if lock is None:
                            _record_filename_skip()
                        else:
                            with lock:
                                _record_filename_skip()
                    reused_nnz = int(cached_global.get("vector_nnz") or 0)
                    ds = _loader.LCIDataset(
                        filename=spold_path.name,
                        activity_id=activity_id,
                        activity_name="",
                        location="",
                        reference_product_name="",
                        reference_product_unit="",
                        reference_product_amount=0.0,
                        reference_product_id=ref_product_id,
                    )
                    process_uuid = str(cached_global.get("process_uuid") or _generate_lci_process_uuid(ds))
                    return ParseResult(
                        spold_path=str(spold_path),
                        dataset=ds,
                        exchanges=[],
                        process_uuid=process_uuid,
                        duration_ms=int((time.time() - t0) * 1000),
                        dataset_uuid=dataset_uuid,
                        global_skip=True,
                        reused_process_uuid=process_uuid,
                        reused_vector_nnz=reused_nnz,
                        parse_stage="filename",
                        vector_status="reused" if reused_nnz > 0 else "empty",
                        vector_nnz=reused_nnz,
                    )
            elif not self.overwrite_existing:
                perf = getattr(self, "_perf_stats", None)
                if isinstance(perf, dict):
                    lock = getattr(self, "_lock", None)

                    def _record_filename_miss() -> None:
                        perf["filename_metadata_miss_count"] = (
                            int(perf.get("filename_metadata_miss_count", 0) or 0) + 1
                        )

                    if lock is None:
                        _record_filename_miss()
                    else:
                        with lock:
                            _record_filename_miss()

            # ── Overwrite path: stream metadata + aggregated vector in one pass.
            if self.overwrite_existing:
                stream_t0 = time.perf_counter()
                streamed = self._parse_streaming_write_plan(spold_path)
                stream_elapsed = time.perf_counter() - stream_t0
                if streamed is not None:
                    ds, procs, dataset_uuid, write_plan = streamed
                    perf = getattr(self, "_perf_stats", None)
                    if isinstance(perf, dict):
                        lock = getattr(self, "_lock", None)
                        def _record_stream_time():
                            perf["xml_parse_seconds"] = (
                                float(perf.get("xml_parse_seconds", 0.0) or 0.0) + stream_elapsed
                            )
                            perf["aggregate_seconds"] = (
                                float(perf.get("aggregate_seconds", 0.0) or 0.0) + stream_elapsed
                            )
                        if lock is None:
                            _record_stream_time()
                        else:
                            with lock:
                                _record_stream_time()
                    return ParseResult(
                        spold_path=str(spold_path),
                        dataset=ds,
                        exchanges=[],
                        process_uuid=procs,
                        duration_ms=int((time.time() - t0) * 1000),
                        dataset_uuid=dataset_uuid,
                        parse_stage="aggregated",
                        write_plan=write_plan,
                    )

            # If the filename already gave us the dataset key and it is not
            # globally imported, go straight to the full streaming parser.
            # This avoids a metadata-only XML pass before the real parse.
            if not self.overwrite_existing and filename_ids is not None:
                streamed = self._parse_streaming_write_plan(spold_path)
                if streamed is not None:
                    ds, procs, dataset_uuid, write_plan = streamed
                    return ParseResult(
                        spold_path=str(spold_path),
                        dataset=ds,
                        exchanges=[],
                        process_uuid=procs,
                        duration_ms=int((time.time() - t0) * 1000),
                        dataset_uuid=dataset_uuid,
                        parse_stage="aggregated",
                        write_plan=write_plan,
                    )

            # ── Metadata parse / single-pass fallback ───────────────────
            single_t0 = time.perf_counter()
            include_exchanges = bool(self.overwrite_existing)
            if include_exchanges:
                single = _loader.parse_spold_dataset_and_exchanges(
                    spold_path,
                    include_exchanges=True,
                )
            else:
                ds = _loader.parse_spold_metadata_early(spold_path)
                single = (
                    _loader._SinglePassResult(dataset=ds, exchanges=[])
                    if ds is not None
                    else None
                )
            single_elapsed = time.perf_counter() - single_t0
            perf = getattr(self, "_perf_stats", None)
            if isinstance(perf, dict):
                lock = getattr(self, "_lock", None)
                def _record_single_pass_time():
                    perf["file_read_seconds"] = (
                        float(perf.get("file_read_seconds", 0.0) or 0.0) + single_elapsed
                    )
                    perf["xml_parse_seconds"] = (
                        float(perf.get("xml_parse_seconds", 0.0) or 0.0) + single_elapsed
                    )
                if lock is None:
                    _record_single_pass_time()
                else:
                    with lock:
                        _record_single_pass_time()
            if single is None:
                return ParseResult(
                    spold_path=str(spold_path),
                    dataset=None,
                    exchanges=[],
                    process_uuid="",
                    duration_ms=int((time.time() - t0) * 1000),
                    error="parse_spold_dataset_and_exchanges returned None",
                    parse_stage="metadata",
                )

            ds = single.dataset
            exchanges = single.exchanges  # list[LCIElementaryExchange]
            procs = _generate_lci_process_uuid(ds)
            dataset_uuid = self._dataset_uuid_for(ds)

            # ── Global dedup (fast skip — no exchange parsing needed) ───
            cached_global = self._global_import_cache.get(dataset_uuid)
            if not self.overwrite_existing and cached_global and cached_global.get("status") == "imported":
                # Fast skip: record file read time only
                perf = getattr(self, "_perf_stats", None)
                if isinstance(perf, dict):
                    lock = getattr(self, "_lock", None)
                    def _record_fast_skip_time():
                        perf["file_read_seconds"] = (
                            float(perf.get("file_read_seconds", 0.0) or 0.0) + single_elapsed
                        )
                    if lock is None:
                        _record_fast_skip_time()
                    else:
                        with lock:
                            _record_fast_skip_time()
                reused_nnz = int(cached_global.get("vector_nnz") or 0)
                return ParseResult(
                    spold_path=str(spold_path),
                    dataset=ds,
                    exchanges=[],
                    process_uuid=str(cached_global.get("process_uuid") or procs),
                    duration_ms=int((time.time() - t0) * 1000),
                    dataset_uuid=dataset_uuid,
                    global_skip=True,
                    reused_process_uuid=str(cached_global.get("process_uuid") or procs),
                    reused_vector_nnz=reused_nnz,
                    parse_stage="metadata",
                    vector_status="reused" if reused_nnz > 0 else "empty",
                    vector_nnz=reused_nnz,
                )

            if not include_exchanges:
                streamed = self._parse_streaming_write_plan(spold_path)
                if streamed is not None:
                    ds, procs, dataset_uuid, write_plan = streamed
                    return ParseResult(
                        spold_path=str(spold_path),
                        dataset=ds,
                        exchanges=[],
                        process_uuid=procs,
                        duration_ms=int((time.time() - t0) * 1000),
                        dataset_uuid=dataset_uuid,
                        parse_stage="aggregated",
                        write_plan=write_plan,
                    )

                single = _loader.parse_spold_dataset_and_exchanges(
                    spold_path,
                    include_exchanges=True,
                )
                if single is None:
                    return ParseResult(
                        spold_path=str(spold_path),
                        dataset=None,
                        exchanges=[],
                        process_uuid="",
                        duration_ms=int((time.time() - t0) * 1000),
                        error="parse_spold_dataset_and_exchanges returned None",
                        parse_stage="metadata",
                    )
                ds = single.dataset
                exchanges = single.exchanges
                procs = _generate_lci_process_uuid(ds)
                dataset_uuid = self._dataset_uuid_for(ds)

            # ── Worker-side aggregation ─────────────────────────────────
            write_plan = self._build_write_plan(
                spold_path=spold_path,
                ds=ds,
                exchanges=exchanges,
                procs=procs,
                dataset_uuid=dataset_uuid,
            )

            return ParseResult(
                spold_path=str(spold_path),
                dataset=ds,
                exchanges=[],
                process_uuid=procs,
                duration_ms=int((time.time() - t0) * 1000),
                dataset_uuid=dataset_uuid,
                parse_stage="aggregated",
                write_plan=write_plan,
            )
        except Exception as exc:
            return ParseResult(
                spold_path=str(spold_path),
                dataset=None,
                exchanges=[],
                process_uuid="",
                duration_ms=int((time.time() - t0) * 1000),
                error=str(exc),
                parse_stage="metadata",
            )

    def _parse_streaming_write_plan(
        self,
        spold_path: Path,
    ) -> tuple[object, str, str, LciWritePlan] | None:
        """Parse and aggregate one SPOLD file directly into a write plan."""
        stream_t0 = time.perf_counter()
        stream = _loader.parse_spold_streaming_agg(
            spold_path,
            unit_conversion_cache=getattr(self, "_unit_conversion_cache", None) or {},
            elem_flow_lookup=getattr(self, "_elem_flow_lookup", None) or {},
            flow_metadata_cache=getattr(self, "_flow_metadata_cache", None) or {},
        )
        elapsed = time.perf_counter() - stream_t0
        perf = getattr(self, "_perf_stats", None)
        if isinstance(perf, dict):
            lock = getattr(self, "_lock", None)

            def _record_stream_stats() -> None:
                perf["streaming_parser_enabled"] = True
                perf["stream_parse_wall_seconds"] = (
                    float(perf.get("stream_parse_wall_seconds", 0.0) or 0.0) + elapsed
                )
                perf["exchange_object_count"] = int(perf.get("exchange_object_count", 0) or 0)

            if lock is None:
                _record_stream_stats()
            else:
                with lock:
                    _record_stream_stats()
        if stream is None:
            return None

        ds = stream.dataset
        procs = _generate_lci_process_uuid(ds)
        dataset_uuid = self._dataset_uuid_for(ds)
        process_json = dict(stream.process_json or {})
        process_json["process_uuid"] = procs

        write_plan = LciWritePlan(
            spold_path=str(spold_path),
            process_uuid=procs,
            dataset_uuid=dataset_uuid,
            process_json=process_json,
            flow_key_aggs=stream.flow_key_aggs,
            missing_flow_uuids=stream.missing_flow_uuids,
            warning=stream.warning,
            has_exchanges=bool(stream.flow_key_aggs),
            canonicalized=stream.canonicalized,
            pack_warnings=stream.pack_warnings,
        )
        perf = getattr(self, "_perf_stats", None)
        if isinstance(perf, dict):
            lock = getattr(self, "_lock", None)

            def _record_aggregate_count() -> None:
                perf["aggregated_result_count"] = (
                    int(perf.get("aggregated_result_count", 0) or 0) + 1
                )

            if lock is None:
                _record_aggregate_count()
            else:
                with lock:
                    _record_aggregate_count()
        return ds, procs, dataset_uuid, write_plan

    def _build_write_plan(
        self,
        *,
        spold_path: str | Path,
        ds: object,
        exchanges: list[object],
        procs: str,
        dataset_uuid: str,
    ) -> LciWritePlan:
        """Build an ``LciWritePlan`` from parsed exchanges in a worker thread.

        No DB access — only in-memory unit canonicalize + logical key
        aggregation.  The writer thread does DB upserts and final vector pack.
        """
        t0 = time.perf_counter()
        spold_str = str(spold_path)
        process_json = {
            "process_uuid": procs,
            "activity_id": ds.activity_id,
            "process_name": ds.activity_name,
            "location": ds.location,
            "reference_product": ds.reference_product_name,
            "reference_product_id": ds.reference_product_id,
            "reference_product_unit": ds.reference_product_unit,
            "reference_product_amount": ds.reference_product_amount,
            "exchange_count": len(exchanges),
            "source": "ecoinvent_3.11",
        }

        # ── Exchange extraction + unit canonicalize + aggregation ─────
        flow_key_aggs: dict[tuple[str, str, str, str, str], float] = {}
        missing_flow_uuids: set[str] = set()
        pack_warnings: list[str] = []
        canonicalized = True
        has_exchanges = False
        elem_lookup = getattr(self, '_elem_flow_lookup', None) or {}
        unit_cache = getattr(self, '_unit_conversion_cache', None) or {}
        meta_cache = getattr(self, '_flow_metadata_cache', None) or {}

        extract_t = time.perf_counter()
        for ex in exchanges:
            exc_id = getattr(ex, 'exchange_id', '')
            if not exc_id or float(getattr(ex, 'amount', 0)) == 0:
                continue
            has_exchanges = True

            unit_raw = getattr(ex, 'unit', '') or ''
            direction = getattr(ex, 'direction', '') or ''
            amount_f = float(getattr(ex, 'amount', 0))

            # Unit canonicalize
            canon_unit = unit_raw
            if unit_raw.strip():
                conv = unit_cache.get(unit_raw.strip())
                if conv is not None:
                    factor, ref = conv
                    amount_f = amount_f * factor
                    canon_unit = ref
                else:
                    canon_unit = unit_raw.strip()
                    canonicalized = False
                    pack_warnings.append(
                        f"Missing unit conversion for process={procs} flow={exc_id} unit={unit_raw}"
                    )

            # Missing flow check
            compartment = ""
            subcompartment = ""
            if elem_lookup and exc_id in elem_lookup:
                ef = elem_lookup[exc_id]
                compartment = getattr(ef, 'compartment', '') or ''
                subcompartment = getattr(ef, 'subcompartment', '') or ''
            elif exc_id in meta_cache:
                meta = meta_cache.get(exc_id, ("", ""))
                compartment = meta[0]
            else:
                missing_flow_uuids.add(exc_id)

            key = (exc_id, compartment, subcompartment, direction, canon_unit)
            flow_key_aggs[key] = flow_key_aggs.get(key, 0.0) + amount_f
        extract_elapsed = time.perf_counter() - extract_t

        warning = None
        if missing_flow_uuids:
            shown = ", ".join(sorted(missing_flow_uuids)[:5])
            if len(missing_flow_uuids) > 5:
                shown += f", ... (+{len(missing_flow_uuids) - 5} more)"
            warning = f"missing elementary flow metadata refs: {shown}"

        elapsed_ms = (time.perf_counter() - t0) * 1000
        perf = getattr(self, "_perf_stats", None)
        if isinstance(perf, dict):
            lock = getattr(self, "_lock", None)

            def _record_worker_aggregate_stats() -> None:
                perf["worker_aggregate_wall_seconds"] = (
                    float(perf.get("worker_aggregate_wall_seconds", 0.0) or 0.0)
                    + elapsed_ms / 1000
                )
                perf["aggregated_result_count"] = (
                    int(perf.get("aggregated_result_count", 0) or 0) + 1
                )
                perf["exchange_extract_seconds"] = (
                    float(perf.get("exchange_extract_seconds", 0.0) or 0.0) + extract_elapsed
                )
                perf["aggregate_seconds"] = (
                    float(perf.get("aggregate_seconds", 0.0) or 0.0) + elapsed_ms / 1000
                )

            if lock is None:
                _record_worker_aggregate_stats()
            else:
                with lock:
                    _record_worker_aggregate_stats()
        return LciWritePlan(
            spold_path=spold_str,
            process_uuid=procs,
            dataset_uuid=dataset_uuid,
            process_json=process_json,
            flow_key_aggs=flow_key_aggs,
            missing_flow_uuids=sorted(missing_flow_uuids),
            warning=warning,
            has_exchanges=has_exchanges,
            canonicalized=canonicalized,
            pack_warnings=pack_warnings,
        )

    def _parse_files_concurrent(self, files: list[Path]) -> list[ParseResult]:
        results: list[ParseResult] = []
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {pool.submit(self._parse_one, f): f for f in files}
            for future in as_completed(futures):
                # Check pause between each future completion
                while self._paused and not self._cancel_requested:
                    self._pause_event.wait(timeout=0.5)
                if self._cancel_requested:
                    # Cancel remaining
                    for f2 in futures:
                        f2.cancel()
                    break
                try:
                    results.append(future.result())
                except Exception as exc:
                    logger.warning(f"Future error: {exc}")
        return results

    def _run_parse_write_pipeline(self, files: list[Path]) -> None:
        """Parse files concurrently while the main thread flushes DB batches."""
        result_queue: Queue[ParseResult | object] = Queue(maxsize=self._queue_maxsize)
        sentinel = object()
        writer_started = time.perf_counter()

        producer = threading.Thread(
            target=self._produce_parse_results,
            args=(files, result_queue, sentinel),
            daemon=True,
        )
        producer.start()

        pending: list[ParseResult] = []
        last_flush = time.perf_counter()
        while True:
            timeout = max(0.1, min(0.5, self._write_flush_interval_seconds - (time.perf_counter() - last_flush)))
            try:
                item = result_queue.get(timeout=timeout)
            except Empty:
                if pending and time.perf_counter() - last_flush >= self._write_flush_interval_seconds:
                    self._flush_parse_result_batch(pending)
                    pending = []
                    last_flush = time.perf_counter()
                continue

            if item is sentinel:
                break
            pending.append(item)  # type: ignore[arg-type]
            if len(pending) >= self._write_batch_size:
                self._flush_parse_result_batch(pending)
                pending = []
                last_flush = time.perf_counter()

        if pending:
            self._flush_parse_result_batch(pending)
        producer.join()
        self._perf_stats["write_wall_seconds"] += time.perf_counter() - writer_started

    def _produce_parse_results(
        self,
        files: list[Path],
        result_queue: Queue[ParseResult | object],
        sentinel: object,
    ) -> None:
        """Producer side of the bounded parse/write pipeline."""
        parse_started = time.perf_counter()
        next_index = 0
        in_flight = {}
        max_in_flight = max(self.workers * 4, self.workers)

        def submit_more(pool: ThreadPoolExecutor) -> None:
            nonlocal next_index
            while (
                next_index < len(files)
                and len(in_flight) < max_in_flight
                and not self._cancel_requested
                and not self._paused
            ):
                path = files[next_index]
                in_flight[pool.submit(self._parse_one, path)] = path
                next_index += 1

        try:
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                submit_more(pool)
                while in_flight:
                    done, _ = wait(in_flight, timeout=0.5, return_when=FIRST_COMPLETED)
                    if not done:
                        sig_pause, sig_cancel = self._check_control_signals()
                        if sig_cancel:
                            self._cancel_requested = True
                        if sig_pause:
                            self._paused = True
                        if self._cancel_requested or self._paused:
                            for future in in_flight:
                                future.cancel()
                            break
                        continue

                    for future in done:
                        path = in_flight.pop(future)
                        if future.cancelled():
                            continue
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = ParseResult(
                                spold_path=str(path),
                                dataset=None,
                                exchanges=[],
                                process_uuid="",
                                duration_ms=0,
                                error=str(exc),
                            )
                        self._record_parse_result_stats(result)
                        self._put_parse_result(result_queue, result)

                    sig_pause, sig_cancel = self._check_control_signals()
                    if sig_cancel:
                        self._cancel_requested = True
                    if sig_pause:
                        self._paused = True
                    if self._cancel_requested or self._paused:
                        for future in in_flight:
                            future.cancel()
                        break
                    submit_more(pool)
        finally:
            self._perf_stats["parse_wall_seconds"] += time.perf_counter() - parse_started
            result_queue.put(sentinel)

    def _put_parse_result(self, result_queue: Queue[ParseResult | object], result: ParseResult) -> None:
        put_t0 = time.perf_counter()
        while True:
            try:
                result_queue.put(result, timeout=0.5)
                put_elapsed = time.perf_counter() - put_t0
                self._perf_stats["queue_put_block_seconds"] = (
                    float(self._perf_stats.get("queue_put_block_seconds", 0.0) or 0.0) + put_elapsed
                )
                occupancy = result_queue.qsize()
                # Sample queue occupancy for p50/p95/max
                samples = self._perf_stats.get("_queue_occupancy_samples", [])
                samples.append(occupancy)
                # Keep a rolling window to avoid unbounded memory growth
                if len(samples) > 10000:
                    self._perf_stats["_queue_occupancy_samples"] = samples[-5000:]
                    samples = self._perf_stats["_queue_occupancy_samples"]
                self._perf_stats["queue_max_observed"] = max(
                    int(self._perf_stats.get("queue_max_observed", 0)),
                    occupancy,
                )
                return
            except Exception:
                if self._cancel_requested:
                    put_elapsed = time.perf_counter() - put_t0
                    self._perf_stats["queue_put_block_seconds"] = (
                        float(self._perf_stats.get("queue_put_block_seconds", 0.0) or 0.0) + put_elapsed
                    )
                    return

    def _flush_parse_result_batch(self, results: list[ParseResult]) -> None:
        t0 = time.perf_counter()
        if hasattr(self.db, "query"):
            self._flush_result_batch_bulk(results)
        else:
            for pr in results:
                self._flush_single(pr)
            self._mark_checkpoints_complete(results)
        self._update_progress()
        commit_t0 = time.perf_counter()
        self.db.commit()
        commit_elapsed = time.perf_counter() - commit_t0
        self._perf_stats["commit_seconds"] = (
            float(self._perf_stats.get("commit_seconds", 0.0) or 0.0) + commit_elapsed
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        self._perf_stats["commit_count"] += 1
        self._perf_stats["flush_duration_ms_total"] += elapsed_ms
        self._perf_stats["flush_result_count"] += len(results)
        self._perf_stats["batch_result_count"] = int(self._perf_stats.get("batch_result_count", 0) or 0) + len(results)

    def _record_parse_result_stats(self, result: ParseResult) -> None:
        duration_ms = max(0, int(result.duration_ms or 0))
        self._perf_stats["parse_duration_ms_total"] = int(self._perf_stats.get("parse_duration_ms_total", 0) or 0) + duration_ms
        self._perf_stats["parse_result_count"] = int(self._perf_stats.get("parse_result_count", 0) or 0) + 1
        if result.parse_stage == "filename":
            pass
        elif result.parse_stage == "metadata":
            self._perf_stats["metadata_duration_ms_total"] = int(self._perf_stats.get("metadata_duration_ms_total", 0) or 0) + duration_ms
            self._perf_stats["metadata_parse_count"] = int(self._perf_stats.get("metadata_parse_count", 0) or 0) + 1
        else:
            self._perf_stats["exchange_duration_ms_total"] = int(self._perf_stats.get("exchange_duration_ms_total", 0) or 0) + duration_ms
            self._perf_stats["exchange_parse_count"] = int(self._perf_stats.get("exchange_parse_count", 0) or 0) + 1
        if result.global_skip:
            self._perf_stats["global_skip_fast_count"] = int(self._perf_stats.get("global_skip_fast_count", 0) or 0) + 1

    def _build_performance_stats(self) -> dict:
        perf = getattr(self, "_perf_stats", {})
        write_batch_size = int(getattr(self, "_write_batch_size", self.DEFAULT_WRITE_BATCH_SIZE))
        parse_count = int(perf.get("parse_result_count", 0) or 0)
        metadata_count = int(perf.get("metadata_parse_count", 0) or 0)
        exchange_count = int(perf.get("exchange_parse_count", 0) or 0)
        flush_count = int(perf.get("flush_result_count", 0) or 0)
        avg_parse_ms = (
            float(perf.get("parse_duration_ms_total", 0) or 0) / parse_count
            if parse_count
            else 0.0
        )
        avg_flush_ms = (
            float(perf.get("flush_duration_ms_total", 0.0) or 0.0) / flush_count
            if flush_count
            else 0.0
        )
        avg_metadata_ms = (
            float(perf.get("metadata_duration_ms_total", 0) or 0) / metadata_count
            if metadata_count
            else 0.0
        )
        avg_exchange_ms = (
            float(perf.get("exchange_duration_ms_total", 0) or 0) / exchange_count
            if exchange_count
            else 0.0
        )
        commit_count = int(perf.get("commit_count", 0) or 0)
        pack_count = int(perf.get("pack_result_count", 0) or 0)
        upsert_count = int(perf.get("db_upsert_batch_count", 0) or 0)
        avg_batch_size = (
            float(perf.get("batch_result_count", 0) or 0) / commit_count
            if commit_count
            else 0.0
        )
        avg_pack_ms = (
            float(perf.get("pack_duration_ms_total", 0.0) or 0.0) / pack_count
            if pack_count
            else 0.0
        )
        avg_db_upsert_ms = (
            float(perf.get("db_upsert_duration_ms_total", 0.0) or 0.0) / upsert_count
            if upsert_count
            else 0.0
        )
        result = {
            "parse_wall_seconds": round(float(perf.get("parse_wall_seconds", 0.0) or 0.0), 3),
            "write_wall_seconds": round(float(perf.get("write_wall_seconds", 0.0) or 0.0), 3),
            "commit_count": commit_count,
            "write_batch_size": int(perf.get("write_batch_size", write_batch_size) or write_batch_size),
            "queue_max_observed": int(perf.get("queue_max_observed", 0) or 0),
            "avg_parse_ms": round(avg_parse_ms, 3),
            "global_skip_fast_count": int(perf.get("global_skip_fast_count", 0) or 0),
            "filename_fast_skip_count": int(perf.get("filename_fast_skip_count", 0) or 0),
            "filename_metadata_hit_count": int(perf.get("filename_metadata_hit_count", 0) or 0),
            "filename_metadata_miss_count": int(perf.get("filename_metadata_miss_count", 0) or 0),
            "metadata_parse_count": metadata_count,
            "exchange_parse_count": exchange_count,
            "avg_metadata_parse_ms": round(avg_metadata_ms, 3),
            "avg_exchange_parse_ms": round(avg_exchange_ms, 3),
            "avg_flush_ms": round(avg_flush_ms, 3),
            "masterdata_reused": bool(perf.get("masterdata_reused", False)),
            "masterdata_wall_seconds": round(float(perf.get("masterdata_wall_seconds", 0.0) or 0.0), 3),
            "batch_prefetch_wall_seconds": round(float(perf.get("batch_prefetch_wall_seconds", 0.0) or 0.0), 3),
            "batch_prefetch_process_wall_seconds": round(float(perf.get("batch_prefetch_process_wall_seconds", 0.0) or 0.0), 3),
            "batch_prefetch_vector_wall_seconds": round(float(perf.get("batch_prefetch_vector_wall_seconds", 0.0) or 0.0), 3),
            "batch_prefetch_global_wall_seconds": round(float(perf.get("batch_prefetch_global_wall_seconds", 0.0) or 0.0), 3),
            "batch_prefetch_checkpoint_wall_seconds": round(float(perf.get("batch_prefetch_checkpoint_wall_seconds", 0.0) or 0.0), 3),
            "batch_pack_wall_seconds": round(float(perf.get("batch_pack_wall_seconds", 0.0) or 0.0), 3),
            "batch_db_upsert_wall_seconds": round(float(perf.get("batch_db_upsert_wall_seconds", 0.0) or 0.0), 3),
            "batch_checkpoint_wall_seconds": round(float(perf.get("batch_checkpoint_wall_seconds", 0.0) or 0.0), 3),
            "avg_batch_size": round(avg_batch_size, 3),
            "avg_pack_ms": round(avg_pack_ms, 3),
            "avg_db_upsert_ms": round(avg_db_upsert_ms, 3),
            "bulk_writer_enabled": bool(perf.get("bulk_writer_enabled", False)),
            "single_parse_enabled": bool(perf.get("single_parse_enabled", False)),
            "worker_aggregate_enabled": bool(perf.get("worker_aggregate_enabled", False)),
            "worker_aggregate_wall_seconds": round(float(perf.get("worker_aggregate_wall_seconds", 0.0) or 0.0), 3),
            "flow_key_resolve_wall_seconds": round(float(perf.get("flow_key_resolve_wall_seconds", 0.0) or 0.0), 3),
            "flow_key_missing_count": int(perf.get("flow_key_missing_count", 0) or 0),
            "aggregated_result_count": int(perf.get("aggregated_result_count", 0) or 0),
            "flow_key_cache_hits": int(perf.get("flow_key_cache_hits", 0) or 0),
            "flow_key_created": int(perf.get("flow_key_created", 0) or 0),
            "flow_key_resolve_query_seconds": round(float(perf.get("flow_key_resolve_query_seconds", 0.0) or 0.0), 3),
            "exchange_object_count": int(perf.get("exchange_object_count", 0) or 0),
            "core_upsert_enabled": bool(perf.get("core_upsert_enabled", False)),
            "core_upsert_wall_seconds": round(float(perf.get("core_upsert_wall_seconds", 0.0) or 0.0), 3),
            "streaming_parser_enabled": bool(perf.get("streaming_parser_enabled", False)),
            "stream_parse_wall_seconds": round(float(perf.get("stream_parse_wall_seconds", 0.0) or 0.0), 3),
            # Writer exclusive timing
            "writer_queue_get_block_seconds": round(float(perf.get("writer_queue_get_block_seconds", 0.0) or 0.0), 3),
            "writer_queue_drain_nowait_seconds": round(float(perf.get("writer_queue_drain_nowait_seconds", 0.0) or 0.0), 3),
            "db_execute_seconds": round(float(perf.get("db_execute_seconds", 0.0) or 0.0), 3),
            "db_fetch_seconds": round(float(perf.get("db_fetch_seconds", 0.0) or 0.0), 3),
            "orm_materialize_seconds": round(float(perf.get("orm_materialize_seconds", 0.0) or 0.0), 3),
            "flow_resolve_sql_seconds": round(float(perf.get("flow_resolve_sql_seconds", 0.0) or 0.0), 3),
            "flow_resolve_python_seconds": round(float(perf.get("flow_resolve_python_seconds", 0.0) or 0.0), 3),
            "pack_sort_seconds": round(float(perf.get("pack_sort_seconds", 0.0) or 0.0), 3),
            "pack_compress_seconds": round(float(perf.get("pack_compress_seconds", 0.0) or 0.0), 3),
            "upsert_execute_seconds": round(float(perf.get("upsert_execute_seconds", 0.0) or 0.0), 3),
            "checkpoint_core_upsert_enabled": bool(perf.get("checkpoint_core_upsert_enabled", False)),
            "checkpoint_upsert_execute_seconds": round(float(perf.get("checkpoint_upsert_execute_seconds", 0.0) or 0.0), 3),
            "vector_compression_level": int(perf.get("vector_compression_level", self.DEFAULT_VECTOR_COMPRESSION_LEVEL)),
            "vector_compression_mode": str(perf.get("vector_compression_mode", "zlib") or "zlib"),
            "commit_seconds": round(float(perf.get("commit_seconds", 0.0) or 0.0), 3),
            "session_clear_gc_seconds": round(float(perf.get("session_clear_gc_seconds", 0.0) or 0.0), 3),
            # Worker exclusive timing
            "file_read_seconds": round(float(perf.get("file_read_seconds", 0.0) or 0.0), 3),
            "xml_parse_seconds": round(float(perf.get("xml_parse_seconds", 0.0) or 0.0), 3),
            "exchange_extract_seconds": round(float(perf.get("exchange_extract_seconds", 0.0) or 0.0), 3),
            "unit_canonicalize_seconds": round(float(perf.get("unit_canonicalize_seconds", 0.0) or 0.0), 3),
            "aggregate_seconds": round(float(perf.get("aggregate_seconds", 0.0) or 0.0), 3),
            "result_serialize_seconds": round(float(perf.get("result_serialize_seconds", 0.0) or 0.0), 3),
            "queue_put_block_seconds": round(float(perf.get("queue_put_block_seconds", 0.0) or 0.0), 3),
            # Queue occupancy stats
            "queue_occupancy_p50": 0.0,
            "queue_occupancy_p95": 0.0,
            "queue_occupancy_max": 0,
            # Debug mode stats
            "debug_parser_blackhole": False,
            "debug_writer_replay": False,
            "blackhole_drain_count": 0,
        }
        # Fill debug mode stats
        result["debug_parser_blackhole"] = bool(perf.get("debug_parser_blackhole", False))
        result["debug_writer_replay"] = bool(perf.get("debug_writer_replay", False))
        result["blackhole_drain_count"] = int(perf.get("blackhole_drain_count", 0) or 0)
        # Compute queue occupancy percentiles from samples
        samples = perf.get("_queue_occupancy_samples", [])
        if samples:
            sorted_samples = sorted(samples)
            n = len(sorted_samples)
            result["queue_occupancy_p50"] = round(float(sorted_samples[n // 2]), 1)
            result["queue_occupancy_p95"] = round(float(sorted_samples[int(n * 0.95)]), 1)
            result["queue_occupancy_max"] = max(sorted_samples)
        return result

    # ── DB flush (single thread) ───────────────────────────────────────

    def _is_sqlite(self) -> bool:
        """Check if the current database is SQLite."""
        bind = self.db.get_bind()
        return getattr(getattr(bind, "dialect", None), "name", None) == "sqlite"

    def _register_driver_timing(self) -> None:
        """Register SQLAlchemy event listeners for cursor-level timing."""
        if getattr(self, "_driver_timing_listeners", None) is not None:
            return
        engine = self.db.get_bind() if hasattr(self.db, "get_bind") else getattr(self.db, "bind", None)
        if engine is None:
            return

        t0_storage = {"t0": 0.0}

        def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            t0_storage["t0"] = time.perf_counter()

        def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            elapsed = time.perf_counter() - t0_storage["t0"]
            lock = getattr(self, "_lock", None)
            perf = getattr(self, "_perf_stats", None)
            if perf is None:
                return

            def _record():
                if isinstance(statement, str) and statement.strip().startswith("SELECT"):
                    perf["db_fetch_seconds"] = (
                        float(perf.get("db_fetch_seconds", 0.0) or 0.0) + elapsed
                    )
                else:
                    perf["db_execute_seconds"] = (
                        float(perf.get("db_execute_seconds", 0.0) or 0.0) + elapsed
                    )

            if lock is None:
                _record()
            else:
                with lock:
                    _record()

        event.listen(engine, "before_cursor_execute", _before_cursor_execute)
        event.listen(engine, "after_cursor_execute", _after_cursor_execute)
        self._driver_timing_listeners = (engine, _before_cursor_execute, _after_cursor_execute)

    def _unregister_driver_timing(self) -> None:
        listeners = getattr(self, "_driver_timing_listeners", None)
        if not listeners:
            return
        engine, before_listener, after_listener = listeners
        for name, listener in (
            ("before_cursor_execute", before_listener),
            ("after_cursor_execute", after_listener),
        ):
            try:
                event.remove(engine, name, listener)
            except Exception:
                pass
        self._driver_timing_listeners = None

    def _core_upsert_reference_processes(self, rows: list[dict]) -> None:
        """SQLite-optimized bulk upsert for ReferenceProcess.

        Each dict must have: process_uuid, process_name, process_name_en, process_type,
        process_json, source_file, import_mode, import_report_json.
        """
        if not rows:
            return
        if self._is_sqlite():
            now = datetime.utcnow()
            values = [{**r, "created_at": now, "updated_at": now} for r in rows]
            stmt = sqlite_insert(ReferenceProcess).values(values)
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                index_elements=["process_uuid"],
                set_={
                    "process_name": excluded.process_name,
                    "process_name_en": excluded.process_name_en,
                    "process_type": excluded.process_type,
                    "reference_flow_uuid": excluded.reference_flow_uuid,
                    "process_json": excluded.process_json,
                    "source_file": excluded.source_file,
                    "source_process_uuid": excluded.source_process_uuid,
                    "import_mode": excluded.import_mode,
                    "import_report_json": excluded.import_report_json,
                    "updated_at": now,
                },
            )
            self.db.execute(stmt)
            self._perf_stats["core_upsert_enabled"] = True
        else:
            # Fallback: ORM bulk
            for r in rows:
                proc = ReferenceProcess(
                    process_uuid=r["process_uuid"],
                    process_name=r["process_name"],
                    process_name_en=r["process_name_en"],
                    process_type=r["process_type"],
                    reference_flow_uuid=r.get("reference_flow_uuid"),
                    process_json=r["process_json"],
                    source_file=r.get("source_file"),
                    import_mode=r.get("import_mode"),
                    import_report_json=r.get("import_report_json"),
                )
                self.db.merge(proc)

    def _core_upsert_lci_process_vectors(self, rows: list[dict]) -> None:
        """SQLite-optimized bulk upsert for LciProcessVector."""
        if not rows:
            return
        if self._is_sqlite():
            now = datetime.utcnow()
            values = [{**r, "created_at": now, "updated_at": now} for r in rows]
            stmt = sqlite_insert(LciProcessVector).values(values)
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                index_elements=["process_uuid"],
                set_={
                    "dataset_level": excluded.dataset_level,
                    "system_model": excluded.system_model,
                    "nnz": excluded.nnz,
                    "axis_id": excluded.axis_id,
                    "flow_key_ids_blob": excluded.flow_key_ids_blob,
                    "amounts_blob": excluded.amounts_blob,
                    "index_dtype": excluded.index_dtype,
                    "amount_dtype": excluded.amount_dtype,
                    "compression": excluded.compression,
                    "canonicalized": excluded.canonicalized,
                    "checksum": excluded.checksum,
                    "source": excluded.source,
                    "source_package_version": excluded.source_package_version,
                    "updated_at": now,
                },
            )
            self.db.execute(stmt)
            self._perf_stats["core_upsert_enabled"] = True
        else:
            for r in rows:
                vec = LciProcessVector(**{k: v for k, v in r.items()
                                         if k in ('process_uuid', 'dataset_level', 'system_model',
                                                  'nnz', 'axis_id', 'flow_key_ids_blob',
                                                  'amounts_blob', 'index_dtype', 'amount_dtype',
                                                  'compression', 'canonicalized', 'checksum',
                                                  'source', 'source_package_version')})
                self.db.merge(vec)

    def _core_upsert_global_dataset_imports(self, rows: list[dict]) -> None:
        """SQLite-optimized bulk upsert for GlobalDatasetImport (unique on source_package_version + dataset_uuid)."""
        if not rows:
            return
        if self._is_sqlite():
            now = datetime.utcnow()
            values = [{**r, "updated_at": now} for r in rows]
            stmt = sqlite_insert(GlobalDatasetImport).values(values)
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                index_elements=["source_package_version", "dataset_uuid"],
                set_={
                    "dataset_filename": excluded.dataset_filename,
                    "process_uuid": excluded.process_uuid,
                    "activity_id": excluded.activity_id,
                    "reference_product_id": excluded.reference_product_id,
                    "status": excluded.status,
                    "vector_nnz": excluded.vector_nnz,
                    "checksum": excluded.checksum,
                    "last_job_id": excluded.last_job_id,
                    "error_message": excluded.error_message,
                    "imported_at": excluded.imported_at,
                    "updated_at": now,
                },
            )
            self.db.execute(stmt)
            self._perf_stats["core_upsert_enabled"] = True
        else:
            for r in rows:
                g = GlobalDatasetImport(**{k: v for k, v in r.items()
                                           if k in ('id', 'source_package_version', 'dataset_uuid',
                                                    'dataset_filename', 'process_uuid', 'activity_id',
                                                    'reference_product_id', 'status', 'vector_nnz',
                                                    'checksum', 'last_job_id', 'error_message',
                                                    'imported_at')})
                self.db.merge(g)
        # Rebuild the in-memory cache after upsert
        for r in rows:
            if r.get("status") == "imported":
                self._global_import_cache[r["dataset_uuid"]] = {
                    "status": "imported",
                    "process_uuid": r.get("process_uuid"),
                    "vector_nnz": int(r.get("vector_nnz") or 0),
                }

    def _core_upsert_dataset_checkpoints(self, rows: list[dict]) -> None:
        """Bulk upsert DatasetCheckpoint rows by job_id + dataset_key."""
        if not rows:
            return
        now = datetime.utcnow()
        if self._is_sqlite():
            values = [{**r, "created_at": now, "updated_at": now} for r in rows]
            stmt = sqlite_insert(DatasetCheckpoint).values(values)
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                index_elements=["job_id", "dataset_key"],
                set_={
                    "status": excluded.status,
                    "process_uuid": excluded.process_uuid,
                    "vector_status": excluded.vector_status,
                    "vector_nnz": excluded.vector_nnz,
                    "duration_ms": excluded.duration_ms,
                    "error_message": excluded.error_message,
                    "updated_at": now,
                },
            )
            self.db.execute(stmt)
            self._perf_stats["checkpoint_core_upsert_enabled"] = True
            return

        checkpoint_keys = [r["dataset_key"] for r in rows]
        existing = (
            self.db.query(DatasetCheckpoint)
            .filter(
                DatasetCheckpoint.job_id == self.job_id,
                DatasetCheckpoint.dataset_key.in_(checkpoint_keys),
            )
            .all()
            if checkpoint_keys
            else []
        )
        by_key = {row.dataset_key: row for row in existing}
        new_rows: list[DatasetCheckpoint] = []
        for r in rows:
            cp = by_key.get(r["dataset_key"])
            if cp is None:
                cp = DatasetCheckpoint(job_id=r["job_id"], dataset_key=r["dataset_key"], status=r["status"])
                by_key[r["dataset_key"]] = cp
                new_rows.append(cp)
            cp.status = r["status"]
            cp.process_uuid = r.get("process_uuid")
            cp.vector_status = r.get("vector_status")
            cp.vector_nnz = r.get("vector_nnz")
            cp.duration_ms = r.get("duration_ms")
            cp.error_message = r.get("error_message")
        if new_rows:
            self.db.add_all(new_rows)

    def debug_replay_writer(self) -> dict:
        """Replay writer logic for cached ``LciWritePlan`` objects.

        This is a **debug-only** method.  It does NOT affect the normal
        import pipeline.  Call it after a ``debug_writer_replay`` run to
        measure pack/resolve/upsert timings in isolation.

        Returns a dict with ``vectors_written``, ``empty_vectors``, ``nnz``,
        and ``flow_keys_created``.
        """
        if not self._replay_write_plans:
            return {
                "vectors_written": 0, "empty_vectors": 0,
                "nnz": 0, "flow_keys_created": 0,
                "error": "No cached write plans",
            }

        write_plan_results: list[LciWritePlan] = []
        all_keys: set[tuple[str, str, str, str, str]] = set()

        for wp in self._replay_write_plans:
            write_plan_results.append(wp)
            all_keys.update(wp.flow_key_aggs.keys())

        # Batch resolve flow keys
        created = self._batch_resolve_flow_keys(all_keys)

        # Resolve key IDs
        key_id_map: dict[tuple[str, str, str, str, str], int] = {}
        for lk in all_keys:
            cached = self._flow_key_cache.get(lk)
            if cached is not None:
                key_id_map[lk] = cached
                continue
            fu, comp, subcomp, direction, canon = lk
            resolved = self._get_flow_key_id(
                flow_uuid=fu, compartment=comp, subcompartment=subcomp,
                direction=direction, canonical_unit=canon,
            )
            if resolved is not None:
                key_id_map[lk] = resolved

        vectors_written = 0
        empty_vectors = 0
        total_nnz = 0

        for wp in write_plan_results:
            if not wp.flow_key_aggs:
                empty_vectors += 1
                continue
            agg: dict[int, float] = {}
            for lk, amount in wp.flow_key_aggs.items():
                fid = key_id_map.get(lk)
                if fid is not None:
                    agg[fid] = agg.get(fid, 0.0) + amount
            if not agg:
                empty_vectors += 1
                continue
            t_sort = time.perf_counter()
            flow_key_ids = sorted(agg)
            sort_elapsed = time.perf_counter() - t_sort
            amounts = [agg[k] for k in flow_key_ids]
            from .lci_vector_codec import pack_lci_vector
            t_compress = time.perf_counter()
            compression_level = int(getattr(self, "_vector_compression_level", self.DEFAULT_VECTOR_COMPRESSION_LEVEL))
            packed = pack_lci_vector(flow_key_ids, amounts, compression_level=compression_level)
            compress_elapsed = time.perf_counter() - t_compress
            self._perf_stats["pack_sort_seconds"] = (
                float(self._perf_stats.get("pack_sort_seconds", 0.0) or 0.0) + sort_elapsed
            )
            self._perf_stats["pack_compress_seconds"] = (
                float(self._perf_stats.get("pack_compress_seconds", 0.0) or 0.0) + compress_elapsed
            )
            vectors_written += 1
            total_nnz += packed.nnz
            self.db.add(ReferenceProcess(
                process_uuid=wp.process_uuid,
                process_name=wp.process_json.get("process_name", ""),
                process_name_en=wp.process_json.get("process_name_en", ""),
                process_type="lci_dataset",
                reference_flow_uuid=None,
                process_json=wp.process_json,
                source_file=wp.spold_path,
                import_mode="debug_replay",
                import_report_json={"debug_replay": True},
            ))
            self.db.add(LciProcessVector(
                process_uuid=wp.process_uuid,
                dataset_level="linked_lci",
                system_model=None,
                nnz=packed.nnz,
                axis_id=None,
                flow_key_ids_blob=packed.flow_key_ids_blob,
                amounts_blob=packed.amounts_blob,
                index_dtype=packed.index_dtype,
                amount_dtype=packed.amount_dtype,
                compression=packed.compression,
                canonicalized=wp.canonicalized,
                checksum=packed.checksum,
                source=self.package_version,
                source_package_version=self.package_version,
            ))

        self.db.commit()
        return {
            "vectors_written": vectors_written,
            "empty_vectors": empty_vectors,
            "nnz": total_nnz,
            "flow_keys_created": created,
        }

    def _flush_result_batch_bulk(self, results: list[ParseResult]) -> None:
        """Flush a parse-result batch with one prefetch/upsert/checkpoint pass.

        If ``debug_parser_blackhole`` is True, drains the queue without
        writing DB / resolving flow-keys / packing vectors.
        """
        if not results:
            return

        # ── Parser blackhole mode ──────────────────────────────────────
        if getattr(self, "debug_parser_blackhole", False):
            # Skip all DB work — just count drained batches
            self._perf_stats["blackhole_drain_count"] = (
                int(self._perf_stats.get("blackhole_drain_count", 0) or 0) + len(results)
            )
            self._perf_stats["debug_parser_blackhole"] = True
            return

        # ── Writer replay mode: cache write_plans without writing DB ───
        if getattr(self, "debug_writer_replay", False):
            for pr in results:
                wp = getattr(pr, "write_plan", None)
                if wp is not None:
                    self._replay_write_plans.append(wp)
            self._perf_stats["debug_writer_replay"] = True
            self._perf_stats["blackhole_drain_count"] = (
                int(self._perf_stats.get("blackhole_drain_count", 0) or 0) + len(results)
            )
            return

        for pr in results:
            if pr.error:
                pr.vector_status = "failed"
                pr.vector_nnz = 0
                with self._lock:
                    self._stats["datasets_processed"] += 1
                    self._stats["processes_failed"] += 1
                    self._failed_datasets.append(pr.spold_path)
            elif pr.global_skip:
                self._record_global_skip_result(pr)

        normal_results = [
            pr for pr in results
            if not pr.error and pr.dataset is not None and not pr.global_skip
        ]
        for pr in normal_results:
            pr.dataset_uuid = pr.dataset_uuid or self._dataset_uuid_for(pr.dataset)

        prefetch_t0 = time.perf_counter()
        fetch_t0 = time.perf_counter()
        global_rows_by_uuid: dict[str, GlobalDatasetImport] = {}
        dataset_uuids = [pr.dataset_uuid for pr in normal_results if pr.dataset_uuid]
        if dataset_uuids:
            global_rows = (
                self.db.query(GlobalDatasetImport)
                .filter(
                    GlobalDatasetImport.source_package_version == self.package_version,
                    GlobalDatasetImport.dataset_uuid.in_(dataset_uuids),
                )
                .all()
            )
            global_rows_by_uuid = {row.dataset_uuid: row for row in global_rows if row.dataset_uuid}
        fetch_elapsed = time.perf_counter() - fetch_t0

        write_results: list[ParseResult] = []
        for pr in normal_results:
            global_row = global_rows_by_uuid.get(pr.dataset_uuid or "")
            if not self.overwrite_existing and global_row is not None and global_row.status == "imported":
                reused_nnz = int(global_row.vector_nnz or 0)
                pr.global_skip = True
                pr.vector_status = "reused" if reused_nnz > 0 else "empty"
                pr.vector_nnz = reused_nnz
                if global_row.process_uuid:
                    pr.process_uuid = global_row.process_uuid
                self._record_global_skip_result(pr)
            else:
                write_results.append(pr)

        global_prefetch_elapsed = time.perf_counter() - prefetch_t0

        # Split prefetch timing
        self._perf_stats["batch_prefetch_global_wall_seconds"] = (
            float(self._perf_stats.get("batch_prefetch_global_wall_seconds", 0.0) or 0.0)
            + global_prefetch_elapsed
        )
        self._perf_stats["batch_prefetch_wall_seconds"] = (
            float(self._perf_stats.get("batch_prefetch_wall_seconds", 0.0) or 0.0)
            + global_prefetch_elapsed
        )
        # Exclusive timing: DB fetch (SQLAlchemy query execution + row fetch)
        self._perf_stats["db_fetch_seconds"] = (
            float(self._perf_stats.get("db_fetch_seconds", 0.0) or 0.0)
            + fetch_elapsed
        )

        pack_t0 = time.perf_counter()
        packed_by_process: dict[str, _PackedVector] = {}

        # ── Batch flow key resolution (writer-side optimization) ──────
        # Collect all logical keys from write_plans and resolve in bulk.
        all_logical_keys: set[tuple[str, str, str, str, str]] = set()

        for pr in write_results:
            wp = getattr(pr, 'write_plan', None)
            if wp is None:
                # Fallback: old-parse ParseResult with exchanges list
                self._apply_missing_flow_warning(pr)
                vector_exchanges = [
                    _VectorExchange(
                        flow_uuid=ex.exchange_id,
                        amount=float(ex.amount),
                        unit=ex.unit,
                        direction=ex.direction,
                    )
                    for ex in pr.exchanges
                    if ex.exchange_id and ex.amount != 0
                ]
                if vector_exchanges:
                    packed = self._pack_vector_exchanges(pr.process_uuid, vector_exchanges)
                    packed_by_process[pr.process_uuid] = packed
                continue

            for logical_key in wp.flow_key_aggs:
                all_logical_keys.add(logical_key)

        # Resolve logical keys -> flow_key_ids in bulk (using job-level cache)
        created_flow_key_count = 0
        if all_logical_keys:
            created_flow_key_count = self._batch_resolve_flow_keys(all_logical_keys)

        key_id_map: dict[tuple[str, str, str, str, str], int] = {}
        for logical_key in all_logical_keys:
            flow_uuid, comp, subcomp, direction, canon_unit = logical_key
            cached = self._flow_key_cache.get(logical_key)
            if cached is not None:
                key_id_map[logical_key] = cached
                continue
            # Fallback: single query (should be cached after batch_resolve)
            resolved = self._get_flow_key_id(
                flow_uuid=flow_uuid, compartment=comp, subcompartment=subcomp,
                direction=direction, canonical_unit=canon_unit,
            )
            if resolved is not None:
                key_id_map[logical_key] = resolved
                self._flow_key_cache[logical_key] = resolved

        # Pack vectors from write plans using resolved key IDs
        flow_key_resolve_ms = (time.perf_counter() - pack_t0) * 1000
        self._perf_stats["flow_key_resolve_wall_seconds"] = (
            float(self._perf_stats.get("flow_key_resolve_wall_seconds", 0.0) or 0.0)
            + flow_key_resolve_ms / 1000
        )
        self._perf_stats["flow_key_missing_count"] = (
            int(self._perf_stats.get("flow_key_missing_count", 0) or 0)
            + created_flow_key_count
        )

        pack_t1 = time.perf_counter()
        for pr in write_results:
            wp = getattr(pr, 'write_plan', None)
            if wp is not None:
                self._apply_missing_flow_warning(pr)
                pr.warning = wp.warning or pr.warning
                self._vector_warnings.extend(wp.pack_warnings)
                if wp.flow_key_aggs:
                    agg: dict[int, float] = {}
                    for logical_key, amount in wp.flow_key_aggs.items():
                        fid = key_id_map.get(logical_key)
                        if fid is not None:
                            agg[fid] = agg.get(fid, 0.0) + amount
                    if agg:
                        t_sort = time.perf_counter()
                        flow_key_ids = sorted(agg)
                        sort_elapsed = time.perf_counter() - t_sort
                        amounts = [agg[k] for k in flow_key_ids]
                        from .lci_vector_codec import pack_lci_vector
                        t_compress = time.perf_counter()
                        compression_level = int(getattr(self, "_vector_compression_level", self.DEFAULT_VECTOR_COMPRESSION_LEVEL))
                        packed = pack_lci_vector(
                            flow_key_ids,
                            amounts,
                            compression_level=compression_level,
                        )
                        compress_elapsed = time.perf_counter() - t_compress
                        self._perf_stats["pack_sort_seconds"] = (
                            float(self._perf_stats.get("pack_sort_seconds", 0.0) or 0.0) + sort_elapsed
                        )
                        self._perf_stats["pack_compress_seconds"] = (
                            float(self._perf_stats.get("pack_compress_seconds", 0.0) or 0.0) + compress_elapsed
                        )
                        packed_by_process[pr.process_uuid] = _PackedVector(
                            nnz=packed.nnz,
                            flow_key_ids_blob=packed.flow_key_ids_blob,
                            amounts_blob=packed.amounts_blob,
                            index_dtype=packed.index_dtype,
                            amount_dtype=packed.amount_dtype,
                            compression=packed.compression,
                            checksum=packed.checksum,
                            canonicalized=wp.canonicalized,
                            compressed_bytes=len(packed.flow_key_ids_blob) + len(packed.amounts_blob),
                            warnings=wp.pack_warnings,
                        )
                        pr.vector_status = "written"
                        pr.vector_nnz = packed.nnz
                        with self._lock:
                            self._stats["vectors_written"] += 1
                            self._stats["nnz_total"] += packed.nnz
                    else:
                        pr.vector_status = "empty"
                        pr.vector_nnz = 0
                else:
                    pr.vector_status = "empty"
                    pr.vector_nnz = 0
            else:
                # Fallback: old-parse ParseResult with exchanges list
                self._apply_missing_flow_warning(pr)
                vector_exchanges = [
                    _VectorExchange(
                        flow_uuid=ex.exchange_id,
                        amount=float(ex.amount),
                        unit=ex.unit,
                        direction=ex.direction,
                    )
                    for ex in pr.exchanges
                    if ex.exchange_id and ex.amount != 0
                ]
                if vector_exchanges:
                    packed = self._pack_vector_exchanges(pr.process_uuid, vector_exchanges)
                    packed_by_process[pr.process_uuid] = packed
                    pr.vector_status = "written"
                    pr.vector_nnz = packed.nnz
                    with self._lock:
                        self._stats["vectors_written"] += 1
                        self._stats["nnz_total"] += packed.nnz
                        self._vector_warnings.extend(packed.warnings)

        # ── Reused / empty vectors (for processes that were NOT written) ──
        # No pre-fetch needed; use INSERT OR REPLACE which handles updates.
        for pr in write_results:
            if pr.process_uuid in packed_by_process:
                continue
            # Only check for reuse when overwrite=False
            if not self.overwrite_existing:
                existing_vector = (
                    self.db.query(LciProcessVector)
                    .filter(LciProcessVector.process_uuid == pr.process_uuid)
                    .first()
                )
                if existing_vector is not None:
                    pr.vector_status = "reused"
                    pr.vector_nnz = int(existing_vector.nnz or 0)
                    with self._lock:
                        self._stats["vectors_reused"] += 1
                    continue
            pr.vector_status = "empty"
            pr.vector_nnz = 0
            with self._lock:
                self._stats["empty_vectors"] += 1
                if self.overwrite_existing:
                    self._vector_warnings.append(
                        f"{Path(pr.spold_path).name}: no elementary vector rows"
                    )

        pack_elapsed = time.perf_counter() - pack_t0
        self._perf_stats["batch_pack_wall_seconds"] = (
            float(self._perf_stats.get("batch_pack_wall_seconds", 0.0) or 0.0)
            + pack_elapsed
        )
        self._perf_stats["pack_duration_ms_total"] = (
            float(self._perf_stats.get("pack_duration_ms_total", 0.0) or 0.0)
            + pack_elapsed * 1000
        )
        self._perf_stats["pack_result_count"] = (
            int(self._perf_stats.get("pack_result_count", 0) or 0)
            + len([pr for pr in write_results if pr.process_uuid in packed_by_process])
        )

        upsert_t0 = time.perf_counter()

        # ── Build upsert data (dict-based, no ORM pre-fetch needed) ──────
        process_upsert_rows: list[dict] = []
        vector_upsert_rows: list[dict] = []
        global_upsert_rows: list[dict] = []

        for pr in write_results:
            ds = pr.dataset
            if ds is None:
                continue
            wp = getattr(pr, 'write_plan', None)
            process_json = wp.process_json if wp else self._build_lci_process_json(pr)
            dataset_uuid = pr.dataset_uuid or self._dataset_uuid_for(ds)
            pr.dataset_uuid = dataset_uuid

            # Collect process upsert data
            process_upsert_rows.append({
                "process_uuid": pr.process_uuid,
                "process_name": ds.activity_name or pr.process_uuid,
                "process_name_en": ds.activity_name,
                "process_type": "lci_dataset",
                "reference_flow_uuid": None,
                "process_json": process_json,
                "source_file": pr.spold_path,
                "import_mode": "ecoinvent_ef31_lci",
                "import_report_json": {"package_version": self.package_version},
            })

            # Collect vector upsert data
            packed = packed_by_process.get(pr.process_uuid)
            if packed is not None:
                vector_upsert_rows.append({
                    "process_uuid": pr.process_uuid,
                    "dataset_level": "linked_lci",
                    "system_model": None,
                    "nnz": packed.nnz,
                    "axis_id": None,
                    "flow_key_ids_blob": packed.flow_key_ids_blob,
                    "amounts_blob": packed.amounts_blob,
                    "index_dtype": packed.index_dtype,
                    "amount_dtype": packed.amount_dtype,
                    "compression": packed.compression,
                    "canonicalized": packed.canonicalized,
                    "checksum": packed.checksum,
                    "source": self.package_version,
                    "source_package_version": self.package_version,
                })

            # Collect global dataset import upsert data
            global_upsert_rows.append({
                "source_package_version": self.package_version,
                "dataset_uuid": dataset_uuid,
                "dataset_filename": pr.spold_path,
                "process_uuid": pr.process_uuid,
                "activity_id": ds.activity_id,
                "reference_product_id": ds.reference_product_id,
                "status": "imported",
                "vector_nnz": int(pr.vector_nnz or 0),
                "last_job_id": self.job_id,
                "error_message": pr.warning,
                "imported_at": datetime.utcnow(),
            })
            with self._lock:
                self._stats["datasets_processed"] += 1

        # ── Core bulk upsert (SQLite-optimized) ─────────────────────────
        existing_process_uuids: set[str] = set()
        if process_upsert_rows:
            process_prefetch_t0 = time.perf_counter()
            process_uuids = [row["process_uuid"] for row in process_upsert_rows]
            existing_process_uuids = {
                row[0]
                for row in self.db.query(ReferenceProcess.process_uuid)
                .filter(ReferenceProcess.process_uuid.in_(process_uuids))
                .all()
            }
            self._perf_stats["batch_prefetch_process_wall_seconds"] = (
                float(self._perf_stats.get("batch_prefetch_process_wall_seconds", 0.0) or 0.0)
                + (time.perf_counter() - process_prefetch_t0)
            )

        core_upsert_t0 = time.perf_counter()
        upsert_exec_t0 = time.perf_counter()
        if process_upsert_rows:
            self._core_upsert_reference_processes(process_upsert_rows)
            updated_count = sum(1 for row in process_upsert_rows if row["process_uuid"] in existing_process_uuids)
            inserted_count = len(process_upsert_rows) - updated_count
            with self._lock:
                self._stats["processes_inserted"] += inserted_count
                self._stats["processes_updated"] += updated_count
        if vector_upsert_rows:
            self._core_upsert_lci_process_vectors(vector_upsert_rows)
        if global_upsert_rows:
            self._core_upsert_global_dataset_imports(global_upsert_rows)
        upsert_exec_elapsed = time.perf_counter() - upsert_exec_t0
        self._perf_stats["upsert_execute_seconds"] = (
            float(self._perf_stats.get("upsert_execute_seconds", 0.0) or 0.0) + upsert_exec_elapsed
        )
        core_upsert_total = time.perf_counter() - core_upsert_t0
        self._perf_stats["core_upsert_wall_seconds"] = (
            float(self._perf_stats.get("core_upsert_wall_seconds", 0.0) or 0.0)
            + core_upsert_total
        )

        self._perf_stats["batch_db_upsert_wall_seconds"] = (
            float(self._perf_stats.get("batch_db_upsert_wall_seconds", 0.0) or 0.0)
            + (time.perf_counter() - upsert_t0)
        )
        self._perf_stats["db_upsert_duration_ms_total"] = (
            float(self._perf_stats.get("db_upsert_duration_ms_total", 0.0) or 0.0)
            + (time.perf_counter() - upsert_t0) * 1000
        )
        self._perf_stats["db_upsert_batch_count"] = int(self._perf_stats.get("db_upsert_batch_count", 0) or 0) + 1

        checkpoint_t0 = time.perf_counter()
        checkpoint_rows: list[dict] = []
        for pr in results:
            key = Path(pr.spold_path).name
            checkpoint_rows.append({
                "job_id": self.job_id,
                "dataset_key": key,
                "status": self._checkpoint_status_for(pr),
                "process_uuid": pr.process_uuid,
                "vector_status": pr.vector_status,
                "vector_nnz": pr.vector_nnz,
                "duration_ms": pr.duration_ms,
                "error_message": pr.error or pr.warning,
            })
        checkpoint_upsert_t0 = time.perf_counter()
        self._core_upsert_dataset_checkpoints(checkpoint_rows)
        self._perf_stats["checkpoint_upsert_execute_seconds"] = (
            float(self._perf_stats.get("checkpoint_upsert_execute_seconds", 0.0) or 0.0)
            + (time.perf_counter() - checkpoint_upsert_t0)
        )
        self._perf_stats["batch_checkpoint_wall_seconds"] = (
            float(self._perf_stats.get("batch_checkpoint_wall_seconds", 0.0) or 0.0)
            + (time.perf_counter() - checkpoint_t0)
        )

        self.db.flush()

    def _record_global_skip_result(self, pr: ParseResult) -> None:
        with self._lock:
            self._stats["datasets_processed"] += 1
            self._stats["datasets_skipped_global"] += 1
            if pr.vector_status == "reused":
                self._stats["vectors_reused"] += 1
            else:
                self._stats["empty_vectors"] += 1
            self._global_skipped_dataset_keys.add(Path(pr.spold_path).name)
        self._update_global_skipped(1)

    def _checkpoint_status_for(self, pr: ParseResult) -> str:
        if pr.error:
            return "failed"
        if pr.global_skip or Path(pr.spold_path).name in self._global_skipped_dataset_keys:
            return "skipped_global"
        if pr.dataset:
            return "imported"
        return "skipped"

    def _batch_resolve_flow_keys(self, logical_keys: set[tuple[str, str, str, str, str]]) -> int:
        """Batch-resolve ``LciBiosphereFlowKey`` records for logical flow keys.

        Called once per batch in the writer thread.  Creates or fetches
        flow key records in a single pass, then updates the job-level
        ``_flow_key_cache``.
        """
        from .models import LciBiosphereFlowKey

        if not logical_keys:
            return 0

        t0 = time.perf_counter()

        # Check cache hits first
        cached_hits = 0
        uncached_keys = set()
        for key in logical_keys:
            if key in self._flow_key_cache:
                cached_hits += 1
            else:
                uncached_keys.add(key)

        self._perf_stats["flow_key_cache_hits"] = (
            int(self._perf_stats.get("flow_key_cache_hits", 0) or 0) + cached_hits
        )

        if not uncached_keys:
            # All hits — record python-only time
            elapsed = time.perf_counter() - t0
            self._perf_stats["flow_resolve_python_seconds"] = (
                float(self._perf_stats.get("flow_resolve_python_seconds", 0.0) or 0.0) + elapsed
            )
            return 0

        uuids_list = sorted({key[0] for key in uncached_keys})
        fetch_t = time.perf_counter()
        existing = (
            self.db.query(LciBiosphereFlowKey)
            .filter(LciBiosphereFlowKey.flow_uuid.in_(uuids_list))
            .all()
        )
        fetch_elapsed = time.perf_counter() - fetch_t
        self._perf_stats["flow_resolve_sql_seconds"] = (
            float(self._perf_stats.get("flow_resolve_sql_seconds", 0.0) or 0.0) + fetch_elapsed
        )
        new_items: list[LciBiosphereFlowKey] = []
        existing_by_key: dict[tuple[str, str, str, str, str], int] = {}
        for row in existing:
            key = (
                row.flow_uuid,
                row.compartment or "",
                row.subcompartment or "",
                row.direction,
                row.canonical_unit,
            )
            existing_by_key[key] = int(row.flow_key_id)

        for logical_key in sorted(uncached_keys):
            if logical_key in existing_by_key:
                continue
            flow_uuid, compartment, subcompartment, direction, canonical_unit = logical_key
            item = LciBiosphereFlowKey(
                flow_uuid=flow_uuid,
                compartment=compartment,
                subcompartment=subcompartment,
                direction=direction,
                canonical_unit=canonical_unit,
                source=self.package_version,
                source_package_version=self.package_version,
            )
            new_items.append(item)

        if new_items:
            exec_t = time.perf_counter()
            self.db.add_all(new_items)
            self.db.flush()
            exec_elapsed = time.perf_counter() - exec_t
            self._perf_stats["db_execute_seconds"] = (
                float(self._perf_stats.get("db_execute_seconds", 0.0) or 0.0) + exec_elapsed
            )
            for item in new_items:
                key = (
                    item.flow_uuid,
                    item.compartment or "",
                    item.subcompartment or "",
                    item.direction,
                    item.canonical_unit,
                )
                existing_by_key[key] = int(item.flow_key_id)

        # Update job-level cache with all resolved keys
        self._flow_key_cache.update(existing_by_key)

        elapsed = time.perf_counter() - t0
        self._perf_stats["flow_key_resolve_query_seconds"] = (
            float(self._perf_stats.get("flow_key_resolve_query_seconds", 0.0) or 0.0) + elapsed
        )
        self._perf_stats["flow_key_created"] = (
            int(self._perf_stats.get("flow_key_created", 0) or 0) + len(new_items)
        )
        return len(new_items)

    def _get_flow_key_id(
        self,
        *,
        flow_uuid: str,
        compartment: str,
        subcompartment: str,
        direction: str,
        canonical_unit: str,
    ) -> int | None:
        """Resolve a single logical flow key → flow_key_id (single query).

        Falls back to creating the record if not found.
        """
        from .models import LciBiosphereFlowKey

        key = (flow_uuid, compartment, subcompartment, direction, canonical_unit)
        cached = self._flow_key_cache.get(key)
        if cached is not None:
            return cached

        row = (
            self.db.query(LciBiosphereFlowKey)
            .filter(
                LciBiosphereFlowKey.flow_uuid == flow_uuid,
                LciBiosphereFlowKey.compartment == compartment,
                LciBiosphereFlowKey.subcompartment == subcompartment,
                LciBiosphereFlowKey.direction == direction,
                LciBiosphereFlowKey.canonical_unit == canonical_unit,
            )
            .first()
        )
        if row is not None:
            fid = int(row.flow_key_id)
            self._flow_key_cache[key] = fid
            return fid

        # Create new record
        item = LciBiosphereFlowKey(
            flow_uuid=flow_uuid,
            compartment=compartment,
            subcompartment=subcompartment,
            direction=direction,
            canonical_unit=canonical_unit,
            source=self.package_version,
            source_package_version=self.package_version,
        )
        self.db.add(item)
        self.db.flush()
        fid = int(item.flow_key_id)
        self._flow_key_cache[key] = fid
        return fid

    def _apply_missing_flow_warning(self, pr: ParseResult) -> None:
        elem_flow_lookup = self._elem_flow_lookup
        missing_refs = []
        if elem_flow_lookup:
            for exc in pr.exchanges:
                if exc.exchange_id and exc.exchange_id not in elem_flow_lookup:
                    missing_refs.append(exc.exchange_id)
        if not missing_refs:
            return
        unique_missing = sorted(set(missing_refs))
        shown = ", ".join(unique_missing[:5])
        if len(unique_missing) > 5:
            shown += f", ... (+{len(unique_missing) - 5} more)"
        pr.warning = f"missing elementary flow metadata refs: {shown}"
        with self._lock:
            self._vector_warnings.append(f"{Path(pr.spold_path).name}: {pr.warning}")

    def _build_lci_process_json(self, pr: ParseResult) -> dict:
        ds = pr.dataset
        # Prefer write_plan for accurate exchange_count (write plan uses
        # the original parsed count, not pr.exchanges which is kept for compat)
        wp = getattr(pr, 'write_plan', None)
        exchange_count = wp.process_json.get("exchange_count", len(pr.exchanges)) if wp else len(pr.exchanges)
        return {
            "process_uuid": pr.process_uuid,
            "activity_id": ds.activity_id,
            "process_name": ds.activity_name,
            "location": ds.location,
            "reference_product": ds.reference_product_name,
            "reference_product_id": ds.reference_product_id,
            "reference_product_unit": ds.reference_product_unit,
            "reference_product_amount": ds.reference_product_amount,
            "exchange_count": exchange_count,
            "source": "ecoinvent_3.11",
        }

    def _pack_vector_exchanges(self, process_uuid: str, exchanges: list[_VectorExchange]) -> _PackedVector:
        from .ingest_ecoinvent import (
            _canonicalize_lci_exchange_unit_cached,
            _get_or_create_lci_flow_key_cached,
        )
        from .lci_vector_codec import pack_lci_vector

        agg: dict[int, float] = {}
        canonicalized = True
        warnings: list[str] = []
        for ex in exchanges:
            amount, canonical_unit, unit_ok = _canonicalize_lci_exchange_unit_cached(
                float(ex.amount), ex.unit, self._unit_conversion_cache
            )
            if not unit_ok:
                canonicalized = False
                warnings.append(
                    f"Missing unit conversion for process={process_uuid} flow={ex.flow_uuid} unit={ex.unit}"
                )
            flow_key_id = _get_or_create_lci_flow_key_cached(
                self.db,
                flow_uuid=ex.flow_uuid,
                direction=ex.direction,
                canonical_unit=canonical_unit,
                package_version=self.package_version,
                flow_key_cache=self._flow_key_cache,
                flow_metadata_cache=self._flow_metadata_cache,
            )
            agg[flow_key_id] = agg.get(flow_key_id, 0.0) + amount

        t_sort = time.perf_counter()
        flow_key_ids = sorted(agg)
        sort_elapsed = time.perf_counter() - t_sort
        amounts = [agg[key] for key in flow_key_ids]
        from .lci_vector_codec import pack_lci_vector
        t_compress = time.perf_counter()
        compression_level = int(getattr(self, "_vector_compression_level", self.DEFAULT_VECTOR_COMPRESSION_LEVEL))
        packed = pack_lci_vector(flow_key_ids, amounts, compression_level=compression_level)
        compress_elapsed = time.perf_counter() - t_compress
        self._perf_stats["pack_sort_seconds"] = (
            float(self._perf_stats.get("pack_sort_seconds", 0.0) or 0.0) + sort_elapsed
        )
        self._perf_stats["pack_compress_seconds"] = (
            float(self._perf_stats.get("pack_compress_seconds", 0.0) or 0.0) + compress_elapsed
        )
        return _PackedVector(
            nnz=packed.nnz,
            flow_key_ids_blob=packed.flow_key_ids_blob,
            amounts_blob=packed.amounts_blob,
            index_dtype=packed.index_dtype,
            amount_dtype=packed.amount_dtype,
            compression=packed.compression,
            checksum=packed.checksum,
            canonicalized=canonicalized,
            compressed_bytes=len(packed.flow_key_ids_blob) + len(packed.amounts_blob),
            warnings=warnings,
        )

    def _new_lci_process_vector(self, process_uuid: str, packed: _PackedVector) -> LciProcessVector:
        return LciProcessVector(
            process_uuid=process_uuid,
            dataset_level="linked_lci",
            system_model=None,
            nnz=packed.nnz,
            axis_id=None,
            flow_key_ids_blob=packed.flow_key_ids_blob,
            amounts_blob=packed.amounts_blob,
            index_dtype=packed.index_dtype,
            amount_dtype=packed.amount_dtype,
            compression=packed.compression,
            canonicalized=packed.canonicalized,
            checksum=packed.checksum,
            source=self.package_version,
            source_package_version=self.package_version,
        )

    def _update_lci_process_vector(self, row: LciProcessVector, packed: _PackedVector) -> None:
        row.dataset_level = "linked_lci"
        row.nnz = packed.nnz
        row.axis_id = None
        row.flow_key_ids_blob = packed.flow_key_ids_blob
        row.amounts_blob = packed.amounts_blob
        row.index_dtype = packed.index_dtype
        row.amount_dtype = packed.amount_dtype
        row.compression = packed.compression
        row.canonicalized = packed.canonicalized
        row.checksum = packed.checksum
        row.source = self.package_version
        row.source_package_version = self.package_version

    def _flush_single(self, pr: ParseResult) -> None:
        """Flush one parse result to the DB (called by main thread)."""
        if pr.error:
            pr.vector_status = "failed"
            pr.vector_nnz = 0
            with self._lock:
                self._stats["datasets_processed"] += 1
                self._stats["processes_failed"] += 1
                self._failed_datasets.append(pr.spold_path)
            return

        if pr.dataset is None:
            return

        if pr.global_skip:
            pr.vector_status = pr.vector_status or ("reused" if (pr.reused_vector_nnz or 0) > 0 else "empty")
            pr.vector_nnz = int(pr.reused_vector_nnz or pr.vector_nnz or 0)
            if pr.reused_process_uuid:
                pr.process_uuid = pr.reused_process_uuid
            with self._lock:
                self._stats["datasets_processed"] += 1
                self._stats["datasets_skipped_global"] += 1
                if pr.vector_status == "reused":
                    self._stats["vectors_reused"] += 1
                else:
                    self._stats["empty_vectors"] += 1
                self._global_skipped_dataset_keys.add(Path(pr.spold_path).name)
            self._update_global_skipped(1)
            return

        ds = pr.dataset
        procs = pr.process_uuid
        exs = pr.exchanges

        # ── Two-stage dedup: global → process/vector write ──
        # Build dataset_uuid = activity_id + ":" + reference_product_id (fallback: activity_id)
        dataset_uuid = pr.dataset_uuid or self._dataset_uuid_for(ds)
        pr.dataset_uuid = dataset_uuid

        if not self.overwrite_existing:
            global_rec = self._get_global_dataset_status(dataset_uuid)
            if global_rec is not None and global_rec.status == "imported":
                # Dataset already imported globally — skip without writing
                existing_vector = None
                if global_rec.process_uuid:
                    try:
                        existing_vector = self.db.get(LciProcessVector, global_rec.process_uuid)
                    except Exception:
                        self.db.rollback()
                        existing_vector = None
                reused_nnz = (
                    existing_vector.nnz
                    if existing_vector is not None
                    else (global_rec.vector_nnz or 0)
                )
                pr.vector_status = "reused" if reused_nnz > 0 else "empty"
                pr.vector_nnz = reused_nnz
                with self._lock:
                    self._stats["datasets_processed"] += 1
                    self._stats["datasets_skipped_global"] += 1
                    if pr.vector_status == "reused":
                        self._stats["vectors_reused"] += 1
                    else:
                        self._stats["empty_vectors"] += 1
                    self._global_skipped_dataset_keys.add(Path(pr.spold_path).name)
                # Update global skipped counter (includes stats_json sync)
                self._update_global_skipped(1)
                return

        # Check for unresolved elementary flow metadata. Do not fail the whole
        # dataset: ecoinvent LCI can reference flows that are absent from the
        # current flow_catalog view because of source-space UUID collisions.
        elem_flow_lookup = self._elem_flow_lookup
        missing_refs = []
        if elem_flow_lookup:
            for exc in exs:
                if exc.exchange_id and exc.exchange_id not in elem_flow_lookup:
                    missing_refs.append(exc.exchange_id)

        if missing_refs:
            unique_missing = sorted(set(missing_refs))
            shown = ", ".join(unique_missing[:5])
            if len(unique_missing) > 5:
                shown += f", ... (+{len(unique_missing) - 5} more)"
            pr.warning = f"missing elementary flow metadata refs: {shown}"
            with self._lock:
                self._vector_warnings.append(
                    f"{Path(pr.spold_path).name}: {pr.warning}"
                )

        # Build process_json
        process_json = {
            "process_uuid": procs,
            "activity_id": ds.activity_id,
            "process_name": ds.activity_name,
            "location": ds.location,
            "reference_product": ds.reference_product_name,
            "reference_product_id": ds.reference_product_id,
            "reference_product_unit": ds.reference_product_unit,
            "reference_product_amount": ds.reference_product_amount,
            "exchange_count": len(exs),
            "source": "ecoinvent_3.11",
        }

        # Upsert ReferenceProcess
        from .models import ReferenceProcess
        existing = self.db.query(ReferenceProcess).filter(
            ReferenceProcess.process_uuid == procs
        ).first()
        if existing is None:
            self.db.add(
                ReferenceProcess(
                    process_uuid=procs,
                    process_name=ds.activity_name or procs,
                    process_name_en=ds.activity_name,
                    process_type="lci_dataset",
                    reference_flow_uuid=None,  # TODO: resolve
                    process_json=process_json,
                    source_file=pr.spold_path,
                    import_mode="ecoinvent_ef31_lci",
                    import_report_json={"package_version": self.package_version},
                )
            )
            with self._lock:
                self._stats["processes_inserted"] += 1
        else:
            existing.process_name = ds.activity_name or procs
            existing.process_name_en = ds.activity_name
            existing.process_type = "lci_dataset"
            existing.process_json = process_json
            existing.source_file = pr.spold_path
            with self._lock:
                self._stats["processes_updated"] += 1

        # Write compressed vector
        from . import ecoinvent_ef31_loader as _l
        from .ef31_db_service import _generate_lci_process_uuid
        matrix_rows = []
        for ex in exs:
            if not ex.exchange_id or ex.amount == 0:
                continue
            matrix_rows.append(
                LciExchangeMatrix(
                    process_uuid=procs,
                    flow_uuid=ex.exchange_id,
                    amount=ex.amount,
                    unit=ex.unit,
                    direction=ex.direction,
                    source="ecoinvent_3.11",
                    source_package_version="ecoinvent_3.11",
                )
            )

        nnz_written = 0
        if matrix_rows:
            # Write vector (caches already built in _load_master_data)
            result = self._write_process_vector(
                procs, matrix_rows, is_intermediate=False
            )
            if result:
                nnz_written = result.get("nnz", 0)
                pr.vector_status = "written"
                pr.vector_nnz = nnz_written
                with self._lock:
                    self._stats["vectors_written"] += result.get("vectors_written", 0)
                    self._stats["nnz_total"] += result.get("nnz", 0)
                    self._vector_warnings.extend(result.get("warnings", []))
        if pr.vector_status is None:
            existing_vector = self.db.get(LciProcessVector, procs)
            if existing_vector is not None and not self.overwrite_existing:
                pr.vector_status = "reused"
                pr.vector_nnz = existing_vector.nnz or 0
                with self._lock:
                    self._stats["vectors_reused"] += 1
            else:
                pr.vector_status = "empty"
                pr.vector_nnz = 0
                with self._lock:
                    self._stats["empty_vectors"] += 1
                    if self.overwrite_existing:
                        self._vector_warnings.append(
                            f"{Path(pr.spold_path).name}: no elementary vector rows"
                        )
        nnz_written = pr.vector_nnz or 0

        # ── Update global import status on success ──
        self._update_global_status(
            dataset_uuid,
            "imported",
            pr.spold_path,
            procs,
            nnz=nnz_written,
            error_msg=pr.warning,
            activity_id=ds.activity_id,
            reference_product_id=ds.reference_product_id,
        )
        self._global_import_cache[dataset_uuid] = {
            "status": "imported",
            "process_uuid": procs,
            "vector_nnz": nnz_written,
        }
        with self._lock:
            self._stats["datasets_processed"] += 1

    # ── Global dataset import state helpers ────────────────────────────

    def _get_global_dataset_status(self, dataset_uuid: str):
        """Query global dedup state. Returns row or None."""
        from .models import GlobalDatasetImport
        try:
            return (
                self.db.query(GlobalDatasetImport)
                .filter(
                    GlobalDatasetImport.source_package_version == self.package_version,
                    GlobalDatasetImport.dataset_uuid == dataset_uuid,
                )
                .first()
            )
        except Exception:
            return None

    def _load_global_import_cache(self) -> None:
        from .models import GlobalDatasetImport

        if self.overwrite_existing:
            self._global_import_cache = {}
            return
        try:
            rows = (
                self.db.query(
                    GlobalDatasetImport.dataset_uuid,
                    GlobalDatasetImport.status,
                    GlobalDatasetImport.process_uuid,
                    GlobalDatasetImport.vector_nnz,
                )
                .filter(
                    GlobalDatasetImport.source_package_version == self.package_version,
                    GlobalDatasetImport.status == "imported",
                )
                .all()
            )
            self._global_import_cache = {
                str(row.dataset_uuid): {
                    "status": row.status,
                    "process_uuid": row.process_uuid,
                    "vector_nnz": int(row.vector_nnz or 0),
                }
                for row in rows
                if row.dataset_uuid
            }
        except Exception:
            self._global_import_cache = {}

    def _dataset_uuid_for(self, dataset: object) -> str:
        activity_id = str(getattr(dataset, "activity_id", "") or "")
        ref_product_id = str(getattr(dataset, "reference_product_id", "") or "")
        return f"{activity_id}:{ref_product_id}" if ref_product_id else activity_id

    def _update_global_status(
        self,
        dataset_uuid: str,
        status: str,
        filename: str,
        process_uuid: str,
        nnz: int = 0,
        error_msg: str | None = None,
        activity_id: str | None = None,
        reference_product_id: str | None = None,
    ) -> None:
        """Upsert global dataset import state."""
        from .models import GlobalDatasetImport
        try:
            row = (
                self.db.query(GlobalDatasetImport)
                .filter(
                    GlobalDatasetImport.source_package_version == self.package_version,
                    GlobalDatasetImport.dataset_uuid == dataset_uuid,
                )
                .first()
            )
            if row is None:
                row = GlobalDatasetImport(
                    source_package_version=self.package_version,
                    dataset_uuid=dataset_uuid,
                    status=status,
                )
                self.db.add(row)
            row.status = status
            row.dataset_filename = filename
            row.process_uuid = process_uuid
            row.activity_id = activity_id
            row.reference_product_id = reference_product_id
            row.vector_nnz = nnz
            row.last_job_id = self.job_id
            row.error_message = error_msg
            if status == "imported":
                row.imported_at = __import__("datetime").datetime.utcnow()
            self.db.flush()
        except Exception:
            # Non-critical: global dedup failure should not break the import
            pass

    def _update_global_skipped(self, count: int) -> None:
        """Increment the in-memory skipped_global counter.

        The DB row is updated by _update_progress at batch boundaries to
        avoid one write lock per skipped dataset.
        """
        with self._lock:
            if not hasattr(self, '_skipped_global_counter'):
                self._skipped_global_counter = 0
            self._skipped_global_counter += count

    def _ensure_flow_exists(self, flow, is_intermediate: bool = False) -> None:
        """Ensure FlowRecord exists — simplified inline version."""
        from .models import FlowRecord
        flow_uuid = getattr(flow, "flow_uuid", None)
        if not flow_uuid:
            return
        existing = self.db.query(FlowRecord).filter(
            FlowRecord.flow_uuid == flow_uuid
        ).first()
        if existing:
            return
        self.db.add(
            FlowRecord(
                flow_uuid=flow_uuid,
                flow_name=getattr(flow, "flow_name", flow_uuid),
                flow_name_en=getattr(flow, "flow_name_en", ""),
                flow_type="Product flow" if is_intermediate else "Elementary flow",
                default_unit=getattr(flow, "default_unit", "kg"),
                unit_group=getattr(flow, "unit_group", None),
                compartment=getattr(flow, "compartment", None),
                source="ef3.1",
                is_custom=False,
            )
        )

    def _write_process_vector(self, process_uuid: str, exchanges: list, is_intermediate: bool = False) -> dict | None:
        """Write compressed vector using the canonical ecoinvent vector helper.

        Passes pre-built job-level caches (unit conversion, flow keys,
        flow metadata) so each process does NOT rebuild them from DB.
        """
        if not exchanges:
            return None
        from .ingest_ecoinvent import write_ecoinvent_process_vector

        return write_ecoinvent_process_vector(
            self.db,
            process_uuid=process_uuid,
            exchanges=exchanges,
            package_version="ecoinvent_3.11",
            dataset_level="linked_lci",
            unit_conversion_cache=self._unit_conversion_cache,
            flow_key_cache=self._flow_key_cache,
            flow_metadata_cache=self._flow_metadata_cache,
            commit=False,
        )

    # ── Job state helpers ──────────────────────────────────────────────

    def _check_control_signals(self) -> tuple[bool, bool]:
        """Check file-based control signals and in-memory state.

        Returns (pause_requested, cancel_requested).
        Signal files take priority; they survive DB lock.
        """
        try:
            from .job_control import read_job_control_signal

            signal = read_job_control_signal(self.job_id)
            if signal:
                cancel = bool(signal.get("cancel_requested", False))
                pause = bool(signal.get("pause_requested", False))
                if cancel:
                    self._cancel_requested = True
                if pause:
                    self._paused = True
                    self._pause_event.set()
                return pause, cancel
        except Exception:
            pass
        # Fallback to in-memory state
        return self._paused, self._cancel_requested

    def _get_requested_status(self) -> str | None:
        try:
            job = self.db.query(ImportJob).filter(
                ImportJob.job_id == self.job_id
            ).first()
            return job.status if job else None
        except Exception:
            return None

    def _update_progress(self) -> None:
        """Update job progress in DB after each dataset.

        Synchronises: progress_pct, stats_json (inserted/skipped/failed
        vectors nnz skipped_global).
        """
        try:
            job = self.db.query(ImportJob).filter(
                ImportJob.job_id == self.job_id
            ).first()
            if job:
                total = self._total_count
                with self._lock:
                    datasets_processed = self._stats["datasets_processed"]
                    inserted = self._stats["processes_inserted"]
                    updated = self._stats["processes_updated"]
                    skipped_global = self._stats["datasets_skipped_global"]
                    failed = self._stats["processes_failed"]
                    vectors_written = self._stats["vectors_written"]
                    vectors_reused = self._stats["vectors_reused"]
                    empty_vectors = self._stats["empty_vectors"]
                    nnz_total = self._stats["nnz_total"]

                if total and total > 0:
                    job.progress_pct = round(min(100.0, datasets_processed / total * 100), 1)
                job.skipped_global = skipped_global

                job.stats_json = {
                    "datasets_processed": datasets_processed,
                    "processes_inserted": inserted,
                    "processes_updated": updated,
                    "processes_skipped": skipped_global,
                    "datasets_skipped_global": skipped_global,
                    "processes_failed": failed,
                    "skipped_global": skipped_global,
                    "vectors_written": vectors_written,
                    "vectors_reused": vectors_reused,
                    "empty_vectors": empty_vectors,
                    "vector_nnz_total": nnz_total,
                    **self._build_performance_stats(),
                }
                job.updated_at = __import__("datetime").datetime.utcnow()
                self.db.flush()
        except Exception:
            pass  # Non-critical

    def _update_job_phase(self, phase: str) -> None:
        try:
            job = self.db.query(ImportJob).filter(
                ImportJob.job_id == self.job_id
            ).first()
            if job:
                job.phase = phase
                job.stats_json = {**(job.stats_json or {}), "phase": phase}
                job.updated_at = __import__("datetime").datetime.utcnow()
                self.db.flush()
        except Exception:
            pass

    def _update_job_status(self, status: str) -> None:
        try:
            job = self.db.query(ImportJob).filter(
                ImportJob.job_id == self.job_id
            ).first()
            if job:
                job.status = status
                job.updated_at = __import__("datetime").datetime.utcnow()
                self.db.flush()
        except Exception:
            pass

    def _finish_result(self, start_time: float, result: ImportResult) -> ImportResult:
        result.duration_seconds = round(time.time() - start_time, 3)
        if self._error_summary:
            result.error_summary = self._error_summary

        # Clear control signal file on completion
        try:
            from .job_control import clear_job_control_signal
            clear_job_control_signal(self.job_id)
        except Exception:
            pass

        logger.info(
            f"[{self.job_id}] Import complete: "
            f"processed={result.datasets_processed} inserted={result.processes_inserted} "
            f"updated={result.processes_updated} global_skipped={result.datasets_skipped_global} "
            f"failed={result.processes_failed} vectors_written={result.vectors_written} "
            f"vectors_reused={result.vectors_reused} empty_vectors={result.empty_vectors} "
            f"nnz={result.vector_nnz_total} duration={result.duration_seconds}s"
        )
        return result
