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

import logging
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

from sqlalchemy.orm import Session

from . import ecoinvent_ef31_loader as _loader
from .ef31_db_service import _generate_lci_process_uuid
from .models import DatasetCheckpoint, ImportJob, LciExchangeMatrix, LciProcessVector

logger = logging.getLogger(__name__)

# ── Event types passed through the parsing queue ────────────────────────────

@dataclass
class ParseResult:
    """Parsed dataset + exchanges from a single .spold file."""
    spold_path: str
    dataset: Optional[object]  # LCIDataset
    exchanges: list[object]    # list[LCIElementaryExchange]
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
            "flush_duration_ms_total": 0.0,
            "flush_result_count": 0,
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
        return units_count >= 100 and elementary_count >= 9000 and intermediate_count >= 4000

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
        """Parse a single .spold file in a worker thread."""
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

            ds = _loader.parse_spold_file(spold_path)
            if ds is None:
                return ParseResult(
                    spold_path=str(spold_path),
                    dataset=None,
                    exchanges=[],
                    process_uuid="",
                    duration_ms=int((time.time() - t0) * 1000),
                    error="parse_spold_file returned None",
                    parse_stage="metadata",
                )

            procs = _generate_lci_process_uuid(ds)
            dataset_uuid = self._dataset_uuid_for(ds)
            cached_global = self._global_import_cache.get(dataset_uuid)
            if not self.overwrite_existing and cached_global and cached_global.get("status") == "imported":
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

            exs = _loader.parse_spold_exchanges(spold_path)
            return ParseResult(
                spold_path=str(spold_path),
                dataset=ds,
                exchanges=exs or [],
                process_uuid=procs,
                duration_ms=int((time.time() - t0) * 1000),
                dataset_uuid=dataset_uuid,
                parse_stage="exchanges",
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
        while True:
            try:
                result_queue.put(result, timeout=0.5)
                self._perf_stats["queue_max_observed"] = max(
                    int(self._perf_stats.get("queue_max_observed", 0)),
                    result_queue.qsize(),
                )
                return
            except Exception:
                if self._cancel_requested:
                    return

    def _flush_parse_result_batch(self, results: list[ParseResult]) -> None:
        t0 = time.perf_counter()
        for pr in results:
            self._flush_single(pr)
        self._mark_checkpoints_complete(results)
        self._update_progress()
        self.db.commit()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        self._perf_stats["commit_count"] += 1
        self._perf_stats["flush_duration_ms_total"] += elapsed_ms
        self._perf_stats["flush_result_count"] += len(results)

    def _record_parse_result_stats(self, result: ParseResult) -> None:
        duration_ms = max(0, int(result.duration_ms or 0))
        self._perf_stats["parse_duration_ms_total"] = int(self._perf_stats.get("parse_duration_ms_total", 0) or 0) + duration_ms
        self._perf_stats["parse_result_count"] = int(self._perf_stats.get("parse_result_count", 0) or 0) + 1
        if result.parse_stage == "metadata":
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
        return {
            "parse_wall_seconds": round(float(perf.get("parse_wall_seconds", 0.0) or 0.0), 3),
            "write_wall_seconds": round(float(perf.get("write_wall_seconds", 0.0) or 0.0), 3),
            "commit_count": int(perf.get("commit_count", 0) or 0),
            "write_batch_size": int(perf.get("write_batch_size", write_batch_size) or write_batch_size),
            "queue_max_observed": int(perf.get("queue_max_observed", 0) or 0),
            "avg_parse_ms": round(avg_parse_ms, 3),
            "global_skip_fast_count": int(perf.get("global_skip_fast_count", 0) or 0),
            "metadata_parse_count": metadata_count,
            "exchange_parse_count": exchange_count,
            "avg_metadata_parse_ms": round(avg_metadata_ms, 3),
            "avg_exchange_parse_ms": round(avg_exchange_ms, 3),
            "avg_flush_ms": round(avg_flush_ms, 3),
            "masterdata_reused": bool(perf.get("masterdata_reused", False)),
            "masterdata_wall_seconds": round(float(perf.get("masterdata_wall_seconds", 0.0) or 0.0), 3),
        }

    # ── DB flush (single thread) ───────────────────────────────────────

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
