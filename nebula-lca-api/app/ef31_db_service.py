"""DB dry-run/commit service for EF 3.1 LCI imports.

This module provides a pure-SQLAlchemy service layer for writing
LCI dataset data into the core tables:
- FlowRecord: elementary/product/waste flows
- UnitDefinition: units and conversions
- ReferenceProcess: LCI datasets (process_type="lci_dataset")

Default mode is dry-run: returns counts/warnings/errors without touching the DB.
Commit requires explicit confirm=true.
"""

import logging
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from . import database as _database
from .models import FlowRecord, UnitDefinition, ReferenceProcess
from .ecoinvent_ef31_loader import (
    ElementaryFlow,
    IntermediateFlow,
    UnitRecord,
    UnitConversion,
    LCIDataset,
    LCIElementaryExchange,
    FoundationReport,
)

logger = logging.getLogger(__name__)


@dataclass
class DbDryRunResult:
    """Result returned by dry-run mode — no DB mutation."""
    flows_new: int = 0
    flows_skipped: int = 0
    flows_error: int = 0
    units_new: int = 0
    units_skipped: int = 0
    units_error: int = 0
    processes_new: int = 0
    processes_skipped: int = 0
    processes_error: int = 0
    warnings: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "flows": {
                "new": self.flows_new,
                "skipped": self.flows_skipped,
                "error": self.flows_error,
            },
            "units": {
                "new": self.units_new,
                "skipped": self.units_skipped,
                "error": self.units_error,
            },
            "processes": {
                "new": self.processes_new,
                "skipped": self.processes_skipped,
                "error": self.processes_error,
            },
            "warnings": self.warnings,
            "errors": self.errors,
        }


@dataclass
class DbCommitResult:
    """Result returned by commit mode — after actual DB writes."""
    flows_new: int = 0
    flows_skipped: int = 0
    flows_error: int = 0
    units_new: int = 0
    units_skipped: int = 0
    units_error: int = 0
    processes_new: int = 0
    processes_skipped: int = 0
    processes_error: int = 0
    warnings: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    job_id: str = ""

    def summary(self) -> dict:
        return {
            "job_id": self.job_id,
            "flows": {
                "new": self.flows_new,
                "skipped": self.flows_skipped,
                "error": self.flows_error,
            },
            "units": {
                "new": self.units_new,
                "skipped": self.units_skipped,
                "error": self.units_error,
            },
            "processes": {
                "new": self.processes_new,
                "skipped": self.processes_skipped,
                "error": self.processes_error,
            },
            "warnings": self.warnings,
            "errors": self.errors,
        }


def _generate_lci_process_uuid(dataset: LCIDataset) -> str:
    """Generate a stable UUID for an LCI dataset.

    Strategy: activity_id:reference_product_uuid as composite key.

    P1 fix: ecoinvent activity can have multiple reference products,
    so we need activity_id + rp_id to avoid merging distinct LCI
    datasets into the same ReferenceProcess.

    Falls back to filename stem if activity_id is empty.
    """
    parts = []
    if dataset.activity_id:
        parts.append(dataset.activity_id)
    if dataset.reference_product_id:
        parts.append(dataset.reference_product_id)
    if parts:
        return ":".join(parts)
    return Path(dataset.filename).stem


def _ensure_flow_exists(
    db: Session,
    flow: ElementaryFlow,
) -> tuple[str, bool]:
    """Ensure a FlowRecord exists for an elementary flow.

    Uses the COMPLETE ElementaryFlow from MasterData (compartment,
    subcompartment, CAS, formula, unit_group) instead of a partial
    construction from exchange data alone.

    P2 fix: previously the caller built a partial ElementaryFlow with
    only uuid/name/unit, losing MasterData quality fields.

    Returns:
        (flow_uuid, was_new)
    """
    existing = db.execute(
        FlowRecord.__table__.select().where(
            FlowRecord.__table__.c.flow_uuid == flow.flow_uuid
        )
    ).first()
    if existing:
        return flow.flow_uuid, False

    db_flow = FlowRecord(
        flow_uuid=flow.flow_uuid,
        flow_name=flow.flow_name,
        flow_name_en=flow.flow_name_en,
        flow_type=flow.flow_type,
        default_unit=flow.default_unit,
        unit_group=flow.unit_group,
        compartment=flow.compartment,
        source=flow.source,
        is_custom=False,
    )
    db.add(db_flow)
    db.flush()
    return flow.flow_uuid, True


def _ensure_intermediate_flow_exists(
    db: Session,
    flow: IntermediateFlow,
) -> tuple[str, bool]:
    """Ensure a FlowRecord exists for an intermediate/product flow."""
    existing = db.execute(
        FlowRecord.__table__.select().where(
            FlowRecord.__table__.c.flow_uuid == flow.flow_uuid
        )
    ).first()
    if existing:
        return flow.flow_uuid, False

    db_flow = FlowRecord(
        flow_uuid=flow.flow_uuid,
        flow_name=flow.flow_name,
        flow_name_en=flow.flow_name_en,
        flow_type=flow.flow_type,
        default_unit=flow.default_unit,
        unit_group=flow.unit_group,
        source="ef3.1",
        is_custom=False,
    )
    db.add(db_flow)
    db.flush()
    return flow.flow_uuid, True


def _ensure_unit_exists(
    db: Session,
    unit: UnitRecord,
) -> tuple[int, bool]:
    """Ensure a UnitDefinition entry exists.

    P2 fix: previously wrote all units into unit_group='ef3.1' with
    factor_to_reference=1.0, polluting the unit catalog.

    This version only creates entries for known mass/volume/energy units
    and does NOT consume UnitConversions.xml.  It is intentionally
    conservative — full unit-group/conversion support is deferred to
    a future stage.
    """
    unit_name = unit.name
    if not unit_name:
        return 0, False

    # Only recognize a handful of base units with sensible mapping
    KNOWN_BASE_UNITS = {
        "kilogram": ("mass", True),
        "gram": ("mass", False),
        "joule": ("energy", False),
        "cubic meter": ("volume", False),
        "litre": ("volume", False),
        "kilowatt hour": ("energy", False),
    }

    name_lower = unit_name.lower()
    if name_lower not in {k.lower(): v for k, v in KNOWN_BASE_UNITS.items()}:
        # Skip unknown units — don't pollute unit_definitions
        return 0, False

    unit_group, is_reference = KNOWN_BASE_UNITS[name_lower]

    existing = db.execute(
        UnitDefinition.__table__.select().where(
            UnitDefinition.__table__.c.unit_group == unit_group,
            UnitDefinition.__table__.c.unit_name == unit_name,
        )
    ).first()
    if existing:
        return existing.id, False

    # Convert factor_to_reference to float
    factor = 1.0
    if unit_name.lower() == "gram":
        factor = 0.001
    elif unit_name.lower() == "kilowatt hour":
        factor = 3600000.0

    db_unit = UnitDefinition(
        unit_group=unit_group,
        unit_name=unit_name,
        factor_to_reference=factor,
        is_reference=is_reference,
    )
    db.add(db_unit)
    db.flush()
    return db_unit.id, True


def _build_elementary_exchanges_json(
    exchanges: list[LCIElementaryExchange],
    elem_flows: list[ElementaryFlow],
) -> list[dict]:
    """Build process_json.exchanges from elementary exchanges with flow lookup.

    Enriches each exchange with flow_uuid, name, compartment from MasterData.
    """
    flow_lookup = {
        f.flow_uuid: f for f in elem_flows
    }
    result = []
    for exc in exchanges:
        flow_ref = flow_lookup.get(exc.exchange_id)
        exchange_entry = {
            "exchange_id": exc.exchange_id,
            "flow_uuid": exc.exchange_id if exc.exchange_id else None,
            "name": exc.exchange_name,
            "compartment": flow_ref.compartment if flow_ref else None,
            "subcompartment": flow_ref.subcompartment if flow_ref else None,
            "unit": exc.unit,
            "amount": exc.amount,
            "direction": exc.direction,
        }
        result.append(exchange_entry)
    return result


def dry_run_lci_import(
    datasets: list[LCIDataset],
    exchanges_map: dict[str, list[LCIElementaryExchange]],
    elementary_flows: list[ElementaryFlow],
    intermediate_flows: list[IntermediateFlow] | None = None,
    units: dict[str, UnitRecord] | None = None,
    db: Session | None = None,
) -> DbDryRunResult:
    """Dry-run LCI import: count what would be written without touching DB.

    Args:
        datasets: LCI dataset list from parse_spold_file.
        exchanges_map: {filename: [elementary exchanges]}.
        elementary_flows: MasterData ElementaryExchanges records.
        intermediate_flows: MasterData IntermediateExchanges records (optional).
        units: MasterData units (optional).
        db: Optional Session. If None, opens one automatically (read-only).

    Returns:
        DbDryRunResult with counts and any warnings/errors.
    """
    result = DbDryRunResult()
    close_on_exit = False

    if db is None:
        db = _database.SessionLocal()
        close_on_exit = True

    try:
        # Build lookup sets
        flow_rows = db.execute(FlowRecord.__table__.select()).all()
        existing_flow_uuids = {row.flow_uuid for row in flow_rows}

        # Build flow lookup from MasterData
        elem_flow_lookup = {f.flow_uuid: f for f in elementary_flows}

        for dataset in datasets:
            proc_uuid = _generate_lci_process_uuid(dataset)

            # Check if process already exists
            proc_exists = db.execute(
                ReferenceProcess.__table__.select().where(
                    ReferenceProcess.__table__.c.process_uuid == proc_uuid
                )
            ).first() is not None
            if proc_exists:
                result.processes_skipped += 1
                continue

            result.processes_new += 1

            # Check for missing elementary flow refs (P1: block datasets with missing refs)
            file_exchanges = exchanges_map.get(dataset.filename, [])
            for exc in file_exchanges:
                if exc.exchange_id and exc.exchange_id not in elem_flow_lookup:
                    result.errors.append(
                        f"Missing elementary flow ref: {exc.exchange_id} "
                        f"in {dataset.filename} (activity {dataset.activity_id})"
                    )
                    result.flows_error += 1

        # Count intermediate flows
        if intermediate_flows:
            for flow in intermediate_flows:
                if flow.flow_uuid not in existing_flow_uuids:
                    result.flows_new += 1
                else:
                    result.flows_skipped += 1
    finally:
        if close_on_exit:
            db.close()

    return result


def commit_lci_import(
    datasets: list[LCIDataset],
    exchanges_map: dict[str, list[LCIElementaryExchange]],
    elementary_flows: list[ElementaryFlow],
    intermediate_flows: list[IntermediateFlow] | None = None,
    units: dict[str, UnitRecord] | None = None,
    limit: int | None = None,
    db: Session | None = None,
) -> DbCommitResult:
    """Commit LCI import to DB.

    P1 fix: missing elementary flow refs now block the dataset (not just warn).
    P2 fix: FlowRecord insert now uses MasterData ElementaryFlow for full fields.
    Stage 1: accepts optional db session so the API layer can control the
    session lifecycle (DI from FastAPI).  If db=None, opens SessionLocal
    automatically (for tests / CLI usage).

    Args:
        datasets: LCI dataset list from parse_spold_file.
        exchanges_map: {filename: [elementary exchanges]}.
        elementary_flows: MasterData ElementaryExchanges records.
        intermediate_flows: MasterData IntermediateExchanges records (optional).
        units: MasterData units (optional).
        limit: Max datasets to commit (None = all).
        db: Optional Session. If None, opens SessionLocal automatically.

    Returns:
        DbCommitResult with actual write counts.
    """
    job_id = str(uuid.uuid4())
    result = DbCommitResult(job_id=job_id)

    if units is None:
        units = {}

    close_on_exit = False
    if db is None:
        db = _database.SessionLocal()
        close_on_exit = True

    try:
        # Build MasterData lookup for full flow data
        elem_flow_lookup = {f.flow_uuid: f for f in elementary_flows}

        count = 0
        for dataset in datasets:
            if limit and count >= limit:
                break

            proc_uuid = _generate_lci_process_uuid(dataset)

            # Skip if process already exists
            existing = db.execute(
                ReferenceProcess.__table__.select().where(
                    ReferenceProcess.__table__.c.process_uuid == proc_uuid
                )
            ).first()
            if existing:
                result.processes_skipped += 1
                continue

            file_exchanges = exchanges_map.get(dataset.filename, [])

            # P1 fix: pre-check ALL exchanges for missing refs.
            # If ANY exchange is missing its MasterData flow ref,
            # block this dataset — don't create a half-broken process.
            missing_refs = []
            for exc in file_exchanges:
                if exc.exchange_id and exc.exchange_id not in elem_flow_lookup:
                    missing_refs.append(exc.exchange_id)

            if missing_refs:
                result.processes_skipped += 1
                for ref_id in missing_refs:
                    result.errors.append(
                        f"Missing elementary flow ref: {ref_id} "
                        f"in {dataset.filename} (activity {dataset.activity_id}). "
                        f"Dataset SKIPPED."
                    )
                continue

            # All refs found — proceed with FlowRecord insertion.
            # P2 fix: use the complete MasterData ElementaryFlow.
            flow_ok = True
            for exc in file_exchanges:
                try:
                    full_flow = elem_flow_lookup[exc.exchange_id]
                    _, was_new = _ensure_flow_exists(db, full_flow)
                    if was_new:
                        result.flows_new += 1
                    else:
                        result.flows_skipped += 1
                except Exception as e:
                    result.flows_error += 1
                    result.errors.append(
                        f"FlowRecord insert error for {exc.exchange_id}: {e}"
                    )
                    flow_ok = False
                    # Skip this dataset but continue with next datasets
                    break

            if not flow_ok:
                continue

            # Ensure units exist (conservative: only known base units)
            seen_units = set()
            for exc in file_exchanges:
                if exc.unit and exc.unit not in seen_units:
                    seen_units.add(exc.unit)
                    try:
                        _, was_new = _ensure_unit_exists(db, UnitRecord(
                            unit_id="",
                            name=exc.unit,
                        ))
                        if was_new:
                            result.units_new += 1
                        else:
                            result.units_skipped += 1
                    except Exception:
                        pass

            # Build process_json
            exchanges_json = _build_elementary_exchanges_json(
                file_exchanges, elementary_flows
            )
            process_json = {
                "reference_flow_uuid": dataset.reference_product_id or None,
                "reference_flow_internal_id": None,
                "reference_product": dataset.reference_product_name,
                "reference_product_unit": dataset.reference_product_unit,
                "reference_product_amount": dataset.reference_product_amount,
                "geography": dataset.location,
                "activity_id": dataset.activity_id,
                "activity_name": dataset.activity_name,
                "reference_product_id": dataset.reference_product_id,
                "exchanges": exchanges_json,
                "elementary_exchanges": exchanges_json,
                "exchange_count": len(file_exchanges),
                "source": "ecoinvent",
                "version": "3.11",
                "method": "cutoff_lci",
            }

            # Create ReferenceProcess
            ref_process = ReferenceProcess(
                process_uuid=proc_uuid,
                process_name=dataset.activity_name,
                process_name_en=dataset.activity_name,
                process_type="lci_dataset",
                reference_flow_uuid=dataset.reference_product_id or None,
                process_json=process_json,
                source_file=dataset.filename,
                source_process_uuid=proc_uuid,
                import_mode="ecoinvent_ef31_lci",
                import_report_json={"job_id": job_id, "status": "imported"},
            )
            db.add(ref_process)
            result.processes_new += 1
            count += 1

        # Commit intermediate flows
        if intermediate_flows:
            for flow in intermediate_flows:
                try:
                    _, was_new = _ensure_intermediate_flow_exists(db, flow)
                    if was_new:
                        result.flows_new += 1
                    else:
                        result.flows_skipped += 1
                except Exception as e:
                    result.flows_error += 1
                    result.errors.append(
                        f"Intermediate flow insert error for {flow.flow_uuid}: {e}"
                    )

        db.commit()
    except Exception as e:
        db.rollback()
        result.errors.append(f"Transaction error: {e}")
        raise
    finally:
        if close_on_exit:
            db.close()

    return result
