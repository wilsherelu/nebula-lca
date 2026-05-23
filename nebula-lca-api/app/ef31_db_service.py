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
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from . import database as _database
from .models import FlowRecord, UnitDefinition, ReferenceProcess, UnitGroup
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
        updates = {}
        if flow.default_unit and not existing.default_unit:
            updates["default_unit"] = flow.default_unit
        if flow.unit_group and not existing.unit_group:
            updates["unit_group"] = flow.unit_group
        if flow.compartment and not existing.compartment:
            updates["compartment"] = flow.compartment
        if updates:
            db.query(FlowRecord).filter(FlowRecord.flow_uuid == flow.flow_uuid).update(updates)
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
        updates = {}
        if flow.default_unit and not existing.default_unit:
            updates["default_unit"] = flow.default_unit
        if flow.unit_group and not existing.unit_group:
            updates["unit_group"] = flow.unit_group
        if updates:
            db.query(FlowRecord).filter(FlowRecord.flow_uuid == flow.flow_uuid).update(updates)
        return flow.flow_uuid, False

    db_flow = FlowRecord(
        flow_uuid=flow.flow_uuid,
        flow_name=flow.flow_name,
        flow_name_en=flow.flow_name_en,
        flow_type=flow.flow_type,
        default_unit=flow.default_unit,
        unit_group=flow.unit_group,
        source=flow.source,
        is_custom=False,
    )
    db.add(db_flow)
    db.flush()
    return flow.flow_uuid, True


_FALLBACK_UNIT_DEFS = {
    "kg": ("mass", 1.0, True),
    "kilogram": ("mass", 1.0, True),
    "g": ("mass", 0.001, False),
    "gram": ("mass", 0.001, False),
    "mj": ("energy", 1.0, True),
    "megajoule": ("energy", 1.0, True),
    "j": ("energy", 0.000001, False),
    "joule": ("energy", 0.000001, False),
    "kwh": ("energy", 3.6, False),
    "kilowatt hour": ("energy", 3.6, False),
    "m3": ("volume", 1.0, True),
    "cubic meter": ("volume", 1.0, True),
    "l": ("volume", 0.001, False),
    "litre": ("volume", 0.001, False),
    "kbq": ("radioactivity", 1.0, True),
}


def _unit_key(unit_name: str | None) -> str:
    return str(unit_name or "").strip().lower()


def _unit_group_key(unit_type: str | None) -> str:
    return str(unit_type or "").strip()


def _safe_factor(value: float | int | str | None) -> float | None:
    try:
        factor = float(value)
    except (TypeError, ValueError):
        return None
    if factor <= 0:
        return None
    return factor


def _build_ecoinvent_unit_catalog(
    units: dict[str, UnitRecord],
    unit_conversions: list[UnitConversion],
) -> tuple[dict[str, dict[str, float]], dict[str, str], dict[str, str]]:
    """Return factor maps from ecoinvent UnitConversions.xml.

    The conversion factor is interpreted as:
    ``amount(unitFromName) * factor = amount(unitToName)``.
    We choose one reference unit per ecoinvent unitType, then derive every
    reachable unit's factor_to_reference by graph traversal.
    """
    graph: dict[str, dict[str, list[tuple[str, float]]]] = defaultdict(lambda: defaultdict(list))
    units_by_group: dict[str, set[str]] = defaultdict(set)
    reference_votes: dict[str, Counter[str]] = defaultdict(Counter)
    from_units_by_group: dict[str, set[str]] = defaultdict(set)

    for conversion in unit_conversions:
        group = _unit_group_key(conversion.unit_type)
        unit_from = str(conversion.unit_from_name or "").strip()
        unit_to = str(conversion.unit_to_name or "").strip()
        factor = _safe_factor(conversion.factor)
        if not group or not unit_from or not unit_to or factor is None:
            continue

        graph[group][unit_from].append((unit_to, factor))
        graph[group][unit_to].append((unit_from, 1.0 / factor))
        units_by_group[group].update([unit_from, unit_to])
        reference_votes[group][unit_to] += 1
        from_units_by_group[group].add(unit_from)

    for unit in units.values():
        group = _unit_group_key(unit.unit_type)
        name = str(unit.name or "").strip()
        if group and name:
            units_by_group[group].add(name)
            reference_votes[group][name] += 1

    factor_by_group: dict[str, dict[str, float]] = {}
    reference_by_group: dict[str, str] = {}
    unit_group_by_unit: dict[str, str] = {}

    for group, unit_names in units_by_group.items():
        if not unit_names:
            continue
        terminal_votes = Counter(
            {
                unit_name: count
                for unit_name, count in reference_votes[group].items()
                if unit_name not in from_units_by_group[group]
            }
        )
        votes = terminal_votes or reference_votes[group]
        reference = votes.most_common(1)[0][0] if votes else sorted(unit_names)[0]
        reference_by_group[group] = reference

        factors: dict[str, float] = {reference: 1.0}
        queue = deque([reference])
        while queue:
            current = queue.popleft()
            current_factor = factors[current]
            for neighbor, edge_factor in graph[group].get(current, []):
                if neighbor in factors:
                    continue
                factors[neighbor] = current_factor / edge_factor
                queue.append(neighbor)

        for name in unit_names:
            if name not in factors and _unit_key(name) == _unit_key(reference):
                factors[name] = 1.0
            if name in factors:
                unit_group_by_unit[_unit_key(name)] = group

        factor_by_group[group] = factors

    return factor_by_group, reference_by_group, unit_group_by_unit


def _ensure_unit_group(db: Session, group: str, reference_unit: str) -> tuple[UnitGroup, bool]:
    source_uuid = f"ecoinvent:unit-type:{group}"
    existing = db.get(UnitGroup, group)
    if existing is not None:
        if reference_unit and not existing.reference_unit:
            existing.reference_unit = reference_unit
        if not existing.source_uuid:
            existing.source_uuid = source_uuid
        if not existing.source_version:
            existing.source_version = "ecoinvent"
        if not existing.source_package_version:
            existing.source_package_version = "ecoinvent 3.11"
        if not existing.source_file:
            existing.source_file = "MasterData/UnitConversions.xml"
        return existing, False

    row = UnitGroup(
        name=group,
        reference_unit=reference_unit,
        source_uuid=source_uuid,
        source_version="ecoinvent",
        source_package_version="ecoinvent 3.11",
        source_file="MasterData/UnitConversions.xml",
    )
    db.add(row)
    db.flush()
    return row, True


def _ensure_unit_definition(
    db: Session,
    unit_group: str,
    unit_name: str,
    factor_to_reference: float,
    is_reference: bool,
) -> tuple[int, bool]:
    existing = db.execute(
        UnitDefinition.__table__.select().where(
            UnitDefinition.__table__.c.unit_group == unit_group,
            UnitDefinition.__table__.c.unit_name == unit_name,
        )
    ).first()
    if existing:
        db.query(UnitDefinition).filter(UnitDefinition.id == existing.id).update(
            {
                "factor_to_reference": factor_to_reference,
                "is_reference": is_reference,
            }
        )
        return existing.id, False

    db_unit = UnitDefinition(
        unit_group=unit_group,
        unit_name=unit_name,
        factor_to_reference=factor_to_reference,
        is_reference=is_reference,
    )
    db.add(db_unit)
    db.flush()
    return db_unit.id, True


def _ensure_unit_exists(
    db: Session,
    unit: UnitRecord,
) -> tuple[int, bool]:
    """Fallback for minimal fixtures without UnitConversions.xml."""
    unit_name = str(unit.name or "").strip()
    fallback = _FALLBACK_UNIT_DEFS.get(_unit_key(unit_name))
    if not unit_name or fallback is None:
        return 0, False
    unit_group, factor, is_reference = fallback
    _ensure_unit_group(db, unit_group, unit_name if is_reference else "")
    return _ensure_unit_definition(db, unit_group, unit_name, factor, is_reference)


def _ensure_ecoinvent_unit_catalog(
    db: Session,
    units: dict[str, UnitRecord],
    unit_conversions: list[UnitConversion] | None,
) -> tuple[int, int, dict[str, str]]:
    """Persist the ecoinvent unit catalog and return unit-name -> group."""
    conversions = unit_conversions or []
    factor_by_group, reference_by_group, unit_group_by_unit = _build_ecoinvent_unit_catalog(units, conversions)
    units_new = 0
    units_skipped = 0

    for group, factors in factor_by_group.items():
        reference = reference_by_group.get(group, "")
        _ensure_unit_group(db, group, reference)
        for unit_name, factor in factors.items():
            _, was_new = _ensure_unit_definition(
                db,
                group,
                unit_name,
                factor,
                _unit_key(unit_name) == _unit_key(reference),
            )
            if was_new:
                units_new += 1
            else:
                units_skipped += 1

    for unit in units.values():
        unit_name = str(unit.name or "").strip()
        unit_key = _unit_key(unit_name)
        if unit_group_by_unit.get(unit_key):
            continue
        _, was_new = _ensure_unit_exists(db, unit)
        fallback = _FALLBACK_UNIT_DEFS.get(unit_key)
        if fallback is not None:
            unit_group_by_unit[unit_key] = fallback[0]
        if was_new:
            units_new += 1
        else:
            units_skipped += 1

    return units_new, units_skipped, unit_group_by_unit


def _assign_flow_unit_groups(
    flows: list[ElementaryFlow] | list[IntermediateFlow],
    unit_group_by_unit: dict[str, str],
) -> None:
    for flow in flows:
        if getattr(flow, "unit_group", None):
            continue
        unit_group = unit_group_by_unit.get(_unit_key(getattr(flow, "default_unit", "")))
        if unit_group:
            flow.unit_group = unit_group


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


def _build_reference_product_exchange_json(dataset: LCIDataset) -> dict | None:
    """Build the single product output required by an LCI dataset node."""
    if not dataset.reference_product_id:
        return None
    return {
        "exchange_id": dataset.reference_product_id,
        "exchange_internal_id": dataset.reference_product_id,
        "flow_uuid": dataset.reference_product_id,
        "flow_name": dataset.reference_product_name,
        "unit": dataset.reference_product_unit,
        "amount": dataset.reference_product_amount or 1,
        "direction": "output",
        "flow_type": "Product flow",
        "is_allocated_product": True,
        "is_reference_flow": True,
        "isProduct": True,
    }


def dry_run_lci_import(
    datasets: list[LCIDataset],
    exchanges_map: dict[str, list[LCIElementaryExchange]],
    elementary_flows: list[ElementaryFlow],
    intermediate_flows: list[IntermediateFlow] | None = None,
    units: dict[str, UnitRecord] | None = None,
    unit_conversions: list[UnitConversion] | None = None,
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
        unit_group_by_unit = _build_ecoinvent_unit_catalog(units or {}, unit_conversions or [])[2]
        _assign_flow_unit_groups(elementary_flows, unit_group_by_unit)
        if intermediate_flows:
            _assign_flow_unit_groups(intermediate_flows, unit_group_by_unit)

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
    unit_conversions: list[UnitConversion] | None = None,
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
        units_new, units_skipped, unit_group_by_unit = _ensure_ecoinvent_unit_catalog(
            db, units, unit_conversions
        )
        result.units_new += units_new
        result.units_skipped += units_skipped
        _assign_flow_unit_groups(elementary_flows, unit_group_by_unit)
        if intermediate_flows:
            _assign_flow_unit_groups(intermediate_flows, unit_group_by_unit)

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

            # Ensure exchange units exist for minimal fixtures without UnitConversions.xml.
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
            reference_product_exchange = _build_reference_product_exchange_json(dataset)
            if reference_product_exchange is not None:
                exchanges_json = [reference_product_exchange, *exchanges_json]
            process_json = {
                "reference_flow_uuid": dataset.reference_product_id or None,
                "reference_flow_internal_id": dataset.reference_product_id or None,
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
                reference_flow_internal_id=dataset.reference_product_id or None,
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
