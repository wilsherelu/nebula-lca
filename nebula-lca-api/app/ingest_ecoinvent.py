"""Ecoinvent reference data import for Nebula LCA.

Import paths:
- Units.xml + UnitConversions.xml -> unit_groups + unit_definitions
- ElementaryExchanges.xml -> flow_catalog (elementary flows)
- IntermediateExchanges.xml -> flow_catalog (product/waste flows)
- .spold files -> reference_processes (lightweight metadata only)
- .spold elementary exchanges -> lci_exchange_matrix (sparse inventory)

All parsing delegates to ecoinvent_ef31_loader where possible.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from .ecoinvent_ef31_loader import (
    parse_units,
    parse_unit_conversions,
    parse_elementary_exchanges,
    parse_intermediate_exchanges,
    parse_spold_file,
    parse_spold_exchanges,
)
from .ef31_db_service import (
    _assign_flow_unit_groups,
    _build_ecoinvent_unit_catalog,
    _ensure_ecoinvent_unit_catalog,
    _generate_lci_process_uuid,
)
from .models import FlowRecord, ReferenceProcess, UnitGroup, LciExchangeMatrix

logger = logging.getLogger(__name__)


# ======================================================================
# Unit catalog import
# ======================================================================


def import_ecoinvent_units(
    db: Session,
    *,
    data_dir: str,
    package_version: str = "ecoinvent_3.11",
    source_package_version: str | None = None,
) -> dict:
    """Parse Units.xml + UnitConversions.xml and write to unit_groups / unit_definitions.

    Returns:
        {groups_inserted, units_inserted, conversions_count, warnings}
    """
    data_path = Path(data_dir)
    units_map = parse_units(data_path)
    conversions = parse_unit_conversions(data_path)

    before_groups = db.query(UnitGroup).count()
    units_inserted, units_skipped, unit_group_by_unit = _ensure_ecoinvent_unit_catalog(db, units_map, conversions)

    db.commit()
    groups_after = db.query(UnitGroup).count()

    return {
        "groups_inserted": max(0, groups_after - before_groups),
        "units_inserted": units_inserted,
        "units_skipped": units_skipped,
        "unit_groups": len(set(unit_group_by_unit.values())),
        "conversions_count": len(conversions),
        "warnings": [],
    }


def _guess_unit_group_from_name(unit_name: str) -> str | None:
    """Guess unit group from unit name.

    Maps common ecoinvent unit names to standard groups.
    Returns None if unrecognizable.
    """
    name_lower = unit_name.lower().strip()

    # Mass
    if name_lower in ("kilogram", "kg", "gram", "g", "milligram", "mg", "metric ton", "tonne", "t"):
        return "mass"

    # Energy
    if name_lower in ("megajoule", "mj", "kilojoule", "kj", "watthour", "wh", "watthour lt", "wh lt", "kilowatthour", "kwh"):
        return "energy"

    # Volume
    if name_lower in ("cubic meter", "m3", "liter", "l", "milliliter", "ml"):
        return "volume"

    return None


# ======================================================================
# Flow catalog import
# ======================================================================


def import_ecoinvent_elementary_flows(
    db: Session,
    *,
    data_dir: str,
    source: str = "ecoinvent_3.11",
) -> dict:
    """Parse ElementaryExchanges.xml and write to flow_catalog.

    Returns:
        {inserted, updated, skipped, errors}
    """
    data_path = Path(data_dir)
    units_map = parse_units(data_path)
    conversions = parse_unit_conversions(data_path)
    _, _, unit_group_by_unit = _build_ecoinvent_unit_catalog(units_map, conversions)
    flows = parse_elementary_exchanges(data_path, units_map)
    _assign_flow_unit_groups(flows, unit_group_by_unit)

    inserted = 0
    updated = 0
    skipped = 0
    errors: list[str] = []

    existing_uuids = {fr.flow_uuid for fr in db.query(FlowRecord.flow_uuid).all()}

    for flow in flows:
        if not flow.flow_uuid:
            skipped += 1
            continue

        unit_group = flow.unit_group or _guess_unit_group_from_name(flow.default_unit) or "mass"

        if flow.flow_uuid in existing_uuids:
            item = db.get(FlowRecord, flow.flow_uuid)
            if item is None:
                skipped += 1
                continue
            # Update missing fields
            if not item.flow_name or item.flow_name == flow.flow_uuid:
                item.flow_name = flow.flow_name
            if not item.flow_name_en:
                item.flow_name_en = flow.flow_name_en
            if not item.default_unit:
                item.default_unit = flow.default_unit
            if not item.unit_group:
                item.unit_group = unit_group
            if not item.compartment:
                item.compartment = flow.compartment
            if item.source is None or item.source == "ef3.1":
                item.source = source
            updated += 1
        else:
            db.add(
                FlowRecord(
                    flow_uuid=flow.flow_uuid,
                    flow_name=flow.flow_name,
                    flow_name_en=flow.flow_name_en,
                    flow_type="Elementary flow",
                    default_unit=flow.default_unit or "kg",
                    unit_group=unit_group,
                    compartment=flow.compartment,
                    source=source,
                )
            )
            existing_uuids.add(flow.flow_uuid)
            inserted += 1

    db.commit()
    return {"inserted": inserted, "updated": updated, "skipped": skipped, "errors": errors}


def import_ecoinvent_intermediate_flows(
    db: Session,
    *,
    data_dir: str,
    source: str = "ecoinvent_3.11",
) -> dict:
    """Parse IntermediateExchanges.xml and write to flow_catalog.

    Returns:
        {inserted, updated, skipped, errors}
    """
    data_path = Path(data_dir)
    units_map = parse_units(data_path)
    conversions = parse_unit_conversions(data_path)
    _, _, unit_group_by_unit = _build_ecoinvent_unit_catalog(units_map, conversions)
    flows = parse_intermediate_exchanges(data_path, units_map)
    _assign_flow_unit_groups(flows, unit_group_by_unit)

    inserted = 0
    updated = 0
    skipped = 0
    errors: list[str] = []

    existing_uuids = {fr.flow_uuid for fr in db.query(FlowRecord.flow_uuid).all()}

    for flow in flows:
        if not flow.flow_uuid:
            skipped += 1
            continue

        unit_group = flow.unit_group or _guess_unit_group_from_name(flow.default_unit) or "mass"
        flow_type = "Product flow"
        if flow.flow_type == "Waste flow":
            flow_type = "Waste flow"

        if flow.flow_uuid in existing_uuids:
            item = db.get(FlowRecord, flow.flow_uuid)
            if item is None:
                skipped += 1
                continue
            if not item.flow_name or item.flow_name == flow.flow_uuid:
                item.flow_name = flow.flow_name
            if not item.flow_name_en:
                item.flow_name_en = flow.flow_name_en
            if not item.default_unit:
                item.default_unit = flow.default_unit
            if not item.unit_group:
                item.unit_group = unit_group
            item.flow_type = flow_type
            if item.source is None or item.source == "ef3.1":
                item.source = source
            updated += 1
        else:
            db.add(
                FlowRecord(
                    flow_uuid=flow.flow_uuid,
                    flow_name=flow.flow_name,
                    flow_name_en=flow.flow_name_en,
                    flow_type=flow_type,
                    default_unit=flow.default_unit or "kg",
                    unit_group=unit_group,
                    source=source,
                )
            )
            existing_uuids.add(flow.flow_uuid)
            inserted += 1

    db.commit()
    return {"inserted": inserted, "updated": updated, "skipped": skipped, "errors": errors}


# ======================================================================
# Process metadata import (lightweight, no full exchanges JSON)
# ======================================================================


def import_ecoinvent_processes(
    db: Session,
    *,
    spold_dir: str,
    package_version: str = "ecoinvent_3.11",
    limit: Optional[int] = None,
) -> dict:
    """Scan .spold files and write lightweight process metadata to reference_processes.

    Does NOT write full exchanges to process_json.
    Elementary exchanges are aggregated and written to lci_exchange_matrix.

    Returns:
        {processes_inserted, processes_updated, skipped, errors, exchange_count,
         matrix_rows_inserted, matrix_rows_updated, matrix_rows_aggregated, duration_seconds}
    """
    spold_path = Path(spold_dir)
    spold_files = sorted(p for p in spold_path.rglob("*.spold") if p.is_file())

    # Also try .xml files (some tools unzip .spold to .xml)
    if not spold_files:
        spold_files = sorted(p for p in spold_path.rglob("*.xml") if p.is_file())

    if limit is not None and limit > 0:
        spold_files = spold_files[:limit]

    inserted = 0
    updated = 0
    skipped = 0
    errors: list[str] = []
    exchange_count = 0
    matrix_rows_inserted = 0
    matrix_rows_updated = 0
    matrix_rows_aggregated = 0

    start_time = time.time()

    for spold_file in spold_files:
        try:
            # Parse process metadata
            dataset = parse_spold_file(spold_file)
            if dataset is None:
                skipped += 1
                continue
            process_uuid = _generate_lci_process_uuid(dataset)

            # Parse elementary exchanges
            elementary_exchanges = parse_spold_exchanges(spold_file)

            # Find reference flow UUID from intermediate flows
            ref_flow_uuid = _find_reference_flow_uuid(
                db,
                dataset.reference_product_name,
                dataset.reference_product_unit,
            )

            # Build lightweight process_json (NO full exchanges)
            process_json = {
                "process_uuid": process_uuid,
                "activity_id": dataset.activity_id,
                "process_name": dataset.activity_name,
                "location": dataset.location,
                "reference_product": dataset.reference_product_name,
                "reference_product_id": dataset.reference_product_id,
                "reference_product_unit": dataset.reference_product_unit,
                "reference_product_amount": dataset.reference_product_amount,
                "exchange_count": len(elementary_exchanges),
                "source": package_version,
            }

            existing = db.get(ReferenceProcess, process_uuid)
            if existing is None:
                db.add(
                    ReferenceProcess(
                        process_uuid=process_uuid,
                        process_name=dataset.activity_name or process_uuid,
                        process_name_en=dataset.activity_name,
                        process_type="lci_dataset",
                        reference_flow_uuid=ref_flow_uuid,
                        process_json=process_json,
                        source_file=str(spold_file),
                        import_mode="ecoinvent_ef31_lci",
                        import_report_json={"package_version": package_version},
                    )
                )
                inserted += 1
            else:
                existing.process_name = dataset.activity_name or process_uuid
                existing.process_name_en = dataset.activity_name
                existing.process_type = "lci_dataset"
                existing.reference_flow_uuid = ref_flow_uuid
                existing.process_json = process_json
                existing.source_file = str(spold_file)
                existing.import_mode = "ecoinvent_ef31_lci"
                existing.import_report_json = {"package_version": package_version}
                updated += 1

            matrix_rows = []
            for ex in elementary_exchanges:
                if not ex.exchange_id or ex.amount == 0:
                    continue
                matrix_rows.append(
                    LciExchangeMatrix(
                        process_uuid=process_uuid,
                        flow_uuid=ex.exchange_id,
                        amount=ex.amount,
                        unit=ex.unit,
                        direction=ex.direction,
                        source=package_version,
                        source_package_version=package_version,
                    )
                )
            matrix_result = write_ecoinvent_exchanges_to_matrix(db, matrix_rows, commit=False)
            exchange_count += len(matrix_rows)
            matrix_rows_inserted += matrix_result["rows_inserted"]
            matrix_rows_updated += matrix_result["rows_updated"]
            matrix_rows_aggregated += matrix_result["rows_aggregated"]

        except Exception as exc:
            errors.append(f"{spold_file.name}: {exc}")
            logger.warning(f"Failed to process {spold_file}: {exc}")

    db.commit()
    duration = time.time() - start_time

    return {
        "processes_inserted": inserted,
        "processes_updated": updated,
        "skipped": skipped,
        "errors": errors,
        "exchange_count": exchange_count,
        "matrix_rows_inserted": matrix_rows_inserted,
        "matrix_rows_updated": matrix_rows_updated,
        "matrix_rows_aggregated": matrix_rows_aggregated,
        "duration_seconds": round(duration, 3),
    }


def write_ecoinvent_exchanges_to_matrix(
    db: Session,
    exchanges: list[LciExchangeMatrix],
    *,
    commit: bool = True,
) -> dict:
    """Write collected elementary exchanges to lci_exchange_matrix table.

    Aggregates by (process_uuid, flow_uuid, direction, unit): sums amounts.

    Returns:
        {rows_inserted, rows_updated, rows_aggregated, warnings}
    """
    agg: dict[tuple[str, str, str, str], dict[str, object]] = {}
    raw_keys = 0

    for ex in exchanges:
        if not ex.process_uuid or not ex.flow_uuid or not ex.unit or not ex.direction:
            continue
        raw_keys += 1
        key = (ex.process_uuid, ex.flow_uuid, ex.direction, ex.unit)
        if key in agg:
            agg[key]["amount"] = float(agg[key]["amount"]) + ex.amount
        else:
            agg[key] = {
                "amount": ex.amount,
                "source": ex.source,
                "source_package_version": ex.source_package_version,
            }

    rows_inserted = 0
    rows_updated = 0
    rows_aggregated = max(0, raw_keys - len(agg))
    warnings: list[str] = []

    for (proc_uuid, flow_uuid, direction, unit), values in agg.items():
        amount = float(values["amount"])
        source = values.get("source")
        source_package_version = values.get("source_package_version")
        existing = db.query(LciExchangeMatrix).filter(
            LciExchangeMatrix.process_uuid == proc_uuid,
            LciExchangeMatrix.flow_uuid == flow_uuid,
            LciExchangeMatrix.direction == direction,
            LciExchangeMatrix.unit == unit,
        ).first()

        if existing:
            existing.amount = amount
            existing.source = source
            existing.source_package_version = source_package_version
            rows_updated += 1
        else:
            db.add(
                LciExchangeMatrix(
                    process_uuid=proc_uuid,
                    flow_uuid=flow_uuid,
                    amount=amount,
                    unit=unit,
                    direction=direction,
                    source=source,
                    source_package_version=source_package_version,
                )
            )
            rows_inserted += 1

    if commit:
        db.commit()
    else:
        db.flush()
    return {
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_aggregated": rows_aggregated,
        "warnings": warnings,
    }


# ======================================================================
# Helpers
# ======================================================================


def _find_reference_flow_uuid(
    db: Session,
    product_name: str,
    product_unit: str,
) -> str | None:
    """Try to find a matching intermediate flow by name + unit.

    Returns the flow_uuid of the best match, or None.
    """
    if not product_name:
        return None

    # Search by exact name match first
    matches = (
        db.query(FlowRecord)
        .filter(
            FlowRecord.flow_name == product_name,
            FlowRecord.flow_type.in_(["Product flow", "Waste flow"]),
        )
        .all()
    )
    if len(matches) == 1:
        return matches[0].flow_uuid

    # Partial name match
    partial = (
        db.query(FlowRecord)
        .filter(
            FlowRecord.flow_name.contains(product_name[:20]),
            FlowRecord.flow_type.in_(["Product flow", "Waste flow"]),
        )
        .all()
    )
    if len(partial) == 1:
        return partial[0].flow_uuid

    return None
