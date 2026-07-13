"""UUID-based ecoinvent elementary-flow context backfill."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from .ecoinvent_ef31_loader import parse_elementary_exchanges, parse_units
from .models import FlowRecord


def backfill_ecoinvent_flow_contexts(
    db: Session,
    *,
    masterdata_dir: Path,
    source: str = "ecoinvent_3.11",
    apply: bool = False,
) -> dict[str, object]:
    elementary_path = masterdata_dir / "ElementaryExchanges.xml"
    if not elementary_path.is_file():
        raise FileNotFoundError(f"ElementaryExchanges.xml not found in {masterdata_dir}")

    units = parse_units(masterdata_dir)
    parsed_flows = parse_elementary_exchanges(masterdata_dir, units)
    source_contexts = {
        flow.flow_uuid: (flow.compartment or "", flow.subcompartment or "")
        for flow in parsed_flows
        if flow.flow_uuid
    }

    catalog_rows = {
        row.flow_uuid: row
        for row in db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(source_contexts)).all()
    }
    eligible_rows = {
        flow_uuid: row
        for flow_uuid, row in catalog_rows.items()
        if row.source == source and row.flow_type == "Elementary flow"
    }
    conflicts = sorted(set(catalog_rows) - set(eligible_rows))
    missing = sorted(set(source_contexts) - set(catalog_rows))

    changed = 0
    unchanged = 0
    for flow_uuid, row in eligible_rows.items():
        compartment, subcompartment = source_contexts[flow_uuid]
        if (row.compartment or "", row.subcompartment or "") == (compartment, subcompartment):
            unchanged += 1
            continue
        changed += 1
        if apply:
            row.compartment = compartment or None
            row.subcompartment = subcompartment or None

    if apply:
        db.commit()
    else:
        db.rollback()

    return {
        "status": "applied" if apply else "dry_run",
        "source": source,
        "masterdata_dir": str(masterdata_dir),
        "masterdata_flows": len(source_contexts),
        "eligible_catalog_flows": len(eligible_rows),
        "changed": changed,
        "unchanged": unchanged,
        "missing_flow_uuids": missing,
        "conflicting_flow_uuids": conflicts,
    }
