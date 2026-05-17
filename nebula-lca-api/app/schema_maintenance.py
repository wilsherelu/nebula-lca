from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .models import UnitGroup
from .tidas_reference import load_tidas_reference_seed


def ensure_flow_catalog_tidas_columns(engine: Engine) -> dict:
    added_columns: list[str] = []
    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table("flow_catalog"):
            return {"table": "flow_catalog", "added_columns": added_columns, "status": "skipped_table_missing"}
        columns = {col["name"] for col in inspector.get_columns("flow_catalog")}
        dialect_name = conn.engine.dialect.name
        for col_name, col_type in [
            ("source", "VARCHAR(64)"),
            ("is_custom", "BOOLEAN NOT NULL DEFAULT false"),
            ("tidas_compatible", "BOOLEAN NOT NULL DEFAULT false"),
            ("tidas_unit_group", "VARCHAR(128)"),
            ("tidas_flow_property_uuid", "VARCHAR(64)"),
            ("tidas_reference_source", "VARCHAR(128)"),
        ]:
            if col_name in columns:
                continue
            if dialect_name == "postgresql":
                conn.execute(text(f"ALTER TABLE flow_catalog ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            elif dialect_name == "sqlite":
                conn.execute(text(f"ALTER TABLE flow_catalog ADD COLUMN {col_name} {col_type}"))
            else:
                raise RuntimeError(f"Unsupported database dialect '{dialect_name}'")
            added_columns.append(col_name)
    return {
        "table": "flow_catalog",
        "added_columns": added_columns,
        "status": "ok" if added_columns else "already_complete",
    }


def ensure_unit_group_source_columns(engine: Engine) -> dict:
    added_columns: list[str] = []
    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table("unit_groups"):
            return {"table": "unit_groups", "added_columns": added_columns, "status": "skipped_table_missing"}
        columns = {col["name"] for col in inspector.get_columns("unit_groups")}
        dialect_name = conn.engine.dialect.name
        for col_name, col_type in [
            ("source_uuid", "VARCHAR(64)"),
            ("source_version", "VARCHAR(32)"),
            ("source_package_version", "VARCHAR(255)"),
            ("source_file", "VARCHAR(1024)"),
        ]:
            if col_name in columns:
                continue
            if dialect_name == "postgresql":
                conn.execute(text(f"ALTER TABLE unit_groups ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            elif dialect_name == "sqlite":
                conn.execute(text(f"ALTER TABLE unit_groups ADD COLUMN {col_name} {col_type}"))
            else:
                raise RuntimeError(f"Unsupported database dialect '{dialect_name}'")
            added_columns.append(col_name)
        if dialect_name == "postgresql":
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_unit_groups_source_uuid ON unit_groups (source_uuid)"))
        elif dialect_name == "sqlite":
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_unit_groups_source_uuid ON unit_groups (source_uuid)"))
    return {
        "table": "unit_groups",
        "added_columns": added_columns,
        "status": "ok" if added_columns else "already_complete",
    }


def backfill_tidas_unit_group_sources(db: Session) -> dict:
    seed = load_tidas_reference_seed()
    package_version = str(seed.get("source_package_version") or "")
    updated = 0

    def keys_for(item: dict) -> set[tuple[str, str]]:
        names = {
            str(item.get("name") or "").strip(),
            str(item.get("source_unit_group") or "").strip(),
        }
        for alias in item.get("aliases") or []:
            if alias:
                names.add(str(alias).strip())
        reference_unit = str(item.get("reference_unit") or "").strip().lower()
        return {
            (name.strip().lower(), reference_unit)
            for name in names
            if name.strip() and reference_unit
        }

    by_key: dict[tuple[str, str], dict] = {}
    by_reference_unit: dict[str, list[dict]] = {}
    for item in seed.get("unit_groups") or []:
        if not isinstance(item, dict):
            continue
        ref = str(item.get("reference_unit") or "").strip().lower()
        if ref:
            by_reference_unit.setdefault(ref, []).append(item)
        for key in keys_for(item):
            by_key[key] = item

    for row in db.query(UnitGroup).all():
        reference_unit = str(row.reference_unit or "").strip().lower()
        name_key = (str(row.name or "").strip().lower(), reference_unit)
        item = by_key.get(name_key)
        if item is None and reference_unit:
            candidates = by_reference_unit.get(reference_unit) or []
            if len(candidates) == 1:
                item = candidates[0]
        if item is None:
            continue
        source_uuid = str(item.get("uuid") or item.get("source_uuid") or "").strip()
        if not source_uuid:
            continue
        changed = False
        for attr, value in {
            "source_uuid": source_uuid,
            "source_version": str(item.get("version") or "").strip() or None,
            "source_package_version": package_version or None,
            "source_file": str(item.get("source_file") or item.get("ref_uri") or "").strip() or None,
        }.items():
            if getattr(row, attr, None) != value:
                setattr(row, attr, value)
                changed = True
        if changed:
            updated += 1

    if updated:
        db.commit()
    return {"updated": updated, "seed_unit_groups": len(seed.get("unit_groups") or [])}
