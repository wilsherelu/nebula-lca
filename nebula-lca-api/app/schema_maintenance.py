from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .models import LciBiosphereFlowKey, LciExchangeMatrix, LciProcessVector, LciVectorAxis, UnitGroup, ImportJob, ImportJobPauseRequest, DatasetCheckpoint, GlobalDatasetImport
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
            ("allocation_properties", "JSONB" if engine.dialect.name == "postgresql" else "JSON"),
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


def ensure_lci_exchange_matrix_table(engine: Engine) -> dict:
    """Ensure LCI sparse row and compressed-vector tables exist."""
    added_tables: list[str] = []
    added_indexes: list[str] = []
    if not inspect(engine).has_table("lci_exchange_matrix"):
        LciExchangeMatrix.__table__.create(bind=engine, checkfirst=True)
        added_tables.append("lci_exchange_matrix")
    for table_model, table_name in [
        (LciBiosphereFlowKey, "lci_biosphere_flow_keys"),
        (LciVectorAxis, "lci_vector_axes"),
        (LciProcessVector, "lci_process_vectors"),
    ]:
        if not inspect(engine).has_table(table_name):
            table_model.__table__.create(bind=engine, checkfirst=True)
            added_tables.append(table_name)

    with engine.begin() as conn:
        inspector = inspect(conn)
        columns = {col["name"] for col in inspector.get_columns("lci_exchange_matrix")}
        needed = {
            "process_uuid": "VARCHAR(64)",
            "flow_uuid": "VARCHAR(64)",
            "amount": "FLOAT",
            "unit": "VARCHAR(64)",
            "direction": "VARCHAR(8)",
            "source": "VARCHAR(64)",
            "source_package_version": "VARCHAR(255)",
        }
        for col_name, col_type in needed.items():
            if col_name not in columns:
                conn.execute(text(f"ALTER TABLE lci_exchange_matrix ADD COLUMN {col_name} {col_type}"))
                added_indexes.append(f"col:{col_name}")

        existing_indexes = {idx["name"] for idx in inspector.get_indexes("lci_exchange_matrix")}
        dialect_name = conn.engine.dialect.name
        for idx_name, idx_sql in [
            ("ix_lci_process_uuid", "CREATE INDEX {if_not_exists} ix_lci_process_uuid ON lci_exchange_matrix (process_uuid)"),
            ("ix_lci_flow_uuid", "CREATE INDEX {if_not_exists} ix_lci_flow_uuid ON lci_exchange_matrix (flow_uuid)"),
        ]:
            if idx_name in existing_indexes:
                continue
            if_not_exists = "IF NOT EXISTS" if dialect_name in {"postgresql", "sqlite"} else ""
            conn.execute(text(idx_sql.format(if_not_exists=if_not_exists)))
            added_indexes.append(idx_name)

    return {
        "status": "ok" if added_tables or added_indexes else "already_complete",
        "added_tables": added_tables,
        "added_indexes": added_indexes,
    }


def ensure_import_tables(engine: Engine) -> dict:
    """Ensure import task system tables exist."""
    added_tables: list[str] = []
    added_indexes: list[str] = []
    added_columns: list[str] = []

    for model, table_name in [
        (ImportJob, "import_jobs"),
        (ImportJobPauseRequest, "import_job_pause_requests"),
        (DatasetCheckpoint, "dataset_checkpoints"),
        (GlobalDatasetImport, "global_dataset_imports"),
    ]:
        if not inspect(engine).has_table(table_name):
            model.__table__.create(bind=engine, checkfirst=True)
            added_tables.append(table_name)

    with engine.begin() as conn:
        dialect_name = conn.engine.dialect.name
        if_if_not_exists = "IF NOT EXISTS" if dialect_name in {"postgresql", "sqlite"} else ""
        inspector = inspect(conn)

        if inspector.has_table("import_jobs"):
            import_job_columns = {col["name"] for col in inspector.get_columns("import_jobs")}
            for col_name, col_type in [
                ("skipped_global", "INTEGER NOT NULL DEFAULT 0"),
                ("overwrite_existing", "BOOLEAN NOT NULL DEFAULT false"),
            ]:
                if col_name in import_job_columns:
                    continue
                if dialect_name == "postgresql":
                    conn.execute(text(f"ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
                elif dialect_name == "sqlite":
                    conn.execute(text(f"ALTER TABLE import_jobs ADD COLUMN {col_name} {col_type}"))
                else:
                    raise RuntimeError(f"Unsupported database dialect '{dialect_name}'")
                added_columns.append(f"import_jobs.{col_name}")

        if inspector.has_table("dataset_checkpoints"):
            checkpoint_columns = {col["name"] for col in inspector.get_columns("dataset_checkpoints")}
            for col_name, col_type in [
                ("vector_status", "VARCHAR(32)"),
            ]:
                if col_name in checkpoint_columns:
                    continue
                if dialect_name == "postgresql":
                    conn.execute(text(f"ALTER TABLE dataset_checkpoints ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
                elif dialect_name == "sqlite":
                    conn.execute(text(f"ALTER TABLE dataset_checkpoints ADD COLUMN {col_name} {col_type}"))
                else:
                    raise RuntimeError(f"Unsupported database dialect '{dialect_name}'")
                added_columns.append(f"dataset_checkpoints.{col_name}")

        idx_defs = [
            ("import_jobs", "ix_import_jobs_status", f"CREATE INDEX {if_if_not_exists} ix_import_jobs_status ON import_jobs (status)"),
            ("dataset_checkpoints", "ix_dataset_checkpoint_job_id", f"CREATE INDEX {if_if_not_exists} ix_dataset_checkpoint_job_id ON dataset_checkpoints (job_id)"),
            ("dataset_checkpoints", "ix_dataset_checkpoint_status", f"CREATE INDEX {if_if_not_exists} ix_dataset_checkpoint_status ON dataset_checkpoints (status)"),
            ("dataset_checkpoints", "uq_dataset_checkpoint_job_dataset", f"CREATE UNIQUE INDEX {if_if_not_exists} uq_dataset_checkpoint_job_dataset ON dataset_checkpoints (job_id, dataset_key)"),
            ("global_dataset_imports", "ix_global_dataset_pv", f"CREATE INDEX {if_if_not_exists} ix_global_dataset_pv ON global_dataset_imports (source_package_version, dataset_uuid)"),
            ("global_dataset_imports", "ix_global_dataset_status", f"CREATE INDEX {if_if_not_exists} ix_global_dataset_status ON global_dataset_imports (status)"),
        ]
        existing_by_table = {
            table_name: {idx["name"] for idx in inspector.get_indexes(table_name)}
            for table_name in {"import_jobs", "dataset_checkpoints", "global_dataset_imports"}
            if inspector.has_table(table_name)
        }
        for table_name, idx_name, idx_sql in idx_defs:
            existing_indexes = existing_by_table.get(table_name, set())
            if idx_name in existing_indexes:
                continue
            if table_name == "dataset_checkpoints" and idx_name == "uq_dataset_checkpoint_job_dataset":
                if dialect_name == "sqlite":
                    conn.execute(text(
                        "DELETE FROM dataset_checkpoints "
                        "WHERE id NOT IN ("
                        "SELECT MAX(id) FROM dataset_checkpoints GROUP BY job_id, dataset_key"
                        ")"
                    ))
                elif dialect_name == "postgresql":
                    conn.execute(text(
                        "DELETE FROM dataset_checkpoints a USING dataset_checkpoints b "
                        "WHERE a.job_id = b.job_id "
                        "AND a.dataset_key = b.dataset_key "
                        "AND a.id < b.id"
                    ))
            conn.execute(text(idx_sql))
            added_indexes.append(idx_name)

    return {
        "status": "ok" if added_tables or added_columns or added_indexes else "already_complete",
        "added_tables": added_tables,
        "added_columns": added_columns,
        "added_indexes": added_indexes,
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


def backfill_ecoinvent_unit_group_sources(db: Session) -> dict:
    updated = 0
    for row in db.query(UnitGroup).all():
        source_version = str(getattr(row, "source_version", "") or "").strip().lower()
        source_package_version = str(getattr(row, "source_package_version", "") or "").strip().lower()
        source_file = str(getattr(row, "source_file", "") or "").strip()
        is_ecoinvent = (
            source_version == "ecoinvent"
            or source_package_version.startswith("ecoinvent")
            or source_file == "MasterData/UnitConversions.xml"
        )
        if not is_ecoinvent:
            continue
        changed = False
        for attr, value in {
            "source_uuid": getattr(row, "source_uuid", None) or f"ecoinvent:unit-type:{row.name}",
            "source_version": getattr(row, "source_version", None) or "ecoinvent",
            "source_package_version": getattr(row, "source_package_version", None) or "ecoinvent 3.11",
            "source_file": getattr(row, "source_file", None) or "MasterData/UnitConversions.xml",
        }.items():
            if getattr(row, attr, None) != value:
                setattr(row, attr, value)
                changed = True
        if changed:
            updated += 1
    if updated:
        db.commit()
    return {"updated": updated}
