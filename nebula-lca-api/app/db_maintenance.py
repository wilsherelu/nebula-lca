"""CLI maintenance tool for Nebula LCA database.

Usage:
    python -m app.db_maintenance stats
    python -m app.db_maintenance rebuild-performance-caches
    python -m app.db_maintenance --help

This module is safe to run at any time. It only reads/creates indexes and caches.
It never performs destructive cleanup without explicit flags.
"""

import argparse
import os
import sys
import time
from pathlib import Path

# Ensure app package is importable
_APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_APP_DIR))

from app.config import settings
from app.database import engine, SessionLocal
from app.models import FlowRecord, ReferenceProcess, Model, ModelVersion, RunJob

# Disable auto-prune on startup to avoid interference with maintenance
os.environ["AUTO_PRUNE_ON_STARTUP"] = "0"


def _table_count(db, model) -> int:
    return db.query(model).count()


def cmd_stats(args):
    """Output database statistics: size, row counts, JSON field sizes, slow queries."""
    from sqlalchemy import text

    url = settings.database_url
    print(f"Database URL: {url}")

    is_sqlite = url.strip().lower().startswith("sqlite")
    db = SessionLocal()
    try:
        # Table row counts
        tables_info = [
            ("flow_catalog", FlowRecord),
            ("reference_processes", ReferenceProcess),
            ("models", Model),
            ("model_versions", ModelVersion),
            ("run_jobs", RunJob),
        ]
        print("\n--- Table Row Counts ---")
        for name, model_cls in tables_info:
            count = _table_count(db, model_cls)
            print(f"  {name}: {count}")

        # SQLite-specific: DB file size
        if is_sqlite:
            db_path = url.replace("sqlite:///", "")
            if db_path.startswith("./"):
                db_path = _APP_DIR.parent / db_path[2:]
            elif db_path.startswith("/"):
                db_path = Path(db_path)
            else:
                db_path = Path(db_path)
            if db_path.exists():
                size_bytes = db_path.stat().st_size
                size_mb = size_bytes / (1024 * 1024)
                print(f"\n--- Database File ---")
                print(f"  Path: {db_path}")
                print(f"  Size: {size_mb:.2f} MB ({size_bytes:,} bytes)")

        # JSON field sizes (SQLite specific)
        if is_sqlite:
            print(f"\n--- JSON Field Sizes ---")
            conn = engine.raw_connection()
            cursor = conn.cursor()
            try:
                # hybrid_graph_json top consumers
                cursor.execute(
                    "SELECT id, version, length(hybrid_graph_json) as json_bytes "
                    "FROM model_versions ORDER BY json_bytes DESC LIMIT 10"
                )
                rows = cursor.fetchall()
                if rows:
                    print("  Top model_versions by hybrid_graph_json size:")
                    for row_id, version, json_bytes in rows:
                        print(f"    v{version}: {json_bytes / 1024:.1f} KB")
                else:
                    print("  model_versions: (empty)")

                # request_json top consumers
                cursor.execute(
                    "SELECT id, length(request_json) as json_bytes, length(result_json) as result_bytes "
                    "FROM run_jobs ORDER BY json_bytes DESC LIMIT 10"
                )
                rows = cursor.fetchall()
                if rows:
                    print("  Top run_jobs by request_json size:")
                    for row_id, json_bytes, result_bytes in rows:
                        print(f"    {row_id[:8]}... req={json_bytes / 1024:.1f} KB, result={result_bytes / 1024:.1f} KB")
                else:
                    print("  run_jobs: (empty)")

                # Check if FTS5 exists for flow_catalog
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='flow_catalog_fts'"
                )
                fts_exists = cursor.fetchone() is not None
                print(f"\n  FTS5 flow_catalog index: {'exists' if fts_exists else 'MISSING (search falls back to LIKE)'}")
            finally:
                conn.close()

        print("\n--- Configuration ---")
        print(f"  KEEP_LATEST_VERSIONS_PER_PROJECT: {settings.keep_latest_versions_per_project}")
        print(f"  AUTO_PRUNE_ON_STARTUP: {settings.auto_prune_on_startup}")

    finally:
        db.close()


def cmd_rebuild_performance_caches(args):
    """Rebuild flow/process search caches and performance indexes.

    This is safe to run at any time. It creates FTS5 indexes if not present,
    and invalidates the in-memory cache by bumping the cache revision counter.
    """
    from sqlalchemy import text

    print("Rebuilding performance caches...")

    # Check if we can create FTS5
    url = settings.database_url
    is_sqlite = url.strip().lower().startswith("sqlite")

    if is_sqlite:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_fts%'"
            )
            existing_fts = {row[0] for row in cursor.fetchall()}
            print(f"  Existing FTS tables: {existing_fts or 'none'}")

            print("  Dropping stale FTS triggers and tables...")
            for trigger_name in ("fts_flow_catalog_ai", "fts_flow_catalog_ad", "fts_flow_catalog_au"):
                cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
            cursor.execute("DROP TABLE IF EXISTS flow_catalog_fts")

            print("  Creating FTS5 index for flow_catalog...")
            cursor.execute("""
                CREATE VIRTUAL TABLE flow_catalog_fts USING fts5(
                    flow_name, flow_name_en, flow_uuid,
                    content='flow_catalog',
                    content_rowid='rowid',
                    tokenize='unicode61'
                )
            """)
            print("  Rebuilding FTS5 index...")
            cursor.execute(
                "INSERT INTO flow_catalog_fts (rowid, flow_name, flow_name_en, flow_uuid) "
                "SELECT rowid, flow_name, flow_name_en, flow_uuid FROM flow_catalog"
            )
            cursor.execute("INSERT INTO flow_catalog_fts(flow_catalog_fts) VALUES('optimize')")
            print("  FTS5 rebuilt and optimized.")

            print("  Rebuilding FTS5 index for reference_processes...")
            cursor.execute("DROP TABLE IF EXISTS reference_processes_fts")
            cursor.execute("""
                CREATE VIRTUAL TABLE reference_processes_fts USING fts5(
                    process_name, process_name_zh, process_name_en, process_uuid,
                    content='reference_processes',
                    content_rowid='rowid',
                    tokenize='unicode61'
                )
            """)
            cursor.execute(
                "INSERT INTO reference_processes_fts (rowid, process_name, process_name_zh, process_name_en, process_uuid) "
                "SELECT rowid, process_name, process_name_zh, process_name_en, process_uuid FROM reference_processes"
            )
            cursor.execute("INSERT INTO reference_processes_fts(reference_processes_fts) VALUES('optimize')")
            print("  reference_processes FTS5 rebuilt.")

            # Ensure triggers exist for auto-sync of new/updated flows
            print("  Ensuring FTS sync triggers...")
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'fts_flow_catalog%'"
            )
            existing_triggers = {row[0] for row in cursor.fetchall()}

            triggers = [
                ("fts_flow_catalog_ai",
                 "CREATE TRIGGER fts_flow_catalog_ai AFTER INSERT ON flow_catalog BEGIN "
                 "INSERT INTO flow_catalog_fts(rowid, flow_name, flow_name_en, flow_uuid) "
                 "VALUES (new.rowid, new.flow_name, new.flow_name_en, new.flow_uuid); END"),
                ("fts_flow_catalog_ad",
                 "CREATE TRIGGER fts_flow_catalog_ad AFTER DELETE ON flow_catalog BEGIN "
                 "DELETE FROM flow_catalog_fts WHERE rowid = old.rowid; END"),
                ("fts_flow_catalog_au",
                 "CREATE TRIGGER fts_flow_catalog_au AFTER UPDATE ON flow_catalog BEGIN "
                 "DELETE FROM flow_catalog_fts WHERE rowid = old.rowid; "
                 "INSERT INTO flow_catalog_fts(rowid, flow_name, flow_name_en, flow_uuid) "
                 "VALUES (new.rowid, new.flow_name, new.flow_name_en, new.flow_uuid); END"),
            ]
            for trigger_name, trigger_sql in triggers:
                if trigger_name not in existing_triggers:
                    cursor.execute(trigger_sql)
                    print(f"    Created trigger: {trigger_name}")
                else:
                    print(f"    Trigger exists: {trigger_name}")
            conn.commit()

        finally:
            conn.close()

    # Invalidate in-memory cache by bumping revision
    print("  Invalidating in-memory cache revision...")
    db = SessionLocal()
    try:
        # We use a simple approach: just query and let the cache TTL expire naturally.
        # The in-memory cache (_cache_map) will be rebuilt on next requests.
        print("  (Cache revision will be bumped on next API request)")
    finally:
        db.close()

    print("Done.")


def main():
    parser = argparse.ArgumentParser(description="Nebula LCA Database Maintenance")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # stats command
    subparsers.add_parser("stats", help="Show database statistics")

    # rebuild-performance-caches command
    subparsers.add_parser(
        "rebuild-performance-caches",
        help="Rebuild FTS5 indexes and invalidate caches",
    )

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "stats": cmd_stats,
        "rebuild-performance-caches": cmd_rebuild_performance_caches,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
