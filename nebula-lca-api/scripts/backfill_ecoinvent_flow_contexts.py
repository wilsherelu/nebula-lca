"""Backfill ecoinvent flow contexts from an authoritative MasterData folder."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from app.flow_context_backfill import backfill_ecoinvent_flow_contexts
from app.schema_maintenance import ensure_flow_catalog_context_columns

DEFAULT_DATABASE = API_ROOT / "lca_demo.db"
DEFAULT_MASTERDATA = Path(r"D:\ecoinvent\ecoinvent 3.11_cutoff_lci_ecoSpold02\MasterData")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--masterdata", type=Path, default=DEFAULT_MASTERDATA)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    database = args.database.resolve()
    if not database.is_file():
        parser.error(f"database not found: {database}")

    engine = create_engine(f"sqlite:///{database.as_posix()}")
    schema_result = ensure_flow_catalog_context_columns(engine)
    with Session(engine) as db:
        result = backfill_ecoinvent_flow_contexts(
            db,
            masterdata_dir=args.masterdata.resolve(),
            apply=bool(args.apply),
        )
    result["database"] = str(database)
    result["schema"] = schema_result
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
