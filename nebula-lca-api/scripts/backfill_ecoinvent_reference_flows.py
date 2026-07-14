from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database import SessionLocal
from app.services.intermediate_flow_linking_service import backfill_ecoinvent_reference_flow_uuids


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    db = SessionLocal()
    try:
        report = backfill_ecoinvent_reference_flow_uuids(db, commit=bool(args.commit))
        print(json.dumps(report, ensure_ascii=True, indent=2))
        return 0 if report["conflict_count"] == 0 else 2
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
