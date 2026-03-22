from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run tiangong-nebula-solver PTS compiler and print JSON.")
    parser.add_argument("--solver-root", required=True, help="Path to tiangong-nebula-solver root")
    parser.add_argument("--payload", required=True, help="Path to payload json")
    args = parser.parse_args()

    solver_root = Path(args.solver_root).resolve()
    payload_path = Path(args.payload).resolve()

    if not solver_root.exists():
        raise FileNotFoundError(f"solver root not found: {solver_root}")
    if not payload_path.exists():
        raise FileNotFoundError(f"payload not found: {payload_path}")

    if str(solver_root) not in sys.path:
        sys.path.insert(0, str(solver_root))

    from app.core.pts_compile import compile_pts_from_payload  # type: ignore

    with payload_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    result = compile_pts_from_payload(payload)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
