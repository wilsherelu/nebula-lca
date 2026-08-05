"""Run the source API against an existing Nebula desktop data directory."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--solver-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    os.environ["NEBULA_DATA_DIR"] = str(data_dir)
    os.environ["NEBULA_API_PORT"] = str(args.port)
    os.environ["NEBULA_LCA_SOLVER_API_URL"] = args.solver_url

    from app.desktop_entry import main as desktop_main

    desktop_main()


if __name__ == "__main__":
    main()
