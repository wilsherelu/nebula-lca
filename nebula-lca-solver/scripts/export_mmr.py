import argparse
from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.mmr import default_mmr_path, export_mmr_from_snapshot


def export_mmr(
    snapshot_path: str,
    output_path: str,
    mmr_version: str = "1.0",
    solver_version: str = "matrix_builder_v0.1",
) -> Path:
    return export_mmr_from_snapshot(
        snapshot_path,
        output_path,
        mmr_version=mmr_version,
        solver_version=solver_version,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export MMR protobuf from snapshot.")
    parser.add_argument("--snapshot", required=True, help="Path to snapshot JSON.")
    parser.add_argument(
        "--output",
        default="",
        help="Output .pb path (default: exports/mmr.pb).",
    )
    parser.add_argument(
        "--mmr-version",
        default="1.0",
        help="MMR schema version.",
    )
    parser.add_argument(
        "--solver-version",
        default="matrix_builder_v0.1",
        help="Solver version string.",
    )
    args = parser.parse_args()

    default_dir = os.environ.get("TIANGONG_OUTPUT_DIR", str(ROOT / "exports"))
    output_path = args.output or str(default_mmr_path(default_dir))
    out_path = export_mmr(
        args.snapshot,
        output_path,
        mmr_version=args.mmr_version,
        solver_version=args.solver_version,
    )
    print(f"wrote {out_path} ({out_path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
