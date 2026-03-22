import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.export_mmr import export_mmr
from scripts.run_lcia import run_lcia
from app.core.mmr import default_mmr_path, timestamp_tag


def main() -> None:
    parser = argparse.ArgumentParser(description="Main pipeline: JSON -> MMR -> LCIA.")
    parser.add_argument("--snapshot", required=True, help="Path to snapshot JSON.")
    parser.add_argument("--ef31", required=True, help="Path to EF3.1 folder.")
    parser.add_argument(
        "--mmr-out",
        default="",
        help="Output MMR protobuf path.",
    )
    parser.add_argument(
        "--lcia-out",
        default="",
        help="Output LCIA CSV path.",
    )
    args = parser.parse_args()

    output_dir = os.environ.get("TIANGONG_OUTPUT_DIR", str(ROOT / "exports"))
    mmr_out = args.mmr_out or str(default_mmr_path(output_dir))
    lcia_out = args.lcia_out or str(Path(output_dir) / f"LCIA_results_{timestamp_tag()}.csv")

    mmr_path = export_mmr(args.snapshot, mmr_out)
    print(f"wrote {mmr_path}")

    lcia_path = run_lcia(
        ef31_dir=args.ef31,
        output_path=lcia_out,
        mmr_path=str(mmr_path),
    )
    print(f"wrote {lcia_path}")


if __name__ == "__main__":
    main()
