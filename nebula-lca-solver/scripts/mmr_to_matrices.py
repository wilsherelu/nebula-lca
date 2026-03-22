import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.matrix_builder import build_matrices_from_mmr
from proto import mmr_pb2


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild A/B matrices from MMR protobuf.")
    parser.add_argument("--mmr", required=True, help="Path to .pb MMR file.")
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "exports" / "mmr_matrices"),
        help="Output directory for CSV matrices.",
    )
    args = parser.parse_args()

    mmr_data = Path(args.mmr).read_bytes()
    mmr = mmr_pb2.MMR()
    mmr.ParseFromString(mmr_data)

    matrices = build_matrices_from_mmr(mmr)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    _write_matrix(out_dir / "A_data.csv", matrices["A"])
    _write_matrix(out_dir / "B_data.csv", matrices["B"])
    _write_index(out_dir / "A_index.csv", matrices["A"]["rows"])
    _write_index(out_dir / "B_rows.csv", matrices["B"]["rows"])

    print(f"wrote {out_dir / 'A_data.csv'}")
    print(f"wrote {out_dir / 'B_data.csv'}")


def _write_index(path: Path, ids) -> None:
    import csv

    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["index", "uuid"])
        for idx, item in enumerate(ids):
            writer.writerow([idx, item])


def _write_matrix(path: Path, matrix: dict) -> None:
    import csv

    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["row_index", "col_index", "row_id", "col_id", "value"])
        for entry in matrix.get("data", []):
            writer.writerow(
                [
                    entry.get("row_index", -1),
                    entry.get("col_index", -1),
                    entry.get("row", ""),
                    entry.get("col", ""),
                    entry.get("value", 0.0),
                ]
            )


if __name__ == "__main__":
    main()
