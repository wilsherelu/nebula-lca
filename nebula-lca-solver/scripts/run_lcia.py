import argparse
import os
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.matrix_builder import (
    build_c_matrix_from_ef31,
    build_matrices_from_mmr,
    build_matrices_from_snapshot,
    load_snapshot,
)
from proto import mmr_pb2


def run_lcia(
    ef31_dir: str,
    output_path: str,
    snapshot_path: str | None = None,
    mmr_path: str | None = None,
) -> Path:
    if not Path(ef31_dir).exists():
        raise FileNotFoundError(f"EF3.1 dir not found: {ef31_dir}")
    if mmr_path:
        if not Path(mmr_path).exists():
            raise FileNotFoundError(f"MMR file not found: {mmr_path}")
        mmr_data = Path(mmr_path).read_bytes()
        mmr = mmr_pb2.MMR()
        mmr.ParseFromString(mmr_data)
        base = build_matrices_from_mmr(mmr)
    else:
        if not snapshot_path:
            raise ValueError("snapshot_path is required when mmr_path is not provided")
        snapshot = load_snapshot(snapshot_path)
        base = build_matrices_from_snapshot(snapshot)

    b_matrix = base["B"]
    c_pack = build_c_matrix_from_ef31(ef31_dir, b_matrix, issues=base.get("issues"))
    c_matrix = c_pack["C"]
    indicator_lookup = c_pack["indicator_lookup"]
    process_lookup = base.get("process_lookup", {})

    a_matrix = base["A"]
    n = a_matrix["shape"][0]
    m = b_matrix["shape"][0]
    k = c_matrix["shape"][0]

    a = np.zeros((n, n), dtype=float)
    for entry in a_matrix["data"]:
        a[entry["row_index"], entry["col_index"]] = entry["value"]

    b = np.zeros((m, n), dtype=float)
    for entry in b_matrix["data"]:
        b[entry["row_index"], entry["col_index"]] = entry["value"]

    c = np.zeros((k, m), dtype=float)
    for entry in c_matrix["data"]:
        c[entry["row_index"], entry["col_index"]] = entry["value"]

    try:
        a_inv = np.linalg.inv(a)
    except np.linalg.LinAlgError as exc:
        cond = np.linalg.cond(a)
        raise RuntimeError(f"inversion failed: {exc}; condition number: {cond}") from exc

    g = b @ a_inv
    lcia = c @ g

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _write_lcia_csv(out_path, lcia, a_matrix["rows"], process_lookup, indicator_lookup)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute LCIA results for all processes.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--snapshot", help="Path to snapshot JSON.")
    input_group.add_argument("--mmr", help="Path to MMR protobuf (.pb).")
    parser.add_argument("--ef31", required=True, help="Path to EF3.1 folder.")
    parser.add_argument(
        "--output",
        help="Output CSV path for LCIA results (default: exports/LCIA_results.csv).",
    )
    args = parser.parse_args()

    default_dir = os.environ.get("TIANGONG_OUTPUT_DIR", str(ROOT / "exports"))
    if args.output:
        output_path = args.output
    else:
        from app.core.mmr import timestamp_tag
        output_path = str(Path(default_dir) / f"LCIA_results_{timestamp_tag()}.csv")
    out_path = run_lcia(
        ef31_dir=args.ef31,
        output_path=output_path,
        snapshot_path=args.snapshot,
        mmr_path=args.mmr,
    )
    print(f"wrote LCIA results to {out_path}")


def _write_lcia_csv(
    path: Path,
    lcia: np.ndarray,
    process_ids: list[str],
    process_lookup: dict,
    indicator_lookup: dict,
) -> None:
    import csv

    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        header = ["indicator_index", "method_en", "method_zh", "indicator_en"]
        for idx, proc_uuid in enumerate(process_ids):
            proc_name = process_lookup.get(proc_uuid, {}).get("process_name", "")
            header.append(f"col_{idx}:{proc_uuid}:{proc_name}")
        writer.writerow(header)

        for idx in range(lcia.shape[0]):
            info = indicator_lookup.get(idx, {})
            row = [
                idx,
                info.get("method_en", ""),
                info.get("method_zh", ""),
                info.get("indicator_en", ""),
            ]
            row.extend(lcia[idx, :].tolist())
            writer.writerow(row)


if __name__ == "__main__":
    main()
