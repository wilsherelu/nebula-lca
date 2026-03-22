import argparse
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.matrix_builder import build_matrices_from_snapshot, load_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Solve demo demand on A matrix.")
    parser.add_argument(
        "--process",
        help="Process UUID or index for the demand vector (optional).",
    )
    parser.add_argument(
        "--snapshot",
        help="Path to snapshot JSON (default: repo demo snapshot).",
    )
    parser.add_argument(
        "--output",
        help="Optional CSV path to write the full requirement matrix (A inverse).",
    )
    parser.add_argument(
        "--topn",
        type=int,
        default=5,
        help="Top-N process requirements to export per column.",
    )
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot) if args.snapshot else (
        ROOT / "lca-snapshot-e8e394cb-bb3a-48fd-9149-17ab5bc67be6-01.01.000.json"
    )
    snapshot = load_snapshot(str(snapshot_path))
    result = build_matrices_from_snapshot(snapshot)

    a_matrix = result["A"]
    issues = result.get("issues", [])
    if issues:
        print("issues:")
        for item in issues:
            print(f"- {item}")
        print()
    size = a_matrix["shape"][0]
    a = np.zeros((size, size), dtype=float)
    for entry in a_matrix["data"]:
        a[entry["row_index"], entry["col_index"]] = entry["value"]

    off_diag = [
        entry for entry in a_matrix["data"] if entry["row_index"] != entry["col_index"]
    ]
    print(f"A non-diagonal entries: {len(off_diag)}")
    for entry in off_diag[:10]:
        print(
            f"{entry['row_index']},{entry['col_index']}\t{entry['row']}\t{entry['col']}\t{entry['value']}"
        )
    if len(off_diag) > 10:
        print("...")
    print()

    try:
        a_inv = np.linalg.inv(a)
    except np.linalg.LinAlgError as exc:
        cond = np.linalg.cond(a)
        print(f"inversion failed: {exc}")
        print(f"matrix condition number: {cond}")
        return

    process_lookup = result.get("process_lookup", {})
    process_ids = a_matrix["rows"]

    output_path = args.output
    if not output_path:
        output_path = str(ROOT / "exports" / "A_inverse.csv")
    _write_inverse_csv(output_path, a_inv, process_ids, process_lookup)
    print(f"wrote requirement matrix to {output_path}")

    topn_path = str(ROOT / "exports" / "A_inverse_topn.csv")
    _write_inverse_topn_csv(topn_path, a_inv, process_ids, process_lookup, args.topn)
    print(f"wrote top-{args.topn} requirements to {topn_path}")

    target = args.process
    if target is not None:
        if target.isdigit():
            target_index = int(target)
        else:
            try:
                target_index = a_matrix["rows"].index(target)
            except ValueError:
                print("unknown process uuid for demand, available:")
                for idx, proc_uuid in enumerate(a_matrix["rows"]):
                    proc_name = process_lookup.get(proc_uuid, {}).get("process_name", "")
                    print(f"{idx}\t{proc_uuid}\t{proc_name}")
                return
        if target_index < 0 or target_index >= size:
            print("demand index out of range")
            return
        print(f"requirements for demand process index: {target_index}")
        for idx, amount in enumerate(a_inv[:, target_index]):
            proc_uuid = process_ids[idx]
            proc_name = process_lookup.get(proc_uuid, {}).get("process_name", "")
            print(f"{idx}\t{proc_uuid}\t{proc_name}\t{amount}")


def _write_inverse_csv(
    path: str,
    a_inv: np.ndarray,
    process_ids: list[str],
    process_lookup: dict,
) -> None:
    import csv
    from pathlib import Path

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        header = ["row_index", "row_process_uuid", "row_process_name"]
        for idx, proc_uuid in enumerate(process_ids):
            name = process_lookup.get(proc_uuid, {}).get("process_name", "")
            header.append(f"col_{idx}:{proc_uuid}:{name}")
        writer.writerow(header)
        for row_idx, proc_uuid in enumerate(process_ids):
            name = process_lookup.get(proc_uuid, {}).get("process_name", "")
            row = [row_idx, proc_uuid, name]
            row.extend(a_inv[row_idx, :].tolist())
            writer.writerow(row)


def _write_inverse_topn_csv(
    path: str,
    a_inv: np.ndarray,
    process_ids: list[str],
    process_lookup: dict,
    topn: int,
) -> None:
    import csv
    from pathlib import Path

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "demand_process_index",
                "demand_process_uuid",
                "demand_process_name",
                "rank",
                "required_process_index",
                "required_process_uuid",
                "required_process_name",
                "value",
            ]
        )
        for col_idx, demand_uuid in enumerate(process_ids):
            demand_name = process_lookup.get(demand_uuid, {}).get("process_name", "")
            col = a_inv[:, col_idx]
            order = np.argsort(-np.abs(col))
            for rank, row_idx in enumerate(order[:topn], start=1):
                req_uuid = process_ids[row_idx]
                req_name = process_lookup.get(req_uuid, {}).get("process_name", "")
                writer.writerow(
                    [
                        col_idx,
                        demand_uuid,
                        demand_name,
                        rank,
                        row_idx,
                        req_uuid,
                        req_name,
                        float(col[row_idx]),
                    ]
                )


if __name__ == "__main__":
    main()
