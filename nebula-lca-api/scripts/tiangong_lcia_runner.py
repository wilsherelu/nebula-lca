from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Run tiangong-nebula-solver LCIA and print JSON.")
    parser.add_argument("--solver-root", required=True, help="Path to tiangong-nebula-solver root")
    parser.add_argument("--ef31-dir", required=True, help="Path to EF3.1 folder")
    parser.add_argument("--snapshot", required=True, help="Path to snapshot json")
    args = parser.parse_args()

    solver_root = Path(args.solver_root).resolve()
    ef31_dir = Path(args.ef31_dir).resolve()
    snapshot_path = Path(args.snapshot).resolve()

    if not solver_root.exists():
        raise FileNotFoundError(f"solver root not found: {solver_root}")
    if not ef31_dir.exists():
        raise FileNotFoundError(f"EF3.1 dir not found: {ef31_dir}")
    if not snapshot_path.exists():
        raise FileNotFoundError(f"snapshot not found: {snapshot_path}")

    if str(solver_root) not in sys.path:
        sys.path.insert(0, str(solver_root))

    from app.core.lcia import compute_lcia  # type: ignore
    from app.core.matrix_builder import build_c_matrix_from_ef31, build_matrices_from_snapshot  # type: ignore

    with open(snapshot_path, "r", encoding="utf-8") as handle:
        snapshot = json.load(handle)

    base = build_matrices_from_snapshot(snapshot)
    b_matrix = base["B"]
    c_pack = build_c_matrix_from_ef31(str(ef31_dir), b_matrix, issues=base.get("issues"))
    c_matrix = c_pack["C"]
    indicator_lookup = c_pack.get("indicator_lookup", {})
    b_flow_rows = b_matrix.get("rows", []) or []
    ef_flow_uuid_set: set[str] = set()
    flow_index_path = ef31_dir / "flow_index.csv"
    with flow_index_path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, [])
        if header:
            header[0] = header[0].lstrip("\ufeff")
        col_map = {name: idx for idx, name in enumerate(header)}
        uuid_idx = col_map.get("FlowUUID")
        if uuid_idx is not None:
            for row in reader:
                if uuid_idx < len(row):
                    flow_uuid = row[uuid_idx].strip()
                    if flow_uuid:
                        ef_flow_uuid_set.add(flow_uuid)

    flow_name_map = {
        item.get("flow_uuid", ""): item.get("flow_name", "")
        for item in (snapshot.get("flows", []) or [])
        if item.get("flow_uuid")
    }
    missing_ef31_flow_uuids = [uuid for uuid in b_flow_rows if uuid not in ef_flow_uuid_set]
    missing_ef31_flows = [
        {"flow_uuid": uuid, "flow_name": flow_name_map.get(uuid, "")}
        for uuid in missing_ef31_flow_uuids
    ]

    lcia_values = compute_lcia(base["A"], b_matrix, c_matrix)

    result = {
        "summary": {
            "process_count": len(base["A"]["rows"]),
            "elementary_flow_count": len(b_matrix["rows"]),
            "indicator_count": len(c_matrix["rows"]),
            "issue_count": len(base.get("issues", [])),
            "missing_ef31_flow_count": len(missing_ef31_flow_uuids),
        },
        "issues": base.get("issues", []),
        "missing_ef31_flow_uuids": missing_ef31_flow_uuids,
        "missing_ef31_flows": missing_ef31_flows,
        "indicator_index": [
            {
                "indicator_index": idx,
                **(indicator_lookup.get(idx, {})),
            }
            for idx in c_matrix["rows"]
        ],
        "process_index": base["A"]["rows"],
        "values": lcia_values.tolist(),
    }

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
