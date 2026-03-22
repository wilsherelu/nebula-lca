from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.core.matrix_builder import build_matrices_from_snapshot
from proto import mmr_pb2


def build_mmr(
    snapshot: dict,
    mmr_version: str = "1.0",
    solver_version: str = "matrix_builder_v0.1",
) -> mmr_pb2.MMR:
    matrices = build_matrices_from_snapshot(snapshot)

    model_info = snapshot.get("model", {}) or {}
    source_snapshot_id = model_info.get("model_id") or model_info.get("model_uuid") or ""

    mmr = mmr_pb2.MMR()
    mmr.mmr_version = mmr_version
    mmr.generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    mmr.source_snapshot_id = str(source_snapshot_id)
    mmr.solver_version = solver_version
    mmr.params["allocation_rule"] = "allocation_fraction_non_null_sum_amount"

    a_matrix = matrices["A"]
    b_matrix = matrices["B"]

    mmr.process_index.extend(a_matrix["rows"])
    mmr.elementary_flow_index.extend(b_matrix["rows"])
    _fill_sparse_matrix(mmr.A, a_matrix)
    _fill_sparse_matrix(mmr.B, b_matrix)
    return mmr


def write_mmr(mmr: mmr_pb2.MMR, output_path: str) -> Path:
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(mmr.SerializeToString())
    return out_path


def timestamp_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def default_mmr_path(output_dir: str) -> Path:
    return Path(output_dir) / f"mmr_{timestamp_tag()}.pb"


def export_mmr_from_snapshot(
    snapshot_path: str,
    output_path: str,
    mmr_version: str = "1.0",
    solver_version: str = "matrix_builder_v0.1",
) -> Path:
    from app.core.matrix_builder import load_snapshot

    snapshot = load_snapshot(snapshot_path)
    mmr = build_mmr(snapshot, mmr_version=mmr_version, solver_version=solver_version)
    return write_mmr(mmr, output_path)


def read_mmr(path: str) -> mmr_pb2.MMR:
    mmr = mmr_pb2.MMR()
    mmr.ParseFromString(Path(path).read_bytes())
    return mmr


def _fill_sparse_matrix(target: mmr_pb2.SparseMatrix, matrix: dict) -> None:
    target.rows = int(matrix["shape"][0])
    target.cols = int(matrix["shape"][1])
    for entry in matrix.get("data", []):
        item = target.entries.add()
        item.row = int(entry["row_index"])
        item.col = int(entry["col_index"])
        item.value = float(entry["value"])
