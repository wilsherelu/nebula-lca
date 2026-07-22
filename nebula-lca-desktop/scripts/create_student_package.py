"""Build a portable student ZIP from the unpacked Electron app and local reference DB."""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import tempfile
from pathlib import Path


SENSITIVE_TABLES = (
    "data_platform_account_sessions",
    "data_platform_accounts",
    "data_platform_remote_cache",
    "data_platform_sync_jobs",
    "debug_diagnostics",
    "external_data_sync_records",
    "models",
    "model_versions",
    "run_jobs",
    "import_job_pause_requests",
    "import_jobs",
    "dataset_checkpoints",
    "pts_compile_artifacts",
    "pts_definitions",
    "pts_external_artifacts",
    "pts_resources",
)


LAUNCHER = """@echo off
setlocal
set \"ROOT=%~dp0\"
set \"NEBULA_DB_PATH=%ROOT%student-data\\nebula-lca.db\"
start \"\" \"%ROOT%app\\Nebula LCA.exe\"
"""


README = """Nebula LCA 学生版

1. 将 ZIP 完整解压到本机可写目录，不要在压缩包内直接运行。
2. 双击“启动学生版.cmd”。
3. 本包已包含参考数据库；学生的模型和计算记录会保存在自己的数据目录中。

不要将 student-data/nebula-lca.db 删除或移动，否则参考数据将不可用。
"""


def create_student_database(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as read_conn, sqlite3.connect(target) as write_conn:
        read_conn.backup(write_conn)
        write_conn.execute("PRAGMA foreign_keys=OFF")
        for table in SENSITIVE_TABLES:
            write_conn.execute(f"DELETE FROM [{table}]")
        write_conn.commit()
        write_conn.execute("VACUUM")

    with sqlite3.connect(target) as conn:
        retained_flows = conn.execute("SELECT count(*) FROM flow_catalog").fetchone()[0]
        retained_processes = conn.execute("SELECT count(*) FROM reference_processes").fetchone()[0]
        remaining_accounts = conn.execute("SELECT count(*) FROM data_platform_accounts").fetchone()[0]
    if retained_flows <= 0 or retained_processes <= 0 or remaining_accounts != 0:
        raise RuntimeError("Student database validation failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--unpacked", type=Path, required=True)
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    unpacked = args.unpacked.resolve()
    source_db = args.source_db.resolve()
    output = args.output.resolve()
    if not (unpacked / "Nebula LCA.exe").is_file():
        raise FileNotFoundError(f"Unpacked Electron app not found: {unpacked}")
    if not source_db.is_file():
        raise FileNotFoundError(f"Source database not found: {source_db}")
    output.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="nebula-lca-student-", dir=output.parent) as temp_dir:
        root = Path(temp_dir) / "Nebula-LCA-Student"
        shutil.copytree(unpacked, root / "app")
        create_student_database(source_db, root / "student-data" / "nebula-lca.db")
        (root / "启动学生版.cmd").write_text(LAUNCHER, encoding="utf-8", newline="\r\n")
        (root / "README.txt").write_text(README, encoding="utf-8", newline="\r\n")
        archive_base = output.with_suffix("")
        created = Path(shutil.make_archive(str(archive_base), "zip", root_dir=root.parent, base_dir=root.name))
        if created != output:
            created.replace(output)

    print(output)


if __name__ == "__main__":
    main()
