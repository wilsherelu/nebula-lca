from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the public refinery benchmark package.")
    parser.add_argument("--case-root", type=Path, required=True)
    parser.add_argument("--validation-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    source = args.case_root.resolve()
    output = args.output_dir.resolve()
    if not (source / "acceptance_index.json").is_file():
        raise SystemExit(f"Not a benchmark package: {source}")
    if output.exists():
        shutil.rmtree(output)
    shutil.copytree(source, output)

    index_path = output / "acceptance_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    index.pop("base_url", None)
    health = index.get("health")
    if isinstance(health, dict):
        health.pop("database", None)
    index["limitations"] = [
        "Quantities are declared benchmark assumptions, not a representative refinery inventory.",
        "The PTS comparison covers one exposed-product basis column, not a complete operator basis.",
        "The non-exposed hydrogen by-product is reported separately and is not represented as a compiled boundary exchange.",
    ]
    index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    validation_target = output / "independent_validation"
    validation_target.mkdir()
    for name in (
        "allocation_closure.json",
        "pts_independent_oracle.json",
        "pts_component_comparison.csv",
        "figure5_pts_equivalence.svg",
        "figure5_pts_equivalence.png",
    ):
        shutil.copy2(args.validation_dir.resolve() / name, validation_target / name)

    readme = """# Nebula LCA refinery-inspired benchmark package v1.0

This package contains five progressively composed, hypothetical benchmark projects. It includes immutable input graphs, raw API results, audit reports, provenance, chart data, Sankey figures, and independent allocation and PTS validation evidence.

The quantities are methodological assumptions, not measured refinery data. No licensed background inventory is redistributed. Exact machine paths, runtime database locations, and local service endpoints have been removed from this public copy.
"""
    (output / "README.md").write_text(readme, encoding="utf-8")

    manifest_path = output / "manifest.sha256"
    manifest_path.unlink(missing_ok=True)
    files = sorted(path for path in output.rglob("*") if path.is_file())
    manifest_path.write_text(
        "".join(f"{sha256(path)}  {path.relative_to(output).as_posix()}\n" for path in files),
        encoding="utf-8",
    )
    print(json.dumps({"files": len(files), "output": str(output)}, indent=2))


if __name__ == "__main__":
    main()
