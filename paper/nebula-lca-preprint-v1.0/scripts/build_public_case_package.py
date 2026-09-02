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
    parser.add_argument("--native-bundle-dir", type=Path)
    args = parser.parse_args()

    source = args.case_root.resolve()
    output = args.output_dir.resolve()
    if not (source / "acceptance_index.json").is_file():
        raise SystemExit(f"Not a benchmark package: {source}")
    preserved_native_bundles = {}
    if args.native_bundle_dir is None and output.exists():
        preserved_native_bundles = {
            path.parent.name: path.read_bytes()
            for path in output.glob("case_*/native_project.nebula.zip")
        }
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
        "ef31_indicator_comparison.csv",
    ):
        shutil.copy2(args.validation_dir.resolve() / name, validation_target / name)

    if args.native_bundle_dir:
        bundle_source = args.native_bundle_dir.resolve()
        case_dirs = sorted(path for path in output.glob("case_*") if path.is_dir())
        bundle_files = sorted(bundle_source.glob("case-*.nebula.zip"))
        if len(bundle_files) != len(case_dirs):
            raise SystemExit("Expected exactly one native bundle for each benchmark case")
        for case_dir, bundle_file in zip(case_dirs, bundle_files, strict=True):
            shutil.copy2(bundle_file, case_dir / "native_project.nebula.zip")
    else:
        for case_name, payload in preserved_native_bundles.items():
            case_dir = output / case_name
            if case_dir.is_dir():
                (case_dir / "native_project.nebula.zip").write_bytes(payload)

    readme = """# Nebula LCA refinery-inspired benchmark package v1.0

This package contains five progressively composed, hypothetical benchmark projects. PTS means partially terminated system: an aggregated representation that retains selected intermediate technosphere exchanges for further modeling. The package includes immutable input graphs, raw API results, audit reports, provenance, chart data, Sankey figures, independent allocation and PTS validation evidence, and one importable native Nebula project backup per case.

Restore `native_project.nebula.zip` from Nebula LCA project management or POST it to `/api/native-project-bundles/import`. Licensed background inventory is represented only as an external dependency and must be supplied separately by an authorized user.

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
