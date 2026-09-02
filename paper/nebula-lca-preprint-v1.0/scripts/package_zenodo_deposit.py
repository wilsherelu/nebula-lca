from __future__ import annotations

import hashlib
from pathlib import Path
import shutil
from zipfile import ZIP_DEFLATED, ZipFile


PAPER_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = PAPER_DIR.parents[1]
OUTPUT_DIR = PAPER_DIR / "output"


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_zip(path: Path, files: list[tuple[Path, str]]) -> None:
    path.unlink(missing_ok=True)
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        for source, name in sorted(files, key=lambda item: item[1]):
            archive.write(source, name)


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    manuscript = OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0.pdf"
    supporting = OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-Supporting-Information.pdf"
    if not manuscript.is_file() or not supporting.is_file():
        raise SystemExit("Render the manuscript and Supporting Information PDFs before packaging")

    final_manuscript = OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-final.pdf"
    final_supporting = OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-Supporting-Information-final.pdf"
    shutil.copy2(manuscript, final_manuscript)
    shutil.copy2(supporting, final_supporting)

    benchmark_root = PAPER_DIR / "benchmark-package"
    manifest = benchmark_root / "manifest.sha256"
    manifest.unlink(missing_ok=True)
    benchmark_files = sorted(path for path in benchmark_root.rglob("*") if path.is_file())
    manifest.write_text(
        "".join(f"{file_hash(path)}  {path.relative_to(benchmark_root).as_posix()}\n" for path in benchmark_files),
        encoding="utf-8",
    )
    benchmark_files.append(manifest)
    benchmark_zip = OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-Benchmark-Package.zip"
    write_zip(benchmark_zip, [(path, path.relative_to(benchmark_root).as_posix()) for path in benchmark_files])

    source_paths: list[Path] = []
    for relative in ("data", "figures", "scripts"):
        source_paths.extend(path for path in (PAPER_DIR / relative).rglob("*") if path.is_file())
    for name in (
        "build.cmd", "CITATION.cff", "header.tex", "LICENSES.md", "paper.qmd", "paper.tex",
        "README.md", "supporting_information.qmd", "supporting_information.tex",
    ):
        source_paths.append(PAPER_DIR / name)
    source_zip = OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-Manuscript-Source.zip"
    write_zip(source_zip, [(path, path.relative_to(PAPER_DIR).as_posix()) for path in source_paths])

    deposit_sources = [
        PAPER_DIR / "CITATION.cff",
        PAPER_DIR / "LICENSES.md",
        PAPER_DIR / "README.md",
        PAPER_DIR / "data" / "ef31_indicator_comparison.csv",
        PAPER_DIR / "data" / "pts_component_comparison.csv",
        REPO_DIR / "LICENSE",
        benchmark_zip,
        OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-Editable-Figures.pptx",
        final_manuscript,
        source_zip,
        final_supporting,
    ]
    deposit_names = {
        PAPER_DIR / "CITATION.cff": "CITATION.cff",
        PAPER_DIR / "LICENSES.md": "LICENSES.md",
        PAPER_DIR / "README.md": "README.md",
        PAPER_DIR / "data" / "ef31_indicator_comparison.csv": "ef31_indicator_comparison.csv",
        PAPER_DIR / "data" / "pts_component_comparison.csv": "pts_component_comparison.csv",
        REPO_DIR / "LICENSE": "SOFTWARE-LICENSE-APACHE-2.0.txt",
    }
    checksum_path = OUTPUT_DIR / "SHA256SUMS.txt"
    checksum_path.write_text(
        "".join(f"{file_hash(path)}  {deposit_names.get(path, path.name)}\n" for path in deposit_sources),
        encoding="utf-8",
    )
    deposit_sources.append(checksum_path)
    files = [(path, deposit_names.get(path, path.name)) for path in deposit_sources]
    zenodo_zip = OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-Zenodo-Bundle.zip"
    write_zip(zenodo_zip, files)
    shutil.copy2(zenodo_zip, OUTPUT_DIR / "Nebula-LCA-Preprint-v1.0-Zenodo-Pre-Reservation-Bundle.zip")
    print(f"Packaged {len(files)} Zenodo deposit files: {zenodo_zip}")


if __name__ == "__main__":
    main()
