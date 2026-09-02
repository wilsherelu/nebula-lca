# Nebula LCA preprint v1.0

This directory contains the Quarto manuscript, Supporting Information, independent validation scripts, publication figures, and the public refinery-inspired benchmark package used by the paper. In this package, PTS means *partially terminated system*: an aggregated representation that retains selected intermediate technosphere exchanges for further modeling. The benchmark quantities are hypothetical and do not represent measured refinery performance.

## Rebuild

Install Quarto and TinyTeX, then run from Command Prompt:

```cmd
build.cmd D:\path\to\the\frozen\five-case-package
```

When the argument is omitted, the script uses the local development snapshot under `tmp/refinery-paper-cases/20260831T135829Z`. The public package deliberately removes local API endpoints and database paths.

## Release files

The two final PDFs in `output/` are the manuscript and Supporting Information. `benchmark-package/` contains the five frozen input snapshots, raw API results, audit reports, provenance, plot data, and its own manifest. `scripts/independent_validation.py` reconstructs the Case 1 allocation and Case 5 PTS results without importing the production solver. `data/pts_component_comparison.csv` records the identity-indexed boundary comparison, and `data/ef31_indicator_comparison.csv` records the 25 EF 3.1 outputs with corrected identity-matched labels and units while preserving the raw numeric results. The editable PowerPoint and exported figure assets are included for figure provenance.

The executed receipts pin Nebula LCA engine commit `4c7cb4b69ae6a0647e3e08d048d6c6724cd93ae3` and TIDAS contract commit `b10585ba7695ec66636d5078d970ac54105bdf0e`. The manuscript identifies the European Commission EF 3.1 reference package released in July 2022 and the repository method metadata used for the comparison. Licensed background inventories are not redistributed.

The final Zenodo deposit should include the manuscript PDF, Supporting Information PDF, source bundle, benchmark ZIP, editable figures, comparison tables, `CITATION.cff`, license notice, and `SHA256SUMS.txt`. Reserve the Zenodo DOI first, insert it into the manuscript, README, and citation metadata, then rebuild and hash the immutable upload files. No DOI is fabricated in this pre-reservation source.
