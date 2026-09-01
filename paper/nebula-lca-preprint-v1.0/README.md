# Nebula LCA preprint v1.0

This directory contains the Quarto manuscript, Supporting Information, independent validation scripts, publication figures, and the public refinery-inspired benchmark package used by the paper.

## Rebuild

Install Quarto and TinyTeX, then run from Command Prompt:

```cmd
build.cmd D:\path\to\the\frozen\five-case-package
```

When the argument is omitted, the script uses the local development snapshot under `tmp/refinery-paper-cases/20260831T135829Z`. The public package deliberately removes local API endpoints and database paths.

## Release files

The two PDFs in `output/`, the `benchmark-package/` directory, the manuscript sources, and the generated SHA-256 manifests form the Zenodo-ready research object. A DOI is not fabricated in the source; reserve the Zenodo DOI and insert it before the final public deposit if desired.
