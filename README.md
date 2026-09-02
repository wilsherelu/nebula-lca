# Nebula LCA

Nebula LCA is a graph-based life cycle assessment framework built on top of the Tiangong 1.0 data foundation.

It combines:

- visual LCA modeling and editing
- process, flow, and model import
- balancing, normalization, and conservation support
- Partially terminated system (PTS) packaging for modular modeling and controlled publishing
- LCI and LCIA calculation workflows

## Repository Structure

- `nebula-lca-web`: frontend modeling and result interface
- `nebula-lca-api`: backend validation, model management, and orchestration
- `nebula-lca-solver`: matrix construction and LCI/LCIA computation service
- `nebula-lca-desktop`: standalone Windows desktop packaging
- `deploy`: deployment-related files

## Core Capabilities

- Visual modeling for processes, flows, products, and markets
- Model import from Tiangong assets
- Rule-guided modeling with normalization and conservation support
- Partially terminated system (PTS) packaging for reusable and privacy-aware modular release
- Calculation and impact assessment result presentation

## Windows Desktop

Download one of the Windows packages from the GitHub Releases page:

- **Installer**: run `Nebula LCA Setup 0.1.0.exe` and choose an installation directory.
- **Portable ZIP**: extract the entire archive, keep the `win-unpacked` directory intact, and run `Nebula LCA.exe` inside it. Do not run the application directly from the ZIP or copy only the executable.

Application data is stored separately under `%APPDATA%\nebula-lca-desktop`, so uninstalling or replacing the application files does not automatically remove the local database and settings.

> The current Windows packages are not code-signed. Windows Defender SmartScreen may display an "unknown publisher" warning. Verify the SHA-256 checksum published in the corresponding Release before running a downloaded package.

## Web / Docker Quick Start

1. Copy `env.example` to `.env`
2. Start the stack from the repository root

```bash
docker compose up -d --build
```

3. Open the web interface at `http://localhost:16988` by default

For deployment details, see `deploy/README.md`.
