# Nebula LCA

Nebula LCA is a graph-based life cycle assessment framework built on top of the Tiangong 1.0 data foundation.

It combines:

- visual LCA modeling and editing
- process, flow, and model import
- balancing, normalization, and conservation support
- PTS packaging for modular modeling and controlled publishing
- LCI and LCIA calculation workflows

## Repository Structure

- `nebula-lca-web`: frontend modeling and result interface
- `nebula-lca-api`: backend validation, model management, and orchestration
- `nebula-lca-solver`: matrix construction and LCI/LCIA computation service
- `deploy`: deployment-related files

## Core Capabilities

- Visual modeling for processes, flows, products, and markets
- Model import from Tiangong assets
- Rule-guided modeling with normalization and conservation support
- PTS packaging for reusable and privacy-aware modular release
- Calculation and impact assessment result presentation

## Quick Start

1. Copy `env.example` to `.env`
2. Start the stack from the repository root

```bash
docker compose up -d --build
```

3. Open the web interface at `http://localhost:16988` by default

For deployment details, see `deploy/README.md`.
