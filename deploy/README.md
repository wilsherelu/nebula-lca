# Nebula LCA Deployment

This project can be deployed directly from the repository root with Docker Compose.

## Required Files

- `docker-compose.yml`
- `.env`
- `nebula-lca-web/`
- `nebula-lca-api/`
- `nebula-lca-solver/`

Reference data is mounted from:

- `nebula-lca-api/data/Tiangong`
- `nebula-lca-api/data/EF3.1`

Runtime SQLite data is persisted in the named volume `nebula-lca-api-data`.

## First-Time Deployment

From the repository root:

```bash
cp .env.example .env
docker compose up -d --build
```

Then verify:

```bash
docker compose ps
docker compose logs --tail 100 nebula-lca-api
docker compose logs --tail 100 nebula-lca-solver
docker compose logs --tail 100 nebula-lca-web
```

Default frontend address:

```text
http://<host-ip>:16988
```

If `WEB_PORT` is changed in `.env`, use that port instead.

## Runtime Behavior

- On first startup, the backend may initialize the database and bootstrap baseline data once.
- On later restarts, the named volume keeps the runtime database.
- Baseline data stays mounted read-only from the source tree.

## Upgrade

After updating the source code:

```bash
docker compose down
docker compose up -d --build
```

To keep existing project data, do not remove the named volume.

## Notes

- Only the frontend port is exposed publicly.
- The API and solver stay on the internal Docker network.
- If the browser still shows an old frontend after upgrade, force refresh the page.
