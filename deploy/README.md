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
Large upload sessions and generated LCIA runtime artifacts are persisted in:

- `nebula-lca-api-import-cache`
- `nebula-lca-api-runtime` shared by API and solver, so uploaded ecoinvent LCIA
  methods are visible during calculation.

For a full ecoinvent LCI/LCIA import, reserve enough disk space for the uploaded
archives, extracted working files, SQLite vectors, and generated runtime files.
For a clean cloud test, 30-50 GB free disk is a practical lower bound.

## First-Time Deployment

From the repository root:

```bash
cp env.example .env
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

## Clean Reinstall Test

Use this only when you intentionally want to remove all runtime project data,
uploaded import files, and generated LCIA runtime artifacts:

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
```

Then watch startup:

```bash
docker compose ps
docker compose logs -f nebula-lca-api
```

Stop following logs with `Ctrl+C`; this does not stop the containers.

## Runtime Behavior

- On first startup, the backend may initialize the database and bootstrap baseline data once.
- On later restarts, the named volume keeps the runtime database.
- Baseline data stays mounted read-only from the source tree.
- LCI/LCIA upload sessions are chunked and resumable. The import cache volume
  keeps uploaded chunks and merged archives across container restarts.
- Completed and cancelled import jobs clean up their uploaded archive and
  extracted working directory automatically. Failed or paused jobs are retained
  for `IMPORT_CACHE_RETENTION_HOURS` so they can be retried or inspected.
- Generated LCIA runtime artifacts are stored in the runtime volume.
- The web container proxies `/api/*` to the backend over the internal Docker network.

## LCI/LCIA Import After Deployment

Full LCIA use requires two ecoinvent archives:

1. Upload the LCI archive first, usually ending with
   `cutoff_lci_ecoSpold02.7z`.
2. Upload the LCIA archive next, ending with `LCIA_implementation.7z`, to
   generate the LCIA runtime.

The filename may include a version prefix, for example an ecoinvent 3.x prefix.
The UI defaults to a full import with 8 parser workers; the backend caps the
effective worker count to the CPU count visible inside the container, up to 8.
On a typical cloud Docker host, a full import may take about 1 hour depending on
CPU, disk, and package version.

Advanced settings can limit the import to the first N datasets for batched
validation, or enable overwrite import to repair old vectors or refresh package
versions.

## Upgrade

After updating the source code:

```bash
docker compose down
docker compose up -d --build
```

To keep existing project data, do not remove the named volume.

If imports are running or uploaded archives should be kept, do not remove the
`nebula-lca-api-import-cache` volume.

## Import Cache Cleanup

The default cleanup settings are:

```bash
IMPORT_CACHE_CLEANUP_ON_TERMINAL=true
IMPORT_CACHE_RETENTION_HOURS=24
```

This prevents completed full imports from leaving tens of GB of uploaded
archives and extracted working files in the Docker volume. If a failed import
needs deeper debugging, copy the relevant logs before the retention window
expires or increase `IMPORT_CACHE_RETENTION_HOURS` temporarily.

## Notes

- Only the frontend port is exposed publicly.
- The API and solver stay on the internal Docker network.
- If the browser still shows an old frontend after upgrade, force refresh the page.
- Nginx accepts 512 MB request bodies and disables proxy request buffering for
  `/api/*`; the frontend uploads LCI/LCIA archives in 64 MB chunks.
