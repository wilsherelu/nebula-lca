# Historical Archive Index

Last updated: 2026-05-24

This index records completed or superseded development notes so future agents do not treat old handoff files as the current plan. Current active ecoinvent LCI import follow-up work lives in `docs/dev/ecoinvent-lci-import-future-plan.md`.

## ecoinvent LCI Import and Runtime

| Path | Stage | Topic | Current status | Current reference |
| --- | --- | --- | --- | --- |
| `agent-memory/2026-05-24-ecoinvent-import-and-runtime-handoff.md` | 2026-05-24 handoff | ecoinvent import/runtime state before stale-job and performance follow-up | Historical context. The stale-job fix, MasterData reuse, fast skip, batch writer, Core upsert, and streaming aggregation have since moved into implementation or the active follow-up plan. | `docs/dev/ecoinvent-lci-import-future-plan.md` |
| `agent-memory/2026-05-20-ecoinvent-compressed-lci-runtime-plan.md` | 2026-05-20 plan | compressed LCI vector runtime direction | Historical design input. Do not use as the latest task list without checking current code and the active plan. | `docs/dev/ecoinvent-lci-import-future-plan.md`, `docs/dev/lci-lcia-algorithm.md` |
| `agent-memory/2026-05-19-ecoinvent-lci-adaptation-alignment.md` | 2026-05-19 alignment | ecoinvent LCI adaptation and source-system alignment | Historical alignment note. Current source-system guardrails are summarized in the active ecoinvent LCI import plan. | `docs/dev/ecoinvent-lci-import-future-plan.md` |
| `docs/dev/ecoinvent-lci-import-future-plan.md` | active plan | ecoinvent LCI import performance, MasterData refresh split, runtime guardrails | Current active plan. Its "已完成归档" section lists completed items that should not be re-planned. | Same file |

## Completed ecoinvent Import Items

These items are archived as completed unless new regressions are proven by tests or local import runs:

- stale job recovery and 10-minute frontend auto-attach guard.
- MasterData reuse for incremental import and resumable import.
- global imported metadata-only fast skip.
- parser/queue/single-writer pipeline.
- batch writer replacing per-dataset flush on the main path.
- worker-side `LciWritePlan` aggregation.
- SQLite Core upsert and streaming aggregation path.
- official broad LCIA runtime overwrite guard.
- ecoinvent MasterData priority over historical conflicting flow rows.

## Documentation Cleanup Rules

- Keep current architecture and restart instructions in `agent-memory/main-handoff.md`.
- Keep active development work in topic-specific files under `docs/dev/`.
- Keep ecoinvent import follow-up in `docs/dev/ecoinvent-lci-import-future-plan.md`; do not create a generic `future-development-plan.md` for this topic.
- Keep historical long-form handoffs indexed here instead of copying them into active plans.
