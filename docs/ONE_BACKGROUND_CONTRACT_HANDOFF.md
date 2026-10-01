# Nebula One background-provider API contracts

Scope: versioned open-leaf and mapped-background provider contracts. Native inventory and native climate contracts are documented separately in [native climate](PROVIDER_NATIVE_CLIMATE_TOTAL_V1.md) and [native inventory design and implementation status](PROVIDER_NATIVE_LCI_ONLY_V1_DESIGN.md). This document does not authorize redistribution of licensed ecoinvent data. Steel seed versions discussed below describe their explicit versioned migration rather than the state of every installation.

## 1. Current public mapping contract

Nebula LCA contains an accepted, data-only `nebula-flow-mapping` release at `nebula-lca-api/data/flow_mappings/nebula-flow-mapping-v1`:

- schema: `nebula-flow-mapping-release.v1`
- dataset version: `1.0.0`
- direction: `TIANGONG_TO_ECOINVENT`
- ecoinvent compatibility: `3.11`
- intermediate mappings: 1,379 (`L1=147`, `L2=1,232`)
- elementary mappings: 9,764 (`L1=6,354`, `L2=3,410`)
- manifest file hashes and package acceptance are checked before the registry is loaded

The public package contains Flow interoperability identifiers and unit-conversion rules. It does **not** contain ecoinvent Process inventories, matrices, caches, characterization datasets, or a right to use ecoinvent. A user must provide a lawful local database copy.

### Resolve a TianGong intermediate Flow

Existing endpoint:

```http
POST /api/intermediate-flow-links/resolve-batch
Content-Type: application/json
```

```json
{
  "items": [
    {
      "node_id": "blast-furnace",
      "port_id": "electricity-in",
      "flow_uuid": "<exact-tiangong-flow-uuid>",
      "flow_source_namespace": "tiangong_open_data",
      "flow_version": "<exact-tidas-version>",
      "direction": "input",
      "exchange_type": "technosphere",
      "unit": "MJ",
      "unit_group": "Units of energy"
    }
  ],
  "include_unreviewed_candidates": false,
  "l2_limit": 5
}
```

The response freezes `package_id`, `package_version`, `package_hash`, mapping level, exact target Flow UUID, amount factor, units, rule ID, relationship, warnings, and whether manual confirmation is required. L1 can be applied automatically. L2 must remain reviewable and should be explicitly confirmed through `POST /api/intermediate-flow-links/confirm-l2`. Cross-unit-group use requires an explicit positive conversion factor; the service does not guess one.

The endpoint is not a name matcher. The source Flow must exist with its exact source namespace and version. A template-owned/custom UUID is therefore not a substitute for a TianGong Flow UUID and will not acquire an authoritative mapping merely because its name is “electricity”, “iron ore”, or similar.

### Find locally licensed provider candidates

After resolving the target ecoinvent Flow UUID:

```http
GET /api/intermediate-flow-links/providers?target_flow_uuid=<ecoinvent-flow-uuid>
```

This returns provider candidates found in the user's local database. It does not auto-select a provider (`auto_selected=false`). A catalog candidate alone is not a solvable background leaf and must not be represented as one.

Relevant implementation:

- `nebula-lca-api/app/api/intermediate_flow_links.py`
- `nebula-lca-api/app/services/intermediate_flow_linking_service.py`
- `nebula-lca-api/app/services/public_flow_mapping_service.py`
- `nebula-lca-api/app/release_assets.py`

## 2. Graph serialization and `providerByPort`

The web editor persists Flow identity and mapping evidence on each port through `intermediateFlowLink`, including source and target UUIDs, source namespace/version, amount factor, units, mapping level/reason, rule ID/origin/status, and mapping package ID/version/hash. It also serializes graph edges with provider/consumer amounts and intermediate-link rule/factor fields.

`NodeInspector.providerByPortId` is only a derived UI view. It scans graph edges whose target is the inspected input and whose source is a hidden `lci_dataset`/provider node. The map itself is not serialized. Persistence is the provider node, edge, port identities, and `intermediateFlowLink` evidence.

This editor representation is useful evidence, but `/api/provider/v1/solve` does not currently consume `providerByPortId` or an `intermediateFlowLink` as an executable cross-database background Process binding.

Relevant implementation:

- `nebula-lca-web/src/model/node.ts`
- `nebula-lca-web/src/model/exchange.ts`
- `nebula-lca-web/src/store/lcaGraphStore.ts`
- `nebula-lca-web/src/components/Inspector/NodeInspector.tsx`

## 3. Current provider-v1 background solve contract

The executable background contract is `background_process_pins` on:

```http
POST /api/provider/v1/solve
```

A pin currently requires:

```json
{
  "schema_version": "provider.background-process-pin.v1",
  "consumer_exchange_id": "blast-furnace::electricity-in",
  "source_namespace": "tiangong_open_data",
  "process_uuid": "<exact-process-uuid>",
  "version": "01.01.000",
  "expected_process_content_hash": "<sha256>",
  "expected_process_snapshot_hash": "<sha256>",
  "quantitative_reference_exchange_internal_id": "2",
  "claim_role": "partial_background_leaf"
}
```

The engine loads the configured read-only snapshots from:

- `NEBULA_PROVIDER_TIDAS_FLOW_SNAPSHOT_PATH`
- `NEBULA_PROVIDER_TIDAS_PROCESS_SNAPSHOT_PATH`
- `NEBULA_PROVIDER_TIDAS_REFERENCE_DEPENDENCY_SNAPSHOT_PATH`

It then validates the exact Process version and hashes. The Process quantitative-reference Flow must exactly match the consumer port in namespace, UUID, version, Flow Property UUID/version, Unit Group UUID/version, and unit. The selected Process must be a closed leaf: other Process exchanges may only be resolvable elementary flows. Technosphere dependencies inside the provider Process are rejected. Duplicate providers and already-connected consumer ports are also rejected.

Successful solve returns `background_process_receipts` and provenance containing the pins hash, Flow snapshot hash, Process snapshot/content hashes, scaled exchanges, and the claim limit:

```text
partial_background_leaf_only_not_complete_cradle_to_gate
```

This is intentionally not a complete cradle-to-gate or Scope 3 claim.

Relevant implementation:

- `nebula-lca-api/app/provider_schemas.py`
- `nebula-lca-api/app/api/provider_v1.py`
- `nebula-lca-api/app/services/provider_background_leaf.py`
- `nebula-lca-api/app/services/provider_v1.py`
- `nebula-lca-api/tests/test_provider_background_leaf.py`

## 4. What One can consume now

For a template port that already uses an exact TianGong Flow identity, One can:

1. call `resolve-batch` and retain the complete mapping receipt;
2. require confirmation for L2 or cross-group mappings;
3. call `/providers` for the mapped ecoinvent Flow against the user's licensed local catalog;
4. save the selected provider and mapping evidence in its revision;
5. submit `background_process_pins` only when the selected Process is an exact open-TianGong closed leaf satisfying the current provider-v1 pin contract.

The old steel seed uses seed-owned/custom technosphere Flow UUIDs. Those ports cannot be mapped authoritatively by the public package and cannot be pinned to TianGong background leaves under the current exact-identity solve contract. The steel template must first replace those identities with authoritative, versioned TianGong/TIDAS Flow identities where available. Names must not be used as a migration rule.

## 5. Mapped licensed-provider contract

Provider v1 now accepts `mapped_background_process_pins` in addition to the unchanged `background_process_pins` v1 contract. Each mapped pin combines:

- exact consumer TianGong Flow identity;
- accepted `nebula-flow-mapping` package ID/version/hash and rule evidence;
- exact target ecoinvent Flow identity and ecoinvent release;
- exact locally licensed provider Process identity/content hash;
- explicit unit conversion and confirmation state;
- a solve-time receipt proving which local database release supplied the inventory.

Schema: `provider.mapped-background-process-pin.v1`. Required fields are `consumer_exchange_id`, exact source namespace/UUID/version/unit, target ecoinvent Flow UUID/unit, amount factor, mapping level/status/rule/package ID/version/hash, database release, local Process UUID, expected Process content hash, and expected vector checksum.

```json
{
  "mapped_background_process_pins": [
    {
      "schema_version": "provider.mapped-background-process-pin.v1",
      "consumer_exchange_id": "blast-furnace::electricity-in",
      "source_flow_namespace": "tiangong_open_data",
      "source_flow_uuid": "<exact-tiangong-uuid>",
      "source_flow_version": "<exact-tidas-version>",
      "target_flow_uuid": "<mapped-ecoinvent-flow-uuid>",
      "source_unit": "MJ",
      "target_unit": "kWh",
      "amount_factor": 0.2777777777777778,
      "mapping_level": "L2",
      "mapping_status": "user_confirmed",
      "mapping_rule_id": "nebula-flow-mapping-v1:intermediate:<exact-tiangong-uuid>",
      "mapping_package_id": "nebula-flow-mapping",
      "mapping_package_version": "1.0.0",
      "mapping_package_hash": "<sha256>",
      "database_release": "3.11",
      "process_uuid": "<local-licensed-process-uuid>",
      "expected_process_content_hash": "<sha256-from-providers-endpoint>",
      "expected_vector_checksum": "<sha256-from-providers-endpoint>",
      "claim_role": "partial_background_leaf"
    }
  ]
}
```

The engine validates the source Flow against the configured exact TianGong snapshot, validates the mapping against the accepted public package, requires explicit confirmation for L2, resolves the target Flow and Process only from the user's local database, verifies release/content/vector hashes, and expands the local compressed inventory vector at solve time. Elementary vector rows must have an unambiguous accepted mapping to an exact standard Flow; missing or ambiguous mappings fail closed. The response freezes `mapped_background_process_receipts` and `provenance.mapped_background_process_pins_hash` while retaining the partial-leaf claim limit.

`GET /api/intermediate-flow-links/providers` now includes `database_release`, `vector_checksum`, and `process_content_hash` for executable local-vector candidates. A candidate with no vector or missing hashes remains non-executable.

The implementation reads licensed inventory only from local database tables. No commercial Process inventory, matrix, vector, or cache is included in source, tests, public seeds, or response fixtures. The contract still represents an individually selected partial background leaf; it does not establish complete Scope 2, Scope 3, or cradle-to-gate coverage.

The current steel seed remains blocked from this contract because its principal technosphere inputs use seed-owned UUIDs. Exact authoritative TianGong UUID/version replacements are required before One can request mappings. The current audit has not identified authoritative replacements for all steel inputs, so no name-based substitutions are proposed.

## 6. Steel seed exact-Flow audit

The versioned machine-readable result is `nebula-lca-api/data/provider_profiles/steel_external_flows.v1.json`. It audits the tracked `nebula.iron-steel.bf-bof-crude-steel` 1.1.0 graph at Nebula One commit `ad37d2107e7a7427cfc4808e9af6857abcf99a3f`. The audit does not modify that graph.

One replacement is ready for a new template version: both seed-owned grid-electricity inputs may use TianGong Flow `3d76981f-964a-4865-b588-0e067a2a1163@01.01.003`, with Net calorific value `93a60a56-a3c8-11da-a746-0800200c9a66@03.00.003`, Units of energy `93a60a57-a3c8-11da-a746-0800200c9a66@03.00.003`, and unit `MJ`. The accepted mapping is L2 to ecoinvent Flow `759b89bd-3aa6-42ad-b767-5bb9ef5d331d`, using `energy-mj-to-kwh` and factor `1/3.6`; explicit user confirmation remains mandatory. The current `600 MJ` sintering and `25 MJ` casting amounts are preserved by the identity migration. A licensed provider Process is still selected locally and must be frozen by database release, vector checksum, and Process content hash.

No other old external input is ready for automatic replacement. Iron ore and limestone candidates do not prove the seed's composition assumptions; coke breeze and metallurgical coke have precise TianGong candidates but no accepted mapping; PCI coal is ambiguous across grades and heating-value bases; natural gas conflicts with the seed's pure-methane mass basis; steel scrap does not prove the seed's composition and has no accepted mapping. A steelworks-specific BOF oxygen Flow exists, but the old graph has no oxygen port or amount, so adding it is a process-balance change rather than an identity migration.

The live exact electricity pull succeeded. A full current reference-master pull failed closed because an unrelated Flow Property references unavailable Unit Group `f6b07a80-d411-312b-9815-f8e4cb8b1bd4`; the runtime therefore uses a minimal exact open-data dependency snapshot containing only the pinned Net calorific value and Units of energy revisions. Those two records were fetched through the authenticated TianGong contract, not copied from a synthetic fixture. Their public identities, source display metadata, content hashes, and snapshot hashes are recorded in `nebula-lca-api/data/provider_profiles/steel_electricity_tiangong_metadata.v1.json`. No commercial provider Process or inventory is stored in that asset.

## 7. Steel electricity identity-upgrade artifact

The derived graph artifact is `nebula-lca-api/data/provider_profiles/steel_electricity_identity_upgrade.v1.json`. It creates template version `1.2.0` from the audited `1.1.0` graph without changing the original seed. Its only graph edits are:

- `ore-sintering::grid-electricity-in`: preserve `600 MJ`, replace the seed-owned Flow identity with `tiangong_open_data/3d76981f-964a-4865-b588-0e067a2a1163/01.01.003`;
- `continuous-casting::grid-electricity-in`: preserve `25 MJ`, apply the same exact Flow identity.

Both ports also pin Net calorific value `93a60a56-a3c8-11da-a746-0800200c9a66@03.00.003` and Units of energy `93a60a57-a3c8-11da-a746-0800200c9a66@03.00.003`. The exact canonical graph SHA-256 is `1f2f763134c2fe61eb75aa0ce1cc5a42daf9b6a026809d8050c7221c92d94d98`; the provider-normalized graph hash is `27b6de665c8db7062d1d94518aecdea35330cf7b84a4e781c62be97d3a20accf`.

The minimum public-API path is:

1. create or reuse an authenticated TianGong account under `POST /api/data-platforms/accounts`;
2. sync the exact electricity Flow with `POST /api/data-platforms/accounts/{account_id}/flows/sync`, setting `remote_flow_id` to the Flow UUID and `remote_version` to `01.01.003`;
3. call `POST /api/intermediate-flow-links/resolve-batch` with the exact source namespace/version and retain the mapping receipt;
4. after the user has imported a legitimately licensed ecoinvent release, call `GET /api/intermediate-flow-links/providers` for target Flow `759b89bd-3aa6-42ad-b767-5bb9ef5d331d`, choose a local provider, and confirm the L2 rule;
5. keep the installed `1.1.0` seed unchanged and consume the derived graph as an independent project revision; record the public profile identity/hash on that revision, but do not rewrite the seed or bake a local provider choice or licensed inventory into public template data;
6. if the graph is also to be persisted as a Nebula LCA model version, first restore every other exact custom Flow dependency through its owning seed/bundle, then create the project with `POST /api/projects` and save the graph with `POST /api/projects/{project_id}/versions`. A clean catalog correctly rejects the graph while those non-electricity dependencies are absent.

The isolated HTTP smoke is reproducible with `nebula-lca-api/scripts/steel_electricity_identity_http_smoke.py`. With no commercial database installed, the old custom electricity identity is rejected as `SOURCE_FLOW_NOT_FOUND`; the exact TianGong identity passes the source gate and stops at `TARGET_FLOW_NOT_FOUND`; provider candidates are empty and are not auto-selected. This is the intended fail-closed boundary. The repository contract test separately covers the same accepted mapping with a minimal non-commercial target identity fixture and verifies L2 target UUID and `1/3.6` conversion.

Mapped provider expansion now supports multiple consumer pins that select the same exact local Process identity. The engine creates one hidden provider activity, connects it to every declared consumer port, and lets the technology matrix sum the per-consumer demand coefficients. The licensed LCI vector is therefore characterized once at the shared total activity rather than once per pin. A collision remains an error only when the Process UUID already belongs to a node in the submitted foreground graph.

The pin request schema remains `provider.mapped-background-process-pin.v1`. Existing single-consumer clients remain valid. Each `provider.mapped-background-process-receipt.v1` now adds the following optional evidence fields under capability marker `shared_activity_schema_version=provider.mapped-background-shared-activity.v1`:

- `provider_instance_id`: deterministic shared hidden-node identity;
- `consumer_process_uuid`: the foreground consumer represented by this receipt;
- `consumer_converted_amount`: the consumer port amount after the accepted Flow mapping conversion;
- `consumer_attributed_activity_amount`: the share of provider activity induced by this consumer;
- `shared_process_total_activity_amount`: the single solved activity of the shared provider Process;
- `attributed_process_scale`: the consumer-attributed activity divided by the provider quantitative-reference amount.

For all receipts sharing one provider instance, the attributed activity amounts sum to the shared total activity. `activity_amount` and `process_scale` retain their previous meaning and continue to report the shared provider totals, preserving compatibility with existing clients. One should persist one receipt per consumer and group them by `provider_instance_id`; it must not merge foreground exchanges, clone Process UUIDs, or sum the repeated shared total fields.

## 8. Mapping evidence and coverage limits

Mapped-background receipts and solve provenance expose `elementary_mapping_dependencies`. Each entry freezes the package ID, version, SHA-256 and role. The same dependency identities participate in the idempotent runtime fingerprint. Missing or ambiguous mappings remain errors; an absent method factor is distinct from a missing Flow identity or a valid zero factor. Never infer identities by name, discard required exchanges, or turn missing data into zero. Exact model-specific coverage is reported with each solve.
