# Native background LCI-only API contract v1

Status: **v1 implemented on the existing provider solve endpoint**. The optional `native_background_inventory` request uses a separate, pinned local inventory manifest and returns physical inventory without selecting an LCIA method. Application boundaries and CBAM rules remain in One; this contract does not authorize redistribution of licensed data.

## Current API

`POST /api/provider/v1/solve` already returns `activity_vector`, per-process `scaled_exchanges`, elementary `inventory_totals`, and mapped-background receipts. Each scaled exchange has `process_uuid`, `exchange_id`, exact Flow namespace/UUID/version, unit and reference identities, direction, exchange type, boundary role, and solved amount. The mapped receipt relates each `consumer_exchange_id` to a shared `provider_instance_id`, `consumer_process_uuid`, converted demand, attributed activity, and one shared total. `inventory_totals` has already discarded process and consumer attribution; it must never be used for process-boundary selection.

Native inventory uses its own pinned runtime manifest and does not require a climate method or CF package. Native climate remains a separate option. Missing source identity or unit binding fails closed; the physical inventory is not an LCIA result.

## Request and source assets

The optional `native_background_inventory` is available with schema `provider.native-background-inventory.request.v1` to the existing `provider.solve.request.v1`. It is mutually exclusive with `native_background_climate`. LCI-only requires `lcia_methods` absent or empty; it rejects an LCIA method in this mode instead of returning an empty score. Keep `inline_snapshot` or `snapshot_ref`, `demand`, exact mapped background pins, process identity sidecars and idempotency key as today. Supported scope: ecoinvent 3.11, one distinct mapped Process UUID shared by one or more consumer ports, no other background pins. Partial foreground association remains possible; uncovered inputs stay explicit in the graph. These are runtime limits, not a One product rule.

The request member has this shape; the values are exact caller-supplied pins, not defaults:

```json
{
  "native_background_inventory": {
    "schema_version": "provider.native-background-inventory.request.v1",
    "database_release": "ecoinvent_3.11",
    "inventory_runtime_manifest_sha256": "<64 lowercase hex>",
    "flow_snapshot_hash": "<64 lowercase hex>",
    "reference_dependency_snapshot_hash": "<64 lowercase hex>"
  }
}
```

The option pins `database_release`, `inventory_runtime_manifest_sha256`, `flow_snapshot_hash`, and `reference_dependency_snapshot_hash`. The **separate local inventory manifest** (`provider.native-inventory-runtime-manifest.v1`) pins the original `ElementaryExchanges.xml` and `Units.xml` hashes, the reviewed native-unit binding package and public reference seed hashes, the exact electricity Flow payload identity, and accepted vector source/release labels. Its loader builds or verifies a source Flow index directly from the original MasterData; it does not read the climate method package, `indicator_index.csv`, `lcia_factors.csv`, the climate audit, or a CF-derived Flow index. Every vector row must resolve uniquely to the pinned original Flow and agree on namespace/release, direction, unit, compartment and applicable subcompartment. Every used unit must have the existing reviewed reference binding. A dimension-only binding supplies reference dimensions but permits no conversion or renamed unit.

The source engine uses the existing mapped-pin validation, one-provider expansion, `A*x=f` matrix solve, and scaling code. It does not call `characterize_native_climate` or the projection/EF31 characterization branch. This separation must be in the runtime identity as well as the response: an inventory idempotency fingerprint includes engine build, graph and demand, process/vector/mapping pins, original source and unit-binding hashes, and reference snapshots, but no climate method or CF package. Changes to any inventory dependency cannot replay an older result under the same key.

## Response and limits

The response retains `provider.solve.response.v1`, `activity_vector`, `scaled_exchanges`, `inventory_totals` and mapped receipts. Native inventory returns before characterization: `lcia` and `native_background_climate` are null. The `native_background_inventory` sidecar has these implemented fields:

- `schema_version`: `provider.native-background-inventory.response.v1`
- `database_release`
- `inventory_runtime_manifest_sha256`
- `source_asset_sha256`
- `unit_binding_sha256`
- `provider_process_uuid`
- `native_exchange_count`
- `claim_limit`: `partial_background_leaf_only_no_lcia_score`

Runtime and request identity participate in the existing idempotent solve response. This implementation does not expose a separate inventory digest, exchange-context sidecar or later characterization lookup endpoint. Consumers retain the exact response, source pins and per-exchange evidence rather than inventing those fields or resolving through `latest`.

The current scope is one distinct mapped provider shared by one or more consumer ports, with no other background pins. Invalid source assets, pins, Flow/unit/context, demand or unsupported scope fail closed. Required source data and bindings remain operator-supplied licensed assets; the public repository does not bundle them. Neither physical inventory nor native climate is a regulatory CBAM result.
