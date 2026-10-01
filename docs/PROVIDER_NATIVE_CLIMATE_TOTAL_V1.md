# Provider native-background climate total contract v1

This is an opt-in extension of `POST /api/provider/v1/solve`. Its only method is **EF v3.1 / climate change / GWP100**, reported in `kg CO2-Eq`. It uses the existing foreground graph and mapped-background Process pin, matrix solve, and shared-provider receipt. A request without `native_background_climate` keeps the existing projection behavior.

## Request

Add `native_background_climate` to `provider.solve.request.v1`:

```json
{
  "schema_version": "provider.native-background-climate-total.request.v1",
  "database_release": "ecoinvent_3.11",
  "method_package_id": "ecoinvent-ef31-climate-total-3.11",
  "method_package_version": "1.0.0",
  "method_package_sha256": "fddec69b814db025ed438f65b9ac58c2023f82c770045d5e134c61577bab94b2",
  "runtime_manifest_sha256": "157d8c4752adff5202f50368dff7c1692ad08c057ba64df4813841df974ac81f",
  "flow_snapshot_hash": "70dda0cf2d79108ca4ff96a203cb61eb76a9e550288e05be133e684b4747b1dd",
  "reference_dependency_snapshot_hash": "92016cdbbd2abd4b1a619d0048432313f9e0cf5327cf1c98a049b5a5c1155f29"
}
```

All existing `inline_snapshot` or `snapshot_ref`, `demand`, `lcia_methods: ["EF v3.1"]`, and `mapped_background_process_pins` fields remain. The mapped pin still freezes source Flow UUID/version, L2 confirmation, mapping package ID/version/hash, ecoinvent Process UUID, process content hash, vector checksum, and release. For the current steel request the exact electricity Flow payload content hash is `d0a8812bc220b6e15e860ca727afeb8bb87e29d8374410ca6456ea8a55e8dc9c`; this is verified through the configured exact Flow snapshot and must agree with the request's existing pin and graph.

The method package and local runtime manifest identify original source files, generated CSVs, v2 audit evidence, indicator index, database release, and exact method scope. The server verifies their hashes on every execution, including idempotent replay. It never resolves a saved analysis through `active` or `latest`. The local runtime manifest is an ignored deployment asset; the public repository contains only its schema and method declaration, no licensed rows or factors.

The manifest also pins a local native-unit reference binding package and the public TIDAS reference seed hash. Each generated native elementary exchange and inventory total retains its original ecoinvent Flow UUID, release, unit, direction, and context, while exposing a separately sourced exact reference `flow_property_uuid`/version and `unit_group_uuid`/version. The binding is checked against the original pinned `Units.xml` unit ID and label, and against the reference seed's FlowProperty and UnitGroup identities. Missing bindings fail with `NATIVE_CLIMATE_UNIT_BINDING_MISSING`. The generated provider reference-product exchange obtains these reference fields from the exact mapped source Flow snapshot, after verifying that its target `kWh` unit is in that source Flow's UnitGroup. A dimension-only binding does not authorize a unit conversion, a renamed unit, or a cross-namespace Flow identity match. In this steel vector, `m2*year`, `m3*year`, and `Sm3` have dimension-only group bindings because those exact unit labels are not members of the public reference groups; the response keeps their original labels and amounts.

## Response

The existing `provider.solve.response.v1` fields remain. Add `native_background_climate` with schema `provider.native-background-climate-total.response.v1`:

- `method_package_id`, `method_package_version`, `method_package_sha256`, `runtime_manifest_sha256`, and `indicator`: exact source/method/runtime identity.
- `coverage`: separate foreground and native-background counts for `explicit_nonzero`, `explicit_zero`, `no_cf`, and `unresolved`; no CF row is reported as an explicit zero. A `no_cf` term has no numerical contribution under this selected sparse implementation and remains visible in coverage.
- `domains`: foreground and native-background climate-total contributions and their source labels. The native domain retains ecoinvent Flow UUID, direction and signed CF provenance, including the two negative carbon-storage coefficients. It makes no permanence or full-PCF claim.
- `carbon_storage_permanence: "not_assessed"`: negative characterization coefficients do not establish permanent removal.
- `merge_status`: `merged` only when every **contributing** foreground Flow has an accepted same-method identity and compatible coefficient/unit/compartment in this exact runtime; otherwise `split_diagnostic`, with `merge_blockers` and no unified total.
- `unified_climate_total`: a number only for `merged`; absent for `split_diagnostic`. The response never adds precomputed Process LCIA scores or counts the shared provider twice.
- Each existing `mapped_background_process_receipt` still has one row per consumer and the same `provider_instance_id`; `activity_amount` and `shared_process_total_activity_amount` represent one solved shared total, while `consumer_attributed_activity_amount` records each consumer's share. Do not sum repeated shared totals.

`consumer_converted_amount` is the raw converted electricity input per consumer process reference amount. `consumer_attributed_activity_amount` applies the solved consumer activity and allocation denominator for the requested functional unit. These two quantities need not have the same numerical scale. For the current steel graph, the foreground reference is 1,000 kg and the requested functional unit is 1 kg, so the solved shared activity is 0.001 times the raw converted electricity total. Report the raw graph demand and solved functional-unit demand separately, both in kWh.

The response provenance includes `native_unit_binding_sha256` alongside the exact method/runtime hashes and original Flow/reference snapshot hashes. The idempotency fingerprint includes the binding through the pinned runtime manifest. A changed runtime or method package invalidates an earlier idempotent result.

## Fail-closed errors

HTTP 422 uses the existing `{ "detail": { "code", "message", "details" } }` shape. The new branch uses `NATIVE_CLIMATE_METHOD_PIN_MISMATCH`, `NATIVE_CLIMATE_RUNTIME_HASH_MISMATCH`, `NATIVE_CLIMATE_RUNTIME_UNAVAILABLE`, `NATIVE_CLIMATE_FLOW_IDENTITY_MISSING`, `NATIVE_CLIMATE_FLOW_IDENTITY_DUPLICATE`, `NATIVE_CLIMATE_CF_DUPLICATE`, `NATIVE_CLIMATE_VECTOR_IDENTITY_MISMATCH`, `NATIVE_CLIMATE_UNIT_BINDING_INVALID`, `NATIVE_CLIMATE_UNIT_BINDING_MISSING`, `NATIVE_CLIMATE_REFERENCE_UNIT_BINDING_MISSING`, and `NATIVE_CLIMATE_UNSUPPORTED_SCOPE`. Method disagreement between foreground and background yields a successful **split diagnostic**, without a unified score; it does not silently choose a fallback CF.

## Scope and claim

This contract supports one climate-total indicator for one locally authorized ecoinvent 3.11 installation. It does not assert complete EF v3.1 multi-indicator coverage, CBAM/enterprise-GHG compliance, permanent carbon removal, or a complete product carbon footprint. Public code and synthetic fixtures can exercise the contract; local licensed files and commercial row evidence stay ignored. A local asset's presence alone is not a license to distribute it.

The selected provider vector pins an exact Process UUID, release, content hash, and vector checksum. Its local `system_model` metadata is empty; this contract does not infer a system model from mixed Flow-key source labels. A future system-model claim requires separate source evidence.
