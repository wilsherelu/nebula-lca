# Nebula Flow Mapping

Nebula Flow Mapping is a data-only interoperability dataset linking TianGong Flow UUIDs to ecoinvent Flow UUIDs. Version 1.0.0 contains accepted intermediate-flow and elementary-flow mappings. It does not contain process datasets, exchanges, inventories, Flow names or descriptions, emission factors, LCIA characterization factors, supplier information, or database exports.

Version 1.0.0 contains 11,143 accepted relationships: 1,379 intermediate-flow mappings (L1 147, L2 1,232) and 9,764 elementary-flow mappings (L1 6,354, L2 3,410).

## Data files

- `data/intermediate-flow-mappings.v1.jsonl`: accepted intermediate-flow mappings.
- `data/elementary-flow-mappings.v1.jsonl`: accepted elementary-flow mappings.
- `data/unit-conversions.v1.json`: independently maintained standard physical unit conversions used by some mappings. These are not emission or LCIA factors.

Mappings that require one of these generic conversions reference it through `conversion_rule_id`; no numerical conversion or impact factor is embedded in a mapping row.

L1 is strict identity within the published L1 subgraph: every TianGong UUID and ecoinvent UUID occurs at most once. L2 is reviewed compatibility: each TianGong UUID selects one ecoinvent UUID, while one ecoinvent UUID may be reused by several TianGong Flows. L2 should be confirmed by the consuming user.

The dataset is an identifier interoperability layer. Users must obtain lawful access to the underlying databases separately.
