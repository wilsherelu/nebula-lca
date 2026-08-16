# Mapping method

## Public levels

L1 is a strict identity relationship in the frozen public L1 subgraph. Both UUID sides are unique after all certified component packages are combined.

L2 is a reviewed compatibility relationship. Each TianGong Flow has one selected ecoinvent Flow, but an ecoinvent Flow may be reused by multiple TianGong Flows. L2 also covers one-to-one pairs whose context or identity granularity is not strict enough for L1.

The public release recalculates uniqueness after combining all certified internal components. Complete L1 collision groups are conservatively classified as L2 instead of selecting one pair by precedence. This normalization affected 30 intermediate-flow rows and 54 elementary-flow rows in version 1.0.0.

## Review basis

Intermediate Flows were reviewed with structured Flow type, direction, unit dimension, semantic qualifiers, catalog evidence, and bilateral uniqueness checks. Elementary Flows were reviewed with environmental compartment, direction, unit dimension, chemical identity evidence, and independent method-consistency checks. Numerical method values are not part of this public dataset.

Model output never created UUIDs or directly established a published relationship. All published pairs passed deterministic gates and the applicable human-review contract.

## Publication boundary

The public projection contains UUIDs and independently assigned relationship classifications only. Numerical unit conversion data is kept in a separate generic registry. Flow names, descriptions, CAS numbers, formulae, content hashes, audit evidence, model output, amount factors, emission factors, and LCIA characterization factors are excluded.
