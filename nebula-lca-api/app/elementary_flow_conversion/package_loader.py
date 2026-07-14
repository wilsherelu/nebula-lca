"""Load a reviewed JSON mapping package into the certified conversion core."""

from __future__ import annotations

import json
from pathlib import Path

from .certification import compile_bidirectional_core
from .contracts import (
    CfPresence,
    CfValue,
    CompilationResult,
    DirectionalMapping,
    FlowIdentity,
    MappingGrade,
    MethodScope,
    SemanticStatus,
)


def _identity(mapping: dict[str, object], namespace: str) -> FlowIdentity:
    prefix = "ecoinvent" if namespace == "ecoinvent" else "ef"
    context = mapping[f"{prefix}_context"]
    semantic = mapping["semantic"]
    assert isinstance(context, dict) and isinstance(semantic, dict)
    qualifiers = semantic.get("qualifiers", {})
    assert isinstance(qualifiers, dict)
    return FlowIdentity(
        namespace=namespace,
        namespace_version=str(mapping[f"{prefix}_version"]),
        flow_id=str(mapping[f"{prefix}_flow_uuid"]),
        substance_id=str(semantic["substance_id"]),
        context_id=str(context["canonical_context_id"]),
        direction=str(mapping["direction"]),
        flow_property=str(semantic["flow_property"]),
        unit_dimension=str(semantic["unit_dimension"]),
        qualifiers=tuple(sorted((str(key), str(value)) for key, value in qualifiers.items())),
    )


def _edge(
    mapping: dict[str, object],
    source: FlowIdentity,
    target: FlowIdentity,
    factor_key: str,
) -> DirectionalMapping:
    return DirectionalMapping(
        source=source,
        target=target,
        unit_factor=float(mapping[factor_key]),
        semantic_status=SemanticStatus.EXACT,
        grade=MappingGrade(str(mapping["semantic_grade"])),
        review_status=str(mapping["review_status"]),
        evidence_ids=tuple(str(item) for item in mapping["evidence_ids"]),
    )


def load_mapping_package(path: Path) -> CompilationResult:
    """Compile a reviewed package and reject any row that breaks CBC rules."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    mappings = payload["mappings"]
    one_way_rows = payload.get("one_way_mappings", [])
    if payload.get("review_status") != "approved":
        raise ValueError("mapping package is not approved")
    if payload.get("mapping_count") != len(mappings):
        raise ValueError("mapping_count does not match mappings")
    if payload.get("one_way_mapping_count", len(one_way_rows)) != len(one_way_rows):
        raise ValueError("one_way_mapping_count does not match one_way_mappings")
    evidence_ids = {
        str(item["evidence_id"])
        for item in payload.get("evidence", [])
    }
    unresolved = {
        str(evidence_id)
        for mapping in (*mappings, *one_way_rows)
        for evidence_id in mapping.get("evidence_ids", [])
        if str(evidence_id) not in evidence_ids
    }
    if unresolved:
        raise ValueError(f"unresolved mapping evidence: {sorted(unresolved)}")
    scope_row = payload["method_scope"]
    scope = MethodScope(
        method_id=str(scope_row["method_id"]),
        method_version=str(scope_row["method_version"]),
        indicator_ids=tuple(str(item) for item in scope_row["indicator_ids"]),
        relative_tolerance=float(scope_row.get("relative_tolerance", 1e-8)),
        absolute_tolerance=float(scope_row.get("absolute_tolerance", 1e-15)),
    )
    forward: list[DirectionalMapping] = []
    reverse: list[DirectionalMapping] = []
    one_way: list[DirectionalMapping] = []
    cf_registry: dict[str, dict[str, CfValue]] = {}

    for mapping in mappings:
        eco = _identity(mapping, "ecoinvent")
        ef = _identity(mapping, "EF")
        forward.append(_edge(mapping, eco, ef, "eco_to_ef_factor"))
        reverse.append(_edge(mapping, ef, eco, "ef_to_eco_factor"))
        vector = mapping["cf_vector"]
        cf_values = {
            indicator_id: CfValue(
                CfPresence.ZERO if float(vector[indicator_id]) == 0.0 else CfPresence.NONZERO,
                float(vector[indicator_id]),
            )
            for indicator_id in scope.indicator_ids
        }
        cf_registry[eco.key] = cf_values
        cf_registry[ef.key] = dict(cf_values)

    for mapping in one_way_rows:
        eco = _identity(mapping, "ecoinvent")
        ef = _identity(mapping, "EF")
        source_namespace = str(mapping["source_namespace"])
        if source_namespace == "ecoinvent":
            source, target = eco, ef
        elif source_namespace == "EF":
            source, target = ef, eco
        else:
            raise ValueError(f"unsupported one-way source namespace: {source_namespace}")
        one_way.append(_edge(mapping, source, target, "amount_factor"))
        vector = mapping["cf_vector"]
        cf_values = {
            indicator_id: CfValue(
                CfPresence.ZERO if float(vector[indicator_id]) == 0.0 else CfPresence.NONZERO,
                float(vector[indicator_id]),
            )
            for indicator_id in scope.indicator_ids
        }
        cf_registry[eco.key] = cf_values
        cf_registry[ef.key] = dict(cf_values)

    return compile_bidirectional_core(
        package_id=str(payload["package_id"]),
        package_version=str(payload["package_version"]),
        method_scope=scope,
        forward_candidates=forward,
        reverse_candidates=reverse,
        one_way_candidates=one_way,
        cf_registry=cf_registry,
    )
