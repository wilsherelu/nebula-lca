"""Compile independently reviewed directional mappings into a lossless core."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence

from .contracts import (
    CfPresence,
    CfValue,
    CompilationResult,
    ConversionPackage,
    DirectionalMapping,
    FlowIdentity,
    MappingGrade,
    MethodScope,
    RejectedMapping,
    SemanticStatus,
)

CfRegistry = Mapping[str, Mapping[str, CfValue]]


def _cf_codes(
    source_key: str,
    target_key: str,
    amount_factor: float,
    cf_registry: CfRegistry,
    scope: MethodScope,
) -> list[str]:
    source_vector = cf_registry.get(source_key, {})
    target_vector = cf_registry.get(target_key, {})
    codes: list[str] = []
    informative = False

    for indicator_id in scope.indicator_ids:
        source_cf = source_vector.get(indicator_id, CfValue(CfPresence.MISSING))
        target_cf = target_vector.get(indicator_id, CfValue(CfPresence.MISSING))
        if source_cf.presence != target_cf.presence:
            codes.append("CF_PRESENCE_MISMATCH")
            continue
        if source_cf.presence == CfPresence.NONZERO:
            informative = True
            assert source_cf.value is not None and target_cf.value is not None
            expected = amount_factor * target_cf.value
            if not math.isclose(
                source_cf.value,
                expected,
                rel_tol=scope.relative_tolerance,
                abs_tol=scope.absolute_tolerance,
            ):
                codes.append("CF_VALUE_MISMATCH")

    if not informative:
        codes.append("CF_UNINFORMATIVE")
    return codes


def _mapping_codes(
    mapping: DirectionalMapping,
    reverse: DirectionalMapping | None,
    source_degree: Counter[str],
    target_degree: Counter[str],
    reverse_source_degree: Counter[str],
    reverse_target_degree: Counter[str],
    cf_registry: CfRegistry,
    scope: MethodScope,
) -> list[str]:
    codes: list[str] = []
    if mapping.grade != MappingGrade.S1 or mapping.semantic_status != SemanticStatus.EXACT:
        codes.append("NOT_S1_EXACT")
    if mapping.review_status != "approved":
        codes.append("NOT_APPROVED")
    if not mapping.evidence_ids:
        codes.append("MISSING_EVIDENCE")
    if mapping.source.semantic_key != mapping.target.semantic_key:
        codes.append("SEMANTIC_KEY_MISMATCH")
    if source_degree[mapping.source.key] != 1 or target_degree[mapping.target.key] != 1:
        codes.append("AMBIGUOUS_OR_NON_BIJECTIVE")
    if reverse is None:
        codes.append("MISSING_INDEPENDENT_REVERSE")
    else:
        if (
            reverse_source_degree[reverse.source.key] != 1
            or reverse_target_degree[reverse.target.key] != 1
        ):
            codes.append("AMBIGUOUS_REVERSE")
        if reverse.grade != MappingGrade.S1 or reverse.semantic_status != SemanticStatus.EXACT:
            codes.append("REVERSE_NOT_S1_EXACT")
        if reverse.review_status != "approved" or not reverse.evidence_ids:
            codes.append("REVERSE_NOT_APPROVED")
        if not math.isclose(
            mapping.amount_factor * reverse.amount_factor,
            1.0,
            rel_tol=scope.relative_tolerance,
            abs_tol=scope.absolute_tolerance,
        ):
            codes.append("NON_RECIPROCAL_FACTORS")
    codes.extend(
        _cf_codes(
            mapping.source.key,
            mapping.target.key,
            mapping.amount_factor,
            cf_registry,
            scope,
        )
    )
    return list(dict.fromkeys(codes))


def _package_hash(
    package_id: str,
    package_version: str,
    scope: MethodScope,
    forward: Sequence[DirectionalMapping],
    reverse: Sequence[DirectionalMapping],
    cf_registry: CfRegistry,
) -> str:
    def flow_row(flow: FlowIdentity) -> dict[str, object]:
        return {
            "key": flow.key,
            "semantic_key": list(flow.semantic_key),
        }

    def edge_row(edge: DirectionalMapping) -> dict[str, object]:
        return {
            "source": flow_row(edge.source),
            "target": flow_row(edge.target),
            "unit_factor": edge.unit_factor,
            "basis_factor": edge.basis_factor,
            "semantic_status": edge.semantic_status.value,
            "grade": edge.grade.value,
            "review_status": edge.review_status,
            "evidence_ids": list(edge.evidence_ids),
        }

    relevant_flow_keys = sorted(
        {edge.source.key for edge in (*forward, *reverse)}
        | {edge.target.key for edge in (*forward, *reverse)}
    )
    cf_rows = {
        flow_key: {
            indicator_id: {
                "presence": cf_registry.get(flow_key, {})
                .get(indicator_id, CfValue(CfPresence.MISSING))
                .presence.value,
                "value": cf_registry.get(flow_key, {})
                .get(indicator_id, CfValue(CfPresence.MISSING))
                .value,
            }
            for indicator_id in scope.indicator_ids
        }
        for flow_key in relevant_flow_keys
    }

    payload = {
        "package_id": package_id,
        "package_version": package_version,
        "method_id": scope.method_id,
        "method_version": scope.method_version,
        "indicator_ids": list(scope.indicator_ids),
        "relative_tolerance": scope.relative_tolerance,
        "absolute_tolerance": scope.absolute_tolerance,
        "forward": [edge_row(edge) for edge in sorted(forward, key=lambda item: item.source.key)],
        "reverse": [edge_row(edge) for edge in sorted(reverse, key=lambda item: item.source.key)],
        "characterization_factors": cf_rows,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compile_bidirectional_core(
    *,
    package_id: str,
    package_version: str,
    method_scope: MethodScope,
    forward_candidates: Sequence[DirectionalMapping],
    reverse_candidates: Sequence[DirectionalMapping],
    cf_registry: CfRegistry,
) -> CompilationResult:
    """Publish only reviewed, bijective and method-invariant flow pairs.

    Reverse mappings are supplied and checked independently. They are never
    generated by matrix inversion or by taking a reciprocal in this function.
    """

    source_degree = Counter(edge.source.key for edge in forward_candidates)
    target_degree = Counter(edge.target.key for edge in forward_candidates)
    reverse_source_degree = Counter(edge.source.key for edge in reverse_candidates)
    reverse_target_degree = Counter(edge.target.key for edge in reverse_candidates)
    reverse_by_pair = {
        (edge.target.key, edge.source.key): edge for edge in reverse_candidates
    }
    accepted_forward: list[DirectionalMapping] = []
    accepted_reverse: list[DirectionalMapping] = []
    rejected: list[RejectedMapping] = []

    for mapping in forward_candidates:
        reverse = reverse_by_pair.get((mapping.source.key, mapping.target.key))
        codes = _mapping_codes(
            mapping,
            reverse,
            source_degree,
            target_degree,
            reverse_source_degree,
            reverse_target_degree,
            cf_registry,
            method_scope,
        )
        if codes:
            rejected.append(
                RejectedMapping(mapping.source.key, mapping.target.key, tuple(codes))
            )
            continue
        assert reverse is not None
        accepted_forward.append(mapping)
        accepted_reverse.append(reverse)

    accepted_forward.sort(key=lambda item: item.source.key)
    accepted_reverse.sort(key=lambda item: item.source.key)
    package_hash = _package_hash(
        package_id,
        package_version,
        method_scope,
        accepted_forward,
        accepted_reverse,
        cf_registry,
    )
    package = ConversionPackage(
        package_id=package_id,
        package_version=package_version,
        method_scope=method_scope,
        forward_mappings=tuple(accepted_forward),
        reverse_mappings=tuple(accepted_reverse),
        package_hash=package_hash,
    )
    return CompilationResult(package=package, rejected=tuple(rejected))
