from __future__ import annotations

import hashlib
import json
from typing import Any

from ..provider_schemas import (
    ProviderBackgroundProcessReceipt,
    ProviderIssue,
    ProviderMappedBackgroundProcessReceipt,
)

_ATTRIBUTION_TOLERANCE = 1e-9


def _canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _identity_fields(term: dict[str, Any]) -> dict[str, Any]:
    """Fields that describe WHAT emission this row characterizes; copied verbatim per path."""
    return {
        "process_uuid": term["process_uuid"],
        "exchange_id": term["exchange_id"],
        "source_namespace": term["source_namespace"],
        "flow_uuid": term["flow_uuid"],
        "flow_version": term["flow_version"],
        "flow_property_uuid": term.get("flow_property_uuid"),
        "flow_property_version": term.get("flow_property_version"),
        "unit_group_uuid": term.get("unit_group_uuid"),
        "unit_group_version": term.get("unit_group_version"),
        "amount": term["amount"],
        "amount_unit": term["amount_unit"],
        "direction": term["direction"],
        "compartment": term.get("compartment"),
        "cf_method": term["cf_method"],
        "cf_database_release": term["cf_database_release"],
        "cf_indicator_key": term["cf_indicator_key"],
        "cf_version": term["cf_version"],
        "cf_presence": term["cf_presence"],
        "cf_value": term["cf_value"],
        "cf_hash": term["cf_hash"],
        "contribution_value": term["contribution_value"],
        "contribution_unit": term["contribution_unit"],
        "flow_snapshot_hash": term.get("flow_snapshot_hash"),
        "snapshot_hash": term["snapshot_hash"],
        "provider_graph_hash": term["provider_graph_hash"],
        "solve_hash": term["solve_hash"],
        "provider_commit": term.get("provider_commit"),
    }


def _consumer_path_row(
    term: dict[str, Any],
    *,
    attribution_basis: str,
    path_share: float,
    consumer_exchange_id: str | None,
    consumer_process_uuid: str | None,
    provider_instance_id: str | None,
    background_process_content_hash: str | None,
    mapping_rule_id: str | None,
    mapping_package_id: str | None,
    mapping_package_version: str | None,
    mapping_package_hash: str | None,
    source_flow_uuid: str | None,
    source_flow_version: str | None,
    target_flow_uuid: str | None,
    amount_factor: float | None,
    mapping_level: str | None,
    mapping_status: str | None,
    consumer_attributed_activity_amount: float | None,
    shared_process_total_activity_amount: float | None,
    claim_role: str | None,
    claim_limit: str | None,
) -> dict[str, Any]:
    contribution = float(term["contribution_value"])
    return {
        "schema_version": "provider.lcia.path-weighted-contribution-term.v1",
        "attribution_basis": attribution_basis,
        "consumer_exchange_id": consumer_exchange_id,
        "consumer_process_uuid": consumer_process_uuid,
        "provider_instance_id": provider_instance_id,
        "background_process_content_hash": background_process_content_hash,
        "mapping_rule_id": mapping_rule_id,
        "mapping_package_id": mapping_package_id,
        "mapping_package_version": mapping_package_version,
        "mapping_package_hash": mapping_package_hash,
        "source_flow_uuid": source_flow_uuid,
        "source_flow_version": source_flow_version,
        "target_flow_uuid": target_flow_uuid,
        "amount_factor": amount_factor,
        "mapping_level": mapping_level,
        "mapping_status": mapping_status,
        **_identity_fields(term),
        "path_share": path_share,
        "consumer_attributed_activity_amount": consumer_attributed_activity_amount,
        "shared_process_total_activity_amount": shared_process_total_activity_amount,
        "path_contribution_value": contribution * path_share,
        "claim_role": claim_role,
        "claim_limit": claim_limit,
    }


def _foreground_row(term: dict[str, Any]) -> dict[str, Any]:
    return _consumer_path_row(
        term,
        attribution_basis="foreground_direct",
        path_share=1.0,
        consumer_exchange_id=None,
        consumer_process_uuid=None,
        provider_instance_id=None,
        background_process_content_hash=None,
        mapping_rule_id=None,
        mapping_package_id=None,
        mapping_package_version=None,
        mapping_package_hash=None,
        source_flow_uuid=None,
        source_flow_version=None,
        target_flow_uuid=None,
        amount_factor=None,
        mapping_level=None,
        mapping_status=None,
        consumer_attributed_activity_amount=None,
        shared_process_total_activity_amount=None,
        claim_role=None,
        claim_limit=None,
    )


def _unattributed_row(term: dict[str, Any], share: float) -> dict[str, Any]:
    return _consumer_path_row(
        term,
        attribution_basis="background_unattributed",
        path_share=share,
        consumer_exchange_id=None,
        consumer_process_uuid=None,
        provider_instance_id=None,
        background_process_content_hash=None,
        mapping_rule_id=None,
        mapping_package_id=None,
        mapping_package_version=None,
        mapping_package_hash=None,
        source_flow_uuid=None,
        source_flow_version=None,
        target_flow_uuid=None,
        amount_factor=None,
        mapping_level=None,
        mapping_status=None,
        consumer_attributed_activity_amount=None,
        shared_process_total_activity_amount=None,
        claim_role="partial_background_leaf",
        claim_limit="attribution_incomplete",
    )


def _project_rows_for_term(term: dict[str, Any], receipt: dict[str, Any]) -> list[dict[str, Any]]:
    """Attribute a whole project-background term to the one consumer path it was pinned onto.

    A project background pin expands one closed-leaf supply version onto exactly one open
    technosphere input and the solver gives each pin a distinct expanded process instance, so the
    term's activity belongs entirely to that ``consumer_exchange_id`` (``path_share`` is 1.0 by
    construction, never a fabricated split). The elementary exchange stays engine-characterized;
    this row only records which user-selectable consumption path it belongs to.
    """
    pin = receipt.get("pin") or {}
    activity = float(receipt.get("activity_amount") or 0.0)
    return [
        _consumer_path_row(
            term,
            attribution_basis="background_consumer_path",
            path_share=1.0,
            consumer_exchange_id=pin.get("consumer_exchange_id"),
            consumer_process_uuid=None,
            provider_instance_id=None,
            background_process_content_hash=receipt.get("source_graph_hash"),
            mapping_rule_id=None,
            mapping_package_id=None,
            mapping_package_version=None,
            mapping_package_hash=None,
            source_flow_uuid=receipt.get("reference_flow_uuid"),
            source_flow_version=None,
            target_flow_uuid=None,
            amount_factor=receipt.get("unit_conversion_factor"),
            mapping_level=None,
            mapping_status=None,
            consumer_attributed_activity_amount=activity,
            shared_process_total_activity_amount=activity,
            claim_role=pin.get("claim_role"),
            claim_limit=receipt.get("claim_limit"),
        )
    ]


def _mapped_rows_for_term(
    term: dict[str, Any], receipts: list[ProviderMappedBackgroundProcessReceipt]
) -> tuple[list[dict[str, Any]], bool, dict[str, Any] | None]:
    """Split a characterized term across mapped consumers with dimensionless shares.

    Returns (rows, background_seen, process_gap_or_none). A `background_unattributed` remainder
    row is emitted whenever the mapped consumer shares sum below 1 so the parent term is fully
    accounted for without inventing attribution.
    """
    background_seen = True
    rows: list[dict[str, Any]] = []
    share_sum = 0.0
    for receipt in receipts:
        attributed = receipt.consumer_attributed_activity_amount
        shared = receipt.shared_process_total_activity_amount
        if attributed is None or shared is None or shared <= 0:
            continue
        share = float(attributed) / float(shared)
        if share <= 0:
            continue
        if share > 1.0 + _ATTRIBUTION_TOLERANCE:
            # Consumer attribution exceeds the shared pool: this is a mapping/engine drift,
            # report it as an unattributed gap rather than propagating a wrong per-path number.
            gap = {
                "process_uuid": receipt.process_uuid,
                "consumer_exchange_id": receipt.consumer_exchange_id,
                "reason": "attribution_exceeds_shared_pool",
                "consumer_attributed_activity_amount": float(attributed),
                "shared_process_total_activity_amount": float(shared),
            }
            return [], background_seen, gap
        rows.append(
            _consumer_path_row(
                term,
                attribution_basis="background_consumer_path",
                path_share=share,
                consumer_exchange_id=receipt.consumer_exchange_id,
                consumer_process_uuid=receipt.consumer_process_uuid,
                provider_instance_id=receipt.provider_instance_id,
                background_process_content_hash=receipt.process_content_hash,
                mapping_rule_id=receipt.mapping_rule_id,
                mapping_package_id=receipt.mapping_package_id,
                mapping_package_version=receipt.mapping_package_version,
                mapping_package_hash=receipt.mapping_package_hash,
                source_flow_uuid=receipt.source_flow_uuid,
                source_flow_version=receipt.source_flow_version,
                target_flow_uuid=receipt.target_flow_uuid,
                amount_factor=receipt.amount_factor,
                mapping_level=receipt.mapping_level,
                mapping_status=receipt.mapping_status,
                consumer_attributed_activity_amount=float(attributed),
                shared_process_total_activity_amount=float(shared),
                claim_role=receipt.claim_role,
                claim_limit=receipt.claim_limit,
            )
        )
        share_sum += share
    remainder = 1.0 - share_sum
    if remainder > _ATTRIBUTION_TOLERANCE:
        rows.append(_unattributed_row(term, remainder))
        gap = {
            "process_uuid": receipts[0].process_uuid,
            "reason": "attribution_incomplete",
            "unattributed_share": remainder,
            "consumer_exchange_ids": [
                r.consumer_exchange_id for r in receipts if r.consumer_exchange_id
            ],
        }
        return rows, background_seen, gap
    return rows, background_seen, None


def _leaf_rows_for_term(
    term: dict[str, Any], receipts: list[ProviderBackgroundProcessReceipt]
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Split a characterized term across leaf consumer_exchange_ids.

    Leaf receipts pin the whole background activity to one specific consumer port. When two or
    more leaf rows point at the same process_uuid, they represent the same solved activity
    attributed to different ports and there is no engine-published per-port share; report a gap
    rather than fabricating a 1/N split.
    """
    if len(receipts) == 1:
        receipt = receipts[0]
        return [
            _consumer_path_row(
                term,
                attribution_basis="background_consumer_path",
                path_share=1.0,
                consumer_exchange_id=receipt.consumer_exchange_id,
                consumer_process_uuid=None,
                provider_instance_id=None,
                background_process_content_hash=receipt.process_content_hash,
                mapping_rule_id=None,
                mapping_package_id=None,
                mapping_package_version=None,
                mapping_package_hash=None,
                source_flow_uuid=receipt.quantitative_reference_flow_uuid,
                source_flow_version=receipt.quantitative_reference_flow_version,
                target_flow_uuid=None,
                amount_factor=None,
                mapping_level=None,
                mapping_status=None,
                consumer_attributed_activity_amount=float(receipt.activity_amount),
                shared_process_total_activity_amount=float(receipt.activity_amount),
                claim_role=receipt.claim_role,
                claim_limit=receipt.claim_limit,
            )
        ], None
    return [
        _unattributed_row(term, 1.0)
    ], {
        "process_uuid": receipts[0].process_uuid,
        "reason": "leaf_activity_not_per_path_attributable",
        "consumer_exchange_ids": [r.consumer_exchange_id for r in receipts],
    }


def _build_indicator_receipt(
    indicator_receipt: dict[str, Any],
    *,
    mapped_by_process: dict[str, list[ProviderMappedBackgroundProcessReceipt]],
    leaf_by_process: dict[str, list[ProviderBackgroundProcessReceipt]],
    project_by_process: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    background_seen = False
    for term in indicator_receipt["terms"]:
        process_uuid = term["process_uuid"]
        if process_uuid in mapped_by_process:
            background_seen = True
            term_rows, _, gap = _mapped_rows_for_term(term, mapped_by_process[process_uuid])
            if gap is not None or not term_rows:
                # Fall back to a fully unattributed row if mapped attribution collapsed or was
                # rejected, so the characterized term stays represented.
                if not term_rows:
                    term_rows = [_unattributed_row(term, 1.0)]
            rows.extend(term_rows)
            if gap is not None:
                gaps.append(gap)
            continue
        if process_uuid in leaf_by_process:
            background_seen = True
            term_rows, gap = _leaf_rows_for_term(term, leaf_by_process[process_uuid])
            rows.extend(term_rows)
            if gap is not None:
                gaps.append(gap)
            continue
        if process_uuid in project_by_process:
            # A user-pinned closed-leaf supply project: the whole term belongs to its one consumer
            # input path, attributed in full. This is a complete attribution, not a gap.
            background_seen = True
            rows.extend(_project_rows_for_term(term, project_by_process[process_uuid]))
            continue
        rows.append(_foreground_row(term))

    source_total = sum(float(item["contribution_value"]) for item in indicator_receipt["terms"])
    path_total = sum(float(item["path_contribution_value"]) for item in rows)
    unattributed_total = sum(
        float(item["path_contribution_value"])
        for item in rows
        if item["attribution_basis"] == "background_unattributed"
    )
    if not background_seen:
        coverage = "foreground_only"
    elif unattributed_total <= _ATTRIBUTION_TOLERANCE and not gaps:
        coverage = "complete"
    elif any(
        item["attribution_basis"] == "background_consumer_path" for item in rows
    ):
        coverage = "partial"
    else:
        coverage = "missing"

    receipt = {
        "schema_version": "provider.lcia.path-weighted-contribution-receipt.v1",
        "method": indicator_receipt["method"],
        "database_release": indicator_receipt["database_release"],
        "canonical_indicator_key": indicator_receipt["canonical_indicator_key"],
        "indicator_index": indicator_receipt.get("indicator_index"),
        "indicator_unit": indicator_receipt["indicator_unit"],
        "indicator_value": float(indicator_receipt["indicator_value"]),
        "source_contribution_total": source_total,
        "path_weighted_total": path_total,
        "reconciliation_delta": path_total - source_total,
        "attribution_coverage": coverage,
        "unattributed_contribution_value": unattributed_total,
        "terms": rows,
        "processes_without_path_attribution": gaps,
        "snapshot_hash": indicator_receipt["snapshot_hash"],
        "provider_graph_hash": indicator_receipt["provider_graph_hash"],
        "solve_hash": indicator_receipt["solve_hash"],
        "provider_commit": indicator_receipt.get("provider_commit"),
        "runtime_assets_hash": indicator_receipt.get("runtime_assets_hash"),
    }
    receipt["content_hash"] = _canonical_hash(receipt)
    return receipt, gaps


def build_path_weighted_contribution_receipts(
    *,
    lcia: dict[str, Any],
    mapped_background_process_receipts: list[ProviderMappedBackgroundProcessReceipt],
    background_process_receipts: list[ProviderBackgroundProcessReceipt],
    project_background_process_receipts: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[ProviderIssue]]:
    """Join characterized terms with mapped/leaf/project attribution into a per-consumption-path view.

    Scope 1/2/3 stays a One-side classification; this receipt only proves that each characterized
    contribution belongs to a specific consumption path (or is honestly labelled unattributed), and
    that the per-path total reconciles back to the parent term without double-scaling.
    """
    mapped_by_process: dict[str, list[ProviderMappedBackgroundProcessReceipt]] = {}
    for receipt in mapped_background_process_receipts:
        mapped_by_process.setdefault(receipt.process_uuid, []).append(receipt)
    leaf_by_process: dict[str, list[ProviderBackgroundProcessReceipt]] = {}
    for receipt in background_process_receipts:
        leaf_by_process.setdefault(receipt.process_uuid, []).append(receipt)
    # Each project-background receipt carries a unique expanded process_uuid, so the index is a
    # process -> single receipt map (a repeat would signal a solver identity drift; keep first).
    project_by_process: dict[str, dict[str, Any]] = {}
    for receipt in project_background_process_receipts or []:
        if isinstance(receipt, dict) and receipt.get("source_process_uuid"):
            project_by_process.setdefault(str(receipt["source_process_uuid"]), receipt)

    indicator_receipts = lcia.get("indicator_contribution_receipts") or []
    if not indicator_receipts:
        return [], []

    path_receipts: list[dict[str, Any]] = []
    all_gaps: list[dict[str, Any]] = []
    for indicator_receipt in indicator_receipts:
        built, gaps = _build_indicator_receipt(
            indicator_receipt,
            mapped_by_process=mapped_by_process,
            leaf_by_process=leaf_by_process,
            project_by_process=project_by_process,
        )
        path_receipts.append(built)
        all_gaps.extend(gaps)

    issues: list[ProviderIssue] = []
    if all_gaps:
        issues.append(
            ProviderIssue(
                code="PATH_WEIGHTED_ATTRIBUTION_INCOMPLETE",
                message=(
                    "Some background characterized contributions could not be attributed to a"
                    " specific consumer path; they are labelled as background_unattributed so"
                    " totals still reconcile but per-path claims remain limited."
                ),
                severity="info",
                path="lcia.path_weighted_contribution_receipts",
                details={"processes": all_gaps},
            )
        )
    return path_receipts, issues
