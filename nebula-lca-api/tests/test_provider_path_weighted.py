from __future__ import annotations

from app.provider_schemas import (
    ProviderBackgroundProcessReceipt,
    ProviderMappedBackgroundProcessReceipt,
)
from app.services.provider_path_weighted import build_path_weighted_contribution_receipts

CO2_UUID = "00000000-0000-4000-8000-0000000000c0"
STEEL_PROCESS = "11111111-1111-4111-9111-111111111111"
CONSUMER_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
CONSUMER_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
BG_COAL = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
BG_GAS = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

_TERM_BASE = {
    "schema_version": "provider.lcia.contribution-term.v1",
    "source_namespace": "ecoinvent",
    "flow_uuid": CO2_UUID,
    "flow_version": "01.00.000",
    "flow_property_uuid": None,
    "flow_property_version": None,
    "unit_group_uuid": None,
    "unit_group_version": None,
    "amount_unit": "kg",
    "direction": "output",
    "compartment": "air/fossil",
    "cf_method": "EF v3.1",
    "cf_database_release": "EF3.1",
    "cf_indicator_key": "gwp100-new",
    "cf_version": "EF3.1",
    "cf_presence": "explicit_nonzero",
    "cf_value": 1.0,
    "cf_hash": "cf-" + CO2_UUID,
    "contribution_unit": "kg CO2-Eq",
    "flow_snapshot_hash": "flow-snapshot",
    "snapshot_hash": "consumer-graph-hash",
    "provider_graph_hash": "provider-graph-hash",
    "solve_hash": "solve-hash",
    "provider_commit": "commit-abc",
}

_INDICATOR_BASE = {
    "schema_version": "provider.lcia.indicator-contribution-receipt.v1",
    "method": "EF v3.1",
    "database_release": "EF3.1",
    "canonical_indicator_key": "gwp100-new",
    "indicator_index": 0,
    "indicator_unit": "kg CO2-Eq",
    "indicator_value": 0.0,
    "contribution_total": 0.0,
    "reconciliation_delta": 0.0,
    "snapshot_hash": "consumer-graph-hash",
    "provider_graph_hash": "provider-graph-hash",
    "solve_hash": "solve-hash",
    "provider_commit": "commit-abc",
    "runtime_assets_hash": "runtime-abc",
}


def _term(*, process_uuid: str, exchange_id: str, amount: float, contribution: float) -> dict:
    return {
        **_TERM_BASE,
        "process_uuid": process_uuid,
        "exchange_id": exchange_id,
        "amount": amount,
        "contribution_value": contribution,
    }


def _indicator(terms: list[dict]) -> dict:
    total = sum(float(item["contribution_value"]) for item in terms)
    return {
        **_INDICATOR_BASE,
        "indicator_value": total,
        "contribution_total": total,
        "reconciliation_delta": 0.0,
        "terms": terms,
    }


def _lcia(receipts: list[dict]) -> dict:
    return {"schema_version": "provider.lcia.response.v1", "indicator_contribution_receipts": receipts}


def _mapped_receipt(
    *,
    background_process_uuid: str,
    consumer_exchange_id: str,
    consumer_process_uuid: str,
    attributed: float,
    shared_total: float,
    provider_instance_id: str | None = None,
    mapping_rule_id: str = "rule-1",
) -> ProviderMappedBackgroundProcessReceipt:
    return ProviderMappedBackgroundProcessReceipt(
        consumer_exchange_id=consumer_exchange_id,
        source_flow_namespace="ecoinvent",
        source_flow_uuid="11111111-1111-4111-9111-11111111111e",
        source_flow_version="01.00.000",
        target_flow_uuid="22222222-2222-4222-8222-22222222222e",
        source_unit="MJ",
        target_unit="MJ",
        amount_factor=1.0,
        mapping_level="L1",
        mapping_status="auto",
        mapping_rule_id=mapping_rule_id,
        mapping_package_id="pkg-1",
        mapping_package_version="1.0.0",
        mapping_package_hash="a" * 64,
        database_release="EF3.1",
        process_uuid=background_process_uuid,
        process_content_hash="bg-content-" + background_process_uuid[:8],
        vector_checksum="vec-" + background_process_uuid[:8],
        vector_nnz=1,
        quantitative_reference_amount=1.0,
        activity_amount=shared_total,
        process_scale=shared_total,
        provider_instance_id=provider_instance_id or f"prov-{background_process_uuid[:8]}",
        consumer_process_uuid=consumer_process_uuid,
        consumer_converted_amount=attributed,
        consumer_attributed_activity_amount=attributed,
        shared_process_total_activity_amount=shared_total,
        attributed_process_scale=attributed,
    )


def _leaf_receipt(
    *,
    background_process_uuid: str,
    consumer_exchange_id: str,
    activity_amount: float,
) -> ProviderBackgroundProcessReceipt:
    return ProviderBackgroundProcessReceipt(
        consumer_exchange_id=consumer_exchange_id,
        source_namespace="ecoinvent",
        process_uuid=background_process_uuid,
        version="01.00.000",
        process_type="lci_dataset",
        process_content_hash="leaf-" + background_process_uuid[:8],
        process_snapshot_hash="snap-" + background_process_uuid[:8],
        quantitative_reference_exchange_internal_id="ref-1",
        quantitative_reference_flow_uuid="33333333-3333-4333-8333-33333333333e",
        quantitative_reference_flow_version="01.00.000",
        quantitative_reference_amount=1.0,
        activity_amount=activity_amount,
        process_scale=activity_amount,
        exchanges=[],
    )


def _by_basis(receipt: dict) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in receipt["terms"]:
        grouped.setdefault(row["attribution_basis"], []).append(row)
    return grouped


# ---- 1. Two consumers sharing one background reconcile to that process's characterized term ----


def test_two_consumers_sharing_one_background_split_the_term_by_dimensionless_activity_share():
    lcia = _lcia([_indicator([
        _term(
            process_uuid=STEEL_PROCESS,
            exchange_id="steel::co2",
            amount=2.0,
            contribution=2.0,
        ),
        _term(
            process_uuid=BG_COAL,
            exchange_id="bg-coal::co2",
            amount=3.0,
            contribution=9.0,
        ),
    ])])
    mapped = [
        _mapped_receipt(
            background_process_uuid=BG_COAL,
            consumer_exchange_id="consumer-a::coal-in",
            consumer_process_uuid=CONSUMER_A,
            attributed=1.0,
            shared_total=3.0,
        ),
        _mapped_receipt(
            background_process_uuid=BG_COAL,
            consumer_exchange_id="consumer-b::coal-in",
            consumer_process_uuid=CONSUMER_B,
            attributed=2.0,
            shared_total=3.0,
        ),
    ]
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=mapped,
        background_process_receipts=[],
    )
    assert issues == []
    receipt = receipts[0]
    grouped = _by_basis(receipt)
    assert [row["consumer_exchange_id"] for row in grouped["background_consumer_path"]] == [
        "consumer-a::coal-in",
        "consumer-b::coal-in",
    ]
    assert grouped["background_consumer_path"][0]["path_share"] == 1.0 / 3.0
    assert grouped["background_consumer_path"][1]["path_share"] == 2.0 / 3.0
    assert grouped["background_consumer_path"][0]["path_contribution_value"] == 3.0
    assert grouped["background_consumer_path"][1]["path_contribution_value"] == 6.0
    assert "background_unattributed" not in grouped
    assert receipt["attribution_coverage"] == "complete"
    # Foreground direct term still present, share=1, consumer identity stays null.
    assert len(grouped["foreground_direct"]) == 1
    assert grouped["foreground_direct"][0]["path_share"] == 1.0
    # Σ path == Σ term (indicator total), no double count between foreground + background.
    assert receipt["reconciliation_delta"] == 0.0
    assert receipt["path_weighted_total"] == receipt["source_contribution_total"] == 11.0


# ---- 2. One process with multiple consumption ports keeps each port's row independent ----


def test_one_consumer_with_multiple_ports_yields_independent_path_rows():
    lcia = _lcia([_indicator([
        _term(
            process_uuid=BG_GAS,
            exchange_id="bg-gas::co2",
            amount=4.0,
            contribution=8.0,
        ),
    ])])
    mapped = [
        _mapped_receipt(
            background_process_uuid=BG_GAS,
            consumer_exchange_id="consumer-a::heat-in",
            consumer_process_uuid=CONSUMER_A,
            attributed=1.5,
            shared_total=4.0,
        ),
        _mapped_receipt(
            background_process_uuid=BG_GAS,
            consumer_exchange_id="consumer-a::power-in",
            consumer_process_uuid=CONSUMER_A,
            attributed=2.5,
            shared_total=4.0,
        ),
    ]
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=mapped,
        background_process_receipts=[],
    )
    assert issues == []
    receipt = receipts[0]
    paths = _by_basis(receipt)["background_consumer_path"]
    assert [row["consumer_exchange_id"] for row in paths] == [
        "consumer-a::heat-in",
        "consumer-a::power-in",
    ]
    # Both rows share the SAME consumer process but stay independent per port.
    assert all(row["consumer_process_uuid"] == CONSUMER_A for row in paths)
    assert sum(row["path_share"] for row in paths) == 1.0
    assert sum(row["path_contribution_value"] for row in paths) == 8.0
    # Each row still carries its own path identity: attributed / shared activity are distinct.
    assert [row["consumer_attributed_activity_amount"] for row in paths] == [1.5, 2.5]


# ---- 3. Doubling the demand scales the term once, then path-share splits it once ----


def test_doubling_demand_scales_each_path_contribution_exactly_once():
    # A term at 9.0 (as if for the pre-doubling solve) with the same 1/3 + 2/3 attribution.
    single_receipt = build_path_weighted_contribution_receipts(
        lcia=_lcia([_indicator([_term(process_uuid=BG_COAL, exchange_id="bg-coal::co2", amount=1.0, contribution=3.0)])]),
        mapped_background_process_receipts=[
            _mapped_receipt(
                background_process_uuid=BG_COAL,
                consumer_exchange_id="consumer-a::coal-in",
                consumer_process_uuid=CONSUMER_A,
                attributed=1.0,
                shared_total=3.0,
            ),
            _mapped_receipt(
                background_process_uuid=BG_COAL,
                consumer_exchange_id="consumer-b::coal-in",
                consumer_process_uuid=CONSUMER_B,
                attributed=2.0,
                shared_total=3.0,
            ),
        ],
        background_process_receipts=[],
    )[0][0]
    # The doubled solve reports the same dimensionless attribution but the parent term doubles.
    doubled_receipt = build_path_weighted_contribution_receipts(
        lcia=_lcia([_indicator([_term(process_uuid=BG_COAL, exchange_id="bg-coal::co2", amount=2.0, contribution=6.0)])]),
        mapped_background_process_receipts=[
            _mapped_receipt(
                background_process_uuid=BG_COAL,
                consumer_exchange_id="consumer-a::coal-in",
                consumer_process_uuid=CONSUMER_A,
                attributed=2.0,
                shared_total=6.0,
            ),
            _mapped_receipt(
                background_process_uuid=BG_COAL,
                consumer_exchange_id="consumer-b::coal-in",
                consumer_process_uuid=CONSUMER_B,
                attributed=4.0,
                shared_total=6.0,
            ),
        ],
        background_process_receipts=[],
    )[0][0]

    # The share stays fixed and the path contribution scales once (3.0 -> 6.0), not twice.
    single_paths = {row["consumer_exchange_id"]: row for row in single_receipt["terms"]}
    doubled_paths = {row["consumer_exchange_id"]: row for row in doubled_receipt["terms"]}
    assert single_paths["consumer-a::coal-in"]["path_share"] == 1.0 / 3.0
    assert doubled_paths["consumer-a::coal-in"]["path_share"] == 1.0 / 3.0
    assert (
        doubled_paths["consumer-a::coal-in"]["path_contribution_value"]
        == 2.0 * single_paths["consumer-a::coal-in"]["path_contribution_value"]
    )
    assert (
        doubled_paths["consumer-b::coal-in"]["path_contribution_value"]
        == 2.0 * single_paths["consumer-b::coal-in"]["path_contribution_value"]
    )


# ---- 4. Foreground + background together reconcile to the indicator total (no double count) ----


def test_foreground_and_background_reconcile_to_indicator_total():
    lcia = _lcia([_indicator([
        _term(process_uuid=STEEL_PROCESS, exchange_id="steel::co2", amount=1.0, contribution=5.0),
        _term(process_uuid=BG_COAL, exchange_id="bg-coal::co2", amount=2.0, contribution=3.0),
    ])])
    mapped = [
        _mapped_receipt(
            background_process_uuid=BG_COAL,
            consumer_exchange_id="consumer-a::coal-in",
            consumer_process_uuid=CONSUMER_A,
            attributed=2.0,
            shared_total=2.0,
        ),
    ]
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=mapped,
        background_process_receipts=[],
    )
    assert issues == []
    receipt = receipts[0]
    # Σ over ALL path rows == Σ over characterized terms == indicator value.
    assert receipt["path_weighted_total"] == 8.0 == receipt["source_contribution_total"]
    assert receipt["reconciliation_delta"] == 0.0
    assert receipt["indicator_value"] == 8.0
    grouped = _by_basis(receipt)
    # Foreground rows and background rows both present, no row shares its contribution twice.
    assert len(grouped["foreground_direct"]) == 1
    assert len(grouped["background_consumer_path"]) == 1
    assert "background_unattributed" not in grouped


# ---- 5. Partial attribution is reported explicitly, never silently zeroed ----


def test_partial_background_attribution_is_reported_as_an_unattributed_remainder():
    lcia = _lcia([_indicator([
        _term(process_uuid=BG_COAL, exchange_id="bg-coal::co2", amount=4.0, contribution=12.0),
    ])])
    mapped = [
        _mapped_receipt(
            background_process_uuid=BG_COAL,
            consumer_exchange_id="consumer-a::coal-in",
            consumer_process_uuid=CONSUMER_A,
            attributed=1.0,
            shared_total=4.0,
        ),
    ]
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=mapped,
        background_process_receipts=[],
    )
    receipt = receipts[0]
    grouped = _by_basis(receipt)
    assert grouped["background_consumer_path"][0]["path_share"] == 0.25
    # The 0.75 remainder is emitted as a labelled background_unattributed row, NOT dropped.
    unattributed = grouped["background_unattributed"]
    assert len(unattributed) == 1
    assert unattributed[0]["path_share"] == 0.75
    assert unattributed[0]["path_contribution_value"] == 9.0
    assert receipt["attribution_coverage"] == "partial"
    assert receipt["unattributed_contribution_value"] == 9.0
    # Reconciliation still holds: 12.0 = 3.0 attributed + 9.0 unattributed.
    assert receipt["path_weighted_total"] == 12.0
    assert receipt["reconciliation_delta"] == 0.0
    # A provider issue names the incomplete process so downstream consumers see the gap.
    codes = [item.code for item in issues]
    assert "PATH_WEIGHTED_ATTRIBUTION_INCOMPLETE" in codes
    details = next(item.details for item in issues if item.code == "PATH_WEIGHTED_ATTRIBUTION_INCOMPLETE")
    assert details["processes"][0]["process_uuid"] == BG_COAL
    assert details["processes"][0]["reason"] == "attribution_incomplete"
    assert details["processes"][0]["unattributed_share"] == 0.75


def test_background_process_whose_mapped_attribution_is_none_reports_missing_coverage():
    """A mapped receipt exists (so the process is genuinely background) but per-path attribution
    fields are missing. The join must emit a labelled background_unattributed row (not a
    foreground_direct row and not a zeroed drop) so the totals still reconcile."""
    lcia = _lcia([_indicator([
        _term(process_uuid=BG_GAS, exchange_id="bg-gas::co2", amount=1.0, contribution=7.0),
    ])])
    drifted = _mapped_receipt(
        background_process_uuid=BG_GAS,
        consumer_exchange_id="consumer-a::gas-in",
        consumer_process_uuid=CONSUMER_A,
        attributed=1.0,
        shared_total=1.0,
    )
    drifted.consumer_attributed_activity_amount = None
    drifted.shared_process_total_activity_amount = None
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=[drifted],
        background_process_receipts=[],
    )
    receipt = receipts[0]
    grouped = _by_basis(receipt)
    # BG_GAS appears in the mapped index (so we KNOW it is background) but has no usable
    # per-path attribution: the whole term must become background_unattributed.
    assert "background_consumer_path" not in grouped
    assert "foreground_direct" not in grouped
    assert len(grouped["background_unattributed"]) == 1
    assert grouped["background_unattributed"][0]["process_uuid"] == BG_GAS
    assert grouped["background_unattributed"][0]["path_contribution_value"] == 7.0
    assert receipt["attribution_coverage"] == "missing"
    assert receipt["path_weighted_total"] == 7.0 == receipt["source_contribution_total"]
    codes = [item.code for item in issues]
    assert "PATH_WEIGHTED_ATTRIBUTION_INCOMPLETE" in codes


# ---- 6. Leaf receipt with a single consumer path gets share=1 (proves leaf activity is one-path) ----


def test_leaf_background_with_one_consumer_path_yields_a_full_share_row():
    lcia = _lcia([_indicator([
        _term(process_uuid=BG_GAS, exchange_id="bg-gas::co2", amount=1.0, contribution=4.0),
    ])])
    leaves = [
        _leaf_receipt(
            background_process_uuid=BG_GAS,
            consumer_exchange_id="consumer-a::gas-in",
            activity_amount=2.0,
        ),
    ]
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=[],
        background_process_receipts=leaves,
    )
    receipt = receipts[0]
    grouped = _by_basis(receipt)
    assert len(grouped["background_consumer_path"]) == 1
    assert grouped["background_consumer_path"][0]["path_share"] == 1.0
    assert grouped["background_consumer_path"][0]["consumer_exchange_id"] == "consumer-a::gas-in"
    assert receipt["attribution_coverage"] == "complete"
    assert receipt["reconciliation_delta"] == 0.0
    assert issues == []


def test_leaf_background_shared_by_two_ports_reports_a_gap_instead_of_fabricating_a_split():
    lcia = _lcia([_indicator([
        _term(process_uuid=BG_GAS, exchange_id="bg-gas::co2", amount=1.0, contribution=6.0),
    ])])
    leaves = [
        _leaf_receipt(
            background_process_uuid=BG_GAS,
            consumer_exchange_id="consumer-a::gas-in",
            activity_amount=1.0,
        ),
        _leaf_receipt(
            background_process_uuid=BG_GAS,
            consumer_exchange_id="consumer-b::gas-in",
            activity_amount=1.0,
        ),
    ]
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=[],
        background_process_receipts=leaves,
    )
    receipt = receipts[0]
    grouped = _by_basis(receipt)
    # Leaf receipts pin whole activity to each consumer_exchange_id; the join cannot split them
    # without fabricating 1/N, so the term becomes one labelled background_unattributed row.
    assert "background_consumer_path" not in grouped
    assert grouped["background_unattributed"][0]["path_contribution_value"] == 6.0
    assert receipt["attribution_coverage"] == "missing"
    codes = [item.code for item in issues]
    assert "PATH_WEIGHTED_ATTRIBUTION_INCOMPLETE" in codes


# ---- 7. Path share exceeding 1 (drift) is refused as an attribution gap, not propagated ----


def test_consumer_attributed_exceeding_shared_pool_is_reported_not_propagated():
    lcia = _lcia([_indicator([
        _term(process_uuid=BG_COAL, exchange_id="bg-coal::co2", amount=1.0, contribution=5.0),
    ])])
    mapped = [
        _mapped_receipt(
            background_process_uuid=BG_COAL,
            consumer_exchange_id="consumer-a::coal-in",
            consumer_process_uuid=CONSUMER_A,
            attributed=2.0,
            shared_total=1.0,
        ),
    ]
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=mapped,
        background_process_receipts=[],
    )
    receipt = receipts[0]
    grouped = _by_basis(receipt)
    # A share of 2.0 must never propagate; the term falls back to background_unattributed.
    assert "background_consumer_path" not in grouped
    assert grouped["background_unattributed"][0]["path_share"] == 1.0
    gap_reason = next(
        item["reason"]
        for item in receipt["processes_without_path_attribution"]
    )
    assert gap_reason == "attribution_exceeds_shared_pool"
    codes = [item.code for item in issues]
    assert "PATH_WEIGHTED_ATTRIBUTION_INCOMPLETE" in codes


# ---- 8. A user-pinned closed-leaf project background is attributed to its one consumer path ----


BG_PROJECT = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def _project_receipt(*, expanded_process_uuid: str, consumer_exchange_id: str,
                     activity_amount: float) -> dict:
    return {
        "pin": {
            "schema_version": "provider.project-background-pin.v1",
            "consumer_exchange_id": consumer_exchange_id,
            "project_id": BG_PROJECT,
            "version": 1,
            "claim_role": "user_declared_partial_background_leaf",
        },
        "source_graph_hash": "src-graph-hash",
        "source_process_uuid": expanded_process_uuid,
        "source_model_process_uuid": "ee" * 16,
        "expanded_node_id": f"project-background-{expanded_process_uuid[:16]}",
        "reference_flow_uuid": "ff" * 16,
        "reference_unit": "MJ",
        "consumer_unit": "MJ",
        "unit_conversion_factor": 1.0,
        "activity_amount": activity_amount,
        "claim_limit": "user_declared_partial_background_leaf_only",
    }


def test_project_background_pin_attributes_whole_term_to_its_consumer_path():
    # A foreground onsite CO2 term plus a background term emitted by the solver for a user-pinned
    # closed-leaf supply project (its process_uuid is the expanded instance, not the source model).
    lcia = _lcia([_indicator([
        _term(process_uuid=STEEL_PROCESS, exchange_id="steel::co2", amount=1.0, contribution=1940.739734),
        _term(process_uuid=BG_PROJECT, exchange_id="project-background-x::1", amount=1.0, contribution=94.732787),
    ])])
    receipts, issues = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=[],
        background_process_receipts=[],
        project_background_process_receipts=[
            _project_receipt(
                expanded_process_uuid=BG_PROJECT,
                consumer_exchange_id="ore-sintering::grid-electricity-in",
                activity_amount=600.0,
            )
        ],
    )
    receipt = receipts[0]
    grouped = _by_basis(receipt)
    # The pinned background term is no longer mislabelled foreground_direct; it is a consumer-path
    # row keyed by the exact user-selectable input port, attributed in full (share 1.0).
    assert "foreground_direct" in grouped and len(grouped["foreground_direct"]) == 1
    assert grouped["foreground_direct"][0]["exchange_id"] == "steel::co2"
    assert len(grouped["background_consumer_path"]) == 1
    path = grouped["background_consumer_path"][0]
    assert path["exchange_id"] == "project-background-x::1"
    assert path["consumer_exchange_id"] == "ore-sintering::grid-electricity-in"
    assert path["path_share"] == 1.0
    assert path["path_contribution_value"] == 94.732787
    assert "background_unattributed" not in grouped
    # Foreground and background together reconcile to the indicator total, no double count.
    assert receipt["attribution_coverage"] == "complete"
    assert receipt["path_weighted_total"] == receipt["source_contribution_total"] == 1940.739734 + 94.732787
    assert receipt["reconciliation_delta"] == 0.0
    assert issues == []


def test_absent_project_receipts_leave_a_foreground_only_receipt_unchanged():
    # Byte-identity invariant: with no project background receipts the builder must produce the
    # exact same receipt as before the contract extension (all terms foreground_direct).
    lcia = _lcia([_indicator([
        _term(process_uuid=STEEL_PROCESS, exchange_id="steel::co2", amount=1.0, contribution=1940.739734),
    ])])
    baseline = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=[],
        background_process_receipts=[],
    )
    with_empty = build_path_weighted_contribution_receipts(
        lcia=lcia,
        mapped_background_process_receipts=[],
        background_process_receipts=[],
        project_background_process_receipts=[],
    )
    assert baseline[0][0]["content_hash"] == with_empty[0][0]["content_hash"]
    assert baseline[0][0]["attribution_coverage"] == "foreground_only"
    assert {row["attribution_basis"] for row in baseline[0][0]["terms"]} == {"foreground_direct"}
