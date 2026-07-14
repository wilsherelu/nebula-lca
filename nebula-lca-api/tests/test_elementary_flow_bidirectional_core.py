import math
import json
from pathlib import Path

from app.elementary_flow_conversion import (
    CfPresence,
    CfValue,
    DirectionalMapping,
    FlowIdentity,
    InventoryExchange,
    MappingGrade,
    MappingStatus,
    MethodScope,
    SemanticStatus,
    ContextMappingMode,
    compile_bidirectional_core,
    convert_inventory,
    load_context_crosswalk,
    load_mapping_package,
)


REAL_GHG_PACKAGE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "flow_mappings"
    / "ghg_ef31_v1.json"
)
REAL_GHG_CONTEXT_CROSSWALK = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "flow_mappings"
    / "ghg_air_context_crosswalk_v1.json"
)


def _flow(namespace: str, flow_id: str, *, qualifier: str = "fossil") -> FlowIdentity:
    return FlowIdentity(
        namespace=namespace,
        namespace_version="test-v1",
        flow_id=flow_id,
        substance_id="carbon-dioxide",
        context_id="emission/air/unspecified",
        direction="output",
        flow_property="mass",
        unit_dimension="mass",
        qualifiers=(("carbon_origin", qualifier),),
    )


def _edge(source: FlowIdentity, target: FlowIdentity, factor: float) -> DirectionalMapping:
    return DirectionalMapping(
        source=source,
        target=target,
        unit_factor=factor,
        semantic_status=SemanticStatus.EXACT,
        grade=MappingGrade.S1,
        review_status="approved",
        evidence_ids=("review:test-fixture",),
    )


def _scope() -> MethodScope:
    return MethodScope(
        method_id="EF climate change",
        method_version="3.1",
        indicator_ids=("climate_total", "climate_fossil"),
    )


def test_certified_pair_converts_both_directions_without_result_change():
    eco = _flow("ecoinvent", "eco-co2")
    ef = _flow("EF", "ef-co2")
    result = compile_bidirectional_core(
        package_id="ghg-core",
        package_version="1",
        method_scope=_scope(),
        forward_candidates=[_edge(eco, ef, 1000.0)],
        reverse_candidates=[_edge(ef, eco, 0.001)],
        cf_registry={
            eco.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 1.0),
                "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
            },
            ef.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 0.001),
                "climate_fossil": CfValue(CfPresence.NONZERO, 0.001),
            },
        },
    )

    assert not result.rejected
    forward = convert_inventory(
        [InventoryExchange(eco, 2.5)],
        package=result.package,
        source_namespace="ecoinvent",
        target_namespace="EF",
    )
    reverse = convert_inventory(
        forward.converted,
        package=result.package,
        source_namespace="EF",
        target_namespace="ecoinvent",
    )

    assert forward.traces[0].status == MappingStatus.MAPPED
    assert math.isclose(forward.converted[0].amount, 2500.0)
    assert math.isclose(reverse.converted[0].amount, 2.5)
    assert result.package.package_hash == compile_bidirectional_core(
        package_id="ghg-core",
        package_version="1",
        method_scope=_scope(),
        forward_candidates=[_edge(eco, ef, 1000.0)],
        reverse_candidates=[_edge(ef, eco, 0.001)],
        cf_registry={
            eco.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 1.0),
                "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
            },
            ef.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 0.001),
                "climate_fossil": CfValue(CfPresence.NONZERO, 0.001),
            },
        },
    ).package.package_hash


def test_missing_and_zero_cf_are_not_treated_as_lossless_evidence():
    eco = _flow("ecoinvent", "eco-co2")
    ef = _flow("EF", "ef-co2")
    result = compile_bidirectional_core(
        package_id="ghg-core",
        package_version="1",
        method_scope=_scope(),
        forward_candidates=[_edge(eco, ef, 1.0)],
        reverse_candidates=[_edge(ef, eco, 1.0)],
        cf_registry={
            eco.key: {
                "climate_total": CfValue(CfPresence.MISSING),
                "climate_fossil": CfValue(CfPresence.ZERO, 0.0),
            },
            ef.key: {
                "climate_total": CfValue(CfPresence.MISSING),
                "climate_fossil": CfValue(CfPresence.ZERO, 0.0),
            },
        },
    )

    assert not result.package.forward_mappings
    assert "CF_UNINFORMATIVE" in result.rejected[0].codes


def test_cf_change_changes_package_hash_and_mismatch_blocks_mapping():
    eco = _flow("ecoinvent", "eco-co2")
    ef = _flow("EF", "ef-co2")

    def compile_with_target_cf(target_cf: float):
        return compile_bidirectional_core(
            package_id="ghg-core",
            package_version="1",
            method_scope=_scope(),
            forward_candidates=[_edge(eco, ef, 1.0)],
            reverse_candidates=[_edge(ef, eco, 1.0)],
            cf_registry={
                eco.key: {
                    "climate_total": CfValue(CfPresence.NONZERO, 1.0),
                    "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
                },
                ef.key: {
                    "climate_total": CfValue(CfPresence.NONZERO, target_cf),
                    "climate_fossil": CfValue(CfPresence.NONZERO, target_cf),
                },
            },
        )

    accepted = compile_with_target_cf(1.0)
    accepted_with_cf_change = compile_with_target_cf(1.0 + 1e-10)
    rejected = compile_with_target_cf(2.0)

    assert accepted.package.forward_mappings
    assert not rejected.package.forward_mappings
    assert "CF_VALUE_MISMATCH" in rejected.rejected[0].codes
    assert accepted_with_cf_change.package.forward_mappings
    assert accepted.package.package_hash != accepted_with_cf_change.package.package_hash


def test_qualifier_conflict_and_missing_reverse_are_rejected():
    eco = _flow("ecoinvent", "eco-co2", qualifier="fossil")
    ef = _flow("EF", "ef-co2", qualifier="biogenic")
    result = compile_bidirectional_core(
        package_id="ghg-core",
        package_version="1",
        method_scope=_scope(),
        forward_candidates=[_edge(eco, ef, 1.0)],
        reverse_candidates=[],
        cf_registry={
            eco.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 1.0),
                "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
            },
            ef.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 1.0),
                "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
            },
        },
    )

    assert set(result.rejected[0].codes) >= {
        "SEMANTIC_KEY_MISMATCH",
        "MISSING_INDEPENDENT_REVERSE",
    }


def test_unmapped_exchange_is_preserved_as_residual_with_trace():
    eco = _flow("ecoinvent", "eco-co2")
    ef = _flow("EF", "ef-co2")
    other = FlowIdentity(
        namespace="ecoinvent",
        namespace_version="test-v1",
        flow_id="eco-water",
        substance_id="water",
        context_id="resource/water/ground",
        direction="input",
        flow_property="volume",
        unit_dimension="volume",
    )
    compiled = compile_bidirectional_core(
        package_id="ghg-core",
        package_version="1",
        method_scope=_scope(),
        forward_candidates=[_edge(eco, ef, 1.0)],
        reverse_candidates=[_edge(ef, eco, 1.0)],
        cf_registry={
            eco.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 1.0),
                "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
            },
            ef.key: {
                "climate_total": CfValue(CfPresence.NONZERO, 1.0),
                "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
            },
        },
    )
    converted = convert_inventory(
        [InventoryExchange(eco, 1.0), InventoryExchange(other, 3.0)],
        package=compiled.package,
        source_namespace="ecoinvent",
        target_namespace="EF",
    )

    assert converted.residual == (InventoryExchange(other, 3.0),)
    assert [trace.status for trace in converted.traces] == [
        MappingStatus.MAPPED,
        MappingStatus.RESIDUAL,
    ]


def test_real_ghg_package_compiles_and_round_trips_all_reviewed_pairs():
    compiled = load_mapping_package(REAL_GHG_PACKAGE)

    assert not compiled.rejected
    assert len(compiled.package.forward_mappings) == 180
    assert len(compiled.package.one_way_mappings) == 16
    inventory = tuple(
        InventoryExchange(mapping.source, float(index + 1))
        for index, mapping in enumerate(compiled.package.forward_mappings)
    )
    forward = convert_inventory(
        inventory,
        package=compiled.package,
        source_namespace="ecoinvent",
        target_namespace="EF",
    )
    reverse = convert_inventory(
        forward.converted,
        package=compiled.package,
        source_namespace="EF",
        target_namespace="ecoinvent",
    )

    assert not forward.residual
    assert not reverse.residual
    assert len(reverse.converted) == 180
    assert all(
        math.isclose(original.amount, restored.amount)
        for original, restored in zip(
            sorted(inventory, key=lambda item: item.flow.key),
            sorted(reverse.converted, key=lambda item: item.flow.key),
        )
    )

    alias = compiled.package.one_way_mappings[0]
    canonicalized = convert_inventory(
        [InventoryExchange(alias.source, 1.0)],
        package=compiled.package,
        source_namespace="EF",
        target_namespace="ecoinvent",
    )
    restored = convert_inventory(
        canonicalized.converted,
        package=compiled.package,
        source_namespace="ecoinvent",
        target_namespace="EF",
    )
    assert canonicalized.traces[0].warning_codes == ("ONE_WAY_CANONICALIZATION",)
    assert restored.converted[0].flow != alias.source


def test_real_ghg_package_rejects_unresolved_evidence(tmp_path):
    payload = json.loads(REAL_GHG_PACKAGE.read_text(encoding="utf-8"))
    payload["mappings"][0]["evidence_ids"].append("missing:evidence")
    invalid_package = tmp_path / "invalid-ghg-package.json"
    invalid_package.write_text(json.dumps(payload), encoding="utf-8")

    try:
        load_mapping_package(invalid_package)
    except ValueError as exc:
        assert "unresolved mapping evidence" in str(exc)
    else:
        raise AssertionError("unresolved evidence must reject the mapping package")


def test_context_crosswalk_separates_bijective_canonical_and_rejected_rows():
    rows = load_context_crosswalk(REAL_GHG_CONTEXT_CROSSWALK)

    assert sum(row.mode == ContextMappingMode.BIJECTIVE for row in rows) == 5
    assert sum(
        row.mode == ContextMappingMode.EF_TO_ECOINVENT_CANONICALIZATION
        for row in rows
    ) == 7
    assert sum(row.mode == ContextMappingMode.REJECTED for row in rows) == 1


def test_ef_alias_can_canonicalize_one_way_without_weakening_bijective_pair():
    eco = _flow("ecoinvent", "eco-co2")
    ef_canonical = _flow("EF", "ef-co2-canonical")
    ef_alias = _flow("EF", "ef-co2-alias")
    cf = {
        "climate_total": CfValue(CfPresence.NONZERO, 1.0),
        "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
    }
    compiled = compile_bidirectional_core(
        package_id="ghg-core",
        package_version="1",
        method_scope=_scope(),
        forward_candidates=[_edge(eco, ef_canonical, 1.0)],
        reverse_candidates=[_edge(ef_canonical, eco, 1.0)],
        one_way_candidates=[_edge(ef_alias, eco, 1.0)],
        cf_registry={eco.key: cf, ef_canonical.key: cf, ef_alias.key: cf},
    )

    assert not compiled.rejected
    assert len(compiled.package.one_way_mappings) == 1
    to_eco = convert_inventory(
        [InventoryExchange(ef_canonical, 1.0), InventoryExchange(ef_alias, 2.0)],
        package=compiled.package,
        source_namespace="EF",
        target_namespace="ecoinvent",
    )
    back_to_ef = convert_inventory(
        to_eco.converted,
        package=compiled.package,
        source_namespace="ecoinvent",
        target_namespace="EF",
    )

    assert to_eco.converted == (InventoryExchange(eco, 3.0),)
    assert to_eco.traces[1].warning_codes == ("ONE_WAY_CANONICALIZATION",)
    assert back_to_ef.converted == (InventoryExchange(ef_canonical, 3.0),)


def test_one_way_candidate_with_only_equal_cf_but_different_semantics_is_rejected():
    eco = _flow("ecoinvent", "eco-co2", qualifier="fossil")
    ef_canonical = _flow("EF", "ef-co2-canonical", qualifier="fossil")
    ef_wrong_alias = _flow("EF", "ef-co2-wrong", qualifier="biogenic")
    cf = {
        "climate_total": CfValue(CfPresence.NONZERO, 1.0),
        "climate_fossil": CfValue(CfPresence.NONZERO, 1.0),
    }
    compiled = compile_bidirectional_core(
        package_id="ghg-core",
        package_version="1",
        method_scope=_scope(),
        forward_candidates=[_edge(eco, ef_canonical, 1.0)],
        reverse_candidates=[_edge(ef_canonical, eco, 1.0)],
        one_way_candidates=[_edge(ef_wrong_alias, eco, 1.0)],
        cf_registry={eco.key: cf, ef_canonical.key: cf, ef_wrong_alias.key: cf},
    )

    assert not compiled.package.one_way_mappings
    assert "SEMANTIC_KEY_MISMATCH" in compiled.rejected[0].codes
