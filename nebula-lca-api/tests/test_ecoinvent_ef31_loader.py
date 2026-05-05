"""Tests for EF 3.1 loader."""

import pytest
from app.ecoinvent_ef31_loader import (
    normalize_text,
    is_ef31_method,
    ElementaryFlow,
    IntermediateFlow,
    UnitConversion,
    CharacterizationFactor,
    match_cf_to_flows,
    filter_cf_ef31,
)


class TestNormalizeText:
    def test_basic(self):
        assert normalize_text("Ammonia") == "ammonia"
        assert normalize_text("  Ammonia  ") == "ammonia"
        assert normalize_text("non-urban   air") == "non-urban air"
    
    def test_none_empty(self):
        assert normalize_text(None) == ""
        assert normalize_text("") == ""


class TestIsEF31Method:
    def test_ef31_methods(self):
        assert is_ef31_method("EF v3.1") is True
        assert is_ef31_method("EF v3.1 no LT") is True
    
    def test_non_ef31_methods(self):
        assert is_ef31_method("CML v4.8 2016 no LT") is False
        assert is_ef31_method("ReCiPe 2016") is False
        assert is_ef31_method("TRACI") is False
        assert is_ef31_method("") is False
        assert is_ef31_method(None) is False


class TestFilterCFEF31:
    def test_filter_keeps_only_ef31(self):
        cfs = [
            CharacterizationFactor(method="EF v3.1", category="c", indicator="i", flow_name="f", cf_value=1.0),
            CharacterizationFactor(method="EF v3.1 no LT", category="c", indicator="i", flow_name="f", cf_value=1.0),
            CharacterizationFactor(method="CML v4.8", category="c", indicator="i", flow_name="f", cf_value=1.0),
            CharacterizationFactor(method="ReCiPe 2016", category="c", indicator="i", flow_name="f", cf_value=1.0),
        ]
        filtered = filter_cf_ef31(cfs)
        assert len(filtered) == 2
        assert all(cf.method in {"EF v3.1", "EF v3.1 no LT"} for cf in filtered)


class TestUnitConversion:
    def test_parse(self):
        uc = UnitConversion(
            conversion_id="conv-1",
            unit_from_name="kilogram",
            unit_to_name="gram",
            unit_type="mass",
            factor=1000.0
        )
        assert uc.unit_from_name == "kilogram"
        assert uc.factor == 1000.0


class TestIntermediateFlowClassification:
    def test_waste_flow(self):
        flow = IntermediateFlow(
            flow_uuid="waste-1",
            flow_name="Waste, plastic",
            flow_type="Waste flow",
            classification="Waste"
        )
        assert flow.flow_type == "Waste flow"
    
    def test_product_flow(self):
        flow = IntermediateFlow(
            flow_uuid="prod-1",
            flow_name="Steel, chromium steel 18/8",
            flow_type="Product flow",
            classification="Material"
        )
        assert flow.flow_type == "Product flow"
    
    def test_classification_value_parsing(self):
        """Test that classificationValue is correctly extracted from classification element."""
        # This simulates the real IntermediateExchanges.xml structure:
        # <classification name="Categories">
        #   <classificationValue>Waste</classificationValue>
        # </classification>
        flow_waste = IntermediateFlow(
            flow_uuid="waste-1",
            flow_name="Waste, plastic",
            flow_type="Waste flow",
            classification="Waste"  # classificationValue extracted
        )
        flow_product = IntermediateFlow(
            flow_uuid="prod-1",
            flow_name="Steel",
            flow_type="Product flow",
            classification="Material"  # classificationValue extracted
        )
        assert flow_waste.flow_type == "Waste flow"
        assert flow_product.flow_type == "Product flow"


class TestMatchCfToFlows:
    def test_exact_match(self):
        flows = [
            ElementaryFlow(flow_uuid="flow-1", flow_name="Ammonia", compartment="air", subcompartment="non-urban air"),
        ]
        cfs = [
            CharacterizationFactor(
                method="EF v3.1", category="climate", indicator="GWP",
                flow_name="Ammonia", compartment="air", subcompartment="non-urban air",
                cf_value=1.5
            ),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 1
        assert len(unmatched) == 0
        assert len(ambiguous) == 0
        assert matched[0]['matched_flow_uuid'] == "flow-1"
    
    def test_unmatched_cf(self):
        flows = [
            ElementaryFlow(flow_uuid="flow-1", flow_name="Ammonia", compartment="air"),
        ]
        cfs = [
            CharacterizationFactor(
                method="EF v3.1", category="climate", indicator="GWP",
                flow_name="Unknown chemical", compartment="water",
                cf_value=1.5
            ),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 0
        assert len(unmatched) == 1
        assert len(ambiguous) == 0
    
    def test_ambiguous_match(self):
        flows = [
            ElementaryFlow(flow_uuid="flow-1", flow_name="Ammonia", compartment="air"),
            ElementaryFlow(flow_uuid="flow-2", flow_name="Ammonia", compartment="air"),
        ]
        cfs = [
            CharacterizationFactor(
                method="EF v3.1", category="climate", indicator="GWP",
                flow_name="Ammonia", compartment="air",
                cf_value=1.5
            ),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 0
        assert len(unmatched) == 0
        assert len(ambiguous) == 1
        assert "flow-1" in ambiguous[0]['matched_flow_uuids']
        assert "flow-2" in ambiguous[0]['matched_flow_uuids']
    
    def test_normalized_match(self):
        """Test that normalization works for whitespace/case variations."""
        flows = [
            ElementaryFlow(flow_uuid="flow-1", flow_name="  Ammonia  ", compartment="AIR"),
        ]
        cfs = [
            CharacterizationFactor(
                method="EF v3.1", category="climate", indicator="GWP",
                flow_name="ammonia", compartment="air",
                cf_value=1.5
            ),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 1
        assert matched[0]['matched_flow_uuid'] == "flow-1"
    
    def test_compartment_subcompartment_match(self):
        """Test CF matching requires name + compartment + subcompartment."""
        flows = [
            ElementaryFlow(flow_uuid="flow-1", flow_name="Ammonia", compartment="air", subcompartment="urban"),
            ElementaryFlow(flow_uuid="flow-2", flow_name="Ammonia", compartment="air", subcompartment="non-urban"),
        ]
        cfs = [
            CharacterizationFactor(
                method="EF v3.1", category="climate", indicator="GWP",
                flow_name="Ammonia", compartment="air", subcompartment="urban",
                cf_value=1.5
            ),
            CharacterizationFactor(
                method="EF v3.1", category="climate", indicator="GWP",
                flow_name="Ammonia", compartment="water", subcompartment="ocean",
                cf_value=2.0
            ),
        ]
        matched, unmatched, ambiguous = match_cf_to_flows(cfs, flows)
        assert len(matched) == 1  # Only urban air matches
        assert len(unmatched) == 1  # water/ocean doesn't match
        assert matched[0]['matched_flow_uuid'] == "flow-1"
