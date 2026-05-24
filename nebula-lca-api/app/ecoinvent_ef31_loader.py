"""EF 3.1 LCI/LCIA Foundation Loader.

Parses EF 3.1 data files:
- MasterData/ElementaryExchanges.xml
- MasterData/IntermediateExchanges.xml
- MasterData/Units.xml
- MasterData/UnitConversions.xml
- MasterData/ImpactMethods.xml
- LCIA Implementation 3.11.xlsx (CFs + Indicators sheets)

Three-stage approach:
1. Foundation loader (MasterData + LCIA Excel)
2. LCI dataset preview (extracted directories)
3. Official .7z archive support
"""

import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict, is_dataclass
from pathlib import Path
from typing import Optional, Union, Any, Dict, List

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import py7zr
except ImportError:
    py7zr = None

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

NS = {'es': 'http://www.EcoInvent.org/EcoSpold02'}

# EF 3.1 method filter
EF31_METHODS = {'EF v3.1', 'EF v3.1 no LT'}
_UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _spold_process_uuid(ds):
    """Generate process UUID from a dataset — mirrors ef31_db_service._generate_lci_process_uuid."""
    activity_id = str(getattr(ds, "activity_id", "") or "")
    reference_product_id = str(getattr(ds, "reference_product_id", "") or "")
    parts = [part for part in (activity_id, reference_product_id) if part]
    if parts:
        return ":".join(parts)
    return Path(getattr(ds, "filename", "") or "").stem


def parse_spold_filename_dataset_ids(spold_path: Path | str) -> tuple[str, str, str] | None:
    """Parse activity/product UUIDs from ``activity_uuid_product_uuid.spold``."""
    path = Path(spold_path)
    if path.suffix.lower() != ".spold":
        return None
    parts = path.stem.split("_", 1)
    if len(parts) != 2:
        return None
    activity_id, reference_product_id = parts
    if not (_UUID_PATTERN.match(activity_id) and _UUID_PATTERN.match(reference_product_id)):
        return None
    return activity_id, reference_product_id, f"{activity_id}:{reference_product_id}"


@dataclass
class UnitRecord:
    unit_id: str
    name: str
    unit_type: Optional[str] = None


@dataclass
class UnitConversion:
    conversion_id: str
    unit_from_name: str
    unit_to_name: str
    unit_type: str
    factor: float


@dataclass
class ElementaryFlow:
    flow_uuid: str
    flow_name: str
    flow_name_en: Optional[str] = None
    flow_type: str = "Elementary flow"
    default_unit: str = ""
    unit_group: str = ""
    compartment: Optional[str] = None
    subcompartment: Optional[str] = None
    cas_number: Optional[str] = None
    formula: Optional[str] = None
    source: str = "ecoinvent"


@dataclass
class IntermediateFlow:
    flow_uuid: str
    flow_name: str
    flow_name_en: Optional[str] = None
    flow_type: str = "Product flow"
    default_unit: str = ""
    unit_group: str = ""
    classification: Optional[str] = None
    source: str = "ecoinvent"


@dataclass
class Indicator:
    method: str
    category: str
    indicator: str
    indicator_unit: str


@dataclass
class CharacterizationFactor:
    method: str
    category: str
    indicator: str
    flow_name: str
    compartment: Optional[str] = None
    subcompartment: Optional[str] = None
    cf_value: float = 0.0


@dataclass
class LCIDataset:
    filename: str
    activity_id: str
    activity_name: str
    location: str
    reference_product_name: str
    reference_product_unit: str
    reference_product_amount: float
    exchange_count: int = 0
    # P1 fix: reference product id for composite process key
    reference_product_id: str = ""


@dataclass
class LCIElementaryExchange:
    dataset_filename: str
    exchange_id: str
    exchange_name: str
    unit: str
    direction: str
    amount: float


@dataclass
class FoundationReport:
    master_data_dir: str
    lcia_implementation_path: str
    elementary_flows_count: int = 0
    intermediate_flows_count: int = 0
    units_count: int = 0
    unit_conversions_count: int = 0
    indicators_total: int = 0
    indicators_ef31: int = 0
    cf_rows_total: int = 0
    cf_rows_ef31: int = 0
    cf_rows_matched: int = 0
    cf_rows_unmatched: int = 0
    cf_rows_ambiguous: int = 0
    warnings: list = field(default_factory=list)


@dataclass
class LCIPreviewReport:
    lci_dir: str
    master_data_dir: str
    limit: int
    datasets_scanned: int = 0
    datasets_parsed: int = 0
    lci_elementary_exchanges_count: int = 0
    missing_elementary_refs: int = 0
    warnings: list = field(default_factory=list)


def normalize_text(text: Optional[str]) -> str:
    """Normalize text for matching: lowercase, strip, collapse whitespace."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip()).lower()


def child_text(elem: Optional[ET.Element], child_name: str) -> str:
    """Return stripped text from a direct child by local tag name."""
    if elem is None:
        return ""
    for child in elem:
        tag = child.tag.split('}')[-1]
        if tag == child_name:
            return (child.text or '').strip()
    return ""


def is_ef31_method(method_name: str) -> bool:
    """Check if method is EF v3.1 or EF v3.1 no LT."""
    if not method_name:
        return False
    return method_name.strip() in EF31_METHODS


def parse_units(data_dir: Path) -> Dict[str, UnitRecord]:
    """Parse Units.xml into {unit_id: UnitRecord}."""
    units_file = data_dir / "Units.xml"
    if not units_file.exists():
        logger.warning(f"Units.xml not found at {units_file}")
        return {}
    
    units = {}
    context = ET.iterparse(str(units_file), events=['end'])
    
    for event, elem in context:
        if elem.tag.endswith('unit'):
            unit_id = elem.get('id', '')
            name = ""
            unit_type = None
            
            for child in elem:
                tag = child.tag.split('}')[-1]
                if tag == 'name':
                    name = (child.text or '').strip()
                elif tag == 'unitType':
                    unit_type = (child.text or '').strip()
            
            if unit_id:
                units[unit_id] = UnitRecord(unit_id=unit_id, name=name, unit_type=unit_type)
            elem.clear()
    
    logger.info(f"Parsed {len(units)} units from Units.xml")
    return units


def parse_unit_conversions(data_dir: Path) -> List[UnitConversion]:
    """Parse UnitConversions.xml into list of UnitConversion."""
    conversions_file = data_dir / "UnitConversions.xml"
    if not conversions_file.exists():
        logger.warning(f"UnitConversions.xml not found at {conversions_file}")
        return []
    
    conversions = []
    context = ET.iterparse(str(conversions_file), events=['end'])
    
    for event, elem in context:
        if elem.tag.endswith('unitConversion'):
            conv_id = elem.get('id', '')
            factor_str = elem.get('factor', '1')
            try:
                factor = float(factor_str)
            except ValueError:
                factor = 1.0
            
            unit_from = ""
            unit_to = ""
            unit_type = ""
            
            for child in elem:
                tag = child.tag.split('}')[-1]
                if tag == 'unitFromName':
                    unit_from = (child.text or '').strip()
                elif tag == 'unitToName':
                    unit_to = (child.text or '').strip()
                elif tag == 'unitType':
                    unit_type = (child.text or '').strip()
            
            if conv_id:
                conversions.append(UnitConversion(
                    conversion_id=conv_id,
                    unit_from_name=unit_from,
                    unit_to_name=unit_to,
                    unit_type=unit_type,
                    factor=factor
                ))
            elem.clear()
    
    logger.info(f"Parsed {len(conversions)} unit conversions from UnitConversions.xml")
    return conversions


def parse_elementary_exchanges(data_dir: Path, units: Dict[str, UnitRecord]) -> List[ElementaryFlow]:
    """Parse ElementaryExchanges.xml into list of ElementaryFlow."""
    file_path = data_dir / "ElementaryExchanges.xml"
    if not file_path.exists():
        logger.warning(f"ElementaryExchanges.xml not found at {file_path}")
        return []
    
    flows = []
    context = ET.iterparse(str(file_path), events=['end'])
    
    for event, elem in context:
        elem_tag = elem.tag.split('}')[-1]
        if elem_tag in ('elementaryExchange', 'ElementaryFlow'):
            flow_uuid = elem.get('id', '') or child_text(elem, 'uuid')
            unit_id = elem.get('unitId', '')
            cas_number = elem.get('casNumber') or child_text(elem, 'CAS') or None
            formula = elem.get('formula') or child_text(elem, 'formula') or None
            
            flow_name = ""
            flow_name_en = ""
            compartment = ""
            subcompartment = ""
            
            for child in elem:
                tag = child.tag.split('}')[-1]
                if tag == 'name':
                    flow_name_en = (child.text or '').strip()
                    flow_name = flow_name_en
                elif tag in ('shortNameEN', 'nameEN'):
                    if not flow_name_en:
                        flow_name_en = (child.text or '').strip()
                    if not flow_name:
                        flow_name = flow_name_en
                elif tag == 'unit' and not unit_id:
                    unit_id = (child.text or '').strip()
                elif tag == 'compartment':
                    if list(child):
                        for sub in child:
                            sub_tag = sub.tag.split('}')[-1]
                            if sub_tag == 'compartment':
                                compartment = (sub.text or '').strip()
                            elif sub_tag == 'subcompartment':
                                subcompartment = (sub.text or '').strip()
                    else:
                        compartment = (child.text or '').strip()
            
            unit_name = units.get(unit_id, UnitRecord(unit_id, unit_id)).name if unit_id else ""
            
            flow = ElementaryFlow(
                flow_uuid=flow_uuid,
                flow_name=flow_name,
                flow_name_en=flow_name_en,
                default_unit=unit_name,
                compartment=compartment if compartment else None,
                subcompartment=subcompartment if subcompartment else None,
                cas_number=cas_number,
                formula=formula,
            )
            flows.append(flow)
            elem.clear()
    
    logger.info(f"Parsed {len(flows)} elementary exchanges")
    return flows


def parse_intermediate_exchanges(data_dir: Path, units: Dict[str, UnitRecord]) -> List[IntermediateFlow]:
    """Parse IntermediateExchanges.xml into list of IntermediateFlow with classification."""
    file_path = data_dir / "IntermediateExchanges.xml"
    if not file_path.exists():
        logger.warning(f"IntermediateExchanges.xml not found at {file_path}")
        return []
    
    flows = []
    context = ET.iterparse(str(file_path), events=['end'])
    
    for event, elem in context:
        if elem.tag.endswith('intermediateExchange'):
            flow_uuid = elem.get('id', '')
            unit_id = elem.get('unitId', '')

            flow_name = ""
            flow_name_en = ""
            classification = None
            classification_value = ""

            for child in elem:
                tag = child.tag.split('}')[-1]
                if tag == 'name':
                    flow_name_en = (child.text or '').strip()
                    flow_name = flow_name_en
                elif tag == 'classification':
                    class_system = child_text(child, 'classificationSystem')
                    class_value = child_text(child, 'classificationValue')
                    if class_system == 'By-product classification':
                        classification_value = class_value
                        classification = class_value
                    elif classification is None and class_value:
                        classification = class_value

            # Check classification as attribute on <intermediateExchange> itself
            # (ecoSpold02 format uses attributes, not child <classification> element)
            if not classification_value:
                class_system = str(elem.get('classification', '')).strip()
                class_value = str(elem.get('classificationValue', '')).strip()
                if class_system == 'By-product classification':
                    classification_value = class_value
                    classification = class_value

            unit_name = units.get(unit_id, UnitRecord("", "")).name if unit_id else ""

            # Only the By-product classification decides product vs waste semantics.
            flow_type = "Product flow"
            if classification_value.lower() == 'waste':
                flow_type = "Waste flow"

            flow = IntermediateFlow(
                flow_uuid=flow_uuid,
                flow_name=flow_name,
                flow_name_en=flow_name_en,
                flow_type=flow_type,
                default_unit=unit_name,
                classification=classification,
            )
            flows.append(flow)
            elem.clear()
    
    logger.info(f"Parsed {len(flows)} intermediate exchanges")
    return flows


def parse_lcia_excel(lcia_path: Path) -> tuple[List[Indicator], List[CharacterizationFactor]]:
    """Parse LCIA Implementation 3.11.xlsx CFs and Indicators sheets."""
    if not lcia_path.exists():
        logger.warning(f"LCIA Excel file not found at {lcia_path}")
        return [], []
    
    if pd is None:
        logger.error("pandas not installed, cannot parse Excel files")
        return [], []
    
    indicators = []
    cfs = []

    def _excel_text(value: object) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value).strip()

    def _excel_float(value: object) -> float:
        if value is None:
            return 0.0
        try:
            if pd.isna(value):
                return 0.0
        except Exception:
            pass
        return float(value or 0)
    
    try:
        df_ind = pd.read_excel(lcia_path, sheet_name='Indicators')
        for _, row in df_ind.iterrows():
            indicators.append(Indicator(
                method=_excel_text(row.get('Method', '')),
                category=_excel_text(row.get('Category', '')),
                indicator=_excel_text(row.get('Indicator', '')),
                indicator_unit=_excel_text(row.get('Indicator Unit', '')),
            ))
        logger.info(f"Parsed {len(indicators)} indicators")
    except Exception as e:
        logger.warning(f"Failed to read Indicators sheet: {e}")
    
    try:
        df_cf = pd.read_excel(lcia_path, sheet_name='CFs')
        for _, row in df_cf.iterrows():
            cfs.append(CharacterizationFactor(
                method=_excel_text(row.get('Method', '')),
                category=_excel_text(row.get('Category', '')),
                indicator=_excel_text(row.get('Indicator', '')),
                flow_name=_excel_text(row.get('Name', '')),
                compartment=_excel_text(row.get('Compartment', '')) or None,
                subcompartment=_excel_text(row.get('Subcompartment', '')) or None,
                cf_value=_excel_float(row.get('CF', 0)),
            ))
        logger.info(f"Parsed {len(cfs)} characterization factors")
    except Exception as e:
        logger.warning(f"Failed to read CFs sheet: {e}")
    
    return indicators, cfs


def filter_cf_ef31(cfs: List[CharacterizationFactor]) -> List[CharacterizationFactor]:
    """Filter CFs to only include EF v3.1 and EF v3.1 no LT methods."""
    return [cf for cf in cfs if is_ef31_method(cf.method)]


def match_cf_to_flows(
    cfs: List[CharacterizationFactor],
    elementary_flows: List[ElementaryFlow]
) -> tuple[List[dict], List[dict], List[dict]]:
    """Match CFs to elementary flows using normalized name + compartment + subcompartment.
    
    Returns:
        (matched_rows, unmatched_rows, ambiguous_rows)
    """
    flow_index: Dict[tuple, List[ElementaryFlow]] = {}
    for flow in elementary_flows:
        key = (
            normalize_text(flow.flow_name),
            normalize_text(flow.compartment),
            normalize_text(flow.subcompartment),
        )
        flow_index.setdefault(key, []).append(flow)
    
    matched = []
    unmatched = []
    ambiguous = []
    
    for cf in cfs:
        key = (
            normalize_text(cf.flow_name),
            normalize_text(cf.compartment),
            normalize_text(cf.subcompartment),
        )
        
        flows = flow_index.get(key, [])
        
        if len(flows) == 1:
            matched.append({
                'cf_method': cf.method,
                'cf_category': cf.category,
                'cf_indicator': cf.indicator,
                'cf_flow_name': cf.flow_name,
                'cf_compartment': cf.compartment,
                'cf_subcompartment': cf.subcompartment,
                'cf_value': cf.cf_value,
                'matched_flow_uuid': flows[0].flow_uuid,
                'matched_flow_name': flows[0].flow_name,
            })
        elif len(flows) > 1:
            ambiguous.append({
                'cf_method': cf.method,
                'cf_category': cf.category,
                'cf_indicator': cf.indicator,
                'cf_flow_name': cf.flow_name,
                'cf_compartment': cf.compartment,
                'cf_subcompartment': cf.subcompartment,
                'cf_value': cf.cf_value,
                'matched_flow_uuids': '|'.join(f.flow_uuid for f in flows),
                'matched_flow_names': '|'.join(f.flow_name for f in flows),
            })
        else:
            unmatched.append({
                'cf_method': cf.method,
                'cf_category': cf.category,
                'cf_indicator': cf.indicator,
                'cf_flow_name': cf.flow_name,
                'cf_compartment': cf.compartment,
                'cf_subcompartment': cf.subcompartment,
                'cf_value': cf.cf_value,
            })
    
    logger.info(f"CF matching: {len(matched)} matched, {len(unmatched)} unmatched, {len(ambiguous)} ambiguous")
    return matched, unmatched, ambiguous


def parse_filename_to_activity(csv_path: Path) -> List[dict]:
    """Parse FilenameToActivityLookup.csv."""
    if not csv_path.exists():
        logger.warning(f"FilenameToActivityLookup.csv not found at {csv_path}")
        return []
    
    mappings = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = ';' if sample.count(';') > sample.count(',') else ','
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            mappings.append({
                'filename': row.get('Filename', ''),
                'activity_name': row.get('ActivityName', ''),
                'location': row.get('Location', ''),
                'reference_product': row.get('ReferenceProduct', ''),
            })
    
    logger.info(f"Parsed {len(mappings)} filename-to-activity mappings")
    return mappings


def parse_spold_metadata_early(spold_path: Path) -> Optional[LCIDataset]:
    """Parse only dataset metadata, stopping before elementary exchanges.

    Falls back to ``parse_spold_file`` when the file is missing so existing
    tests that monkeypatch the legacy parser continue to work.
    """
    if not spold_path.exists():
        return parse_spold_file(spold_path)

    try:
        activity_id = ""
        activity_name = ""
        location = ""
        ref_product_name = ""
        ref_product_unit = ""
        ref_product_amount = 0.0
        ref_product_id = ""
        fallback_intermediate: ET.Element | None = None

        def fill_reference_product(inter_exc: ET.Element) -> None:
            nonlocal ref_product_name, ref_product_unit, ref_product_amount, ref_product_id
            ref_product_name = child_text(inter_exc, 'name') or inter_exc.get('name', '')
            ref_product_unit = child_text(inter_exc, 'unitName') or inter_exc.get('unit', '')
            amount_str = inter_exc.get('amount', '0')
            try:
                ref_product_amount = float(amount_str)
            except ValueError:
                ref_product_amount = 0.0
            rp_exchange_id = inter_exc.get('intermediateExchangeId', '') or inter_exc.get('id', '')
            if rp_exchange_id:
                ref_product_id = rp_exchange_id

        context = ET.iterparse(str(spold_path), events=['end'])
        for _, elem in context:
            tag = elem.tag.split('}')[-1]
            if tag == 'activity':
                activity_id = elem.get('id', '')
                activity_name = child_text(elem, 'activityName') or elem.get('activityName', '')
                if not location:
                    location = elem.get('location', '')
            elif tag == 'geography':
                location = child_text(elem, 'shortname') or location
            elif tag == 'intermediateExchange':
                if fallback_intermediate is None:
                    fallback_intermediate = elem
                if elem.get('variableName', '') == "RP":
                    fill_reference_product(elem)
                elif not ref_product_name and child_text(elem, 'outputGroup') == "0":
                    fill_reference_product(elem)
            elif tag == 'elementaryExchange':
                break

            if activity_id and ref_product_id:
                break

        if not ref_product_name and fallback_intermediate is not None:
            fill_reference_product(fallback_intermediate)
        if not activity_id:
            return None
        return LCIDataset(
            filename=spold_path.name,
            activity_id=activity_id,
            activity_name=activity_name,
            location=location,
            reference_product_name=ref_product_name,
            reference_product_unit=ref_product_unit,
            reference_product_amount=ref_product_amount,
            reference_product_id=ref_product_id,
        )
    except Exception as e:
        logger.warning(f"Failed to parse metadata from {spold_path}: {e}")
        return parse_spold_file(spold_path)


def parse_spold_file(spold_path: Path) -> Optional[LCIDataset]:
    """Parse a single .spold LCI dataset file (ecoSpold02 format).
    
    Real ecoSpold02 LCI structure:
    - activity/@id: activity UUID
    - activity/activityName: activity name (child element, not attribute)
    - activity/geography/shortname: location (optional)
    - intermediateExchange with @variableName="RP" = reference product
    - elementaryExchange: emissions/resources with @elementaryExchangeId
    """
    try:
        tree = ET.parse(str(spold_path))
        root = tree.getroot()
        
        # Handle namespace
        ns_match = re.match(r'\{(.+)\}', root.tag)
        ns = {'es': ns_match.group(1)} if ns_match else {}
        
        # Extract activity info
        activity_elem = root.find('.//es:activity', ns)
        activity_id = ""
        activity_name = ""
        location = ""
        
        if activity_elem is not None:
            activity_id = activity_elem.get('id', '')
            
            # activityName is a child element (not attribute in ecoSpold02 LCI)
            activity_name = child_text(activity_elem, 'activityName')
            
            # Fallback: try activityName attribute (older format)
            if not activity_name:
                activity_name = activity_elem.get('activityName', '')
            
        # geography is under activityDescription, not activity.
        geo_elem = root.find('.//es:activityDescription/es:geography', ns)
        location = child_text(geo_elem, 'shortname')
        if not location:
            location = activity_elem.get('location', '') if activity_elem is not None else ""
        
        # Extract reference product
        # Real ecoSpold02 LCI uses:
        # - intermediateExchange with @variableName="RP" (Reference Product)
        # - OR intermediateExchange with @outputGroup="0"
        ref_product_name = ""
        ref_product_unit = ""
        ref_product_amount = 0.0
        ref_product_id = ""  # P1 fix: capture RP exchange id for composite key

        inter_exchanges = root.findall('.//es:intermediateExchange', ns)

        def fill_reference_product(inter_exc: ET.Element) -> None:
            nonlocal ref_product_name, ref_product_unit, ref_product_amount, ref_product_id
            ref_product_name = child_text(inter_exc, 'name') or inter_exc.get('name', '')
            ref_product_unit = child_text(inter_exc, 'unitName') or inter_exc.get('unit', '')
            amount_str = inter_exc.get('amount', '0')
            try:
                ref_product_amount = float(amount_str)
            except ValueError:
                ref_product_amount = 0.0
            # Use the product UUID first; fallback to the exchange row id for older fixtures.
            rp_exchange_id = inter_exc.get('intermediateExchangeId', '') or inter_exc.get('id', '')
            if rp_exchange_id:
                ref_product_id = rp_exchange_id

        # First try: variableName="RP"; second: outputGroup child == 0; fallback: first intermediate exchange.
        for inter_exc in inter_exchanges:
            if inter_exc.get('variableName', '') == "RP":
                fill_reference_product(inter_exc)
                break

        if not ref_product_name:
            for inter_exc in inter_exchanges:
                if child_text(inter_exc, 'outputGroup') == "0":
                    fill_reference_product(inter_exc)
                    break

        if not ref_product_name and inter_exchanges:
            fill_reference_product(inter_exchanges[0])

        if not activity_id:
            return None

        return LCIDataset(
            filename=spold_path.name,
            activity_id=activity_id,
            activity_name=activity_name,
            location=location,
            reference_product_name=ref_product_name,
            reference_product_unit=ref_product_unit,
            reference_product_amount=ref_product_amount,
            reference_product_id=ref_product_id,
        )
    except Exception as e:
        logger.warning(f"Failed to parse {spold_path}: {e}")
        return None


def parse_spold_exchanges(spold_path: Path) -> List[LCIElementaryExchange]:
    """Parse elementary exchanges from a .spold file (ecoSpold02 format).
    
    Real ecoSpold02 LCI uses:
    - elementaryExchange with @elementaryExchangeId linking to MasterData
    - Children: name, unitName, compartment, outputGroup
    - inputGroup indicates input (resource from environment)
    - outputGroup indicates output (emission to environment)
    """
    exchanges = []
    try:
        tree = ET.parse(str(spold_path))
        root = tree.getroot()
        
        ns_match = re.match(r'\{(.+)\}', root.tag)
        ns = {'es': ns_match.group(1)} if ns_match else {}
        
        # First try: parse elementaryExchange elements directly (ecoSpold02 LCI)
        for elem_exc in root.findall('.//es:elementaryExchange', ns):
            # Use elementaryExchangeId as the flow UUID (links to MasterData)
            exc_id = elem_exc.get('elementaryExchangeId', '') or elem_exc.get('id', '')
            amount_str = elem_exc.get('amount', '0')
            try:
                amount = float(amount_str)
            except ValueError:
                amount = 0.0
            
            # Parse child elements for name, unit, direction
            exc_name = ""
            unit_name = ""
            output_group = 0
            input_group = 0
            direction_hint = ""
            raw_input_group = elem_exc.get('inputGroup', '')
            if raw_input_group:
                try:
                    input_group = int(raw_input_group)
                except ValueError:
                    input_group = 1 if raw_input_group.strip() else 0
            raw_output_group = elem_exc.get('outputGroup', '')
            if raw_output_group:
                try:
                    output_group = int(raw_output_group)
                except ValueError:
                    output_group = 0
                    if raw_output_group.strip().lower() in {"output", "input"}:
                        direction_hint = raw_output_group.strip().lower()
            
            for child in elem_exc:
                tag = child.tag.split('}')[-1]
                if tag == 'name':
                    exc_name = (child.text or '').strip()
                elif tag == 'unitName':
                    unit_name = (child.text or '').strip()
                elif tag == 'outputGroup':
                    raw_child_output_group = (child.text or '').strip()
                    try:
                        output_group = int(raw_child_output_group)
                    except ValueError:
                        output_group = 0
                        if raw_child_output_group.lower() in {"output", "input"}:
                            direction_hint = raw_child_output_group.lower()
                elif tag == 'inputGroup':
                    raw_child_input_group = (child.text or '').strip()
                    try:
                        input_group = int(raw_child_input_group)
                    except ValueError:
                        input_group = 1 if raw_child_input_group else 0
            
            # Determine direction from ecoSpold groups.
            # Real ecoSpold02 resource flows commonly use inputGroup; older fixtures may encode
            # resources as negative outputGroup.
            direction = "input" if input_group > 0 else ("output" if output_group > 0 else ("input" if output_group < 0 else direction_hint))
            
            if exc_id:
                exchanges.append(LCIElementaryExchange(
                    dataset_filename=spold_path.name,
                    exchange_id=exc_id,
                    exchange_name=exc_name,
                    unit=unit_name if unit_name else f"[unitId:{elem_exc.get('unitId', '')}]",
                    direction=direction,
                    amount=amount,
                ))
        
        # Fallback: try exchange elements with type='elementary' (older format)
        if not exchanges:
            for exc in root.findall('.//es:exchange', ns):
                exc_type = exc.get('type', '')
                if exc_type != 'elementary':
                    continue
                
                exc_id = exc.get('id', '')
                exc_name = exc.get('name', '')
                unit = exc.get('unit', '')
                direction = exc.get('direction', '')
                amount_str = exc.get('amount', '0')
                try:
                    amount = float(amount_str)
                except ValueError:
                    amount = 0.0
                
                if exc_id:
                    exchanges.append(LCIElementaryExchange(
                        dataset_filename=spold_path.name,
                        exchange_id=exc_id,
                        exchange_name=exc_name,
                        unit=unit,
                        direction=direction,
                        amount=amount,
                    ))
    except Exception as e:
        logger.warning(f"Failed to parse exchanges from {spold_path}: {e}")
    
    return exchanges


# ── Single-pass parser ─────────────────────────────────────────────────────
# Combines metadata + elementary exchange parsing into one ET.parse() call
# to reduce I/O and DOM traversal cost on .spold files.


@dataclass
class _SinglePassResult:
    """Internal payload for single-pass SPOLD parsing."""
    dataset: LCIDataset
    exchanges: list[LCIElementaryExchange]


@dataclass
class _StreamingAggResult:
    """Streaming-aggregate result: metadata + flow_key_aggs dict.

    Used by the streaming parser to skip creating intermediate exchange
    objects entirely — directly accumulates aggregated amounts per
    logical flow key in one pass.
    """
    dataset: LCIDataset
    # (flow_uuid, compartment, subcompartment, direction, canonical_unit) -> amount
    flow_key_aggs: dict[tuple[str, str, str, str, str], float] = field(default_factory=dict)
    # Process JSON ready for ReferenceProcess
    process_json: dict = field(default_factory=dict)
    missing_flow_uuids: list[str] = field(default_factory=list)
    warning: str | None = None
    canonicalized: bool = True
    pack_warnings: list[str] = field(default_factory=list)


def parse_spold_streaming_agg(
    spold_path: Path,
    *,
    unit_conversion_cache: dict[str, tuple[float, str]] | None = None,
    elem_flow_lookup: dict[str, object] | None = None,
    flow_metadata_cache: dict[str, tuple[str, str]] | None = None,
) -> Optional[_StreamingAggResult]:
    """Streaming parse of a .spold file: metadata + aggregated flow keys.

    Walks the parsed XML tree and directly accumulates ``flow_key_aggs``
    without creating
    ``LCIElementaryExchange`` dataclass objects. This is the key
    optimization for reducing memory and CPU overhead.

    Returns ``None`` when metadata parsing fails.
    """
    if not spold_path.exists():
        return None

    t0 = time.perf_counter()
    unit_cache = unit_conversion_cache or {}
    elem_lookup = elem_flow_lookup or {}
    meta_cache = flow_metadata_cache or {}

    try:
        tree = ET.parse(str(spold_path))
        root = tree.getroot()

        # Handle namespace
        ns_match = re.match(r'\{(.+)\}', root.tag)
        ns = {'es': ns_match.group(1)} if ns_match else {}

        # ── Extract metadata (same as single-pass) ────────────────────
        activity_elem = root.find('.//es:activity', ns)
        activity_id = ""
        activity_name = ""
        location = ""

        if activity_elem is not None:
            activity_id = activity_elem.get('id', '')
            activity_name = child_text(activity_elem, 'activityName')
            if not activity_name:
                activity_name = activity_elem.get('activityName', '')

        geo_elem = root.find('.//es:activityDescription/es:geography', ns)
        location = child_text(geo_elem, 'shortname')
        if not location:
            location = activity_elem.get('location', '') if activity_elem is not None else ""

        # Extract reference product
        ref_product_name = ""
        ref_product_unit = ""
        ref_product_amount = 0.0
        ref_product_id = ""
        inter_exchanges = root.findall('.//es:intermediateExchange', ns)

        def _fill_rp(inter_exc: ET.Element) -> None:
            nonlocal ref_product_name, ref_product_unit, ref_product_amount, ref_product_id
            ref_product_name = child_text(inter_exc, 'name') or inter_exc.get('name', '')
            ref_product_unit = child_text(inter_exc, 'unitName') or inter_exc.get('unit', '')
            amount_str = inter_exc.get('amount', '0')
            try:
                ref_product_amount = float(amount_str)
            except ValueError:
                ref_product_amount = 0.0
            rp_exchange_id = inter_exc.get('intermediateExchangeId', '') or inter_exc.get('id', '')
            if rp_exchange_id:
                ref_product_id = rp_exchange_id

        for inter_exc in inter_exchanges:
            if inter_exc.get('variableName', '') == "RP":
                _fill_rp(inter_exc)
                break
        if not ref_product_name:
            for inter_exc in inter_exchanges:
                if child_text(inter_exc, 'outputGroup') == "0":
                    _fill_rp(inter_exc)
                    break
        if not ref_product_name and inter_exchanges:
            _fill_rp(inter_exchanges[0])

        if not activity_id:
            return None

        ds = LCIDataset(
            filename=spold_path.name,
            activity_id=activity_id,
            activity_name=activity_name,
            location=location,
            reference_product_name=ref_product_name,
            reference_product_unit=ref_product_unit,
            reference_product_amount=ref_product_amount,
            reference_product_id=ref_product_id,
        )

        flow_key_aggs: dict[tuple[str, str, str, str, str], float] = {}
        missing_flow_uuids: set[str] = set()
        pack_warnings: list[str] = []
        canonicalized = True
        exchange_count = 0

        # ── Streaming aggregation of elementary exchanges ─────────────
        for elem_exc in root.iter():
            tag = elem_exc.tag.split('}')[-1]
            if tag not in ('elementaryExchange', 'exchange'):
                continue
            if tag == 'exchange' and elem_exc.get('type', '') != 'elementary':
                continue

            exc_id = (
                elem_exc.get('elementaryExchangeId', '') or
                elem_exc.get('id', '')
            )
            if not exc_id:
                continue

            amount_str = elem_exc.get('amount', '0')
            try:
                amount = float(amount_str)
            except ValueError:
                amount = 0.0

            if amount == 0:
                continue

            # Get direction
            output_group = 0
            input_group = 0
            direction_hint = ""
            raw_input_group = elem_exc.get('inputGroup', '')
            if raw_input_group:
                try:
                    input_group = int(raw_input_group)
                except ValueError:
                    input_group = 1 if raw_input_group.strip() else 0
            raw_output_group = elem_exc.get('outputGroup', '')
            if raw_output_group:
                try:
                    output_group = int(raw_output_group)
                except ValueError:
                    output_group = 0
                    if raw_output_group.strip().lower() in {"output", "input"}:
                        direction_hint = raw_output_group.strip().lower()
            for child in elem_exc:
                child_tag = child.tag.split('}')[-1]
                child_text_val = (child.text or '').strip()
                if child_tag == 'outputGroup':
                    try:
                        output_group = int(child_text_val)
                    except ValueError:
                        output_group = 0
                        if child_text_val.lower() in {"output", "input"}:
                            direction_hint = child_text_val.lower()
                elif child_tag == 'inputGroup':
                    try:
                        input_group = int(child_text_val)
                    except ValueError:
                        input_group = 1 if child_text_val else 0

            direction = "input" if input_group > 0 else (
                "output" if output_group > 0 else (
                    "input" if output_group < 0 else direction_hint
                )
            )

            # Get unit
            unit_elem = None
            for child in elem_exc:
                if child.tag.split('}')[-1] == 'unitName':
                    unit_elem = (child.text or '').strip()
                    break
            unit_raw = unit_elem if unit_elem else elem_exc.get('unit', '')

            # Unit canonicalization
            canon_unit = unit_raw
            if unit_raw.strip():
                conv = unit_cache.get(unit_raw.strip())
                if conv is not None:
                    factor, ref = conv
                    amount = amount * factor
                    canon_unit = ref
                else:
                    canon_unit = unit_raw.strip()
                    canonicalized = False
                    pack_warnings.append(
                        f"Missing unit conversion for flow={exc_id} unit={unit_raw}"
                    )

            # Get compartment
            compartment = ""
            subcompartment = ""
            ef = elem_lookup.get(exc_id)
            if ef is not None and not isinstance(ef, str):
                compartment = getattr(ef, 'compartment', '') or ''
                subcompartment = getattr(ef, 'subcompartment', '') or ''
            elif exc_id in meta_cache:
                compartment = meta_cache.get(exc_id, ("", ""))[0]
            else:
                missing_flow_uuids.add(exc_id)

            key = (exc_id, compartment, subcompartment, direction, canon_unit)
            flow_key_aggs[key] = flow_key_aggs.get(key, 0.0) + amount
            exchange_count += 1

        # Also handle fallback format
        if not flow_key_aggs:
            for exc in root.iter():
                if exc.tag.split('}')[-1] != 'exchange':
                    continue
                if exc.get('type', '') != 'elementary':
                    continue
                exc_id = exc.get('id', '')
                if not exc_id:
                    continue
                exc_name = exc.get('name', '')
                unit = exc.get('unit', '')
                direction = exc.get('direction', '')
                amount_str = exc.get('amount', '0')
                try:
                    amount = float(amount_str)
                except ValueError:
                    amount = 0.0
                if amount == 0:
                    continue
                flow_key_aggs[(exc_id, '', '', direction, unit)] = flow_key_aggs.get((exc_id, '', '', direction, unit), 0.0) + amount
                exchange_count += 1

        warning = None
        if missing_flow_uuids:
            shown = ", ".join(sorted(missing_flow_uuids)[:5])
            if len(missing_flow_uuids) > 5:
                shown += f", ... (+{len(missing_flow_uuids) - 5} more)"
            warning = f"missing elementary flow metadata refs: {shown}"

        process_json = {
            "process_uuid": _spold_process_uuid(ds),
            "activity_id": activity_id,
            "process_name": activity_name,
            "location": location,
            "reference_product": ref_product_name,
            "reference_product_id": ref_product_id,
            "reference_product_unit": ref_product_unit,
            "reference_product_amount": ref_product_amount,
            "exchange_count": exchange_count,
            "source": "ecoinvent_3.11",
        }

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.debug(
            "Streaming parse %s: %d exchanges, %d keys, %.1fms",
            spold_path.name, exchange_count, len(flow_key_aggs), elapsed_ms,
        )

        return _StreamingAggResult(
            dataset=ds,
            flow_key_aggs=flow_key_aggs,
            process_json=process_json,
            missing_flow_uuids=sorted(missing_flow_uuids),
            warning=warning,
            canonicalized=canonicalized,
            pack_warnings=pack_warnings,
        )

    except Exception as e:
        logger.warning("Streaming parse failed for %s: %s (falling back)", spold_path, e)
        # Fall back to single-pass
        return None  # Let caller handle fallback


def parse_spold_dataset_and_exchanges(
    spold_path: Path,
    *,
    include_exchanges: bool = True,
) -> Optional[_SinglePassResult]:
    """Single-pass parse of a .spold file: metadata + elementary exchanges.

    Uses one ``ET.parse()`` call and walks the DOM once to extract both
    the ``LCIDataset`` and, when requested, all ``elementaryExchange`` elements.
    Falls back to the original two-pass path on error or when the file
    doesn't exist (e.g. during tests with mocked parsers).

    Returns ``None`` when metadata parsing fails (same as ``parse_spold_file``).
    """
    # Fast path: if the file doesn't exist (e.g. mocked tests),
    # delegate to the two-pass functions which may be monkey-patched.
    if not spold_path.exists():
        ds = parse_spold_file(spold_path)
        if ds is None:
            return None
        exchanges = parse_spold_exchanges(spold_path) if include_exchanges else []
        return _SinglePassResult(dataset=ds, exchanges=exchanges or [])
    try:
        tree = ET.parse(str(spold_path))
        root = tree.getroot()

        # Handle namespace
        ns_match = re.match(r'\{(.+)\}', root.tag)
        ns = {'es': ns_match.group(1)} if ns_match else {}

        # ── Extract metadata ──────────────────────────────────────────
        activity_elem = root.find('.//es:activity', ns)
        activity_id = ""
        activity_name = ""
        location = ""

        if activity_elem is not None:
            activity_id = activity_elem.get('id', '')
            activity_name = child_text(activity_elem, 'activityName')
            if not activity_name:
                activity_name = activity_elem.get('activityName', '')

        geo_elem = root.find('.//es:activityDescription/es:geography', ns)
        location = child_text(geo_elem, 'shortname')
        if not location:
            location = activity_elem.get('location', '') if activity_elem is not None else ""

        # Extract reference product
        ref_product_name = ""
        ref_product_unit = ""
        ref_product_amount = 0.0
        ref_product_id = ""
        inter_exchanges = root.findall('.//es:intermediateExchange', ns)

        def _fill_rp(inter_exc: ET.Element) -> None:
            nonlocal ref_product_name, ref_product_unit, ref_product_amount, ref_product_id
            ref_product_name = child_text(inter_exc, 'name') or inter_exc.get('name', '')
            ref_product_unit = child_text(inter_exc, 'unitName') or inter_exc.get('unit', '')
            amount_str = inter_exc.get('amount', '0')
            try:
                ref_product_amount = float(amount_str)
            except ValueError:
                ref_product_amount = 0.0
            rp_exchange_id = inter_exc.get('intermediateExchangeId', '') or inter_exc.get('id', '')
            if rp_exchange_id:
                ref_product_id = rp_exchange_id

        for inter_exc in inter_exchanges:
            if inter_exc.get('variableName', '') == "RP":
                _fill_rp(inter_exc)
                break
        if not ref_product_name:
            for inter_exc in inter_exchanges:
                if child_text(inter_exc, 'outputGroup') == "0":
                    _fill_rp(inter_exc)
                    break
        if not ref_product_name and inter_exchanges:
            _fill_rp(inter_exchanges[0])

        if not activity_id:
            return None

        ds = LCIDataset(
            filename=spold_path.name,
            activity_id=activity_id,
            activity_name=activity_name,
            location=location,
            reference_product_name=ref_product_name,
            reference_product_unit=ref_product_unit,
            reference_product_amount=ref_product_amount,
            reference_product_id=ref_product_id,
        )

        if not include_exchanges:
            return _SinglePassResult(dataset=ds, exchanges=[])

        # ── Extract elementary exchanges (single pass) ────────────────
        exchanges: list[LCIElementaryExchange] = []
        for elem_exc in root.findall('.//es:elementaryExchange', ns):
            exc_id = elem_exc.get('elementaryExchangeId', '') or elem_exc.get('id', '')
            amount_str = elem_exc.get('amount', '0')
            try:
                amount = float(amount_str)
            except ValueError:
                amount = 0.0

            exc_name = ""
            unit_name = ""
            output_group = 0
            input_group = 0
            direction_hint = ""
            raw_input_group = elem_exc.get('inputGroup', '')
            if raw_input_group:
                try:
                    input_group = int(raw_input_group)
                except ValueError:
                    input_group = 1 if raw_input_group.strip() else 0
            raw_output_group = elem_exc.get('outputGroup', '')
            if raw_output_group:
                try:
                    output_group = int(raw_output_group)
                except ValueError:
                    output_group = 0
                    if raw_output_group.strip().lower() in {"output", "input"}:
                        direction_hint = raw_output_group.strip().lower()

            for child in elem_exc:
                tag = child.tag.split('}')[-1]
                if tag == 'name':
                    exc_name = (child.text or '').strip()
                elif tag == 'unitName':
                    unit_name = (child.text or '').strip()
                elif tag == 'outputGroup':
                    raw_child = (child.text or '').strip()
                    try:
                        output_group = int(raw_child)
                    except ValueError:
                        output_group = 0
                        if raw_child.lower() in {"output", "input"}:
                            direction_hint = raw_child.lower()
                elif tag == 'inputGroup':
                    raw_child = (child.text or '').strip()
                    try:
                        input_group = int(raw_child)
                    except ValueError:
                        input_group = 1 if raw_child else 0

            direction = "input" if input_group > 0 else (
                "output" if output_group > 0 else (
                    "input" if output_group < 0 else direction_hint
                )
            )

            if exc_id:
                exchanges.append(LCIElementaryExchange(
                    dataset_filename=spold_path.name,
                    exchange_id=exc_id,
                    exchange_name=exc_name,
                    unit=unit_name if unit_name else f"[unitId:{elem_exc.get('unitId', '')}]",
                    direction=direction,
                    amount=amount,
                ))

        # Fallback: older format with type='elementary'
        if not exchanges:
            for exc in root.findall('.//es:exchange', ns):
                if exc.get('type', '') != 'elementary':
                    continue
                exc_id = exc.get('id', '')
                exc_name = exc.get('name', '')
                unit = exc.get('unit', '')
                direction = exc.get('direction', '')
                amount_str = exc.get('amount', '0')
                try:
                    amount = float(amount_str)
                except ValueError:
                    amount = 0.0
                if exc_id:
                    exchanges.append(LCIElementaryExchange(
                        dataset_filename=spold_path.name,
                        exchange_id=exc_id,
                        exchange_name=exc_name,
                        unit=unit,
                        direction=direction,
                        amount=amount,
                    ))

        return _SinglePassResult(dataset=ds, exchanges=exchanges)

    except Exception as e:
        logger.warning("Single-pass parse failed for %s: %s (falling back to two-pass)", spold_path, e)
        # Fall back to original two-pass
        ds = parse_spold_file(spold_path)
        if ds is None:
            return None
        if include_exchanges and spold_path.exists():
            return _SinglePassResult(dataset=ds, exchanges=parse_spold_exchanges(spold_path))
        return _SinglePassResult(dataset=ds, exchanges=[])


def write_csv(data: List[Union[dict, Any]], filepath: Path, fieldnames: Optional[List[str]] = None):
    """Write list of dicts or dataclasses to CSV."""
    if not data:
        logger.info(f"Skipping empty CSV: {filepath}")
        return
    
    if is_dataclass(data[0]) and not isinstance(data[0], dict):
        data = [asdict(row) for row in data]
    
    if not fieldnames:
        fieldnames = list(data[0].keys())
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    
    logger.info(f"Wrote {len(data)} rows to {filepath}")


# =============================================================================
# Stage 1: Foundation Loader
# =============================================================================

def cmd_inspect_foundation(args):
    """Inspect EF 3.1 foundation data (MasterData + LCIA Excel)."""
    master_dir = Path(args.master_data_dir)
    lcia_path = Path(args.lcia_implementation)
    
    if not master_dir.exists():
        logger.error(f"Master data directory not found: {master_dir}")
        sys.exit(1)
    if not lcia_path.exists():
        logger.error(f"LCIA implementation file not found: {lcia_path}")
        sys.exit(1)
    
    print(f"\n=== EF 3.1 Foundation Inspection ===")
    print(f"Master data directory: {master_dir}")
    print(f"LCIA implementation: {lcia_path}\n")
    
    units = parse_units(master_dir)
    print(f"Units: {len(units)}")
    
    conversions = parse_unit_conversions(master_dir)
    print(f"Unit conversions: {len(conversions)}")
    
    elem_flows = parse_elementary_exchanges(master_dir, units)
    print(f"Elementary flows: {len(elem_flows)}")
    
    inter_flows = parse_intermediate_exchanges(master_dir, units)
    print(f"Intermediate flows: {len(inter_flows)}")
    
    indicators, cfs = parse_lcia_excel(lcia_path)
    print(f"Indicators (total): {len(indicators)}")
    print(f"Characterization factors (total): {len(cfs)}")
    
    # EF 3.1 filter
    ef31_cfs = filter_cf_ef31(cfs)
    print(f"CFs (EF v3.1 only): {len(ef31_cfs)}")
    
    # Compartment distribution
    compartments = {}
    for f in elem_flows:
        comp = f.compartment or "(none)"
        compartments[comp] = compartments.get(comp, 0) + 1
    
    print(f"\nElementary flow compartments (top 10):")
    for comp, count in sorted(compartments.items(), key=lambda x: -x[1])[:10]:
        print(f"  {comp}: {count}")
    
    # Intermediate flow types
    flow_types = {}
    for f in inter_flows:
        ft = f.flow_type
        flow_types[ft] = flow_types.get(ft, 0) + 1
    
    print(f"\nIntermediate flow types:")
    for ft, count in sorted(flow_types.items(), key=lambda x: -x[1]):
        print(f"  {ft}: {count}")


def cmd_export_foundation_preview(args):
    """Export foundation preview CSVs and JSON report."""
    master_dir = Path(args.master_data_dir)
    lcia_path = Path(args.lcia_implementation)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    if not master_dir.exists():
        logger.error(f"Master data directory not found: {master_dir}")
        sys.exit(1)
    if not lcia_path.exists():
        logger.error(f"LCIA implementation file not found: {lcia_path}")
        sys.exit(1)
    
    print(f"\n=== EF 3.1 Foundation Export Preview ===")
    print(f"Master data directory: {master_dir}")
    print(f"LCIA implementation: {lcia_path}")
    print(f"Output directory: {out_dir}\n")
    
    units = parse_units(master_dir)
    conversions = parse_unit_conversions(master_dir)
    elem_flows = parse_elementary_exchanges(master_dir, units)
    inter_flows = parse_intermediate_exchanges(master_dir, units)
    indicators, cfs = parse_lcia_excel(lcia_path)
    
    # Filter to EF 3.1
    ef31_indicators = [ind for ind in indicators if is_ef31_method(ind.method)]
    ef31_cfs = filter_cf_ef31(cfs)
    
    # Match CFs
    matched, unmatched, ambiguous = match_cf_to_flows(ef31_cfs, elem_flows)
    
    # Write CSVs
    write_csv(elem_flows, out_dir / "elementary_flows.csv", fieldnames=[
        'flow_uuid', 'flow_name', 'flow_name_en', 'flow_type',
        'default_unit', 'unit_group', 'compartment', 'subcompartment',
        'cas_number', 'formula', 'source'
    ])
    
    write_csv(inter_flows, out_dir / "intermediate_flows.csv", fieldnames=[
        'flow_uuid', 'flow_name', 'flow_name_en', 'flow_type',
        'default_unit', 'unit_group', 'classification', 'source'
    ])

    # Convert units dict to list of UnitRecord for CSV export
    units_list = list(units.values())
    write_csv(units_list, out_dir / "units.csv", fieldnames=[
        'unit_id', 'name', 'unit_type'
    ])

    write_csv(conversions, out_dir / "unit_conversions.csv", fieldnames=[
        'conversion_id', 'unit_from_name', 'unit_to_name', 'unit_type', 'factor'
    ])
    
    write_csv(ef31_indicators, out_dir / "ef31_indicators.csv", fieldnames=[
        'method', 'category', 'indicator', 'indicator_unit'
    ])
    
    write_csv(matched, out_dir / "ef31_characterization_factors.csv", fieldnames=[
        'cf_method', 'cf_indicator', 'cf_flow_name', 'cf_compartment',
        'cf_subcompartment', 'cf_value', 'matched_flow_uuid', 'matched_flow_name'
    ])
    
    write_csv(unmatched, out_dir / "unmatched_cf_rows.csv", fieldnames=[
        'cf_method', 'cf_indicator', 'cf_flow_name', 'cf_compartment',
        'cf_subcompartment', 'cf_value'
    ])
    
    write_csv(ambiguous, out_dir / "ambiguous_cf_rows.csv", fieldnames=[
        'cf_method', 'cf_indicator', 'cf_flow_name', 'cf_compartment',
        'cf_subcompartment', 'cf_value', 'matched_flow_uuids', 'matched_flow_names'
    ])
    
    # Write report
    report = FoundationReport(
        master_data_dir=str(master_dir),
        lcia_implementation_path=str(lcia_path),
        elementary_flows_count=len(elem_flows),
        intermediate_flows_count=len(inter_flows),
        units_count=len(units),
        unit_conversions_count=len(conversions),
        indicators_total=len(indicators),
        indicators_ef31=len(ef31_indicators),
        cf_rows_total=len(cfs),
        cf_rows_ef31=len(ef31_cfs),
        cf_rows_matched=len(matched),
        cf_rows_unmatched=len(unmatched),
        cf_rows_ambiguous=len(ambiguous),
    )
    
    with open(out_dir / "foundation_report.json", 'w', encoding='utf-8') as f:
        json.dump(asdict(report), f, indent=2, ensure_ascii=False)
    logger.info(f"Wrote report to {out_dir / 'foundation_report.json'}")
    
    print(f"\n=== Summary ===")
    print(f"Elementary flows: {len(elem_flows)}")
    print(f"Intermediate flows: {len(inter_flows)}")
    print(f"Units: {len(units)}")
    print(f"Unit conversions: {len(conversions)}")
    print(f"Indicators: {len(ef31_indicators)} / {len(indicators)} (EF 3.1 / total)")
    print(f"CF rows: {len(ef31_cfs)} / {len(cfs)} (EF 3.1 / total)")
    print(f"CF matched: {len(matched)}, unmatched: {len(unmatched)}, ambiguous: {len(ambiguous)}")
    print(f"\nOutput files:")
    for f in [
        "elementary_flows.csv", "intermediate_flows.csv", "units.csv",
        "unit_conversions.csv", "ef31_indicators.csv",
        "ef31_characterization_factors.csv", "unmatched_cf_rows.csv",
        "ambiguous_cf_rows.csv", "foundation_report.json"
    ]:
        print(f"  {f}")


# =============================================================================
# Stage 2: LCI Dataset Preview
# =============================================================================

def cmd_preview_lci(args):
    """Preview LCI datasets from extracted directory."""
    lci_dir = Path(args.lci_dir)
    master_dir = Path(args.master_data_dir)
    limit = int(args.limit) if args.limit else 100
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    if not lci_dir.exists():
        logger.error(f"LCI directory not found: {lci_dir}")
        sys.exit(1)
    if not master_dir.exists():
        logger.error(f"Master data directory not found: {master_dir}")
        sys.exit(1)
    
    print(f"\n=== EF 3.1 LCI Dataset Preview ===")
    print(f"LCI directory: {lci_dir}")
    print(f"Master data directory: {master_dir}")
    print(f"Limit: {limit} datasets")
    print(f"Output directory: {out_dir}\n")
    
    # Parse master data for elementary flow registry
    units = parse_units(master_dir)
    elem_flows = parse_elementary_exchanges(master_dir, units)
    elem_flow_ids = {f.flow_uuid for f in elem_flows}
    
    # Find datasets directory
    datasets_dir = lci_dir / "datasets"
    if not datasets_dir.exists():
        datasets_dir = lci_dir
    
    spold_files = list(datasets_dir.glob("*.spold"))[:limit]
    print(f"Found {len(spold_files)} .spold files (limit={limit})")
    
    datasets = []
    all_exchanges = []
    missing_refs = []
    
    for i, spold_path in enumerate(spold_files):
        dataset = parse_spold_file(spold_path)
        if dataset:
            datasets.append(dataset)
            exchanges = parse_spold_exchanges(spold_path)
            dataset.exchange_count = len(exchanges)
            all_exchanges.extend(exchanges)
            
            # Check for missing elementary flow refs
            for exc in exchanges:
                if exc.exchange_id and exc.exchange_id not in elem_flow_ids:
                    if len(missing_refs) < 1000:
                        missing_refs.append({
                            'dataset_filename': spold_path.name,
                            'exchange_id': exc.exchange_id,
                            'exchange_name': exc.exchange_name,
                        })
        
        if (i + 1) % 20 == 0:
            logger.info(f"Parsed {i + 1}/{len(spold_files)} datasets...")
    
    # Write CSVs
    write_csv(datasets, out_dir / "lci_datasets.csv", fieldnames=[
        'filename', 'activity_id', 'activity_name', 'location',
        'reference_product_name', 'reference_product_unit',
        'reference_product_amount', 'exchange_count'
    ])
    
    write_csv(all_exchanges, out_dir / "lci_elementary_exchanges.csv", fieldnames=[
        'dataset_filename', 'exchange_id', 'exchange_name', 'unit', 'direction', 'amount'
    ])
    
    write_csv(missing_refs, out_dir / "missing_elementary_flow_refs.csv", fieldnames=[
        'dataset_filename', 'exchange_id', 'exchange_name'
    ])
    
    # Write report
    report = LCIPreviewReport(
        lci_dir=str(lci_dir),
        master_data_dir=str(master_dir),
        limit=limit,
        datasets_scanned=len(spold_files),
        datasets_parsed=len(datasets),
        lci_elementary_exchanges_count=len(all_exchanges),
        missing_elementary_refs=len(missing_refs),
    )
    
    with open(out_dir / "lci_preview_report.json", 'w', encoding='utf-8') as f:
        json.dump(asdict(report), f, indent=2, ensure_ascii=False)
    logger.info(f"Wrote report to {out_dir / 'lci_preview_report.json'}")
    
    print(f"\n=== Summary ===")
    print(f"Datasets scanned: {len(spold_files)}")
    print(f"Datasets parsed: {len(datasets)}")
    print(f"LCI elementary exchanges: {len(all_exchanges)}")
    print(f"Missing elementary flow refs: {len(missing_refs)}")
    print(f"\nOutput files:")
    for f in ["lci_datasets.csv", "lci_elementary_exchanges.csv",
              "missing_elementary_flow_refs.csv", "lci_preview_report.json"]:
        print(f"  {f}")


# =============================================================================
# Stage 3: Archive Support
# =============================================================================

def selective_extract_7z(archive_path: Path, dest_dir: Path, 
                          spold_limit: Optional[int] = None) -> dict:
    """Selectively extract files from .7z archive.
    
    Only extracts:
    - MasterData/*.xml
    - FilenameToActivityLookup.csv (if present)
    - LCIA Implementation 3.11.xlsx
    - First N datasets/*.spold files (controlled by spold_limit)
    
    Returns dict of extracted paths.
    """
    if py7zr is None:
        logger.error("py7zr not installed, cannot extract .7z archives")
        sys.exit(1)
    
    result = {
        'master_dir': None,
        'datasets_dir': None,
        'lcia_excel': None,
        'spold_count': 0,          # selected (after limit)
        'spold_count_total': 0,     # total found in archive (before limit)
        'master_data_count': 0,
    }
    
    logger.info(f"Scanning archive: {archive_path}")
    with py7zr.SevenZipFile(str(archive_path), mode='r') as z:
        all_members = z.getnames()
        
        # Categorize members
        master_data_files = []
        spold_files = []
        lcia_excel_path = None
        filename_lookup_path = None
        
        for member in all_members:
            # Normalize backslashes (Windows py7zr may return \)
            m = member.replace('\\', '/')
            # MasterData/*.xml
            if 'MasterData/' in m and m.endswith('.xml'):
                master_data_files.append(member)
            # LCIA Excel
            elif 'LCIA Implementation' in m and m.endswith('.xlsx'):
                lcia_excel_path = member
            # FilenameToActivityLookup.csv
            elif 'FilenameToActivityLookup.csv' in m:
                filename_lookup_path = member
            # datasets/*.spold
            elif '/datasets/' in m and m.endswith('.spold'):
                spold_files.append(member)
            # Also check for datasets at root level
            elif m.startswith('datasets/') and m.endswith('.spold'):
                spold_files.append(member)
        
        # Record total before applying limit
        result['spold_count_total'] = len(spold_files)

        # Limit spold files
        if spold_limit and spold_limit > 0:
            spold_files = spold_files[:spold_limit]
        
        # Build extraction list
        extract_list = master_data_files + spold_files
        if lcia_excel_path:
            extract_list.append(lcia_excel_path)
        if filename_lookup_path:
            extract_list.append(filename_lookup_path)
        
        if not extract_list:
            logger.warning(f"No relevant files found in archive: {archive_path}")
            return result
        
        logger.info(f"Extracting {len(extract_list)} files from archive...")
        logger.info(f"  - MasterData files: {len(master_data_files)}")
        logger.info(f"  - LCI datasets (.spold): {len(spold_files)}")
        if lcia_excel_path:
            logger.info(f"  - LCIA Excel: {lcia_excel_path}")
        if filename_lookup_path:
            logger.info(f"  - FilenameToActivityLookup.csv: {filename_lookup_path}")
        
        # Extract selected files
        with py7zr.SevenZipFile(str(archive_path), mode='r') as z:
            z.extract(path=str(dest_dir), targets=extract_list)
        
        result['spold_count'] = len(spold_files)
        result['master_data_count'] = len(master_data_files)

        # Find extracted directories
        for item in dest_dir.rglob("MasterData"):
            if item.is_dir():
                result['master_dir'] = item
                break
        
        for item in dest_dir.rglob("datasets"):
            if item.is_dir():
                result['datasets_dir'] = item
                break
        
        # Find LCIA Excel
        if lcia_excel_path:
            # Extract relative path from archive
            relative_name = lcia_excel_path.split('/')[-1]
            for item in dest_dir.rglob(relative_name):
                if item.is_file():
                    result['lcia_excel'] = item
                    break
    
    return result


def find_archive_root(extract_dir: Path) -> tuple[Optional[Path], Optional[Path], Optional[Path]]:
    """Find MasterData dir, datasets dir, and LCIA Excel in extracted archive."""
    master_dir = None
    datasets_dir = None
    lcia_excel = None

    # Search for MasterData directory
    for item in extract_dir.rglob("MasterData"):
        if item.is_dir():
            master_dir = item
            break

    # Search for datasets directory
    for item in extract_dir.rglob("datasets"):
        if item.is_dir():
            datasets_dir = item
            break

    # Search for LCIA Excel
    for pattern in ["LCIA Implementation 3.11.xlsx", "LCIA*.xlsx"]:
        for item in extract_dir.rglob(pattern):
            if item.is_file():
                lcia_excel = item
                break
        if lcia_excel:
            break

    return master_dir, datasets_dir, lcia_excel


def cmd_preview_archives(args):
    """Preview LCI/LCIA data from official .7z archives."""
    lci_archive = Path(args.lci_archive) if args.lci_archive else None
    lcia_archive = Path(args.lcia_archive) if args.lcia_archive else None
    limit = int(args.limit) if args.limit else 100
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if lci_archive and not lci_archive.exists():
        logger.error(f"LCI archive not found: {lci_archive}")
        sys.exit(1)
    if lcia_archive and not lcia_archive.exists():
        logger.error(f"LCIA archive not found: {lcia_archive}")
        sys.exit(1)
    if not lci_archive and not lcia_archive:
        logger.error("At least one archive file must be specified")
        sys.exit(1)

    print(f"\n=== EF 3.1 Archive Preview ===")
    print(f"LCI archive: {lci_archive}")
    print(f"LCIA archive: {lcia_archive}")
    print(f"Limit: {limit} datasets")
    print(f"Output directory: {out_dir}\n")

    # Create temp directory for selective extraction
    temp_base = tempfile.mkdtemp(prefix="ef31_archive_")
    temp_dir = Path(temp_base)

    try:
        master_dir = None
        datasets_dir = None
        lcia_excel = None
        spold_extracted = 0

        # Selective extract LCI archive (only MasterData + first N spold files)
        if lci_archive:
            lci_extract = temp_dir / "lci"
            result = selective_extract_7z(lci_archive, lci_extract, spold_limit=limit)
            master_dir = result.get('master_dir')
            datasets_dir = result.get('datasets_dir')
            spold_extracted = result.get('spold_count', 0)

        # Selective extract LCIA archive (only LCIA Excel)
        if lcia_archive:
            lcia_extract = temp_dir / "lcia"
            result = selective_extract_7z(lcia_archive, lcia_extract, spold_limit=0)
            lcia_excel = result.get('lcia_excel')

        if not master_dir:
            logger.error("Could not find MasterData directory in archives")
            sys.exit(1)

        # Run foundation preview
        print("\n--- Running Foundation Preview ---")
        units = parse_units(master_dir)
        elem_flows = parse_elementary_exchanges(master_dir, units)
        elem_flow_ids = {f.flow_uuid for f in elem_flows}
        print(f"Elementary flows: {len(elem_flows)}")

        if lcia_excel:
            indicators, cfs = parse_lcia_excel(lcia_excel)
            ef31_cfs = filter_cf_ef31(cfs)
            print(f"CFs (EF 3.1): {len(ef31_cfs)}")

        # Run LCI preview
        if datasets_dir:
            print("\n--- Running LCI Preview ---")
            spold_files = list(datasets_dir.glob("*.spold"))
            print(f"Found {len(spold_files)} .spold files (limit already applied during extraction)")

            datasets = []
            all_exchanges = []
            missing_refs = []

            for i, spold_path in enumerate(spold_files):
                dataset = parse_spold_file(spold_path)
                if dataset:
                    datasets.append(dataset)
                    exchanges = parse_spold_exchanges(spold_path)
                    dataset.exchange_count = len(exchanges)
                    all_exchanges.extend(exchanges)

                    for exc in exchanges:
                        if exc.exchange_id and exc.exchange_id not in elem_flow_ids:
                            if len(missing_refs) < 1000:
                                missing_refs.append({
                                    'dataset_filename': spold_path.name,
                                    'exchange_id': exc.exchange_id,
                                    'exchange_name': exc.exchange_name,
                                })

            write_csv(datasets, out_dir / "lci_datasets.csv", fieldnames=[
                'filename', 'activity_id', 'activity_name', 'location',
                'reference_product_name', 'reference_product_unit',
                'reference_product_amount', 'exchange_count'
            ])
            
            write_csv(all_exchanges, out_dir / "lci_elementary_exchanges.csv", fieldnames=[
                'dataset_filename', 'exchange_id', 'exchange_name', 'unit', 'direction', 'amount'
            ])
            
            write_csv(missing_refs, out_dir / "missing_elementary_flow_refs.csv", fieldnames=[
                'dataset_filename', 'exchange_id', 'exchange_name'
            ])
            
            print(f"Datasets parsed: {len(datasets)}")
            print(f"LCI elementary exchanges: {len(all_exchanges)}")
        
        # Write archive report
        report = {
            'lci_archive': str(lci_archive) if lci_archive else None,
            'lcia_archive': str(lcia_archive) if lcia_archive else None,
            'limit': limit,
            'spold_extracted': spold_extracted,
            'elementary_flows_count': len(elem_flows),
            'datasets_parsed': len(datasets) if datasets_dir else 0,
            'lci_exchanges_count': len(all_exchanges) if datasets_dir else 0,
            'missing_refs_count': len(missing_refs) if datasets_dir else 0,
        }

        with open(out_dir / "archive_preview_report.json", 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Wrote report to {out_dir / 'archive_preview_report.json'}")

    finally:
        # Cleanup temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_base, ignore_errors=True)
            logger.info(f"Cleaned up temp directory: {temp_base}")


def main():
    parser = argparse.ArgumentParser(
        description='EF 3.1 LCI/LCIA Foundation Loader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stage 1: Foundation inspection
  python -m app.ecoinvent_ef31_loader inspect-foundation \\
    --master-data-dir "D:\\ecoinvent\\MasterData" \\
    --lcia-implementation "D:\\ecoinvent\\LCIA Implementation 3.11.xlsx"

  # Stage 1: Foundation export preview
  python -m app.ecoinvent_ef31_loader export-foundation-preview \\
    --master-data-dir "D:\\ecoinvent\\MasterData" \\
    --lcia-implementation "D:\\ecoinvent\\LCIA Implementation 3.11.xlsx" \\
    --out ".\\tmp\\ef31_foundation_preview"

  # Stage 2: LCI dataset preview
  python -m app.ecoinvent_ef31_loader preview-lci \\
    --lci-dir "D:\\ecoinvent\\ecoinvent 3.11_cutoff_lci_ecoSpold02" \\
    --master-data-dir "D:\\ecoinvent\\MasterData" \\
    --limit 100 \\
    --out ".\\tmp\\ef31_lci_preview"

  # Stage 3: Archive preview
  python -m app.ecoinvent_ef31_loader preview-archives \\
    --lci-archive "D:\\ecoinvent\\ecoinvent 3.11_cutoff_lci_ecoSpold02.7z" \\
    --lcia-archive "D:\\ecoinvent\\ecoinvent 3.11_LCIA_implementation.7z" \\
    --limit 100 \\
    --out ".\\tmp\\ef31_archive_preview"
"""
    )
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Stage 1: inspect-foundation
    inspect_parser = subparsers.add_parser('inspect-foundation', help='Inspect EF 3.1 foundation data')
    inspect_parser.add_argument('--master-data-dir', required=True, help='Path to MasterData directory')
    inspect_parser.add_argument('--lcia-implementation', required=True, help='Path to LCIA Implementation 3.11.xlsx')
    inspect_parser.set_defaults(func=cmd_inspect_foundation)
    
    # Stage 1: export-foundation-preview
    export_parser = subparsers.add_parser('export-foundation-preview', help='Export foundation preview CSVs')
    export_parser.add_argument('--master-data-dir', required=True, help='Path to MasterData directory')
    export_parser.add_argument('--lcia-implementation', required=True, help='Path to LCIA Implementation 3.11.xlsx')
    export_parser.add_argument('--out', required=True, help='Output directory for CSVs and report')
    export_parser.set_defaults(func=cmd_export_foundation_preview)
    
    # Stage 2: preview-lci
    lci_parser = subparsers.add_parser('preview-lci', help='Preview LCI datasets')
    lci_parser.add_argument('--lci-dir', required=True, help='Path to extracted LCI directory')
    lci_parser.add_argument('--master-data-dir', required=True, help='Path to MasterData directory')
    lci_parser.add_argument('--limit', type=int, default=100, help='Max datasets to parse (default: 100)')
    lci_parser.add_argument('--out', required=True, help='Output directory')
    lci_parser.set_defaults(func=cmd_preview_lci)
    
    # Stage 3: preview-archives
    archive_parser = subparsers.add_parser('preview-archives', help='Preview from .7z archives')
    archive_parser.add_argument('--lci-archive', help='Path to LCI .7z archive')
    archive_parser.add_argument('--lcia-archive', help='Path to LCIA .7z archive')
    archive_parser.add_argument('--limit', type=int, default=100, help='Max datasets to parse (default: 100)')
    archive_parser.add_argument('--out', required=True, help='Output directory')
    archive_parser.set_defaults(func=cmd_preview_archives)
    
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
