"""TIDAS Bundle Export Module.

Exports project models to TIDAS bundle ZIP format compatible with existing importers.
"""

import io
import json
import zipfile
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from .models import Model, ModelVersion, FlowRecord, ReferenceProcess, UnitDefinition
from .schemas import HybridGraph


class ExportError(Exception):
    """Raised when export validation fails."""

    pass


class ExportWarning:
    """Represents a non-blocking warning during export."""

    def __init__(self, category: str, message: str, context: dict | None = None):
        self.category = category
        self.message = message
        self.context = context or {}

    def to_dict(self) -> dict:
        return {
            "category": self.category,
            "message": self.message,
            "context": self.context,
        }


class ExportReport:
    """Collects export diagnostics."""

    def __init__(self):
        self.warnings: list[ExportWarning] = []
        self.errors: list[str] = []
        self.flow_count = 0
        self.process_count = 0
        self.exported_model_count = 0
        # Allocation tracking for multi-product processes
        self.multi_product_process_count = 0
        self.allocation_warnings: list[ExportWarning] = []
        self.manual_allocation_required_processes: list[str] = []
        self.reference_flow_by_process: dict[str, str] = {}

    def add_warning(self, category: str, message: str, context: dict | None = None):
        self.warnings.append(ExportWarning(category, message, context))
        if category == "allocation":
            self.allocation_warnings.append(ExportWarning(category, message, context))

    def add_error(self, message: str):
        self.errors.append(message)

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def to_dict(self) -> dict:
        return {
            "exported_at": datetime.utcnow().isoformat(),
            "flow_count": self.flow_count,
            "process_count": self.process_count,
            "exported_model_count": self.exported_model_count,
            "multi_product_process_count": self.multi_product_process_count,
            "allocation_warnings": [w.to_dict() for w in self.allocation_warnings],
            "manual_allocation_required_processes": self.manual_allocation_required_processes,
            "reference_flow_by_process": self.reference_flow_by_process,
            "warnings": [w.to_dict() for w in self.warnings],
            "errors": self.errors,
        }


PTS_TIDAS_EXPORT_ERROR_CODE = "PTS_MODULE_NOT_SUPPORTED_FOR_TIDAS_EXPORT"
PTS_TIDAS_EXPORT_MESSAGE = (
    "Current TIDAS export does not support PTS modules. "
    "Please unpack PTS in the modeling canvas before exporting unit process, market process, or LCI data."
)


SOURCE_SPACE_TIDAS_BLOCKED_CODE = "TIANGONG_TIDAS_BLOCKED_BY_SOURCE_SPACE"
SOURCE_SPACE_TIDAS_BLOCKED_MESSAGE = (
    "开源版不支持 EF / ecoinvent 基本流的自动转换。模型中包含 ecoinvent 或其他非 EF/Tiangong 来源的基本流，不能导出天工 TIDAS。"
)

# Source-space classification constants
SOURCE_SPACE_EF_TIANGONG = "ef_tiangong"
SOURCE_SPACE_ECOSPREAD = "ecoinvent"  # legacy naming preserved
SOURCE_SPACE_UNKNOWN = "unknown"

# Tokens that explicitly indicate EF / TianGong source space.
# These are checked as whole-word matches against the lower-cased source string
# to avoid false positives like "reference" → "ef".
EF_TIANGONG_TOKENS = ("tiangong", "ef3.1", "ef31", "official")
# EF is matched as a stand-alone token, not as a substring:
#   "ef"        → match
#   "reference" → no match (no "ef" token boundary)
#   "ef3.1"     → also matched above, but "ef" won't catch it anyway
EF_TOKEN = "ef"


def _classify_source_space(source: str | None) -> str:
    """Classify a flow's source space from its FlowRecord.source field.

    Returns one of:
        SOURCE_SPACE_EF_TIANGONG  – matches EF / TianGong tokens (case-insensitive)
        SOURCE_SPACE_ECOSPREAD    – contains "ecoinvent"
        SOURCE_SPACE_UNKNOWN      – empty / unclassifiable

    Token matching rules (avoid "reference"/"undefined" false positives):
        - Multi-char tokens (tiangong, ef3.1, ef31, official): word-boundary match
          using split() + set containment.
        - "ef" (stand-alone only): split() + set containment.
          "reference" → "reference" in split set → no "ef" token found.
          "ef3.1.104" → tokens: ["ef3.1.104"] → matches "ef3.1" as sub-token.
    """
    if not source:
        return SOURCE_SPACE_UNKNOWN

    src_lower = source.lower()
    src_tokens = set(src_lower.split())

    # ecoinvent check (no false-positive risk since EF/TG tokens don't contain it)
    if "ecoinvent" in src_lower:
        return SOURCE_SPACE_ECOSPREAD

    # Check each token for EF/TianGong prefixes.
    # "ef3.1.104".split() → {"ef3.1.104"} → startswith("ef3.1") → match
    # "reference".split() → {"reference"} → no match (safe)
    # "undefined".split() → {"undefined"} → no match (safe)
    for token in src_tokens:
        for match_token in EF_TIANGONG_TOKENS:
            if token == match_token or token.startswith(match_token + ".") or token.startswith(match_token + "-"):
                return SOURCE_SPACE_EF_TIANGONG
        # Stand-alone "ef" check: exact match only (already excluded by ef3.1/ef31 above)
        if token == EF_TOKEN:
            return SOURCE_SPACE_EF_TIANGONG

    return SOURCE_SPACE_UNKNOWN


def _scan_source_space(
    db: Session, flow_uuids: set[str], report: ExportReport
) -> bool:
    """Scan elementary flows in the graph for unsupported source spaces.

    Logic:
    - Only elementary flows are subject to source-space blocking.
    - Product / waste flows are allowed regardless of source.
    - If ANY elementary flow has source space != ef_tiangong, block export.

    Returns:
        True if source-space check passed (no blocking issues).
        False if blocking issues were found (errors/warnings already added to report).
    """
    # Collect elementary flows grouped by source space
    blocked_by_space: dict[str, list[str]] = {}  # space -> [flow_uuids]

    for flow_uuid in flow_uuids:
        flow_record = db.get(FlowRecord, flow_uuid)
        if flow_record is None:
            continue  # already reported as missing

        # Only elementary flows are subject to source-space check.
        # Normalize common DB variations: "Elementary flow", "elementary_flow",
        # "Elementary_flow", "elementary flow", etc.
        ft = (flow_record.flow_type or "").replace("_", " ").strip().lower()
        if ft != "elementary flow":
            continue

        source = str(flow_record.source or "").strip()
        space = _classify_source_space(source)

        if space == SOURCE_SPACE_EF_TIANGONG:
            # Allowed – no action needed
            continue

        blocked_by_space.setdefault(space, []).append(flow_uuid)

    # If no blocked elementary flows, everything is fine
    if not blocked_by_space:
        return True

    # Report blocking errors per source space
    all_blocked_uuids: list[str] = []
    for space, uuids in blocked_by_space.items():
        all_blocked_uuids.extend(uuids)
        count = len(uuids)
        preview_uuids = uuids[:10]  # first 10 for the warning context

        if space == SOURCE_SPACE_UNKNOWN:
            detail = f"basic flow source unknown: {', '.join(preview_uuids)}"
            if count > 10:
                detail = f"{detail}, ..."
            msg = f"{SOURCE_SPACE_TIDAS_BLOCKED_MESSAGE} (source-space unknown, {count} flow(s))"
        elif space == SOURCE_SPACE_ECOSPREAD:
            detail = f"ecoinvent basic flows: {', '.join(preview_uuids)}"
            if count > 10:
                detail = f"{detail}, ..."
            msg = f"{SOURCE_SPACE_TIDAS_BLOCKED_MESSAGE} (ecoinvent basic flows, {count} flow(s))"
        else:
            detail = f"unsupported source '{space}': {', '.join(preview_uuids)}"
            if count > 10:
                detail = f"{detail}, ..."
            msg = f"{SOURCE_SPACE_TIDAS_BLOCKED_MESSAGE} (unsupported source '{space}', {count} flow(s))"

        error_code = f"{SOURCE_SPACE_TIDAS_BLOCKED_CODE}: {space}"
        report.add_error(f"{error_code}: {msg}")
        report.add_warning(
            "unsupported_source_space",
            msg,
            {
                "source_space": space,
                "blocked_count": count,
                "flow_uuids": preview_uuids,
            },
        )

    return False


def _get_model_version(db: Session, project_id: str, version: int | None = None) -> ModelVersion | None:
    """Fetch model version by project_id and optional version number.

    If version is None, fetches the latest version.
    """
    query = db.query(ModelVersion).filter(ModelVersion.model_id == project_id)

    if version is not None:
        return query.filter(ModelVersion.version == version).first()

    # Latest version
    latest = query.order_by(ModelVersion.version.desc()).first()
    return latest


def _find_pts_nodes(graph_json: dict) -> list[dict]:
    """Return PTS nodes from a graph JSON payload."""
    pts_nodes: list[dict] = []
    for node in graph_json.get("nodes", []):
        if isinstance(node, dict) and str(node.get("node_kind") or "").strip() == "pts_module":
            pts_nodes.append(node)
    return pts_nodes


def _add_pts_export_blocker(report: ExportReport, pts_nodes: list[dict]) -> None:
    """Record a blocking export error for graphs that still contain PTS modules."""
    pts_refs = [
        str(node.get("process_uuid") or node.get("pts_uuid") or node.get("id") or "").strip()
        for node in pts_nodes
    ]
    pts_refs = [ref for ref in pts_refs if ref]
    detail = PTS_TIDAS_EXPORT_MESSAGE
    if pts_refs:
        detail = f"{detail} PTS nodes: {', '.join(pts_refs[:5])}"
        if len(pts_refs) > 5:
            detail = f"{detail}, ..."
    report.add_error(f"{PTS_TIDAS_EXPORT_ERROR_CODE}: {detail}")
    report.add_warning(
        "unsupported_pts_export",
        PTS_TIDAS_EXPORT_MESSAGE,
        {"pts_nodes": pts_refs},
    )


def _build_flow_type_map(db: Session, flow_uuids: set[str]) -> dict[str, str]:
    """Build a map of flow_uuid -> flow_type for quick lookup.

    Args:
        db: Database session
        flow_uuids: Set of flow UUIDs to look up

    Returns:
        Dict mapping flow_uuid to flow_type
    """
    flow_type_map = {}
    for flow_uuid in flow_uuids:
        flow_record = db.get(FlowRecord, flow_uuid)
        if flow_record:
            flow_type_map[flow_uuid] = flow_record.flow_type
    return flow_type_map


def _extract_graph_data(graph_json: dict) -> tuple[set[str], set[str]]:
    """Extract unique flow_uuids and process_uuids from graph.

    Returns:
        Tuple of (flow_uuids, process_uuids)
    """
    flow_uuids: set[str] = set()
    process_uuids: set[str] = set()

    nodes = graph_json.get("nodes", [])
    for node in nodes:
        if not isinstance(node, dict):
            continue

        # Use process_uuid from node, fallback to id only if process_uuid missing
        process_uuid = node.get("process_uuid") or node.get("id")
        if process_uuid:
            process_uuids.add(str(process_uuid))

        # Collect flow UUIDs from ports
        for bucket in ("inputs", "outputs"):
            ports = node.get(bucket, [])
            for port in ports:
                if not isinstance(port, dict):
                    continue
                flow_uuid = port.get("flowUuid")
                if flow_uuid:
                    flow_uuids.add(str(flow_uuid))

    # Also check exchanges if present
    exchanges = graph_json.get("exchanges", [])
    for exchange in exchanges:
        if not isinstance(exchange, dict):
            continue
        flow_uuid = exchange.get("flowUuid")
        if flow_uuid:
            flow_uuids.add(str(flow_uuid))

    return flow_uuids, process_uuids


def _identify_product_outputs(
    outputs: list,
    flow_type_map: dict[str, str] | None = None,
) -> list[dict]:
    """Identify product outputs from a process node's outputs.

    A product output is:
    - isProduct=True, OR
    - flow_type is "Product flow" or "waste_flow" (looked up by flowUuid)

    Args:
        outputs: List of output ports
        flow_type_map: Optional dict mapping flow_uuid -> flow_type

    Returns list of product output ports with their flow info.
    """
    product_outputs = []
    for port in outputs:
        if not isinstance(port, dict):
            continue
        
        # Check isProduct flag
        is_product = bool(port.get("isProduct", False))
        
        # Also check flow type from flowUuid lookup
        if not is_product and flow_type_map:
            flow_uuid = port.get("flowUuid")
            if flow_uuid:
                flow_type = flow_type_map.get(flow_uuid, "")
                is_product = flow_type in ("Product flow", "waste_flow")
        
        if is_product:
            product_outputs.append(port)
    
    return product_outputs


def _select_reference_flow(
    product_outputs: list[dict],
    process_uuid: str,
    model: Model | None,
    report: ExportReport,
) -> tuple[dict | None, str | None]:
    """Select the reference (quantitative reference) flow for a process.

    Priority:
    1. User-specified reference_product (from Model)
    2. Single product output (automatic, allocation=1.0)
    3. Multi-product: choose first product output as fallback

    Returns:
        Tuple of (reference_port, reference_flow_uuid)
    """
    if not product_outputs:
        return None, None

    # Single product output - automatic reference
    if len(product_outputs) == 1:
        ref_port = product_outputs[0]
        ref_flow_uuid = ref_port.get("flowUuid")
        report.reference_flow_by_process[process_uuid] = ref_flow_uuid
        return ref_port, ref_flow_uuid

    # Multi-product process
    report.multi_product_process_count += 1

    # Try to match model's reference_product
    if model and model.reference_product:
        for port in product_outputs:
            flow_uuid = port.get("flowUuid")
            # Match by flow UUID or flow name
            if flow_uuid == model.reference_product or port.get("name") == model.reference_product:
                ref_port = port
                ref_flow_uuid = flow_uuid
                report.reference_flow_by_process[process_uuid] = ref_flow_uuid
                return ref_port, ref_flow_uuid

    # Fallback: use first product output
    ref_port = product_outputs[0]
    ref_flow_uuid = ref_port.get("flowUuid")
    report.reference_flow_by_process[process_uuid] = ref_flow_uuid
    
    # Warn that we're using fallback
    report.add_warning(
        "allocation",
        f"Multi-product process {process_uuid}: using first product output as reference (no explicit reference_product specified)",
        {"process_uuid": process_uuid, "reference_flow_uuid": ref_flow_uuid},
    )
    
    return ref_port, ref_flow_uuid


def _calculate_allocation_factors(
    product_outputs: list[dict],
    reference_port: dict | None,
    process_uuid: str,
    report: ExportReport,
    db: Session | None = None,
) -> dict[str, float]:
    """Calculate allocation factors for multi-product process outputs.

    Allocation is based on physical relationships (output quantities) when products
    share the same unit group. For different unit groups, manual allocation is required.

    Args:
        product_outputs: List of product output ports
        reference_port: Selected reference flow port
        process_uuid: Process UUID for reporting
        report: ExportReport for warnings
        db: Optional DB session for unit conversion

    Returns:
        Dict mapping port_id -> allocation_factor (0.0 to 1.0)
    """
    if not product_outputs or len(product_outputs) == 1:
        # Single product - full allocation to reference
        if product_outputs:
            return {product_outputs[0].get("id"): 1.0}
        return {}

    # Check if user already specified allocation factors
    user_allocation = {}
    for port in product_outputs:
        alloc_factor = port.get("allocationFactor")
        if alloc_factor is not None:
            user_allocation[port.get("id")] = float(alloc_factor)

    if user_allocation:
        user_sum = sum(user_allocation.values())
        if abs(user_sum - 1.0) < 0.01:
            # User-specified allocation sums to ~1.0, use it
            return user_allocation
        else:
            # User specified incomplete/invalid allocation - warn and fall through
            report.add_warning(
                "allocation",
                f"Multi-product process {process_uuid}: user-specified allocation factors sum to {user_sum:.4f} (expected 1.0), will auto-calculate",
                {
                    "process_uuid": process_uuid,
                    "user_allocation": user_allocation,
                    "sum": user_sum,
                },
            )
            report.manual_allocation_required_processes.append(process_uuid)

    # Group products by unit group and convert to reference units
    unit_groups: dict[str, list[tuple[dict, float]]] = {}  # ug -> [(port, converted_amount)]
    conversion_failed = False

    for port in product_outputs:
        ug = port.get("unitGroup", port.get("unit", "unknown"))
        unit = port.get("unit", "")
        amount = abs(port.get("amount", 0) or 0)

        # Convert to reference unit if db is available
        converted_amount = amount
        if db and ug and unit:
            try:
                # Find unit definition to get conversion factor
                unit_def = db.query(UnitDefinition).filter(
                    UnitDefinition.unit_group == ug,
                    UnitDefinition.unit_name == unit,
                ).first()

                if unit_def and unit_def.factor_to_reference:
                    # Convert to reference unit
                    converted_amount = amount * unit_def.factor_to_reference
                elif unit_def is None:
                    # Unit definition not found - cannot safely auto-allocate
                    conversion_failed = True
                # else: unit_def exists but no factor_to_reference, use original amount
            except Exception:
                # DB query failed - cannot safely auto-allocate
                conversion_failed = True
        elif ug and unit:
            # No db available but has unit info - mark for manual allocation
            conversion_failed = True

        if ug not in unit_groups:
            unit_groups[ug] = []
        unit_groups[ug].append((port, converted_amount))

    # If unit conversion failed for any product, cannot auto-allocate safely
    if conversion_failed and len(unit_groups) > 0:
        report.add_warning(
            "allocation",
            f"Multi-product process {process_uuid}: unit conversion failed for one or more products, manual allocation required",
            {
                "process_uuid": process_uuid,
                "unit_groups": list(unit_groups.keys()),
            },
        )
        report.manual_allocation_required_processes.append(process_uuid)
        return None

    # If all products share the same unit group, allocate by converted quantity
    if len(unit_groups) == 1:
        group = list(unit_groups.values())[0]
        total_amount = sum(amt for _, amt in group)
        
        if total_amount > 0:
            allocation = {}
            for port, amt in group:
                port_id = port.get("id")
                allocation[port_id] = amt / total_amount
            
            # Verify allocation sums to ~1.0
            if abs(sum(allocation.values()) - 1.0) < 0.01:
                return allocation

    # Different unit groups - cannot auto-allocate physically
    # Do NOT return fake allocation factors; let caller skip @allocatedFraction
    report.add_warning(
        "allocation",
        f"Multi-product process {process_uuid}: products have different unit groups, manual allocation required",
        {
            "process_uuid": process_uuid,
            "unit_groups": list(unit_groups.keys()),
            "product_count": len(product_outputs),
        },
    )
    report.manual_allocation_required_processes.append(process_uuid)

    # Return None to signal: no automatic allocation possible
    return None


def _build_flow_data(db: Session, flow_uuid: str, report: ExportReport) -> dict | None:
    """Build minimal flowDataSet for a single flow.

    Returns None if flow not found (will be reported as error).
    Output format: {"flowDataSet": {"flowInformation": {...}}} (TIDAS/ILCD style)
    """
    flow_record = db.get(FlowRecord, flow_uuid)

    if flow_record is None:
        report.add_error(f"Flow not found: {flow_uuid}")
        return None

    # Build name structure that _extract_ilcd_name() can read
    name_obj = {
        "baseName": [
            {"@xml:lang": "zh", "#text": flow_record.flow_name},
        ],
    }
    if flow_record.flow_name_en:
        name_obj["baseName"].append({"@xml:lang": "en", "#text": flow_record.flow_name_en})

    # Build ILCD-style flowInformation with single dataSetInformation block
    dsi = {
        "common:UUID": flow_record.flow_uuid,
        "name": name_obj,  # For _extract_ilcd_name()
        "common:generalInformation": {
            "common:referenceFunction": flow_record.flow_name,
            "common:derogation": "false",
        },
    }

    # Add optional fields to dataSetInformation
    if flow_record.source_updated_at:
        dsi["common:timeStamp"] = flow_record.source_updated_at

    # Build flowProperties for unit inference
    flow_props = []
    if flow_record.default_unit or flow_record.unit_group:
        prop = {
            "referenceToFlowPropertyDataSet": {
                "common:shortDescription": [
                    {"@xml:lang": "en", "#text": flow_record.unit_group or "Units"},
                ],
            },
        }
        flow_props.append(prop)

    flow_info = {
        "flowInformation": {
            "dataSetInformation": dsi,
            "flowType": flow_record.flow_type or "elementary_flow",
        },
    }

    # Add optional fields to flowInformation level (for importer fallback)
    if flow_record.default_unit:
        flow_info["flowInformation"]["referenceUnit"] = flow_record.default_unit
    if flow_record.unit_group:
        flow_info["flowInformation"]["unitGroup"] = flow_record.unit_group
    if flow_record.compartment:
        flow_info["flowInformation"]["compartment"] = flow_record.compartment

    # Add flowProperties for _infer_unit_defaults_from_flow_dataset()
    if flow_props:
        flow_info["flowProperties"] = {"flowProperty": flow_props}

    # Check for missing TianGong-specific fields (warnings only)
    if not flow_record.compartment:
        report.add_warning(
            "missing_tiangong_field",
            f"Flow {flow_uuid} missing compartment (category)",
            {"flow_uuid": flow_uuid},
        )

    report.flow_count += 1

    # Return in TIDAS/ILCD root format
    return {"flowDataSet": flow_info}


def _build_process_data(
    db: Session, process_uuid: str, graph_json: dict, report: ExportReport, model: Model | None = None, flow_type_map: dict[str, str] | None = None
) -> dict | None:
    """Build minimal processDataSet for a single process.

    Priority:
    1. Reuse existing ReferenceProcess JSON if available
    2. Synthesize from graph node data

    Output format: {"processDataSet": {...}} (TIDAS/ILCD style)
    """
    # First, try to find existing reference process
    ref_process = db.get(ReferenceProcess, process_uuid)

    if ref_process is not None and ref_process.process_json:
        # Reuse existing process JSON - wrap in processDataSet if not already
        existing = ref_process.process_json
        if "processDataSet" in existing:
            process_data = dict(existing)
        else:
            # Wrap simplified format in processDataSet
            process_data = {"processDataSet": dict(existing)}

        # Add ILCD structure for platform compatibility if not present
        pi = process_data["processDataSet"]
        if "processInformation" not in pi and pi.get("process_uuid"):
            dsi = {
                "common:UUID": pi["process_uuid"],
                "name": {
                    "baseName": [{"@xml:lang": "zh", "#text": pi.get("process_name", "")}],
                },
                "common:generalInformation": {
                    "common:referenceFunction": pi.get("process_name", ""),
                    "common:derogation": "false",
                },
            }
            if pi.get("location"):
                dsi["common:geography"] = pi["location"]
            pi["processInformation"] = {"dataSetInformation": dsi}

        # Add quantitative reference and allocation for platform compatibility
        # Always process if we have exchanges (need to build exchanges_ilcd with allocation)
        if pi.get("exchanges"):
            # Ensure processInformation exists
            if "processInformation" not in pi:
                dsi = {
                    "common:UUID": pi["process_uuid"],
                    "name": {
                        "baseName": [{"@xml:lang": "zh", "#text": pi.get("process_name", "")}],
                    },
                    "common:generalInformation": {
                        "common:referenceFunction": pi.get("process_name", ""),
                        "common:derogation": "false",
                    },
                }
                if pi.get("location"):
                    dsi["common:geography"] = pi["location"]
                pi["processInformation"] = {"dataSetInformation": dsi}

            # Add quantitative reference if not present
            if "quantitativeReference" not in pi["processInformation"]:
                # Identify product outputs using the same logic as synthesized processes
                exchanges = pi["exchanges"]
                # Convert exchanges to port-like format for _identify_product_outputs
                exchange_ports = []
                for exc in exchanges:
                    if exc.get("direction") == "output":
                        exchange_ports.append({
                            "id": exc.get("internal_id"),
                            "flowUuid": exc.get("flow_uuid"),
                            "unit": exc.get("unit"),
                            "unitGroup": exc.get("unit_group"),
                            "amount": exc.get("amount"),
                            "isProduct": True,  # Already filtered to outputs
                            "allocationFactor": exc.get("allocationFactor"),
                        })

                product_outputs = _identify_product_outputs(exchange_ports, flow_type_map)

                # Select reference flow
                ref_flow_uuid = pi.get("reference_flow_source_uuid")
                ref_internal_id = pi.get("reference_flow_internal_id")

                # Use _select_reference_flow logic for consistency
                if not ref_flow_uuid and product_outputs:
                    ref_port_temp, ref_flow_uuid = _select_reference_flow(product_outputs, process_uuid, model, report)
                    if ref_port_temp:
                        ref_internal_id = ref_port_temp.get("id")

                if not ref_flow_uuid and product_outputs:
                    # Fallback: use first output
                    ref_flow_uuid = product_outputs[0].get("flowUuid")
                    ref_internal_id = product_outputs[0].get("id")

                # Add quantitative reference
                if ref_flow_uuid:
                    pi["processInformation"]["quantitativeReference"] = {
                        "referenceToReferenceFlow": {
                            "@refObjectId": ref_flow_uuid,
                            "@dataSetInternalID": ref_internal_id or "0",
                        },
                    }

            # Calculate allocation factors (always recalculate for current export)
            exchanges = pi["exchanges"]
            exchange_ports = []
            for exc in exchanges:
                if exc.get("direction") == "output":
                    exchange_ports.append({
                        "id": exc.get("internal_id"),
                        "flowUuid": exc.get("flow_uuid"),
                        "unit": exc.get("unit"),
                        "unitGroup": exc.get("unit_group"),
                        "amount": exc.get("amount"),
                        "isProduct": True,
                        "allocationFactor": exc.get("allocationFactor"),
                    })

            product_outputs = _identify_product_outputs(exchange_ports, flow_type_map)
            allocation_factors: dict | None = {}
            if product_outputs:
                allocation_factors = _calculate_allocation_factors(product_outputs, None, process_uuid, report, db)

            # Add ILCD exchanges with allocation (only if auto-allocation succeeded)
            ilcd_exchanges = []
            for exc in exchanges:
                ilcd_exc = {
                    "@id": exc.get("internal_id", "0"),
                    "flow": {
                        "@refObjectId": exc.get("flow_uuid", ""),
                    },
                    "@direction": exc.get("direction", "input"),
                }
                if exc.get("amount") is not None:
                    ilcd_exc["@amount"] = exc["amount"]
                if exc.get("unit") is not None:
                    ilcd_exc["@unit"] = exc["unit"]

                # Add allocation factor for outputs (only if auto-allocation succeeded)
                if exc.get("direction") == "output" and allocation_factors is not None:
                    alloc_factor = allocation_factors.get(exc.get("internal_id"))
                    if alloc_factor is not None:
                        ilcd_exc["@allocatedFraction"] = alloc_factor
                        if alloc_factor < 1.0:
                            ilcd_exc["allocation"] = {
                                "allocationFactor": alloc_factor,
                                "isReferenceFlow": alloc_factor == max(allocation_factors.values()) if allocation_factors else False,
                            }

                ilcd_exchanges.append(ilcd_exc)
            pi["exchanges_ilcd"] = ilcd_exchanges

        report.process_count += 1
        return process_data

    # Synthesize from graph
    graph = HybridGraph.model_validate(graph_json)

    # Find node in graph - use process_uuid not node id
    node_data = None
    for node in graph.nodes:
        if node.process_uuid == process_uuid or node.id == process_uuid:
            node_data = node
            break

    if node_data is None:
        report.add_error(f"Process node not found in graph: {process_uuid}")
        return None

    # Build minimal process structure
    exchanges = []

    # Build input exchanges
    for port in node_data.inputs:
        exchange = {
            "internal_id": port.id,
            "flow_uuid": port.flowUuid,
            "direction": "input",
            "port_name": port.name,
        }
        if port.amount is not None:
            exchange["amount"] = port.amount
        if port.unit is not None:
            exchange["unit"] = port.unit
        exchanges.append(exchange)

    # Build output exchanges
    for port in node_data.outputs:
        exchange = {
            "internal_id": port.id,
            "flow_uuid": port.flowUuid,
            "direction": "output",
            "port_name": port.name,
        }
        if port.amount is not None:
            exchange["amount"] = port.amount
        if port.unit is not None:
            exchange["unit"] = port.unit
        exchanges.append(exchange)

    # Identify product outputs and calculate allocation
    product_outputs = _identify_product_outputs(
        [p.model_dump() if hasattr(p, "model_dump") else p for p in node_data.outputs],
        flow_type_map,
    )
    ref_port, ref_flow_uuid = _select_reference_flow(product_outputs, process_uuid, model, report)

    # Calculate allocation factors for product outputs
    allocation_factors = {}
    if product_outputs:
        allocation_factors = _calculate_allocation_factors(product_outputs, ref_port, process_uuid, report, db)

    # Build process info with both simplified and ILCD-style fields
    process_info = {
        "process_uuid": process_uuid,
        "process_name": node_data.name,
        "process_name_zh": node_data.name,  # Assume zh for Nebula
        "process_name_en": None,  # Not available in graph, will be warned
        "location": node_data.location or "GLO",
        "reference_flow_internal_id": ref_port.get("id") if ref_port else None,
        "reference_flow_source_uuid": ref_flow_uuid,
        "reference_flow_source_name": ref_port.get("name") if ref_port else None,
        "exchanges": exchanges,
        "process_type": "unit_process",
    }

    # Add standard ILCD/TIDAS structure for platform compatibility
    # processInformation.dataSetInformation
    dsi = {
        "common:UUID": process_uuid,
        "name": {
            "baseName": [{"@xml:lang": "zh", "#text": node_data.name}],
        },
        "common:generalInformation": {
            "common:referenceFunction": node_data.name,
            "common:derogation": "false",
        },
    }
    if node_data.location:
        dsi["common:geography"] = node_data.location

    # Build exchange array in ILCD format with allocation info
    ilcd_exchanges = []
    for exc in exchanges:
        ilcd_exc = {
            "@id": exc["internal_id"],
            "flow": {
                "@refObjectId": exc["flow_uuid"],
            },
            "@direction": exc["direction"],
        }
        if exc.get("amount") is not None:
            ilcd_exc["@amount"] = exc["amount"]
        if exc.get("unit") is not None:
            ilcd_exc["@unit"] = exc["unit"]
        
        # Add allocation factor for product outputs
        if exc["direction"] == "output":
            alloc_factor = (
                allocation_factors.get(exc["internal_id"])
                if allocation_factors is not None
                else None
            )
            if alloc_factor is not None:
                ilcd_exc["@allocatedFraction"] = alloc_factor
                # Also add Nebula extension field for allocation metadata
                if alloc_factor < 1.0:
                    ilcd_exc["allocation"] = {
                        "allocationFactor": alloc_factor,
                        "isReferenceFlow": exc["internal_id"] == (ref_port.get("id") if ref_port else None),
                    }
            elif product_outputs:
                ilcd_exc["allocation"] = {
                    "manualAllocationRequired": True,
                    "isReferenceFlow": exc["internal_id"] == (ref_port.get("id") if ref_port else None),
                }
        
        ilcd_exchanges.append(ilcd_exc)

    # Add quantitative reference for TIDAS compatibility
    # Each process must have exactly one quantitative reference
    quantitative_reference = {}
    if ref_port and ref_flow_uuid:
        quantitative_reference = {
            "referenceToReferenceFlow": {
                "@refObjectId": ref_flow_uuid,
                "@dataSetInternalID": ref_port.get("id", "0"),
            },
        }

    # Add ILCD-style structure for platform compatibility
    process_info["processInformation"] = {
        "dataSetInformation": dsi,
        "quantitativeReference": quantitative_reference,
    }
    process_info["exchanges_ilcd"] = ilcd_exchanges

    # Check for missing TianGong fields (warnings only)
    if not node_data.location or node_data.location == "GLO":
        report.add_warning(
            "missing_tiangong_field",
            f"Process {process_uuid} missing specific location",
            {"process_uuid": process_uuid},
        )

    report.process_count += 1

    # Return in TIDAS/ILCD root format
    return {"processDataSet": process_info}


def _build_model_data(
    db: Session, model: Model, graph_json: dict, report: ExportReport
) -> dict:
    """Build lifeCycleModelDataSet from Model and graph.

    Exports in TIDAS/ILCD format with Nebula extension fields:
    - lifeCycleModelDataSet: standard ILCD structure
    - json_tg.xflow: Nebula graph extension
    - xflow_nodes/xflow_edges/process_instances: for _build_tidas_graph_from_model_record
    """
    # Extract process refs from graph
    _, process_uuids = _extract_graph_data(graph_json)

    # Extract xflow nodes and edges for reconstruction
    xflow_nodes = graph_json.get("nodes", [])
    xflow_edges = graph_json.get("exchanges", [])

    # Build process instances from nodes
    process_instances = []
    for idx, node in enumerate(xflow_nodes):
        if not isinstance(node, dict):
            continue
        process_uuid = node.get("process_uuid") or node.get("id")
        if not process_uuid:
            continue
        process_instances.append({
            "internal_id": str(idx),
            "process_uuid": str(process_uuid),
        })

    # Build ILCD-style lifeCycleModelDataSet with Nebula extension
    model_info = {
        "lifeCycleModelInformation": {
            "dataSetInformation": {
                "common:UUID": model.id,
                "common:name": model.name,
                "common:name xml:lang": "zh",
                "common:generalInformation": {
                    "common:referenceFunction": model.functional_unit or model.name,
                    "common:derogation": "false",
                },
            },
        },
    }

    # Add optional fields to dataSetInformation
    if model.name:
        model_info["lifeCycleModelInformation"]["dataSetInformation"]["common:name xml:lang=en"] = model.name
    if model.description:
        model_info["lifeCycleModelInformation"]["dataSetInformation"]["common:description"] = model.description
    if model.system_boundary:
        model_info["lifeCycleModelInformation"]["dataSetInformation"]["common:systemBoundary"] = model.system_boundary
    if model.time_representativeness:
        model_info["lifeCycleModelInformation"]["dataSetInformation"]["common:timeRepresentativeness"] = model.time_representativeness
    if model.geography:
        model_info["lifeCycleModelInformation"]["dataSetInformation"]["common:geography"] = model.geography

    # Add Nebula extension with graph topology under json_tg (TIDAS extension)
    model_info["lifeCycleModelInformation"]["dataSetInformation"]["json_tg"] = {
        "xflow": graph_json,
    }

    # Build combined model data with both ILCD and Nebula fields
    model_data = {
        # Simplified fields for quick Nebula import
        "model_uuid": model.id,
        "model_name": model.name,
        "reference_product": model.reference_product,
        "functional_unit": model.functional_unit,
        "system_boundary": model.system_boundary,
        "time_representativeness": model.time_representativeness,
        "geography": model.geography,
        "description": model.description,
        "process_refs": list(process_uuids),
        # ILCD root structure (with json_tg extension inside)
        "lifeCycleModelDataSet": model_info,
        # Nebula graph extensions for _build_tidas_graph_from_model_record
        "xflow_nodes": xflow_nodes,
        "xflow_edges": xflow_edges,
        "process_instances": process_instances,
        # Legacy flat json_tg.xflow for compatibility
        "json_tg": {"xflow": graph_json},
    }

    # Check for missing TianGong fields (warnings only)
    if not model.reference_product:
        report.add_warning(
            "missing_tiangong_field",
            f"Model {model.id} missing reference_product",
            {"model_uuid": model.id},
        )
    if not model.functional_unit:
        report.add_warning(
            "missing_tiangong_field",
            f"Model {model.id} missing functional_unit",
            {"model_uuid": model.id},
        )
    if not model.geography:
        report.add_warning(
            "missing_tiangong_field",
            f"Model {model.id} missing geography",
            {"model_uuid": model.id},
        )

    report.exported_model_count += 1
    return model_data


def _build_manifest(
    model_uuid: str,
    flow_uuids: list[str],
    process_uuids: list[str],
) -> dict:
    """Build v2 manifest.json for TIDAS bundle.

    Entries point to actual files in ZIP (array format).
    """
    entries = [
        {
            "table": "lifecyclemodels",
            "file_path": f"models/{model_uuid}.json",
        },
        {
            "table": "unitprocesses",
            "file_path": "processes/processDataSet.json",
        },
        {
            "table": "flows",
            "file_path": "flows/flowDataSet.json",
        },
    ]

    return {
        "format": "tiangong-tidas-package",
        "version": "2",
        "bundle_schema_version": "tidas-lca-bundle-v1",
        "model_file": f"models/{model_uuid}.json",
        "process_dir": "processes",
        "flow_dir": "flows",
        "entries": entries,
    }


def preview_export(
    db: Session, project_id: str, version: int | None = None
) -> dict[str, Any]:
    """Preview export without generating ZIP.

    Returns:
        Dictionary with:
        - can_export: bool
        - flow_count: int
        - process_count: int
        - model_count: int
        - warnings: list[dict]
        - errors: list[str]
        - missing_flows: list[str]
        - missing_processes: list[str]
    """
    report = ExportReport()

    # Fetch model version
    model_version = _get_model_version(db, project_id, version)

    if model_version is None:
        version_info = f" version {version}" if version else ""
        report.add_error(f"No model version found for project {project_id}{version_info}")
        return {
            "can_export": False,
            "flow_count": 0,
            "process_count": 0,
            "exported_model_count": 0,
            "warnings": [],
            "errors": report.errors,
            "missing_flows": [],
            "missing_processes": [],
        }

    # Extract graph data
    graph_json = model_version.hybrid_graph_json
    if not graph_json:
        report.add_error(f"Empty graph for project {project_id}")
        return {
            "can_export": False,
            "flow_count": 0,
            "process_count": 0,
            "exported_model_count": 0,
            "warnings": [],
            "errors": report.errors,
            "missing_flows": [],
            "missing_processes": [],
        }

    pts_nodes = _find_pts_nodes(graph_json)
    if pts_nodes:
        _add_pts_export_blocker(report, pts_nodes)
        return {
            "can_export": False,
            "flow_count": 0,
            "process_count": 0,
            "exported_model_count": 0,
            "multi_product_process_count": 0,
            "allocation_warnings": [w.to_dict() for w in report.allocation_warnings],
            "manual_allocation_required_processes": report.manual_allocation_required_processes,
            "reference_flow_by_process": report.reference_flow_by_process,
            "warnings": [w.to_dict() for w in report.warnings],
            "errors": report.errors,
            "missing_flows": [],
            "missing_processes": [],
        }

    flow_uuids, process_uuids = _extract_graph_data(graph_json)

    # Source-space check: block if unsupported elementary flow sources are found
    _scan_source_space(db, flow_uuids, report)

    # Validate flows exist
    missing_flows: list[str] = []
    for flow_uuid in flow_uuids:
        flow_record = db.get(FlowRecord, flow_uuid)
        if flow_record is None:
            missing_flows.append(flow_uuid)
            report.add_error(f"Missing flow: {flow_uuid}")

    # Build flow type map for product output identification
    flow_type_map = _build_flow_type_map(db, flow_uuids)

    # Validate processes exist (either in ReferenceProcess or graph)
    missing_processes: list[str] = []
    for process_uuid in process_uuids:
        ref_process = db.get(ReferenceProcess, process_uuid)
        if ref_process is None:
            # Check if node exists in graph
            node_found = False
            for node in graph_json.get("nodes", []):
                if isinstance(node, dict) and (node.get("process_uuid") or node.get("id")) == process_uuid:
                    node_found = True
                    break
            if not node_found:
                missing_processes.append(process_uuid)
                report.add_error(f"Missing process: {process_uuid}")

    # Dry-run build to collect all structural warnings (flow/process/model fields)
    for flow_uuid in sorted(flow_uuids):
        _build_flow_data(db, flow_uuid, report)

    # Get model for reference_product lookup
    model = db.get(Model, project_id)

    for process_uuid in sorted(process_uuids):
        _build_process_data(db, process_uuid, graph_json, report, model, flow_type_map)

    # Build model to collect model warnings and count
    if model:
        _build_model_data(db, model, graph_json, report)

    can_export = not report.has_errors()

    return {
        "can_export": can_export,
        "flow_count": report.flow_count,
        "process_count": report.process_count,
        "exported_model_count": report.exported_model_count,
        "multi_product_process_count": report.multi_product_process_count,
        "allocation_warnings": [w.to_dict() for w in report.allocation_warnings],
        "manual_allocation_required_processes": report.manual_allocation_required_processes,
        "reference_flow_by_process": report.reference_flow_by_process,
        "warnings": [w.to_dict() for w in report.warnings],
        "errors": report.errors,
        "missing_flows": missing_flows,
        "missing_processes": missing_processes,
    }


def export_bundle(
    db: Session,
    project_id: str,
    version: int | None = None,
    display_lang: str = "zh",
) -> tuple[bytes, ExportReport]:
    """Export project to TIDAS bundle ZIP.

    Args:
        db: Database session
        project_id: Project/model ID
        version: Optional version number (None = latest)
        display_lang: Display language preference (zh/en)

    Returns:
        Tuple of (ZIP bytes, export report)

    Raises:
        ExportError: If validation fails
    """
    report = ExportReport()

    # Fetch model
    model = db.get(Model, project_id)
    if model is None:
        raise ExportError(f"Project not found: {project_id}")

    # Fetch model version
    model_version = _get_model_version(db, project_id, version)
    if model_version is None:
        version_info = f" version {version}" if version else ""
        raise ExportError(f"No model version found for project {project_id}{version_info}")

    # Extract graph data
    graph_json = model_version.hybrid_graph_json
    if not graph_json:
        raise ExportError(f"Empty graph for project {project_id}")

    pts_nodes = _find_pts_nodes(graph_json)
    if pts_nodes:
        _add_pts_export_blocker(report, pts_nodes)
        raise ExportError(report.errors[-1])

    flow_uuids, process_uuids = _extract_graph_data(graph_json)

    # Source-space check: same logic as preview — block if unsupported sources found
    if not _scan_source_space(db, flow_uuids, report):
        raise ExportError(report.errors[-1])

    # Validate flows
    missing_flows: list[str] = []
    for flow_uuid in flow_uuids:
        flow_record = db.get(FlowRecord, flow_uuid)
        if flow_record is None:
            missing_flows.append(flow_uuid)
            report.add_error(f"Missing flow: {flow_uuid}")

    if missing_flows:
        raise ExportError(f"Cannot export: {len(missing_flows)} flow(s) not found in database")

    # Build flow type map for product output identification
    flow_type_map = _build_flow_type_map(db, flow_uuids)

    # Build ZIP in memory
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Build and write flow data (wrapped in array as flowDataSet)
        # Always write even if empty to satisfy importer expectations
        flow_uuid_list: list[str] = []
        flow_data_array: list[dict] = []
        for flow_uuid in sorted(flow_uuids):
            flow_data = _build_flow_data(db, flow_uuid, report)
            if flow_data:
                flow_uuid_list.append(flow_uuid)
                flow_data_array.append(flow_data)
        zf.writestr(f"flows/flowDataSet.json", json.dumps(flow_data_array, ensure_ascii=False, indent=2))

        # Build and write process data (wrapped in array as processDataSet)
        # Always write even if empty to satisfy importer expectations
        process_uuid_list: list[str] = []
        process_data_array: list[dict] = []
        for process_uuid in sorted(process_uuids):
            process_data = _build_process_data(db, process_uuid, graph_json, report, model, flow_type_map)
            if process_data:
                process_uuid_list.append(process_uuid)
                process_data_array.append(process_data)
        zf.writestr(f"processes/processDataSet.json", json.dumps(process_data_array, ensure_ascii=False, indent=2))

        # Build and write model data
        model_data = _build_model_data(db, model, graph_json, report)
        zf.writestr(f"models/{project_id}.json", json.dumps(model_data, ensure_ascii=False, indent=2))

        # Build and write manifest
        manifest = _build_manifest(project_id, flow_uuid_list, process_uuid_list)
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

        # Write export report
        export_report_data = report.to_dict()
        export_report_data["project_id"] = project_id
        export_report_data["project_name"] = model.name
        export_report_data["version"] = model_version.version
        export_report_data["display_lang"] = display_lang
        zf.writestr("export_report.json", json.dumps(export_report_data, ensure_ascii=False, indent=2))

    zip_buffer.seek(0)
    return zip_buffer.getvalue(), report
