"""Source-policy and source classifier for nebula-lca.

Phase 1 of the source-compliance plan:
- Enumerated source policies and allowed LCIA scopes.
- Source classifier (classify_flow_source, classify_process_source).
- validate_project_source_policy(model, graph, db).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class SourcePolicy(str, Enum):
    OPEN_MIXED = "open_mixed"
    TIDAS_COMPLIANT = "tidas_compliant"
    ECOINVENT_STRICT = "ecoinvent_strict"
    EXPLICIT_MAPPED_MIXED = "explicit_mapped_mixed"


class AllowedLciaScope(str, Enum):
    EF31_ONLY = "ef31_only"
    ECOINVENT_RUNTIME = "ecoinvent_runtime"
    MAPPED_RUNTIME = "mapped_runtime"


# ---------------------------------------------------------------------------
# Source-space classifier (single canonical source)
# ---------------------------------------------------------------------------

# Recognised source categories.
SOURCE_SPACE_TIANGONG = "tidas_bundle"   # tiangong/TIDAS
SOURCE_SPACE_ECOSPREAD = "ecoinvent"
SOURCE_SPACE_CUSTOM = "custom"
SOURCE_SPACE_TEST = "test"
SOURCE_SPACE_UNKNOWN = "unknown"

# Keywords that indicate ecoinvent source (case-insensitive substring match).
_ECOSPREAD_KEYWORDS = ["ecoinvent", "ecospread"]
# Keywords that indicate tiangong/TIDAS source.
_TIANGONG_KEYWORDS = ["tiangong", "tidas", "tidal", "官方 ilcd", "official ilcd", "ef3", "ef 3", "ef.", "ilcd"]


def _classify_by_keyword(text: str | None) -> str:
    """Return a source-space label based on keyword matching."""
    if not text:
        return SOURCE_SPACE_UNKNOWN
    lower = text.lower()
    for kw in _ECOSPREAD_KEYWORDS:
        if kw in lower:
            return SOURCE_SPACE_ECOSPREAD
    for kw in _TIANGONG_KEYWORDS:
        if kw in lower:
            return SOURCE_SPACE_TIANGONG
    return SOURCE_SPACE_UNKNOWN


# ---------------------------------------------------------------------------
# Public classifiers
# ---------------------------------------------------------------------------


def classify_flow_source(source: str | None, is_custom: bool = False) -> str:
    """Classify a flow's source into a canonical source-space label.

    Parameters
    ----------
    source :
        ``FlowRecord.source`` value (may be ``None``).
    is_custom :
        ``FlowRecord.is_custom`` flag.

    Returns one of:
        * ``SOURCE_SPACE_TIANGONG``
        * ``SOURCE_SPACE_ECOSPREAD``
        * ``SOURCE_SPACE_CUSTOM``
        * ``SOURCE_SPACE_TEST``
        * ``SOURCE_SPACE_UNKNOWN``
    """
    if is_custom:
        return SOURCE_SPACE_CUSTOM
    return _classify_by_keyword(source)


def classify_process_source(
    source_file: str | None = None,
    import_mode: str | None = None,
) -> str:
    """Classify a reference process's source.

    Parameters
    ----------
    source_file :
        ``ReferenceProcess.source_file`` value.
    import_mode :
        ``ReferenceProcess.import_mode`` value (e.g. "locked", "user_import").

    Returns one of the same labels as ``classify_flow_source``.
    """
    if import_mode == "test":
        return SOURCE_SPACE_TEST
    return _classify_by_keyword(source_file)


# ---------------------------------------------------------------------------
# Validation result data class
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Aggregated validation result for a project graph."""
    ok: bool = True
    source_policy: str = "open_mixed"
    errors: list[dict] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)
    info: list[dict] = field(default_factory=list)

    def add_error(self, code: str, message: str, details: dict | None = None) -> None:
        entry = {"code": code, "message": message}
        if details:
            entry["details"] = details
        self.errors.append(entry)
        self.ok = len(self.errors) == 0

    def add_warning(self, code: str, message: str, details: dict | None = None) -> None:
        entry = {"code": code, "message": message}
        if details:
            entry["details"] = details
        self.warnings.append(entry)

    def add_info(self, code: str, message: str, details: dict | None = None) -> None:
        entry = {"code": code, "message": message}
        if details:
            entry["details"] = details
        self.info.append(entry)


# ---------------------------------------------------------------------------
# Allowed unit groups for TIDAS compliant projects
# ---------------------------------------------------------------------------

# The 14 unit groups officially supported by the Tiangong platform.
# If the reference seed data is available, this list should be populated
# from it at startup.  This sentinel list is the fallback.
_TIDAS_ALLOWED_UNIT_GROUPS: list[str] | None = None


def set_tidas_allowed_unit_groups(groups: list[str] | None) -> None:
    """Override the sentinel list of allowed unit groups for TIDAS projects."""
    global _TIDAS_ALLOWED_UNIT_GROUPS
    if groups is None:
        _TIDAS_ALLOWED_UNIT_GROUPS = None
    else:
        _TIDAS_ALLOWED_UNIT_GROUPS = [g.strip().lower() for g in groups]


def get_tidas_allowed_unit_groups() -> list[str]:
    """Return the current list of allowed unit groups.

    Falls back to an empty list (meaning "allow everything") if not configured.
    """
    if _TIDAS_ALLOWED_UNIT_GROUPS is not None:
        return _TIDAS_ALLOWED_UNIT_GROUPS
    return []


# ---------------------------------------------------------------------------
# Core validation function
# ---------------------------------------------------------------------------


def validate_project_source_policy(
    model_id: str,
    graph: dict,
    db: Session,
    source_policy: str | None = None,
) -> ValidationResult:
    """Validate a project graph against its source policy.

    This is the single validation entry-point called during version-save
    and optionally during /api/model/run.

    Parameters
    ----------
    model_id :
        The project / model UUID.
    graph :
        The hybrid graph dict (same shape as stored in ``model_versions.hybrid_graph_json``).
    db :
        Database session for flow catalogue lookups.
    source_policy :
        Explicit policy to check against.  If ``None``, the current row
        from the ``models`` table is read.

    Returns
    -------
    ValidationResult with errors, warnings, and info collected.
    """
    from app.models import FlowRecord, Model

    result = ValidationResult()

    # 1. Resolve policy
    if source_policy:
        policy = SourcePolicy(source_policy)
    else:
        row = db.query(Model).filter(Model.id == model_id).first()
        if row is None:
            raise HTTPException(status_code=404, detail="Project not found")
        policy = SourcePolicy(row.source_policy or "open_mixed")
        # Ensure old projects without the column have a value written
        if row.source_policy is None:
            row.source_policy = "open_mixed"
            db.commit()

    result.source_policy = policy.value

    if policy == SourcePolicy.OPEN_MIXED:
        return _validate_open_mixed(graph, db, result)
    if policy == SourcePolicy.TIDAS_COMPLIANT:
        return _validate_tidas_compliant(graph, db, result)
    if policy == SourcePolicy.ECOINVENT_STRICT:
        return _validate_ecoinvent_strict(graph, db, result)
    # explicit_mapped_mixed not yet enforced
    return result


# ---------------------------------------------------------------------------
# Policy-specific validators
# ---------------------------------------------------------------------------

def _collect_biosphere_flow_uuids(graph: dict) -> set[str]:
    """Return all biosphere/elementary flow UUIDs referenced in the graph."""
    uuids: set[str] = set()
    for node in (graph.get("nodes") or []):
        for port in (node.get("inputs") or []):
            if (port.get("type") or "") == "biosphere":
                flow_uuid = (port.get("flowUuid") or port.get("flow_uuid") or "").strip().lower()
                if flow_uuid:
                    uuids.add(flow_uuid)
        for port in (node.get("outputs") or []):
            if (port.get("type") or "") == "biosphere":
                flow_uuid = (port.get("flowUuid") or port.get("flow_uuid") or "").strip().lower()
                if flow_uuid:
                    uuids.add(flow_uuid)
    return uuids


def _collect_unit_groups(graph: dict) -> set[str]:
    """Collect all unit_group values from node ports (inputs + outputs)."""
    groups: set[str] = set()
    for node in (graph.get("nodes") or []):
        for port in (node.get("inputs") or []) + (node.get("outputs") or []):
            ug = (port.get("unitGroup") or port.get("unit_group") or "").strip()
            if ug:
                groups.add(ug.lower())
        # Also check node-level fields
        for field_key in ("unit_group", "unitGroup"):
            ug = (node.get(field_key) or "").strip()
            if ug:
                groups.add(ug.lower())
    return groups


def _batch_lookup_flow_sources(db: Session, flow_uuids: set[str]) -> dict[str, str]:
    """Batch-lookup flow source for a set of UUIDs using db.get().

    Returns a dict mapping lower-cased UUID -> source (already lowercased).
    """
    from app.models import FlowRecord
    mapping: dict[str, str] = {}
    for uuid_val in flow_uuids:
        row = db.get(FlowRecord, uuid_val)
        if row is None:
            mapping[uuid_val] = ""
        else:
            source = str(row.source if hasattr(row, "source") else None).strip().lower()
            mapping[uuid_val] = source
    return mapping


def _is_ecoinvent_source(source: str) -> bool:
    """Return True if source string indicates ecoinvent."""
    return _classify_by_keyword(source) == SOURCE_SPACE_ECOSPREAD


def _is_tidas_source(source: str) -> bool:
    """Return True if source string indicates tiangong/TIDAS."""
    return _classify_by_keyword(source) == SOURCE_SPACE_TIANGONG


def _validate_open_mixed(
    graph: dict,
    db: Session,
    result: ValidationResult,
) -> ValidationResult:
    """Open mixed mode: no hard restrictions, but record source mix summary."""
    biosphere_uuids = _collect_biosphere_flow_uuids(graph)
    unit_groups = _collect_unit_groups(graph)

    sources_by_uuid = _batch_lookup_flow_sources(db, biosphere_uuids)

    source_counts: dict[str, int] = {}
    for uuid_val, source in sources_by_uuid.items():
        space = classify_flow_source(source)
        source_counts[space] = source_counts.get(space, 0) + 1

    # Check for unknown sources
    for uuid_val, source in sources_by_uuid.items():
        space = classify_flow_source(source)
        if space == SOURCE_SPACE_UNKNOWN:
            result.add_warning(
                "unknown_flow_source",
                f"Flow {uuid_val} has unknown source (source={source!r}).",
                {"flow_uuid": uuid_val, "source": source},
            )

    result.add_info(
        "source_mix_summary",
        f"Open mixed: source distribution = {source_counts}",
        {"source_distribution": source_counts, "unit_groups": sorted(unit_groups)},
    )
    return result


def _validate_tidas_compliant(
    graph: dict,
    db: Session,
    result: ValidationResult,
) -> ValidationResult:
    """TIDAS compliant mode validation.

    Hard rules:
    - Only TIDAS allowed unit groups.
    - No ecoinvent elementary flows.
    - No ecoinvent LCI dataset nodes.
    - All flows must be from allowed catalog or TIDAS-compatible custom flows.
    """
    biosphere_uuids = _collect_biosphere_flow_uuids(graph)
    unit_groups = _collect_unit_groups(graph)
    sources_by_uuid = _batch_lookup_flow_sources(db, biosphere_uuids)
    allowed_ug = get_tidas_allowed_unit_groups()

    # --- Unit group check ---
    if allowed_ug:
        for ug in unit_groups:
            if ug not in allowed_ug:
                result.add_error(
                    "unsupported_unit_group",
                    f"Unit group {ug!r} is not allowed in TIDAS compliant mode.",
                    {"unit_group": ug},
                )

    # --- Elementary flow source check ---
    for uuid_val, source in sources_by_uuid.items():
        space = classify_flow_source(source)

        # ecoinvent elementary flow → block
        if space == SOURCE_SPACE_ECOSPREAD:
            result.add_error(
                "ecoinvent_elementary_flow",
                f"Ecoinvent elementary flow {uuid_val} is not allowed in TIDAS compliant mode.",
                {"flow_uuid": uuid_val, "source": source},
            )
        elif space == SOURCE_SPACE_UNKNOWN:
            result.add_error(
                "unknown_flow_source",
                f"Elementary flow {uuid_val} has unknown source {source!r}.",
                {"flow_uuid": uuid_val, "source": source},
            )

    # --- Check for ecoinvent LCI dataset nodes ---
    for node in (graph.get("nodes") or []):
        nk = (node.get("node_kind") or "").lower().strip()
        if nk == "lci_dataset":
            result.add_error(
                "ecoinvent_lci_dataset_node",
                "LCI dataset nodes (ecoinvent LCI datasets) are not allowed in TIDAS compliant mode.",
                {"node_id": node.get("id"), "node_kind": nk},
            )

    return result


def _validate_ecoinvent_strict(
    graph: dict,
    db: Session,
    result: ValidationResult,
) -> ValidationResult:
    """Ecoinvent strict mode validation.

    Hard rules:
    - Elementary flows must come from ecoinvent.
    - LCI dataset nodes are allowed.
    - Tiangong/TIDAS elementary flows are NOT allowed.
    """
    biosphere_uuids = _collect_biosphere_flow_uuids(graph)
    sources_by_uuid = _batch_lookup_flow_sources(db, biosphere_uuids)

    for uuid_val, source in sources_by_uuid.items():
        space = classify_flow_source(source)

        if space == SOURCE_SPACE_TIANGONG:
            result.add_error(
                "tiangong_elementary_flow",
                f"Tiangong/TIDAS elementary flow {uuid_val} is not allowed in ecoinvent strict mode.",
                {"flow_uuid": uuid_val, "source": source},
            )
        elif space == SOURCE_SPACE_UNKNOWN:
            result.add_warning(
                "unknown_elementary_flow_source",
                f"Elementary flow {uuid_val} has unknown source {source!r} in ecoinvent strict mode.",
                {"flow_uuid": uuid_val, "source": source},
            )
        elif space != SOURCE_SPACE_ECOSPREAD:
            # Custom or test source elementary flow in eco-strict
            result.add_warning(
                "non_eco_elementary_flow",
                f"Elementary flow {uuid_val} has non-ecoinvent source {source!r} in ecoinvent strict mode.",
                {"flow_uuid": uuid_val, "source": source},
            )

    return result


# ---------------------------------------------------------------------------
# LCIA scope validator (used during run_model pre-check)
# ---------------------------------------------------------------------------

def validate_lcia_scope_compatibility(
    model_id: str,
    graph: dict,
    lcia_methods: list[str],
    db: Session,
) -> ValidationResult:
    """Validate that requested LCIA methods are compatible with the project's LCIA scope.

    Parameters
    ----------
    model_id :
        Project UUID.
    graph :
        Hybrid graph dict.
    lcia_methods :
        List of method names being requested (e.g. ["EF v3.1"]).
    db :
        Database session.

    Raises HTTPException (400) if methods are incompatible.
    """
    from app.models import Model

    row = db.query(Model).filter(Model.id == model_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")

    policy = SourcePolicy(row.source_policy or "open_mixed")
    scope = AllowedLciaScope(row.allowed_lcia_scope or "ef31_only")

    result = ValidationResult()
    result.source_policy = policy.value
    result.add_info("lcia_scope", f"Policy={policy.value}, Scope={scope.value}")

    # --- Policy-specific LCIA checks ---

    if policy == SourcePolicy.TIDAS_COMPLIANT:
        # TIDAS compliant: ONLY EF v3.1 allowed
        for method in lcia_methods:
            if method != "EF v3.1":
                result.add_error(
                    "lcia_method_not_allowed",
                    f"LCIA method {method!r} is not allowed in TIDAS compliant mode (only EF v3.1).",
                    {"method": method, "allowed": ["EF v3.1"]},
                )

    elif policy == SourcePolicy.ECOINVENT_STRICT:
        # ecoinvent strict: all methods in the runtime are allowed
        pass

    # --- Cross-source + non-EF31 check (applies to all policies) ---
    if lcia_methods and lcia_methods != ["EF v3.1"]:
        biosphere_uuids = _collect_biosphere_flow_uuids(graph)
        if biosphere_uuids:
            sources_by_uuid = _batch_lookup_flow_sources(db, biosphere_uuids)
            has_non_eco = any(
                not _is_ecoinvent_source(source)
                for source in sources_by_uuid.values()
            )
            if has_non_eco:
                # Count non-ecoinvent flows
                non_eco_uuids = [
                    u for u, s in sources_by_uuid.items()
                    if not _is_ecoinvent_source(s)
                ]
                result.add_error(
                    "lcia_method_incompatible_with_elementary_sources",
                    f"LCIA method {lcia_methods!r} requires all elementary flows to be ecoinvent, "
                    f"but {len(non_eco_uuids)} non-ecoinvent elementary flow(s) exist in the graph.",
                    {"requested_methods": lcia_methods, "non_eco_flow_count": len(non_eco_uuids)},
                )

    if not result.ok:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "SOURCE_POLICY_VIOLATION",
                "message": "Source policy validation failed",
                "evidence": {
                    "errors": result.errors,
                    "warnings": result.warnings,
                    "info": result.info,
                },
            },
        )

    return result
