"""Intermediate-flow linking APIs for Tiangong foreground inputs."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..database import get_db
from ..flow_unit_semantics import _unit_group_key
from ..models import FlowRecord, IntermediateFlowLinkRule, IntermediateFlowLinkRuleVersion
from ..schemas import FlowPort, IntermediateFlowLink
from ..services.intermediate_flow_linking_service import (
    get_intermediate_flow_link_registry,
    deterministic_default_unit_factor,
    list_l2_candidates,
    list_provider_candidates,
    resolve_ecoinvent_target_flow,
    resolve_intermediate_flow,
    resolve_source_flow_context,
    validate_intermediate_flow_link,
)
api_router = APIRouter(prefix="/api/intermediate-flow-links", tags=["intermediate-flow-links"])


class ResolvePortRequest(BaseModel):
    node_id: str | None = None
    port_id: str | None = None
    flow_uuid: str
    direction: Literal["input", "output"] = "input"
    exchange_type: Literal["technosphere", "biosphere"] = "technosphere"
    unit: str | None = None
    unit_group: str | None = None
    flow_source_namespace: str | None = None
    flow_version: str | None = None
    intermediate_flow_link: dict[str, Any] | None = None


class ResolveBatchRequest(BaseModel):
    items: list[ResolvePortRequest] = Field(default_factory=list, max_length=1000)
    l2_limit: int = Field(default=5, ge=1, le=20)
    include_unreviewed_candidates: bool = False


class UserRuleCreateRequest(BaseModel):
    source_flow_uuid: str
    target_flow_uuid: str
    amount_factor: float | None = Field(default=None, gt=0)
    mapping_reason: str = Field(default="", max_length=1024)
    source_unit: str | None = None
    source_unit_group: str | None = None
    source_flow_namespace: str | None = None
    source_flow_version: str | None = None


class ConfirmL2Request(BaseModel):
    source_flow_uuid: str
    rule_id: str
    source_unit: str | None = None
    source_unit_group: str | None = None
    source_flow_namespace: str | None = None
    source_flow_version: str | None = None


def _flow_or_404(db: Session, flow_uuid: str) -> Any:
    row = resolve_ecoinvent_target_flow(db, str(flow_uuid or "").strip())
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "FLOW_NOT_FOUND", "message": f"Flow not found: {flow_uuid}"},
        )
    return row


def _rule_payload(db: Session, row: IntermediateFlowLinkRule | IntermediateFlowLinkRuleVersion) -> dict[str, Any]:
    source = db.get(FlowRecord, row.source_flow_uuid)
    target = resolve_ecoinvent_target_flow(db, row.target_flow_uuid, source=source)
    versioned = isinstance(row, IntermediateFlowLinkRuleVersion)
    return {
        "id": row.id,
        "source_flow_uuid": row.source_flow_uuid,
        "target_flow_uuid": row.target_flow_uuid,
        "amount_factor": row.amount_factor,
        "source_unit": row.source_unit,
        "target_unit": row.target_unit,
        "source_unit_group": row.source_unit_group if versioned else (source.unit_group if source is not None else None),
        "target_unit_group": row.target_unit_group if versioned else (target.unit_group if target is not None else None),
        "source_flow_namespace": row.source_namespace if versioned else None,
        "source_flow_version": row.source_version if versioned else None,
        "mapping_level": row.mapping_level,
        "mapping_reason": row.mapping_reason,
        "rule_origin": row.rule_origin,
        "status": row.status,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _resolution_payload(db: Session, resolution: Any) -> dict[str, Any]:
    payload = resolution.to_dict()
    target = resolve_ecoinvent_target_flow(
        db,
        resolution.target_flow_uuid,
        allow_mapped_catalog=resolution.rule_origin == "builtin",
    )
    payload["target_flow_name"] = target.flow_name if target is not None else ""
    payload["target_flow_name_en"] = target.flow_name_en if target is not None else ""
    return payload


@api_router.post("/resolve-batch")
def resolve_batch(payload: ResolveBatchRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        registry = get_intermediate_flow_link_registry()
    except (OSError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": "INTERMEDIATE_FLOW_PACKAGE_UNAVAILABLE", "message": str(exc)},
        ) from exc
    results: list[dict[str, Any]] = []
    counts = {"explicit": 0, "L1": 0, "L2": 0, "L3": 0, "unmatched": 0, "blocked": 0}
    for item in payload.items:
        base = {"node_id": item.node_id, "port_id": item.port_id, "source_flow_uuid": item.flow_uuid}
        if item.direction != "input" or item.exchange_type != "technosphere":
            results.append({**base, "status": "skipped", "reason": "only technosphere inputs are linkable"})
            continue
        explicit = item.intermediate_flow_link if isinstance(item.intermediate_flow_link, dict) else None
        if explicit and explicit.get("status") in {"auto", "user_confirmed"}:
            try:
                parsed_link = IntermediateFlowLink.model_validate(explicit)
                request_port = FlowPort(
                    id=item.port_id or "resolve-port",
                    flowUuid=item.flow_uuid,
                    flowSourceNamespace=item.flow_source_namespace,
                    flowVersion=item.flow_version,
                    name=item.flow_uuid,
                    unit=item.unit or parsed_link.source_unit,
                    unitGroup=item.unit_group,
                    amount=0,
                    type=item.exchange_type,
                    direction=item.direction,
                )
                issue = validate_intermediate_flow_link(db, item.flow_uuid, parsed_link, port=request_port)
            except (TypeError, ValueError) as exc:
                issue = f"INVALID_EXPLICIT_LINK: {exc}"
            if issue == "PACKAGE_HASH_DRIFT":
                results.append({**base, "status": "explicit", "resolution": explicit, "l2_candidates": []})
                counts["explicit"] += 1
            elif issue:
                results.append({**base, "status": "blocked", "reason": issue})
                counts["blocked"] += 1
            else:
                results.append({**base, "status": "explicit", "resolution": explicit, "l2_candidates": []})
                counts["explicit"] += 1
            continue
        source = db.get(FlowRecord, item.flow_uuid)
        if source is None:
            results.append({**base, "status": "blocked", "reason": "SOURCE_FLOW_NOT_FOUND"})
            counts["blocked"] += 1
            continue
        resolution, issue = resolve_intermediate_flow(
            db,
            item.flow_uuid,
            source_namespace=item.flow_source_namespace,
            source_version=item.flow_version,
            source_unit=item.unit,
            source_unit_group=item.unit_group,
        )
        if issue == "UNIT_GROUP_MISMATCH" and resolution is not None and resolution.rule_origin == "builtin":
            candidate = _resolution_payload(db, resolution)
            candidate["requires_manual_factor"] = True
            candidate["warnings"] = [*candidate.get("warnings", []), "CROSS_GROUP_FACTOR_REQUIRED"]
            results.append({
                **base,
                "status": "L2",
                "resolution": candidate,
                "l2_candidates": [],
            })
            counts["L2"] += 1
            continue
        if issue:
            results.append({**base, "status": "blocked", "reason": issue})
            counts["blocked"] += 1
            continue
        if resolution is not None:
            results.append({
                **base,
                "status": resolution.mapping_level,
                "resolution": _resolution_payload(db, resolution),
                "l2_candidates": [],
            })
            counts[resolution.mapping_level] += 1
            continue
        candidates = (
            list_l2_candidates(db, source, payload.l2_limit)
            if payload.include_unreviewed_candidates
            else []
        )
        status = "L2" if candidates else "unmatched"
        results.append({**base, "status": status, "resolution": None, "l2_candidates": candidates})
        counts[status] += 1
    return {
        "package_id": registry.package_id,
        "package_version": registry.package_version,
        "package_hash": registry.package_hash,
        "link_direction": "tiangong_to_ecoinvent",
        "counts": counts,
        "items": results,
    }


@api_router.post("/confirm-l2")
def confirm_l2(payload: ConfirmL2Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    resolution, issue = resolve_intermediate_flow(
        db,
        payload.source_flow_uuid,
        source_namespace=payload.source_flow_namespace,
        source_version=payload.source_flow_version,
        source_unit=payload.source_unit,
        source_unit_group=payload.source_unit_group,
    )
    if issue:
        raise HTTPException(status_code=422, detail={"code": issue})
    if resolution is None or resolution.mapping_level != "L2" or resolution.rule_origin != "builtin":
        raise HTTPException(status_code=422, detail={"code": "L2_RULE_NOT_FOUND"})
    if resolution.rule_id != payload.rule_id:
        raise HTTPException(status_code=422, detail={"code": "L2_EVIDENCE_MISMATCH"})
    return {**_resolution_payload(db, resolution), "status": "user_confirmed"}


@api_router.get("/providers")
def get_providers(
    target_flow_uuid: str = Query(min_length=1),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    target = resolve_ecoinvent_target_flow(db, target_flow_uuid)
    if target is None:
        return {
            "target_flow_uuid": target_flow_uuid,
            "target_flow_name": "",
            "target_unit": "",
            "providers": [],
            "total": 0,
            "auto_selected": False,
        }
    providers = list_provider_candidates(db, target.flow_uuid)
    return {
        "target_flow_uuid": target.flow_uuid,
        "target_flow_name": target.flow_name,
        "target_unit": target.default_unit,
        "providers": providers,
        "total": len(providers),
        "auto_selected": False,
    }


@api_router.get("/user-rules")
def list_user_rules(
    include_inactive: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    query = db.query(IntermediateFlowLinkRule)
    if not include_inactive:
        query = query.filter(IntermediateFlowLinkRule.status == "active")
    rows = query.order_by(IntermediateFlowLinkRule.updated_at.desc()).all()
    versioned_query = db.query(IntermediateFlowLinkRuleVersion)
    if not include_inactive:
        versioned_query = versioned_query.filter(IntermediateFlowLinkRuleVersion.status == "active")
    rows.extend(versioned_query.order_by(IntermediateFlowLinkRuleVersion.updated_at.desc()).all())
    rows.sort(key=lambda item: item.updated_at, reverse=True)
    return {"items": [_rule_payload(db, row) for row in rows], "total": len(rows)}


@api_router.post("/user-rules", status_code=201)
def create_user_rule(payload: UserRuleCreateRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    context, context_issue = resolve_source_flow_context(
        db,
        flow_uuid=payload.source_flow_uuid,
        source_namespace=payload.source_flow_namespace,
        source_version=payload.source_flow_version,
        source_unit=payload.source_unit,
        source_unit_group=payload.source_unit_group,
    )
    if context_issue or context is None:
        raise HTTPException(status_code=422, detail={"code": context_issue or "SOURCE_FLOW_VERSION_NOT_FOUND"})
    source = context.record
    target = resolve_ecoinvent_target_flow(db, payload.target_flow_uuid, source=source)
    if target is None:
        raise HTTPException(status_code=404, detail={"code": "TARGET_FLOW_NOT_FOUND"})
    source_type = "waste" if "waste" in source.flow_type.casefold() else "product"
    target_type = "waste" if "waste" in target.flow_type.casefold() else "product"
    if source_type != target_type:
        raise HTTPException(status_code=422, detail={"code": "FLOW_TYPE_MISMATCH"})
    same_unit_group = _unit_group_key(source.unit_group) == _unit_group_key(target.unit_group)
    if same_unit_group:
        amount_factor = deterministic_default_unit_factor(db, source, target)
        if amount_factor is None:
            raise HTTPException(status_code=422, detail={"code": "UNIT_CONVERSION_NOT_DETERMINISTIC"})
        if payload.amount_factor is not None and abs(payload.amount_factor - amount_factor) > 1e-12:
            raise HTTPException(status_code=422, detail={"code": "UNIT_FACTOR_MISMATCH", "expected": amount_factor})
    else:
        if payload.amount_factor is None:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "CROSS_GROUP_FACTOR_REQUIRED",
                    "message": "A positive conversion factor is required when source and target unit groups differ.",
                },
            )
        amount_factor = payload.amount_factor
    existing = (
        db.query(IntermediateFlowLinkRuleVersion)
        .filter(
            IntermediateFlowLinkRuleVersion.source_namespace == context.namespace,
            IntermediateFlowLinkRuleVersion.source_flow_uuid == payload.source_flow_uuid,
            IntermediateFlowLinkRuleVersion.source_version == context.version,
            IntermediateFlowLinkRuleVersion.target_flow_uuid == target.flow_uuid,
        )
        .first()
    )
    if existing is None:
        existing = IntermediateFlowLinkRuleVersion(
            id=str(uuid.uuid4()),
            source_flow_uuid=payload.source_flow_uuid,
            source_namespace=context.namespace,
            source_version=context.version,
            target_flow_uuid=target.flow_uuid,
            source_unit=source.default_unit,
            source_unit_group=str(source.unit_group or ""),
            target_unit=target.default_unit,
            target_unit_group=str(target.unit_group or ""),
        )
        db.add(existing)
    existing.amount_factor = amount_factor
    existing.mapping_level = "L3"
    existing.mapping_reason = payload.mapping_reason.strip()
    existing.rule_origin = "user"
    existing.status = "active"
    existing.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(existing)
    result = _rule_payload(db, existing)
    result["status"] = "user_confirmed"
    return result


@api_router.delete("/user-rules/{rule_id}")
def deactivate_user_rule(rule_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    row = db.get(IntermediateFlowLinkRuleVersion, rule_id) or db.get(IntermediateFlowLinkRule, rule_id)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "RULE_NOT_FOUND"})
    row.status = "inactive"
    row.updated_at = datetime.utcnow()
    db.commit()
    return {"id": row.id, "status": row.status}
