"""External LCA data platform account and sync APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..database import get_db
from ..lci_vector_codec import pack_lci_vector
from ..models import (
    DataPlatformAccount,
    DataPlatformAccountSession,
    DataPlatformRemoteCache,
    DataPlatformSyncJob,
    ExternalDataSyncRecord,
    FlowRecord,
    LciBiosphereFlowKey,
    LciProcessVector,
    Model,
    ReferenceProcess,
    UnitDefinition,
    UnitGroup,
)
from ..schemas import (
    DataPlatformAccountCreateRequest,
    DataPlatformAccountOut,
    DataPlatformAccountUpdateRequest,
    DataPlatformConnectionTestResponse,
    DataPlatformSearchResponse,
    DataPlatformSyncFlowRequest,
    DataPlatformSyncFlowResponse,
    DataPlatformSyncModelRequest,
    DataPlatformSyncModelResponse,
    DataPlatformSyncProcessRequest,
    DataPlatformSyncProcessResponse,
    HybridGraph,
    RemoteFlowItem,
    RemoteModelItem,
    RemoteProcessItem,
)
from ..services.catalog_cache import invalidate_management_caches
from ..services.data_platform_connectors import (
    ConnectorError,
    PlatformAccountContext,
    RemoteFlowDTO,
    RemoteModelDTO,
    RemoteProcessDTO,
    RemoteUnitGroupDTO,
    connector_for_account,
    decrypt_credential,
    encrypt_credential,
)
from ..services.project_versions import _create_project_version_from_graph_json


api_router = APIRouter(prefix="/api/data-platforms", tags=["data-platforms"])


def _safe_str(value: object) -> str:
    return str(value or "").strip()


def _account_or_404(db: Session, account_id: str) -> DataPlatformAccount:
    row = db.get(DataPlatformAccount, account_id)
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "DATA_PLATFORM_ACCOUNT_NOT_FOUND", "message": f"Data platform account not found: {account_id}"})
    return row


def _account_context(row: DataPlatformAccount, db: Session | None = None) -> PlatformAccountContext:
    session_row = None
    if db is not None:
        session_row = (
            db.query(DataPlatformAccountSession)
            .filter(DataPlatformAccountSession.account_id == row.id)
            .first()
        )

    def save_session(ciphertext: str, expires_at: datetime | None) -> None:
        if db is None:
            return
        existing = (
            db.query(DataPlatformAccountSession)
            .filter(DataPlatformAccountSession.account_id == row.id)
            .first()
        )
        if existing is None:
            db.add(DataPlatformAccountSession(account_id=row.id, session_ciphertext=ciphertext, expires_at=expires_at))
        else:
            existing.session_ciphertext = ciphertext
            existing.expires_at = expires_at
            existing.updated_at = datetime.utcnow()

    return PlatformAccountContext(
        account_id=row.id,
        platform=row.platform,
        alias=row.alias,
        base_url=row.base_url,
        auth_type=row.auth_type,
        credential=decrypt_credential(row.credential_ciphertext),
        metadata=row.metadata_json if isinstance(row.metadata_json, dict) else {},
        session_ciphertext=session_row.session_ciphertext if session_row is not None else None,
        session_expires_at=session_row.expires_at if session_row is not None else None,
        save_session=save_session,
    )


def _account_out(row: DataPlatformAccount) -> DataPlatformAccountOut:
    return DataPlatformAccountOut(
        id=row.id,
        platform=row.platform,
        alias=row.alias,
        base_url=row.base_url,
        auth_type=row.auth_type,
        status=row.status,
        has_credential=bool(row.credential_ciphertext),
        last_validated_at=row.last_validated_at,
        last_validation_status=row.last_validation_status,
        last_validation_message=row.last_validation_message,
        metadata=row.metadata_json if isinstance(row.metadata_json, dict) else {},
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _remote_flow_item(item: RemoteFlowDTO) -> RemoteFlowItem:
    return RemoteFlowItem(
        remote_id=item.remote_id,
        flow_uuid=item.flow_uuid,
        flow_name=item.flow_name,
        flow_name_en=item.flow_name_en,
        flow_type=item.flow_type,
        default_unit=item.default_unit,
        unit_group=item.unit_group,
        source=item.source,
        remote_version=item.remote_version,
        metadata=item.metadata,
    )


def _remote_process_item(item: RemoteProcessDTO) -> RemoteProcessItem:
    return RemoteProcessItem(
        remote_id=item.remote_id,
        process_uuid=item.process_uuid,
        process_name=item.process_name,
        process_type=item.process_type,
        reference_flow_uuid=item.reference_flow_uuid,
        source=item.source,
        remote_version=item.remote_version,
        metadata=item.metadata,
    )


def _remote_model_item(item: RemoteModelDTO) -> RemoteModelItem:
    return RemoteModelItem(
        remote_id=item.remote_id,
        model_uuid=item.model_uuid,
        model_name=item.model_name,
        source=item.source,
        remote_version=item.remote_version,
        metadata=item.metadata,
    )


def _write_cache(db: Session, *, account: DataPlatformAccount, remote_kind: str, query_key: str, remote_id: str, payload: dict[str, Any]) -> None:
    existing = (
        db.query(DataPlatformRemoteCache)
        .filter(
            DataPlatformRemoteCache.account_id == account.id,
            DataPlatformRemoteCache.remote_kind == remote_kind,
            DataPlatformRemoteCache.remote_id == remote_id,
        )
        .first()
    )
    if existing is None:
        db.add(
            DataPlatformRemoteCache(
                account_id=account.id,
                platform=account.platform,
                remote_kind=remote_kind,
                remote_id=remote_id,
                query_key=query_key,
                payload_json=payload,
            )
        )
    else:
        existing.query_key = query_key
        existing.payload_json = payload
        existing.cached_at = datetime.utcnow()


def _upsert_sync_record(
    db: Session,
    *,
    account: DataPlatformAccount,
    local_kind: str,
    local_uuid: str,
    remote_id: str,
    remote_version: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing = (
        db.query(ExternalDataSyncRecord)
        .filter(
            ExternalDataSyncRecord.account_id == account.id,
            ExternalDataSyncRecord.local_kind == local_kind,
            ExternalDataSyncRecord.local_uuid == local_uuid,
        )
        .first()
    )
    if existing is None:
        existing = ExternalDataSyncRecord(
            account_id=account.id,
            platform=account.platform,
            local_kind=local_kind,
            local_uuid=local_uuid,
            remote_id=remote_id,
            remote_version=remote_version,
            metadata_json=metadata or {},
        )
        db.add(existing)
    else:
        existing.remote_id = remote_id
        existing.remote_version = remote_version
        existing.metadata_json = metadata or {}
        existing.synced_at = datetime.utcnow()
    return {
        "local_kind": local_kind,
        "local_uuid": local_uuid,
        "remote_id": remote_id,
        "remote_version": remote_version,
    }


def _upsert_flow(db: Session, *, account: DataPlatformAccount, item: RemoteFlowDTO, warnings: list[str]) -> FlowRecord:
    if not item.flow_uuid:
        raise ConnectorError("Remote flow is missing flow_uuid")
    if not item.unit_group or not item.default_unit:
        warnings.append(f"flow {item.flow_uuid} is missing unit_group/default_unit")
    row = db.get(FlowRecord, item.flow_uuid)
    if row is None:
        row = FlowRecord(
            flow_uuid=item.flow_uuid,
            flow_name=item.flow_name or item.flow_uuid,
            flow_name_en=item.flow_name_en,
            flow_type=item.flow_type or "Product flow",
            default_unit=item.default_unit or "",
            unit_group=item.unit_group or "",
            source=item.source or account.platform,
            source_updated_at=datetime.utcnow().isoformat(),
            tidas_unit_group=item.unit_group or None,
            tidas_flow_property_uuid=_safe_str(item.metadata.get("flow_property_id")) if isinstance(item.metadata, dict) else None,
            tidas_reference_source=account.platform,
        )
        db.add(row)
    else:
        row.flow_name = item.flow_name or row.flow_name
        row.flow_name_en = item.flow_name_en or row.flow_name_en
        row.flow_type = item.flow_type or row.flow_type
        row.default_unit = item.default_unit or row.default_unit
        row.unit_group = item.unit_group or row.unit_group
        row.source = item.source or account.platform
        row.source_updated_at = datetime.utcnow().isoformat()
        row.tidas_unit_group = item.unit_group or row.tidas_unit_group
        if isinstance(item.metadata, dict) and _safe_str(item.metadata.get("flow_property_id")):
            row.tidas_flow_property_uuid = _safe_str(item.metadata.get("flow_property_id"))
        row.tidas_reference_source = account.platform
    return row


def _upsert_unit_group(db: Session, *, account: DataPlatformAccount, item: RemoteUnitGroupDTO) -> list[dict[str, Any]]:
    synced: list[dict[str, Any]] = []
    row = db.get(UnitGroup, item.name)
    if row is None:
        row = UnitGroup(
            name=item.name,
            reference_unit=item.reference_unit,
            source_uuid=item.source_uuid,
            source_version=item.source_version,
            source_package_version=item.source_package_version or account.platform,
        )
        db.add(row)
    else:
        row.reference_unit = item.reference_unit or row.reference_unit
        row.source_uuid = item.source_uuid or row.source_uuid
        row.source_version = item.source_version or row.source_version
        row.source_package_version = item.source_package_version or row.source_package_version or account.platform
    synced.append({"local_kind": "unit_group", "local_uuid": item.name, "remote_id": item.source_uuid, "remote_version": item.source_version})
    for definition in item.definitions:
        existing = (
            db.query(UnitDefinition)
            .filter(UnitDefinition.unit_group == item.name, UnitDefinition.unit_name == definition.unit_name)
            .first()
        )
        if existing is None:
            db.add(
                UnitDefinition(
                    unit_group=item.name,
                    unit_name=definition.unit_name,
                    factor_to_reference=definition.factor_to_reference,
                    is_reference=definition.is_reference,
                )
            )
        else:
            existing.factor_to_reference = definition.factor_to_reference
            existing.is_reference = definition.is_reference
        synced.append({"local_kind": "unit_definition", "local_uuid": f"{item.name}:{definition.unit_name}", "remote_id": item.source_uuid, "remote_version": item.source_version})
    return synced


def _upsert_flow_dependencies(db: Session, *, account: DataPlatformAccount, connector: Any, flow: RemoteFlowDTO, warnings: list[str]) -> list[dict[str, Any]]:
    synced: list[dict[str, Any]] = []
    for unit_group in connector.get_flow_dependency_unit_groups(flow):
        if not unit_group.name:
            warnings.append(f"flow {flow.flow_uuid} dependency unit group is missing name")
            continue
        synced.extend(_upsert_unit_group(db, account=account, item=unit_group))
        synced.append(
            _upsert_sync_record(
                db,
                account=account,
                local_kind="unit_group",
                local_uuid=unit_group.name,
                remote_id=unit_group.source_uuid or unit_group.name,
                remote_version=unit_group.source_version,
                metadata={"source_package_version": unit_group.source_package_version},
            )
        )
    return synced


def _upsert_lci_vector(db: Session, *, account: DataPlatformAccount, process_uuid: str, vector_payload: dict[str, Any], warnings: list[str]) -> bool:
    items = vector_payload.get("items") if isinstance(vector_payload.get("items"), list) else []
    if not items:
        return False
    flow_key_ids: list[int] = []
    amounts: list[float] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        flow_uuid = _safe_str(raw.get("flow_uuid"))
        direction = "input" if _safe_str(raw.get("direction")).lower() == "input" else "output"
        unit = _safe_str(raw.get("unit")) or "kg"
        if not flow_uuid:
            warnings.append("skipped vector item without flow_uuid")
            continue
        existing = (
            db.query(LciBiosphereFlowKey)
            .filter(
                LciBiosphereFlowKey.flow_uuid == flow_uuid,
                LciBiosphereFlowKey.compartment == (_safe_str(raw.get("compartment")) or None),
                LciBiosphereFlowKey.subcompartment == (_safe_str(raw.get("subcompartment")) or None),
                LciBiosphereFlowKey.direction == direction,
                LciBiosphereFlowKey.canonical_unit == unit,
            )
            .first()
        )
        if existing is None:
            existing = LciBiosphereFlowKey(
                flow_uuid=flow_uuid,
                compartment=_safe_str(raw.get("compartment")) or None,
                subcompartment=_safe_str(raw.get("subcompartment")) or None,
                direction=direction,
                canonical_unit=unit,
                source=account.platform,
            )
            db.add(existing)
            db.flush()
        flow_key_ids.append(int(existing.flow_key_id))
        amounts.append(float(raw.get("amount") or 0.0))
    if not flow_key_ids:
        return False
    pairs = sorted(zip(flow_key_ids, amounts), key=lambda pair: pair[0])
    packed = pack_lci_vector([pair[0] for pair in pairs], [pair[1] for pair in pairs])
    row = db.get(LciProcessVector, process_uuid)
    if row is None:
        row = LciProcessVector(
            process_uuid=process_uuid,
            nnz=packed.nnz,
            flow_key_ids_blob=packed.flow_key_ids_blob,
            amounts_blob=packed.amounts_blob,
            index_dtype=packed.index_dtype,
            amount_dtype=packed.amount_dtype,
            compression=packed.compression,
            checksum=packed.checksum,
            source=account.platform,
            source_package_version=_safe_str(vector_payload.get("remote_version")) or None,
        )
        db.add(row)
    else:
        row.nnz = packed.nnz
        row.axis_id = None
        row.flow_key_ids_blob = packed.flow_key_ids_blob
        row.amounts_blob = packed.amounts_blob
        row.index_dtype = packed.index_dtype
        row.amount_dtype = packed.amount_dtype
        row.compression = packed.compression
        row.canonicalized = True
        row.checksum = packed.checksum
        row.source = account.platform
        row.source_package_version = _safe_str(vector_payload.get("remote_version")) or row.source_package_version
    return True


def _extract_hybrid_graph_from_remote_model(payload: dict[str, Any]) -> dict[str, Any]:
    candidates: list[Any] = [
        payload,
        payload.get("graph"),
        payload.get("hybrid_graph"),
    ]
    for key in ("json_tg", "json"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidates.extend([nested, nested.get("graph"), nested.get("hybrid_graph")])
    for candidate in candidates:
        if isinstance(candidate, dict) and {"functionalUnit", "nodes", "exchanges"}.issubset(candidate.keys()):
            graph = dict(candidate)
            graph.setdefault("metadata", {})
            return graph
    raise ConnectorError("Remote model payload does not contain a Nebula-compatible HybridGraph at json_tg/json graph fields.")


def _graph_flow_uuids(graph_json: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    for node in graph_json.get("nodes") if isinstance(graph_json.get("nodes"), list) else []:
        if not isinstance(node, dict):
            continue
        for bucket in ("inputs", "outputs", "emissions"):
            for port in node.get(bucket) if isinstance(node.get(bucket), list) else []:
                if isinstance(port, dict):
                    flow_uuid = _safe_str(port.get("flowUuid") or port.get("flow_uuid"))
                    if flow_uuid:
                        seen.add(flow_uuid)
    for exchange in graph_json.get("exchanges") if isinstance(graph_json.get("exchanges"), list) else []:
        if isinstance(exchange, dict):
            flow_uuid = _safe_str(exchange.get("flowUuid") or exchange.get("flow_uuid"))
            if flow_uuid:
                seen.add(flow_uuid)
    return sorted(seen)


@api_router.get("/accounts", response_model=list[DataPlatformAccountOut])
def list_data_platform_accounts(db: Session = Depends(get_db)) -> list[DataPlatformAccountOut]:
    rows = db.query(DataPlatformAccount).order_by(DataPlatformAccount.updated_at.desc()).all()
    return [_account_out(row) for row in rows]


@api_router.post("/accounts", response_model=DataPlatformAccountOut, status_code=201)
def create_data_platform_account(payload: DataPlatformAccountCreateRequest, db: Session = Depends(get_db)) -> DataPlatformAccountOut:
    row = DataPlatformAccount(
        platform=payload.platform,
        alias=payload.alias.strip(),
        base_url=payload.base_url,
        auth_type=payload.auth_type,
        credential_ciphertext=encrypt_credential(payload.credential.model_dump(mode="python") if payload.credential else None),
        status=payload.status,
        metadata_json=payload.metadata,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _account_out(row)


@api_router.patch("/accounts/{account_id}", response_model=DataPlatformAccountOut)
def update_data_platform_account(account_id: str, payload: DataPlatformAccountUpdateRequest, db: Session = Depends(get_db)) -> DataPlatformAccountOut:
    row = _account_or_404(db, account_id)
    reset_session = False
    if payload.alias is not None:
        row.alias = payload.alias.strip()
    if payload.base_url is not None:
        row.base_url = payload.base_url
        reset_session = True
    if payload.auth_type is not None:
        row.auth_type = payload.auth_type
        reset_session = True
    if payload.credential is not None:
        row.credential_ciphertext = encrypt_credential(payload.credential.model_dump(mode="python"))
        reset_session = True
    if payload.status is not None:
        row.status = payload.status
    if payload.metadata is not None:
        row.metadata_json = payload.metadata
        reset_session = True
    if reset_session:
        db.query(DataPlatformAccountSession).filter(DataPlatformAccountSession.account_id == row.id).delete(synchronize_session=False)
    db.commit()
    db.refresh(row)
    return _account_out(row)


@api_router.delete("/accounts/{account_id}")
def delete_data_platform_account(account_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    row = _account_or_404(db, account_id)
    db.query(DataPlatformAccountSession).filter(DataPlatformAccountSession.account_id == row.id).delete(synchronize_session=False)
    db.delete(row)
    db.commit()
    return {"deleted": True, "account_id": account_id}


@api_router.post("/accounts/{account_id}/test", response_model=DataPlatformConnectionTestResponse)
def test_data_platform_account(account_id: str, db: Session = Depends(get_db)) -> DataPlatformConnectionTestResponse:
    row = _account_or_404(db, account_id)
    checked_at = datetime.utcnow()
    try:
        ok, message = connector_for_account(_account_context(row, db)).test_connection()
    except Exception as exc:  # noqa: BLE001
        ok, message = False, str(exc)
    row.last_validated_at = checked_at
    row.last_validation_status = "ok" if ok else "failed"
    row.last_validation_message = message
    db.commit()
    return DataPlatformConnectionTestResponse(ok=ok, status=row.last_validation_status, message=message, checked_at=checked_at)


@api_router.get("/accounts/{account_id}/flows/search", response_model=DataPlatformSearchResponse)
def search_remote_flows(
    account_id: str,
    q: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    data_source: str = Query(default="tg"),
    state_code: int = Query(default=100),
    db: Session = Depends(get_db),
) -> DataPlatformSearchResponse:
    account = _account_or_404(db, account_id)
    result = connector_for_account(_account_context(account, db)).search_flows(q, page=page, page_size=page_size, data_source=data_source, state_code=state_code)
    for item in result.items:
        _write_cache(db, account=account, remote_kind="flow", query_key=q, remote_id=item.remote_id, payload=_remote_flow_item(item).model_dump(mode="python"))
    db.commit()
    return DataPlatformSearchResponse(
        account_id=account.id,
        platform=account.platform,
        query=q,
        page=result.page,
        page_size=result.page_size,
        has_more=result.has_more,
        total=result.total,
        items=[_remote_flow_item(item) for item in result.items],
    )


@api_router.get("/accounts/{account_id}/processes/search", response_model=DataPlatformSearchResponse)
def search_remote_processes(
    account_id: str,
    q: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    data_source: str = Query(default="tg"),
    state_code: int = Query(default=100),
    db: Session = Depends(get_db),
) -> DataPlatformSearchResponse:
    account = _account_or_404(db, account_id)
    result = connector_for_account(_account_context(account, db)).search_processes(q, page=page, page_size=page_size, data_source=data_source, state_code=state_code)
    for item in result.items:
        _write_cache(db, account=account, remote_kind="process", query_key=q, remote_id=item.remote_id, payload=_remote_process_item(item).model_dump(mode="python"))
    db.commit()
    return DataPlatformSearchResponse(
        account_id=account.id,
        platform=account.platform,
        query=q,
        page=result.page,
        page_size=result.page_size,
        has_more=result.has_more,
        total=result.total,
        items=[_remote_process_item(item) for item in result.items],
    )


@api_router.get("/accounts/{account_id}/models/search", response_model=DataPlatformSearchResponse)
def search_remote_models(
    account_id: str,
    q: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    data_source: str = Query(default="tg"),
    state_code: int = Query(default=100),
    db: Session = Depends(get_db),
) -> DataPlatformSearchResponse:
    account = _account_or_404(db, account_id)
    result = connector_for_account(_account_context(account, db)).search_models(q, page=page, page_size=page_size, data_source=data_source, state_code=state_code)
    for item in result.items:
        _write_cache(db, account=account, remote_kind="model", query_key=q, remote_id=item.remote_id, payload=_remote_model_item(item).model_dump(mode="python"))
    db.commit()
    return DataPlatformSearchResponse(
        account_id=account.id,
        platform=account.platform,
        query=q,
        page=result.page,
        page_size=result.page_size,
        has_more=result.has_more,
        total=result.total,
        items=[_remote_model_item(item) for item in result.items],
    )


@api_router.post("/accounts/{account_id}/flows/sync", response_model=DataPlatformSyncFlowResponse)
def sync_remote_flow(account_id: str, payload: DataPlatformSyncFlowRequest, db: Session = Depends(get_db)) -> DataPlatformSyncFlowResponse:
    account = _account_or_404(db, account_id)
    job = DataPlatformSyncJob(account_id=account.id, platform=account.platform, remote_process_id=payload.remote_flow_id, status="running", phase="fetch", stats_json={"remote_kind": "flow"})
    db.add(job)
    db.flush()
    warnings: list[str] = []
    synced: list[dict[str, Any]] = []
    try:
        connector = connector_for_account(_account_context(account, db))
        flow = connector.get_flow_detail(payload.remote_flow_id, payload.remote_version)
        job.phase = "upsert"
        _upsert_flow(db, account=account, item=flow, warnings=warnings)
        synced.extend(_upsert_flow_dependencies(db, account=account, connector=connector, flow=flow, warnings=warnings))
        synced.append(_upsert_sync_record(db, account=account, local_kind="flow", local_uuid=flow.flow_uuid, remote_id=flow.remote_id, remote_version=flow.remote_version, metadata=flow.metadata))
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        job.stats_json = {"remote_kind": "flow", "warnings": warnings, "synced_count": len(synced)}
        invalidate_management_caches(flows=True, stats=True)
        db.commit()
        return DataPlatformSyncFlowResponse(job_id=job.id, account_id=account.id, platform=account.platform, status=job.status, flow_uuid=flow.flow_uuid, warnings=warnings, synced_records=synced)
    except Exception as exc:  # noqa: BLE001
        job.status = "failed"
        job.phase = "failed"
        job.error_summary = str(exc)
        job.finished_at = datetime.utcnow()
        db.commit()
        raise HTTPException(status_code=400, detail={"code": "DATA_PLATFORM_SYNC_FAILED", "message": str(exc), "job_id": job.id}) from exc


@api_router.post("/accounts/{account_id}/processes/sync", response_model=DataPlatformSyncProcessResponse)
def sync_remote_process(account_id: str, payload: DataPlatformSyncProcessRequest, db: Session = Depends(get_db)) -> DataPlatformSyncProcessResponse:
    account = _account_or_404(db, account_id)
    job = DataPlatformSyncJob(account_id=account.id, platform=account.platform, remote_process_id=payload.remote_process_id, status="running", phase="fetch")
    db.add(job)
    db.flush()
    warnings: list[str] = []
    synced: list[dict[str, Any]] = []
    try:
        connector = connector_for_account(_account_context(account, db))
        detail = connector.get_process_detail(payload.remote_process_id, payload.remote_version)
        job.phase = "upsert"
        flows_by_uuid = {flow.flow_uuid: flow for flow in detail.flows if flow.flow_uuid}
        for exchange in (detail.process_json or {}).get("exchanges", []):
            if not isinstance(exchange, dict):
                continue
            flow_uuid = _safe_str(exchange.get("flow_uuid") or exchange.get("flowUuid"))
            if flow_uuid and flow_uuid not in flows_by_uuid:
                try:
                    fetched = connector.get_flow_detail(flow_uuid)
                    flows_by_uuid[fetched.flow_uuid] = fetched
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"could not fetch referenced flow {flow_uuid}: {exc}")
        for flow in flows_by_uuid.values():
            _upsert_flow(db, account=account, item=flow, warnings=warnings)
            synced.extend(_upsert_flow_dependencies(db, account=account, connector=connector, flow=flow, warnings=warnings))
            synced.append(_upsert_sync_record(db, account=account, local_kind="flow", local_uuid=flow.flow_uuid, remote_id=flow.remote_id, remote_version=flow.remote_version, metadata=flow.metadata))
        process = detail.process
        if not process.process_uuid:
            raise ConnectorError("Remote process is missing process_uuid")
        process_json = detail.process_json or {
            "process_uuid": process.process_uuid,
            "process_name": process.process_name,
            "reference_flow_uuid": process.reference_flow_uuid,
            "exchanges": [],
        }
        row = db.get(ReferenceProcess, process.process_uuid)
        if row is None:
            row = ReferenceProcess(
                process_uuid=process.process_uuid,
                process_name=process.process_name,
                process_name_zh=process.process_name,
                process_name_en=process.process_name,
                process_type=process.process_type,
                reference_flow_uuid=process.reference_flow_uuid,
                process_json=process_json,
                source_process_uuid=process.remote_id,
                import_mode="locked",
                import_report_json={**detail.import_report, "source_platform": account.platform, "account_id": account.id},
            )
            db.add(row)
        else:
            row.process_name = process.process_name or row.process_name
            row.process_name_zh = process.process_name or row.process_name_zh
            row.process_name_en = process.process_name or row.process_name_en
            row.process_type = process.process_type or row.process_type
            row.reference_flow_uuid = process.reference_flow_uuid or row.reference_flow_uuid
            row.process_json = process_json
            row.source_process_uuid = process.remote_id
            row.import_mode = "locked"
            row.import_report_json = {**detail.import_report, "source_platform": account.platform, "account_id": account.id}
        synced.append(_upsert_sync_record(db, account=account, local_kind="process", local_uuid=process.process_uuid, remote_id=process.remote_id, remote_version=process.remote_version, metadata=process.metadata))
        if detail.vector and _upsert_lci_vector(db, account=account, process_uuid=process.process_uuid, vector_payload=detail.vector, warnings=warnings):
            synced.append(_upsert_sync_record(db, account=account, local_kind="vector", local_uuid=process.process_uuid, remote_id=process.remote_id, remote_version=process.remote_version, metadata={"vector": True}))
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        job.stats_json = {"flow_count": len(flows_by_uuid), "warnings": warnings, "synced_count": len(synced)}
        invalidate_management_caches(flows=True, reference_processes=True, stats=True)
        db.commit()
        return DataPlatformSyncProcessResponse(
            job_id=job.id,
            account_id=account.id,
            platform=account.platform,
            status=job.status,
            process_uuid=process.process_uuid,
            flow_count=len(flows_by_uuid),
            warnings=warnings,
            synced_records=synced,
        )
    except Exception as exc:  # noqa: BLE001
        job.status = "failed"
        job.phase = "failed"
        job.error_summary = str(exc)
        job.finished_at = datetime.utcnow()
        db.commit()
        raise HTTPException(status_code=400, detail={"code": "DATA_PLATFORM_SYNC_FAILED", "message": str(exc), "job_id": job.id}) from exc


@api_router.post("/accounts/{account_id}/models/sync", response_model=DataPlatformSyncModelResponse)
def sync_remote_model(account_id: str, payload: DataPlatformSyncModelRequest, db: Session = Depends(get_db)) -> DataPlatformSyncModelResponse:
    account = _account_or_404(db, account_id)
    job = DataPlatformSyncJob(account_id=account.id, platform=account.platform, remote_process_id=payload.remote_model_id, status="running", phase="fetch", stats_json={"remote_kind": "model"})
    db.add(job)
    db.flush()
    job_id = job.id
    warnings: list[str] = []
    synced: list[dict[str, Any]] = []
    try:
        connector = connector_for_account(_account_context(account, db))
        detail = connector.get_model_detail(payload.remote_model_id, payload.remote_version)
        job.phase = "validate"
        graph_json = _extract_hybrid_graph_from_remote_model(detail.model_json)
        graph = HybridGraph.model_validate(graph_json)
        job.phase = "dependencies"
        for flow_uuid in _graph_flow_uuids(graph.model_dump(mode="python")):
            try:
                flow = connector.get_flow_detail(flow_uuid)
                _upsert_flow(db, account=account, item=flow, warnings=warnings)
                synced.extend(_upsert_flow_dependencies(db, account=account, connector=connector, flow=flow, warnings=warnings))
                synced.append(_upsert_sync_record(db, account=account, local_kind="flow", local_uuid=flow.flow_uuid, remote_id=flow.remote_id, remote_version=flow.remote_version, metadata=flow.metadata))
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"could not sync model dependency flow {flow_uuid}: {exc}")
        job.phase = "upsert"
        project = Model(
            name=(payload.project_name or detail.model.model_name or detail.model.model_uuid).strip(),
            functional_unit=graph.functionalUnit,
            description=f"Imported on demand from {account.platform}:{detail.model.remote_id}",
            source_policy="open_mixed",
            allowed_lcia_scope="ef31_only",
            status="active",
        )
        db.add(project)
        db.flush()
        version = _create_project_version_from_graph_json(db=db, project_id=project.id, graph_json=graph.model_dump(mode="python"))
        synced.append(
            _upsert_sync_record(
                db,
                account=account,
                local_kind="model",
                local_uuid=project.id,
                remote_id=detail.model.remote_id,
                remote_version=detail.model.remote_version,
                metadata={**detail.lineage, "model_uuid": detail.model.model_uuid},
            )
        )
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        job.stats_json = {"remote_kind": "model", "project_id": project.id, "version": version.version, "warnings": warnings, "synced_count": len(synced)}
        db.commit()
        return DataPlatformSyncModelResponse(
            job_id=job.id,
            account_id=account.id,
            platform=account.platform,
            status=job.status,
            project_id=project.id,
            version=version.version,
            warnings=warnings,
            synced_records=synced,
        )
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        failed_job = db.get(DataPlatformSyncJob, job_id)
        if failed_job is None:
            failed_job = DataPlatformSyncJob(account_id=account.id, platform=account.platform, remote_process_id=payload.remote_model_id)
            db.add(failed_job)
            db.flush()
        failed_job.status = "failed"
        failed_job.phase = "failed"
        failed_job.error_summary = str(exc)
        failed_job.finished_at = datetime.utcnow()
        failed_job.stats_json = {"remote_kind": "model", "lineage": {"remote_id": payload.remote_model_id, "remote_version": payload.remote_version}}
        db.commit()
        raise HTTPException(status_code=400, detail={"code": "DATA_PLATFORM_MODEL_SYNC_FAILED", "message": str(exc), "job_id": failed_job.id}) from exc
