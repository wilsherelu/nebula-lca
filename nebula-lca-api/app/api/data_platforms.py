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
    LciBiosphereFlowKey,
    LciProcessVector,
    UnitDefinition,
    UnitGroup,
)
from ..schemas import (
    DataPlatformAccountCreateRequest,
    DataPlatformAccountOut,
    DataPlatformAccountUpdateRequest,
    DataPlatformConnectionTestResponse,
    DataPlatformRemotePreviewResponse,
    DataPlatformSearchResponse,
    DataPlatformSyncFlowRequest,
    DataPlatformSyncFlowResponse,
    DataPlatformSyncJobOut,
    DataPlatformSyncModelRequest,
    DataPlatformSyncModelResponse,
    DataPlatformSyncProcessRequest,
    DataPlatformSyncProcessResponse,
    DataPlatformSyncRecordOut,
    RemoteFlowItem,
    RemoteModelItem,
    RemoteProcessItem,
)
from ..services.catalog_cache import invalidate_management_caches
from ..services.data_platform_connectors import (
    ConnectorError,
    CredentialError,
    PlatformAccountContext,
    RemoteFlowDTO,
    RemoteModelDTO,
    RemoteProcessDTO,
    RemoteUnitGroupDTO,
    connector_for_account,
    decrypt_credential,
    encrypt_credential,
)
from ..services.tidas_import_core import (
    import_tidas_flow_rows,
    import_tidas_model_rows,
    import_tidas_process_rows,
)


api_router = APIRouter(prefix="/api/data-platforms", tags=["data-platforms"])


def _safe_str(value: object) -> str:
    return str(value or "").strip()


def _credential_config_error(exc: CredentialError) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail={
            "code": "DATA_PLATFORM_CREDENTIAL_KEY_REQUIRED",
            "message": str(exc),
        },
    )


def _connector_error(exc: ConnectorError) -> HTTPException:
    return HTTPException(
        status_code=502,
        detail={
            "code": "DATA_PLATFORM_CONNECTOR_ERROR",
            "message": str(exc),
        },
    )


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


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _localized_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        preferred = ""
        for item in value:
            if isinstance(item, dict) and str(item.get("@xml:lang") or "").lower().startswith("zh"):
                preferred = _localized_text(item)
                if preferred:
                    return preferred
            text = _localized_text(item)
            if text and not preferred:
                preferred = text
        return preferred
    if isinstance(value, dict):
        for key in ("#text", "@value", "value", "text"):
            text = _safe_str(value.get(key))
            if text:
                return text
        for key in ("baseName", "common:baseName", "name", "common:name", "shortDescription", "common:shortDescription"):
            text = _localized_text(value.get(key))
            if text:
                return text
    return ""


def _dataset_info(payload: dict[str, Any], dataset_key: str, info_key: str) -> dict[str, Any]:
    dataset = payload.get(dataset_key) if isinstance(payload.get(dataset_key), dict) else payload
    info = dataset.get(info_key) if isinstance(dataset, dict) and isinstance(dataset.get(info_key), dict) else {}
    data_set_info = info.get("dataSetInformation") if isinstance(info.get("dataSetInformation"), dict) else {}
    return data_set_info


def _dataset_comment(data_set_info: dict[str, Any]) -> str:
    return _localized_text(data_set_info.get("common:generalComment") or data_set_info.get("generalComment"))


def _dataset_name(data_set_info: dict[str, Any], fallback: str) -> str:
    return _localized_text(data_set_info.get("name") or data_set_info.get("common:name")) or fallback


def _classification_path(data_set_info: dict[str, Any]) -> str:
    raw = (
        data_set_info.get("classificationInformation", {})
        .get("common:classification", {})
        .get("common:class")
        if isinstance(data_set_info.get("classificationInformation"), dict)
        else None
    )
    names = [_localized_text(item) for item in _as_list(raw)]
    return " > ".join(item for item in names if item)


def _process_exchanges_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    dataset = payload.get("processDataSet") if isinstance(payload.get("processDataSet"), dict) else payload
    exchanges = dataset.get("exchanges") if isinstance(dataset, dict) else None
    raw = exchanges.get("exchange") if isinstance(exchanges, dict) else exchanges
    return [item for item in _as_list(raw) if isinstance(item, dict)]


def _model_process_instances(payload: dict[str, Any]) -> list[dict[str, Any]]:
    dataset = payload.get("lifeCycleModelDataSet") if isinstance(payload.get("lifeCycleModelDataSet"), dict) else payload
    info = dataset.get("lifeCycleModelInformation") if isinstance(dataset, dict) else None
    technology = info.get("technology") if isinstance(info, dict) else None
    processes = technology.get("processes") if isinstance(technology, dict) else None
    raw = processes.get("processInstance") if isinstance(processes, dict) else None
    return [item for item in _as_list(raw) if isinstance(item, dict)]


def _preview_from_flow(account: DataPlatformAccount, flow: RemoteFlowDTO) -> DataPlatformRemotePreviewResponse:
    row = flow.metadata.get("row") if isinstance(flow.metadata, dict) and isinstance(flow.metadata.get("row"), dict) else {}
    payload = _remote_raw_row(flow.metadata, row)
    data_info = _dataset_info(payload, "flowDataSet", "flowInformation")
    return DataPlatformRemotePreviewResponse(
        account_id=account.id,
        platform=account.platform,
        remote_kind="flow",
        remote_id=flow.remote_id,
        remote_version=flow.remote_version,
        title=flow.flow_name or _dataset_name(data_info, flow.flow_uuid),
        description=_dataset_comment(data_info),
        summary={
            "uuid": flow.flow_uuid,
            "flow_type": flow.flow_type,
            "default_unit": flow.default_unit,
            "unit_group": flow.unit_group,
            "classification": _classification_path(data_info),
        },
    )


def _preview_from_process(account: DataPlatformAccount, detail: Any) -> DataPlatformRemotePreviewResponse:
    process = detail.process
    payload = detail.process_json if isinstance(detail.process_json, dict) else {}
    data_info = _dataset_info(payload, "processDataSet", "processInformation")
    exchanges = _process_exchanges_from_payload(payload)
    input_count = sum(1 for item in exchanges if _safe_str(item.get("exchangeDirection")).lower() == "input")
    output_count = sum(1 for item in exchanges if _safe_str(item.get("exchangeDirection")).lower() == "output")
    samples = []
    for exchange in exchanges[:5]:
        ref = exchange.get("referenceToFlowDataSet") if isinstance(exchange.get("referenceToFlowDataSet"), dict) else {}
        samples.append({
            "direction": exchange.get("exchangeDirection"),
            "flow_id": ref.get("@refObjectId") or ref.get("refObjectId") or exchange.get("flow_uuid") or exchange.get("flowUuid"),
            "name": _localized_text(ref.get("common:shortDescription") or ref.get("shortDescription")) or _safe_str(exchange.get("flow_name") or exchange.get("flowName")),
            "amount": exchange.get("meanAmount") or exchange.get("resultingAmount"),
        })
    return DataPlatformRemotePreviewResponse(
        account_id=account.id,
        platform=account.platform,
        remote_kind="process",
        remote_id=process.remote_id,
        remote_version=process.remote_version,
        title=process.process_name or _dataset_name(data_info, process.process_uuid),
        description=_dataset_comment(data_info),
        summary={
            "uuid": process.process_uuid,
            "process_type": process.process_type,
            "reference_flow_uuid": process.reference_flow_uuid,
            "classification": _classification_path(data_info),
            "exchange_count": len(exchanges),
            "input_count": input_count,
            "output_count": output_count,
        },
        related=samples,
    )


def _preview_from_model(account: DataPlatformAccount, detail: Any) -> DataPlatformRemotePreviewResponse:
    model = detail.model
    payload = detail.model_json if isinstance(detail.model_json, dict) else {}
    data_info = _dataset_info(payload, "lifeCycleModelDataSet", "lifeCycleModelInformation")
    instances = _model_process_instances(payload)
    samples = []
    for item in instances[:5]:
        ref = item.get("referenceToProcess") if isinstance(item.get("referenceToProcess"), dict) else {}
        samples.append({
            "process_id": ref.get("@refObjectId") or ref.get("refObjectId"),
            "name": _localized_text(ref.get("common:shortDescription") or ref.get("shortDescription")),
            "factor": item.get("@multiplicationFactor") or item.get("multiplicationFactor"),
        })
    return DataPlatformRemotePreviewResponse(
        account_id=account.id,
        platform=account.platform,
        remote_kind="model",
        remote_id=model.remote_id,
        remote_version=model.remote_version,
        title=model.model_name or _dataset_name(data_info, model.model_uuid),
        description=_dataset_comment(data_info),
        summary={
            "uuid": model.model_uuid,
            "classification": _classification_path(data_info),
            "process_count": len(instances),
        },
        related=samples,
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


def _remote_raw_row(metadata: dict[str, Any] | None, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(metadata, dict):
        row = metadata.get("row")
        if isinstance(row, dict):
            return row
    return fallback if isinstance(fallback, dict) else {}


def _tidas_report_summary(report: Any) -> dict[str, Any]:
    data = report.model_dump(mode="python") if hasattr(report, "model_dump") else {}
    return {
        "job_id": data.get("job_id"),
        "import_type": data.get("import_type"),
        "inserted": data.get("inserted", 0),
        "updated": data.get("updated", 0),
        "skipped": data.get("skipped", 0),
        "failed": data.get("failed", 0),
        "warning_count": data.get("warning_count", 0),
        "unresolved_count": data.get("unresolved_count", 0),
        "created_projects": data.get("created_projects", []),
        "errors": list(data.get("errors") or [])[:10],
        "warnings": list(data.get("warnings") or [])[:10],
    }


@api_router.get("/accounts", response_model=list[DataPlatformAccountOut])
def list_data_platform_accounts(db: Session = Depends(get_db)) -> list[DataPlatformAccountOut]:
    rows = db.query(DataPlatformAccount).order_by(DataPlatformAccount.updated_at.desc()).all()
    return [_account_out(row) for row in rows]


@api_router.post("/accounts", response_model=DataPlatformAccountOut, status_code=201)
def create_data_platform_account(payload: DataPlatformAccountCreateRequest, db: Session = Depends(get_db)) -> DataPlatformAccountOut:
    try:
        credential_ciphertext = encrypt_credential(payload.credential.model_dump(mode="python") if payload.credential else None)
    except CredentialError as exc:
        raise _credential_config_error(exc) from exc
    row = DataPlatformAccount(
        platform=payload.platform,
        alias=payload.alias.strip(),
        base_url=payload.base_url,
        auth_type=payload.auth_type,
        credential_ciphertext=credential_ciphertext,
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
        try:
            row.credential_ciphertext = encrypt_credential(payload.credential.model_dump(mode="python"))
        except CredentialError as exc:
            raise _credential_config_error(exc) from exc
        row.last_validated_at = None
        row.last_validation_status = None
        row.last_validation_message = None
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
    state_code: int | None = Query(default=100),
    state_scope: str = Query(default="open"),
    flow_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> DataPlatformSearchResponse:
    account = _account_or_404(db, account_id)
    effective_state_code = None if state_scope == "all" else state_code
    try:
        result = connector_for_account(_account_context(account, db)).search_flows(q, page=page, page_size=page_size, data_source=data_source, state_code=effective_state_code, flow_type=flow_type)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc
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
    state_code: int | None = Query(default=100),
    state_scope: str = Query(default="open"),
    process_type: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> DataPlatformSearchResponse:
    account = _account_or_404(db, account_id)
    effective_state_code = None if state_scope == "all" else state_code
    try:
        result = connector_for_account(_account_context(account, db)).search_processes(q, page=page, page_size=page_size, data_source=data_source, state_code=effective_state_code, process_type=process_type)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc
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
    state_code: int | None = Query(default=100),
    state_scope: str = Query(default="open"),
    db: Session = Depends(get_db),
) -> DataPlatformSearchResponse:
    account = _account_or_404(db, account_id)
    effective_state_code = None if state_scope == "all" else state_code
    try:
        result = connector_for_account(_account_context(account, db)).search_models(q, page=page, page_size=page_size, data_source=data_source, state_code=effective_state_code)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc
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


@api_router.get("/accounts/{account_id}/flows/{remote_id}/preview", response_model=DataPlatformRemotePreviewResponse)
def preview_remote_flow(
    account_id: str,
    remote_id: str,
    remote_version: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> DataPlatformRemotePreviewResponse:
    account = _account_or_404(db, account_id)
    try:
        flow = connector_for_account(_account_context(account, db)).get_flow_detail(remote_id, remote_version)
        return _preview_from_flow(account, flow)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc


@api_router.get("/accounts/{account_id}/processes/{remote_id}/preview", response_model=DataPlatformRemotePreviewResponse)
def preview_remote_process(
    account_id: str,
    remote_id: str,
    remote_version: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> DataPlatformRemotePreviewResponse:
    account = _account_or_404(db, account_id)
    try:
        detail = connector_for_account(_account_context(account, db)).get_process_detail(remote_id, remote_version)
        return _preview_from_process(account, detail)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc


@api_router.get("/accounts/{account_id}/models/{remote_id}/preview", response_model=DataPlatformRemotePreviewResponse)
def preview_remote_model(
    account_id: str,
    remote_id: str,
    remote_version: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> DataPlatformRemotePreviewResponse:
    account = _account_or_404(db, account_id)
    try:
        detail = connector_for_account(_account_context(account, db)).get_model_detail(remote_id, remote_version)
        return _preview_from_model(account, detail)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc


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
        synced.extend(_upsert_flow_dependencies(db, account=account, connector=connector, flow=flow, warnings=warnings))
        flow_row = _remote_raw_row(
            flow.metadata,
            {
                "id": flow.flow_uuid or flow.remote_id,
                "name": flow.flow_name,
                "version": flow.remote_version,
                "default_unit": flow.default_unit,
                "unit_group": flow.unit_group,
                "flow_type": flow.flow_type,
                "source": account.platform,
            },
        )
        tidas_report = import_tidas_flow_rows(
            db,
            [flow_row],
            source_path=f"{account.platform}://flows/{flow.remote_id}",
            upsert_mode="update" if payload.overwrite else "skip",
            source_label=account.platform,
        )
        if tidas_report.failed:
            raise ConnectorError(f"TIDAS flow import failed: {tidas_report.errors[:3]}")
        synced.append(_upsert_sync_record(db, account=account, local_kind="flow", local_uuid=flow.flow_uuid, remote_id=flow.remote_id, remote_version=flow.remote_version, metadata=flow.metadata))
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        report_summary = _tidas_report_summary(tidas_report)
        job.stats_json = {"remote_kind": "flow", "warnings": warnings, "synced_count": len(synced), "tidas_import": report_summary}
        invalidate_management_caches(flows=True, stats=True)
        db.commit()
        return DataPlatformSyncFlowResponse(
            job_id=job.id,
            account_id=account.id,
            platform=account.platform,
            status=job.status,
            flow_uuid=flow.flow_uuid,
            tidas_import_job_id=tidas_report.job_id,
            tidas_import_report=report_summary,
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


@api_router.post("/accounts/{account_id}/processes/sync", response_model=DataPlatformSyncProcessResponse)
def sync_remote_process(account_id: str, payload: DataPlatformSyncProcessRequest, db: Session = Depends(get_db)) -> DataPlatformSyncProcessResponse:
    account = _account_or_404(db, account_id)
    job = DataPlatformSyncJob(account_id=account.id, platform=account.platform, remote_process_id=payload.remote_process_id, status="running", phase="fetch", stats_json={"remote_kind": "process"})
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
            synced.extend(_upsert_flow_dependencies(db, account=account, connector=connector, flow=flow, warnings=warnings))
            flow_row = _remote_raw_row(
                flow.metadata,
                {
                    "id": flow.flow_uuid or flow.remote_id,
                    "name": flow.flow_name,
                    "version": flow.remote_version,
                    "default_unit": flow.default_unit,
                    "unit_group": flow.unit_group,
                    "flow_type": flow.flow_type,
                    "source": account.platform,
                },
            )
            flow_report = import_tidas_flow_rows(
                db,
                [flow_row],
                source_path=f"{account.platform}://flows/{flow.remote_id}",
                upsert_mode="update" if payload.overwrite else "skip",
                source_label=account.platform,
            )
            if flow_report.failed:
                warnings.append(f"flow {flow.flow_uuid} TIDAS import warnings/errors: {flow_report.errors[:3]}")
            synced.append(_upsert_sync_record(db, account=account, local_kind="flow", local_uuid=flow.flow_uuid, remote_id=flow.remote_id, remote_version=flow.remote_version, metadata=flow.metadata))
        process = detail.process
        if not process.process_uuid:
            raise ConnectorError("Remote process is missing process_uuid")
        process_row = _remote_raw_row(
            process.metadata,
            detail.process_json or {
                "process_uuid": process.process_uuid,
                "process_name": process.process_name,
                "reference_flow_uuid": process.reference_flow_uuid,
                "exchanges": [],
            },
        )
        process_report = import_tidas_process_rows(
            db,
            [process_row],
            source_path=f"{account.platform}://processes/{process.remote_id}",
            upsert_mode="update" if payload.overwrite else "skip",
        )
        if process_report.failed:
            raise ConnectorError(f"TIDAS process import failed: {process_report.errors[:3]}")
        synced.append(_upsert_sync_record(db, account=account, local_kind="process", local_uuid=process.process_uuid, remote_id=process.remote_id, remote_version=process.remote_version, metadata=process.metadata))
        if detail.vector and _upsert_lci_vector(db, account=account, process_uuid=process.process_uuid, vector_payload=detail.vector, warnings=warnings):
            synced.append(_upsert_sync_record(db, account=account, local_kind="vector", local_uuid=process.process_uuid, remote_id=process.remote_id, remote_version=process.remote_version, metadata={"vector": True}))
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        report_summary = _tidas_report_summary(process_report)
        job.stats_json = {"remote_kind": "process", "flow_count": len(flows_by_uuid), "warnings": warnings, "synced_count": len(synced), "tidas_import": report_summary}
        invalidate_management_caches(flows=True, reference_processes=True, stats=True)
        db.commit()
        return DataPlatformSyncProcessResponse(
            job_id=job.id,
            account_id=account.id,
            platform=account.platform,
            status=job.status,
            process_uuid=process.process_uuid,
            flow_count=len(flows_by_uuid),
            tidas_import_job_id=process_report.job_id,
            tidas_import_report=report_summary,
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
        graph_json: dict[str, Any] | None = None
        try:
            graph_json = _extract_hybrid_graph_from_remote_model(detail.model_json)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"model HybridGraph will be validated by TIDAS import core: {exc}")
        job.phase = "dependencies"
        if graph_json is not None:
            for flow_uuid in _graph_flow_uuids(graph_json):
                try:
                    flow = connector.get_flow_detail(flow_uuid)
                    synced.extend(_upsert_flow_dependencies(db, account=account, connector=connector, flow=flow, warnings=warnings))
                    flow_row = _remote_raw_row(
                        flow.metadata,
                        {
                            "id": flow.flow_uuid or flow.remote_id,
                            "name": flow.flow_name,
                            "version": flow.remote_version,
                            "default_unit": flow.default_unit,
                            "unit_group": flow.unit_group,
                            "flow_type": flow.flow_type,
                            "source": account.platform,
                        },
                    )
                    flow_report = import_tidas_flow_rows(
                        db,
                        [flow_row],
                        source_path=f"{account.platform}://flows/{flow.remote_id}",
                        upsert_mode="update",
                        source_label=account.platform,
                    )
                    if flow_report.failed:
                        warnings.append(f"model dependency flow {flow_uuid} TIDAS import failed: {flow_report.errors[:3]}")
                    synced.append(_upsert_sync_record(db, account=account, local_kind="flow", local_uuid=flow.flow_uuid, remote_id=flow.remote_id, remote_version=flow.remote_version, metadata=flow.metadata))
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"could not sync model dependency flow {flow_uuid}: {exc}")
        job.phase = "upsert"
        model_row = _remote_raw_row(detail.lineage, detail.model_json or {"id": detail.model.model_uuid, "name": detail.model.model_name})
        model_report = import_tidas_model_rows(
            db,
            [model_row],
            source_path=f"{account.platform}://models/{detail.model.remote_id}",
            project_name=(payload.project_name or detail.model.model_name or detail.model.model_uuid).strip(),
        )
        if model_report.failed:
            raise ConnectorError(f"TIDAS model import failed: {model_report.errors[:3]}")
        created_projects = list(model_report.created_projects or [])
        if not created_projects:
            raise ConnectorError("TIDAS model import did not create a project")
        project_id = str(created_projects[0].get("project_id") or "")
        version_value = int(created_projects[0].get("version") or 1)
        synced.append(
            _upsert_sync_record(
                db,
                account=account,
                local_kind="model",
                local_uuid=project_id,
                remote_id=detail.model.remote_id,
                remote_version=detail.model.remote_version,
                metadata={**detail.lineage, "model_uuid": detail.model.model_uuid},
            )
        )
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        report_summary = _tidas_report_summary(model_report)
        job.stats_json = {"remote_kind": "model", "project_id": project_id, "version": version_value, "warnings": warnings, "synced_count": len(synced), "tidas_import": report_summary}
        db.commit()
        return DataPlatformSyncModelResponse(
            job_id=job.id,
            account_id=account.id,
            platform=account.platform,
            status=job.status,
            project_id=project_id,
            version=version_value,
            tidas_import_job_id=model_report.job_id,
            tidas_import_report=report_summary,
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


# ======================================================================
# Sync History Endpoints
# ======================================================================


def _sync_job_out(job: DataPlatformSyncJob) -> DataPlatformSyncJobOut:
    return DataPlatformSyncJobOut(
        id=job.id,
        account_id=job.account_id,
        platform=job.platform,
        remote_process_id=job.remote_process_id,
        status=job.status,
        phase=job.phase,
        stats=job.stats_json,
        error_summary=job.error_summary,
        created_at=job.created_at,
        updated_at=job.updated_at,
        finished_at=job.finished_at,
    )


def _sync_record_out(record: ExternalDataSyncRecord) -> DataPlatformSyncRecordOut:
    return DataPlatformSyncRecordOut(
        id=record.id,
        account_id=record.account_id,
        platform=record.platform,
        local_kind=record.local_kind,
        local_uuid=record.local_uuid,
        remote_id=record.remote_id,
        remote_version=record.remote_version,
        metadata=record.metadata_json,
        synced_at=record.synced_at,
    )


@api_router.get(
    "/accounts/{account_id}/sync-jobs",
    response_model=list[DataPlatformSyncJobOut],
)
def list_sync_jobs(
    account_id: str,
    platform: str | None = Query(default=None, description="Filter by platform name"),
    status: str | None = Query(default=None, description="Filter by job status"),
    remote_kind: str | None = Query(default=None, description="Filter by stats.remote_kind (e.g. flow, process, model)"),
    limit: int = Query(default=50, ge=1, le=200, description="Max number of jobs to return (default 50, max 200)"),
    db: Session = Depends(get_db),
) -> list[DataPlatformSyncJobOut]:
    account = _account_or_404(db, account_id)

    query = db.query(DataPlatformSyncJob).filter(DataPlatformSyncJob.account_id == account.id)

    if platform is not None:
        query = query.filter(DataPlatformSyncJob.platform == platform)
    if status is not None:
        query = query.filter(DataPlatformSyncJob.status == status)
    if remote_kind is not None:
        query = query.filter(DataPlatformSyncJob.stats_json["remote_kind"].as_string() == remote_kind)

    jobs = query.order_by(DataPlatformSyncJob.created_at.desc()).limit(limit).all()
    return [_sync_job_out(job) for job in jobs]


@api_router.get(
    "/accounts/{account_id}/sync-records",
    response_model=list[DataPlatformSyncRecordOut],
)
def list_sync_records(
    account_id: str,
    local_kind: str | None = Query(default=None, description="Filter by local kind (e.g. flow, process, vector)"),
    platform: str | None = Query(default=None, description="Filter by platform name"),
    remote_id: str | None = Query(default=None, description="Filter by remote ID"),
    local_uuid: str | None = Query(default=None, description="Filter by local UUID"),
    limit: int = Query(default=50, ge=1, le=200, description="Max number of records to return (default 50, max 200)"),
    db: Session = Depends(get_db),
) -> list[DataPlatformSyncRecordOut]:
    account = _account_or_404(db, account_id)

    query = db.query(ExternalDataSyncRecord).filter(ExternalDataSyncRecord.account_id == account.id)

    if platform is not None:
        query = query.filter(ExternalDataSyncRecord.platform == platform)
    if local_kind is not None:
        query = query.filter(ExternalDataSyncRecord.local_kind == local_kind)
    if remote_id is not None:
        query = query.filter(ExternalDataSyncRecord.remote_id == remote_id)
    if local_uuid is not None:
        query = query.filter(ExternalDataSyncRecord.local_uuid == local_uuid)

    records = query.order_by(ExternalDataSyncRecord.synced_at.desc()).limit(limit).all()
    return [_sync_record_out(record) for record in records]
