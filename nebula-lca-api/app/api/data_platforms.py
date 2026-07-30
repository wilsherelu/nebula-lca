"""External LCA data platform account and sync APIs."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any
from uuid import UUID

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
    ReferenceProcess,
    UnitDefinition,
    UnitGroup,
)
from ..schemas import (
    DataPlatformAccountCreateRequest,
    DataPlatformAccountOut,
    DataPlatformAccountUpdateRequest,
    DataPlatformConnectionTestResponse,
    DataPlatformPublishRequest,
    DataPlatformPublishResponse,
    DataPlatformRefreshImportItem,
    DataPlatformRefreshImportsRequest,
    DataPlatformRefreshImportsResponse,
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
    HiqlcdAccountRequest,
    HiqlcdDatasetImportRequest,
    HiqlcdDatasetImportResponse,
    RemoteFlowItem,
    RemoteModelItem,
    RemoteProcessItem,
    TianGongFlowRefreshResponse,
)
from ..services.catalog_cache import invalidate_management_caches
from ..services.data_platform_connectors import (
    ConnectorError,
    CredentialError,
    PlatformAccountContext,
    RemoteFlowDTO,
    RemoteFlowPublishReferenceDTO,
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


def _validate_tiangong_credential(
    *,
    auth_type: str,
    credential: Any | None,
    credential_required: bool,
) -> None:
    """Keep TianGong's two user-facing authentication paths mutually exclusive."""
    if auth_type not in {"basic", "api_key"}:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "TIANGONG_AUTH_TYPE_INVALID",
                "message": "TianGong authentication must use either account login or TianGong API Key.",
            },
        )
    if credential is None:
        if credential_required:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "TIANGONG_CREDENTIAL_REQUIRED",
                    "message": "Provide credentials for the selected TianGong authentication method.",
                },
            )
        return

    values = credential.model_dump(mode="python")
    api_key = _safe_str(values.get("api_key"))
    token = _safe_str(values.get("token"))
    username = _safe_str(values.get("username"))
    password = str(values.get("password") or "")
    if credential_required and not any((api_key, token, username, password)):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "TIANGONG_CREDENTIAL_REQUIRED",
                "message": "Provide credentials for the selected TianGong authentication method.",
            },
        )
    if auth_type == "api_key":
        if not api_key or token or username or password:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "TIANGONG_API_KEY_CREDENTIAL_INVALID",
                    "message": "TianGong API Key authentication accepts only credential.api_key.",
                },
            )
        return
    if not username or not password or api_key or token:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "TIANGONG_LOGIN_CREDENTIAL_INVALID",
                "message": "TianGong account login accepts only credential.username and credential.password.",
            },
        )


def _validate_hiqlcd_credential(*, credential: Any | None, credential_required: bool) -> None:
    if credential is None:
        if credential_required:
            raise HTTPException(
                status_code=422,
                detail={"code": "HIQLCD_API_KEY_REQUIRED", "message": "Provide a HiQLCD API Key."},
            )
        return
    values = credential.model_dump(mode="python")
    if credential_required and not _safe_str(values.get("api_key")):
        raise HTTPException(
            status_code=422,
            detail={"code": "HIQLCD_API_KEY_REQUIRED", "message": "Provide a HiQLCD API Key."},
        )
    if _safe_str(values.get("token")) or _safe_str(values.get("username")) or str(values.get("password") or ""):
        raise HTTPException(
            status_code=422,
            detail={"code": "HIQLCD_AUTH_TYPE_INVALID", "message": "HiQLCD only accepts its API Key."},
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
        process_name_en=item.process_name_en,
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
        model_name_en=item.model_name_en,
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


def _preview_exchange_direction(item: dict[str, Any]) -> str:
    return _safe_str(item.get("direction") or item.get("exchangeDirection")).lower()


def _preview_exchange_amount(item: dict[str, Any]) -> Any:
    for key in ("amount", "meanAmount", "resultingAmount", "meanValue"):
        if item.get(key) is not None:
            return item.get(key)
    return None


def _preview_exchange_unit(item: dict[str, Any]) -> str:
    value = item.get("unit") or item.get("referenceToUnit")
    if isinstance(value, dict):
        return _localized_text(value.get("common:shortDescription") or value.get("shortDescription") or value.get("name") or value.get("common:name")) or _safe_str(value.get("@refObjectId") or value.get("refObjectId"))
    return _safe_str(value)


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
    deps = flow.metadata.get("dependencies") if isinstance(flow.metadata, dict) and isinstance(flow.metadata.get("dependencies"), dict) else {}
    flow_property = deps.get("flow_property") if isinstance(deps, dict) and isinstance(deps.get("flow_property"), dict) else {}
    unit_group_ref = deps.get("unit_group_ref") if isinstance(deps, dict) and isinstance(deps.get("unit_group_ref"), dict) else {}
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
            "flow_property_uuid": flow.metadata.get("flow_property_id") or flow_property.get("id") or "",
            "unit_group_uuid": flow.metadata.get("unit_group_id") or unit_group_ref.get("id") or "",
            "classification": _classification_path(data_info),
        },
    )


def _preview_from_process(account: DataPlatformAccount, detail: Any) -> DataPlatformRemotePreviewResponse:
    process = detail.process
    payload = detail.process_json if isinstance(detail.process_json, dict) else {}
    data_info = _dataset_info(payload, "processDataSet", "processInformation")
    exchanges = _process_exchanges_from_payload(payload)
    if not exchanges and isinstance(payload.get("exchanges"), list):
        exchanges = [item for item in payload["exchanges"] if isinstance(item, dict)]
    input_count = sum(1 for item in exchanges if _preview_exchange_direction(item) == "input")
    output_count = sum(1 for item in exchanges if _preview_exchange_direction(item) == "output")
    reference_internal_id = _safe_str(payload.get("reference_flow_internal_id"))
    reference_flow_name = _safe_str(payload.get("reference_flow_source_name") or payload.get("reference_flow_name") or payload.get("reference_product"))
    samples = []
    for exchange in exchanges[:5]:
        ref = exchange.get("referenceToFlowDataSet") if isinstance(exchange.get("referenceToFlowDataSet"), dict) else {}
        samples.append({
            "direction": exchange.get("direction") or exchange.get("exchangeDirection"),
            "flow_id": ref.get("@refObjectId") or ref.get("refObjectId") or exchange.get("flow_uuid") or exchange.get("flowUuid"),
            "name": _localized_text(ref.get("common:shortDescription") or ref.get("shortDescription")) or _safe_str(exchange.get("flow_name") or exchange.get("flowName")),
            "amount": _preview_exchange_amount(exchange),
            "unit": _preview_exchange_unit(exchange),
            "is_reference_flow": bool(exchange.get("is_reference_flow")),
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
            "reference_flow_internal_id": reference_internal_id,
            "reference_flow_name": reference_flow_name,
            "reference_product": _safe_str(payload.get("reference_product")),
            "reference_product_unit": _safe_str(payload.get("reference_product_unit")),
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


def _tiangong_lang_text(text: str, lang: str = "zh") -> dict[str, str]:
    return {"#text": text, "@xml:lang": lang}


def _tiangong_dataset_version() -> str:
    return "01.01.000"


def _flow_publish_preflight(flow: FlowRecord, reference: RemoteFlowPublishReferenceDTO) -> None:
    errors: list[str] = []
    try:
        UUID(_safe_str(flow.flow_uuid))
    except (TypeError, ValueError):
        errors.append("Flow UUID must be a valid UUID.")
    if not _safe_str(flow.flow_name):
        errors.append("Flow name is required.")
    if not _safe_str(flow.flow_type):
        errors.append("Flow type is required.")
    if not _safe_str(flow.default_unit):
        errors.append("Flow reference unit is required.")
    if not _safe_str(flow.tidas_flow_property_uuid):
        errors.append("Flow property UUID is required.")
    if _safe_str(flow.tidas_flow_property_uuid) != reference.flow_property_uuid:
        errors.append("Resolved Flow property UUID does not match the local Flow.")
    unit_group = reference.unit_group
    if not unit_group.source_uuid or not unit_group.source_version:
        errors.append("Resolved unit group identity is incomplete.")
    if not any(item.unit_name == _safe_str(flow.default_unit) for item in unit_group.definitions):
        errors.append("Flow reference unit is not available in the resolved unit group.")
    if errors:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "TIANGONG_FLOW_PUBLISH_PREFLIGHT_FAILED",
                "message": "TianGong Flow publish preflight failed.",
                "errors": errors,
            },
        )


def _flow_json_ordered(flow: FlowRecord, reference: RemoteFlowPublishReferenceDTO) -> dict[str, Any]:
    version = _tiangong_dataset_version()
    name = _safe_str(flow.flow_name) or flow.flow_uuid
    name_en = _safe_str(flow.flow_name_en)
    name_node: dict[str, Any] = {"baseName": _tiangong_lang_text(name, "zh")}
    if name_en and name_en != name:
        name_node["common:baseName"] = [_tiangong_lang_text(name, "zh"), _tiangong_lang_text(name_en, "en")]
    return {
        "flowDataSet": {
            "@xmlns": "http://lca.jrc.it/ILCD/Flow",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@version": "1.1",
            "@locations": "../ILCDLocations.xml",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
            "flowInformation": {
                "dataSetInformation": {
                    "common:UUID": flow.flow_uuid,
                    "name": name_node,
                },
                "quantitativeReference": {
                    "referenceToReferenceFlowProperty": "0",
                },
            },
            "modellingAndValidation": {
                "LCIMethod": {
                    "typeOfDataSet": flow.flow_type,
                }
            },
            "flowProperties": {
                "flowProperty": {
                    "@dataSetInternalID": "0",
                    "referenceToFlowPropertyDataSet": {
                        "@refObjectId": reference.flow_property_uuid,
                        "@version": reference.flow_property_version,
                        "@type": "flow property data set",
                        "common:shortDescription": _tiangong_lang_text(
                            reference.flow_property_name or reference.unit_group.name,
                            "en",
                        ),
                    },
                    "meanValue": 1,
                }
            },
            "administrativeInformation": {
                "dataEntryBy": {
                    "common:timeStamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": version,
                    "common:permanentDataSetURI": f"nebula-lca://flows/{flow.flow_uuid}?version={version}",
                }
            },
        }
    }


def _process_exchange_json(exchange: dict[str, Any], index: int) -> dict[str, Any]:
    flow_uuid = _safe_str(exchange.get("flow_uuid") or exchange.get("flowUuid"))
    flow_name = _safe_str(exchange.get("flow_name") or exchange.get("flowName") or flow_uuid)
    direction = _safe_str(exchange.get("direction") or exchange.get("exchangeDirection")).lower()
    if direction not in {"input", "output"}:
        direction = "output" if bool(exchange.get("isProduct") or exchange.get("is_product") or exchange.get("is_reference_flow")) else "input"
    try:
        amount = float(exchange.get("amount") if exchange.get("amount") is not None else exchange.get("meanAmount") if exchange.get("meanAmount") is not None else exchange.get("resultingAmount") or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return {
        "@dataSetInternalID": _safe_str(exchange.get("exchange_internal_id")) or str(index),
        "exchangeDirection": direction,
        "meanAmount": amount,
        "resultingAmount": amount,
        "unit": _safe_str(exchange.get("unit")) or "kg",
        "referenceToFlowDataSet": {
            "@refObjectId": flow_uuid,
            "@version": _safe_str(exchange.get("flow_version")) or _tiangong_dataset_version(),
            "common:shortDescription": _tiangong_lang_text(flow_name, "zh"),
        },
        "productOutput": bool(exchange.get("isProduct") or exchange.get("is_product") or exchange.get("is_reference_flow")),
    }


def _process_json_ordered(process: ReferenceProcess) -> dict[str, Any]:
    version = _tiangong_dataset_version()
    source = process.process_json if isinstance(process.process_json, dict) else {}
    name = _safe_str(process.process_name_zh or process.process_name) or process.process_uuid
    name_en = _safe_str(process.process_name_en)
    name_node: dict[str, Any] = {"baseName": _tiangong_lang_text(name, "zh")}
    if name_en and name_en != name:
        name_node["common:baseName"] = [_tiangong_lang_text(name, "zh"), _tiangong_lang_text(name_en, "en")]
    exchanges = [
        _process_exchange_json(item, idx)
        for idx, item in enumerate(_as_list(source.get("exchanges")), start=1)
        if isinstance(item, dict)
    ]
    ref_internal_id = _safe_str(process.reference_flow_internal_id or source.get("reference_flow_internal_id"))
    if not ref_internal_id and exchanges:
        ref_internal_id = _safe_str(exchanges[0].get("@dataSetInternalID"))
    return {
        "processDataSet": {
            "processInformation": {
                "dataSetInformation": {
                    "common:UUID": process.process_uuid,
                    "UUID": process.process_uuid,
                    "name": name_node,
                },
                "quantitativeReference": {
                    "referenceToReferenceFlow": ref_internal_id or None,
                },
                "geography": {
                    "locationOfOperationSupplyOrProduction": _safe_str(source.get("location")) or None,
                },
            },
            "modellingAndValidation": {
                "LCIMethodAndAllocation": {
                    "typeOfDataSet": process.process_type,
                }
            },
            "exchanges": {"exchange": exchanges},
            "administrativeInformation": {
                "publicationAndOwnership": {
                    "common:dataSetVersion": version,
                    "common:permanentDataSetURI": f"nebula-lca://processes/{process.process_uuid}?version={version}",
                }
            },
        }
    }


def _process_flow_uuids(process: ReferenceProcess) -> set[str]:
    source = process.process_json if isinstance(process.process_json, dict) else {}
    flow_uuids = {_safe_str(process.reference_flow_uuid)}
    for item in _as_list(source.get("exchanges")):
        if isinstance(item, dict):
            flow_uuids.add(_safe_str(item.get("flow_uuid") or item.get("flowUuid")))
    return {item for item in flow_uuids if item}


def _missing_published_flow_uuids(db: Session, *, account: DataPlatformAccount, process: ReferenceProcess) -> list[str]:
    missing: list[str] = []
    for flow_uuid in sorted(_process_flow_uuids(process)):
        record = (
            db.query(ExternalDataSyncRecord)
            .filter(
                ExternalDataSyncRecord.account_id == account.id,
                ExternalDataSyncRecord.local_kind == "flow",
                ExternalDataSyncRecord.local_uuid == flow_uuid,
            )
            .first()
        )
        if record is None:
            missing.append(flow_uuid)
    return missing


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


def _is_lci_result_process(process: RemoteProcessDTO) -> bool:
    return "lci result" in _safe_str(process.process_type).casefold()


def _lci_vector_from_process_detail(detail: Any) -> dict[str, Any] | None:
    """Derive a local vector from an already-solved TianGong LCI Process.

    TianGong stores an LCI result as a normal TIDAS Process dataset.  It does
    not add a Nebula-specific ``vector`` property, so the elementary exchanges
    are the authoritative inventory representation.
    """
    process = detail.process
    if not _is_lci_result_process(process):
        return detail.vector if isinstance(detail.vector, dict) else None

    process_json = detail.process_json if isinstance(detail.process_json, dict) else {}
    reference_flow_uuid = _safe_str(process_json.get("reference_flow_uuid") or process.reference_flow_uuid)
    if not reference_flow_uuid:
        raise ConnectorError("TianGong LCI result is missing its quantitative reference flow.")

    flows_by_uuid = {
        flow.flow_uuid: flow
        for flow in detail.flows
        if _safe_str(flow.flow_uuid)
    }
    items: list[dict[str, Any]] = []
    for exchange in process_json.get("exchanges", []):
        if not isinstance(exchange, dict):
            continue
        flow_uuid = _safe_str(exchange.get("flow_uuid") or exchange.get("flowUuid"))
        if not flow_uuid or flow_uuid == reference_flow_uuid:
            continue
        flow = flows_by_uuid.get(flow_uuid)
        if flow is None:
            raise ConnectorError(f"TianGong LCI result flow detail is unavailable: {flow_uuid}")
        if "elementary" not in _safe_str(flow.flow_type).casefold():
            raise ConnectorError(
                f"TianGong LCI result contains a non-elementary exchange outside its reference product: {flow_uuid}"
            )
        unit = _safe_str(exchange.get("unit"))
        if not unit:
            raise ConnectorError(f"TianGong LCI result elementary exchange has no unit: {flow_uuid}")
        items.append(
            {
                "flow_uuid": flow_uuid,
                "direction": _safe_str(exchange.get("direction")).lower(),
                "unit": unit,
                "amount": exchange.get("amount"),
            }
        )
    if not items:
        raise ConnectorError("TianGong LCI result has no elementary exchanges to import.")
    return {"items": items, "remote_version": process.remote_version}


def _hiqlcd_account_or_404(db: Session, account_id: str) -> DataPlatformAccount:
    account = _account_or_404(db, account_id)
    if account.platform != "hiqlcd":
        raise HTTPException(status_code=404, detail={"code": "HIQLCD_ACCOUNT_NOT_FOUND", "message": "HiQLCD account not found."})
    return account


def _upsert_hiqlcd_flow(db: Session, flow: RemoteFlowDTO) -> None:
    if not _safe_str(flow.flow_uuid) or not _safe_str(flow.flow_name) or not _safe_str(flow.default_unit):
        raise ConnectorError("HiQLCD Flow is missing its identity, name, or unit.")
    existing = db.get(FlowRecord, flow.flow_uuid)
    if existing is not None:
        if _safe_str(existing.source).casefold() != "hiqlcd" or _safe_str(existing.default_unit) != _safe_str(flow.default_unit):
            raise ConnectorError(f"HiQLCD Flow identity conflicts with an existing local Flow: {flow.flow_uuid}")
        existing.flow_name = flow.flow_name
        existing.flow_name_en = flow.flow_name_en
        existing.flow_type = flow.flow_type
        existing.unit_group = flow.unit_group
        existing.source_updated_at = _safe_str(flow.metadata.get("updated_at")) or existing.source_updated_at
        return
    db.add(FlowRecord(
        flow_uuid=flow.flow_uuid,
        flow_name=flow.flow_name,
        flow_name_en=flow.flow_name_en,
        flow_type=flow.flow_type,
        default_unit=flow.default_unit,
        unit_group=flow.unit_group,
        compartment=_safe_str(flow.metadata.get("category")) or None,
        source_updated_at=_safe_str(flow.metadata.get("updated_at")) or None,
        source="hiqlcd",
        is_custom=False,
        tidas_compatible=False,
    ))


def _import_hiqlcd_lci(db: Session, *, account: DataPlatformAccount, detail: Any) -> HiqlcdDatasetImportResponse:
    process = detail.process
    process_json = detail.process_json if isinstance(detail.process_json, dict) else {}
    vector = detail.vector if isinstance(detail.vector, dict) else None
    reference_flow_uuid = _safe_str(process.reference_flow_uuid or process_json.get("reference_flow_uuid"))
    reference_product = _safe_str(process_json.get("reference_product"))
    if not reference_flow_uuid or not reference_product or vector is None:
        raise ConnectorError("HiQLCD LCI is missing its reference product or elementary inventory vector.")
    for flow in detail.flows:
        _upsert_hiqlcd_flow(db, flow)
    row = db.get(ReferenceProcess, process.process_uuid)
    if row is None:
        row = ReferenceProcess(
            process_uuid=process.process_uuid,
            process_name=process.process_name,
            process_name_zh=process.process_name,
            process_type="lci_dataset",
            reference_flow_uuid=reference_flow_uuid,
            process_json=process_json,
            source_file=f"hiqlcd://datasets/{process.remote_id}",
            source_process_uuid=_safe_str(process.metadata.get("dataset_uuid")) or None,
            import_mode="hiqlcd",
            import_report_json=detail.import_report if isinstance(detail.import_report, dict) else {},
        )
        db.add(row)
    else:
        if row.reference_flow_uuid and row.reference_flow_uuid != reference_flow_uuid:
            raise ConnectorError("HiQLCD dataset identity conflicts with its existing reference product.")
        row.process_name = process.process_name
        row.process_name_zh = process.process_name
        row.process_type = "lci_dataset"
        row.reference_flow_uuid = reference_flow_uuid
        row.process_json = process_json
        row.source_file = f"hiqlcd://datasets/{process.remote_id}"
        row.source_process_uuid = _safe_str(process.metadata.get("dataset_uuid")) or None
        row.import_mode = "hiqlcd"
        row.import_report_json = detail.import_report if isinstance(detail.import_report, dict) else {}
    db.flush()
    warnings: list[str] = []
    if not _upsert_lci_vector(db, account=account, process_uuid=process.process_uuid, vector_payload=vector, warnings=warnings):
        raise ConnectorError("HiQLCD LCI has no valid elementary inventory vector.")
    _upsert_sync_record(
        db,
        account=account,
        local_kind="process",
        local_uuid=process.process_uuid,
        remote_id=process.remote_id,
        remote_version=process.remote_version,
        metadata={**process.metadata, "reference_flow_uuid": reference_flow_uuid, "kind": "hiqlcd_lci_dataset"},
    )
    _upsert_sync_record(
        db,
        account=account,
        local_kind="vector",
        local_uuid=process.process_uuid,
        remote_id=process.remote_id,
        remote_version=process.remote_version,
        metadata={"kind": "hiqlcd_lci_vector", "nnz": len(vector.get("items", []))},
    )
    return HiqlcdDatasetImportResponse(
        account_id=account.id,
        process_uuid=process.process_uuid,
        reference_flow_uuid=reference_flow_uuid,
        reference_product_name=reference_product,
        vector_nnz=len(vector.get("items", [])),
        remote_dataset_id=process.remote_id,
        remote_dataset_version=process.remote_version,
    )


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
    raw: dict[str, Any]
    if isinstance(metadata, dict):
        row = metadata.get("row")
        if isinstance(row, dict):
            raw = dict(row)
        else:
            raw = {}
    else:
        raw = {}

    # Detect whether raw carries a binary JSON payload (json/json_tg/json_ordered keys).
    # When present, preserve the payload as the base, but still merge missing or empty
    # scalar fields from the fallback so that default_unit / unit_group / flow_type / name
    # are never left blank.
    has_json_payload = any(key in raw for key in ("json", "json_tg", "json_ordered"))

    if not isinstance(fallback, dict):
        return raw

    merged: dict[str, Any] = {}
    # Copy raw values first, but skip empty/None placeholders
    for key, value in raw.items():
        if value is not None and value != "":
            merged[key] = value
        elif key not in merged:
            merged[key] = value

    # Fill missing or empty raw scalar fields from fallback
    for key, value in fallback.items():
        if key not in merged:
            merged[key] = value
        elif (merged[key] is None or merged[key] == "") and value is not None and value != "":
            merged[key] = value

    # If the row carries a binary JSON payload and the fallback has scalar fields that
    # the raw row is missing, attach them under the same JSON payload key.
    if has_json_payload:
        for key in ("json", "json_tg", "json_ordered"):
            if key in raw and isinstance(raw[key], dict):
                payload = raw[key]
                for fk, fv in merged.items():
                    # Only write into the nested payload when:
                    #  - the fallback key is a known scalar field, AND
                    #  - the payload does not already have a non-empty value for it
                    if isinstance(fv, str) and fv and fk not in payload:
                        payload[fk] = fv
                break

    return merged


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
    if payload.platform == "tiangong":
        _validate_tiangong_credential(
            auth_type=payload.auth_type,
            credential=payload.credential,
            credential_required=True,
        )
    if payload.platform == "hiqlcd":
        if payload.auth_type != "api_key":
            raise HTTPException(status_code=422, detail={"code": "HIQLCD_AUTH_TYPE_INVALID", "message": "HiQLCD only accepts its API Key."})
        _validate_hiqlcd_credential(credential=payload.credential, credential_required=True)
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
    selected_auth_type = payload.auth_type or row.auth_type
    if row.platform == "tiangong" and (payload.auth_type is not None or payload.credential is not None):
        _validate_tiangong_credential(
            auth_type=selected_auth_type,
            credential=payload.credential,
            credential_required=payload.auth_type is not None and payload.auth_type != row.auth_type,
        )
    if row.platform == "hiqlcd" and (payload.auth_type is not None or payload.credential is not None):
        if selected_auth_type != "api_key":
            raise HTTPException(status_code=422, detail={"code": "HIQLCD_AUTH_TYPE_INVALID", "message": "HiQLCD only accepts its API Key."})
        _validate_hiqlcd_credential(credential=payload.credential, credential_required=False)
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


@api_router.get("/hiqlcd/accounts", response_model=list[DataPlatformAccountOut])
def list_hiqlcd_accounts(db: Session = Depends(get_db)) -> list[DataPlatformAccountOut]:
    rows = (
        db.query(DataPlatformAccount)
        .filter(DataPlatformAccount.platform == "hiqlcd")
        .order_by(DataPlatformAccount.updated_at.desc())
        .all()
    )
    return [_account_out(row) for row in rows]


@api_router.post("/hiqlcd/accounts", response_model=DataPlatformAccountOut, status_code=201)
def create_hiqlcd_account(payload: HiqlcdAccountRequest, db: Session = Depends(get_db)) -> DataPlatformAccountOut:
    if not _safe_str(payload.api_key):
        raise HTTPException(status_code=422, detail={"code": "HIQLCD_API_KEY_REQUIRED", "message": "Provide a HiQLCD API Key."})
    try:
        credential_ciphertext = encrypt_credential({"api_key": payload.api_key.strip()})
    except CredentialError as exc:
        raise _credential_config_error(exc) from exc
    row = DataPlatformAccount(
        platform="hiqlcd",
        alias=_safe_str(payload.alias) or "HiQLCD LCI",
        base_url="https://x.hiqlcd.com",
        auth_type="api_key",
        credential_ciphertext=credential_ciphertext,
        status=payload.status,
        metadata_json={"connector": "hiqlcd_lci"},
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _account_out(row)


@api_router.patch("/hiqlcd/accounts/{account_id}", response_model=DataPlatformAccountOut)
def update_hiqlcd_account(account_id: str, payload: HiqlcdAccountRequest, db: Session = Depends(get_db)) -> DataPlatformAccountOut:
    row = _hiqlcd_account_or_404(db, account_id)
    row.alias = _safe_str(payload.alias) or row.alias
    row.status = payload.status
    if _safe_str(payload.api_key):
        try:
            row.credential_ciphertext = encrypt_credential({"api_key": payload.api_key.strip()})
        except CredentialError as exc:
            raise _credential_config_error(exc) from exc
        row.last_validated_at = None
        row.last_validation_status = None
        row.last_validation_message = None
    db.commit()
    db.refresh(row)
    return _account_out(row)


@api_router.delete("/hiqlcd/accounts/{account_id}")
def delete_hiqlcd_account(account_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    row = _hiqlcd_account_or_404(db, account_id)
    db.query(DataPlatformAccountSession).filter(DataPlatformAccountSession.account_id == row.id).delete(synchronize_session=False)
    db.delete(row)
    db.commit()
    return {"deleted": True, "account_id": account_id}


@api_router.post("/hiqlcd/accounts/{account_id}/test", response_model=DataPlatformConnectionTestResponse)
def test_hiqlcd_account(account_id: str, db: Session = Depends(get_db)) -> DataPlatformConnectionTestResponse:
    return test_data_platform_account(_hiqlcd_account_or_404(db, account_id).id, db)


@api_router.get("/hiqlcd/accounts/{account_id}/datasets/search", response_model=DataPlatformSearchResponse)
def search_hiqlcd_datasets(
    account_id: str,
    q: str = Query(min_length=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    locale: str = Query(default="zh", pattern="^(zh|en)$"),
    db: Session = Depends(get_db),
) -> DataPlatformSearchResponse:
    account = _hiqlcd_account_or_404(db, account_id)
    try:
        result = connector_for_account(_account_context(account, db)).search_lci_datasets(q, page=page, page_size=page_size, locale=locale)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc
    items = [_remote_process_item(item) for item in result.items]
    for item in items:
        _write_cache(db, account=account, remote_kind="hiqlcd_lci_dataset", query_key=q, remote_id=item.remote_id, payload=item.model_dump(mode="python"))
    db.commit()
    return DataPlatformSearchResponse(
        account_id=account.id,
        platform="hiqlcd",
        query=q,
        page=result.page,
        page_size=result.page_size,
        has_more=result.has_more,
        total=result.total,
        items=items,
    )


@api_router.get("/hiqlcd/accounts/{account_id}/datasets/{dataset_id}/preview", response_model=DataPlatformRemotePreviewResponse)
def preview_hiqlcd_dataset(
    account_id: str,
    dataset_id: str,
    dataset_version: str | None = Query(default=None),
    locale: str = Query(default="zh", pattern="^(zh|en)$"),
    db: Session = Depends(get_db),
) -> DataPlatformRemotePreviewResponse:
    account = _hiqlcd_account_or_404(db, account_id)
    try:
        detail = connector_for_account(_account_context(account, db)).get_lci_detail(dataset_id, dataset_version, locale=locale)
        return _preview_from_process(account, detail)
    except ConnectorError as exc:
        raise _connector_error(exc) from exc


@api_router.post("/hiqlcd/accounts/{account_id}/datasets/import", response_model=HiqlcdDatasetImportResponse)
def import_hiqlcd_dataset(
    account_id: str,
    payload: HiqlcdDatasetImportRequest,
    db: Session = Depends(get_db),
) -> HiqlcdDatasetImportResponse:
    account = _hiqlcd_account_or_404(db, account_id)
    try:
        connector = connector_for_account(_account_context(account, db))
        detail = connector.get_lci_detail(payload.dataset_id, payload.dataset_version, locale=payload.locale)
        response = _import_hiqlcd_lci(db, account=account, detail=detail)
        db.commit()
        invalidate_management_caches(flows=True, reference_processes=True, stats=True)
        return response
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        if isinstance(exc, ConnectorError):
            raise _connector_error(exc) from exc
        raise


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
        detail = connector_for_account(_account_context(account, db)).get_process_detail(
            remote_id,
            remote_version,
            resolve_flows=False,
        )
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
    job_id = job.id
    try:
        connector = connector_for_account(_account_context(account, db))
        flow = connector.get_flow_detail(payload.remote_flow_id, payload.remote_version)
        synced, warnings, tidas_report = _import_single_flow(
            db, connector, account, flow, upsert_mode="update" if payload.overwrite else "skip", source_label=account.platform,
        )
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        report_summary = _tidas_report_summary(tidas_report)
        job.stats_json = {"remote_kind": "flow", "warnings": warnings, "synced_count": len(synced), "tidas_import": report_summary}
        db.commit()
        invalidate_management_caches(flows=True, stats=True)
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
        db.rollback()
        failed_job = _persist_failed_sync_job(
            db,
            job_id=job_id,
            account=account,
            remote_id=payload.remote_flow_id,
            remote_kind="flow",
            error=exc,
        )
        raise _sync_http_exception(exc, job_id=failed_job.id, default_code="DATA_PLATFORM_SYNC_FAILED") from exc


@api_router.post("/accounts/{account_id}/processes/sync", response_model=DataPlatformSyncProcessResponse)
def sync_remote_process(account_id: str, payload: DataPlatformSyncProcessRequest, db: Session = Depends(get_db)) -> DataPlatformSyncProcessResponse:
    account = _account_or_404(db, account_id)
    job = DataPlatformSyncJob(account_id=account.id, platform=account.platform, remote_process_id=payload.remote_process_id, status="running", phase="fetch", stats_json={"remote_kind": "process"})
    db.add(job)
    db.flush()
    job_id = job.id
    dependency_flow_uuid: str | None = None
    try:
        connector = connector_for_account(_account_context(account, db))
        detail = connector.get_process_detail(payload.remote_process_id, payload.remote_version)
        job.phase = "dependencies"
        failed_flow_uuids = (
            detail.import_report.get("failed_flow_uuids", [])
            if isinstance(detail.import_report, dict)
            else []
        )
        if failed_flow_uuids:
            dependency_flow_uuid = _safe_str(failed_flow_uuids[0])
            raise ConnectorError(f"Could not fetch referenced flow: {dependency_flow_uuid}")
        # Collect unique flow UUIDs from flows-by-reference and process exchanges
        flows_by_uuid: dict[str, RemoteFlowDTO] = {}
        for flow in detail.flows:
            if flow.flow_uuid:
                flows_by_uuid[flow.flow_uuid] = flow
        for exchange in (detail.process_json or {}).get("exchanges", []):
            if not isinstance(exchange, dict):
                continue
            flow_uuid = _safe_str(exchange.get("flow_uuid") or exchange.get("flowUuid"))
            if flow_uuid and flow_uuid not in flows_by_uuid:
                # Fetch sequentially, fail immediately on any error
                dependency_flow_uuid = flow_uuid
                fetched = connector.get_flow_detail(flow_uuid)
                flows_by_uuid[flow_uuid] = fetched
        # Import all flows atomically (no commit)
        warnings: list[str] = []
        synced: list[dict[str, Any]] = []
        for expected_flow_uuid, flow_dto in flows_by_uuid.items():
            dependency_flow_uuid = expected_flow_uuid
            flow_dto = _resolve_flow_unit_metadata_from_local_catalog(
                db,
                account=account,
                flow=flow_dto,
            )
            flow_synced, flow_warnings, flow_report = _import_single_flow(
                db,
                connector,
                account,
                flow_dto,
                upsert_mode="update" if payload.overwrite else "skip",
                source_label=account.platform,
                expected_uuid=expected_flow_uuid,
            )
            synced.extend(flow_synced)
            warnings.extend(flow_warnings)
        dependency_flow_uuid = None
        # Import process atomically (no commit)
        process = detail.process
        if not process.process_uuid:
            raise ConnectorError("Remote process is missing process_uuid")
        process_row = detail.process_json or {
            "process_uuid": process.process_uuid,
            "process_name": process.process_name,
            "reference_flow_uuid": process.reference_flow_uuid,
            "exchanges": [],
        }
        process_report = import_tidas_process_rows(
            db,
            [process_row],
            source_path=f"{account.platform}://processes/{process.remote_id}",
            upsert_mode="update" if payload.overwrite else "skip",
            valid_flow_uuids=set(flows_by_uuid),
            with_transaction=True,
        )
        if process_report.failed:
            raise ConnectorError(f"TIDAS process import failed: {process_report.errors[:3]}")
        vector_payload = _lci_vector_from_process_detail(detail)
        if vector_payload is not None:
            db.flush()
            local_process = db.get(ReferenceProcess, process.process_uuid)
            if local_process is None:
                raise ConnectorError(f"Imported TianGong process is unavailable locally: {process.process_uuid}")
            local_process.process_type = "lci_dataset"
            local_process_json = local_process.process_json if isinstance(local_process.process_json, dict) else {}
            local_process_json["process_type"] = "lci_dataset"
            local_process_json["source_dataset_type"] = process.process_type
            local_process.process_json = local_process_json
        # Upsert process and vector lineage
        synced.append(_upsert_sync_record(db, account=account, local_kind="process", local_uuid=process.process_uuid, remote_id=process.remote_id, remote_version=process.remote_version, metadata=process.metadata))
        if vector_payload and _upsert_lci_vector(db, account=account, process_uuid=process.process_uuid, vector_payload=vector_payload, warnings=warnings):
            synced.append(_upsert_sync_record(db, account=account, local_kind="vector", local_uuid=process.process_uuid, remote_id=process.remote_id, remote_version=process.remote_version, metadata={"vector": True, "source_dataset_type": process.process_type}))
        # Commit all at once
        job.status = "completed"
        job.phase = "done"
        job.finished_at = datetime.utcnow()
        report_summary = _tidas_report_summary(process_report)
        job.stats_json = {"remote_kind": "process", "flow_count": len(flows_by_uuid), "warnings": warnings, "synced_count": len(synced), "tidas_import": report_summary}
        db.commit()
        invalidate_management_caches(flows=True, reference_processes=True, stats=True)
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
        db.rollback()
        failed_job = _persist_failed_sync_job(
            db,
            job_id=job_id,
            account=account,
            remote_id=payload.remote_process_id,
            remote_kind="process",
            error=exc,
        )
        raise _sync_http_exception(
            exc,
            job_id=failed_job.id,
            default_code="DATA_PLATFORM_SYNC_FAILED",
            failed_flow_uuid=dependency_flow_uuid,
            rolled_back=True,
        ) from exc


@api_router.post("/accounts/{account_id}/models/sync", response_model=DataPlatformSyncModelResponse)
def sync_remote_model(account_id: str, payload: DataPlatformSyncModelRequest, db: Session = Depends(get_db)) -> DataPlatformSyncModelResponse:
    account = _account_or_404(db, account_id)
    job = DataPlatformSyncJob(account_id=account.id, platform=account.platform, remote_process_id=payload.remote_model_id, status="running", phase="fetch", stats_json={"remote_kind": "model"})
    db.add(job)
    db.flush()
    job_id = job.id
    warnings: list[str] = []
    synced: list[dict[str, Any]] = []
    dependency_flow_uuid: str | None = None
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
                # Fetch sequentially, fail immediately on any error
                dependency_flow_uuid = flow_uuid
                flow = connector.get_flow_detail(flow_uuid)
                flow_synced, flow_warnings, flow_report = _import_single_flow(
                    db,
                    connector,
                    account,
                    flow,
                    upsert_mode="update",
                    source_label=account.platform,
                    expected_uuid=flow_uuid,
                )
                synced.extend(flow_synced)
                warnings.extend(flow_warnings)
        dependency_flow_uuid = None
        job.phase = "upsert"
        model_row = _remote_raw_row(detail.lineage, detail.model_json or {"id": detail.model.model_uuid, "name": detail.model.model_name})
        model_report = import_tidas_model_rows(
            db,
            [model_row],
            source_path=f"{account.platform}://models/{detail.model.remote_id}",
            project_name=(payload.project_name or detail.model.model_name or detail.model.model_uuid).strip(),
            with_transaction=True,
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
        flow_count = len(_graph_flow_uuids(graph_json)) if graph_json is not None else 0
        job.stats_json = {"remote_kind": "model", "project_id": project_id, "version": version_value, "flow_count": flow_count, "warnings": warnings, "synced_count": len(synced), "tidas_import": report_summary}
        db.commit()
        return DataPlatformSyncModelResponse(
            job_id=job.id,
            account_id=account.id,
            platform=account.platform,
            status=job.status,
            project_id=project_id,
            version=version_value,
            flow_count=flow_count,
            tidas_import_job_id=model_report.job_id,
            tidas_import_report=report_summary,
            warnings=warnings,
            synced_records=synced,
        )
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        failed_job = _persist_failed_sync_job(
            db,
            job_id=job_id,
            account=account,
            remote_id=payload.remote_model_id,
            remote_kind="model",
            error=exc,
            metadata={"remote_version": payload.remote_version},
        )
        raise _sync_http_exception(
            exc,
            job_id=failed_job.id,
            default_code="DATA_PLATFORM_MODEL_SYNC_FAILED",
            failed_flow_uuid=dependency_flow_uuid,
            rolled_back=True,
        ) from exc


@api_router.post("/accounts/{account_id}/flows/{flow_uuid}/publish", response_model=DataPlatformPublishResponse)
def publish_local_flow(
    account_id: str,
    flow_uuid: str,
    payload: DataPlatformPublishRequest,
    db: Session = Depends(get_db),
) -> DataPlatformPublishResponse:
    account = _account_or_404(db, account_id)
    flow = db.get(FlowRecord, flow_uuid)
    if flow is None:
        raise HTTPException(status_code=404, detail={"code": "FLOW_NOT_FOUND", "message": f"Flow not found: {flow_uuid}"})
    try:
        connector = connector_for_account(_account_context(account, db))
        reference = connector.resolve_flow_publish_reference(_safe_str(flow.tidas_flow_property_uuid))
        _flow_publish_preflight(flow, reference)
        json_ordered = _flow_json_ordered(flow, reference)
        remote = connector.publish_flow(
            flow_uuid=flow.flow_uuid,
            json_ordered=json_ordered,
            rule_verification=payload.rule_verification,
            overwrite=payload.overwrite,
        )
        remote_version = _safe_str(remote.get("version") if isinstance(remote, dict) else None) or _tiangong_dataset_version()
        synced = _upsert_sync_record(
            db,
            account=account,
            local_kind="flow",
            local_uuid=flow.flow_uuid,
            remote_id=flow.flow_uuid,
            remote_version=remote_version,
            metadata={"direction": "publish", "scope": "personal_draft", "published_at": datetime.utcnow().isoformat()},
        )
        db.commit()
        invalidate_management_caches(flows=True, stats=True)
        return DataPlatformPublishResponse(
            account_id=account.id,
            platform=account.platform,
            local_kind="flow",
            local_uuid=flow.flow_uuid,
            remote_id=flow.flow_uuid,
            remote_version=remote_version,
            status="published",
            remote_response=remote if isinstance(remote, dict) else {"data": remote},
            synced_record=synced,
        )
    except ConnectorError as exc:
        db.rollback()
        raise _connector_error(exc) from exc


@api_router.post("/accounts/{account_id}/processes/{process_uuid}/publish", response_model=DataPlatformPublishResponse)
def publish_local_process(
    account_id: str,
    process_uuid: str,
    payload: DataPlatformPublishRequest,
    db: Session = Depends(get_db),
) -> DataPlatformPublishResponse:
    account = _account_or_404(db, account_id)
    process = db.get(ReferenceProcess, process_uuid)
    if process is None:
        raise HTTPException(status_code=404, detail={"code": "PROCESS_NOT_FOUND", "message": f"Process not found: {process_uuid}"})
    missing = _missing_published_flow_uuids(db, account=account, process=process)
    if missing:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "DATA_PLATFORM_PROCESS_DEPENDENCIES_NOT_PUBLISHED",
                "message": "Process has flow dependencies that have not been published or synced for this TianGong account.",
                "missing_flow_uuids": missing,
            },
        )
    try:
        connector = connector_for_account(_account_context(account, db))
        json_ordered = _process_json_ordered(process)
        remote = connector.publish_process(
            process_uuid=process.process_uuid,
            json_ordered=json_ordered,
            rule_verification=payload.rule_verification,
            overwrite=payload.overwrite,
        )
        remote_version = _safe_str(remote.get("version") if isinstance(remote, dict) else None) or _tiangong_dataset_version()
        synced = _upsert_sync_record(
            db,
            account=account,
            local_kind="process",
            local_uuid=process.process_uuid,
            remote_id=process.process_uuid,
            remote_version=remote_version,
            metadata={"direction": "publish", "scope": "personal_draft", "published_at": datetime.utcnow().isoformat()},
        )
        db.commit()
        invalidate_management_caches(reference_processes=True, stats=True)
        return DataPlatformPublishResponse(
            account_id=account.id,
            platform=account.platform,
            local_kind="process",
            local_uuid=process.process_uuid,
            remote_id=process.process_uuid,
            remote_version=remote_version,
            status="published",
            remote_response=remote if isinstance(remote, dict) else {"data": remote},
            synced_record=synced,
        )
    except ConnectorError as exc:
        db.rollback()
        raise _connector_error(exc) from exc


# ======================================================================
# Refresh Imports Endpoint
# ======================================================================


def _preferred_tiangong_account(db: Session) -> DataPlatformAccount:
    accounts = (
        db.query(DataPlatformAccount)
        .filter(
            DataPlatformAccount.platform == "tiangong",
            DataPlatformAccount.status == "active",
        )
        .order_by(DataPlatformAccount.updated_at.desc())
        .all()
    )
    account = next((item for item in accounts if item.last_validation_status == "ok"), None)
    account = account or (accounts[0] if accounts else None)
    if account is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "DATA_PLATFORM_TIANGONG_ACCOUNT_NOT_FOUND",
                "message": "No active TianGong account is configured.",
            },
        )
    return account


@api_router.post("/tiangong/refresh-imports", response_model=DataPlatformRefreshImportsResponse)
def refresh_tiangong_imports(
    payload: DataPlatformRefreshImportsRequest,
    db: Session = Depends(get_db),
) -> DataPlatformRefreshImportsResponse:
    """Refresh previously imported TianGong flow/process records using the active account."""
    account = _preferred_tiangong_account(db)
    return refresh_account_imports(account.id, payload, db)


@api_router.post("/tiangong/flows/{flow_uuid}/refresh", response_model=TianGongFlowRefreshResponse)
def refresh_tiangong_flow(
    flow_uuid: str,
    db: Session = Depends(get_db),
) -> TianGongFlowRefreshResponse:
    """Refresh a single TianGong flow by local flow UUID.

    - Selects the preferred validated TianGong account.
    - Accepts only an existing local flow whose exact source is `tiangong`.
    - Resolves the remote id from flow sync lineage when present, otherwise uses the local UUID.
    - Fetches the current remote version without name guessing.
    - Rejects a remote UUID that differs from the requested local UUID.
    - Uses shared single-flow synchronization helper.
    - Propagates HTTP 429 as `TIANGONG_RATE_LIMITED` with `retry_after_seconds` when provided.
    """
    account = _preferred_tiangong_account(db)
    connector = connector_for_account(_account_context(account, db))

    flow_row = db.get(FlowRecord, flow_uuid)
    if flow_row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "FLOW_NOT_FOUND",
                "message": f"Flow not found: {flow_uuid}",
            },
        )

    if flow_row.source != "tiangong":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIANGONG_REFRESH_INVALID_SOURCE",
                "message": f"Flow source is '{flow_row.source}', expected 'tiangong'. Only flows with source='tiangong' can be refreshed via this endpoint.",
            },
        )

    sync_record = (
        db.query(ExternalDataSyncRecord)
        .filter(
            ExternalDataSyncRecord.account_id == account.id,
            ExternalDataSyncRecord.local_kind == "flow",
            ExternalDataSyncRecord.local_uuid == flow_uuid,
        )
        .first()
    )

    remote_id = sync_record.remote_id if sync_record and sync_record.remote_id else flow_uuid

    try:
        remote_flow = connector.get_flow_detail(remote_id, None)
    except ConnectorError as exc:
        if exc.status_code == 429:
            retry_after = _parse_retry_after(exc)
            raise HTTPException(
                status_code=429,
                detail={
                    "code": "TIANGONG_RATE_LIMITED",
                    "message": str(exc),
                    "retry_after_seconds": retry_after,
                },
            ) from exc
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIANGONG_REMOTE_FETCH_FAILED",
                "message": str(exc),
            },
        ) from exc

    if remote_flow.flow_uuid != flow_uuid:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "TIANGONG_UUID_MISMATCH",
                "message": f"Remote flow UUID '{remote_flow.flow_uuid}' does not match requested local UUID '{flow_uuid}'.",
            },
        )

    try:
        synced, warnings, tidas_report = _import_single_flow(
            db,
            connector,
            account,
            remote_flow,
            upsert_mode="update",
            source_label=account.platform,
            expected_uuid=flow_uuid,
            lineage_remote_id=remote_id,
        )
    except ConnectorError as exc:
        db.rollback()
        if exc.status_code == 429:
            retry_after = _parse_retry_after(exc)
            raise HTTPException(
                status_code=429,
                detail={
                    "code": "TIANGONG_RATE_LIMITED",
                    "message": str(exc),
                    "retry_after_seconds": retry_after,
                },
            ) from exc
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TIANGONG_IMPORT_FAILED",
                "message": str(exc),
            },
        ) from exc

    db.commit()
    invalidate_management_caches(flows=True, stats=True)

    return TianGongFlowRefreshResponse(
        account_id=account.id,
        platform=account.platform,
        flow_uuid=remote_flow.flow_uuid,
        remote_id=remote_id,
        remote_version=remote_flow.remote_version,
        status="refreshed",
        tidas_import_job_id=tidas_report.job_id,
        tidas_import_report=_tidas_report_summary(tidas_report),
        warnings=warnings,
        synced_record=synced[-1] if synced else None,
    )


def _parse_retry_after(exc: ConnectorError) -> int | None:
    """Extract retry_after_seconds from ConnectorError if available."""
    if exc.status_code != 429:
        return None
    return exc.retry_after


def _import_single_flow(
    db: Session,
    connector: Any,
    account: DataPlatformAccount,
    flow: RemoteFlowDTO,
    *,
    upsert_mode: str = "update",
    source_label: str = "tiangong",
    expected_uuid: str | None = None,
    lineage_remote_id: str | None = None,
) -> tuple[list[dict[str, Any]], list[str], TidasImportReportResponse]:
    """Upsert dependencies, import flow via TIDAS (no commit), upsert lineage.

    Returns (synced_records, warnings, tidas_report).
    The caller is responsible for committing or rolling back.
    Raises ConnectorError on import failure.
    """
    warnings: list[str] = []
    synced: list[dict[str, Any]] = []
    if expected_uuid and flow.flow_uuid != expected_uuid:
        raise ConnectorError(
            f"Remote flow UUID '{flow.flow_uuid}' does not match expected UUID '{expected_uuid}'."
        )
    if not str(flow.default_unit or "").strip() or not str(flow.unit_group or "").strip():
        raise ConnectorError(
            f"Remote flow '{flow.flow_uuid}' has unresolved unit metadata; sync was blocked."
        )
    synced.extend(_upsert_flow_dependencies(db, account=account, connector=connector, flow=flow, warnings=warnings))

    flow_json_row = _remote_raw_row(
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
        [flow_json_row],
        source_path=f"{account.platform}://flows/{flow.remote_id}",
        upsert_mode=upsert_mode,
        source_label=source_label,
        with_transaction=True,
    )

    if tidas_report.failed:
        raise ConnectorError(f"TIDAS flow import failed: {tidas_report.errors[:3]}")

    synced.append(
        _upsert_sync_record(
            db,
            account=account,
            local_kind="flow",
            local_uuid=flow.flow_uuid,
            remote_id=lineage_remote_id or flow.remote_id,
            remote_version=flow.remote_version,
            metadata=flow.metadata,
        )
    )
    return synced, warnings, tidas_report


def _resolve_flow_unit_metadata_from_local_catalog(
    db: Session,
    *,
    account: DataPlatformAccount,
    flow: RemoteFlowDTO,
) -> RemoteFlowDTO:
    """Reuse a previously synced, same-source Flow only for missing units.

    TianGong's protected standard flow-property records are not always readable
    through the account API.  A prior TianGong Flow sync is still an auditable
    unit source for the exact same UUID, so it can complete that metadata
    without guessing a unit or accepting an unreadable Flow.
    """
    if _safe_str(flow.default_unit) and _safe_str(flow.unit_group):
        return flow
    local = db.get(FlowRecord, flow.flow_uuid)
    if local is None or _safe_str(local.source).casefold() != _safe_str(account.platform).casefold():
        return flow
    if not _safe_str(local.default_unit) or not _safe_str(local.unit_group):
        return flow
    metadata = dict(flow.metadata) if isinstance(flow.metadata, dict) else {}
    metadata["unit_metadata_source"] = "local_same_source_flow"
    return replace(
        flow,
        default_unit=local.default_unit,
        unit_group=local.unit_group,
        metadata=metadata,
    )


def _persist_failed_sync_job(
    db: Session,
    *,
    job_id: str,
    account: DataPlatformAccount,
    remote_id: str,
    remote_kind: str,
    error: Exception,
    metadata: dict[str, Any] | None = None,
) -> DataPlatformSyncJob:
    failed_job = db.get(DataPlatformSyncJob, job_id)
    if failed_job is None:
        failed_job = DataPlatformSyncJob(
            id=job_id,
            account_id=account.id,
            platform=account.platform,
            remote_process_id=remote_id,
        )
        db.add(failed_job)
    failed_job.status = "failed"
    failed_job.phase = "failed"
    failed_job.error_summary = str(error)
    failed_job.finished_at = datetime.utcnow()
    failed_job.stats_json = {
        "remote_kind": remote_kind,
        "lineage": {"remote_id": remote_id, **(metadata or {})},
    }
    db.commit()
    return failed_job


def _sync_http_exception(
    exc: Exception,
    *,
    job_id: str,
    default_code: str,
    failed_flow_uuid: str | None = None,
    rolled_back: bool = False,
) -> HTTPException:
    extra = {
        "failed_flow_uuid": failed_flow_uuid,
        "rolled_back": rolled_back,
    }
    if isinstance(exc, ConnectorError) and exc.status_code == 429:
        return HTTPException(
            status_code=429,
            detail={
                "code": "TIANGONG_RATE_LIMITED",
                "message": str(exc),
                "retry_after_seconds": exc.retry_after,
                "job_id": job_id,
                **extra,
            },
        )
    return HTTPException(
        status_code=400,
        detail={"code": default_code, "message": str(exc), "job_id": job_id, **extra},
    )


@api_router.post("/accounts/{account_id}/refresh-imports", response_model=DataPlatformRefreshImportsResponse)
def refresh_account_imports(
    account_id: str,
    payload: DataPlatformRefreshImportsRequest,
    db: Session = Depends(get_db),
) -> DataPlatformRefreshImportsResponse:
    """Refresh previously synced local_kind (flow/process) records for this account.

    Uses ExternalDataSyncRecord as the source of remote_id and remote_version,
    deduplicates by (local_kind, remote_id, remote_version), refreshes flows
    before processes, and returns a structured summary.
    """
    account = _account_or_404(db, account_id)
    connector = connector_for_account(_account_context(account, db))

    # Determine which kinds to refresh
    allowed_kinds = set(payload.kinds)
    upsert_mode = "update" if payload.overwrite else "skip"

    # Fetch all sync records for this account
    records = (
        db.query(ExternalDataSyncRecord)
        .filter(ExternalDataSyncRecord.account_id == account.id)
        .order_by(ExternalDataSyncRecord.local_kind, ExternalDataSyncRecord.remote_id)
        .all()
    )

    # Deduplicate by (local_kind, remote_id, remote_version), keeping latest synced_at
    seen: dict[tuple[str, str, str | None], ExternalDataSyncRecord] = {}
    for rec in records:
        # Skip records for kinds not requested
        if rec.local_kind not in allowed_kinds:
            continue
        key = (rec.local_kind, rec.remote_id, rec.remote_version)
        existing = seen.get(key)
        if existing is None or (rec.synced_at and existing.synced_at and rec.synced_at > existing.synced_at):
            seen[key] = rec

    deduped = list(seen.values())

    # Separate flows and processes; flows go first
    flow_records: list[ExternalDataSyncRecord] = []
    process_records: list[ExternalDataSyncRecord] = []
    for rec in deduped:
        if rec.local_kind == "flow":
            flow_records.append(rec)
        elif rec.local_kind == "process":
            process_records.append(rec)

    items: list[dict[str, Any]] = []
    total = len(flow_records) + len(process_records)
    refreshed = 0
    failed = 0
    skipped = 0

    # --- Refresh flows first ---
    for rec in flow_records:
        try:
            if not rec.remote_id:
                skipped += 1
                items.append({
                    "local_kind": rec.local_kind,
                    "remote_id": rec.remote_id or "",
                    "remote_version": rec.remote_version,
                    "status": "skipped",
                    "error": "missing remote_id",
                })
                continue
            flow_dto = connector.get_flow_detail(rec.remote_id, rec.remote_version)
            flow_row = _remote_raw_row(
                flow_dto.metadata,
                {
                    "id": flow_dto.flow_uuid or flow_dto.remote_id,
                    "name": flow_dto.flow_name,
                    "version": flow_dto.remote_version,
                    "default_unit": flow_dto.default_unit,
                    "unit_group": flow_dto.unit_group,
                    "flow_type": flow_dto.flow_type,
                    "source": account.platform,
                },
            )
            flow_report = import_tidas_flow_rows(
                db,
                [flow_row],
                source_path=f"{account.platform}://flows/{rec.remote_id}",
                upsert_mode=upsert_mode,
                source_label=account.platform,
            )
            if flow_report.failed:
                failed += 1
                items.append({
                    "local_kind": rec.local_kind,
                    "remote_id": rec.remote_id,
                    "remote_version": rec.remote_version,
                    "status": "failed",
                    "error": "; ".join(list(flow_report.errors or [])[:3]),
                })
            else:
                refreshed += 1
                # Update sync record metadata after successful refresh
                _upsert_sync_record(
                    db,
                    account=account,
                    local_kind="flow",
                    local_uuid=rec.local_uuid or flow_dto.flow_uuid or rec.remote_id,
                    remote_id=rec.remote_id,
                    remote_version=flow_dto.remote_version,
                    metadata={"last_refreshed_at": datetime.utcnow().isoformat(), "upsert_mode": upsert_mode},
                )
                items.append({
                    "local_kind": rec.local_kind,
                    "remote_id": rec.remote_id,
                    "remote_version": rec.remote_version,
                    "status": "refreshed",
                })
        except Exception as exc:  # noqa: BLE001
            failed += 1
            items.append({
                "local_kind": rec.local_kind,
                "remote_id": rec.remote_id or "",
                "remote_version": rec.remote_version,
                "status": "failed",
                "error": str(exc),
            })

    # --- Refresh processes ---
    for rec in process_records:
        try:
            if not rec.remote_id:
                skipped += 1
                items.append({
                    "local_kind": rec.local_kind,
                    "remote_id": rec.remote_id or "",
                    "remote_version": rec.remote_version,
                    "status": "skipped",
                    "error": "missing remote_id",
                })
                continue
            detail = connector.get_process_detail(rec.remote_id, rec.remote_version)
            process = detail.process
            # Sync referenced flows first
            flows_by_uuid: dict[str, RemoteFlowDTO] = {}
            for exchange in (detail.process_json or {}).get("exchanges", []):
                if not isinstance(exchange, dict):
                    continue
                flow_uuid = _safe_str(exchange.get("flow_uuid") or exchange.get("flowUuid"))
                if flow_uuid and flow_uuid not in flows_by_uuid:
                    try:
                        fetched = connector.get_flow_detail(flow_uuid)
                        flows_by_uuid[fetched.flow_uuid] = fetched
                    except Exception:  # noqa: BLE001
                        pass

            for flow_dto in flows_by_uuid.values():
                flow_row = _remote_raw_row(
                    flow_dto.metadata,
                    {
                        "id": flow_dto.flow_uuid or flow_dto.remote_id,
                        "name": flow_dto.flow_name,
                        "version": flow_dto.remote_version,
                        "default_unit": flow_dto.default_unit,
                        "unit_group": flow_dto.unit_group,
                        "flow_type": flow_dto.flow_type,
                        "source": account.platform,
                    },
                )
                flow_report = import_tidas_flow_rows(
                    db,
                    [flow_row],
                    source_path=f"{account.platform}://flows/{flow_dto.remote_id}",
                    upsert_mode=upsert_mode,
                    source_label=account.platform,
                )
                if flow_report.failed:
                    pass  # process still proceeds
            # Sync the process itself
            process_row = detail.process_json or {
                "process_uuid": process.process_uuid,
                "process_name": process.process_name,
                "reference_flow_uuid": process.reference_flow_uuid,
                "exchanges": [],
            }
            process_report = import_tidas_process_rows(
                db,
                [process_row],
                source_path=f"{account.platform}://processes/{process.remote_id}",
                upsert_mode=upsert_mode,
            )
            if process_report.failed:
                failed += 1
                items.append({
                    "local_kind": rec.local_kind,
                    "remote_id": rec.remote_id,
                    "remote_version": rec.remote_version,
                    "status": "failed",
                    "error": "; ".join(list(process_report.errors or [])[:3]),
                })
            else:
                refreshed += 1
                # Update sync record metadata after successful refresh
                _upsert_sync_record(
                    db,
                    account=account,
                    local_kind="process",
                    local_uuid=rec.local_uuid or process.process_uuid or rec.remote_id,
                    remote_id=rec.remote_id,
                    remote_version=process.remote_version,
                    metadata={"last_refreshed_at": datetime.utcnow().isoformat(), "upsert_mode": upsert_mode},
                )
                items.append({
                    "local_kind": rec.local_kind,
                    "remote_id": rec.remote_id,
                    "remote_version": rec.remote_version,
                    "status": "refreshed",
                })
        except Exception as exc:  # noqa: BLE001
            failed += 1
            items.append({
                "local_kind": rec.local_kind,
                "remote_id": rec.remote_id or "",
                "remote_version": rec.remote_version,
                "status": "failed",
                "error": str(exc),
            })

    # Commit all changes at once
    db.commit()
    invalidate_management_caches(flows=True, reference_processes=True, stats=True)

    return DataPlatformRefreshImportsResponse(
        account_id=account.id,
        platform=account.platform,
        total=total,
        refreshed=refreshed,
        failed=failed,
        skipped=skipped,
        items=[DataPlatformRefreshImportItem(**item) for item in items],
    )


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
