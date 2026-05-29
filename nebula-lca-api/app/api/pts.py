"""PTS API routes.

Extracted from ``app.main`` for Stage 6C.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import PtsCompileArtifact, PtsDefinition, PtsExternalArtifact, PtsResource
from ..schemas import (
    HybridGraph,
    PtsCompileHistoryResponse,
    PtsCompileRequest,
    PtsCompileResponse,
    PtsCompiledExternalResponse,
    PtsCompiledGetResponse,
    PtsPublishRequest,
    PtsPublishResponse,
    PtsResourceOut,
    PtsResourceUpdateRequest,
    PtsUnpackPortBinding,
    PtsPackFinalizeRequest,
    PtsPackFinalizeResponse,
    PtsUnpackRequest,
    PtsUnpackResponse,
    PtsValidateRequest,
    PtsValidateResponse,
    PtsPublishedHistoryResponse,
    PtsVersionItem,
    normalize_same_flow_uuid_opposite_direction_ports,
)
from ..services import pts_compile_service as _pcs
from ..services import pts_resources as _pr
from ..pts_compile import compile_pts
from ..pts_validate import validate_pts_compile
from ..services.graph_contract import is_graph_non_empty, normalize_graph_product_flags, validate_graph_contract

_apply_pts_resource_policy_override = _pr._apply_pts_resource_policy_override
_build_compile_graph_from_pts_resource = _pr._build_compile_graph_from_pts_resource
_build_pts_resource_out = _pr._build_pts_resource_out
_build_pts_port_id_map = _pr._build_pts_port_id_map
_build_pts_publish_warnings = _pr._build_pts_publish_warnings
_build_pts_shell_snapshot_from_external = _pr._build_pts_shell_snapshot_from_external
_build_pts_unpack_port_bindings = _pr._build_pts_unpack_port_bindings
_apply_default_visible_port_ids_to_external_payload = _pr._apply_default_visible_port_ids_to_external_payload
_apply_default_visible_port_ids_to_shell_node = _pr._apply_default_visible_port_ids_to_shell_node
_enrich_frontend_ports_flow_name_en = _pr._enrich_frontend_ports_flow_name_en
_enrich_pts_external_payload_flow_name_en = _pr._enrich_pts_external_payload_flow_name_en
_resolve_default_visible_port_ids = _pr._resolve_default_visible_port_ids
_get_or_materialize_pts_resource_row = _pr._get_or_materialize_pts_resource_row
_get_pts_resource_ports_policy = _pr._get_pts_resource_ports_policy
_load_pts_external_artifact = _pr._load_pts_external_artifact
_normalize_pts_ports_policy_from_graph = _pr._normalize_pts_ports_policy_from_graph
_raise_if_pack_finalize_obviously_reentered = _pr._raise_if_pack_finalize_obviously_reentered
_resolve_compile_row_for_publish = _pr._resolve_compile_row_for_publish
_resolve_pts_shell_snapshot_for_resource = _pr._resolve_pts_shell_snapshot_for_resource
_upsert_pts_resource_from_definition = _pr._upsert_pts_resource_from_definition
_raise_if_pts_market_has_no_internal_share = _pr._raise_if_pts_market_has_no_internal_share
_validate_pts_market_supplier_coverage = _pr._validate_pts_market_supplier_coverage
_build_frontend_ports_from_external_payload = _pr._build_frontend_ports_from_external_payload
build_pts_external_payload = _pr.build_pts_external_payload
extract_pts_definition = _pr.extract_pts_definition
upsert_pts_compile_artifact = _pr.upsert_pts_compile_artifact
upsert_pts_definition = _pr.upsert_pts_definition
upsert_pts_external_artifact = _pr.upsert_pts_external_artifact
_raise_pts_compile_value_error_http = _pcs._raise_pts_compile_value_error_http

# -- Routers ----------------------------------------------------------------

_pts_base_router = APIRouter(tags=["pts"])
_pts_api_router = APIRouter(tags=["api-pts"])

@_pts_base_router.post("/pts/validate", response_model=PtsValidateResponse)
def validate_pts(payload: PtsValidateRequest) -> PtsValidateResponse:
    validate_graph_contract(payload.graph, require_non_empty=False, allow_pts_nodes=True)
    result = validate_pts_compile(
        graph=payload.graph,
        internal_node_ids=payload.internal_node_ids,
        product_node_ids=payload.product_node_ids,
    )
    return PtsValidateResponse(
        ok=result.ok,
        errors=result.errors,
        warnings=result.warnings,
        matrix_size=result.matrix_size,
        invertible=result.invertible,
    )


@_pts_api_router.post("/api/pts/compile", response_model=PtsCompileResponse)
@_pts_base_router.post("/pts/compile", response_model=PtsCompileResponse)
def compile_pts_endpoint(payload: PtsCompileRequest, db: Session = Depends(get_db)) -> PtsCompileResponse:
    validate_graph_contract(payload.graph, require_non_empty=False, allow_pts_nodes=True)
    pts_uuid = str(payload.pts_uuid or "").strip()
    compile_graph = payload.graph
    matched_pts_nodes = [
        node
        for node in compile_graph.nodes
        if node.node_kind == "pts_module" and str(node.pts_uuid or node.process_uuid or "").strip() == pts_uuid
    ]
    if not matched_pts_nodes:
        resource = (
            db.query(PtsResource)
            .filter(PtsResource.project_id == payload.project_id, PtsResource.pts_uuid == pts_uuid)
            .first()
        )
        compile_graph = _build_compile_graph_from_pts_resource(resource) if resource is not None else None
        matched_pts_nodes = [
            node
            for node in (compile_graph.nodes if compile_graph is not None else [])
            if node.node_kind == "pts_module" and str(node.pts_uuid or node.process_uuid or "").strip() == pts_uuid
        ]
        if not matched_pts_nodes:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "PTS_NODE_NOT_FOUND_BY_UUID",
                    "message": f"No PTS node found in graph for pts_uuid={pts_uuid}",
                    "evidence": [{"pts_uuid": pts_uuid}],
                },
            )
    if len(matched_pts_nodes) > 1:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "PTS_UUID_NOT_UNIQUE_IN_GRAPH",
                "message": f"Multiple PTS nodes found in graph for pts_uuid={pts_uuid}",
                "evidence": [{"pts_uuid": pts_uuid, "matched_node_ids": [node.id for node in matched_pts_nodes]}],
            },
        )
    normalize_graph_product_flags(compile_graph)
    normalize_same_flow_uuid_opposite_direction_ports(compile_graph)
    resolved_pts_node_id = matched_pts_nodes[0].id
    ports_policy = _get_pts_resource_ports_policy(db=db, project_id=payload.project_id, pts_uuid=pts_uuid) if pts_uuid else None
    try:
        compile_result = compile_pts(compile_graph, resolved_pts_node_id, ports_policy=ports_policy, db=db)
    except ValueError as exc:
        _raise_pts_compile_value_error_http(exc, resolved_pts_node_id)
    definition = extract_pts_definition(
        graph=compile_graph,
        pts_node_id=resolved_pts_node_id,
        graph_hash=compile_result["graph_hash"],
    )
    definition = _apply_pts_resource_policy_override(
        db=db,
        project_id=payload.project_id,
        definition=definition,
    )
    definition_row = upsert_pts_definition(
        db=db,
        project_id=payload.project_id,
        definition=definition,
    )
    row, cached = upsert_pts_compile_artifact(
        db=db,
        project_id=payload.project_id,
        pts_node_id=resolved_pts_node_id,
        force_recompile=payload.force_recompile,
        compile_result=compile_result,
    )
    try:
        external_payload = build_pts_external_payload(
            project_id=payload.project_id,
            pts_uuid=definition_row.pts_uuid,
            definition=definition_row.definition_json or definition,
            compile_row=row,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "PTS_EXTERNAL_PAYLOAD_BUILD_ERROR",
                "message": str(exc),
                "evidence": [{"pts_node_id": resolved_pts_node_id, "compile_id": row.id}],
            },
        ) from exc
    return PtsCompileResponse(
        compile_id=row.id,
        project_id=row.project_id,
        pts_node_id=row.pts_node_id,
        pts_uuid=row.pts_uuid,
        graph_hash=row.graph_hash,
        compile_version=row.compile_version,
        cached=cached,
        ok=row.ok,
        errors=list(row.errors_json or []),
        warnings=list(row.warnings_json or []),
        matrix_size=row.matrix_size,
        invertible=row.invertible,
        artifact=row.artifact_json or {},
        external_preview=external_payload,
    )


@_pts_api_router.get("/api/pts/{pts_uuid}/compiled", response_model=PtsCompiledGetResponse)
@_pts_base_router.get("/pts/{pts_uuid}/compiled", response_model=PtsCompiledGetResponse)
def get_pts_compiled(pts_uuid: str, project_id: str, db: Session = Depends(get_db)) -> PtsCompiledGetResponse:
    definition = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == project_id, PtsDefinition.pts_uuid == pts_uuid)
        .first()
    )
    if definition is None:
        raise HTTPException(status_code=404, detail="PTS definition not found")

    compiled = (
        db.query(PtsCompileArtifact)
        .filter(
            PtsCompileArtifact.project_id == project_id,
            PtsCompileArtifact.pts_node_id == definition.pts_node_id,
        )
        .order_by(PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .first()
    )
    if compiled is None:
        raise HTTPException(status_code=404, detail="PTS compiled artifact not found")

    return PtsCompiledGetResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compiled.pts_node_id,
        graph_hash=compiled.graph_hash,
        compile_version=compiled.compile_version,
        ok=compiled.ok,
        errors=list(compiled.errors_json or []),
        warnings=list(compiled.warnings_json or []),
        matrix_size=compiled.matrix_size,
        invertible=compiled.invertible,
        definition=definition.definition_json or {},
        artifact=compiled.artifact_json or {},
    )


@_pts_api_router.get("/api/pts/{pts_uuid}/compiled-external", response_model=PtsCompiledExternalResponse)
@_pts_base_router.get("/pts/{pts_uuid}/compiled-external", response_model=PtsCompiledExternalResponse)
def get_pts_compiled_external(
    pts_uuid: str,
    project_id: str,
    published_version: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> PtsCompiledExternalResponse:
    effective_published_version = published_version
    if effective_published_version is None:
        resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
        if resource is not None and resource.active_published_version is not None:
            effective_published_version = int(resource.active_published_version)
    row = _load_pts_external_artifact(
        db=db,
        project_id=project_id,
        pts_uuid=pts_uuid,
        published_version=effective_published_version,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="PTS external artifact not found")
    payload = row.artifact_json if isinstance(row.artifact_json, dict) else {}
    if not payload.get("pts_uuid"):
        payload = dict(payload)
        payload["pts_uuid"] = str(payload.get("pts_id") or pts_uuid)
    payload = dict(payload)
    payload["published_version"] = int(row.published_version or 0) if row.published_version is not None else None
    payload["source_compile_id"] = str(row.source_compile_id or "") or None
    payload["source_compile_version"] = int(row.source_compile_version or 0) if row.source_compile_version is not None else None
    return PtsCompiledExternalResponse.model_validate(payload)


@_pts_api_router.get("/api/pts/{pts_uuid}", response_model=PtsResourceOut)
@_pts_base_router.get("/pts/{pts_uuid}", response_model=PtsResourceOut)
def get_pts_resource(pts_uuid: str, db: Session = Depends(get_db)) -> PtsResourceOut:
    row = _get_or_materialize_pts_resource_row(db=db, pts_uuid=pts_uuid)
    return _build_pts_resource_out(row=row, db=db)


@_pts_api_router.post("/api/pts/{pts_uuid}/unpack", response_model=PtsUnpackResponse)
@_pts_base_router.post("/pts/{pts_uuid}/unpack", response_model=PtsUnpackResponse)
def unpack_pts_resource(
    pts_uuid: str,
    payload: PtsUnpackRequest | None = None,
    db: Session = Depends(get_db),
) -> PtsUnpackResponse:
    row = _get_or_materialize_pts_resource_row(db=db, pts_uuid=pts_uuid)
    resource = _build_pts_resource_out(row=row, db=db)
    project_id = str((payload.project_id if payload else None) or resource.project_id or row.project_id or "").strip()
    pts_graph = dict(resource.pts_graph or {})
    if not is_graph_non_empty(pts_graph):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "PTS_UNPACK_GRAPH_EMPTY",
                "message": f"PTS pts_uuid={pts_uuid} has no internal graph to unpack.",
                "evidence": {
                    "pts_uuid": pts_uuid,
                    "project_id": project_id or None,
                },
            },
        )
    shell_node = _resolve_pts_shell_snapshot_for_resource(db=db, row=row)
    port_bindings = _build_pts_unpack_port_bindings(
        pts_graph=pts_graph,
        ports_policy=dict(resource.ports_policy or {}),
        shell_node=shell_node,
    )
    resolved_pts_node_id = str((payload.pts_node_id if payload else None) or resource.pts_node_id or row.pts_node_id or shell_node.get("id") or "").strip() or None
    return PtsUnpackResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        pts_node_id=resolved_pts_node_id,
        shell_node=shell_node,
        pts_graph=pts_graph,
        port_bindings=port_bindings,
        resource=resource,
    )


@_pts_api_router.post("/api/pts/{pts_uuid}/pack-finalize", response_model=PtsPackFinalizeResponse)
@_pts_base_router.post("/pts/{pts_uuid}/pack-finalize", response_model=PtsPackFinalizeResponse)
def pack_finalize_pts_resource(
    pts_uuid: str,
    payload: PtsPackFinalizeRequest,
    db: Session = Depends(get_db),
) -> PtsPackFinalizeResponse:
    _validate_pts_market_supplier_coverage(
        pts_uuid=pts_uuid,
        pts_node_id=payload.pts_node_id,
        pts_graph=dict(payload.pts_graph or {}),
    )
    derived_ports_policy = _normalize_pts_ports_policy_from_graph(
        pts_graph=dict(payload.pts_graph or {}),
        fallback_policy=None,
    )
    existing_row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    _raise_if_pack_finalize_obviously_reentered(
        pts_uuid=pts_uuid,
        payload=payload,
        derived_ports_policy=derived_ports_policy,
        existing_row=existing_row,
    )
    save_payload = PtsResourceUpdateRequest(
        project_id=payload.project_id,
        name=payload.name,
        pts_node_id=payload.pts_node_id,
        latest_graph_hash=payload.latest_graph_hash,
        pts_graph=dict(payload.pts_graph or {}),
        ports_policy=derived_ports_policy,
        shell_node=dict(payload.shell_node or {}),
    )
    put_pts_resource(pts_uuid=pts_uuid, payload=save_payload, db=db)

    compile_request = PtsCompileRequest(
        graph=HybridGraph.model_validate(
            {
                "functionalUnit": "PTS",
                "nodes": [],
                "exchanges": [],
                "metadata": {},
            }
        ),
        pts_uuid=pts_uuid,
        project_id=payload.project_id,
        force_recompile=payload.force_recompile,
    )
    compile_response = compile_pts_endpoint(payload=compile_request, db=db)

    publish_request = PtsPublishRequest(
        project_id=payload.project_id,
        compile_id=compile_response.compile_id,
        set_active=payload.set_active,
    )
    publish_response = publish_pts_artifact(pts_uuid=pts_uuid, payload=publish_request, db=db)

    row = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    external = db.query(PtsExternalArtifact).filter(PtsExternalArtifact.id == publish_response.published_artifact_id).first()
    if row is None or external is None:
        raise HTTPException(status_code=500, detail="PTS pack/finalize completed but resource refresh failed")

    shell_snapshot = _build_pts_shell_snapshot_from_external(row=row, external=external)
    port_id_map = _build_pts_port_id_map(shell_node=shell_snapshot)
    default_visible_port_ids = _resolve_default_visible_port_ids(
        shell_node=shell_snapshot,
        hints=list(payload.default_visible_port_hints or []),
    )
    if default_visible_port_ids:
        shell_snapshot = _apply_default_visible_port_ids_to_shell_node(
            shell_node=shell_snapshot,
            default_visible_port_ids=default_visible_port_ids,
        )
        external_payload = _apply_default_visible_port_ids_to_external_payload(
            payload=dict(external.artifact_json or {}),
            shell_node=shell_snapshot,
            default_visible_port_ids=default_visible_port_ids,
        )
        external.artifact_json = external_payload
    row.shell_node_json = dict(shell_snapshot)
    db.commit()
    db.refresh(row)
    if default_visible_port_ids:
        db.refresh(external)

    return PtsPackFinalizeResponse(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=str(row.pts_node_id or "") or None,
        compile_id=str(compile_response.compile_id),
        compile_version=(int(compile_response.compile_version) if compile_response.compile_version is not None else None),
        published_artifact_id=str(publish_response.published_artifact_id),
        published_version=int(publish_response.published_version),
        active_published_version=(int(publish_response.active_published_version) if publish_response.active_published_version is not None else None),
        graph_hash=str(publish_response.graph_hash),
        shell_node=shell_snapshot,
        port_id_map=port_id_map,
        default_visible_port_ids=default_visible_port_ids,
        warnings=list(publish_response.warnings or []),
    )


@_pts_api_router.put("/api/pts/{pts_uuid}", response_model=PtsResourceOut)
@_pts_base_router.put("/pts/{pts_uuid}", response_model=PtsResourceOut)
def put_pts_resource(pts_uuid: str, payload: PtsResourceUpdateRequest, db: Session = Depends(get_db)) -> PtsResourceOut:
    row = _pr.upsert_pts_resource_from_update(db=db, pts_uuid=pts_uuid, payload=payload)
    return _build_pts_resource_out(row=row, db=db)


@_pts_api_router.post("/api/pts/{pts_uuid}/publish", response_model=PtsPublishResponse)
@_pts_base_router.post("/pts/{pts_uuid}/publish", response_model=PtsPublishResponse)
def publish_pts_artifact(pts_uuid: str, payload: PtsPublishRequest, db: Session = Depends(get_db)) -> PtsPublishResponse:
    definition_row = (
        db.query(PtsDefinition)
        .filter(PtsDefinition.project_id == payload.project_id, PtsDefinition.pts_uuid == pts_uuid)
        .first()
    )
    if definition_row is None:
        raise HTTPException(status_code=404, detail="PTS definition not found")

    compile_row = _resolve_compile_row_for_publish(db=db, pts_uuid=pts_uuid, payload=payload)
    definition = dict(definition_row.definition_json or {})
    definition = _apply_pts_resource_policy_override(
        db=db,
        project_id=payload.project_id,
        definition=definition,
    )
    resource_for_validation = (
        db.query(PtsResource)
        .filter(PtsResource.project_id == payload.project_id, PtsResource.pts_uuid == pts_uuid)
        .first()
    )
    validation_graph = dict(definition.get("pts_graph") or {})
    if not is_graph_non_empty(validation_graph) and resource_for_validation is not None:
        validation_graph = dict(resource_for_validation.pts_graph_json or {})
    market_warnings = _validate_pts_market_supplier_coverage(
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        pts_graph=validation_graph,
    )
    _raise_if_pts_market_has_no_internal_share(
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        pts_graph=validation_graph,
    )
    external_payload = build_pts_external_payload(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        definition=definition,
        compile_row=compile_row,
    )
    _enrich_pts_external_payload_flow_name_en(external_payload, db=db)
    published_row = upsert_pts_external_artifact(
        db=db,
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        graph_hash=compile_row.graph_hash,
        payload=external_payload,
        source_compile_id=str(compile_row.id),
        source_compile_version=(int(compile_row.compile_version) if compile_row.compile_version is not None else None),
        set_active=payload.set_active,
    )
    resource = _upsert_pts_resource_from_definition(
        db=db,
        definition_row=definition_row,
        compile_row=compile_row,
        external_row=published_row,
    )
    resource.shell_node_json = _build_pts_shell_snapshot_from_external(row=resource, external=published_row)
    if payload.set_active:
        resource.active_published_version = int(published_row.published_version or 0) or resource.active_published_version
    db.commit()
    db.refresh(resource)
    active_version = int(resource.active_published_version) if resource and resource.active_published_version is not None else None
    publish_warnings = _build_pts_publish_warnings(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        pts_graph=dict(resource.pts_graph_json or {}) if resource is not None else {},
        external_payload=external_payload,
    )
    publish_warnings = [*market_warnings, *publish_warnings]
    return PtsPublishResponse(
        project_id=payload.project_id,
        pts_uuid=pts_uuid,
        pts_node_id=compile_row.pts_node_id,
        published_artifact_id=str(published_row.id),
        published_version=int(published_row.published_version or 0),
        source_compile_id=str(compile_row.id),
        source_compile_version=(int(compile_row.compile_version) if compile_row.compile_version is not None else None),
        graph_hash=compile_row.graph_hash,
        active_published_version=active_version,
        published_at=published_row.updated_at or published_row.created_at,
        external_preview=external_payload,
        warnings=publish_warnings,
    )


@_pts_api_router.get("/api/pts/{pts_uuid}/compile-history", response_model=PtsCompileHistoryResponse)
@_pts_base_router.get("/pts/{pts_uuid}/compile-history", response_model=PtsCompileHistoryResponse)
def get_pts_compile_history(pts_uuid: str, project_id: str, db: Session = Depends(get_db)) -> PtsCompileHistoryResponse:
    rows = (
        db.query(PtsCompileArtifact)
        .filter(PtsCompileArtifact.project_id == project_id, PtsCompileArtifact.pts_uuid == pts_uuid)
        .order_by(PtsCompileArtifact.compile_version.desc(), PtsCompileArtifact.updated_at.desc(), PtsCompileArtifact.created_at.desc())
        .all()
    )
    return PtsCompileHistoryResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        items=[
            PtsVersionItem(
                id=str(row.id),
                graph_hash=str(row.graph_hash),
                version=(int(row.compile_version) if row.compile_version is not None else None),
                created_at=row.created_at,
                updated_at=row.updated_at,
                ok=bool(row.ok),
                matrix_size=int(row.matrix_size),
                invertible=bool(row.invertible),
            )
            for row in rows
        ],
    )


@_pts_api_router.get("/api/pts/{pts_uuid}/published-history", response_model=PtsPublishedHistoryResponse)
@_pts_base_router.get("/pts/{pts_uuid}/published-history", response_model=PtsPublishedHistoryResponse)
def get_pts_published_history(pts_uuid: str, project_id: str, db: Session = Depends(get_db)) -> PtsPublishedHistoryResponse:
    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    rows = (
        db.query(PtsExternalArtifact)
        .filter(PtsExternalArtifact.project_id == project_id, PtsExternalArtifact.pts_uuid == pts_uuid)
        .order_by(PtsExternalArtifact.published_version.desc(), PtsExternalArtifact.updated_at.desc(), PtsExternalArtifact.created_at.desc())
        .all()
    )
    return PtsPublishedHistoryResponse(
        project_id=project_id,
        pts_uuid=pts_uuid,
        active_published_version=(int(resource.active_published_version) if resource and resource.active_published_version is not None else None),
        items=[
            PtsVersionItem(
                id=str(row.id),
                graph_hash=str(row.graph_hash),
                version=(int(row.published_version) if row.published_version is not None else None),
                created_at=row.created_at,
                updated_at=row.updated_at,
                source_compile_id=(str(row.source_compile_id) if row.source_compile_id else None),
                source_compile_version=(int(row.source_compile_version) if row.source_compile_version is not None else None),
            )
            for row in rows
        ],
    )


@_pts_api_router.get("/api/pts/{pts_uuid}/ports")
@_pts_base_router.get("/pts/{pts_uuid}/ports")
def get_pts_ports(
    pts_uuid: str,
    project_id: str,
    published_version: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    # Open-source contract: main-graph and PTS shell reads must always consume the
    # current active/latest published artifact. Historical published-version binding
    # is reserved for future SaaS/commercial version management and is intentionally
    # ignored here to prevent stale frontend callers from pulling old shell ports.
    resource = db.query(PtsResource).filter(PtsResource.pts_uuid == pts_uuid).first()
    effective_published_version = None
    if resource is not None and resource.active_published_version is not None:
        effective_published_version = int(resource.active_published_version)
    row = _load_pts_external_artifact(
        db=db,
        project_id=project_id,
        pts_uuid=pts_uuid,
        published_version=effective_published_version,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="PTS external artifact not found")
    payload = dict(row.artifact_json or {}) if isinstance(row.artifact_json, dict) else {}
    _enrich_pts_external_payload_flow_name_en(payload, db=db)
    frontend_ports = _build_frontend_ports_from_external_payload(payload)
    _enrich_frontend_ports_flow_name_en(frontend_ports, db=db)
    return {
        "project_id": project_id,
        "pts_uuid": pts_uuid,
        "pts_node_id": payload.get("pts_node_id"),
        "graph_hash": payload.get("graph_hash"),
        "published_version": (int(row.published_version) if row.published_version is not None else None),
        "source_compile_version": (int(row.source_compile_version) if row.source_compile_version is not None else None),
        "ports": frontend_ports,
        "output_virtual_process_bindings": payload.get("output_virtual_process_bindings", []),
        "virtual_processes": payload.get("virtual_processes", []),
    }
