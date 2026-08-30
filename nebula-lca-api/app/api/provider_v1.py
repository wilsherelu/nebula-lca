from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import ModelVersion
from ..provider_schemas import (
    ProviderCatalogResolveRequest,
    ProviderCatalogResolveResponse,
    ProviderModelSnapshot,
    ProviderSolveRequest,
    ProviderSolveResponse,
)
from ..schemas import RunRequest
from ..services.provider_v1 import (
    ProviderContractError,
    get_model_snapshot,
    resolve_catalog,
    solve_provider,
)


router = APIRouter(prefix="/api/provider/v1", tags=["provider-v1"])


def _raise_contract_error(exc: ProviderContractError) -> None:
    detail = {"code": exc.code, "message": exc.message}
    if exc.details:
        detail["details"] = exc.details
    raise HTTPException(status_code=exc.status_code, detail=detail) from exc


@router.get(
    "/models/{project_id}/versions/{version}/snapshot",
    response_model=ProviderModelSnapshot,
)
def model_snapshot(project_id: str, version: int, db: Session = Depends(get_db)) -> ProviderModelSnapshot:
    try:
        return get_model_snapshot(db, project_id, version)
    except ProviderContractError as exc:
        _raise_contract_error(exc)


@router.post("/solve", response_model=ProviderSolveResponse)
def solve(payload: ProviderSolveRequest, db: Session = Depends(get_db)) -> ProviderSolveResponse:
    try:
        response = solve_provider(db, payload)
    except ProviderContractError as exc:
        _raise_contract_error(exc)

    if payload.lcia_methods:
        model_version_id = None
        project_id = None
        if payload.snapshot_ref is not None:
            row = (
                db.query(ModelVersion)
                .filter(
                    ModelVersion.model_id == payload.snapshot_ref.project_id,
                    ModelVersion.version == payload.snapshot_ref.version,
                )
                .one_or_none()
            )
            model_version_id = row.id if row is not None else None
            project_id = payload.snapshot_ref.project_id
        elif payload.inline_snapshot and payload.inline_snapshot.base_snapshot_ref:
            project_id = payload.inline_snapshot.base_snapshot_ref.project_id

        # Local import avoids a module cycle while preserving the exact existing
        # /api/model/run validation, calculation, and persisted RunJob path.
        from ..main import run_model

        run_response = run_model(
            RunRequest(
                graph=(payload.inline_snapshot.graph if payload.inline_snapshot else response_graph(payload, db)),
                model_version_id=model_version_id,
                project_id=project_id,
                lcia_methods=payload.lcia_methods,
            ),
            db=db,
        )
        response.lcia = {
            "run_id": run_response.run_id,
            "status": run_response.status,
            "summary": run_response.summary,
            "lci_result": run_response.lci_result,
        }
    return response


def response_graph(payload: ProviderSolveRequest, db: Session):
    assert payload.snapshot_ref is not None
    return get_model_snapshot(db, payload.snapshot_ref.project_id, payload.snapshot_ref.version).graph


@router.post("/catalog/resolve", response_model=ProviderCatalogResolveResponse)
def catalog_resolve(
    payload: ProviderCatalogResolveRequest,
    db: Session = Depends(get_db),
) -> ProviderCatalogResolveResponse:
    return ProviderCatalogResolveResponse(items=resolve_catalog(db, payload))
