from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..database import get_db
from ..provider_schemas import (
    ProviderCatalogResolveRequest,
    ProviderCatalogResolveResponse,
    ProviderModelSnapshot,
    ProviderSolveRequest,
    ProviderSolveResponse,
)
from ..services.provider_v1 import (
    ProviderContractError,
    get_model_snapshot,
    resolve_catalog,
    resolve_flow_candidates,
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

    return response


@router.post("/catalog/resolve", response_model=ProviderCatalogResolveResponse)
def catalog_resolve(
    payload: ProviderCatalogResolveRequest,
    db: Session = Depends(get_db),
) -> ProviderCatalogResolveResponse:
    return ProviderCatalogResolveResponse(
        items=resolve_catalog(db, payload),
        candidate_sets=resolve_flow_candidates(db, payload),
    )
