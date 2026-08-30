from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
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
    provider_solve_runtime_fingerprint,
    solve_provider,
)
from ..services.provider_idempotency import execute_idempotent_solve


router = APIRouter(prefix="/api/provider/v1", tags=["provider-v1"])


def _raise_contract_error(exc: ProviderContractError) -> None:
    detail = {"code": exc.code, "message": exc.message}
    if exc.details:
        detail["details"] = exc.details
    headers = None
    retry_after = exc.details.get("retry_after_seconds")
    if exc.code == "IDEMPOTENCY_REQUEST_IN_PROGRESS" and retry_after is not None:
        headers = {"Retry-After": str(retry_after)}
    raise HTTPException(status_code=exc.status_code, detail=detail, headers=headers) from exc


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
def solve(
    payload: ProviderSolveRequest,
    db: Session = Depends(get_db),
) -> ProviderSolveResponse | Response:
    try:
        if payload.idempotency_key is not None:
            frozen_body = execute_idempotent_solve(
                db,
                payload,
                runtime_fingerprint_factory=lambda: provider_solve_runtime_fingerprint(
                    db,
                    payload,
                ),
                solve=lambda run_id: solve_provider(db, payload, run_id=run_id),
            )
            return Response(content=frozen_body, media_type="application/json")
        response = solve_provider(db, payload)
    except ProviderContractError as exc:
        _raise_contract_error(exc)

    return response


@router.post("/catalog/resolve", response_model=ProviderCatalogResolveResponse)
def catalog_resolve(
    payload: ProviderCatalogResolveRequest,
    db: Session = Depends(get_db),
) -> ProviderCatalogResolveResponse:
    try:
        return ProviderCatalogResolveResponse(
            items=resolve_catalog(db, payload),
            candidate_sets=resolve_flow_candidates(db, payload),
        )
    except ProviderContractError as exc:
        _raise_contract_error(exc)
