"""TIDAS Export API routes (POST /api/export/tidas/bundle/preview, POST /api/export/tidas/bundle).

Extracted from ``app.main`` for Stage 5A. Uses ``APIRouter`` pattern;
the router is included in main.py via ``app.include_router(_base_router)``.

URL paths preserved to match the original ``@app.xxx`` registrations.
"""

from __future__ import annotations

import io

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas import (
    TidasExportPreviewRequest,
    TidasExportPreviewResponse,
    TidasExportRequest,
    TidasExportReadinessResponse,
)
from ..tidas_export import ExportError, build_tidas_readiness, export_bundle, preview_export

# ── Routers ────────────────────────────────────────────────────────────────
# Both /export and /api/export prefixes are registered so that the external
# gateway path and the internal API path coexist — exactly as the original
# @app.post decorators in main.py did.
_base_router = APIRouter(prefix="/export")
_api_router = APIRouter(prefix="/api/export")


# ── Preview routes ─────────────────────────────────────────────────────────

@_api_router.post("/tidas/bundle/preview", response_model=TidasExportPreviewResponse)
@_base_router.post("/tidas/bundle/preview", response_model=TidasExportPreviewResponse)
def preview_tidas_bundle_export(
    payload: TidasExportPreviewRequest,
    db: Session = Depends(get_db),
) -> TidasExportPreviewResponse:
    """Preview TIDAS bundle export.

    Returns export feasibility report without generating ZIP.
    Use this to check for missing flows/processes before actual export.
    """
    result = preview_export(db=db, project_id=payload.project_id, version=payload.version)
    return TidasExportPreviewResponse(**result)


# ── Bundle export routes ───────────────────────────────────────────────────

@_api_router.post("/tidas/bundle")
@_base_router.post("/tidas/bundle")
def export_tidas_bundle(
    payload: TidasExportRequest,
    db: Session = Depends(get_db),
):
    """Export project to TIDAS bundle ZIP.

    Returns a ZIP file containing:
    - manifest.json (v2 package format)
    - flows/{flow_uuid}.json
    - processes/{process_uuid}.json
    - models/{model_uuid}.json
    - export_report.json

    The exported ZIP is compatible with existing /api/import/tidas/bundle endpoint.
    """
    try:
        zip_bytes, report = export_bundle(
            db=db,
            project_id=payload.project_id,
            version=payload.version,
            display_lang=payload.display_lang,
        )
    except ExportError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "EXPORT_FAILED",
                "message": str(exc),
            },
        )

    # Generate filename
    project_name_safe = "".join(c for c in payload.project_id if c.isalnum() or c in "-_")[:50]
    version_suffix = f"_v{payload.version}" if payload.version else "_latest"
    filename = f"tidas_export_{project_name_safe}{version_suffix}.zip"

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Readiness endpoint ─────────────────────────────────────────────────────

@_api_router.post("/tidas/bundle/readiness", response_model=TidasExportReadinessResponse)
@_base_router.post("/tidas/bundle/readiness", response_model=TidasExportReadinessResponse)
def readiness_tidas_bundle_export(
    payload: TidasExportPreviewRequest,
    db: Session = Depends(get_db),
) -> TidasExportReadinessResponse:
    """Check TIDAS bundle export readiness.

    Reports blocking issues, warnings, and info separately.
    blocking prevents export; warnings remain downloadable/reportable
    and do not block.

    This is intended for UI readiness indicators and pre-flight checks
    before triggering the actual ZIP export.
    """
    result = build_tidas_readiness(db=db, project_id=payload.project_id, version=payload.version)
    return TidasExportReadinessResponse(**result)
