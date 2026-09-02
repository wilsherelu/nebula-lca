from __future__ import annotations

from io import BytesIO
import re

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from ..database import get_db
from ..services.native_project_bundle import NativeBundleError, export_project_bundle, import_project_bundle


router = APIRouter(prefix="/api/native-project-bundles", tags=["native-project-bundles"])


def _filename(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-.")
    return (safe or "nebula-project")[:96] + ".nebula.zip"


@router.get("/projects/{project_id}")
def download_project_bundle(project_id: str, db: Session = Depends(get_db)):
    try:
        payload, manifest = export_project_bundle(db, project_id)
    except NativeBundleError as exc:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": str(exc)}) from exc
    headers = {
        "Content-Disposition": f'attachment; filename="{_filename(manifest["project_name"])}"',
        "X-Nebula-Bundle-Schema": str(manifest["schema_version"]),
    }
    return StreamingResponse(BytesIO(payload), media_type="application/zip", headers=headers)


@router.post("/import")
async def restore_project_bundle(
    file: UploadFile = File(...),
    conflict_policy: str = Query("rename", pattern="^(rename|fail)$"),
    db: Session = Depends(get_db),
):
    payload = await file.read()
    try:
        return import_project_bundle(db, payload, conflict_policy=conflict_policy)
    except NativeBundleError as exc:
        raise HTTPException(status_code=422, detail={"code": "INVALID_NATIVE_PROJECT_BUNDLE", "message": str(exc)}) from exc
