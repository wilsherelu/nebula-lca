import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1 import router as v1_router

app = FastAPI(title="Nebula LCA Solver", version="0.1.0")
cors_origins = os.environ.get("NEBULA_LCA_CORS_ORIGINS", "http://localhost:8001")
allow_origins = [origin.strip() for origin in cors_origins.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(v1_router, prefix="/v1")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
