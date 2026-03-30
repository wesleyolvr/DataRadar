"""DataRadar — FastAPI app principal."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from routers.bronze import router as bronze_router
from routers.ingest import router as ingest_router
from routers.pipeline import router as pipeline_router

app = FastAPI(
    title="DataRadar API",
    description="Insights de comunidades tech do Reddit — Pipeline Medallion",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(bronze_router)
app.include_router(ingest_router)
app.include_router(pipeline_router)


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "dataradar"}


STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
