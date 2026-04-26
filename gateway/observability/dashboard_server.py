#!/usr/bin/env python3
"""Observability Dashboard Server — standalone FastAPI app."""

from pathlib import Path
from typing import Optional

try:
    from fastapi import FastAPI, Query
    from fastapi.responses import FileResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
except ImportError:
    print("FastAPI not installed. Run: pip install fastapi uvicorn")
    exit(1)

from gateway.observability.api import (
    api_list_traces,
    api_get_trace,
    api_stats_daily,
    api_stats_cost,
)

app = FastAPI(title="Hermes Observability Dashboard", version="1.0.0")

# Static files
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def root():
    """Serve dashboard UI."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "Dashboard UI not found"}


@app.get("/api/traces")
async def list_traces(
    limit: int = Query(20, ge=1, le=100),
    user_id: Optional[str] = None,
    status: Optional[str] = None,
):
    """List recent traces."""
    try:
        result = api_list_traces(limit=limit, user_id=user_id, status=status)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/traces/{trace_id}")
async def get_trace(trace_id: str):
    """Get trace details."""
    try:
        result = api_get_trace(trace_id)
        if result is None:
            return JSONResponse({"error": "Trace not found"}, status_code=404)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/stats/daily")
async def stats_daily(days: int = Query(7, ge=1, le=90)):
    """Get daily statistics."""
    try:
        result = api_stats_daily(days=days)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/cost")
async def stats_cost(
    days: int = Query(30, ge=1, le=365),
    group_by: Optional[str] = None,
):
    """Get cost statistics."""
    try:
        result = api_stats_cost(days=days, group_by=group_by)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Observability Dashboard...")
    print("📊 Dashboard: http://localhost:8765")
    print("📡 API: http://localhost:8765/api")
    
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
