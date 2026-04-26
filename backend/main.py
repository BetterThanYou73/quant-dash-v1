"""
FastAPI entrypoint for Quant Dash v2.

Run locally with:
    uvicorn backend.main:app --reload

Then open:
    http://127.0.0.1:8000/docs   (interactive API explorer)
    http://127.0.0.1:8000/api/health
"""

from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# We import from `core` — the pure-Python layer with no web/UI dependencies.
# This is the seam that lets us swap Streamlit out without rewriting the math.
from core import data_engine as de

# Route modules. Each one defines an APIRouter we mount below.
from backend import routes_signals
from backend import routes_quote
from backend import routes_pairs
from backend import routes_risk


# --- App instance ---------------------------------------------------------
# `title`, `version`, `description` show up in the auto-generated /docs page
# and in the OpenAPI schema. The OpenAPI schema is what Phase 2 (Claude/GPT)
# will consume to understand our endpoints, so make these meaningful.
app = FastAPI(
    title="Quant Dash API",
    version="0.1.0",
    description="REST API for the Quant Dash quantitative dashboard.",
)


# --- CORS -----------------------------------------------------------------
# Browsers block JavaScript on origin A from calling an API on origin B
# unless the API explicitly opts in via CORS headers. Our frontend will be
# served from a different origin (e.g. GitHub Pages), so we must allow it.
#
# allow_origins=["*"] is fine for a public read-only API. When we add auth
# in Phase 2, lock this down to a specific domain.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# --- Routes ---------------------------------------------------------------

# Mount the modular routers. Each call adds a group of endpoints to the app.
app.include_router(routes_signals.router)
app.include_router(routes_quote.router)
app.include_router(routes_pairs.router)
app.include_router(routes_risk.router)


@app.get("/api/health")
def health():
    """Liveness probe. If this returns 200, the process is up.

    Hosting platforms (Render, Fly, etc.) ping an endpoint like this to
    decide whether to keep the container alive or restart it.
    """
    return {
        "status": "ok",
        "service": "quant-dash-api",
        "version": app.version,
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/cache-status")
def cache_status():
    """Report on the local market-data cache.

    The API does NOT fetch from Yahoo on request — that's the worker's job.
    This endpoint just inspects the pickle file the worker writes to.
    Useful for the frontend to display a "Last updated: …" badge.
    """
    cache_path: Path = de.CACHE_DATA_PATH
    exists = cache_path.exists()

    if not exists:
        return {
            "cache_exists": False,
            "message": "No cache yet. Run the worker: python -m core.workers",
        }

    # Load lazily — only when the endpoint is actually called.
    data, cache_ts = de.load_cached_market_data()
    return {
        "cache_exists": True,
        "last_updated_utc": cache_ts,
        "rows": int(data.shape[0]) if not data.empty else 0,
        "columns": int(data.shape[1]) if not data.empty else 0,
    }
