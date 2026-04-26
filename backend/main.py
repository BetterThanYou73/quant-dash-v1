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
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles

# We import from `core` — the pure-Python layer with no web/UI dependencies.
# This is the seam that lets us swap Streamlit out without rewriting the math.
from core import data_engine as de

# Route modules. Each one defines an APIRouter we mount below.
from backend import routes_signals
from backend import routes_quote
from backend import routes_pairs
from backend import routes_risk
from backend import routes_universe
from backend import routes_sectors
from backend import routes_regime
from backend import routes_macro
from backend import routes_news
from backend import routes_screener


# --- App instance ---------------------------------------------------------
# `title`, `version`, `description` show up in the auto-generated /docs page
# and in the OpenAPI schema. The OpenAPI schema is what Phase 2 (Claude/GPT)
# will consume to understand our endpoints, so make these meaningful.
app = FastAPI(
    title="Quant Dash API",
    version="0.1.0",
    description="REST API for the Quant Dash quantitative dashboard.",
)


# --- Startup preload ------------------------------------------------------
# On Heroku Basic the cache lives in Postgres as a ~5 MB BYTEA blob. The
# very first request that hits get_market_data() pays ~5-8 s to SELECT and
# unpickle, and any concurrent requests queue behind the same lock. With
# the dashboard firing 8+ cards in parallel on page load, the slowest
# request can blow past Heroku's 30 s H12 router timeout.
#
# Preloading at startup means the dyno is "ready to serve" *before* it
# starts accepting traffic. Heroku waits for `app[web.1] state changed
# from starting to up` before routing requests, so this naturally
# absorbs the load latency into boot time instead of user wait time.
@app.on_event("startup")
def _preload_cache():
    try:
        df, ts = de.get_market_data()
        rows = int(df.shape[0]) if df is not None and not df.empty else 0
        cols = int(df.shape[1]) if df is not None and not df.empty else 0
        print(f"[startup] cache preloaded: backend={de._cache_backend()} rows={rows} cols={cols} ts={ts}")
    except Exception as exc:
        # Don't crash the dyno if the cache isn't available yet — endpoints
        # will surface a clear 503/422 message when called.
        print(f"[startup] cache preload failed: {exc}")


# --- CORS -----------------------------------------------------------------
# Browsers block JavaScript on origin A from calling an API on origin B
# unless the API explicitly opts in via CORS headers. Our frontend will be
# served from a different origin (e.g. GitHub Pages), so we must allow it.
#
# allow_origins=["*"] is fine for a public read-only API. When we add auth
# in Phase 2, lock this down to a specific domain.
#
# In production we read from ALLOWED_ORIGIN env var (comma-separated). The
# Heroku deploy serves the frontend from the same origin via StaticFiles,
# so CORS is mostly a non-issue there — this matters when you point a
# different frontend (e.g. local dev) at the deployed API.
_allowed = os.environ.get("ALLOWED_ORIGIN", "*")
_origins = ["*"] if _allowed.strip() == "*" else [o.strip() for o in _allowed.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# GZip compresses JSON responses larger than ~1KB. Our biggest payloads
# (signals + screener + correlation) are 5–80 KB each — gzip cuts them by
# ~75-85% which matters a lot when we're hosted on a free tier with
# bandwidth caps and serving global users with high RTT.
app.add_middleware(GZipMiddleware, minimum_size=1024)


# --- Routes ---------------------------------------------------------------

# Mount the modular routers. Each call adds a group of endpoints to the app.
app.include_router(routes_signals.router)
app.include_router(routes_quote.router)
app.include_router(routes_pairs.router)
app.include_router(routes_risk.router)
app.include_router(routes_universe.router)
app.include_router(routes_sectors.router)
app.include_router(routes_regime.router)
app.include_router(routes_macro.router)
app.include_router(routes_news.router)
app.include_router(routes_screener.router)


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

    # The cache may live in Postgres (Heroku) or on local disk. Probe the
    # active backend by calling load_cached_market_data() rather than
    # stat-ing the file path \u2014 on Heroku the file path will never exist.
    data, cache_ts = de.get_market_data()

    if data.empty and not cache_path.exists():
        return {
            "cache_exists": False,
            "backend": de._cache_backend(),
            "message": "No cache yet. Run the worker: python -m core.workers --once --task=daily",
        }

    return {
        "cache_exists": True,
        "backend": de._cache_backend(),
        "last_updated_utc": cache_ts,
        "rows": int(data.shape[0]) if not data.empty else 0,
        "columns": int(data.shape[1]) if not data.empty else 0,
    }


# --- Static frontend ------------------------------------------------------
# Serve the `web/` folder at the root so the API and the UI share an
# origin. Same-origin means: no CORS preflights, cookies just work
# (Phase 2 auth), and the user sees a single URL like quantdash.tech.
#
# IMPORTANT: this mount MUST be the last route registered. StaticFiles
# matches every path under "/" so it would shadow /api/* if mounted earlier.
_WEB_DIR = Path(__file__).resolve().parents[1] / "web"
if _WEB_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(_WEB_DIR), html=True), name="web")