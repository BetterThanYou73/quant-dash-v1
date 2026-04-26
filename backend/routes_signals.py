"""
Routes for signal generation — /api/signals.

This file owns the HTTP layer only:
  - parses query parameters
  - loads cached prices
  - calls into core.signals (the pure-logic layer)
  - shapes the result into JSON
"""

from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from core import data_engine as de
from core import signals as sig


# APIRouter is FastAPI's mini-app for grouping related routes.
# We attach it to the main app in backend/main.py.
router = APIRouter(prefix="/api", tags=["signals"])


# Default tickers if the caller doesn't pass a watchlist.
# Matches the Streamlit DEFAULT_WATCHLIST so behavior is consistent.
DEFAULT_WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "INTC", "AMD", "NVDA", "TSLA", "SIEN"]


def _clean_for_json(value):
    """JSON has no concept of NaN/Inf — these would crash the response.

    pandas + numpy happily produce NaN/±Inf from missing data or zero
    division. We coerce them to None so the JSON response is valid.
    """
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    if isinstance(value, (np.floating,)):
        f = float(value)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(value, (np.integer,)):
        return int(value)
    return value


@router.get("/signals")
def get_signals(
    lookback: int = Query(126, ge=63, le=504, description="Trading days of history to use"),
    watchlist: Optional[str] = Query(
        None,
        description="Comma-separated tickers, e.g. 'AAPL,MSFT,NVDA'. Defaults to a fixed mega-cap list.",
    ),
):
    """Ranked signal table for the requested watchlist.

    The response shape is intentionally flat (a list of row objects) so the
    frontend can drop it straight into a table component.
    """
    # --- Parse watchlist -----------------------------------------------
    if watchlist:
        # Split, strip whitespace, uppercase, drop empties, dedupe.
        tickers = sorted({t.strip().upper() for t in watchlist.split(",") if t.strip()})
    else:
        tickers = sorted(DEFAULT_WATCHLIST)

    if not tickers:
        raise HTTPException(status_code=400, detail="watchlist resolved to zero tickers")

    # --- Load cache ----------------------------------------------------
    data, cache_ts = de.load_cached_market_data()
    if data.empty:
        # The API never fetches from Yahoo on demand — that's the worker's job.
        # Returning 503 (Service Unavailable) tells the client to retry later.
        raise HTTPException(
            status_code=503,
            detail="No cached market data. Run the worker first: python -m core.workers",
        )

    close_prices = sig.extract_close_prices(data)
    if close_prices.empty:
        raise HTTPException(status_code=500, detail="Cache present but contains no Close prices")

    # Filter to requested tickers and trim to the requested lookback window.
    close_prices = close_prices.reindex(columns=[t for t in tickers if t in close_prices.columns])
    close_prices = close_prices.dropna(axis=1, how="all").tail(lookback)

    if close_prices.shape[1] < 2:
        raise HTTPException(
            status_code=422,
            detail=f"Need at least 2 valid tickers in cache. Got: {list(close_prices.columns)}",
        )

    # --- Run the brain -------------------------------------------------
    summary_df, skipped = sig.build_summary(close_prices)
    if summary_df.empty:
        raise HTTPException(status_code=422, detail="No tickers had enough history to score")

    # --- Enrich with sector + company name -----------------------------
    # Pull from the built-in SP500.csv. For tickers added by the user that
    # aren't in SP500 (e.g. POET), fall back to the user_meta sidecar
    # populated by /api/cache/ensure. If still unknown, default gracefully.
    meta = de.get_ticker_metadata()
    sector_map = meta.set_index("Symbol")["Sector"].to_dict()
    name_map = meta.set_index("Symbol")["Name"].to_dict()

    user_meta = de.read_user_meta()
    for sym, info in user_meta.items():
        sector_map.setdefault(sym, info.get("sector") or "Unknown")
        name_map.setdefault(sym, info.get("name") or sym)

    summary_df["Sector"] = summary_df["Ticker"].map(sector_map).fillna("Unknown")
    summary_df["Name"] = summary_df["Ticker"].map(name_map).fillna(summary_df["Ticker"])

    # --- Shape for JSON ------------------------------------------------
    # Convert each row to a dict and clean NaN/Inf so the response validates.
    rows = [
        {k: _clean_for_json(v) for k, v in row.items()}
        for row in summary_df.to_dict(orient="records")
    ]

    return {
        "as_of_utc": cache_ts,
        "lookback_days": lookback,
        "requested_tickers": tickers,
        "scored_count": len(rows),
        "skipped_tickers": sorted(set(skipped)),
        "results": rows,
    }
