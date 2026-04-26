"""
Routes for the screener — /api/screener.

This is a thin wrapper around the same Multi-Factor Composite that powers
/api/signals, but instead of restricting to a watchlist it returns the
top-N tickers from the FULL cached universe after applying simple
filters (minimum composite z-score, signal label, sector).

Why a separate endpoint:
  - /api/signals is filtered by the user's watchlist so we don't ship
    500 rows on every dashboard render.
  - The screener panel intentionally browses the entire universe to
    surface ideas the user hasn't watchlisted yet.
  - Filters live server-side so the UI never has to download all 500
    rows and toss most of them.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from core import data_engine as de
from core import signals as sig

from backend._helpers import clean_for_json


router = APIRouter(prefix="/api", tags=["screener"])

BENCHMARK_TICKER = "SPY"

# Categorical signal labels we accept in the `signal` filter.
ALLOWED_SIGNALS = {"STRONG_BUY", "BUY", "HOLD", "AVOID", "HIGH_RISK"}


@router.get("/screener")
def screen(
    min_z: float = Query(0.0, description="Minimum composite z-score (filter)"),
    signal: Optional[str] = Query(
        None,
        description="Comma-separated signal labels to keep (STRONG_BUY,BUY,HOLD,AVOID,HIGH_RISK).",
    ),
    sector: Optional[str] = Query(None, description="Exact GICS sector name."),
    limit: int = Query(50, ge=1, le=200, description="Max rows to return after filtering."),
):
    """Return the top-N MFC results from the full universe, filtered."""
    # Load cached panel + benchmark.
    data, cache_ts = de.get_market_data()
    if data.empty:
        raise HTTPException(status_code=503, detail="No cached market data. Run worker.")

    close_all = sig.extract_close_prices(data)
    vols_all = sig.extract_volumes(data)
    if BENCHMARK_TICKER not in close_all.columns:
        raise HTTPException(status_code=503, detail=f"Benchmark {BENCHMARK_TICKER} missing.")

    benchmark = close_all[BENCHMARK_TICKER].dropna()
    universe_close = close_all.drop(columns=[BENCHMARK_TICKER])
    universe_vols = vols_all.drop(columns=[BENCHMARK_TICKER], errors="ignore")

    # Score the entire universe by passing watchlist=all-tickers.
    full_list = sorted(universe_close.columns)
    df, _ = sig.build_composite_signals(
        close_prices=universe_close,
        volumes=universe_vols if not universe_vols.empty else None,
        benchmark_prices=benchmark,
        watchlist=full_list,
    )
    if df.empty:
        raise HTTPException(status_code=422, detail="No tickers had enough history.")

    # Enrich with sector/name (same pattern as /api/signals).
    meta = de.get_ticker_metadata()
    sector_map = meta.set_index("Symbol")["Sector"].to_dict()
    name_map = meta.set_index("Symbol")["Name"].to_dict() if "Name" in meta.columns else {}
    for sym, info in de.read_user_meta().items():
        sector_map.setdefault(sym, info.get("sector") or "Unknown")
        name_map.setdefault(sym, info.get("name") or sym)
    df["Sector"] = df["Ticker"].map(sector_map).fillna("Unknown")
    df["Name"] = df["Ticker"].map(name_map).fillna(df["Ticker"])

    # --- Apply filters ----------------------------------------------------
    if min_z is not None:
        df = df[df["Composite_Z"] >= min_z]
    if signal:
        wanted = {s.strip().upper() for s in signal.split(",") if s.strip()}
        unknown = wanted - ALLOWED_SIGNALS
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown signal label(s): {sorted(unknown)}. Allowed: {sorted(ALLOWED_SIGNALS)}",
            )
        # The signal column uses title-case with spaces ("Strong Buy"); the
        # filter accepts the underscored UPPER form ("STRONG_BUY"). Normalize
        # the column the same way before comparing.
        normalized = df["Signal"].str.upper().str.replace(" ", "_", regex=False)
        df = df[normalized.isin(wanted)]
    if sector:
        df = df[df["Sector"].str.lower() == sector.strip().lower()]

    df = df.sort_values("Composite_Z", ascending=False).head(limit)

    # Trim columns + round for compact payload.
    keep_cols = [
        "Ticker", "Name", "Sector", "Price",
        "Composite_Z", "Composite_Percentile", "Signal",
        "Momentum_12_1", "Sortino", "Alpha_Annualized", "CVaR_5",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    rounding = {
        "Price": 2, "Composite_Z": 3, "Composite_Percentile": 1,
        "Momentum_12_1": 4, "Sortino": 3, "Alpha_Annualized": 4, "CVaR_5": 4,
    }
    for col, d in rounding.items():
        if col in df.columns:
            df[col] = df[col].round(d)

    rows = [{k: clean_for_json(v) for k, v in r.items()} for r in df.to_dict(orient="records")]

    return {
        "as_of_utc": cache_ts,
        "filters": {"min_z": min_z, "signal": signal, "sector": sector, "limit": limit},
        "count": len(rows),
        "results": rows,
    }
