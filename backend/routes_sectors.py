"""
Routes for sector-level aggregation — /api/sectors.

Aggregates the per-ticker MFC composite scores into per-sector rollups.
This is what tells you "Information Technology is +0.3 z on average; it's
the strongest sector right now" without you having to eyeball the table.

Method
------
For each GICS sector with at least `min_tickers` constituents in the cache:
  - mean of Composite_Z       → which sectors are factor-favored
  - median Momentum_12_1       → trend health
  - median CVaR_5              → tail risk
  - count of Strong Buy + Buy  → conviction count
  - constituent count

We use **median** for the per-factor rollups because momentum/CVaR
distributions are skewed at the single-stock level; mean would be pulled
around by 1-2 outliers (looking at you, CIEN with +580% momentum).
The composite *is* averaged — z-scores are already symmetric/centered, so
the mean is appropriate there.
"""

from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from core import data_engine as de
from core import signals as sig


router = APIRouter(prefix="/api", tags=["sectors"])


BENCHMARK_TICKER = "SPY"


def _clean(v):
    """JSON-safe scalar."""
    if isinstance(v, (np.floating, float)):
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(v, (np.integer,)):
        return int(v)
    return v


@router.get("/sectors")
def get_sectors(
    min_tickers: int = Query(3, ge=2, le=20, description="Sectors with fewer constituents are dropped"),
):
    """Sector-level rollup of the MFC factor model.

    Same pipeline as /api/signals but the result is grouped by GICS sector
    and aggregated, rather than returned per-ticker.
    """
    data, cache_ts = de.load_cached_market_data()
    if data.empty:
        raise HTTPException(status_code=503, detail="No cached market data.")

    close_all = sig.extract_close_prices(data)
    vols_all = sig.extract_volumes(data)
    if close_all.empty or BENCHMARK_TICKER not in close_all.columns:
        raise HTTPException(status_code=503, detail="Cache missing benchmark or prices.")

    benchmark = close_all[BENCHMARK_TICKER].dropna()
    universe_close = close_all.drop(columns=[BENCHMARK_TICKER])
    universe_vols = vols_all.drop(columns=[BENCHMARK_TICKER], errors="ignore")

    if universe_close.shape[1] < 30:
        raise HTTPException(status_code=422, detail="Universe too small.")

    # Build full-universe scores. No watchlist filter — we want every
    # cached ticker so the per-sector aggregates are statistically meaningful.
    signal_df, _skipped = sig.build_composite_signals(
        close_prices=universe_close,
        volumes=universe_vols if not universe_vols.empty else None,
        benchmark_prices=benchmark,
        watchlist=None,
    )
    if signal_df.empty:
        raise HTTPException(status_code=422, detail="No tickers had enough history to score.")

    # Attach sector. SP500.csv is the source of truth; user-added tickers
    # without a known sector get "Unknown" and are reported separately.
    meta = de.get_ticker_metadata()
    sector_map = meta.set_index("Symbol")["Sector"].to_dict()
    for sym, info in de.read_user_meta().items():
        sector_map.setdefault(sym, info.get("sector") or "Unknown")
    signal_df["Sector"] = signal_df["Ticker"].map(sector_map).fillna("Unknown")

    # Group + aggregate. `agg` returns a DataFrame; reset_index makes it
    # iterable as records.
    grouped = signal_df.groupby("Sector").agg(
        constituent_count=("Ticker", "count"),
        avg_composite=("Composite_Z", "mean"),
        median_momentum=("Momentum_12_1", "median"),
        median_cvar=("CVaR_5", "median"),
        median_alpha=("Alpha_Annualized", "median"),
        strong_buy=("Signal", lambda s: int((s == "Strong Buy").sum())),
        buy=("Signal", lambda s: int((s == "Buy").sum())),
        avoid=("Signal", lambda s: int((s == "Avoid").sum())),
        high_risk=("Signal", lambda s: int((s == "High Risk").sum())),
    ).reset_index()

    # Drop sectors below the minimum-constituents threshold (typically just
    # filters out "Unknown" with 1-2 user-added rows). Keeping them would
    # bias the dashboard toward whatever random small sample landed there.
    grouped = grouped[grouped["constituent_count"] >= min_tickers]

    # Rank: descending by average composite. The "best" sector is on top.
    grouped = grouped.sort_values("avg_composite", ascending=False)

    rounding = {
        "avg_composite": 3,
        "median_momentum": 4,
        "median_cvar": 4,
        "median_alpha": 4,
    }
    for col, digits in rounding.items():
        grouped[col] = grouped[col].round(digits)

    rows = [
        {k: _clean(v) for k, v in row.items()}
        for row in grouped.to_dict(orient="records")
    ]

    return {
        "as_of_utc": cache_ts,
        "min_tickers": min_tickers,
        "sector_count": len(rows),
        "results": rows,
    }
