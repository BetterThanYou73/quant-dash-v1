"""
Routes for signal generation — /api/signals.

This file owns the HTTP layer only:
  - parses query parameters
  - loads cached prices + volumes + benchmark (SPY)
  - calls into core.signals.build_composite_signals() (the pure-logic layer)
  - shapes the result into JSON

The composite is computed against the FULL cached universe (typically the
S&P 500). The optional `watchlist` query param filters the *output* — but
the per-factor z-scores are universe-wide, so a stock's score is identical
whether or not it appears in someone's personal watchlist.
"""

from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from core import data_engine as de
from core import signals as sig


router = APIRouter(prefix="/api", tags=["signals"])


# Default tickers if the caller doesn't pass a watchlist.
DEFAULT_WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "INTC", "AMD", "NVDA", "TSLA"]

# Benchmark used by the alpha factor. Excluded from signal output (we don't
# rate the benchmark against itself).
BENCHMARK_TICKER = "SPY"


def _clean_for_json(value):
    """JSON has no concept of NaN/Inf — these would crash the response."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    if isinstance(value, (np.floating,)):
        f = float(value)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value


@router.get("/signals")
def get_signals(
    watchlist: Optional[str] = Query(
        None,
        description="Comma-separated tickers to filter results (e.g. 'AAPL,MSFT,NVDA'). "
                    "If omitted, returns scores for the default watchlist. "
                    "Ranks are computed over the FULL cached universe regardless.",
    ),
):
    """Multi-Factor Composite signal table.

    Each row = one ticker with its raw factor values, factor z-scores,
    composite score, percentile rank, and categorical signal label.

    The model: equal-weighted z-score composite of four academic factors
    (momentum 12-1, Sortino, alpha vs SPY, CVaR). See core/factors.py and
    core/signals.py for the full method, citations, and label rules.
    """
    # --- Parse watchlist filter ---------------------------------------
    if watchlist:
        watchlist_filter = sorted({t.strip().upper() for t in watchlist.split(",") if t.strip()})
    else:
        watchlist_filter = sorted(DEFAULT_WATCHLIST)

    if not watchlist_filter:
        raise HTTPException(status_code=400, detail="watchlist resolved to zero tickers")

    # --- Load cache ---------------------------------------------------
    data, cache_ts = de.load_cached_market_data()
    if data.empty:
        raise HTTPException(
            status_code=503,
            detail="No cached market data. Run the worker first: python -m core.workers",
        )

    close_all = sig.extract_close_prices(data)
    vols_all = sig.extract_volumes(data)

    if close_all.empty:
        raise HTTPException(status_code=500, detail="Cache present but contains no Close prices")

    # --- Pull benchmark out of the universe before scoring ------------
    if BENCHMARK_TICKER not in close_all.columns:
        raise HTTPException(
            status_code=503,
            detail=f"Benchmark {BENCHMARK_TICKER} missing from cache. The worker must include it.",
        )
    benchmark_prices = close_all[BENCHMARK_TICKER].dropna()
    universe_close = close_all.drop(columns=[BENCHMARK_TICKER])
    universe_vols = vols_all.drop(columns=[BENCHMARK_TICKER], errors="ignore")

    if universe_close.shape[1] < 30:
        # The composite is statistically meaningless with a tiny universe.
        # 30 tickers is the absolute floor — z-scores below that are noise.
        raise HTTPException(
            status_code=422,
            detail=f"Universe too small for cross-sectional ranking. Got {universe_close.shape[1]} tickers; "
                   f"need at least 30. Worker may still be populating.",
        )

    # --- Run the model on the FULL universe ---------------------------
    signal_df, skipped = sig.build_composite_signals(
        close_prices=universe_close,
        volumes=universe_vols if not universe_vols.empty else None,
        benchmark_prices=benchmark_prices,
        watchlist=watchlist_filter,
    )

    if signal_df.empty:
        raise HTTPException(
            status_code=422,
            detail="No tickers from the watchlist had enough history to score. "
                   "They may not be in the cache yet, or the worker hasn't fetched 1+ year of data.",
        )

    # --- Enrich with sector + name -----------------------------------
    meta = de.get_ticker_metadata()
    sector_map = meta.set_index("Symbol")["Sector"].to_dict()
    name_map = meta.set_index("Symbol")["Name"].to_dict() if "Name" in meta.columns else {}

    signal_df["Sector"] = signal_df["Ticker"].map(sector_map).fillna("Unknown")
    signal_df["Name"] = signal_df["Ticker"].map(name_map).fillna(signal_df["Ticker"])

    # --- Round and shape for JSON -------------------------------------
    # Round at the boundary so the JSON is small and the UI doesn't have to
    # re-round on every render. Keep enough precision to be honest about
    # the underlying scale.
    rounding = {
        "Price": 2,
        "Momentum_12_1": 4,
        "Sortino": 3,
        "Alpha_Annualized": 4,
        "Beta": 3,
        "CVaR_5": 4,
        "Max_Drawdown_252d": 4,
        "Downside_Dev_126d": 4,
        "Avg_Dollar_Vol_21d": 0,
        "z_momentum": 3,
        "z_sortino": 3,
        "z_alpha": 3,
        "z_cvar": 3,
        "Composite_Z": 3,
        "Composite_Percentile": 1,
    }
    for col, digits in rounding.items():
        if col in signal_df.columns:
            signal_df[col] = signal_df[col].round(digits)

    rows = [
        {k: _clean_for_json(v) for k, v in row.items()}
        for row in signal_df.to_dict(orient="records")
    ]

    return {
        "as_of_utc": cache_ts,
        "model": "MFC v1 (Multi-Factor Composite, equal-weighted z-scores)",
        "factors": ["Momentum 12-1", "Sortino", "Alpha vs SPY (126d)", "CVaR 5% (252d)"],
        "universe_size": int(universe_close.shape[1]),
        "scored_count": len(rows),
        "skipped_count": len(skipped),
        "watchlist": watchlist_filter,
        "results": rows,
        "disclosure": (
            "Composite reflects four academic factors computed on the cached universe. "
            "Factor investing has historical evidence at the portfolio level over multi-year "
            "horizons. Single-stock signals are noisy. This is research analysis, not investment "
            "advice. Past performance does not predict future returns."
        ),
    }
