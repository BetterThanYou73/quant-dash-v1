"""
Routes for pair-trading analytics — /api/pairs.

Given two tickers, compute:
  - hedge ratio (beta)  =  the slope of A regressed on B
  - spread              =  A - beta * B
  - rolling z-score     =  how many stdevs from the mean the spread is right now
  - prescriptive signal =  Long A / Short B, etc., based on z-score thresholds

These are the four numbers a pairs trader looks at every morning.
"""

from __future__ import annotations

import numpy as np
from fastapi import APIRouter, HTTPException, Query

from backend._helpers import clean_for_json, load_close_prices
from core import metrics

router = APIRouter(prefix="/api", tags=["pairs"])


@router.get("/pairs")
def get_pair(
    a: str = Query(..., description="First ticker (e.g. KO)"),
    b: str = Query(..., description="Second ticker (e.g. PEP)"),
    lookback: int = Query(252, ge=63, le=504, description="History window in trading days"),
    z_window: int = Query(30, ge=10, le=120, description="Rolling window for the z-score"),
    entry: float = Query(2.0, ge=1.0, le=4.0, description="|z| threshold to enter a trade"),
    exit_: float = Query(0.5, ge=0.1, le=2.0, alias="exit", description="|z| threshold to exit"),
):
    """Compute pair-trading analytics for two tickers."""
    a = a.upper().strip()
    b = b.upper().strip()
    if a == b:
        raise HTTPException(status_code=400, detail="Pick two different tickers")

    # Load both at once so we get a guaranteed-aligned date index.
    close = load_close_prices([a, b], lookback)
    if a not in close.columns or b not in close.columns:
        raise HTTPException(
            status_code=404,
            detail=f"One or both tickers missing in cache after filtering. Got: {list(close.columns)}",
        )

    series_a = close[a].dropna()
    series_b = close[b].dropna()

    # --- Compute the pair stats ---------------------------------------
    beta = metrics.calculate_hedge_ratio(series_a, series_b)
    spread = metrics.calculate_spread(series_a, series_b, beta)
    zscore = metrics.rolling_zscore(spread, window=z_window).dropna()

    if spread.empty or zscore.empty:
        raise HTTPException(status_code=422, detail="Not enough overlapping history to compute spread/z-score")

    current_z = float(zscore.iloc[-1])
    signal = metrics.pair_signal(current_z, entry_threshold=entry, exit_threshold=exit_)

    # --- Build the time-series payload for the chart ------------------
    # We zip date + spread + z so the frontend can plot both with one render
    # pass. Aligning on zscore.index because z requires a warmup window
    # (first `z_window-1` rows are NaN and we already dropped them).
    points = [
        {
            "date": idx.strftime("%Y-%m-%d"),
            "spread": clean_for_json(spread.loc[idx]),
            "z": clean_for_json(zscore.loc[idx]),
        }
        for idx in zscore.index
    ]

    beta_valid = isinstance(beta, (int, float, np.floating)) and np.isfinite(beta)

    return {
        "a": a,
        "b": b,
        "lookback_days": lookback,
        "z_window": z_window,
        "entry_threshold": entry,
        "exit_threshold": exit_,
        "hedge_ratio_beta": clean_for_json(beta) if beta_valid else None,
        "current_z": clean_for_json(current_z),
        "signal": signal,
        "spread_mean": clean_for_json(float(spread.mean())),
        "spread_std": clean_for_json(float(spread.std())),
        "series": points,
    }
