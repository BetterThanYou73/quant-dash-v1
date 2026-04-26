"""
Routes for risk analytics — /api/risk/correlation.

Returns the pairwise correlation matrix of daily returns across a watchlist,
plus an optional rolling pair correlation series. Powers the heatmap and the
"how correlated are A and B over time?" line chart.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from backend._helpers import clean_for_json, load_close_prices

router = APIRouter(prefix="/api/risk", tags=["risk"])


# Same default universe as /api/signals so behavior is consistent.
DEFAULT_WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "INTC", "AMD", "NVDA", "TSLA"]


@router.get("/correlation")
def get_correlation(
    watchlist: Optional[str] = Query(None, description="Comma-separated tickers"),
    lookback: int = Query(63, ge=21, le=504, description="Trading days for the correlation window"),
    rolling_pair_a: Optional[str] = Query(None, description="If set with rolling_pair_b, also return rolling pair corr"),
    rolling_pair_b: Optional[str] = Query(None),
    rolling_window: int = Query(60, ge=10, le=252, description="Window for the rolling pair correlation"),
):
    """Pairwise correlation matrix + optional rolling pair correlation."""
    if watchlist:
        tickers = sorted({t.strip().upper() for t in watchlist.split(",") if t.strip()})
    else:
        tickers = sorted(DEFAULT_WATCHLIST)

    # Load enough history to support BOTH the corr matrix and the rolling
    # pair series (the rolling series needs `lookback + rolling_window` rows
    # to have a full series after warmup).
    history = max(lookback, rolling_window + lookback)
    close = load_close_prices(tickers, history)

    # Returns, not prices. Correlation of prices is meaningless for stocks
    # that trend (everything correlates ~1.0 just because they all go up).
    # `fill_method=None` opts out of the deprecated forward-fill behavior.
    returns = close.pct_change(fill_method=None).dropna(axis=1, how="all").dropna(how="all")
    if returns.shape[1] < 2:
        raise HTTPException(status_code=422, detail="Need at least 2 tickers with return history")

    # --- Correlation matrix over the requested lookback ---------------
    horizon_returns = returns.tail(lookback).dropna(axis=1, how="all").dropna(how="all")
    if horizon_returns.shape[1] < 2:
        raise HTTPException(status_code=422, detail="Lookback window contains too few tickers")

    corr = horizon_returns.corr().round(4)

    # Convert the 2D matrix into a JSON-friendly dict-of-dicts.
    # Frontend can consume this as a flat heatmap or a nested object.
    corr_payload = {
        row: {col: clean_for_json(corr.at[row, col]) for col in corr.columns}
        for row in corr.index
    }

    payload = {
        "tickers": list(corr.columns),
        "lookback_days": lookback,
        "matrix": corr_payload,
    }

    # --- Optional rolling pair correlation -----------------------------
    if rolling_pair_a and rolling_pair_b:
        ra = rolling_pair_a.upper().strip()
        rb = rolling_pair_b.upper().strip()
        if ra == rb:
            raise HTTPException(status_code=400, detail="rolling_pair_a and rolling_pair_b must differ")
        if ra not in returns.columns or rb not in returns.columns:
            raise HTTPException(status_code=404, detail=f"Rolling pair tickers not in cache: {ra}, {rb}")

        pair_ret = returns[[ra, rb]].dropna(how="any")
        roll = pair_ret[ra].rolling(rolling_window).corr(pair_ret[rb]).dropna()

        if roll.empty:
            payload["rolling_pair"] = None
        else:
            payload["rolling_pair"] = {
                "a": ra,
                "b": rb,
                "window": rolling_window,
                "current": clean_for_json(float(roll.iloc[-1])),
                "avg_21d": clean_for_json(float(roll.tail(21).mean())),
                "avg_63d": clean_for_json(float(roll.tail(63).mean())),
                "series": [
                    {"date": idx.strftime("%Y-%m-%d"), "corr": clean_for_json(val)}
                    for idx, val in roll.items()
                ],
            }

    return payload
