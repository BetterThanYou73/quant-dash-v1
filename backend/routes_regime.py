"""
Routes for market regime + volatility forecast — /api/regime.

Endpoint
--------
GET /api/regime?ticker=SPY&lookback=504

Returns three things for a single ticker (default SPY = "the market"):

1. **Trend regime** — a simple, robust SMA-based classifier:
       BULL  : price > SMA-50 > SMA-200   (golden-cross territory)
       BEAR  : price < SMA-50 < SMA-200   (death-cross territory)
       MIXED : anything else              (chop / transition)
   No fancy ML. Bull/bear via dual-MA is the most cited regime filter in
   trend-following literature (e.g. Faber 2007 "A Quantitative Approach
   to Tactical Asset Allocation"). It's not optimal, but it's honest:
   what the rule says is exactly what you see.

2. **Realized volatility** — annualized stdev of daily log returns over
   the last 21 trading days, expressed as a percentage. This is what
   "the stock has been moving X% per year" actually means.

3. **EWMA volatility forecast** — RiskMetrics-style exponentially
   weighted moving average with λ=0.94 (J.P. Morgan's standard since
   1996). Gives more weight to recent days; reacts to vol clusters
   without the parameter-tuning pain of a full GARCH model.

   Why not GARCH(1,1)? It's marginally better in academic backtests but
   needs the `arch` library (extra dependency, slow to fit on every
   request). EWMA captures ~80% of the value at 0% of the cost. We can
   upgrade later if the dashboard ever needs term-structure of vol.

4. **Anomaly flag** — fires when today's |return| > 3 × EWMA σ. That's
   a roughly-3-sigma move on the conditional vol scale, which is the
   threshold most desks use for "something happened today."
"""

from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from backend._helpers import clean_for_json, load_close_prices


router = APIRouter(prefix="/api", tags=["regime"])


# RiskMetrics 1996 (J.P. Morgan) lambda for daily equity returns.
# 0.94 means each day's weight = 6% of the previous, half-life ~11 days.
EWMA_LAMBDA = 0.94

# Trading days per year. Used to annualize realized + forecast vol.
TRADING_DAYS_YEAR = 252


def _classify_regime(price, sma50, sma200):
    """Classify trend regime from price vs two moving averages.

    Returns ("BULL"|"BEAR"|"MIXED", short human description).
    """
    if price is None or sma50 is None or sma200 is None:
        return "UNKNOWN", "Insufficient history (need ≥200 trading days)."
    if math.isnan(price) or math.isnan(sma50) or math.isnan(sma200):
        return "UNKNOWN", "Insufficient history (need ≥200 trading days)."

    if price > sma50 > sma200:
        return "BULL", "Price > 50-day > 200-day. Trend-followers go long."
    if price < sma50 < sma200:
        return "BEAR", "Price < 50-day < 200-day. Trend-followers stay flat or short."
    return "MIXED", "Moving averages disagree. Chop / transition zone."


def _ewma_vol(returns: pd.Series, lam: float = EWMA_LAMBDA) -> pd.Series:
    """Compute EWMA volatility series (RiskMetrics convention).

        sigma_t^2 = lambda * sigma_{t-1}^2 + (1 - lambda) * r_{t-1}^2

    Implemented via pandas .ewm with alpha = 1 - lambda. We square returns
    first, then take the running EWM mean, then sqrt. Matches the
    standard RiskMetrics formula exactly.
    """
    sq = returns.fillna(0.0) ** 2
    var = sq.ewm(alpha=(1.0 - lam), adjust=False).mean()
    return np.sqrt(var)


@router.get("/regime")
def get_regime(
    ticker: str = Query("SPY", description="Single ticker. Default SPY = 'the market'."),
    lookback: int = Query(504, ge=252, le=1500, description="Days of history to load (need ≥200 for SMA-200)"),
):
    sym = ticker.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="ticker is required")

    close_df = load_close_prices([sym], lookback)
    if sym not in close_df.columns:
        raise HTTPException(status_code=404, detail=f"{sym} not in cache")

    close = close_df[sym].dropna()
    if len(close) < 200:
        raise HTTPException(
            status_code=422,
            detail=f"{sym} has only {len(close)} cached days; need ≥200 for SMA-200."
        )

    # --- 1. Trend regime ----------------------------------------------
    sma50 = close.rolling(50).mean().iloc[-1]
    sma200 = close.rolling(200).mean().iloc[-1]
    last_price = float(close.iloc[-1])
    regime_code, regime_desc = _classify_regime(last_price, sma50, sma200)

    # --- 2 & 3. Volatility (realized + EWMA forecast) -----------------
    # Log returns are conventional for volatility; the difference vs. simple
    # returns is negligible at daily frequency but log returns are more
    # mathematically tractable (additive across time).
    log_ret = np.log(close / close.shift(1)).dropna()

    # Realized: simple stdev of last 21 days, annualized.
    realized_21 = float(log_ret.tail(21).std(ddof=1) * math.sqrt(TRADING_DAYS_YEAR))

    # EWMA forecast: today's conditional sigma, annualized.
    ewma = _ewma_vol(log_ret)
    ewma_today = float(ewma.iloc[-1] * math.sqrt(TRADING_DAYS_YEAR))

    # --- 4. Anomaly flag ----------------------------------------------
    last_ret = float(log_ret.iloc[-1])
    last_ewma_daily = float(ewma.iloc[-2]) if len(ewma) > 1 else float(ewma.iloc[-1])
    anomaly = abs(last_ret) > 3.0 * last_ewma_daily if last_ewma_daily > 0 else False

    # --- Vol series for the chart -------------------------------------
    # Send back the last ~6 months of EWMA so the frontend can plot a
    # "vol over time" line. Annualized + percentage for display.
    series_n = min(len(ewma), 126)
    ewma_series = (ewma.tail(series_n) * math.sqrt(TRADING_DAYS_YEAR) * 100).round(2)
    series_payload = [
        {"date": idx.strftime("%Y-%m-%d"), "vol_pct": clean_for_json(val)}
        for idx, val in ewma_series.items()
    ]

    return {
        "ticker": sym,
        "as_of": close.index[-1].strftime("%Y-%m-%d"),
        "price": round(last_price, 2),
        "regime": {
            "code": regime_code,
            "description": regime_desc,
            "sma_50": clean_for_json(round(float(sma50), 2)) if pd.notna(sma50) else None,
            "sma_200": clean_for_json(round(float(sma200), 2)) if pd.notna(sma200) else None,
        },
        "vol": {
            "realized_21d_annualized": round(realized_21 * 100, 2),
            "ewma_today_annualized": round(ewma_today * 100, 2),
            "lambda": EWMA_LAMBDA,
        },
        "anomaly": {
            "flagged": bool(anomaly),
            "last_return_pct": round(last_ret * 100, 3),
            "threshold_sigma": 3.0,
        },
        "ewma_series": series_payload,
        "method": (
            "Trend = SMA-50 vs SMA-200 (Faber 2007). Vol = RiskMetrics EWMA "
            f"(λ={EWMA_LAMBDA}). Anomaly = |today's return| > 3 × yesterday's EWMA σ."
        ),
    }
