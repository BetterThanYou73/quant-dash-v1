"""
Routes for the Money Multiplier (Phase 2E) — /api/multiplier.

Bootstrap historical daily returns for a ticker (or basket) and simulate
forward to estimate the *trading-day distribution* needed to reach an X
multiple of starting capital.

Pure numpy. ~2,000-5,000 paths, vectorized. <300ms server-side, no LLM.

Why bootstrap and not parametric:
  - Real return distributions are fat-tailed and skewed; assuming Normal
    severely understates left-tail risk. Bootstrap preserves the empirical
    distribution including the bad days.
  - We sample WITH replacement from the trailing window so the simulation
    reflects the recent regime, not 30 years of stale history.

Output:
  - p10 / p25 / p50 / p75 / p90 trading days to reach the target multiple
  - probability the path NEVER reaches it within the horizon
  - distribution histogram for plotting
  - max drawdown distribution (so user sees the cost of the upside)
"""

from __future__ import annotations

import numpy as np
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core import data_engine as de
from core import signals as sig

from backend._helpers import clean_for_json


router = APIRouter(prefix="/api", tags=["multiplier"])


class MultiplierIn(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10, description="Single ticker to bootstrap")
    target_multiple: float = Field(..., gt=1.0, le=100.0, description="Target X (e.g. 2.0 for 2x)")
    horizon_days: int = Field(1260, ge=21, le=2520, description="Max trading days to simulate (~5y default)")
    lookback_days: int = Field(504, ge=63, le=2520, description="Trailing window for bootstrap (~2y default)")
    n_paths: int = Field(3000, ge=500, le=10000, description="Monte Carlo paths")
    seed: int | None = Field(None, ge=0, le=2**31 - 1)


@router.post("/multiplier/simulate")
def simulate(body: MultiplierIn) -> dict:
    """Bootstrap-simulate a single ticker to target multiple."""
    tk = body.ticker.upper().strip()

    data, cache_ts = de.get_market_data()
    if data.empty:
        raise HTTPException(status_code=503, detail="No cached market data yet.")
    close_all = sig.extract_close_prices(data)
    if tk not in close_all.columns:
        raise HTTPException(status_code=404, detail=f"{tk} not in cached universe.")

    series = close_all[tk].dropna()
    if len(series) < 90:
        raise HTTPException(status_code=422, detail=f"{tk} has only {len(series)} days of history; need \u226590.")

    rets = series.pct_change().dropna().tail(body.lookback_days).to_numpy()
    if rets.size < 60:
        raise HTTPException(status_code=422, detail=f"Trailing window too short ({rets.size} days).")

    # --- Vectorized bootstrap ----------------------------------------
    rng = np.random.default_rng(body.seed)
    H = body.horizon_days
    N = body.n_paths
    # Sample H*N daily returns with replacement, reshape into paths.
    sampled = rng.choice(rets, size=(N, H), replace=True)
    # Cumulative growth factor along each path. start = 1.0
    paths = np.cumprod(1.0 + sampled, axis=1)

    target = float(body.target_multiple)

    # First crossing day per path; \u221e if never crossed.
    crossed = paths >= target
    any_cross = crossed.any(axis=1)
    # argmax returns 0 for all-False rows; mask those out.
    first_idx = crossed.argmax(axis=1)
    days_to_target = np.where(any_cross, first_idx + 1, -1).astype(float)

    reached_paths = days_to_target[any_cross]
    prob_reached = float(any_cross.mean())

    if reached_paths.size:
        pcts = np.percentile(reached_paths, [10, 25, 50, 75, 90])
        p10, p25, p50, p75, p90 = (float(x) for x in pcts)
        mean_days = float(reached_paths.mean())
    else:
        p10 = p25 = p50 = p75 = p90 = mean_days = None

    # --- Drawdown distribution (cost of the journey) -----------------
    # Per path: max drawdown from running peak.
    running_max = np.maximum.accumulate(paths, axis=1)
    drawdowns = paths / running_max - 1.0  # \u22640
    max_dd = drawdowns.min(axis=1)
    dd_pcts = np.percentile(max_dd, [10, 25, 50, 75, 90])
    dd10, dd25, dd50, dd75, dd90 = (float(x) for x in dd_pcts)

    # --- Histogram for chart -----------------------------------------
    # Build a histogram of days-to-target for paths that reached it.
    if reached_paths.size:
        hist_max = max(int(reached_paths.max()), int(p90 or 0), 21)
        # 24 bins is a nice round number for visual purposes.
        bin_edges = np.linspace(0, hist_max, 25)
        counts, edges = np.histogram(reached_paths, bins=bin_edges)
        histogram = {
            "bin_centers": [float((edges[i] + edges[i + 1]) / 2) for i in range(len(edges) - 1)],
            "counts": [int(c) for c in counts],
            "max_day": hist_max,
        }
    else:
        histogram = {"bin_centers": [], "counts": [], "max_day": H}

    # --- Final equity-curve percentiles for the headline chart -------
    # Down-sample to ~120 points so the JSON is small.
    step = max(1, H // 120)
    sample_idx = list(range(0, H, step))
    eq_pcts = np.percentile(paths[:, sample_idx], [10, 25, 50, 75, 90], axis=0)
    equity_curve = {
        "days": [int(i + 1) for i in sample_idx],
        "p10": [float(x) for x in eq_pcts[0]],
        "p25": [float(x) for x in eq_pcts[1]],
        "p50": [float(x) for x in eq_pcts[2]],
        "p75": [float(x) for x in eq_pcts[3]],
        "p90": [float(x) for x in eq_pcts[4]],
    }

    # --- Daily-return summary stats (so the user sees what's driving it).
    daily_mean = float(rets.mean())
    daily_std = float(rets.std(ddof=1))
    annual_mean = float((1.0 + daily_mean) ** 252 - 1.0)
    annual_std = float(daily_std * np.sqrt(252))

    return {
        "as_of_utc": cache_ts,
        "ticker": tk,
        "target_multiple": target,
        "horizon_days": H,
        "lookback_days": int(body.lookback_days),
        "n_paths": N,
        "regime": {
            "daily_mean": clean_for_json(daily_mean),
            "daily_std": clean_for_json(daily_std),
            "annual_return_est": clean_for_json(annual_mean),
            "annual_vol_est": clean_for_json(annual_std),
            "sample_size": int(rets.size),
        },
        "results": {
            "prob_reached": clean_for_json(prob_reached),
            "p10_days": clean_for_json(p10),
            "p25_days": clean_for_json(p25),
            "p50_days": clean_for_json(p50),
            "p75_days": clean_for_json(p75),
            "p90_days": clean_for_json(p90),
            "mean_days": clean_for_json(mean_days),
        },
        "drawdown": {
            "p10": clean_for_json(dd10),
            "p25": clean_for_json(dd25),
            "p50": clean_for_json(dd50),
            "p75": clean_for_json(dd75),
            "p90": clean_for_json(dd90),
        },
        "histogram": histogram,
        "equity_curve": equity_curve,
    }
