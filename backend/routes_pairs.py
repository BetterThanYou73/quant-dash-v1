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


# Curated universe of pairs traders actually watch. Kept short so the
# opportunities scan stays under ~100ms and the AI batch call stays cheap.
# Ordering = display order on the card.
_CURATED_PAIRS: list[tuple[str, str]] = [
    ("NVDA", "AMD"),
    ("AAPL", "MSFT"),
    ("META", "GOOGL"),
    ("XOM",  "CVX"),
    ("KO",   "PEP"),
    ("V",    "MA"),
    ("JPM",  "BAC"),
    ("HD",   "LOW"),
]


@router.get("/pairs/opportunities")
def get_pair_opportunities(
    lookback: int = Query(252, ge=63, le=504),
    z_window: int = Query(30, ge=10, le=120),
    entry: float = Query(2.0, ge=1.0, le=4.0),
    exit_: float = Query(0.5, ge=0.1, le=2.0, alias="exit"),
):
    """Scan a curated list of correlated pairs and return their current
    correlation, hedge ratio, z-score, and signal. Pure stats — no LLM
    call here. The frontend can hit /api/advisor/explain_pairs_batch
    afterwards to attach AI commentary on user demand.
    """
    # One bulk price load instead of N — load_close_prices reuses the
    # snapshot frame, so this is a single column-slice operation.
    tickers = sorted({t for pair in _CURATED_PAIRS for t in pair})
    close = load_close_prices(tickers, lookback)

    rows = []
    for a, b in _CURATED_PAIRS:
        if a not in close.columns or b not in close.columns:
            continue
        sa = close[a].dropna()
        sb = close[b].dropna()
        # Align on shared dates so the corr/regression numbers are honest.
        joined = sa.to_frame("a").join(sb.to_frame("b"), how="inner").dropna()
        if len(joined) < z_window + 5:
            continue

        sa2, sb2 = joined["a"], joined["b"]
        try:
            corr = float(sa2.pct_change().corr(sb2.pct_change()))
            beta = metrics.calculate_hedge_ratio(sa2, sb2)
            spread = metrics.calculate_spread(sa2, sb2, beta)
            z_series = metrics.rolling_zscore(spread, window=z_window).dropna()
            if z_series.empty:
                continue
            cz = float(z_series.iloc[-1])
            signal = metrics.pair_signal(cz, entry_threshold=entry, exit_threshold=exit_)
        except Exception:
            continue

        rows.append({
            "a": a,
            "b": b,
            "correlation": clean_for_json(corr),
            "hedge_ratio_beta": clean_for_json(beta) if np.isfinite(beta) else None,
            "current_z": clean_for_json(cz),
            "signal": signal,
        })

    # Most-actionable first: largest |z|, then highest correlation.
    rows.sort(key=lambda r: (-abs(r.get("current_z") or 0.0), -(r.get("correlation") or 0.0)))

    return {
        "lookback_days": lookback,
        "z_window": z_window,
        "entry_threshold": entry,
        "exit_threshold": exit_,
        "count": len(rows),
        "pairs": rows,
    }
