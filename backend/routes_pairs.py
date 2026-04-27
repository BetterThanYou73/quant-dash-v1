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
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional

from backend._helpers import clean_for_json, load_close_prices
from core import data_engine as de
from core import signals as sig
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


# Default scan universe — large-cap liquid names across sectors. Big enough
# to surface real opportunities, small enough that C(N,2) stays cheap
# (~25 names = 300 pairs, vectorized corr is ~10ms).
_BULK_UNIVERSE: list[str] = [
    # Tech mega-caps
    "AAPL", "MSFT", "GOOGL", "META", "AMZN", "NVDA", "AMD", "INTC",
    "AVGO", "ORCL", "CRM", "ADBE",
    # Financials
    "JPM", "BAC", "GS", "MS", "WFC", "C",
    # Energy
    "XOM", "CVX", "COP", "SLB",
    # Consumer / retail
    "KO", "PEP", "WMT", "COST", "HD", "LOW", "MCD", "SBUX",
    # Payments
    "V", "MA", "PYPL",
    # Healthcare
    "JNJ", "PFE", "MRK", "LLY", "ABBV", "UNH",
    # Auto / industrials
    "TSLA", "F", "GM", "BA", "CAT",
]


class BulkScanIn(BaseModel):
    tickers: Optional[list[str]] = Field(
        None,
        description="Custom ticker list. Empty/null → use the default large-cap universe.",
        max_length=60,
    )
    lookback: int = Field(252, ge=63, le=504)
    z_window: int = Field(30, ge=10, le=120)
    entry: float = Field(2.0, ge=1.0, le=4.0)
    exit_: float = Field(0.5, ge=0.1, le=2.0, alias="exit")
    min_correlation: float = Field(0.60, ge=0.0, le=0.99,
        description="Pre-filter: only run the z-score on pairs whose return correlation \u2265 this. Cuts compute and noise.")
    max_results: int = Field(20, ge=1, le=50)

    class Config:
        populate_by_name = True


@router.post("/pairs/scan")
def scan_pairs(body: BulkScanIn) -> dict:
    """Bulk pair scanner.

    Two modes, same endpoint:
      - Default (no tickers): scan the curated large-cap universe.
      - Custom: pass `tickers` to scan only your list (e.g. your watchlist
        or a sector slice).

    Filters out neutral/exit/monitor signals — returns only pairs with an
    active LONG/SHORT setup so the user can see actionable opportunities
    only. Click-through on a row should call the existing /api/pairs?a=&b=
    endpoint to get the full chart + analysis.
    """
    raw = body.tickers if body.tickers else _BULK_UNIVERSE
    # Normalize, dedupe, validate ticker shape (defense in depth).
    seen = set()
    universe: list[str] = []
    for t in raw:
        if not isinstance(t, str):
            continue
        u = t.strip().upper()
        if not u or len(u) > 10 or u in seen:
            continue
        # Allow letters, digits, dot, hyphen — same shape as our watchlist regex.
        if not all(ch.isalnum() or ch in ".-" for ch in u):
            continue
        seen.add(u)
        universe.append(u)

    if len(universe) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 valid tickers to scan for pairs.")

    # Load prices. Use the cache directly so we can report partial coverage
    # instead of 404'ing on a single missing ticker.
    data, _ts = de.load_cached_market_data()
    if data.empty:
        raise HTTPException(status_code=503, detail="No cached market data available.")
    close_all = sig.extract_close_prices(data)
    if close_all.empty:
        raise HTTPException(status_code=500, detail="Cache present but contains no Close prices.")

    available = [t for t in universe if t in close_all.columns]
    missing = [t for t in universe if t not in close_all.columns]
    if len(available) < 2:
        raise HTTPException(
            status_code=404,
            detail=f"Fewer than 2 tickers available in the cache. Missing: {missing}",
        )

    close = close_all.reindex(columns=available).dropna(axis=1, how="all").tail(body.lookback)
    # Drop columns that are mostly NaN (recently-listed names with too little history).
    min_obs = max(body.z_window + 5, 60)
    keep_cols = [c for c in close.columns if close[c].dropna().shape[0] >= min_obs]
    dropped_thin = [c for c in close.columns if c not in keep_cols]
    close = close[keep_cols]
    if close.shape[1] < 2:
        raise HTTPException(status_code=422, detail="Not enough price history to scan.")

    # Vectorized correlation matrix on returns. This is the cheap pre-filter.
    rets = close.pct_change().dropna(how="all")
    corr_mx = rets.corr()

    candidates: list[tuple[str, str, float]] = []
    cols = list(close.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a, b = cols[i], cols[j]
            c = corr_mx.iat[i, j]
            if c is None or not np.isfinite(c) or c < body.min_correlation:
                continue
            candidates.append((a, b, float(c)))

    pairs_out = []
    skipped_no_signal = 0
    for a, b, corr in candidates:
        sa = close[a].dropna()
        sb = close[b].dropna()
        joined = sa.to_frame("a").join(sb.to_frame("b"), how="inner").dropna()
        if len(joined) < min_obs:
            continue
        sa2, sb2 = joined["a"], joined["b"]
        try:
            beta = metrics.calculate_hedge_ratio(sa2, sb2)
            if not np.isfinite(beta):
                continue
            spread = metrics.calculate_spread(sa2, sb2, beta)
            z_series = metrics.rolling_zscore(spread, window=body.z_window).dropna()
            if z_series.empty:
                continue
            cz = float(z_series.iloc[-1])
            signal = metrics.pair_signal(cz, entry_threshold=body.entry, exit_threshold=body.exit_)
        except Exception:
            continue

        # Filter to ACTIVE setups only — skip neutral / monitor / exit / revert.
        sig_u = (signal or "").upper()
        is_active = ("LONG" in sig_u or "SHORT" in sig_u) and "EXIT" not in sig_u
        if not is_active:
            skipped_no_signal += 1
            continue

        pairs_out.append({
            "a": a,
            "b": b,
            "correlation": clean_for_json(corr),
            "hedge_ratio_beta": clean_for_json(beta),
            "current_z": clean_for_json(cz),
            "signal": signal,
        })

    # Best opportunities first: largest |z|, tiebreak on correlation.
    pairs_out.sort(key=lambda r: (-abs(r.get("current_z") or 0.0), -(r.get("correlation") or 0.0)))
    pairs_out = pairs_out[: body.max_results]

    return {
        "mode": "custom" if body.tickers else "bulk",
        "universe_size": len(close.columns),
        "candidates_scanned": len(candidates),
        "skipped_no_signal": skipped_no_signal,
        "missing_tickers": missing,
        "thin_history_skipped": dropped_thin,
        "lookback_days": body.lookback,
        "z_window": body.z_window,
        "entry_threshold": body.entry,
        "exit_threshold": body.exit_,
        "min_correlation": body.min_correlation,
        "count": len(pairs_out),
        "pairs": pairs_out,
    }
