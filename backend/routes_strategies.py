"""
Routes for the Strategies feature — /api/strategies.

Six strategy "lenses" on the same factor panel that powers /api/signals
and /api/screener. Each strategy applies its own filter + sort rule on
top of the precomputed Momentum / Sortino / Alpha / Beta / CVaR / DD
columns so we don't have to recompute anything.

Strategies:
  - momentum         : top by 12-1 momentum, must have Composite_Z > 0
  - mean_reversion   : recent pullback (last-21d return < 0) on quality names
                       (Composite_Z > 0). Buys the dip on otherwise-strong stocks.
  - breakout         : price within 5% of trailing-252d high + high dollar volume
  - value            : low beta + positive alpha (defensive quality proxy)
  - dividend_capture : low drawdown + low downside dev (income-stable proxy;
                       proper div-yield filter pending fundamentals feed)
  - quant_signals    : Strong Buy / Buy from composite — same as the
                       headline screener, included for completeness.

Pure read endpoint — uses the cached Postgres market-data row, no I/O.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from core import data_engine as de
from core import signals as sig

from backend._helpers import clean_for_json


router = APIRouter(prefix="/api", tags=["strategies"])

BENCHMARK_TICKER = "SPY"

ALLOWED_STRATEGIES = {
    "momentum", "mean_reversion", "breakout",
    "value", "dividend_capture", "quant_signals",
}

# Human-readable descriptions returned with each result so the UI doesn't
# have to duplicate strings.
STRATEGY_INFO = {
    "momentum": {
        "name": "Momentum",
        "thesis": "Top names by 12-1 month momentum with positive composite signal.",
        "fits": "Trend-following / breakout traders. Avoid in mean-reverting markets.",
    },
    "mean_reversion": {
        "name": "Mean Reversion",
        "thesis": "Quality names (positive composite) that have pulled back over the last 21 days.",
        "fits": "Buy-the-dip on otherwise-strong stocks. Watch for catalysts \u2014 not all dips reverse.",
    },
    "breakout": {
        "name": "Breakout",
        "thesis": "Price within 5% of trailing 252-day high, high dollar volume confirms.",
        "fits": "Momentum continuation plays. Tight stops below the breakout level.",
    },
    "value": {
        "name": "Value (Low-Beta Quality)",
        "thesis": "Low-beta names with positive alpha vs SPY \u2014 defensive quality without paying for growth.",
        "fits": "Risk-off tilts, retirees, drawdown-averse portfolios.",
    },
    "dividend_capture": {
        "name": "Dividend Capture (Income Proxy)",
        "thesis": "Low max-drawdown + low downside deviation \u2014 stable names usually pay reliable dividends.",
        "fits": "Income / yield seekers. Proper dividend-yield filter pending fundamentals integration.",
    },
    "quant_signals": {
        "name": "Quant Signals",
        "thesis": "Composite Strong Buy / Buy \u2014 the multi-factor model's headline picks.",
        "fits": "Default systematic ideas. Already powers the main dashboard.",
    },
}


def _load_panel():
    """Score the full universe and return the enriched DataFrame.

    Returns (df, cache_ts) where df has all factor columns + Sector + Name.
    Computing close-derived stats (last_21d_return, distance_from_52w_high)
    happens here too so each strategy can branch off them.
    """
    data, cache_ts = de.get_market_data()
    if data.empty:
        raise HTTPException(status_code=503, detail="No cached market data yet.")

    close_all = sig.extract_close_prices(data)
    vols_all = sig.extract_volumes(data)
    if BENCHMARK_TICKER not in close_all.columns:
        raise HTTPException(status_code=503, detail=f"Benchmark {BENCHMARK_TICKER} missing from cache.")

    benchmark = close_all[BENCHMARK_TICKER].dropna()
    universe_close = close_all.drop(columns=[BENCHMARK_TICKER])
    universe_vols = vols_all.drop(columns=[BENCHMARK_TICKER], errors="ignore")

    full_list = sorted(universe_close.columns)
    df, _ = sig.build_composite_signals(
        close_prices=universe_close,
        volumes=universe_vols if not universe_vols.empty else None,
        benchmark_prices=benchmark,
        watchlist=full_list,
    )
    if df.empty:
        raise HTTPException(status_code=422, detail="Universe scoring returned no rows.")

    # Enrich with metadata.
    meta = de.get_ticker_metadata()
    sector_map = meta.set_index("Symbol")["Sector"].to_dict()
    name_map = meta.set_index("Symbol")["Name"].to_dict() if "Name" in meta.columns else {}
    for sym, info in de.read_user_meta().items():
        sector_map.setdefault(sym, info.get("sector") or "Unknown")
        name_map.setdefault(sym, info.get("name") or sym)
    df["Sector"] = df["Ticker"].map(sector_map).fillna("Unknown")
    df["Name"] = df["Ticker"].map(name_map).fillna(df["Ticker"])

    # --- Add price-derived columns the strategies need ---------------
    # Last 21d return for mean-reversion; distance from 52w high for breakout.
    last_21d_ret = {}
    dist_from_high = {}
    for t in df["Ticker"]:
        if t not in universe_close.columns:
            continue
        s = universe_close[t].dropna()
        if len(s) < 22:
            continue
        last_21d_ret[t] = float(s.iloc[-1] / s.iloc[-22] - 1.0)
        win = s.tail(252) if len(s) >= 252 else s
        hi = float(win.max())
        if hi > 0:
            dist_from_high[t] = float(s.iloc[-1] / hi - 1.0)  # \u22640, closer to 0 = nearer high

    df["Return_21d"] = df["Ticker"].map(last_21d_ret)
    df["Dist_52w_High"] = df["Ticker"].map(dist_from_high)

    return df, cache_ts


def _apply_strategy(df: pd.DataFrame, strategy: str) -> pd.DataFrame:
    """Filter + sort the panel for a given strategy. Returns a new DF."""
    s = strategy.lower()
    if s == "momentum":
        out = df[(df["Composite_Z"] > 0) & df["Momentum_12_1"].notna()]
        return out.sort_values("Momentum_12_1", ascending=False)

    if s == "mean_reversion":
        # Quality names that pulled back: positive composite, negative 21d return.
        out = df[
            (df["Composite_Z"] > 0)
            & (df["Return_21d"].notna())
            & (df["Return_21d"] < -0.03)
        ]
        # Most-pulled-back first (deepest dip on quality name).
        return out.sort_values("Return_21d", ascending=True)

    if s == "breakout":
        out = df[
            (df["Dist_52w_High"].notna())
            & (df["Dist_52w_High"] >= -0.05)  # within 5% of 52w high
            & (df["Avg_Dollar_Vol_21d"].notna())
        ]
        # Highest dollar volume = most confirmation.
        return out.sort_values("Avg_Dollar_Vol_21d", ascending=False)

    if s == "value":
        # Defensive quality: low beta + positive alpha.
        out = df[
            (df["Beta"].notna())
            & (df["Beta"] < 1.0)
            & (df["Alpha_Annualized"].notna())
            & (df["Alpha_Annualized"] > 0)
        ]
        # Highest alpha for the lowest beta = best defensive pick.
        return out.sort_values("Alpha_Annualized", ascending=False)

    if s == "dividend_capture":
        # Stable names proxy: small drawdown + low downside deviation.
        out = df[
            (df["Max_Drawdown_252d"].notna())
            & (df["Max_Drawdown_252d"] > -0.20)  # less than 20% drawdown in last yr
            & (df["Downside_Dev_126d"].notna())
        ]
        # Lowest downside deviation = most stable.
        return out.sort_values("Downside_Dev_126d", ascending=True)

    # quant_signals
    normalized = df["Signal"].str.upper().str.replace(" ", "_", regex=False)
    out = df[normalized.isin({"STRONG_BUY", "BUY"})]
    return out.sort_values("Composite_Z", ascending=False)


@router.get("/strategies/screen")
def screen_strategy(
    strategy: str = Query(..., description=f"One of {sorted(ALLOWED_STRATEGIES)}"),
    sector: Optional[str] = Query(None, description="Optional GICS sector filter."),
    limit: int = Query(20, ge=1, le=100),
):
    """Run a single strategy screen and return ranked picks."""
    s = strategy.lower()
    if s not in ALLOWED_STRATEGIES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy '{strategy}'. Allowed: {sorted(ALLOWED_STRATEGIES)}",
        )

    df, cache_ts = _load_panel()

    if sector:
        df = df[df["Sector"].str.lower() == sector.strip().lower()]

    df = _apply_strategy(df, s).head(limit)

    keep_cols = [
        "Ticker", "Name", "Sector", "Price",
        "Composite_Z", "Composite_Percentile", "Signal",
        "Momentum_12_1", "Sortino", "Alpha_Annualized", "Beta",
        "CVaR_5", "Max_Drawdown_252d", "Downside_Dev_126d",
        "Avg_Dollar_Vol_21d", "Return_21d", "Dist_52w_High",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    rounding = {
        "Price": 2, "Composite_Z": 3, "Composite_Percentile": 1,
        "Momentum_12_1": 4, "Sortino": 3, "Alpha_Annualized": 4,
        "Beta": 3, "CVaR_5": 4, "Max_Drawdown_252d": 4,
        "Downside_Dev_126d": 4, "Return_21d": 4, "Dist_52w_High": 4,
    }
    for c, d in rounding.items():
        if c in df.columns:
            df[c] = df[c].round(d)

    rows = [{k: clean_for_json(v) for k, v in r.items()} for r in df.to_dict(orient="records")]

    info = STRATEGY_INFO[s]
    return {
        "as_of_utc": cache_ts,
        "strategy": s,
        "name": info["name"],
        "thesis": info["thesis"],
        "fits": info["fits"],
        "filters": {"sector": sector, "limit": limit},
        "count": len(rows),
        "results": rows,
    }


@router.get("/strategies/list")
def list_strategies() -> dict:
    """Return all strategy metadata so the UI can render cards from one source."""
    return {
        "strategies": [
            {"key": k, **STRATEGY_INFO[k]} for k in [
                "momentum", "mean_reversion", "breakout",
                "value", "dividend_capture", "quant_signals",
            ]
        ]
    }
