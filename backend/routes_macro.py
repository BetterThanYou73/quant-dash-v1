"""
Routes for macro context — /api/macro.

Pulls a small fixed basket of macro proxies via yfinance:

  ^VIX         — CBOE Volatility Index (S&P implied vol, "fear gauge")
  ^TNX         — 10-year US Treasury yield (interest rate proxy)
  CL=F         — WTI crude oil futures (energy / inflation proxy)
  GC=F         — Gold futures (real-rate / safe-haven proxy)
  DX-Y.NYB     — US Dollar Index (DXY) (currency strength)
  ^GSPC        — S&P 500 spot (market reference)

For each: latest close + 1d / 5d / 21d / 252d % change.

Why yfinance instead of FRED?
  - No API key needed. FRED's CPI/Fed-funds-rate data is gold-standard
    but requires registering for a key and handling lag (CPI is monthly,
    released ~mid-month). The yfinance tickers above are intraday-fresh
    and don't need any extra config.
  - We can add FRED later for CPI/Unemployment/GDP if it's needed.

This route fetches LIVE from yfinance on each call (NOT from the worker
cache) because:
  - These tickers aren't in SP500.csv and the worker doesn't track them.
  - Macro doesn't move on intraday speed; one-call-per-page-load is fine.
  - Adding them to the worker would mean another batched fetch + cache
    growth for ~6 tickers, which isn't worth the complexity yet.

If yfinance fails (rate limit, network), the affected ticker returns
`null` for its price/changes and the rest still come back.
"""

from __future__ import annotations

import math
from typing import Optional

import pandas as pd
import yfinance as yf
from fastapi import APIRouter


router = APIRouter(prefix="/api", tags=["macro"])


# Static basket. Order matters — frontend renders left-to-right.
MACRO_TICKERS = [
    {"symbol": "^VIX",     "label": "VIX",     "description": "S&P 500 implied volatility (30-day)"},
    {"symbol": "^TNX",     "label": "10Y",     "description": "US 10-Year Treasury yield"},
    {"symbol": "CL=F",     "label": "Oil",     "description": "WTI Crude Oil futures (front-month)"},
    {"symbol": "GC=F",     "label": "Gold",    "description": "Gold futures (front-month)"},
    {"symbol": "DX-Y.NYB", "label": "DXY",     "description": "US Dollar Index"},
    {"symbol": "^GSPC",    "label": "S&P 500", "description": "S&P 500 spot index"},
]


def _pct_change(series: pd.Series, n: int) -> Optional[float]:
    """Percent change over the last `n` rows. Returns None if not enough data."""
    if len(series) <= n:
        return None
    try:
        old = float(series.iloc[-(n + 1)])
        new = float(series.iloc[-1])
        if old == 0 or math.isnan(old) or math.isnan(new):
            return None
        return round((new / old - 1.0) * 100.0, 2)
    except Exception:
        return None


@router.get("/macro")
def get_macro():
    """Latest level + 1d/5d/21d/252d % changes for the macro basket.

    Uses yfinance.download with a 14-month period to ensure we have at
    least 252 trading days for the 1-year change. Single batched call —
    if it fails, every ticker returns null but the response still comes
    back (frontend can render whatever succeeded).
    """
    symbols = [m["symbol"] for m in MACRO_TICKERS]

    try:
        # `period="14mo"` is just enough to give 252 trading days for the
        # 1y % change. group_by="column" keeps the response easy to slice.
        raw = yf.download(
            tickers=" ".join(symbols),
            period="14mo",
            group_by="column",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
    except Exception:
        raw = pd.DataFrame()

    # Pull the Close column. yfinance returns MultiIndex columns when
    # given multiple symbols — normalize to a flat DataFrame[date, symbol].
    if not raw.empty and isinstance(raw.columns, pd.MultiIndex):
        if "Close" in raw.columns.get_level_values(0):
            close = raw["Close"]
        else:
            close = pd.DataFrame()
    elif not raw.empty and "Close" in raw.columns:
        # Single-ticker fallback
        close = raw[["Close"]].rename(columns={"Close": symbols[0]})
    else:
        close = pd.DataFrame()

    results = []
    for meta in MACRO_TICKERS:
        sym = meta["symbol"]
        if close.empty or sym not in close.columns:
            results.append({
                **meta,
                "price": None,
                "change_1d_pct": None,
                "change_5d_pct": None,
                "change_21d_pct": None,
                "change_252d_pct": None,
            })
            continue

        s = close[sym].dropna()
        if s.empty:
            results.append({
                **meta,
                "price": None,
                "change_1d_pct": None,
                "change_5d_pct": None,
                "change_21d_pct": None,
                "change_252d_pct": None,
            })
            continue

        results.append({
            **meta,
            "price": round(float(s.iloc[-1]), 2),
            "change_1d_pct":   _pct_change(s, 1),
            "change_5d_pct":   _pct_change(s, 5),
            "change_21d_pct":  _pct_change(s, 21),
            "change_252d_pct": _pct_change(s, 252),
        })

    return {"results": results}
