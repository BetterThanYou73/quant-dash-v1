"""
Shared helpers for backend route modules.

Keeping these in one place avoids three slightly-different copies of the
same NaN-cleaning and cache-loading logic across the route files.
"""

from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd
from fastapi import HTTPException

from core import data_engine as de
from core import signals as sig


def clean_for_json(value):
    """JSON has no NaN/Infinity. Convert numpy/pandas floats safely.

    We hit this whenever a metric divides by zero or a window has missing
    data — pandas yields NaN/±Inf which would otherwise crash response
    serialization or send invalid JSON to the browser.
    """
    if isinstance(value, (np.floating, float)):
        f = float(value)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(value, (np.integer,)):
        return int(value)
    return value


def load_close_prices(tickers: Iterable[str], lookback: int) -> pd.DataFrame:
    """Load Close prices from cache, filtered to `tickers` and trimmed to last `lookback` rows.

    Raises HTTPException so callers don't have to handle the empty-cache or
    missing-ticker cases themselves — the response status code conveys the
    problem to the client directly.
    """
    data, _cache_ts = de.load_cached_market_data()
    if data.empty:
        raise HTTPException(
            status_code=503,
            detail="No cached market data. Run the worker: python -m core.workers",
        )

    close = sig.extract_close_prices(data)
    if close.empty:
        raise HTTPException(status_code=500, detail="Cache present but contains no Close prices")

    wanted = [t for t in tickers if t in close.columns]
    missing = [t for t in tickers if t not in close.columns]
    if not wanted:
        raise HTTPException(
            status_code=404,
            detail=f"None of the requested tickers are in the cache. Missing: {missing}",
        )

    close = close.reindex(columns=wanted).dropna(axis=1, how="all").tail(lookback)
    return close
