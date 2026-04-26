import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

# Resolve paths relative to the project root, not the current working dir
# __file__ is the path of this script file.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CACHE_DIR = PROJECT_ROOT / "cache"
CACHE_DATA_PATH = CACHE_DIR / "market_data.pkl"
CACHE_META_PATH = CACHE_DIR / "market_data_meta.json"
# Sidecar file: tracks tickers the user has added through the UI so the
# background worker keeps fetching them on each refresh (otherwise the
# next worker tick would wipe them out by overwriting the cache with the
# worker's hard-coded universe).
USER_TICKERS_PATH = CACHE_DIR / "user_tickers.json"
# Sidecar for user-added ticker metadata (company name + sector). Populated
# by /api/cache/ensure when a ticker outside SP500.csv is added so that the
# UI can still display the company name and sector.
USER_META_PATH = CACHE_DIR / "user_meta.json"

# load the S&P 500 universe once at import time
ticker_df = pd.read_csv(DATA_DIR / "SP500.csv")
ticker_list = ticker_df["Symbol"].tolist()

def fetch_stock_data(ticker=None, period='1y'):
    """
    Fetch historical stock data for the given ticker and period.
    """
    tickers = ticker if ticker is not None else ticker_list

    if isinstance(tickers, str):
        tickers = [tickers]

    tickers = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    if not tickers:
        return pd.DataFrame()

    try:
        data = yf.download(
            tickers=" ".join(tickers),
            period=period,
            group_by="column",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
    except Exception:
        return pd.DataFrame()

    return data if isinstance(data, pd.DataFrame) else pd.DataFrame()


def get_ticker_metadata():
    """Return ticker metadata from SP500.csv with normalized symbols."""
    meta = ticker_df.copy()
    meta["Symbol"] = meta["Symbol"].astype(str).str.upper().str.strip()
    return meta


def load_cached_market_data():
    """Load cached market data and metadata timestamp if available."""
    if not CACHE_DATA_PATH.exists():
        return pd.DataFrame(), None

    try:
        data = pd.read_pickle(CACHE_DATA_PATH)
    except Exception:
        return pd.DataFrame(), None

    cache_ts = None
    if CACHE_META_PATH.exists():
        try:
            with open(CACHE_META_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
            cache_ts = payload.get("last_updated_utc")
        except Exception:
            cache_ts = None

    if isinstance(data, pd.DataFrame):
        return data, cache_ts
    return pd.DataFrame(), cache_ts


def save_market_data_cache(data):
    """Persist market data in a local cache file plus metadata timestamp."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame()

    data.to_pickle(CACHE_DATA_PATH)

    payload = {
        "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": int(data.shape[0]),
        "col_count": int(data.shape[1]),
    }
    with open(CACHE_META_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def refresh_market_data_cache(tickers=None, period="1y"):
    """Fetch fresh market data and overwrite local cache."""
    data = fetch_stock_data(tickers, period=period)
    if data.empty:
        return pd.DataFrame(), None

    save_market_data_cache(data)
    _, cache_ts = load_cached_market_data()
    return data, cache_ts


# --- User-added ticker tracking ------------------------------------------
# These tickers come from the frontend (watchlist add). We persist them so
# the worker can include them in its periodic refresh — otherwise the next
# refresh would overwrite the cache and drop the user's additions.

def read_user_tickers():
    """Return the list of tickers the user has added through the UI."""
    if not USER_TICKERS_PATH.exists():
        return []
    try:
        with open(USER_TICKERS_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        items = payload.get("tickers", [])
        return sorted({str(t).strip().upper() for t in items if str(t).strip()})
    except Exception:
        return []


def write_user_tickers(tickers):
    """Persist the user-added ticker list."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleaned = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    with open(USER_TICKERS_PATH, "w", encoding="utf-8") as f:
        json.dump({"tickers": cleaned, "updated_utc": datetime.now(timezone.utc).isoformat()}, f, indent=2)


def add_user_tickers(new_tickers):
    """Add tickers to the persisted user list (idempotent)."""
    current = set(read_user_tickers())
    current.update({str(t).strip().upper() for t in new_tickers if str(t).strip()})
    write_user_tickers(sorted(current))
    return sorted(current)


def read_user_meta():
    """Return per-symbol metadata for user-added tickers.

    Shape: { "POET": {"name": "POET Technologies Inc", "sector": "Technology"}, ... }
    """
    if not USER_META_PATH.exists():
        return {}
    try:
        with open(USER_META_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def write_user_meta(meta_map):
    """Persist per-symbol user metadata."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(USER_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta_map, f, indent=2)


def upsert_user_meta(symbol, name=None, sector=None):
    """Add/update a single symbol's metadata. No-op if both fields are None."""
    if not name and not sector:
        return
    meta = read_user_meta()
    entry = meta.get(symbol.upper(), {})
    if name:   entry["name"] = name
    if sector: entry["sector"] = sector
    meta[symbol.upper()] = entry
    write_user_meta(meta)


def merge_tickers_into_cache(new_tickers, period="1y"):
    """Fetch `new_tickers` and merge their columns into the existing cache.

    Returns (added, failed) where:
      - added = tickers that came back from yfinance with usable data
      - failed = tickers that yfinance returned nothing for

    Why merge instead of overwrite: the user adds tickers one at a time
    through the UI. Overwriting would drop everything the worker already
    fetched. We concat new columns onto the existing DataFrame, keeping
    the most recent values for any duplicate column.
    """
    syms = sorted({str(t).strip().upper() for t in new_tickers if str(t).strip()})
    if not syms:
        return [], []

    new_data = fetch_stock_data(syms, period=period)
    if new_data.empty:
        return [], syms

    # Figure out which tickers actually came back. yfinance returns
    # MultiIndex columns (Field, Ticker) when given a list. For a single
    # ticker, it may return flat columns — handle both.
    if isinstance(new_data.columns, pd.MultiIndex):
        returned = sorted({str(t) for t in new_data.columns.get_level_values(-1).unique()})
    else:
        returned = syms[:]  # assume single-ticker request fully succeeded

    added = [s for s in syms if s in returned]
    failed = [s for s in syms if s not in returned]

    existing, _ = load_cached_market_data()
    if existing.empty:
        merged = new_data
    else:
        # Align indexes (date), then put columns side-by-side.
        # `keep="last"` means a freshly-fetched column replaces a stale one.
        merged = pd.concat([existing, new_data], axis=1)
        merged = merged.loc[:, ~merged.columns.duplicated(keep="last")]

    save_market_data_cache(merged)
    return added, failed


