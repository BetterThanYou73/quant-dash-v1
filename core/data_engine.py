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


# --- Batched fetch for large universes ------------------------------------
# yfinance is unreliable when asked for 500+ tickers in a single call —
# the response either truncates silently or raises a JSON decode error.
# Fetching in chunks of ~50 with a small inter-batch sleep is the standard
# workaround. We concat the per-batch DataFrames at the end.

def fetch_stock_data_batched(tickers, period="2y", batch_size=50, pause_seconds=0.5):
    """Fetch many tickers from yfinance in chunks and concatenate the result.

    Returns a DataFrame with the same MultiIndex column shape that
    fetch_stock_data() produces. Empty if every batch failed.
    """
    import time as _time  # local import keeps top-of-file imports clean

    syms = sorted({str(t).strip().upper() for t in (tickers or []) if str(t).strip()})
    if not syms:
        return pd.DataFrame()

    pieces = []
    total_batches = (len(syms) + batch_size - 1) // batch_size

    for batch_index in range(total_batches):
        batch = syms[batch_index * batch_size : (batch_index + 1) * batch_size]
        print(f"[data_engine] batch {batch_index + 1}/{total_batches}: fetching {len(batch)} tickers")
        try:
            chunk = yf.download(
                tickers=" ".join(batch),
                period=period,
                group_by="column",
                progress=False,
                auto_adjust=False,
                threads=True,
            )
        except Exception as exc:
            print(f"[data_engine]   batch {batch_index + 1} failed: {exc}")
            continue

        if isinstance(chunk, pd.DataFrame) and not chunk.empty:
            pieces.append(chunk)

        # Be polite to yahoo's servers between batches.
        if batch_index < total_batches - 1:
            _time.sleep(pause_seconds)

    if not pieces:
        return pd.DataFrame()

    # Concatenate side-by-side on the column axis. The date index aligns
    # naturally; missing values land as NaN and downstream code already
    # handles those (factor calcs require dropna anyway).
    merged = pd.concat(pieces, axis=1)
    return merged


def refresh_market_data_cache_batched(tickers, period="2y", batch_size=50):
    """Batched variant of refresh_market_data_cache() — overwrites cache."""
    data = fetch_stock_data_batched(tickers, period=period, batch_size=batch_size)
    if data.empty:
        return pd.DataFrame(), None
    save_market_data_cache(data)
    _, cache_ts = load_cached_market_data()
    return data, cache_ts


