import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import json

ticker_df = pd.read_csv("SP500.csv")
ticker_list = ticker_df["Symbol"].tolist()

CACHE_DIR = Path("cache")
CACHE_DATA_PATH = CACHE_DIR / "market_data.pkl"
CACHE_META_PATH = CACHE_DIR / "market_data_meta.json"

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


