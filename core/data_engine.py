import json
import io
import os
import pickle
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
# Sidecar file: tracks tickers the user added through the UI so the
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


# --- Cache backend (file vs Postgres) ------------------------------------
# On Heroku the dyno filesystem is ephemeral — anything we write to
# `cache/` evaporates on the next dyno restart (which happens at least
# every 24h). So when DATABASE_URL is set we mirror the pickle to a
# single-row Postgres table. The web dyno reads from there on startup;
# the worker (release phase or scheduler) writes to it.
#
# CACHE_BACKEND env var overrides auto-detection:
#   "postgres" → use Postgres (requires DATABASE_URL)
#   "file"     → use local pickle only (good for local dev)
#   unset      → postgres if DATABASE_URL else file

def _cache_backend():
    explicit = (os.environ.get("CACHE_BACKEND") or "").strip().lower()
    if explicit in {"postgres", "file"}:
        return explicit
    return "postgres" if os.environ.get("DATABASE_URL") else "file"


def _pg_conn():
    """Open a psycopg connection. Heroku gives `postgres://` URLs; psycopg
    needs `postgresql://` — normalize that. Returns None if psycopg or
    DATABASE_URL is missing (so the caller can fall back to file mode)."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    try:
        import psycopg  # type: ignore
    except ImportError:
        return None
    return psycopg.connect(url, sslmode="require")


def _pg_ensure_schema(conn):
    """Create the cache table on first use. id=1 is the only row we ever
    write — it's a key/value blob, not a real table."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_data_cache (
                id INTEGER PRIMARY KEY,
                payload BYTEA NOT NULL,
                updated_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                row_count INTEGER NOT NULL DEFAULT 0,
                col_count INTEGER NOT NULL DEFAULT 0
            )
        """)
    conn.commit()


def _pg_save(data: pd.DataFrame):
    conn = _pg_conn()
    if conn is None:
        return False
    try:
        _pg_ensure_schema(conn)
        buf = io.BytesIO()
        # protocol=4 keeps the blob smaller than the default for big DFs
        pickle.dump(data, buf, protocol=pickle.HIGHEST_PROTOCOL)
        blob = buf.getvalue()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_data_cache (id, payload, updated_utc, row_count, col_count)
                VALUES (1, %s, NOW(), %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    payload     = EXCLUDED.payload,
                    updated_utc = EXCLUDED.updated_utc,
                    row_count   = EXCLUDED.row_count,
                    col_count   = EXCLUDED.col_count
            """, (blob, int(data.shape[0]), int(data.shape[1])))
        conn.commit()
        return True
    finally:
        conn.close()


def _pg_load():
    """Return (DataFrame, iso_timestamp) or (None, None) if no row yet."""
    conn = _pg_conn()
    if conn is None:
        return None, None
    try:
        _pg_ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT payload, updated_utc FROM market_data_cache WHERE id = 1")
            row = cur.fetchone()
        if not row:
            return None, None
        payload, ts = row
        df = pickle.loads(payload)
        return df, ts.isoformat() if ts else None
    finally:
        conn.close()

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
    """Load cached market data and metadata timestamp if available.

    Tries Postgres first when CACHE_BACKEND=postgres (Heroku), then falls
    back to the local pickle. The fallback matters during the brief
    window after deploy but before the worker has populated Postgres.
    """
    if _cache_backend() == "postgres":
        df, ts = _pg_load()
        if df is not None:
            return df, ts
        # Fall through to file in case the worker hasn't written yet but
        # an old pickle exists in the slug.

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


# --- In-process memo over load_cached_market_data ------------------------
# Without this, every concurrent /api/* request that needs the cache
# triggers its own Postgres SELECT + pickle.loads, each transiently
# allocating ~250 MB for the full S&P 500 DataFrame. On a 512 MB Heroku
# Basic dyno that immediately trips R14/R15 (memory exceeded). With this
# memo, the DataFrame is loaded once per process and reused.
#
# We hold the cached object for `_MEMO_TTL` seconds so a fresh worker
# write (release phase or Scheduler) is picked up without restarting
# the web dyno.

import threading

_MEMO_LOCK = threading.Lock()
_MEMO_DATA = None
_MEMO_TS = None
_MEMO_LOADED_AT = 0.0
_MEMO_TTL = 300.0  # seconds


def get_market_data():
    """Process-wide memoized accessor. Returns (DataFrame, iso_timestamp).

    Use this from request handlers instead of load_cached_market_data()
    to avoid concurrent re-decodes of the same pickle blob.
    """
    import time as _time
    global _MEMO_DATA, _MEMO_TS, _MEMO_LOADED_AT
    now = _time.time()
    if _MEMO_DATA is not None and (now - _MEMO_LOADED_AT) < _MEMO_TTL:
        return _MEMO_DATA, _MEMO_TS

    with _MEMO_LOCK:
        # Double-check pattern: another thread may have populated it
        # while we were waiting for the lock.
        now = _time.time()
        if _MEMO_DATA is not None and (now - _MEMO_LOADED_AT) < _MEMO_TTL:
            return _MEMO_DATA, _MEMO_TS

        df, ts = load_cached_market_data()
        _MEMO_DATA = df
        _MEMO_TS = ts
        _MEMO_LOADED_AT = now
        return df, ts


def invalidate_memo():
    """Drop the in-process memo. Called after the worker writes a new cache
    so the API serves fresh data without a dyno restart."""
    global _MEMO_DATA, _MEMO_TS, _MEMO_LOADED_AT
    with _MEMO_LOCK:
        _MEMO_DATA = None
        _MEMO_TS = None
        _MEMO_LOADED_AT = 0.0


def save_market_data_cache(data):
    """Persist market data. Writes to Postgres on Heroku, pickle locally.

    The metadata sidecar is still written when the file backend is in use
    so /api/cache-status keeps working in local dev.
    """
    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame()

    if _cache_backend() == "postgres":
        ok = _pg_save(data)
        if ok:
            return
        # If Postgres failed (e.g. transient network), fall through to
        # the file backend so the worker run isn't a total loss.

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
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


# --- User-added ticker tracking ------------------------------------------
# Tickers come from the frontend (watchlist add). Persist them so the
# worker can include them in its periodic refresh — otherwise the next
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
        json.dump(
            {"tickers": cleaned, "updated_utc": datetime.now(timezone.utc).isoformat()},
            f, indent=2,
        )


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


def merge_tickers_into_cache(new_tickers, period="2y"):
    """Fetch `new_tickers` and merge their columns into the existing cache.

    Returns (added, failed) where:
      - added  = tickers yfinance returned usable data for
      - failed = tickers yfinance returned nothing for

    Why merge instead of overwrite: the user adds tickers one at a time
    through the UI. Overwriting would drop everything the worker already
    fetched. We concat new columns onto the existing DataFrame and dedupe
    (keeping the most recent values for any duplicate column).
    """
    syms = sorted({str(t).strip().upper() for t in new_tickers if str(t).strip()})
    if not syms:
        return [], []

    new_data = fetch_stock_data(syms, period=period)
    if new_data.empty:
        return [], syms

    # Figure out which tickers actually came back. yfinance returns
    # MultiIndex columns when given a list. For a single ticker it may
    # return flat columns — handle both.
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
        # Align indexes (date), then put columns side-by-side. `keep="last"`
        # means a freshly-fetched column replaces a stale one.
        merged = pd.concat([existing, new_data], axis=1)
        merged = merged.loc[:, ~merged.columns.duplicated(keep="last")]

    save_market_data_cache(merged)
    return added, failed


