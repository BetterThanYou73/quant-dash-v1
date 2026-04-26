"""
Background worker — periodically refreshes the local market-data cache.

Universe: full S&P 500 from data/SP500.csv plus SPY (the benchmark used
by the alpha factor in the MFC signal model). Runs in its own process so
the API never has to wait on yfinance.

Usage:
    # local dev (loop forever, every 10 min)
    python -m core.workers

    # one-off refresh (used by Heroku release phase + Scheduler)
    python -m core.workers --once
    python -m core.workers --once --task=daily      # full S&P 500 EOD pull
    python -m core.workers --once --task=intraday   # macro + indices only
    python -m core.workers --once --task=quotes_warm  # reserved for Phase 2

Why split into tasks:
  Yahoo's free endpoint will throttle us if we hammer it. Splitting the
  refresh by cadence lets us pull the slow stuff (500-ticker S&P) once a
  day and the fast stuff (~10 macro symbols) hourly without overlap.

Defaults are tuned for the MFC model:
  - period=2y so the 12-month momentum factor has a comfortable buffer
  - interval=600 (10 minutes) — local-dev loop only; Heroku uses Scheduler
"""

import argparse
import time
from datetime import datetime

import pandas as pd

import core.data_engine as de


# SPY is the benchmark for the alpha factor and MUST be in the cache.
# QQQ is included so the header ticker bar (Nasdaq-100 pulse) can render.
# Both are hard-coded here rather than added to SP500.csv — that file is
# meant to be the index constituents only.
BENCHMARK_TICKER = "SPY"
EXTRA_INDICES = ["QQQ"]

# Macro / regime symbols: small, high-frequency-friendly universe used by
# the macro factors card and the regime card. Refreshing these hourly
# costs ~10 yfinance calls per tick, well within rate limits.
MACRO_TICKERS = ["SPY", "QQQ", "VIX", "^VIX", "DXY", "DX-Y.NYB", "TLT", "GLD", "USO", "HYG", "LQD"]


def build_universe(task: str = "daily"):
    """Return the ticker universe to refresh for the given task.

    daily    → full S&P 500 + benchmark + user-added (~510 tickers, slow)
    intraday → macro/regime tickers only (~10 tickers, fast)
    """
    if task == "intraday":
        return sorted(set(MACRO_TICKERS + [BENCHMARK_TICKER]))

    meta = de.get_ticker_metadata()
    sp500 = (
        meta["Symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
        .tolist()
    )
    extras = de.read_user_tickers()
    return sorted(set(sp500 + [BENCHMARK_TICKER] + EXTRA_INDICES + extras))


def refresh_once(task: str, period: str, batch_size: int):
    """Run one refresh cycle. Returns True on success.

    Behavior by task:
      daily    → full overwrite of the cache (slow, ~25 s for 500 tickers)
      intraday → fetch only the macro/regime tickers and MERGE into the
                 existing cache so the daily S&P 500 columns survive.
                 If no daily cache exists yet, this falls back to a full
                 overwrite of just the intraday universe.
    """
    universe = build_universe(task)
    now = datetime.utcnow().isoformat()
    print(f"[{now}] task={task} universe_size={len(universe)} period={period}")

    fresh = de.fetch_stock_data_batched(universe, period=period, batch_size=batch_size)
    if fresh.empty:
        print(f"[{now}] refresh failed: no data returned from any batch")
        return False

    if task == "intraday":
        # Merge into the existing cache so we don't blow away the full
        # S&P 500 daily columns. We refresh the intraday symbols' columns
        # in place and keep everything else untouched.
        existing, _ = de.load_cached_market_data()
        if existing is not None and not existing.empty:
            # Drop any columns in `existing` whose ticker we just refreshed,
            # then concat the fresh data on the column axis. yfinance
            # returns a (field, ticker) MultiIndex on columns when given >1
            # ticker; handle both single- and multi-ticker shapes.
            try:
                fresh_tickers = set(universe)
                if isinstance(existing.columns, pd.MultiIndex):
                    keep_mask = [tkr not in fresh_tickers for tkr in existing.columns.get_level_values(-1)]
                    pruned = existing.loc[:, keep_mask]
                else:
                    pruned = existing  # single-ticker shape; nothing to prune
                merged = pd.concat([pruned, fresh], axis=1)
                de.save_market_data_cache(merged)
                cache_ts = merged.index[-1].isoformat() if len(merged.index) else now
                print(f"[{now}] cache merged: rows={merged.shape[0]} cols={merged.shape[1]} "
                      f"intraday_refreshed={len(universe)} ts={cache_ts}")
                _rebuild_snapshot_safe()
                return True
            except Exception as exc:
                print(f"[{now}] merge failed ({exc}); falling back to overwrite")
                # fall through to overwrite

    # DAILY task: previously did a full overwrite, which blew away any
    # user-added tickers (SOXL, TLO, XEQT, etc.) that the API merged in
    # via /api/portfolio/refresh. The user_tickers.json file lives on
    # the web dyno's ephemeral filesystem and isn't visible from the
    # release/scheduler dyno that runs this code, so build_universe()
    # never sees them. Solution: merge rather than overwrite — keep any
    # columns from `existing` whose ticker is NOT in the universe we
    # just refreshed (those are by definition user-added).
    if task == "daily":
        existing, _ = de.load_cached_market_data()
        if existing is not None and not existing.empty and isinstance(existing.columns, pd.MultiIndex):
            try:
                fresh_tickers = set(universe)
                user_mask = [tkr not in fresh_tickers for tkr in existing.columns.get_level_values(-1)]
                if any(user_mask):
                    user_cols = existing.loc[:, user_mask]
                    merged = pd.concat([fresh, user_cols], axis=1)
                    merged = merged.loc[:, ~merged.columns.duplicated(keep="first")]
                    de.save_market_data_cache(merged)
                    print(f"[{now}] cache refreshed (preserving user tickers): "
                          f"rows={merged.shape[0]} cols={merged.shape[1]} "
                          f"universe_requested={len(universe)} "
                          f"user_tickers_kept={user_cols.columns.get_level_values(-1).nunique()}")
                    _rebuild_snapshot_safe()
                    return True
            except Exception as exc:
                print(f"[{now}] daily-preserve-user-tickers failed ({exc}); falling back to overwrite")

    de.save_market_data_cache(fresh)
    print(f"[{now}] cache refreshed: rows={fresh.shape[0]} cols={fresh.shape[1]} "
          f"universe_requested={len(universe)}")

    # Rebuild the dashboard snapshot so the next web request hits a hot
    # snapshot. On Heroku the release dyno does this and persists it to
    # Postgres; the web dyno then loads it from Postgres at startup.
    _rebuild_snapshot_safe()
    return True


def _rebuild_snapshot_safe() -> None:
    """Force-rebuild the dashboard snapshot. Safe to call after every cache
    refresh — failures are logged, never raised."""
    try:
        from core import snapshot as snap
        snap.invalidate()
        s = snap.get_snapshot(force_rebuild=True)
        errs = len(s.get("errors", {}) or {})
        print(f"[snapshot] post-refresh rebuild ok build_seconds={s.get('build_seconds')} errors={errs}")
    except Exception as exc:
        print(f"[snapshot] post-refresh rebuild failed (non-fatal): {exc}")


def run_worker(interval_seconds, period, batch_size, task: str):
    print(f"[worker] task={task} period={period} batch_size={batch_size} interval={interval_seconds}s")
    print(f"[worker] first refresh starting now (this can take a few minutes)…")
    while True:
        refresh_once(task, period, batch_size)
        time.sleep(interval_seconds)


def parse_args():
    parser = argparse.ArgumentParser(description="Background market data cache updater")
    # 10 minutes is a sensible default for ~500 tickers — frequent enough
    # for a dashboard, infrequent enough not to anger yahoo.
    parser.add_argument("--interval", type=int, default=600, help="Refresh interval in seconds (min 60)")
    # 2 years gives the 12-month momentum factor a buffer.
    parser.add_argument("--period", type=str, default="2y", help="yfinance period (e.g. 1y, 2y)")
    parser.add_argument("--batch-size", type=int, default=50, help="Tickers per yfinance call")
    parser.add_argument("--once", action="store_true", help="Run a single refresh and exit (cron-friendly)")
    parser.add_argument(
        "--task",
        choices=["daily", "intraday", "quotes_warm", "snapshot"],
        default="daily",
        help="Which slice of the universe to refresh. 'snapshot' rebuilds only the dashboard snapshot from the existing cache (no yfinance calls).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.once:
        # 'snapshot' task is special — it skips the yfinance fetch entirely
        # and just rebuilds the dashboard snapshot from whatever is in cache.
        # Useful as a cheap Heroku Scheduler job to keep the snapshot warm.
        if args.task == "snapshot":
            _rebuild_snapshot_safe()
            raise SystemExit(0)
        ok = refresh_once(task=args.task, period=args.period, batch_size=max(10, min(100, args.batch_size)))
        # Exit 0 on success, 1 on empty refresh — Heroku release phase will
        # ABORT the deploy on non-zero, which is exactly what we want.
        raise SystemExit(0 if ok else 1)

    run_worker(
        interval_seconds=max(60, args.interval),
        period=args.period,
        batch_size=max(10, min(100, args.batch_size)),
        task=args.task,
    )
