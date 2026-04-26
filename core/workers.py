"""
Background worker — periodically refreshes the local market-data cache.

Universe: full S&P 500 from data/SP500.csv plus SPY (the benchmark used
by the alpha factor in the MFC signal model). Runs in its own process so
the API never has to wait on yfinance.

Usage:
    python -m core.workers --interval 600 --period 2y

Defaults are tuned for the MFC model:
  - period=2y so the 12-month momentum factor has a comfortable buffer
  - interval=600 (10 minutes) — balance freshness vs Yahoo rate limits
"""

import argparse
import time
from datetime import datetime

import core.data_engine as de


# SPY is the benchmark for the alpha factor and MUST be in the cache.
# We hard-code it here rather than putting it in SP500.csv because SP500.csv
# is meant to be the index constituents only.
BENCHMARK_TICKER = "SPY"


def build_universe():
    """Full S&P 500 from CSV + SPY benchmark, deduped and uppercased."""
    meta = de.get_ticker_metadata()
    sp500 = (
        meta["Symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
        .tolist()
    )
    return sorted(set(sp500 + [BENCHMARK_TICKER]))


def run_worker(interval_seconds, period, batch_size):
    universe = build_universe()
    print(f"[worker] universe size: {len(universe)} (S&P 500 + benchmark)")
    print(f"[worker] period={period}  batch_size={batch_size}  interval={interval_seconds}s")
    print(f"[worker] first refresh starting now (this can take a few minutes)…")

    while True:
        now = datetime.utcnow().isoformat()
        data, cache_ts = de.refresh_market_data_cache_batched(
            universe, period=period, batch_size=batch_size
        )

        if data.empty:
            print(f"[{now}] refresh failed: no data returned from any batch")
        else:
            print(
                f"[{now}] cache refreshed: rows={data.shape[0]} cols={data.shape[1]} "
                f"universe_requested={len(universe)} ts={cache_ts}"
            )

        time.sleep(interval_seconds)


def parse_args():
    parser = argparse.ArgumentParser(description="Background market data cache updater")
    # 10 minutes is a sensible default for ~500 tickers — frequent enough
    # for a dashboard, infrequent enough not to anger yahoo.
    parser.add_argument("--interval", type=int, default=600, help="Refresh interval in seconds (min 60)")
    # 2 years gives the 12-month momentum factor a buffer.
    parser.add_argument("--period", type=str, default="2y", help="yfinance period (e.g. 1y, 2y)")
    parser.add_argument("--batch-size", type=int, default=50, help="Tickers per yfinance call")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_worker(
        interval_seconds=max(60, args.interval),
        period=args.period,
        batch_size=max(10, min(100, args.batch_size)),
    )
