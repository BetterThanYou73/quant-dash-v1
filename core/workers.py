import argparse
import time
from datetime import datetime

import core.data_engine as de


def build_default_universe(sectors, max_sector_names):
    base = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "INTC", "AMD", "NVDA", "TSLA"]
    meta = de.get_ticker_metadata()

    if sectors:
        sector_symbols = (
            meta.loc[meta["Sector"].isin(sectors), "Symbol"]
            .astype(str)
            .str.upper()
            .head(max_sector_names)
            .tolist()
        )
    else:
        sector_symbols = []

    # Tickers the user added through the UI. We include them so the periodic
    # refresh keeps their data fresh instead of letting it go stale.
    user_added = de.read_user_tickers()

    return sorted(set(base + sector_symbols + user_added))


def run_worker(interval_seconds, period, sectors, max_sector_names):
    print(f"[worker] refresh interval: {interval_seconds}s")

    while True:
        # Re-read the user-added tickers each iteration so additions made
        # while the worker is running get picked up on the next refresh.
        tickers = build_default_universe(sectors, max_sector_names)
        now = datetime.utcnow().isoformat()
        data, cache_ts = de.refresh_market_data_cache(tickers, period=period)

        if data.empty:
            print(f"[{now}] refresh failed: no data returned")
        else:
            print(f"[{now}] cache refreshed: {data.shape[0]} rows x {data.shape[1]} cols | universe={len(tickers)} | ts={cache_ts}")

        time.sleep(interval_seconds)


def parse_args():
    parser = argparse.ArgumentParser(description="Background market data cache updater")
    parser.add_argument("--interval", type=int, default=60, help="Refresh interval in seconds")
    parser.add_argument("--period", type=str, default="1y", help="yfinance period, e.g. 6mo, 1y")
    parser.add_argument(
        "--sectors",
        type=str,
        nargs="*",
        default=[],
        help="Optional sectors to include in universe",
    )
    parser.add_argument(
        "--max-sector-names",
        type=int,
        default=25,
        help="Maximum tickers from selected sectors",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_worker(
        interval_seconds=max(10, args.interval),
        period=args.period,
        sectors=args.sectors,
        max_sector_names=max(5, args.max_sector_names),
    )
