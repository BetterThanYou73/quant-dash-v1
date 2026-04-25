"""
Signal generation logic — pure functions, no UI/web dependencies.

This module is the "brain" behind the Market Overview view. Given a set of
close prices, it computes per-ticker metrics (momentum, vol, skew, tail risk,
drawdown, hit rate), combines them into a single profitability score via
percentile-rank weighting, and assigns a categorical signal.

It is intentionally framework-agnostic so that:
  - the legacy Streamlit app can call it
  - the new FastAPI backend can call it
  - a future CLI / notebook / cron job can call it
without any of them depending on each other.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from core import metrics


# --- Helpers --------------------------------------------------------------

def pct_rank(series: pd.Series) -> pd.Series:
    """Convert raw values to percentile ranks in [0, 1].

    Why: different metrics live on different scales (momentum is a %, skew is
    unitless, CVaR is a small negative number). To combine them into one score
    we first put them all on the same 0..1 scale via percentile rank.
    """
    return series.rank(pct=True)


def extract_close_prices(data: pd.DataFrame) -> pd.DataFrame:
    """Pull the 'Close' columns out of a yfinance multi-ticker download.

    yfinance returns a DataFrame with a MultiIndex on the columns when you
    download multiple tickers. The structure depends on the `group_by` arg,
    so we defensively check both layouts and a single-level fallback.
    """
    if data.empty:
        return pd.DataFrame()

    if isinstance(data.columns, pd.MultiIndex):
        level0 = set(data.columns.get_level_values(0))
        level1 = set(data.columns.get_level_values(1))
        if "Close" in level0:
            close = data["Close"]
        elif "Close" in level1:
            close = data.xs("Close", axis=1, level=1)
        else:
            return pd.DataFrame()
    else:
        if "Close" not in data.columns:
            return pd.DataFrame()
        close = data["Close"]

    # Single-ticker downloads return a Series; normalize to DataFrame so the
    # rest of the pipeline can treat all cases uniformly.
    if isinstance(close, pd.Series):
        close = close.to_frame()
    return close


# --- Main builder ---------------------------------------------------------

# Minimum trading days required to compute meaningful 63-day windows.
# Below this, momentum/drawdown numbers are too noisy to rank against peers.
MIN_HISTORY_DAYS = 64


def build_summary(close_prices: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Compute the per-ticker summary table and profitability score.

    Returns
    -------
    summary_df : pd.DataFrame
        One row per ticker, sorted by Profitability Score descending.
        Includes a categorical 'Signal' column ('Long Candidate' | 'Watch' | 'High Risk').
    skipped_tickers : list[str]
        Tickers dropped due to insufficient history.
    """
    summary_data: list[dict] = []
    skipped_tickers: list[str] = []

    # --- Per-ticker pass: compute raw metrics --------------------------
    for ticker in close_prices.columns:
        stock_series = close_prices[ticker].dropna()
        if stock_series.empty or len(stock_series) < MIN_HISTORY_DAYS:
            skipped_tickers.append(ticker)
            continue

        returns_series, vol_series, skewness = metrics.calculate_metrics(stock_series)
        if returns_series.empty:
            skipped_tickers.append(ticker)
            continue

        tail = metrics.calculate_tail_metrics(stock_series)

        # Latest snapshot values
        latest_price = float(stock_series.iloc[-1])
        daily_return = float(returns_series.iloc[-1])
        latest_volatility = float(vol_series.iloc[-1]) if pd.notna(vol_series.iloc[-1]) else float("nan")
        skewness_value = float(skewness) if pd.notna(skewness) else float("nan")

        # Trend / momentum: % change over the trailing window
        mom_21 = float(stock_series.pct_change(21).iloc[-1])
        mom_63 = float(stock_series.pct_change(63).iloc[-1])

        # Hit rate: fraction of up-days in the last month — proxy for trend reliability
        hit_rate_21 = float(returns_series.tail(21).gt(0).mean())

        # 63-day max drawdown: worst peak-to-trough drop in the trailing quarter
        rolling_peak_63 = stock_series.tail(63).cummax()
        drawdown_63 = float(((stock_series.tail(63) / rolling_peak_63) - 1.0).min())

        summary_data.append({
            "Ticker": ticker,
            "Price": latest_price,
            "Daily Return Numeric": daily_return,
            "21d Momentum Numeric": mom_21,
            "63d Momentum Numeric": mom_63,
            "20d Volatility Numeric": latest_volatility,
            "20d Skewness Numeric": skewness_value,
            "Hit Rate 21d Numeric": hit_rate_21,
            "Max Drawdown 63d Numeric": drawdown_63,
            "5% VaR Numeric": tail["var_5"],
            "5% CVaR Numeric": tail["cvar_5"],
            "Tail Ratio Numeric": tail["tail_ratio"],
            "Excess Kurtosis Numeric": tail["excess_kurtosis"],
            "JB p-Value Numeric": tail["jb_pvalue"],
        })

    if not summary_data:
        return pd.DataFrame(), skipped_tickers

    summary_df = pd.DataFrame(summary_data)

    # --- Cross-sectional pass: clean + rank ----------------------------
    # Replace inf and fill NaNs with the column median so one bad ticker
    # doesn't poison the percentile-rank calculation.
    rank_cols = [
        "21d Momentum Numeric",
        "63d Momentum Numeric",
        "20d Volatility Numeric",
        "20d Skewness Numeric",
        "5% CVaR Numeric",
        "Max Drawdown 63d Numeric",
        "Hit Rate 21d Numeric",
    ]
    for col in rank_cols:
        summary_df[col] = summary_df[col].replace([np.inf, -np.inf], np.nan)
        summary_df[col] = summary_df[col].fillna(summary_df[col].median())

    # Weighted composite score. Weights are hand-tuned, sum to 1.0.
    # Note the negation on volatility, CVaR, and drawdown — for these
    # "less is better", so we flip the sign before ranking.
    summary_df["Profitability Score"] = (
        0.25 * pct_rank(summary_df["21d Momentum Numeric"])
        + 0.20 * pct_rank(summary_df["63d Momentum Numeric"])
        + 0.15 * pct_rank(-summary_df["20d Volatility Numeric"])
        + 0.15 * pct_rank(summary_df["20d Skewness Numeric"])
        + 0.10 * pct_rank(-summary_df["5% CVaR Numeric"].abs())
        + 0.10 * pct_rank(-summary_df["Max Drawdown 63d Numeric"].abs())
        + 0.05 * pct_rank(summary_df["Hit Rate 21d Numeric"])
    )

    # Categorical signal — applied AFTER scoring so thresholds are absolute.
    summary_df["Signal"] = "Watch"
    summary_df.loc[
        (summary_df["Profitability Score"] >= 0.70)
        & (summary_df["5% CVaR Numeric"] > -0.10)
        & (summary_df["Max Drawdown 63d Numeric"] > -0.20),
        "Signal",
    ] = "Long Candidate"
    summary_df.loc[
        (summary_df["Profitability Score"] < 0.40)
        | (summary_df["5% CVaR Numeric"] <= -0.12)
        | (summary_df["Max Drawdown 63d Numeric"] <= -0.25),
        "Signal",
    ] = "High Risk"

    summary_df["Profitability Score"] = summary_df["Profitability Score"].round(3)
    return summary_df.sort_values("Profitability Score", ascending=False), skipped_tickers
