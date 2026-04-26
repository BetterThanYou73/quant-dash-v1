"""
Factor definitions for the Multi-Factor Composite (MFC) signal model.

Each factor is a pure function over price/return series. They get combined
into a cross-sectional z-score composite in core.signals. Keeping them
isolated here makes them individually testable and lets us cite each one
to its academic source.

References
----------
- Momentum 12-1: Jegadeesh & Titman (1993) "Returns to Buying Winners
  and Selling Losers"; Asness, Moskowitz & Pedersen (2013) "Value and
  Momentum Everywhere"
- Sortino ratio: Sortino & Price (1994) — downside-deviation analog of
  the Sharpe ratio
- Alpha vs benchmark: Jensen (1968) — CAPM-implied excess return
- CVaR / Expected Shortfall: Rockafellar & Uryasev (2000)
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# Treat the year as 252 trading days. Used for annualizing returns and vol.
ANNUAL_TRADING_DAYS = 252

# Window constants. Defined here so all factor calls stay consistent.
MOMENTUM_LOOKBACK_DAYS = 252   # 12 months
MOMENTUM_SKIP_DAYS = 21        # exclude most recent month (the "1" in 12-1)
SORTINO_LOOKBACK = 126         # 6 months
ALPHA_LOOKBACK = 126           # 6 months
CVAR_LOOKBACK = 252            # 12 months → 12-13 tail observations
DRAWDOWN_LOOKBACK = 252        # 12 months
LIQUIDITY_LOOKBACK = 21        # 1 month

MIN_HISTORY_DAYS = MOMENTUM_LOOKBACK_DAYS + 5  # need a buffer over momentum window


# --- Individual factor calculations --------------------------------------

def momentum_12_1(prices: pd.Series) -> float:
    """12-month total return EXCLUDING the most recent month.

    Why "excluding": short-term reversal effect. Last month's winners tend
    to mean-revert in the next month; last 11 months of winners (skipping
    the most recent) tend to continue. This is the canonical academic
    momentum factor used by MTUM, IWMO, and most factor funds.
    """
    p = prices.dropna()
    if len(p) < MOMENTUM_LOOKBACK_DAYS:
        return float("nan")
    p_recent = p.iloc[-MOMENTUM_SKIP_DAYS - 1]   # price ~1 month ago
    p_old = p.iloc[-MOMENTUM_LOOKBACK_DAYS]      # price ~12 months ago
    if p_old <= 0 or pd.isna(p_recent) or pd.isna(p_old):
        return float("nan")
    return float(p_recent / p_old - 1.0)


def downside_deviation(returns: pd.Series, window: int = SORTINO_LOOKBACK) -> float:
    """Annualized stdev computed over NEGATIVE returns only.

    Replaces symmetric volatility — does not punish upside swings.
    Requires at least 5 negative observations to be meaningful.
    """
    r = returns.tail(window).dropna()
    neg = r[r < 0]
    if len(neg) < 5:
        return float("nan")
    sd = float(neg.std(ddof=1))
    if not np.isfinite(sd) or sd <= 0:
        return float("nan")
    return sd * np.sqrt(ANNUAL_TRADING_DAYS)


def sortino_ratio(prices: pd.Series, lookback: int = SORTINO_LOOKBACK) -> float:
    """Annualized return divided by downside deviation.

    A risk-adjusted return that only penalizes downside vol. High Sortino
    means "this stock went up smoothly without big crashes." Low Sortino
    means either it didn't go up, or it crashed a lot on the way.
    """
    p = prices.dropna().tail(lookback + 1)
    if len(p) < lookback + 1:
        return float("nan")

    period_return = float(p.iloc[-1] / p.iloc[0] - 1.0)
    # Annualize the period return geometrically
    if 1 + period_return <= 0:
        return float("nan")
    ann_return = (1.0 + period_return) ** (ANNUAL_TRADING_DAYS / lookback) - 1.0

    rets = p.pct_change().dropna()
    dd_dev = downside_deviation(rets, window=lookback)
    if pd.isna(dd_dev) or dd_dev <= 0:
        return float("nan")

    return float(ann_return / dd_dev)


def alpha_beta_vs_benchmark(
    returns: pd.Series,
    benchmark_returns: pd.Series,
    lookback: int = ALPHA_LOOKBACK,
) -> tuple[float, float]:
    """OLS regression: r_stock = α + β · r_benchmark + ε.

    Returns (alpha_annualized, beta). Alpha is the daily intercept scaled
    to a yearly figure so it's interpretable as "% per year above what
    beta-exposure to SPY would give you."

    Requires ~30+ aligned observations to be statistically meaningful.
    """
    pair = pd.concat([returns, benchmark_returns], axis=1).dropna().tail(lookback)
    if len(pair) < 30:
        return float("nan"), float("nan")

    y = pair.iloc[:, 0].to_numpy(dtype=float)
    x = pair.iloc[:, 1].to_numpy(dtype=float)

    x_var = float(np.var(x, ddof=1))
    if x_var <= 0:
        return float("nan"), float("nan")

    beta = float(np.cov(y, x, ddof=1)[0, 1] / x_var)
    # Alpha is the intercept of the regression line.
    alpha_daily = float(y.mean() - beta * x.mean())
    alpha_annualized = alpha_daily * ANNUAL_TRADING_DAYS

    if not np.isfinite(beta) or not np.isfinite(alpha_annualized):
        return float("nan"), float("nan")
    return alpha_annualized, beta


def cvar_5(returns: pd.Series, lookback: int = CVAR_LOOKBACK) -> float:
    """Expected return on the worst 5% of days (Conditional Value-at-Risk).

    With a 252-day window we get ~13 tail observations — still noisy but
    defensible. Result is a NEGATIVE number (e.g. -0.04 = "on the worst
    5% of days you lose 4% on average").
    """
    r = returns.tail(lookback).dropna()
    if len(r) < 50:
        return float("nan")
    threshold = float(np.percentile(r, 5))
    tail = r[r <= threshold]
    if tail.empty:
        return float("nan")
    return float(tail.mean())


def max_drawdown(prices: pd.Series, lookback: int = DRAWDOWN_LOOKBACK) -> float:
    """Worst peak-to-trough percentage drop over the lookback window.

    Result is a NEGATIVE number (e.g. -0.30 = "fell 30% from a high").
    """
    p = prices.dropna().tail(lookback)
    if len(p) < 30:
        return float("nan")
    rolling_peak = p.cummax()
    dd = (p / rolling_peak) - 1.0
    return float(dd.min())


def avg_dollar_volume(prices: pd.Series, volumes: pd.Series, lookback: int = LIQUIDITY_LOOKBACK) -> float:
    """Mean of (price × volume) over the recent window.

    Crude liquidity proxy. Dollar volume below ~$5M/day means you can't
    actually trade the stock at scale without moving the price — a hard
    veto on any "Buy" recommendation regardless of how good the metrics
    look on paper.
    """
    p = prices.tail(lookback).dropna()
    v = volumes.tail(lookback).dropna()
    aligned = pd.concat([p, v], axis=1).dropna()
    if aligned.empty:
        return float("nan")
    dv = aligned.iloc[:, 0] * aligned.iloc[:, 1]
    return float(dv.mean())


# --- Cross-sectional panel assembly --------------------------------------

# Columns produced by compute_factor_panel(). Frozen here so downstream
# consumers (signals.py, the API layer) can rely on a stable schema.
FACTOR_COLUMNS = [
    "Price",
    "Momentum_12_1",
    "Sortino",
    "Alpha_Annualized",
    "Beta",
    "CVaR_5",
    "Max_Drawdown_252d",
    "Downside_Dev_126d",
    "Avg_Dollar_Vol_21d",
]


def compute_factor_panel(
    close_prices: pd.DataFrame,
    volumes: pd.DataFrame | None,
    benchmark_prices: pd.Series,
) -> pd.DataFrame:
    """Build a per-ticker factor panel for the entire universe.

    Parameters
    ----------
    close_prices : DataFrame indexed by date, one column per ticker.
    volumes      : DataFrame in the same shape (or None — liquidity is then NaN).
    benchmark_prices : Series of benchmark closes (typically SPY).

    Returns
    -------
    DataFrame indexed by Ticker with FACTOR_COLUMNS columns.
    Tickers with insufficient history are dropped silently.

    The returned frame is the input to signals.build_composite_signals().
    Computing per-ticker in a loop is fine here — we're at <600 tickers so
    vectorizing across the whole panel buys us very little and costs a lot
    of readability.
    """
    benchmark_returns = benchmark_prices.pct_change().dropna()

    rows: list[dict] = []
    for ticker in close_prices.columns:
        prices = close_prices[ticker].dropna()
        if len(prices) < MIN_HISTORY_DAYS:
            continue

        rets = prices.pct_change().dropna()
        alpha, beta = alpha_beta_vs_benchmark(rets, benchmark_returns)

        if volumes is not None and ticker in volumes.columns:
            v = volumes[ticker]
            adv = avg_dollar_volume(prices, v)
        else:
            adv = float("nan")

        rows.append({
            "Ticker": ticker,
            "Price": float(prices.iloc[-1]),
            "Momentum_12_1": momentum_12_1(prices),
            "Sortino": sortino_ratio(prices),
            "Alpha_Annualized": alpha,
            "Beta": beta,
            "CVaR_5": cvar_5(rets),
            "Max_Drawdown_252d": max_drawdown(prices),
            "Downside_Dev_126d": downside_deviation(rets),
            "Avg_Dollar_Vol_21d": adv,
        })

    if not rows:
        return pd.DataFrame(columns=["Ticker"] + FACTOR_COLUMNS).set_index("Ticker")

    return pd.DataFrame(rows).set_index("Ticker")
