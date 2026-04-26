"""
Multi-Factor Composite (MFC) signal generation.

Pipeline:
  1. core.factors.compute_factor_panel(...) → per-ticker raw factor values
  2. winsorize each factor at ±3σ to neutralize outliers
  3. z-score each factor cross-sectionally across the FULL universe
  4. equal-weight average → Composite_Z
  5. percentile-rank Composite_Z → Composite_Percentile (0–100)
  6. apply distribution-based label rules → "Strong Buy" / "Buy" / "Watch" / "Avoid" / "High Risk"

Why equal weights instead of optimized: DeMiguel, Garlappi & Uppal (2009)
showed that 1/N portfolios beat optimized weights out-of-sample because
estimation error in the optimizer dominates any in-sample fit. Same
phenomenon applies to factor weights.

Why z-scores cross-sectionally and not within the watchlist: ranks are only
meaningful when computed against a large reference universe (we use the full
S&P 500). Otherwise a stock's "score" depends on which other stocks happen
to be in the watchlist, which is nonsense.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from core import factors


# --- Helpers consumed by other modules (do not remove) -------------------

def extract_close_prices(data: pd.DataFrame) -> pd.DataFrame:
    """Pull the 'Close' columns out of a yfinance multi-ticker download.

    yfinance returns a DataFrame with a MultiIndex on the columns when you
    download multiple tickers. The structure depends on the `group_by` arg,
    so we defensively check both layouts and a single-level fallback.

    Used by routes_quote, routes_pairs, routes_risk via backend._helpers.
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

    if isinstance(close, pd.Series):
        close = close.to_frame()
    return close


def extract_volumes(data: pd.DataFrame) -> pd.DataFrame:
    """Pull 'Volume' columns from a yfinance multi-ticker download.

    Mirror of extract_close_prices for the Volume field. Used by the MFC
    pipeline to compute the average-dollar-volume liquidity factor.
    """
    if data.empty:
        return pd.DataFrame()

    if isinstance(data.columns, pd.MultiIndex):
        level0 = set(data.columns.get_level_values(0))
        level1 = set(data.columns.get_level_values(1))
        if "Volume" in level0:
            vol = data["Volume"]
        elif "Volume" in level1:
            vol = data.xs("Volume", axis=1, level=1)
        else:
            return pd.DataFrame()
    else:
        if "Volume" not in data.columns:
            return pd.DataFrame()
        vol = data["Volume"]

    if isinstance(vol, pd.Series):
        vol = vol.to_frame()
    return vol


# Kept for back-compat with old callers / tests. Not used by MFC.
MIN_HISTORY_DAYS = factors.MIN_HISTORY_DAYS


# --- Composite construction ----------------------------------------------

# Liquidity floor for "Buy"-class labels. Below this, the stock cannot be
# meaningfully accumulated without moving the price. $5M/day is a common
# heuristic for institutional-tradeable size; lower it to $1M for retail.
MIN_DOLLAR_VOLUME_BUY = 5_000_000


def winsorize_zscore(series: pd.Series, cap: float = 3.0) -> pd.Series:
    """Cross-sectional z-score, then clip extreme values at ±cap.

    Winsorization stops one wild outlier (e.g. a stock that just IPO'd
    and tripled) from dominating the composite for everyone else.
    """
    s = series.replace([np.inf, -np.inf], np.nan)
    mu = s.mean()
    sd = s.std(ddof=1)
    if pd.isna(sd) or sd == 0:
        return pd.Series(0.0, index=series.index)
    z = (s - mu) / sd
    return z.clip(lower=-cap, upper=cap)


def _classify_row(row: pd.Series, cvar_floor: float) -> str:
    """Apply the label rules to a single row of the panel.

    Distribution-based thresholds — adapt automatically to market state.
    A stock can never be both "Buy" and "High Risk"; rules are evaluated
    in priority order with the first match winning.
    """
    pct = row.get("Composite_Percentile")
    if pd.isna(pct):
        return "Insufficient Data"

    alpha = row.get("Alpha_Annualized", float("nan"))
    sortino = row.get("Sortino", float("nan"))
    cvar = row.get("CVaR_5", float("nan"))
    dd = row.get("Max_Drawdown_252d", float("nan"))
    adv = row.get("Avg_Dollar_Vol_21d", float("nan"))

    # --- High Risk: bottom decile AND tail loss in worst 5% of universe.
    # Both conditions must hold so we don't mark every cheap stock as
    # dangerous — only the ones with *demonstrated* tail damage.
    if pct <= 10 and pd.notna(cvar) and pd.notna(cvar_floor) and cvar <= cvar_floor:
        return "High Risk"

    # --- Avoid: weak score OR severe drawdown OR significantly underperforms SPY.
    if pct <= 25:
        return "Avoid"
    if pd.notna(dd) and dd <= -0.35:
        return "Avoid"
    if pd.notna(alpha) and alpha <= -0.05:  # losing >5%/yr to SPY
        return "Avoid"

    # --- Strong Buy: top decile + outperforming SPY + smooth uptrend + tradeable.
    if (
        pct >= 90
        and pd.notna(alpha) and alpha > 0
        and pd.notna(sortino) and sortino > 0
        and pd.notna(adv) and adv >= MIN_DOLLAR_VOLUME_BUY
    ):
        return "Strong Buy"

    # --- Buy: top quartile, with the same outperformance/quality gates.
    if (
        pct >= 75
        and pd.notna(alpha) and alpha > 0
        and pd.notna(sortino) and sortino > 0
    ):
        return "Buy"

    return "Watch"


def build_composite_signals(
    close_prices: pd.DataFrame,
    volumes: pd.DataFrame | None,
    benchmark_prices: pd.Series,
    watchlist: list[str] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Build the full signal table.

    Computes factor z-scores against the FULL `close_prices` universe
    (typically the S&P 500), then optionally filters the result to a
    watchlist subset. The ranks are stable regardless of watchlist —
    that's the whole point of computing them universe-wide.

    Returns
    -------
    signal_df : DataFrame with Ticker as a column. Includes raw factor
                values, z-scores, Composite_Z, Composite_Percentile, Signal.
                Sorted by Composite_Z descending.
    skipped   : list of tickers dropped due to insufficient history.
    """
    universe_tickers = list(close_prices.columns)

    panel = factors.compute_factor_panel(close_prices, volumes, benchmark_prices)
    skipped = sorted(set(universe_tickers) - set(panel.index))

    if panel.empty:
        return pd.DataFrame(), skipped

    # --- Z-scores. CVaR is negated because less-negative is better. ----
    # The z-score on a *negated* CVaR series means "the worse your tail
    # loss, the lower your z" which matches the other factors' convention
    # (higher z = better factor exposure).
    panel["z_momentum"] = winsorize_zscore(panel["Momentum_12_1"])
    panel["z_sortino"]  = winsorize_zscore(panel["Sortino"])
    panel["z_alpha"]    = winsorize_zscore(panel["Alpha_Annualized"])
    panel["z_cvar"]     = winsorize_zscore(-panel["CVaR_5"])

    # --- Composite: equal-weighted mean. ------------------------------
    # If a row is missing one factor we skipna so the others still count
    # — better than dropping the ticker entirely for one missing input.
    panel["Composite_Z"] = panel[["z_momentum", "z_sortino", "z_alpha", "z_cvar"]].mean(axis=1)
    panel["Composite_Percentile"] = panel["Composite_Z"].rank(pct=True) * 100.0

    # --- Distribution-based gates. -------------------------------------
    # The CVaR floor for "High Risk" is the 5th percentile of the universe's
    # CVaR distribution (i.e. the worst 5% of tails). Adapts to market state.
    cvar_floor = float(panel["CVaR_5"].quantile(0.05)) if panel["CVaR_5"].notna().any() else float("nan")

    panel["Signal"] = panel.apply(lambda row: _classify_row(row, cvar_floor), axis=1)

    # --- Optional watchlist filter -------------------------------------
    # Filtering happens AFTER ranks are computed so a watchlist-sized
    # universe doesn't distort the percentiles.
    if watchlist:
        wanted = {t.upper() for t in watchlist}
        panel = panel[panel.index.isin(wanted)]

    panel = panel.sort_values("Composite_Z", ascending=False)

    return panel.reset_index(), skipped


# --- Back-compat shim ----------------------------------------------------

# Old code in legacy/ may still call build_summary(). We don't use it from
# the new API. Soft-fail rather than silently returning an old shape.
def build_summary(*args, **kwargs):  # pragma: no cover
    raise NotImplementedError(
        "build_summary() was the old percentile-rank composite and has been "
        "replaced. Use signals.build_composite_signals() instead."
    )
