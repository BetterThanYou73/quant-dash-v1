import numpy as np
import scipy.stats as stats
import pandas as pd

def calculate_metrics(price_series):

    returns = price_series.pct_change().dropna() # converts raw prices into percentages changes

    # 20-day rolling volatility (annualized)
    # it is a measure of how much the stock price fluctuates over time
    # higher volatility means more risk and lower means less
    volatility = returns.rolling(window=20).std() * np.sqrt(252)

    # current skewness over the last 20 days using scipy.stats
    # finding asymmetry of the return distribution
    current_skewness = returns.tail(20).skew()

    return returns, volatility, current_skewness


def calculate_tail_metrics(price_series, alpha=0.05):

    returns = price_series.pct_change().dropna() # converts raw prices into percent changes

    if returns.empty:
        return {
            "skew_full": float("nan"),
            "excess_kurtosis": float("nan"),
            "var_5": float("nan"),
            "cvar_5": float("nan"),
            "tail_ratio": float("nan"),
            "jb_pvalue": float("nan")
        }
    
    # distribution shape
    skew_full = float(returns.skew()) if pd.notna(returns.skew()) else float("nan")
    # bigger than 0 means fatter right tail, smaller than 0 means fatter left tail and 0 means symmetric
    excess_kurtosis = float(returns.kurtosis()) if pd.notna(returns.kurtosis()) else float("nan")

    # left tail risk (alpha is expected as decimal, e.g. 0.05 = 5th percentile)
    alpha_pct = float(alpha) * 100.0
    var_5 = float(np.percentile(returns, alpha_pct)) if pd.notna(np.percentile(returns, alpha_pct)) else float("nan")
    left_tail = returns[returns <= var_5]
    # expect stocs to lose more than var_5 5% of the time and cvar is the average loss in those worst 5% cases
    cvar_5 = float(left_tail.mean()) if not left_tail.empty and pd.notna(left_tail.mean()) else var_5

    # tail blaance -> higher can indicate better upside potential relative to downside risk
    q95 = np.percentile(returns, 100.0 - alpha_pct) if pd.notna(np.percentile(returns, 100.0 - alpha_pct)) else float("nan")
    q05 = np.percentile(returns, alpha_pct) if pd.notna(np.percentile(returns, alpha_pct)) else float("nan")
    tail_ratio = float(q95 / abs(q05)) if pd.notna(q95) and pd.notna(q05) and q05 != 0 else float("nan")

    # normality test using jarque bera test
    jb_result = stats.jarque_bera(returns.to_numpy(dtype=float))
    jb_pvalue = float(np.asarray(jb_result[1]).item())

    return {
    "skew_full": skew_full,
    "excess_kurtosis": excess_kurtosis,
    "var_5": var_5,
    "cvar_5": cvar_5,
    "tail_ratio": tail_ratio,
    "jb_pvalue": jb_pvalue,
    }