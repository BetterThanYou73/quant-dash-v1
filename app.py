import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

import data_engine as de
import metrics


DEFAULT_WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "INTC", "AMD", "NVDA", "TSLA"]
CORR_HORIZON_MAP = {
    "1M (21d)": 21,
    "3M (63d)": 63,
    "6M (126d)": 126,
    "1Y (252d)": 252,
}


def pct_rank(series):
    return series.rank(pct=True)


def extract_close_prices(data):
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


def build_summary(close_prices):
    summary_data = []
    skipped_tickers = []

    for ticker in close_prices.columns:
        stock_series = close_prices[ticker].dropna()
        if stock_series.empty or len(stock_series) < 64:
            skipped_tickers.append(ticker)
            continue

        returns_series, vol_series, skewness = metrics.calculate_metrics(stock_series)
        if returns_series.empty:
            skipped_tickers.append(ticker)
            continue

        tail = metrics.calculate_tail_metrics(stock_series)

        latest_price = float(stock_series.iloc[-1])
        daily_return = float(returns_series.iloc[-1])
        latest_volatility = float(vol_series.iloc[-1]) if pd.notna(vol_series.iloc[-1]) else float("nan")
        skewness_value = float(skewness) if pd.notna(skewness) else float("nan")

        mom_21 = float(stock_series.pct_change(21).iloc[-1])
        mom_63 = float(stock_series.pct_change(63).iloc[-1])
        hit_rate_21 = float(returns_series.tail(21).gt(0).mean())
        rolling_peak_63 = stock_series.tail(63).cummax()
        drawdown_63 = float(((stock_series.tail(63) / rolling_peak_63) - 1.0).min())

        summary_data.append(
            {
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
            }
        )

    if not summary_data:
        return pd.DataFrame(), skipped_tickers

    summary_df = pd.DataFrame(summary_data)
    for col in [
        "21d Momentum Numeric",
        "63d Momentum Numeric",
        "20d Volatility Numeric",
        "20d Skewness Numeric",
        "5% CVaR Numeric",
        "Max Drawdown 63d Numeric",
        "Hit Rate 21d Numeric",
    ]:
        summary_df[col] = summary_df[col].replace([np.inf, -np.inf], np.nan)
        summary_df[col] = summary_df[col].fillna(summary_df[col].median())

    summary_df["Profitability Score"] = (
        0.25 * pct_rank(summary_df["21d Momentum Numeric"])
        + 0.20 * pct_rank(summary_df["63d Momentum Numeric"])
        + 0.15 * pct_rank(-summary_df["20d Volatility Numeric"])
        + 0.15 * pct_rank(summary_df["20d Skewness Numeric"])
        + 0.10 * pct_rank(-summary_df["5% CVaR Numeric"].abs())
        + 0.10 * pct_rank(-summary_df["Max Drawdown 63d Numeric"].abs())
        + 0.05 * pct_rank(summary_df["Hit Rate 21d Numeric"])
    )

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


st.set_page_config(layout="wide", page_title="Quantitative Dashboard V2")
st.title("S&P 500 Quant Tracker V2")

if "custom_tickers" not in st.session_state:
    st.session_state.custom_tickers = []

meta = de.get_ticker_metadata()
sector_options = sorted(meta["Sector"].dropna().unique().tolist())

st.sidebar.header("Control")
ticker_input = st.sidebar.text_input("Add a Stock (e.g, SPY, JPM):").upper().strip()
if st.sidebar.button("Add To Dashboard"):
    if ticker_input and ticker_input not in st.session_state.custom_tickers:
        st.session_state.custom_tickers.append(ticker_input)
        st.rerun()

if st.session_state.custom_tickers:
    st.sidebar.caption("Custom: " + ", ".join(st.session_state.custom_tickers))

if st.sidebar.button("Clear Custom Tickers"):
    st.session_state.custom_tickers = []
    st.rerun()

st.sidebar.subheader("Universe")
selected_sectors = st.sidebar.multiselect(
    "Sector Filter",
    options=sector_options,
    default=["Information Technology"],
)
max_sector_names = st.sidebar.slider("Max Sector Tickers", min_value=5, max_value=80, value=25, step=5)

st.sidebar.subheader("Analysis Settings")
analysis_lookback = st.sidebar.slider("Lookback Window (trading days)", min_value=63, max_value=252, value=126, step=21)
corr_horizon_label = st.sidebar.selectbox("Correlation Horizon", options=list(CORR_HORIZON_MAP.keys()), index=1)
rolling_corr_window = st.sidebar.slider("Rolling Pair Corr Window", min_value=20, max_value=120, value=60, step=5)
zscore_window = st.sidebar.slider("Spread Z-Score Window", min_value=20, max_value=120, value=30, step=5)
entry_threshold = st.sidebar.slider("Pair Entry |Z| Threshold", min_value=1.0, max_value=3.5, value=2.0, step=0.1)
exit_threshold = st.sidebar.slider("Pair Exit |Z| Threshold", min_value=0.1, max_value=1.5, value=0.5, step=0.1)

st.sidebar.subheader("Data Engine")
refresh_cache = st.sidebar.button("Refresh Local Cache Now")
st.sidebar.caption("For full decoupling, run: python data_worker.py")

sector_tickers = []
if selected_sectors:
    sector_tickers = meta.loc[meta["Sector"].isin(selected_sectors), "Symbol"].head(max_sector_names).tolist()

all_tickers = sorted(set(DEFAULT_WATCHLIST + st.session_state.custom_tickers + sector_tickers))

if refresh_cache:
    with st.spinner("Refreshing cache from market source..."):
        _, cache_ts = de.refresh_market_data_cache(all_tickers, period="1y")
        if cache_ts:
            st.sidebar.success("Cache refreshed")

data, cache_ts = de.load_cached_market_data()
if data.empty:
    with st.spinner("No local cache found. Fetching initial dataset..."):
        data, cache_ts = de.refresh_market_data_cache(all_tickers, period="1y")

if data.empty:
    st.error("No market data available in cache. Try refreshing cache.")
    st.stop()

close_prices = extract_close_prices(data)
if close_prices.empty:
    st.error("Cached data does not contain close prices.")
    st.stop()

close_prices = close_prices.reindex(columns=[t for t in all_tickers if t in close_prices.columns])
close_prices = close_prices.dropna(axis=1, how="all").tail(analysis_lookback)

if close_prices.empty or close_prices.shape[1] < 2:
    st.error("Need at least two valid tickers after filtering.")
    st.stop()

summary_df, skipped_tickers = build_summary(close_prices)
if summary_df.empty:
    st.error("No valid ticker data available after filtering.")
    st.stop()

sector_map = meta.set_index("Symbol")["Sector"].to_dict()
summary_df["Sector"] = summary_df["Ticker"].map(sector_map).fillna("Unknown")

top_signal = summary_df.iloc[0]
highest_vol = summary_df.loc[summary_df["20d Volatility Numeric"].idxmax()]
worst_cvar = summary_df.loc[summary_df["5% CVaR Numeric"].idxmin()]

card1, card2, card3, card4 = st.columns(4)
card1.metric("Top Candidate", f"{top_signal['Ticker']} ({top_signal['Signal']})", f"Score {top_signal['Profitability Score']:.3f}")
card2.metric("Highest Volatility", str(highest_vol["Ticker"]), f"{highest_vol['20d Volatility Numeric']:.3f}")
card3.metric("Worst Left Tail (CVaR)", str(worst_cvar["Ticker"]), f"{worst_cvar['5% CVaR Numeric']:.2%}")
card4.metric("Universe", f"{close_prices.shape[1]} Tickers", f"Cache: {cache_ts if cache_ts else 'unknown'}")

tab_overview, tab_pairs, tab_risk = st.tabs(["Market Overview", "Pairs Trading", "Risk"])

with tab_overview:
    st.subheader("Market Overview")
    display_df = summary_df.copy()
    display_df["Price"] = display_df["Price"].map(lambda x: f"${x:.2f}")
    display_df["Daily Return"] = display_df["Daily Return Numeric"].map(lambda x: f"{x:.2%}")
    display_df["20d Volatility"] = display_df["20d Volatility Numeric"].map(lambda x: f"{x:.3f}")
    display_df["20d Skewness"] = display_df["20d Skewness Numeric"].map(lambda x: f"{x:.3f}")
    display_df["5% VaR"] = display_df["5% VaR Numeric"].map(lambda x: f"{x:.2%}")
    display_df["5% CVaR"] = display_df["5% CVaR Numeric"].map(lambda x: f"{x:.2%}")
    display_df["63d Max Drawdown"] = display_df["Max Drawdown 63d Numeric"].map(lambda x: f"{x:.2%}")
    display_df["21d Hit Rate"] = display_df["Hit Rate 21d Numeric"].map(lambda x: f"{x:.1%}")

    st.dataframe(
        display_df[
            [
                "Ticker",
                "Sector",
                "Signal",
                "Price",
                "Daily Return",
                "21d Hit Rate",
                "20d Volatility",
                "20d Skewness",
                "63d Max Drawdown",
                "5% VaR",
                "5% CVaR",
                "Profitability Score",
            ]
        ],
        use_container_width=True,
    )

    scatter_fig = px.scatter(
        summary_df,
        x="20d Volatility Numeric",
        y="21d Momentum Numeric",
        color="Signal",
        hover_name="Ticker",
        size="Profitability Score",
        title="Risk vs 21d Momentum",
    )
    st.plotly_chart(scatter_fig, use_container_width=True)

    if skipped_tickers:
        st.warning("Skipped tickers with insufficient data: " + ", ".join(sorted(set(skipped_tickers))))

with tab_pairs:
    st.subheader("Pairs Trading Engine")
    pair_col1, pair_col2 = st.columns(2)
    with pair_col1:
        pair_a = st.selectbox("Ticker A", options=close_prices.columns.tolist(), index=0)
    with pair_col2:
        default_idx = 1 if close_prices.shape[1] > 1 else 0
        pair_b = st.selectbox("Ticker B", options=close_prices.columns.tolist(), index=default_idx)

    if pair_a == pair_b:
        st.info("Choose two different tickers.")
    else:
        series_a = close_prices[pair_a].dropna()
        series_b = close_prices[pair_b].dropna()
        beta = metrics.calculate_hedge_ratio(series_a, series_b)
        spread = metrics.calculate_spread(series_a, series_b, beta)
        zscore = metrics.rolling_zscore(spread, window=zscore_window).dropna()

        if spread.empty or zscore.empty:
            st.info("Not enough overlapping history to compute spread and z-score.")
        else:
            current_z = float(zscore.iloc[-1])
            signal = metrics.pair_signal(current_z, entry_threshold=entry_threshold, exit_threshold=exit_threshold)
            spread_mean = float(spread.mean())
            spread_std = float(spread.std())
            beta_valid = isinstance(beta, (int, float, np.floating)) and np.isfinite(beta)

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Hedge Ratio (beta)", f"{float(beta):.3f}" if beta_valid else "NA")
            m2.metric("Current Z-Score", f"{current_z:.2f}")
            m3.metric("Pair Signal", signal)
            m4.metric("Spread Regime", f"Mean {spread_mean:.2f} | Std {spread_std:.2f}")

            spread_fig = go.Figure()
            spread_fig.add_trace(go.Scatter(x=spread.index, y=spread.values, mode="lines", name="Spread"))
            spread_fig.update_layout(title=f"Spread: {pair_a} - beta*{pair_b}")
            st.plotly_chart(spread_fig, use_container_width=True)

            z_fig = go.Figure()
            z_fig.add_trace(go.Scatter(x=zscore.index, y=zscore.values, mode="lines", name="Z-Score"))
            z_fig.add_hline(y=entry_threshold, line_dash="dash", line_color="#d95f5f")
            z_fig.add_hline(y=-entry_threshold, line_dash="dash", line_color="#d95f5f")
            z_fig.add_hline(y=exit_threshold, line_dash="dot", line_color="#8dbf67")
            z_fig.add_hline(y=-exit_threshold, line_dash="dot", line_color="#8dbf67")
            z_fig.add_hline(y=0.0, line_dash="dot", line_color="#999999")
            z_fig.update_layout(title=f"Rolling Z-Score ({zscore_window}d window)")
            st.plotly_chart(z_fig, use_container_width=True)

            st.caption("Prescriptive rules: |Z| >= entry threshold suggests mean-reversion entry; |Z| <= exit threshold suggests closing.")

with tab_risk:
    st.subheader("Risk & Correlation")
    returns_df = close_prices.pct_change(fill_method=None).dropna(axis=1, how="all").dropna(how="all")
    if returns_df.empty:
        st.info("Not enough return history to compute risk analytics.")
    else:
        horizon_days = CORR_HORIZON_MAP[corr_horizon_label]
        horizon_returns = returns_df.tail(horizon_days).dropna(axis=1, how="all").dropna(how="all")

        if horizon_returns.shape[1] >= 2:
            corr_matrix = horizon_returns.corr()
            heatmap_fig = px.imshow(
                corr_matrix,
                text_auto=True,
                aspect="auto",
                color_continuous_scale="RdYlGn",
                title=f"Correlation Heatmap ({corr_horizon_label})",
            )
            heatmap_fig.update_traces(texttemplate="%{z:.2f}")
            st.plotly_chart(heatmap_fig, use_container_width=True)

            a, b = st.columns(2)
            with a:
                left = st.selectbox("Rolling Corr A", options=horizon_returns.columns.tolist(), key="risk_pair_a")
            with b:
                right = st.selectbox("Rolling Corr B", options=horizon_returns.columns.tolist(), index=1, key="risk_pair_b")

            if left != right:
                pair_ret = returns_df[[left, right]].dropna(how="any")
                roll_corr = pair_ret[left].rolling(rolling_corr_window).corr(pair_ret[right]).dropna()
                if not roll_corr.empty:
                    roll_fig = px.line(
                        roll_corr.reset_index(),
                        x=roll_corr.index.name or "index",
                        y=0,
                        title=f"Rolling Correlation: {left} vs {right} ({rolling_corr_window}d)",
                        labels={"0": "Correlation"},
                    )
                    roll_fig.update_layout(yaxis_range=[-1, 1])
                    st.plotly_chart(roll_fig, use_container_width=True)

                    r1, r2, r3 = st.columns(3)
                    r1.metric("Current", f"{roll_corr.iloc[-1]:.2f}")
                    r2.metric("21d Avg", f"{roll_corr.tail(21).mean():.2f}")
                    r3.metric("63d Avg", f"{roll_corr.tail(63).mean():.2f}")

        sector_risk = (
            summary_df.groupby("Sector", dropna=False)
            .agg(
                mean_score=("Profitability Score", "mean"),
                mean_vol=("20d Volatility Numeric", "mean"),
                mean_cvar=("5% CVaR Numeric", "mean"),
                names=("Ticker", "count"),
            )
            .reset_index()
            .sort_values("mean_score", ascending=False)
        )

        sector_fig = px.bar(
            sector_risk,
            x="Sector",
            y="mean_score",
            color="mean_cvar",
            title="Sector Context: Average Score Colored by CVaR",
        )
        st.plotly_chart(sector_fig, use_container_width=True)
