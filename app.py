import streamlit as st
import data_engine as de
import metrics
import plotly.express as px
import pandas as pd
import numpy as np
from numbers import Real

st.set_page_config(layout="wide", page_title="Quantitative Dashboard")
st.title("S&P 500 Quant Tracker")


# initializing a sessions state for custom tickers
if "custom_tickers" not in st.session_state:
    st.session_state.custom_tickers = []

# builds the sidebar for the user to enter a stock of their choice
st.sidebar.header("Control")
ticker_input = st.sidebar.text_input("Add a Stock (e.g, SPY, JPM) :").upper()

if st.sidebar.button("Add to Dasboard"):
    if ticker_input and ticker_input not in st.session_state.custom_tickers:
        st.session_state.custom_tickers.append(ticker_input)
        st.rerun() # refresh the data to reflect new stocks

st.sidebar.subheader("Analysis Settings")
analysis_lookback = st.sidebar.slider(
    "Lookback Window (trading days)",
    min_value=63,
    max_value=252,
    value=126,
    step=21,
)
corr_horizon_label = st.sidebar.selectbox(
    "Correlation Horizon",
    options=["1M (21d)", "3M (63d)", "6M (126d)", "1Y (252d)"],
    index=1,
)
rolling_corr_window = st.sidebar.slider(
    "Rolling Pair Corr Window",
    min_value=20,
    max_value=90,
    value=60,
    step=5,
)

@st.cache_data(ttl=3600) # cache the data for 1 hour to avoid redundant API calls
def load_market_data(custom_list) -> pd.DataFrame:
    # Fetching some of the most important stocks in the S&P for analysis
    # Can be customized to include other stocks from the list
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', "INTC", "AMD", "NVDA", "TSLA"]

    all_tickers = list(set(tickers + custom_list)) # combines both custom and curated list
    fetched = de.fetch_stock_data(all_tickers)
    if fetched is None:
        return pd.DataFrame()
    return fetched


data = load_market_data(st.session_state.custom_tickers)

# extracting closing prices and calulating returns
if data.empty:
    st.error("No valid market data returned. Please try again later.")
    st.stop()

if isinstance(data.columns, pd.MultiIndex):
    level0 = set(data.columns.get_level_values(0))
    level1 = set(data.columns.get_level_values(1))

    if "Close" in level0:
        # group_by='column' -> first level contains Open/High/Low/Close/Volume
        close_prices = data["Close"]
    elif "Close" in level1:
        # group_by='ticker' -> second level contains Open/High/Low/Close/Volume
        close_prices = data.xs("Close", axis=1, level=1)
    else:
        st.error("Downloaded data does not contain close prices.")
        st.stop()
else:
    if "Close" not in data.columns:
        st.error("Downloaded data does not contain close prices.")
        st.stop()
    close_prices = data["Close"]

if isinstance(close_prices, pd.Series):
    close_prices = close_prices.to_frame()

close_prices = close_prices.tail(analysis_lookback)

# Market Overview and Risk metrics
st.subheader("Market Overview & Risk Metrics")

summary_data = []
skipped_tickers = []
for col in close_prices:
    stock_series = close_prices[col].dropna()

    # calculate the tail metrics for the stock using the function from metrics module
    tail = metrics.calculate_tail_metrics(stock_series)
    
    # Skip tickers that have no usable history (common for invalid/delisted symbols)
    if stock_series.empty or len(stock_series) < 2:
        skipped_tickers.append(col)
        continue

    # calculates the returns, volatility, and skewness for each stock using the metrics module
    returns_series, vol_series, skewness = metrics.calculate_metrics(stock_series)

    if returns_series.empty:
        skipped_tickers.append(col)
        continue

    # gets us the most recent price, return, and volatility for the stock
    latest_price = stock_series.iloc[-1]
    daily_return = returns_series.iloc[-1]
    latest_volatility = vol_series.iloc[-1] 
    # vol series is a rolling window and this ets us the last one

    mom_5 = stock_series.pct_change(5).iloc[-1] if len(stock_series) > 5 else float("nan")
    mom_21 = stock_series.pct_change(21).iloc[-1] if len(stock_series) > 21 else float("nan")
    mom_63 = stock_series.pct_change(63).iloc[-1] if len(stock_series) > 63 else float("nan")
    hit_rate_21 = returns_series.tail(21).gt(0).mean() if len(returns_series) >= 21 else float("nan")
    rolling_peak_63 = stock_series.tail(63).cummax()
    drawdown_63 = ((stock_series.tail(63) / rolling_peak_63) - 1.0).min() if len(stock_series) >= 63 else float("nan")

    volatility_value = (
        float(latest_volatility)
        if pd.notna(latest_volatility) and isinstance(latest_volatility, Real)
        else float("nan")
    )
    skewness_value = (
        float(skewness)
        if pd.notna(skewness) and isinstance(skewness, Real)
        else float("nan")
    )

    summary_data.append({
        "Ticker": col,
        "Price": f"${latest_price:.2f}",
        "Daily Return Numeric": float(daily_return),
        "Daily Return": f"{daily_return:.2%}",
        "5d Momentum Numeric": mom_5,
        "21d Momentum Numeric": mom_21,
        "63d Momentum Numeric": mom_63,
        "20d Volatility Numeric": volatility_value,
        "20d Volatility": round(volatility_value, 3),
        "20d Skewness Numeric": skewness_value,
        "20d Skewness": round(skewness_value, 3),
        "Hit Rate 21d Numeric": hit_rate_21,
        "Max Drawdown 63d Numeric": drawdown_63,
        "5% VaR Numeric": tail["var_5"],
        "5% CVaR Numeric": tail["cvar_5"],
        "Tail Ratio Numeric": tail["tail_ratio"],
        "Excess Kurtosis Numeric": tail["excess_kurtosis"],
        "JB p-Value Numeric": tail["jb_pvalue"],

        "5% VaR": f"{tail['var_5']:.2%}" if pd.notna(tail["var_5"]) else "NA",
        "5% CVaR": f"{tail['cvar_5']:.2%}" if pd.notna(tail["cvar_5"]) else "NA",
        "Tail Ratio": round(tail["tail_ratio"], 3) if pd.notna(tail["tail_ratio"]) else "NA",
        "Excess Kurtosis": round(tail["excess_kurtosis"], 3) if pd.notna(tail["excess_kurtosis"]) else "NA",
        "JB p-Value": round(tail["jb_pvalue"], 4) if pd.notna(tail["jb_pvalue"]) else "NA",
        "21d Hit Rate": f"{hit_rate_21:.1%}" if pd.notna(hit_rate_21) else "NA",
        "63d Max Drawdown": f"{drawdown_63:.2%}" if pd.notna(drawdown_63) else "NA",
        })

def pct_rank(s):
    return s.rank(pct=True)

if not summary_data:
    st.error("No valid ticker data available for metric calculation.")
    if skipped_tickers:
        st.info(f"Skipped tickers with insufficient data: {', '.join(sorted(set(skipped_tickers)))}")
    st.stop()

# displaying as an interactive table
summary_df = pd.DataFrame(summary_data).sort_values(by="Daily Return Numeric", ascending=False)

for score_col in [
    "21d Momentum Numeric",
    "63d Momentum Numeric",
    "20d Volatility Numeric",
    "20d Skewness Numeric",
    "5% CVaR Numeric",
    "Max Drawdown 63d Numeric",
    "Hit Rate 21d Numeric",
]:
    summary_df[score_col] = summary_df[score_col].replace([np.inf, -np.inf], np.nan)
    summary_df[score_col] = summary_df[score_col].fillna(summary_df[score_col].median())

summary_df["Profitability Score"] = (
0.25 * pct_rank(summary_df["21d Momentum Numeric"]) +
0.20 * pct_rank(summary_df["63d Momentum Numeric"]) +
0.15 * pct_rank(-summary_df["20d Volatility Numeric"]) +
0.15 * pct_rank(summary_df["20d Skewness Numeric"]) +
0.10 * pct_rank(-summary_df["5% CVaR Numeric"].abs()) +
0.10 * pct_rank(-summary_df["Max Drawdown 63d Numeric"].abs()) +
0.05 * pct_rank(summary_df["Hit Rate 21d Numeric"])
)

summary_df["Profitability Score"] = summary_df["Profitability Score"].round(3)

summary_df["Signal"] = "Watch"
summary_df.loc[
    (summary_df["Profitability Score"] >= 0.7)
    & (summary_df["5% CVaR Numeric"] > -0.10)
    & (summary_df["Max Drawdown 63d Numeric"] > -0.20),
    "Signal",
] = "Long Candidate"
summary_df.loc[
    (summary_df["Profitability Score"] < 0.4)
    | (summary_df["5% CVaR Numeric"] <= -0.12)
    | (summary_df["Max Drawdown 63d Numeric"] <= -0.25),
    "Signal",
] = "High Risk"

summary_df = summary_df.sort_values(by="Profitability Score", ascending=False)

display_cols = [
"Ticker", "Signal", "Price", "Daily Return", "21d Hit Rate", "20d Volatility", "20d Skewness",
"63d Max Drawdown", "5% VaR", "5% CVaR", "Tail Ratio", "Excess Kurtosis", "JB p-Value",
"Profitability Score"
]

st.dataframe(summary_df[display_cols], use_container_width=True)

if skipped_tickers:
    st.warning(
        "Skipped tickers with insufficient/invalid data: "
        + ", ".join(sorted(set(skipped_tickers)))
    )


st.divider()



# creating correlation matrix
st.subheader("Correlation Matrix")
returns_df = close_prices.pct_change(fill_method=None)
returns_df = returns_df.dropna(axis=1, how="all").dropna(how="all")

if returns_df.empty:
    st.info("Not enough valid return history to compute a correlation matrix.")
    st.stop()

corr_horizon_map = {
    "1M (21d)": 21,
    "3M (63d)": 63,
    "6M (126d)": 126,
    "1Y (252d)": 252,
}

horizon_days = corr_horizon_map[corr_horizon_label]
horizon_returns = returns_df.tail(horizon_days)
horizon_returns = horizon_returns.dropna(axis=1, how="all").dropna(how="all")

if horizon_returns.shape[1] < 2:
    st.info("Need at least two valid tickers in the selected horizon for correlation analysis.")
    st.stop()

if horizon_returns.shape[1] == 1:
    corr_matrix = pd.DataFrame([[1.0]], index=horizon_returns.columns, columns=horizon_returns.columns)
else:
    corr_matrix = horizon_returns.corr()

st.caption(f"Static correlation computed on the selected horizon: {corr_horizon_label}")

fig = px.imshow(
    corr_matrix, 
    text_auto=True, 
    aspect="auto", 
    color_continuous_scale='RdYlGn'
)
fig.update_traces(texttemplate="%{z:.2f}")
st.plotly_chart(fig)

st.subheader("Rolling Pair Correlation")
valid_tickers = list(horizon_returns.columns)

default_left = valid_tickers.index("AMD") if "AMD" in valid_tickers else 0
if "NVDA" in valid_tickers and valid_tickers.index("NVDA") != default_left:
    default_right = valid_tickers.index("NVDA")
else:
    default_right = 1 if len(valid_tickers) > 1 else 0

pair_col_1, pair_col_2 = st.columns(2)
with pair_col_1:
    pair_left = st.selectbox("Pair Ticker A", options=valid_tickers, index=default_left)
with pair_col_2:
    pair_right = st.selectbox("Pair Ticker B", options=valid_tickers, index=default_right)

if pair_left == pair_right:
    st.info("Choose two different tickers for rolling correlation.")
else:
    pair_data = returns_df[[pair_left, pair_right]].dropna(how="any")
    rolling_corr = pair_data[pair_left].rolling(rolling_corr_window).corr(pair_data[pair_right]).dropna()

    if rolling_corr.empty:
        st.info("Not enough overlapping history for the selected pair and rolling window.")
    else:
        rolling_df = rolling_corr.rename("Rolling Correlation").reset_index()
        date_col = rolling_df.columns[0]
        rolling_fig = px.line(
            rolling_df,
            x=date_col,
            y="Rolling Correlation",
            title=f"{pair_left} vs {pair_right} Rolling Correlation ({rolling_corr_window}d)",
        )
        rolling_fig.add_hline(y=0.5, line_dash="dot", line_color="#7cc96f")
        rolling_fig.add_hline(y=0.2, line_dash="dot", line_color="#f2b84b")
        rolling_fig.add_hline(y=0.0, line_dash="dot", line_color="#ff6b6b")
        rolling_fig.update_layout(yaxis_range=[-1, 1])
        st.plotly_chart(rolling_fig, use_container_width=True)

        corr_metric_1, corr_metric_2, corr_metric_3 = st.columns(3)
        corr_metric_1.metric("Current Rolling Corr", f"{rolling_corr.iloc[-1]:.2f}")
        corr_metric_2.metric("21d Average Corr", f"{rolling_corr.tail(21).mean():.2f}")
        corr_metric_3.metric("63d Average Corr", f"{rolling_corr.tail(63).mean():.2f}")
