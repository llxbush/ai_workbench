from __future__ import annotations

import pandas as pd


def prepare_stock_frame(
    bars: pd.DataFrame,
    dividend: pd.DataFrame | None,
) -> pd.DataFrame:
    if bars.empty:
        return bars

    df = bars.copy()
    df["ma120"] = df["close"].rolling(120).mean()
    df["next_return"] = df["close"].shift(-1) / df["close"] - 1

    if dividend is not None and not dividend.empty:
        merged = pd.merge_asof(
            df.sort_values("date"),
            dividend.sort_values("date"),
            on="date",
            direction="backward",
        )
        df["dividend_yield"] = merged["dividend_yield"]
    else:
        df["dividend_yield"] = pd.NA

    return df


def build_signals(
    stock_df: pd.DataFrame,
    min_dividend_yield: float,
    use_dividend_filter: bool,
    price_to_ma_ratio: float = 1.0,
) -> pd.DataFrame:
    if stock_df.empty:
        return stock_df

    signal = stock_df.copy()
    signal["ma_threshold"] = signal["ma120"] * price_to_ma_ratio
    signal["below_ma_threshold"] = signal["close"] < signal["ma_threshold"]

    if use_dividend_filter:
        signal["dividend_ok"] = signal["dividend_yield"].fillna(-1) >= min_dividend_yield
    else:
        signal["dividend_ok"] = True

    signal["selected"] = signal["below_ma_threshold"] & signal["dividend_ok"]
    return signal


def build_latest_screen_signal(
    stock_df: pd.DataFrame,
    min_dividend_yield: float,
    use_dividend_filter: bool,
    price_to_ma_ratio: float = 0.9,
    trade_date: pd.Timestamp | None = None,
) -> pd.Series | None:
    if stock_df.empty:
        return None

    signal_df = build_signals(
        stock_df=stock_df,
        min_dividend_yield=min_dividend_yield,
        use_dividend_filter=use_dividend_filter,
        price_to_ma_ratio=price_to_ma_ratio,
    )
    if trade_date is not None:
        signal_df = signal_df[signal_df["date"] <= trade_date]
    signal_df = signal_df.dropna(subset=["date", "close", "ma120"])
    if signal_df.empty:
        return None
    return signal_df.sort_values("date").iloc[-1].copy()
