from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from .data import MarketDataClient
from .strategy import build_latest_screen_signal, build_signals, prepare_stock_frame


def run_backtest(
    start_date: str,
    end_date: str,
    min_dividend_yield: float = 5.0,
    rebalance_days: int = 20,
    max_stocks: int = 100,
    use_dividend_filter: bool = True,
    use_cache: bool = True,
    cache_dir: Path | None = None,
) -> dict:
    cache_dir = cache_dir or Path("data") / "cache"
    client = MarketDataClient(cache_dir=cache_dir, use_cache=use_cache)

    messages: list[str] = []
    stock_list = _limit_stock_list(client.get_stock_list(), max_stocks)
    if stock_list.empty:
        raise RuntimeError("获取 A 股股票列表失败。请检查网络连接，或确认本机代理配置不会拦截 AKShare 请求。")
    all_signals: list[pd.DataFrame] = []
    dividend_data_count = 0

    for _, row in stock_list.iterrows():
        symbol = row["symbol"]
        name = row["name"]
        bars = client.get_daily_bars(symbol=symbol, start_date=start_date, end_date=end_date)
        if bars.empty or len(bars) < 121:
            continue

        dividend = client.get_dividend_yield_series(symbol)
        if dividend is not None and not dividend.empty:
            dividend_data_count += 1

        prepared = prepare_stock_frame(bars=bars, dividend=dividend)
        signal = build_signals(
            stock_df=prepared,
            min_dividend_yield=min_dividend_yield,
            use_dividend_filter=use_dividend_filter and dividend is not None,
            price_to_ma_ratio=1.0,
        )
        signal["symbol"] = symbol
        signal["name"] = name
        all_signals.append(signal)

    if not all_signals:
        raise RuntimeError("没有拿到可用的股票数据，请检查日期区间或 AKShare 接口。")

    if use_dividend_filter and dividend_data_count == 0:
        messages.append("没有成功拿到股息率数据，已自动退化为只使用 MA120 条件。")
    elif use_dividend_filter:
        messages.append(f"共 {dividend_data_count} 只股票拿到了股息率数据，其余股票自动跳过股息率过滤。")
    else:
        messages.append("当前已显式跳过股息率过滤。")

    signal_df = pd.concat(all_signals, ignore_index=True)
    signal_df = signal_df.dropna(subset=["date", "next_return"]).sort_values(["date", "symbol"])

    date_groups: dict[pd.Timestamp, pd.DataFrame] = {
        date: frame.copy() for date, frame in signal_df.groupby("date")
    }
    trading_dates = sorted(date_groups.keys())
    rebalance_dates = trading_dates[::rebalance_days]

    portfolio_daily_returns: list[dict] = []
    rebalance_log: list[dict] = []
    holdings_by_period: dict[pd.Timestamp, list[str]] = defaultdict(list)

    for rebalance_date in rebalance_dates:
        frame = date_groups[rebalance_date]
        selected = frame[frame["selected"]].copy()
        symbols = selected["symbol"].tolist()
        holdings_by_period[rebalance_date] = symbols
        rebalance_log.append(
            {
                "rebalance_date": rebalance_date,
                "selected_count": len(symbols),
                "symbols": ",".join(symbols[:20]),
            }
        )

    active_holdings: list[str] = []
    rebalance_set = set(rebalance_dates)
    for current_date in trading_dates:
        if current_date in rebalance_set:
            active_holdings = holdings_by_period[current_date]

        frame = date_groups[current_date]
        if active_holdings:
            current_returns = frame[frame["symbol"].isin(active_holdings)]["next_return"]
            daily_return = float(current_returns.mean()) if not current_returns.empty else 0.0
        else:
            daily_return = 0.0

        portfolio_daily_returns.append(
            {
                "date": current_date,
                "daily_return": daily_return,
            }
        )

    portfolio_df = pd.DataFrame(portfolio_daily_returns).sort_values("date").reset_index(drop=True)
    portfolio_df["net_value"] = (1 + portfolio_df["daily_return"]).cumprod()

    metrics = _calculate_metrics(portfolio_df)
    metrics["stock_universe"] = int(len(stock_list))
    metrics["rebalance_days"] = int(rebalance_days)

    return {
        "metrics": metrics,
        "messages": messages,
        "portfolio": portfolio_df,
        "signals": signal_df,
        "rebalance_log": pd.DataFrame(rebalance_log),
    }


def screen_latest_candidates(
    trade_date: str,
    min_dividend_yield: float = 5.0,
    price_to_ma_ratio: float = 0.9,
    max_stocks: int = 0,
    use_dividend_filter: bool = True,
    require_profitable: bool = True,
    cache_daily_bars: bool = False,
    prefer_local_daily_store: bool = True,
    use_cache: bool = True,
    cache_dir: Path | None = None,
) -> dict:
    cache_dir = cache_dir or Path("data") / "cache"
    client = MarketDataClient(cache_dir=cache_dir, use_cache=use_cache)
    trade_ts = pd.Timestamp(trade_date)
    start_date = (trade_ts - pd.Timedelta(days=240)).strftime("%Y-%m-%d")
    end_date = trade_ts.strftime("%Y-%m-%d")

    stock_list = _limit_stock_list(client.get_stock_list(), max_stocks)
    if stock_list.empty:
        raise RuntimeError("获取 A 股股票列表失败。请检查网络连接，或确认本机代理配置不会拦截 AKShare 请求。")
    rows: list[dict] = []
    messages: list[str] = []
    direct_dividend_count = 0
    estimated_dividend_count = 0
    valuation_count = 0
    profitable_count = 0
    history_success_count = 0
    history_failed_count = 0

    for _, row in stock_list.iterrows():
        symbol = row["symbol"]
        name = row["name"]
        bars = pd.DataFrame()
        if prefer_local_daily_store:
            bars = client.get_bars_from_store(symbol=symbol, start_date=start_date, end_date=end_date)
        if bars.empty or len(bars) < 121:
            bars = client.get_daily_bars(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                persist_cache=cache_daily_bars,
            )
        if bars.empty or len(bars) < 121:
            history_failed_count += 1
            continue
        history_success_count += 1

        latest_bar = bars.sort_values("date").iloc[-1]
        snapshot = client.get_latest_snapshot(
            symbol=symbol,
            latest_date=pd.Timestamp(latest_bar["date"]),
            latest_close=float(latest_bar["close"]),
        )

        if snapshot["dividend_source"] == "direct":
            direct_dividend_count += 1
        elif snapshot["dividend_source"] == "estimated_from_avg_dividend_per_10_shares":
            estimated_dividend_count += 1
        if snapshot["valuation_source"] != "unavailable":
            valuation_count += 1
        if snapshot["profitability_source"] != "unavailable" and bool(snapshot["is_profitable"]):
            profitable_count += 1

        prepared = prepare_stock_frame(bars=bars, dividend=None)
        prepared["dividend_yield"] = snapshot["dividend_yield"]
        latest_signal = build_latest_screen_signal(
            stock_df=prepared,
            min_dividend_yield=min_dividend_yield,
            use_dividend_filter=use_dividend_filter,
            price_to_ma_ratio=price_to_ma_ratio,
            trade_date=trade_ts,
        )
        if latest_signal is None or not bool(latest_signal["selected"]):
            continue
        if require_profitable and snapshot["is_profitable"] is not True:
            continue

        rows.append(
            {
                "trade_date": pd.Timestamp(latest_signal["date"]).strftime("%Y-%m-%d"),
                "symbol": symbol,
                "name": name,
                "close": float(latest_signal["close"]),
                "ma120": float(latest_signal["ma120"]),
                "ma_threshold": float(latest_signal["ma_threshold"]),
                "discount_vs_ma120": float(latest_signal["close"] / latest_signal["ma120"] - 1),
                "dividend_yield": snapshot["dividend_yield"],
                "pe_ttm": snapshot["pe_ttm"],
                "market_cap": snapshot["market_cap"],
                "is_profitable": snapshot["is_profitable"],
                "dividend_source": snapshot["dividend_source"],
                "valuation_source": snapshot["valuation_source"],
                "profitability_source": snapshot["profitability_source"],
            }
        )

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        result_df = result_df.sort_values(
            by=["dividend_yield", "discount_vs_ma120"],
            ascending=[False, True],
        ).reset_index(drop=True)

    messages.append(
        f"筛选日期使用最新可得交易日，不晚于 {trade_ts.strftime('%Y-%m-%d')}。"
    )
    messages.append(
        f"股票池共 {len(stock_list)} 只，成功获取日线并完成 MA120 计算 {history_success_count} 只，失败或数据不足 {history_failed_count} 只。"
    )
    messages.append(
        f"股息率直连数据 {direct_dividend_count} 只，估算数据 {estimated_dividend_count} 只。"
    )
    messages.append(f"PE/总市值估值数据可用 {valuation_count} 只。")
    if require_profitable:
        messages.append(f"已启用非亏损过滤，可确认盈利的股票 {profitable_count} 只。")
    if estimated_dividend_count > 0:
        messages.append("部分股息率来自 `年均股息(按每 10 股换算到每股) / 最新收盘价 * 100` 的估算值，不是严格 TTM 股息率。")

    return {
        "trade_date": trade_ts.strftime("%Y-%m-%d"),
        "candidates": result_df,
        "messages": messages,
    }


def _limit_stock_list(stock_list: pd.DataFrame, max_stocks: int) -> pd.DataFrame:
    if max_stocks and max_stocks > 0:
        return stock_list.head(max_stocks)
    return stock_list


def _calculate_metrics(portfolio_df: pd.DataFrame) -> dict:
    if portfolio_df.empty:
        return {
            "total_return": 0.0,
            "annual_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "trading_days": 0,
        }

    daily_returns = portfolio_df["daily_return"]
    net_value = portfolio_df["net_value"]
    total_return = float(net_value.iloc[-1] - 1)
    trading_days = len(portfolio_df)
    annual_return = float((net_value.iloc[-1] ** (252 / trading_days)) - 1) if trading_days else 0.0

    rolling_max = net_value.cummax()
    drawdown = net_value / rolling_max - 1
    max_drawdown = float(drawdown.min())

    if daily_returns.std(ddof=0) == 0:
        sharpe = 0.0
    else:
        sharpe = float(np.sqrt(252) * daily_returns.mean() / daily_returns.std(ddof=0))

    return {
        "total_return": total_return,
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "trading_days": int(trading_days),
    }
