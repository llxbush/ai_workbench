from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import desc, func, or_, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.quant_backtest.data import MarketDataClient
from src.quant_backtest.strategy import build_latest_screen_signal, prepare_stock_frame

from .db import (
    DEFAULT_DATABASE_URL,
    DailyBar,
    Instrument,
    SyncRun,
    SyncRunItem,
    init_db,
    safe_database_url,
    session_scope,
)


SUCCESS_STATUSES = {"created", "updated", "up_to_date", "no_new_data", "suspended"}
UPSERT_BATCH_SIZE = 1000


def _upsert_daily_bars(
    session,
    symbol: str,
    adjust: str,
    bars: pd.DataFrame,
    name: str | None = None,
) -> int:
    if bars.empty:
        return 0

    normalized_symbol = _normalize_symbol(symbol)
    instrument_id = _get_or_create_instrument_id(
        session=session,
        symbol=normalized_symbol,
        name=name or normalized_symbol,
    )

    prepared = bars.copy()
    prepared["trade_date"] = pd.to_datetime(prepared["date"], errors="coerce").dt.date
    prepared = prepared.dropna(subset=["trade_date"])
    records = []
    for _, row in prepared.iterrows():
        records.append(
            {
                "instrument_id": instrument_id,
                "trade_date": row["trade_date"],
                "adjust_type": adjust,
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "close": _to_float(row.get("close")),
                "volume": _to_int(row.get("volume")),
                "amount": _to_float(row.get("amount")),
                "turnover": _to_float(row.get("turnover")),
                "data_source": "akshare_web",
            }
        )
    if not records:
        return 0

    for index in range(0, len(records), UPSERT_BATCH_SIZE):
        chunk = records[index : index + UPSERT_BATCH_SIZE]
        statement = mysql_insert(DailyBar).values(chunk)
        statement = statement.on_duplicate_key_update(
            open=statement.inserted.open,
            high=statement.inserted.high,
            low=statement.inserted.low,
            close=statement.inserted.close,
            volume=func.coalesce(statement.inserted.volume, DailyBar.volume),
            amount=func.coalesce(statement.inserted.amount, DailyBar.amount),
            turnover=func.coalesce(statement.inserted.turnover, DailyBar.turnover),
            data_source=statement.inserted.data_source,
            updated_at=func.now(),
        )
        session.execute(statement)
    return len(records)


def import_daily_store_to_db(
    database_url: str = DEFAULT_DATABASE_URL,
    daily_store_dir: Path | None = None,
    limit: int = 0,
) -> dict:
    init_db(database_url=database_url)
    daily_store_dir = daily_store_dir or Path("data") / "daily_store"
    files = sorted(daily_store_dir.glob("*_*.csv"))
    if limit and limit > 0:
        files = files[:limit]

    stock_names = _load_stock_names()
    run_id = _create_sync_run(
        database_url=database_url,
        run_type="import_daily_store",
        params={"limit": limit},
    )
    imported_files = 0
    imported_rows = 0

    try:
        with session_scope(database_url=database_url) as session:
            for csv_file in files:
                file_name = csv_file.stem
                if "_" not in file_name:
                    continue
                symbol, adjust = file_name.split("_", 1)
                symbol = _normalize_symbol(symbol)
                bars = pd.read_csv(csv_file, parse_dates=["date"])
                imported_rows += _upsert_daily_bars(
                    session=session,
                    symbol=symbol,
                    adjust=adjust,
                    bars=bars,
                    name=stock_names.get(symbol),
                )
                imported_files += 1
                session.commit()

            run = session.get(SyncRun, run_id)
            if run is not None:
                run.status = "completed"
                run.finished_at = datetime.now()
                run.total_symbols = imported_files
                run.success_symbols = imported_files
                run.message = f"imported_files={imported_files}, imported_rows={imported_rows}"
    except Exception as exc:
        _finish_sync_run(
            database_url=database_url,
            run_id=run_id,
            status="failed",
            message=_short_text(str(exc), 2048),
        )
        raise

    return {
        "imported_files": imported_files,
        "imported_rows": imported_rows,
        "database_url": safe_database_url(database_url),
    }


def run_daily_update_and_ingest(
    end_date: str,
    start_date: str = "2010-01-01",
    workers: int = 8,
    max_stocks: int = 0,
    database_url: str = DEFAULT_DATABASE_URL,
    cache_dir: Path | None = None,
) -> dict:
    init_db(database_url=database_url)
    cache_dir = cache_dir or Path("data") / "cache"
    client = MarketDataClient(cache_dir=cache_dir, use_cache=True)
    run_id = _create_sync_run(
        database_url=database_url,
        run_type="update_daily",
        start_date=start_date,
        end_date=end_date,
        params={
            "workers": workers,
            "max_stocks": max_stocks,
            "cache_dir": str(cache_dir),
        },
    )

    try:
        effective_end_date = client.resolve_effective_end_date(end_date)
        upserted_rows = 0
        rows: list[dict[str, Any]] = []
        with session_scope(database_url=database_url) as session:
            worklist = _build_daily_db_worklist(
                session=session,
                client=client,
                start_date=start_date,
                end_date=effective_end_date,
                max_stocks=max_stocks,
                adjust="qfq",
            )
            symbol_to_instrument_id: dict[str, int] = {}
            if not worklist.empty:
                if workers <= 1:
                    fetch_results = [
                        _fetch_daily_update_for_db(client=client, row=row, end_date=effective_end_date, adjust="qfq")
                        for _, row in worklist.iterrows()
                    ]
                else:
                    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
                        futures = [
                            executor.submit(
                                _fetch_daily_update_for_db,
                                client,
                                row,
                                effective_end_date,
                                "qfq",
                            )
                            for _, row in worklist.iterrows()
                        ]
                        fetch_results = [future.result() for future in as_completed(futures)]

                for result, bars in fetch_results:
                    if bars is not None and not bars.empty:
                        upserted_rows += _upsert_daily_bars(
                            session=session,
                            symbol=result["symbol"],
                            adjust="qfq",
                            bars=bars,
                            name=result.get("name"),
                        )
                        instrument_id = _get_or_create_instrument_id(
                            session=session,
                            symbol=result["symbol"],
                            name=result.get("name"),
                        )
                        symbol_to_instrument_id[result["symbol"]] = instrument_id
                        session.commit()
                    rows.append(result)

            result_df = pd.DataFrame(rows)

            _record_sync_run_items(
                session=session,
                run_id=run_id,
                result_df=result_df,
                symbol_to_instrument_id=symbol_to_instrument_id,
            )
            _finish_sync_run_in_session(
                session=session,
                run_id=run_id,
                status="completed",
                result_df=result_df,
                message=(
                    f"processed={len(result_df)}, upserted_rows={upserted_rows}, "
                    f"effective_end_date={effective_end_date}"
                ),
            )
    except Exception as exc:
        _finish_sync_run(
            database_url=database_url,
            run_id=run_id,
            status="failed",
            message=_short_text(str(exc), 2048),
        )
        raise

    status_counts = result_df["status"].value_counts().to_dict() if not result_df.empty else {}
    return {
        "processed": int(len(result_df)),
        "status_counts": status_counts,
        "database_upsert_rows": int(upserted_rows),
        "database_url": safe_database_url(database_url),
    }


def get_overview(database_url: str = DEFAULT_DATABASE_URL) -> dict:
    init_db(database_url=database_url)
    with session_scope(database_url=database_url) as session:
        total_rows = session.scalar(select(func.count()).select_from(DailyBar)) or 0
        total_symbols = session.scalar(select(func.count(func.distinct(DailyBar.instrument_id)))) or 0
        latest_trade_date = session.scalar(select(func.max(DailyBar.trade_date)))
        recent_runs = session.execute(
            select(SyncRun).order_by(SyncRun.started_at.desc()).limit(10)
        ).scalars().all()

    return {
        "database_url": safe_database_url(database_url),
        "total_rows": int(total_rows),
        "total_symbols": int(total_symbols),
        "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        "recent_runs": [
            {
                "task_name": run.run_type,
                "status": run.status,
                "started_at": run.started_at.isoformat(sep=" ") if run.started_at else "",
                "finished_at": run.finished_at.isoformat(sep=" ") if run.finished_at else "",
                "detail": run.message or "",
            }
            for run in recent_runs
        ],
    }


def search_stocks(
    query: str,
    limit: int = 10,
    database_url: str = DEFAULT_DATABASE_URL,
) -> dict:
    init_db(database_url=database_url)
    keyword = _clean_text(query)
    if not keyword:
        return {"query": "", "results": []}

    symbol_keyword = keyword.zfill(6) if keyword.isdigit() and len(keyword) <= 6 else keyword
    like_keyword = f"%{keyword}%"
    with session_scope(database_url=database_url) as session:
        rows = session.execute(
            select(Instrument)
            .where(
                or_(
                    Instrument.symbol == symbol_keyword,
                    Instrument.symbol.like(f"{keyword}%"),
                    Instrument.name.like(like_keyword),
                )
            )
            .order_by(desc(Instrument.symbol == symbol_keyword), Instrument.symbol)
            .limit(max(1, min(limit, 50)))
        ).scalars().all()

    return {
        "query": keyword,
        "results": [
            {
                "symbol": row.symbol,
                "name": row.name,
                "market": row.market,
                "exchange_code": row.exchange_code,
                "board": row.board,
            }
            for row in rows
        ],
    }


def get_stock_bars(
    query: str,
    adjust: str = "qfq",
    limit: int = 180,
    database_url: str = DEFAULT_DATABASE_URL,
) -> dict:
    init_db(database_url=database_url)
    instrument = _resolve_instrument(query=query, database_url=database_url)
    if instrument is None:
        return {
            "query": query,
            "instrument": None,
            "bars": [],
            "message": "没有找到匹配的股票",
        }

    row_limit = max(1, min(limit, 1200))
    with session_scope(database_url=database_url) as session:
        rows = session.execute(
            select(DailyBar)
            .where(
                DailyBar.instrument_id == instrument["id"],
                DailyBar.adjust_type == adjust,
            )
            .order_by(desc(DailyBar.trade_date))
            .limit(row_limit)
        ).scalars().all()

    bars = [
        {
            "date": row.trade_date.isoformat(),
            "open": _to_float(row.open),
            "high": _to_float(row.high),
            "low": _to_float(row.low),
            "close": _to_float(row.close),
            "volume": _to_int(row.volume),
            "amount": _to_float(row.amount),
            "turnover": _to_float(row.turnover),
        }
        for row in reversed(rows)
    ]
    latest = bars[-1] if bars else None
    previous = bars[-2] if len(bars) >= 2 else None
    change = None
    change_pct = None
    if latest and previous and previous["close"]:
        change = latest["close"] - previous["close"]
        change_pct = change / previous["close"] * 100

    return {
        "query": query,
        "instrument": {
            "symbol": instrument["symbol"],
            "name": instrument["name"],
            "market": instrument["market"],
            "exchange_code": instrument["exchange_code"],
            "board": instrument["board"],
        },
        "adjust": adjust,
        "count": len(bars),
        "latest": latest,
        "change": change,
        "change_pct": change_pct,
        "bars": bars,
    }


def run_screen(
    trade_date: str,
    min_dividend_yield: float = 5.0,
    price_to_ma_ratio: float = 0.9,
    max_stocks: int = 0,
    database_url: str = DEFAULT_DATABASE_URL,
) -> dict:
    result = _screen_latest_candidates_from_db(
        trade_date=trade_date,
        min_dividend_yield=min_dividend_yield,
        price_to_ma_ratio=price_to_ma_ratio,
        max_stocks=max_stocks,
        database_url=database_url,
    )
    candidates = result["candidates"].copy()
    candidates = candidates.where(pd.notna(candidates), None)
    return {
        "trade_date": result["trade_date"],
        "messages": result["messages"],
        "count": int(len(candidates)),
        "candidates": candidates.to_dict(orient="records"),
    }


def _build_daily_db_worklist(
    session,
    client: MarketDataClient,
    start_date: str,
    end_date: str,
    max_stocks: int = 0,
    adjust: str = "qfq",
) -> pd.DataFrame:
    stock_list = client.get_stock_list()
    if stock_list.empty:
        return pd.DataFrame(columns=["symbol", "name", "fetch_start", "latest_db_date"])

    instrument_rows = session.execute(select(Instrument.id, Instrument.symbol)).all()
    instrument_id_by_symbol = {symbol: int(instrument_id) for instrument_id, symbol in instrument_rows}
    latest_rows = session.execute(
        select(Instrument.symbol, func.max(DailyBar.trade_date), func.count(DailyBar.id))
        .join(DailyBar, DailyBar.instrument_id == Instrument.id)
        .where(DailyBar.adjust_type == adjust)
        .group_by(Instrument.symbol)
    ).all()
    latest_by_symbol = {symbol: latest_date for symbol, latest_date, _ in latest_rows}
    row_count_by_symbol = {symbol: int(row_count) for symbol, _, row_count in latest_rows}

    end_ts = pd.Timestamp(end_date)
    rows: list[dict[str, Any]] = []
    for _, row in stock_list.iterrows():
        symbol = _normalize_symbol(row["symbol"])
        name = _clean_text(row.get("name")) or symbol
        latest_db_date = latest_by_symbol.get(symbol)
        latest_ts = pd.Timestamp(latest_db_date) if latest_db_date is not None else None
        if latest_ts is not None and latest_ts >= end_ts:
            continue
        fetch_start = start_date if latest_ts is None else (latest_ts + timedelta(days=1)).strftime("%Y-%m-%d")
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "fetch_start": fetch_start,
                "latest_db_date": latest_ts.strftime("%Y-%m-%d") if latest_ts is not None else "",
                "download_reason": "missing_db" if latest_ts is None else "stale_db",
                "instrument_id": instrument_id_by_symbol.get(symbol),
                "existing_rows": row_count_by_symbol.get(symbol, 0),
            }
        )

    worklist = pd.DataFrame(rows)
    if not worklist.empty:
        worklist["latest_db_date_sort"] = pd.to_datetime(worklist["latest_db_date"], errors="coerce")
        worklist["download_reason_order"] = worklist["download_reason"].map(
            {"missing_db": 0, "stale_db": 1}
        ).fillna(9)
        worklist = worklist.sort_values(
            by=["download_reason_order", "latest_db_date_sort", "symbol"],
            kind="stable",
        ).drop(columns=["latest_db_date_sort", "download_reason_order"])
    if max_stocks and max_stocks > 0:
        worklist = worklist.head(max_stocks)
    return worklist.reset_index(drop=True)


def _fetch_daily_update_for_db(
    client: MarketDataClient,
    row,
    end_date: str,
    adjust: str = "qfq",
) -> tuple[dict[str, Any], pd.DataFrame | None]:
    symbol = _normalize_symbol(row["symbol"])
    name = _clean_text(row.get("name")) or symbol
    fetch_start = _clean_text(row.get("fetch_start"))
    before_latest_date = _clean_text(row.get("latest_db_date"))
    existing_rows = _to_int(row.get("existing_rows")) or 0
    base_result = {
        "symbol": symbol,
        "name": name,
        "planned_start_date": fetch_start,
        "before_latest_date": before_latest_date,
        "download_reason": _clean_text(row.get("download_reason")),
        "suspension_reason": "",
        "expected_resume_date": "",
        "error": "",
    }

    try:
        bars = client.get_daily_bars(
            symbol=symbol,
            start_date=fetch_start,
            end_date=end_date,
            adjust=adjust,
            persist_cache=False,
            raise_on_error=True,
        )
    except Exception as exc:
        suspended_info = client.get_suspension_info(symbol=symbol, date=end_date)
        if suspended_info is not None:
            return (
                {
                    **base_result,
                    "status": "suspended",
                    "rows_added": 0,
                    "total_rows": existing_rows,
                    "latest_date": before_latest_date,
                    "suspension_reason": suspended_info.get("reason", ""),
                    "expected_resume_date": suspended_info.get("expected_resume_date", ""),
                },
                None,
            )
        return (
            {
                **base_result,
                "status": "failed",
                "rows_added": 0,
                "total_rows": existing_rows,
                "latest_date": before_latest_date,
                "error": f"{type(exc).__name__}: {exc}",
            },
            None,
        )

    if bars.empty:
        suspended_info = client.get_suspension_info(symbol=symbol, date=end_date)
        if suspended_info is not None:
            return (
                {
                    **base_result,
                    "status": "suspended",
                    "rows_added": 0,
                    "total_rows": existing_rows,
                    "latest_date": before_latest_date,
                    "suspension_reason": suspended_info.get("reason", ""),
                    "expected_resume_date": suspended_info.get("expected_resume_date", ""),
                },
                None,
            )
        return (
            {
                **base_result,
                "status": "no_new_data" if existing_rows else "failed",
                "rows_added": 0,
                "total_rows": existing_rows,
                "latest_date": before_latest_date,
            },
            None,
        )

    prepared = bars.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared = prepared.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
    if before_latest_date:
        prepared = prepared[prepared["date"] > pd.Timestamp(before_latest_date)]
    prepared = prepared.sort_values("date").reset_index(drop=True)
    if prepared.empty:
        return (
            {
                **base_result,
                "status": "up_to_date",
                "rows_added": 0,
                "total_rows": existing_rows,
                "latest_date": before_latest_date,
            },
            None,
        )

    latest_date = pd.Timestamp(prepared["date"].max()).strftime("%Y-%m-%d")
    return (
        {
            **base_result,
            "status": "updated" if existing_rows else "created",
            "rows_added": int(len(prepared)),
            "total_rows": int(existing_rows + len(prepared)),
            "latest_date": latest_date,
        },
        prepared,
    )


def _screen_latest_candidates_from_db(
    trade_date: str,
    min_dividend_yield: float = 5.0,
    price_to_ma_ratio: float = 0.9,
    max_stocks: int = 0,
    database_url: str = DEFAULT_DATABASE_URL,
) -> dict:
    init_db(database_url=database_url)
    cache_dir = Path("data") / "cache"
    client = MarketDataClient(cache_dir=cache_dir, use_cache=True)
    trade_ts = pd.Timestamp(trade_date)
    start_date = (trade_ts - pd.Timedelta(days=240)).date()
    end_date = trade_ts.date()
    stock_list = client.get_stock_list()
    if max_stocks and max_stocks > 0:
        stock_list = stock_list.head(max_stocks)
    if stock_list.empty:
        raise RuntimeError("获取 A 股股票列表失败。请检查网络连接，或确认本机代理配置不会拦截 AKShare 请求。")

    rows: list[dict[str, Any]] = []
    messages: list[str] = []
    direct_dividend_count = 0
    estimated_dividend_count = 0
    valuation_count = 0
    profitable_count = 0
    history_success_count = 0
    history_failed_count = 0

    with session_scope(database_url=database_url) as session:
        instruments = session.execute(select(Instrument.id, Instrument.symbol)).all()
        instrument_id_by_symbol = {symbol: int(instrument_id) for instrument_id, symbol in instruments}
        for _, row in stock_list.iterrows():
            symbol = _normalize_symbol(row["symbol"])
            name = _clean_text(row.get("name")) or symbol
            instrument_id = instrument_id_by_symbol.get(symbol)
            if instrument_id is None:
                history_failed_count += 1
                continue
            bars = _get_daily_bars_from_db(
                session=session,
                instrument_id=instrument_id,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
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
                use_dividend_filter=True,
                price_to_ma_ratio=price_to_ma_ratio,
                trade_date=trade_ts,
            )
            if latest_signal is None or not bool(latest_signal["selected"]):
                continue
            if snapshot["is_profitable"] is not True:
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

    messages.append(f"筛选日期使用最新可得交易日，不晚于 {trade_ts.strftime('%Y-%m-%d')}。")
    messages.append(
        f"股票池共 {len(stock_list)} 只，成功从 MySQL 获取日线并完成 MA120 计算 {history_success_count} 只，失败或数据不足 {history_failed_count} 只。"
    )
    messages.append(f"股息率直连数据 {direct_dividend_count} 只，估算数据 {estimated_dividend_count} 只。")
    messages.append(f"PE/总市值估值数据可用 {valuation_count} 只。")
    messages.append(f"已启用非亏损过滤，可确认盈利的股票 {profitable_count} 只。")
    if estimated_dividend_count > 0:
        messages.append("部分股息率来自 `年均股息(按每 10 股换算到每股) / 最新收盘价 * 100` 的估算值，不是严格 TTM 股息率。")

    return {
        "trade_date": trade_ts.strftime("%Y-%m-%d"),
        "candidates": result_df,
        "messages": messages,
    }


def _get_daily_bars_from_db(
    session,
    instrument_id: int,
    start_date,
    end_date,
    adjust: str = "qfq",
) -> pd.DataFrame:
    rows = session.execute(
        select(DailyBar)
        .where(
            DailyBar.instrument_id == instrument_id,
            DailyBar.adjust_type == adjust,
            DailyBar.trade_date >= start_date,
            DailyBar.trade_date <= end_date,
        )
        .order_by(DailyBar.trade_date)
    ).scalars().all()
    if not rows:
        return pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume", "amount", "turnover"])
    return pd.DataFrame(
        [
            {
                "date": row.trade_date,
                "open": _to_float(row.open),
                "close": _to_float(row.close),
                "high": _to_float(row.high),
                "low": _to_float(row.low),
                "volume": _to_int(row.volume),
                "amount": _to_float(row.amount),
                "turnover": _to_float(row.turnover),
            }
            for row in rows
        ]
    )


def _create_sync_run(
    database_url: str,
    run_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
    params: dict[str, Any] | None = None,
) -> int:
    with session_scope(database_url=database_url) as session:
        run = SyncRun(
            run_type=run_type,
            status="running",
            triggered_by="web",
            target_date=_parse_date(end_date),
            start_date=_parse_date(start_date),
            end_date=_parse_date(end_date),
            params_json=params or {},
            message="running",
        )
        session.add(run)
        session.flush()
        return int(run.id)


def _finish_sync_run(
    database_url: str,
    run_id: int,
    status: str,
    message: str,
) -> None:
    with session_scope(database_url=database_url) as session:
        run = session.get(SyncRun, run_id)
        if run is None:
            return
        run.status = status
        run.finished_at = datetime.now()
        run.message = message


def _finish_sync_run_in_session(
    session,
    run_id: int,
    status: str,
    result_df: pd.DataFrame,
    message: str,
) -> None:
    run = session.get(SyncRun, run_id)
    if run is None:
        return
    total = int(len(result_df))
    success = int(result_df["status"].isin(SUCCESS_STATUSES).sum()) if not result_df.empty else 0
    failed = int((result_df["status"] == "failed").sum()) if not result_df.empty else 0
    run.status = status
    run.finished_at = datetime.now()
    run.total_symbols = total
    run.success_symbols = success
    run.failed_symbols = failed
    run.skipped_symbols = max(0, total - success - failed)
    run.message = _short_text(message, 2048)


def _record_sync_run_items(
    session,
    run_id: int,
    result_df: pd.DataFrame,
    symbol_to_instrument_id: dict[str, int],
) -> None:
    if result_df.empty:
        return

    for _, row in result_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        name = _clean_text(row.get("name")) or symbol
        instrument_id = symbol_to_instrument_id.get(symbol)
        if instrument_id is None:
            instrument_id = _get_or_create_instrument_id(session=session, symbol=symbol, name=name)
            symbol_to_instrument_id[symbol] = instrument_id

        session.add(
            SyncRunItem(
                run_id=run_id,
                instrument_id=instrument_id,
                symbol=symbol,
                name=_short_text(name, 64),
                status=_short_text(_clean_text(row.get("status")) or "unknown", 16),
                planned_start_date=_parse_date(row.get("planned_start_date")),
                latest_date=_parse_date(row.get("latest_date")),
                before_latest_date=_parse_date(row.get("before_latest_date")),
                rows_added=_to_int(row.get("rows_added")) or 0,
                total_rows=_to_int(row.get("total_rows")) or 0,
                download_reason=_short_text(_clean_text(row.get("download_reason")), 32),
                error_message=_clean_text(row.get("error")),
                suspension_reason=_short_text(_clean_text(row.get("suspension_reason")), 255),
                expected_resume_date=_parse_date(row.get("expected_resume_date")),
            )
        )


def _get_or_create_instrument_id(session, symbol: str, name: str | None = None) -> int:
    normalized_symbol = _normalize_symbol(symbol)
    instrument_id = session.scalar(select(Instrument.id).where(Instrument.symbol == normalized_symbol))
    if instrument_id is not None:
        return int(instrument_id)

    instrument = Instrument(
        symbol=normalized_symbol,
        name=_short_text(name or normalized_symbol, 64) or normalized_symbol,
        market=_infer_market(normalized_symbol),
        exchange_code=_infer_exchange(normalized_symbol),
        board=_infer_board(normalized_symbol),
        status="listed",
        is_active=True,
    )
    session.add(instrument)
    session.flush()
    return int(instrument.id)


def _resolve_instrument(query: str, database_url: str) -> dict | None:
    keyword = _clean_text(query)
    if not keyword:
        return None
    symbol_keyword = keyword.zfill(6) if keyword.isdigit() and len(keyword) <= 6 else keyword
    with session_scope(database_url=database_url) as session:
        row = session.execute(
            select(Instrument)
            .where(Instrument.symbol == symbol_keyword)
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            row = session.execute(
                select(Instrument)
                .where(Instrument.name.like(f"%{keyword}%"))
                .order_by(Instrument.symbol)
                .limit(1)
            ).scalar_one_or_none()
        if row is None:
            return None
        return {
            "id": int(row.id),
            "symbol": row.symbol,
            "name": row.name,
            "market": row.market,
            "exchange_code": row.exchange_code,
            "board": row.board,
        }


def _load_stock_names() -> dict[str, str]:
    stock_list_path = Path("data") / "cache" / "summary" / "stock_list.csv"
    if not stock_list_path.exists():
        return {}
    try:
        stock_df = pd.read_csv(stock_list_path, dtype={"symbol": str})
    except Exception:
        return {}
    if stock_df.empty or not {"symbol", "name"}.issubset(stock_df.columns):
        return {}
    stock_df["symbol"] = stock_df["symbol"].astype(str).str.zfill(6)
    return dict(zip(stock_df["symbol"], stock_df["name"]))


def _normalize_symbol(value: Any) -> str:
    text = _clean_text(value)
    if not text or text.lower() == "nan":
        return ""
    return text.zfill(6)


def _parse_date(value: Any):
    text = _clean_text(value)
    if not text:
        return None
    date_value = pd.to_datetime(text, errors="coerce")
    if pd.isna(date_value):
        return None
    return date_value.date()


def _to_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    return float(number)


def _to_int(value: Any) -> int | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    return int(float(number))


def _clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _short_text(value: str | None, max_length: int) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[:max_length]


def _infer_market(symbol: str) -> str:
    if symbol.startswith(("4", "8", "9")):
        return "BJ"
    if symbol.startswith("6"):
        return "SH"
    return "SZ"


def _infer_exchange(symbol: str) -> str:
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    if symbol.startswith("6"):
        return "SSE"
    return "SZSE"


def _infer_board(symbol: str) -> str:
    if symbol.startswith("688"):
        return "STAR"
    if symbol.startswith("300"):
        return "CHINEXT"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    if symbol.startswith("002"):
        return "SME"
    return "MAIN"
