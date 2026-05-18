from __future__ import annotations

import argparse
import multiprocessing as mp
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import case, func, select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.quant_data.providers.baostock_daily import BaostockDailyBarProvider
from src.quant_data.providers.daily_bars import EastmoneyDirectDailyBarProvider, MootdxDailyBarProvider
from src.quant_data.providers.tencent_quote import tencent_quote
from src.quant_web.db import DEFAULT_DATABASE_URL, DailyBar, Instrument, init_db, safe_database_url, session_scope
from src.quant_web.service import _refresh_instrument_status_from_stock_list, _upsert_daily_bars
from src.quant_backtest.data import MarketDataClient


DEFAULT_FETCH_RETRIES = 2
PRICE_FETCH_TIMEOUT_SECONDS = 45
BAOSTOCK_TIMEOUT_SECONDS = 20
TENCENT_TIMEOUT_SECONDS = 12
RETRYABLE_FETCH_MARKERS = (
    "Broken pipe",
    "接收数据异常",
    "Connection reset",
    "timed out",
    "Timeout",
    "EOF",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Repair daily bar volume/amount/turnover units.")
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--fetch-retries", type=int, default=DEFAULT_FETCH_RETRIES)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    init_db(args.database_url)
    _refresh_instruments_for_repair(database_url=args.database_url)
    worklist, skipped_completed = _build_worklist(
        database_url=args.database_url,
        start=args.start,
        end=args.end,
        symbols=args.symbol,
        max_symbols=args.max_symbols,
        resume=args.resume,
    )
    if not worklist:
        print("No symbols to repair.")
        return

    print(
        f"Repairing {len(worklist)} symbols from {args.start} to {args.end} "
        f"against {safe_database_url(args.database_url)}"
    )
    if args.resume:
        print(f"Resume mode: skipped {skipped_completed} already repaired symbols")
    print("Mode: streaming fetch -> upsert -> commit per symbol")
    repaired = 0
    failed = 0
    rows_written = 0
    failed_symbols: list[str] = []

    with session_scope(args.database_url) as session:
        if args.workers <= 1:
            for index, (symbol, name) in enumerate(worklist, start=1):
                print(f"[{index}/{len(worklist)}] START {symbol} {name}")
                result = _fetch_repair(symbol, name, args.start, args.end, fetch_retries=args.fetch_retries)
                repaired, failed, rows_written, failed_symbols = _apply_repair_result(
                    session=session,
                    result=result,
                    dry_run=args.dry_run,
                    repaired=repaired,
                    failed=failed,
                    rows_written=rows_written,
                    failed_symbols=failed_symbols,
                    index=index,
                    total=len(worklist),
                )
        else:
            with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
                futures = {
                    executor.submit(_fetch_repair, symbol, name, args.start, args.end, args.fetch_retries): (symbol, name)
                    for symbol, name in worklist
                }
                for index, future in enumerate(as_completed(futures), start=1):
                    result = future.result()
                    repaired, failed, rows_written, failed_symbols = _apply_repair_result(
                        session=session,
                        result=result,
                        dry_run=args.dry_run,
                        repaired=repaired,
                        failed=failed,
                        rows_written=rows_written,
                        failed_symbols=failed_symbols,
                        index=index,
                        total=len(worklist),
                    )

    print(
        {
            "symbols": len(worklist),
            "repaired": repaired,
            "failed": failed,
            "rows_written": rows_written,
            "failed_symbols": failed_symbols,
            "dry_run": args.dry_run,
        }
    )


def _apply_repair_result(
    session,
    result: dict[str, Any],
    *,
    dry_run: bool,
    repaired: int,
    failed: int,
    rows_written: int,
    failed_symbols: list[str],
    index: int,
    total: int,
) -> tuple[int, int, int, list[str]]:
    prefix = f"[{index}/{total}]"
    if result["error"]:
        failed += 1
        failed_symbols.append(result["symbol"])
        print(f"{prefix} FAILED {result['symbol']} {result['error']}")
        return repaired, failed, rows_written, failed_symbols
    bars = result["bars"]
    if bars.empty:
        failed += 1
        failed_symbols.append(result["symbol"])
        print(f"{prefix} EMPTY {result['symbol']}")
        return repaired, failed, rows_written, failed_symbols
    if not dry_run:
        if result.get("clear_turnover"):
            _clear_turnover_for_bars(session=session, symbol=result["symbol"], bars=bars)
        rows_written += _upsert_daily_bars(
            session=session,
            symbol=result["symbol"],
            adjust="qfq",
            bars=bars,
            name=result["name"],
            data_source=result["data_source"],
            quality_flags=result["quality_flags"],
        )
        session.commit()
    repaired += 1
    print(f"{prefix} OK {result['symbol']} rows={len(bars)} source={result['data_source']}")
    return repaired, failed, rows_written, failed_symbols


def _build_worklist(
    database_url: str,
    start: str,
    end: str,
    symbols: list[str],
    max_symbols: int,
    resume: bool,
) -> tuple[list[tuple[str, str]], int]:
    start_date = pd.Timestamp(start).date()
    end_date = pd.Timestamp(end).date()
    normalized_symbols = [symbol.zfill(6) for symbol in symbols]
    with session_scope(database_url) as session:
        repaired_rows = func.sum(case((DailyBar.data_source.like("%_unit_repair"), 1), else_=0))
        statement = (
            select(
                Instrument.symbol,
                Instrument.name,
                func.count(DailyBar.id).label("total_rows"),
                repaired_rows.label("repaired_rows"),
            )
            .join(DailyBar, DailyBar.instrument_id == Instrument.id)
            .where(
                DailyBar.trade_date >= start_date,
                DailyBar.trade_date <= end_date,
                DailyBar.adjust_type == "qfq",
            )
            .group_by(Instrument.symbol, Instrument.name)
            .order_by(Instrument.symbol)
        )
        if normalized_symbols:
            statement = statement.where(Instrument.symbol.in_(normalized_symbols))
        else:
            statement = statement.where(
                Instrument.is_active.is_(True),
                Instrument.status == "listed",
                ~Instrument.name.like("%退%"),
            )
        if resume:
            statement = statement.having(repaired_rows < func.count(DailyBar.id))
        if max_symbols and max_symbols > 0:
            statement = statement.limit(max_symbols)
        rows = session.execute(statement).all()
        worklist = [(str(symbol).zfill(6), name or str(symbol).zfill(6)) for symbol, name, _, _ in rows]

        skipped_completed = 0
        if resume:
            total_statement = (
                select(func.count(func.distinct(Instrument.symbol)))
                .join(DailyBar, DailyBar.instrument_id == Instrument.id)
                .where(
                    DailyBar.trade_date >= start_date,
                    DailyBar.trade_date <= end_date,
                    DailyBar.adjust_type == "qfq",
                )
            )
            if normalized_symbols:
                total_statement = total_statement.where(Instrument.symbol.in_(normalized_symbols))
            else:
                total_statement = total_statement.where(
                    Instrument.is_active.is_(True),
                    Instrument.status == "listed",
                    ~Instrument.name.like("%退%"),
                )
            total_symbols = int(session.scalar(total_statement) or 0)
            skipped_completed = max(0, total_symbols - len(worklist))
        return worklist, skipped_completed


def _fetch_repair(symbol: str, name: str, start: str, end: str, fetch_retries: int = DEFAULT_FETCH_RETRIES) -> dict[str, Any]:
    baostock = BaostockDailyBarProvider()
    try:
        price_result = _fetch_price_with_retry(
            symbol=symbol,
            start=start,
            end=end,
            fetch_retries=fetch_retries,
        )
        bars = price_result.data.copy()
        if bars.empty:
            return {"symbol": symbol, "name": name, "bars": bars, "error": price_result.error or "mootdx empty"}

        turnover_flags: list[str] = []
        if _needs_baostock_turnover_enrichment(bars=bars, price_source=price_result.source):
            print(f"FETCH {symbol} baostock_turnover")
            bars, turnover_flags = _run_with_timeout(
                BAOSTOCK_TIMEOUT_SECONDS,
                _merge_baostock_turnover,
                bars=bars,
                provider=baostock,
                symbol=symbol,
                start=start,
                end=end,
            )

        quote_flags: list[str] = []
        if _needs_tencent_latest_quote_enrichment(bars):
            print(f"FETCH {symbol} tencent_latest_quote")
            quote_flags = _run_with_timeout(TENCENT_TIMEOUT_SECONDS, _apply_tencent_latest_quote, bars=bars, symbol=symbol)

        quality_flags = sorted(
            set(
                price_result.quality_flags
                + ["unit_repair:volume_hands_amount_yuan"]
                + turnover_flags
                + quote_flags
            )
        )
        return {
            "symbol": symbol,
            "name": name,
            "bars": bars,
            "error": "",
            "data_source": f"{price_result.source}_unit_repair",
            "quality_flags": quality_flags,
            "clear_turnover": True,
        }
    except Exception as exc:
        return {
            "symbol": symbol,
            "name": name,
            "bars": pd.DataFrame(),
            "error": f"{type(exc).__name__}: {exc}",
            "data_source": "",
            "quality_flags": [],
            "clear_turnover": False,
        }


def _fetch_price_with_retry(symbol: str, start: str, end: str, fetch_retries: int) -> Any:
    last_result = None
    if _is_bj_symbol(symbol):
        print(f"FETCH {symbol} eastmoney_direct")
        fallback_provider = EastmoneyDirectDailyBarProvider(timeout_seconds=12.0)
        try:
            fallback_result = fallback_provider.fetch_daily_bars(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
        except Exception as exc:
            fallback_result = _empty_provider_result("eastmoney_direct", f"{type(exc).__name__}: {exc}")
        if fallback_result.data is not None and not fallback_result.data.empty:
            return fallback_result
        last_result = fallback_result
    else:
        print(f"FETCH {symbol} baostock")
        baostock_provider = BaostockDailyBarProvider()
        baostock_result = _run_with_timeout(
            BAOSTOCK_TIMEOUT_SECONDS,
            baostock_provider.fetch_daily_bars,
            symbol=symbol,
            start_date=start,
            end_date=end,
            adjust="qfq",
        )
        if baostock_result.data is not None and not baostock_result.data.empty:
            return baostock_result
        last_result = baostock_result
    for attempt in range(max(0, fetch_retries) + 1):
        if attempt > 0:
            print(f"RETRY {symbol} mootdx attempt={attempt + 1}/{fetch_retries + 1}")
        result = _fetch_mootdx_with_timeout(
            symbol=symbol,
            start=start,
            end=end,
            timeout_seconds=PRICE_FETCH_TIMEOUT_SECONDS,
        )
        last_result = result
        if result.data is not None and not result.data.empty:
            return result
        if not _is_retryable_fetch_error(result.error):
            return result
        if attempt < fetch_retries:
            time.sleep(0.8 * (attempt + 1))
    print(f"FALLBACK {symbol} eastmoney_direct")
    fallback_provider = EastmoneyDirectDailyBarProvider(timeout_seconds=12.0)
    try:
        fallback_result = fallback_provider.fetch_daily_bars(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
    except Exception as exc:
        fallback_result = _empty_provider_result("eastmoney_direct", f"{type(exc).__name__}: {exc}")
    if fallback_result.data is not None and not fallback_result.data.empty:
        return fallback_result
    if fallback_result is not None:
        return fallback_result
    return last_result if last_result is not None else _empty_provider_result("mootdx", "unknown fetch failure")


def _is_retryable_fetch_error(error: str | None) -> bool:
    if not error:
        return False
    return any(marker.lower() in error.lower() for marker in RETRYABLE_FETCH_MARKERS)


def _run_with_timeout(timeout_seconds: int, func, /, *args, **kwargs):
    if timeout_seconds <= 0:
        return func(*args, **kwargs)
    if not hasattr(signal, "setitimer"):
        return func(*args, **kwargs)

    def _handle_timeout(signum, frame):
        raise TimeoutError(f"{func.__name__} exceeded {timeout_seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        return func(*args, **kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _fetch_mootdx_with_timeout(symbol: str, start: str, end: str, timeout_seconds: int):
    ctx = mp.get_context("spawn")
    queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_mootdx_fetch_worker,
        args=(queue, symbol, start, end),
        daemon=True,
    )
    process.start()
    process.join(timeout_seconds)
    if process.is_alive():
        process.terminate()
        process.join(timeout=1)
        if process.is_alive():
            process.kill()
            process.join(timeout=1)
        queue.close()
        return _empty_provider_result("mootdx", f"TimeoutError: mootdx fetch exceeded {timeout_seconds}s")

    result = None
    try:
        if not queue.empty():
            result = queue.get_nowait()
    except Exception:
        result = None
    finally:
        queue.close()

    if result is None:
        return _empty_provider_result("mootdx", f"RuntimeError: mootdx worker exited unexpectedly (exitcode={process.exitcode})")
    return result


def _mootdx_fetch_worker(queue, symbol: str, start: str, end: str) -> None:
    provider = MootdxDailyBarProvider()
    result = provider.fetch_daily_bars(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
    queue.put(result)


def _empty_provider_result(source: str, error: str):
    from src.quant_data.providers.base import ProviderResult

    return ProviderResult(data=pd.DataFrame(), source=source, quality_flags=["empty"], error=error)


def _is_bj_symbol(symbol: str) -> bool:
    return str(symbol).zfill(6).startswith(("4", "8", "9"))


def _needs_baostock_turnover_enrichment(*, bars: pd.DataFrame, price_source: str) -> bool:
    if bars.empty:
        return False
    if price_source == "baostock":
        return False
    if "turnover" not in bars.columns:
        return True
    turnover = pd.to_numeric(bars["turnover"], errors="coerce")
    return turnover.isna().any()


def _needs_tencent_latest_quote_enrichment(bars: pd.DataFrame) -> bool:
    if bars.empty or "date" not in bars.columns:
        return False
    latest_index = bars["date"].idxmax()
    latest_row = bars.loc[latest_index]
    turnover = pd.to_numeric(pd.Series([latest_row.get("turnover")]), errors="coerce").iloc[0]
    amount = pd.to_numeric(pd.Series([latest_row.get("amount")]), errors="coerce").iloc[0]
    return pd.isna(turnover) or pd.isna(amount) or amount <= 0


def _refresh_instruments_for_repair(database_url: str) -> None:
    client = MarketDataClient(cache_dir=Path("data") / "cache", use_cache=True)
    stock_list = client.get_stock_list()
    if stock_list.empty:
        return
    with session_scope(database_url) as session:
        summary = _refresh_instrument_status_from_stock_list(session=session, stock_list=stock_list)
    print(
        "Instrument refresh:",
        {
            "active_symbols": summary["active"],
            "new_instruments": summary["created"],
            "updated_instruments": summary["updated"],
            "marked_inactive": summary["inactive"],
        },
    )


def _clear_turnover_for_bars(session, symbol: str, bars: pd.DataFrame) -> None:
    if bars.empty:
        return
    instrument_id = session.scalar(select(Instrument.id).where(Instrument.symbol == symbol.zfill(6)))
    if instrument_id is None:
        return
    dates = pd.to_datetime(bars["date"], errors="coerce").dropna().dt.date.tolist()
    if not dates:
        return
    session.query(DailyBar).filter(
        DailyBar.instrument_id == instrument_id,
        DailyBar.adjust_type == "qfq",
        DailyBar.trade_date.in_(dates),
    ).update({DailyBar.turnover: None}, synchronize_session=False)


def _merge_baostock_turnover(
    bars: pd.DataFrame,
    provider: BaostockDailyBarProvider,
    symbol: str,
    start: str,
    end: str,
) -> tuple[pd.DataFrame, list[str]]:
    if bars.empty:
        return bars, []
    try:
        result = provider.fetch_daily_bars(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
    except Exception:
        result = None
    if result is None or result.data.empty or "turnover" not in result.data.columns:
        if "turnover" not in bars.columns:
            bars["turnover"] = pd.NA
        return bars, ["baostock_turnover_unavailable"]

    turnover = result.data[["date", "turnover"]].dropna(subset=["turnover"]).copy()
    merged = bars.drop(columns=["turnover"], errors="ignore").merge(turnover, on="date", how="left")
    return merged, ["baostock_turnover_history_enriched"]


def _apply_tencent_latest_quote(bars: pd.DataFrame, symbol: str) -> list[str]:
    if bars.empty:
        return []
    try:
        quote = tencent_quote([symbol]).get(symbol.zfill(6))
    except Exception:
        return ["tencent_quote_failed"]
    if not quote:
        return ["tencent_quote_empty"]

    latest_index = bars["date"].idxmax()
    latest_date = pd.Timestamp(bars.loc[latest_index, "date"]).date()
    quote_date_text = quote.get("quote_date")
    if not quote_date_text:
        return ["tencent_quote_missing_date"]
    if latest_date != pd.Timestamp(quote_date_text).date():
        return ["tencent_quote_not_applied_to_historical_date"]

    quote_turnover = quote.get("turnover_pct")
    quote_amount = quote.get("amount_yuan")
    if quote_turnover is not None:
        bars.loc[latest_index, "turnover"] = quote_turnover
    if quote_amount is not None and quote_amount > 0:
        bars.loc[latest_index, "amount"] = quote_amount
    return ["tencent_quote_latest_enriched"]


if __name__ == "__main__":
    main()
