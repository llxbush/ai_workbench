from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import func, select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.quant_data.providers.baostock_daily import BaostockDailyBarProvider
from src.quant_data.providers.daily_bars import MootdxDailyBarProvider
from src.quant_data.providers.tencent_quote import tencent_quote
from src.quant_web.db import DEFAULT_DATABASE_URL, DailyBar, Instrument, init_db, safe_database_url, session_scope
from src.quant_web.service import _upsert_daily_bars


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Repair daily bar volume/amount/turnover units.")
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    init_db(args.database_url)
    worklist = _build_worklist(
        database_url=args.database_url,
        start=args.start,
        end=args.end,
        symbols=args.symbol,
        max_symbols=args.max_symbols,
    )
    if not worklist:
        print("No symbols to repair.")
        return

    print(
        f"Repairing {len(worklist)} symbols from {args.start} to {args.end} "
        f"against {safe_database_url(args.database_url)}"
    )
    repaired = 0
    failed = 0
    rows_written = 0

    if args.workers <= 1:
        results = [_fetch_repair(symbol, name, args.start, args.end) for symbol, name in worklist]
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(_fetch_repair, symbol, name, args.start, args.end): (symbol, name)
                for symbol, name in worklist
            }
            results = [future.result() for future in as_completed(futures)]

    with session_scope(args.database_url) as session:
        for result in results:
            if result["error"]:
                failed += 1
                print(f"FAILED {result['symbol']} {result['error']}")
                continue
            bars = result["bars"]
            if bars.empty:
                failed += 1
                print(f"EMPTY {result['symbol']}")
                continue
            if not args.dry_run:
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
            print(f"OK {result['symbol']} rows={len(bars)} source={result['data_source']}")

    print(
        {
            "symbols": len(worklist),
            "repaired": repaired,
            "failed": failed,
            "rows_written": rows_written,
            "dry_run": args.dry_run,
        }
    )


def _build_worklist(
    database_url: str,
    start: str,
    end: str,
    symbols: list[str],
    max_symbols: int,
) -> list[tuple[str, str]]:
    start_date = pd.Timestamp(start).date()
    end_date = pd.Timestamp(end).date()
    normalized_symbols = [symbol.zfill(6) for symbol in symbols]
    with session_scope(database_url) as session:
        statement = (
            select(Instrument.symbol, Instrument.name)
            .join(DailyBar, DailyBar.instrument_id == Instrument.id)
            .where(DailyBar.trade_date >= start_date, DailyBar.trade_date <= end_date)
            .group_by(Instrument.symbol, Instrument.name)
            .order_by(Instrument.symbol)
        )
        if normalized_symbols:
            statement = statement.where(Instrument.symbol.in_(normalized_symbols))
        if max_symbols and max_symbols > 0:
            statement = statement.limit(max_symbols)
        return [(str(symbol).zfill(6), name or str(symbol).zfill(6)) for symbol, name in session.execute(statement).all()]


def _fetch_repair(symbol: str, name: str, start: str, end: str) -> dict[str, Any]:
    mootdx = MootdxDailyBarProvider()
    baostock = BaostockDailyBarProvider()
    try:
        price_result = mootdx.fetch_daily_bars(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
        bars = price_result.data.copy()
        if bars.empty:
            return {"symbol": symbol, "name": name, "bars": bars, "error": price_result.error or "mootdx empty"}

        bars, turnover_flags = _merge_baostock_turnover(bars=bars, provider=baostock, symbol=symbol, start=start, end=end)
        quote_flags = _apply_tencent_latest_quote(bars=bars, symbol=symbol)

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
            "data_source": "mootdx_unit_repair",
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
