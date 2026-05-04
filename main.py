from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import uvicorn

from src.quant_backtest.backtest import run_backtest
from src.quant_web.app import create_app
from src.quant_web.db import DEFAULT_DATABASE_URL, init_db
from src.quant_web.service import run_daily_update_and_ingest, run_screen


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="A-share simple backtest with AKShare")
    parser.add_argument(
        "--mode",
        choices=["backtest", "screen", "download-daily", "update-daily", "serve"],
        default="backtest",
    )
    parser.add_argument("--start", help="Start date, e.g. 2020-01-01")
    parser.add_argument("--end", help="End date, e.g. 2024-12-31")
    parser.add_argument("--trade-date", help="Screen date, e.g. 2026-04-08")
    parser.add_argument("--min-dividend-yield", type=float, default=5.0)
    parser.add_argument("--price-ma-ratio", type=float, default=0.9)
    parser.add_argument("--rebalance-days", type=int, default=20)
    parser.add_argument("--max-stocks", type=int, default=0, help="0 means all A-share stocks")
    parser.add_argument("--workers", type=int, default=8, help="Worker threads for daily data download/update")
    parser.add_argument("--skip-dividend-filter", action="store_true")
    parser.add_argument("--allow-loss-making", action="store_true")
    parser.add_argument("--cache-daily-bars", action="store_true", help="Persist daily bars in screen mode")
    parser.add_argument("--disable-local-daily-store", action="store_true", help="Deprecated; screen mode now reads from MySQL")
    parser.add_argument(
        "--skip-existing-store",
        action="store_true",
        help="Deprecated; daily sync now uses MySQL as the primary store",
    )
    parser.add_argument("--retry-failures-only", action="store_true", help="For download/update daily modes, retry only stocks recorded in the last failure manifest")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--output", help="Optional output CSV path for screen results")
    parser.add_argument(
        "--cache-dir",
        default=str(Path("data") / "cache"),
        help="Cache directory for downloaded data",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="MySQL database URL, e.g. mysql+pymysql://root:password@127.0.0.1:3306/quant?charset=utf8mb4",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host for web service")
    parser.add_argument("--port", type=int, default=8000, help="Port for web service")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.mode == "serve":
        init_db(database_url=args.database_url)
        app = create_app(database_url=args.database_url)
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if args.mode in {"download-daily", "update-daily"}:
        if not args.end:
            raise SystemExit("--mode download-daily/update-daily 时必须提供 --end")
        start_date = args.start or "2010-01-01"
        result = run_daily_update_and_ingest(
            start_date=start_date,
            end_date=args.end,
            max_stocks=args.max_stocks,
            workers=max(1, args.workers),
            database_url=args.database_url,
            cache_dir=Path(args.cache_dir),
        )
        print("\n=== MySQL 日线更新结果 ===")
        print(f"计划处理股票数: {result['processed']}")
        print(f"写入/更新行数: {result['database_upsert_rows']}")
        print(f"状态分布: {result['status_counts']}")
        print(f"数据库: {result['database_url']}")
        return

    if args.mode == "backtest":
        if not args.start or not args.end:
            raise SystemExit("--mode backtest 时必须提供 --start 和 --end")

        result = run_backtest(
            start_date=args.start,
            end_date=args.end,
            min_dividend_yield=args.min_dividend_yield,
            rebalance_days=args.rebalance_days,
            max_stocks=args.max_stocks,
            use_dividend_filter=not args.skip_dividend_filter,
            use_cache=not args.no_cache,
            cache_dir=Path(args.cache_dir),
        )

        print("\n=== 回测结果 ===")
        for key, value in result["metrics"].items():
            if isinstance(value, float):
                print(f"{key}: {value:.4f}")
            else:
                print(f"{key}: {value}")

        print("\n=== 数据提示 ===")
        for message in result["messages"]:
            print(f"- {message}")

        print("\n=== 最近几次调仓 ===")
        trades = result["rebalance_log"]
        if trades.empty:
            print("没有产生调仓记录。")
        else:
            print(trades.tail(10).to_string(index=False))
        return

    trade_date = args.trade_date or __import__("datetime").date.today().isoformat()
    result = run_screen(
        trade_date=trade_date,
        min_dividend_yield=args.min_dividend_yield,
        price_to_ma_ratio=args.price_ma_ratio,
        max_stocks=args.max_stocks,
        database_url=args.database_url,
    )

    print("\n=== 选股结果 ===")
    candidates = pd.DataFrame(result["candidates"])
    if candidates.empty:
        print("没有筛出满足条件的股票。")
    else:
        display_df = candidates.copy()
        numeric_cols = ["close", "ma120", "ma_threshold", "discount_vs_ma120", "dividend_yield", "pe_ttm", "market_cap"]
        for col in numeric_cols:
            if col in display_df.columns:
                display_df[col] = pd.to_numeric(display_df[col], errors="coerce").round(4)
        print(display_df.to_string(index=False))

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            candidates.to_csv(output_path, index=False)
            print(f"\n结果已保存到: {output_path}")

    print("\n=== 数据提示 ===")
    for message in result["messages"]:
        print(f"- {message}")


if __name__ == "__main__":
    main()
