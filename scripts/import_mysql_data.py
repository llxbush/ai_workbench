from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Build quant MySQL schema and import local CSV data.")
    parser.add_argument("--mysql-bin", default="/usr/local/mysql/bin/mysql")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="quant")
    parser.add_argument("--schema-file", default="sql/schema_mysql.sql")
    parser.add_argument("--work-dir", default="tmp/mysql_import")
    parser.add_argument("--skip-core", action="store_true")
    parser.add_argument("--skip-daily-bars", action="store_true")
    parser.add_argument("--skip-indicators", action="store_true")
    parser.add_argument("--skip-dividend", action="store_true")
    parser.add_argument("--skip-suspension", action="store_true")
    parser.add_argument("--skip-sync-reports", action="store_true")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    work_dir = repo_root / args.work_dir
    work_dir.mkdir(parents=True, exist_ok=True)

    run_mysql_file(args, repo_root / args.schema_file)
    if not args.skip_core:
        build_and_load_core_tables(args, repo_root, work_dir)
    if not args.skip_daily_bars:
        build_and_load_daily_bars(args, repo_root, work_dir)
    if not args.skip_indicators:
        build_and_load_indicators(args, repo_root, work_dir)
    if not args.skip_dividend:
        build_and_load_dividend_summary(args, repo_root, work_dir)
    if not args.skip_suspension:
        build_and_load_suspension_events(args, repo_root, work_dir)
    if not args.skip_sync_reports:
        build_and_load_sync_reports(args, repo_root, work_dir)


def run_mysql_file(args, file_path: Path) -> None:
    sql = file_path.read_text(encoding="utf-8")
    subprocess.run(
        [
            args.mysql_bin,
            f"-u{args.user}",
            f"-p{args.password}",
            "--local-infile=1",
        ],
        input=sql,
        text=True,
        check=True,
    )


def run_mysql(args, sql: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            args.mysql_bin,
            f"-u{args.user}",
            f"-p{args.password}",
            "--local-infile=1",
            args.database,
            "-e",
            sql,
        ],
        check=True,
        text=True,
        capture_output=True,
    )


def load_local_file(args, table_name: str, file_path: Path, columns: list[str]) -> None:
    sql = f"""
LOAD DATA LOCAL INFILE '{escape_sql_path(file_path)}'
REPLACE INTO TABLE {table_name}
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\\t'
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\\r\\n'
IGNORE 1 LINES
({', '.join(columns)});
"""
    run_mysql(args, sql)


def build_and_load_core_tables(args, repo_root: Path, work_dir: Path) -> None:
    stock_df = pd.read_csv(repo_root / "data/cache/summary/stock_list.csv", dtype={"symbol": str})
    stock_df["symbol"] = stock_df["symbol"].astype(str).str.zfill(6)
    stock_df["market"] = stock_df["symbol"].map(infer_market)
    stock_df["exchange_code"] = stock_df["symbol"].map(infer_exchange)
    stock_df["board"] = stock_df["symbol"].map(infer_board)
    stock_df["status"] = "listed"
    stock_df["is_active"] = 1
    instruments_path = work_dir / "instruments.tsv"
    stock_df[["symbol", "name", "market", "exchange_code", "board", "status", "is_active"]].to_csv(
        instruments_path,
        sep="\t",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        na_rep="\\N",
        lineterminator="\r\n",
    )
    load_local_file(
        args,
        "instruments",
        instruments_path,
        ["symbol", "name", "market", "exchange_code", "board", "status", "is_active"],
    )

    calendar_df = pd.read_csv(repo_root / "data/cache/summary/trade_calendar.csv")
    calendar_df["exchange_code"] = "CN-A"
    calendar_df["is_open"] = 1
    calendar_path = work_dir / "trade_calendar.tsv"
    calendar_df[["trade_date", "exchange_code", "is_open"]].to_csv(
        calendar_path,
        sep="\t",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        na_rep="\\N",
        lineterminator="\r\n",
    )
    load_local_file(args, "trade_calendar", calendar_path, ["trade_date", "exchange_code", "is_open"])


def build_and_load_daily_bars(args, repo_root: Path, work_dir: Path) -> None:
    rows_written = 0
    raw_path = work_dir / "daily_bars_raw.tsv"
    daily_store_dir = repo_root / "data/daily_store"

    with raw_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(
            [
                "symbol",
                "trade_date",
                "adjust_type",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "turnover",
                "data_source",
            ]
        )
        for csv_file in sorted(daily_store_dir.glob("*_*.csv")):
            symbol, adjust_type = csv_file.stem.split("_", 1)
            df = pd.read_csv(csv_file, dtype={"date": str})
            if df.empty:
                continue
            for row in df.itertuples(index=False):
                writer.writerow(
                    [
                        str(symbol).zfill(6),
                        row.date,
                        adjust_type,
                        row.open,
                        row.high,
                        row.low,
                        row.close,
                        normalize_int(row.volume),
                        normalize_nullable(row.amount),
                        normalize_nullable(row.turnover),
                        "akshare_csv",
                    ]
                )
                rows_written += 1

    run_mysql(
        args,
        """
DROP TABLE IF EXISTS staging_daily_bars_raw;
CREATE TABLE staging_daily_bars_raw (
    symbol VARCHAR(16) NOT NULL,
    trade_date DATE NOT NULL,
    adjust_type VARCHAR(8) NOT NULL,
    open DECIMAL(18,4) NULL,
    high DECIMAL(18,4) NULL,
    low DECIMAL(18,4) NULL,
    close DECIMAL(18,4) NULL,
    volume BIGINT NULL,
    amount DECIMAL(20,2) NULL,
    turnover DECIMAL(12,4) NULL,
    data_source VARCHAR(32) NOT NULL
);
""",
    )
    load_local_file(
        args,
        "staging_daily_bars_raw",
        raw_path,
        ["symbol", "trade_date", "adjust_type", "open", "high", "low", "close", "volume", "amount", "turnover", "data_source"],
    )
    run_mysql(
        args,
        """
INSERT INTO daily_bars (
    instrument_id, trade_date, adjust_type, open, high, low, close, volume, amount, turnover, data_source
)
SELECT
    i.id, s.trade_date, s.adjust_type, s.open, s.high, s.low, s.close, s.volume, s.amount, s.turnover, s.data_source
FROM staging_daily_bars_raw s
JOIN instruments i ON i.symbol = s.symbol
ON DUPLICATE KEY UPDATE
    open = VALUES(open),
    high = VALUES(high),
    low = VALUES(low),
    close = VALUES(close),
    volume = VALUES(volume),
    amount = VALUES(amount),
    turnover = VALUES(turnover),
    data_source = VALUES(data_source),
    updated_at = CURRENT_TIMESTAMP;
DROP TABLE staging_daily_bars_raw;
""",
    )
    print(f"Imported daily bar rows: {rows_written}")


def build_and_load_indicators(args, repo_root: Path, work_dir: Path) -> None:
    indicator_path = work_dir / "daily_bar_indicators.tsv"
    daily_store_dir = repo_root / "data/daily_store"
    with indicator_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["symbol", "trade_date", "adjust_type", "ma5", "ma10", "ma20", "ma30", "ma60", "ma120", "ma250"])
        for csv_file in sorted(daily_store_dir.glob("*_*.csv")):
            symbol, adjust_type = csv_file.stem.split("_", 1)
            df = pd.read_csv(csv_file, usecols=["date", "close"], dtype={"date": str})
            if df.empty:
                continue
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["close"])
            if df.empty:
                continue
            df["ma5"] = df["close"].rolling(5).mean().round(4)
            df["ma10"] = df["close"].rolling(10).mean().round(4)
            df["ma20"] = df["close"].rolling(20).mean().round(4)
            df["ma30"] = df["close"].rolling(30).mean().round(4)
            df["ma60"] = df["close"].rolling(60).mean().round(4)
            df["ma120"] = df["close"].rolling(120).mean().round(4)
            df["ma250"] = df["close"].rolling(250).mean().round(4)
            for row in df.itertuples(index=False):
                writer.writerow(
                    [
                        str(symbol).zfill(6),
                        row.date,
                        adjust_type,
                        normalize_nullable(row.ma5),
                        normalize_nullable(row.ma10),
                        normalize_nullable(row.ma20),
                        normalize_nullable(row.ma30),
                        normalize_nullable(row.ma60),
                        normalize_nullable(row.ma120),
                        normalize_nullable(row.ma250),
                    ]
                )
    run_mysql(
        args,
        """
DROP TABLE IF EXISTS staging_daily_bar_indicators_raw;
CREATE TABLE staging_daily_bar_indicators_raw (
    symbol VARCHAR(16) NOT NULL,
    trade_date DATE NOT NULL,
    adjust_type VARCHAR(8) NOT NULL,
    ma5 DECIMAL(18,4) NULL,
    ma10 DECIMAL(18,4) NULL,
    ma20 DECIMAL(18,4) NULL,
    ma30 DECIMAL(18,4) NULL,
    ma60 DECIMAL(18,4) NULL,
    ma120 DECIMAL(18,4) NULL,
    ma250 DECIMAL(18,4) NULL
);
""",
    )
    load_local_file(
        args,
        "staging_daily_bar_indicators_raw",
        indicator_path,
        ["symbol", "trade_date", "adjust_type", "ma5", "ma10", "ma20", "ma30", "ma60", "ma120", "ma250"],
    )
    run_mysql(
        args,
        """
INSERT INTO daily_bar_indicators (
    instrument_id, trade_date, adjust_type, ma5, ma10, ma20, ma30, ma60, ma120, ma250
)
SELECT
    i.id, s.trade_date, s.adjust_type, s.ma5, s.ma10, s.ma20, s.ma30, s.ma60, s.ma120, s.ma250
FROM staging_daily_bar_indicators_raw s
JOIN instruments i ON i.symbol = s.symbol
ON DUPLICATE KEY UPDATE
    ma5 = VALUES(ma5),
    ma10 = VALUES(ma10),
    ma20 = VALUES(ma20),
    ma30 = VALUES(ma30),
    ma60 = VALUES(ma60),
    ma120 = VALUES(ma120),
    ma250 = VALUES(ma250),
    updated_at = CURRENT_TIMESTAMP;
DROP TABLE staging_daily_bar_indicators_raw;
""",
    )


def build_and_load_dividend_summary(args, repo_root: Path, work_dir: Path) -> None:
    path = repo_root / "data/cache/summary/stock_history_dividend.csv"
    if not path.exists():
        return
    df = pd.read_csv(path, dtype={"代码": str})
    df["symbol"] = df["代码"].astype(str).str.zfill(6)
    df["name"] = df["名称"]
    df["list_date"] = normalize_date_series(df["上市日期"])
    df["total_dividend"] = df["累计股息"]
    df["avg_annual_dividend"] = df["年均股息"]
    df["dividend_count"] = pd.to_numeric(df["分红次数"], errors="coerce").astype("Int64")
    df["total_financing"] = df["融资总额"]
    df["financing_count"] = pd.to_numeric(df["融资次数"], errors="coerce").astype("Int64")
    export_path = work_dir / "dividend_summary.tsv"
    df[
        [
            "symbol",
            "name",
            "list_date",
            "total_dividend",
            "avg_annual_dividend",
            "dividend_count",
            "total_financing",
            "financing_count",
        ]
    ].to_csv(
        export_path,
        sep="\t",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        na_rep="\\N",
        lineterminator="\r\n",
    )
    run_mysql(
        args,
        """
DROP TABLE IF EXISTS staging_dividend_summary_raw;
CREATE TABLE staging_dividend_summary_raw (
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NULL,
    list_date DATE NULL,
    total_dividend DECIMAL(18,4) NULL,
    avg_annual_dividend DECIMAL(18,4) NULL,
    dividend_count INT NULL,
    total_financing DECIMAL(18,4) NULL,
    financing_count INT NULL
);
""",
    )
    load_local_file(
        args,
        "staging_dividend_summary_raw",
        export_path,
        ["symbol", "name", "list_date", "total_dividend", "avg_annual_dividend", "dividend_count", "total_financing", "financing_count"],
    )
    run_mysql(
        args,
        """
INSERT INTO dividend_summary (
    instrument_id, symbol, name, list_date, total_dividend, avg_annual_dividend, dividend_count, total_financing, financing_count
)
SELECT
    i.id, s.symbol, s.name, s.list_date, s.total_dividend, s.avg_annual_dividend, s.dividend_count, s.total_financing, s.financing_count
FROM staging_dividend_summary_raw s
LEFT JOIN instruments i ON i.symbol = s.symbol
ON DUPLICATE KEY UPDATE
    instrument_id = VALUES(instrument_id),
    name = VALUES(name),
    list_date = VALUES(list_date),
    total_dividend = VALUES(total_dividend),
    avg_annual_dividend = VALUES(avg_annual_dividend),
    dividend_count = VALUES(dividend_count),
    total_financing = VALUES(total_financing),
    financing_count = VALUES(financing_count),
    updated_at = CURRENT_TIMESTAMP;
DROP TABLE staging_dividend_summary_raw;
""",
    )


def build_and_load_suspension_events(args, repo_root: Path, work_dir: Path) -> None:
    files = sorted((repo_root / "data/cache/summary").glob("suspension_*.csv"))
    if not files:
        return
    export_path = work_dir / "suspension_events.tsv"
    with export_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(
            [
                "symbol",
                "name",
                "snapshot_date",
                "suspend_start_date",
                "suspend_end_date",
                "suspend_reason",
                "market_name",
                "expected_resume_date",
            ]
        )
        for path in files:
            snapshot_match = re.search(r"suspension_(\d{4}-\d{2}-\d{2})\.csv$", path.name)
            if not snapshot_match:
                continue
            snapshot_date = snapshot_match.group(1)
            df = pd.read_csv(path, dtype={"代码": str})
            if df.empty:
                continue
            for row in df.itertuples(index=False):
                writer.writerow(
                    [
                        str(getattr(row, "代码")).zfill(6),
                        getattr(row, "名称", ""),
                        snapshot_date,
                        normalize_scalar_date(getattr(row, "停牌时间", "")),
                        normalize_scalar_date(getattr(row, "停牌截止时间", "")),
                        getattr(row, "停牌原因", ""),
                        getattr(row, "所属市场", ""),
                        normalize_scalar_date(getattr(row, "预计复牌时间", "")),
                    ]
                )
    run_mysql(
        args,
        """
DROP TABLE IF EXISTS staging_suspension_events_raw;
CREATE TABLE staging_suspension_events_raw (
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NULL,
    snapshot_date DATE NOT NULL,
    suspend_start_date DATE NULL,
    suspend_end_date DATE NULL,
    suspend_reason VARCHAR(255) NULL,
    market_name VARCHAR(64) NULL,
    expected_resume_date DATE NULL
);
""",
    )
    load_local_file(
        args,
        "staging_suspension_events_raw",
        export_path,
        [
            "symbol",
            "name",
            "snapshot_date",
            "suspend_start_date",
            "suspend_end_date",
            "suspend_reason",
            "market_name",
            "expected_resume_date",
        ],
    )
    run_mysql(
        args,
        """
INSERT INTO suspension_events (
    instrument_id, symbol, name, snapshot_date, suspend_start_date, suspend_end_date, suspend_reason, market_name, expected_resume_date
)
SELECT
    i.id, s.symbol, s.name, s.snapshot_date, s.suspend_start_date, s.suspend_end_date, s.suspend_reason, s.market_name, s.expected_resume_date
FROM staging_suspension_events_raw s
LEFT JOIN instruments i ON i.symbol = s.symbol
ON DUPLICATE KEY UPDATE
    instrument_id = VALUES(instrument_id),
    name = VALUES(name),
    suspend_start_date = VALUES(suspend_start_date),
    suspend_end_date = VALUES(suspend_end_date),
    suspend_reason = VALUES(suspend_reason),
    market_name = VALUES(market_name),
    expected_resume_date = VALUES(expected_resume_date),
    updated_at = CURRENT_TIMESTAMP;
DROP TABLE staging_suspension_events_raw;
""",
    )


def build_and_load_sync_reports(args, repo_root: Path, work_dir: Path) -> None:
    report_files = sorted((repo_root / "data/reports").glob("*.csv"))
    if not report_files:
        return

    runs = []
    items = []
    for run_index, path in enumerate(report_files, start=1):
        df = pd.read_csv(path, dtype={"symbol": str})
        if df.empty:
            continue
        run_type, target_date = infer_run_type_and_date(path.name)
        runs.append(
            {
                "import_run_id": run_index,
                "run_type": run_type,
                "source_file": path.name,
                "target_date": target_date,
                "status": summarize_run_status(df),
                "total_symbols": int(len(df)),
                "success_symbols": int(df["status"].isin(["created", "updated", "up_to_date", "no_new_data", "suspended"]).sum()) if "status" in df.columns else 0,
                "failed_symbols": int((df["status"] == "failed").sum()) if "status" in df.columns else 0,
                "skipped_symbols": 0,
                "params_json": json.dumps({"source_file": path.name}, ensure_ascii=False),
                "message": f"Imported from historical report {path.name}",
            }
        )
        for row in df.to_dict(orient="records"):
            items.append(
                {
                    "import_run_id": run_index,
                    "symbol": str(row.get("symbol", "")).zfill(6) if row.get("symbol") == row.get("symbol") else "",
                    "name": row.get("name", ""),
                    "status": row.get("status", "unknown"),
                    "planned_start_date": normalize_scalar_date(row.get("planned_start_date", "")),
                    "latest_date": normalize_scalar_date(row.get("latest_date", "")),
                    "before_latest_date": normalize_scalar_date(row.get("before_latest_date", "")),
                    "rows_added": normalize_int(row.get("rows_added", 0)),
                    "total_rows": normalize_int(row.get("total_rows", 0)),
                    "download_reason": row.get("download_reason", ""),
                    "error_message": row.get("error", ""),
                    "suspension_reason": row.get("suspension_reason", ""),
                    "expected_resume_date": normalize_scalar_date(row.get("expected_resume_date", "")),
                }
            )

    if not runs:
        return

    runs_path = work_dir / "sync_runs.tsv"
    pd.DataFrame(runs).to_csv(
        runs_path,
        sep="\t",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        na_rep="\\N",
        lineterminator="\r\n",
    )
    items_path = work_dir / "sync_run_items.tsv"
    pd.DataFrame(items).to_csv(
        items_path,
        sep="\t",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        na_rep="\\N",
        lineterminator="\r\n",
    )

    run_mysql(
        args,
        """
DELETE sri
FROM sync_run_items sri
JOIN sync_runs sr ON sr.id = sri.run_id
WHERE sr.triggered_by = 'migration';
DELETE FROM sync_runs WHERE triggered_by = 'migration';

DROP TABLE IF EXISTS staging_sync_runs_raw;
CREATE TABLE staging_sync_runs_raw (
    import_run_id INT NOT NULL,
    run_type VARCHAR(32) NOT NULL,
    source_file VARCHAR(255) NULL,
    target_date DATE NULL,
    status VARCHAR(16) NOT NULL,
    total_symbols INT NOT NULL,
    success_symbols INT NOT NULL,
    failed_symbols INT NOT NULL,
    skipped_symbols INT NOT NULL,
    params_json JSON NULL,
    message TEXT NULL
);
DROP TABLE IF EXISTS staging_sync_run_items_raw;
CREATE TABLE staging_sync_run_items_raw (
    import_run_id INT NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NULL,
    status VARCHAR(16) NOT NULL,
    planned_start_date DATE NULL,
    latest_date DATE NULL,
    before_latest_date DATE NULL,
    rows_added INT NOT NULL,
    total_rows INT NOT NULL,
    download_reason VARCHAR(32) NULL,
    error_message TEXT NULL,
    suspension_reason VARCHAR(255) NULL,
    expected_resume_date DATE NULL
);
""",
    )
    load_local_file(
        args,
        "staging_sync_runs_raw",
        runs_path,
        [
            "import_run_id",
            "run_type",
            "source_file",
            "target_date",
            "status",
            "total_symbols",
            "success_symbols",
            "failed_symbols",
            "skipped_symbols",
            "params_json",
            "message",
        ],
    )
    load_local_file(
        args,
        "staging_sync_run_items_raw",
        items_path,
        [
            "import_run_id",
            "symbol",
            "name",
            "status",
            "planned_start_date",
            "latest_date",
            "before_latest_date",
            "rows_added",
            "total_rows",
            "download_reason",
            "error_message",
            "suspension_reason",
            "expected_resume_date",
        ],
    )
    run_mysql(
        args,
        """
INSERT INTO sync_runs (
    run_type, source_file, target_date, status, triggered_by, total_symbols, success_symbols, failed_symbols, skipped_symbols, params_json, message, started_at, finished_at
)
SELECT
    run_type,
    source_file,
    target_date,
    status,
    'migration',
    total_symbols,
    success_symbols,
    failed_symbols,
    skipped_symbols,
    params_json,
    message,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM staging_sync_runs_raw
ORDER BY import_run_id;
""",
    )
    run_mysql(
        args,
        """
INSERT INTO sync_run_items (
    run_id, instrument_id, symbol, name, status, planned_start_date, latest_date, before_latest_date, rows_added, total_rows, download_reason, error_message, suspension_reason, expected_resume_date
)
SELECT
    sr.id,
    i.id,
    s.symbol,
    NULLIF(s.name, ''),
    s.status,
    s.planned_start_date,
    s.latest_date,
    s.before_latest_date,
    s.rows_added,
    s.total_rows,
    NULLIF(s.download_reason, ''),
    NULLIF(s.error_message, ''),
    NULLIF(s.suspension_reason, ''),
    s.expected_resume_date
FROM staging_sync_run_items_raw s
JOIN staging_sync_runs_raw r ON r.import_run_id = s.import_run_id
JOIN sync_runs sr
  ON sr.triggered_by = 'migration'
 AND sr.source_file = r.source_file
 AND sr.run_type = r.run_type
LEFT JOIN instruments i ON i.symbol = s.symbol;
DROP TABLE staging_sync_run_items_raw;
DROP TABLE staging_sync_runs_raw;
""",
    )


def infer_market(symbol: str) -> str:
    if symbol.startswith(("4", "8", "9")):
        return "CN-BJ"
    return "CN-A"


def infer_exchange(symbol: str) -> str:
    if symbol.startswith(("5", "6", "9")):
        return "SH"
    if symbol.startswith(("4", "8")):
        return "BJ"
    return "SZ"


def infer_board(symbol: str) -> str:
    if symbol.startswith(("688",)):
        return "STAR"
    if symbol.startswith(("300",)):
        return "ChiNext"
    if symbol.startswith(("8", "4", "9")):
        return "BSE"
    if symbol.startswith(("002", "003")):
        return "SME"
    return "Main"


def normalize_int(value) -> int:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return 0
    return int(float(number))


def normalize_scalar_date(value) -> str:
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return "\\N"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return "\\N"
    return parsed.strftime("%Y-%m-%d")


def normalize_date_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d").fillna("\\N")


def normalize_nullable(value):
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return "\\N"
    return number


def infer_run_type_and_date(file_name: str) -> tuple[str, str]:
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", file_name)
    target_date = date_match.group(1) if date_match else "\\N"
    if file_name.startswith("update_existing"):
        return "update_daily", target_date
    if file_name.startswith("retry_tail"):
        return "retry_failures", target_date
    if file_name.startswith("tail_recheck"):
        return "tail_recheck", target_date
    if file_name.startswith("daily_store_failures"):
        return "failure_manifest", target_date
    return "historical_report", target_date


def summarize_run_status(df: pd.DataFrame) -> str:
    if "status" not in df.columns:
        return "completed"
    if (df["status"] == "failed").any():
        return "partial_failed"
    return "completed"


def escape_sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "\\\\").replace("'", "\\'")


if __name__ == "__main__":
    main()
