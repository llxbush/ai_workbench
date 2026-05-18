from __future__ import annotations

import argparse
import csv
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
    parser.add_argument("--skip-dividend", action="store_true")
    parser.add_argument("--skip-suspension", action="store_true")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    work_dir = repo_root / args.work_dir
    work_dir.mkdir(parents=True, exist_ok=True)

    run_mysql_file(args, repo_root / args.schema_file)
    if not args.skip_core:
        build_and_load_core_tables(args, repo_root, work_dir)
    if not args.skip_dividend:
        build_and_load_dividend_summary(args, repo_root, work_dir)
    if not args.skip_suspension:
        build_and_load_suspension_events(args, repo_root, work_dir)


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


def escape_sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "\\\\").replace("'", "\\'")


if __name__ == "__main__":
    main()
