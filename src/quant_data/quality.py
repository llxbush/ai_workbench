from __future__ import annotations

import pandas as pd


REQUIRED_DAILY_BAR_COLUMNS = ("date", "open", "close", "high", "low")


def assess_daily_bars(
    bars: pd.DataFrame,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    flags: list[str] = []
    if bars is None or bars.empty:
        return ["empty"]

    missing = [column for column in REQUIRED_DAILY_BAR_COLUMNS if column not in bars.columns]
    if missing:
        flags.append("missing_required_columns:" + "|".join(missing))
        return flags

    df = bars.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().any():
        flags.append("invalid_date")
    if df["date"].duplicated().any():
        flags.append("duplicate_dates")

    for column in ("open", "close", "high", "low"):
        values = pd.to_numeric(df[column], errors="coerce")
        if values.isna().any():
            flags.append(f"null_{column}")
        if (values <= 0).any():
            flags.append(f"non_positive_{column}")

    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    open_ = pd.to_numeric(df["open"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    if ((high < low) | (high < open_) | (high < close) | (low > open_) | (low > close)).any():
        flags.append("invalid_ohlc")

    if "volume" not in df.columns:
        flags.append("missing_volume")
    else:
        volume = pd.to_numeric(df["volume"], errors="coerce")
        if volume.isna().any():
            flags.append("null_volume")

    if "amount" not in df.columns:
        flags.append("missing_amount")
    else:
        amount = pd.to_numeric(df["amount"], errors="coerce")
        if amount.isna().any():
            flags.append("null_amount")

    if end_date is not None and df["date"].notna().any():
        latest = df["date"].max()
        if latest < pd.Timestamp(end_date):
            flags.append("partial_range")
    if start_date is not None and df["date"].notna().any():
        earliest = df["date"].min()
        if earliest > pd.Timestamp(start_date):
            flags.append("late_start")

    return sorted(set(flags))


def normalize_daily_bars(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume", "amount", "turnover"])

    bars = raw.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "换手率": "turnover",
            "vol": "volume",
            "datetime": "date",
        }
    )
    bars = bars.loc[:, ~bars.columns.duplicated(keep="last")]
    keep_columns = ["date", "open", "close", "high", "low", "volume", "amount", "turnover"]
    for column in keep_columns:
        if column not in bars.columns:
            bars[column] = pd.NA
    bars = bars[keep_columns].copy()
    bars["date"] = pd.to_datetime(bars["date"], errors="coerce")
    bars = bars.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
    for column in ("open", "close", "high", "low", "volume", "amount", "turnover"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars = _normalize_daily_units(bars)
    bars = bars.reset_index(drop=True)
    return bars.sort_values("date").reset_index(drop=True)


def _normalize_daily_units(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return bars

    close = pd.to_numeric(bars["close"], errors="coerce")
    volume = pd.to_numeric(bars["volume"], errors="coerce")
    amount = pd.to_numeric(bars["amount"], errors="coerce")
    implied_share_ratio = amount / close / volume
    share_volume_mask = (
        close.gt(0)
        & volume.gt(0)
        & amount.gt(0)
        & implied_share_ratio.ge(0.5)
        & implied_share_ratio.le(2.0)
    )
    if share_volume_mask.any():
        bars.loc[share_volume_mask, "volume"] = volume[share_volume_mask] / 100.0

    return bars
