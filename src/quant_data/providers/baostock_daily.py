from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.quant_data.quality import assess_daily_bars, normalize_daily_bars

from .base import ProviderCapability, ProviderResult


@dataclass
class BaostockDailyBarProvider:
    name: str = "baostock"
    frequency: str = "d"
    capability: ProviderCapability = ProviderCapability(
        domain="daily_bar",
        markets=("sh", "sz"),
        supports_adjust=True,
        supports_history=True,
        supports_realtime=False,
        reliability=7,
        rate_limit_risk=2,
    )

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> ProviderResult:
        try:
            import baostock as bs
        except ImportError as exc:
            return ProviderResult(pd.DataFrame(), self.name, ["provider_unavailable"], str(exc))

        code = _to_baostock_symbol(symbol)
        adjustflag = _to_adjustflag(adjust)
        login_result = bs.login()
        if login_result.error_code != "0":
            return ProviderResult(pd.DataFrame(), self.name, ["login_failed"], login_result.error_msg)

        try:
            query = bs.query_history_k_data_plus(
                code,
                "date,code,open,high,low,close,volume,amount,turn,tradestatus",
                start_date=start_date,
                end_date=end_date,
                frequency=self.frequency,
                adjustflag=adjustflag,
            )
            if query.error_code != "0":
                return ProviderResult(pd.DataFrame(), self.name, ["query_failed"], query.error_msg)

            rows: list[list[str]] = []
            while query.error_code == "0" and query.next():
                rows.append(query.get_row_data())
        finally:
            bs.logout()

        frame = pd.DataFrame(rows, columns=["date", "code", "open", "high", "low", "close", "volume", "amount", "turnover", "tradestatus"])
        if not frame.empty and "tradestatus" in frame.columns:
            frame = frame[frame["tradestatus"].astype(str) == "1"].copy()
        if not frame.empty:
            frame["volume"] = (pd.to_numeric(frame["volume"], errors="coerce") / 100.0).round(0)
        data = normalize_daily_bars(frame)
        return ProviderResult(data=data, source=self.name, quality_flags=assess_daily_bars(data, start_date, end_date))


def _to_baostock_symbol(symbol: str) -> str:
    normalized = str(symbol).zfill(6)
    if normalized.startswith(("6", "9")):
        return f"sh.{normalized}"
    return f"sz.{normalized}"


def _to_adjustflag(adjust: str) -> str:
    adjust_text = (adjust or "").lower()
    if adjust_text in {"qfq", "forward"}:
        return "2"
    if adjust_text in {"hfq", "backward"}:
        return "1"
    return "3"
