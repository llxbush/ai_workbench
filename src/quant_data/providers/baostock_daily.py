from __future__ import annotations

import atexit
import os
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from dataclasses import dataclass
from threading import Lock

import pandas as pd

from src.quant_data.quality import assess_daily_bars, normalize_daily_bars

from .base import ProviderCapability, ProviderResult


_BAOSTOCK_POOL_LOCK = Lock()
_BAOSTOCK_POOL: ProcessPoolExecutor | None = None
_BAOSTOCK_POOL_SIZE = 0
_WORKER_BAOSTOCK_MODULE = None
_WORKER_BAOSTOCK_LOGGED_IN = False


def _shutdown_baostock_pool() -> None:
    global _BAOSTOCK_POOL
    with _BAOSTOCK_POOL_LOCK:
        if _BAOSTOCK_POOL is None:
            return
        _BAOSTOCK_POOL.shutdown(wait=False, cancel_futures=True)
        _BAOSTOCK_POOL = None


atexit.register(_shutdown_baostock_pool)


@dataclass
class BaostockDailyBarProvider:
    name: str = "baostock"
    frequency: str = "d"
    timeout_seconds: float = 20.0
    max_concurrency: int = max(1, min(8, (os.cpu_count() or 4)))
    capability: ProviderCapability = ProviderCapability(
        domain="daily_bar",
        markets=("sh", "sz"),
        supports_adjust=True,
        supports_history=True,
        supports_realtime=False,
        reliability=7,
        rate_limit_risk=1,
    )

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> ProviderResult:
        if _is_bj_symbol(symbol):
            return ProviderResult(
                pd.DataFrame(),
                self.name,
                ["unsupported_market"],
                "baostock only accepts sh./sz. code prefixes; bj. returns 10004011",
            )
        try:
            pool = _get_baostock_pool(self.max_concurrency)
            payload = pool.submit(
                _fetch_baostock_rows,
                symbol,
                start_date,
                end_date,
                self.frequency,
                _to_adjustflag(adjust),
            ).result(timeout=self.timeout_seconds)
        except TimeoutError:
            return ProviderResult(pd.DataFrame(), self.name, ["timeout"], f"baostock query timed out after {self.timeout_seconds:.0f}s")
        except Exception as exc:
            return ProviderResult(pd.DataFrame(), self.name, ["provider_error"], f"{type(exc).__name__}: {exc}")

        if payload["error_code"] != "0":
            quality_flag = payload.get("quality_flag") or "query_failed"
            return ProviderResult(pd.DataFrame(), self.name, [quality_flag], payload["error_msg"])

        rows = payload["rows"]
        frame = pd.DataFrame(rows, columns=["date", "code", "open", "high", "low", "close", "volume", "amount", "turnover", "tradestatus"])
        if not frame.empty and "tradestatus" in frame.columns:
            frame = frame[frame["tradestatus"].astype(str) == "1"].copy()
        if not frame.empty:
            frame["volume"] = (pd.to_numeric(frame["volume"], errors="coerce") / 100.0).round(0)
        data = normalize_daily_bars(frame)
        return ProviderResult(data=data, source=self.name, quality_flags=assess_daily_bars(data, start_date, end_date))


def _to_baostock_symbol(symbol: str) -> str:
    normalized = str(symbol).zfill(6)
    if normalized.startswith("6"):
        return f"sh.{normalized}"
    return f"sz.{normalized}"


def _is_bj_symbol(symbol: str) -> bool:
    return str(symbol).zfill(6).startswith(("4", "8", "9"))


def _to_adjustflag(adjust: str) -> str:
    adjust_text = (adjust or "").lower()
    if adjust_text in {"qfq", "forward"}:
        return "2"
    if adjust_text in {"hfq", "backward"}:
        return "1"
    return "3"


def _get_baostock_pool(max_concurrency: int) -> ProcessPoolExecutor:
    global _BAOSTOCK_POOL, _BAOSTOCK_POOL_SIZE
    target_size = max(1, int(max_concurrency))
    with _BAOSTOCK_POOL_LOCK:
        if _BAOSTOCK_POOL is None or _BAOSTOCK_POOL_SIZE != target_size:
            if _BAOSTOCK_POOL is not None:
                _BAOSTOCK_POOL.shutdown(wait=False, cancel_futures=True)
            _BAOSTOCK_POOL = ProcessPoolExecutor(max_workers=target_size)
            _BAOSTOCK_POOL_SIZE = target_size
        return _BAOSTOCK_POOL


def _fetch_baostock_rows(
    symbol: str,
    start_date: str,
    end_date: str,
    frequency: str,
    adjustflag: str,
) -> dict[str, object]:
    global _WORKER_BAOSTOCK_MODULE, _WORKER_BAOSTOCK_LOGGED_IN
    try:
        import baostock as bs
    except ImportError as exc:
        return {
            "error_code": "import_error",
            "error_msg": str(exc),
            "quality_flag": "provider_unavailable",
            "rows": [],
        }

    if _WORKER_BAOSTOCK_MODULE is None:
        _WORKER_BAOSTOCK_MODULE = bs
        atexit.register(_logout_baostock_worker_session)

    if not _WORKER_BAOSTOCK_LOGGED_IN:
        login_result = bs.login()
        if login_result.error_code != "0":
            return {
                "error_code": login_result.error_code,
                "error_msg": str(login_result.error_msg),
                "quality_flag": "login_failed",
                "rows": [],
            }
        _WORKER_BAOSTOCK_LOGGED_IN = True

    query = bs.query_history_k_data_plus(
        _to_baostock_symbol(symbol),
        "date,code,open,high,low,close,volume,amount,turn,tradestatus",
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        adjustflag=adjustflag,
    )
    if query.error_code != "0":
        return {
            "error_code": query.error_code,
            "error_msg": str(query.error_msg),
            "quality_flag": "query_failed",
            "rows": [],
        }

    rows: list[list[str]] = []
    while query.error_code == "0" and query.next():
        rows.append(query.get_row_data())
    return {"error_code": "0", "error_msg": "success", "quality_flag": "", "rows": rows}


def _logout_baostock_worker_session() -> None:
    global _WORKER_BAOSTOCK_LOGGED_IN
    if _WORKER_BAOSTOCK_MODULE is None or not _WORKER_BAOSTOCK_LOGGED_IN:
        return
    try:
        _WORKER_BAOSTOCK_MODULE.logout()
    except Exception:
        pass
    _WORKER_BAOSTOCK_LOGGED_IN = False
