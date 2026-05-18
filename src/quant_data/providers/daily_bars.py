from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from threading import Lock, local
from typing import Callable

import pandas as pd
import requests

from src.quant_data.quality import assess_daily_bars, normalize_daily_bars

from .base import DailyBarProvider, ProviderCapability, ProviderResult


def _market_code(symbol: str) -> str:
    symbol = str(symbol).zfill(6)
    if symbol.startswith(("4", "8", "9")):
        return "bj"
    if symbol.startswith("6"):
        return "sh"
    return "sz"


def _eastmoney_secid(symbol: str) -> str:
    symbol = str(symbol).zfill(6)
    if symbol.startswith("6"):
        return f"1.{symbol}"
    return f"0.{symbol}"


def _fqt(adjust: str) -> str:
    adjust = (adjust or "").lower()
    if adjust in {"qfq", "forward"}:
        return "1"
    if adjust in {"hfq", "backward"}:
        return "2"
    return "0"


MOOTDX_FALLBACK_SERVERS: tuple[tuple[str, int], ...] = (
    ("110.41.147.114", 7709),
    ("8.129.13.54", 7709),
    ("120.24.149.49", 7709),
    ("47.100.236.28", 7709),
    ("124.70.199.56", 7709),
)


def _patch_pandas_fillna_for_mootdx() -> Callable[[], None]:
    original_series_fillna = pd.Series.fillna
    original_frame_fillna = pd.DataFrame.fillna

    def series_fillna_compat(self, value=None, *, method=None, axis=None, inplace=False, limit=None, **kwargs):
        if method == "ffill":
            result = self.ffill(axis=axis, limit=limit)
        elif method == "bfill":
            result = self.bfill(axis=axis, limit=limit)
        else:
            return original_series_fillna(self, value=value, axis=axis, inplace=inplace, limit=limit, **kwargs)
        if inplace:
            self.update(result)
            return None
        return result

    def frame_fillna_compat(self, value=None, *, method=None, axis=None, inplace=False, limit=None, **kwargs):
        if method == "ffill":
            result = self.ffill(axis=axis, limit=limit)
        elif method == "bfill":
            result = self.bfill(axis=axis, limit=limit)
        else:
            return original_frame_fillna(self, value=value, axis=axis, inplace=inplace, limit=limit, **kwargs)
        if inplace:
            self.update(result)
            return None
        return result

    pd.Series.fillna = series_fillna_compat
    pd.DataFrame.fillna = frame_fillna_compat

    def restore() -> None:
        pd.Series.fillna = original_series_fillna
        pd.DataFrame.fillna = original_frame_fillna

    return restore


def _patch_mootdx_bestip(server: tuple[str, int]) -> Callable[[], None]:
    from mootdx import config

    original_setup = config.setup

    def setup_with_server():
        result = original_setup()
        bestip = config.get("BESTIP") or {}
        if not bestip.get("HQ"):
            bestip = dict(bestip)
            bestip["HQ"] = [server[0], server[1]]
            config.set("BESTIP", bestip)
        return result

    config.setup = setup_with_server

    def restore() -> None:
        config.setup = original_setup

    return restore


@dataclass
class EastmoneyDirectDailyBarProvider:
    name: str = "eastmoney_direct"
    timeout_seconds: float = 10.0
    capability: ProviderCapability = ProviderCapability(
        domain="daily_bar",
        markets=("sh", "sz", "bj"),
        supports_adjust=True,
        supports_history=True,
        supports_realtime=False,
        reliability=8,
        rate_limit_risk=6,
    )

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> ProviderResult:
        params = {
            "secid": _eastmoney_secid(symbol),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": _fqt(adjust),
            "beg": start_date.replace("-", ""),
            "end": end_date.replace("-", ""),
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "close",
            "Referer": "https://quote.eastmoney.com/",
        }
        session = requests.Session()
        session.trust_env = False
        response = session.get(
            "https://push2his.eastmoney.com/api/qt/stock/kline/get",
            params=params,
            headers=headers,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        klines = (payload.get("data") or {}).get("klines") or []
        rows = []
        for item in klines:
            parts = str(item).split(",")
            if len(parts) < 11:
                continue
            rows.append(
                {
                    "date": parts[0],
                    "open": parts[1],
                    "close": parts[2],
                    "high": parts[3],
                    "low": parts[4],
                    "volume": parts[5],
                    "amount": parts[6],
                    "turnover": parts[10],
                }
            )
        data = normalize_daily_bars(pd.DataFrame(rows))
        return ProviderResult(
            data=data,
            source=self.name,
            quality_flags=assess_daily_bars(data, start_date=start_date, end_date=end_date),
        )


@dataclass
class AkshareEastmoneyDailyBarProvider:
    ak: object
    name: str = "eastmoney_akshare"
    capability: ProviderCapability = ProviderCapability(
        domain="daily_bar",
        markets=("sh", "sz", "bj"),
        supports_adjust=True,
        supports_history=True,
        supports_realtime=False,
        reliability=6,
        rate_limit_risk=7,
    )

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> ProviderResult:
        raw = self.ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=adjust,
        )
        data = normalize_daily_bars(raw)
        return ProviderResult(data=data, source=self.name, quality_flags=assess_daily_bars(data, start_date, end_date))


@dataclass
class AkshareTencentDailyBarProvider:
    ak: object
    name: str = "tencent_akshare"
    capability: ProviderCapability = ProviderCapability(
        domain="daily_bar",
        markets=("sh", "sz"),
        supports_adjust=True,
        supports_history=True,
        supports_realtime=False,
        reliability=5,
        rate_limit_risk=5,
    )

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> ProviderResult:
        if not hasattr(self.ak, "stock_zh_a_hist_tx"):
            return ProviderResult(pd.DataFrame(), self.name, ["provider_unavailable"], "stock_zh_a_hist_tx missing")
        raw = self.ak.stock_zh_a_hist_tx(
            symbol=f"{_market_code(symbol)}{str(symbol).zfill(6)}",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=adjust,
        )
        if raw is not None and not raw.empty and "amount" in raw.columns and "volume" not in raw.columns:
            raw = raw.rename(columns={"amount": "volume"})
        data = normalize_daily_bars(raw)
        return ProviderResult(data=data, source=self.name, quality_flags=assess_daily_bars(data, start_date, end_date))


@dataclass
class AkshareSinaDailyBarProvider:
    ak: object
    lock: object
    name: str = "sina_akshare"
    capability: ProviderCapability = ProviderCapability(
        domain="daily_bar",
        markets=("sh", "sz"),
        supports_adjust=True,
        supports_history=True,
        supports_realtime=False,
        reliability=5,
        rate_limit_risk=5,
    )

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> ProviderResult:
        if not hasattr(self.ak, "stock_zh_a_daily"):
            return ProviderResult(pd.DataFrame(), self.name, ["provider_unavailable"], "stock_zh_a_daily missing")
        with self.lock:
            raw = self.ak.stock_zh_a_daily(
                symbol=f"{_market_code(symbol)}{str(symbol).zfill(6)}",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust=adjust,
            )
        data = normalize_daily_bars(raw)
        return ProviderResult(data=data, source=self.name, quality_flags=assess_daily_bars(data, start_date, end_date))


@dataclass
class MootdxDailyBarProvider:
    name: str = "mootdx"
    timeout_seconds: int = 8
    capability: ProviderCapability = ProviderCapability(
        domain="daily_bar",
        markets=("sh", "sz"),
        supports_adjust=True,
        supports_history=True,
        supports_realtime=True,
        reliability=9,
        rate_limit_risk=1,
    )

    def __post_init__(self) -> None:
        self._local = local()
        self._server_lock = Lock()
        self._server_index = 0

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> ProviderResult:
        raw = pd.DataFrame()
        last_error: Exception | None = None
        end_exclusive = (pd.Timestamp(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
        restore_fillna = _patch_pandas_fillna_for_mootdx()
        try:
            for attempt in range(len(MOOTDX_FALLBACK_SERVERS)):
                server = self._current_server()
                restore_bestip = _patch_mootdx_bestip(server)
                try:
                    client = self._client(server=server)
                    raw = client.k(
                        symbol=str(symbol).zfill(6),
                        begin=start_date,
                        end=end_exclusive,
                        adjust=adjust or "qfq",
                    )
                    if raw is not None and not raw.empty:
                        break
                except Exception as exc:
                    last_error = exc
                    raw = pd.DataFrame()
                    self._drop_thread_client()
                    self._advance_server()
                finally:
                    restore_bestip()
        finally:
            restore_fillna()

        data = normalize_daily_bars(raw)
        if not data.empty:
            data = data[(data["date"] >= pd.Timestamp(start_date)) & (data["date"] <= pd.Timestamp(end_date))]
        if data.empty and last_error is not None:
            return ProviderResult(
                data=data,
                source=self.name,
                quality_flags=["empty"],
                error=f"{type(last_error).__name__}: {last_error}",
            )
        return ProviderResult(data=data, source=self.name, quality_flags=assess_daily_bars(data, start_date, end_date))

    def _client(self, server: tuple[str, int]):
        cached_client = getattr(self._local, "client", None)
        cached_server = getattr(self._local, "server", None)
        if cached_client is not None and cached_server == server:
            return cached_client

        try:
            from mootdx.quotes import Quotes
        except ImportError as exc:
            raise RuntimeError(f"mootdx is not installed: {exc}") from exc

        client = Quotes.factory(market="std", server=server, timeout=self.timeout_seconds)
        self._local.client = client
        self._local.server = server
        return client

    def _drop_thread_client(self) -> None:
        if hasattr(self._local, "client"):
            del self._local.client
        if hasattr(self._local, "server"):
            del self._local.server

    def _current_server(self) -> tuple[str, int]:
        with self._server_lock:
            return MOOTDX_FALLBACK_SERVERS[self._server_index % len(MOOTDX_FALLBACK_SERVERS)]

    def _advance_server(self) -> None:
        with self._server_lock:
            self._server_index = (self._server_index + 1) % len(MOOTDX_FALLBACK_SERVERS)
