from __future__ import annotations

import os
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from src.quant_data.providers.base import ProviderResult
from src.quant_data.providers.baostock_daily import BaostockDailyBarProvider
from src.quant_data.providers.daily_bars import (
    AkshareSinaDailyBarProvider,
    AkshareTencentDailyBarProvider,
    EastmoneyDirectDailyBarProvider,
    MootdxDailyBarProvider,
)
from src.quant_data.router import DailyBarRouter

try:
    import akshare as ak
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "akshare is not installed. Please run: pip install -r requirements.txt"
    ) from exc


DIVIDEND_DATE_COLUMNS = ["date", "trade_date", "日期", "公告日期", "统计日期"]
DIVIDEND_VALUE_COLUMNS = [
    "dividend_yield",
    "dividend_yield_ttm",
    "dv_ratio",
    "dv_ttm",
    "股息率",
    "股息率TTM",
]
DIVIDEND_PER_SHARE_COLUMNS = ["每股股利", "每股分红", "每股派息", "派息(税前)"]
VALUATION_DATE_COLUMNS = ["date", "日期"]
PROFIT_DATE_COLUMNS = ["日期", "date"]
PROFIT_VALUE_COLUMNS = [
    "净利润",
    "扣除非经常性损益后的净利润",
    "扣非净利润",
    "归属于母公司股东的净利润",
]
SINA_DAILY_LOCK = threading.Lock()


@dataclass
class MarketDataClient:
    cache_dir: Path
    use_cache: bool = True

    def __post_init__(self) -> None:
        self._disable_env_proxy()
        self._disable_requests_proxy()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir = self.cache_dir.parent
        (self.cache_dir / "daily").mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "dividend").mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "valuation").mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "summary").mkdir(parents=True, exist_ok=True)
        self.daily_bar_router = DailyBarRouter(
            providers=[
                BaostockDailyBarProvider(),
                MootdxDailyBarProvider(),
                AkshareTencentDailyBarProvider(ak=ak),
                AkshareSinaDailyBarProvider(ak=ak, lock=SINA_DAILY_LOCK),
                EastmoneyDirectDailyBarProvider(),
            ],
            min_interval_by_source={
                "tencent_akshare": 0.25,
                "sina_akshare": 0.4,
            },
        )

    def get_stock_list(self) -> pd.DataFrame:
        cache_file = self.cache_dir / "summary" / "stock_list.csv"
        loaders = [
            self._load_stock_list_from_info_api,
            self._load_stock_list_from_spot_api,
        ]

        for loader in loaders:
            try:
                df = loader()
            except Exception:
                df = pd.DataFrame()
            if df is not None and not df.empty:
                df = df[["symbol", "name"]].drop_duplicates().reset_index(drop=True)
                if self.use_cache:
                    df.to_csv(cache_file, index=False)
                return df

        if cache_file.exists():
            cached = pd.read_csv(cache_file)
            if not cached.empty:
                cached["symbol"] = cached["symbol"].astype(str).str.zfill(6)
                return cached[["symbol", "name"]].drop_duplicates().reset_index(drop=True)

        return pd.DataFrame(columns=["symbol", "name"])

    def _load_stock_list_from_info_api(self) -> pd.DataFrame:
        df = ak.stock_info_a_code_name()
        df = df.rename(columns={"code": "symbol", "name": "name"})
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        return df[["symbol", "name"]]

    def _load_stock_list_from_spot_api(self) -> pd.DataFrame:
        if not hasattr(ak, "stock_zh_a_spot_em"):
            return pd.DataFrame()
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return pd.DataFrame()
        rename_map = {"代码": "symbol", "名称": "name"}
        existing = {key: value for key, value in rename_map.items() if key in df.columns}
        if len(existing) < 2:
            return pd.DataFrame()
        df = df.rename(columns=existing)
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        return df[["symbol", "name"]]

    def get_daily_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        persist_cache: bool = True,
        raise_on_error: bool = False,
        retries: int = 2,
        retry_wait_seconds: float = 0.8,
    ) -> pd.DataFrame:
        return self.get_daily_bars_result(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            persist_cache=persist_cache,
            raise_on_error=raise_on_error,
            retries=retries,
            retry_wait_seconds=retry_wait_seconds,
        ).data

    def get_daily_bars_result(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        persist_cache: bool = True,
        raise_on_error: bool = False,
        retries: int = 2,
        retry_wait_seconds: float = 0.8,
    ) -> ProviderResult:
        cache_file = self.cache_dir / "daily" / f"{symbol}_{start_date}_{end_date}_{adjust}.csv"
        if self.use_cache and persist_cache and cache_file.exists():
            cached = pd.read_csv(cache_file, parse_dates=["date"])
            return ProviderResult(
                data=cached.sort_values("date").reset_index(drop=True),
                source="local_cache",
                quality_flags=[],
            )

        last_error: Exception | None = None
        result: ProviderResult | None = None
        for attempt in range(retries + 1):
            try:
                result = self.daily_bar_router.fetch(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                    require_complete_volume=True,
                )
                raw = result.data
                break
            except Exception as exc:
                last_error = exc
                raw = None
                if attempt < retries:
                    time.sleep(retry_wait_seconds * (attempt + 1))
        else:
            raw = None

        if raw is None:
            if raise_on_error and last_error is not None:
                raise last_error
            return ProviderResult(
                data=pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"]),
                source="none",
                quality_flags=["empty"],
                error=f"{type(last_error).__name__}: {last_error}" if last_error is not None else None,
            )
        if raw.empty:
            if raise_on_error and result is not None and result.error:
                raise RuntimeError(result.error)
            return ProviderResult(
                data=pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"]),
                source=result.source if result is not None else "none",
                quality_flags=result.quality_flags if result is not None else ["empty"],
                error=result.error if result is not None else None,
            )

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
            }
        )
        keep_columns = ["date", "open", "close", "high", "low", "volume", "amount", "turnover"]
        existing_columns = [col for col in keep_columns if col in bars.columns]
        bars = bars[existing_columns].copy()
        bars["date"] = pd.to_datetime(bars["date"])
        bars = bars.sort_values("date").reset_index(drop=True)

        if self.use_cache and persist_cache:
            bars.to_csv(cache_file, index=False)
        return ProviderResult(
            data=bars,
            source=result.source if result is not None else "unknown",
            quality_flags=result.quality_flags if result is not None else [],
            error=result.error if result is not None else None,
        )

    def _fetch_daily_bars_from_any_source(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        if str(symbol).startswith(("4", "8", "9")):
            loaders = []
        else:
            loaders = [
                lambda: self._fetch_daily_bars_from_tencent(symbol, start_date, end_date, adjust),
                lambda: self._fetch_daily_bars_from_sina(symbol, start_date, end_date, adjust),
            ]
        last_error: Exception | None = None
        for loader in loaders:
            try:
                df = loader()
            except Exception as exc:
                last_error = exc
                df = pd.DataFrame()
            if df is not None and not df.empty:
                return df
        if last_error is not None:
            raise last_error
        return pd.DataFrame()

    @staticmethod
    def _fetch_daily_bars_from_eastmoney(
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        return ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=adjust,
        )

    def _fetch_daily_bars_from_tencent(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        if not hasattr(ak, "stock_zh_a_hist_tx"):
            return pd.DataFrame()

        tx_symbol = self._to_tx_symbol(symbol)
        raw = ak.stock_zh_a_hist_tx(
            symbol=tx_symbol,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=adjust,
        )
        if raw is None or raw.empty:
            return pd.DataFrame()

        df = raw.copy()
        if "date" not in df.columns:
            df = df.reset_index()
        if "date" not in df.columns and "日期" in df.columns:
            df = df.rename(columns={"日期": "date"})
        required_cols = {"date", "open", "close", "high", "low"}
        if not required_cols.issubset(df.columns):
            return pd.DataFrame()
        if "amount" not in df.columns:
            df["amount"] = pd.NA
        if "volume" not in df.columns:
            df["volume"] = pd.NA
        if "turnover" not in df.columns:
            df["turnover"] = pd.NA
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df[["date", "open", "close", "high", "low", "volume", "amount", "turnover"]]

    def _fetch_daily_bars_from_sina(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        if not hasattr(ak, "stock_zh_a_daily"):
            return pd.DataFrame()

        sina_symbol = self._to_sina_symbol(symbol)
        with SINA_DAILY_LOCK:
            raw = ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust=adjust,
            )
        if raw is None or raw.empty:
            return pd.DataFrame()

        df = raw.copy()
        if "date" not in df.columns:
            df = df.reset_index()
        if "date" not in df.columns and "日期" in df.columns:
            df = df.rename(columns={"日期": "date"})
        required_cols = {"date", "open", "close", "high", "low"}
        if not required_cols.issubset(df.columns):
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "turnover" not in df.columns:
            df["turnover"] = pd.NA
        return df[["date", "open", "close", "high", "low", "volume", "amount", "turnover"]]

    def get_suspension_info(self, symbol: str, date: str) -> dict | None:
        tfp_df = self.get_suspension_snapshot(date=date)
        if tfp_df.empty:
            return None
        matched = tfp_df[tfp_df["代码"].astype(str).str.zfill(6) == str(symbol).zfill(6)]
        if matched.empty:
            return None
        row = matched.iloc[0]
        return {
            "reason": str(row.get("停牌原因", "") or ""),
            "expected_resume_date": (
                pd.Timestamp(row["预计复牌时间"]).strftime("%Y-%m-%d")
                if pd.notna(row.get("预计复牌时间"))
                else ""
            ),
            "market": str(row.get("所属市场", "") or ""),
        }

    def get_suspension_snapshot(self, date: str) -> pd.DataFrame:
        cache_file = self.cache_dir / "summary" / f"suspension_{date}.csv"
        if self.use_cache and cache_file.exists():
            try:
                cached = pd.read_csv(cache_file, parse_dates=["停牌时间", "停牌截止时间", "预计复牌时间"])
                return cached
            except Exception:
                pass

        if not hasattr(ak, "stock_tfp_em"):
            return pd.DataFrame()

        try:
            df = ak.stock_tfp_em(date=date.replace("-", ""))
        except Exception:
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["代码"] = df["代码"].astype(str).str.zfill(6)
        if self.use_cache:
            df.to_csv(cache_file, index=False)
        return df

    @staticmethod
    def _build_suspended_result(symbol: str, existing: pd.DataFrame, suspended_info: dict) -> dict:
        latest_date = ""
        if not existing.empty:
            latest_date = pd.Timestamp(existing["date"].max()).strftime("%Y-%m-%d")
        return {
            "symbol": symbol,
            "status": "suspended",
            "rows_added": 0,
            "total_rows": int(len(existing)),
            "latest_date": latest_date,
            "suspension_reason": suspended_info.get("reason", ""),
            "expected_resume_date": suspended_info.get("expected_resume_date", ""),
        }

    def resolve_effective_end_date(self, end_date: str) -> str:
        today = pd.Timestamp.now().normalize()
        target = min(pd.Timestamp(end_date).normalize(), today)
        calendar = self.get_trade_calendar()
        if calendar.empty:
            return target.strftime("%Y-%m-%d")
        valid_dates = calendar[calendar["trade_date"] <= target]
        if valid_dates.empty:
            return target.strftime("%Y-%m-%d")
        return pd.Timestamp(valid_dates.iloc[-1]["trade_date"]).strftime("%Y-%m-%d")

    def get_trade_calendar(self) -> pd.DataFrame:
        cache_file = self.cache_dir / "summary" / "trade_calendar.csv"
        if self.use_cache and cache_file.exists():
            cached = pd.read_csv(cache_file, parse_dates=["trade_date"])
            if not cached.empty:
                return cached.sort_values("trade_date").reset_index(drop=True)

        if not hasattr(ak, "tool_trade_date_hist_sina"):
            return pd.DataFrame(columns=["trade_date"])

        try:
            calendar = ak.tool_trade_date_hist_sina()
        except Exception:
            return pd.DataFrame(columns=["trade_date"])

        if calendar is None or calendar.empty:
            return pd.DataFrame(columns=["trade_date"])

        calendar["trade_date"] = pd.to_datetime(calendar["trade_date"], errors="coerce")
        calendar = calendar.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
        if self.use_cache:
            calendar.to_csv(cache_file, index=False)
        return calendar

    def get_dividend_yield_series(self, symbol: str) -> pd.DataFrame | None:
        cache_file = self.cache_dir / "dividend" / f"{symbol}.csv"
        if self.use_cache and cache_file.exists():
            cached = pd.read_csv(cache_file, parse_dates=["date"])
            return cached.sort_values("date").reset_index(drop=True)

        loader_candidates = []
        if hasattr(ak, "stock_a_indicator_lg"):
            loader_candidates.append(lambda: ak.stock_a_indicator_lg(symbol=symbol))
        if hasattr(ak, "stock_financial_analysis_indicator"):
            loader_candidates.append(lambda: ak.stock_financial_analysis_indicator(symbol=symbol))

        raw = None
        for loader in loader_candidates:
            try:
                raw = loader()
            except Exception:
                raw = None
            if raw is not None and not raw.empty:
                break

        if raw is None or raw.empty:
            return None

        date_col = self._pick_column(raw.columns, DIVIDEND_DATE_COLUMNS)
        value_col = self._pick_column(raw.columns, DIVIDEND_VALUE_COLUMNS)
        if not date_col or not value_col:
            return None

        dividend = raw[[date_col, value_col]].copy()
        dividend.columns = ["date", "dividend_yield"]
        dividend["date"] = pd.to_datetime(dividend["date"], errors="coerce")
        dividend["dividend_yield"] = pd.to_numeric(dividend["dividend_yield"], errors="coerce")
        dividend = dividend.dropna(subset=["date", "dividend_yield"])
        dividend = dividend.sort_values("date").drop_duplicates(subset=["date"], keep="last")

        if dividend.empty:
            return None

        if self.use_cache:
            dividend.to_csv(cache_file, index=False)
        return dividend.reset_index(drop=True)

    def get_valuation_series(self, symbol: str, indicator: str, period: str = "近一年") -> pd.DataFrame | None:
        if not hasattr(ak, "stock_zh_valuation_baidu"):
            return None

        indicator_slug = indicator.replace("/", "_").replace("(", "_").replace(")", "_")
        cache_file = self.cache_dir / "valuation" / f"{symbol}_{indicator_slug}_{period}.csv"
        if self.use_cache and cache_file.exists():
            cached = pd.read_csv(cache_file, parse_dates=["date"])
            return cached.sort_values("date").reset_index(drop=True)

        try:
            raw = ak.stock_zh_valuation_baidu(symbol=symbol, indicator=indicator, period=period)
        except Exception:
            return None

        if raw is None or raw.empty:
            return None

        date_col = self._pick_column(raw.columns, VALUATION_DATE_COLUMNS)
        value_col = "value" if "value" in raw.columns else None
        if not date_col or not value_col:
            return None

        result = raw[[date_col, value_col]].copy()
        result.columns = ["date", "value"]
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        result["value"] = pd.to_numeric(result["value"], errors="coerce")
        result = result.dropna(subset=["date", "value"]).sort_values("date")
        result = result.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        if result.empty:
            return None

        if self.use_cache:
            result.to_csv(cache_file, index=False)
        return result

    def get_dividend_summary(self) -> pd.DataFrame | None:
        if not hasattr(ak, "stock_history_dividend"):
            return None

        cache_file = self.cache_dir / "summary" / "stock_history_dividend.csv"
        if self.use_cache and cache_file.exists():
            return pd.read_csv(cache_file)

        try:
            raw = ak.stock_history_dividend()
        except Exception:
            return None

        if raw is None or raw.empty:
            return None

        if self.use_cache:
            raw.to_csv(cache_file, index=False)
        return raw

    def get_latest_snapshot(self, symbol: str, latest_date: pd.Timestamp, latest_close: float) -> dict:
        snapshot = {
            "dividend_yield": pd.NA,
            "pe_ttm": pd.NA,
            "market_cap": pd.NA,
            "is_profitable": pd.NA,
            "dividend_source": "unavailable",
            "valuation_source": "unavailable",
            "profitability_source": "unavailable",
        }

        dividend_series = self.get_dividend_yield_series(symbol)
        if dividend_series is not None and not dividend_series.empty:
            dividend_value = self._latest_value_from_series(dividend_series, latest_date)
            if pd.notna(dividend_value):
                snapshot["dividend_yield"] = dividend_value
                snapshot["dividend_source"] = "direct"

        if pd.isna(snapshot["dividend_yield"]) and latest_close > 0:
            dividend_summary = self.get_dividend_summary()
            if dividend_summary is not None and not dividend_summary.empty:
                matched = dividend_summary[dividend_summary["代码"].astype(str).str.zfill(6) == symbol]
                if not matched.empty and "年均股息" in matched.columns:
                    avg_dividend = pd.to_numeric(matched.iloc[0]["年均股息"], errors="coerce")
                    if pd.notna(avg_dividend):
                        # 新浪历史分红里的“年均股息”更接近“每 10 股股息”，这里先换算到每股口径
                        snapshot["dividend_yield"] = (avg_dividend / 10) / latest_close * 100
                        snapshot["dividend_source"] = "estimated_from_avg_dividend_per_10_shares"

        pe_series = self.get_valuation_series(symbol=symbol, indicator="市盈率(TTM)")
        market_cap_series = self.get_valuation_series(symbol=symbol, indicator="总市值")

        pe_value = self._latest_value_from_series(pe_series, latest_date)
        market_cap_value = self._latest_value_from_series(market_cap_series, latest_date)
        if pd.notna(pe_value) or pd.notna(market_cap_value):
            snapshot["valuation_source"] = "baidu_valuation"
        snapshot["pe_ttm"] = pe_value
        snapshot["market_cap"] = market_cap_value
        if pd.notna(pe_value):
            snapshot["is_profitable"] = bool(pe_value > 0)
            snapshot["profitability_source"] = "pe_ttm"
            return snapshot

        profit_series = self.get_profit_series(symbol=symbol)
        profit_value = self._latest_value_from_series(profit_series, latest_date)
        if pd.notna(profit_value):
            snapshot["is_profitable"] = bool(profit_value > 0)
            snapshot["profitability_source"] = "net_profit"
        return snapshot

    def get_profit_series(self, symbol: str) -> pd.DataFrame | None:
        if not hasattr(ak, "stock_financial_analysis_indicator"):
            return None

        cache_file = self.cache_dir / "summary" / f"{symbol}_profit.csv"
        if self.use_cache and cache_file.exists():
            cached = pd.read_csv(cache_file, parse_dates=["date"])
            return cached.sort_values("date").reset_index(drop=True)

        try:
            raw = ak.stock_financial_analysis_indicator(symbol=symbol)
        except Exception:
            return None

        if raw is None or raw.empty:
            return None

        date_col = self._pick_column(raw.columns, PROFIT_DATE_COLUMNS)
        value_col = self._pick_column(raw.columns, PROFIT_VALUE_COLUMNS)
        if not date_col or not value_col:
            return None

        result = raw[[date_col, value_col]].copy()
        result.columns = ["date", "value"]
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        result["value"] = pd.to_numeric(result["value"], errors="coerce")
        result = result.dropna(subset=["date", "value"]).sort_values("date")
        result = result.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        if result.empty:
            return None

        if self.use_cache:
            result.to_csv(cache_file, index=False)
        return result

    @staticmethod
    def _latest_value_from_series(series_df: pd.DataFrame | None, target_date: pd.Timestamp) -> float | pd._libs.missing.NAType:
        if series_df is None or series_df.empty:
            return pd.NA

        temp_df = series_df.copy()
        temp_df["date"] = pd.to_datetime(temp_df["date"], errors="coerce")
        filtered = temp_df[temp_df["date"] <= target_date].sort_values("date")
        if filtered.empty:
            filtered = temp_df.sort_values("date")
        if filtered.empty:
            return pd.NA
        return pd.to_numeric(filtered.iloc[-1]["value"], errors="coerce")

    @staticmethod
    def _pick_column(columns: Iterable[str], candidates: list[str]) -> str | None:
        for candidate in candidates:
            if candidate in columns:
                return candidate
        return None

    @staticmethod
    def _disable_env_proxy() -> None:
        proxy_keys = [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        ]
        for key in proxy_keys:
            os.environ.pop(key, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"

    @staticmethod
    def _disable_requests_proxy() -> None:
        if getattr(requests.sessions.Session, "_quant_proxy_disabled", False):
            return

        original_merge = requests.sessions.Session.merge_environment_settings
        original_request = requests.sessions.Session.request
        original_api_request = requests.api.request

        def merge_environment_settings_no_proxy(self, url, proxies, stream, verify, cert):
            settings = original_merge(self, url, proxies, stream, verify, cert)
            settings["proxies"] = {}
            return settings

        def request_with_defaults(self, method, url, **kwargs):
            headers = dict(kwargs.pop("headers", {}) or {})
            headers.setdefault(
                "User-Agent",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            )
            headers.setdefault("Accept", "application/json,text/plain,*/*")
            headers.setdefault("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
            headers.setdefault("Connection", "close")
            kwargs["headers"] = headers
            kwargs.setdefault("timeout", 15)
            return original_request(self, method, url, **kwargs)

        def api_request_with_defaults(method, url, **kwargs):
            session = requests.sessions.Session()
            session.trust_env = False
            try:
                return request_with_defaults(session, method=method, url=url, **kwargs)
            finally:
                session.close()

        requests.sessions.Session.merge_environment_settings = merge_environment_settings_no_proxy
        requests.sessions.Session.request = request_with_defaults
        requests.api.request = api_request_with_defaults
        requests.request = api_request_with_defaults
        requests.get = lambda url, params=None, **kwargs: api_request_with_defaults(
            "get",
            url,
            params=params,
            **kwargs,
        )
        requests.post = lambda url, data=None, json=None, **kwargs: api_request_with_defaults(
            "post",
            url,
            data=data,
            json=json,
            **kwargs,
        )
        requests.sessions.Session._quant_original_api_request = original_api_request
        requests.sessions.Session._quant_proxy_disabled = True

    @staticmethod
    def _to_tx_symbol(symbol: str) -> str:
        symbol = str(symbol).zfill(6)
        if symbol.startswith(("5", "6", "9")):
            return f"sh{symbol}"
        if symbol.startswith(("4", "8")):
            return f"bj{symbol}"
        return f"sz{symbol}"

    @staticmethod
    def _to_sina_symbol(symbol: str) -> str:
        symbol = str(symbol).zfill(6)
        if symbol.startswith(("5", "6", "9")):
            return f"sh{symbol}"
        return f"sz{symbol}"
