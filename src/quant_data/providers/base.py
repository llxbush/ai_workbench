from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class ProviderCapability:
    domain: str
    markets: tuple[str, ...]
    supports_adjust: bool
    supports_history: bool
    supports_realtime: bool
    reliability: int
    rate_limit_risk: int


@dataclass
class ProviderResult:
    data: pd.DataFrame
    source: str
    quality_flags: list[str] = field(default_factory=list)
    error: str | None = None


class DailyBarProvider(Protocol):
    name: str
    capability: ProviderCapability

    def fetch_daily_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> ProviderResult:
        ...


class ProviderCircuitBreaker:
    def __init__(self, failure_threshold: int = 5, cooldown_seconds: float = 120.0) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._failures = 0
        self._opened_at: float | None = None
        self._lock = Lock()

    def allow_request(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return True
            if time.monotonic() - self._opened_at >= self.cooldown_seconds:
                self._failures = 0
                self._opened_at = None
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._opened_at = time.monotonic()


class ProviderRateLimiter:
    def __init__(self, min_interval_seconds: float = 0.0) -> None:
        self.min_interval_seconds = min_interval_seconds
        self._last_request_at = 0.0
        self._lock = Lock()

    def wait(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait_seconds = self.min_interval_seconds - (now - self._last_request_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._last_request_at = time.monotonic()
