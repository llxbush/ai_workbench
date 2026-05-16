from __future__ import annotations

import time
from dataclasses import dataclass, field

import pandas as pd

from src.quant_data.providers.base import (
    DailyBarProvider,
    ProviderCircuitBreaker,
    ProviderRateLimiter,
    ProviderResult,
)


@dataclass
class DailyBarRouter:
    providers: list[DailyBarProvider]
    min_interval_by_source: dict[str, float] = field(default_factory=dict)
    failure_threshold: int = 5
    cooldown_seconds: float = 120.0

    def __post_init__(self) -> None:
        self._limiters = {
            provider.name: ProviderRateLimiter(self.min_interval_by_source.get(provider.name, 0.0))
            for provider in self.providers
        }
        self._breakers = {
            provider.name: ProviderCircuitBreaker(self.failure_threshold, self.cooldown_seconds)
            for provider in self.providers
        }

    def fetch(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        require_complete_volume: bool = False,
    ) -> ProviderResult:
        errors: list[str] = []
        partial_result: ProviderResult | None = None
        for provider in self.providers:
            if adjust and not provider.capability.supports_adjust:
                continue
            market = self._market(symbol)
            if market not in provider.capability.markets:
                continue
            breaker = self._breakers[provider.name]
            if not breaker.allow_request():
                errors.append(f"{provider.name}:circuit_open")
                continue
            try:
                self._limiters[provider.name].wait()
                result = provider.fetch_daily_bars(symbol, start_date, end_date, adjust)
            except Exception as exc:
                breaker.record_failure()
                errors.append(f"{provider.name}:{type(exc).__name__}:{exc}")
                continue

            if result.data is not None and not result.data.empty:
                breaker.record_success()
                if not require_complete_volume or "null_volume" not in result.quality_flags:
                    return result
                if partial_result is None:
                    partial_result = result
            else:
                breaker.record_failure()
                if result.error:
                    errors.append(f"{provider.name}:{result.error}")

        if partial_result is not None:
            partial_result.quality_flags = sorted(set(partial_result.quality_flags + ["returned_with_incomplete_volume"]))
            return partial_result
        return ProviderResult(pd.DataFrame(), "none", sorted(set(errors + ["empty"])), "; ".join(errors) or None)

    @staticmethod
    def _market(symbol: str) -> str:
        symbol = str(symbol).zfill(6)
        if symbol.startswith(("6", "9")):
            return "sh"
        if symbol.startswith(("4", "8")):
            return "bj"
        return "sz"
