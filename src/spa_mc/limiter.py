"""Adaptive concurrency limiter: TCP-Vegas queue estimation with AIMD backoff.

Pure, clock-free logic (samples are passed in), so it is deterministic to test and
usable from any asyncio application, not only inside a spa-mc worker.

The steady-state rule follows Netflix concurrency-limits `VegasLimit`:

    queue = ceil(limit * (1 - rtt_noload / rtt))
    app-limited (in_flight * 2 < limit) -> no change
    queue <= threshold                  -> limit + beta
    queue <  alpha                      -> limit + threshold
    queue >  beta                       -> limit - threshold
    otherwise (alpha <= queue <= beta)  -> hold

with threshold = log10(limit), alpha = 3 * log10(limit), beta = 6 * log10(limit).
Hard signals (drop, timeout, event-loop stall) apply a multiplicative decrease.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class LimiterConfig:
    initial_limit: int = 100
    min_limit: int = 2
    max_limit: int = 1000
    backoff_ratio: float = 0.9
    timeout: float | None = None
    smoothing: float = 1.0
    probe_multiplier: int = 30


@dataclass(slots=True)
class VegasLimiter:
    config: LimiterConfig = field(default_factory=LimiterConfig)
    limit: float = 0.0
    in_flight: int = 0
    rtt_noload: float = math.inf
    rtt_short: float = 0.0
    completed: int = 0
    shed: int = 0
    drops: int = 0
    stalls: int = 0
    _samples_since_probe: int = 0

    def __post_init__(self) -> None:
        self.limit = float(self.config.initial_limit)

    @property
    def effective_limit(self) -> int:
        return max(self.config.min_limit, min(self.config.max_limit, int(self.limit)))

    @property
    def has_capacity(self) -> bool:
        return self.in_flight < self.effective_limit

    def try_acquire(self) -> bool:
        if self.in_flight >= self.effective_limit:
            self.shed += 1
            return False
        self.in_flight += 1
        return True

    def release(self, rtt: float, dropped: bool = False) -> None:
        in_flight_at_sample = self.in_flight
        self.in_flight = max(0, self.in_flight - 1)
        self.completed += 1
        timed_out = self.config.timeout is not None and rtt > self.config.timeout
        if dropped or timed_out:
            self.drops += 1
            self._multiplicative_decrease()
            return
        if rtt <= 0.0:
            return
        self._observe_rtt(rtt)
        self._vegas_update(rtt, in_flight_at_sample)

    def on_stall(self) -> None:
        self.stalls += 1
        self._multiplicative_decrease()

    def _multiplicative_decrease(self) -> None:
        self.limit = max(float(self.config.min_limit), math.floor(self.limit * self.config.backoff_ratio))

    def _observe_rtt(self, rtt: float) -> None:
        self.rtt_short = rtt if self.rtt_short == 0.0 else 0.3 * rtt + 0.7 * self.rtt_short
        self._samples_since_probe += 1
        # Periodically forget the minimum so a permanently slower workload re-baselines.
        if self._samples_since_probe >= self.config.probe_multiplier * max(1, self.effective_limit):
            self._samples_since_probe = 0
            self.rtt_noload = rtt
        elif rtt < self.rtt_noload:
            self.rtt_noload = rtt

    def _vegas_update(self, rtt: float, in_flight: int) -> None:
        limit = self.limit
        if in_flight * 2 < limit:
            return
        log_l = max(1.0, math.log10(limit))
        threshold, alpha, beta = log_l, 3 * log_l, 6 * log_l
        queue = math.ceil(limit * (1 - self.rtt_noload / rtt))
        if queue <= threshold:
            target = limit + beta
        elif queue < alpha:
            target = limit + threshold
        elif queue > beta:
            target = limit - threshold
        else:
            return
        target = max(float(self.config.min_limit), min(float(self.config.max_limit), target))
        self.limit = (1 - self.config.smoothing) * limit + self.config.smoothing * target
