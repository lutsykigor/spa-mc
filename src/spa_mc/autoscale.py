"""Decides, once per interval, how many workers should take traffic.

Pure logic: the engine feeds it per-worker samples and applies the decision, so every rule is testable without threads.

Two inputs drive it:
- the CPU budget (allowed CPUs, capped by the container quota), which can change at run time;
- each worker's *steal ratio*: the share of wall time its thread was ready to run but off the CPU.
  A high steal ratio means the core is being taken by something else (another process, or more workers than CPUs).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class WorkerSample:
    worker_id: int
    active: bool
    steal_ratio: float


@dataclass(frozen=True, slots=True)
class Decision:
    park: frozenset[int] = frozenset()
    unpark: frozenset[int] = frozenset()
    spawn: int = 0


@dataclass(slots=True)
class Autoscaler:
    # Off by default: under fair-share CPU contention parking hands our CPU time to the competing processes.
    park_on_steal: bool = False
    steal_high: float = 0.3
    steal_low: float = 0.1
    park_after: int = 2
    unpark_after: int = 3
    _high_streak: dict[int, int] = field(default_factory=dict)
    _low_streak: dict[int, int] = field(default_factory=dict)

    def decide(self, samples: list[WorkerSample], budget: int) -> Decision:
        budget = max(1, budget)
        for sample in samples:
            high = sample.steal_ratio > self.steal_high
            low = sample.steal_ratio < self.steal_low
            self._high_streak[sample.worker_id] = self._high_streak.get(sample.worker_id, 0) + 1 if high else 0
            self._low_streak[sample.worker_id] = self._low_streak.get(sample.worker_id, 0) + 1 if low else 0

        active = [s for s in samples if s.active]
        parked = [s for s in samples if not s.active]
        spawn = max(0, budget - len(samples))

        if len(active) > budget:
            excess = sorted(active, key=lambda s: (-s.steal_ratio, -s.worker_id))[: len(active) - budget]
            return Decision(park=frozenset(s.worker_id for s in excess))

        if self.park_on_steal and len(active) > 1:
            stolen = [s for s in active if self._high_streak[s.worker_id] >= self.park_after]
            if stolen:
                worst = max(stolen, key=lambda s: (s.steal_ratio, s.worker_id))
                self._high_streak[worst.worker_id] = 0
                return Decision(park=frozenset({worst.worker_id}), spawn=spawn)

        room = budget - len(active)
        ready = [s for s in parked if self._low_streak[s.worker_id] >= self.unpark_after]
        unpark = sorted(ready, key=lambda s: (s.steal_ratio, s.worker_id))[:room]
        return Decision(unpark=frozenset(s.worker_id for s in unpark), spawn=spawn)
