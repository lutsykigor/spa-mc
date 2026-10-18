"""spa-mc - Single Process Adaptive Multi-Core asyncio engine (Python 3.14+)."""

from spa_mc import topology
from spa_mc.engine import Engine, RouterStats, WorkerStartError
from spa_mc.limiter import LimiterConfig, VegasLimiter
from spa_mc.telemetry import WorkerState, WorkerStats
from spa_mc.worker import WorkerContext, current

__all__ = [
    'Engine',
    'LimiterConfig',
    'RouterStats',
    'VegasLimiter',
    'WorkerContext',
    'WorkerStartError',
    'WorkerState',
    'WorkerStats',
    'current',
    'topology',
]
