import math
import random

import pytest

from spa_mc.limiter import LimiterConfig, VegasLimiter

NOLOAD = 0.001


def limiter_at(limit: int, **overrides) -> VegasLimiter:
    config = LimiterConfig(initial_limit=limit, min_limit=2, max_limit=1000, **overrides)
    limiter = VegasLimiter(config)
    limiter.rtt_noload = NOLOAD
    return limiter


def sample(limiter: VegasLimiter, rtt: float, in_flight: int) -> None:
    limiter.in_flight = in_flight
    limiter.release(rtt)


def rtt_for_queue(limit: float, queue: float) -> float:
    return NOLOAD / (1 - queue / limit)


def test_no_queue_increases_by_beta():
    limiter = limiter_at(100)
    sample(limiter, NOLOAD, in_flight=60)
    assert limiter.limit == pytest.approx(100 + 6 * math.log10(100))


def test_small_queue_increases_by_log():
    limiter = limiter_at(100)
    sample(limiter, rtt_for_queue(100, 4), in_flight=60)
    assert limiter.limit == pytest.approx(102)


def test_queue_between_alpha_and_beta_holds():
    limiter = limiter_at(100)
    sample(limiter, rtt_for_queue(100, 9), in_flight=60)
    assert limiter.limit == 100


def test_queue_above_beta_decreases_by_log():
    limiter = limiter_at(100)
    sample(limiter, rtt_for_queue(100, 20), in_flight=60)
    assert limiter.limit == pytest.approx(98)


def test_app_limited_samples_do_not_move_the_limit():
    limiter = limiter_at(100)
    sample(limiter, rtt_for_queue(100, 50), in_flight=10)
    assert limiter.limit == 100


def test_drop_applies_multiplicative_decrease():
    limiter = limiter_at(100)
    limiter.in_flight = 1
    limiter.release(NOLOAD, dropped=True)
    assert limiter.limit == 90
    assert limiter.drops == 1


def test_timeout_applies_multiplicative_decrease():
    limiter = limiter_at(100, timeout=0.05)
    sample(limiter, 0.06, in_flight=60)
    assert limiter.limit == 90


def test_stall_applies_multiplicative_decrease_down_to_min():
    limiter = limiter_at(100)
    limiter.on_stall()
    assert limiter.limit == 90
    for _ in range(100):
        limiter.on_stall()
    assert limiter.effective_limit == 2
    assert limiter.stalls == 101


def test_limit_never_exceeds_max():
    limiter = VegasLimiter(LimiterConfig(initial_limit=95, max_limit=100))
    limiter.rtt_noload = NOLOAD
    for _ in range(10):
        sample(limiter, NOLOAD, in_flight=90)
    assert limiter.effective_limit == 100


def test_try_acquire_sheds_at_the_limit():
    limiter = VegasLimiter(LimiterConfig(initial_limit=3, min_limit=3, max_limit=3))
    assert [limiter.try_acquire() for _ in range(5)] == [True, True, True, False, False]
    assert limiter.in_flight == 3
    assert limiter.shed == 2


def test_light_jittery_traffic_keeps_the_limit_stable():
    """Sequential traffic with latency jitter must not erode the limit (no hold band / gate would)."""
    limiter = VegasLimiter(LimiterConfig(initial_limit=50))
    rng = random.Random(7)
    for _ in range(2000):
        assert limiter.try_acquire()
        limiter.release(NOLOAD * rng.uniform(1.0, 1.5))
    assert limiter.limit == 50


def test_probe_rebaselines_after_a_permanent_workload_shift():
    limiter = VegasLimiter(LimiterConfig(initial_limit=10, probe_multiplier=5))
    for _ in range(3):
        sample(limiter, NOLOAD, in_flight=8)
    shifted = NOLOAD * 5
    for _ in range(200):
        sample(limiter, shifted, in_flight=int(limiter.limit))
    assert limiter.rtt_noload == pytest.approx(shifted)
    assert limiter.effective_limit > 10
