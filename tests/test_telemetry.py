import threading
import time
from concurrent import interpreters

from spa_mc import telemetry
from spa_mc.telemetry import STRIDE, WorkerState

FIELDS = dict(
    state=WorkerState.READY,
    in_flight=3,
    limit=40,
    core=2,
    received=10,
    completed=7,
    shed=1,
    stalls=0,
    rtt=0.002,
    rtt_noload=0.001,
    loop_lag=0.0005,
    max_loop_lag=0.004,
)

STRESS_WRITER = """
import time
from spa_mc import telemetry
from spa_mc.telemetry import WorkerState
writer = telemetry.SlotWriter(slab, 1)
deadline = time.monotonic() + duration
k = 0
while time.monotonic() < deadline:
    k += 1
    writer.publish(state=WorkerState.READY, in_flight=k % 1000, limit=k % 1000, core=k % 1000,
                   received=k, completed=k, shed=k, stalls=k, rtt=k / 1000, rtt_noload=k / 1000,
                   loop_lag=k / 1000, max_loop_lag=k / 1000)
"""


def test_slots_round_trip_and_are_cache_line_spaced():
    slab = telemetry.allocate(3)
    assert len(slab) == 3 * STRIDE
    telemetry.SlotWriter(slab, 1).publish(**FIELDS)
    stats = telemetry.read(slab, 1)
    assert (stats.state, stats.in_flight, stats.limit, stats.core) == (WorkerState.READY, 3, 40, 2)
    assert (stats.received, stats.completed, stats.shed) == (10, 7, 1)
    assert stats.rtt_ms == 2.0 and stats.loop_lag_ms == 0.5
    assert telemetry.read(slab, 0).state is WorkerState.STARTING
    assert bytes(slab[2 * STRIDE :]) == bytes(STRIDE)


def test_writes_from_a_subinterpreter_are_visible_without_copying():
    slab = telemetry.allocate(2)
    interp = interpreters.create()
    try:
        interp.prepare_main(slab=slab)
        interp.exec(
            'from spa_mc import telemetry\nfrom spa_mc.telemetry import WorkerState\n'
            'telemetry.SlotWriter(slab, 1).publish(state=WorkerState.READY, in_flight=5, limit=9, '
            'core=-1, received=1, completed=0, shed=0, stalls=0, rtt=0.0, rtt_noload=float("inf"), '
            'loop_lag=0.0, max_loop_lag=0.0)'
        )
    finally:
        interp.close()
    stats = telemetry.read(slab, 1)
    assert (stats.state, stats.in_flight, stats.limit, stats.core) == (WorkerState.READY, 5, 9, -1)


def test_seqlock_reads_are_never_torn_under_a_parallel_writer():
    slab = telemetry.allocate(2)
    interp = interpreters.create()
    interp.prepare_main(slab=slab, duration=1.0)
    writer = threading.Thread(target=interp.exec, args=(STRESS_WRITER,))
    writer.start()
    reads = inconsistent = 0
    try:
        while writer.is_alive():
            stats = telemetry.read(slab, 1)
            if stats.received == 0:
                continue
            reads += 1
            k = stats.received
            expected = (k, k, k, k % 1000, k % 1000)
            if (stats.completed, stats.shed, stats.stalls, stats.limit, stats.in_flight) != expected:
                inconsistent += 1
            elif abs(stats.rtt_ms - k) > 1e-6 or abs(stats.max_loop_lag_ms - k) > 1e-6:
                inconsistent += 1
            time.sleep(0)
    finally:
        writer.join()
        interp.close()
    assert inconsistent == 0
    assert reads > 100


def test_try_read_gives_up_on_a_slot_left_mid_update():
    slab = telemetry.allocate(1)
    telemetry.SlotWriter(slab, 0).publish(**FIELDS)
    telemetry._SEQ.pack_into(slab, 0, 3)
    assert telemetry.try_read(slab, 0) is None
