import asyncio
import os
import sys
import threading
import time
from concurrent import interpreters

import pytest

import spa_mc
from spa_mc import LimiterConfig, WorkerState, telemetry

linux = pytest.mark.skipif(not sys.platform.startswith('linux'), reason='Linux-only kernel feature')
can_pin = pytest.mark.skipif(sys.platform == 'darwin', reason='macOS has no thread-affinity API')


async def request(address: tuple[str, int], line: bytes) -> tuple[bytes, float]:
    started = time.perf_counter()
    reader, writer = await asyncio.open_connection(*address)
    writer.write(line)
    await writer.drain()
    reply = await reader.read()
    writer.close()
    return reply, time.perf_counter() - started


def open_fds() -> int | None:
    return len(os.listdir('/dev/fd')) if os.path.isdir('/dev/fd') else None


def test_workers_are_separate_interpreters_and_threads_in_one_process():
    async def scenario():
        async with spa_mc.Engine('apps:whoami', workers=4) as engine:
            address = await engine.serve_tcp()
            replies = await asyncio.gather(*(request(address, b'?\n') for _ in range(40)))
            return [reply.split() for reply, _ in replies]

    rows = asyncio.run(scenario())
    worker_ids = {row[0] for row in rows}
    interp_ids = {row[1] for row in rows}
    thread_ids = {row[2] for row in rows}
    pids = {row[3] for row in rows}
    assert len(worker_ids) == len(interp_ids) == len(thread_ids) == 4
    assert str(interpreters.get_main().id).encode() not in interp_ids
    assert pids == {str(os.getpid()).encode()}


def test_many_concurrent_clients_get_their_own_echo():
    async def scenario():
        async with spa_mc.Engine('apps:echo', workers=4) as engine:
            address = await engine.serve_tcp()
            payloads = [f'client-{i}\n'.encode() for i in range(300)]
            replies = await asyncio.gather(*(request(address, payload) for payload in payloads))
            return payloads, [reply for reply, _ in replies], engine.router_stats(), engine.stats()

    payloads, replies, router, stats = asyncio.run(scenario())
    assert replies == payloads
    assert sum(router.dispatched) == 300 and router.shed == 0
    assert all(count > 0 for count in router.dispatched)
    assert sum(s.completed for s in stats) == 300


@pytest.mark.timing
def test_cpu_bound_handlers_run_in_parallel_across_workers():
    work = b'1500000\n'
    expected = {str(sum(i * i for i in range(1500000))).encode() + b'\n'}

    async def elapsed(workers: int) -> float:
        async with spa_mc.Engine('apps:cpu', workers=workers) as engine:
            address = await engine.serve_tcp()
            await request(address, b'10\n')
            started = time.perf_counter()
            replies = await asyncio.gather(*(request(address, work) for _ in range(8)))
            finished = time.perf_counter()
            assert {reply for reply, _ in replies} == expected
            return finished - started

    one = asyncio.run(elapsed(1))
    four = asyncio.run(elapsed(4))
    assert one / four >= 2.0, f'1 worker {one:.3f}s, 4 workers {four:.3f}s'


@pytest.mark.parametrize(('routing', 'expect_avoided'), [('least_loaded', True), ('round_robin', False)])
def test_router_avoids_a_worker_whose_loop_is_stalled(routing, expect_avoided):
    async def scenario():
        async with spa_mc.Engine('apps:stall_on_command', workers=2, routing=routing) as engine:
            address = await engine.serve_tcp()
            stalled = asyncio.create_task(request(address, b'STALL 0.5\n'))
            await asyncio.sleep(0.1)
            quick = await asyncio.gather(*(request(address, b'go\n') for _ in range(20)))
            stalled_reply, _ = await stalled
            await asyncio.sleep(0.05)
            return int(stalled_reply), quick, engine.stats()

    stalled_worker, quick, stats = asyncio.run(scenario())
    served_by_stalled = [latency for reply, latency in quick if int(reply) == stalled_worker]
    if expect_avoided:
        assert served_by_stalled == []
        assert max(latency for _, latency in quick) < 0.25
    else:
        assert served_by_stalled and min(served_by_stalled) > 0.25
    assert stats[stalled_worker].stalls >= 1
    assert stats[stalled_worker].max_loop_lag_ms >= 300


def test_a_worker_frozen_mid_telemetry_write_does_not_stop_the_acceptor():
    async def scenario():
        async with spa_mc.Engine('apps:stall_on_command', workers=2) as engine:
            address = await engine.serve_tcp()
            stalled = asyncio.create_task(request(address, b'STALL 0.6\n'))
            await asyncio.sleep(0.1)
            frozen = next(s.worker_id for s in engine.stats() if s.received == 1)
            # Corrupt the frozen worker's record, as if it were preempted halfway through copying it.
            engine.slab[frozen * telemetry.STRIDE + 10] ^= 0xFF
            replies = await asyncio.wait_for(asyncio.gather(*(request(address, b'go\n') for _ in range(10))), 0.4)
            await stalled
            return frozen, [int(reply) for reply, _ in replies]

    frozen, served_by = asyncio.run(scenario())
    assert len(served_by) == 10 and frozen not in served_by


def test_valve_contracts_after_a_loop_stall():
    async def scenario():
        limiter = LimiterConfig(initial_limit=50)
        async with spa_mc.Engine('apps:stall_on_command', workers=1, limiter=limiter) as engine:
            address = await engine.serve_tcp()
            await request(address, b'STALL 0.1\n')
            await asyncio.sleep(0.05)
            return engine.stats()[0]

    stats = asyncio.run(scenario())
    assert stats.stalls >= 1
    assert stats.limit <= 45


def test_a_loop_busy_with_its_own_work_is_not_mistaken_for_a_stall():
    async def scenario():
        limiter = LimiterConfig(initial_limit=50)
        async with spa_mc.Engine('apps:cpu', workers=1, limiter=limiter) as engine:
            address = await engine.serve_tcp()
            for _ in range(8):
                await request(address, b'3000000\n')
            await asyncio.sleep(0.05)
            return engine.stats()[0]

    stats = asyncio.run(scenario())
    assert stats.max_loop_lag_ms >= 30
    # Treating lag as a stall would count 8+; real preemption on a busy machine may add one or two.
    assert stats.stalls <= 2
    assert stats.limit >= 50 * 0.9**2


FIXED_3 = LimiterConfig(initial_limit=3, min_limit=3, max_limit=3)


def test_excess_connections_wait_and_all_complete_without_exceeding_the_limit():
    async def scenario():
        async with spa_mc.Engine('apps:peak_concurrency', workers=1, limiter=FIXED_3) as engine:
            address = await engine.serve_tcp()
            started = time.perf_counter()
            replies = await asyncio.gather(*(request(address, b'x\n') for _ in range(10)))
            elapsed = time.perf_counter() - started
            return [reply for reply, _ in replies], elapsed, engine.router_stats(), engine.stats()[0]

    replies, elapsed, router, stats = asyncio.run(scenario())
    assert all(reply.strip().isdigit() for reply in replies)
    assert max(int(reply) for reply in replies) == 3
    assert elapsed >= 0.75
    assert router.shed == stats.shed == 0
    assert stats.completed == 10 and stats.queued == 0


def test_shed_mode_rejects_the_excess_with_the_shed_response():
    async def scenario():
        engine = spa_mc.Engine('apps:slow', workers=1, limiter=FIXED_3, overload='shed', shed_response=b'BUSY\n')
        async with engine:
            address = await engine.serve_tcp()
            replies = await asyncio.gather(*(request(address, b'x\n') for _ in range(10)))
            return [reply for reply, _ in replies], engine.router_stats(), engine.stats()[0]

    replies, router, stats = asyncio.run(scenario())
    assert replies.count(b'done\n') == 3
    assert replies.count(b'BUSY\n') == 7
    assert router.shed + stats.shed == 7


def test_max_queue_wait_sheds_only_connections_that_waited_too_long():
    async def scenario():
        limiter = LimiterConfig(initial_limit=1, min_limit=1, max_limit=1)
        engine = spa_mc.Engine('apps:slow', workers=1, limiter=limiter, max_queue_wait=0.1, shed_response=b'BUSY\n')
        async with engine:
            address = await engine.serve_tcp()
            first = asyncio.create_task(request(address, b'x\n'))
            await asyncio.sleep(0.05)
            late = await asyncio.gather(*(request(address, b'x\n') for _ in range(2)))
            return await first, late, engine.stats()[0]

    (first, _), late, stats = asyncio.run(scenario())
    assert first == b'done\n'
    assert [reply for reply, _ in late] == [b'BUSY\n', b'BUSY\n']
    assert all(0.09 <= latency < 0.19 for _, latency in late)
    assert stats.shed == 2


def test_router_pauses_accepting_when_every_worker_is_full():
    async def scenario():
        engine = spa_mc.Engine('apps:peak_concurrency', workers=1, limiter=FIXED_3, worker_queue=1)
        async with engine:
            address = await engine.serve_tcp()
            replies = await asyncio.gather(*(request(address, b'x\n') for _ in range(9)))
            return [reply for reply, _ in replies], engine.router_stats(), engine.stats()[0]

    replies, router, stats = asyncio.run(scenario())
    assert max(int(reply) for reply in replies) == 3
    assert router.paused_ms > 0
    assert router.shed == stats.shed == 0
    assert stats.completed == 9


def test_stop_finishes_connections_that_are_still_queued():
    async def scenario():
        limiter = LimiterConfig(initial_limit=1, min_limit=1, max_limit=1)
        engine = spa_mc.Engine('apps:slow', workers=1, limiter=limiter)
        await engine.start()
        address = await engine.serve_tcp()
        clients = [asyncio.create_task(request(address, b'x\n')) for _ in range(3)]
        await asyncio.sleep(0.1)
        assert engine.stats()[0].queued == 2
        await engine.stop()
        return [reply for reply, _ in await asyncio.gather(*clients)]

    assert asyncio.run(scenario()) == [b'done\n'] * 3


def test_overload_policy_is_validated():
    with pytest.raises(ValueError, match='overload'):
        spa_mc.Engine('apps:echo', overload='drop')


def test_request_timeout_counts_as_a_drop():
    async def scenario():
        limiter = LimiterConfig(initial_limit=20)
        async with spa_mc.Engine('apps:slow', workers=1, limiter=limiter, request_timeout=0.05) as engine:
            address = await engine.serve_tcp()
            reply, latency = await request(address, b'x\n')
            await asyncio.sleep(0.02)
            return reply, latency, engine.stats()[0]

    reply, latency, stats = asyncio.run(scenario())
    assert reply == b'' and latency < 0.15
    assert stats.limit == 18


def test_a_crashing_handler_does_not_take_the_worker_down():
    async def scenario():
        async with spa_mc.Engine('apps:crash_or_echo', workers=1) as engine:
            address = await engine.serve_tcp()
            crashed, _ = await request(address, b'crash\n')
            echoed, _ = await request(address, b'still alive\n')
            return crashed, echoed, engine.stats()[0]

    crashed, echoed, stats = asyncio.run(scenario())
    assert crashed == b''
    assert echoed == b'still alive\n'
    assert stats.state is WorkerState.READY and stats.completed == 2


@pytest.mark.parametrize(
    ('handler', 'on_start', 'message'),
    [('apps:echo', 'apps:fail_on_start', 'worker 1 refuses to start'), ('apps:missing', None, 'missing')],
)
def test_startup_failures_are_raised_and_cleaned_up(handler, on_start, message):
    threads_before = threading.active_count()

    async def scenario():
        async with spa_mc.Engine(handler, workers=2, on_start=on_start):
            pass

    with pytest.raises(spa_mc.WorkerStartError, match=message):
        asyncio.run(scenario())
    assert threading.active_count() == threads_before
    assert interpreters.list_all() == [interpreters.get_main()]


def test_stop_releases_threads_interpreters_and_descriptors():
    fds_before = open_fds()
    threads_before = threading.active_count()

    async def scenario():
        async with spa_mc.Engine('apps:echo', workers=3) as engine:
            address = await engine.serve_tcp()
            await asyncio.gather(*(request(address, b'x\n') for _ in range(30)))
            assert threading.active_count() == threads_before + 3

    asyncio.run(scenario())
    assert threading.active_count() == threads_before
    assert interpreters.list_all() == [interpreters.get_main()]
    if fds_before is not None:
        assert open_fds() <= fds_before


def test_handlers_see_their_own_worker_context():
    with pytest.raises(RuntimeError, match='not running inside a spa-mc worker'):
        spa_mc.current()


@linux
def test_reuseport_routing_spreads_connections_over_worker_listeners():
    async def scenario():
        engine = spa_mc.Engine('apps:whoami', workers=4, routing='reuseport')
        try:
            address = await engine.serve_tcp()
            replies = await asyncio.gather(*(request(address, b'?\n') for _ in range(200)))
            return [reply.split()[0] for reply, _ in replies], engine.stats()
        finally:
            await engine.stop()

    workers, stats = asyncio.run(scenario())
    assert len(set(workers)) == 4
    assert sum(s.completed for s in stats) == 200


@can_pin
def test_each_worker_thread_is_pinned_to_its_own_cpu():
    async def scenario():
        async with spa_mc.Engine('apps:whoami', workers=2) as engine:
            address = await engine.serve_tcp()
            replies = await asyncio.gather(*(request(address, b'?\n') for _ in range(20)))
            return {reply.split()[2]: (int(reply.split()[4]), reply.split()[5]) for reply, _ in replies}

    if len(spa_mc.topology.available_cpus()) < 2:
        pytest.skip('needs 2 CPUs')
    by_thread = asyncio.run(scenario())
    cores = {core for core, _ in by_thread.values()}
    assert len(by_thread) == 2 and len(cores) == 2
    assert all(cpus == str(core).encode() for core, cpus in by_thread.values())


async def wait_until(predicate, timeout: float) -> float:
    started = time.perf_counter()
    while not predicate():
        if time.perf_counter() - started > timeout:
            raise AssertionError(f'condition not met within {timeout}s')
        await asyncio.sleep(0.02)
    return time.perf_counter() - started


def test_auto_starts_one_worker_per_cpu_in_the_budget(monkeypatch):
    monkeypatch.setattr(spa_mc.topology, 'cpu_budget', lambda: [0, 1])

    async def scenario():
        async with spa_mc.Engine('apps:echo', pin=False) as engine:
            return engine.workers, engine.router_stats().active

    assert asyncio.run(scenario()) == (2, [True, True])


def test_a_worker_whose_core_is_taken_is_parked_then_returns(monkeypatch):
    monkeypatch.setattr(spa_mc.topology, 'cpu_budget', lambda: [0, 1, 2])

    async def scenario():
        engine = spa_mc.Engine(
            'apps:stall_on_command',
            pin=False,
            adapt_interval=0.1,
            park_on_steal=True,
            on_start='apps:take_worker_zeros_core_for_a_second',
        )
        async with engine:
            address = await engine.serve_tcp()
            await wait_until(lambda: not engine.router_stats().active[0], timeout=1.0)
            served_by = [
                int(reply) for reply, _ in await asyncio.gather(*(request(address, b'go\n') for _ in range(12)))
            ]
            await wait_until(lambda: engine.router_stats().active[0], timeout=3.0)
            return served_by, engine.router_stats().active

    served_by, active_after = asyncio.run(scenario())
    assert 0 not in served_by
    assert active_after == [True, True, True]


def test_the_worker_count_follows_a_changing_cpu_budget(monkeypatch):
    budget = [0, 1]
    monkeypatch.setattr(spa_mc.topology, 'cpu_budget', lambda: list(budget))

    async def scenario():
        async with spa_mc.Engine('apps:whoami', pin=False, adapt_interval=0.05) as engine:
            address = await engine.serve_tcp()
            budget[:] = [0]
            await wait_until(lambda: engine.router_stats().active.count(True) == 1, timeout=1.0)
            shrunk = {
                reply.split()[0] for reply, _ in await asyncio.gather(*(request(address, b'?\n') for _ in range(8)))
            }
            budget[:] = [0, 1, 2]
            await wait_until(lambda: engine.router_stats().active == [True, True, True], timeout=2.0)
            grown = {
                reply.split()[0] for reply, _ in await asyncio.gather(*(request(address, b'?\n') for _ in range(30)))
            }
            return shrunk, grown, engine.workers

    shrunk, grown, workers = asyncio.run(scenario())
    assert len(shrunk) == 1
    assert grown == {b'0', b'1', b'2'} and workers == 3


def test_fixed_worker_counts_are_never_adapted(monkeypatch):
    monkeypatch.setattr(spa_mc.topology, 'cpu_budget', lambda: [0])

    async def scenario():
        async with spa_mc.Engine('apps:echo', workers=3, pin=False, adapt_interval=0.05) as engine:
            await asyncio.sleep(0.3)
            return engine.workers, engine.router_stats().active

    assert asyncio.run(scenario()) == (3, [True, True, True])


@pytest.mark.parametrize('workers', [0, -1, 'many', None])
def test_worker_count_is_validated(workers):
    with pytest.raises(ValueError, match='workers'):
        spa_mc.Engine('apps:echo', workers=workers)


@linux
def test_auto_respects_the_container_cpu_quota():
    quota = spa_mc.topology.cpu_quota()
    if quota is None:
        pytest.skip('no cgroup CPU quota (run in a container started with --cpus)')

    async def scenario():
        async with spa_mc.Engine('apps:echo') as engine:
            return engine.workers

    assert asyncio.run(scenario()) == min(len(spa_mc.topology.physical_cpus()), max(1, -(-quota // 1)))


def test_steal_parking_is_off_by_default(monkeypatch):
    monkeypatch.setattr(spa_mc.topology, 'cpu_budget', lambda: [0, 1])

    async def scenario():
        engine = spa_mc.Engine(
            'apps:echo', pin=False, adapt_interval=0.1, on_start='apps:take_worker_zeros_core_for_a_second'
        )
        async with engine:
            await asyncio.sleep(0.8)
            return engine.router_stats().active, engine.stats()[0].steal_s

    active, steal = asyncio.run(scenario())
    assert steal > 0.3
    assert active == [True, True]
