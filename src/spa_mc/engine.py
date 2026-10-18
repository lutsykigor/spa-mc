"""Main-interpreter side: starts the worker subinterpreters and routes connections to them."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import socket
import sys
import threading
import time
import traceback
from collections.abc import Callable
from concurrent import interpreters
from typing import Literal

from spa_mc import handlers, telemetry, topology
from spa_mc.autoscale import Autoscaler, WorkerSample
from spa_mc.limiter import LimiterConfig
from spa_mc.telemetry import WorkerState, WorkerStats
from spa_mc.worker import STOP, shed

Routing = Literal['least_loaded', 'round_robin', 'reuseport']
Overload = Literal['queue', 'shed']

_BOOTSTRAP = """
import sys
sys.path[:] = list(_spa_mc_path)
from spa_mc.worker import worker_main
worker_main(_spa_mc_config, _spa_mc_slab, _spa_mc_inbox, _spa_mc_doorbell)
"""

UNLIMITED = 1 << 30
INBOX_CAPACITY = 1 << 16
CAPACITY_POLL = 0.001
DOORBELL = b'\x01'


class WorkerStartError(RuntimeError):
    pass


@dataclasses.dataclass(slots=True)
class RouterStats:
    dispatched: list[int]
    shed: int
    paused_ms: float
    active: list[bool]


class Engine:
    def __init__(
        self,
        handler: str | Callable,
        *,
        workers: int | Literal['auto'] = 'auto',
        pin: bool = True,
        routing: Routing = 'least_loaded',
        limiter: LimiterConfig | None = LimiterConfig(),
        overload: Overload = 'queue',
        worker_queue: int = 1024,
        max_queue_wait: float | None = None,
        shed_response: bytes = b'',
        on_start: str | Callable | None = None,
        request_timeout: float | None = None,
        watchdog_interval: float = 0.005,
        stall_threshold: float = 0.02,
        startup_timeout: float = 10.0,
        shutdown_grace: float = 5.0,
        adapt_interval: float = 1.0,
        park_on_steal: bool = False,
        stream_limit: int = 2**16,
    ) -> None:
        if routing == 'reuseport' and not hasattr(socket, 'SO_REUSEPORT'):
            raise ValueError('reuseport routing needs SO_REUSEPORT')
        if overload not in ('queue', 'shed'):
            raise ValueError(f"overload must be 'queue' or 'shed', not {overload!r}")
        self.auto = workers == 'auto'
        if self.auto:
            budget = topology.cpu_budget()
            self.capacity = max(len(topology.available_cpus()), len(budget))
            self._initial_cores = budget if pin else [-1] * len(budget)
        elif isinstance(workers, int) and workers > 0:
            cpus = topology.physical_cpus()
            self.capacity = workers
            self._initial_cores = [cpus[i % len(cpus)] if pin else -1 for i in range(workers)]
        else:
            raise ValueError(f"workers must be a positive int or 'auto', not {workers!r}")
        self.pin = pin
        self.workers = 0
        self.cores: list[int] = []
        self.adapt_interval = adapt_interval
        self.routing = routing
        self.overload = overload
        self.worker_queue = worker_queue if overload == 'queue' else 0
        self.shed_response = shed_response
        self.watchdog_interval = watchdog_interval
        self.stall_threshold = stall_threshold
        self.startup_timeout = startup_timeout
        self._stale_after = max(stall_threshold, 4 * watchdog_interval)
        self._base_config = {
            'handler': handlers.reference(handler),
            'on_start': handlers.reference(on_start, 'on_start') if on_start else None,
            'main_path': handlers.main_path(),
            'stream_limit': stream_limit,
            'limiter': dataclasses.asdict(limiter) if limiter else None,
            'overload': overload,
            'max_queue_wait': max_queue_wait,
            'shed_response': shed_response.decode('latin-1'),
            'request_timeout': request_timeout,
            'watchdog_interval': watchdog_interval,
            'stall_threshold': stall_threshold,
            'shutdown_grace': shutdown_grace,
            'routing': routing,
            'host': None,
            'port': None,
            'backlog': 1024,
        }
        self.slab = telemetry.allocate(self.capacity)
        self._interps: list[interpreters.Interpreter] = []
        self._threads: list[threading.Thread] = []
        self._inboxes: list[interpreters.Queue] = []
        self._doorbells: list[socket.socket] = []
        self._failures: dict[int, BaseException] = {}
        self._dispatched: list[int] = []
        self._active: list[bool] = []
        self._autoscaler = Autoscaler(park_on_steal=park_on_steal)
        self._steal_seen: dict[int, tuple[float, float]] = {}
        self._adapter: asyncio.Task | None = None
        self._router_shed = 0
        self._paused = 0.0
        self._next = 0
        self._acceptor: asyncio.Task | None = None
        self._stopped_acceptors: list[asyncio.Task] = []
        self._listener: socket.socket | None = None
        self._started = False

    async def __aenter__(self) -> Engine:
        if self.routing != 'reuseport':
            await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._started:
            raise RuntimeError('engine already started')
        if self.routing == 'reuseport' and self._base_config['port'] is None:
            raise RuntimeError('reuseport workers bind their own listeners: call serve_tcp() instead of start()')
        self._started = True
        for core in self._initial_cores:
            self._spawn(core)
        failure = await self._wait_ready(range(self.workers))
        if failure:
            await self.stop()
            raise WorkerStartError(failure)
        # With reuseport the kernel picks the worker, so there is nothing to park; the count stays as started.
        if self.auto and self.routing != 'reuseport':
            self._adapter = asyncio.get_running_loop().create_task(self._adapt_loop())

    async def serve_tcp(
        self,
        host: str | None = '127.0.0.1',
        port: int = 0,
        backlog: int = 1024,
        *,
        sock: socket.socket | None = None,
        start_serving: bool = True,
    ) -> tuple[str, int]:
        """Listen on `host:port` (all interfaces when `host` is None) or on an already-bound `sock`."""
        if self.routing == 'reuseport':
            if sock is not None:
                raise ValueError('reuseport workers bind their own listeners; sock= is not supported')
            host = host or '0.0.0.0'
            port = port or _free_port(host)
            self._base_config.update(host=host, port=port, backlog=backlog)
            await self.start()
            return host, port
        if not self._started:
            await self.start()
        if sock is not None:
            listener = sock
        elif host is None and socket.has_dualstack_ipv6():
            listener = socket.create_server(('', port), family=socket.AF_INET6, backlog=backlog, dualstack_ipv6=True)
        else:
            listener = socket.create_server((host or '', port), backlog=backlog)
        listener.setblocking(False)
        self._listener = listener
        if start_serving:
            self.start_serving()
        return listener.getsockname()[:2]

    @property
    def listener(self) -> socket.socket | None:
        return self._listener

    def start_serving(self) -> None:
        if self._listener is not None and self._acceptor is None:
            self._acceptor = asyncio.get_running_loop().create_task(self._accept_loop(self._listener))

    def is_serving(self) -> bool:
        if self.routing == 'reuseport':
            return self._started and bool(self._threads)
        return self._acceptor is not None

    def stop_serving(self) -> None:
        """Stop accepting new connections at once; connections already accepted carry on."""
        if self._acceptor is not None:
            self._acceptor.cancel()
            self._stopped_acceptors.append(self._acceptor)
            self._acceptor = None
        if self._listener is not None:
            # Unregister before closing: the next socket can reuse this fd number while the loop still watches it.
            try:
                asyncio.get_running_loop().remove_reader(self._listener.fileno())
            except NotImplementedError:
                pass
            self._listener.close()
            self._listener = None

    def dispatch(self, sock: socket.socket) -> int | None:
        """Hand an accepted socket to a worker; returns its id, or None when the connection was shed.

        With `overload='queue'` a connection is only shed if no worker is running at all; when every worker is
        full it goes to the least loaded one and waits there.
        """
        worker_id = self._pick()
        if worker_id is None and self.overload == 'queue':
            worker_id = self._pick(ignore_capacity=True)
        if worker_id is None:
            self._shed(sock)
            return None
        fd = sock.detach()
        try:
            self._inboxes[worker_id].put_nowait(fd)
        except interpreters.QueueFull:
            self._shed(socket.socket(fileno=fd))
            return None
        self._dispatched[worker_id] += 1
        self._ring(worker_id)
        return worker_id

    def stats(self) -> list[WorkerStats]:
        return [telemetry.read(self.slab, worker_id) for worker_id in range(self.workers)]

    def router_stats(self) -> RouterStats:
        return RouterStats(
            dispatched=list(self._dispatched),
            shed=self._router_shed,
            paused_ms=self._paused * 1000,
            active=list(self._active),
        )

    async def stop(self) -> None:
        if self._adapter:
            self._adapter.cancel()
            await asyncio.gather(self._adapter, return_exceptions=True)
            self._adapter = None
        self.stop_serving()
        await asyncio.gather(*self._stopped_acceptors, return_exceptions=True)
        self._stopped_acceptors.clear()
        for worker_id, inbox in enumerate(self._inboxes):
            inbox.put(STOP)
            self._ring(worker_id)
        for thread in self._threads:
            await asyncio.to_thread(thread.join)
        for interp in self._interps:
            interp.close()
        for doorbell in self._doorbells:
            doorbell.close()
        self._interps.clear()
        self._threads.clear()
        self._inboxes.clear()
        self._doorbells.clear()

    def _spawn(self, core: int) -> int:
        worker_id = self.workers
        inbox = interpreters.create_queue(maxsize=INBOX_CAPACITY)
        doorbell, worker_end = socket.socketpair()
        doorbell.setblocking(False)
        config = dict(self._base_config, worker_id=worker_id, core=core)
        interp = interpreters.create()
        interp.prepare_main(
            _spa_mc_path=tuple(sys.path),
            _spa_mc_config=json.dumps(config),
            _spa_mc_slab=self.slab,
            _spa_mc_inbox=inbox,
            _spa_mc_doorbell=worker_end.detach(),
        )
        thread = threading.Thread(target=self._run, args=(worker_id, interp), name=f'spa-mc-worker-{worker_id}')
        self._interps.append(interp)
        self._threads.append(thread)
        self._inboxes.append(inbox)
        self._doorbells.append(doorbell)
        self.cores.append(core)
        self._dispatched.append(0)
        self._active.append(True)
        self.workers += 1
        thread.start()
        return worker_id

    def _ring(self, worker_id: int) -> None:
        try:
            self._doorbells[worker_id].send(DOORBELL)
        except BlockingIOError:
            # The buffer is full of unread rings, so the worker is already due to wake and drain its inbox.
            pass

    def _run(self, worker_id: int, interp: interpreters.Interpreter) -> None:
        try:
            interp.exec(_BOOTSTRAP)
        except interpreters.ExecutionFailed as exc:
            self._failures[worker_id] = exc

    async def _wait_ready(self, worker_ids: range | list[int]) -> str | None:
        """None once every given worker is READY; otherwise a description of why not."""
        deadline = time.monotonic() + self.startup_timeout
        while True:
            states = [telemetry.read(self.slab, worker_id).state for worker_id in worker_ids]
            failed = {w: exc for w, exc in self._failures.items() if w in worker_ids}
            if failed or WorkerState.FAILED in states:
                threads = [self._threads[w] for w in worker_ids]
                await asyncio.to_thread(lambda: [t.join(1.0) for t in threads])
                return _describe_failures({w: exc for w, exc in self._failures.items() if w in worker_ids})
            if all(state is WorkerState.READY for state in states):
                return None
            if time.monotonic() > deadline:
                return f'workers not ready after {self.startup_timeout}s: {states}'
            await asyncio.sleep(0.005)

    async def _adapt_loop(self) -> None:
        while True:
            await asyncio.sleep(self.adapt_interval)
            try:
                await self._adapt()
            except Exception:
                # Adaptation is an optimisation; a failed round must not take the engine down.
                traceback.print_exc(file=sys.stderr)

    async def _adapt(self) -> None:
        now = time.monotonic()
        samples = []
        for worker_id in range(self.workers):
            stats = telemetry.try_read(self.slab, worker_id)
            if stats is None or stats.state is not WorkerState.READY:
                continue
            seen_steal, seen_at = self._steal_seen.get(worker_id, (stats.steal_s, now))
            self._steal_seen[worker_id] = (stats.steal_s, now)
            ratio = (stats.steal_s - seen_steal) / (now - seen_at) if now > seen_at else 0.0
            samples.append(WorkerSample(worker_id, self._active[worker_id], ratio))
        budget = topology.cpu_budget()
        decision = self._autoscaler.decide(samples, len(budget))
        for worker_id in decision.park:
            self._active[worker_id] = False
        for worker_id in decision.unpark:
            self._active[worker_id] = True
        for _ in range(min(decision.spawn, self.capacity - self.workers)):
            free = [cpu for cpu in budget if cpu not in self.cores]
            if self.pin and not free:
                break
            worker_id = self._spawn(free[0] if self.pin else -1)
            failure = await self._wait_ready([worker_id])
            if failure:
                self._active[worker_id] = False
                print(f'spa-mc: could not add a worker: {failure}', file=sys.stderr)

    async def _accept_loop(self, listener: socket.socket) -> None:
        loop = asyncio.get_running_loop()
        while True:
            if self.overload == 'queue':
                await self._wait_for_capacity()
            try:
                sock, _ = await loop.sock_accept(listener)
            except OSError:
                traceback.print_exc(file=sys.stderr)
                await asyncio.sleep(0.01)
                continue
            try:
                self.dispatch(sock)
            except Exception:
                # One bad connection must never stop the acceptor.
                traceback.print_exc(file=sys.stderr)
                sock.close()

    async def _wait_for_capacity(self) -> None:
        """Stop accepting while every worker is full, so new clients wait in the kernel's accept queue."""
        if self._pick(peek=True) is not None:
            return
        started = time.monotonic()
        while self._pick(peek=True) is None:
            await asyncio.sleep(CAPACITY_POLL)
        self._paused += time.monotonic() - started

    def _pick(self, *, ignore_capacity: bool = False, peek: bool = False) -> int | None:
        if self.routing == 'round_robin':
            return self._pick_round_robin(ignore_capacity, peek)
        return self._pick_least_loaded(ignore_capacity, peek)

    def _capacity(self, stats: WorkerStats, ignore_capacity: bool) -> float:
        if ignore_capacity or not stats.limit:
            return UNLIMITED
        return stats.limit + self.worker_queue

    def _outstanding(self, stats: WorkerStats) -> int:
        return self._dispatched[stats.worker_id] - stats.completed - stats.shed

    def _pick_round_robin(self, ignore_capacity: bool, peek: bool) -> int | None:
        for offset in range(self.workers):
            worker_id = (self._next + offset) % self.workers
            if not self._active[worker_id]:
                continue
            stats = telemetry.try_read(self.slab, worker_id)
            if stats is None or stats.state is not WorkerState.READY:
                continue
            if self._outstanding(stats) >= self._capacity(stats, ignore_capacity):
                continue
            if not peek:
                self._next = (worker_id + 1) % self.workers
            return worker_id
        return None

    def _pick_least_loaded(self, ignore_capacity: bool, peek: bool) -> int | None:
        now = time.monotonic()
        healthy: tuple[float, int] | None = None
        fallback: tuple[float, int] | None = None
        threshold_ms = self.stall_threshold * 1000.0
        for offset in range(self.workers):
            worker_id = (self._next + offset) % self.workers
            if not self._active[worker_id]:
                continue
            stats = telemetry.try_read(self.slab, worker_id)
            if stats is None or stats.state is not WorkerState.READY:
                continue
            limit = stats.limit or UNLIMITED
            outstanding = self._outstanding(stats)
            if outstanding >= self._capacity(stats, ignore_capacity):
                continue
            # Queue depth dominates; loop lag scales it so sub-threshold jitter cannot outweigh one request.
            score = (outstanding + 1) / limit * (1 + stats.loop_lag_ms / threshold_ms)
            candidate = (score, worker_id)
            if now - stats.heartbeat <= self._stale_after:
                healthy = min(healthy, candidate) if healthy else candidate
            else:
                fallback = min(fallback, candidate) if fallback else candidate
        if not peek:
            self._next = (self._next + 1) % self.workers
        chosen = healthy or fallback
        return chosen[1] if chosen else None

    def _shed(self, sock: socket.socket) -> None:
        self._router_shed += 1
        shed(sock, self.shed_response, asyncio.get_running_loop())


def _free_port(host: str) -> int:
    with socket.socket() as probe:
        probe.bind((host, 0))
        return probe.getsockname()[1]


def _describe_failures(failures: dict[int, BaseException]) -> str:
    if not failures:
        return 'a worker reported FAILED during startup'
    return '; '.join(f'worker {worker_id}: {exc}' for worker_id, exc in sorted(failures.items()))
