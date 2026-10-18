"""Code that runs inside each worker subinterpreter: one pinned asyncio loop per core.

Module state here is per-interpreter (PEP 684), so `current()` returns the context of
whichever worker the caller is running in.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import inspect
import json
import socket
import sys
import time
import traceback
from collections import deque
from collections.abc import Awaitable, Callable, Iterator
from concurrent import interpreters
from dataclasses import dataclass

from spa_mc import handlers, telemetry, topology
from spa_mc.limiter import LimiterConfig, VegasLimiter
from spa_mc.telemetry import WorkerState

STOP = -1
SHED_LINGER = 1.0
# Idle timer wake-ups run up to ~1 ms late (on Windows once _fine_timers is on); only off-CPU time beyond this
# counts as steal.
TIMER_SLACK = 0.002
# time.thread_time() advances in whole scheduler ticks on Windows (1/64 s), so one watchdog tick can under-read its
# CPU time by that much; elsewhere it is precise.
CPU_CLOCK_STEP = 1 / 64 if sys.platform == 'win32' else 0.0

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@dataclass(slots=True, eq=False)
class _Waiting:
    sock: socket.socket
    expiry: asyncio.TimerHandle | None = None


@dataclass(frozen=True, slots=True)
class WorkerContext:
    worker_id: int
    core: int
    pinned: bool
    limiter: VegasLimiter | None


_context: WorkerContext | None = None


def current() -> WorkerContext:
    if _context is None:
        raise RuntimeError('not running inside a spa-mc worker')
    return _context


def worker_main(config_json: str, slab: memoryview, inbox: interpreters.Queue, doorbell_fd: int) -> None:
    config = json.loads(config_json)
    slot = telemetry.SlotWriter(slab, config['worker_id'])
    try:
        with _fine_timers():
            asyncio.run(_Worker(config, slot, inbox, doorbell_fd).run())
    except BaseException:
        _publish_failed(slot)
        raise


@contextlib.contextmanager
def _fine_timers() -> Iterator[None]:
    """Windows wakes event loops on a ~15.6 ms clock tick by default; ask for 1 ms while the worker runs.

    Otherwise every short sleep overshoots by ~10 ms, which the watchdog would read as loop lag and steal.
    """
    if sys.platform != 'win32':
        yield
        return
    winmm = ctypes.WinDLL('winmm')
    winmm.timeBeginPeriod(1)
    try:
        yield
    finally:
        winmm.timeEndPeriod(1)


def _publish_failed(slot: telemetry.SlotWriter) -> None:
    slot.publish(
        state=WorkerState.FAILED,
        in_flight=0,
        limit=0,
        core=-1,
        received=0,
        completed=0,
        shed=0,
        stalls=0,
        rtt=0.0,
        rtt_noload=0.0,
        loop_lag=0.0,
        max_loop_lag=0.0,
    )


class _Worker:
    def __init__(self, config: dict, slot: telemetry.SlotWriter, inbox: interpreters.Queue, doorbell_fd: int) -> None:
        self.config = config
        self.slot = slot
        self.inbox = inbox
        self.doorbell_fd = doorbell_fd
        self.state = WorkerState.STARTING
        self.core = -1
        self.limiter = VegasLimiter(LimiterConfig(**config['limiter'])) if config['limiter'] else None
        self.shed_response = config['shed_response'].encode('latin-1')
        self.overload = config['overload']
        self.max_queue_wait = config['max_queue_wait']
        self.waiting: deque[_Waiting] = deque()
        self.received = 0
        self.completed = 0
        self.shed = 0
        self.stalls = 0
        self.loop_lag = 0.0
        self.max_loop_lag = 0.0
        self.steal = 0.0
        self.tasks: set[asyncio.Task] = set()
        self.stopping = asyncio.Event()

    async def run(self) -> None:
        global _context
        self.loop = asyncio.get_running_loop()
        pinned = self._pin()
        _context = WorkerContext(self.config['worker_id'], self.core, pinned, self.limiter)
        self.handler: Handler = handlers.resolve(self.config['handler'], self.config['main_path'])
        if self.config['on_start']:
            result = handlers.resolve(self.config['on_start'], self.config['main_path'])(_context)
            if inspect.isawaitable(result):
                await result

        watchdog = self.loop.create_task(self._watchdog())
        close_ingress = self._open_ingress()
        self.state = WorkerState.READY
        self.publish()

        await self.stopping.wait()
        self.state = WorkerState.STOPPING
        self.publish()
        close_ingress()
        # Connections already handed to this worker are finished, queued ones included, within the grace period.
        deadline = self.loop.time() + self.config['shutdown_grace']
        while (self.tasks or self.waiting) and self.loop.time() < deadline:
            await asyncio.sleep(0.01)
        for entry in self.waiting:
            entry.sock.close()
        self.waiting.clear()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        watchdog.cancel()
        await asyncio.gather(watchdog, return_exceptions=True)
        self.state = WorkerState.STOPPED
        self.publish()

    def publish(self) -> None:
        limiter = self.limiter
        self.slot.publish(
            state=self.state,
            in_flight=limiter.in_flight if limiter else len(self.tasks),
            queued=len(self.waiting),
            limit=limiter.effective_limit if limiter else 0,
            core=self.core,
            received=self.received,
            completed=self.completed,
            shed=self.shed,
            stalls=self.stalls,
            rtt=limiter.rtt_short if limiter else 0.0,
            rtt_noload=limiter.rtt_noload if limiter else 0.0,
            loop_lag=self.loop_lag,
            max_loop_lag=self.max_loop_lag,
            steal=self.steal,
        )

    def _pin(self) -> bool:
        if self.config['core'] < 0:
            return False
        pinned = topology.pin_current_thread(self.config['core'])
        if pinned:
            self.core = self.config['core']
        return pinned

    def _open_ingress(self) -> Callable[[], None]:
        # The inbox also carries STOP, so it is read in every routing mode.
        doorbell = socket.socket(fileno=self.doorbell_fd)
        doorbell.setblocking(False)
        tasks = [self.loop.create_task(self._inbox_loop(doorbell))]
        listener = None
        if self.config['routing'] == 'reuseport':
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            listener.bind((self.config['host'], self.config['port']))
            listener.listen(self.config['backlog'])
            listener.setblocking(False)
            tasks.append(self.loop.create_task(self._accept_loop(listener)))

        def close() -> None:
            for task in tasks:
                task.cancel()
            doorbell.close()
            if listener:
                listener.close()

        return close

    async def _accept_loop(self, listener: socket.socket) -> None:
        while True:
            sock, _ = await self.loop.sock_accept(listener)
            self._admit(sock)

    async def _inbox_loop(self, doorbell: socket.socket) -> None:
        while True:
            if not await self.loop.sock_recv(doorbell, 4096):
                self.stopping.set()
                return
            while True:
                try:
                    fd = self.inbox.get_nowait()
                except interpreters.QueueEmpty:
                    break
                if fd == STOP:
                    self.stopping.set()
                    return
                sock = socket.socket(fileno=fd)
                sock.setblocking(False)
                self._admit(sock)

    def _admit(self, sock: socket.socket) -> None:
        self.received += 1
        if self._has_capacity() and not self.waiting:
            self._start(sock)
        elif self.overload == 'shed':
            self._shed(sock)
        else:
            self._enqueue(sock)
        self.publish()

    def _has_capacity(self) -> bool:
        return self.limiter is None or self.limiter.has_capacity

    def _start(self, sock: socket.socket) -> None:
        if self.limiter:
            self.limiter.try_acquire()
        task = self.loop.create_task(self._serve(sock))
        self.tasks.add(task)
        task.add_done_callback(self._on_done)

    def _enqueue(self, sock: socket.socket) -> None:
        entry = _Waiting(sock)
        if self.max_queue_wait is not None:
            entry.expiry = self.loop.call_later(self.max_queue_wait, self._expire, entry)
        self.waiting.append(entry)

    def _expire(self, entry: _Waiting) -> None:
        self.waiting.remove(entry)
        self._shed(entry.sock)
        self.publish()

    def _on_done(self, task: asyncio.Task) -> None:
        self.tasks.discard(task)
        while self.waiting and self._has_capacity():
            entry = self.waiting.popleft()
            if entry.expiry:
                entry.expiry.cancel()
            self._start(entry.sock)
        self.publish()

    def _shed(self, sock: socket.socket) -> None:
        self.shed += 1
        shed(sock, self.shed_response, self.loop)

    async def _serve(self, sock: socket.socket) -> None:
        started = self.loop.time()
        dropped = False
        try:
            reader = asyncio.StreamReader(limit=self.config['stream_limit'])
            protocol = asyncio.StreamReaderProtocol(reader)
            transport, _ = await self.loop.connect_accepted_socket(lambda: protocol, sock)
            writer = asyncio.StreamWriter(transport, protocol, reader, self.loop)
            try:
                async with asyncio.timeout(self.config['request_timeout']):
                    await self.handler(reader, writer)
            except TimeoutError:
                dropped = True
            finally:
                writer.close()
        except Exception:
            traceback.print_exc(file=sys.stderr)
        finally:
            self.completed += 1
            if self.limiter:
                self.limiter.release(self.loop.time() - started, dropped=dropped)
            self.publish()

    async def _watchdog(self) -> None:
        interval = self.config['watchdog_interval']
        threshold = self.config['stall_threshold'] + CPU_CLOCK_STEP
        last_wall, last_cpu = self.loop.time(), time.thread_time()
        while True:
            await asyncio.sleep(interval)
            wall, cpu = self.loop.time(), time.thread_time()
            elapsed = wall - last_wall
            # Lag spent running our own code is load (Vegas sees it in RTT); only time off the CPU is a stall.
            off_cpu = elapsed - (cpu - last_cpu) - interval
            last_wall, last_cpu = wall, cpu
            self.loop_lag = max(0.0, elapsed - interval)
            self.max_loop_lag = max(self.max_loop_lag, self.loop_lag)
            self.steal += max(0.0, off_cpu - TIMER_SLACK)
            if off_cpu >= threshold:
                self.stalls += 1
                if self.limiter:
                    self.limiter.on_stall()
            self.publish()


def shed(sock: socket.socket, response: bytes, loop: asyncio.AbstractEventLoop) -> None:
    """Reply and half-close; the full close is deferred so unread request bytes cannot turn it into an RST."""
    try:
        if response:
            sock.send(response)
        sock.shutdown(socket.SHUT_WR)
    except OSError:
        sock.close()
        return
    loop.call_later(SHED_LINGER, sock.close)
