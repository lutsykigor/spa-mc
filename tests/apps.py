"""Handlers loaded by the worker subinterpreters during tests (imported by module path)."""

import asyncio
import os
import threading
import time
from concurrent import interpreters

import spa_mc


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = await reader.readline()
    writer.write(data)
    await writer.drain()


async def whoami(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await reader.readline()
    ctx = spa_mc.current()
    interp_id = interpreters.get_current().id
    cpus = spa_mc.topology.current_thread_cpus()
    affinity = ','.join(map(str, sorted(cpus))) if cpus is not None else '-'
    writer.write(
        f'{ctx.worker_id} {interp_id} {threading.get_native_id()} {os.getpid()} {ctx.core} {affinity}\n'.encode()
    )
    await writer.drain()


async def cpu(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    line = await reader.readline()
    n = int(line)
    total = 0
    for i in range(n):
        total += i * i
    writer.write(f'{total}\n'.encode())
    await writer.drain()


async def slow(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await reader.readline()
    await asyncio.sleep(0.2)
    writer.write(b'done\n')
    await writer.drain()


async def stall_on_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    line = await reader.readline()
    if line.startswith(b'STALL'):
        time.sleep(float(line.split()[1]))
    writer.write(f'{spa_mc.current().worker_id}\n'.encode())
    await writer.drain()


async def crash_or_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = await reader.readline()
    if data == b'crash\n':
        raise ValueError('boom')
    writer.write(data)
    await writer.drain()


def stall_worker_zero_periodically(ctx: spa_mc.WorkerContext) -> None:
    """Simulated noisy neighbour: worker 0 loses its core for 60 ms every 250 ms."""
    if ctx.worker_id != 0:
        return
    loop = asyncio.get_running_loop()

    def steal() -> None:
        time.sleep(0.06)
        loop.call_later(0.25, steal)

    loop.call_later(0.25, steal)


def fail_on_start(ctx: spa_mc.WorkerContext) -> None:
    if ctx.worker_id == 1:
        raise RuntimeError('worker 1 refuses to start')


_active = 0
_peak = 0


async def peak_concurrency(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Holds each request 0.2 s and replies with the highest number of requests seen running at once."""
    global _active, _peak
    await reader.readline()
    _active += 1
    _peak = max(_peak, _active)
    try:
        await asyncio.sleep(0.2)
    finally:
        _active -= 1
    writer.write(f'{_peak}\n'.encode())
    await writer.drain()


def take_worker_zeros_core_for_a_second(ctx: spa_mc.WorkerContext) -> None:
    """Keeps worker 0 off its CPU 15 ms out of every 20 ms for about one second, then leaves it alone."""
    if ctx.worker_id != 0:
        return
    loop = asyncio.get_running_loop()
    until = loop.time() + 1.0

    def steal() -> None:
        time.sleep(0.015)
        if loop.time() < until:
            loop.call_later(0.005, steal)

    loop.call_soon(steal)
