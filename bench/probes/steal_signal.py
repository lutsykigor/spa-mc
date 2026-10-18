"""Checks the worker steal signal: near 0 when idle or merely busy, clearly up when other processes take the CPUs.

usage: python bench/probes/steal_signal.py
"""

import asyncio
import multiprocessing
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import spa_mc  # noqa: E402


def hog(seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        pass


async def measure(engine, seconds):
    first = {s.worker_id: s.steal_s for s in engine.stats()}
    t0 = time.monotonic()
    await asyncio.sleep(seconds)
    dt = time.monotonic() - t0
    return [round((s.steal_s - first[s.worker_id]) / dt, 3) for s in engine.stats()]


async def main():
    async with spa_mc.Engine('bench_app:handle', workers=4, pin=False) as engine:
        print('idle                 :', await measure(engine, 2))
        host, port = await engine.serve_tcp()
        stop = asyncio.Event()

        async def client():
            while not stop.is_set():
                reader, writer = await asyncio.open_connection(host, port)
                writer.write(b'50000\n')
                await writer.drain()
                await reader.read()
                writer.close()

        clients = [asyncio.create_task(client()) for _ in range(16)]
        print('busy, no hogs        :', await measure(engine, 2))
        hogs = [multiprocessing.Process(target=hog, args=(4,)) for _ in range(2 * os.cpu_count())]
        for h in hogs:
            h.start()
        await asyncio.sleep(0.5)
        print(f'busy, {len(hogs)} CPU hogs  :', await measure(engine, 2))
        for h in hogs:
            h.join()
        stop.set()
        await asyncio.gather(*clients, return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(main())
