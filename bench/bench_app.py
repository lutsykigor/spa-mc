"""The request handler every benchmarked server runs: read `n`, burn n loop iterations, reply."""

import asyncio
import time


def work(n: int) -> int:
    total = 0
    for i in range(n):
        total += i * i
    return total


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    n = int(await reader.readline())
    writer.write(f'{work(n)}\n'.encode())
    await writer.drain()
    writer.close()


def stall_worker_zero(ctx) -> None:
    """Simulated noisy neighbour: worker 0 loses its core for 60 ms every 250 ms."""
    if ctx.worker_id != 0:
        return
    loop = asyncio.get_running_loop()

    def steal() -> None:
        time.sleep(0.06)
        loop.call_later(0.25, steal)

    loop.call_later(0.25, steal)
