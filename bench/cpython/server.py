"""Benchmark server for the CPython asyncio multi-core branch.

Prints `READY <port>`, serves until its stdin is closed, then shuts down (Linux, macOS, Windows).
Runs on any Python: modes that need `asyncio.start_server(workers=...)` fail clearly on a build without it.

usage: server.py MODE N [--work ITERATIONS] [--protocol line|http] [--routing R] [--noisy]

MODE   single   one asyncio event loop (N is ignored)
       workers  asyncio.start_server(..., workers=N); N may be `auto`
       prefork  N processes, each one asyncio loop, sharing one listening socket
       threads  one asyncio loop handing the work to N threads (meaningful on a free-threaded build)
--work       pure-Python loop iterations per request (0 = I/O only: measures connection handling)
--protocol   `line`: read one line, reply one line; `http`: minimal HTTP/1.1 with `Connection: close`,
             for external load tools (bombardier, oha, wrk) run with keep-alive disabled
"""

import argparse
import asyncio
import multiprocessing
import os
import signal
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor

WORK = int(os.environ.get('BENCH_WORK', '50000'))
PROTOCOL = os.environ.get('BENCH_PROTOCOL', 'line')
HTTP_REPLY = b'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s'


def work(n: int) -> int:
    total = 0
    for i in range(n):
        total += i * i
    return total


async def read_request(reader: asyncio.StreamReader) -> None:
    if PROTOCOL == 'http':
        await reader.readuntil(b'\r\n\r\n')
    else:
        await reader.readline()


def reply(writer: asyncio.StreamWriter, result: int) -> None:
    body = b'%d\n' % result
    writer.write(HTTP_REPLY % (len(body), body) if PROTOCOL == 'http' else body)


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await read_request(reader)
    reply(writer, work(WORK))
    await writer.drain()
    writer.close()


async def threaded_handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await read_request(reader)
    reply(writer, await asyncio.to_thread(work, WORK))
    await writer.drain()
    writer.close()


def steal_worker_zero(worker) -> None:
    """Simulated noisy neighbour: worker 0 loses its CPU for 60 ms every 250 ms."""
    if worker.worker_id != 0:
        return
    loop = asyncio.get_running_loop()

    def block() -> None:
        time.sleep(0.06)
        loop.call_later(0.25, block)

    loop.call_later(0.25, block)


def main() -> None:
    global WORK, PROTOCOL
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['single', 'workers', 'prefork', 'threads'])
    parser.add_argument('n')
    parser.add_argument('--work', type=int, default=WORK)
    parser.add_argument('--protocol', choices=['line', 'http'], default=PROTOCOL)
    parser.add_argument('--routing', default='least_loaded')
    parser.add_argument('--noisy', action='store_true')
    args = parser.parse_args()
    # Worker interpreters and spawned processes import this module afresh and read these.
    os.environ['BENCH_WORK'] = str(args.work)
    os.environ['BENCH_PROTOCOL'] = args.protocol
    WORK, PROTOCOL = args.work, args.protocol
    if args.mode == 'prefork':
        prefork(int(args.n))
    else:
        asyncio.run(serve(args))


async def serve(args: argparse.Namespace) -> None:
    if args.mode == 'workers':
        n = args.n if args.n == 'auto' else int(args.n)
        options = {'routing': args.routing}
        if args.noisy:
            options['on_start'] = steal_worker_zero
        server = await asyncio.start_server(handle, '127.0.0.1', 0, backlog=4096, workers=n, **options)
    else:
        callback = handle
        if args.mode == 'threads':
            asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=int(args.n)))
            callback = threaded_handle
        server = await asyncio.start_server(callback, '127.0.0.1', 0, backlog=4096)
    announce(server.sockets[0].getsockname()[1])
    await asyncio.to_thread(sys.stdin.buffer.read)
    if args.mode == 'workers':
        active = server.active_workers()
        print(f'  {active.count(True)} of {len(active)} workers active at the end', file=sys.stderr)
    server.close()
    await server.wait_closed()


def prefork(workers: int) -> None:
    listener = socket.create_server(('127.0.0.1', 0), backlog=4096)
    listener.setblocking(False)
    if sys.platform == 'win32':
        context = multiprocessing.get_context('spawn')
        children = []
        for _ in range(workers):
            parent_end, child_end = context.Pipe()
            child = context.Process(target=windows_child, args=(child_end, WORK, PROTOCOL))
            child.start()
            parent_end.send(listener.share(child.pid))
            children.append(child)
    else:
        children = []
        for _ in range(workers):
            pid = os.fork()
            if pid == 0:
                asyncio.run(child_loop(listener))
                os._exit(0)
            children.append(pid)
    announce(listener.getsockname()[1])
    sys.stdin.buffer.read()
    for child in children:
        if isinstance(child, int):
            os.kill(child, signal.SIGTERM)
            os.waitpid(child, 0)
        else:
            child.terminate()
            child.join()


def windows_child(pipe, work_iterations: int, protocol: str) -> None:
    global WORK, PROTOCOL
    WORK, PROTOCOL = work_iterations, protocol
    listener = socket.fromshare(pipe.recv())
    listener.setblocking(False)
    asyncio.run(child_loop(listener))


async def child_loop(listener: socket.socket) -> None:
    server = await asyncio.start_server(handle, sock=listener)
    await server.serve_forever()


def announce(port: int) -> None:
    print(f'READY {port}', flush=True)


if __name__ == '__main__':
    main()
