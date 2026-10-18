"""Benchmark server in one of several concurrency models.

Prints `READY <port>`, serves until its stdin is closed, then shuts down (works on Linux, macOS and Windows).

usage: server.py MODE WORKERS [--noisy] [--routing R] [--no-limiter]
"""

import argparse
import asyncio
import multiprocessing
import os
import signal
import socket
import sys
from concurrent.futures import ThreadPoolExecutor

import bench_app

import spa_mc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['single', 'threads', 'prefork', 'spa-mc'])
    parser.add_argument('workers', type=int)
    parser.add_argument('--noisy', action='store_true')
    parser.add_argument('--routing', default='least_loaded')
    parser.add_argument('--no-limiter', action='store_true')
    args = parser.parse_args()
    if args.mode == 'prefork':
        prefork(args.workers)
    else:
        asyncio.run(serve(args))


async def serve(args: argparse.Namespace) -> None:
    if args.mode == 'spa-mc':
        limiter = None if args.no_limiter else spa_mc.LimiterConfig()
        on_start = 'bench_app:stall_worker_zero' if args.noisy else None
        engine = spa_mc.Engine(
            'bench_app:handle',
            workers=args.workers,
            routing=args.routing,
            limiter=limiter,
            on_start=on_start,
            shed_response=b'BUSY\n',
        )
        _, port = await engine.serve_tcp('127.0.0.1', 0, backlog=4096)
        announce(port)
        await wait_for_stdin_eof()
        await engine.stop()
        return
    handler = bench_app.handle
    if args.mode == 'threads':
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=args.workers))
        handler = threaded_handle
    server = await asyncio.start_server(handler, '127.0.0.1', 0, backlog=4096)
    announce(server.sockets[0].getsockname()[1])
    await wait_for_stdin_eof()
    server.close()


async def threaded_handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    n = int(await reader.readline())
    result = await asyncio.to_thread(bench_app.work, n)
    writer.write(f'{result}\n'.encode())
    await writer.drain()
    writer.close()


async def wait_for_stdin_eof() -> None:
    await asyncio.to_thread(sys.stdin.buffer.read)


def prefork(workers: int) -> None:
    """N single-loop processes sharing one listening socket (the gunicorn-style baseline)."""
    listener = socket.create_server(('127.0.0.1', 0), backlog=4096)
    listener.setblocking(False)
    if sys.platform == 'win32':
        children = _spawn_windows_children(listener, workers)
    else:
        children = _fork_children(listener, workers)
    announce(listener.getsockname()[1])
    sys.stdin.buffer.read()
    for child in children:
        child.terminate()
    for child in children:
        child.join()


def _fork_children(listener: socket.socket, workers: int) -> list:
    children = []
    for _ in range(workers):
        pid = os.fork()
        if pid == 0:
            asyncio.run(child_loop(listener))
            os._exit(0)
        children.append(_ForkedChild(pid))
    return children


def _spawn_windows_children(listener: socket.socket, workers: int) -> list:
    context = multiprocessing.get_context('spawn')
    children = []
    for _ in range(workers):
        parent_end, child_end = context.Pipe()
        child = context.Process(target=windows_child, args=(child_end,))
        child.start()
        parent_end.send(listener.share(child.pid))
        children.append(child)
    return children


def windows_child(pipe) -> None:
    listener = socket.fromshare(pipe.recv())
    listener.setblocking(False)
    asyncio.run(child_loop(listener))


async def child_loop(listener: socket.socket) -> None:
    server = await asyncio.start_server(bench_app.handle, sock=listener)
    await server.serve_forever()


class _ForkedChild:
    def __init__(self, pid: int) -> None:
        self.pid = pid

    def terminate(self) -> None:
        os.kill(self.pid, signal.SIGTERM)

    def join(self) -> None:
        os.waitpid(self.pid, 0)


def announce(port: int) -> None:
    print(f'READY {port}', flush=True)


if __name__ == '__main__':
    main()
