import asyncio
import os
import socket
import subprocess
import sys
import textwrap
import threading
from concurrent import interpreters
from pathlib import Path

import apps
import pytest
from test_engine import request

import spa_mc

ROOT = Path(__file__).resolve().parents[1]


def address(server: asyncio.AbstractServer) -> tuple[str, int]:
    return server.sockets[0].getsockname()[:2]


def test_start_server_takes_the_handler_function_like_asyncio_does():
    async def scenario():
        server = await spa_mc.start_server(apps.whoami, '127.0.0.1', 0, workers=3)
        async with server:
            replies = await asyncio.gather(*(request(address(server), b'?\n') for _ in range(30)))
            return server, {reply.split()[0] for reply, _ in replies}

    server, workers = asyncio.run(scenario())
    assert isinstance(server, asyncio.AbstractServer)
    assert workers == {b'0', b'1', b'2'}
    assert not server.is_serving()


def test_serve_forever_runs_until_cancelled_then_closes_everything():
    threads_before = threading.active_count()

    async def scenario():
        server = await spa_mc.start_server(apps.echo, '127.0.0.1', 0, workers=2)
        forever = asyncio.create_task(server.serve_forever())
        await asyncio.sleep(0.05)
        reply, _ = await request(address(server), b'hello\n')
        assert server.is_serving()
        forever.cancel()
        with pytest.raises(asyncio.CancelledError):
            await forever
        return reply, server.is_serving(), server.sockets

    reply, serving, sockets = asyncio.run(scenario())
    assert reply == b'hello\n'
    assert not serving and sockets == ()
    assert threading.active_count() == threads_before
    assert interpreters.list_all() == [interpreters.get_main()]


def test_start_serving_false_holds_connections_until_start_serving():
    async def scenario():
        server = await spa_mc.start_server(apps.echo, '127.0.0.1', 0, workers=1, start_serving=False)
        async with server:
            pending = asyncio.create_task(request(address(server), b'wait\n'))
            await asyncio.sleep(0.2)
            served_early = pending.done()
            await server.start_serving()
            reply, _ = await pending
            return served_early, reply

    assert asyncio.run(scenario()) == (False, b'wait\n')


def test_close_refuses_new_connections_but_finishes_accepted_ones():
    async def scenario():
        server = await spa_mc.start_server(apps.slow, '127.0.0.1', 0, workers=1)
        where = address(server)
        in_flight = asyncio.create_task(request(where, b'x\n'))
        await asyncio.sleep(0.05)
        server.close()
        with pytest.raises(ConnectionRefusedError):
            await request(where, b'x\n')
        await server.wait_closed()
        reply, _ = await in_flight
        return reply

    assert asyncio.run(scenario()) == b'done\n'


def test_limit_is_the_stream_reader_limit_as_in_asyncio():
    async def scenario(limit: int) -> bytes:
        async with await spa_mc.start_server(apps.crash_or_echo, '127.0.0.1', 0, workers=1, limit=limit) as server:
            reply, _ = await request(address(server), b'a line longer than sixteen bytes\n')
            return reply

    assert asyncio.run(scenario(2**16)) == b'a line longer than sixteen bytes\n'
    assert asyncio.run(scenario(16)) == b''


def test_an_existing_listening_socket_can_be_passed_as_sock():
    async def scenario():
        sock = socket.create_server(('127.0.0.1', 0))
        async with await spa_mc.start_server(apps.echo, sock=sock, workers=1) as server:
            assert server.sockets == (sock,)
            reply, _ = await request(sock.getsockname()[:2], b'own socket\n')
            return reply

    assert asyncio.run(scenario()) == b'own socket\n'


def test_host_none_listens_on_all_interfaces():
    async def scenario():
        async with await spa_mc.start_server(apps.echo, None, 0, workers=1) as server:
            port = server.sockets[0].getsockname()[1]
            reply, _ = await request(('127.0.0.1', port), b'any\n')
            return server.sockets[0].getsockname()[0], reply

    host, reply = asyncio.run(scenario())
    assert host in ('::', '0.0.0.0')
    assert reply == b'any\n'


@pytest.mark.parametrize(
    'handler',
    [lambda reader, writer: None, (lambda: lambda reader, writer: None)(), apps.Greeter().handle],
    ids=['lambda', 'closure', 'bound method'],
)
def test_handlers_that_cannot_be_imported_by_name_are_rejected_up_front(handler):
    async def scenario():
        server = await spa_mc.start_server(handler, '127.0.0.1', 0, workers=1)
        # Only reached if the handler was wrongly accepted: close it so the test fails instead of hanging.
        server.close()
        await server.wait_closed()

    with pytest.raises(TypeError, match='module-level function'):
        asyncio.run(scenario())
    assert interpreters.list_all() == [interpreters.get_main()]


def test_a_handler_defined_in_the_script_being_run_works(tmp_path):
    script = tmp_path / 'app.py'
    script.write_text(
        textwrap.dedent(
            """
            import asyncio
            import spa_mc

            print('top level runs in the main interpreter and in each worker', flush=True)


            async def handle(reader, writer):
                writer.write(b'from ' + __name__.encode() + b': ' + await reader.readline())
                await writer.drain()


            async def main():
                async with await spa_mc.start_server(handle, '127.0.0.1', 0, workers=2) as server:
                    host, port = server.sockets[0].getsockname()[:2]
                    reader, writer = await asyncio.open_connection(host, port)
                    writer.write(b'hi\\n')
                    print('REPLY', (await reader.read()).decode().strip(), flush=True)
                    writer.close()


            if __name__ == '__main__':
                print('main block runs once', flush=True)
                asyncio.run(main())
            """
        )
    )
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=60,
        env=os.environ | {'PYTHONPATH': str(ROOT / 'src')},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.count('main block runs once') == 1
    assert 'REPLY from __spa_mc_main__: hi' in result.stdout
