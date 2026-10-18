# spa-mc — Single Process Adaptive Multi-Core asyncio

**One asyncio event loop per CPU core, each in its own subinterpreter with its own GIL, inside one Python process —
plus a per-core adaptive concurrency valve and load-aware connection routing.**

> Status: **proof of concept.** CPython **3.14+** only. 33 tests, passing on Linux and macOS; Windows runs in CI.
> Benchmark numbers so far come from a throttling laptop and are preliminary; see [results](results/README.md).

```python
import asyncio
import spa_mc

# myapp.py:  async def handle(reader, writer): ...   (a normal asyncio streams handler)

async def main():
    async with spa_mc.Engine('myapp:handle', workers=4, shed_response=b'HTTP/1.1 503 Busy\r\n\r\n') as engine:
        host, port = await engine.serve_tcp('0.0.0.0', 8080)
        await asyncio.Event().wait()

asyncio.run(main())
```

## What it does

```
 one process ─ main interpreter: accept() → pick healthiest worker → hand over the socket fd
             ├ worker 0: own interpreter + own GIL + own thread (pinned to a core on Linux/Windows) + asyncio loop + valve
             ├ worker 1: …
             └ shared 128-byte-per-worker telemetry slab (memoryview, seqlock) read by the router
```

- **Real parallelism in one PID.** PEP 684 gives each subinterpreter its own GIL, so CPU-bound handlers on different
  workers run at the same time. Four workers ran CPU-bound requests **3.2x faster** than one (test `test_cpu_bound_handlers_run_in_parallel_across_workers`).
- **Zero-serialisation hand-off.** Interpreters share the process FD table: the router detaches an accepted socket and
  puts only the integer on the worker's cross-interpreter queue, rings a one-byte doorbell, and the worker adopts it with
  `loop.connect_accepted_socket`. Same path on Linux, macOS and Windows.
- **Adaptive valve per core.** A Netflix-style TCP-Vegas concurrency limit, with multiplicative backoff on drops,
  timeouts and *real* stalls. A stall is lag spent **off the CPU** (`time.thread_time()` vs wall clock), so a worker that is
  merely busy is not mistaken for a preempted one.
- **Routing that sees a frozen core now, not afterwards.** Workers publish a heartbeat; the router skips a worker whose
  heartbeat is stale, and otherwise ranks by outstanding work scaled by recent loop lag.
  Alternatives: `routing='round_robin'`, or `routing='reuseport'` (Linux; kernel `SO_REUSEPORT`, no router).
- **Plain asyncio API.** Handlers are ordinary `async def handler(reader, writer)`; `spa_mc.current()` gives a handler its
  worker id, core and limiter; `on_start='module:function'` runs per-worker setup.

## Results

**Tests** (full logs in [results/](results/README.md)): 33/33 on Linux, 31/31 on macOS (2 platform-specific skips).
Among other things, they show 4 workers running CPU-bound requests **3.2x** faster than 1 in a single PID, a frozen
worker receiving **no** new traffic, and a busy-but-healthy loop **not** being throttled.

**Benchmarks** are preliminary: a fanless laptop that throttled (calibration drifted 49 → 96 ms) produced two runs that
agree on the following:

| 4 workers, CPU-bound handler, 64 clients | throughput vs single loop | p99 vs pre-fork |
|---|---:|---:|
| asyncio + threads (GIL) | ~1.0x | — |
| pre-fork processes | 2.2x | — |
| **spa-mc** | **2.25x** | **28–35% lower** |
| asyncio + threads (free-threaded 3.14t) | 2.05–2.1x | lower still |

With a simulated noisy neighbour (worker 0 loses its core 60 ms out of every 250 ms), spa-mc's load-aware routing plus valve
cut p99 by **40–53%** against blind round-robin at equal throughput.
Free-threaded CPython is the honest yardstick here: on this workload spa-mc does not beat it.
Re-measurement on an actively cooled machine is pending.

## Where this sits

Multi-core Python in one process does not start here. **Free-threaded CPython** (PEP 703, supported since 3.14) runs
ordinary threads in parallel with shared objects, and `InterpreterPoolExecutor` (3.14) already pools subinterpreters.
spa-mc is a composition: long-lived per-core asyncio loops, per-core admission control and load-aware routing,
packaged and tested. It suits I/O servers with CPU-heavy handlers that want isolation between cores without separate
processes. It does not suit code that needs C extensions without subinterpreter support (NumPy and PyTorch, at the time of writing).

## The source architecture, verified

spa-mc began as an implementation of a written architecture. Each load-bearing claim was tested before building on it;
**21 claims were checked: several hold, several do not.** Highlights:
the per-interpreter-GIL parallelism and zero-copy `memoryview` sharing are real;
the published proof-of-concept is a `SyntaxError`, cannot run on 3.12 as claimed, uses a "lock-free" slab that can tear,
deviates from the Vegas algorithm it cites, and treats a busy loop as a preempted one.
Full table with evidence: [docs/research-verification.md](docs/research-verification.md).

## Limitations

- CPython 3.14+. Pinning on Linux and Windows (first 64 CPUs); macOS has no thread-affinity API, so workers run unpinned there.
- The acceptor is one asyncio loop on the main interpreter; it caps the *connection rate*. Use `reuseport` on Linux to remove it.
- Admission is per connection; keep-alive protocols should admit per request via `spa_mc.current().limiter`.
- Requests already admitted to a worker when it stalls still wait out the stall; the valve and router protect new work.
- Telemetry is advisory: seqlock-guarded but without memory fences.
- Proof of concept: no TLS helpers, no HTTP layer, no production hardening beyond what the tests cover.

Design notes: [docs/design.md](docs/design.md).

## Running it

```bash
uv sync --python 3.14
uv run pytest -q             # 33 tests; on macOS 2 are skipped (pinning, reuseport)
```

```bash
docker run --rm -v "$PWD":/src:ro python:3.14-slim sh -c 'cp -r /src /w && cd /w && pip install -q pytest -e . && python -m pytest -q'
```

```bash
uv run python bench/run_bench.py --python-ft "$(uv python find 3.14t)"
```

On Windows (PowerShell), install uv (`winget install astral-sh.uv`), then:

```powershell
uv sync --python 3.14
uv run pytest -q
uv python install 3.14t
uv run python bench/run_bench.py --python-ft (uv python find 3.14t) --out results/windows.json
```

`reuseport` routing and the pre-fork baseline's `fork` are POSIX-only; on Windows the pre-fork baseline shares its
listening socket with spawned processes through `socket.share()` instead.

The benchmark rests and re-checks whenever a calibration loop (one core and four at once) is more than 10% slower than
the cold reference, and records the factor next to every result. Pass `--baseline-ms` with a value measured on a cold machine
(`uv run python bench/run_bench.py --calibrate`).

## Layout

```
src/spa_mc/engine.py      main interpreter: workers lifecycle, acceptor, router
src/spa_mc/worker.py      inside each subinterpreter: loop, admission, watchdog, telemetry
src/spa_mc/limiter.py     VegasLimiter (pure, reusable in any asyncio app)
src/spa_mc/telemetry.py   shared per-worker records with a seqlock
src/spa_mc/topology.py    CPU discovery and per-thread pinning
tests/                   unit + integration tests (tests/apps.py holds the handlers workers load)
bench/                   benchmark harness, servers for each concurrency model, probes for the verification
docs/                    design notes and the verification of the source architecture
results/                 test logs and (preliminary) benchmark outputs
.github/workflows/       CI: tests + benchmark smoke run on Linux, macOS and Windows
```

## License

[MIT](LICENSE)
