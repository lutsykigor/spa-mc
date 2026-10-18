# spa-mc — Single Process Adaptive Multi-Core asyncio

**One asyncio event loop per CPU core, each in its own subinterpreter with its own GIL, inside one Python process —
plus per-core adaptive concurrency control and load-aware connection routing. Every request is served; none is
rejected unless you ask for that.**

> Status: **proof of concept.** Python **3.14+** only. 76 tests, passing on Linux and macOS; Windows runs in CI.
> Requires the standard Python (CPython). Alternative implementations such as PyPy and GraalPy don't have
> per-interpreter GILs, so spa-mc can't run on them.
> Benchmark numbers so far come from a throttling laptop and are preliminary; see [results](results/README.md).

It is used like `asyncio.start_server`; change one word and the server runs on every core:

```python
import asyncio
import spa_mc


async def handle(reader, writer):          # an ordinary asyncio streams handler
    data = await reader.readline()
    writer.write(data)
    await writer.drain()


async def main():
    server = await spa_mc.start_server(handle, '0.0.0.0', 8080)   # was: asyncio.start_server(...)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
```

`start_server` takes the same arguments as `asyncio.start_server` (`host`, `port`, `limit`, `sock`, `backlog`,
`start_serving`) plus any spa-mc setting below, and returns a server with the same methods (`serve_forever`,
`start_serving`, `close`, `wait_closed`, `sockets`, `async with`); it is an `asyncio.AbstractServer`.
`server.engine` gives the statistics (`stats()`, `router_stats()`).

**One difference from asyncio, and it is fundamental:** each core runs the handler in its own interpreter.
So the handler must be a module-level `async def` (not a lambda, closure or method; spa-mc says so up front),
and it cannot share global variables or objects with the other cores or with the code that started the server.
Share state through something outside the process (a database, a cache) or through spa-mc's zero-copy
`memoryview` sharing. A handler defined in the script you run works; as with `multiprocessing`, keep start-up code
under `if __name__ == '__main__':`, because each worker imports the script.

For lower-level control, `spa_mc.Engine` is the class `start_server` is built on.

## What it does

```
 one process ─ main interpreter: accept() → pick healthiest worker → hand over the socket fd
             ├ worker 0: own interpreter + own GIL + own thread (pinned to a core on Linux/Windows) + asyncio loop
             │           + adaptive concurrency limit + FIFO wait queue
             ├ worker 1: …
             └ shared 128-byte-per-worker telemetry slab (memoryview, CRC-checked records) read by the router
```

- **Real parallelism in one PID.** PEP 684 gives each subinterpreter its own GIL, so CPU-bound handlers on different
  workers run at the same time. Four workers ran CPU-bound requests **3.2x faster** than one (test `test_cpu_bound_handlers_run_in_parallel_across_workers`).
- **Zero-serialisation hand-off.** Interpreters share the process FD table: the router detaches an accepted socket and
  puts only the integer on the worker's cross-interpreter queue, rings a one-byte doorbell, and the worker adopts it with
  `loop.connect_accepted_socket`. Same path on Linux, macOS and Windows.
- **Adaptive concurrency per core, no rejected work.** Each core runs only as many requests at once as gives it peak
  throughput; a Netflix-style TCP-Vegas limit keeps finding that number. Requests beyond it wait in a FIFO queue on the
  worker and start the moment a slot frees. When every worker's queue is full, the router stops accepting, so new
  clients wait in the kernel's accept queue: slower, never refused.
  The limit backs off on timeouts and on *real* stalls: lag spent **off the CPU** (`time.thread_time()` vs wall clock),
  so a worker that is merely busy is not mistaken for a preempted one.
- **Routing that sees a frozen core now, not afterwards.** Workers publish a heartbeat; the router skips a worker whose
  heartbeat is stale, and otherwise ranks by outstanding work scaled by recent loop lag.
  Alternatives: `routing='round_robin'`, or `routing='reuseport'` (Linux; kernel `SO_REUSEPORT`, no router).
- **Sized to the machine, and kept sized.** `workers='auto'` (the default) starts one worker per CPU the process may
  really use: allowed CPUs (affinity mask), one per physical core, capped by the **container CPU limit**, which Python's
  own CPU counts ignore (in a `--cpus=2` container on a 10-core host they report 10; spa-mc uses 2).
  A controller re-checks once a second and follows changes live: it parks workers when the budget shrinks and
  unparks or starts new ones when it grows. `workers=N` pins an exact number instead.
- **The asyncio API.** `spa_mc.start_server` mirrors `asyncio.start_server`; handlers are ordinary
  `async def handler(reader, writer)`; `spa_mc.current()` gives a handler its worker id, core and limiter;
  `on_start=` runs per-worker setup (a function, like the handler).

## Adapting to the machine

| setting | default | effect |
|---|---|---|
| `workers` | `'auto'` | `'auto'`: one worker per usable CPU, re-checked every `adapt_interval` (1 s) and followed live. An int fixes the count |
| `pin` | `True` | pin each worker thread to its own CPU (Linux, Windows) |
| `park_on_steal` | `False` | opt-in: stop routing to a worker whose CPU is being taken by other processes, and bring it back when the CPU is free again |

Why parking is off by default: when other processes compete for every core, the OS shares CPU time fairly among
runnable threads, so parking workers mostly hands their share to the competitors. Measured with 10 busy-loop processes on
a 10-CPU machine, parking cut throughput by **18%** and raised median latency by 57%, while improving p99 by 20%
(with 4 competitors: −2% throughput, p99 −13%). Use it when tail latency matters more than throughput.
How a stolen CPU is detected: each worker compares wall-clock time with its own CPU time; time spent off the CPU beyond
normal timer slack is *steal*. An idle or merely busy worker reads 0; a worker sharing its CPU with two busy processes
reads about 0.5 ([probe](bench/probes/steal_signal.py)).

## Overload behaviour

| setting | default | effect |
|---|---|---|
| `overload` | `'queue'` | `'queue'`: excess requests wait (FIFO per worker, then in the kernel accept queue). `'shed'`: excess requests get `shed_response` and are closed at once |
| `worker_queue` | `1024` | how many connections a worker may hold waiting beyond its concurrency limit before the router pauses accepting |
| `max_queue_wait` | `None` | optional: a connection that has waited this long gets `shed_response` (for services where a fast "busy" beats a slow answer) |
| `request_timeout` | `None` | optional: cancel a handler that runs this long; counted by the limiter as a drop |

If requests arrive faster than all cores can finish them *for a sustained period*, waiting time grows without bound;
no design can finish more work than the hardware does. Bursts and short stalls are absorbed with no errors.

## Results

**Tests** (full logs in [results/](results/README.md)): 75/75 on Linux plus the container-limit test in a `--cpus=2`
container, 73/73 on macOS (3 platform-specific skips).
Among other things, they show 4 workers running CPU-bound requests **3.2x** faster than 1 in a single PID, 10 requests
against a limit of 3 all completing without more than 3 ever running at once, a frozen worker receiving **no** new
traffic, a busy-but-healthy loop **not** being throttled, and the worker count following a CPU budget that shrinks
and grows while the engine runs.

**Benchmarks** are preliminary: a fanless laptop that throttled (calibration drifted 49 → 96 ms) produced two runs that
agree on the following:

| 4 workers, CPU-bound handler, 64 clients | throughput vs single loop | p99 vs pre-fork |
|---|---:|---:|
| asyncio + threads (GIL) | ~1.0x | — |
| pre-fork processes | 2.2x | — |
| **spa-mc** | **2.25x** | **28–35% lower** |
| asyncio + threads (free-threaded 3.14t) | 2.05–2.1x | lower still |

With a simulated noisy neighbour (worker 0 loses its core 60 ms out of every 250 ms), spa-mc's load-aware routing plus limit
cut p99 by **40–53%** against blind round-robin at equal throughput.
Free-threaded Python is the honest yardstick here: on this workload spa-mc does not beat it.
These runs predate queue-by-default; they shed no requests except 1–73 in the round-robin noisy scenario.
Re-measurement on an actively cooled machine is pending.

## Where this sits

Multi-core Python in one process does not start here. **Free-threaded Python** (PEP 703, supported since 3.14) runs
ordinary threads in parallel with shared objects, and `InterpreterPoolExecutor` (3.14) already pools subinterpreters.
spa-mc is a composition: long-lived per-core asyncio loops, per-core concurrency control and load-aware routing,
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

- Python 3.14+ (the standard CPython). Pinning on Linux and Windows (first 64 CPUs); macOS has no thread-affinity API, so workers run unpinned there.
- The acceptor is one asyncio loop on the main interpreter; it caps the *connection rate*. Use `reuseport` on Linux to remove it.
- The limit counts connections; keep-alive protocols should limit per request via `spa_mc.current().limiter`.
- Requests already on a worker when it stalls still wait out the stall; routing and the limit protect new work.
- The CPU budget is re-read once a second, so a changed container limit takes up to `adapt_interval` to apply.
  With `routing='reuseport'` the kernel picks the worker, so the count is fixed at start-up.
- Under sustained overload, `queue` mode trades errors for waiting; set `max_queue_wait` or `overload='shed'` if callers need a bound.
- Telemetry is advisory routing data: CRC-checked against torn reads, never used for correctness.
- Proof of concept: no TLS helpers, no HTTP layer, no production hardening beyond what the tests cover.

Design notes: [docs/design.md](docs/design.md).

## Running it

```bash
uv sync --python 3.14
uv run pytest -q             # 76 tests; skipped where the platform lacks the feature (pinning, reuseport, cgroup quota)
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
src/spa_mc/server.py      start_server and Server: the asyncio-style front door
src/spa_mc/engine.py      main interpreter: workers lifecycle, acceptor, router
src/spa_mc/handlers.py    turns a handler function into a name each worker can import (including from __main__)
src/spa_mc/worker.py      inside each subinterpreter: loop, concurrency limit, wait queue, watchdog, telemetry
src/spa_mc/limiter.py     VegasLimiter (pure, reusable in any asyncio app)
src/spa_mc/telemetry.py   shared per-worker records, CRC-checked
src/spa_mc/topology.py    CPU budget (affinity, physical cores, container CPU limit) and per-thread pinning
src/spa_mc/autoscale.py   decides how many workers take traffic (budget changes, optional steal parking)
tests/                   unit + integration tests (tests/apps.py holds the handlers workers load)
bench/                   benchmark harness, servers for each concurrency model, probes for the verification
docs/                    design notes and the verification of the source architecture
results/                 test logs and (preliminary) benchmark outputs
.github/workflows/       CI: tests + benchmark smoke run on Linux, macOS and Windows
```

## License

[MIT](LICENSE)
