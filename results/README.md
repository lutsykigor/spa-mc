# Results

## Tests

| platform | Python | result | log |
|---|---|---|---|
| Linux (Docker `python:3.14-slim`, arm64, 10 vCPUs) | 3.14.8 | **38 passed** | [tests-linux-py3.14.txt](tests-linux-py3.14.txt) |
| macOS (Apple M5) | 3.14.8 | **36 passed, 2 skipped** (pinning and `reuseport` need Linux/Windows) | [tests-macos-py3.14.txt](tests-macos-py3.14.txt) |
| Windows | 3.14 | via GitHub Actions on every push ([workflow](../.github/workflows/tests.yml)); no local run yet | — |

The suite was run three to five times per platform at each stage. Repetition caught one intermittent failure: a torn
telemetry read on Apple silicon, about once in twenty runs, which led to the checksummed records described below.

What the suite proves, in short:

- workers are distinct interpreters and distinct OS threads in **one PID**, and CPU-bound handlers on 4 workers finish
  **≥ 2x** faster than on 1 (3.2x measured locally);
- 300 concurrent clients each get their own reply; connections spread over all workers;
- the router sends **no** traffic to a worker whose loop is frozen (`least_loaded`), while `round_robin` does and those
  requests wait > 250 ms;
- 10 requests against a limit of 3 **all complete**, never more than 3 running at once; when every worker is full the
  router pauses accepting and still completes everything; `stop()` serves connections that were still queued;
- shedding happens only when asked for: `overload='shed'` rejects exactly the excess (replies arrive intact), and
  `max_queue_wait` rejects only connections that waited too long;
- the limit contracts after a real stall, does **not** contract when the loop is merely busy, and treats a request
  timeout as a drop;
- a crashing handler does not take its worker down; start-up failures raise with the worker's error and leave no threads
  or interpreters behind; `stop()` releases threads, interpreters and descriptors;
- a worker frozen in the middle of a telemetry write does not stop the acceptor;
- checksummed telemetry reads under a truly parallel writer are never torn (plain reads tear 1–2% of the time; see `bench/probes/torn_reads.py`);
- on Linux (and Windows), each worker thread's real OS affinity is exactly its assigned CPU.

## Benchmarks: preliminary, do not quote

The [harness](../bench/run_bench.py) compares five ways to serve the same handler (read a number, burn ~1 ms of
pure-Python CPU, reply) under 64 concurrent closed-loop clients, plus a noisy-neighbour scenario in which worker 0
loses its core for 60 ms every 250 ms.

The two complete runs below come from a **fanless MacBook Air (Apple M5) that throttled during the session**:
its calibration loop drifted from 49 ms to 96 ms, and endpoint-security agents used about one core throughout.
Scenarios were interleaved per round, so they shared the same conditions, but absolute numbers are not representative.
They also predate the cross-platform handoff and queue-by-default (in these runs only the round-robin noisy
scenario shed any requests: 1–73 per round).
Re-measure on an actively cooled machine before drawing conclusions.

Median of 3 rounds, 4 workers, 8 s per scenario:

| scenario | run 1 req/s | run 1 p99 ms | run 2 req/s | run 2 p99 ms |
|---|---:|---:|---:|---:|
| asyncio, single loop | 650 | 123 | 959 | 88 |
| asyncio + 4 threads (GIL build) | 624 | 147 | 942 | 105 |
| pre-fork, 4 processes | 1444 | 99 | 2093 | 89 |
| **spa-mc, 4 workers** | **1460** | **71** | **2157** | **58** |
| asyncio + 4 threads (free-threaded 3.14t) | 1331 | 67 | 2006 | 49 |
| noisy: spa-mc round-robin, no valve | 1273 | 207 | 1989 | 133 |
| **noisy: spa-mc least-loaded + valve** | **1313** | **97** | **2047** | **79** |

What both runs agree on, and no more:

- The GIL build gains nothing from threads for this CPU-bound handler; every multi-core model gives roughly 2.0–2.3x over the single loop.
- spa-mc matches pre-fork processes on throughput (within 1–3%) with a lower p99, in one process.
- The free-threaded build is within about 10% on throughput with a lower p99 in both runs, and has no subinterpreter
  restrictions at all. On this workload spa-mc has no clear advantage over it.
- Under the simulated noisy neighbour, load-aware routing plus the valve cut p99 by 40–53% at equal throughput,
  trading a higher median (the stalled worker is avoided, so the others queue a little more).

Raw logs and JSON: [macos-preliminary/](macos-preliminary/) (`cooldown-attempt.log` shows the machine failing to
return to its cold calibration within 10 minutes of rest).

### Reproducing on a cooled machine

```bash
uv sync --python 3.14
uv python install 3.14t
uv run python bench/run_bench.py --python-ft "$(uv python find 3.14t)" --out results/<machine>.json
```

On Windows (PowerShell), the last line is:

```powershell
uv run python bench/run_bench.py --python-ft (uv python find 3.14t) --out results/windows.json
```
