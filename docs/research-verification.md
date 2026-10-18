# Verification of the source architecture

spa-mc started from a written architecture,
*"Architecture of a Single-Process, Multi-Core, Non-Blocking Engine in Standard CPython"*.
Before building on it, each load-bearing claim was checked against a real interpreter.
This page records what held, what did not, and what spa-mc does differently as a result.

Environment: Apple M5 (4 performance + 6 efficiency cores, 128-byte cache lines), macOS,
Python 3.12.15 and 3.14.8; Linux checks ran in a `python:3.14-slim` container (arm64, 10 vCPUs).
Probe scripts are reproduced in [`bench/probes/`](../bench/probes/).

## Verdicts

| # | Claim in the source document | Verdict | Evidence |
|---|---|---|---|
| 1 | A per-interpreter GIL (PEP 684) gives real multi-core parallelism inside one process | **Confirmed** | 4 threads, each running its own subinterpreter: 3.70x (3.12) and 3.31x (3.14) faster than one; 4 plain threads: 1.15x / 0.95x |
| 2 | The design runs on "standard CPython 3.12+" | **Refuted** | On 3.12 `ctypes` cannot be imported in an isolated subinterpreter (`module _ctypes does not support loading in subinterpreters`), and the PoC depends on it; the workers die and the main thread then blocks forever. The fallback import `_xxsubinterpreters` no longer exists on 3.14 (renamed `_interpreters` in 3.13; 3.13 itself not tested). spa-mc requires 3.14 |
| 3 | The published proof-of-concept runs | **Refuted as published** | It is a `SyntaxError`: `select_optimal_worker` has an escaped docstring (`\"\"\"`) outside any string. With that one line fixed it runs on 3.14 |
| 4 | `memoryview` buffers are shared between interpreters without copying | **Confirmed (3.14)** | A 64 MiB `memoryview` sent through `concurrent.interpreters` Queue in 0.07 ms; writes in the subinterpreter are visible to the parent. The ctypes raw-address trick is unnecessary on 3.14 |
| 5 | `os.sched_setaffinity(0, {cpu})` pins the *calling thread* | **Confirmed on Linux** | Three subinterpreter threads each reported affinity `{n}` while the main thread kept all 10 CPUs. macOS has no such API: the PoC silently runs unpinned there |
| 6 | `os.setpriority` can lower the niceness of worker threads to resist preemption | **Needs privilege** | `EACCES` even as root inside a default container (needs `CAP_SYS_NICE`). Not used by spa-mc |
| 7 | Windows pinning via `SetThreadAffinityMask` "or psutil" | **Partly wrong** | psutil exposes process affinity, not per-thread. spa-mc calls `SetThreadAffinityMask` through `ctypes` (loadable in 3.14 subinterpreters); Windows is covered by CI, not yet by a local run |
| 8 | `SO_REUSEPORT` spreads connections over per-loop listeners | **Confirmed on Linux** | 400 connections over 4 listeners: 88 / 109 / 82 / 121. BSD/macOS semantics do not load-balance (not measured here); spa-mc offers `reuseport` routing on Linux only |
| 9 | FD handoff (`sock.detach()` → `socket(fileno=fd)` → `loop.connect_accepted_socket`) moves connections between interpreters without serialisation | **Confirmed** | Every spa-mc integration test uses it |
| 10 | Packing stats into a shared buffer with `struct.pack_into` is a "lock-free" telemetry slab | **Unsafe as written** | Writer and reader run truly in parallel (separate GILs), so multi-field records can be torn. A plain read tears in 1–2% of reads under a parallel writer. A seqlock is not enough either: without memory fences it let one torn record through on Apple silicon. spa-mc stores a CRC32 with each record and accepts a copy only if it matches: 0 torn reads in about 25 million ([probe](../bench/probes/torn_reads.py)) |
| 11 | 64-byte slots are cache-line aligned, avoiding false sharing | **Not on Apple silicon** | `hw.cachelinesize` = 128 on M-series; a ctypes array has no alignment guarantee either. spa-mc uses 128-byte slots |
| 12 | The valve follows Netflix concurrency-limits' Vegas algorithm | **Deviates** | The PoC shrinks the limit between `alpha` and `beta` (Netflix holds there), skips the app-limited check before shrinking, and decreases multiplicatively above `beta` (Netflix subtracts `log10(limit)`). Observed effect: with 25 *sequential* light requests the PoC's limit fell from 50 to 46. spa-mc implements the Netflix rules, with a test that 2 000 jittery light requests leave the limit unchanged |
| 13 | The valve uses Gradient2 dual-EMA drift protection | **Not implemented in the PoC** | The short and long EMAs are computed but never read. spa-mc uses Netflix's periodic `rtt_noload` re-probe instead, with a test for a permanent workload shift |
| 14 | Loop-lag sensing lets the router divert traffic away from a preempted core | **Incomplete** | Lag is only measured *after* the stall ends, so during the stall the router still sees a healthy worker. spa-mc also publishes a heartbeat; the router treats a stale heartbeat as "frozen right now". In the test, 0 of 20 requests reached the stalled worker with `least_loaded`; with `round_robin`, the ones that did waited > 250 ms |
| 15 | Router score `in_flight / available + 0.5 * lag_ms` picks the healthiest worker | **Flawed** | Normal sub-millisecond lag jitter outweighs one extra in-flight request (1/50 = 0.02 vs 0.25). spa-mc's first router had the same flaw and spread 9 requests over 4 workers as 3/1/0/5; the fix makes lag a multiplier on queue depth (now 2/2/2/3) |
| 16 | Shedding replies `429` then closes | **Reply can be lost** | Closing a socket with unread request bytes sends an RST, which can discard the reply (reproduced as `ConnectionResetError`). spa-mc sends, half-closes, and closes later |
| 17 | Handoff via a stream `socketpair`, reading 8 bytes per readiness event | **Fragile** | Stream sockets may split messages. spa-mc passes descriptors through a bounded `concurrent.interpreters` queue and uses the socketpair only as a one-byte doorbell; the worker drains the whole queue per wake-up |
| 18 | Start-up waits `time.sleep(0.15)` for workers | **Racy** | spa-mc waits for every worker to publish `READY`, and raises with the worker's traceback if one fails |
| 19 | Pinned workers keep L1/L2 hot, avoid ob_refcnt cache-line bouncing, and see "zero TLB flush" | **Not measured; overstated** | Isolation of allocator state is real (PEP 684), but a preemption by another process still switches address space, and multi-threaded processes still get TLB shootdowns (e.g. `munmap`). No hardware-counter measurements were made |
| 20 | Third-party C extensions may not load in subinterpreters | **Correct caveat** | Not re-tested; NumPy and PyTorch issues are cited in the source. This is the main practical limit of the approach |
| 21 | A loop-lag spike means the OS preempted the worker, so the valve should back off multiplicatively | **False positives under load** | A loop busy running its own CPU-bound handlers lags too. In the first benchmark round this collapsed spa-mc's limits and shed ~28 000 requests in 8 s. spa-mc now compares the thread's CPU time with wall time: only lag spent *off* the CPU (preempted or blocked) counts as a stall, while self-inflicted lag is left to the Vegas RTT signal. A test runs 60 ms CPU-bound requests and asserts no stall is recorded; it fails with the document's rule |

## Context the document leaves out

Multi-core Python in one process is not new in 3.14, and spa-mc is not the only route to it:

- **Free-threaded Python** (PEP 703, officially supported since 3.14 via PEP 779) runs ordinary threads in parallel
  with shared objects and no per-interpreter isolation. The benchmarks include it as a baseline.
- **`concurrent.futures.InterpreterPoolExecutor`** (3.14) already gives a pool of subinterpreters for CPU work.
- **Pre-fork worker processes** (gunicorn-style) remain a strong, simple baseline, and are in the benchmarks too.

spa-mc's contribution is the *combination*: one long-lived asyncio loop per core, per-core concurrency control that queues rather than rejects,
and load-aware connection routing inside one PID, as a small library with tests.
Whether that is worth it over the alternatives is a workload question; the [results](../results/README.md) give numbers.
