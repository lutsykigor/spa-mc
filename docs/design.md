# spa-mc design

## Shape

```
                       one OS process (one PID)
 ┌──────────────────────────────────────────────────────────────────────┐
 │ main interpreter: asyncio acceptor + router                          │
 │   accept() ──► pick worker ──► detach fd ──► inbox queue + doorbell ┐ │
 │                     ▲                                               │ │
 │        telemetry slab (bytearray, 128 B per worker, CRC-checked)    │ │
 │                     │ written by workers, read by router            │ │
 ├─────────────────────┼───────────────────────────────────────────────┼─┤
 │ worker 0 (own interpreter, own GIL, own thread, pinned to cpu 0)    ▼ │
 │   asyncio loop: inbox ─► under limit? run : FIFO wait ─► handler task │
 │                 lag watchdog ─► limiter.on_stall, heartbeat           │
 │ worker 1 … worker N-1 (same)                                          │
 └──────────────────────────────────────────────────────────────────────┘
```

## Modules

| module | runs in | responsibility |
|---|---|---|
| `spa_mc.server` | main interpreter | `start_server` and `Server`, the `asyncio.start_server` / `asyncio.Server` look-alikes over `Engine` |
| `spa_mc.handlers` | both | turns a handler function into an importable `module:name`, and imports it in a worker |
| `spa_mc.engine` | main interpreter | creates the subinterpreters and their threads, waits for `READY`, accepts connections, routes them, stops everything |
| `spa_mc.worker` | each subinterpreter | the per-core asyncio loop: fd handoff (or `SO_REUSEPORT` listener), concurrency limit and FIFO wait queue, handler tasks, lag watchdog, telemetry publishing |
| `spa_mc.limiter` | anywhere | `VegasLimiter`, a pure (clock-free) adaptive concurrency limit |
| `spa_mc.telemetry` | both | the shared per-worker record, CRC-checked against torn reads |
| `spa_mc.topology` | both | CPU budget (affinity, SMT siblings collapsed on Linux, cgroup v1/v2 CPU limit) and per-thread pinning (Linux `sched_setaffinity`, Windows `SetThreadAffinityMask`) |
| `spa_mc.autoscale` | main interpreter | pure decision logic: which workers take traffic, when to start more |

## Decisions

**Python 3.14 only.** It is the first release where `concurrent.interpreters` is public, a `memoryview` is shareable,
and `ctypes` loads in an isolated subinterpreter. 3.12 cannot run the design (see the verification page).

**The public API copies asyncio's.** `spa_mc.start_server` takes `asyncio.start_server`'s arguments and returns an
`asyncio.AbstractServer` with the same lifecycle: `close()` stops listening at once (the listener is unregistered from
the loop before it is closed, because epoll otherwise keeps watching a descriptor number the next socket may reuse),
`wait_closed()` waits until accepted connections are finished, and cancelling `serve_forever()` closes the server.
asyncio's other extension points do not fit: a custom event loop (`loop_factory`) is still one thread, one core.

**Code reaches workers by import, not by source templates.** The worker bootstrap sets `sys.path` from the parent
and imports `spa_mc.worker`. Handlers can be passed as functions; the main interpreter turns them into
`"module:qualname"`, rejecting lambdas, closures and methods with a clear error, since another interpreter cannot import
those. A function in the script being run (`__main__`) is loaded in each worker from the script's file under the name
`__spa_mc_main__`, the way `multiprocessing` uses `__mp_main__`, so the script's `if __name__ == '__main__':` block runs once.

**Connections move as file descriptors.** All interpreters share the process FD (or Windows handle) table,
so the router detaches the accepted socket and puts the integer on the worker's bounded
`concurrent.interpreters` queue (65 536 slots), then writes one byte to a `socketpair()` doorbell.
The worker awaits the doorbell with `loop.sock_recv` and drains the queue on every wake-up.
This one path works on the selector loop (Linux, macOS) and the proactor loop (Windows, which has no `add_reader`
and no `AF_UNIX` datagram sockets). A full doorbell buffer is ignored, because a wake-up is already pending.
The inbox is sized far above any worker's capacity, so it is never the limiting queue; if it ever fills,
the connection is shed as a last resort rather than blocking the acceptor.

**Routing uses two health signals.** The *loop-lag* value reports past stalls; the *heartbeat age* reports a stall
happening right now (a frozen loop cannot publish).
`least_loaded` prefers workers with a fresh heartbeat, ranks them by
`(outstanding + 1) / limit * (1 + lag / stall_threshold)`, and falls back to stale-but-not-full workers.
`outstanding` is counted by the router (dispatched minus completed and shed), so it includes connections still in the
inbox and in the worker's wait queue. A worker is *full* at `limit + worker_queue` outstanding (`limit` in shed mode).

**A stall means time off the CPU.** The watchdog ticks every 5 ms and compares wall time with the thread's CPU time
(`time.thread_time()`). Lag spent running the worker's own code is load, which the Vegas RTT signal already handles;
only lag the thread spent descheduled or blocked (`off_cpu >= stall_threshold`) counts as a stall and triggers
the multiplicative decrease. Without this distinction a merely busy worker looks preempted and its limit collapses.

**The limit follows Netflix `VegasLimit`**, with multiplicative decrease reserved for hard signals
(drop, request timeout, loop stall). It caps how many requests a core runs *at once*, the point past which more
concurrency only adds contention; it does not cap how many it accepts. The RTT sample is the handler's duration,
excluding time spent waiting, so queueing does not distort it.

**Overload queues by default.** A connection that arrives when its worker is at the limit waits in a FIFO on that
worker and starts when a handler finishes. When every worker is full the acceptor stops calling `accept()`
(it polls capacity every millisecond), so further clients wait in the kernel's accept queue and see latency, not an error;
`router_stats().paused_ms` records how long that lasted. Rejection is opt-in: `overload='shed'` replies `shed_response`
to anything over the limit, and `max_queue_wait` sheds only connections that have waited too long.
On `stop()`, queued connections are still served within `shutdown_grace`.

**Shedding half-closes.** When shedding is enabled, the reply is sent, the write side shut down, and the socket closed
one second later, so unread request bytes cannot turn the close into an RST that destroys the reply.

**Telemetry records carry a CRC32.** Python cannot issue memory fences, and a seqlock was observed to let one torn record
through on Apple silicon. Each record is written with one copy and read with one copy, accepted only if the checksum matches:
0 torn reads in about 25 million under a parallel writer, against 1–2% for plain multi-field reads
([probe](../bench/probes/torn_reads.py)). The values steer routing only; nothing correctness-critical depends on them.

**The worker count follows the CPU budget.** With `workers='auto'` the engine starts one worker per CPU in the budget:
the CPUs the process may run on (affinity), one per physical core, cut to the cgroup CPU limit (`cpu.max` on v2,
CFS quota on v1, the tightest along the cgroup path). Python's `os.cpu_count()` and `os.process_cpu_count()` ignore
that limit, so a `--cpus=2` container on a 10-core host otherwise looks like 10 CPUs. The telemetry slab is sized for
every CPU the machine has, so the engine can grow. Every `adapt_interval` the main interpreter re-reads the budget:
workers beyond it are parked (the router stops sending them work; they stay alive and idle), parked workers come back
when it has room again, and new workers are started, and pinned to a free CPU, when it grows past the current count.

**Steal parking is measured and opt-in.** Each worker accumulates *steal*: off-CPU time per watchdog tick beyond 2 ms
of timer slack (idle wake-ups run up to ~1 ms late on Linux and macOS, which first read as a false 18% steal).
With `park_on_steal=True`, a worker whose steal ratio stays above 0.3 for two intervals is parked, one per interval,
never the last one; it returns after three intervals below 0.1. Off by default because, under fair-share scheduling,
it lowered throughput: −18% with a busy-loop process per CPU, −2% with four, for a 13–20% better p99.

## Robustness rules learned from testing

- **The acceptor must never die.** Per-connection errors are logged and the loop continues;
  a dead acceptor leaves every new client stuck in the kernel accept queue (this happened once under load).
- **Telemetry reads on the routing path never raise.** `try_read` gives up after a small retry budget and the router
  skips that worker; a worker descheduled halfway through writing its record must not stall routing for everyone.
- **Measure the measurement.** The first steal estimate read 18% on idle workers; only probing idle, busy and contended
  workers side by side ([probe](../bench/probes/steal_signal.py)) showed the timer-slack bias.
- **Weak memory ordering is real.** Fence-free schemes that pass on x86 and Linux can still tear on Apple silicon;
  only a self-validating record (checksum) held up.

## Not built

- **The shared payload arena** (main interpreter frames L7 requests and passes `(offset, length)`).
  It would put all parsing on the main interpreter's single GIL, the bottleneck the design exists to remove.
  Zero-copy `memoryview` sharing itself is verified and available to users.
- **Thread priority (`os.setpriority`)**: needs `CAP_SYS_NICE`.
- **Gradient2**: replaced by Vegas plus periodic re-probe of the no-load RTT.

## Known limits

- The acceptor is one asyncio loop under the main GIL, so connection *rate* is capped by it (tens of thousands per second).
  Use `routing='reuseport'` on Linux to remove it, at the cost of load awareness.
- The limit counts connections; long-lived keep-alive connections need per-request limiting inside the handler
  (`spa_mc.current().limiter`).
- Under sustained overload, queue mode converts errors into unbounded waiting; bound it with `max_queue_wait`.
- C extensions must support isolated subinterpreters (multi-phase init); many popular ones do not yet.
- Requests already on a worker when it stalls still wait out the stall; routing and the limit only protect *new* work.
