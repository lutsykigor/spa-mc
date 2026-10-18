# spa-mc design

## Shape

```
                       one OS process (one PID)
 ┌──────────────────────────────────────────────────────────────────────┐
 │ main interpreter: asyncio acceptor + router                          │
 │   accept() ──► pick worker ──► detach fd ──► inbox queue + doorbell ┐ │
 │                     ▲                                               │ │
 │        telemetry slab (bytearray, 128 B per worker, seqlock)        │ │
 │                     │ written by workers, read by router            │ │
 ├─────────────────────┼───────────────────────────────────────────────┼─┤
 │ worker 0 (own interpreter, own GIL, own thread, pinned to cpu 0)    ▼ │
 │   asyncio loop: inbox reader ─► valve.try_acquire ─► handler task     │
 │                 lag watchdog ─► valve.on_stall, heartbeat             │
 │ worker 1 … worker N-1 (same)                                          │
 └──────────────────────────────────────────────────────────────────────┘
```

## Modules

| module | runs in | responsibility |
|---|---|---|
| `spa_mc.engine` | main interpreter | creates the subinterpreters and their threads, waits for `READY`, accepts connections, routes them, stops everything |
| `spa_mc.worker` | each subinterpreter | the per-core asyncio loop: fd handoff (or `SO_REUSEPORT` listener), admission, handler tasks, lag watchdog, telemetry publishing |
| `spa_mc.limiter` | anywhere | `VegasLimiter`, a pure (clock-free) adaptive concurrency limit |
| `spa_mc.telemetry` | both | the shared per-worker record and its seqlock |
| `spa_mc.topology` | worker | CPU discovery (SMT siblings collapsed on Linux) and per-thread pinning (Linux `sched_setaffinity`, Windows `SetThreadAffinityMask`) |

## Decisions

**Python 3.14 only.** It is the first release where `concurrent.interpreters` is public, a `memoryview` is shareable,
and `ctypes` loads in an isolated subinterpreter. 3.12 cannot run the design (see the verification page).

**Code reaches workers by import, not by source templates.** The worker bootstrap sets `sys.path` from the parent
and imports `spa_mc.worker`. Handlers are `"module:function"` strings resolved inside the worker,
so they are ordinary, testable Python modules.

**Connections move as file descriptors.** All interpreters share the process FD (or Windows handle) table,
so the router detaches the accepted socket and puts the integer on the worker's bounded
`concurrent.interpreters` queue (1024 slots), then writes one byte to a `socketpair()` doorbell.
The worker awaits the doorbell with `loop.sock_recv` and drains the queue on every wake-up.
This one path works on the selector loop (Linux, macOS) and the proactor loop (Windows, which has no `add_reader`
and no `AF_UNIX` datagram sockets). The bounded queue is the backpressure: when a stalled worker's inbox is full,
`put_nowait` fails and the router sheds instead of blocking. A full doorbell buffer is ignored, because a wake-up is already pending.

**Routing uses two health signals.** The *loop-lag* value reports past stalls; the *heartbeat age* reports a stall
happening right now (a frozen loop cannot publish).
`least_loaded` prefers workers with a fresh heartbeat, ranks them by
`(outstanding + 1) / limit * (1 + lag / stall_threshold)`, and falls back to stale-but-under-limit workers before shedding.
`outstanding` is counted by the router (dispatched minus completed and shed), so it includes fds still in the mailbox.

**A stall means time off the CPU.** The watchdog ticks every 5 ms and compares wall time with the thread's CPU time
(`time.thread_time()`). Lag spent running the worker's own code is load, which the Vegas RTT signal already handles;
only lag the thread spent descheduled or blocked (`off_cpu >= stall_threshold`) counts as a stall and triggers
the multiplicative decrease. Without this distinction a merely busy worker looks preempted and sheds most traffic.

**The valve follows Netflix `VegasLimit`**, with multiplicative decrease reserved for hard signals
(drop, request timeout, loop stall). Admission is per connection; the RTT sample is the handler's duration.

**Shedding half-closes.** A shed reply is sent, the write side shut down, and the socket closed one second later,
so unread request bytes cannot turn the close into an RST that destroys the reply.

**Telemetry is advisory.** Seqlock reads are consistent in practice, but Python offers no memory fences;
nothing correctness-critical depends on these values, only routing preference.

## Robustness rules learned from testing

- **The acceptor must never die.** Per-connection errors are logged and the loop continues;
  a dead acceptor leaves every new client stuck in the kernel accept queue (this happened once under load).
- **Telemetry reads on the routing path never raise.** `try_read` gives up after a small retry budget and the router
  skips that worker; a worker descheduled between its two counter writes must not stall routing for everyone.
- **A decoded record is sanity-checked** (`state` in range) before use, because Python gives no memory fences
  and Apple silicon is weakly ordered.

## Not built

- **The shared payload arena** (main interpreter frames L7 requests and passes `(offset, length)`).
  It would put all parsing on the main interpreter's single GIL, the bottleneck the design exists to remove.
  Zero-copy `memoryview` sharing itself is verified and available to users.
- **Thread priority (`os.setpriority`)**: needs `CAP_SYS_NICE`.
- **Gradient2**: replaced by Vegas plus periodic re-probe of the no-load RTT.

## Known limits

- The acceptor is one asyncio loop under the main GIL, so connection *rate* is capped by it (tens of thousands per second).
  Use `routing='reuseport'` on Linux to remove it, at the cost of load awareness.
- Admission is per connection; long-lived keep-alive connections need per-request admission inside the handler
  (`spa_mc.current().limiter`).
- C extensions must support isolated subinterpreters (multi-phase init); many popular ones do not yet.
- Requests already admitted to a worker when it stalls still wait out the stall; the valve and router only protect *new* work.
