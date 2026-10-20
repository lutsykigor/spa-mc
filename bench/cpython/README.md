# Benchmarking the CPython `asyncio` multi-core branch

The `asyncio-multicore` branch of the CPython fork adds `asyncio.start_server(..., workers=N)`:
the ideas of spa-mc, built into the standard library as an experimental, opt-in mode.
This kit measures it the way the Python community does, on a machine with active cooling.
Run it on Windows (or Linux) with release builds; never benchmark the debug builds used for testing.

It answers two questions:

1. **Does the branch slow down ordinary asyncio?** It should not: without `workers=` the code path is unchanged.
   Measured with [pyperformance](https://github.com/python/pyperformance), the official CPython benchmark suite
   (its `async_*`, `coroutines` and `asyncio_*` benchmarks), comparing the branch with the commit it started from.
2. **What does `workers=N` buy, and what does it cost?** Measured with this kit's load test, comparing one handler
   served by one asyncio loop, by `workers=N`, by N pre-forked processes (the usual multi-process baseline) and,
   optionally, by N threads on a free-threaded build.

## 1. Build both interpreters (PowerShell)

Needs Git and Visual Studio 2022 with *Desktop development with C++*.

```powershell
.\build.ps1 -Root C:\src -FreeThreaded
```

This clones the fork, checks out the branch and the commit it started from side by side, builds both as release
builds (plus a free-threaded build of main with `-FreeThreaded`), and runs `test_asyncio` on both, so a broken build
is caught before anything is measured. It prints the paths of the interpreters.

## 2. No-regression check: pyperformance

```powershell
.\pyperformance.ps1 -Base C:\src\cpython-main\PCbuild\amd64\python.exe `
                    -Branch C:\src\cpython-multicore\PCbuild\amd64\python.exe -Rigorous
```

Expected: every benchmark "not significant". pyperformance installs each benchmark's dependencies into a venv for
the interpreter under test; on a 3.16 alpha some third-party packages (for example the one `asyncio_websockets`
uses) may not build yet. Drop those from `-Benchmarks` and note it.

## 3. What multi-core buys: the load test

Run from a stable Python 3.12+ (the harness itself), pointing at the built interpreters:

```powershell
py -3.14 run.py --python-base C:\src\cpython-main\PCbuild\amd64\python.exe `
                --python-multicore C:\src\cpython-multicore\PCbuild\amd64\python.exe `
                --python-ft C:\src\cpython-main\PCbuild\amd64\python3.16t.exe `
                --workers 4 --rounds 3 --duration 10 --out results-windows.json
```

(Check `PCbuild\amd64` for the exact free-threaded executable name; leave `--python-ft` out to skip it.)

Scenarios, interleaved round by round, each reported as the median of the rounds:

| scenario | what it shows |
|---|---|
| `main, single loop` | the baseline: one event loop, one core |
| `branch, single loop (default path)` | the branch without `workers=`: should equal the line above |
| `main, prefork N processes` | the established way to use N cores: N processes sharing one socket |
| `branch, workers=N` / `workers=auto` | the new mode, fixed and sized to the machine |
| `free-threaded, N threads` | the other route to multiple cores in one process |
| `noisy: ... round_robin` vs `least_loaded` | one worker loses its CPU 60 ms in every 250 ms: does load-aware routing protect the tail? |

Two workloads: `cpu` (about 1 ms of pure-Python work per request, where multiple cores should pay off) and `echo`
(no work at all, which exposes the cost of accepting every connection in the main interpreter and handing it over;
expect the single loop or pre-fork to win there, and report it).

Before each scenario the harness times a calibration loop on one core and on four; if the machine has become more than
10% slower than at the start it rests and checks again, and every result records that factor.

### With external load tools

`server.py` also speaks minimal HTTP, so standard tools can drive it. Each request must use its own connection,
because the multi-core mode closes a connection when the callback returns:

```powershell
py -3.14 server.py workers 4 --protocol http      # prints READY <port>
bombardier -c 64 -d 10s --disableKeepAlives http://127.0.0.1:<port>/
oha -c 64 -z 10s --disable-keepalive http://127.0.0.1:<port>/
```

Run the server with the interpreter under test (for example `C:\src\cpython-multicore\PCbuild\amd64\python.exe server.py ...`).
`wrk`, the tool used in uvloop's benchmarks, does not run on Windows; on Linux use `wrk -c 64 -d 10s -H "Connection: close"`.
uvloop itself does not support Windows, so it is not part of the Windows comparison.

## What to report

For a write-up (for example on discuss.python.org, Async-SIG), include: the machine and OS, the exact commits,
the pyperformance comparison table, this kit's table for both workloads, the trade-offs (the callback must be a
module-level function and does not share state with other workers; the connection closes when the callback returns;
the echo workload's overhead), and the scripts, so anyone can reproduce the numbers.
