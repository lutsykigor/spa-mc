# Probes

Small scripts used to check the source architecture's claims (see [docs/research-verification.md](../../docs/research-verification.md)).

| script | checks | run with |
|---|---|---|
| `parallelism_and_imports.py` | per-interpreter GIL speedup; which stdlib modules import in a subinterpreter | `python3.12 ...` and `python3.14 ...` |
| `memoryview_sharing.py` | zero-copy `memoryview` across interpreters | Python 3.14 |
| `linux_thread_affinity.py` | `sched_setaffinity(0, ...)` pins only the calling (subinterpreter) thread | `docker run --rm -v "$PWD":/w -w /w python:3.14-slim python linux_thread_affinity.py` |
| `torn_reads.py` | raw multi-field reads of the shared slab can tear; seqlock reads do not | `uv run python bench/probes/torn_reads.py` |
