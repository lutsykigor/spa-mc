import sys
import textwrap
import threading
import time

print('==', sys.version.split()[0], sys.platform)
mods = {}
for name in ('concurrent.interpreters', '_interpreters', '_xxsubinterpreters', '_xxinterpchannels', '_interpqueues'):
    try:
        __import__(name)
        mods[name] = True
    except ImportError as e:
        mods[name] = f'no ({e})'
print('modules:', mods)

try:
    from concurrent import interpreters as I

    def create():
        return I.create()

    def run(interp, code):
        interp.exec(code)
except ImportError:
    try:
        import _interpreters as R
    except ImportError:
        import _xxsubinterpreters as R

    def create():
        return R.create()

    def run(interp, code):
        r = R.run_string(interp, code)
        if r is not None:
            print('run_string returned', r)


# 1. stdlib imports inside an isolated subinterpreter
interp = create()
for m in ('asyncio', 'socket', 'select', 'struct', 'ctypes', '_ctypes', 'ssl', 'json', 'hashlib'):
    try:
        run(interp, f'import {m}')
        print(f'  import {m}: OK')
    except Exception as e:
        print(f'  import {m}: FAIL {type(e).__name__}: {str(e)[:120]}')

# 2. Parallel CPU speedup (per-interpreter GIL)
CPU = textwrap.dedent("""
    s = 0
    for i in range(6_000_000):
        s += i * i
""")


def bench(n, use_interp):
    interps = [create() for _ in range(n)] if use_interp else None

    def work(k):
        if use_interp:
            run(interps[k], CPU)
        else:
            exec(CPU, {})

    ts = [threading.Thread(target=work, args=(k,)) for k in range(n)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return time.perf_counter() - t0


one = bench(1, True)
for n in (4,):
    ti = bench(n, True)
    tt = bench(n, False)
    print(
        f'  CPU x{n}: 1 interp={one:.2f}s  '
        f'{n} subinterp threads={ti:.2f}s (speedup {n * one / ti:.2f}x)  '
        f'{n} plain threads={tt:.2f}s (speedup {n * one / tt:.2f}x)'
    )
